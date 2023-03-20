#pragma once

#include <capnp/message.h>
#include <capnp/serialize-packed.h>

#include <algorithm>
#include <functional>
#include <map>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <atomic>

#include "index/pbtree.h"
#include "rpc/SNinterface.capnp.h"
#include "servers/config.h"
#include "storage/multi_ver_record.h"
#include "storage/page_manager.h"
#include "storage/segment.h"
#include "txn/batch_manager.h"
#include "txn/redo_log.h"
#include "util/exceptions.h"
#include "util/macros.h"
#include "util/memory_arena.h"
#include "util/timer.h"
#include "util/types.h"
#include "util/txn_lat.h"

using txn_kv_t = std::unordered_map<txn_id_t, uint8_t*>;
using batch_kv_t = std::unordered_map<RedoLogKvKey, txn_kv_t, RedoLogKvKeyHasher>;

struct TxnSegInfo {
    TxnSegInfo(seg_id_t seg_id) : seg_id(seg_id) {}
    seg_id_t seg_id;
    // We assume the page cache being accessed by a transaction can not be evicted.
    std::unordered_map<g_page_id_t, PageSnapshot> pages;
    uint32_t num_reads = 0, num_writes = 0;
};
struct TxnSnInfo {
    msg_id_t msg_id = NULL_MSG_ID;
    std::unordered_set<TxnSegInfo *> sn_seg_infos;
};

struct TxnRecord {
    TxnRecord(uint32_t table_id, PageMeta *page_meta, offset_t record_offset, const void *value,
              txn_id_t writer, uint32_t value_size, uint8_t* record_buf, 
              uint8_t* prev_value_buf = nullptr, const void *prev_value = nullptr)
        : g_page_id(page_meta->gp_id.g_page_id),
          page_meta(page_meta),
          value_size(value_size),
          record_page_offset(record_offset),
          record_buf(record_buf),
          prev_value_buf(prev_value_buf),
          table_id(table_id),
          used(false),
          is_write(value != nullptr) {        
        uint8_t* record = get_record_ptr();
        memcpy(record_buf, r_get_key_ptr(record) , KEY_SIZE);
        if (!is_write) {
            // read operation
read_retry:
            writer = r_get_writer(record);
            asm volatile("" ::: "memory");
            if(unlikely(writer == 1)) {
                goto read_retry;
            }

            set_value(r_get_value(record));
            asm volatile("" ::: "memory");
            if(unlikely(writer != r_get_writer(record))) {
                goto read_retry;
            }
        } else {
            // write operation
            txn_id_t ori_writer;
write_retry:
            ori_writer = r_get_writer(record);
            asm volatile("" ::: "memory");
            if(unlikely(ori_writer == 1)) {
                goto write_retry;
            }

            set_prev_value(r_get_value(record));
            asm volatile("" ::: "memory");
            if(unlikely(ori_writer != r_get_writer(record))) {
                goto write_retry;
            }
            set_prev_writer(ori_writer);
            set_value(value);
        }
        set_version(writer);
    }

    // TxnRecord(TxnRecord &&other) = delete;
    // TxnRecord(const TxnRecord &other) = delete;
    TxnRecord(TxnRecord &&other) { memcpy(this, &other, sizeof(TxnRecord)); }
    TxnRecord(const TxnRecord &other) { memcpy(this, &other, sizeof(TxnRecord)); }

    ~TxnRecord() {}
    inline uint32_t get_value_size() const { return value_size; }

    inline uint8_t *get_record_ptr() const { return page_meta->get_data() + record_page_offset; }
    inline uint8_t *get_buf() { return record_buf; }
    inline uint8_t *get_prev_buf() { return prev_value_buf; }
    
    inline const uint8_t *get_key_buf() const { return record_buf; }
    inline uint8_t *key_buf() { return record_buf; }
    
    inline const uint8_t *get_value_buf() const { return record_buf + KEY_SIZE; }
    inline uint8_t *value_buf() { return record_buf + KEY_SIZE; }
    template <class Value_t>
    Value_t *get_value_ptr_as() {
        return reinterpret_cast<Value_t *>(value_buf());
    }

    inline txn_id_t get_writer() const {
        return *reinterpret_cast<const txn_id_t *>(get_value_buf() + value_size);
    }

    inline void set_value(const void* value) { 
        memcpy(value_buf(), value, value_size); 
    }

    inline void set_prev_value(const void* value) { 
        memcpy(prev_value_buf, value, value_size); 
    }

    inline void set_prev_writer(txn_id_t writer) { 
        *((txn_id_t*)(prev_value_buf + value_size)) = writer; 
    }

    inline txn_id_t get_prev_writer() { 
        return *((txn_id_t*)(prev_value_buf + value_size)); 
    }

    inline void set_version(txn_id_t new_version) { 
        memcpy(value_buf() + value_size, &new_version, sizeof(txn_id_t));
    }

    inline Key_t get_key() { 
        return *reinterpret_cast<Key_t *>(key_buf());
    }

    inline bool is_local_version() { return get_writer() != NULL_TXN_ID; }
    inline void use() { this->used = true; }
    inline bool is_used() const { return this->used; }

    static uint32_t record_buf_size(uint32_t value_size) {
        return sizeof(Key_t) + value_size + sizeof(txn_id_t) + sizeof(version_ts_t);
    }

    g_page_id_t g_page_id;
    PageMeta *page_meta;
    uint32_t value_size;
    offset_t record_page_offset;
    uint8_t* record_buf;
    uint8_t* prev_value_buf;
    uint32_t table_id;

    bool used;  // A flag indicate whether the record is used in repair phase
    bool is_write;
};

class BatchTxn;

struct DbClient {
    uint64_t id;
    uint64_t key;
    uint64_t txn_seed;

    DbClient(uint64_t id, uint64_t key, uint64_t txn_seed) : id(id), key(key), txn_seed(txn_seed) {}
};

enum RepairStatus {
    VALID = 0,
    REPAIRED,
    NOT_LOCAL
};

class DbTxn {
   public:
    enum DbTxnStatus {
        BEFORE_EXECUTION = 0,
        EXECUTING,
        WAIT_TIMESTAMP,
        WAIT_PREPARE_RESPONSE,
        WAIT_COMMIT_RESPONSE,
        FINISHED
    };
    
    // return value of exec_logic: false if the txn has potential contention access; true otherwise
    using txn_logic_func = std::function<bool(DbTxn *txn, thread_id_t worker_id)>;
    using txn_logic_t = std::array<txn_logic_func, 3>;
    using partition_logic_func = std::function<bool(table_id_t, node_id_t)>;
    using remote_read_func = std::function<void(DbTxn*, std::unordered_map<g_page_id_t, std::vector<offset_t>>)>;

    DbClient *client = nullptr;
    int txn_type = -1;
    uint32_t txn_seed;
    uint32_t txn_input_size;

    PageManager &page_manager;
    const Configuration &conf;
    txn_id_t txn_id = 1;
    void *input;
    uint64_t msg_count;
    uint32_t primary_sn_id;
    BatchManager *batch_manager;
    bool is_repaired = false;
    bool cur_phase_success;
#ifdef LOCK_FAIL_BLOCK
    bool blocked = false;
#endif
    bool fastCommitEnabled = true;
    bool repaired = false;
    volatile bool released = false;
    MultiThreadTimer p_timer;
#ifdef TXN_LAT_STAT
    TxnLatencyTimer timer;
#endif

    std::vector<std::pair<g_page_id_t, PageSnapshot>> pages;
    std::vector<TxnRecord> read_set, write_set, repair_write_set;
    std::unordered_map<g_page_id_t, TxRedoLog *> redo_logs;
    std::set<RecordLock *> hold_locks, hold_batch_locks;
    BatchCache *batch_cache = nullptr;
    void *batch;
    batch_id_t gather_batch_num = 0;
    capnp::List<ReadSet, capnp::Kind::STRUCT>::Reader sn_read_set;
    std::vector<uint8_t*> record_bufs;
    std::unordered_map<txn_id_t, bool> *prev_txn_in_batch_is_repaired;
    batch_kv_t* batch_kv;
   public:
   
    std::unordered_map<g_page_id_t, std::vector<offset_t>> pages_for_remote_read;
    txn_logic_func execution_logic;
    txn_logic_func repair_logic;
    txn_logic_func gc_logic;
    remote_read_func remote_read_logic;
    partition_logic_func partition_logic;
    std::mutex mutex;
    uint32_t txn_i;
    uint32_t phase = 0;
    void* temp_buf = nullptr;
#ifdef BREAKDOWN_TRAVERSING
    uint64_t total_traversing_nsec = 0;
#endif
#ifdef LOCK_FAIL_BLOCK
    uint32_t part_id;
#endif

    DbTxn(PageManager &page_manager, txn_logic_t txn_logic, void *input,
          partition_logic_func partition_logic)
        : page_manager(page_manager),
          conf(page_manager.get_conf()),
          input(input),
          execution_logic(txn_logic[0]),
          repair_logic(txn_logic[1]),
          gc_logic(txn_logic[2]),
          partition_logic(partition_logic) {
        // Begin count for tranasction latency
#ifdef TXN_LAT_STAT
        timer.start();
#endif
        // page_meta[0] = page_meta[1] = nullptr;
    }

#ifdef TXN_LAT_STAT
    void stopTimer(){
        // Stop timer and record latency for current transaction
        timer.end(txn_type);
    }
#endif

    ~DbTxn() {
        gc_buf();
        clear_access_info();
        free_redo_logs();
    }

   public:
    // TODO: store txn type (e.g. new_order, delivery, ...) here
    inline int get_txn_seed() { return txn_seed; }

    inline void *get_input_ptr() const { return input; }

    template <class Input_t>
    Input_t *get_input() const {
        return reinterpret_cast<Input_t *>(input);
    }

    template <class Input_t>
    Input_t &get_input_as_ref() const {
        return *reinterpret_cast<Input_t *>(input);
    }

    template <class Temp_t>
    Temp_t* get_or_new_temp() {
        if (phase == 0) {
            if (temp_buf) {
                delete_temp<Temp_t>();
            }
            temp_buf = new Temp_t;
        }
        return reinterpret_cast<Temp_t*>(temp_buf);

    }

    template <class Temp_t>
    void delete_temp() {
        delete reinterpret_cast<Temp_t*>(temp_buf);
        temp_buf = nullptr;
    }

    // only for retry txn
    inline txn_id_t get_txn_id() { return txn_id; }

    inline void set_txn_id(txn_id_t new_txn_id) { txn_id = new_txn_id; }

    inline uint8_t* alloc_buf(uint32_t size) {
        uint8_t* buf = new uint8_t[size];
        record_bufs.push_back(buf);
        return buf;
    }

    inline void gc_buf() {
        for (uint8_t* buf : record_bufs) {
            delete [] buf;
        }
    }

    template <class Key_t, class Value_t>
    Value_t get_ro(IndexInterface<Key_t, Value_t> &table, const Key_t &key) {
        auto &leaf = table.get_leaf_node(key);
        // PageMeta *page_meta = leaf.get_page_meta();
        auto *record = leaf.get(key);
        
        uint8_t* ret_data = record->value;
        return *reinterpret_cast<Value_t *>(ret_data);
    }

    template <class Key_t, class Value_t>
    Value_t get_print(IndexInterface<Key_t, Value_t> &table, const Key_t &key, bool use_lock, bool write_lock) {
        auto &leaf = table.get_leaf_node(key);
        PageMeta *page_meta = leaf.get_page_meta();
        access_page(page_meta, false);
        auto *record = leaf.get(key);
        // ASSERT(record != nullptr) << "key doesn't exist: " << key;

#ifndef NO_LOCK
        if (use_lock) {
            RecordLock *rl = &record->lock;
            LockStatus status;
            if (write_lock) {
                status = rl->lock(txn_id, this->gather_batch_num);
            } else {
                status = rl->read_lock(txn_id, this->gather_batch_num);
            }
            // LOG(2) << "txn lock1" << key << " " << std::hex << txn_id << " " << rl;
            handle_lock_status(rl, status);
        }        
#endif

        offset_t record_offset = page_meta->get_offset(record);
        uint32_t value_size = page_meta->get_value_size();
        uint8_t* record_buf = alloc_buf(TxnRecord::record_buf_size(value_size));
        
        read_set.emplace_back(table.get_table_id(), page_meta, record_offset, nullptr,
                              0, value_size, record_buf);
        LOG(2) << std::hex << txn_id << ", get key: " << key << ", page: " << page_meta->gp_id.seg_id 
            << " " << page_meta->gp_id.page_id << " " << read_set.back().get_writer();

        uint8_t* ret_data = read_set.back().value_buf();
#if defined(EMULATE_AURORA) && defined(READ_COMMITTED)
        // if the value is in the write set, we have to read that new value.
        ValVersion vv;
        if (batch_cache->try_get(key, vv)) {
            ret_data = vv.val;
            read_set.back().set_version(vv.version);
        }
#endif
        // LOG(2) << "get: " << key << " " << *reinterpret_cast<Value_t *>(read_set.back().get_value_buf());
        return  *reinterpret_cast<Value_t *>(ret_data);
    }


    template <class Key_t, class Value_t>
    Value_t get(IndexInterface<Key_t, Value_t> &table, const Key_t &key, bool use_lock, bool write_lock) {
        auto &leaf = table.get_leaf_node(key);
        PageMeta *page_meta = leaf.get_page_meta();
        access_page(page_meta, false);
        auto *record = leaf.get(key);
        // ASSERT(record != nullptr) << "key doesn't exist: " << key;
        // LOG(2) << "get key: " << key << ", page: " << page_meta->gp_id.g_page_id;

#ifndef NO_LOCK
        if (use_lock) {
            RecordLock *rl = &record->lock;
            LockStatus status;
            if (write_lock) {
                status = rl->lock(txn_id, this->gather_batch_num);
            } else {
                status = rl->read_lock(txn_id, this->gather_batch_num);
            }
            // LOG(2) << "txn lock1" << key << " " << std::hex << txn_id << " " << rl;
            handle_lock_status(rl, status);
        }        
#endif

        offset_t record_offset = page_meta->get_offset(record);
        uint32_t value_size = page_meta->get_value_size();
        uint8_t* record_buf = alloc_buf(TxnRecord::record_buf_size(value_size));
        
        read_set.emplace_back(table.get_table_id(), page_meta, record_offset, nullptr,
                              0, value_size, record_buf);
        
        uint8_t* ret_data = read_set.back().value_buf();
#if defined(EMULATE_AURORA) && defined(READ_COMMITTED)
        // if the value is in the write set, we have to read that new value.
        ValVersion vv;
        if (batch_cache->try_get(key, vv)) {
            ret_data = vv.val;
            read_set.back().set_version(vv.version);
        }
#endif
        // LOG(2) << "get: " << key << " " << *reinterpret_cast<Value_t *>(read_set.back().get_value_buf());
        return  *reinterpret_cast<Value_t *>(ret_data);
    }

    template <class Key_t, class Value_t>
    Value_t remote_get_ro(IndexInterface<Key_t, Value_t> &table, const Key_t &key) {
        auto &leaf = table.get_leaf_node(key);
        PageMeta *page_meta = leaf.get_page_meta();
        auto *record = leaf.get(key);

        std::vector<offset_t>& offsets = pages_for_remote_read[page_meta->gp_id.g_page_id];
        offset_t record_offset = page_meta->get_offset(record);
        offsets.push_back(record_offset);

        uint8_t* ret_data = record->value;
        return *reinterpret_cast<Value_t *>(ret_data);
    }

    template <class Key_t, class Value_t>
    Value_t ts_get(IndexInterface<Key_t, Value_t> &table, const Key_t &key, bool use_lock, bool write_lock) {
        auto &leaf = table.get_leaf_node(key);
        PageMeta *page_meta = leaf.get_page_meta();
        access_page(page_meta, false);
        return Value_t();
    }

    template <class Key_t, class Value_t>
    Value_t remote_get(IndexInterface<Key_t, Value_t> &table, const Key_t &key, bool use_lock, bool write_lock) {
        auto &leaf = table.get_leaf_node(key);
        PageMeta *page_meta = leaf.get_page_meta();
        access_page(page_meta, false);
        auto *record = leaf.get(key);

#ifndef NO_LOCK
        if (use_lock) {
            RecordLock *rl = &record->lock;
            LockStatus status;
            if (write_lock) {
                status = rl->lock(txn_id, this->gather_batch_num);
            } else {
                status = rl->read_lock(txn_id, this->gather_batch_num);
            }
            // LOG(2) << "txn lock2 " << key << " " << std::hex << txn_id << " " << rl << " " << gather_batch_num;
            handle_lock_status(rl, status);
        }        
#endif

        offset_t record_offset = page_meta->get_offset(record);
        uint32_t value_size = page_meta->get_value_size();
        uint8_t* record_buf = alloc_buf(TxnRecord::record_buf_size(value_size));
        
        read_set.emplace_back(table.get_table_id(), page_meta, record_offset, nullptr,
                              0, value_size, record_buf);
        
        uint8_t* ret_data = read_set.back().value_buf();
#if defined(EMULATE_AURORA) && defined(READ_COMMITTED)
        // if the value is in the write set, we have to read that new value.
        ValVersion vv;
        if (batch_cache->try_get(key, vv)) {
            ret_data = vv.val;
            read_set.back().set_version(vv.version);
        }
#endif

        std::vector<offset_t>& offsets = pages_for_remote_read[page_meta->gp_id.g_page_id];
        offsets.push_back(record_offset);

        return  *reinterpret_cast<Value_t *>(ret_data);
    }

    void read_remote() {
        remote_read_logic(this, pages_for_remote_read);
    }

    inline void handle_lock_status(RecordLock* rl, LockStatus status) {
        if (status != LockStatus::SUCCESS) {
#ifdef READ_COMMITTED
            if (status == LockStatus::RECORD_FAIL) {
                rl->unlock_batch(gather_batch_num);
            }
#endif
           throw TxnAbortException();
        }
        hold_locks.insert(rl);
#ifdef READ_COMMITTED
        hold_batch_locks.insert(rl);
#endif
    }

    template <class Value_t>
    std::vector<Value_t> remote_scan(IndexInterface<Key_t, Value_t> &table,
                        const Key_t &lo, const Key_t &hi) {
        auto leaves = table.get_leaf_nodes(lo, hi);

        std::vector<Value_t> results;
        for (auto leaf : leaves) {
            std::vector<MultiVersionRecord *> node_result 
                = leaf->scan_oneshot(lo, hi, true, false);
            if (node_result.size() == 0) continue;
            PageMeta *page_meta = leaf->get_page_meta();
            access_page(page_meta, false);
            std::vector<offset_t>& offsets = pages_for_remote_read[page_meta->gp_id.g_page_id];
            uint32_t value_size = page_meta->get_value_size();
            for (MultiVersionRecord* record : node_result) {
                offset_t record_offset = page_meta->get_offset(record);
                uint8_t* record_buf = alloc_buf(TxnRecord::record_buf_size(value_size));
            
#ifndef NO_LOCK
                RecordLock *rl = &record->lock;
                LockStatus status = rl->read_lock(txn_id, this->gather_batch_num);
                handle_lock_status(rl, status);
#endif

                offsets.push_back(record_offset);

                read_set.emplace_back(table.get_table_id(), page_meta, record_offset, nullptr,
                              0, value_size, record_buf);
                results.push_back(*reinterpret_cast<Value_t *>(read_set.back().value_buf()));
            }
        }
        return results;
    }

    template <class Value_t>
    std::vector<Value_t> scan(IndexInterface<Key_t, Value_t> &table,
                        const Key_t &lo, const Key_t &hi) {
        auto leaves = table.get_leaf_nodes(lo, hi);

        std::vector<Value_t> results;
        for (auto leaf : leaves) {
            std::vector<MultiVersionRecord *> node_result 
                = leaf->scan_oneshot(lo, hi, true, false);
            if (node_result.size() == 0) continue;
            PageMeta *page_meta = leaf->get_page_meta();
            access_page(page_meta, false);
            uint32_t value_size = page_meta->get_value_size();
            for (MultiVersionRecord* record : node_result) {
                offset_t record_offset = page_meta->get_offset(record);
                uint8_t* record_buf = alloc_buf(TxnRecord::record_buf_size(value_size));
            
#ifndef NO_LOCK
                RecordLock *rl = &record->lock;
                LockStatus status = rl->read_lock(txn_id, this->gather_batch_num);
                handle_lock_status(rl, status);
#endif

                read_set.emplace_back(table.get_table_id(), page_meta, record_offset, nullptr,
                              0, value_size, record_buf);
                results.push_back(*reinterpret_cast<Value_t *>(read_set.back().value_buf()));
            }
        }
        return results;
    }

    template <class Key_t, class Value_t>
    void put(IndexInterface<Key_t, Value_t> &table, const Key_t &key, const Value_t &value) {
        // ASSERT(conf.get_my_node_type() == Configuration::NodeType::DB);
        // if (table.wait_range_lock(key) == false) {
        //     ASSERT(false);
        //     // throw TxnRangeLockAbortException();
        // }
        // LatencyLogger::start(LatencyType::DBTXN_PUT1);
        auto &leaf = table.get_leaf_node(key);
        // LatencyLogger::end(LatencyType::DBTXN_PUT1);

        PageMeta *page_meta = leaf.get_page_meta();
        access_page(page_meta, true);
        auto *record = leaf.get(key);

#ifndef NO_LOCK
        RecordLock *rl = &record->lock;
        if (hold_locks.find(rl) == hold_locks.end()) {
            LockStatus status = rl->lock(txn_id, this->gather_batch_num);
            handle_lock_status(rl, status);
        }
#endif

        offset_t record_offset = page_meta->get_offset(record);
        uint32_t value_size = sizeof(Value_t);
        uint8_t* record_buf = alloc_buf(TxnRecord::record_buf_size(value_size));
        uint8_t* prev_value_buf = alloc_buf(value_size + sizeof(version_ts_t));

        write_set.emplace_back(table.get_table_id(), page_meta, record_offset, &value,
                            txn_id, value_size, record_buf, prev_value_buf, &record->value);
    }

    template <class Key_t, class Value_t>
    void ts_put(IndexInterface<Key_t, Value_t> &table, const Key_t &key, const Value_t &value) {
        auto &leaf = table.get_leaf_node(key);
        // LatencyLogger::end(LatencyType::DBTXN_PUT1);

        PageMeta *page_meta = leaf.get_page_meta();
        access_page(page_meta, true);
    }

    template <class Key_t, class Value_t>
    Value_t sn_repair_get(IndexInterface<Key_t, Value_t> &table, const Key_t &key,
                            uint32_t code_loc, RepairStatus& r_status) {
        table_id_t table_id = table.get_table_id();
        if (!partition_logic(table_id, primary_sn_id)) {  // if this table is not in this SN
            r_status = NOT_LOCAL;
            return Value_t{};
        }
#ifdef BREAKDOWN_TRAVERSING
            p_timer.start();
#endif
        auto &leaf = table.get_leaf_node(key);
        auto *pair = leaf.get(key);
        PageMeta* page_meta = leaf.get_page_meta();
        seg_id_t seg_id = page_meta->gp_id.seg_id;

        // for (auto record : sn_read_set) {
        {
            auto record = sn_read_set[code_loc];
            ASSERT (record.getOffset() == page_meta->get_offset(pair)) 
                << seg_id << " " << page_meta->gp_id.page_id 
                << " " << record.getOffset() << " " << page_meta->get_offset(pair)
                << " " << key;
            txn_id_t writer = record.getWriter();
            txn_id_t writer_in_storage = pair->writer;
            if (prev_txn_in_batch_is_repaired->find(writer) !=
                prev_txn_in_batch_is_repaired->end()) {
                // in-batch dependency
                txn_id_t prev_writer_id_if_repaired = TxnIdWrapper::set_repaired(writer);
                if (prev_writer_id_if_repaired == writer_in_storage) {
                    // LOG(2) << std::hex << prev_writer_id_if_repaired << " " << writer_in_storage;
                    // the previous txn was repaired
                    r_status = REPAIRED;
#ifdef BREAKDOWN_TRAVERSING
                    total_traversing_nsec += p_timer.passed_nsec();
#endif
                    return *reinterpret_cast<Value_t*>(pair->value);
                } else {
                    uint8_t* ptr = batch_kv->at({seg_id, key}).at(writer);
                    r_status = VALID;
#ifdef BREAKDOWN_TRAVERSING
                    total_traversing_nsec += p_timer.passed_nsec();
#endif
                    return *reinterpret_cast<const Value_t *>(ptr);
                }
            } else {
                if (writer == writer_in_storage) {
                    r_status = VALID;
                } else {
                    // LOG(2) << std::hex << writer << " " << writer_in_storage << " " << txn_id;
                    r_status = REPAIRED;
                }
#ifdef BREAKDOWN_TRAVERSING
                total_traversing_nsec += p_timer.passed_nsec();
#endif
                return *reinterpret_cast<Value_t*>(pair->value);
            }
        }
        ASSERT(false) << "not found " << key;
        return *reinterpret_cast<Value_t*>(pair->value);
    }
    template <class Key_t, class Value_t>
    void sn_repair_put(IndexInterface<Key_t, Value_t> &table, const Key_t &key,
                       const Value_t &value, bool repair) {
        if (!repair)
            return;
        auto &leaf = table.get_leaf_node(key);
        PageMeta *page_meta = leaf.get_page_meta();
        auto *pair = leaf.get(key);
        pair->set_value(&value, sizeof(Value_t));
        pair->writer = TxnIdWrapper::set_repaired(txn_id);
        offset_t record_offset = page_meta->get_offset(pair);
        
        uint32_t value_size = page_meta->get_value_size();
        uint8_t* record_buf = alloc_buf(TxnRecord::record_buf_size(value_size));
        uint8_t* prev_value_buf = alloc_buf(value_size + sizeof(version_ts_t));
        write_set.emplace_back(table.get_table_id(), page_meta, record_offset, &value,
                               pair->writer, value_size, record_buf, prev_value_buf, &pair->value);
    }

    // TODO: re-write this function to read from redo-logs, not from the table
    // TODO: update ts
    template <class Key_t, class Value_t>
    Value_t repair_get(IndexInterface<Key_t, Value_t> &table, const Key_t &key, 
            uint32_t code_loc, RepairStatus& r_status) {
#ifndef EMULATE_AURORA
        if (this->fastCommitEnabled)
            return sn_repair_get(table, key, code_loc, r_status);
        uint32_t table_id = table.get_table_id();
        r_status = VALID;
        // for (auto &record : read_set) {
#ifdef BREAKDOWN_TRAVERSING
        p_timer.start();
#endif
        {
            auto &record = read_set[code_loc];
            ASSERT (record.table_id == table_id && record.get_key() == key);
            PageMeta *page_meta = page_manager.get_page_from_cache(record.g_page_id);
            access_page(page_meta, false, true);
            seg_id_t seg_id = GlobalPageId(record.g_page_id).seg_id;
            // get value from record buffer
            uint8_t *value_ptr = record.value_buf();

            // if this key appears in the redo-log, we might need to repair it
            // LOG(2) << "repair_get: " << batch_manager->redo_log_kv.size();
            auto iter = batch_manager->redo_log_kv.find({seg_id, key});
            if (iter != batch_manager->redo_log_kv.end()) {
                uint8_t *new_value_ptr = iter->second.val;
                version_ts_t new_version = iter->second.version;

                if (record.get_writer() != new_version) {
                    value_ptr = new_value_ptr;
                    r_status = REPAIRED;
                }
            }
#ifdef BREAKDOWN_TRAVERSING
            total_traversing_nsec += p_timer.passed_nsec();
#endif
            return *reinterpret_cast<Value_t *>(value_ptr);
        }
        ASSERT(false);
#endif
        return get(table, key, false, false);
    }

    // TODO: update ts
    template <class Key_t, class Value_t>
    void repair_put(IndexInterface<Key_t, Value_t> &table, const Key_t &key, const Value_t &value,
                    bool repair) {
// prevent compile errors
#ifndef EMULATE_AURORA
        if (this->fastCommitEnabled)
            return sn_repair_put(table, key, value, repair);
        uint32_t table_id = table.get_table_id();
        for (auto &record : write_set) {
            if (table_id == record.table_id && record.get_key() == key) {
                PageMeta *page_meta = page_manager.get_page_from_cache(record.g_page_id);

                if (repair) {
                    access_page(page_meta, true, true);
                    record.set_value(&value);
                    version_ts_t new_version = TxnIdWrapper::set_repaired(txn_id);
                    record.set_version(new_version);
                    repair_write_set.push_back(record);

                    // update redo log, do not inplace update
                    seg_id_t seg_id = GlobalPageId(record.g_page_id).seg_id;
                    batch_manager->redo_log_kv[{seg_id, key}] = {record.value_buf(), new_version};
                }
                record.use();
                return;
            }
        }
        ASSERT(false);
        put(table, key, value);
#endif
    }

   public:
#ifdef READ_COMMITTED
    void release_batch_locks() {
        for (RecordLock *rl : hold_batch_locks) {
            // LOG(2) << "batch unlock " << rl << " " << std::hex << " " << txn_id << " " << this->gather_batch_num;
            rl->unlock_batch(gather_batch_num);
        }
        hold_batch_locks.clear();
    }
#endif

    void release_locks() {
        // LOG(2) << "Release locks: " << std::hex << txn_id;
        for (RecordLock *rl : hold_locks) {
            rl->unlock(txn_id);
        }
        hold_locks.clear();

        released = true;
    }

    void local_commit() {
        for (auto &record : write_set) {
            auto pair = record.get_record_ptr();
            // copy value and writer
            if (r_get_writer(pair) == record.get_prev_writer()) {
                set_writer(pair, 1);
                asm volatile("" ::: "memory");
                set_value(pair, record.value_buf(), record.get_value_size());
                asm volatile("" ::: "memory");
                set_writer(pair, record.get_writer());
            } 
            // else {
            //     LOG(2) << "different prev_writer: " << std::hex << record.get_prev_writer();
            // }
        }
        // LOG(2) << "commit: " << std::hex<< txn_id;
    }

    void sn_local_commit() {
        for (auto &record : write_set) {
            auto pair = record.get_record_ptr();
            // copy value and writer
            set_value(pair, record.value_buf(), record.get_value_size());
            set_writer(pair, record.get_writer());
        }
    }

    void roll_back() {
        for (int i = write_set.size() - 1; i >= 0; --i) {
            auto& record = write_set[i];
            auto pair = record.get_record_ptr();
            // LOG(2) << "rollback " << record.get_key() << " " << record.page_meta->gp_id.seg_id << " " 
            //     << record.record_page_offset << " " << std::hex << txn_id << " to " << record.get_prev_writer();
            if (r_get_writer(pair) == txn_id) {
                // LOG(2) << "success";
                // copy value and writer
                set_value(pair, record.get_prev_buf(), record.get_value_size());
                set_writer(pair, record.get_prev_writer());
            }
        }
    }

    void clear_all() {
        clear_access_info();
        read_set.clear();
        write_set.clear();
        repair_write_set.clear();
        // deps.clear();
        for (auto &log : redo_logs) {
            delete log.second;
        }
        redo_logs.clear();
        // LOG(2) << VAR(hold_locks.size());
        hold_locks.clear();
        released = false;
        phase = 0;
#ifdef BREAKDOWN_TRAVERSING
        total_traversing_nsec = 0;
#endif
    }

   private:
    inline bool record_in_use(TxnRecord &rec) {
        return !this->is_repaired || rec.is_used();
    }

    void clear_access_info() { pages.clear(); }
    
    void access_page(PageMeta *page_meta, bool is_write, bool is_repair = false) {
        g_page_id_t g_page_id = page_meta->gp_id.g_page_id;
        PageSnapshot *snapshot;
        bool found = false;
        for (auto &page_iter : pages) {
            if (page_iter.first == g_page_id) {
                snapshot = &page_iter.second;
                found = true;
                break;
            }
        }
        if (!found) {
            PageSnapshot tmp;
            pages.push_back(std::make_pair(g_page_id, tmp));
            snapshot = &pages.back().second;
        }

        if (is_write) {
            snapshot->is_write = true;
        } else {
            snapshot->is_read = true;
        }
    }

    void free_redo_logs() {
        for (auto &iter : redo_logs) {
            delete iter.second;
        }
        redo_logs.clear();
    }

   private:

    void pack_commit_req(CommitArgs::Builder req) {
        req.setTxnId(txn_id);
        req.setCommit(true);
    }

    friend class DatabaseNode;
    friend class BatchTxn;
};
