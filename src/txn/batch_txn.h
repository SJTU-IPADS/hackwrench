#pragma once

#include "benchmarks/interface.h"
#include "rpc/rpc.h"
#include "txn/batch_manager.h"
#include "txn/db_txn.h"
#include "util/memory_arena.h"
#include "util/static_hash_map.h"
#include "util/timer.h"

#include <map>

#ifdef WORKLOAD_TPCC
#define SEGS_BUFFER_SIZE 4096
#else
#define SEGS_BUFFER_SIZE 128
#endif
class BatchSegInfo {
   public:
    BatchSegInfo(seg_id_t seg_id) : seg_id(seg_id) {}
    BatchSegInfo() : seg_id(0) {}
    seg_id_t seg_id;
    // We assume the page cache being accessed by a transaction can not be evicted.
    std::unordered_map<g_page_id_t, BatchSnapshot> pages;
    uint32_t num_reads = 0, num_writes = 0;
    TxnDependency dep;
    inline void reset(seg_id_t seg_id) {
        this->seg_id = seg_id;
        pages.clear();
        num_reads = num_writes = 0;
    }
    inline void clear() { reset(0); }
};

class BatchSnInfo {
   public:
    BatchSnInfo() {
        rp_msg_counter = 0;
    }
#ifdef DETERMINISTIC_VALIDATION
    uint64_t seq;
#endif
    size_t sn_seg_infos_size = 0;
    std::vector<DbTxn *> fast_txns;
    std::atomic<uint> rp_msg_counter;
    inline void clear() { 
        sn_seg_infos_size = 0;
        rp_msg_counter = 0;
        fast_txns.clear();
#ifdef DETERMINISTIC_VALIDATION
        seq = 0;
#endif
    }
};

class LargeBatch;
class BatchTxn {
    friend std::ostream &operator<<(std::ostream &os, const DbTxn &txn);

   public:
    enum BatchStatus {
        INIT = 0,
        WAIT_TIMESTAMP,
        SEND_TIMESTAMP
    };

    using batch_logic_func = std::function<void(BatchTxn *txn, uint8_t *key_ptr)>;

    MultiThreadTimer timer;
    std::mutex mutex;
    std::vector<DbTxn *> ordered_txns;
    std::vector<MsgBuffer> msg_bufs;
    std::vector<PrepareResponse::Reader> resps;

    BatchStatus cur_phase = BatchStatus::INIT;

    batch_id_t batch_id;

    PageManager &page_manager;
    const Configuration &conf;
    BatchManager batch_manager;

    std::atomic<uint64_t> msg_count;
    std::atomic<bool> sending;

    const uint32_t NUM_TXN_PER_BATCH;
    uint64_t seq = 0;

    bool cur_phase_success;
    bool is_repaired;
    bool is_aborted;
    bool fastCommitEnabled = true;  // sn repair
    bool clients_returned = false;

    LargeBatch *lbatch = nullptr;

    BatchSnInfo *sn_infos[MAX_SN_NUM];
    std::mutex mu;
#ifdef USE_STATIC_HASHMAP
    static_unordered_map<seg_id_t, BatchSegInfo *, SEGS_BUFFER_SIZE> seg_infos;
#else
    std::map<seg_id_t, BatchSegInfo *> seg_infos;
#endif

    // std::unordered_map<seg_id_t, TxnDependency> deps;
#ifdef USE_STATIC_HASHMAP
    static_unordered_map<g_page_id_t, TxRedoLog *, SEGS_BUFFER_SIZE> redo_logs;
#else
    std::unordered_map<g_page_id_t, TxRedoLog *> redo_logs;
#endif

    BatchTxn(PageManager &page_manager, thread_id_t num_threads, uint32_t batching_size = 100)
        : page_manager(page_manager),
          conf(page_manager.get_conf()),
          NUM_TXN_PER_BATCH(batching_size),
          is_repaired(false),
          is_aborted(false) {
        //   acquire_lock_logic(acquire_lock_logic) {
        ordered_txns.reserve(NUM_TXN_PER_BATCH);
#ifdef ABORT_ON_FAIL
        fastCommitEnabled = false;
#endif
        memset(sn_infos, 0, sizeof(sn_infos));
    }

    ~BatchTxn() {
        for (auto txn : ordered_txns) {
            if (!txn) {
                continue;
            }
            txn->clear_access_info();
        }
        clear_access_info();
        free_redo_logs();
        msg_bufs.clear();
        resps.clear();
        memset(sn_infos, 0, sizeof(sn_infos));
    }

    void clear() {
        is_repaired = false;
        is_aborted = false;
        ordered_txns.clear();
        ordered_txns.reserve(NUM_TXN_PER_BATCH);
        for (const auto &sn_iter : sn_infos) {
            delete sn_iter;
        }
        memset(sn_infos, 0, sizeof(sn_infos));
    }

    void clear_access_info() {
        for (const auto &seg_iter : seg_infos) {
            delete seg_iter.second;
        }
        seg_infos.clear();

        for (auto *sn_iter : sn_infos) {
            if (sn_iter != nullptr) {
                sn_iter->clear();
            }
        }
    }

    void free_redo_logs() {
        for (auto &iter : redo_logs) {
            redoLogArena.free(iter.second);
        }
        redo_logs.clear();
    }

    void attach_msg_buffer(MsgBuffer& zmq_msg, PrepareResponse::Reader& resp) {
        std::lock_guard<std::mutex> guard(mu);
        msg_bufs.emplace_back(std::move(zmq_msg));
        resps.emplace_back(resp);
    }

    void release_locks() { batch_manager.release_locks(); }
#ifdef READ_COMMITTED
    void release_aurora_locks() {
        // LOG(2) << "try release batch";
        for (auto txn : ordered_txns) {
            txn->release_locks();
            txn->release_batch_locks();
        }
    }
#endif

    void merge_rw_infos() {
#ifndef TEST_ECHO
        for (auto txn : ordered_txns) {
            merge_single_txn(txn);
        }
#endif
    }

    void merge_single_txn(DbTxn *txn) {
        std::set<node_id_t> sn_accessed;
        for (auto &page_iter : txn->pages) {
            seg_id_t seg_id = GlobalPageId(page_iter.first).seg_id;
            BatchSegInfo *&seg_info = seg_infos[seg_id];
            node_id_t sn_id = conf.segToSnID(seg_id);
            if (seg_info == nullptr) {
                seg_info = create_seg_info(seg_id, sn_id);
            }

            merge_one_page(seg_info, page_iter.first, &(page_iter.second));

            sn_accessed.insert(sn_id);
        }
        for (auto sn_id : sn_accessed) {
            sn_infos[sn_id]->fast_txns.push_back(txn);
        }
    }

    void merge_rw_infos_repair() {
        for (auto txn : ordered_txns) {
            merge_single_txn_repair(txn);
        }
    }

    void merge_single_txn_repair(DbTxn *txn) {
        for (auto &page_iter : txn->pages) {
            if (page_iter.second.is_write) {
                seg_id_t seg_id = GlobalPageId(page_iter.first).seg_id;
                BatchSegInfo *&seg_info = seg_infos[seg_id];
                node_id_t sn_id = conf.segToSnID(seg_id);
                if (seg_info == nullptr) {
                    seg_info = create_seg_info(seg_id, sn_id);
                }

                auto snapshot = get_page(seg_info, page_iter.first);

                if (!snapshot->is_write) {
                    ++seg_info->num_writes;
                    snapshot->is_write = true;
                }
            }
        }
    }

    BatchSnapshot *get_page(BatchSegInfo *seg_info, g_page_id_t g_page_id) {
        auto& pages = seg_info->pages;
        auto iter = pages.find(g_page_id);
        if (iter == pages.end()) {
            auto& snapshot = pages[g_page_id];
            return &snapshot;
        } else {
            return &iter->second;
        }
    }

    void merge_one_page(BatchSegInfo *seg_info, g_page_id_t g_page_id,
                        PageSnapshot *source_snapshot) {
        auto snapshot = get_page(seg_info, g_page_id);
        if (source_snapshot->is_write) {
            if (!snapshot->is_write) {
                ++seg_info->num_writes;
                snapshot->is_write = true;
            }
        }
        if (source_snapshot->is_read) {
            if (!snapshot->is_read) {
                ++seg_info->num_reads;
                snapshot->is_read = true;
            }
        }
    }

    BatchSegInfo *create_seg_info(seg_id_t seg_id, node_id_t sn_id) {
        BatchSegInfo *seg_info = new BatchSegInfo();
        seg_info->reset(seg_id);

        BatchSnInfo *&sn_info = sn_infos[sn_id];
        if (sn_info == nullptr) {
            sn_info = new BatchSnInfo();
        }
        sn_info->sn_seg_infos_size++;

        return seg_info;
    }

#ifdef UNIQUE_REDO_LOG
    void gen_redo_logs() {
        if (fastCommitEnabled) {
            gen_redo_logs_fast();
        } else {
            gen_redo_logs_normal();
        }
    }

    void gen_redo_logs_fast() {
        std::unordered_map<g_page_id_t, std::unordered_map<uint64_t, TxnRecord *>> first_reads, last_writes;
        for (auto txn : ordered_txns) {
            Logger::count(CountType::READ_SET_SIZE, txn->read_set.size() * 16);
            for (auto &record : txn->read_set) {
                if (last_writes[record.g_page_id].count(record.record_page_offset) > 0) {
                    continue;
                }
                TxnRecord *&r = first_reads[record.g_page_id][record.record_page_offset];
                if (r == nullptr) {
                    r = &record;
                }
            }
            for (auto &record : txn->write_set) {
                TxnRecord *&r = last_writes[record.g_page_id][record.record_page_offset];
                r = &record;

                TxRedoLog *&redo_log = redo_logs[record.g_page_id];
                if (redo_log == nullptr) {
                    redo_log = redoLogArena.alloc(record.value_size);
                }
                redo_log->add_write_entry(record.record_page_offset, record.get_buf(),
                                    record.get_writer());

            }
        }
        for (auto iter : first_reads) {
            g_page_id_t g_page_id = iter.first;
            TxRedoLog *&redo_log = redo_logs[g_page_id];
            if (redo_log == nullptr) {
                auto r = iter.second.begin();
                redo_log = redoLogArena.alloc(r->second->value_size);
            }
            for (auto &iter_j : iter.second) {
                auto &record = *iter_j.second;
                redo_log->add_read_entry(record.record_page_offset, record.get_writer());  // mark 64th bit to be read
            }
        }
        freeze_redo_logs();
    }

    void gen_redo_logs_normal() {
        std::unordered_map<g_page_id_t, std::unordered_map<uint64_t, TxnRecord *>> first_reads, last_writes;
        for (auto txn : ordered_txns) {
            for (auto &record : txn->read_set) {
                if (last_writes[record.g_page_id].count(record.record_page_offset) > 0) {
                    continue;
                }
                TxnRecord *&r = first_reads[record.g_page_id][record.record_page_offset];
                if (r == nullptr) {
                    r = &record;
                }
            }
            for (auto &record : txn->write_set) {
                if (txn->record_in_use(record)) {
                    TxnRecord *&r = last_writes[record.g_page_id][record.record_page_offset];
                    r = &record;
                }
            }
        }
        for (auto iter : first_reads) {
            g_page_id_t g_page_id = iter.first;
            TxRedoLog *&redo_log = redo_logs[g_page_id];
            if (redo_log == nullptr) {
                auto r = iter.second.begin();
                redo_log = redoLogArena.alloc(r->second->value_size);
            }
            for (auto &iter_j : iter.second) {
                auto &record = *iter_j.second;
                redo_log->add_read_entry(record.record_page_offset, record.get_writer());  // mark 64th bit to be read
            }
        }
        for (auto iter : last_writes) {
            g_page_id_t g_page_id = iter.first;
            TxRedoLog *&redo_log = redo_logs[g_page_id];
            if (redo_log == nullptr) {
                auto r = iter.second.begin();
                redo_log = redoLogArena.alloc(r->second->value_size);
            }
            for (auto &iter_j : iter.second) {
                auto &record = *iter_j.second;
                redo_log->add_write_entry(record.record_page_offset, record.get_buf(),
                                    record.get_writer());
            }
        }
        freeze_redo_logs();
    }

    void gen_redo_logs_repair() {
        std::unordered_map<g_page_id_t, std::unordered_map<uint64_t, TxnRecord *>> last_writes;
        for (auto txn : ordered_txns) {
            for (auto &record : txn->repair_write_set) {
                if (txn->record_in_use(record)) {
                    TxnRecord *&r = last_writes[record.g_page_id][record.record_page_offset];
                    r = &record;
                }
            }
        }
        for (auto iter : last_writes) {
            g_page_id_t g_page_id = iter.first;
            TxRedoLog *&redo_log = redo_logs[g_page_id];
            if (redo_log == nullptr) {
                auto r = iter.second.begin();
                redo_log = redoLogArena.alloc(r->second->value_size);
            }
            for (auto &iter_j : iter.second) {
                auto &record = *iter_j.second;
                redo_log->add_write_entry(record.record_page_offset, record.get_buf(),
                                    record.get_writer());
            }
        }
        freeze_redo_logs();
    }
#else

    void gen_redo_logs() {
        for (auto txn : ordered_txns) {
            for (auto &record : txn->read_set) {
                TxRedoLog *&redo_log = redo_logs[record.g_page_id];
                if (redo_log == nullptr) {
                    redo_log = redoLogArena.alloc(record.value_size);
                }
                redo_log->add_read_entry(record.record_page_offset, record.get_writer());  // mark 64th bit to be read
            }
            for (auto &record : txn->write_set) {
                TxRedoLog *&redo_log = redo_logs[record.g_page_id];
                if (redo_log == nullptr) {
                    redo_log = redoLogArena.alloc(record.value_size);
                }
                redo_log->add_write_entry(record.record_page_offset, record.get_buf(),
                                    record.get_writer());
            }
        }
        freeze_redo_logs();
    }

    void gen_redo_logs_repair() {
        for (auto txn : ordered_txns) {
            for (auto &record : txn->repair_write_set) {
                if (txn->record_in_use(record)) {
                    TxRedoLog *&redo_log = redo_logs[record.g_page_id];
                    if (redo_log == nullptr) {
                        redo_log = redoLogArena.alloc(record.value_size);
                    }
                    redo_log->add_write_entry(record.record_page_offset, record.get_buf(),
                                        record.get_writer());
                }
            }
        }
        freeze_redo_logs();
    }

#endif
    void freeze_redo_logs() {
        for (auto &iter : redo_logs) {
            iter.second->freeze();
        }
    }

    uint repair(thread_id_t worker_id) {
        uint repaired = 0;
#ifdef BREAKDOWN_TRAVERSING
        uint64_t total_traversing_nsec = 0;
#endif
        for (auto txn : ordered_txns) {
            txn->batch_manager = &batch_manager;
            // txn->deps = &deps;
            bool txn_repaired = txn->repair_logic(txn, worker_id);
            if (txn_repaired) {
                repaired++;
            }
#ifdef BREAKDOWN_TRAVERSING
            total_traversing_nsec += txn->total_traversing_nsec;
#endif
        }
#ifdef BREAKDOWN_TRAVERSING
        _RdtscTimer::end_ns(LatencyType::TRAVERSING, total_traversing_nsec);
#endif 
        return repaired;
    }

    void commit() {
#ifdef EMULATE_AURORA
        for (auto txn : ordered_txns) {
            txn->local_commit();
        }
#endif
#ifdef READ_COMMITTED
        release_aurora_locks();
#endif

        if (is_repaired) {
            // release locks hold by repair put
            release_locks();
        }
    }

    void pack_get_timestamp_req(GetTimestampArgs::Builder req) {
        req.setTxnId(batch_id);
#ifndef TEST_ECHO
        auto rw_seg_ids = req.initRwSegIds(seg_infos.size());
        uint32_t i = 0;
        for (auto &seg_iter : seg_infos) {
            BatchSegInfo *seg_info = seg_iter.second;
            seg_id_t seg_id = seg_iter.first;
            if (!seg_info->num_writes == 0) {
                SEG_SET_WRITE(seg_id);
            }

            rw_seg_ids.set(i, seg_id);
            ++i;
        }
#endif  // TEST_ECHO
    }

    void parse_get_timestamp_resp(GetTimestampResponse::Reader resp) {
        // this is exactly the same with db_txn
        ASSERT(resp.getOk());
#ifndef TOTAL_ORDER_TS
        auto reply_deps = resp.getDeps();
        uint32_t i = 0;
        for (auto &seg_iter : seg_infos) {
            // seg_id_t seg_id = seg_iter.first;
            auto reply_dep = reply_deps[i];
            auto &dep = seg_iter.second->dep;

            dep.ts = reply_dep.getTimestamp();
            dep.num_reads = reply_dep.getNumReads();
            ++i;
        }
#else
        seq = resp.getSeq();
#endif
    }

    /**
     * Read the stored info for all segments stored in a storage node, then convert them into a
     * prepare request message.
     */
#ifndef BREAKDOWN_MSG_SIZE 
    void pack_prepare_req(PrepareArgs::Builder req, node_id_t sn_id) {
#else   
    void pack_prepare_req(PrepareArgs::Builder req, node_id_t sn_id, ::capnp::MallocMessageBuilder* msgBuilder = nullptr) {
#endif
        // initialize the message
        req.setTxnId(batch_id);
        req.setPrimarySnId(sn_id);
        req.setFastPathEnabled(fastCommitEnabled);
        BatchSnInfo *sn_info = sn_infos[sn_id];
#ifdef TOTAL_ORDER_TS
        req.setSeq(seq);
        if (sn_info == nullptr) {
            req.initSegments(0);
            return;
        }
#endif
#ifdef DETERMINISTIC_VALIDATION
        req.setSeq(sn_info->seq);
#endif

#ifdef BREAKDOWN_MSG_SIZE
        if (msgBuilder) {
            Logger::count(CountType::PREPARE_MSG_SIZE_BASE, msgBuilder->sizeInWords() * 8);
        }
#endif

#ifndef TEST_ECHO
        if (this->fastCommitEnabled) {
            pack_all_txn_inputs(req);
#ifdef BREAKDOWN_MSG_SIZE
            if (msgBuilder) {
                Logger::count(CountType::PREPARE_MSG_SIZE_INPUT, msgBuilder->sizeInWords() * 8);
            }
#endif
            pack_fast_txns(req, sn_id);
#ifdef BREAKDOWN_MSG_SIZE
            if (msgBuilder) {
                Logger::count(CountType::PREPARE_MSG_SIZE_DEP, msgBuilder->sizeInWords() * 8);
            }
#endif
            pack_all_timestamps(req);
#ifdef BREAKDOWN_MSG_SIZE
            if (msgBuilder) {
                Logger::count(CountType::PREPARE_MSG_SIZE_TS, msgBuilder->sizeInWords() * 8);
            }
#endif
        } else {
#ifdef NORMAL_PATH_OPT
            pack_relevant_txn_inputs(req, sn_id);
#ifdef BREAKDOWN_MSG_SIZE
            if (msgBuilder) {
                Logger::count(CountType::PREPARE_MSG_SIZE_INPUT, msgBuilder->sizeInWords() * 8);
                Logger::count(CountType::PREPARE_MSG_SIZE_DEP, msgBuilder->sizeInWords() * 8);
            }
#endif
#endif
            pack_relevant_timestamps(req, sn_id);
#ifdef BREAKDOWN_MSG_SIZE
            if (msgBuilder) {
                Logger::count(CountType::PREPARE_MSG_SIZE_TS, msgBuilder->sizeInWords() * 8);
            }
#endif
        }

        auto req_segs = req.initSegments(sn_info->sn_seg_infos_size);

        // for each segment touched by this batch in this SN
        uint32_t seg_i = 0;
        // LOG(2) << "--------------------------------------";
        for (auto &iter : seg_infos) {
            if (sn_id != conf.segToSnID(iter.first))
                continue;
            BatchSegInfo *seg_info = iter.second;
            // set segmentId and the dependency
            auto req_seg = req_segs[seg_i];
            req_seg.setSegId(seg_info->seg_id);

            capnp::List<Page, capnp::Kind::STRUCT>::Builder r_pages, w_pages;
            if (!this->is_repaired) {
                auto seg_dep = req_seg.getDep();
                auto &dep = seg_info->dep;
                seg_dep.setTimestamp(dep.ts);
                seg_dep.setNumReads(dep.num_reads);
                if (seg_info->num_reads) {
                    r_pages = req_seg.initReadPages(seg_info->num_reads);
                }
            }
            if (seg_info->num_writes) {
                w_pages = req_seg.initWritePages(seg_info->num_writes);
            }
            uint32_t read_i = 0, write_i = 0;
            for (auto &pair : seg_info->pages) {
                g_page_id_t g_page_id = pair.first;
                auto& page_snapshot = pair.second;
                if (page_snapshot.is_write) {
                    auto w_page = w_pages[write_i++];
                    w_page.setGlobalPageId(g_page_id);

                    auto iter = redo_logs.find(g_page_id);
                    if (iter != redo_logs.end()) {
                        TxRedoLog *tx_redo_log = iter->second;
                        w_page.setRedoLog(
                            kj::arrayPtr(tx_redo_log->get_log(), tx_redo_log->get_log_size()));
                    }
                }
                if (!this->is_repaired && page_snapshot.is_read) {
                    auto r_page = r_pages[read_i++];
                    r_page.setGlobalPageId(g_page_id);
                    // set extra read redo-logs to represent read records
                    if (!page_snapshot.is_write) {
                        // std::out_of_range
                        TxRedoLog *tx_redo_log =redo_logs.at(g_page_id);
                        r_page.setRedoLog(
                            kj::arrayPtr(tx_redo_log->get_log(), tx_redo_log->get_log_size()));
                        // r_page.setData(kj::arrayPtr(tx_redo_log->get_log(), tx_redo_log->get_log_size()));
                    }
                }
            }

            ++seg_i;
        }
#else   // defined TEST_ECHO
        auto req_segs = req.initSegments(NUM_TXN_PER_BATCH * 1.5);
        for (uint64_t i = 0; i < NUM_TXN_PER_BATCH * 1.5; ++i) {
            auto req_seg = req_segs[i];
            req_seg.initReadPages(1);
            auto w_page = req_seg.initWritePages(1);
            w_page[0].initRedoLogs(1);
        }
#endif  // TEST_ECHO

#ifdef BREAKDOWN_MSG_SIZE
        if (msgBuilder) {
            Logger::count(CountType::PREPARE_MSG_SIZE_RWSET, msgBuilder->sizeInWords() * 8);
        }
#endif
    }

    void pack_all_timestamps(PrepareArgs::Builder req) {
        auto timestamps = req.initTimestamps(seg_infos.size());
        uint32_t i = 0;
        for (auto &iter : seg_infos) {
            BatchSegInfo *seg_info = iter.second;
            // set segmentId and the dependency
            auto ts = timestamps[i];
            ts.setTimestamp(seg_info->dep.ts);
            ts.setNumReads(seg_info->dep.num_reads);
            ++i;
        }
    }

    void pack_relevant_timestamps(PrepareArgs::Builder req, node_id_t sn_id) {
        auto timestamps = req.initTimestamps(sn_infos[sn_id]->sn_seg_infos_size);
        uint32_t i = 0;
        for (auto &iter : seg_infos) {
            if (sn_id != conf.segToSnID(iter.first))
                continue;
            BatchSegInfo *seg_info = iter.second;
            // set segmentId and the dependency
            auto ts = timestamps[i];
            ts.setTimestamp(seg_info->dep.ts);
            ts.setNumReads(seg_info->dep.num_reads);
            ++i;
        }
    }

    void pack_fast_txns(PrepareArgs::Builder req, node_id_t sn_id) {
        auto& fast_txns = sn_infos[sn_id]->fast_txns;
        auto req_txns = req.initTxns(fast_txns.size());
        for (uint i = 0; i < fast_txns.size(); i++) {
            auto *txn = fast_txns[i];
            auto req_txn = req_txns[i];
            capnp::Data::Reader reader((unsigned char *)txn->get_input_ptr(),
                                        txn->txn_input_size);
            req_txn.setTxnI(txn->txn_i);
            auto read_set = req_txn.initReadSet(txn->read_set.size());
            uint j = 0;
            for (const auto& r_record : txn->read_set) {
                read_set[j].setOffset(r_record.record_page_offset);
                read_set[j].setWriter(r_record.get_writer());
                j++;
            }
        }
    }

    void pack_txn_inputs(PrepareArgs::Builder req, const std::vector<DbTxn*>& txns) {
        auto req_txns = req.initTxnInputs(txns.size());
        for (uint i = 0; i < txns.size(); i++) {
            auto *txn = txns[i];
            auto req_txn = req_txns[i];
            txn->txn_i = i;
            req_txn.setTxnId(txn->get_txn_id());
            req_txn.setTxnType(txn->txn_seed);
            capnp::Data::Reader reader((unsigned char *)txn->get_input_ptr(),
                                        txn->txn_input_size);
            req_txn.setInput(reader);
        }
    }

    void pack_all_txn_inputs(PrepareArgs::Builder req) {
       pack_txn_inputs(req, ordered_txns);
    }

    void pack_relevant_txn_inputs(PrepareArgs::Builder req, node_id_t sn_id) {
        pack_txn_inputs(req, sn_infos[sn_id]->fast_txns);
    }

    void pack_prepare_req_repair(PrepareArgs::Builder req) {
        // initialize the message
        req.setTxnId(batch_id);
        uint32_t seg_i = 0;
        auto req_segs = req.initSegments(seg_infos.size());
        for (auto &iter : seg_infos) {
            BatchSegInfo *seg_info = iter.second;
            if (seg_info->num_writes == 0) {
                continue;
            }
            // set segmentId and the dependency
            auto req_seg = req_segs[seg_i];
            req_seg.setSegId(seg_info->seg_id);

            auto w_pages = req_seg.initWritePages(seg_info->num_writes);
            uint32_t write_i = 0;
            for (auto &pair : seg_info->pages) {
                g_page_id_t g_page_id = pair.first;
                auto& page_snapshot = pair.second;
                if (page_snapshot.is_write) {
                    auto w_page = w_pages[write_i++];
                    w_page.setGlobalPageId(g_page_id);

                    auto iter = redo_logs.find(g_page_id);
                    if (iter != redo_logs.end()) {
                        TxRedoLog *tx_redo_log = iter->second;
                        w_page.setRedoLog(
                            kj::arrayPtr(tx_redo_log->get_log(), tx_redo_log->get_log_size()));
                    }
                }
            }
            ++seg_i;
        }
    }


    void pack_commit_req(CommitArgs::Builder req, node_id_t sn_id) {
        req.setTxnId(batch_id);
        req.setPrimarySnId(sn_id);
        req.setCommit(!is_aborted);
    }
};

class LargeBatch {
   public:
    batch_id_t id, gather_batch_num;
    std::vector<BatchTxn *> sub_batches;
    BatchTxn::BatchStatus cur_phase = BatchTxn::BatchStatus::INIT;
    std::mutex mutex;
    std::atomic<uint64_t> sub_count;

    LargeBatch(batch_id_t id) : id(id) {
        sub_count = 0;
    }
};

std::ostream &operator<<(std::ostream &os, const BatchTxn &batch) {
    os << batch.batch_id << " " << batch.ordered_txns.size();
    return os;
}

thread_local std::queue<BatchTxn *> batch_txn_arena;
inline BatchTxn *NewBatchTxn(PageManager &page_manager, thread_id_t num_threads,
                             uint32_t batching_size = 100) {
    // return new BatchTxn(page_manager, num_threads, batching_size);
    if (batch_txn_arena.size() == 0) {
        return new BatchTxn(page_manager, num_threads, batching_size);
    } else {
        BatchTxn *tmp = batch_txn_arena.front();
        batch_txn_arena.pop();
        // re-use the previous memory;

        BatchTxn *b = new (tmp) BatchTxn(page_manager, num_threads, batching_size);
        ASSERT(b->redo_logs.size() == 0);
        return tmp;
    }
}

void delete_batch_txn(BatchTxn *p) {
    // LOG(3) << "delete " << p->batch_id << " " << p;

    p->ordered_txns.clear();
    p->free_redo_logs();
    batch_txn_arena.push(p);
    // delete p;
}

void clear_batch_txn() {
    while (!batch_txn_arena.empty()) {
        BatchTxn *tmp = batch_txn_arena.front();
        batch_txn_arena.pop();
        delete tmp;
    }
}

thread_local std::queue<LargeBatch *> lbatch_txn_arena;
inline LargeBatch *new_large_batch(batch_id_t b_id) {
    if (lbatch_txn_arena.size() == 0) {
        return new LargeBatch(b_id);
    } else {
        LargeBatch *tmp = lbatch_txn_arena.front();
        lbatch_txn_arena.pop();
        // re-use the previous memory;

        LargeBatch *b = new (tmp) LargeBatch(b_id);
        return b;
    }
}

void delete_large_batch(LargeBatch *p) {
    p->sub_batches.clear();
    lbatch_txn_arena.push(p);
    // delete p;
}

void clear_large_batch() {
    while (!lbatch_txn_arena.empty()) {
        LargeBatch *tmp = lbatch_txn_arena.front();
        lbatch_txn_arena.pop();
        delete tmp;
    }
}

// thread_local std::queue<BatchCache *> batch_cache_arena;
inline BatchCache *new_batch_cache(thread_id_t num_threads) {
    return new BatchCache(num_threads);
    // if (batch_cache_arena.size() == 0) {
    //     return new BatchCache(num_threads);
    // } else {
    //     BatchCache *tmp = batch_cache_arena.front();
    //     batch_cache_arena.pop();
    //     // re-use the previous memory;
    //     tmp->clear();
    //     return tmp;
    // }
}

void delete_batch_cache(BatchCache *p) {
#ifdef EMULATE_AURORA
    delete p;
    // batch_cache_arena.push(p);
#endif
}