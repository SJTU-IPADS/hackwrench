#pragma once

#include "interface.h"
#include "servers/config.h"
#include "servers/db_node.h"
#include "storage/storage.h"
#include "util/fast_random.h"
#include "util/zipf.h"

#include <time.h>
#include <set>

#define NUM_KEY_PER_TX 10

struct Ycsb10Client {
    // uint32_t own_seg_id;
    Key_t own_client_id;
};

class WorkloadYcsbt10 : public BenchmarkInterface {
    using Value_t = BytesArray<20>;
    static const uint32_t TEST_DURATION = 10;

    const Configuration &conf;
    const uint32_t NUM_RECORDS_PER_PAGE;
    const uint32_t my_db_id;
    const uint32_t num_sns, num_dbs;
    const uint32_t num_segs, num_pages;
    const uint32_t num_segs_per_sn, num_pages_per_sn;
    const uint32_t num_records;
    const uint32_t num_threads;
    const uint32_t num_clients;
    const uint32_t num_segs_per_client;
    const uint32_t num_records_per_sn;
    const uint32_t num_records_per_seg;
    const uint32_t num_records_per_db;
    const uint32_t num_records_per_client;
    const uint32_t contention_factor, distributed_ratio, cache_miss_ratio, batch_size;
    LooseThreadSafeQueue<Ycsb10Client *> free_clients;

#ifdef TEST_TS
    const uint PERCENT_READ_ONLY = 0;
    const uint PERCENT_WRITE = 100;
#else
    const uint PERCENT_READ_ONLY = 80;
    const uint PERCENT_WRITE = 20;
#endif
    fast_random frandom;

   public:
    struct YcsbtInput {
        Key_t key[NUM_KEY_PER_TX];
        bool needWrite[NUM_KEY_PER_TX];
        bool cacheMiss[NUM_KEY_PER_TX];
        Ycsb10Client *client;
        // the Input owns the graph

        ~YcsbtInput() {
        }
    };

    WorkloadYcsbt10(const Configuration &conf, uint32_t num_threads,
                  uint32_t num_clients, uint32_t contention_factor, uint32_t distributed_ratio, uint32_t cache_miss_ratio, uint32_t batch_size)
        : BenchmarkInterface(),
          conf(conf),
          NUM_RECORDS_PER_PAGE(LocalPBtreeNode<Key_t, Value_t>::max_leaf_slots),
          my_db_id(conf.get_my_db_id()),
          num_sns(conf.numLogicalSN()),
          num_dbs(conf.numDB()),
          num_segs(conf.numSegments()),
          num_pages(conf.numPages()),
          num_segs_per_sn(num_segs / conf.numLogicalSN()),
          num_pages_per_sn(num_pages / conf.numLogicalSN()),
          num_records(num_pages * NUM_RECORDS_PER_PAGE),
          num_threads(num_threads),
          num_clients(num_clients),
          num_segs_per_client((num_segs - 1) / conf.numDB() / (num_clients + conf.numLogicalSN() - 1)),
          num_records_per_sn(num_records / conf.numLogicalSN()),
          num_records_per_seg(num_records_per_sn / num_segs_per_sn),
          num_records_per_db(num_records/conf.numDB()),
          num_records_per_client(num_records_per_db / num_clients),
          contention_factor(contention_factor),
          distributed_ratio(distributed_ratio),
          cache_miss_ratio(cache_miss_ratio),
          batch_size(batch_size),
          free_clients(num_threads),
          frandom(fast_random((unsigned)time(NULL))),
#ifdef ENABLE_DISTRIBUTED_RATIO
          zipf(0, num_records_per_db - 1, contention_factor / 100.0) {
#else
          zipf(0, num_records - 1, contention_factor / 100.0) {
#endif
        ASSERT(PERCENT_READ_ONLY + PERCENT_WRITE ==100);
        LOG(2) << "zipf_constant: " << contention_factor / 100.0;
        LOG(2) << VAR(num_records);

        // if (contention_factor == 0) {
        //     uint32_t numSN = conf.numLogicalSN();
        //     uint32_t numDB = conf.numDB();
        //     ASSERT(num_segs_per_client >= NUM_KEY_PER_TX)
        //         << "Each client should have no less than 10 tables for no contention." << 
        //         num_segs_per_client;
        //     ASSERT(num_segs_per_sn >= (num_clients + numSN - 1) / numSN * num_segs_per_client * numDB);
        // }
        /**
         *      |sn_position0|sn_position1|...
         *      |DB0|DB1|DB2 |DB0|DB1|...
         *  SN0 | 0 |   |    | 2 |   |...
         *  SN1 | 1 |   |    | 3 |   |...
         */
        for (uint32_t i = 0; i < num_clients; i++) {
            Ycsb10Client *c = new Ycsb10Client();
            // uint32_t sn_id = i % conf.numLogicalSN();
            // uint32_t sn_position = i / conf.numLogicalSN();
            // c->own_seg_id =
            //     sn_id * num_segs_per_sn +
            //     (sn_position * conf.numDB() + conf.get_my_db_id()) * num_segs_per_client;
            // c->own_client_id = num_records_per_db * conf.get_my_db_id() + i * num_records_per_client;
            c->own_client_id = i * conf.numDB() + conf.get_my_db_id();
            // LOG(2) << VAR2(i, ", ") << VAR2(sn_id, ", ")<< VAR2(sn_position, ", ")<< VAR2(c->own_seg_id, ", ")
            //     << VAR2(conf.get_my_db_id(), ", ")<< VAR2(num_segs_per_client, ", ");
            free_clients.enqueue(c, i % num_threads);
        }

    }

    ~WorkloadYcsbt10() {
        if (tbls){
            for(uint i=0; i < conf.numLogicalSN(); i++){
                delete tbls[i];
            }
            delete tbls;
        }
            
        if (nodes) {
            uint32_t n_pages = conf.get_my_node_type() == Configuration::DB ?
                num_pages : num_pages_per_sn;
            for (uint32_t g_page_i = 0; g_page_i < n_pages; ++g_page_i) {
                delete nodes[g_page_i];
            }
            delete[] nodes;
        }
    }

    uint32_t get_num_clients() override { return num_clients; };

    uint64_t get_time_duration() override { return TEST_DURATION; }

    void init_page_manager(PageManager &page_manager) override {}

    void init_tables() {
        uint numSN = conf.numLogicalSN();
        tbls = new PBtree<Key_t, Value_t>*[numSN];
        for(uint i=0; i<numSN; i++){
            tbls[i] = new PBtree<Key_t, Value_t>;
            tbls[i]->init();
            tbls[i]->set_table_id(i);    
        }
    }

    void init_database(DatabaseNode &db_node) override {
        init_tables();

        nodes = new LocalPBtreeNode<Key_t, Value_t> *[num_pages];
        for (uint32_t g_page_i = 0; g_page_i < num_pages; ++g_page_i) {
            seg_id_t seg_id = g_page_i / NUM_PAGES_PER_SEGMENT;
            page_id_t page_id = g_page_i % NUM_PAGES_PER_SEGMENT;

            PageMeta *page_meta = db_node.init_empty_page(seg_id, page_id);
            auto node = new LocalPBtreeNode<Key_t, Value_t>;
            node->set_page_meta(page_meta);
            node->initialize(sizeof(Value_t));
            for (uint64_t i = 0; i < NUM_RECORDS_PER_PAGE; ++i) {
                node->insert(get_key(g_page_i, i), 0, NULL_TXN_ID);
            }
            node->update_byte_size();
            nodes[g_page_i] = node;

            uint SNIndex = g_page_i / num_pages_per_sn;
            tbls[SNIndex]->insert_range(get_key(g_page_i, 0), nodes[g_page_i]);
        }
    }

    void init_storage(InMemSegments &segments) override {
        init_tables();

        nodes = new LocalPBtreeNode<Key_t, Value_t> *[num_pages_per_sn];
        memset(nodes, 0, sizeof(*nodes));
        node_id_t my_sn_id = conf.get_my_logical_sn_id();
        for (uint32_t l_page_i = 0; l_page_i < num_pages_per_sn; ++l_page_i) {
            uint32_t g_page_i = my_sn_id * num_pages_per_sn + l_page_i;
            seg_id_t seg_id = g_page_i / NUM_PAGES_PER_SEGMENT;
            page_id_t page_id = g_page_i % NUM_PAGES_PER_SEGMENT;
            ASSERT(conf.isLocalSnSegment(seg_id));
            PageMeta *page_meta = segments.alloc_next_page(seg_id);
            ASSERT(page_meta->gp_id.page_id == page_id);
            auto node = new LocalPBtreeNode<Key_t, Value_t>;
            node->set_page_meta(page_meta);
            node->initialize(sizeof(Value_t));

            for (uint64_t i = 0; i < NUM_RECORDS_PER_PAGE; ++i) {
                node->insert(get_key(g_page_i, i), 0, NULL_TXN_ID);
            }
            node->update_byte_size();
            ASSERT(node->byte_size() < PAGE_SIZE);
            nodes[l_page_i] = node;

            tbls[my_sn_id]->insert_range(get_key(g_page_i, 0), node);
        }
    }

    uint32_t key2table_id(Key_t key){
        return key / num_records_per_sn;
    }

    void check_correctness_reduce(std::vector<uint64_t> &sums) override {}

    uint64_t check_correctness_map(InMemSegments &segments) override {
        return 0;
    }

    DbTxn::txn_logic_t get_txn_logic(uint32_t txn_seed) override {
        if (txn_seed < PERCENT_READ_ONLY) {
            return get_txn_logic_readonly();
        } else {
            return get_txn_logic_write();
        }
    }

    DbTxn::txn_logic_t get_txn_logic_readonly() {
#ifndef NON_CACHING
        auto execution_logic = [this](DbTxn *txn, thread_id_t worker_id) {
            txn->txn_type = 0;
            txn->txn_input_size = sizeof(YcsbtInput);
            txn->fastCommitEnabled = WORKLOAD_FAST_PATH;
            YcsbtInput &input = txn->get_input_as_ref<YcsbtInput>();
            for (uint i = 0; i < NUM_KEY_PER_TX; ++i) {
                Value_t val;
                val = txn->get(*tbls[key2table_id(input.key[i])], input.key[i], true, false);
            }
            return true;
        };
#else
        auto execution_logic = [this](DbTxn *txn, thread_id_t worker_id) {
            txn->txn_type = 0;
            txn->txn_input_size = sizeof(YcsbtInput);
            txn->fastCommitEnabled = WORKLOAD_FAST_PATH;
            YcsbtInput &input = txn->get_input_as_ref<YcsbtInput>();
            
            for (;txn->phase < NUM_KEY_PER_TX;) {
                uint i = txn->phase;
                if (input.cacheMiss[i]) {
                    txn->remote_get(*tbls[key2table_id(input.key[i])], input.key[i], true, false);
                } else {
                    txn->get(*tbls[key2table_id(input.key[i])], input.key[i], true, false);
                }
                txn->phase += 1;
                if (input.cacheMiss[i]) {
                    txn->read_remote();
                    return false;
                }
            }
            return true;
        };
#endif
        auto repair_logic = [this](DbTxn *txn, thread_id_t worker_id) {
            YcsbtInput &input = txn->get_input_as_ref<YcsbtInput>();
            uint32_t r_code_loc = 0;
            for (uint i = 0; i < NUM_KEY_PER_TX; ++i) {
                Value_t val;
                RepairStatus r_status;
                val = txn->repair_get(*tbls[key2table_id(input.key[i])], input.key[i], r_code_loc++, r_status);
            }
            return false;
        };
        auto gc_logic = [this](DbTxn *txn, thread_id_t worker_id) {
            Ycsb10Client *c = txn->get_input_as_ref<YcsbtInput>().client;
            this->free_clients.enqueue(c, socket_t_id);
            delete txn->get_input<YcsbtInput>();
            return true;
        };

        return {execution_logic, repair_logic, gc_logic};
    }

    DbTxn::txn_logic_t get_txn_logic_write() {
#ifndef NON_CACHING
#if defined(TEST_TS) && defined(TEST_TS_KEYGEN)
    auto execution_logic = [this](DbTxn *txn, thread_id_t worker_id) {
        txn->txn_type = 1;
        txn->fastCommitEnabled = WORKLOAD_FAST_PATH;
        std::set<Key_t> key_set;
        for (uint i = 0; i < batch_size * NUM_KEY_PER_TX; ++ i) {
            node_id_t db_id = my_db_id;
            // if (is_remote) {
            if (i % NUM_KEY_PER_TX == 0) {
                bool is_remote = (thread_rand.randint(1, 100) <= distributed_ratio) && (num_dbs > 1);
                if (is_remote) {
                    db_id = get_remote_db_id(my_db_id);
                    ASSERT(db_id < num_dbs);
                }
            }
            Key_t key;
            key = get_key_input(db_id);
            key_set.insert(key);
            ASSERT(key2table_id(key) < num_sns) << VAR2(db_id, " ") 
                << VAR2(key, " ") << VAR2(key2table_id(key), " ")
                << VAR2(num_records_per_db, " ")
                << VAR2(num_records_per_sn, " ");
        }
        for (auto key : key_set) {
            bool needWrite = (thread_rand.randint(1, 100) <= 50);
            Value_t val;
            auto &table = *tbls[key2table_id(key)]; 
            val = txn->ts_get(table, key, true, needWrite);
            if(needWrite){
                val.set_int(val.to_int() + 1);
                txn->ts_put(table, key, val);
            }
        }
        return true;
    };
#else 
        auto execution_logic = [this](DbTxn *txn, thread_id_t worker_id) {
            txn->txn_type = 1;
            txn->txn_input_size = sizeof(YcsbtInput);
            txn->fastCommitEnabled = WORKLOAD_FAST_PATH;
            YcsbtInput &input = txn->get_input_as_ref<YcsbtInput>();
            for (uint i = 0; i < NUM_KEY_PER_TX; i++) {
                Value_t val;
                auto &table = *tbls[key2table_id(input.key[i])]; 
                val = txn->get(table, input.key[i], true, input.needWrite[i]);
                if(input.needWrite[i]){
                    val.set_int(val.to_int() + 1);
                    txn->put(table, input.key[i], val);
                }
            }
            return true;
        };
#endif
#else
        auto execution_logic = [this](DbTxn *txn, thread_id_t worker_id) {
            txn->txn_type = 1;
            txn->txn_input_size = sizeof(YcsbtInput);
            txn->fastCommitEnabled = WORKLOAD_FAST_PATH;
            YcsbtInput &input = txn->get_input_as_ref<YcsbtInput>();

            for (;txn->phase < NUM_KEY_PER_TX;) {
                uint i = txn->phase;
                Value_t val;
                auto &table = *tbls[key2table_id(input.key[i])]; 
                if (input.cacheMiss[i]) {
                    val = txn->remote_get(table, input.key[i], true, input.needWrite[i]);
                } else {
                    val = txn->get(table, input.key[i], true, input.needWrite[i]);
                }
                if(input.needWrite[i]){
                    val.set_int(val.to_int() + 1);
                    txn->put(table, input.key[i], val);
                }

                txn->phase += 1;
                if (input.cacheMiss[i]) {
                    txn->read_remote();
                    return false;
                }
            }

            return true;
        };
#endif
        auto repair_logic = [this](DbTxn *txn, thread_id_t worker_id) {
            bool repaired = false;
            YcsbtInput &input = txn->get_input_as_ref<YcsbtInput>();
            uint32_t r_code_loc = 0;
            // uint loop_time = NUM_KEY_PER_TX / 2;
            for (uint i = 0; i < NUM_KEY_PER_TX; i++) {
                Value_t val;
                RepairStatus r_status{};
                // LOG(2) << input.key[i] << " " << key2table_id(input.key[i]);
                auto &table = *tbls[key2table_id(input.key[i])]; 
                val = txn->repair_get(table, input.key[i], r_code_loc++, r_status);
                if (r_status == REPAIRED) {
                    val.set_int(val.to_int() + 1);
                    if(input.needWrite[i]){
                        txn->repair_put(table, input.key[i], val, true);
                    }
                    repaired = true;
                }
            }
            return repaired;
        };
        auto gc_logic = [this](DbTxn *txn, thread_id_t worker_id) {
            Ycsb10Client *c = txn->get_input_as_ref<YcsbtInput>().client;
            this->free_clients.enqueue(c, socket_t_id);
            delete txn->get_input<YcsbtInput>();
            return true;
        };

        return {execution_logic, repair_logic, gc_logic};
    }

    void *get_input(DbClient *client) override {
        Ycsb10Client *c;
        try {
            c = free_clients.dequeue(socket_t_id);
        } catch (const WorkloadNoClientException &e) {
            return nullptr;
        }
        YcsbtInput *p = new YcsbtInput();
        p->client = c;
        std::set<Key_t> key_set;
        // if (contention_factor == 0) {
        //     // contention free
        //     for (uint i = 0; i < NUM_KEY_PER_TX; ++ i) {
        //         Key_t key;
        //         do {
        //             // seg_id_t seg_id = frandom.randint(c->own_seg_id, c->own_seg_id + num_segs_per_client - 1);
        //             // key = get_key(seg_id * NUM_PAGES_PER_SEGMENT, 0);
        //             key = frandom.randint(0, num_records_per_client - 1);
        //             key = c->own_client_id + key * conf.numDB() * num_clients;
        //             key_set.insert(key);
        //         } while (key_set.size() != i + 1);
        //         p->key[i] = key;
        //     }
        // } 
        // if(contention_factor == 101) {
        //     // uniform
        //     for (uint i = 0; i < NUM_KEY_PER_TX; ++ i) {
        //         Key_t key;
        //         do {
        //             key = frandom.randint(0, num_records - 1);
        //             key_set.insert(key);
        //         } while (key_set.size() != i + 1);
        //         p->key[i] = key;
        //     }
        // } else {


        // zipf
#ifdef ENABLE_DISTRIBUTED_RATIO
        for (uint i = 0; i < NUM_KEY_PER_TX; ++ i) {
            bool is_remote = (thread_rand.randint(1, 100) <= distributed_ratio) && (num_dbs > 1);
            node_id_t db_id = my_db_id;
            // if (is_remote) {
            if (is_remote && i == 0) {
                db_id = get_remote_db_id(my_db_id);
                ASSERT(db_id < num_dbs);
            }
            ASSERT(db_id < num_dbs);
            Key_t key;
            do {
                key = get_key_input(db_id);
                key_set.insert(key);
            } while (key_set.size() != i + 1);
            ASSERT(key2table_id(key) < num_sns) << VAR2(db_id, " ") 
                << VAR2(key, " ") << VAR2(key2table_id(key), " ")
                << VAR2(num_records_per_db, " ")
                << VAR2(num_records_per_sn, " ");

            p->key[i] = key;
        }
#else
        for (uint i = 0; i < NUM_KEY_PER_TX; ++ i) {
            Key_t key;
            do {
                key = get_key_input();
                key_set.insert(key);
            } while (key_set.size() != i + 1);
            p->key[i] = key;
        }
#endif
        for(int i=0; i<NUM_KEY_PER_TX; i++){
            p->needWrite[i] = false;
            p->cacheMiss[i] = frandom.randint(0,999999) < cache_miss_ratio;
        }
        uint writeKey = NUM_KEY_PER_TX / 2;
        while(writeKey > 0){
            int w = frandom.randint(0, NUM_KEY_PER_TX - 1);
            if(!p->needWrite[w]){
                p->needWrite[w] = true;
                writeKey--;
            }
        }
        std::sort(&p->key[0], &p->key[NUM_KEY_PER_TX]);

        return p;
    }

    uint32_t get_input_size(uint32_t txn_seed) override {
        return sizeof(YcsbtInput);
    }

    DbTxn::partition_logic_func get_partition_logic() override {
        return [this](table_id_t table_id, node_id_t sn_id) { return table_id == sn_id; };
    }

    GlobalPageId get_g_page_id(void *input, uint i) override {
        YcsbtInput *p = reinterpret_cast<YcsbtInput *>(input);
        return get_g_page_id_from_key(p->key[i]);
    }

   private:
    Key_t get_key(table_id_t page_i, uint64_t index) {
        return page_i * NUM_RECORDS_PER_PAGE + index;
    }

    GlobalPageId get_g_page_id_from_key(Key_t key) { 
        uint32_t g_page_i = key / NUM_RECORDS_PER_PAGE;
        // LOG(2) << g_page_i;
        return GlobalPageId(g_page_i / NUM_PAGES_PER_SEGMENT, g_page_i % NUM_PAGES_PER_SEGMENT); 
    }

    node_id_t get_remote_db_id(node_id_t my_db_id) {
        node_id_t r_db_id = thread_rand.randint(0, num_dbs - 2);
        if (r_db_id >= my_db_id) {
            r_db_id++;
        }
        return r_db_id;
    }

    Key_t get_key_input() {
        uint64_t key = zipf.nextValue();
#ifndef ENABLE_DISTRIBUTED_RATIO
        key = get_dispatched_key(key);
#endif
        return key;
    }

    Key_t get_key_input(node_id_t db_id) {
        uint64_t key = zipf.nextValue();
        key += db_id * num_records_per_db;
        return key;
    }

    Key_t get_dispatched_key(Key_t index) {
        Key_t sn_id = index % num_sns;
        Key_t sn_index = index / num_sns;
        Key_t seg_id = sn_index / num_records_per_seg;
        Key_t seg_index = sn_index % num_records_per_seg;
        Key_t page_id = seg_index / NUM_RECORDS_PER_PAGE;
        Key_t page_index = seg_index % NUM_RECORDS_PER_PAGE;
        Key_t key = sn_id * num_records_per_sn + 
            NUM_RECORDS_PER_PAGE * (seg_id * NUM_PAGES_PER_SEGMENT + page_id) + page_index;
        ASSERT(key < num_records);
        return key;
    }



   private:
    ZipfianGenerator zipf;
    PBtree<Key_t, Value_t> ** tbls;
    LocalPBtreeNode<Key_t, Value_t> **nodes;
};
