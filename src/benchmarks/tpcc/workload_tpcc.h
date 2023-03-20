#pragma once

#include "../interface.h"
#include "servers/config.h"
#include "servers/db_node.h"
#include "storage/storage.h"
#include "util/fast_random.h"
#include "schema.h"

// NOTE: all ids start from 1!!!

using alloc_page_func_t = std::function<PageMeta*(seg_id_t, page_id_t)>;

struct TpccClient {
    uint16_t belong_wid;
};

class WorkloadTpcc : public BenchmarkInterface {
    using tpcc_id_t = uint32_t;
    using Value_t = BytesArray<12>;

    enum TxnType {
        TXN_NEWORDER = 0,
        TXN_PAYMENT,
        TXN_ORDERSTATUS,
        TXN_DELIVERY,
        TXN_STOCKLEVEL
    };

    enum tpcc_tables {
        WAREHOUSE = 0,
        DISTRICT1,
        DISTRICT2,
        DISTRICT3,
        DISTRICT4,
        DISTRICT5,
        DISTRICT6,
        DISTRICT7,
        DISTRICT8,
        DISTRICT9,
        DISTRICT10,
        CUSTOMER,
        CUSTOMER_LAST_ORDER,
        ORDER,
        NEWORDER,
        ITEM,
        STOCK,
        STOCK_DATA,
        ORDERLINE,
        HISTORY,
        CUSTOMER_NAME,
        TPCC_TABLE_NUM
    };
#ifdef HALF_TPCC
    const uint PERCENT_NEW_ORDER = 50;
    const uint PERCENT_PAYMENT = 50;
    const uint PERCENT_DELIVERY = 0;
    const uint PERCENT_ORDERSTATUS = 0;
    const uint PERCENT_STOCKLEVEL = 0;
#else
    const uint PERCENT_NEW_ORDER = 45;
    const uint PERCENT_PAYMENT = 43;
    const uint PERCENT_DELIVERY = 4;
    const uint PERCENT_ORDERSTATUS = 4;
    const uint PERCENT_STOCKLEVEL = 4;
#endif
    Configuration &conf;

    static const uint32_t MAXITEMS = 100000;
    static const uint32_t CUST_PER_DIST = 3000;
    static const uint32_t DIST_PER_WARE = 10;
    static const uint32_t ORD_PER_DIST = 3000;

    uint32_t records_per_table[TPCC_TABLE_NUM] = {10,     2,      2,      2,      2,     2,     2,
                                       2,      2,      2,      2,      30000, 30000, 150000,
                                       150000, 100000, 100000, 100000, 150000, 10000, 30000};
    uint32_t value_size[TPCC_TABLE_NUM] = {
        sizeof(W_val),sizeof(D_val),sizeof(D_val),sizeof(D_val),sizeof(D_val),sizeof(D_val),sizeof(D_val),
        sizeof(D_val),sizeof(D_val),sizeof(D_val),sizeof(D_val),sizeof(C_val),sizeof(Value_t),sizeof(O_val),
        sizeof(NO_val),sizeof(I_val),sizeof(S_val),sizeof(SD_val),sizeof(OL_val),sizeof(H_val), sizeof(Key_t)};
    uint32_t records_per_page[TPCC_TABLE_NUM];

    uint32_t init_table_information() {
        for (uint32_t i = 0; i < TPCC_TABLE_NUM; ++i) {
            records_per_page[i] = get_max_leaf_slots(get_record_size(value_size[i]));
            uint32_t records_per_seg = records_per_page[i] * NUM_PAGES_PER_SEGMENT;
            page_used[i] = (records_per_table[i] + records_per_page[i] - 1) / records_per_page[i]; 
            records_per_table[i] = round_up(records_per_table[i], records_per_seg);
            pages_per_table[i] = records_per_table[i] / records_per_page[i];
        }
        page_used[ORDERLINE] = pages_per_table[ORDERLINE];
        page_used[HISTORY] = pages_per_table[HISTORY];

        uint32_t i_offset = 0;
        for (uint32_t i = 0; i < TPCC_TABLE_NUM; ++i) {
            page_i_offsets[i] = i_offset;
            i_offset += pages_per_table[i];
        }
        page_i_offsets[TPCC_TABLE_NUM] = i_offset;
        uint offset = 0;
        for (uint32_t i = 0; i < TPCC_TABLE_NUM; ++i) {
            table_key_offsets[i] = offset;
            LOG(2) << VAR2(i, " ") 
                << VAR2(records_per_table[i], " ")
                << VAR2(pages_per_table[i], " ")
                << VAR2(page_i_offsets[i], " ")
                << VAR2(page_used[i], " ")
                << VAR2(records_per_page[i], " ")
                << VAR2(table_key_offsets[i], " ");
            offset += records_per_table[i];
        }
        return i_offset;
    }
    uint32_t num_segs;
    uint32_t num_segs_per_warehouse;
    uint32_t used_pages_per_warehouse;
    uint table_key_offsets[TPCC_TABLE_NUM];
    uint pages_per_table[TPCC_TABLE_NUM];
    uint page_i_offsets[TPCC_TABLE_NUM + 1];
    uint page_used[TPCC_TABLE_NUM];

    const int random_C_C_LAST_RUN = 223;
    const int random_C_C_LAST_LOAD = 157;
    const int random_C_C_ID = thread_rand.randint(0, 1023);
    const int random_C_OL_I_ID = thread_rand.randint(0, 8191);

    const uint32_t num_logical_sn;
    const uint32_t num_threads;
    const uint32_t num_clients;
    const uint32_t remote_neworder_item_pro;
    const uint32_t remote_payment_pro;

    const int table_offset = 150000;
    const uint32_t num_warehouse;
    const uint32_t num_warehouse_per_db;
    const uint32_t num_warehouse_per_worker;
    const uint32_t num_warehouse_per_sn;

    struct Tables {
        PBtree<Key_t, W_val> w_tab;
        LocalPBtreeNode<Key_t, W_val> **w_nodes;
        PBtree<Key_t, D_val> d_tab[DIST_PER_WARE];
        LocalPBtreeNode<Key_t, D_val> **d_nodes[DIST_PER_WARE];
        PBtree<Key_t, C_val> c_tab;
        LocalPBtreeNode<Key_t, C_val> **c_nodes;
        PBtree<Key_t, Key_t> cn_tab;
        LocalPBtreeNode<Key_t, Key_t> **cn_nodes;
        PBtree<Key_t, Value_t> cl_tab;
        LocalPBtreeNode<Key_t, Value_t> **cl_nodes;
        PBtree<Key_t, O_val> o_tab;
        LocalPBtreeNode<Key_t, O_val> **o_nodes;
        PBtree<Key_t, NO_val> no_tab;
        LocalPBtreeNode<Key_t, NO_val> **no_nodes;
        PBtree<Key_t, I_val> i_tab;
        LocalPBtreeNode<Key_t, I_val> **i_nodes;
        PBtree<Key_t, S_val> s_tab;
        LocalPBtreeNode<Key_t, S_val> **s_nodes;
        PBtree<Key_t, SD_val> sd_tab;
        LocalPBtreeNode<Key_t, SD_val> **sd_nodes;
        PBtree<Key_t, OL_val> ol_tab;
        LocalPBtreeNode<Key_t, OL_val> **ol_nodes;
        PBtree<Key_t, H_val> h_tab;
        LocalPBtreeNode<Key_t, H_val> **h_nodes;

        void set_table_id(uint32_t w) {
            w_tab.set_table_id(w);
            for (uint32_t i = 0; i < DIST_PER_WARE; ++i) {
                d_tab[i].set_table_id(w);
            }
            c_tab.set_table_id(w);
            cl_tab.set_table_id(w);
            o_tab.set_table_id(w);
            no_tab.set_table_id(w);
            i_tab.set_table_id(w);
            s_tab.set_table_id(w);
            sd_tab.set_table_id(w);
            ol_tab.set_table_id(w);
            h_tab.set_table_id(w);
            cn_tab.set_table_id(w);
        }

        void init(WorkloadTpcc* wl) {
            w_tab.init(wl->page_used[WAREHOUSE]);
            w_nodes = new LocalPBtreeNode<Key_t, W_val>*[wl->page_used[WAREHOUSE]];
            for (uint32_t i = 0; i < DIST_PER_WARE; ++i) {
                d_tab[i].init(wl->page_used[DISTRICT1 + i]);
                d_nodes[i] = new LocalPBtreeNode<Key_t, D_val>*[wl->page_used[DISTRICT1 + i]];
            }
            c_tab.init(wl->page_used[CUSTOMER]);
            c_nodes = new LocalPBtreeNode<Key_t, C_val>*[wl->page_used[CUSTOMER]];
            cl_tab.init(wl->page_used[CUSTOMER_LAST_ORDER]);
            cl_nodes = new LocalPBtreeNode<Key_t, Value_t>*[wl->page_used[CUSTOMER_LAST_ORDER]];
            o_tab.init(wl->page_used[ORDER]);
            o_nodes = new LocalPBtreeNode<Key_t, O_val>*[wl->page_used[ORDER]];
            no_tab.init(wl->page_used[NEWORDER]);
            no_nodes = new LocalPBtreeNode<Key_t, NO_val>*[wl->page_used[NEWORDER]];
            i_tab.init(wl->page_used[ITEM]);
            i_nodes = new LocalPBtreeNode<Key_t, I_val>*[wl->page_used[ITEM]];
            s_tab.init(wl->page_used[STOCK]);
            s_nodes = new LocalPBtreeNode<Key_t, S_val>*[wl->page_used[STOCK]];
            sd_tab.init(wl->page_used[STOCK_DATA]);
            sd_nodes = new LocalPBtreeNode<Key_t, SD_val>*[wl->page_used[STOCK_DATA]];
            ol_tab.init(wl->page_used[ORDERLINE]);
            ol_nodes = new LocalPBtreeNode<Key_t, OL_val>*[wl->page_used[ORDERLINE]];
            h_tab.init(wl->page_used[HISTORY]);
            h_nodes = new LocalPBtreeNode<Key_t, H_val>*[wl->page_used[HISTORY]];
            cn_tab.init(wl->page_used[CUSTOMER_NAME]);
            cn_nodes = new LocalPBtreeNode<Key_t, Key_t>*[wl->page_used[CUSTOMER_NAME]];
        }

        ~Tables() {
            if (w_nodes == nullptr) return;
            delete [] w_nodes;
            for (uint32_t i = 0; i < DIST_PER_WARE; ++i) {
                delete [] d_nodes[i];
            }
            delete [] c_nodes;
            delete [] cl_nodes;
            delete [] o_nodes;
            delete [] no_nodes;
            delete [] i_nodes;
            delete [] s_nodes;
            delete [] sd_nodes;
            delete [] ol_nodes;
            delete [] h_nodes;
            delete [] cn_nodes;
        }
    };

    Tables *tables;
    LocalPBtreeNode<Key_t, Value_t> ***pages;
    LooseThreadSafeQueue<TpccClient *> free_clients;
    DatabaseNode *dbnode = nullptr;

    std::atomic<int> num_delivery;
    std::atomic<int> success_delivery;

   public:
    static const uint32_t TEST_DURATION = 10;

    class Item {
       public:
        uint i_id;
        tpcc_id_t ol_supply_wid;
        uint quantity;
        double price;

        Item(uint i_id, tpcc_id_t wid, uint quantity)
            : i_id(i_id), ol_supply_wid(wid), quantity(quantity), price(0.0) {}

        Item() : Item(0, 0, 0) {}

        bool operator<(const Item &other) {
            if (ol_supply_wid != other.ol_supply_wid) {
                return ol_supply_wid < other.ol_supply_wid;
            }
            return i_id < other.i_id;
        }
    };

    // for NewOrder
    struct NewOrderInput{
        TpccClient *client;
        uint32_t w_id, d_id, c_id, ol_cnt, o_entry_d;
        double w_tax, d_tax, c_discount;
        bool rbk, ol_all_local;
        Item items[15];
    };
    struct NewOrderTemp{
        tpcc_id_t o_id;
        double total_amount = 0.0;
    };
    struct PaymentInput{
        TpccClient *client;
        uint32_t w_id, d_id, c_id;
        tpcc_id_t c_d_id, c_w_id;
        float h_amount;
        Key_t history_key;
        uint32_t scan_count;
        varibale_str<16> last_name;
        bool by_last_name;
    };
    struct OrderStatusInput {
        TpccClient *client;
        uint32_t w_id, d_id, c_id;
        uint32_t scan_count;
        varibale_str<16> last_name;
        bool by_last_name;
    };
    struct OrderStatusTemp{
        tpcc_id_t o_id;
        uint32_t num;
    };
    struct DeliveryInput {
        TpccClient *client;
        uint32_t w_id;
        uint32_t o_carrier_id, ol_delivery_d;
        uint32_t original_oid[16];
    };
    struct DeliveryTemp{
        tpcc_id_t o_ids[DIST_PER_WARE];
        uint32_t ol_counts[DIST_PER_WARE];
        tpcc_id_t o_c_ids[DIST_PER_WARE];
        bool not_oks[DIST_PER_WARE];
    };
    struct StockLevelInput {
        TpccClient *client;
        uint32_t w_id, d_id, threshold;
    };
    struct StockLevelTemp{
        tpcc_id_t o_id;
        std::set<Key_t> k_s_set;
    };

   public:
    WorkloadTpcc(Configuration &conf, uint32_t num_segs, uint32_t num_threads,
                 uint32_t num_clients, uint32_t remote_neworder_item_pro, uint32_t remote_payment_pro, 
                 uint32_t num_partition_per_db)
        : BenchmarkInterface(),
          conf(conf),
          num_logical_sn(conf.numLogicalSN()),
          num_threads(num_threads),
          num_clients(num_clients),
          remote_neworder_item_pro(remote_neworder_item_pro),
          remote_payment_pro(remote_payment_pro),
          num_warehouse(num_partition_per_db * conf.numDB()),
          num_warehouse_per_db(num_warehouse / conf.numDB()),
          num_warehouse_per_worker(num_warehouse_per_db / num_threads),
          num_warehouse_per_sn((num_warehouse + num_logical_sn - 1) / num_logical_sn),
          free_clients(num_threads) {
        if (remote_neworder_item_pro == 0) {
            ASSERT(remote_payment_pro == 0);
        }

        ASSERT(PERCENT_NEW_ORDER + PERCENT_PAYMENT + PERCENT_DELIVERY + PERCENT_ORDERSTATUS +
                   PERCENT_STOCKLEVEL == 100) << "please check transactions mixing ratio";
        ASSERT(num_warehouse_per_db == num_partition_per_db);

        num_segs_per_warehouse = (num_segs + num_warehouse - 1) / num_warehouse;
        num_segs = num_segs_per_warehouse * num_warehouse_per_sn * num_logical_sn;
        ASSERT(num_segs % num_logical_sn == 0);
        this->conf.setNumSegments(num_segs);

        used_pages_per_warehouse = init_table_information();
        LOG(2) << VAR2(used_pages_per_warehouse, " ");

        uint32_t used_segs_per_warehouse = used_pages_per_warehouse / NUM_PAGES_PER_SEGMENT;
        ASSERT(used_segs_per_warehouse <= num_segs_per_warehouse) 
            << used_segs_per_warehouse << " " << num_segs_per_warehouse;

        for (uint32_t i = 0; i < num_clients; i++) {
            TpccClient *c = new TpccClient();
            c->belong_wid = num_warehouse_per_db * conf.get_my_db_id() +
                            thread_rand.randint(0, num_warehouse_per_db - 1) + 1;
            free_clients.enqueue(c, i % num_threads);
        }

        init_tables();
        // hot_keys_item();
    }
    ~WorkloadTpcc() {
        for (uint w = 0; w < num_warehouse; w++) {
            if (pages) {
                for (uint32_t page_i = 0; page_i < used_pages_per_warehouse; ++page_i) {
                    if(pages[w] != nullptr){
                        delete pages[w][page_i];
                    }
                }
            }
        }
        delete [] pages;
        delete [] tables;
    }

    inline node_id_t wid_to_sn_id(uint wid) {
        return (wid - 1) / num_warehouse_per_sn;
    }

    inline node_id_t next_sn_of(node_id_t sn_id) {
        return (sn_id + 1) % num_logical_sn;
    }

    uint32_t get_num_clients() override { return num_clients; };

    uint64_t get_time_duration() override { return TEST_DURATION; }

    inline Key_t tpcc_key(uint table_name, uint key_i) {
        if (table_name == WAREHOUSE)
            return 0;
        ASSERT(key_i <= records_per_table[table_name]) << key_i << " " << table_name;
        Key_t key = table_key_offsets[table_name] + key_i;
        return key;
    }

    inline Key_t tpcc_cust_key(int d, int c) {
        return tpcc_key(CUSTOMER, (d - 1) * CUST_PER_DIST + c - 1);
    }

    inline Key_t tpcc_cust_last_order_key(int d, int c) {
        return tpcc_key(CUSTOMER_LAST_ORDER, (d - 1) * CUST_PER_DIST + c - 1);
    }

    inline uint32_t district_table_i(uint32_t d_id) {
        return DISTRICT1 + d_id - 1;
    }

    inline Key_t order_key_i(int table_name, int d, int oid) {
        return d * 10000 + oid % (records_per_table[table_name] / DIST_PER_WARE);
    }

    inline Key_t tpcc_order_key(int table_name, int key_i) {
        return tpcc_key(table_name, key_i);
    }

    inline Key_t tpcc_order_key(int table_name, int d, int oid) {
        return tpcc_key(table_name, order_key_i(table_name, d, oid));
    }

    inline Key_t orderline_key_i(int d, int oid, int ol) {
        return d * 10000 + (oid * 15) % (records_per_table[ORDERLINE] / DIST_PER_WARE) + ol;
    }

    inline Key_t tpcc_orderline_key(int key_i) {
        return tpcc_key(ORDERLINE, key_i);  // max 15 lines per order
    }

    inline Key_t tpcc_orderline_key(int d, int oid, int ol) {
        return tpcc_key(ORDERLINE, orderline_key_i(d, oid, ol));  // max 15 lines per order
    }

    inline Key_t tpcc_history_key(thread_id_t worker_id) {
        // TODO: fix this after we support insert operation
        // since we don't support insertion, and history is a write-only table,
        // we just randomly update a line and wish there is no conflict.
        const tpcc_id_t num_tables_per_thread = records_per_table[HISTORY] / num_threads;
        const tpcc_id_t table_n = worker_id * num_tables_per_thread + 
            thread_rand.randint(0, num_tables_per_thread - 1);
        return tpcc_key(HISTORY, table_n);
    }

    inline uint64_t tpcc_page_off(int table_name, int key_i) {
        return key_i / records_per_page[table_name];
    }

    void init_tables() {
        tables = new Tables[num_warehouse];
        pages = new LocalPBtreeNode<Key_t, Value_t> **[num_warehouse];
        for (uint32_t w = 0; w < num_warehouse; ++w) {
            tables[w].set_table_id(w);
            pages[w] = nullptr;
        }
    }

    std::multimap<Key_t, Key_t> init_tpcc_pages(uint32_t w, bool has_locality) {
        Tables& tabs = tables[w];
        if (has_locality) {
            fast_random r = fast_random(w);
            {
                W_val v;
                v.w_ytd = 300000;
                v.w_tax = (float)RandomNumber(r, 0, 2000) / 10000.0;
                v.w_name.assign(RandomStr(r, RandomNumber(r, 6, 10)));
                v.w_street_1.assign(RandomStr(r, RandomNumber(r, 10, 20)));
                v.w_street_2.assign(RandomStr(r, RandomNumber(r, 10, 20)));
                v.w_city.assign(RandomStr(r, RandomNumber(r, 10, 20)));
                v.w_state.assign(RandomStr(r, 3));
                v.w_zip.assign("123456789");
                tabs.w_nodes[tpcc_page_off(WAREHOUSE, 1)]->put(
                    tpcc_key(WAREHOUSE, 1), v, NULL_TXN_ID);
            }

            for (uint32_t i = 1; i <= DIST_PER_WARE; i++) {
                uint d_tab_i = district_table_i(i);
                uint page_off = tpcc_page_off(d_tab_i, 0);
                
                {
                    Key_t key = tpcc_key(d_tab_i, 0);
                    D_val v;
                    v.d_ytd = 30000;
                    v.d_tax = (float)RandomNumber(r, 0, 2000) / 10000.0;
                    v.d_next_o_id = 3001;
                    v.d_name.assign(RandomStr(r, RandomNumber(r, 6, 10)));
                    v.d_street1.assign(RandomStr(r, RandomNumber(r, 10, 20)));
                    v.d_street2.assign(RandomStr(r, RandomNumber(r, 10, 20)));
                    v.d_city.assign(RandomStr(r, RandomNumber(r, 10, 20)));
                    v.d_state.assign(RandomStr(r, 3));
                    v.d_zip.assign("123456789");
                    tabs.d_nodes[d_tab_i-1][page_off]->put(key, v, NULL_TXN_ID);
                }

                {
                    Key_t key = tpcc_key(d_tab_i, 1);
                    D_val v;
                    v.d_next_o_id = 3001;
                    tabs.d_nodes[d_tab_i-1][page_off]->put(key, v, NULL_TXN_ID);
                }
            }

            // last order id of the customer
            for (uint32_t d = 1; d <= DIST_PER_WARE; d++) {
                for (uint32_t c = 1; c <= CUST_PER_DIST; c++) {
                    Value_t v;
                    v.current_size = 1;
                    v.s[0] = -1;

                    uint page_off = tpcc_page_off(CUSTOMER_LAST_ORDER, (d - 1) * CUST_PER_DIST + c - 1);
                    uint key = tpcc_cust_last_order_key(d, c);
                    tabs.cl_nodes[page_off]->put(key, v, NULL_TXN_ID);
                }
            }

            uint32_t ts = w * DIST_PER_WARE * CUST_PER_DIST;
            for (uint32_t d = 1; d <= DIST_PER_WARE; d++) {
                // o_c_id selected sequentially from a random permutation of [1 .. 3,000]
                std::set<uint> c_ids_s;
                std::vector<uint> c_ids;
                while (c_ids.size() != CUST_PER_DIST) {
                    const auto x = (r.next() % CUST_PER_DIST) + 1;
                    if (c_ids_s.count(x)) continue;
                    c_ids_s.insert(x);
                    c_ids.emplace_back(x);
                }
                for (uint32_t c = 1; c <= CUST_PER_DIST; c++) {
                    // order table
                    O_val v_o;
                    v_o.o_c_id = c_ids[c - 1];
                    if (c < 2101)
                        v_o.o_carrier_id = RandomNumber(r, 1, 10);
                    else
                        v_o.o_carrier_id = 0;
                    v_o.o_ol_cnt = RandomNumber(r, 5, 15);
                    v_o.o_all_local = 1;
                    v_o.o_entry_d = ++ts;

                    Key_t k_o_i = order_key_i(ORDER, d, c);
                    uint page_off = tpcc_page_off(ORDER, k_o_i);
                    uint key = tpcc_order_key(ORDER, k_o_i);
                    tabs.o_nodes[page_off]->put(key, v_o, NULL_TXN_ID);

                    // 900 rows in the NEW-ORDER table corresponding
                    // to the last 900 rows in the ORDER
                    if (c >= 2101) {
                        NO_val v_no{};
                        v_no.no_dummy.assign("a");
                        v_no.no_exist = true;

                        Key_t k_no_i = order_key_i(NEWORDER, d, c);
                        uint page_off = tpcc_page_off(NEWORDER, k_no_i);
                        uint key = tpcc_order_key(NEWORDER, k_no_i);
                        tabs.no_nodes[page_off]->put(key, v_no, NULL_TXN_ID);
                    }

                    // order line
                    for (uint32_t l_i = 0; l_i < v_o.o_ol_cnt; l_i++) {
                        OL_val v_ol;
                        v_ol.ol_i_id = RandomNumber(r, 1, 100000);
                        if (c < 2101) {
                            v_ol.ol_delivery_d = v_o.o_entry_d;
                            v_ol.ol_amount = 0;
                        } else {
                            v_ol.ol_delivery_d = 0;
                            v_ol.ol_amount = (float)(RandomNumber(r, 1, 999999) / 100.0);
                        }
                        v_ol.ol_supply_w_id = w + 1;
                        v_ol.ol_quantity = 5;

                        Key_t k_ol_i = orderline_key_i(d, c, l_i);
                        uint page_off = tpcc_page_off(ORDERLINE, k_ol_i);
                        uint key = tpcc_orderline_key(k_ol_i);
                        tabs.ol_nodes[page_off]->put(key, v_ol, NULL_TXN_ID);
                    }
                }
            }

            // item
            for (uint32_t i = 1; i <= MAXITEMS; i++) {
                I_val v;
                v.i_name.assign(RandomStr(r, RandomNumber(r, 14, 24)));
                v.i_price = (float)RandomNumber(r, 100, 10000) / 100.0;
                const int len = RandomNumber(r, 26, 50);
                if (RandomNumber(r, 1, 100) > 10) {
                    v.i_data.assign(RandomStr(r, len));
                } else {
                    const int startOriginal = RandomNumber(r, 2, (len - 8));
                    v.i_data.assign(RandomStr(r, startOriginal + 1) + "ORIGINAL" +
                                    RandomStr(r, len - startOriginal - 7));
                }
                v.i_im_id = RandomNumber(r, 1, 10000);

                uint page_off = tpcc_page_off(ITEM, i - 1);
                uint key = tpcc_key(ITEM, i - 1);
                tabs.i_nodes[page_off]->put(key, v, NULL_TXN_ID);
            }

        }
        fast_random r = fast_random(w);

        // stock
        for (uint32_t i = 1; i <= MAXITEMS; i++) {
            S_val v;
            v.s_quantity = RandomNumber(r, 10, 100);
            v.s_ytd = 0;
            v.s_order_cnt = 0;
            v.s_remote_cnt = 0;
            uint page_off = tpcc_page_off(STOCK, i - 1);
            uint key = tpcc_key(STOCK, i - 1);
            tabs.s_nodes[page_off]->put(key, v, NULL_TXN_ID);
        }

        // stock_data
        for (uint32_t i = 1; i <= MAXITEMS; i++) {
            SD_val v;
            const int len = RandomNumber(r, 26, 50);
            if (RandomNumber(r, 1, 100) > 10) {
                v.s_data.assign(RandomStr(r, len));
            } else {
                const int startOriginal = RandomNumber(r, 2, (len - 8));
                v.s_data.assign(RandomStr(r, startOriginal + 1) + "ORIGINAL" +
                                RandomStr(r, len - startOriginal - 7));
            }
            v.s_dist_01.assign(RandomStr(r, 24));
            v.s_dist_02.assign(RandomStr(r, 24));
            v.s_dist_03.assign(RandomStr(r, 24));
            v.s_dist_04.assign(RandomStr(r, 24));
            v.s_dist_05.assign(RandomStr(r, 24));
            v.s_dist_06.assign(RandomStr(r, 24));
            v.s_dist_07.assign(RandomStr(r, 24));
            v.s_dist_08.assign(RandomStr(r, 24));
            v.s_dist_09.assign(RandomStr(r, 24));
            v.s_dist_10.assign(RandomStr(r, 24));

            uint page_off = tpcc_page_off(STOCK_DATA, i - 1);
            uint key = tpcc_key(STOCK_DATA, i - 1);
            tabs.sd_nodes[page_off]->put(key, v, NULL_TXN_ID);
        }

        // customer
        std::multimap<Key_t, Key_t> cn_map;
        for (uint32_t d = 1; d <= DIST_PER_WARE; d++) {
            for (uint32_t c = 1; c <= CUST_PER_DIST; c++) {
                C_val v;
                v.c_discount = (float)(RandomNumber(r, 1, 5000) / 10000.0);
                if (RandomNumber(r, 1, 100) <= 10)
                    v.c_credit.assign("BC");
                else
                    v.c_credit.assign("GC");

                std::string last_name;
                if (c <= 1000)
                    last_name = GetCustomerLastName(r, c - 1);
                else
                    last_name = GetNonUniformCustomerLastName(r, random_C_C_LAST_LOAD);
                v.c_last.assign(last_name);

                std::string first_name = RandomStr(r, RandomNumber(r, 8, 16));
                v.c_first.assign(first_name);

                v.c_credit_lim = 50000;

                v.c_balance = -10;
                v.c_ytd_payment = 10;
                v.c_payment_cnt = 1;
                v.c_delivery_cnt = 0;

                v.c_street_1.assign(RandomStr(r, RandomNumber(r, 10, 20)));
                v.c_street_2.assign(RandomStr(r, RandomNumber(r, 10, 20)));
                v.c_city.assign(RandomStr(r, RandomNumber(r, 10, 20)));
                v.c_state.assign(RandomStr(r, 3));
                v.c_zip.assign(RandomNStr(r, 4) + "11111");
                v.c_phone.assign(RandomNStr(r, 16));
                v.c_since = GetCurrentTimeMillis();
                v.c_middle.assign("OE");
                v.c_data.assign(RandomStr(r, RandomNumber(r, 300, 500)));

                uint page_off = tpcc_page_off(CUSTOMER, (d - 1) * CUST_PER_DIST + c - 1);
                Key_t key = tpcc_cust_key(d, c);
                tabs.c_nodes[page_off]->put(key, v, NULL_TXN_ID);

                Key_t cn_key = GetCustomerNameIndex(d, last_name);
                cn_map.insert({cn_key, c});
            }
        }
        
        return cn_map;
    }

    template<class Value_t>
    void load_page(uint32_t table_name, PBtree<Key_t, Value_t>* tbl,
            LocalPBtreeNode<Key_t, Value_t> **nodes,
            uint32_t off, uint32_t w, Key_t& key_base, 
            bool has_locality,
            alloc_page_func_t alloc_page_func) {
        uint32_t r_per_table = records_per_page[table_name];
        if (has_locality) {
            Value_t null_val;
            //TODO: remove
            memset(&null_val, 0, sizeof(Value_t));

            uint32_t page_i = page_i_offsets[table_name] + off;
            seg_id_t seg_id = num_segs_per_warehouse * w + page_i / NUM_PAGES_PER_SEGMENT;
            page_id_t page_id = page_i % NUM_PAGES_PER_SEGMENT;
            PageMeta *page_meta = alloc_page_func(seg_id, page_id);
            auto* table = new LocalPBtreeNode<Key_t, Value_t>;
            table->set_page_meta(page_meta);

            uint32_t v_size = value_size[table_name];
            table->initialize(v_size);
            for (uint64_t i = 0; i < r_per_table; ++i) {
                table->insert(key_base + i, null_val, NULL_TXN_ID);
            }
            table->update_byte_size();
            nodes[off] = table;
            tbl->insert_range(key_base, table);
        }
        key_base += r_per_table;
    }

    template<class Value_t>
    void load_page_with_map(uint32_t table_name,
            PBtree<Key_t, Value_t>* tbl,
            LocalPBtreeNode<Key_t, Value_t> **nodes,
            std::multimap<Key_t, Value_t>& v_map,
            uint32_t w, alloc_page_func_t alloc_page_func) {
        uint32_t r_per_table = records_per_page[table_name];
        uint32_t v_size = value_size[table_name];
        uint32_t page_i_offset = page_i_offsets[table_name];
        uint32_t page_i = page_i_offset;
        LocalPBtreeNode<Key_t, Value_t>* table = nullptr;
        uint32_t count = 0;
        for (auto &pair : v_map) {
            if (table == nullptr) {
                seg_id_t seg_id = num_segs_per_warehouse * w + page_i / NUM_PAGES_PER_SEGMENT;
                page_id_t page_id = page_i % NUM_PAGES_PER_SEGMENT;
                PageMeta *page_meta = alloc_page_func(seg_id, page_id);
                table = new LocalPBtreeNode<Key_t, Value_t>;
                table->set_page_meta(page_meta);
                table->initialize(v_size);
                tbl->insert_range(pair.first, table);
            }
            // LOG(2) << std::hex << pair.first << " " << pair.second;
            table->insert(pair.first, pair.second, NULL_TXN_ID);
            if (++count == r_per_table) {
                table->update_byte_size();
                uint32_t off = page_i++ - page_i_offset;
                nodes[off] = table;
                table = nullptr;
                count = 0;
            }
            // LOG(2) << std::hex << pair.first << " " << pair.second;
        }
        if (table) {
            table->update_byte_size();
            uint32_t off = page_i - page_i_offset;
            nodes[off] = table;
        }
    }

    template<class Value_t>
    void load_one_table(uint32_t table_name,
            PBtree<Key_t, Value_t>* tbl,
            LocalPBtreeNode<Key_t, Value_t> **nodes,
            uint32_t w, bool has_locality,
            alloc_page_func_t alloc_page_func) {
        Key_t key_base = table_key_offsets[table_name];
        for (uint32_t i = 0; i < page_used[table_name]; ++i) {
            load_page<Value_t>(table_name, tbl, nodes, 
                i, w, key_base, has_locality, alloc_page_func);
        }
    }

    void load_one_warehouse(uint32_t w, alloc_page_func_t alloc_page_func,
                            bool has_locality) {
        thread_rand.set_seed(w);
        auto* nodes = new LocalPBtreeNode<Key_t, Value_t> *[used_pages_per_warehouse];
        pages[w] = nodes;
        Tables& tabs = tables[w];

        load_one_table<W_val>(WAREHOUSE, &tabs.w_tab, tabs.w_nodes, w, has_locality, alloc_page_func);
        
        for (uint32_t d = 0 ; d < DIST_PER_WARE; ++d) {
            uint32_t t_name = DISTRICT1 + d;
            load_one_table<D_val>(t_name, &tabs.d_tab[d], tabs.d_nodes[d], w, has_locality, alloc_page_func);
        }

        load_one_table<C_val>(CUSTOMER, &tabs.c_tab, tabs.c_nodes, w, true, alloc_page_func);
        load_one_table<Value_t>(CUSTOMER_LAST_ORDER, &tabs.cl_tab, tabs.cl_nodes, w, has_locality, alloc_page_func);
        load_one_table<O_val>(ORDER, &tabs.o_tab, tabs.o_nodes, w, has_locality, alloc_page_func);
        load_one_table<NO_val>(NEWORDER, &tabs.no_tab, tabs.no_nodes, w, has_locality, alloc_page_func);
        load_one_table<I_val>(ITEM, &tabs.i_tab, tabs.i_nodes, w, has_locality, alloc_page_func);
        load_one_table<S_val>(STOCK, &tabs.s_tab, tabs.s_nodes, w, true, alloc_page_func);
        load_one_table<SD_val>(STOCK_DATA, &tabs.sd_tab, tabs.sd_nodes, w, true, alloc_page_func);
        load_one_table<OL_val>(ORDERLINE, &tabs.ol_tab, tabs.ol_nodes, w, has_locality, alloc_page_func);
        load_one_table<H_val>(HISTORY, &tabs.h_tab, tabs.h_nodes, w, has_locality, alloc_page_func);

        auto cn_map = init_tpcc_pages(w, has_locality);
        load_page_with_map<Key_t>(CUSTOMER_NAME, &tabs.cn_tab, tabs.cn_nodes, 
            cn_map, w, alloc_page_func);

        LOG(2) << VAR2(w, " ") << (has_locality ? "local" : "remote");

        // Key_t key = tpcc_key(WAREHOUSE, 1);
        // LOG(2) << "warehouse " << w + 1 << " tax: 0." << leaf.get(key)->value;
    }

    void init_database(DatabaseNode &db_node) override {
        std::vector<std::thread> init_threads;
        this->dbnode = &db_node;

        auto init_func = [this, &db_node](uint w, bool has_locality) {
            auto alloc_page_func = [&db_node](seg_id_t seg_id, page_id_t page_id) {
                return db_node.init_empty_page(seg_id, page_id);
            };
            load_one_warehouse(w, alloc_page_func, has_locality);
        };

        node_id_t my_first_wid = get_first_wid_of(conf.get_my_db_id());
        for (uint w = 0; w < num_warehouse; w++) {
            bool has_locality = (w + 1 >= my_first_wid && w + 1 < my_first_wid + num_warehouse_per_db);
            tables[w].init(this);
            init_threads.emplace_back(init_func, w, has_locality);
        }
        for (uint w = 0; w < init_threads.size(); w++) {
            init_threads[w].join();
        }
        LOG(2) << "TPC-C initializtion finished";
    }

    void init_storage(InMemSegments &segments) override {
        std::vector<std::thread> init_threads;
        auto init_func = [this, &segments](uint w) {
            auto alloc_page_func = [&segments](seg_id_t seg_id, page_id_t page_id) {
                auto* page_meta = segments.alloc_next_page(seg_id);
                return page_meta;
            };
            load_one_warehouse(w, alloc_page_func, true);
        };
        for (uint w = 0; w < num_warehouse; w++) {
            node_id_t primary_sn_id = wid_to_sn_id(w + 1);
#ifdef NEW_REPLICATION
            if (conf.isMyReplica(primary_sn_id)) {
#else
            if (primary_sn_id == conf.get_my_logical_sn_id()) {
#endif
                tables[w].init(this);
                init_threads.emplace_back(init_func, w);
            }
        }
        for (uint i = 0; i < init_threads.size(); i++) {
            init_threads[i].join();
        }
        LOG(2) << "init finished";
    }

    DbTxn::txn_logic_t get_txn_logic(uint32_t txn_seed) override {
        if (txn_seed < PERCENT_NEW_ORDER) {
            return get_txn_logic_neworder();
        } else if (txn_seed < PERCENT_NEW_ORDER + PERCENT_PAYMENT) {
            return get_txn_logic_payment();
        } else if (txn_seed < PERCENT_NEW_ORDER + PERCENT_PAYMENT + PERCENT_DELIVERY) {
            return get_txn_logic_delivery();
        } else if (txn_seed <
                   PERCENT_NEW_ORDER + PERCENT_PAYMENT + PERCENT_DELIVERY + PERCENT_ORDERSTATUS) {
            return get_txn_logic_orderstatus();
        } else {
            return get_txn_logic_stocklevel();
        }
    }

    DbTxn::partition_logic_func get_partition_logic() override {
        return [this](table_id_t table_id, node_id_t sn_id) {
            node_id_t primary_sn_id = wid_to_sn_id(table_id + 1);
            // bool is_local = conf.isReplicaOf(primary_sn_id, sn_id);
            bool is_local = primary_sn_id == sn_id;
            return is_local;
        };
    }

    void *get_input(DbClient *c) override {
        if (c->txn_seed < PERCENT_NEW_ORDER) {
            return get_input_neworder();
        } else if (c->txn_seed < PERCENT_NEW_ORDER + PERCENT_PAYMENT) {
            return get_input_payment();
        } else if (c->txn_seed < PERCENT_NEW_ORDER + PERCENT_PAYMENT + PERCENT_DELIVERY) {
            return get_input_delivery();
        } else if (c->txn_seed <
                   PERCENT_NEW_ORDER + PERCENT_PAYMENT + PERCENT_DELIVERY + PERCENT_ORDERSTATUS) {
            return get_input_orderstatus();
        } else {
            return get_input_stocklevel();
        }
    }

    uint32_t get_input_size(uint32_t txn_seed) override {
        if (txn_seed < PERCENT_NEW_ORDER) {
            return sizeof(NewOrderInput);
        } else if (txn_seed < PERCENT_NEW_ORDER + PERCENT_PAYMENT) {
            return sizeof(PaymentInput);
        } else if (txn_seed < PERCENT_NEW_ORDER + PERCENT_PAYMENT + PERCENT_DELIVERY) {
            return sizeof(DeliveryInput);
        } else if (txn_seed <
                   PERCENT_NEW_ORDER + PERCENT_PAYMENT + PERCENT_DELIVERY + PERCENT_ORDERSTATUS) {
            return sizeof(OrderStatusInput);
        } else {
            return sizeof(StockLevelInput);
        }
    }

    GlobalPageId get_g_page_id(void *input, uint i) override { return GlobalPageId(0); }

    void check_correctness_reduce(std::vector<uint64_t> &sums) override {}

    uint64_t check_correctness_map(InMemSegments &segments) override { return 0; }

    void print_debug_msg() override {
        // LOG(2) << success_delivery << "/" << num_delivery << " = "
        //        << (double)success_delivery / num_delivery;
    }

   private:
    // **********************************************************************************
    // **                                Txn 1                                         **
    // **********************************************************************************
    DbTxn::txn_logic_t get_txn_logic_neworder() {
#ifndef NON_CACHING
        auto execution_logic = [this](DbTxn *txn, thread_id_t worker_id) {
            auto &input = txn->get_input_as_ref<NewOrderInput>();
            Tables& tabs = tables[input.w_id-1];

            txn->fastCommitEnabled = WORKLOAD_FAST_PATH;
            txn->txn_type = TXN_NEWORDER;
            txn->txn_input_size = sizeof(NewOrderInput);

            // 1st retrieval
            Key_t k_w = tpcc_key(WAREHOUSE, 1);
            W_val warehouse = txn->get(tabs.w_tab, k_w, true, false);
            double w_tax = warehouse.w_tax;
            input.w_tax = w_tax;

            // 2nd retrieval
            // 1st update
            uint32_t d_tab_i = district_table_i(input.d_id);
            D_val dist = txn->get(tabs.d_tab[d_tab_i-1], tpcc_key(d_tab_i, 0), true, true);
            // D_val dist = txn->get_print(tabs.d_tab[d_tab_i-1], tpcc_key(d_tab_i, 0), true, true);
            double d_tax = dist.d_tax;
            input.d_tax = d_tax;
            tpcc_id_t o_id = dist.d_next_o_id++;
            txn->put(tabs.d_tab[d_tab_i-1], tpcc_key(d_tab_i, 0), dist);

            // 3rd retrieval
            C_val customer = txn->get(tabs.c_tab, 
                tpcc_cust_key(input.d_id, input.c_id), true, false);
            double c_discount = customer.c_discount;
            input.c_discount = c_discount;

            Key_t clo_key = tpcc_cust_last_order_key(
                input.d_id, input.c_id);
            Value_t v_cl = txn->get(tabs.cl_tab, clo_key, true, true);
            v_cl.s[0] = o_id;
            txn->put(tabs.cl_tab, clo_key, v_cl);

            // 1st insertion
            Key_t order_key = tpcc_order_key(ORDER, input.d_id, o_id);
            O_val v_o;
            v_o.o_ol_cnt = input.ol_cnt;
            v_o.o_all_local = input.ol_all_local;
            v_o.o_carrier_id = 0;
            v_o.o_c_id = input.c_id;
            v_o.o_entry_d = input.o_entry_d;
            txn->put(tabs.o_tab, order_key, v_o);

            // 2nd insertion
            Key_t neworder_key = tpcc_order_key(NEWORDER, input.d_id, o_id);
            NO_val v_no;
            v_no.no_dummy.assign("no");
            v_no.no_exist = true;
            txn->put(tabs.no_tab, neworder_key, v_no);
            
            double total_amount = 0.0;
            OL_val v_ol;
            for (uint ol_num = 0; ol_num < input.ol_cnt; ol_num++) {
                auto &item = input.items[ol_num];
                I_val v_i = txn->get_ro(tabs.i_tab, tpcc_key(ITEM, item.i_id - 1));
                double price = v_i.i_price;

                item.price = price;

                Tables& s_tabs = tables[item.ol_supply_wid - 1];
                Key_t k_s = tpcc_key(STOCK, item.i_id - 1);
                S_val v_s = txn->get(s_tabs.s_tab, k_s, true, true);  // 2nd retrieval
                if (v_s.s_quantity >= item.quantity + 10) {
                    v_s.s_quantity -= item.quantity;
                } else {
                    v_s.s_quantity += 91 - item.quantity;
                }
                v_s.s_ytd += item.quantity;
                v_s.s_order_cnt++;
                v_s.s_remote_cnt += (item.ol_supply_wid == input.w_id) ? 0 : 1;
                txn->put(s_tabs.s_tab, k_s, v_s);  // 1st update

                Key_t k_sd = tpcc_key(STOCK_DATA, item.i_id - 1);
                SD_val v_sd = txn->get_ro(s_tabs.sd_tab, k_sd);  // 2nd retrieval

                double ol_amount = item.quantity * price;
                total_amount += ol_amount;
                Key_t ol_key = tpcc_orderline_key(input.d_id, o_id, ol_num);

                uint32_t off = ol_key - table_key_offsets[ORDERLINE];
                ASSERT(off < records_per_table[ORDERLINE]) 
                    << VAR2(off, " ") << VAR2(input.d_id, " ") << VAR2(o_id, " ") << VAR2(ol_num, " ");

                v_ol.ol_i_id = item.i_id;
                v_ol.ol_supply_w_id = item.ol_supply_wid;
                v_ol.ol_quantity = item.quantity;
                v_ol.ol_amount = ol_amount;
                v_ol.ol_delivery_d = 0;

                switch (input.d_id) {
                    case 1:
                    v_ol.ol_dist.assign((char*)v_sd.s_dist_01.buf);
                    break;
                    case 2:
                    v_ol.ol_dist.assign((char*)v_sd.s_dist_02.buf);
                    break;
                    case 3:
                    v_ol.ol_dist.assign((char*)v_sd.s_dist_03.buf);
                    break;
                    case 4:
                    v_ol.ol_dist.assign((char*)v_sd.s_dist_04.buf);
                    break;
                    case 5:
                    v_ol.ol_dist.assign((char*)v_sd.s_dist_05.buf);
                    break;
                    case 6:
                    v_ol.ol_dist.assign((char*)v_sd.s_dist_06.buf);
                    break;
                    case 7:
                    v_ol.ol_dist.assign((char*)v_sd.s_dist_07.buf);
                    break;
                    case 8:
                    v_ol.ol_dist.assign((char*)v_sd.s_dist_08.buf);
                    break;
                    case 9:
                    v_ol.ol_dist.assign((char*)v_sd.s_dist_09.buf);
                    break;
                    case 10:
                    v_ol.ol_dist.assign((char*)v_sd.s_dist_10.buf);
                    break;
                    default:
                    ASSERT(false);
                    break;
                }
                txn->put(tabs.ol_tab, ol_key, v_ol);
            }
            total_amount = total_amount * (1 + w_tax + d_tax) * (1 - c_discount);
            // LOG(2) << VAR(total_amount);
            return true;
        };
#else
        auto execution_logic = [this](DbTxn *txn, thread_id_t worker_id) {
            auto &input = txn->get_input_as_ref<NewOrderInput>();
            Tables& tabs = tables[input.w_id-1];
            auto* temp = txn->get_or_new_temp<NewOrderTemp>();

            if(txn->phase == 0) {
                txn->fastCommitEnabled = WORKLOAD_FAST_PATH;
                txn->txn_type = TXN_NEWORDER;
                txn->txn_input_size = sizeof(NewOrderInput);

                // 1st retrieval
                Key_t k_w = tpcc_key(WAREHOUSE, 1);
                W_val warehouse = txn->remote_get(tabs.w_tab, k_w, true, false);
                double w_tax = warehouse.w_tax;
                input.w_tax = w_tax;

                txn->read_remote();
                txn->phase += 1;
                return false;
            }

            if(txn->phase == 1) {
                uint32_t d_tab_i = district_table_i(input.d_id);
                D_val dist = txn->remote_get(tabs.d_tab[d_tab_i-1], tpcc_key(d_tab_i, 0), true, true);
                double d_tax = dist.d_tax;
                input.d_tax = d_tax;
                tpcc_id_t o_id = dist.d_next_o_id++;
                temp->o_id = o_id;
                txn->put(tabs.d_tab[d_tab_i-1], tpcc_key(d_tab_i, 0), dist);

                // 3rd retrieval
                C_val customer = txn->remote_get(tabs.c_tab, 
                    tpcc_cust_key(input.d_id, input.c_id), true, false);
                double c_discount = customer.c_discount;
                input.c_discount = c_discount;

                Key_t clo_key = tpcc_cust_last_order_key(
                    input.d_id, input.c_id);
                Value_t v_cl = txn->get(tabs.cl_tab, clo_key, true, true);
                v_cl.s[0] = o_id;
                txn->put(tabs.cl_tab, clo_key, v_cl);

                // 1st insertion
                Key_t order_key = tpcc_order_key(ORDER, input.d_id, o_id);
                O_val v_o;
                v_o.o_ol_cnt = input.ol_cnt;
                v_o.o_all_local = input.ol_all_local;
                v_o.o_carrier_id = 0;
                v_o.o_c_id = input.c_id;
                v_o.o_entry_d = input.o_entry_d;
                txn->put(tabs.o_tab, order_key, v_o);

                // 2nd insertion
                Key_t neworder_key = tpcc_order_key(NEWORDER, input.d_id, o_id);
                NO_val v_no;
                v_no.no_dummy.assign("no");
                v_no.no_exist = true;
                txn->put(tabs.no_tab, neworder_key, v_no);
            
                txn->read_remote();
                txn->phase += 1;
                return false;
            }   

            if(txn->phase == 2) {
                OL_val v_ol;
                for (uint ol_num = 0; ol_num < input.ol_cnt; ol_num++) {
                    auto &item = input.items[ol_num];
                    I_val v_i = txn->remote_get_ro(tabs.i_tab, tpcc_key(ITEM, item.i_id - 1));
                    double price = v_i.i_price;

                    item.price = price;

                    Tables& s_tabs = tables[item.ol_supply_wid - 1];
                    Key_t k_s = tpcc_key(STOCK, item.i_id - 1);
                    S_val v_s = txn->remote_get(s_tabs.s_tab, k_s, true, true);  // 2nd retrieval
                    if (v_s.s_quantity >= item.quantity + 10) {
                        v_s.s_quantity -= item.quantity;
                    } else {
                        v_s.s_quantity += 91 - item.quantity;
                    }
                    v_s.s_ytd += item.quantity;
                    v_s.s_order_cnt++;
                    v_s.s_remote_cnt += (item.ol_supply_wid == input.w_id) ? 0 : 1;
                    txn->put(s_tabs.s_tab, k_s, v_s);  // 1st update

                    Key_t k_sd = tpcc_key(STOCK_DATA, item.i_id - 1);
                    SD_val v_sd = txn->get_ro(s_tabs.sd_tab, k_sd);  // 2nd retrieval

                    double ol_amount = item.quantity * price;
                    temp->total_amount += ol_amount;
                    Key_t ol_key = tpcc_orderline_key(input.d_id, temp->o_id, ol_num);

                    uint32_t off = ol_key - table_key_offsets[ORDERLINE];
                    ASSERT(off < records_per_table[ORDERLINE]) 
                        << VAR2(off, " ") << VAR2(input.d_id, " ") << VAR2(temp->o_id, " ") << VAR2(ol_num, " ");

                    v_ol.ol_i_id = item.i_id;
                    v_ol.ol_supply_w_id = item.ol_supply_wid;
                    v_ol.ol_quantity = item.quantity;
                    v_ol.ol_amount = ol_amount;
                    v_ol.ol_delivery_d = 0;

                    switch (input.d_id) {
                        case 1:
                        v_ol.ol_dist.assign((char*)v_sd.s_dist_01.buf);
                        break;
                        case 2:
                        v_ol.ol_dist.assign((char*)v_sd.s_dist_02.buf);
                        break;
                        case 3:
                        v_ol.ol_dist.assign((char*)v_sd.s_dist_03.buf);
                        break;
                        case 4:
                        v_ol.ol_dist.assign((char*)v_sd.s_dist_04.buf);
                        break;
                        case 5:
                        v_ol.ol_dist.assign((char*)v_sd.s_dist_05.buf);
                        break;
                        case 6:
                        v_ol.ol_dist.assign((char*)v_sd.s_dist_06.buf);
                        break;
                        case 7:
                        v_ol.ol_dist.assign((char*)v_sd.s_dist_07.buf);
                        break;
                        case 8:
                        v_ol.ol_dist.assign((char*)v_sd.s_dist_08.buf);
                        break;
                        case 9:
                        v_ol.ol_dist.assign((char*)v_sd.s_dist_09.buf);
                        break;
                        case 10:
                        v_ol.ol_dist.assign((char*)v_sd.s_dist_10.buf);
                        break;
                        default:
                        ASSERT(false);
                        break;
                    }
                    txn->put(tabs.ol_tab, ol_key, v_ol);
                }
                txn->read_remote();
                txn->phase += 1;
                return false;
            }   

            if(txn->phase == 3) {
                temp->total_amount = temp->total_amount * (1 + input.w_tax + input.d_tax) * (1 - input.c_discount);
            }
            // LOG(2) << VAR(total_amount);
            return true;
        };
#endif
        auto repair_logic = [this](DbTxn *txn, thread_id_t worker_id) {
            bool repaired = false;
            auto &input = txn->get_input_as_ref<NewOrderInput>();
            // Tables& tabs = tables[input.w_id-1];
            uint32_t r_code_loc = 0;

            // bool w_repaired = false;
            // Key_t k_w = tpcc_key(WAREHOUSE, 1);
            // W_val warehouse = txn->repair_get(tabs.w_tab, k_w, &w_repaired);
            ++r_code_loc;
            double w_tax = input.w_tax;
            // ASSERT(warehouse.w_tax == w_tax);

            // 2nd retrieval
            // RepairStatus d_r_status{};
            // uint32_t d_tab_i = district_table_i(input.d_id);
            // D_val dist = txn->repair_get(tabs.d_tab[d_tab_i-1], 
            //     tpcc_key(d_tab_i, 0), r_code_loc++, d_r_status);
            // ASSERT (d_r_status != REPAIRED);
            // double d_tax = dist.d_tax;
            r_code_loc++;
            double d_tax = input.d_tax;

            // 3rd retrieval: buffered customer and customer_last_order
            r_code_loc += 2;
            double c_discount = input.c_discount;

            double total_amount = 0.0;
            for (uint ol_num = 1; ol_num <= input.ol_cnt; ol_num++) {
                auto item = input.items[ol_num - 1];
                // 1st retrieval: buffered
                double price = item.price;

                // select from warehouse item.ol_supply_wid
                RepairStatus s_r_status{};
                Tables& s_tabs = tables[item.ol_supply_wid - 1];
                Key_t k_s = tpcc_key(STOCK, item.i_id - 1);
                S_val v_s = txn->repair_get(s_tabs.s_tab, k_s, r_code_loc++, s_r_status);  // 2nd retrieval

                if (s_r_status == REPAIRED) {
                    repaired = true;
                    if (v_s.s_quantity >= item.quantity + 10) {
                        v_s.s_quantity -= item.quantity;
                    } else {
                        v_s.s_quantity += 91 - item.quantity;
                    }
                    v_s.s_ytd += item.quantity;
                    v_s.s_order_cnt++;
                    v_s.s_remote_cnt += (item.ol_supply_wid == input.w_id) ? 0 : 1;
                    txn->repair_put(s_tabs.s_tab, k_s, v_s, true);  // 1st update
                }

                double ol_amount = item.quantity * price;
                total_amount += ol_amount;
            }
            total_amount = total_amount * (1 + w_tax + d_tax) * (1 - c_discount);
            // LOG(2) << "repair finished! " << VAR(total_amount);
            return repaired;
        };

        auto gc_logic = [this](DbTxn *txn, thread_id_t worker_id) {
            auto* input = txn->get_input<NewOrderInput>();
#ifndef RPC_CLIENTS
            this->free_clients.enqueue(input->client, socket_t_id);
#endif
            delete input;

#ifdef NON_CACHING
            txn->delete_temp<NewOrderTemp>();
#endif
            return true;
        };

        return {execution_logic, repair_logic, gc_logic};
    }

    DbTxn::txn_logic_t get_txn_logic_payment() {
        // all customers are based on customer number
#ifndef NON_CACHING
        auto execution_logic = [this](DbTxn *txn, thread_id_t worker_id) {
            txn->fastCommitEnabled = WORKLOAD_FAST_PATH;
            txn->txn_type = TXN_PAYMENT;
            txn->txn_input_size = sizeof(PaymentInput);
            auto &input = txn->get_input_as_ref<PaymentInput>();
            
            // auto tbl = tbls[wid - 1];
            Tables& tabs = tables[input.w_id - 1];

            // 1st retrieval
            Key_t k_w = tpcc_key(WAREHOUSE, 1);
            W_val v_w = txn->get(tabs.w_tab, k_w, true, true);
            v_w.w_ytd += input.h_amount;
            txn->put(tabs.w_tab, k_w, v_w);

            // 2nd retrieval
            uint32_t d_tab_i = district_table_i(input.d_id);
            Key_t k_d = tpcc_key(d_tab_i, 0);
            D_val v_d = txn->get(tabs.d_tab[d_tab_i-1], k_d, true, true);
            // D_val v_d = txn->get_print(tabs.d_tab[d_tab_i-1], k_d, true, true);
            v_d.d_ytd += input.h_amount;
            txn->put(tabs.d_tab[d_tab_i-1], k_d, v_d);

            uint32_t c_id;
            if (input.by_last_name) {
                std::string name = input.last_name.to_string();
                Key_t low = GetCustomerNameIndex(input.c_d_id, name);
                name.back()++;
                Key_t high = GetCustomerNameIndex(input.c_d_id, name);
                auto c_ids = txn->scan(tabs.cn_tab, low, high);
                c_id = c_ids[(c_ids.size() + 1) / 2 - 1];
                input.scan_count = c_ids.size();
                input.c_id = c_id;
            } else {
                c_id = input.c_id;
            }

            // 3rd retrieval
            auto& c_tabs = tables[input.c_w_id - 1];
            Key_t k_c = tpcc_cust_key(input.c_d_id, c_id);
            C_val v_c = txn->get(c_tabs.c_tab, k_c, true, true); // TODO: remove lock?
            v_c.c_balance -= input.h_amount;
            v_c.c_ytd_payment += input.h_amount;
            v_c.c_payment_cnt++;
            if (memcmp(v_c.c_credit.buf, "BC", 2) == 0) {
                char buf[501];
                snprintf(buf, sizeof(buf), "%d %d %d %d %d %f | %s", 
                        input.c_id, input.c_d_id, input.c_w_id, 
                        input.d_id, input.w_id, input.h_amount,
                        v_c.c_data.buf);
                memcpy(v_c.c_data.buf, buf, 500);
            }
            txn->put(c_tabs.c_tab, k_c, v_c);

            // 1st insert
            input.history_key = tpcc_history_key(worker_id);
            // Value_t history;
            H_val v_h;
            v_h.h_amount = input.h_amount;
            memcpy(v_h.h_data.buf, v_w.w_name.buf, sizeof(v_w.w_name.buf));
            memset(v_h.h_data.buf + sizeof(v_w.w_name.buf), ' ', 4);
            memcpy(v_h.h_data.buf + sizeof(v_w.w_name.buf) + 4, v_d.d_name.buf,
                    sizeof(v_d.d_name.buf));
            txn->put(tabs.h_tab, input.history_key, v_h);

            return true;
        };
#else
        auto execution_logic = [this](DbTxn *txn, thread_id_t worker_id) {
            auto &input = txn->get_input_as_ref<PaymentInput>();
            Tables& tabs = tables[input.w_id - 1];

            if (txn->phase == 0) {
                txn->fastCommitEnabled = WORKLOAD_FAST_PATH;
                txn->txn_type = TXN_PAYMENT;
                txn->txn_input_size = sizeof(PaymentInput);

                // 1st retrieval
                Key_t k_w = tpcc_key(WAREHOUSE, 1);
                W_val v_w = txn->remote_get(tabs.w_tab, k_w, true, true);
                v_w.w_ytd += input.h_amount;
                txn->put(tabs.w_tab, k_w, v_w);

                // 2nd retrieval
                uint32_t d_tab_i = district_table_i(input.d_id);
                Key_t k_d = tpcc_key(d_tab_i, 0);
                D_val v_d = txn->remote_get(tabs.d_tab[d_tab_i-1], k_d, true, true);
                v_d.d_ytd += input.h_amount;
                txn->put(tabs.d_tab[d_tab_i-1], k_d, v_d);

                // 1st insert
                input.history_key = tpcc_history_key(worker_id);
                // Value_t history;
                H_val v_h;
                v_h.h_amount = input.h_amount;
                memcpy(v_h.h_data.buf, v_w.w_name.buf, sizeof(v_w.w_name.buf));
                memset(v_h.h_data.buf + sizeof(v_w.w_name.buf), ' ', 4);
                memcpy(v_h.h_data.buf + sizeof(v_w.w_name.buf) + 4, v_d.d_name.buf,
                        sizeof(v_d.d_name.buf));
                txn->put(tabs.h_tab, input.history_key, v_h);

                uint32_t c_id;
                if (input.by_last_name) {
                    std::string name = input.last_name.to_string();
                    Key_t low = GetCustomerNameIndex(input.c_d_id, name);
                    name.back()++;
                    Key_t high = GetCustomerNameIndex(input.c_d_id, name);
                    auto c_ids = txn->remote_scan(tabs.cn_tab, low, high);
                    c_id = c_ids[(c_ids.size() + 1) / 2 - 1];
                    input.scan_count = c_ids.size();
                    input.c_id = c_id;
                } else {
                    c_id = input.c_id;
                }

                txn->read_remote();
                txn->phase += 1;
                return false;
            }

            if (txn->phase == 1) {
                // 3rd retrieval
                auto& c_tabs = tables[input.c_w_id - 1];
                Key_t k_c = tpcc_cust_key(input.c_d_id, input.c_id);
                C_val v_c = txn->remote_get(c_tabs.c_tab, k_c, true, true); // TODO: remove lock?
                v_c.c_balance -= input.h_amount;
                v_c.c_ytd_payment += input.h_amount;
                v_c.c_payment_cnt++;
                if (memcmp(v_c.c_credit.buf, "BC", 2) == 0) {
                    char buf[501];
                    snprintf(buf, sizeof(buf), "%d %d %d %d %d %f | %s", 
                            input.c_id, input.c_d_id, input.c_w_id, 
                            input.d_id, input.w_id, input.h_amount,
                            v_c.c_data.buf);
                    memcpy(v_c.c_data.buf, buf, 500);
                }
                txn->put(c_tabs.c_tab, k_c, v_c);

                txn->read_remote();
                txn->phase += 1;
                return false;
            }

            return true;
        };
#endif
        auto repair_logic = [this](DbTxn *txn, thread_id_t worker_id) {
            bool repaired = false;
            auto &input = txn->get_input_as_ref<PaymentInput>();
            // Tables& tabs = tables[input.w_id - 1];
            uint32_t r_code_loc = 0;

            // 1st retrieval
            // Key_t k_w = tpcc_key(WAREHOUSE, 1);
            // RepairStatus w_r_status{};
            // txn->repair_get(tabs.w_tab, k_w, r_code_loc++, w_r_status);
            // ASSERT (w_r_status != REPAIRED);
            ++r_code_loc;

            // if (w_r_status == VALID) {
            //     // 2nd retrieval
            //     RepairStatus d_r_status{};
            //     uint32_t d_tab_i = district_table_i(input.d_id);
            //     Key_t k_d = tpcc_key(d_tab_i, 0);
            //     txn->repair_get(tabs.d_tab[d_tab_i-1], k_d, r_code_loc, d_r_status);
            //     ASSERT (d_r_status == VALID);
            // }
            ++r_code_loc;

            if (input.by_last_name) {
                // skip repair for read-only table
                r_code_loc += input.scan_count;
            }

            // 3rd retrieval
            Key_t customer_key = tpcc_cust_key(input.c_d_id, input.c_id);
            RepairStatus c_r_status{};
            auto& c_tabs = tables[input.c_w_id - 1];
            C_val v_c = txn->repair_get(c_tabs.c_tab, customer_key, r_code_loc++, c_r_status);
            if (c_r_status == REPAIRED) {
                repaired = true;
                v_c.c_balance -= input.h_amount;
                v_c.c_ytd_payment += input.h_amount;
                v_c.c_payment_cnt++;
                if (memcmp(v_c.c_credit.buf, "BC", 2) == 0) {
                    char buf[501];
                    snprintf(buf, sizeof(buf), "%d %d %d %d %d %f | %s", 
                            input.c_id, input.c_d_id, input.c_w_id, 
                            input.d_id, input.w_id, input.h_amount,
                            v_c.c_data.buf);
                    memcpy(v_c.c_data.buf, buf, 500);
                }
                // 3rd update
                txn->repair_put(c_tabs.c_tab, customer_key, v_c, true);
            }

            // the last blind write never fails.
            return repaired;
        };
        auto gc_logic = [this](DbTxn *txn, thread_id_t worker_id) {
            auto* input = txn->get_input<PaymentInput>();
#ifndef RPC_CLIENTS
            this->free_clients.enqueue(input->client, socket_t_id);
#endif
            delete input;
            return true;
        };
        return {execution_logic, repair_logic, gc_logic};
    }

    DbTxn::txn_logic_t get_txn_logic_delivery() {
#ifndef NON_CACHING
        auto execution_logic = [this](DbTxn *txn, thread_id_t worker_id) {
            txn->fastCommitEnabled = WORKLOAD_FAST_PATH;
            txn->txn_type = TXN_DELIVERY;
            txn->txn_input_size = sizeof(DeliveryInput);
            auto &input = txn->get_input_as_ref<DeliveryInput>();
            Tables& tabs = tables[input.w_id - 1];

            for (uint32_t did = 1; did <= DIST_PER_WARE; did++) {
                int total_amount = 0;
                // 1st data retrieval: neworder
                uint32_t d_tab_i = district_table_i(did);
                Key_t k_do = tpcc_key(d_tab_i, 1);
                D_val v_do = txn->get(tabs.d_tab[d_tab_i-1], k_do, true, true);
                tpcc_id_t oid = v_do.d_next_o_id;
                input.original_oid[did] = oid;

                // get and remove new order
                Key_t k_no = tpcc_order_key(NEWORDER, did, oid);
                NO_val v_no = txn->get(tabs.no_tab, k_no, true, true);
                // num_delivery++;
                if(!v_no.no_exist) {
                    continue;
                }
                // success_delivery++;

                // 1st update: neworder
                // d_first_oid.s[0]++;
                v_do.d_next_o_id++;
                txn->put(tabs.d_tab[d_tab_i-1], k_do, v_do);

                v_no.no_exist = false; // logical delete...
                txn->put(tabs.no_tab, k_no, v_no);

                // 2nd data retrieval: order
                Key_t k_o = tpcc_order_key(ORDER, did, oid);
                O_val v_o = txn->get(tabs.o_tab, k_o, true, true);
                tpcc_id_t o_c_id = v_o.o_c_id;
                v_o.o_carrier_id = input.o_carrier_id;
                uint ol_count = v_o.o_ol_cnt;

                // 2nd update: order
                txn->put(tabs.o_tab, k_o, v_o);

                for (uint ol = 0; ol < ol_count; ol++) {
                    Key_t k_ol = tpcc_orderline_key(did, oid, ol);
                    // Value_t ol_data = txn->get(*tbl, k_ol);
                    // total_amount += ol_data.s[3];
                    // txn->put(*tbl, k_ol, ol_data);

                    OL_val v_ol = txn->get(tabs.ol_tab, k_ol, true, true);
                    total_amount += v_ol.ol_amount;
                    v_ol.ol_delivery_d = input.ol_delivery_d;
                    txn->put(tabs.ol_tab, k_ol, v_ol);
                }

#ifdef ABORT_ON_FAIL
                if (o_c_id == 0) {
                    continue;
                }
#endif
                Key_t k_c = tpcc_cust_key(did, o_c_id);
                C_val v_c = txn->get(tabs.c_tab, k_c, true, true);
                v_c.c_balance += total_amount;
                v_c.c_delivery_cnt++;
                txn->put(tabs.c_tab, k_c, v_c);
            }

            return true;
        };
#else
        auto execution_logic = [this](DbTxn *txn, thread_id_t worker_id) {
            auto &input = txn->get_input_as_ref<DeliveryInput>();
            Tables& tabs = tables[input.w_id - 1];
            auto* temp = txn->get_or_new_temp<DeliveryTemp>();

            if (txn->phase == 0) {
                txn->fastCommitEnabled = WORKLOAD_FAST_PATH;
                txn->txn_type = TXN_DELIVERY;
                txn->txn_input_size = sizeof(DeliveryInput);

                for (uint32_t did = 1; did <= DIST_PER_WARE; did++) {
                    // 1st data retrieval: neworder
                    uint32_t d_tab_i = district_table_i(did);
                    Key_t k_do = tpcc_key(d_tab_i, 1);
                    D_val v_do = txn->remote_get(tabs.d_tab[d_tab_i-1], k_do, true, true);
                    tpcc_id_t oid = v_do.d_next_o_id;
                    input.original_oid[did] = oid;

                    // get and remove new order
                    Key_t k_no = tpcc_order_key(NEWORDER, did, oid);
                    NO_val v_no = txn->remote_get(tabs.no_tab, k_no, true, true);

                    temp->o_ids[did-1] = oid;
                    temp->not_oks[did-1] = !v_no.no_exist;
                    if(!v_no.no_exist) {
                        continue;
                    }

                    // 1st update: neworder
                    v_do.d_next_o_id++;
                    txn->put(tabs.d_tab[d_tab_i-1], k_do, v_do);

                    v_no.no_exist = false; // logical delete...
                    txn->put(tabs.no_tab, k_no, v_no);
                }
                txn->read_remote();
                txn->phase += 1;
                return false;
            }

            if (txn->phase == 1) {
                bool remote = false;
                for (uint32_t did = 1; did <= DIST_PER_WARE; did++) {
                    if (temp->not_oks[did-1]) {
                        continue;
                    }
                    remote = true;

                    // 2nd data retrieval: order
                    Key_t k_o = tpcc_order_key(ORDER, did, temp->o_ids[did-1]);
                    O_val v_o = txn->remote_get(tabs.o_tab, k_o, true, true);
                    temp->o_c_ids[did-1] = v_o.o_c_id;
                    v_o.o_carrier_id = input.o_carrier_id;
                    temp->ol_counts[did-1] = v_o.o_ol_cnt;

                    // 2nd update: order
                    txn->put(tabs.o_tab, k_o, v_o);
                }
                txn->phase += 1;
                if (remote) {
                    txn->read_remote();
                    return false;
                }
            }

            if (txn->phase == 2) {
                bool remote = false;
                for (uint32_t did = 1; did <= DIST_PER_WARE; did++) {
                    if (temp->not_oks[did-1]) {
                        continue;
                    }
                    remote = true;

                    int total_amount = 0;
                    for (uint ol = 0; ol < temp->ol_counts[did-1]; ol++) {
                        Key_t k_ol = tpcc_orderline_key(did, temp->o_ids[did-1], ol);

                        OL_val v_ol = txn->remote_get(tabs.ol_tab, k_ol, true, true);
                        total_amount += v_ol.ol_amount;
                        v_ol.ol_delivery_d = input.ol_delivery_d;
                        txn->put(tabs.ol_tab, k_ol, v_ol);
                    }

#ifdef ABORT_ON_FAIL
                    if (temp->o_c_ids[did-1] == 0) {
                        continue;
                    }
#endif
                    Key_t k_c = tpcc_cust_key(did, temp->o_c_ids[did-1]);
                    C_val v_c = txn->remote_get(tabs.c_tab, k_c, true, true);
                    v_c.c_balance += total_amount;
                    v_c.c_delivery_cnt++;
                    txn->put(tabs.c_tab, k_c, v_c);
                }

                txn->phase += 1;
                if (remote) {
                    txn->read_remote();
                    return false;
                }
            }

            return true;
        };
#endif
        auto repair_logic = [this](DbTxn *txn, thread_id_t worker_id) {
            bool repaired = false;
            auto& input = txn->get_input_as_ref<DeliveryInput>();
            Tables& tabs = tables[input.w_id - 1];
            uint32_t r_code_loc = 0;

            for (uint32_t did = 1; did <= DIST_PER_WARE; did++) {
                int total_amount = 0;

                RepairStatus d_r_status{};
                uint32_t d_tab_i = district_table_i(did);
                Key_t k_do = tpcc_key(d_tab_i, 1);
                D_val v_do = txn->repair_get(tabs.d_tab[d_tab_i-1], k_do, r_code_loc++, d_r_status);
                ASSERT(d_r_status == VALID);
                tpcc_id_t oid = v_do.d_next_o_id;

                // 1st data retrieval: neworder
                RepairStatus no_r_status{};
                Key_t k_no = tpcc_order_key(NEWORDER, did, oid);
                NO_val v_no = txn->repair_get(tabs.no_tab, k_no, r_code_loc++, no_r_status);
                ASSERT(no_r_status == VALID);
                if(!v_no.no_exist) {
                    continue;
                }

                // 2nd data retrieval: order
                Key_t k_o = tpcc_order_key(ORDER, did, oid);
                RepairStatus o_r_status{};
                O_val v_o = txn->repair_get(tabs.o_tab, k_o, r_code_loc++, o_r_status);
                ASSERT(o_r_status == VALID);
                tpcc_id_t o_c_id = v_o.o_c_id;

                uint ol_count = v_o.o_ol_cnt;
                for (uint ol = 0; ol < ol_count; ol++) {
                    Key_t k_ol = tpcc_orderline_key(did, oid, ol);
                    RepairStatus ol_r_status{};
                    OL_val v_ol = txn->repair_get(tabs.ol_tab, k_ol, r_code_loc++, ol_r_status);
                    total_amount += v_ol.ol_amount;
                    ASSERT(ol_r_status == VALID);
                }
                    
                Key_t k_c = tpcc_cust_key(did, o_c_id);
                RepairStatus cust_r_status{};
                C_val v_c = txn->repair_get(tabs.c_tab, k_c, r_code_loc++, cust_r_status);
                if (cust_r_status == REPAIRED) {
                    repaired = true;
                    v_c.c_balance += total_amount;
                    v_c.c_delivery_cnt++;
                    txn->repair_put(tabs.c_tab, k_c, v_c, cust_r_status == REPAIRED);
                }
            }
            return repaired;
        };
        auto gc_logic = [this](DbTxn *txn, thread_id_t worker_id) {
            auto* input = txn->get_input<DeliveryInput>();
#ifndef RPC_CLIENTS
            this->free_clients.enqueue(input->client, socket_t_id);
#endif
            delete input;
#ifdef NON_CACHING
                txn->delete_temp<DeliveryTemp>();
#endif
            return true;
        };
        return {execution_logic, repair_logic, gc_logic};
    }

    DbTxn::txn_logic_t get_txn_logic_orderstatus() {
#ifndef NON_CACHING
        auto execution_logic = [this](DbTxn *txn, thread_id_t worker_id) {
            txn->fastCommitEnabled = WORKLOAD_FAST_PATH;
            txn->txn_type = TXN_ORDERSTATUS;
            txn->txn_input_size = sizeof(OrderStatusInput);
            auto &input = txn->get_input_as_ref<OrderStatusInput>();
            Tables& tabs = tables[input.w_id - 1];

            uint32_t c_id;
            if (input.by_last_name) {
                std::string name = input.last_name.to_string();
                Key_t low = GetCustomerNameIndex(input.d_id, name);
                name.back()++;
                Key_t high = GetCustomerNameIndex(input.d_id, name);
                auto c_ids = txn->scan(tabs.cn_tab, low, high);
                c_id = c_ids[(c_ids.size() + 1) / 2 - 1];
                input.scan_count = c_ids.size();
                input.c_id = c_id;
            } else {
                c_id = input.c_id;
            }

            Key_t customer_key = tpcc_cust_key(input.d_id, c_id);
            // txn->get(*tbl, customer_key);
            txn->get(tabs.c_tab, customer_key, true, false);

            Key_t clo_key = tpcc_cust_last_order_key(input.d_id, c_id);
            Value_t v_cl = txn->get(tabs.cl_tab, clo_key, true, false);
            tpcc_id_t oid = v_cl.s[0];

            if (oid == (uint)-1) {
                return true;
            }
            Key_t order_key = tpcc_order_key(ORDER, input.d_id, oid);
            O_val v_o = txn->get(tabs.o_tab, order_key, true, false);
            uint num = v_o.o_ol_cnt;

#ifdef ABORT_ON_FAIL
            if (num == 0) {
                return true;
            }
#endif

            ASSERT(num >= 5 && num <= 15) << num << " " << oid 
                << " "<< input.w_id << " "<< input.d_id << " "<< c_id;
            for (uint i = 0; i < num; i++) {
                Key_t ol_key = tpcc_orderline_key(input.d_id, oid, i);
                // txn->get(*tbl, ol_key);
                txn->get(tabs.ol_tab, ol_key, true, false);
            }

            return true;
        };
#else
        auto execution_logic = [this](DbTxn *txn, thread_id_t worker_id) {
            auto &input = txn->get_input_as_ref<OrderStatusInput>();
            Tables& tabs = tables[input.w_id - 1];
            auto* temp = txn->get_or_new_temp<OrderStatusTemp>();

            if (txn->phase == 0) {
                txn->fastCommitEnabled = WORKLOAD_FAST_PATH;
                txn->txn_type = TXN_ORDERSTATUS;
                txn->txn_input_size = sizeof(OrderStatusInput);

                uint32_t c_id;
                bool remote = false;
                if (input.by_last_name) {
                    std::string name = input.last_name.to_string();
                    Key_t low = GetCustomerNameIndex(input.d_id, name);
                    name.back()++;
                    Key_t high = GetCustomerNameIndex(input.d_id, name);
                    auto c_ids = txn->remote_scan(tabs.cn_tab, low, high);
                    c_id = c_ids[(c_ids.size() + 1) / 2 - 1];
                    input.scan_count = c_ids.size();
                    input.c_id = c_id;
                    remote = true;
                } else {
                    c_id = input.c_id;
                }
                txn->phase += 1;
                if (remote) {
                    txn->read_remote();
                    return false;
                }
            }
            if (txn->phase == 1) {
                Key_t customer_key = tpcc_cust_key(input.d_id, input.c_id);
                txn->remote_get(tabs.c_tab, customer_key, true, false);

                Key_t clo_key = tpcc_cust_last_order_key(input.d_id, input.c_id);
                Value_t v_cl = txn->remote_get(tabs.cl_tab, clo_key, true, false);
                temp->o_id = v_cl.s[0];

                txn->phase += 1;
                txn->read_remote();
                return false;
            }
            if (txn->phase == 2) {
                if (temp->o_id == (uint)-1) {
                    return true;
                }
                Key_t order_key = tpcc_order_key(ORDER, input.d_id, temp->o_id);
                O_val v_o = txn->remote_get(tabs.o_tab, order_key, true, false);
                temp->num = v_o.o_ol_cnt;
                txn->phase += 1;
                txn->read_remote();
                return false;
            }
            if (txn->phase == 3) {
#ifdef ABORT_ON_FAIL
                if (temp->num == 0) {
                    return true;
                }
#endif
                ASSERT(temp->num >= 5 && temp->num <= 15) << temp->num << " " << temp->o_id 
                    << " "<< input.w_id << " "<< input.d_id << " "<< input.c_id;
                for (uint i = 0; i < temp->num; i++) {
                    Key_t ol_key = tpcc_orderline_key(input.d_id, temp->o_id, i);
                    // txn->get(*tbl, ol_key);
                    txn->remote_get(tabs.ol_tab, ol_key, true, false);
                }
                txn->phase += 1;
                txn->read_remote();
                return false;
            }

            return true;
        };
#endif
        auto repair_logic = [this](DbTxn *txn, thread_id_t worker_id) {
#ifdef TPCC_STATIC_ANALYSIS
            return false;
#else
            bool repaired = false;
            auto &input = txn->get_input_as_ref<OrderStatusInput>();
            Tables& tabs = tables[input.w_id - 1];
            uint32_t r_code_loc = 0;

            if (input.by_last_name) {
                // skip repair for read-only table
                r_code_loc += input.scan_count;
            }

            RepairStatus c_r_status{};
            Key_t customer_key = tpcc_cust_key(input.d_id, input.c_id);
            txn->repair_get(tabs.c_tab, customer_key, r_code_loc++, c_r_status);
            if (c_r_status == REPAIRED) {
                repaired = true;
            }

            RepairStatus cl_r_status{};
            Key_t clo_key = tpcc_cust_last_order_key(input.d_id, input.c_id);
            Value_t v_cl = txn->repair_get(tabs.cl_tab, clo_key, r_code_loc++, cl_r_status);
            ASSERT(cl_r_status == VALID);
            tpcc_id_t oid = v_cl.s[0];

            if (oid == (uint)-1) {
                return repaired;
            }
            RepairStatus o_r_status{};
            Key_t order_key = tpcc_order_key(ORDER, input.d_id, oid);
            O_val v_o = txn->repair_get(tabs.o_tab, order_key, r_code_loc++, o_r_status);
            ASSERT(o_r_status == VALID);
            uint num = v_o.o_ol_cnt;

            RepairStatus ol_r_status{};
            ASSERT(num >= 5 && num <= 15) << num << " " << oid 
                << " "<< input.w_id << " "<< input.d_id << " "<< input.c_id;
            for (uint i = 0; i < num; i++) {
                Key_t ol_key = tpcc_orderline_key(input.d_id, oid, i);
                // txn->get(*tbl, ol_key);
                txn->repair_get(tabs.ol_tab, ol_key, r_code_loc++, ol_r_status);
                ASSERT(ol_r_status == VALID);
            }

            return repaired;
#endif
        };
        auto gc_logic = [this](DbTxn *txn, thread_id_t worker_id) {
            auto* input = txn->get_input<OrderStatusInput>();
#ifndef RPC_CLIENTS
            this->free_clients.enqueue(input->client, socket_t_id);
#endif
            delete input;
#ifdef NON_CACHING
            txn->delete_temp<OrderStatusTemp>();
#endif
            return true;
        };
        return {execution_logic, repair_logic, gc_logic};
    }

    DbTxn::txn_logic_t get_txn_logic_stocklevel() {
#ifndef NON_CACHING
        auto execution_logic = [this](DbTxn *txn, thread_id_t worker_id) {
            txn->fastCommitEnabled = WORKLOAD_FAST_PATH;
            txn->txn_type = TXN_STOCKLEVEL;
            txn->txn_input_size = sizeof(StockLevelInput);
            auto &input = txn->get_input_as_ref<StockLevelInput>();
            Tables& tabs = tables[input.w_id - 1];

            uint32_t d_tab_i = district_table_i(input.d_id);
            Key_t k_d = tpcc_key(d_tab_i, 0);
            D_val v_d = txn->get(tabs.d_tab[d_tab_i-1], k_d, true, false);
            tpcc_id_t next_oid = v_d.d_next_o_id;

            std::set<Key_t> k_s_set;

            uint32_t count = 0;
            for (uint32_t i = 0; i < 20; i++) {
                tpcc_id_t oid = next_oid - i - 1;
                for (uint32_t j = 0; j < 15; j++) {
                    Key_t k_ol = tpcc_orderline_key(input.d_id, oid, j);
                    OL_val v_ol = txn->get(tabs.ol_tab, k_ol, true, false);
                    if(v_ol.ol_quantity == 0) {
                        // null order line
                        break;
                    }
                    
                    Key_t k_s = tpcc_key(STOCK, v_ol.ol_i_id - 1);
                    k_s_set.insert(k_s);
                }
            }

            for (Key_t k_s : k_s_set) {
                S_val v_s = txn->get(tabs.s_tab, k_s, true, false);
                if(v_s.s_quantity < input.threshold) { // threshold
                    count++;
                }
            }

            return true;
        };
#else
        auto execution_logic = [this](DbTxn *txn, thread_id_t worker_id) {
            auto &input = txn->get_input_as_ref<StockLevelInput>();
            Tables& tabs = tables[input.w_id - 1];
            auto* temp = txn->get_or_new_temp<StockLevelTemp>();

            if (txn->phase == 0) {
                txn->fastCommitEnabled = WORKLOAD_FAST_PATH;
                txn->txn_type = TXN_STOCKLEVEL;
                txn->txn_input_size = sizeof(StockLevelInput);

                uint32_t d_tab_i = district_table_i(input.d_id);
                Key_t k_d = tpcc_key(d_tab_i, 0);
                D_val v_d = txn->remote_get(tabs.d_tab[d_tab_i-1], k_d, true, false);
                temp->o_id = v_d.d_next_o_id;

                txn->phase += 1;
                txn->read_remote();
                return false;
            }
            if (txn->phase == 1) {
                for (uint32_t i = 0; i < 20; i++) {
                    tpcc_id_t oid = temp->o_id - i - 1;
                    for (uint32_t j = 0; j < 15; j++) {
                        Key_t k_ol = tpcc_orderline_key(input.d_id, oid, j);
                        OL_val v_ol = txn->remote_get(tabs.ol_tab, k_ol, true, false);
                        if(v_ol.ol_quantity == 0) {
                            // null order line
                            break;
                        }
                        
                        Key_t k_s = tpcc_key(STOCK, v_ol.ol_i_id - 1);
                        temp->k_s_set.insert(k_s);
                    }
                }
                txn->phase += 1;
                txn->read_remote();
                return false;
            }
            if (txn->phase == 2) {
                uint32_t count = 0;
                for (Key_t k_s : temp->k_s_set) {
                    S_val v_s = txn->remote_get(tabs.s_tab, k_s, true, false);
                    if(v_s.s_quantity < input.threshold) { // threshold
                        count++;
                    }
                }
                txn->phase += 1;
                txn->read_remote();
                return false;
            }

            return true;
        };
#endif
        auto repair_logic = [this](DbTxn *txn, thread_id_t worker_id) {
#ifdef TPCC_STATIC_ANALYSIS
            return false;
#else
            bool repaired = false;
            auto &input = txn->get_input_as_ref<StockLevelInput>();
            Tables& tabs = tables[input.w_id - 1];
            uint32_t r_code_loc = 0;

            RepairStatus d_r_status{};
            uint32_t d_tab_i = district_table_i(input.d_id);
            Key_t k_d = tpcc_key(d_tab_i, 0);
            D_val v_d = txn->repair_get(tabs.d_tab[d_tab_i-1], k_d, r_code_loc++, d_r_status);
            ASSERT(d_r_status == VALID);
            tpcc_id_t next_oid = v_d.d_next_o_id;

            std::set<Key_t> k_s_set;

            uint32_t count = 0;
            RepairStatus ol_r_status{};
            for (uint32_t i = 0; i < 20; i++) {
                tpcc_id_t oid = next_oid - i - 1;
                for (uint32_t j = 0; j < 15; j++) {
                    Key_t k_ol = tpcc_orderline_key(input.d_id, oid, j);
                    OL_val v_ol = txn->repair_get(tabs.ol_tab, k_ol, r_code_loc++, ol_r_status);
                    ASSERT(ol_r_status == VALID);
                    if(v_ol.ol_quantity == 0) {
                        // null order line
                        break;
                    }
                    
                    Key_t k_s = tpcc_key(STOCK, v_ol.ol_i_id - 1);
                    k_s_set.insert(k_s);
                }
            }

            RepairStatus s_r_status{};
            for (Key_t k_s : k_s_set) {
                S_val v_s = txn->repair_get(tabs.s_tab, k_s, r_code_loc++, s_r_status);
                if (s_r_status == REPAIRED) {
                    repaired = true;
                }
                if(v_s.s_quantity < input.threshold) { // threshold
                    count++;
                }
            }

            return repaired;
#endif
        };
        auto gc_logic = [this](DbTxn *txn, thread_id_t worker_id) {
            auto* input = txn->get_input<StockLevelInput>();
#ifndef RPC_CLIENTS
            this->free_clients.enqueue(input->client, socket_t_id);
#endif
            delete input;
#ifdef NON_CACHING
            txn->delete_temp<StockLevelTemp>();
#endif
            return true;
        };
        return {execution_logic, repair_logic, gc_logic};
    }

    inline tpcc_id_t get_rand_wid_of_db(node_id_t db_id) {
        return num_warehouse_per_db * db_id + 
            thread_rand.randint(1, num_warehouse_per_db);
    }

    inline tpcc_id_t get_rand_wid_of_sn(node_id_t sn_id) {
        return num_warehouse_per_sn * sn_id + 
            thread_rand.randint(1, num_warehouse_per_sn);
    }

    inline tpcc_id_t get_first_wid_of(node_id_t db_id) {
        return num_warehouse_per_db * db_id + 1;
    }

    tpcc_id_t get_my_wid() {
        tpcc_id_t my_w_id;
        if (num_warehouse_per_worker == 0) {
            my_w_id = num_warehouse_per_db * conf.get_my_db_id() +
                     thread_rand.randint(0, num_warehouse_per_db - 1) + 1;
        } else {
            my_w_id = num_warehouse_per_db * conf.get_my_db_id() +
                     socket_t_id * num_warehouse_per_worker +
                     thread_rand.randint(0, num_warehouse_per_worker - 1) + 1;
        }
        // return my_w_id + 1;
        return my_w_id;
    }

    tpcc_id_t get_remote_wid(tpcc_id_t my_w_id) {
        tpcc_id_t r_w_id = thread_rand.randint(1, num_warehouse - 1);
        if (r_w_id >= my_w_id) {
            r_w_id++;
        }
        return r_w_id;
    }

    void *get_input_neworder() {
        auto *input = new NewOrderInput();
#ifndef RPC_CLIENTS
        try {
            input->client = free_clients.dequeue(socket_t_id);
        } catch (const WorkloadNoClientException &e) {
            delete input;
            return nullptr;
        }
#endif
        input->w_id = get_my_wid();
        input->d_id = thread_rand.randint(1, DIST_PER_WARE);
        // input->d_id = 1;
        input->c_id = thread_rand.NURand(1023, random_C_C_ID, 1, CUST_PER_DIST);
        // input->c_id = 1023;
        input->ol_cnt = thread_rand.randint(5, 15);
        // input->ol_cnt = 10;
        input->rbk = thread_rand.randint(1, 100) <= 1;
        input->ol_all_local = true;
        input->o_entry_d = GetCurrentTimeMillis();
        for (uint i = 0; i < input->ol_cnt; i++) {
            tpcc_id_t i_id = 0;
            // make sure all items are different
            bool need_rerand = true;
            while (need_rerand) {
                i_id = thread_rand.NURand(8191, random_C_OL_I_ID, 1, MAXITEMS);
                // i_id = thread_rand.randint(1, MAXITEMS);
                // i_id = i + 1;
                need_rerand = false;
                for (uint j = 0; j < i; j++) {
                    if (input->items[j].i_id == i_id) {
                        need_rerand = true;
                        break;
                    }
                }
            }
            ASSERT(i_id != 0);
            tpcc_id_t ol_supply_wid = input->w_id;
            if (thread_rand.randint(1, 1000) <= remote_neworder_item_pro && num_warehouse > 1) {
                input->ol_all_local = false;
                // make sure ol_supply_wid is another warehouse.
                ol_supply_wid = get_remote_wid(input->w_id);
            }
            int ol_quantity = thread_rand.randint(1, 10);

            // input->items.emplace_back(i_id, ol_supply_wid, ol_quantity);
            input->items[i] = Item(i_id, ol_supply_wid, ol_quantity);
        }
        std::sort(&input->items[0], &input->items[input->ol_cnt]);
        return input;
    }

    void *get_input_payment() {
        auto *input = new PaymentInput();
#ifndef RPC_CLIENTS
        try {
            input->client = free_clients.dequeue(socket_t_id);
        } catch (const WorkloadNoClientException &e) {
            delete input;
            return nullptr;
        }
#endif
        input->w_id = get_my_wid();
        input->d_id = thread_rand.randint(1, DIST_PER_WARE);
        if (thread_rand.randint(0, 99) < 100 - remote_payment_pro || num_warehouse == 1) {
            // home warehouse
            input->c_w_id = input->w_id;
            input->c_d_id = input->d_id;
        } else {
            // remote warehouse
            input->c_w_id = get_remote_wid(input->w_id);
            input->c_d_id = thread_rand.randint(1, DIST_PER_WARE);
        }

        if (thread_rand.randint(0, 99) < 60) {
            input->by_last_name = true;
            input->last_name.assign(GetNonUniformCustomerLastName(thread_rand, random_C_C_LAST_RUN));
        } else {
            input->by_last_name = false;
            input->c_id = thread_rand.NURand(1023, random_C_C_ID, 1, CUST_PER_DIST);
        }
        
        input->h_amount = (float)(RandomNumber(thread_rand, 100, 500000) / 100.0);
        return input;
    }

    void *get_input_delivery() {
        auto *input = new DeliveryInput();
#ifndef RPC_CLIENTS
        try {
            input->client = free_clients.dequeue(socket_t_id);
        } catch (const WorkloadNoClientException &e) {
            delete input;
            return nullptr;
        }
#endif
        input->w_id = get_my_wid();
        input->o_carrier_id = thread_rand.randint(1, 10);
        input->ol_delivery_d = GetCurrentTimeMillis();
        return input;
    }

    void *get_input_orderstatus() {
        auto *input = new OrderStatusInput();
#ifndef RPC_CLIENTS
        try {
            input->client = free_clients.dequeue(socket_t_id);
        } catch (const WorkloadNoClientException &e) {
            delete input;
            return nullptr;
        }
#endif
        input->w_id = get_my_wid();
        input->d_id = thread_rand.randint(1, DIST_PER_WARE);
        if (thread_rand.randint(0, 99) < 60) {
            input->by_last_name = true;
            input->last_name.assign(GetNonUniformCustomerLastName(thread_rand, random_C_C_LAST_RUN));
        } else {
            input->by_last_name = false;
            input->c_id = thread_rand.NURand(1023, random_C_C_ID, 1, CUST_PER_DIST);
        }
        return input;
    }

    void *get_input_stocklevel() {
        auto *input = new StockLevelInput();
#ifndef RPC_CLIENTS
        try {
            input->client = free_clients.dequeue(socket_t_id);
        } catch (const WorkloadNoClientException &e) {
            delete input;
            return nullptr;
        }
#endif
        input->w_id = get_my_wid();
        input->d_id = thread_rand.randint(1, DIST_PER_WARE);
        input->threshold = RandomNumber(thread_rand, 10, 20);
        return input;
    }
    void init_page_manager(PageManager &page_manager) override {}
};