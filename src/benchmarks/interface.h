#pragma once

#include "txn/db_txn.h"

class DatabaseNode;
class InMemSegments;
class BenchmarkInterface {
   public:
    BenchmarkInterface() {}

    virtual ~BenchmarkInterface(){};
    virtual uint32_t get_num_clients() = 0;

    virtual uint64_t get_time_duration() = 0;

    virtual void init_database(DatabaseNode &db_node) = 0;

    virtual void init_storage(InMemSegments &segments) = 0;

    virtual DbTxn::txn_logic_t get_txn_logic(uint32_t txn_seed = 0) = 0;
    virtual DbTxn::partition_logic_func get_partition_logic() = 0;

    virtual void *get_input(DbClient *c) = 0;
    virtual uint32_t get_input_size(uint32_t txn_seed) = 0;

    virtual uint64_t check_correctness_map(InMemSegments &) = 0;
    virtual void check_correctness_reduce(std::vector<uint64_t> &) = 0;

    virtual GlobalPageId get_g_page_id(void *input, uint i) = 0;

    // debug only
    virtual void init_page_manager(PageManager &page_manager) = 0;
    virtual void print_debug_msg(){};
};