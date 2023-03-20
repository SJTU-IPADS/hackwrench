#pragma once
#include <jemalloc/jemalloc.h>

#include <cassert>
#include <fstream>
#include <map>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "util/dbug_logging.h"
#include "util/types.h"

#ifndef WORKLOAD_FAST_PATH 
#define WORKLOAD_FAST_PATH false
#endif

#define NO_TIMER
#define TXN_LAT_STAT
// #define DETAILED_TXN_LAT

// disable assertions and timers in evaluation mode
// #define EVAL_MODE

#ifdef EVAL_MODE
#define NO_ASSERTION
#define NO_TIMER
#endif

// ==========================Microbenchmark=====================

#define ENABLE_DISTRIBUTED_RATIO

// ==========================Normal Path Opt=====================

// #define NORMAL_PATH_OPT

// ==========================Batching Design=====================

#define BATCH_GET_TS
#define BATCH_PREPARE

// ==========================Factor Analysis in contention workload=====================

// #define VALIDATION_ALWAYS_SUCCESS
// #define VALIDATION_NON_BLOCKING

// #define SPLIT_ONE_CONTENTION

// ==========================Reduce prepare message & redo log size=====================

#define UNIQUE_REDO_LOG

// ========================== TimeServer =====================

// #define TOTAL_ORDER_TS

// ==========================Factor analysis=====================

// #define NO_TS
// #define ABORT_ON_FAIL
// #define DETERMINISTIC_VALIDATION
// #define READ_COMMITTED
// #define LOCK_FAIL_BLOCK
// #define NO_CONTENTION 

// ==========================Fine-grained Validation=====================

// #define FINE_VALIDATION
// #define PAGE_LEVEL_LOCK

// ==========================Fine-grained Diff Pages=====================

#define FINE_DIFF

// ========================== TPCC =====================

#define TPCC_STATIC_ANALYSIS
// #define HALF_TPCC

// ==========================Profling=====================

// #define BREAKDOWN_MSG_SIZE
// #define BREAKDOWN_CASCADING
// #define BREAKDOWN_TRAVERSING

#ifdef BREAKDOWN_TRAVERSING
#undef TPCC_STATIC_ANALYSIS
#endif

// ===============================================================================

// #define TEST_TS
// #define TEST_TS_KEYGEN
// #define TEST_ECHO
// #define TEST_LOCAL_COMMIT
// #define TEST_SINGLE_LOCAL_COMMIT
#define INDEPENDENT_GC
// #define SN_ECHO
// #define SKIP_NO_REPAIR
// #define CHECK_CONTENTION
#define USE_STD
// #define USE_ROBIN
// #define USE_STATIC_HASHMAP
// #define LOCK_FREE_BATCH_QUEUE
// #define BIND_CORES
// #define RPC_CLIENTS
// #define PREPARE_LAST_LOG
// #define COUNT_BATCH_SIZE
#define SPLIT_ON_READ_WRITE
// #define SN_BROADCAST

// #define NEW_REPLICATION

// ========================== Hackwrench_occ =====================

// #define PURE_OCC

// #define EMULATE_AURORA
#ifdef EMULATE_AURORA
#define NO_TS
#define ABORT_ON_FAIL
#define SN_BROADCAST

#ifdef PURE_OCC
#define NO_LOCK
#else
#define READ_COMMITTED
#endif
#undef NEW_REPLICATION
#endif

// #define NON_CACHING


#ifdef COCO
#define HALF_TPCC
#define FINE_VALIDATION
#undef NEW_REPLICATION
#endif

static const page_size_t SEG_SIZE = (1 << 16) << 10;
static const page_size_t PAGE_SIZE = (1 << 10) << 10;
static const page_size_t NUM_PAGES_PER_SEGMENT = SEG_SIZE / PAGE_SIZE;

#ifdef TEST_ECHO
#define SN_ECHO
#endif

#ifdef TEST_SINGLE_LOCAL_COMMIT
#define TEST_LOCAL_COMMIT
#endif

#ifdef TEST_LOCAL_COMMIT
#undef RPC_CLIENTS
#endif

#define SN_Str "StorageNode"
#define DB_Str "DataBase"
#define TS_Str "TimeServer"
#define CL_Str "Client"

// each node has a unique integer ID.
// ID      NodeType        Host      Port Threads
// 0       StorageNode     localhost 9989 16
// 1       DataBase        localhost 9990 8
// 2       TimeServer      localhost 9991 1
// From Calvin
class Configuration {
   public:
    enum NodeType { SN = 0, DB, TS, CLIENT };
    bool sn_replication;
    uint32_t REPLICA_COUNT;

    struct Node {
        NodeType node_type;

        // Globally unique node identifier.
        node_id_t node_id;
        // int replica_id;

        // IP address of this node's machine.
        std::string host;
        // Port on which to listen for messages from other nodes.
        uint32_t port;

        // Total number of worker threads (same number for network threads)
        uint32_t num_threads;
    };

    Configuration(const Configuration &) = delete;
    Configuration(node_id_t this_id, uint32_t num_segments, bool sn_replication,
                  const std::string &filename)
        : sn_replication(sn_replication), num_segments(num_segments), 
          num_pages(num_segments * NUM_PAGES_PER_SEGMENT) {
        if (sn_replication) {
            REPLICA_COUNT = 3;
        } else {
            REPLICA_COUNT = 1;
        }
        ReadFromFile(filename);

        // Currently, to avoid sn_id->node_id mapping
        // we should assign storage node first
        for (node_id_t node_id = 0; node_id < num_sn; ++node_id) {
            ASSERT(all_nodes[node_id]->node_type == SN);
        }

        if (!empty()) {
            this_node_info = {all_nodes[this_id]->node_type, all_nodes[this_id]->node_id};
        }

        ASSERT(num_sn <= MAX_SN_NUM);
    }
    ~Configuration() {
        for (auto iter : all_nodes) {
            ASSERT(iter.second != nullptr);
            delete iter.second;
        }
    }

    uint64_t numSegments() const { return num_segments; }

    uint64_t numPages() const { return num_pages; }

    void setNumSegments(uint64_t num_segments) {
        this->num_segments = num_segments;
        this->num_pages = num_segments * NUM_PAGES_PER_SEGMENT;
    }

    uint64_t numSN() const { return num_sn; }

    uint64_t numNodes() const { return num_sn + ts_nodes.size() + db_nodes.size(); }

    uint64_t numLogicalSN() const {
#ifdef NEW_REPLICATION
        return num_sn;
#else
        if (sn_replication) {
            return num_sn / 3;
        } else {
            return num_sn;
        }
#endif
    }

    inline node_id_t get_physical_sn_id(node_id_t sn_id) const {
#ifdef NEW_REPLICATION
        return sn_id;
#else
        return sn_id * REPLICA_COUNT;
#endif
    }

    const Node *get_sn_node(uint32_t db_i = 0) const { return sn_nodes[db_i]; }

    uint64_t numDB() const { return db_nodes.size(); }

    const Node *get_db_node(uint32_t db_i = 0) const { return db_nodes[db_i]; }

    const Node *get_primary_db_node() const { return db_nodes[0]; }

    uint64_t numTS() const { return 1; }

    uint64_t numClient() const { return client_nodes.size(); }

    const Node *get_ts_node(uint32_t ts_i = 0) const {
        return ts_nodes[ts_i];
    }

    bool isLocalSnSegment(const seg_id_t &seg_id) const {
        return segToSnID(seg_id) == get_my_logical_sn_id();
    }

    inline bool isReplicated() const {
        return sn_replication;
    }

    bool isMySeg(seg_id_t seg_id) const {
#ifdef NEW_REPLICATION
        return isReplicaOf(segToSnID(seg_id), get_my_sn_id());
#else
        return isLocalSnSegment(seg_id);
#endif
    }

    bool isMyReplica(node_id_t primary_sn_id) const {
        return isReplicaOf(primary_sn_id, get_my_sn_id());
    }

    bool isReplicaOf(node_id_t primary_sn_id, node_id_t sn_id) const {
        node_id_t last_sn_id = (primary_sn_id + REPLICA_COUNT - 1) % num_sn;
        if (primary_sn_id < last_sn_id) {
            return primary_sn_id <= sn_id && sn_id <= last_sn_id;
        } else {
            return primary_sn_id <= sn_id || sn_id <= last_sn_id;
        }
    }

    uint32_t get_maximal_msg_count() const {
#ifdef NEW_REPLICATION
        return numSN() * REPLICA_COUNT;
#else
        return numSN();
#endif
    }

    node_id_t segToSnID(const seg_id_t &seg_id) const {
        return seg_id * numLogicalSN() / numSegments();
    }
    // node_id_t segToDbID(const seg_id_t &seg_id) const { return (seg_id - 1) / numDB() + 1; }

    // Dump the current config into the file in key=value format.
    // Returns true when success.
    bool WriteToFile(const std::string &filename) const {
        std::ofstream ofs(filename.c_str(), std::ofstream::out);
        if (!ofs.is_open()) {
            ASSERT(false);
            return false;
        }
        write(ofs);
        ofs.close();
        return true;
    }

    void print() const { write(std::cout); }

    bool empty() const { return all_nodes.size() == 0; }

    inline const NodeType &get_my_node_type() const { return this_node_info.first; }

    inline node_id_t get_my_id() const { return this_node_info.second; }

    inline node_id_t get_my_logical_sn_id() const {
#ifdef NEW_REPLICATION
        return this_node_info.second;
#else
        return this_node_info.second / REPLICA_COUNT;
#endif
    }

    inline node_id_t get_my_sn_id() const { return this_node_info.second; }

    inline node_id_t get_my_db_id() const { return this_node_info.second - numSN() - numTS(); }

    inline node_id_t get_db_id(node_id_t node_id) const { return node_id - numSN() - numTS(); }

    inline node_id_t get_client_node_id(node_id_t client_id) const {
        return client_id + numSN() + numTS() + numDB();
    }

    inline const Node &get_node(node_id_t id) const {
        return *all_nodes.at(id);
    }

    inline const Node &get_my_node() const { return get_node(get_my_id()); }

    inline const std::unordered_map<node_id_t, Node *> &get_all_nodes() const { return all_nodes; }

    void print_nodes() const {
        for (const auto node : all_nodes) {
            std::cout << node.first << std::endl;
        }
    }

    // Tracks the set of current active nodes in the system.

   private:
    uint32_t num_segments, num_pages, num_sn;
    // This node's node_id.
    std::pair<NodeType, node_id_t> this_node_info;
    std::unordered_map<node_id_t, Node *> all_nodes;
    // Note, the node with partition_id==1 is stored in sn_nodes[0];
    std::vector<Node *> sn_nodes;
    std::vector<Node *> db_nodes;
    std::vector<Node *> ts_nodes;
    std::vector<Node *> client_nodes;

    void addNode(Node *node) {
        node_id_t &id = node->node_id;
        ASSERT(all_nodes.find(id) == all_nodes.end());
        all_nodes[id] = node;
        switch (node->node_type) {
            case SN:
                sn_nodes.push_back(node);
                break;
            case DB:
                db_nodes.push_back(node);
                break;
            case TS:
                ts_nodes.push_back(node);
                break;
            case CLIENT:
                client_nodes.push_back(node);
                break;
            default:
                ASSERT(false);
        }
    }

    void ReadFromFile(const std::string &filename) {
        std::ifstream ifs(filename.c_str(), std::fstream::in);
        ASSERT(ifs.is_open());
        node_id_t id;
        uint32_t port;
        uint32_t num_threads;
        std::string node_type, host;
        std::getline(ifs, host);  // skip the first line
        while (ifs >> id >> node_type >> host >> port >> num_threads) {
            auto node = new Node{.node_type = strToNT(node_type),
                                 .node_id = id,
                                 .host = host,
                                 .port = port,
                                 .num_threads = num_threads};
            addNode(node);
        }
        num_sn = sn_nodes.size();
    }

    void write(std::ostream &os) const {
        os << "ID\tNodeType\tHost:Port" << std::endl;
        for (const auto &iter : all_nodes) {
            Node *node = iter.second;
            os << node->node_id << "\t" << ntToStr(node->node_type) << "\t" << node->host << " "
               << node->port << std::endl;
        }
    }

   public:
    static std::string ntToStr(NodeType nt) {
        switch (nt) {
            case SN:
                return SN_Str;
            case DB:
                return DB_Str;
            case TS:
                return TS_Str;
            default:
                ASSERT(false);
                return "";
        }
    }

    static NodeType strToNT(const std::string &str) {
        if (str == SN_Str) {
            return SN;
        }
        if (str == DB_Str) {
            return DB;
        }
        if (str == TS_Str) {
            return TS;
        }
        if (str == CL_Str) {
            return CLIENT;
        }
        ASSERT(false);
        return SN;
    }
};