#include <execinfo.h>
#include <jemalloc/jemalloc.h>

#include <csignal>
#include <thread>

#include "benchmarks/tpcc/workload_tpcc.h"
#include "benchmarks/workload_ycsbt10.h"

#define WORKLOAD_NONE 0
#define WORKLOAD_TPCC 1
#define WORKLOAD_XPP 2
#define WORKLOAD_YCSB 3
#define WORKLOAD_SWAP 4
#define WORKLOAD_YCSB10 5
#define WORKLOAD_TPCCLIKE 6

#include "servers/clients.h"
#include "servers/db_node.h"
#include "servers/storage_node.h"
#include "servers/time_server.h"

std::unique_ptr<Configuration> config;
std::unique_ptr<BenchmarkInterface> benchmark;
static const std::string config_filename = "./hackwrench.ini";

void run_server() {
    InMemSegments *segments = new InMemSegments(*config);

    StorageNodeRpcServer server(*config, *segments);
    server.run();
    // delete segments;
}

void run_timestamp_server() {
    TimestampLayer *timeServer = new TimestampLayer(*config);
    TimestampRpcServer server(*config, timeServer);
    server.run();
    delete timeServer;
}

void run_database(uint32_t num_segs, uint32_t batching_size, uint32_t split_batch_num,
                  uint32_t cache_miss_ratio) {
    DatabaseRpcServer database(*config, *benchmark, num_segs, batching_size, split_batch_num,
                               cache_miss_ratio);
    database.run();
}

void run_client(uint32_t num_clients) {
    // ClientServer clients(*config, *benchmark, num_clients_per_db * config->numDB());
    ClientServer clients(*config, *benchmark, num_clients);
    clients.run();
}

void signalHandler(int signum) {
    switch (signum) {
        case SIGINT:
            LOG(4) << "Received SIGINT";
            break;
        case SIGKILL:
            LOG(4) << "Server is killed";
            break;
        case SIGTERM:
            LOG(4) << "Server is terminated";
            break;
        case SIGSEGV:
            void *array[10];
            size_t size;

            // get void*'s for all entries on the stack
            size = backtrace(array, 10);

            // print out all the frames to stderr
            LOG(4) << "segment fault";
            backtrace_symbols_fd(array, size, STDERR_FILENO);
            exit(1);
            break;
        default:
            break;
    }
    exit(0);
}

void register_signals() {
    signal(SIGINT, signalHandler);
    signal(SIGKILL, signalHandler);
    signal(SIGTERM, signalHandler);
    // signal(SIGSEGV, signalHandler);
}

void init() {
    _RdtscTimer timer;  // This will initialize the CYCLES_PER_NS
    register_signals();
}

void print_macros() {
#ifdef RPC_CLIENTS
    LOG(3) << "Networked clients are enabled!";
#else
    LOG(3) << "Networked clients are disabled!";
#endif 

#ifdef PAGE_LEVEL_LOCK
    LOG(3) << "PAGE_LEVEL_LOCK is enabled!";
#else
    LOG(3) << "PAGE_LEVEL_LOCK is disabled!";
#endif
}

int main(int argc, char **argv) {
    LOG(2) << "Current version: " << VERSION_STRING;
    if (argc < 11) {
        std::cout << "usage: " << argv[0] << " nodeId" << std::endl;
        exit(0);
    }

    init();

#ifdef EMULATE_AURORA
    LOG(4) << "testing aurora";
#else
    LOG(4) << "testing txn repair system";
#endif

    // As we have already give the Node_id->Node_Type mapping
    // we do not need to encode server/client/timeserver here
    node_id_t my_node_id = std::stoul(argv[1]);
    uint32_t workload_id = std::stoul(argv[2]);
    uint32_t num_segs = std::stoul(argv[3]);
    uint32_t num_clients = std::stoul(argv[4]);
    uint32_t contention_factor = 0, contention_factor1 = 0;
    uint32_t batching_size = 100;
    uint32_t split_batch_num = 1;
    uint32_t optional = 1;
    uint32_t cache_miss_ratio = 0;
    bool sn_replication = false;

    if (workload_id == WORKLOAD_YCSB10){
        // num_segs = 16 * 1024 * 1024 / (SEG_SIZE >> 10);
        num_segs = 12 * 1024 * 1024 / (SEG_SIZE >> 10);
    }

    contention_factor = std::stoul(argv[5]);
    contention_factor1 = std::stoul(argv[6]);
    batching_size = std::stoul(argv[7]);
    split_batch_num = std::stoul(argv[8]);
#ifdef EMULATE_AURORA
    batching_size = 1;
    split_batch_num = 1;
#endif
#ifdef NO_CONTENTION
    contention_factor = 0;
    contention_factor1 = 0;
#endif
    ASSERT(num_clients >= batching_size) << num_clients << " " << batching_size;
    optional = std::stoul(argv[9]);
    cache_miss_ratio = std::stoul(argv[10]);
    sn_replication = std::stoul(argv[11]) != 0;

    config =
        std::make_unique<Configuration>(my_node_id, num_segs, sn_replication, config_filename);
    uint32_t num_threads = config->get_my_node().num_threads;

    Logger::init(num_threads);
    LatencyLogger::init(num_threads);
    TxnLatencyReporter::init(num_threads);

    switch (workload_id) {
        case WORKLOAD_TPCC:
            LOG(4) << "Running TPCC";
            benchmark = std::make_unique<WorkloadTpcc>(*config, num_segs, num_threads,
                                                       num_clients, contention_factor, contention_factor1, optional);
            break;
        case WORKLOAD_YCSB10:
            LOG(4) << "Running YCSB10";
            benchmark = std::make_unique<WorkloadYcsbt10>(
                *config, num_threads, num_clients, contention_factor, contention_factor1, cache_miss_ratio, optional);
            break;
        default:
            ASSERT(0);
    }

    LOG(3) << VAR2(config->get_my_id(), " ") << VAR2(num_threads, " ") 
           << VAR2(config->numSegments(), " ") << VAR2(config->numPages(), " ")
           << VAR2(num_clients, " ") << VAR2(contention_factor, " ") << VAR2(contention_factor1, " ")
           << VAR2(batching_size, " ") << VAR2(split_batch_num, " ")
           << VAR2(optional, " ") << VAR2(cache_miss_ratio, " ")
           << VAR2(sn_replication, " ");
    print_macros();

    RdtscTimer time;  // init the CYCLE_PER_NS;

    const auto &node = config->get_my_node();
    switch (node.node_type) {
        case Configuration::SN:
            run_server();
            break;
        case Configuration::DB:
            run_database(num_segs, batching_size, split_batch_num, cache_miss_ratio);
            break;
        case Configuration::TS:
            run_timestamp_server();
            break;
        case Configuration::CLIENT:
            run_client(num_clients);
            break;
        default:
            ASSERT(false) << "Unknown mode: " << argv[1];
    }
    delete benchmark.release();
    delete config.release();
    // malloc_stats_print(NULL, NULL, NULL);

    return 0;
}