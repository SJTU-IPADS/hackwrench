#pragma once

#include <stdint.h>
#include <unistd.h>

#include <algorithm>
#include <cassert>
#include <chrono>
#include <vector>
#include <thread>

#include "servers/config.h"
#include "util/types.h"

#define SIMPLIFY_OUTPUT

static double CYCLES_PER_NS = -1.0;
static thread_local thread_id_t latency_worker_id;

enum LatencyType {
    WORKER_TOTAL,
    POPEVENT,
    RPCEVENT,    
    TASKEVENT,
    EXEEVENT,
    RECV_GETTS,
    RECV_PREPARE,
    RECV_COMMIT,
    LAUNCH_NEW_TXN,
    LOCAL_INDEPENDENT_GC,
    PREV_EXECUTION,
    EXECUTION,
    LOCAL_ABORT_EXECUTION,
    ENQUEUE_BATCH,
    POP_TXN_QUEUE,
    DEQUEUE_BATCH,
    NEW_BATCH,
    SPLIT_BATCH_LAT,
    MERGE_RW_INFO,
    SEND_GETTS,
    SEND_GETTS_RPC,
    LOCAL_MERGEBATCH,
    PARSE_GETTS,
    LOCAL_MERGEBATCH2,
    PREPARE_MSG,
    GENREDOLOG,
    LOCAL_PREPAREMSG,
    SEND_PREPAREMSG,
    LOCAL_COMMITMSG,
    PARSE_COMMIT,
    APPLY_DIRTY,
    LOCAL_GC,
    BATCH_REPAIR,
    PREPARE_REPAIR,
    PREPARE_AFTER_REPAIR,
    PREPARE_AFTER_REPAIR2,
    APPLY_DIFF_PAGES,
    TRAVERSING,
    DB_EXE,
    DB_GETTS,
    DB_PREPARE,
    DB_PREPARE2,
    DB_COMMIT,
    DB_REPLY_CLIENT,
    SN_CHECK_BLOCK,
    SN_BLOCKED,
    SN_PREPARE,
    SN_COMMIT_PHASE,
    SN_COMMIT_PHASE2,
    SN_COMMIT_BLOCK,
    SN_COMMIT,
    SN_VALID,
    SN_TEST2,
    SN_PUSH_UNBLOCK,
    SN_PREPARE_COMMIT,
    SN_CHECK_KEY_CONFLICT,
    RECV_GET_PAGES,
    RECV_GET_PAGES1,
    RECV_EXE,
    RECV_EXE1,
    RECV_EXE2,
    GET_INDEX_SEARCH,
    GET_INDEX_SEARCH2,
    GET_HASH_MAP,
    GET_LOCK,
    LOCAL_GC_DELETE_BATCH,
    FREE_REDO_LOGS,
    TMP,
    CAPNP_TIME,
    TYPE_COUNT
};

static const std::string latencyTypeNames[] = {
                            "WORKER_TOTAL",
                            "POPEVENT",
                            "RPCEVENT",
                            "TASKEVENT",
                            "EXEEVENT",
                            "RECV_GETTS",
                            "RECV_PREPARE",
                            "RECV_COMMIT",
                            "LAUNCH_NEW_TXN",
                            "LOCAL_INDEPENDENT_GC",
                            "PREV_EXECUTION",
                            "EXECUTION",
                            "LOCAL_ABORT_EXECUTION",
                            "ENQUEUE_BATCH",
                            "POP_TXN_QUEUE",
                            "DEQUEUE_BATCH",
                            "NEW_BATCH",
                            "SPLIT_BATCH_LAT",
                            "MERGE_RW_INFO",
                            "SEND_GETTS",
                            "SEND_GETTS_RPC",
                            "LOCAL_MERGEBATCH",
                            "PARSE_GETTS",
                            "LOCAL_MERGEBATCH2",
                            "PREPARE_MSG",
                            "GENREDOLOG",
                            "LOCAL_PREPAREMSG",
                            "SEND_PREPAREMSG",
                            "LOCAL_COMMITMSG",
                            "PARSE_COMMIT",
                            "APPLY_DIRTY",
                            "LOCAL_GC",
                            "BATCH_REPAIR",
                            "PREPARE_REPAIR",
                            "PREPARE_AFTER_REPAIR",
                            "PREPARE_AFTER_REPAIR2",
                            "APPLY_DIFF_PAGES",
                            "TRAVERSING",
                            "DB_EXE",
                            "DB_GETTS",
                            "DB_PREPARE",
                            "DB_PREPARE2",
                            "DB_COMMIT",
                            "DB_REPLY_CLIENT",
                            "SN_CHECK_BLOCK",
                            "SN_BLOCKED",
                            "SN_PREPARE",
                            "SN_COMMIT_PHASE",
                            "SN_SCOMMIT_PHASE2",
                            "SN_COMMIT_BLOCK",
                            "SN_COMMIT",
                            "SN_VALID",
                            "SN_TEST2",
                            "SN_PUSH_UNBLOCK",
                            "SN_PREPARE_COMMIT",
                            "SN_CHECK_KEY_CONFLICT",
                            "RECV_GET_PAGES",
                            "RECV_GET_PAGES1",
                            "RECV_EXE",
                            "RECV_EXE1",
                            "RECV_EXE2",
                            "GET_INDEX_SEARCH",
                            "GET_INDEX_SEARCH2",
                            "GET_HASH_MAP",
                            "GET_LOCK",
                            "LOCAL_GC_DELETE_BATCH",
                            "FREE_REDO_LOGS",
                            "TMP",
                            "CAPNP_TIME",
                            "TYPE_COUNT"};

enum CountType {
    SN_TPUT = 0,
    GETTS_MSG_SIZE,
    BATCH_SIZE,
    CONFLICT_BATCH_SIZE,
    NON_CONFLICT_BATCH_SIZE,
    NO_KEY_CONFLICT,
    TXN_NUM,
    READ_RECORD_NUM,
    PREPARE_MSG_SIZE_BASE,
    PREPARE_MSG_SIZE_INPUT,
    PREPARE_MSG_SIZE_DEP,
    PREPARE_MSG_SIZE_TS,
    PREPARE_MSG_SIZE_RWSET,
    PREPARE_MSG_SIZE_DELTA,
    READ_SET_SIZE,
    PREPARE_MSG_SIZE,
    PREPARE_DIFF_MSG_SIZE,
    CASCADING_NUM,
    NUM_DIFF_PAGES,
    REDO_LOG_SIZE,
    REMOTE_READS,
    COUNT_TYPE_COUNT
};

static const std::string countTypeNames[] = {
    "SN_TPUT",
    "GETTS_MSG_SIZE",
    "BATCH_SIZE",
    "CONFLICT_BATCH_SIZE",
    "NON_CONFLICT_BATCH_SIZE",
    "NO_KEY_CONFLICT",
    "TXN_NUM",
    "READ_RECORD_NUM",
    "PREPARE_MSG_SIZE_BASE",
    "PREPARE_MSG_SIZE_INPUT",
    "PREPARE_MSG_SIZE_DEP",
    "PREPARE_MSG_SIZE_TS",
    "PREPARE_MSG_SIZE_RWSET",
    "PREPARE_MSG_SIZE_DELTA",
    "READ_SET_SIZE",
    "PREPARE_MSG_SIZE",
    "PREPARE_DIFF_MSG_SIZE",
    "CASCADING_NUM",
    "NUM_DIFF_PAGES",
    "REDO_LOG_SIZE",
    "REMOTE_READS",
    "COUNT_TYPE_COUNT"};


static std::vector<std::vector<std::vector<uint64_t>>> latency_records;
static std::vector<std::vector<std::vector<uint64_t>>> count_records;
static const std::vector<double> percents = {0.5, 0.9, 0.99};

class TimerPlaceHolder {
   public:
    TimerPlaceHolder(){};
    void start() {}
    uint64_t passed_nsec() { return 0; }
    double double_passed_sec() { return 0.0f; }
    void end(LatencyType lat_type) {
        latency_records[lat_type][latency_worker_id].emplace_back(passed_nsec());
    }
};

class _RdtscTimer {
   public:
    _RdtscTimer() {
        if (unlikely(CYCLES_PER_NS < 0)) {
            std::chrono::high_resolution_clock clock;

            auto start = clock.now();
            uint64_t cycles_start = read_tsc();
            std::this_thread::sleep_for(std::chrono::microseconds(100000));
            uint64_t cycles_duration = read_tsc() - cycles_start;
            double duration = std::chrono::duration<double, std::nano>(clock.now() - start).count();

            CYCLES_PER_NS = cycles_duration / duration;
            // LOG(4) << "CYCLES_PER_NS is set to: " << CYCLES_PER_NS;
        }
    }

    void start() { passed_cycles = read_tsc(); }

    uint64_t passed_sec() {
        return (uint64_t)((read_tsc() - passed_cycles) / CYCLES_PER_NS / 1000000000);
    }

    uint64_t passed_msec() {
        return (uint64_t)((read_tsc() - passed_cycles) / CYCLES_PER_NS / 1000000);
    }

    uint64_t passed_usec() {
        return (uint64_t)((read_tsc() - passed_cycles) / CYCLES_PER_NS / 1000);
    }

    double double_passed_sec() { return passed_usec() / 1000000.; }

    uint64_t passed_nsec() { return (uint64_t)((read_tsc() - passed_cycles) / CYCLES_PER_NS); }

    static uint64_t passed_nsec(uint64_t passed_c) { 
        return (uint64_t)((read_tsc() - passed_c) / CYCLES_PER_NS); 
    }

    static void end_ns(LatencyType lat_type, uint64_t passed_ns) {
        latency_records[lat_type][latency_worker_id].emplace_back(passed_ns);
    }

    void end(LatencyType lat_type) {
        latency_records[lat_type][latency_worker_id].emplace_back(passed_nsec());
    }

    uint64_t get_count(LatencyType lat_type) {
        return latency_records[lat_type][latency_worker_id].size();
    }

    void summary(uint64_t num_op) {
        uint64_t nsec = passed_nsec();
        uint64_t msec = nsec / 1000000;
        LOG(2) << "passed_msec: " << msec << "ms. Per op: " << nsec / num_op << "ns.";
    }

    static inline unsigned long read_tsc(void) {
        unsigned a, d;
        __asm __volatile("rdtsc" : "=a"(a), "=d"(d));
        return ((unsigned long)a) | (((unsigned long)d) << 32);
    }

    static const uint64_t NSEC_PER_SEC = 1000000000;
    static const uint64_t USEC_PER_SEC = 1000000;
    static const uint64_t MSEC_PER_SEC = 1000;

   private:
    uint64_t passed_cycles;
};

class _MultiThreadTimer {
    typedef std::chrono::high_resolution_clock Clock;

   public:
    _MultiThreadTimer() {}

    inline void start() { start_time = Clock::now(); }

    inline uint64_t passed_nsec() {
        auto end_time = Clock::now();
        return std::chrono::duration_cast<std::chrono::nanoseconds>(end_time - start_time).count();
    }

    void end(LatencyType lat_type) {
        latency_records[lat_type][latency_worker_id].emplace_back(passed_nsec());
    }

   private:
    std::chrono::time_point<Clock> start_time;
};

typedef _RdtscTimer BenchmarkTimer;

#ifdef NO_TIMER
typedef TimerPlaceHolder RdtscTimer;
typedef TimerPlaceHolder MultiThreadTimer;
#else
typedef _RdtscTimer RdtscTimer;
typedef _MultiThreadTimer MultiThreadTimer;
#endif

class Logger {
   public:
    Logger() {}
    static void worker_register(thread_id_t _worker_id) { latency_worker_id = _worker_id; }
    static void init(thread_id_t num_threads) {
        count_records.resize(COUNT_TYPE_COUNT, std::vector<std::vector<uint64_t>>(num_threads));
    }

    static void count(CountType type, uint64_t count) {
#ifndef EVAL_MODE
        count_records[type][latency_worker_id].push_back(count);
#endif
    }

    static void report() {
#ifdef NO_TIMER
        return;
#endif
        uint32_t type_i = -1;
        for (auto &records : count_records) {
            ++type_i;
            if (type_i == SN_TPUT) {
                uint64_t total_sn_tput = 0;
                for (auto &vec : records) {
                    for (auto &count : vec) {
                        total_sn_tput += count;
                    }
                }
                // LOG(4) << "total sn tput " << total_sn_tput / 10;
            } else {
                // for (uint32_t i = 1; i < records.size(); ++i) {
                //     records[0].insert(records[0].end(), records[i].begin(), records[i].end());
                // }
                auto &record = records[0];
                if (record.empty())
                    continue;
                std::sort(record.begin(), record.end());
                uint64_t sum = 0;
                for (auto &count : record) {
                    sum += count;
                }
                LOG(4) << "[Logger] " << type_i << " " << countTypeNames[type_i] << ": sum " << sum
                    << ",count " << record.size()
                    << ",average " << (double) sum / record.size();

                for (auto &percent : percents) {
                    LOG(3) << "percent is " << percent << " " << record[record.size() * percent];
                }
            }
        }
    }
};

class LatencyLogger {
   public:
    LatencyLogger() {}
    static void worker_register(thread_id_t _worker_id) { latency_worker_id = _worker_id; }
    static void init(thread_id_t num_threads) {
        timers.resize(TYPE_COUNT, std::vector<RdtscTimer>(num_threads));
        latency_records.resize(TYPE_COUNT, std::vector<std::vector<uint64_t>>(num_threads));
    }

    // Prevent multiple workers from using same timer
    static void start(LatencyType lat_type) {
#ifndef NO_TIMER
        timers[lat_type][latency_worker_id].start();
#endif
    }
    static void end(LatencyType lat_type) {
#ifndef NO_TIMER
        timers[lat_type][latency_worker_id].end(lat_type);
#endif
    }

    static uint64_t get_count(LatencyType lat_type) {
#ifndef NO_TIMER
        return timers[lat_type][latency_worker_id].get_count(lat_type);
#else
        return 0;
#endif
    }

    static void report() {
#ifdef NO_TIMER
        return;
#endif
        uint32_t type_i = -1;
        for (auto &lat_type : latency_records) {
            type_i++;
            // Combine all workers' latencies of same Type
            // for (uint32_t i = 1; i < lat_type.size(); ++i) {
            //     lat_type[0].insert(lat_type[0].end(), lat_type[i].begin(), lat_type[i].end());
            // }
            auto &lat = lat_type[0];
            // No collected latencies, or no legal latencies(all 0), just skip
            if (lat.empty())
                continue;
            std::sort(lat.begin(), lat.end());
            if (lat.back() == 0)
                continue;

            uint64_t sum = 0;
            for (uint64_t j = 0; j < lat.size(); ++j) {
                sum += lat[j];
            }

#ifdef SIMPLIFY_OUTPUT
            LOG(4) << type_i << " " << latencyTypeNames[type_i] << " " << sum / lat.size() << " "
                   << lat.size() << " " << sum;
#else
            LOG(4) << type_i;
            LOG(4) << "lat avg " << sum / lat.size();

            for (auto &percent : percents) {
                LOG(4) << "lat " << percent << " " << lat[lat.size() * percent];
            }
#endif
        }
    }

    static void clear() {
        latency_records.clear();
        init(1);
    }

    static std::vector<std::vector<RdtscTimer>> timers;
};

inline void thread_sleep(uint seconds) {
    std::this_thread::sleep_for(std::chrono::seconds(seconds));
}

inline void thread_sleep_micros(uint micros) {
    std::this_thread::sleep_for(std::chrono::microseconds(micros));
}

inline void timer_start(std::function<void(void)> func, unsigned int interval) {
    std::thread([func, interval]() {
        while (true) {
            func();
            std::this_thread::sleep_for(std::chrono::milliseconds(interval));
        }
    }).detach();
}

std::vector<std::vector<RdtscTimer>> LatencyLogger::timers(0);
