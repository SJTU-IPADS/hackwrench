#pragma once

#ifdef TXN_LAT_STAT

#include <stdint.h>
#include <unistd.h>

#include <algorithm>
#include <cassert>
#include <chrono>
#include <vector>

#include "servers/config.h"
#include "util/types.h"


// This file is copied from timer.h, we use these file to stat transactions' latency only
#define TXN_LAT_STAT_FREQ 10
static double CPU_CYCLES_PER_NS = -1.0;
static thread_local thread_id_t txn_latency_worker_id;
static thread_local uint64_t worker_records_count;
#ifdef DETAILED_TXN_LAT
static std::vector<std::vector<std::vector<uint64_t>>> txn_latency_records;
#else
static std::vector<std::vector<uint64_t>> txn_latency_records;
#endif
// Modify _RdtscTimer a little and then get this class 
class TxnLatencyTimer {
    public:
    TxnLatencyTimer() {
        if (unlikely(CPU_CYCLES_PER_NS < 0)) {
            std::chrono::high_resolution_clock clock;

            auto start = clock.now();
            uint64_t cycles_start = read_tsc();
            std::this_thread::sleep_for(std::chrono::microseconds(100000));
            uint64_t cycles_duration = read_tsc() - cycles_start;
            double duration = std::chrono::duration<double, std::nano>(clock.now() - start).count();

            CPU_CYCLES_PER_NS = cycles_duration / duration;
            // LOG(4) << "CPU_CYCLES_PER_NS is set to: " << CPU_CYCLES_PER_NS;
        }
    }

    void start() { passed_cycles = read_tsc(); }

    uint64_t passed_sec() {
        return (uint64_t)((read_tsc() - passed_cycles) / CPU_CYCLES_PER_NS / 1000000000);
    }

    uint64_t passed_msec() {
        return (uint64_t)((read_tsc() - passed_cycles) / CPU_CYCLES_PER_NS / 1000000);
    }

    uint64_t passed_usec() {
        return (uint64_t)((read_tsc() - passed_cycles) / CPU_CYCLES_PER_NS / 1000);
    }

    double double_passed_sec() { return passed_usec() / 1000000.; }

    uint64_t passed_nsec() { return (uint64_t)((read_tsc() - passed_cycles) / CPU_CYCLES_PER_NS); }

#ifdef DETAILED_TXN_LAT
    void end(int txn_type) {
        ASSERT(txn_type != -1);
        if(worker_records_count % TXN_LAT_STAT_FREQ == 0){
            txn_latency_records[txn_latency_worker_id][txn_type].emplace_back(passed_nsec());
        }
        worker_records_count++;
    }
#else
    void end(int txn_type) {
        if(worker_records_count % TXN_LAT_STAT_FREQ == 0){
            txn_latency_records[txn_latency_worker_id].emplace_back(passed_nsec());
        }
        worker_records_count++;
    }
#endif
    void summary(uint64_t num_op) {
        uint64_t nsec = passed_nsec();
        uint64_t msec = nsec / 1000000;
        LOG(0) << "passed_msec: " << msec << "ms. Per op: " << nsec / (double)num_op << "ns.";
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

class TxnLatencyReporter {
   public:
    static void worker_register(thread_id_t _worker_id) { txn_latency_worker_id = _worker_id; }
#ifdef DETAILED_TXN_LAT
    static void init(thread_id_t num_threads) {
        txn_latency_records.resize(num_threads, std::vector<std::vector<uint64_t>>());
        for(uint i=0; i<num_threads; i++){
            txn_latency_records[i].resize(5, std::vector<uint64_t>());
        }
    }

    static void report() {
        std::vector<uint64_t> ave;
        std::vector<uint64_t> p50;
        std::vector<uint64_t> p90;
        std::vector<uint64_t> p99;
        for(int txn_type=0; txn_type<5; txn_type++){
            // Combine all workers' latencies
            for (uint32_t i = 1; i < txn_latency_records.size(); ++i) {
                txn_latency_records[0][txn_type].insert(txn_latency_records[0][txn_type].end(), 
                    txn_latency_records[i][txn_type].begin(), txn_latency_records[i][txn_type].end());
            }
            auto &lat = txn_latency_records[0][txn_type];
            std::sort(lat.begin(), lat.end());
            uint64_t sum = 0;
            for (uint64_t i = 0; i < lat.size(); ++i) {
                sum += lat[i];
            }
            ave.emplace_back(sum / lat.size());
            p50.emplace_back(lat[lat.size() * 0.5]);
            p90.emplace_back(lat[lat.size() * 0.9]);
            p99.emplace_back(lat[lat.size() * 0.99]);
        }
        LOG(4) << "Average Transaction Latency:" << ave[0] << "-" << ave[1] << "-" << ave[2] << "-" << ave[3] << "-" << ave[4];
        LOG(4) << "P50 Transaction Latency:"     << p50[0] << "-" << p50[1] << "-" << p50[2] << "-" << p50[3] << "-" << p50[4];
        LOG(4) << "P90 Transaction Latency:"     << p90[0] << "-" << p90[1] << "-" << p90[2] << "-" << p90[3] << "-" << p90[4];
        LOG(4) << "P99 Transaction Latency:"     << p99[0] << "-" << p99[1] << "-" << p99[2] << "-" << p99[3] << "-" << p99[4];
    }
#else
    static void init(thread_id_t num_threads) {
        txn_latency_records.resize(num_threads, std::vector<uint64_t>());
    }

    static void report() {
        // Combine all workers' latencies
        for (uint32_t i = 1; i < txn_latency_records.size(); ++i) {
            txn_latency_records[0].insert(txn_latency_records[0].end(), 
                txn_latency_records[i].begin(), txn_latency_records[i].end());
        }
        auto &lat = txn_latency_records[0];
        std::sort(lat.begin(), lat.end());
        uint64_t sum = 0;
        for (uint64_t i = 0; i < lat.size(); ++i) {
            sum += lat[i];
        }
        LOG(4) << "Average Transaction Latency:" << sum / lat.size();
        LOG(4) << "P50 Transaction Latency:" << lat[lat.size() * 0.5]; 
        LOG(4) << "P90 Transaction Latency:" << lat[lat.size() * 0.9];
        LOG(4) << "P99 Transaction Latency:" << lat[lat.size() * 0.99];
    }
#endif
};

#else

class TxnLatencyReporter {
   public:
    static void worker_register(thread_id_t _worker_id) { }
    static void init(thread_id_t num_threads) { }
    static void report() { }
};

#endif