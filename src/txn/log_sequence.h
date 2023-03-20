#pragma once

#include <map>
#include <vector>

#include "redo_log.h"
#include "util/types.h"

class LogSequence {
    static const uint64_t APPLY_THRESHOLD = 100;
    // <ptr, log_size>
    using log_seq_t = std::map<ts_t, std::pair<log_t, uint32_t>>;

   private:
    log_seq_t logs;

   public:
    std::pair<log_seq_t::iterator, log_seq_t::iterator> get_log_range(
        ts_t cts) {
        return {logs.upper_bound(cts), logs.end()};
    }

    std::pair<log_seq_t::iterator, log_seq_t::iterator> get_dep_log_range(
        ts_t begin_cts, ts_t end_cts) {
        auto begin_iter = logs.upper_bound(begin_cts);
        auto end_iter = logs.upper_bound(end_cts);
        return {begin_iter, end_iter};
    }

    log_seq_t::iterator get_applyable_log_range() {
        auto iter = logs.begin();
        while (iter != logs.end() && iter->second.first != nullptr) {
            iter++;
        }
        ASSERT(iter == logs.end());
        return iter;
    }

    void erase_until(log_seq_t::iterator &end) { logs.erase(logs.begin(), end); }

    std::pair<log_seq_t::iterator, log_seq_t::iterator> get_last_log_range(
        ts_t cts) {
        return {--logs.end(), logs.end()};
    }

    void put_log(ts_t cts, log_t log, uint32_t log_size) { 
        logs[cts] = std::make_pair(log, log_size); 
    }

    uint64_t get_num_logs() { return logs.size(); }

    bool need_apply_bg() { return logs.size() > APPLY_THRESHOLD; }

    log_seq_t &get_logs() { return logs; }
};