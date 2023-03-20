#pragma once

#if defined(TOTAL_ORDER_TS) || defined(DETERMINISTIC_VALIDATION)

#include <deque>
#include <set>
#include <unordered_map>

#include "txn_info.h"

class SequenceManager {
   public:
    std::atomic<uint64_t> seq;
    BatchThreadSafeMap<uint64_t, TxnInfo *> waiting_txns;

    SequenceManager(thread_id_t num_threads) : waiting_txns(num_threads) {
        seq.store(0);
    }

    bool check_for_noblock(TxnInfo* txn_info) {
        bool success;
        uint64_t batch_seq = txn_info->seq;
        uint64_t cur_seq = seq.load();
        // LOG(2) << "ready_to_prepare: " << std::hex << txn_info << " " << txn_info->txn_id << " " << txn_info->seq << " " << batch_seq << " " << cur_seq;
        if (cur_seq == batch_seq) {
            // LOG(2) << "ready_to_prepare1: " << std::hex << txn_info << " " << txn_info->txn_id << " " << batch_seq << " " << cur_seq;
            success = true;
        } else {
            txn_info->num_blocked_segs.fetch_add(1);
            waiting_txns.put(batch_seq, txn_info);
            success = false;
            if (seq.load() == batch_seq && waiting_txns.try_get_then_erase(batch_seq, txn_info)) {
                uint64_t o = txn_info->num_blocked_segs.fetch_sub(1);
                ASSERT(o == 1);
                success = true;
                // LOG(2) << "ready_to_prepare2: " << std::hex << txn_info << " " << txn_info->txn_id << " " << batch_seq << " " << cur_seq;
            }
        }
        return success;
    }

    TxnInfo* unblock(TxnInfo* txn_info) {
        TxnInfo* ret = nullptr;
        uint64_t batch_seq = txn_info->seq;
        uint64_t cur_seq = seq.fetch_add(1);
        ASSERT(cur_seq == batch_seq) << std::hex << cur_seq << " " << batch_seq << " " << txn_info->seq << " " << txn_info->txn_id << " " << txn_info;
        TxnInfo* info = nullptr;
        ++batch_seq;
        bool get_success = waiting_txns.try_get_then_erase(batch_seq, info);
        if (get_success) {
            // LOG(2) << "unblock: " << std::hex << info << " " << batch_seq << " " << info->txn_id << " success " << txn_info << " " << txn_info->txn_id;
            ret = info;
            waiting_txns.erase(batch_seq);
        }
        return ret;
    }

};

#endif