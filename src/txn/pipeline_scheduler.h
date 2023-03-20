#pragma once

#include "txn/batch_txn.h"

class PipelineScheduler {
    volatile batch_id_t last_batch_id;
    // std::mutex mutex;

   public:
    PipelineScheduler(batch_id_t starting_batch_id) : last_batch_id(starting_batch_id) {}

    inline batch_id_t next_get_timestamp() {
        // std::lock_guard<std::mutex> guard(mutex);
        batch_id_t ret = last_batch_id + 1;
        return ret;
    }

    void finish_get_timestamp(batch_id_t batch_id) {
        // std::lock_guard<std::mutex> guard(mutex);
        ASSERT(last_batch_id + 1 == batch_id)
            << "wait_ts_batch is " << last_batch_id << " " << batch_id;
        last_batch_id = batch_id;
    }
};