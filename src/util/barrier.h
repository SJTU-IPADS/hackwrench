#pragma once

#include <pthread.h>
#include <atomic>

/**
 * a simple wrapper over pthread
 */
class Barrier {
   public:
    explicit Barrier(uint32_t num) : wait_num_(num) {
        pthread_barrier_init(&barrier_, nullptr, num);
    }

    ~Barrier() { pthread_barrier_destroy(&barrier_); }

    void wait() {
        wait_num_ -= 1;
        pthread_barrier_wait(&barrier_);
    }

    void done() { wait_num_ -= 1; }

    bool ready() const { return wait_num_ == 0; }

    uint32_t wait_num() const { return wait_num_; }

   private:
    pthread_barrier_t barrier_;
    std::atomic<uint32_t> wait_num_;

   private:
    Barrier(const Barrier &) = delete;
    Barrier &operator=(const Barrier &) = delete;
};
