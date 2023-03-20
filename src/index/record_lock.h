#pragma once

#include <atomic>
#include <list>
#include <map>

#include "servers/config.h"
#include "util/thread_safe_structures.h"
#include "util/timer.h"

#define LOCKS_PER_MANAGER 1024

#ifdef NON_CACHING
static const uint32_t RETRY_TIMES = 1000;
#else
static const uint32_t RETRY_TIMES = 10000000;
#endif

enum LockStatus {
    SUCCESS = 0,
    BATCH_FAIL = 1,
    RECORD_FAIL = 2
};

class RecordRWLock {
   private:
    std::atomic<int64_t> content;
#ifdef READ_COMMITTED
    std::atomic<batch_id_t> batch_owner;
#endif
   public:
    uint64_t key;  // for debugging
    RecordRWLock(uint64_t key = 0) : key(key) {
        init();
    }
    void init() {
        content = 0;
#ifdef READ_COMMITTED
        batch_owner = NULL_BATCH_ID;
#endif
    }

    LockStatus read_lock(txn_id_t txn_id, batch_id_t batch_id = NULL_BATCH_ID) {
#ifdef READ_COMMITTED
        batch_id_t old_owner = acquire_batch(batch_id);
        if (old_owner != NULL_BATCH_ID) {
#ifdef LOCK_FAIL_BLOCK
            throw BatchLockException(old_owner);
#endif
            return LockStatus::BATCH_FAIL;
        }
#endif

        int64_t x;
        uint try_times = 0;
        while (true) {
            x = content.load();
            if (x >= 0 && content.compare_exchange_weak(x, x+1)) {
                break;
            }
            if (++try_times > RETRY_TIMES) {
                return LockStatus::RECORD_FAIL;
            }
        }
        return LockStatus::SUCCESS;
    }

    union Wrapper {
        Wrapper(uint64_t word) : word(word){}
        Wrapper(uint64_t b_id, uint16_t count) : b_id(b_id), count(count){}
        struct {
            uint64_t b_id : 48;
            uint64_t count : 16;
        };
        uint64_t word;
    };

    LockStatus lock(txn_id_t txn_id, batch_id_t batch_id = NULL_BATCH_ID) {
#ifdef READ_COMMITTED
        batch_id_t old_owner = acquire_batch(batch_id);
        if (old_owner != NULL_BATCH_ID) {
#ifdef LOCK_FAIL_BLOCK
            throw BatchLockException(old_owner);
#endif
            return LockStatus::BATCH_FAIL;
        }
#endif

        uint try_times = 0;
        int64_t x;
        while (true) {
            x = content.load();
            if (x == 0 && content.compare_exchange_weak(x, -1)) {
                break;
            }
            if (++try_times > RETRY_TIMES) {
                // ASSERT(false);
                // LOG(2) << "lock record " << this << " failed " << std::hex << x << " " << batch_id;
                // try_times = 0;
                return LockStatus::RECORD_FAIL;
            }
        }
        return LockStatus::SUCCESS;
    }

    void unlock(txn_id_t txn_id) {
        int64_t x = content.load();
        content.fetch_add(x < 0 ? 1 : -1);
    }

#ifdef READ_COMMITTED
    batch_id_t acquire_batch(batch_id_t batch_id) {
        uint try_times = 0;
        batch_id_t tmp_owner = NULL_BATCH_ID;
        Wrapper w(batch_id, 1);
        Wrapper temp(tmp_owner);
        while (true) {
            temp.word = batch_owner.load();
            if (temp.word == NULL_BATCH_ID) {
                w.count = 1;
                if (batch_owner.compare_exchange_weak(temp.word, w.word)) {
                    break;
                }
            } else if (temp.b_id == w.b_id) {
                w.count = temp.count + 1;
                if (batch_owner.compare_exchange_weak(temp.word, w.word)) {
                    break;
                }
            } 
            if (++try_times > 1000) {
                // LOG(2) << "lock batch " << this << " failed " << std::hex << temp.word << " " << batch_id;
                // ASSERT(!(w.count > temp.count) || w.count - temp.count < 20000) 
                //     << "lock batch " << this << " failed " << std::hex << temp.word << " " << batch_id;;
                return temp.b_id;
            }
        }
        // LOG(2) << "lock batch " << this << " success " << std::hex << temp.word << " " << w.word;
        ASSERT(temp.count + 1 == w.count);
        return NULL_BATCH_ID;
    }

    void unlock_batch(batch_id_t batch_id) {
        Wrapper temp(batch_owner);
        ASSERT(temp.b_id == batch_id) << this << " " << std::hex << temp.word << " " << batch_id;
        ASSERT(temp.count > 0);
        Wrapper w(temp.word);
        if (w.count == 1) {
            w.word = NULL_BATCH_ID;
        } else {
            w.count -= 1;
        }

        while (!batch_owner.compare_exchange_weak(temp.word, w.word)) {
            w.word = temp.word;
            ASSERT(temp.b_id == batch_id);
            ASSERT(temp.count > 0);
            if (w.count == 1) {
                w.word = NULL_BATCH_ID;
            } else {
                w.count -= 1;
            }
        }
    }
#endif
};

using RecordLock = RecordRWLock;
