#pragma once

#include <set>
#include <unordered_map>

#include "index/interface.h"
#include "servers/config.h"
#include "storage/multi_ver_record.h"
#include "util/exceptions.h"
#include "util/macros.h"
#include "util/types.h"

struct RedoLogKvKey {
    seg_id_t seg_id;  // different tables might have same keys.
    Key_t key;
};

bool operator==(const RedoLogKvKey &one, const RedoLogKvKey &other) {
    return (one.seg_id == other.seg_id) && (one.key == other.key);
}

struct RedoLogKvKeyHasher {
    std::size_t operator()(const RedoLogKvKey &k) const {
        using std::hash;
        using std::size_t;
        size_t k1 = hash<seg_id_t>()(k.seg_id);
        size_t k2 = hash<Key_t>()(k.key);
        // https://en.wikipedia.org/wiki/Pairing_function#Cantor_pairing_function
        return (k1 + k2) * (k1 + k2 + 1) / 2 + k2;
    }
};

class BatchManager {
   public:

    inline void set_batch_id(batch_id_t batch_id) { this->batch_id = batch_id; }

    inline batch_id_t get_batch_id() { return batch_id; }

    template <class Key_t, class Value_t>
    bool acquire_lock(IndexInterface<Key_t, Value_t> &table, Key_t key) {
        auto *pair = table.get(key);
        RecordLock *rl = get_record_lock(pair);
        bool success = rl->lock(999999999999, 999999);
        if (!success) {
            return false;
        }
        ASSERT(success) << "batch record lock fail";
        hold_locks.insert(rl);
        // LOG(3) << batch_id << " acquire " << key;
        return false;
    }

    bool acquire_lock(void *record) {
        RecordLock *rl = get_record_lock(record);
        bool success = rl->lock(999999999999, 999999);
        // ASSERT(success) << "batch record lock fail";
        if (!success) {
            return false;
        }
        hold_locks.insert(rl);
        return true;
    }

    void release_locks() {
        for (RecordLock *rl : hold_locks) {
            // LOG(4) << batch_id << " release " << rl->key;
            rl->unlock(999999999999);
#ifdef READ_COMMITTED
            rl->unlock_batch(999999);
#endif
        }

        hold_locks.clear();
    }

    struct RedoLogKvCache {
        uint8_t* val;
        version_ts_t version;
    };
    
    std::mutex redo_log_mu;

    std::unordered_map<RedoLogKvKey, RedoLogKvCache, RedoLogKvKeyHasher> redo_log_kv;

    batch_id_t batch_id;

   private:
    std::unordered_set<RecordLock *> hold_locks;
};