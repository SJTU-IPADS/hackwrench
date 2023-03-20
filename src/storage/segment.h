#pragma once

#include <map>
#include <vector>
#include <mutex>
#include <unordered_map>

#include "servers/config.h"
#include "storage/txn_info.h"
#include "txn/log_sequence.h"
#include "util/static_hash_map.h"

#if defined(DETERMINISTIC_VALIDATION) || defined(FINE_VALIDATION)
enum LockMode {
    UNLOCKED = 0,
    READ = 1,
    WRITE = 2,
};

struct LockRequest {
    LockRequest(LockMode m, TxnInfo* t) : txn_info(t), mode(m) {}
    TxnInfo* txn_info;  // Pointer to txn requesting the lock.
    LockMode mode;  // Specifies whether this is a read or write lock request.
};
#endif

#ifdef FINE_VALIDATION
struct PageLocks{
#ifdef PAGE_LEVEL_LOCK
    std::deque<LockRequest> plock;
#else
    std::unordered_map<uint32_t, std::vector<LockRequest>> locks;
#endif
    std::mutex mtx;
};
#endif

struct TxnDependency {
    ts_t ts = 0;
    uint32_t num_reads = 0;

    TxnDependency() : ts(0), num_reads(0) {}
    TxnDependency(const TxnDependency &other)
        : ts(other.ts), num_reads(other.num_reads) {}

    TxnDependency &operator=(const TxnDependency &other) {
        ts = other.ts;
        num_reads = other.num_reads;
        return *this;
    }
    bool is_concurrent_with(const TxnDependency &other, bool is_read) const {
        return this->ts == other.ts && is_read;
    }

    bool operator<(const TxnDependency &other) const {
        return (this->ts < other.ts) || (this->ts == other.ts && this->num_reads < other.num_reads);
    }
    bool operator==(const TxnDependency &other) const {
        return (this->ts == other.ts) && (this->num_reads == other.num_reads);
    }
    bool operator<=(const TxnDependency &other) const {
        return (*this < other) || (*this == other);
    }
};

struct TxnDependencyWithLock {
    TxnDependency dep;
    std::mutex mtx;
};


struct TxnDepHasher {
    std::size_t operator()(const TxnDependency &k) const {
        using std::hash;
        using std::size_t;

        return hash<uint64_t>()(k.ts) ^ (hash<uint64_t>()(k.num_reads) << 1);
    }
};

std::ostream &operator<<(std::ostream &os, const TxnDependency &dep) {
    os << dep.ts << " " << dep.num_reads;
    return os;
}

struct SegTxnInfo {
    TxnDependency dep;
    std::unordered_set<uint64_t> read_offsets;
    std::unordered_set<uint64_t> write_offsets;
};

class Segment {
   public:

    Segment(seg_id_t seg_id) : seg_id(seg_id) {
#ifdef NO_TS
    for (uint32_t i = 0; i < NUM_PAGES_PER_SEGMENT; ++i) {
        aurora_locked[i].store(0);
    }
#endif
    }

    ~Segment() {
        for (auto* page_meta : page_metas) {
            if (page_meta == nullptr) {
                continue;
            }
            delete page_meta->get_data();
            delete page_meta;
        }
    }

    PageMeta *acquire_page() {
        LogSequence* log_sequence = new LogSequence;

        PageMeta *page_meta = new PageMeta;
        page_meta->gp_id.seg_id = seg_id;
        page_meta->gp_id.page_id = cur_page_id;
        page_meta->cur_page_size = 0;
        page_meta->max_page_size = PAGE_SIZE;
        page_meta->set_data(PageMeta::new_data());
        memset(page_meta->get_data(), 0, PAGE_SIZE);
        page_meta->set_cts(0);
        page_meta->reserved_ptr = log_sequence;
        page_metas[cur_page_id] = page_meta;

#ifdef FINE_VALIDATION
        page_locks[cur_page_id] = new PageLocks;
#endif
#ifdef FINE_AURORA
        record_locks[cur_page_id] = new std::unordered_map<uint32_t, std::atomic<txn_id_t>>;
#endif
        ++cur_page_id;

        return page_meta;
    }

    PageMeta *get_page_meta(page_id_t page_id) {
        PageMeta* page_meta = page_metas[page_id];
        return page_meta;
    }


    void release_page(const PageMeta &page_meta) { ASSERT(false); }

    bool page_is_free(page_id_t next_page_id) const {
        ASSERT(next_page_id == cur_page_id);
        return next_page_id == cur_page_id;
    }

    // return true if there is a true key conflict
    // return false if there is no key conflict found so far.
    // ret is set to non-zero value if it needs blocking
    // ret is set to all zero if it doesn't need blocking
    bool get_last_conflict_dep(const TxnDependency &dep, bool is_read) {
        if (dep == curr_dep) {
            ASSERT(false);
            return false;
        }

        if (dep.is_concurrent_with(curr_dep, is_read))
            return false;

        return true;
    }

    bool check_if_txn_should_block(const Seg::Reader &seg_reader, TxnInfo *txn_info, bool is_read,
                                   node_id_t req_db_id) {
        TxnDependency dep;
        const Dependency::Reader &dep_reader = seg_reader.getDep();
        dep.ts = dep_reader.getTimestamp();
        dep.num_reads = dep_reader.getNumReads();

        std::lock_guard<std::mutex> lock(blocking_txn_m);
        if (curr_dep < dep) {
            if (get_last_conflict_dep(dep, is_read)) {
                txns_blocked_by[dep].push_back(txn_info);
                return true;
            }
        }
        return false;
    }

    void update_segment_dep(bool is_read,
                            std::vector<TxnInfo *> &txn_infos_to_unblock,
                            node_id_t req_db_id) {
        std::lock_guard<std::mutex> lock(blocking_txn_m);
        if (is_read) {
            curr_dep.num_reads++;
        } else {
            curr_dep.ts = get_cts();
            curr_dep.num_reads = 0;
        }

        auto find_iter = txns_blocked_by.find(curr_dep);
        if (find_iter != txns_blocked_by.end()) {
            auto &vec = find_iter->second;
            auto iter = vec.begin();
            for (; iter != vec.end(); ++iter) {
                TxnInfo *to_unblock = *iter;
                txn_infos_to_unblock.push_back(to_unblock);
            }
            txns_blocked_by.erase(find_iter);
        }
    }

    ts_t get_cts() {
        return curr_dep.ts + 1;
    }

#ifdef FINE_VALIDATION
    PageLocks* get_page_locks(page_id_t page_id) {
        return page_locks[page_id];
    }
#endif
#ifdef FINE_AURORA
    std::unordered_map<uint32_t, std::atomic<txn_id_t>> *get_page_record_locks(page_id_t page_id) {
        return record_locks[page_id];
    }
#endif

   public:
    seg_id_t seg_id;
    page_id_t cur_page_id = 0;
    ts_t cts;
    uint32_t value_size;

    TxnDependency curr_dep;
    // Whether put logSequence into PageMeta?
    PageMeta* page_metas[NUM_PAGES_PER_SEGMENT];

    std::unordered_map<TxnDependency, std::list<TxnInfo *>, TxnDepHasher> txns_blocked_by;

#ifdef DETERMINISTIC_VALIDATION
    std::deque<LockRequest> lock_reqs;
#endif
#ifdef FINE_VALIDATION
    PageLocks* page_locks[NUM_PAGES_PER_SEGMENT];
#endif
#ifdef FINE_AURORA
    std::unordered_map<uint32_t, std::atomic<txn_id_t>> *record_locks[NUM_PAGES_PER_SEGMENT];
    std::mutex rl_mu[NUM_PAGES_PER_SEGMENT];
#endif
#ifdef NO_TS
    std::atomic<txn_id_t> aurora_locked[NUM_PAGES_PER_SEGMENT];
#endif
    // static size should be # of DB
    mutable std::mutex blocking_txn_m;
};