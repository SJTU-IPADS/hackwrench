#pragma once

#include "servers/config.h"

#ifdef FINE_VALIDATION

#include <set>
#include <unordered_map>

#include "storage.h"
#include "txn_info.h"

using unblock_txn_func_t = std::function<void(TxnInfo*)>;

class LockManager {
   public:
    LockManager(InMemSegments &segments) : segments(segments) {}

    bool lock(TxnInfo* txn_info, 
            const capnp::List<Seg, capnp::Kind::STRUCT>::Reader &req_segs,
            std::vector<TxnInfo *>& txn_infos_to_unblock) {
        int32_t not_acquire = 0;
        for (const auto &req_seg : req_segs) {
            seg_id_t seg_id = req_seg.getSegId();
            auto r_pages = req_seg.getReadPages();
            auto w_pages = req_seg.getWritePages();
            auto &segment = segments.get_segment(seg_id);

            not_acquire += lock(txn_info, segment, w_pages, true);
            not_acquire += lock(txn_info, segment, r_pages, false);
            
#ifndef TOTAL_ORDER_TS
            bool is_read = w_pages.size() == 0;
            segment.update_segment_dep(is_read, txn_infos_to_unblock, txn_info->msg.getSender());
#endif
        }

        txn_info->lock_acquired = true;
        if (not_acquire > 0) {
            int remaining = txn_info->num_blocked_locks.fetch_add(not_acquire);
            if (remaining == - not_acquire) {
                return true;
            } else {
                return false;
            }
        } else {
            return true;
        }
    }

    int32_t lock(TxnInfo* txn_info, Segment& segment,
            const capnp::List<Page, capnp::Kind::STRUCT>::Reader& pages,
            bool is_write) {
        int32_t not_acquire = 0;
        for (const auto &page : pages) {
            if (!page.hasRedoLog()) {
                continue;
            }
            if (is_write) {
                not_acquire += lock(txn_info, segment, page, is_write);
            } else {
                not_acquire += lock_read(txn_info, segment, page);
            }
        }
        return not_acquire;
    }

    int32_t lock_read(TxnInfo* txn_info, Segment& segment, 
            const Page::Reader& page) {
        int32_t not_acquire = 0;
        GlobalPageId gp_id(page.getGlobalPageId());
        auto* page_locks = segment.get_page_locks(gp_id.page_id);
#ifdef PAGE_LEVEL_LOCK
        std::lock_guard<std::mutex> l(page_locks->mtx);
        not_acquire += lock(txn_info, page_locks, 0, false);
#else
        auto bytes = page.getRedoLog().asBytes();
        RedoLogReader log_reader(bytes, segment.value_size);
        std::lock_guard<std::mutex> l(page_locks->mtx);
        while (log_reader.valid()) {
            offset_t page_offset = log_reader.get_real_page_offset();
            not_acquire += lock(txn_info, page_locks, page_offset, false);
            log_reader.next();
        }
#endif
        return not_acquire;
    }

    int32_t lock(TxnInfo* txn_info, Segment& segment, 
            const Page::Reader& page,
            bool is_write) {
        int32_t not_acquire = 0;
        GlobalPageId gp_id(page.getGlobalPageId());
        auto* page_locks = segment.get_page_locks(gp_id.page_id);
#ifdef PAGE_LEVEL_LOCK
        std::lock_guard<std::mutex> l(page_locks->mtx);
        not_acquire += lock(txn_info, page_locks, 0, is_write);
#else
        auto bytes = page.getRedoLog().asBytes();
        std::unordered_map<offset_t, bool> offset2rw;
        RedoLogReader log_reader(bytes, segment.value_size);
        while (log_reader.valid()) {
            offset_t page_offset = log_reader.get_real_page_offset();
            bool is_write = log_reader.is_write_entry();
            offset2rw[page_offset] |= is_write;
            log_reader.next();
        }
        
        std::lock_guard<std::mutex> l(page_locks->mtx);

        for (const auto& pair: offset2rw) {
            not_acquire += lock(txn_info, page_locks, pair.first, pair.second);
        }
#endif
        return not_acquire;
    }

    int32_t lock(TxnInfo* txn_info, PageLocks* page_locks, uint32_t offset, bool is_write) {
        int32_t not_acquire = 0;
#ifdef PAGE_LEVEL_LOCK        
        auto* lock_reqs = &page_locks->plock;
#else
        auto* lock_reqs = &page_locks->locks[offset];
#endif
        if (is_write) {
            if (lock_reqs->empty() || txn_info != lock_reqs->back().txn_info) {
                // Write lock request fails if there is any previous request at all.
                if (lock_reqs->size() > 0) {
                    // LOG(2) << "blocked found " << std::hex << txn_info->txn_id << " " << lock_reqs->back().txn_info->txn_id;
                    // txn_info->blocked_segs.push_back(std::make_pair(seg_id, lock_reqs->back().txn_info->txn_id));
#ifdef BREAKDOWN_CASCADING
                        bool first = true;
                        uint32_t len = 0;
                        // for (const LockRequest& r : *lock_reqs) {
                        //     if (&r == &lock_req) {
                        //         len = 0;
                        //     }
                        //     len += 1;
                        // }
                        for (const LockRequest& r : *lock_reqs) {
                            len += 1;
                            if (r.mode == WRITE && first) {
                                len = 0;
                                first = false;
                            }
                        }
                        Logger::count(CountType::CASCADING_NUM, len);
#endif
                    not_acquire = 1;
                }
                lock_reqs->emplace_back(WRITE, txn_info);
            }
        } else {
            if (lock_reqs->empty() || txn_info != lock_reqs->back().txn_info) {
                lock_reqs->emplace_back(READ, txn_info);
                // Read lock request fails if there is any previous write request.
                for (const LockRequest& lock_req : *lock_reqs) {
                    if (lock_req.mode == WRITE) {
                        // LOG(2) << "blocked found " << std::hex << txn_info->txn_id << " " << lock_reqs->back().txn_info->txn_id;
                        // txn_info->blocked_segs.push_back(std::make_pair(seg_id, lock_req.txn_info->txn_id));
#ifdef BREAKDOWN_CASCADING
                        bool first = true;
                        uint32_t len = 0;
                        // for (const LockRequest& r : *lock_reqs) {
                        //     if (&r == &lock_req) {
                        //         len = 0;
                        //     }
                        //     len += 1;
                        // }
                        for (const LockRequest& r : *lock_reqs) {
                            len += 1;
                            if (r.mode == WRITE && first) {
                                len = 0;
                                first = false;
                            }
                        }
                        Logger::count(CountType::CASCADING_NUM, len);
#endif
                        not_acquire = 1;
                        break;
                    }
                }
            }
        }

        return not_acquire;
    }

    void unlock(TxnInfo* txn_info, 
            const capnp::List<Seg, capnp::Kind::STRUCT>::Reader &req_segs,
            unblock_txn_func_t unblock_txn_func) {
        for (const auto &req_seg : req_segs) {
            seg_id_t seg_id = req_seg.getSegId();
            auto r_pages = req_seg.getReadPages();
            auto w_pages = req_seg.getWritePages();
            auto &segment = segments.get_segment(seg_id);

            unlock(txn_info, segment, w_pages, true, unblock_txn_func);
            unlock(txn_info, segment, r_pages, false, unblock_txn_func);
        }
    }

    void unlock(TxnInfo* txn_info, Segment& segment,
            const capnp::List<Page, capnp::Kind::STRUCT>::Reader& pages,
            bool is_write,
            unblock_txn_func_t unblock_txn_func) {
        for (const auto &page : pages) {
            if (!page.hasRedoLog()) {
                continue;
            }
            if (is_write) {
                unlock(txn_info, segment, page, is_write, unblock_txn_func);            
            } else {
                unlock_read(txn_info, segment, page, unblock_txn_func);            
            }
        }
    }

    void unlock_read(TxnInfo* txn_info, Segment& segment, 
            const Page::Reader& page,
            unblock_txn_func_t unblock_txn_func) {
        GlobalPageId gp_id(page.getGlobalPageId());
        auto* page_locks = segment.get_page_locks(gp_id.page_id);
#ifdef PAGE_LEVEL_LOCK
        std::lock_guard<std::mutex> l(page_locks->mtx);
        unlock(txn_info, page_locks, 0, false, unblock_txn_func);
#else
        auto bytes = page.getRedoLog().asBytes();
        std::unordered_map<offset_t, bool> offset2rw;
        RedoLogReader log_reader(bytes, segment.value_size);
        std::lock_guard<std::mutex> l(page_locks->mtx);
        while (log_reader.valid()) {
            offset_t page_offset = log_reader.get_real_page_offset();
            unlock(txn_info, page_locks, page_offset, false, unblock_txn_func);
            log_reader.next();
        }
#endif
    }

    void unlock(TxnInfo* txn_info, Segment& segment, 
            const Page::Reader& page,
            bool is_write,
            unblock_txn_func_t unblock_txn_func) {
        GlobalPageId gp_id(page.getGlobalPageId());
        auto* page_locks = segment.get_page_locks(gp_id.page_id);
#ifdef PAGE_LEVEL_LOCK
        std::lock_guard<std::mutex> l(page_locks->mtx);
        unlock(txn_info, page_locks, 0, is_write, unblock_txn_func);
#else
        auto bytes = page.getRedoLog().asBytes();
        std::unordered_map<offset_t, bool> offset2rw;
        RedoLogReader log_reader(bytes, segment.value_size);
        while (log_reader.valid()) {
            offset_t page_offset = log_reader.get_real_page_offset();
            bool is_write = log_reader.is_write_entry();
            offset2rw[page_offset] |= is_write;
            log_reader.next();
        }
        
        std::lock_guard<std::mutex> l(page_locks->mtx);

        for (const auto& pair: offset2rw) {
            unlock(txn_info, page_locks, pair.first, pair.second, unblock_txn_func);
        }
#endif
    }

    void unlock(TxnInfo* txn_info, PageLocks* page_locks, uint32_t offset, bool is_write,
            unblock_txn_func_t unblock_txn_func) {        
#ifdef PAGE_LEVEL_LOCK        
        auto* lock_reqs = &page_locks->plock;
#else
        auto* lock_reqs = &page_locks->locks[offset];
#endif
        bool write_precede = false;
        std::vector<LockRequest>::iterator it;
        for (it = lock_reqs->begin();
                it != lock_reqs->end() && it->txn_info != txn_info; ++it) {
            if (it->mode == WRITE) write_precede = true;
        }

        if (it != lock_reqs->end()) {
            std::vector<LockRequest>::iterator target = it;

            ++it;
            if (it != lock_reqs->end()) {
                std::vector<TxnInfo*> new_owners;

                if (target == lock_reqs->begin() &&
                    (target->mode == WRITE ||
                    (target->mode == READ && it->mode == WRITE))) {  // (a) or (b)
                    // If a write lock request follows, grant it.
                    if (it->mode == WRITE) new_owners.push_back(it->txn_info);
                    // If a sequence of read lock requests follows, grant all of them.
                    for (; it != lock_reqs->end() && it->mode == READ; ++it)
                        new_owners.push_back(it->txn_info);
                } else if (!write_precede && target->mode == WRITE &&
                            it->mode == READ) {  // (c)
                    // If a sequence of read lock requests follows, grant all of them.
                    for (; it != lock_reqs->end() && it->mode == READ; ++it)
                        new_owners.push_back(it->txn_info);
                }

                for (auto* new_owner : new_owners) {
                    int32_t remaining = new_owner->num_blocked_locks.fetch_sub(1);
                    // LOG(2) << "new_owner: " << std::hex << new_owner->txn_id << " " << remaining;
                    if (remaining == 1) {
                        unblock_txn_func(new_owner);
                    }
                }
            }

            // Now it is safe to actually erase the target request.
            lock_reqs->erase(target);
#ifndef PAGE_LEVEL_LOCK
            if (lock_reqs->size() == 0) {
                page_locks->locks.erase(offset);
            }
#endif
        }
    }

 private:
    InMemSegments &segments;
};

#endif