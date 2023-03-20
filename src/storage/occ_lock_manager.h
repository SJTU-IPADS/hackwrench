#pragma once

#include "servers/config.h"

#ifdef NO_TS

#include <unordered_map>

#include "txn/redo_log.h"
#include "storage.h"
#include "txn_info.h"

class OCCLockManager {
    friend class Segment;
   public:
    OCCLockManager(InMemSegments &segments) : segments(segments) {}

#ifdef FINE_AURORA
    bool aurora_lock(TxnInfo *txn_info, const Seg::Reader &seg) {
        auto r_pages = seg.getReadPages();
        auto w_pages = seg.getWritePages();
        seg_id_t seg_id = seg.getSegId();
        auto &segment = segments.get_segment(seg_id);
        int locked_page_cnt = 0;
        for (const auto &page : r_pages) {
            if (!occ_fine_lock_page(txn_info, segment, page)) {
                goto failed;
            } else {
                ++locked_page_cnt;
            }
        }
        for (const auto &page : w_pages) {
            if (!occ_fine_lock_page(txn_info, segment, page)) {
                goto failed;
            } else {
                ++locked_page_cnt;
            }
        }
        return true;
failed:
        for (const auto &page : r_pages) {
            if (locked_page_cnt-- > 0) {
                occ_fine_unlock_page(txn_info, segment, page);
            } else {
                return false;
            }
        }
        for (const auto &page : w_pages) {
            if (locked_page_cnt-- > 0) {
                occ_fine_unlock_page(txn_info, segment, page);
            } else {
                return false;
            }
        }
        return false;
    }

    bool occ_fine_lock_page(TxnInfo *txn_info, Segment& segment, const Page::Reader &page) {
        if (!page.hasRedoLog()) {
            return true;
        }
        return occ_fine_lock_page_internal(txn_info, segment, page);
    }

    bool occ_fine_lock_page_internal(TxnInfo *txn_info, Segment& segment, const Page::Reader &page) {
        GlobalPageId gp_id(page.getGlobalPageId());
        auto *record_locks = segment.get_page_record_locks(gp_id.page_id);
        auto bytes = page.getRedoLog().asBytes();
        RedoLogReader log_reader(bytes, segment.value_size);
        std::vector<offset_t> locked_records;
        std::lock_guard<std::mutex> guard(segment.rl_mu[gp_id.page_id]);
        while (log_reader.valid()) {
            offset_t page_offset = log_reader.get_real_page_offset();
            if (occ_fine_lock_record(txn_info->txn_id, record_locks->operator[](page_offset))) {
                locked_records.push_back(page_offset);
            } else {
                for (auto page_offset : locked_records) {
                    occ_fine_unlock_record(txn_info->txn_id, record_locks->operator[](page_offset));
                }
                return false;
            }
            log_reader.next();
        }
        return true;
    }

    void aurora_unlock(TxnInfo *txn_info, const Seg::Reader &seg) {
        auto r_pages = seg.getReadPages();
        auto w_pages = seg.getWritePages();
        seg_id_t seg_id = seg.getSegId();
        auto &segment = segments.get_segment(seg_id);
        for (const auto &page : r_pages) {
            occ_fine_unlock_page(txn_info, segment, page);
        }
        for (const auto &page : w_pages) {
            occ_fine_unlock_page(txn_info, segment, page);
        }
    }

    void occ_fine_unlock_page(TxnInfo *txn_info, Segment& segment, const Page::Reader &page) {
        if (!page.hasRedoLog()) {
            return;
        }
        GlobalPageId gp_id(page.getGlobalPageId());
        auto *record_locks = segment.get_page_record_locks(gp_id.page_id);
        auto bytes = page.getRedoLog().asBytes();
        RedoLogReader log_reader(bytes, segment.value_size);
        std::lock_guard<std::mutex> guard(segment.rl_mu[gp_id.page_id]);
        while (log_reader.valid()) {
            offset_t page_offset = log_reader.get_real_page_offset();
            occ_fine_unlock_record(txn_info->txn_id, record_locks->operator[](page_offset));
            log_reader.next();
        }
    }

    bool occ_fine_lock_record(txn_id_t txn_id, std::atomic<txn_id_t> &locked) {
        if (locked.load() == txn_id) {
            return true;
        }
        txn_id_t holder = 0;
        uint64_t try_times = 0;
        while (!locked.compare_exchange_weak(holder, txn_id)) {
            if (++try_times > 1000) {
                return false;
            }
            holder = 0;
        }
        return true;
    }

    void occ_fine_unlock_record(txn_id_t txn_id, std::atomic<txn_id_t> &locked) {
        if (locked.load() != txn_id)
            return;
        locked.store(0);
    }
    
#else
        
    bool aurora_lock(TxnInfo *txn_info, const Seg::Reader &seg) {
        seg_id_t seg_id = seg.getSegId();
        auto &segment = segments.get_segment(seg_id);
        for (const auto &page : seg.getReadPages()) {
            if (!page.hasRedoLog()) {
                continue;
            }
            GlobalPageId gp_id(page.getGlobalPageId());
            bool success = lock(txn_info, segment, gp_id.page_id);
            if (!success) {
                return false;
            }
        }
        for (const auto &page : seg.getWritePages()) {
            if (!page.hasRedoLog()) {
                continue;
            }
            GlobalPageId gp_id(page.getGlobalPageId());
            bool success = lock(txn_info, segment, gp_id.page_id);
            if (!success) {
                return false;
            }
        }
        return true;
    }

    bool lock(TxnInfo *txn_info, Segment& seg, page_id_t page_id) {
        txn_id_t txn_id = txn_info->txn_id;
        auto& lock = seg.aurora_locked[page_id];
        if (lock.load() == txn_id) {
            return true;    
        }
        txn_id_t holder = 0;
        uint64_t try_times = 0;
        while (!lock.compare_exchange_weak(holder, txn_id)) {
            if (++try_times > 1000) {
                // LOG(3) << "Txn_ID: " << std::hex << txn_info->txn_id << " seg_id: " << std::dec << seg_id << " holder_id: " << std::hex << holder;
                return false;
            }
            holder = 0;
        }
        return true;
    }

    void aurora_unlock(TxnInfo *txn_info, const Seg::Reader &seg) {
        seg_id_t seg_id = seg.getSegId();
        auto &segment = segments.get_segment(seg_id);
        for (const auto &page : seg.getReadPages()) {
            if (!page.hasRedoLog()) {
                continue;
            }
            GlobalPageId gp_id(page.getGlobalPageId());
            bool success = unlock(txn_info, segment, gp_id.page_id);
            if (!success) {
                return;
            }
        }
        for (const auto &page : seg.getWritePages()) {
            if (!page.hasRedoLog()) {
                continue;
            }
            GlobalPageId gp_id(page.getGlobalPageId());
            bool success = unlock(txn_info, segment, gp_id.page_id);
            if (!success) {
                return;
            }
        }
    }

    bool unlock(TxnInfo *txn_info, Segment& seg, page_id_t page_id) {
        txn_id_t txn_id = txn_info->txn_id;
        auto& lock = seg.aurora_locked[page_id];
        if (lock.load() != txn_id)
            return false;
        lock.store(0);
        return true;
    }
#endif

   private:
   InMemSegments &segments;

};

#endif