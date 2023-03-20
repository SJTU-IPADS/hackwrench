#pragma once

#include <map>

#include "page.h"
#include "rpc/SNinterface.capnp.h"
#include "servers/config.h"

class PageManager {
    static const uint64_t MAX_PAGE_NUM = 1024 * 1024;

    struct Seg {
        PageMeta *page_metas[NUM_PAGES_PER_SEGMENT];
        Seg() {
            memset(page_metas, 0, sizeof(PageMeta*) * NUM_PAGES_PER_SEGMENT);
        }
    };

   public:
    PageManager(const Configuration &conf)
        : conf(conf), num_segs(conf.numSegments()) {
        segs = new Seg[num_segs];
    }

    ~PageManager() {
        for (seg_id_t seg_id = 0; seg_id < num_segs; ++seg_id) {
            Seg& seg = segs[seg_id];
            for (PageMeta* page_meta : seg.page_metas) {
                if(page_meta) {
                    delete page_meta;
                }
            }
        }
        delete [] segs;
        // for (uint i = 0; i < num_pages; ++i) {
        //     PageMeta *p = page_cache[i];
        //     if (p)
        //         delete p;
        // }

        // delete[] page_cache;
    }

    PageMeta *optimistic_alloc_new_page(seg_id_t seg_id) {
        // This API does not add the data to multi version page, as it does not know the ts
        // The caller should do so
        auto &seg_info = local_seg_infos[seg_id];

        PageMeta *new_page_meta = get_new_page_meta();
        new_page_meta->gp_id.seg_id = seg_id;
        new_page_meta->gp_id.page_id = seg_info.next_page_id++;
        return new_page_meta;
    }

    inline PageMeta *get_page_from_cache(g_page_id_t g_page_id) {
        GlobalPageId gp_id(g_page_id);
        return get_page_from_cache(gp_id.seg_id, gp_id.page_id);
    }

    inline PageMeta *get_page_from_cache(seg_id_t seg_id, page_id_t page_id) {
        Seg& seg = segs[seg_id];
        PageMeta* ret = seg.page_metas[page_id];
        ASSERT(ret != nullptr) << std::hex << seg_id << " " << page_id;
        return ret;
    }

    inline PageMeta *try_get_page_from_cache(g_page_id_t g_page_id) {
        GlobalPageId gp_id(g_page_id);
        return try_get_page_from_cache(gp_id.seg_id, gp_id.page_id);
    }

    inline PageMeta *try_get_page_from_cache(seg_id_t seg_id, page_id_t page_id) {
        Seg& seg = segs[seg_id];
        return seg.page_metas[page_id];
    }

    PageMeta *init_page_cache(g_page_id_t g_page_id) {
        PageMeta *new_page_meta = get_new_page_meta();
        new_page_meta->gp_id.g_page_id = g_page_id;
        new_page_meta->cur_page_size = 0;
        new_page_meta->set_cts(0);

        GlobalPageId gp_id(g_page_id);
        Seg& seg = segs[gp_id.seg_id];
        seg.page_metas[gp_id.page_id] = new_page_meta;

        // page_cache[g_page_id] = new_page_meta;
        // LOG(2) << new_page_meta->gp_id.g_page_id << " " << new_page_meta;

        // try_update_max_page_id(new_page_meta->gp_id.seg_id, new_page_meta->gp_id.page_id);
        return new_page_meta;
    }

    PageMeta *set_page_cache(MultiLogPage::Reader page) {
        g_page_id_t g_page_id = page.getGlobalPageId();
        PageMeta *page_meta = get_page_from_cache(g_page_id);
        ASSERT(page_meta->gp_id.g_page_id == g_page_id);

        auto array = page.getData();
        auto byte_array = array.asBytes();
        uint8_t *data = const_cast<uint8_t *>(byte_array.begin());
        page_meta->cur_page_size = byte_array.size();

        auto* meta = reinterpret_cast<LeafMeta* >(data);
        auto* cache_meta = reinterpret_cast<LeafMeta* >(page_meta->get_data());
        ASSERT(meta->value_size == cache_meta->value_size);
        for (slot_t i = 0; i < meta->max_leaf_slots; ++i) {
            uint8_t* pair = meta->get_record_ptr(meta->get_offset(i));
            uint8_t* cache_pair = cache_meta->get_record_ptr(cache_meta->get_offset(i));
            r_copy(cache_pair, pair, meta->value_size);
        }

        // new_page_meta->mutli_version_data->put_data(page.getCts(), new_page_meta->get_data());
        // try_update_max_page_id(page_meta->gp_id.seg_id, page_meta->gp_id.page_id);
        return page_meta;
    }

    void set_page_cache(PageMeta *page_meta) {
        GlobalPageId gp_id(page_meta->gp_id.g_page_id);
        Seg& seg = segs[gp_id.seg_id];
        seg.page_metas[gp_id.page_id] = page_meta;
        
        // page_cache[page_meta->gp_id.g_page_id] = page_meta;

        // try_update_max_page_id(page_meta->gp_id.seg_id, page_meta->gp_id.page_id);
    }

    const Configuration &get_conf() const { return conf; }

   private:
    const Configuration &conf;

   private:
    inline PageMeta *get_new_page_meta() {
        PageMeta *new_page_meta = new PageMeta;
        new_page_meta->set_data(PageMeta::new_data());
        memset(new_page_meta->get_data(), 0, PAGE_SIZE);

        return new_page_meta;
    };

    // inline void try_update_max_page_id(seg_id_t seg_id, page_id_t page_id) {
    //     return;
    //     auto &seg_info = local_seg_infos[seg_id];
    //     if (seg_info.next_page_id <= page_id) {
    //         seg_info.next_page_id = page_id + 1;
    //     }
    // }

    struct LocalSegInfo {
        page_id_t next_page_id = 0;
    };
    page_id_t cur_page_id = 1;  // NOTE: page id starts from 1
    std::map<seg_id_t, LocalSegInfo> local_seg_infos;

    Seg* segs;
    uint32_t num_segs;
};