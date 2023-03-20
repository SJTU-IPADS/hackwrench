#pragma once

#include <thread>
#include <unordered_map>
#include <vector>

#include "servers/config.h"
#include "rpc/SNinterface.capnp.h"
#include "util/macros.h"
#include "segment.h"

class InMemSegments {
   public:
    const Configuration &conf;
    InMemSegments(const Configuration &conf) : conf(conf) {
        segments = (Segment **)malloc(sizeof(Segment *) * conf.numSegments());
        for (seg_id_t seg_id = 0; seg_id < conf.numSegments(); ++seg_id) {
            if (conf.isMySeg(seg_id)) {
                segments[seg_id] = new Segment(seg_id);
            } else {
                segments[seg_id] = nullptr;
            }
        }
    }

    ~InMemSegments() {}

    PageMeta *try_alloc_page(seg_id_t seg_id, page_id_t page_id) {
        auto &segment = get_segment(seg_id);
        if (segment.page_is_free(page_id)) {
            PageMeta *page_meta = segment.acquire_page();
            ASSERT(page_meta->gp_id.page_id == page_id);

            return page_meta;
        } else {
            ASSERT(false);
            return nullptr;
        }
    }

    PageMeta *alloc_next_page(seg_id_t seg_id) {
        auto &segment = get_segment(seg_id);
        PageMeta *page_meta = segment.acquire_page();
        return page_meta;
    }

    bool setPage(seg_id_t seg_id, page_id_t page_id, ts_t cts, const uint8_t *data) {
        auto &segment = get_segment(seg_id);
        auto page_meta = segment.get_page_meta(page_id);

        if (page_meta == nullptr) {
            // pages[page_id] = std::make_unique<SNPage>(page_id, cts, data);
            // TODO: should we use it as insert?
            LOG(FATAL) << "No such page (page_id=" << page_id << ")\n";
            return false;
        } else {
            page_meta->copy_data(data);
            page_meta->set_cts(cts);
            ASSERT(page_meta->gp_id.page_id == page_id);
            return true;
        }
    }

    PageMeta *get_page_meta(GlobalPageId gp_id) {
        auto &segment = get_segment(gp_id.seg_id);
        return segment.get_page_meta(gp_id.page_id);
    }

    void finalize_value_size() {
        for (seg_id_t seg_id = 0; seg_id < conf.numSegments(); ++seg_id) {
            Segment* seg = segments[seg_id];
            if (seg != nullptr && seg->cur_page_id != 0) {
                seg->value_size = seg->get_page_meta(0)->get_value_size();
            }
        }
    }

    inline Segment &get_segment(seg_id_t seg_id) {
        return *segments[seg_id];
    }

    inline Segment **&get_segments() { return segments; }

   private:
    // std::unordered_map<seg_id_t, std::unique_ptr<Segment>> segments;
    Segment **segments;
};