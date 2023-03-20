#pragma once
/**
 * Page structure (Format)
 */

#include <stdint.h>

#include <cstring>
#include <iostream>
#include <mutex>

#include "util/types.h"
#include "txn/page_snapshot.h"

static const page_id_t INVALID_PAGE_ID = -1;
struct PageMeta {
    GlobalPageId gp_id;
    page_size_t cur_page_size;
    page_size_t max_page_size;
    bool need_async_update = false;

   private:
    uint8_t *data = nullptr;

   public:
    std::mutex mutex;

    // TODO: split DbPageMeta and SnPageMeta
    void *reserved_ptr;  // used for different usage on different type of server
                         // LogSequence for storage node

   public:
    void set_data(uint8_t *d) { this->data = d; }

    inline uint8_t *get_data() const { return data; }

    void copy_data(const uint8_t *that, uint64_t len = PAGE_SIZE) { memcpy(this->data, that, len); }

    bool operator==(const PageMeta &other) {
        return (this->gp_id.g_page_id == other.gp_id.g_page_id) &&
               (memcmp(this->data, other.data, PAGE_SIZE) == 0);
    }

    static uint8_t *new_data() { return new uint8_t[PAGE_SIZE]; }

    // the first 64 bits of data is global timestamp, refer to multi_ver_record.h
    inline static ts_t get_data_ts(uint8_t *data) { return *((ts_t *)data); }
    inline static void set_data_ts(uint8_t *data, ts_t ts) { *((ts_t *)data) = ts; }
    inline static uint32_t get_value_size(uint8_t *data) { return *((ts_t *)(data+sizeof(ts_t))); }
    inline static void set_value_size(uint8_t *data, uint32_t value_size) { 
        *((ts_t *)(data+sizeof(ts_t))) = value_size; 
    }
    inline ts_t get_cts() const { return get_data_ts(data); }
    inline ts_t get_value_size() const { return get_value_size(data); }
    inline void set_cts(ts_t ts) {
        ts = std::max(ts, get_data_ts(data));
        set_data_ts(data, ts);
    }

    inline offset_t get_offset(void* ptr) {
        return reinterpret_cast<uint64_t>(ptr) - reinterpret_cast<uint64_t>(data);
    }

    void require_async_update() {
        need_async_update = true;
    }

    ~PageMeta() {
        if (data)
            delete[] data;
    }
};
