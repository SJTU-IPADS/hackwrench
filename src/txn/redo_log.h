#pragma once

#include <iostream>

#include "temp_log.h"
#include "util/types.h"


// A wrapper of temp_log
// As we maintain a redo_log for each page and each page belongs to one table
// currently we assert all the key/value has the same size in one redo_log

// | ----------------------------------------------- alloc_size --------------------------------- |
// start                     current                                          end                 |
// |                         |                                                |                   |
// | ---- reserved_size ---- | --------------------- entry ------------------ |                   |
// | --------- meta -------- | -8byte(offset)-| - key_size - | - value_size - |                   |

#define READ_PAGE_OFFSET(page_offset) ((1ull << 31) | page_offset)
#define GET_READ_BIT(page_offset) (page_offset >> 31)
#define GET_OFFSET(page_offset) (0x7fffffff & page_offset)

class TxRedoLog {
   public:
    TxRedoLog(uint32_t value_size) {
        reset(value_size);
    }

    void reset(uint32_t value_size) {
        this->value_size = value_size;
        init();
    }

    void init() {
        temp_log.start();
        // index.clear();
    }

    void add_write_entry(offset_t page_offset, void *kv_ptr, version_ts_t version) {
        add_write_entry(page_offset, kv_ptr, (char *)kv_ptr + KEY_SIZE, version);
    }

    void add_read_entry(offset_t page_offset, version_ts_t version) {
        page_offset = READ_PAGE_OFFSET(page_offset);
        uint32_t total_size = sizeof(offset_t) + sizeof(version_ts_t);

        ASSERT(GET_OFFSET(page_offset) < PAGE_SIZE);
        uint8_t *ptr = temp_log.get_start_ptr();
        ptr = temp_log.append_entry(total_size);
        temp_log.close_entry();
        ASSERT(ptr >= temp_log.get_start_ptr() && ptr <= temp_log.get_current_ptr());

        *((offset_t *)ptr) = page_offset;
        ptr += sizeof(offset_t);
        memcpy(ptr, &version, sizeof(version_ts_t));
    }

    void add_write_entry(offset_t page_offset, void *k_ptr, void *v_ptr, version_ts_t version) {
        uint32_t kv_size = KEY_SIZE + value_size;
        uint32_t total_size = sizeof(offset_t) + kv_size + sizeof(version_ts_t);

        ASSERT(GET_OFFSET(page_offset) < PAGE_SIZE);
        uint8_t *ptr = temp_log.get_start_ptr();
        ptr = temp_log.append_entry(total_size);
        temp_log.close_entry();
        ASSERT(ptr >= temp_log.get_start_ptr() && ptr <= temp_log.get_current_ptr());

        *((offset_t *)ptr) = page_offset;
        ptr += sizeof(offset_t);
        memcpy(ptr, k_ptr, KEY_SIZE);
        ptr += KEY_SIZE;
        memcpy(ptr, v_ptr, value_size);
        ptr += value_size;
        memcpy(ptr, &version, sizeof(version_ts_t));
    }

    void clear() {
        temp_log.restart();
        // index.clear();
        // ASSERT(index.empty());
    }

    void freeze() {
        temp_log.end();
    }

    log_t get_log() { return temp_log.get_start_ptr(); }

    uint32_t get_log_size() const { return temp_log.get_log_size(); }

   private:
    TempLog<0> temp_log;
    uint32_t value_size;
};

struct LogMeta {
    log_t log;
    ts_t cts;
    uint32_t log_size;
    PageMeta * page_meta;
};


class RedoLogArena {
   public:
    std::queue<TxRedoLog *> arena;
    TxRedoLog *alloc(uint32_t value_size) {
        if (arena.size() == 0) {
            auto *p = new TxRedoLog(value_size);
            return p;
        } else {
            TxRedoLog *p = arena.front();
            arena.pop();
            p->reset(value_size);
            return p;
        }
    }
    void free(TxRedoLog *p) {
        p->clear();
        arena.push(p);
    }
    ~RedoLogArena() {
        while (arena.size() != 0) {
            TxRedoLog *p = arena.front();
            arena.pop();
            delete p;
        }
    }
};

thread_local RedoLogArena redoLogArena;

// Must be careful of overflow!!
void append_redo_logs(log_t log1, const unsigned char *log2, size_t size1, size_t size2) {
    memcpy(log1 + size1, log2, size2);
}

class RedoLogReader {
   private:
    log_t start, cur, end;
    uint32_t value_size;
    bool is_write = true;

   public:
    RedoLogReader(log_t log, uint32_t log_size, uint32_t value_size) : 
            start(log), cur(log), value_size(value_size) {
        end = start + log_size;
        reset();
    }
    RedoLogReader(const uint8_t *log, uint32_t log_size, uint32_t value_size)
        : RedoLogReader(reinterpret_cast<log_t>(const_cast<uint8_t *>(log)), log_size, value_size) {}
    RedoLogReader(kj::ArrayPtr<const kj::byte> bytes, uint32_t value_size)
        : RedoLogReader(reinterpret_cast<log_t>(const_cast<uint8_t *>(bytes.begin())), bytes.size(), value_size) {}

    uint32_t get_kv_size() { return KEY_SIZE + value_size; }

private:
    inline offset_t get_page_offset() const {
        return *((offset_t *)cur);
    }
public:

    inline offset_t get_real_page_offset() const {
        return GET_OFFSET(get_page_offset());
    }

    inline uint8_t *get_key_ptr() {
        return cur + sizeof(offset_t); 
    }

    template <class Key_t>
    Key_t *get_key_ptr_as() {
        return reinterpret_cast<Key_t *>(get_key_ptr());
    }

    template <class Key_t>
    Key_t& get_key_ref_as() {
        return *reinterpret_cast<Key_t *>(get_key_ptr());
    }

    inline uint32_t get_value_size() const { return value_size; } 

    inline uint8_t *get_value_ptr() {
        return cur + sizeof(offset_t) + KEY_SIZE; 
    }

    template <class Value_t>
    Value_t *get_value_ptr_as() {
        return reinterpret_cast<Value_t *>(get_value_ptr());
    }

    version_ts_t get_version() {
        if (is_write) {
            return *reinterpret_cast<version_ts_t *>(cur + sizeof(offset_t) + KEY_SIZE +
                                                    value_size);
        } else {
            return *reinterpret_cast<version_ts_t *>(cur + sizeof(offset_t));
        }
    }

    bool is_write_entry() const { return is_write; }

    bool valid() { 
        if (cur >= end){
            return false;
        }
        is_write = (GET_READ_BIT(get_page_offset()) == 0);
        return true; 
    }

    void next() {
        if (is_write) {
            cur += sizeof(offset_t) + KEY_SIZE + value_size + sizeof(version_ts_t);
        } else {
            cur += sizeof(offset_t) + sizeof(version_ts_t);
        }
    }

    void reset() { cur = start; }
};
