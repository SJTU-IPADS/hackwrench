#pragma once

#include <cstring>

#include "servers/config.h"

// An append-only log, with auto-increasing size
// | ----------------------------------------------- alloc_size --------------------------------- |
// start                     current                         end                                  |
// |                         |                               |                                    |
// | ---- reserved_size ---- | ----------- entry ----------- |                                    |
template <uint32_t RESERVED_SIZE = 0, uint32_t BASIC_MEM_ALLOC_SIZE = 2048>
class TempLog {
   public:
    bool in_use_;

    // start of the memory of temp log
    uint8_t *start_, *current_, *end_;

    uint32_t alloced_size_;

    TempLog()
        : in_use_(false), start_(nullptr), current_(nullptr), end_(nullptr), alloced_size_(0) {
        alloced_size_ = BASIC_MEM_ALLOC_SIZE;
        start_ = (uint8_t *)malloc(alloced_size_);
        end_ = current_ = get_reserved_ptr();

        // alignment assert
        // ASSERT(RESERVED_SIZE % ROUND_UP_BASE == 0);
    }

    ~TempLog() { free(start_); }

    void start() {
        ASSERT(in_use_ == false);

        end_ = current_ = get_reserved_ptr();
        in_use_ = true;
    }

    void end() {
        ASSERT(in_use_ = true);
        in_use_ = false;
    }

    void restart() {
        in_use_ = false;
        end_ = current_ = get_reserved_ptr();
    }

    inline uint8_t *append_entry(uint32_t size) {
        ASSERT(current_ == end_);
        resize(size);
        end_ += size;
        return current_;
    }

    inline void close_entry() { current_ = end_; }

    void resize(uint32_t size) {
        bool need_resize = false;
        uint32_t log_size = get_log_size();
        while (log_size + size > alloced_size_) {
            alloced_size_ = alloced_size_ << 1;
            need_resize = true;
        }
        if (need_resize) {
            uint8_t *new_start = (uint8_t *)malloc(alloced_size_);
            uint32_t old_size = log_size;
            memcpy(new_start, start_, old_size);
            free(start_);
            start_ = new_start;
            end_ = current_ = start_ + old_size;
        }
    }

    inline uint8_t *get_start_ptr() { return start_; }

    inline uint8_t *get_current_ptr() { return current_; }

    inline uint8_t *get_reserved_ptr() { return start_ + RESERVED_SIZE; }

    inline uint32_t get_log_size() const { return (uint32_t)(end_ - start_); }
};
