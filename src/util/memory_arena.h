#pragma once
#include <cstring>

#include "servers/config.h"
#include "util/utils.h"

template <class T, size_t ArenaSize = 1000>
class MemoryArena {
    static const size_t object_size = round_up_mult_of_4(sizeof(T));

   private:
    char arena[ArenaSize * object_size];
    size_t next = 0;

   public:
    MemoryArena() {}
    inline T *alloc() {
        ASSERT(next < ArenaSize);
        T *p = (T *)&arena[(next++) * object_size];
        p = new (p) T();
        return p;
    }
    inline void clear() { next = 0; };
};

template <class T>
class DynamicArena {
   private:
    std::queue<T *> arenas[16];

   public:
    T *alloc(thread_id_t thread_id) {
        std::queue<T *> &arena = arenas[thread_id];
        if (arena.empty()) {
            T *p = new T;
            memset(p, 0, sizeof(*p));
            return p;
        } else {
            T *p = arena.front();
            arena.pop();
            return p;
        }
    }

    void free(T *p, thread_id_t thread_id) {
        std::queue<T *> &arena = arenas[thread_id];
        arena.push(p);
    }
};