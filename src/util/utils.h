#pragma once

#include <execinfo.h>
#include <stdint.h>
#include <stdlib.h>

#define likely(x) __builtin_expect(!!(x), 1)
#define unlikely(x) __builtin_expect(!!(x), 0)

inline bool ptr_aligned_to(void *ptr, uint64_t size) { return ((uint64_t)ptr) % size == 0; }

inline constexpr size_t round_up_pow_of_2(size_t v) {
    v--;
    v |= v >> 1;
    v |= v >> 2;
    v |= v >> 4;
    v |= v >> 8;
    v |= v >> 16;
    v++;
    return v;
}

inline constexpr size_t round_up_mult_of_4(size_t v) { return (v + 3) & ~0x03; }

template <class T>
inline T round_up(const T &numToRound, const T &multiple) {
    T remainder = numToRound % multiple;
    if (remainder == 0)
        return numToRound;
    return numToRound + multiple - remainder;
}

template <class T>
inline T min(T a, T b) {
    return a < b ? a : b;
}

template <class T>
inline T max(T a, T b) {
    return a > b ? a : b;
}

void print_trace(void) {
    char **strings;
    size_t i, size;
    enum Constexpr { MAX_SIZE = 1024 };
    void *array[MAX_SIZE];
    size = backtrace(array, MAX_SIZE);
    strings = backtrace_symbols(array, size);
    for (i = 0; i < size; i++)
        printf("%s\n", strings[i]);
    puts("");
    free(strings);
}