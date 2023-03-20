#pragma once

#include "timer.h"

#include <cstdint>

#define STATICS 0

#if STATICS == 1
// Performance counting stats
// To be more self-contained
inline __attribute__((always_inline)) uint64_t db_rdtsc(void) {
    uint32_t hi, lo;
    __asm volatile("rdtsc" : "=a"(lo), "=d"(hi));
    return ((uint64_t)lo) | (((uint64_t)hi) << 32);
}

#define LAT_VARS(X)                  \
    uint64_t _##X##_cycles_ = 0;     \
    uint64_t _pre_##X##_cycles_ = 0; \
    uint64_t _##X##count_ = 0;       \
    uint64_t _pre_##X##count_ = 0;   \
    uint64_t _##X##start = 0;

#define INIT_LAT_VARS(X) \
    _##X##_cycles_ = 0, _pre_##X##_cycles_ = 0, _##X##count_ = 0, _pre_##X##count_ = 0;

#define START(X) _##X##start = db_rdtsc();

#define END(X)                                      \
    if (_##X##start != 0) {                         \
        _##X##_cycles_ += db_rdtsc() - _##X##start; \
        _##X##count_ += 1;                          \
    }

#define REPORT(X)                                                                \
    {                                                                            \
        auto counts = _##X##count_ - _pre_##X##count_;                           \
        counts = counts == 0 ? 1 : counts;                                       \
        auto temp = _##X##_cycles_;                                              \
        auto total_time = (temp - _pre_##X##_cycles_) / CYCLES_PER_NS;           \
        auto latency = total_time / (double)counts;                              \
        LOG(2) << "(" << #X << "):" << VAR2(latency, "ns,") << VAR2(counts, ",") \
               << VAR2(total_time, "ns");                                        \
        _pre_##X##count_ = _##X##count_;                                         \
        _pre_##X##_cycles_ = temp;                                               \
    }

#else

#define LAT_VARS(X) ;
#define INIT_LAT_VARS(X) ;
#define START(X) ;
#define END(X) ;
#define END_C(C, X) ;
#define REPORT(X) ;

#endif
