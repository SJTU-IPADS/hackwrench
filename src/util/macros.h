#pragma once

#include <assert.h>
#include <iostream>

#define CACHE_LINE_SIZE 64
#define COMPILER_MEMORY_FENCE() asm volatile("" ::: "memory")
#define CPU_MEMORY_FENCE() __sync_synchronize()
#define CACHE_PADOUT char __padout__COUNTER__[0] __attribute__((aligned(CACHE_LINE_SIZE)))
