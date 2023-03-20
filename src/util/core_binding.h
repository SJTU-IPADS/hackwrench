#pragma once

#include <sched.h>

#include <array>
#include <cstdint>
#include <iostream>

class CPUBinder {
   public:
    static void bind(std::thread::native_handle_type thread_handle, uint64_t core) {
#ifndef BIND_CORES
        return;
#endif
        core = core % std::thread::hardware_concurrency();
        cpu_set_t cpuset;
        CPU_ZERO(&cpuset);
        CPU_SET(core, &cpuset);
        int rc = pthread_setaffinity_np(thread_handle, sizeof(cpu_set_t), &cpuset);
        if (rc != 0) {
            std::cout << "error" << std::endl;
            exit(-1);
        }
    }
};