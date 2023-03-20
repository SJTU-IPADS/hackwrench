#include <iostream>

#if defined(BENCH_TPCC)
#include "benchmark/tpcc/tpcc.hpp"
#endif

uint64_t nthreads = 1;

int main(int argc, char** argv) {
  do_bench(argc, argv);

  return 0;
}