#!/bin/bash
./eval/3_factor_analysis.py aws tpcc 0 hackwrench_pure_occ_non_caching
./eval/3_factor_analysis.py aws tpcc 1 hackwrench_pure_occ
./eval/3_factor_analysis.py aws tpcc 2 hackwrench_occ_non_caching
./eval/3_factor_analysis.py aws tpcc 3 hackwrench_occ
./eval/3_factor_analysis.py aws tpcc 4 hackwrench_batch_abort_committed
./eval/3_factor_analysis.py aws tpcc 5 hackwrench_batch_abort
./eval/3_factor_analysis.py aws tpcc 6 hackwrench_batch_abort_0
./eval/3_factor_analysis.py aws tpcc 7 hackwrench_ts
./eval/3_factor_analysis.py aws tpcc 8 hackwrench
./eval/3_factor_analysis.py aws tpcc 9 hackwrench_normal
./eval/3_factor_analysis.py aws tpcc 10 hackwrench_fast
./eval/3_factor_analysis.py aws tpcc 11 hackwrench_normal_to
./eval/3_factor_analysis.py aws tpcc 12 hackwrench_fast_to

