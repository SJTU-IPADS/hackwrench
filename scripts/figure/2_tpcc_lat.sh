#!/bin/bash
./eval/2_tpcc_lat.py aws tpcc 0 hackwrench_occ_non_caching_lat
./eval/2_tpcc_lat.py aws tpcc 1 hackwrench_occ_lat
./eval/2_tpcc_lat.py aws tpcc 2 hackwrench_normal_lat
./eval/2_tpcc_lat.py aws tpcc 11 hackwrench_normal_lat
./eval/2_tpcc_lat.py aws tpcc 3 hackwrench_fast_lat
./eval/2_tpcc_lat.py aws tpcc 12 hackwrench_fast_lat
