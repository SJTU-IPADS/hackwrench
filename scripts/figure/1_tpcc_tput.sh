#!/bin/bash
./eval/1_tpcc_tput_lat.py aws tpcc 0 hackwrench_occ_non_caching
./eval/1_tpcc_tput_lat.py aws tpcc 1 hackwrench_occ
./eval/1_tpcc_tput_lat.py aws tpcc 2 hackwrench
./eval/1_tpcc_tput_lat.py aws tpcc 3 hackwrench_normal
./eval/1_tpcc_tput_lat.py aws tpcc 4 hackwrench_fast