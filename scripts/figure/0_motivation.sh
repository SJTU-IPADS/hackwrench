#!/bin/bash
./eval/0_motivation_ycsb.py aws ycsb10 0 hackwrench_occ_non_caching
./eval/0_motivation_ycsb.py aws ycsb10 1 hackwrench_occ
./eval/0_motivation_ycsb.py aws ycsb10 2 hackwrench_batch_abort

