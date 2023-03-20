#!/bin/bash
./eval/7_micro_contention_sundial.py aws ycsb10 0 hackwrench_occ
./eval/7_micro_contention_sundial.py aws ycsb10 1 hackwrench_normal
./eval/7_micro_contention_sundial.py aws ycsb10 2 hackwrench_fast
./eval/7_micro_contention.py aws ycsb10 3 hackwrench_occ
./eval/7_micro_contention.py aws ycsb10 4 hackwrench_normal
./eval/7_micro_contention.py aws ycsb10 5 hackwrench_fast
