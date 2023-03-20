#!/bin/bash
./eval/10_micro_cache_effect.py aws ycsb10 6 hackwrench_occ_non_caching
./eval/10_micro_cache_effect.py aws ycsb10 7 hackwrench_normal_non_caching
./eval/10_micro_cache_effect.py aws ycsb10 8 hackwrench_fast_non_caching
