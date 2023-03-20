useBinary(){
    rm -rf ../build
    cp -r ../${1} ../build
}

cleanResult(){
    rm -rf aws/results
    mkdir aws/results
}

mkdir finalV2-results

# 1. ycsb ratio
cleanResult
useBinary hw-build
python3 finalV2/1_ycsb_ratio.py aws ycsb10 0
mv aws/results finalV2-results/1_ycsb_ratio_results

# 2-1. ycsb batch effect with contention free
cleanResult
useBinary hw-build
python3 finalV2/2_ycsb_batch_effect.py aws ycsb10 0
mv aws/results finalV2-results/2-1_ycsb_batch_effect_contention_free

# 2-2. ycsb batch effect with uniform
cleanResult
useBinary hw-build
python3 finalV2/2_ycsb_batch_effect.py aws ycsb10 101
mv aws/results finalV2-results/2-2_ycsb_batch_effect_uniform

# 3-1. ycsb cache effect with contention free
cleanResult
useBinary hw-build
python3 finalV2/3_ycsb_cache_effect.py aws ycsb10 0
mv aws/results finalV2-results/3-1_ycsb_cache_effect_contention_free

# 3-2. ycsb cache effect with uniform
cleanResult
useBinary hw-build
python3 finalV2/3_ycsb_cache_effect.py aws ycsb10 101
mv aws/results finalV2-results/3-2_ycsb_cache_effect_uniform

# 4-1. ycsb contention with hw
cleanResult
useBinary hw-build
python3 finalV2/4_ycsb_contention.py aws ycsb10 0
mv aws/results finalV2-results/4-1_ycsb_contention_hw

# 4-2. ycsb contentino with occ
cleanResult
useBinary occ-build
python3 finalV2/4_ycsb_contention.py aws ycsb10 1
mv aws/results finalV2-results/4-2_ycsb_contention_occ

# 5. tpcc ratio
cleanResult
useBinary hw-build
python3 finalV2/5_tpcc_ratio.py aws tpcc 0
mv aws/results finalV2-results/5_tpcc_ratio

# 6. tpcc scalability
cleanResult
useBinary hw-build
python3 finalV2/6_tpcc_scalability.py aws tpcc 0
mv aws/results finalV2-results/6_tpcc_scalability

# 7-1. tpcc tput-lat hw
cleanResult
useBinary hw-build
python3 finalV2/7_tpcc_tput_lat.py aws tpcc 0
mv aws/results finalV2-results/7_tpcc_tput_lat_hw_batch

# 7-2. tpcc tput-lat occ
cleanResult
useBinary occ-build
python3 finalV2/7_tpcc_tput_lat.py aws tpcc 1
mv aws/results finalV2-results/7_tpcc_tput_lat_occ

# 8-1. tpcc abort
cleanResult
useBinary abort-build
python3 finalV2/8_abort_repair.py aws tpcc 0
mv aws/results finalV2-results/8_tpcc_abort

# 8-2. tpcc nfc
cleanResult
useBinary nfc-build
python3 finalV2/8_abort_repair.py aws tpcc 0
mv aws/results finalV2-results/8_tpcc_nfc

# 9. tpcc replication affect
cleanResult
useBinary hw-build
python3 finalV2/9_tpcc_replication_effect.py aws tpcc 0
mv aws/results finalV2-results/9_tpcc_replication_effect

# 10-1. occ ycsb batch effect
cleanResult
useBinary occ-build
python3 finalV2/10_occ_batch_effect.py aws ycsb10 0
mv aws/results finalV2-results/10_occ_batch_effect_ycsb

# 10-2. occ tpcc batch effect
cleanResult
useBinary occ-build
python3 finalV2/10_occ_batch_effect.py aws tpcc 0
mv aws/results finalV2-results/10_occ_batch_effect_tpcc
