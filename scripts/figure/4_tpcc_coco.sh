#!/bin/bash
./eval/4_tpcc_coco.py aws tpcc 0 hackwrench_coco
./eval/4_tpcc_coco.py aws tpcc 1 hackwrench_coco_normal
./eval/4_tpcc_coco.py aws tpcc 2 hackwrench_coco_fast
