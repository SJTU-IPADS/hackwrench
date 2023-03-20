#!/usr/bin/env python3
import basic_run
import sys
from datetime import datetime
from get_result import get_test_result

def config_by_workload(workload):
    if basic_run.contention_factor == 0:
        basic_run.contention_factor1 = 0
    else:
        basic_run.contention_factor1 = 15
    if workload == "tpcc":
        if basic_run.bin_file_name == "hackwrench_coco_fast":
            basic_run.num_of_clients = 900
        else:
            basic_run.num_of_clients = 600
        basic_run.optional = 8
        basic_run.num_of_warehouses = basic_run.num_of_nodes["DataBase"] * basic_run.optional
        basic_run.num_of_tables = basic_run.num_tables_per_warehouse * basic_run.num_of_warehouses
    else:
        assert False

def run_one_line(data_dir, workload, option):
    trial_name = datetime.now().strftime("%m-%d-%H-%M-%S")
    choice = [0, 10, 20, 30, 40, 50, 60,70,80,90, 100, 150, 200, 250]
    for x in choice:
        # basic_run.split_num = x
        # basic_run.batch_size = x
        basic_run.contention_factor = x
        config_by_workload(workload)
        basic_run.run_test(
            data_dir,
            workload,
            basic_run.num_of_tables,
            basic_run.num_of_clients,
            basic_run.contention_factor,
            basic_run.batch_size,
            basic_run.split_num,
            basic_run.optional,
            basic_run.cache_miss_ratio,
            basic_run.sn_replication,
            f'{trial_name}/{basic_run.num_of_nodes["DataBase"]}nodes-{x}clients'
        )

    result_of_line = get_test_result(
        f"{data_dir}/logs/{trial_name}",
        f"results/{choice}-{workload}-{option}.data",
    )
    return result_of_line

if __name__ == "__main__":
    data_dir = sys.argv[1]
    workload = sys.argv[2]
    option = int(sys.argv[3])
    if len(sys.argv) == 5:
        basic_run.bin_file_name = sys.argv[4]

    basic_run.prepare_binaries(data_dir)
    basic_run.num_of_nodes["DataBase"] = 6
    basic_run.num_of_nodes["StorageNode"] = 12
    basic_run.num_of_threads["StorageNode"] = 8
    basic_run.num_of_threads["DataBase"] = 8
    basic_run.sn_replication = True

    choice = [100]
    results = []
    for x in choice:
        basic_run.split_num = x
        basic_run.batch_size = x
        result = str(basic_run.run_one_line_wrapper(run_one_line, data_dir, workload, option))
        results.append(result)

    lines = basic_run.get_res_lines(results)
    csv_res = basic_run.get_csv_res(choice, lines)
    basic_run.write_to(f'{data_dir}/results/{option}.csv', csv_res)
