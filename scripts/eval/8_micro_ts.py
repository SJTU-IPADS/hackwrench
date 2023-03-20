#!/usr/bin/env python3
import basic_run
import sys
from datetime import datetime
from get_result import get_test_result

def config_by_workload(workload):
    if workload == "ycsb10":
        basic_run.num_of_tables = 12000
        basic_run.num_of_clients = 1000
        if basic_run.bin_file_name == "hackwrench_occ":
            basic_run.num_of_clients = 500
    else:
        assert False

def run_one_line(data_dir, workload, option):
    trial_name = datetime.now().strftime("%m-%d-%H-%M-%S")
    choice = [32]
    basic_run.contention_factor1 = 10
    basic_run.cache_miss_ratio = 0
    for x in choice:
        # basic_run.cache_miss_ratio = x
        # basic_run.contention_factor1 = x
        # basic_run.split_num = x
        # basic_run.batch_size = x
        basic_run.num_of_nodes["DataBase"] = x
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

    basic_run.num_of_threads["StorageNode"] = 8
    basic_run.num_of_threads["DataBase"] = 8
    basic_run.num_of_threads["TimeServer"] = 8
    basic_run.sn_replication = False
    basic_run.num_of_nodes["DataBase"] = 48
    basic_run.num_of_nodes["StorageNode"] = 2
    basic_run.split_num = 1
    basic_run.batch_size = 1 
    basic_run.contention_factor = 10

    choice = [40, 80, 160]
    results = []
    for x in choice:
        basic_run.optional = x
        result = str(basic_run.run_one_line_wrapper(run_one_line, data_dir, workload, option))
        results.append(result)

    lines = basic_run.get_res_lines(results)
    csv_res = basic_run.get_csv_res(choice, lines)
    basic_run.write_to(f'{data_dir}/results/{option}.csv', csv_res)


