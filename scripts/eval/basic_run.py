#!/usr/bin/env python3

import invoke
from invoke import run
import fabric
from fabric import ThreadingGroup as Group
from pathlib import Path
import time
import yaml
import os
import threading
from get_result import get_test_result
import traceback

devnull = open(os.devnull, "w")

config_file_name = "hackwrench"
bin_file_name = "hackwrench"
log_path = "./logs"

node_types = ["StorageNode", "TimeServer", "DataBase", "Client"]
num_of_nodes = {"DataBase": 1, "StorageNode": 1, "TimeServer": 1, "Client": 0}
num_of_threads = {"StorageNode": 8, "TimeServer": 8, "DataBase": 8, "Client": 8}

batch_size = 50
split_num = 50
contention_factor = 0
contention_factor1 = 15
cache_miss_ratio = 0
num_of_clients = 100

sn_replication = False
replication_factor = 3 
num_tables_per_warehouse = 256
num_of_tables = (100000) * num_of_nodes["DataBase"]

duration = 80

nodes=[]
n_nodes=[]


class Node:
    def __init__(self, ip, node_type):
        self.ip = ip
        self.node_type = node_type    
        self.host = ""
    
    def script_file_name(self):
        return f"{config_file_name}_{self.host_id()}.sh"

    def host_id(self):
        return self.host[4:]

    def __str__(self):
        return f'{self.ip} {self.node_type} {self.host}'

def get_ips(servers_file):
    with open(servers_file, "r") as f:
        ips = yaml.load(f, Loader=yaml.loader.FullLoader)
        return ips

def initialize(data_dir):
    global nodes,n_nodes
    with open(f"{data_dir}/servers.yaml", "r") as f:
        hosts = yaml.load(f, Loader=yaml.loader.FullLoader)

    n_nodes = {}
    for node_type in node_types:
        n_nodes[node_type] = []
    node_list = []
    
    i = 0
    for ip in hosts["ips"]:
        node_type = hosts["type"][i]
        node = Node(ip, node_type)
        n_nodes[node_type].append(node)
        node_list.append(node)
        i+=1
    
    with open(f"{data_dir}/ssh_config", "r") as f:
        lines = f.readlines()
        host = ""
        i = 0
        for line in lines:
            line = line.replace(" ", "").replace("\n", "")
            if line[0:5] == "Hosth":
                host = line[4:]
                continue
            ip = line[8:]
            node_list[i].host = host
            i+=1

    nodes=[]
    for node_type in node_types:
        for i in range(num_of_nodes[node_type]):
            nodes.append(n_nodes[node_type][i])

def get_ssh_config(data_dir):
    return fabric.config.Config(user_ssh_path=f"{data_dir}/ssh_config")

def remote_run(conn, cmd, retry=True):
    while True:
        try:
            conn.run(cmd)
            return
        except invoke.exceptions.UnexpectedExit as e:
            if not retry:
                return
            print(e)


def local_run(cmd, retry=True):
    while True:
        try:
            run(cmd)
            return
        except invoke.exceptions.UnexpectedExit as e:
            if not retry:
                return
            print(e)

def prepare_binaries(data_dir):
    run(f'./gen_ssh_config.py {data_dir}')
    run(f'rm -f {data_dir}/hackwrench*')
    run(f'cp ../build/hackwrench* {data_dir}/')

def gen_config(data_dir, num_of_nodes):
    # generage config
    output_file = f"{data_dir}/{config_file_name}.ini"

    if len(nodes) < sum(num_of_nodes.values()):
        print(len(nodes), num_of_nodes)
        assert False
    base_port = 9988
    reserve_ports = 50
    node_id = 0
    with open(output_file, "w") as f:
        f.write("ID NodeType Host Port Threads\n")
        for node in nodes:
            config_line = f"{node_id} {node.node_type} {node.ip} {base_port+node_id*reserve_ports} {num_of_threads[node.node_type]}\n"
            f.write(config_line)
            node_id += 1
    print(f'config file "{output_file}" has been generated')

def gen_all_runners(
    data_dir,
    workload,
    num_of_tables,
    num_of_clients,
    contention_factor,
    batch_size,
    split_num,
    optional,
    cache_miss_ratio,
    sn_replication,
):
    print("generating runners...")
    node_id = 0
    for node in nodes:
        fname = data_dir + "/" + node.script_file_name()
        f = open(fname, "w")

        f.write("sleep 0.1\n")
        conf_str = f"{node_id} {workload} {num_of_tables} {num_of_clients} {contention_factor} {contention_factor1} {batch_size} {split_num} {optional} {cache_miss_ratio} {1 if sn_replication else 0}"
        f.write(
            f"stdbuf -e 0 -o 0 ./{bin_file_name} {conf_str} 2>&1 | tee {log_path}/{node_id}-{node.node_type}.log &\n"
        )
        # following is used for AddressSanitizer
        # f.write(
        #     f"ASAN_OPTIONS=\"halt_on_error=0:abort_on_error=0\" LD_PRELOAD=/usr/lib/x86_64-linux-gnu/libasan.so.4 stdbuf -e 0 -o 0 ./{bin_file_name} {conf_str} 2>&1 | tee {log_path}/{node_id}-{node.node_type}.log &\n"
        # )

        node_id += 1
        f.write("wait\n")
        f.write(f"echo {node.host}: all jobs are done!\n")
        f.close()

def send_all_files(data_dir):
    print("sending files...")
    s = "'"
    sent_ips = {}
    for node in nodes:
        if sent_ips.get(node.ip) == 1:
            continue
        sent_ips[node.ip] = 1
        s += node.host + " "
    s += "'"
    local_run(f"./send_code.sh {data_dir} {s}")


def run_all(data_dir):
    print("starting processes...")
    def foo(node):
        config = fabric.config.Config(user_ssh_path=f"{data_dir}/ssh_config")
        conn = fabric.Connection(node.host, config=config)
        remote_run(conn, f"mkdir -p {log_path}")
        # kill session once to prevent new session error
        remote_run(conn, f"tmux kill-session -t txnrepair{node.host_id()} > /dev/null 2>&1", False)

        # if node.host_id() == "3":
        #     return

        # remote_run(
        #     conn,
        #     f'source ~/txn-repair.source || true && tmux new-session -d -s txnrepair{node.host_id()} "./{node.script_file_name()} > {log_path}/host{node.host_id()}.log"',
        # )

        remote_run(
            conn,
            f'tmux new-session -d -s txnrepair{node.host_id()} "bash ./{node.script_file_name()} > {log_path}/host{node.host_id()}.log"',
        )

        # following is used for perf profile, use perf for specific node
        # if node.host_id() in ["13"]:
        #     remote_run(
        #         conn,
        #         f'tmux new-session -d -s txnrepair{node.host_id()} "sudo perf record -a --sample-cpu -F 1000 --call-graph dwarf bash ./{node.script_file_name()} > {log_path}/host{node.host_id()}.log"',
        #     )
        # else:
        #     remote_run(
        #         conn,
        #         f'tmux new-session -d -s txnrepair{node.host_id()} "bash ./{node.script_file_name()} > {log_path}/host{node.host_id()}.log"',
        #     )

    threads = []
    for node in nodes:
        t = threading.Thread(target=foo, args=(node, ))
        t.start()
        threads.append(t)
    for t in threads:
        t.join()


def get_primary_db_host(num_of_nodes):
    return int(num_of_nodes["StorageNode"] + num_of_nodes["TimeServer"])


def wait_db(data_dir, node):
    print(f"waiting for {node.host} to finish...")
    conn = fabric.Connection(f"{node.host}", config=get_ssh_config(data_dir))
    time.sleep(10)
    waittime = 10
    while waittime < duration:
        try:
            result = conn.run(
                f'cat "./logs/{node.host}.log" | grep "Total throughput:\\|Assertion!"',
                out_stream=devnull,
            )
            print(result.stdout)
            if "Total throughput:" in result.stdout:
                # time.sleep(20)
                return True
            elif "Assertion!" in result.stdout:
                return False
        except Exception:
            time.sleep(3)
        waittime += 3
    return False


def kill_all(data_dir):
    print("kill all...")
    threads = []
    for node in nodes:
        conn = fabric.Connection(node.host, config=get_ssh_config(data_dir))
        t = threading.Thread(
            target=remote_run, args=(conn, f"pkill -f './{bin_file_name} '", False)
        )
        t.start()
        threads.append(t)
        if data_dir == "localhost":
            break
    for t in threads:
        t.join()

def clean_up(data_dir):
    local_run(f"rm -f {data_dir}/{config_file_name}*.sh")

def download_log(data_dir, node, test_name):
    conn = fabric.Connection(node.host, config=get_ssh_config(data_dir))
    result_dir = f"{data_dir}/logs/{test_name}"
    local_run(f"mkdir -p {result_dir}")
    conn.get(f"./logs/{node.host}.log", f"{result_dir}/{node.host_id()}.log")

def save_result(data_dir, node, test_name):
    conn = fabric.Connection(node.host, config=get_ssh_config(data_dir))
    result_dir = f"{data_dir}/logs/{test_name}"
    local_run(f"mkdir -p {result_dir}")
    conn.get(f"./logs/{node.host}.log", f"{result_dir}/db.log")
    conn = fabric.Connection(f"host0", config=get_ssh_config(data_dir))
    conn.get(f"./logs/host0.log", f"{result_dir}/sn.log")

def run_one_line_wrapper(func, data_dir, workload, option):
    try:
        ret = func(data_dir, workload, option)
        kill_all(data_dir)
        return ret
    except KeyboardInterrupt:
        print("\nKeyboard interrupted")
        basic_run.kill_all(data_dir)
    except Exception:
        print(traceback.format_exc())
        basic_run.kill_all(data_dir)
    basic_run.devnull.close()

def get_res_lines(results):
    firstTime = True
    lines = []
    for result in results:
        result_lines = result.splitlines() 
        for i in range(len(result_lines)):
            line = result_lines[i]
            if firstTime:
                lines.append(line)
            else:
                items = line.split(",", 1)
                lines[i] += ","+items[1]
        firstTime = False

    return lines

def get_csv_first_row(choice):
    csv_res = "parameter"
    for x in choice:
        csv_res += f", {x}-tput, {x}-remote_abort_ratio, {x}-abort_ratio, {x}-repair_ratio, {x}-batch_repair_ratio, {x}-batch_abort_ratio, {x}-average_batch_size, {x}-mean_lat, {x}-p50, {x}-p90, {x}-p99"
    csv_res += "\n"
    return csv_res

def get_csv_res(choice, lines):
    csv_res = get_csv_first_row(choice)
    for line in lines:
        if line[-1] != "\n":
            line += "\n"
        csv_res += line
    print(csv_res)
    return csv_res

def write_to(pathname, data):
    output_file = Path(pathname)
    output_file.parent.mkdir(exist_ok=True, parents=True)
    output_file.write_text(data)

def run_test(
    data_dir,
    workload,
    num_of_tables,
    num_of_clients,
    contention_factor,
    batch_size,
    split_num,
    optional,
    cache_miss_ratio,
    sn_replication,
    test_name
):

    initialize(data_dir)

    retry = 0
    split_num = min(split_num, batch_size)
    workload_id = {"tpcc": 1, "xpp": 2, "ycsb": 3, "swap": 4, "ycsb10": 5, "tpcclike": 6}[workload]
    
    
    while retry < 3:
        kill_all(data_dir)
        clean_up(data_dir)
        gen_config(data_dir, num_of_nodes)
        gen_all_runners(
            data_dir,
            workload_id,
            num_of_tables,
            num_of_clients,
            contention_factor,
            batch_size,
            split_num,
            optional,
            cache_miss_ratio,
            sn_replication,
        )
        send_all_files(data_dir)

        run_all(data_dir)
        success = wait_db(data_dir, nodes[get_primary_db_host(num_of_nodes)])
        print(f"success? {success}")
        if success:
            break
        else:
            for node in nodes:
                download_log(data_dir, node, test_name)
        retry += 1
    if data_dir != "localhost":
        time.sleep(3)
    save_result(data_dir, nodes[get_primary_db_host(num_of_nodes)], test_name)
