#!/usr/bin/env python

import subprocess
from subprocess import run
import sys
import traceback
from pathlib import Path

def grep_number(log_dir, c, name):
    x = run(f'cat {log_dir}/*-{c}clients/db.log | grep "{name}"', shell=True, stdout=subprocess.PIPE)
    if x.stdout.decode('utf-8') == '':
        return -1
    output = x.stdout.decode('utf-8')
    output = output[output.find(']')+1:]
    return float(output.split(':')[1].split()[0])

def grep_batch_profile(log_dir, c, name):
    x = run(f'cat {log_dir}/*-{c}clients/db.log | grep "{name}"', shell=True, stdout=subprocess.PIPE)
    if x.stdout.decode('utf-8') == '':
        return -1
    output = x.stdout.decode('utf-8')
    output = output[output.find(']')+1:]
    return float(output.split(f'{name}:')[1].split()[0])

def grep_batch_profile_non_float(log_dir, c, name):
    x = run(f'cat {log_dir}/*-{c}clients/db.log | grep "{name}"', shell=True, stdout=subprocess.PIPE)
    if x.stdout.decode('utf-8') == '':
        return -1
    output = x.stdout.decode('utf-8')
    output = output[output.find(']')+1:]
    return output.split(f'{name}:')[1].split()[0]

def get_test_result(log_dir, output_file=None):
    output = run(f'ls {log_dir}', shell=True, stdout=subprocess.PIPE)
    clients = output.stdout.decode('utf-8').replace('clients', '').split()
    clients = sorted([int(i.split('-')[1]) for i in clients])
    output_str = ''
    for t in clients:
        thpt = grep_number(log_dir, t, "Total throu")
        if thpt == -1:
            continue
        run_times = grep_number(log_dir, t, "Total run_times")
        commit_times = grep_number(log_dir, t, "Total commit_times")
        repair_times = grep_number(log_dir, t, "Total repair_times")
        batch_commit_times = grep_batch_profile(log_dir, t, "batch_commit_times")
        if batch_commit_times == 0:
            batch_commit_times = 1
        batch_repair_ratio = grep_batch_profile(log_dir, t, "batch_repair_ratio")
        batch_abort_ratio = grep_batch_profile(log_dir, t, "batch_abort_ratio")
        abort_ratio = grep_batch_profile(log_dir, t, "local_abort_ratio")
        remote_abort_ratio = grep_batch_profile(log_dir, t, "remote_abort_ratio")
        repair_ratio = repair_times / (commit_times + 0.00000000000001)
        avg_batch_size = commit_times / batch_commit_times
        average_transaction_latency = grep_batch_profile_non_float(log_dir, t, "Average Transaction Latency")
        p50_transaction_latency = grep_batch_profile_non_float(log_dir, t, "P50 Transaction Latency")
        p90_transaction_latency = grep_batch_profile_non_float(log_dir, t, "P90 Transaction Latency")
        p99_transaction_latency = grep_batch_profile_non_float(log_dir, t, "P99 Transaction Latency")
        if average_transaction_latency == -1:
            output_str += (f'{t}, {thpt}, {avg_batch_size}, {batch_repair_ratio}\n')
        else:
            output_str += (f'{t}, {thpt}, {remote_abort_ratio}, {abort_ratio}, {repair_ratio}, {batch_repair_ratio}, {batch_abort_ratio}, {avg_batch_size}, {average_transaction_latency}, {p50_transaction_latency}, {p90_transaction_latency}, {p99_transaction_latency}\n')

    if output_file:
        o_file = Path(output_file)
        o_file.parent.mkdir(exist_ok=True, parents=True)
        o_file.write_text(output_str)
    print(output_str)
    return output_str

if __name__ == '__main__':
    log_dir = sys.argv[1]
    output_file = None
    if len(sys.argv) > 2:
        output_file = sys.argv[2]
    get_test_result(log_dir, output_file)
