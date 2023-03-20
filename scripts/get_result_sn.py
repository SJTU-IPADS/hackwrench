#!/usr/bin/env python

import subprocess
from subprocess import run
import sys


def grep_number(log_dir, c, name):
    x = run(f'cat {log_dir}/*-{c}clients/sn.log | grep "{name}"', shell=True, stdout=subprocess.PIPE)
    if x.stdout.decode('utf-8') == '':
        return -1
    output = x.stdout.decode('utf-8')
    output = output[output.find(']')+1:]
    return float(output.split(':')[1].split()[0])

def grep_batch_profile(log_dir, c, name):
    x = run(f'cat {log_dir}/*-{c}clients/sn.log | grep "{name}"', shell=True, stdout=subprocess.PIPE)
    if x.stdout.decode('utf-8') == '':
        return -1
    output = x.stdout.decode('utf-8')
    output = output[output.find(']')+1:]
    return float(output.split(f'{name}:')[1].split()[0])



def get_test_result(log_dir, output_file=None):
    output = run(f'ls {log_dir}', shell=True, stdout=subprocess.PIPE)
    clients = output.stdout.decode('utf-8').replace('clients', '').split()
    clients = sorted([int(i.split('-')[1]) for i in clients])
    output_str = ''
    for t in clients:
        thpt = grep_number(log_dir, t, "SN throughput")
        if thpt == -1:
            continue
        output_str += (f'{t}, {thpt}\n')
        # print(f'{t}, {thpt}, {repair_ratio}, {batch_repair_ratio}, {abort_ratio}')
        # print(f'{t}, {thpt}, {abort_ratio}, {batch_repair_ratio}')
        # print(f'{t}, {thpt}')
    if output_file:
        with open(output_file, 'w') as f:
            f.write(output_str)
    print(output_str)

if __name__ == '__main__':
    log_dir = sys.argv[1]
    output_file = None
    if len(sys.argv) > 2:
        output_file = sys.argv[2]
    get_test_result(log_dir, output_file)
