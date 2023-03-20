#!/usr/bin/env python3
import yaml
import sys

data_dir = sys.argv[1]

with open(f"{data_dir}/servers.yaml", "r") as f:
    hosts = yaml.load(f, Loader=yaml.loader.FullLoader)

ips = {}

with open(f"{data_dir}/ssh_config", "w") as f:
    i = 0
    for ip in hosts["ips"]:
        # if ips.get(ip) == 1:
        #     print("redundant ip: " + ip)
        #     assert False

        f.write(f'Host host{i}\n')
        f.write(f'    HostName {ip}\n')
        i += 1

        ips[ip] = 1

