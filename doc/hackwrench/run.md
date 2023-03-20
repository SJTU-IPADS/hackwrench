# How to Run Hackwrench

## Step 1: Prepare Script Environment and configuration

### Python environment

The scripts for running Hackwrench binaries are in directory `root/scripts`. To use the python scripts, we need to install the requirements:

```bash
cd scripts
pip install -r requirements.txt
```

### SSH Environment

The script uses ssh to control remote machines. For the ease of usage, we need to setup sshd for machines, and add your public key (`.ssh/id_rsa.pub`) to your own `.ssh/authorized_keys`. Therefore, typing ssh password is avoided during experiments.

### Machine Configuration

Directory `root/scripts/aws` contains the cofiguration, log, and results for experiments.

In this directory, `servers.yaml` is the machine configuration with the ip and server type of each machine (aws instance):

```yaml
ips:
- 172.31.23.33
- 172.31.22.66
...
- 172.31.19.160
- 172.31.23.191
- 172.31.26.8
...
type:
- StorageNode
- StorageNode
...
- TimeServer
- DataBase
- DataBase
...
```

Please note that, AWS provide both public and private IP address for each instance. We recomand to use private IP. 

## Step 2: Use the Experiment Scripts for Running 

To run the experiments, please go to `root/scripts` directories. The template command and all commands used in evaluation are shown as following:
* The `script_name` corresponding to the python scripts starts with number in `root/scripts`, which automatically run Hackwrench and collect the execution result distributedly.
* `workload` has two options: `tpcc` corresponds to TPC-C workload and `ycsb10` corresponds to FDB-micro microbenchmark in the paper.
* `option value` is an optional configuration value for each scripts and its meanning changes accordingly.

```bash
# template
./eval/{script_name}.py aws {workload} {option value} {binary_name}

# Figure 4 (19 instances used)
./eval/1_tpcc_tput_lat.py aws tpcc 0 hackwrench
./eval/1_tpcc_tput_lat.py aws tpcc 0 hackwrench_occ

# Table 1 (19 instances used)
./eval/2_tpcc_lat.py aws tpcc 1 hackwrench_lat
./eval/2_tpcc_lat.py aws tpcc 50 hackwrench_lat
./eval/2_tpcc_lat.py aws tpcc 1 hackwrench_occ_lat

# Figure 5 (19 instances used)
./eval/3_factor_analysis.py aws tpcc 0 hackwrench_occ
./eval/3_factor_analysis.py aws tpcc 0 hackwrench_batch_abort
./eval/3_factor_analysis.py aws tpcc 0 hackwrench_ts
./eval/3_factor_analysis.py aws tpcc 0 hackwrench_repair
./eval/3_factor_analysis.py aws tpcc 0 hackwrench

# Figure 6 (46 instances used)
./eval/4_tpcc_scalability.py aws tpcc 0 hackwrench

# Figure 7 (25 instances used)
./eval/5_micro_contention.py aws ycsb10 0 hackwrench
./eval/5_micro_contention.py aws ycsb10 0 hackwrench_occ

# Figure 8 (25 instances used)
./eval/6_micro_batch_effect.py aws ycsb10 10 hackwrench
./eval/6_micro_batch_effect.py aws ycsb10 60 hackwrench

# Figure 9 (25 instances used)
./eval/7_micro_cache_effect.py aws ycsb10 0 hackwrench
./eval/7_micro_cache_effect.py aws ycsb10 0 hackwrench_repair
./eval/7_micro_cache_effect.py aws ycsb10 0 hackwrench_occ

```
### Prepare enough AWS instances

In our experiments, we use a maximal number of 46 m5.2xlarge AWS instances. 
The actual number of instances used for each experiment is shown in the comments.
If you want to reproduce the experiment, please prepare enough AWS instances.

## Check the Experiment results

The scripts will output the CSV-style results after the experiments. The results are also stored in `root/scripts/aws/results/{option}.csv`. The format is shown as following:
```
parameter, 2000-tput, 2000-remote_abort_ratio, 2000-abort_ratio, 2000-repair_ratio, 2000-batch_repair_ratio, 2000-batch_abort_ratio, 2000-average_batch_size, 2000-mean_lat, 2000-p50, 2000-p90, 2000-p99
0, 270203.0, 0.0, 0.0, 0.016899090214084252, 0.018511, 0.0, 1.1115702445344795, 31745023, 30995657, 43072579, 56000276
100, 33770.3, 0.0, 0.0, 0.002342003582584494, 0.00251236, 0.0, 1.0740953931676662, 395469873, 383369138, 731584954, 1108964075
```

The results have two dimension, one is `parameter` and the other is `{x}-{col_name}`'s `x`. The meanings of  `col_name` are:
| `col_name` | description  |
|--------|----------|
| `tput` | Throughput.  |
| `remote_abort_ratio` | The ratio of transactions aborted by cross database nodes conflicts. |
| `abort_ratio` | The ratio of transactions aborted by internal database nodes conflicts. |
| `repair_ratio` | The ratio of transactions repaired. |
| `batch_repair_ratio` | The ratio of `logical transactions`(Sec 4) repaired. |
| `batch_abort_ratio` | The ratio of `logical transactions`(Sec 4) aborted by cross database nodes conflicts. |
| `average_batch_size` | The average number of transactions in one `logical transaction`. |
| `mean_lat`/`p50`/`p90`/`p99` | The average, p50, p90, and p99 latency. |

For each type of scripts, the meaning of `parameter`, `x`, and `option value` are:

| `script_name` | `parameter`  | `x`  | `option_value` |
|--------|----------|----------| -----|
| `1_tpcc_tput_lat` | / | remote access probability (`x/1000`) | / |
| `2_tpcc_lat` | number of clients | controled by `{option value}` | maximal batch size |
| `3_factor_analysis` | / | remote access probability (`x/1000`) | / |
| `4_tpcc_scalability` | number of database nodes | remote access probability (`x/10`) | / |
| `5_micro_contention` | number of database nodes | zipf theta (`x/100`) | / |
| `6_micro_batch_effect` | number of clients | maximal batch size | zipf theta (`x/100`) |
| `7_micro_cache_effect` | cache_miss_ratio | num_of_clients | / |








