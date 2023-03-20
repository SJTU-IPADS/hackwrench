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

Please note that, AWS provides both public and private IP address for each instance. We recommend to use private IP. 

## Step 2: Use the Experiment Scripts for Running 

To run the experiments, please go to `root/scripts` directories. The template command and all commands used in evaluation are shown as following:
* The `script_name` corresponding to the python scripts starts with number in `root/scripts`, which automatically run Hackwrench and collect the execution result distributedly.
* `workload` has two options: `tpcc` corresponds to TPC-C workload and `ycsb10` corresponds to FDB-micro microbenchmark in the paper.
* `option value` is an optional configuration value for each script and its meaning changes accordingly.

```bash
./figure/0_motivation.sh
./figure/1_tpcc_tput.sh
./figure/2_tpcc_lat.sh
./figure/3_factor_analysis.sh
./figure/4_tpcc_coco.sh
./figure/5_tpcc_sundial.sh
./figure/6_tpcc_scalability.sh
./figure/7_micro_contention.sh
./figure/8_micro_ts.sh
./figure/9_micro_batch_effect.sh
./figure/10_micro_cache_effect.sh
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








