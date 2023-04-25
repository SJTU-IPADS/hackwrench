# Hackwrench

Hackwrench is a cloud native database which separates computation (transaction execution) and data storage logic.

# Publication

* Zhiyuan Dong, Zhaoguo Wang, Xiaodong Zhang, Xian Xu, Changgeng Zhao, Haibo Chen, Aurojit Panda, Jinyang Li. **Fine-Grained Re-Execution for Efficient Batched Commit of Distributed Transactions**. The 49th International Conference on Very Large Data Bases (VLDB '23), Vancouver, Canada, 2023.

# Source Repo

The primarily used repository for this project resides in [https://ipads.se.sjtu.edu.cn:1312/opensource/hackwrench](https://ipads.se.sjtu.edu.cn:1312/opensource/hackwrench)

## Code organization

The following figure shows the code structure of the overall project.

```
root
│   CMakeLists.txt
│   README.md
└─── dataflow_api
└─── doc
└─── scripts
└─── src
```
| Folder | description  |
|--------|----------|
| `dataflow_api` | A static analysis tool. It provides the dataflow-based programmingabstraction for users to write transactions' store procedures (`Sec 3.2`) |
| `doc` | The documents of Hackwrench. |
| `scripts` | The scripts used to build and run Hackwrench |
| `src` | Hackwrench's source code |

## The Dataflow Programming Abastraction

Hackwrench requires the users to write transactions in the form of dataflow-based stored procedures. We provide a dataflow programming abstraction to track the dependency inside each transaction and statically analyze whether the transactions can use fast path optimization. The code organization is shown as following:

```
dataflow_api
│   main.cpp
└─── src
    └─── api
    └─── benchmark
    └─── graph
    └─── type
    └─── util
```


| Folder | description  |
|--------|----------|
| `api` | The API used by developers to write stored procedures.  |
| `benchmark` | An illustrating example of the dataflow API, using TPC-C. |
| `graph` | The data structures used to represent a dataflow graph. |
| `type` | The internal data type used. |
| `util` | utilities. |


We first describe the programming interfaces and then use TPC-C as an illustrating example, indicating `how to use the APIs` and `the capability of applying fast path optimization to TPC-C`.

[The Dataflow Programming Abastraction](./doc/dataflow_api/api.md)

[TPC-C as an example](./doc/dataflow_api/tpcc.md)

## The Hackwrench System

Hackwrench's code organization is shown as following:

```
src
│   main.cpp
└─── src
    └─── benchmarks
    └─── index
    └─── rpc
    └─── servers
    └─── storage
    └─── txn
    └─── util
```

| Folder | description  |
|--------|----------|
| `benchmarks` | The benchmarks used for evaluation.  |
| `index` | The data structure for in-memory data indexing. |
| `rpc` | The rpc library. |
| `servers` | The event loop and rpc handling logic of `database node`, `time server`, and `storage node`. |
| `storage` | The storage related data structures for organizing `segment` and `page`. |
| `txn` | The code logic related to `transaction local execution` and `batched transactions`. |
| `util` | utilities. |

[How to Build Hackwrench](./doc/hackwrench/build.md)

[How to Run Hackwrench](./doc/hackwrench/run.md)

<!-- ## Evaluating the FoundationDB

[How to Evaluate FoundationDB](https://anonymous.4open.science/r/FoundationDB-Evaluation/README.md) -->
