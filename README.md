# Hackwrench

This is the code repo for codebase of paper#615 of VLDB'23. 

## Code organization

The following figure shows the code structure of the overall project.

```
root
│   CMakeLists.txt
│   README.md
└─── dataflow_api
└─── doc
└─── plot
└─── scripts
└─── src
```
| Folder | description  |
|--------|----------|
| `dataflow_api` | A static analysis tool. It provides the dataflow-based programmingabstraction for users to write transactions' store procedures (`Sec 3.2`) |
| `doc` | The documents of Hackwrench. |
| `plot` | The gnuplot scripts and evaluation result of figures in the paper. |
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
