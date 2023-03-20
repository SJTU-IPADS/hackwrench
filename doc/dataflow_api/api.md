# The Programming Interface

The Dataflow APIs are shown in `root/dataflow_api/api`. We now describe them in detail.
## Schema

Hackwrench requires the users to define the database schema ([schema.hpp](../../dataflow_api/src/api/schema.hpp)):

```C++
class TableSchema {
    TableSchema(string tbl_name, 
        bool read_only, 
        vector<pair<string, BuiltInType>> columns,
        vector<string> pkey_def, 
        string part_key_def);
};
```

For each table in the database, the users should define:
1. `table_name`: the table's name.
2. `read-only`: whether the table is read only.
3. `columns` : the list of columns, which is pair of column name and the column's BuiltInType (`Int`, `Float`, or `String`).
4. `pkey_def` : the names of columns which consist of the primary key
5. `part_key_def` : the name of column which is used to partition the table.

## Transaction

After the schemas are defined, the users can write the stored procedure with APIs provided in [txn.hpp](../../dataflow_api/src/api/txn.hpp).


```C++
using IterationLogic = std::function<Values(Txn &txn, Input &input, Value &loop_num)>;

class Txn {
    // Read & Write Data Flow
    Row get(string table_name, vector<Value> pkey);
    void put(string table_name, vector<Value> pkey, Row row);

    // If-Branch Control Flow
    void beginIf(Value value);
    void endIf();

    // Loop Control Flow
    Values map(IterationLogic iter_logic, Input input, Value loop_count);

    // Commit or Abort Transaction
    void commit();
    void abort();

    // Hint given by the user
    // Specify the parition (thus the database node) where the transaction should execute
    void setPartitionAffinity(Value value);
};
```

### Data Flow Operation

The `Get/Put` APIs are the basic data access operations, which read and write one database row (`Row`) of target table and primary key. Specifically, `Value` is an instance of given `BuiltInType`, which is also responsible for tracking the dependency inside the transaction. The primary key is a list of values, and each value corresponds to one column of the primary key definition.

### Control Flow Operation

The API also captures the control flows. `beginIf` and `EndIf` are related to if-branch. `beginIf` accepts a value as the branch condition.

On the other hand, `map` handles the loops, whose parameters are:
1. `iter_logic` : The loop body of type `IterationLogic`. `IterationLogic`'s `loop_num` parameter indicates the number of times iteration is executed.
2. `input` : The input of each iteration, which is a map of name and corresponding value.
3. `loop_count` : The value indicating the number of loops executed.

As the loop body (`IterationLogic`) is a C++ lambda function. The shared values among iterations can be `captured` by the lambda, instead of explicitly passing to the `map` API.

### Others

`Commit` and `Abort` are used to commit or abort transactions, accordingly.

Finally, users can leverage `setPartitionAffinity` to give the hint that most data operations of the transaction is executed on target parition. Therefore, the transactions should be executed on the dedicated database of target partition.

## Current Limitation

Due to the time limitation, Hackwrench does not integrate the dataflow-based transaction execution currently. The `dataflow_api` is an individual static analysis tool. Its analyzed result is used to guides the implementation of Hackwrench's hand-written C++ stored procedures.

