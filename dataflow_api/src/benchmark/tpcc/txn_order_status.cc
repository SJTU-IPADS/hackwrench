#include "api/txn.hpp"
#include "tpcc.hpp"
#include "type/input.hpp"

void order_status_input(Txn &txn) {
    Input &input = txn.getInput();
    input.add("W_ID", BuiltInType::INT);
    input.add("D_ID", BuiltInType::INT);
    input.add("C_ID", BuiltInType::INT);

    txn.setPartitionAffinity(input["W_ID"]);
}

void order_status_graph(Txn &txn) {
    Input &input = txn.getInput();
    Value &w_id = input["W_ID"];
    Value &d_id = input["D_ID"];
    Value &c_id = input["C_ID"];

    // Customer
    Row cust = txn.get(CUST, {w_id, d_id, c_id});
    // skip some reading operations

    Row cust_index = txn.get(CUST_INDEX, {w_id, d_id, c_id});
    Value c_o_id = cust_index.getColumn(CI_LAST_ORDER);

    // Order
    Row order = txn.get(ORDR, {w_id, d_id, c_o_id});
    Value o_ol_count = order.getColumn(O_OL_COUNT);

    auto iter_logic = [&w_id, &d_id, &c_o_id](Txn &txn, Input &loop_input, Value &loop_num) {
        // Order Line
        Row order_line = txn.get(ORLI, {w_id, d_id, c_o_id, loop_num});

        Values res;
        return res;
    };

    Input empty_input;
    Values resArray = txn.map(iter_logic, empty_input, o_ol_count);
    txn.commit();
}
