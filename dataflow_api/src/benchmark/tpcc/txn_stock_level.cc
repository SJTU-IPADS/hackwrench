#include "api/txn.hpp"
#include "tpcc.hpp"
#include "type/input.hpp"

void stock_level_input(Txn &txn) {
    Input &input = txn.getInput();
    input.add("W_ID", BuiltInType::INT);
    input.add("D_ID", BuiltInType::INT);

    txn.setPartitionAffinity(input["W_ID"]);
}

void stock_level_graph(Txn &txn) {
    Input &input = txn.getInput();
    Value &w_id = input["W_ID"];
    Value &d_id = input["D_ID"];

    Row dist = txn.get(DIST, {w_id, d_id});
    Value d_tax = dist.getColumn(D_TAX);
    Value d_next_o_id = dist.getColumn(D_NEXT_O_ID);

    auto iter1_logic = [&w_id, &d_id, &d_next_o_id](Txn &txn, Input &loop_input, Value &loop_num) {
        Value o_id = d_next_o_id.apply("Substract", {loop_num});
        auto iter2_logic = [&w_id, &d_id, &o_id](Txn &txn, Input &loop_input, Value &loop_num) {
            Row order_line = txn.get(ORLI, {w_id, d_id, o_id, loop_num});
            Value found = order_line.isFound();

            Value count(BuiltInType::INT, "count");
            txn.beginIf(found);
            {
                Value ol_i_id = order_line.getColumn(OL_I_ID);
                Row stoc = txn.get(STOC, {w_id, ol_i_id});
                count.apply("Add", {stoc.getColumn(S_QUANTITY)});
            }
            txn.endIf();
            Values res;
            res.add("COUNT", count);
            return res;
        };

        Value int_15(BuiltInType::INT, "15");
        Input empty_input;
        Values resArray = txn.map(iter2_logic, empty_input, int_15);
        Values iter2_res = resArray.reduce("SUM");
        Value count = iter2_res["COUNT"];

        Values res;
        res.add("COUNT", count);
        return res;
    };

    Input empty_input;
    Value int_20(BuiltInType::INT, "20");
    Values resArray = txn.map(iter1_logic, empty_input, int_20);
    Values res = resArray.reduce("SUM");
    Value total_count = res["COUNT"];
    txn.commit();
}
