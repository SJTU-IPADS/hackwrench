#include "tpcc.hpp"
#include "api/txn.hpp"
#include "type/input.hpp"

void delivery_input(Txn &txn) {
    Input &input = txn.getInput();
    input.add("W_ID", BuiltInType::INT);
    input.add("D_NUM", BuiltInType::INT); // 10
    input.add("O_CARRIER_ID", BuiltInType::INT);

    txn.setPartitionAffinity(input["W_ID"]);
}

void delivery_graph(Txn &txn) {
    Input &input = txn.getInput();
    Value &w_id = input["W_ID"];
    Value &o_carrier_id = input["O_CARRIER_ID"];
    Value &ol_delivery_d = input["OL_DELIVERY_D"];

    auto iter1_logic = [&w_id, &o_carrier_id, &ol_delivery_d](Txn &txn, Input &loop_input, Value &loop_num) {
        Value &d_id = loop_input["D_ID"];
        // District
        Row dist_deli_index = txn.get(DIST_DELI_INDEX, {w_id, d_id});
        Value ddi_o_id = dist_deli_index.getColumn(DDI_O_ID);

        // New Order
        Row new_order = txn.get(NORD, {w_id, d_id, ddi_o_id});
        Value found = new_order.isFound();

        txn.beginIf(found);
        {
            Value new_ddi_o_id = ddi_o_id.apply("Add", {});
            dist_deli_index.setColumn(DDI_O_ID, new_ddi_o_id);
            txn.put(DIST_DELI_INDEX, {w_id, d_id}, dist_deli_index);

            // delete
            txn.put(NORD, {w_id, d_id, ddi_o_id}, new_order);

            Row order = txn.get(ORDR, {w_id, d_id, ddi_o_id});
            Value o_c_id = order.getColumn(O_C_ID);
            Value o_ol_cnt = order.getColumn(O_OL_COUNT);
            order.setColumn(O_CARRIER_ID, o_carrier_id);
            txn.put(ORDR, {w_id, d_id, ddi_o_id}, order);

            auto iter2_logic = [&w_id, &d_id, &ddi_o_id, &ol_delivery_d](Txn &txn, Input &loop_input, Value &loop_num) {
                Row order_line = txn.get(ORLI, {w_id, d_id, ddi_o_id, loop_num});
                Value ol_amount = order_line.getColumn(OL_AMOUNT);
                order_line.setColumn(OL_DELIVERY_D, ol_delivery_d);
                txn.put(ORLI, {w_id, d_id, ddi_o_id, loop_num}, order_line);

                Values res;
                res.add("OL_AMOUNT", ol_amount);
                return res;
            };

            Input empty_input;
            Values resArray = txn.map(iter2_logic, empty_input, o_ol_cnt);
            Values res = resArray.reduce("SUM");
            Value total_amount = res["OL_AMOUNT"];

            Row cust = txn.get(CUST, {w_id, d_id, o_c_id});
            Value c_delivery_cnt = cust.getColumn(C_DELIVERY_CNT).apply("Add", {});
            cust.setColumn(C_DELIVERY_CNT, c_delivery_cnt);
            Value c_balance = cust.getColumn(C_BALANCE).apply("Add", {total_amount});
            cust.setColumn(C_BALANCE, c_balance);
            txn.put(CUST, {w_id, d_id, o_c_id}, cust);
        }
        txn.endIf();

        Values res;
        return res;
    };

    Input empty_input;
    Values resArray = txn.map(iter1_logic, empty_input, input["D_NUM"]);
    txn.commit();
}
