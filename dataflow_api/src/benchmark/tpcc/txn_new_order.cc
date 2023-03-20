#include "tpcc.hpp"
#include "api/txn.hpp"
#include "type/input.hpp"

void new_order_input(Txn &txn) {
    Input &input = txn.getInput();
    input.add("W_ID", BuiltInType::INT);
    input.add("D_ID", BuiltInType::INT);
    input.add("C_ID", BuiltInType::INT);
    input.add("OL_COUNT", BuiltInType::INT);

    Input loop_vars;
    loop_vars.add("I_ID", BuiltInType::INT);
    loop_vars.add("S_W_ID", BuiltInType::INT);
    loop_vars.add("QUANTITY", BuiltInType::INT);
    input.addArray("LOOP_ORDERLINE", loop_vars);

    txn.setPartitionAffinity(input["W_ID"]);
}

void new_order_graph(Txn &txn) {
    Input &input = txn.getInput();
    Value &w_id = input["W_ID"];
    Value &d_id = input["D_ID"];
    Value &c_id = input["C_ID"];

    // Warehouse
    Row ware = txn.get(WARE, {w_id});
    Value w_tax = ware.getColumn(W_TAX);

    // District
    Row dist = txn.get(DIST, {w_id, d_id});
    Value d_tax = dist.getColumn(D_TAX);
    Value d_next_o_id = dist.getColumn(D_NEXT_O_ID).apply("Add", {});
    dist.setColumn(D_NEXT_O_ID, d_next_o_id);
    txn.put(DIST, {w_id, d_id}, dist);

    // Customer
    Row cust = txn.get(CUST, {w_id, d_id, c_id});
    Value c_discount = cust.getColumn(C_DISCOUNT);

    Row cust_index = txn.alloc(CUST_INDEX);
    cust_index.setColumn(CI_LAST_ORDER, d_next_o_id);
    txn.put(CUST_INDEX, {w_id, d_id, c_id}, cust_index);

    // Order
    Row order = txn.alloc(ORDR);
    // skip static modification to o_ol_cnt, o_all_local, o_carrier_id, o_c_id, o_entry_d
    txn.put(ORDR, {w_id, d_id, d_next_o_id}, order);

    // New Order
    Row new_order = txn.alloc(NORD);
    txn.put(NORD, {w_id, d_id, d_next_o_id}, new_order);

    auto iter_logic = [&w_id, &d_id, &d_next_o_id](Txn &txn, Input &loop_input, Value &loop_num) {
        Value &i_id = loop_input["I_ID"];
        Value &s_w_id = loop_input["S_W_ID"];
        Value &quantity = loop_input["QUANTITY"];

        // Item
        Row item = txn.get(ITEM, {i_id});
        Value price = item.getColumn(I_PRICE);

        // Stock
        Row stoc = txn.get(STOC, {s_w_id, i_id});
        Value s_quantity = stoc.getColumn(S_QUANTITY).apply("StaticUpdates", {quantity});
        stoc.setColumn(S_QUANTITY, s_quantity);
        Value s_ytd = stoc.getColumn(S_YTD).apply("Add", {quantity});
        stoc.setColumn(S_YTD, s_ytd);
        Value s_order_cnt = stoc.getColumn(S_ORDER_CNT).apply("Add", {});
        stoc.setColumn(S_ORDER_CNT, s_order_cnt);
        Value s_remote_cnt = stoc.getColumn(S_REMOTE_CNT).apply("StaticUpdates", {w_id, s_w_id});
        stoc.setColumn(S_REMOTE_CNT, s_remote_cnt);
        txn.put(STOC, {s_w_id, i_id}, stoc);

        Value ol_amount = quantity.apply("MULTIPLY", {price});

        // Order Line
        Row order_line = txn.alloc(ORLI);
        order_line.setColumn(OL_I_ID, i_id);
        order_line.setColumn(OL_AMOUNT, ol_amount);
        order_line.setColumn(OL_SUPPLY_W_ID, s_w_id);
        order_line.setColumn(OL_QUANTITY, quantity);
        txn.put(ORLI, {w_id, d_id, d_next_o_id, loop_num}, order_line);

        Values loop_res;
        loop_res.add("OL_AMOUNT", ol_amount);
        return loop_res;
    };


    Values loop_res_list = txn.map(iter_logic, input.getArray("LOOP_ORDERLINE"), input["OL_COUNT"]);
    Values res = loop_res_list.reduce("SUM");
    Value total_amount = res["OL_AMOUNT"].apply("CALCULATE", {w_tax, d_tax, c_discount});

    txn.commit();
}
