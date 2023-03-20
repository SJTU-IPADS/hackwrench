#include "tpcc.hpp"
#include "api/txn.hpp"
#include "type/input.hpp"

void payment_input(Txn &txn) {
    Input &input = txn.getInput();
    input.add("W_ID", BuiltInType::INT);
    input.add("D_ID", BuiltInType::INT);
    input.add("C_W_ID", BuiltInType::INT);
    input.add("C_D_ID", BuiltInType::INT);
    input.add("C_ID", BuiltInType::INT);
    input.add("H_AMOUNT", BuiltInType::FLOAT);

    txn.setPartitionAffinity(input["W_ID"]);
}

void payment_graph(Txn &txn) {
    Input &input = txn.getInput();
    Value &w_id = input["W_ID"];
    Value &d_id = input["D_ID"];
    Value &c_w_id = input["C_W_ID"];
    Value &c_d_id = input["C_D_ID"];
    Value &c_id = input["C_ID"];
    Value &h_amount = input["H_AMOUNT"];

    // Warehouse
    Row ware = txn.get(WARE, {w_id});
    Value w_ytd = ware.getColumn(W_YTD).apply("Add", {h_amount});
    ware.setColumn(W_YTD, w_ytd);
    txn.put(WARE, {w_id}, ware);

    // District
    Row dist = txn.get(DIST, {w_id, d_id});
    Value d_ytd = dist.getColumn(D_YTD).apply("Add", {h_amount});
    dist.setColumn(D_YTD, d_ytd);
    txn.put(DIST, {w_id, d_id}, dist);

    // Customer
    Row cust = txn.get(CUST, {c_w_id, c_d_id, c_id});
    Value c_balance = cust.getColumn(C_BALANCE).apply("Sub", {h_amount});
    cust.setColumn(C_BALANCE, c_balance);
    Value c_ytd_payment = cust.getColumn(C_YTD_PAYMENT).apply("Add", {h_amount});
    cust.setColumn(C_YTD_PAYMENT, c_ytd_payment);
    Value c_payment_cnt = cust.getColumn(C_PAYMENT_CNT).apply("Add", {});
    cust.setColumn(C_PAYMENT_CNT, c_payment_cnt);
    Value c_credit = cust.getColumn(C_CREDIT).apply("STATIC_UPDATE", {});
    cust.setColumn(C_CREDIT, c_credit);
    txn.put(CUST, {c_w_id, c_d_id, c_id}, cust);

    Row history = txn.alloc(HIST);
    history.setColumn(H_D_ID, d_id);
    history.setColumn(H_W_ID, w_id);
    history.setColumn(H_AMOUNT, h_amount);
    txn.put(HIST, {c_w_id, c_d_id, c_id, w_id, d_id}, history);

    txn.commit();
}
