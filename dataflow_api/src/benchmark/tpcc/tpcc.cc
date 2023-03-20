#include "tpcc.hpp"

#include "api/txn.hpp"
#include "type/input.hpp"

void do_bench(int argc, char **argv) {
    DatabaseSchema db_schema = init_schema();

    Txn new_order_txn(db_schema);
    {
        new_order_input(new_order_txn);
        new_order_graph(new_order_txn);
    }

    Txn payment_txn(db_schema);
    {
        payment_input(payment_txn);
        payment_graph(payment_txn);
    }

    Txn delivery_txn(db_schema);
    {
        delivery_input(delivery_txn);
        delivery_graph(delivery_txn);
    }

    Txn order_status_txn(db_schema);
    {
        order_status_input(order_status_txn);
        order_status_graph(order_status_txn);
    }

    Txn stock_level_txn(db_schema);
    {
        stock_level_input(stock_level_txn);
        stock_level_graph(stock_level_txn);
    }

    new_order_txn.print_graph("new_order.dot");
    payment_txn.print_graph("payment.dot");
    delivery_txn.print_graph("delivery.dot");
    order_status_txn.print_graph("order_status.dot");
    stock_level_txn.print_graph("stock_level.dot");
}