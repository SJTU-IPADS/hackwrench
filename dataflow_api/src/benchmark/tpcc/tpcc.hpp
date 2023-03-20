#pragma once

#include "type/value.hpp"
#include "api/schema.hpp"
#include "api/txn.hpp"

static const TableName_t WARE = "WARE";
static const ColumnName_t W_ID = WARE + "_ID";
static const ColumnName_t W_NAME = WARE + "_NAME";
static const ColumnName_t W_STREET_1 = WARE + "_STREET_1";
static const ColumnName_t W_STREET_2 = WARE + "_STREET_2";
static const ColumnName_t W_CITY = WARE + "_CITY";
static const ColumnName_t W_STATE = WARE + "_STATE";
static const ColumnName_t W_ZIP = WARE + "_ZIP";
static const ColumnName_t W_TAX = WARE + "_TAX";
static const ColumnName_t W_YTD = WARE + "_YTD";

static const TableName_t DIST = "DIST";
static const ColumnName_t D_ID = DIST + "_ID";
static const ColumnName_t D_W_ID = DIST + "_W_ID";
static const ColumnName_t D_W_NAME = DIST + "_W_NAME";
static const ColumnName_t D_W_STREET1 = DIST + "_W_STREET1";
static const ColumnName_t D_W_STREET2 = DIST + "_W_STREET2";
static const ColumnName_t D_CITY = DIST + "_CITY";
static const ColumnName_t D_STATE = DIST + "_STATE";
static const ColumnName_t D_ZIP = DIST + "_ZIP";
static const ColumnName_t D_TAX = DIST + "_TAX";
static const ColumnName_t D_YTD = DIST + "_YTD";
static const ColumnName_t D_NEXT_O_ID = DIST + "_NEXT_O_ID";

static const TableName_t DIST_DELI_INDEX = "DIST_DELI_INDEX";
static const ColumnName_t DDI_D_ID = DIST_DELI_INDEX + "_D_ID";
static const ColumnName_t DDI_W_ID = DIST_DELI_INDEX + "_W_ID";
static const ColumnName_t DDI_O_ID = DIST_DELI_INDEX + "_O_ID";


static const TableName_t CUST = "CUST";
static const ColumnName_t C_ID = CUST + "_ID";
static const ColumnName_t C_W_ID = CUST + "_W_ID";
static const ColumnName_t C_D_ID = CUST + "_D_ID";
static const ColumnName_t C_FIRST = CUST + "_FIRST";
static const ColumnName_t C_MIDDLE = CUST + "_MIDDLE";
static const ColumnName_t C_LAST = CUST + "_LAST";
static const ColumnName_t C_STREET1 = CUST + "_STREET1";
static const ColumnName_t C_STREET2 = CUST + "_STREET2";
static const ColumnName_t C_CITY = CUST + "_CITY";
static const ColumnName_t C_STATE = CUST + "_STATE";
static const ColumnName_t C_ZIP = CUST + "_ZIP";
static const ColumnName_t C_PHONE = CUST + "_PHONE";
static const ColumnName_t C_SINCE = CUST + "_SINCE";
static const ColumnName_t C_CREDIT = CUST + "_CREDIT";
static const ColumnName_t C_CREDIT_LIM = CUST + "_CREDIT_LIM";
static const ColumnName_t C_DISCOUNT = CUST + "_DISCOUNT";
static const ColumnName_t C_BALANCE = CUST + "_BALANCE";
static const ColumnName_t C_YTD_PAYMENT = CUST + "_YTD_PAYMENT";
static const ColumnName_t C_PAYMENT_CNT = CUST + "_PAYMENT_CNT";
static const ColumnName_t C_DELIVERY_CNT = CUST + "_DELIVERY_CNT";
static const ColumnName_t C_DATA = CUST + "_DATA";

static const TableName_t CUST_INDEX = "CUST_INDEX";
static const ColumnName_t CI_C_ID = CUST_INDEX + "_C_ID";
static const ColumnName_t CI_W_ID = CUST_INDEX + "_W_ID";
static const ColumnName_t CI_D_ID = CUST_INDEX + "_D_ID";
static const ColumnName_t CI_LAST_ORDER = CUST_INDEX + "_LAST_ORDER";

static const TableName_t ORDR = "ORDR";
static const ColumnName_t O_ID = ORDR + "_ID";
static const ColumnName_t O_D_ID = ORDR + "_D_ID";
static const ColumnName_t O_W_ID = ORDR + "_W_ID";
static const ColumnName_t O_C_ID = ORDR + "_C_ID";
static const ColumnName_t O_ENTRY_D = ORDR + "_ENTRY_D";
static const ColumnName_t O_CARRIER_ID = ORDR + "_CARRIER_ID";
static const ColumnName_t O_OL_COUNT = ORDR + "_OL_COUNT";
static const ColumnName_t O_ALL_LOCAL = ORDR + "_ALL_LOCAL";

static const TableName_t NORD = "NORD";
static const ColumnName_t NO_O_ID = NORD + "_O_ID";
static const ColumnName_t NO_W_ID = NORD + "_W_ID";
static const ColumnName_t NO_D_ID = NORD + "_D_ID";

static const TableName_t ITEM = "ITEM";
static const ColumnName_t I_ID = ITEM + "_ID";
static const ColumnName_t I_IM_ID = ITEM + "_IM_ID";
static const ColumnName_t I_NAME = ITEM + "_NAME";
static const ColumnName_t I_PRICE = ITEM + "_PRICE";
static const ColumnName_t I_DATA = ITEM + "_DATA";

static const TableName_t STOC = "STOC";
static const ColumnName_t S_I_ID = STOC + "_I_ID";
static const ColumnName_t S_W_ID = STOC + "_W_ID";
static const ColumnName_t S_QUANTITY = STOC + "_QUANTITY";
static const ColumnName_t S_DIST_01 = STOC + "_DIST_01";
static const ColumnName_t S_DIST_02 = STOC + "_DIST_02";
static const ColumnName_t S_DIST_03 = STOC + "_DIST_03";
static const ColumnName_t S_DIST_04 = STOC + "_DIST_04";
static const ColumnName_t S_DIST_05 = STOC + "_DIST_05";
static const ColumnName_t S_DIST_06 = STOC + "_DIST_06";
static const ColumnName_t S_DIST_07 = STOC + "_DIST_07";
static const ColumnName_t S_DIST_08 = STOC + "_DIST_08";
static const ColumnName_t S_DIST_09 = STOC + "_DIST_09";
static const ColumnName_t S_DIST_10 = STOC + "_DIST_10";
static const ColumnName_t S_YTD = STOC + "_YTD";
static const ColumnName_t S_ORDER_CNT = STOC + "_ORDER_CNT";
static const ColumnName_t S_REMOTE_CNT = STOC + "_REMOTE_CNT";
static const ColumnName_t S_DATA = STOC + "_DATA";

static const TableName_t ORLI = "ORLI";
static const ColumnName_t OL_O_ID = ORLI + "_O_ID";
static const ColumnName_t OL_D_ID = ORLI + "_D_ID";
static const ColumnName_t OL_W_ID = ORLI + "_W_ID";
static const ColumnName_t OL_NUMBER = ORLI + "_NUMBER";
static const ColumnName_t OL_I_ID = ORLI + "_I_ID";
static const ColumnName_t OL_SUPPLY_W_ID = ORLI + "_SUPPLY_W_ID";
static const ColumnName_t OL_DELIVERY_D = ORLI + "_DELIVERY_D";
static const ColumnName_t OL_QUANTITY = ORLI + "_QUANTITY";
static const ColumnName_t OL_AMOUNT = ORLI + "_AMOUNT";
static const ColumnName_t OL_DIST_INFO = ORLI + "_DIST_INFO";

static const TableName_t HIST = "HIST";
static const ColumnName_t H_C_ID = HIST + "_C_ID";
static const ColumnName_t H_C_D_ID = HIST + "_C_D_ID";
static const ColumnName_t H_C_W_ID = HIST + "_C_W_ID";
static const ColumnName_t H_D_ID = HIST + "_D_ID";
static const ColumnName_t H_W_ID = HIST + "_W_ID";
static const ColumnName_t H_DATE = HIST + "_DATE";
static const ColumnName_t H_AMOUNT = HIST + "_AMOUNT";
static const ColumnName_t H_DATA = HIST + "_DATA";

void do_bench(int argc, char** argv);
void new_order_input(Txn &txn);
void new_order_graph(Txn &txn);
void payment_input(Txn &txn);
void payment_graph(Txn &txn);
void delivery_input(Txn &txn);
void delivery_graph(Txn &txn);
void order_status_input(Txn &txn);
void order_status_graph(Txn &txn);
void stock_level_input(Txn &txn);
void stock_level_graph(Txn &txn);

DatabaseSchema init_schema();