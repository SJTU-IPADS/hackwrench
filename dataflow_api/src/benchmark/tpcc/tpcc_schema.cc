#include "tpcc.hpp"

DatabaseSchema init_schema() {
    DatabaseSchema dbSchema;
    dbSchema.addTable(TableSchema(WARE, false,
                                  {{W_ID, BuiltInType::INT},
                                   {W_NAME, BuiltInType::STRING},
                                   {W_STREET_1, BuiltInType::STRING},
                                   {W_STREET_2, BuiltInType::STRING},
                                   {W_CITY, BuiltInType::STRING},
                                   {W_STATE, BuiltInType::STRING},
                                   {W_ZIP, BuiltInType::STRING},
                                   {W_TAX, BuiltInType::FLOAT},
                                   {W_YTD, BuiltInType::FLOAT}},
                                  {W_ID}, {W_ID}));

    dbSchema.addTable(TableSchema(DIST, false,
                                  {{D_ID, BuiltInType::INT},
                                   {D_W_ID, BuiltInType::INT},
                                   {D_W_NAME, BuiltInType::STRING},
                                   {D_W_STREET1, BuiltInType::STRING},
                                   {D_W_STREET2, BuiltInType::STRING},
                                   {D_CITY, BuiltInType::STRING},
                                   {D_STATE, BuiltInType::STRING},
                                   {D_ZIP, BuiltInType::STRING},
                                   {D_TAX, BuiltInType::FLOAT},
                                   {D_YTD, BuiltInType::FLOAT},
                                   {D_NEXT_O_ID, BuiltInType::INT}},
                                  {D_W_ID, D_ID}, {D_W_ID}));

    dbSchema.addTable(TableSchema(DIST_DELI_INDEX, false,
                                  {{DDI_D_ID, BuiltInType::INT},
                                   {DDI_W_ID, BuiltInType::INT},
                                   {DDI_O_ID, BuiltInType::INT}},
                                  {DDI_W_ID, DDI_D_ID}, {DDI_W_ID}));

    dbSchema.addTable(TableSchema(CUST, false,
                                  {{C_ID, BuiltInType::INT},
                                   {C_W_ID, BuiltInType::INT},
                                   {C_D_ID, BuiltInType::INT},
                                   {C_FIRST, BuiltInType::STRING},
                                   {C_MIDDLE, BuiltInType::STRING},
                                   {C_LAST, BuiltInType::STRING},
                                   {C_STREET1, BuiltInType::STRING},
                                   {C_STREET2, BuiltInType::STRING},
                                   {C_CITY, BuiltInType::STRING},
                                   {C_STATE, BuiltInType::STRING},
                                   {C_ZIP, BuiltInType::STRING},
                                   {C_PHONE, BuiltInType::STRING},
                                   {C_SINCE, BuiltInType::INT},  // DATE
                                   {C_CREDIT, BuiltInType::STRING},
                                   {C_CREDIT_LIM, BuiltInType::FLOAT},
                                   {C_DISCOUNT, BuiltInType::FLOAT},
                                   {C_BALANCE, BuiltInType::FLOAT},
                                   {C_YTD_PAYMENT, BuiltInType::FLOAT},
                                   {C_PAYMENT_CNT, BuiltInType::FLOAT},
                                   {C_DELIVERY_CNT, BuiltInType::FLOAT},
                                   {C_DATA, BuiltInType::STRING}},
                                  {C_W_ID, C_D_ID, C_ID}, {C_W_ID}));

    dbSchema.addTable(TableSchema(CUST_INDEX, false,
                                  {{CI_C_ID, BuiltInType::INT},
                                   {CI_W_ID, BuiltInType::INT},
                                   {CI_D_ID, BuiltInType::INT},
                                   {CI_LAST_ORDER, BuiltInType::INT}},
                                  {C_W_ID, C_D_ID, CI_C_ID}, {C_W_ID}));

    dbSchema.addTable(TableSchema(ORDR, false,
                                  {{O_ID, BuiltInType::INT},
                                   {O_D_ID, BuiltInType::INT},
                                   {O_W_ID, BuiltInType::INT},
                                   {O_C_ID, BuiltInType::INT},
                                   {O_ENTRY_D, BuiltInType::INT},  // DATE
                                   {O_CARRIER_ID, BuiltInType::INT},
                                   {O_OL_COUNT, BuiltInType::FLOAT},
                                   {O_ALL_LOCAL, BuiltInType::FLOAT}},
                                  {O_W_ID, O_D_ID, O_ID}, {O_W_ID}));

    dbSchema.addTable(TableSchema(
        NORD, false, {{NO_O_ID, BuiltInType::INT}, {NO_W_ID, BuiltInType::INT}, {NO_D_ID, BuiltInType::INT}},
        {NO_W_ID, NO_D_ID, NO_O_ID}, {NO_W_ID}));

    dbSchema.addTable(TableSchema(ITEM, true,
                                  {{I_ID, BuiltInType::INT},
                                   {I_IM_ID, BuiltInType::INT},
                                   {I_NAME, BuiltInType::STRING},
                                   {I_PRICE, BuiltInType::FLOAT},
                                   {I_DATA, BuiltInType::STRING}},
                                  {I_ID}, {}));

    dbSchema.addTable(TableSchema(STOC, false,
                                  {{S_I_ID, BuiltInType::INT},
                                   {S_W_ID, BuiltInType::INT},
                                   {S_QUANTITY, BuiltInType::INT},
                                   {S_DIST_01, BuiltInType::STRING},
                                   {S_DIST_02, BuiltInType::STRING},
                                   {S_DIST_03, BuiltInType::STRING},
                                   {S_DIST_04, BuiltInType::STRING},
                                   {S_DIST_05, BuiltInType::STRING},
                                   {S_DIST_06, BuiltInType::STRING},
                                   {S_DIST_07, BuiltInType::STRING},
                                   {S_DIST_08, BuiltInType::STRING},
                                   {S_DIST_09, BuiltInType::STRING},
                                   {S_DIST_10, BuiltInType::STRING},
                                   {S_YTD, BuiltInType::FLOAT},
                                   {S_ORDER_CNT, BuiltInType::INT},
                                   {S_REMOTE_CNT, BuiltInType::INT},
                                   {S_DATA, BuiltInType::STRING}},
                                  {S_W_ID, S_I_ID}, {S_W_ID}));

    dbSchema.addTable(TableSchema(ORLI, false,
                                  {{OL_O_ID, BuiltInType::INT},
                                   {OL_D_ID, BuiltInType::INT},
                                   {OL_W_ID, BuiltInType::INT},
                                   {OL_NUMBER, BuiltInType::INT},
                                   {OL_I_ID, BuiltInType::INT},
                                   {OL_SUPPLY_W_ID, BuiltInType::INT},
                                   {OL_DELIVERY_D, BuiltInType::INT}, // DATE
                                   {OL_QUANTITY, BuiltInType::INT},
                                   {OL_AMOUNT, BuiltInType::FLOAT},
                                   {OL_DIST_INFO, BuiltInType::STRING}},
                                  {OL_W_ID, OL_D_ID, OL_O_ID, OL_NUMBER}, {OL_W_ID}));

    dbSchema.addTable(TableSchema(HIST, false,
                                  {{H_C_ID, BuiltInType::INT},
                                   {H_C_D_ID, BuiltInType::INT},
                                   {H_C_W_ID, BuiltInType::INT},
                                   {H_D_ID, BuiltInType::INT},
                                   {H_W_ID, BuiltInType::INT},
                                   {H_DATE, BuiltInType::INT}, //DATE
                                   {H_AMOUNT, BuiltInType::FLOAT},
                                   {H_DATA, BuiltInType::STRING}},
                                  {H_C_W_ID, H_C_D_ID, H_C_ID, H_W_ID, H_D_ID}, {H_W_ID}));
    return dbSchema;
}