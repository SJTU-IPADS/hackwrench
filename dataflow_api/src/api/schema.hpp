#pragma once

#include <initializer_list>
#include <map>
#include <vector>

#include "type/value.hpp"

using NameType = std::pair<std::string, BuiltInType>;

class DatabaseSchema;
class TableSchema {
   public:
    TableSchema() {}
    TableSchema(TableSchema &&other) { *this = std::move(other); }
    TableSchema &operator=(TableSchema &&other) {
        tbl_name = other.tbl_name;
        columns.swap(other.columns);
        pkey.swap(other.pkey);
        part_key = other.part_key;
        read_only = other.read_only;
        partitionable = other.partitionable;
        is_local = other.is_local;
        return *this;
    }
    TableSchema(TableName_t tbl_name, bool read_only, std::initializer_list<NameType> &&cols,
                std::initializer_list<ColumnName_t> &&pkey_il, ColumnName_t part_key)
        : tbl_name(tbl_name), pkey(std::move(pkey_il)), part_key(part_key), read_only(read_only) {
        for (const NameType &pair : cols) {
            columns[pair.first] = pair.second;
        }

        partitionable = !part_key.empty();
        if (partitionable) {
            for (const ColumnName_t &name : pkey) {
                if (name == part_key) {
                    break;
                }
                part_pkey_index += 1;
            }
        }
    }

    void access(bool local_op) {
        if (!local_op) {
            is_local = false;
        }
    }

    bool isPartitionable() { return partitionable; }

    bool isReadOnly() { return read_only; }

    bool isLocalTable() { return is_local; }

    uint32_t getPartPkeyIndex() { return part_pkey_index; }

    BuiltInType getBuiltInType(const ColumnName_t &col_name) { return columns.at(col_name); }

    TableName_t getTableName() const { return tbl_name; }

   private:
    TableName_t tbl_name;
    std::map<std::string, BuiltInType> columns;
    std::vector<ColumnName_t> pkey;
    ColumnName_t part_key;         // partition ley
    uint32_t part_pkey_index = 0;  // the index of partition key in pkey
    bool read_only = false;
    bool partitionable = false;
    bool is_local = true;

    friend class DatabaseSchema;
};

class DatabaseSchema {
   public:
    DatabaseSchema() {}

    void addTable(TableSchema &&tbl_schema) { tbls[tbl_schema.tbl_name] = std::move(tbl_schema); }

    TableSchema &getTable(TableName_t tbl_name) { return tbls[tbl_name]; }

   private:
    std::map<TableName_t, TableSchema> tbls;
};