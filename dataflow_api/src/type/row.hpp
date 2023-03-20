#pragma once

#include "graph/node.hpp"

class Row {
   public:
    Row(Node *node, TableSchema &tbl_schema) : node(node), tbl_schema(tbl_schema) {}

    Node *getNode() { return node; }

    Value getColumn(const ColumnName_t &col_name) {
        BuiltInType btype = tbl_schema.getBuiltInType(col_name);
        return Value(btype, {node}, false);
    }

    Value isFound() {
        return Value{BuiltInType::BOOL, {node}, false};
    }

    void setColumn(const ColumnName_t &col_name, Value &value) {
        std::vector<Node *> &others = value.getDeps();
        std::vector<Node *> &mine = col_deps[col_name];
        mine.clear();
        for (Node *other : others) {
            if (node && node == other) {
                continue;
            }
            mine.push_back(other);
        }
    }

    void assignDepsTo(Node *n) {
        n->addDep(node, Node::ValueDep);
        for (auto &pair : col_deps) {
            n->addDeps(pair.second, Node::ValueDep);
        }
    }

   private:
    Node *node = nullptr;
    std::map<ColumnName_t, std::vector<Node *>> col_deps;
    TableSchema &tbl_schema;
};