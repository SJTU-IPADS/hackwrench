#pragma once

#include <deque>
#include <fstream>
#include <functional>
#include <set>

#include "graph/node.hpp"
#include "schema.hpp"
#include "type/input.hpp"
#include "type/value.hpp"
#include "type/row.hpp"
#include "util/logging.h"

class Txn;
class Input;
using IterationLogic = std::function<Values(Txn &txn, Input &input, Value &loop_num)>;

enum ScopeType { LOOP = 0, IF_BRANCH, ELSE_BRANCH };

static const char *ScopeStrings[] = {"Loop", "IfBranch", "ElseBranch"};
class Txn {
   public:
    Txn(DatabaseSchema &db_schema) : db_schema(db_schema) { scopeTrace.push_back(scope_id); }

    Row get(const TableName_t &table_name, std::initializer_list<Value> &&pkey_il) {
        TableSchema &tbl_schema = db_schema.getTable(table_name);
        PKey_t pkey = std::move(pkey_il);

        uint32_t part_id = 0;
        if (tbl_schema.isPartitionable()) {
            Value &value = pkey[tbl_schema.getPartPkeyIndex()];
            if (value.isStatic()) {
                part_id = value.getId();
            }
        }
        if (tbl_schema.isReadOnly()) {
            part_id = partition_affinity;
        }

        GetNode *get_node =
            new GetNode(tbl_schema, scope_id, part_id, partition_affinity, isStaticKey(pkey));
        for (Value &value : pkey) {
            get_node->addDeps(value.getDeps(), Node::KeyDep);
        }
        for (Value &value : depValues) {
            get_node->addDeps(value.getDeps(), Node::CtrlDep);
        }
        addNode(get_node);
        return Row(get_node, tbl_schema);
    }

    void put(const TableName_t &table_name, std::initializer_list<Value> &&pkey_il, Row &row) {
        TableSchema &tbl_schema = db_schema.getTable(table_name);
        PKey_t pkey = std::move(pkey_il);

        uint32_t part_id = 0;
        if (tbl_schema.isPartitionable()) {
            Value &value = pkey[tbl_schema.getPartPkeyIndex()];
            if (value.isStatic()) {
                part_id = value.getId();
            }
        }
        tbl_schema.access(part_id == partition_affinity);

        PutNode *put_node =
            new PutNode(tbl_schema, scope_id, part_id, partition_affinity, isStaticKey(pkey));
        for (Value &value : pkey) {
            put_node->addDeps(value.getDeps(), Node::KeyDep);
        }
        for (Value &value : depValues) {
            put_node->addDeps(value.getDeps(), Node::CtrlDep);
        }

        row.assignDepsTo(put_node);
        addNode(put_node);
    }

    Values map(IterationLogic iter_logic, Input &input, Value &loop_count) {
        pushScope(ScopeType::LOOP, loop_count);
        Values res = iter_logic(*this, input, loop_count);
        popScope();

        return res;
    }


    template<class Logic, class In>
    Values map(Logic iter_logic, Input &input, Value &loop_count) {
        pushScope(ScopeType::LOOP, loop_count);
        Values res = iter_logic(*this, input, loop_count);
        popScope();

        return res;
    }

    void beginIf(Value &value) { pushScope(ScopeType::IF_BRANCH, value); }

    void endIf() { popScope(); }

    void setPartitionAffinity(Value &value) {
        ASSERT(value.isStatic());
        partition_affinity = value.getId();
    }

    Row alloc(const TableName_t &table_name) {
        TableSchema &tbl_schema = db_schema.getTable(table_name);
        return Row(nullptr, tbl_schema);
    }

    void commit() {}

    void abort() {}

    Input &getInput() { return input; }


    void print_graph(const std::string &fileName) {
        std::ofstream out(fileName.c_str());
        std::set<uint32_t> usedScopes;

        out << "digraph G {" << std::endl;
        out << "label=\"" << fileName << "\"" << std::endl;
        print_subgraph(out, 0);
        for (Node *node : nodes) {
            if (node->getRefCount() == 0) {
                node->printDebugInfo(out);
            }
        }
        out << "}" << std::endl;
    }

   private:
    bool isStaticKey(const PKey_t &pkey) {
        bool is_static = true;
        for (const Value &value : pkey) {
            if (!value.isStatic()) {
                is_static = false;
            }
        }
        return is_static;
    }

    void pushScope(ScopeType scopeType, Value &value) {
        uint32_t temp_loop_id = ++scope_id;
        uint32_t cur_scope_id = scopeTrace.back();
        scopeCallGraph[cur_scope_id].push_back(temp_loop_id);
        scopeTrace.push_back(temp_loop_id);
        scopeToValues[temp_loop_id] = scopeType;
        depValues.push_back(value);
    }

    void popScope() {
        scopeTrace.pop_back();
        depValues.pop_back();
    }

    void print_subgraph(std::ostream &out, uint32_t scope) {
        if (scope != 0) {
            out << "subgraph cluster_" << scope << "{" << std::endl;
            out << "color=black" << std::endl;
            out << "label = \"" << ScopeStrings[scopeToValues[scope]] << "#" << scope << "\""
                << std::endl;
        }
        for (Node *node : scopeToNodes[scope]) {
            node->printNode(out);
        }
        for (uint32_t sub_scope : scopeCallGraph[scope]) {
            print_subgraph(out, sub_scope);
        }
        if (scope != 0) {
            out << "}" << std::endl;
        }
    }

    inline void addNode(Node *node) {
        nodes.push_back(node);
        scopeToNodes[scopeTrace.back()].push_back(node);
    }

   private:
    DatabaseSchema &db_schema;
    Input input;
    uint32_t part_key_id = 0;
    std::vector<Node *> nodes;
    std::map<uint32_t, std::vector<Node *>> scopeToNodes;
    std::map<uint32_t, ScopeType> scopeToValues;
    std::map<uint32_t, std::vector<uint32_t>> scopeCallGraph;
    std::deque<uint32_t> scopeTrace;

    uint32_t scope_id = 0;
    std::deque<Value> depValues;

    uint32_t partition_affinity;
};