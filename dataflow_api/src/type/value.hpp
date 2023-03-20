#pragma once

#include <sys/time.h>

#include <cstdint>
#include <initializer_list>
#include <iostream>
#include <map>
#include <mutex>
#include <sstream>
#include <string>
#include <vector>

#include "graph/node.hpp"

enum BuiltInType { INT, FLOAT, STRING, BOOL, NOPE };

static uint32_t g_value_id = 0;
class Value {
   public:
    Value() : btype(BuiltInType::NOPE) {}
    Value(BuiltInType btype, const std::string &name)
        : value_id(++g_value_id), btype(btype), is_static(true) {
        deps.push_back(new InputNode(name));
    }
    Value(BuiltInType btype, std::initializer_list<Node *> &&nodes, bool is_static)
        : value_id(++g_value_id), btype(btype), deps(std::move(nodes)), is_static(is_static) {}
    Value(BuiltInType btype, const std::vector<Node *> &nodes, bool is_static)
        : value_id(++g_value_id), btype(btype), deps(nodes), is_static(is_static) {}
    Value(const Value &other) : value_id(other.value_id), btype(other.btype), is_static(other.is_static) {
        deps.insert(deps.begin(), other.deps.begin(), other.deps.end());
    }

    std::vector<Node *> &getDeps() { return deps; }

    Value apply(const std::string &method_name, std::initializer_list<Value> &&_values) {
        std::vector<Value> values = std::move(_values);
        std::vector<Node *> temp_deps;
        temp_deps.insert(temp_deps.begin(), deps.begin(), deps.end());
        bool op_static = this->is_static;
        for (Value &value : values) {
            temp_deps.insert(temp_deps.begin(), value.deps.begin(), value.deps.end());
            if (!value.is_static) {
                op_static = false;
            }
        }
        return Value(btype, std::move(temp_deps), false);
    }

    bool isStatic() const { return is_static; }

    uint32_t getId() const {return value_id; }

   private:
    uint32_t value_id;
    BuiltInType btype;
    bool is_static = false;
    std::vector<Node *> deps;
};

using PKey_t = std::vector<Value>;
using TableName_t = std::string;
using ColumnName_t = std::string;