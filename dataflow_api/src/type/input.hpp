#pragma once

#include <map>

#include "value.hpp"

class Values {
   public:
    Values() {}
    Values(const Values &other) {
        elems = other.elems;
    }

    void add(const std::string &name, BuiltInType btype) { elems[name] = Value(btype, name); }
    void add(const std::string &name, Value& value) { elems[name] = value; }
    Value &operator[](const std::string &name) { return elems[name]; }

    // For array of values
    Values reduce(const std::string &method_name) {
        Values res;
        for (auto& pair: elems) {
            res.add(pair.first, pair.second);
        }
        return res;
    }

   private:
    std::map<std::string, Value> elems;
};

class Input {
   public:
    Input() {}
    Input(const Input &other) {
        values = other.values;
        ASSERT(other.array_elems.size() == 0);
    }

    void add(const std::string &name, BuiltInType btype) { values.add(name, btype); }
    void addArray(const std::string &name, Input &input) {
        array_elems[name] = std::move(input);
    }

    Value &operator[](const std::string &name) { return values[name]; }

    Input &getArray(const std::string &name) { return array_elems[name]; }

   private:
    Values values;
    std::map<std::string, Input> array_elems;
};
