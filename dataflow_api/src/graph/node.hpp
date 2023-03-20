#pragma once

#include <sstream>
#include <vector>

#include "util/logging.h"

static uint32_t g_node_id = 0;

static std::string depsTostyle[] = {
    "solid",
    "dashed",
    "dotted"
};

class TableSchema;
class Node {
   public:
    enum DepType { KeyDep = 0, ValueDep, CtrlDep };
    Node() : id(++g_node_id) {}

    ~Node() {
        for (auto &pair : deps) {
            Node *node = pair.first;
            if (node && --node->ref_count == 0) {
                delete node;
            }
        }
    }

    inline void addDeps(std::vector<Node *> &nodes, DepType dep_type) {
        for (Node *node : nodes) {
            addDep(node, dep_type);
        }
    }

    inline void addDep(Node *dep, DepType dep_type) {
        if (dep == nullptr)
            return;
        if (dep == this)
            return;
        ++dep->ref_count;
        deps.push_back({dep, dep_type});
    }

    // inline std::vector<Node *> &getDeps() { return deps; }

    uint32_t getRefCount() const { return ref_count; }

    virtual bool isDbOp() { return false; }

    virtual void printNode(std::ostream &out) {}
    virtual void printDebugInfo(std::ostream &out) {
        if (printed)
            return;
        printed = true;

        for (auto &pair : deps) {
            Node *node = pair.first;
            if (!node->isDbOp())
                continue;
            node->printDebugInfo(out);
        }

        for (auto &pair : deps) {
            Node *node = pair.first;
            if (!node->isDbOp())
                continue;
            out << node->id << "->" << id << " ";
            out << "[style=" << depsTostyle[pair.second] << "]";
            out << std::endl;
        }
    }

   protected:
    void printNode(std::ostream &out, const std::string &nodeType, const std::string &comment) {
        out << id << " [label=\"" << nodeType << "\"] # " << ref_count << ", " << comment
            << std::endl;
    }

    uint32_t id = 0;
    uint32_t ref_count = 0;
    std::vector<std::pair<Node *, DepType>> deps;
    bool printed = false;
};

class OpNode : public Node {
   public:
    OpNode(const std::string &type_name, TableSchema &tbl_schema, uint32_t scope_id,
           uint32_t part_id, uint32_t partition_affinity, bool static_key)
        : type_name(type_name),
          tbl_schema(tbl_schema),
          scope_id(scope_id),
          part_id(part_id),
          partition_affinity(partition_affinity),
          static_key(static_key) {}

    virtual void printNode(std::ostream &out);

    virtual bool isDbOp() { return true; }

   private:
    TableSchema &tbl_schema;
    const std::string type_name;
    uint32_t scope_id, part_id, partition_affinity;
    bool static_key;
};
class GetNode : public OpNode {
   public:
    GetNode(TableSchema &tbl_schema, uint32_t scope_id, uint32_t part_id,
            uint32_t partition_affinity, bool static_key)
        : OpNode("Get", tbl_schema, scope_id, part_id, partition_affinity, static_key) {}
};

class PutNode : public OpNode {
   public:
    PutNode(TableSchema &tbl_schema, uint32_t scope_id, uint32_t part_id,
            uint32_t partition_affinity, bool static_key)
        : OpNode("Put", tbl_schema, scope_id, part_id, partition_affinity, static_key) {}
};
class InputNode : public Node {
   public:
    InputNode(const std::string &name) : Node(), name(name) {}
    virtual void printNode(std::ostream &out) { Node::printNode(out, "Input", name); }

   private:
    std::string name;
};
