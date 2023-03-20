#include "node.hpp"

#include "api/schema.hpp"

void OpNode::printNode(std::ostream &out) {
    TableName_t tbl_name = tbl_schema.getTableName();
    std::stringstream ss;
    ss << type_name << " " << tbl_name;
    // ss << " Scope: " << scope_id;
    // ss << " Part: " << part_id;
    ss << "\nstaticKey: " << (static_key ? "true" : "false");
    ss << "\nlocalTable: " << (tbl_schema.isLocalTable() ? "true" : "false");
    out << id << " [label=\"" << ss.str() << "\"";
    out << ", color=" << (part_id == partition_affinity ? "red" : "blue");
    out << "] # " << ref_count << ", " << tbl_name << std::endl;
}