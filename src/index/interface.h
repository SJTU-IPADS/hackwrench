#pragma once

#include "record_lock.h"
#include "storage/multi_ver_record.h"
// #include "index/scanlist.h"
#include "storage/page.h"

template <class Key_t, class Value_t>
class IndexInterface {
    typedef Key_t key_type;
    typedef Value_t value_type;
    typedef MultiVersionRecord record_type;

   protected:
    // RecordLockManager<Key_t> record_locks;

   public:
    virtual ~IndexInterface() {}

    virtual IndexInterface<Key_t, Value_t> &get_leaf_node(const key_type &key) = 0;
    virtual std::vector<IndexInterface<Key_t, Value_t> *> get_leaf_nodes(const key_type &lo,
                                                                         const key_type &hi) = 0;

    virtual PageMeta *get_page_meta() = 0;

    virtual record_type *get(const key_type &key, uint8_t *data = nullptr) = 0;

    virtual record_type *insert(const key_type &key, const value_type &value, txn_id_t writer) = 0;

    virtual std::vector<record_type *> scan_oneshot(const key_type &lo, const key_type &hi,
                                                    bool lo_inclusive, bool hi_inclusive,
                                                    uint8_t *data = nullptr) = 0;

    uint32_t get_table_id() const { return table_id; }

    uint32_t set_table_id(uint32_t table_id) { return this->table_id = table_id; }

    virtual void print() {}

    uint32_t table_id;
};