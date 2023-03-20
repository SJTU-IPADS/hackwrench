#pragma once

#include <memory>

#include "index/record_lock.h"
#include "util/types.h"

template <class Key_t, class Value_t>
class Record {
   public:
    Key_t key;
    Value_t value;
    txn_id_t writer;  // NULL_TXN_ID means it's a global version.

    Record(Key_t key, Value_t value, txn_id_t writer) : key(key), value(value), writer(writer) {}
};

struct SimpleRecord {
    uint8_t* v;
    version_ts_t writer;
    offset_t page_offset;
};

struct MultiVersionRecord {
    RecordLock lock;
    txn_id_t writer;  // NULL_TXN_ID means initial value
    Key_t key;
    // Val_t value;
    uint8_t value[0];

    MultiVersionRecord() : writer(NULL_TXN_ID), key(0) {}

    inline void set_value(const void* value_ptr, uint32_t value_size) {
        memcpy(value, value_ptr, value_size);
    }
};

inline RecordLock* get_record_lock(void* ptr) {
    return &(reinterpret_cast<MultiVersionRecord*>(ptr)->lock);
}

inline void set_writer(void* ptr, txn_id_t writer) {
    reinterpret_cast<MultiVersionRecord*>(ptr)->writer = writer;
}

inline void set_key(void* ptr, uint8_t* key_ptr) {
    reinterpret_cast<MultiVersionRecord*>(ptr)->key = 
        *reinterpret_cast<Key_t*>(key_ptr);
}

inline uint8_t* r_get_key_ptr(void* ptr) {
    auto* p = &reinterpret_cast<MultiVersionRecord*>(ptr)->key;
    return reinterpret_cast<uint8_t*>(p);
}

inline Key_t get_key(void* ptr) {
    return reinterpret_cast<MultiVersionRecord*>(ptr)->key;
}

inline Key_t r_get_key(void* ptr) {
    return reinterpret_cast<MultiVersionRecord*>(ptr)->key;
}

inline txn_id_t r_get_writer(void* ptr) {
    return reinterpret_cast<MultiVersionRecord*>(ptr)->writer;
}

inline uint8_t* r_get_value(void* ptr) {
    return reinterpret_cast<MultiVersionRecord*>(ptr)->value;
}

inline void set_value(void* ptr, uint8_t* value_ptr, uint32_t value_size) {
    reinterpret_cast<MultiVersionRecord*>(ptr)->set_value(value_ptr, value_size);
}

inline void r_copy(void* to_ptr, void* from_ptr, uint32_t value_size) {
    auto *from = reinterpret_cast<MultiVersionRecord*>(from_ptr);
    auto *to = reinterpret_cast<MultiVersionRecord*>(to_ptr);
    to->writer = from->writer;
    to->key = from->key;
    to->set_value(from->value, value_size);
}

inline constexpr uint64_t get_record_size(uint32_t value_size) {
    return sizeof(MultiVersionRecord) + value_size;
}

// template <class Key_t, class Value_t>
// class MultiVersionRecord {
//     typedef Record<Key_t, Value_t> record_t;
//    public:
//     Key_t key;
//     txn_id_t writer;  // NULL_TXN_ID means initial value
//     RecordLock lock;
//     Value_t value;
//     // std::shared_ptr<record_t> last_version;

//     MultiVersionRecord()
//         : key(0),
//           value(0),
//           writer(NULL_TXN_ID) {}
    
//     // last_version(std::make_shared<record_t>(0, 0, 0))
//     // inline void update_value(std::shared_ptr<record_t> new_record) { last_version = new_record; }
//     // inline std::shared_ptr<record_t> get_last_version() { return last_version; }

//     // inline static std::shared_ptr<record_t> make_version(Key_t key, Value_t value,
//     //                                                      txn_id_t writer) {
//     //     return std::make_shared<record_t>(key, value, writer);
//     // }
// };