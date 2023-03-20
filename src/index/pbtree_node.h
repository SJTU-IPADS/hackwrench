#pragma once

#include "interface.h"
#include "record_lock.h"
#include "storage/multi_ver_record.h"
#include "storage/page.h"
#include "util/types.h"


inline constexpr slot_t get_max_leaf_slots(uint32_t record_size) {
    return (PAGE_SIZE - 32) / (sizeof(slot_t) + record_size);
}

inline constexpr uint32_t get_slots_padding(uint32_t max_leaf_slots) {
    return max_leaf_slots % 2;
}

struct LeafMeta {
    ts_t ts;
    uint32_t value_size;
    uint32_t record_size;
    
    slot_t max_leaf_slots;
    // Number of key num_slots use, so the number of valid children or data pointers
    slot_t num_slots;
    // Number of actual <key,value> pair stored
    // Currently we do not support remove
    slot_t num_pairs;

    uint32_t num_padding_slots;
    slot_t offsets[0];

    slot_t get_offset(slot_t i) {
        return offsets[i];
    }

    uint8_t* get_record_ptr(slot_t off) {
        uint8_t* ptr = reinterpret_cast<uint8_t*>(offsets) + 
            (num_padding_slots) * sizeof(slot_t);
        return ptr + record_size * off;
    }
};

static_assert(sizeof(LeafMeta) == 32);
template <class Key_t, class Value_t, class Compare = std::less<Key_t>>
struct btree_default_traits {
    typedef Key_t key_type;
    typedef Value_t value_type;
    typedef MultiVersionRecord record_type;
    typedef Compare key_compare;
    static const slot_t leaf_slots = get_max_leaf_slots(get_record_size(sizeof(value_type)));
};

template <class Traits>
struct PBtreeLeafNode {
    typedef Traits traits_type;
    typedef typename traits_type::record_type record_type;
    typedef typename traits_type::value_type value_type;
    static const slot_t max_leaf_slots = traits_type::leaf_slots;

    LeafMeta meta;

    // Array of (key, data) pairs
    slot_t offsets[max_leaf_slots + get_slots_padding(traits_type::leaf_slots)];
    uint8_t ptr[0];

    inline record_type& get_pair(slot_t off) {
        uint8_t* record_ptr = ptr + meta.record_size * off;
        return *reinterpret_cast<record_type*>(record_ptr);
    }

    // Set the (key,data) pair in slot.
    // inline void set_key_value(slot_t slot, const Key_t& key, const Value_t& value) {
    //     ASSERT(slot < num_slots);
    //     keys[slot] = key;
    //     offsets[slot] = value;
    // }

    // inline void left_shift_to(slot_t src_slot, slot_t dest_slot) {
    //     std::copy(keys + src_slot, keys + num_slots, keys + dest_slot);
    //     std::copy(offsets + src_slot, offsets + num_slots, offsets + dest_slot);
    // }

    inline void right_shift(slot_t src_slot, slot_t shift_num = 1) {
        std::copy_backward(offsets + src_slot, offsets + meta.num_slots,
                           offsets + meta.num_slots + shift_num);
    }

    //! True if the node's slots are full.
    inline bool is_full() const { return (meta.num_slots == max_leaf_slots); }
};

template <class Key_t, class Value_t>
class LocalPBtreeNode : public IndexInterface<Key_t, Value_t> {
    typedef Key_t key_type;
    typedef Value_t value_type;
    typedef btree_default_traits<key_type, value_type> traits_type;
    typedef typename traits_type::record_type record_type;
    typedef typename traits_type::key_compare key_compare;
    typedef PBtreeLeafNode<traits_type> LeafNode;
   public:
    static const slot_t max_leaf_slots = traits_type::leaf_slots;

    static_assert(sizeof(LeafNode) + get_record_size(sizeof(value_type)) * max_leaf_slots <= PAGE_SIZE);

    IndexInterface<Key_t, Value_t> &get_leaf_node(const key_type &key) override {
        ASSERT(false);
        return *this;
    }

    std::vector<IndexInterface<Key_t, Value_t> *> get_leaf_nodes(const key_type &lo,
                                                                 const key_type &hi) override {
        ASSERT(false);
        return {};
    }

    PageMeta *get_page_meta() override { return page_meta; }

    void set_page_meta(PageMeta *page_meta) {
        this->page_meta = page_meta;
        page_meta->cur_page_size = sizeof(LeafNode);
    }

    void initialize(uint32_t value_size) {
        uint32_t record_size = get_record_size(value_size);
        ASSERT(sizeof(record_type) + value_size == record_size);
        ASSERT(sizeof(value_type) == value_size);
        // ASSERT(sizeof(record_type) + value_size == record_size);
        LeafNode *node = get_node();
        node->meta.ts = 0;
        node->meta.value_size = value_size;
        node->meta.record_size = record_size;
        ASSERT(get_max_leaf_slots(record_size) == max_leaf_slots);
        node->meta.max_leaf_slots = max_leaf_slots;
        node->meta.num_padding_slots = max_leaf_slots+get_slots_padding(max_leaf_slots);
        node->meta.num_slots = 0;
        node->meta.num_pairs = 0;
    }

    slot_t num_slots() const { return get_node()->meta.num_slots; }

    uint64_t byte_size() const { return page_meta->cur_page_size; }

    void update_byte_size() {
        LeafNode *node = get_node();
        page_meta->cur_page_size = sizeof(LeafNode) + 
            node->meta.num_pairs * node->meta.record_size;
    }

    LeafNode *get_node() const { return reinterpret_cast<LeafNode *>(page_meta->get_data()); }

    LeafNode *get_node(uint8_t *data) const {
        return reinterpret_cast<LeafNode *>(data ? data : page_meta->get_data());
    }

    static LeafNode *node_of(uint8_t *data) {
        return reinterpret_cast<LeafNode *>(data);
    }
   private:
    PageMeta *page_meta = nullptr;

   public:
    // Temprarily, the Key/Value interface
    // TODO(DZY): implement a complete PBtree on Database side

    record_type *get(const key_type &key, uint8_t *data = nullptr) override {
        LeafNode *node = get_node(data);
        slot_t slot = find_lower(node, key);

        ASSERT(slot >= 0 && slot <= node->meta.num_slots);
        slot_t offset = node->offsets[slot];
        record_type &pair = get_pair_ref(node, offset);
        if (key_equal(key, pair.key)) {
            return &pair;
        } else {
            // LOG(2) << key << " " << pair.key;
            return nullptr;
        }
    }

    // return all key and values in the range [lo, hi]
    std::vector<record_type *> scan_oneshot(const key_type &lo, const key_type &hi,
                                            bool lo_inclusive = true, bool hi_inclusive = true,
                                            uint8_t *data = nullptr) {
        std::vector<record_type *> results;
        LeafNode *node = get_node(data);
        // slot <= lo
        slot_t slot = find_lower(node, lo);
        if (!in_range(node, slot, lo, hi))
            slot++;

        while (slot < node->meta.num_slots && 
                in_range(node, slot, lo, hi)) {
            results.push_back(&get_pair_ref(node, slot));
            slot++;
        }
        return results;
    }

    record_type *insert(const key_type &key, const value_type &value, txn_id_t writer) {
        LeafNode *node = get_node();
        slot_t slot = find_lower(node, key);

        if (node->is_full()) {
            ASSERT(false) << "Currently do not support split";
        }

        // move items and put data item into correct data slot
        ASSERT(slot >= 0 && slot <= node->meta.num_slots);

        node->right_shift(slot);

        node->meta.num_slots++;
        slot_t offset = node->offsets[slot] = append_new_value(node);

        record_type &pair = get_pair_ref(node, offset);

        // pair.last_version =
        //     MultiVersionRecord<key_type, value_type>::make_version(key, value, writer);
        pair.lock.init();
        // pair.lock.key = key;
        pair.key = key;
        pair.set_value(&value, sizeof(value_type));
        pair.writer = writer;
        return &pair;
    }

    bool put(const key_type &key, const value_type &value, txn_id_t writer) {
        record_type *pair = get(key);
        if (pair == nullptr) {
            print_trace();
            ASSERT(false) << " " << key << " " << max_leaf_slots;
            return false;
        }
        // ASSERT(pair->value.current_size == 0) << key << " " << pair->value;  // for tpcc
        pair->set_value(&value, sizeof(value_type));
        pair->writer = writer;
        return true;
    }

   private:
    slot_t append_new_value(LeafNode *node) {
        slot_t value_slot = node->meta.num_pairs++;
        ASSERT(node->meta.num_pairs == node->meta.num_slots);
        page_meta->cur_page_size += node->meta.record_size;
        return value_slot;
    }

    slot_t find_lower(LeafNode *node, const key_type &key) const {
        slot_t lo = 0, hi = node->meta.num_slots;

        if (hi == 0)
            return 0;

        while (lo < hi) {
            slot_t mid = (lo + hi) >> 1;

            if (key_lessequal(key, get_key_ref(node, mid))) {
                hi = mid;  // key <= mid
            } else {
                lo = mid + 1;  // key > mid
            }
        }

        return lo;
    }

    // True if a < b ? "constructed" from key_less_()
    inline static bool key_less(const key_type &a, const key_type &b) {
        key_compare key_less_;
        return key_less_(a, b);
    }

    // True if a <= b ? constructed from key_less()
    inline static bool key_lessequal(const key_type &a, const key_type &b) {
        return !key_less(b, a);
    }

    inline static bool key_equal(const key_type &a, const key_type &b) {
        return !key_less(a, b) && !key_less(b, a);
    }

    inline static bool key_ge(const key_type &a, const key_type &b) { return !key_less(a, b); }
    inline static bool key_greater(const key_type &a, const key_type &b) {
        return !key_lessequal(a, b);
    }

    inline bool in_range(LeafNode *node, slot_t slot, const key_type &lo, const key_type &hi) {
        const key_type &key = get_key_ref(node, slot);
        return lo <= key && key < hi;
    }

    template <class Key, class Value>
    friend std::ostream &operator<<(std::ostream &os, const LocalPBtreeNode<Key, Value> &node);

    void print() override {
        std::cout << *this << std::endl;
    }

   private:
    inline record_type &get_pair_ref(LeafNode *node, slot_t offset) { 
        return node->get_pair(offset); 
    }
    inline const record_type &get_pair_ref(LeafNode *node, slot_t offset) const {
        return node->get_pair(offset);
    }

    // Some minor helper functions
    inline const key_type &get_key_ref(LeafNode *node, slot_t slot) const {
        slot_t offset = node->offsets[slot];
        return get_pair_ref(node, offset).key;
    }

    inline const value_type &get_value_ref(LeafNode *node, slot_t slot) const {
        slot_t offset = node->offsets[slot];
        return *reinterpret_cast<const value_type*>(get_pair_ref(node, offset).value);
    }
};

template <class Key, class Value>
inline std::ostream &operator<<(std::ostream &os, const LocalPBtreeNode<Key, Value> &n) {
    os << "--------<Num_slots " << n.num_slots() << ">------------" << std::endl;
    os << "keys: ";
    for (slot_t slot_i = 0; slot_i < n.num_slots(); ++slot_i) {
        os << n.get_key_ref(n.get_node(), slot_i) << " ";
    }
    os << std::endl;
    // os << "values: ";
    // for (slot_t slot_i = 0; slot_i < n.num_slots(); ++slot_i) {
    //     os << n.get_value_ref(n.get_node(), slot_i) << " ";
    // }
    return os;
}