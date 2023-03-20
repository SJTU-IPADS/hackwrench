#pragma once

#include "interface.h"
#include "pbtree_node.h"

// #define BITS_PER_TABLE 5

template <class Key_t, class Value_t>
struct PBtree : public IndexInterface<Key_t, Value_t> {
    typedef btree_default_traits<Key_t, Value_t> traits_type;
    typedef Key_t key_type;
    typedef Value_t value_type;
    typedef LocalPBtreeNode<key_type, value_type> leaf_type;
    typedef typename std::pair<key_type, leaf_type *> inner_pair_type;
    typedef MultiVersionRecord record_type;
    typedef typename traits_type::key_compare key_compare;
    static const slot_t max_inner_slots = 1 << 23;

    // Number of key num_slots use, so the number of valid children or data pointers
    slot_t num_slots = 0, max_num_pairs;

    // Array of (key, leafnode*) pairs
    // (0, leaf0), (1, leaf1)
    inner_pair_type *pairs;

    // True if the node's slots are full.
    inline bool is_full() const { return (num_slots == max_num_pairs); }

   public:
    void init(uint32_t max_num_pairs = max_inner_slots) {
        pairs = new inner_pair_type[max_num_pairs];
        this->max_num_pairs = max_num_pairs;
    }

    ~PBtree() {
        if(pairs) {
            delete [] pairs;
        }
    }

    IndexInterface<Key_t, Value_t> &get_leaf_node(const key_type &key) override {
        slot_t s = get_leaf_slot(key);
        ASSERT(s >= 0 && s < max_num_pairs) << s << " " << max_num_pairs;
        ASSERT(pairs[s].second != nullptr) << key;
        return *pairs[s].second;
    }

    std::vector<IndexInterface<Key_t, Value_t> *> get_leaf_nodes(const key_type &lo,
                                                                 const key_type &hi) {
        slot_t s = get_leaf_slot(lo);
        if (s > 1) s--;
        slot_t s1 = get_leaf_slot(hi);
        // ASSERT((uint64_t)s == key / 64) << key << ", s: " << s;
        std::vector<IndexInterface<Key_t, Value_t> *> result;
        for (; s <= s1; s++) {
            ASSERT(s >= 0 && s < max_num_pairs && pairs[s].second != nullptr);
            result.push_back(pairs[s].second);
        }
        return result;
    }

    PageMeta *get_page_meta() override {
        ASSERT(false);
        return nullptr;
    }

    record_type *get(const key_type &key, uint8_t *data = nullptr) override {
        slot_t s = get_leaf_slot(key);
        if (s < 0 || s >= max_num_pairs || pairs[s].second == nullptr) {
            return nullptr;
        }
        leaf_type *leaf = pairs[s].second;
        return leaf->get(key, data);
    }

    std::vector<record_type *> scan_oneshot(const key_type &lo, const key_type &hi,
                                            bool lo_inclusive = true, bool hi_inclusive = true,
                                            uint8_t *data = nullptr) {
        ASSERT(false);
        std::vector<record_type *> results;
        return results;
    }

    record_type *insert(const key_type &key, const value_type &value, txn_id_t writer) override {
        leaf_type *leaf = pairs[get_leaf_slot(key)].second;
        return leaf->insert(key, value, writer);
    }

    void insert_range(const key_type &key, leaf_type *leaf) {
        slot_t slot = find_lower(key);

        if (is_full()) {
            ASSERT(false) << "Currently do not support split" << max_num_pairs;
        }

        // move items and put data item into correct data slot
        ASSERT(slot >= 0 && slot <= num_slots);

        if (num_slots - 1 != slot) {
            right_shift(slot);
        } else {
            ++slot;
        }
        num_slots++;

        auto &pair = pairs[slot];
        pair.first = key;
        pair.second = leaf;

        // for (slot_t slot_i = 0; slot_i < num_slots - 1; ++slot_i) {
        //     if(pairs[slot_i].first >= pairs[slot_i + 1].first) {
        //         LOG(2) << *this;
        //         ASSERT(false);
        //     }
        // }
    }

    // RecordLock *get_lock(const Key_t &key) override {
    //     ASSERT(false);
    //     auto &leaf = get_leaf_node(key);
    //     return leaf.get_lock(key);
    // }

   private:
    /**
     * The actual function for static range partition
     */
    // TODO: may be we do not need a binary search?
    slot_t get_leaf_slot(const key_type &key) {
        slot_t s = find_lower(key);
        if (pairs[s].first > key) {
            --s;
        }
        return s;
    }

    inline void right_shift(slot_t src_slot, slot_t shift_num = 1) {
        std::copy_backward(pairs + src_slot, pairs + num_slots, pairs + num_slots + shift_num);
    }

    slot_t find_lower(const key_type &key) const {
        if (num_slots == 0)
            return 0;

        slot_t lo = 0, hi = num_slots - 1;

        while (lo < hi) {
            slot_t mid = (lo + hi) >> 1;

            if (key_lessequal(key, pairs[mid].first)) {
                hi = mid;  // key <= mid
            } else {
                lo = mid + 1;  // key > mid
            }
        }

        return lo;
    }

    slot_t find_lower_standard(const key_type &key) const {
        if (num_slots == 0)
            return 0;

        for (slot_t i = 0; i < num_slots - 1; ++ i) {
            if (pairs[i].first <= key && key < pairs[i+1].first) {
                return i;
            }
        }
        return num_slots - 1;
    }

    // True if a < b ? "constructed" from key_less_()
    bool key_less(const key_type &a, const key_type &b) const { return key_less_(a, b); }

    // True if a <= b ? constructed from key_less()
    bool key_lessequal(const key_type &a, const key_type &b) const { return !key_less_(b, a); }

    bool key_equal(const key_type &a, const key_type &b) const {
        return !key_less_(a, b) && !key_less_(b, a);
    }

    void print() override {
        std::cout << *this << std::endl;
    }

    key_compare key_less_;

    template <class Key, class Value>
    friend std::ostream &operator<<(std::ostream &os, const PBtree<Key, Value> &node);
};

template <class Key, class Value>
inline std::ostream &operator<<(std::ostream &os, const PBtree<Key, Value> &node) {
    os << "--------<PBtree, Num_slots " << node.num_slots << ">------------" << std::endl;
    os << "table_id: " << node.table_id << std::endl;
    os << "Keys: ";
    for (slot_t slot_i = 0; slot_i < node.num_slots; ++slot_i) {
        os << node.pairs[slot_i].first << " ";
    }
    os << std::endl;
    os << "Leafs: ";
    for (slot_t slot_i = 0; slot_i < node.num_slots; ++slot_i) {
        auto* leaf = node.pairs[slot_i].second;
        if (leaf){
            auto* page_meta = leaf->get_page_meta();
            os << "<" << page_meta->gp_id.seg_id << "," << page_meta->gp_id.page_id << "> ";
        } else
            os << -1 << " ";
    }
    return os;
}