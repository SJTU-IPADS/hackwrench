#pragma once
// copied and modified from silo

#include <cstring>
#include <functional>
#include <iostream>

#include "servers/config.h"

namespace private_ {

template <typename T>
struct is_eq_expensive {
    static const bool value = true;
};

struct cheap_eq {
    static const bool value = false;
};

// equals is cheap for integer types
template <>
struct is_eq_expensive<bool> : public cheap_eq {};
template <>
struct is_eq_expensive<uint8_t> : public cheap_eq {};
template <>
struct is_eq_expensive<int8_t> : public cheap_eq {};
template <>
struct is_eq_expensive<uint16_t> : public cheap_eq {};
template <>
struct is_eq_expensive<int16_t> : public cheap_eq {};
template <>
struct is_eq_expensive<uint32_t> : public cheap_eq {};
template <>
struct is_eq_expensive<int32_t> : public cheap_eq {};
template <>
struct is_eq_expensive<uint64_t> : public cheap_eq {};
template <>
struct is_eq_expensive<int64_t> : public cheap_eq {};

template <typename T>
struct fast_func_param {
    typedef typename std::conditional<std::is_scalar<T>::value, T, const T &>::type type;
};

template <typename T>
struct myhash {
    inline size_t operator()(typename fast_func_param<T>::type t) const {
        return std::hash<T>()(t);
    }
};

template <typename Tp>
struct myhash<Tp *> {
    inline size_t operator()(Tp *t) const {
        // std::hash for ptrs is bad
        // tommyhash: http://tommyds.sourceforge.net/doc/tommyhash_8h_source.html
        size_t key = reinterpret_cast<size_t>(t) >> 3;  // shift out 8-byte pointer alignment
#ifdef USE_TOMMY_HASH
        key = (~key) + (key << 21);  // key = (key << 21) - key - 1;
        key = key ^ (key >> 24);
        key = (key + (key << 3)) + (key << 8);  // key * 265
        key = key ^ (key >> 14);
        key = (key + (key << 2)) + (key << 4);  // key * 21
        key = key ^ (key >> 28);
        key = key + (key << 31);
#else
        key = (~key) + (key << 21);  // key = (key << 21) - key - 1;
        key = key ^ (key >> 24);
#endif
        return key;
    }
};

static inline constexpr size_t TableSize(size_t v) {
    // round up to pow of 2
    v--;
    v |= v >> 1;
    v |= v >> 2;
    v |= v >> 4;
    v |= v >> 8;
    v |= v >> 16;
    v++;
    return v;
}
}  // namespace private_

template <typename Key, typename T, size_t StaticSize = 1024, typename Hash = private_::myhash<Key>>
class static_unordered_map {
   public:
    typedef Key key_type;
    typedef T mapped_type;
    typedef std::pair<const key_type, mapped_type> value_type;
    typedef Hash hasher;
    typedef T &reference;
    typedef const T &const_reference;

   private:
    typedef std::pair<key_type, mapped_type> bucket_value_type;
    static const size_t TableSize = private_::TableSize(StaticSize);
    static_assert(StaticSize >= 1);
    static_assert(TableSize >= 1);

    struct bucket {
        inline bucket_value_type *ptr() { return reinterpret_cast<bucket_value_type *>(&buf[0]); }

        inline const bucket_value_type *ptr() const {
            return reinterpret_cast<const bucket_value_type *>(&buf[0]);
        }

        inline bucket_value_type &ref() { return *ptr(); }

        inline const bucket_value_type &ref() const { return *ptr(); }

        template <class... Args>
        inline void construct(size_t hash, Args &&... args) {
            h = hash;
            new (&ref()) bucket_value_type(std::forward<Args>(args)...);
        }

        inline void destroy() {
            if (false)
                ref().~bucket_value_type();
        }

        struct bucket *bnext;
        size_t h;
        char buf[sizeof(value_type)];
    };

    // iterators are not stable across mutation
    template <typename BucketType, typename ValueType>
    class iterator_ : public std::iterator<std::forward_iterator_tag, ValueType> {
        friend class static_unordered_map;

       public:
        inline iterator_() : b(0) {}

        template <typename B, typename V>
        inline iterator_(const iterator_<B, V> &other) : b(other.b) {}

        inline ValueType &operator*() const { return reinterpret_cast<ValueType &>(b->ref()); }

        inline ValueType *operator->() const { return reinterpret_cast<ValueType *>(b->ptr()); }

        inline bool operator==(const iterator_ &o) const { return b == o.b; }

        inline bool operator!=(const iterator_ &o) const { return !operator==(o); }

        inline iterator_ &operator++() {
            b++;
            return *this;
        }

        inline iterator_ operator++(int) {
            iterator_ cur = *this;
            ++(*this);
            return cur;
        }

       protected:
        inline iterator_(BucketType *b) : b(b) {}

       private:
        BucketType *b;
    };

    static size_t chain_length(bucket *b) {
        size_t ret = 0;
        while (b) {
            ret++;
            b = b->bnext;
        }
        return ret;
    }

   public:
    typedef iterator_<bucket, value_type> iterator;
    typedef iterator_<const bucket, const value_type> const_iterator;

    static_unordered_map() : n(0) { memset(&table[0], 0, sizeof(table)); }

    ~static_unordered_map() {
        for (size_t i = 0; i < n; i++) elems[i].destroy();
#ifdef ENABLE_EVENT_COUNTERS
        size_t ml = 0;
        for (size_t i = 0; i < TableSize; i++) {
            const size_t l = chain_length(table[i]);
            ml = std::max(ml, l);
        }
        if (ml)
            private_::evt_avg_max_unordered_map_chain_length.offer(ml);
#endif
    }

    static_unordered_map(const static_unordered_map &other) : n(0) {
        memset(&table[0], 0, sizeof(table));
        assignFrom(other);
    }

    static_unordered_map &operator=(const static_unordered_map &other) {
        // self assignment
        if (unlikely(this == &other))
            return *this;
        assignFrom(other);
        return *this;
    }

   private:
    bucket *find_bucket(const key_type &k, size_t *hash_value) {
        const size_t h = Hash()(k);
        if (hash_value)
            *hash_value = h;
        const size_t i = h % TableSize;
        bucket *b = table[i];
        while (b) {
            const bool check_hash = private_::is_eq_expensive<key_type>::value;
            if ((!check_hash || b->h == h) && b->ref().first == k)
                return b;
            b = b->bnext;
        }
        return 0;
    }

    inline const bucket *find_bucket(const key_type &k, size_t *hash_value) const {
        return const_cast<static_unordered_map *>(this)->find_bucket(k, hash_value);
    }

   public:
    // XXX(stephentu): template away this stuff

    mapped_type &at(const key_type &k) {
        size_t h;
        bucket *b = find_bucket(k, &h);
        if (b)
            return b->ref().second;
        ASSERT(false) << k;
        return b->ref().second;
    }

    mapped_type &operator[](const key_type &k) {
        size_t h;
        bucket *b = find_bucket(k, &h);
        if (b)
            return b->ref().second;
        ASSERT(n < StaticSize);
        b = &elems[n++];
        b->construct(h, k, mapped_type());
        const size_t i = h % TableSize;
        b->bnext = table[i];
        table[i] = b;
        return b->ref().second;
    }

    mapped_type &operator[](key_type &&k) {
        size_t h;
        bucket *b = find_bucket(k, &h);
        if (b)
            return b->ref().second;
        ASSERT(n < StaticSize);
        b = &elems[n++];
        b->construct(h, std::move(k), mapped_type());
        const size_t i = h % TableSize;
        b->bnext = table[i];
        table[i] = b;
        return b->ref().second;
    }

    inline size_t size() const { return n; }

    inline bool empty() const { return !n; }

    iterator begin() { return iterator(&elems[0]); }

    const_iterator begin() const { return const_iterator(&elems[0]); }

    inline iterator end() { return iterator(&elems[n]); }

    inline const_iterator end() const { return const_iterator(&elems[n]); }

    iterator find(const key_type &k) {
        bucket *const b = find_bucket(k, 0);
        if (b)
            return iterator(b);
        return end();
    }

    const_iterator find(const key_type &k) const {
        const bucket *const b = find_bucket(k, 0);
        if (b)
            return const_iterator(b);
        return end();
    }

    void clear() {
        if (!n)
            return;
        memset(&table[0], 0, sizeof(table));
        for (size_t i = 0; i < n; i++) elems[i].destroy();
        n = 0;
    }

   public:
    // non-standard API
    inline bool is_small_type() const { return true; }

   private:
    // doesn't check for self assignment
    inline void assignFrom(const static_unordered_map &that) {
        clear();
        for (size_t i = 0; i < that.n; i++) {
            bucket *const b = &elems[n++];
            const bucket *const that_b = &that.elems[i];
            b->construct(that_b->h, that_b->ref().first, that_b->ref().second);
            const size_t idx = b->h % TableSize;
            b->bnext = table[idx];
            table[idx] = b;
        }
    }

    size_t n;
    bucket elems[StaticSize];
    bucket *table[TableSize];
};