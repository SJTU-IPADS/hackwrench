#pragma once

#include <cstdint>
#include <cstring>
#include <iostream>

// for versioning
#ifndef VERSION_STRING
#define VERSION_STRING __DATE__ " " __TIME__
#endif

// page
typedef uint32_t seg_id_t;
typedef uint32_t page_id_t;
typedef uint64_t g_page_id_t;
typedef uint32_t page_size_t;

// global_page_t
union GlobalPageId {
    struct {
        seg_id_t seg_id;
        page_id_t page_id;
    };
    g_page_id_t g_page_id;

    GlobalPageId() : g_page_id(0) {}
    GlobalPageId(seg_id_t seg_id, page_id_t page_id) : seg_id(seg_id), page_id(page_id) {}
    GlobalPageId(g_page_id_t g_page_id) : g_page_id(g_page_id) {}
};

// redo_log
typedef uint8_t *log_t;

// txn
typedef uint64_t ts_t;
static const ts_t NULL_TS = 0;
typedef uint64_t txn_id_t;
typedef uint64_t version_ts_t;
static const txn_id_t NULL_TXN_ID = 0;
typedef uint64_t batch_id_t;
static const batch_id_t NULL_BATCH_ID = 0;
typedef uint64_t local_version_t;

union TxnIdWrapper {
    struct {
        uint64_t count : 40;
        uint64_t repaired : 8;
        uint64_t thread_id : 8;
        uint64_t node_id : 8;
    };
    txn_id_t txn_id;

    TxnIdWrapper(txn_id_t txn_id) : txn_id(txn_id) {}
    TxnIdWrapper(uint8_t node_id, uint8_t thread_id, uint64_t count)
        : count(count), repaired(0), thread_id(thread_id), node_id(node_id) {}
    inline static txn_id_t set_repaired(TxnIdWrapper x) {
        x.repaired = 1;
        return x.txn_id;
    }
};

std::ostream &operator<<(std::ostream &os, const TxnIdWrapper &wrapper) {
    os << "(" << wrapper.node_id << "," << wrapper.thread_id << "," << wrapper.count << ")";
    return os;
}

// config
typedef uint32_t node_id_t;
static const node_id_t NULL_DB_ID = 0xdeadffff;
typedef uint32_t thread_id_t;
static const uint32_t MAX_SN_NUM = 30; 

// store
typedef uint32_t table_id_t;

// pbtree
typedef int32_t slot_t;

// rpc
typedef uint64_t msg_id_t;
static const msg_id_t NULL_MSG_ID = 0;

// redo_log
typedef uint32_t offset_t;

// Macros for encoding 1-bit is_write and 31-bit seg_id
#define SEG_IS_WRITE(seg_id) (seg_id >> 31)
#define SEG_SET_WRITE(seg_id) (seg_id |= (1 << 31))
#define SEG_SET_READ(seg_id) (seg_id &= ~(1 << 31))
#define SEG_GET_ID(seg_id) (seg_id & 0x7FFFFFFF)

#define TXN_IS_ABORTED(txn) (txn == reinterpret_cast<DbTxn *>(1))

class IntPair {
   public:
    uint64_t a;
    uint64_t b;
    IntPair(uint64_t a, uint64_t b) : a(a), b(b) {}
};

class FixSizeString {
    static const size_t MAX_SIZE = 64;

   public:
    char s[MAX_SIZE];
    FixSizeString() { clear(); }
    FixSizeString(const char data[]) { strncpy(s, data, MAX_SIZE); }
    FixSizeString(const char data[], size_t size) {
        strncpy(s, data, size < MAX_SIZE ? size : MAX_SIZE);
    }
    FixSizeString(const std::string str) : FixSizeString(str.c_str()) {}
    FixSizeString(const uint32_t data) { snprintf(s, MAX_SIZE, "%d", data); }
    // FixSizeString &&operator=(const FixSizeString &other) {
    //     FixSizeString a(other);
    //     return std::move(a);
    // }

    inline void clear() { s[0] = '\0'; }
    bool operator<(const FixSizeString &other) { return strcmp(s, other.s) < 0; }
    bool operator>(const FixSizeString &other) { return strcmp(s, other.s) > 0; }
    bool operator==(const FixSizeString &other) { return strcmp(s, other.s) == 0; }
    FixSizeString operator+(const FixSizeString &other) {
        FixSizeString res;
        strncat(res.s, s, MAX_SIZE - strlen(res.s) - 1);
        strncat(res.s, other.s, MAX_SIZE - strlen(res.s) - 1);
        return res;
    }
    FixSizeString &operator+=(const char other[]) {
        strncat(s, other, MAX_SIZE - strlen(s) - 1);
        return *this;
    }
    FixSizeString &operator+=(const std::string other) { return *this += other.c_str(); }

    inline int to_int() { return std::atoi(s); }
    inline std::vector<std::string> split() {
        std::vector<std::string> res;
        char *token = strtok(s, ",");
        while (token != NULL) {
            res.emplace_back(token);
            token = strtok(NULL, ",");
        }
        return res;
    }
    inline std::vector<uint> split_uint() {
        std::vector<uint> res;
        char *token = strtok(s, ",");
        while (token != NULL) {
            res.emplace_back(std::atoi(token));
            token = strtok(NULL, ",");
        }
        return res;
    }
    inline void pack(std::vector<uint> &values) {
        clear();
        for (uint val : values) {
            snprintf(s + strlen(s), MAX_SIZE - strlen(s), "%d,", val);
        }
    }
    friend std::ostream &operator<<(std::ostream &os, const FixSizeString &s);
};
std::ostream &operator<<(std::ostream &os, const FixSizeString &s) {
    os << s.s;
    return os;
}

template <uint32_t MAX_SIZE = 16>
class BytesArray {
    static const size_t SLOTS = MAX_SIZE / sizeof(uint);

   public:
    uint s[SLOTS];
    uint current_size = 0;
    BytesArray() {
        // default: an integer
        memset(s, 0, MAX_SIZE);
        current_size = 1;
    }
    BytesArray(const uint32_t data) {
        memset(s, 0, MAX_SIZE);
        s[0] = data;
        current_size = 1;
    }
    BytesArray(const BytesArray &other) {
        current_size = other.current_size;
        for (uint i = 0; i < other.current_size; i++) {
            s[i] = other.s[i];
        }
    }

    bool empty() { return current_size == 0; }

    inline void clear() { current_size = 0; }
    bool operator==(const BytesArray &other) {
        if (current_size != other.current_size)
            return false;
        return memcmp(s, other.s, current_size) == 0;
    }

    bool operator!=(const BytesArray &other) { return !(*this == other); }

    // only for integer
    BytesArray operator+(const BytesArray &a) const {
        BytesArray res;
        res.set_int(a.to_int() + this->to_int());
        return res;
    }

    BytesArray operator-(const BytesArray &a) const {
        BytesArray res;
        res.set_int(this->to_int() - a.to_int());
        return res;
    }

    inline int to_int() const { return s[0]; }
    inline void set_int(int i) { s[0] = i; }
    inline std::vector<uint> split_uint() {
        std::vector<uint> res;
        for (uint i = 0; i < current_size; i++) {
            res.push_back(s[i]);
        }
        return res;
    }
    inline void pack(std::vector<uint> &values) {
        ASSERT(values.size() <= SLOTS);
        for (uint i = 0; i < values.size(); i++) {
            s[i] = values[i];
        }
        current_size = values.size();
    }

    template<uint32_t>
    friend std::ostream &operator<<(std::ostream &os, const BytesArray<MAX_SIZE> &s);
};

template<uint32_t MAX_SIZE>
std::ostream &operator<<(std::ostream &os, const BytesArray<MAX_SIZE> &s) {
    os << "[" << s.current_size << "]";
    for (uint i = 0; i < s.current_size; i++) {
        os << s.s[i] << ",";
    }
    return os;
}

typedef uint64_t Key_t;
typedef BytesArray<16> Val_t;

#define KEY_SIZE 8