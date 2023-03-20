#pragma once

#include <condition_variable>
#include <mutex>
#include <queue>
#include <set>
#include <unordered_map>

#include "servers/config.h"
#include "util/exceptions.h"
#include "util/fast_random.h"
#include "util/macros.h"
#include "util/types.h"

template <class Key_t, class Value_t, class Map = std::unordered_map<Key_t, Value_t>>
class ThreadSafeMap {
   public:
    Map &get_underlying_map() { return map; }
    std::mutex *get_mutex() { return &m; }

    Value_t get(const Key_t &key) {
        std::lock_guard<std::mutex> lock(m);
        if (map.find(key) == map.end()) {
            return nullptr;
        }
        return map.at(key);
    }

    Value_t at(const Key_t &key) { return get(key); }

    bool try_get_then_erase(const Key_t &key, Value_t &ret) {
        std::lock_guard<std::mutex> lock(m);
        auto iter = map.find(key);
        if (iter != map.end()) {
            ret = iter->second;
            map.erase(iter);
            return true;
        } else {
            return false;
        }
    }

    bool try_get(const Key_t &key, Value_t &ret) {
        std::lock_guard<std::mutex> lock(m);
        auto iter = map.find(key);
        if (iter != map.end()) {
            ret = iter->second;
            return true;
        } else {
            return false;
        }
    }

    // ret is always set to the current value in the map after the operation
    bool try_get_otherwise_put(const Key_t &key, Value_t &ret, const Value_t &new_value) {
        std::lock_guard<std::mutex> lock(m);
        auto iter = map.find(key);
        if (iter != map.end()) {
            ret = iter->second;
            return true;
        } else {
            map.emplace(key, new_value);
            ret = new_value;
            return false;
        }
    }

    bool try_get_then_put(const Key_t &key, Value_t &ret, const Value_t &new_value) {
        std::lock_guard<std::mutex> lock(m);
        // ASSERT(check_put(key, false) <= 1);
        auto iter = map.find(key);
        // put_vec.push_back(key);
        // LOG(2) << "put " << key;

        // if (check_erase(key)) {
        //     ASSERT(false);
        // }

        if (iter != map.end()) {
            ret = iter->second;
            // ASSERT(ret != nullptr);
            iter->second = new_value;
            return true;
        } else {
            map.emplace(key, new_value);
            return false;
        }
    }

    void put(const Key_t &key, const Value_t &value) {
        std::lock_guard<std::mutex> lock(m);
        map[key] = value;
    }

    void erase(const Key_t &key) {
        std::lock_guard<std::mutex> lock(m);
        map.erase(key);
    }

    void clear() {
        std::lock_guard<std::mutex> lock(m);
        map.clear();
    }

    size_t size() const {
        std::lock_guard<std::mutex> lock(m);
        return map.size();
    }

    void print() {
        for (auto& iter : map) {
            LOG(2) << "exists: " << std::hex<< iter.first;
        }
    }

    void collect(std::map<Key_t, Value_t>& m) {
        m.insert(map.begin(), map.end());
    }

    // uint32_t check_put(Key_t target, bool print = true) {
    //     uint32_t num = 0;
    //     for (Key_t key : put_vec) {
    //         if (target == key) {
    //             num+=1;
    //         }
    //     }
    //     if (num > 0 && print) {
    //         LOG(2) << "Already put!! " << target << " " << num << " times";
    //     }
    //     return num;
    // }

   private:
    Map map;
    mutable std::mutex m;
    // std::vector<Key_t> erase_vec;
    // std::vector<Key_t> put_vec;

    template <class K, class V, class M>
    friend std::ostream &operator<<(std::ostream &os, const ThreadSafeMap<K, V, M> &m);
};

template <class Key_t, class Value_t, int map_amp = 10>
class LooseThreadSafeMap {
   public:
    LooseThreadSafeMap(uint32_t worker_threads)
        : num_threads(worker_threads), num_maps(num_threads * map_amp) {
        map = new ThreadSafeMap<Key_t, Value_t>[num_maps];
    }

    void print() {
        for (uint32_t i = 0; i < num_maps; ++ i) {
            map[i].print();
        }
    }

    std::map<Key_t, Value_t> collect() {
        std::map<Key_t, Value_t> m;
        for (uint32_t i = 0; i < num_maps; ++ i) {
            map[i].collect(m);
        }
        return m;
    }
    // void check_put(Key_t key) {
    //     for (uint32_t i = 0; i < num_maps; ++ i) {
    //         bool ret = map[i].check_put(key);
    //         if (ret) {
    //             LOG(2) << "map index: " << i;
    //         }

    //     }
    // }

    virtual ~LooseThreadSafeMap() { delete[] map; }

    virtual inline uint64_t get_index(txn_id_t txn_id) {
        TxnIdWrapper wrapper(txn_id);
        return wrapper.thread_id * map_amp + wrapper.count % map_amp;
    }

    inline Value_t get(const Key_t &key) { return map[get_index(key)].at(key); }

    // inline Value_t at(const Key_t &key) { return get(key); }

    inline bool try_get(const Key_t &key, Value_t &ret) {
        return map[get_index(key)].try_get(key, ret);
    }

    inline bool try_get_then_erase(const Key_t &key, Value_t &ret) {
        return map[get_index(key)].try_get_then_erase(key, ret);
    }

    // ret is always set to the current value in the map after the operation
    inline bool try_get_otherwise_put(const Key_t &key, Value_t &ret, const Value_t &new_value) {
        return map[get_index(key)].try_get_otherwise_put(key, ret, new_value);
    }    

    inline bool try_get_then_put(const Key_t &key, Value_t &ret, const Value_t &new_value) {
        return map[get_index(key)].try_get_then_put(key, ret, new_value);
    }

    inline void put(const Key_t &key, const Value_t &value) { map[get_index(key)].put(key, value); }

    inline void erase(const Key_t &key) { map[get_index(key)].erase(key); }

    // inline ThreadSafeMap<Key_t, Value_t> *get_maps() { return map; }

    // inline void clear() {
    //     for (thread_id_t i = 0; i < num_maps; ++i) {
    //         map[i].clear();
    //     }
    // }

    inline size_t size() const {
        size_t res = 0;
        for (thread_id_t i = 0; i < num_maps; ++i) {
            res += map[i].size();
        }
        return res;
    }

   protected:
    thread_id_t num_threads;
    uint32_t num_maps;

   private:
    ThreadSafeMap<Key_t, Value_t> *map;
};

struct ValVersion {
    uint8_t* val;
    version_ts_t version;
};

typedef LooseThreadSafeMap<Key_t, ValVersion, 100> BatchCache;
// class AuroraBatchContainer {
//    public:
//     BatchCache bache_cache;
//     batch_id_t gather_batch_num;
//     size_t curr_batch_size;
// };

template <class Key_t, class Value_t>
class BatchThreadSafeMap : public LooseThreadSafeMap<Key_t, Value_t> {
   public:
    BatchThreadSafeMap(uint32_t worker_threads)
        : LooseThreadSafeMap<Key_t, Value_t>(worker_threads) {}
    inline uint64_t get_index(Key_t batch_id) override { return batch_id % this->num_maps; }
};

template <class Value_t>
class ThreadSafeSet {
   public:
    bool try_get_first_element(Value_t &ret) {
        std::lock_guard<std::mutex> lock(m);
        if (set.size()) {
            ret = *set.begin();
            return true;
        } else {
            return false;
        }
    }

    bool try_pop_first_element(Value_t &ret) {
        std::lock_guard<std::mutex> lock(m);
        if (set.size()) {
            ret = *set.begin();
            set.erase(ret);
            return true;
        } else {
            return false;
        }
    }

    void insert(const Value_t &value) {
        std::lock_guard<std::mutex> lock(m);
        set.insert(value);
    }

    void erase(const Value_t &value) {
        std::lock_guard<std::mutex> lock(m);
        set.erase(value);
    }

   private:
    std::set<Value_t> set;
    mutable std::mutex m;
};

// A threadsafe-queue.
// ref: https://stackoverflow.com/questions/15278343/c11-thread-safe-queue
template <class T>
class ThreadSafeQueue {
   public:
    ThreadSafeQueue() : q(), m(), c() {}

    ~ThreadSafeQueue() {}

    size_t size() {
        std::lock_guard<std::mutex> lock(m);
        return q.size();
    }

    // Add an element to the queue.
    void enqueue(T t) {
        std::lock_guard<std::mutex> lock(m);
        q.push(t);
        c.notify_one();
    }

    // Get the "front"-element.
    // If the queue is empty, wait till a element is avaiable.
    T dequeue(void) {
        std::unique_lock<std::mutex> lock(m);
        while (q.empty()) {
            // release lock as long as the wait and reaquire it afterwards.
            c.wait(lock);
        }
        T val = q.front();
        q.pop();
        return val;
    }

    bool dequeue_once(T &val) {
        std::unique_lock<std::mutex> lock(m);
        if (q.empty()) {
            return false;
        } else {
            val = q.front();
            q.pop();
            return true;
        }
    }

   private:
    std::queue<T> q;
    mutable std::mutex m;
    std::condition_variable c;
};

template <class T>
class LooseThreadSafeQueue {
   public:
    LooseThreadSafeQueue(uint32_t worker_threads) : worker_threads(worker_threads) {
        q = new ThreadSafeQueue<T>[worker_threads];
    }

    ~LooseThreadSafeQueue() { delete[] q; }

    inline void enqueue(T t, uint32_t worker_id) { q[worker_id].enqueue(t); }

    inline T dequeue(uint32_t worker_id) {
        T t;
        uint32_t queue_id = worker_id;
        // uint32_t rr_count = 0;
        uint try_times = 0;
        while (!q[queue_id].dequeue_once(t)) {
            if (try_times++ > worker_threads) {
                throw WorkloadNoClientException();
            }
            queue_id = thread_rand.next_u32() % worker_threads;
            // queue_id = (queue_id == worker_id) ? rr_count++ : worker_id;
            // if (rr_count == worker_threads)
            //     rr_count = 0;
        }
        return t;
    }

    inline size_t size() {
        size_t ret = 0;
        for (uint i = 0; i < worker_threads; i++) ret += q[i].size();
        return ret;
    }

   private:
    ThreadSafeQueue<T> *q;
    uint32_t worker_threads;
};

// Queue implemented by fixed size array
template <class T>
class ArrayQueue {
   public:
    ArrayQueue(uint32_t cap = 65536)
        : array_queue(new T[cap]), head(0), tail(0), len(0), capacity(cap) {}

    ~ArrayQueue() { delete[] array_queue; }

    inline uint32_t size() { return len; }

    inline bool empty() { return len == 0; }

    inline void push(T elem) {
        array_queue[tail] = elem;
        incre_ptr(tail);
        len++;
    }

    inline T front() { return array_queue[head]; }

    inline void pop() {
        incre_ptr(head);
        len--;
    }

   private:
    inline void incre_ptr(uint32_t &ptr) {
        if (++ptr == capacity) {
            ptr = 0;
        }
    }

    T *array_queue;
    uint32_t head;
    uint32_t tail;
    uint32_t len;
    const uint32_t capacity;
};

// Only used for collecting txns for batch
template <class T>
class BatchQueue {
   public:
    BatchQueue(uint64_t batching_size, batch_id_t prev_batch_id, uint64_t min_batch_size = 1)
        : q(),
          m(),
          batching_size(batching_size),
          min_batch_size(min_batch_size),
          prev_batch_id(prev_batch_id),
          gathering_batch(false) {}

    inline bool is_full() { return q.size() >= batching_size; }
    inline uint32_t size() { return q.size(); }
    // TODO: use a lock free link list
    bool enqueue_txn(T t) {
        std::lock_guard<std::mutex> lock(m);
        q.push(t);
        new_tx_since_last_timeout = true;
        // If size enough and there's no batch gathering
        if (q.size() >= batching_size && !gathering_batch) {
            // start batch gathering
            gathering_batch = true;
            return true;
        }
        return false;
    }

    batch_id_t dequeue_batch(std::vector<T> &ordered_txns, int split_number, bool &success) {
        success = true;
        std::unique_lock<std::mutex> lock(m);
        // set a threshold for not creating a small batch
        if (q.size() < batching_size) {
            success = false;
            return 0;
        }
        ASSERT(ordered_txns.size() == 0);
        ordered_txns.reserve(batching_size);
        for (uint32_t i = 0; i < batching_size; ++i) {
            if (q.empty()) {
                break;
            }
            ordered_txns.push_back(q.front());
            q.pop();
        }
        // end batch gathering
        gathering_batch = false;
        prev_batch_id += split_number;
        return prev_batch_id;  // return the last batch_id
    }

    inline batch_id_t new_batch_ids(int number) {
        std::unique_lock<std::mutex> lock(m);
        prev_batch_id += number;
        return prev_batch_id;  // return the last batch_id
    }

    inline batch_id_t new_batch_id() {
        std::unique_lock<std::mutex> lock(m);
        return ++prev_batch_id;
    }

    inline bool check_timeout_pop() {
        std::unique_lock<std::mutex> lock(m);
        bool ret = !new_tx_since_last_timeout;
        new_tx_since_last_timeout = false;
        return ret;
    }

   private:
    ArrayQueue<T> q;
    // std::queue<T> q;
    mutable std::mutex m;
    const uint64_t batching_size;
    const uint64_t min_batch_size;
    batch_id_t prev_batch_id;
    bool gathering_batch;
    bool new_tx_since_last_timeout = false;
};

template <class T>
class ArrayQueueLockfree {
   public:
    ArrayQueueLockfree(uint32_t cap = 65536)
        : array_queue(new T[cap]),
          prod_head(0),
          prod_tail(0),
          cons_head(0),
          cons_tail(0),
          len(0),
          capacity(cap),
          mask(cap - 1) {
        ASSERT(!(cap & (cap - 1))) << "capacity must be power of 2";
    }

    ~ArrayQueueLockfree() { delete[] array_queue; }

    inline uint32_t size() { return len; }

    // multi-producer safe
    inline void push(T elem) {
        ASSERT(len + 1 <= capacity) << len.load();
        uint32_t my_slot = prod_head++;
        ++len;
        array_queue[my_slot & mask] = elem;
        // wait for other producers
        bool success = false;
        do {
            uint32_t tmp = my_slot;
            success = prod_tail.compare_exchange_weak(tmp, my_slot + 1);
            CPU_MEMORY_FENCE();
        } while (!success);
    }

    // multi-consumer safe
    inline uint32_t pop_n(uint32_t n, std::vector<T> &results) {
        if (len < n) {
            return 0;
        }
        bool success = false;
        uint32_t my_head, cons_next;
        do {
            my_head = cons_head;
            uint32_t my_prod_tail = prod_tail;
            uint32_t curr_len = my_prod_tail - my_head;
            n = n < curr_len ? n : curr_len;
            cons_next = my_head + n;
            success = cons_head.compare_exchange_weak(my_head, cons_next);
        } while (!success);
        len -= n;
        results.resize(n);
        for (uint32_t i = 0; i < n; ++i) {
            results[i] = array_queue[(my_head + i) & mask];
        }
        do {
            uint32_t my_head_tmp = my_head;
            success = cons_tail.compare_exchange_weak(my_head_tmp, cons_next);
            CPU_MEMORY_FENCE();
        } while (!success);
        return n;
    }

    T *array_queue;
    std::atomic<uint32_t> prod_head, prod_tail;
    std::atomic<uint32_t> cons_head, cons_tail;
    std::atomic<uint32_t> len;
    const uint32_t capacity;
    const uint32_t mask;
};

// Only used for collecting txns for batch
template <class T>
class BatchQueueLockfree {
   public:
    BatchQueueLockfree(uint64_t batching_size, batch_id_t prev_batch_id)
        : q(), batching_size(batching_size), prev_batch_id(prev_batch_id) {}

    // TODO: use a lock free link list
    bool enqueue_txn(T t) {
        q.push(t);
        // If size enough and there's no batch gathering
        if (q.size() >= batching_size) {
            // start batch gathering
            return true;
        }
        return false;
    }

    batch_id_t dequeue_batch(std::vector<T> &ordered_txns, int split_number, bool &success) {
        success = true;
        if (q.size() < batching_size) {
            success = false;
            return 0;
        }
        ASSERT(ordered_txns.size() == 0);
        uint32_t real_size = q.pop_n(batching_size, ordered_txns);
        if (real_size == 0) {
            success = false;
            return 0;
        }
        // end batch gathering
        batch_id_t ret_batch_id = prev_batch_id.fetch_add(split_number) + split_number;
        return ret_batch_id;  // return the last batch_id
    }

    inline batch_id_t new_batch_id() { return ++prev_batch_id; }

   private:
    ArrayQueueLockfree<T> q;
    const uint64_t batching_size;
    std::atomic<batch_id_t> prev_batch_id;
};

// TODO: thread safe
class Profile {
   public:
    uint64_t commit_times = 0;
    uint64_t run_times = 0;
    uint64_t db_repair_times = 0;
    uint64_t sn_repair_times = 0;
    uint64_t batch_commit_times = 0;
    uint64_t db_batch_repair_times = 0;
    uint64_t sn_batch_repair_times = 0;
    uint64_t batch_abort_times = 0;
    uint64_t remote_abort_times = 0;
    uint64_t local_abort_times = 0;
    double time_duration = 0.0;
    uint64_t padding[6];

    Profile() {}

    Profile(const Profile &p) {
        this->commit_times = p.commit_times;
        this->run_times = p.run_times;
        this->db_repair_times = p.db_repair_times;
        this->sn_repair_times = p.sn_repair_times;
        this->batch_commit_times = p.batch_commit_times;
        this->db_batch_repair_times = p.db_batch_repair_times;
        this->sn_batch_repair_times = p.sn_batch_repair_times;
        this->batch_abort_times = p.batch_abort_times;
        this->remote_abort_times = p.remote_abort_times;
        this->local_abort_times = p.local_abort_times;
        this->time_duration = p.time_duration;
    }

    inline void db_repair(uint64_t num = 1) { db_repair_times += num; }
    inline void sn_repair(uint64_t num = 1) { sn_repair_times += num; }

    inline void db_batch_repair() { ++db_batch_repair_times; }
    inline void sn_batch_repair() { ++sn_batch_repair_times; }

    inline void commit() {
        ++commit_times;
        ++run_times;
    }

    inline void commit_batch() { ++batch_commit_times; }

    inline void abort_batch() { ++batch_abort_times; }

    inline void remote_abort() { ++remote_abort_times; }

    inline void abort() { ++local_abort_times; }

    inline void clear() {
        commit_times = 0;
        run_times = 0;
        db_repair_times = 0;
        sn_repair_times = 0;
        batch_commit_times = 0;
        batch_abort_times = 0;
        db_batch_repair_times = 0;
        sn_batch_repair_times = 0;
        remote_abort_times = 0;
        local_abort_times = 0;
        time_duration = 0;
    }

    inline uint64_t num_repairs() const { return db_repair_times + sn_repair_times; }
    inline uint64_t num_batch_repairs() const {
        return db_batch_repair_times + sn_batch_repair_times;
    }

    double throughput() const { return (double)commit_times / time_duration; }
    double repair_ratio() const { return (double)num_repairs() / commit_times; }
    double abort_ratio() const { return 1.0 - (double)commit_times / run_times; }
    double batch_repair_ratio() const { return (double)num_batch_repairs() / batch_commit_times; }

    friend std::ostream &operator<<(std::ostream &os, const Profile &p);
};
std::ostream &operator<<(std::ostream &os, const Profile &p) {
    std::ios::fmtflags ff = os.setf(std::ios::fixed);
    std::streamsize ss = os.precision(3);
    os << "throughput: " << p.throughput();
    os << ", commit: " << p.commit_times;
    os << ", abort: " << p.local_abort_times;
    os << ", repair: " << p.num_repairs();
    os << ", db_repair: " << p.db_repair_times;
    os << ", sn_repair: " << p.sn_repair_times;

    os.setf(ff);
    os.precision(ss);
    return os;
}

template <class Key_t, class Value_t, class Map>
std::ostream &operator<<(std::ostream &os, const ThreadSafeMap<Key_t, Value_t, Map> &m) {
    for (const auto &iter : m.map) {
        os << iter.first << std::endl;
    }
    return os;
}