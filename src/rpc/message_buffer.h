#pragma once
#include <condition_variable>
#include <deque>
#include <mutex>
#include <unordered_map>

template <typename T>
class blockingQueue {
   private:
    std::mutex d_mutex;
    std::condition_variable d_condition;
    std::deque<T> d_queue;

   public:
    inline void push(T &value) {
        {
            std::lock_guard<std::mutex> lock(this->d_mutex);
            d_queue.emplace_front();
            d_queue.front().move(value);
        }
        this->d_condition.notify_one();
    }

    inline T pop() {
        std::unique_lock<std::mutex> lock(this->d_mutex);
        this->d_condition.wait(lock, [=] { return !this->d_queue.empty(); });
        T rc(std::move(this->d_queue.back()));
        this->d_queue.pop_back();
        return rc;
    }

    inline std::vector<T> pop_batch(int &pop_batch_size) {
        std::vector<T> ret;
        std::unique_lock<std::mutex> lock(this->d_mutex);
        this->d_condition.wait(lock, [=] { return !this->d_queue.empty(); });
        int i = 0;
        for (; i < pop_batch_size && !d_queue.empty(); i++) {
            ret.emplace_back(std::move(this->d_queue.back()));
            this->d_queue.pop_back();
        }
        pop_batch_size = i;
        return ret;
    }

    inline bool pop_once(T &rc) {
        std::lock_guard<std::mutex> lock(this->d_mutex);
        if (this->d_queue.empty()) {
            return false;
        } else {
            rc.move(this->d_queue.back());
            this->d_queue.pop_back();
            return true;
        }
    }
};

template <typename T>
class blockingHash {
   private:
    std::mutex d_mutex;
    std::condition_variable d_condition;
    std::unordered_map<uint64_t, T> hash;

   public:
    inline void push(uint64_t id, T &value) {
        {
            std::lock_guard<std::mutex> lock(this->d_mutex);
            hash.emplace(std::piecewise_construct, std::forward_as_tuple(id),
                         std::forward_as_tuple());
            hash.at(id).move(value);
        }
        this->d_condition.notify_all();
    }

    inline T get(uint64_t id) {
        std::unique_lock<std::mutex> lock(this->d_mutex);
        this->d_condition.wait(lock, [=] { return this->hash.find(id) != this->hash.end(); });
        T rc(std::move(this->hash.at(id)));
        this->hash.erase(id);
        return rc;
    }

    inline bool get_once(uint64_t id, T &rc) {
        std::lock_guard<std::mutex> lock(this->d_mutex);
        auto iter = this->hash.find(id);
        if (iter == this->hash.end()) {
            return false;
        } else {
            rc.move(iter->second);
            this->hash.erase(iter);
            return true;
        }
    }
};