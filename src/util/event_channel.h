#pragma once

#include <atomic>
#include <condition_variable>
#include <deque>
#include <mutex>
#include <zmq.hpp>

#include "types.h"

class MsgBuffer {
    size_t _size;
    uint8_t *_data;

   public:
    MsgBuffer(const MsgBuffer &other) = delete;
    MsgBuffer &operator=(const MsgBuffer &other) = delete;

    MsgBuffer() : _size(0), _data(nullptr) {}
    MsgBuffer(MsgBuffer &&other) : _size(other._size), _data(other._data) {
        other._data = nullptr;
        other._size = 0;
    }
    MsgBuffer &operator=(const MsgBuffer &&other) {
        if (_data) delete[] _data;
        _size = other._size;
        _data = other._data;
        return *this;
    }

    MsgBuffer(size_t size) : _size(size) {
        _data = new uint8_t[_size];
    }

    MsgBuffer(zmq::message_t &zmq_msg) : MsgBuffer(zmq_msg.size()) {
        memcpy(_data, zmq_msg.data(), _size);
    }

    ~MsgBuffer() {
        if (_data != nullptr) {
            delete[] _data;
        }
    }

    inline void *data() { return _data; }
    inline size_t size() { return _size; }
    inline void move(MsgBuffer &from) {
        ASSERT(_data == nullptr && _size == 0);
        ASSERT(from._size != 0 && from._data != nullptr);
        _size = from._size;
        _data = from._data;
        from._data = nullptr;
        from._data = 0;
    }
    friend std::ostream &operator<<(std::ostream &os, const MsgBuffer &buf);
};

std::ostream &operator<<(std::ostream &os, const MsgBuffer &buf) {
    os << "size: " << buf._size << std::endl;
    for (size_t i = 0; i < buf._size; i++) {
        os << (int)buf._data[i] << ", ";
    }
    return os;
}

struct Event {
    enum Type : uint32_t { NONE, RPC, TASK };

   public:
    uint32_t event_type;
    uint32_t task_type;
    union {
        void *ptr;
        uint64_t value;
    };

   public:
    Event() : event_type(NONE) {}
    Event(Type event_type, MsgBuffer &msg_buf) : event_type(event_type) {
        ptr = new MsgBuffer(std::move(msg_buf));
    }
    Event(Type event_type, zmq::message_t &zmq_msg) : event_type(event_type) {
        ptr = new MsgBuffer(zmq_msg);
    }
    Event(Type event_type, uint32_t task_type, txn_id_t txn_id)
        : event_type(event_type), task_type(task_type), value(txn_id) {}

    Event(const Event &other) = delete;
    Event(Event &&other) { *this = std::move(other); }

    Event &operator=(Event &&other) {
        this->event_type = other.event_type;
        switch (this->event_type) {
            case RPC:
                this->ptr = other.ptr;
                break;
            case TASK:
                this->task_type = other.task_type;
                this->value = other.value;
                break;
            case NONE:
                break;
            default:
                ASSERT(false) << this->event_type << " " << other.event_type;
        }
        other.event_type = NONE;
        return *this;
    }

    ~Event() {
        switch (this->event_type) {
            case RPC:
                delete reinterpret_cast<MsgBuffer *>(ptr);
                break;
            case NONE:
            case TASK:
                break;
            default:
                break;
        }
    }
};

class EventChannel {
   private:
    std::mutex d_mutex;
    std::condition_variable d_condition;
    std::deque<Event> d_queue;
    std::deque<Event> prior_queue;

   public:
    inline size_t size() {
        return d_queue.size() + prior_queue.size();
    }

    inline void push_event(Event& event) {
        {
            std::lock_guard<std::mutex> lock(this->d_mutex);
            d_queue.emplace_front(std::move(event));
        }
        this->d_condition.notify_one();
    }

    template<class T>
    inline void push_rpc_event(T &msg) {
        {
            std::lock_guard<std::mutex> lock(this->d_mutex);
            d_queue.emplace_front(Event::RPC, msg);
        }
        this->d_condition.notify_one();
    }

    template<class T>
    inline void push_prior_rpc_event(T &msg) {
        {
            std::lock_guard<std::mutex> lock(this->d_mutex);
            prior_queue.emplace_front(Event::RPC, msg);
        }
        this->d_condition.notify_one();
    }

    template<class T>
    inline void push_prior_rpc_event_back(T &msg) {
        {
            std::lock_guard<std::mutex> lock(this->d_mutex);
            prior_queue.emplace_back(Event::RPC, msg);
        }
        this->d_condition.notify_one();
    }

    inline void push_task_event(uint32_t task_type, txn_id_t txn_id) {
        {
            std::lock_guard<std::mutex> lock(this->d_mutex);
            d_queue.emplace_front(Event::TASK, task_type, txn_id);
        }
        this->d_condition.notify_one();
    }

    template <typename T>
    inline void push_task_event(uint32_t task_type, T ptr) {
        {
            std::lock_guard<std::mutex> lock(this->d_mutex);
            d_queue.emplace_front(Event::TASK, task_type, reinterpret_cast<uint64_t>(ptr));
        }
        this->d_condition.notify_one();
    }

    template <typename T>
    inline void push_prior_task_event(uint32_t task_type, T ptr) {
        {
            std::lock_guard<std::mutex> lock(this->d_mutex);
            prior_queue.emplace_front(Event::TASK, task_type, reinterpret_cast<uint64_t>(ptr));
        }
        this->d_condition.notify_one();
    }

    template <typename T>
    inline void push_prior_task_event_back(uint32_t task_type, T ptr) {
        {
            std::lock_guard<std::mutex> lock(this->d_mutex);
            prior_queue.emplace_back(Event::TASK, task_type, reinterpret_cast<uint64_t>(ptr));
        }
        this->d_condition.notify_one();
    }

    inline void push_null_event() {
        {
            std::lock_guard<std::mutex> lock(this->d_mutex);
            d_queue.emplace_front();
        }
        this->d_condition.notify_one();
    }

    inline void batch_push_task_event(uint32_t task_type, uint32_t count) {
        // ONLY used in pushing new txns
        ASSERT(task_type == 0);
        {
            std::lock_guard<std::mutex> lock(this->d_mutex);
            for (uint32_t i = 0; i < count; ++i) {
                d_queue.emplace_front(Event::TASK, task_type, NULL_TXN_ID);
            }
        }
        this->d_condition.notify_one();
    }

    inline Event pop() {
        std::unique_lock<std::mutex> lock(this->d_mutex);
        this->d_condition.wait(
            lock, [=] { return !this->d_queue.empty() || !this->prior_queue.empty(); });
        // Get ret from prior_queue if prior queue not empty
        auto &q = this->prior_queue.size() ? this->prior_queue : this->d_queue;
        Event ret(std::move(q.back()));
        q.pop_back();
        return ret;
    }

    inline Event pop_prior_once(bool &success) {
        std::lock_guard<std::mutex> lock(this->d_mutex);
        if (this->prior_queue.empty()) {
            success = false;
            return Event();
        } else {
            Event ret(std::move(this->prior_queue.back()));
            this->prior_queue.pop_back();
            success = true;
            return ret;
        }
    }

    inline Event pop_normal_once(bool &success) {
        std::lock_guard<std::mutex> lock(this->d_mutex);
        if (this->d_queue.empty()) {
            success = false;
            return Event();
        } else {
            Event ret(std::move(this->d_queue.back()));
            this->d_queue.pop_back();
            success = true;
            return ret;
        }
    }

    inline Event pop_once(bool &success) {
        std::lock_guard<std::mutex> lock(this->d_mutex);
        if (this->d_queue.empty() && this->prior_queue.empty()) {
            success = false;
            return Event();
        } else {
            auto &q = this->prior_queue.size() ? this->prior_queue : this->d_queue;
            Event ret(std::move(q.back()));
            q.pop_back();
            success = true;
            return ret;
        }
    }

    void print() {
        LOG(2) << VAR2(prior_queue.size(), " ") << VAR2(d_queue.size(), " ");
    }
};