#pragma once

#include <capnp/message.h>
#include <capnp/serialize-packed.h>

#include <functional>
#include <map>
#include <mutex>
#include <typeinfo>
#include <unordered_map>
#include <vector>
#include <zmq.hpp>

#include "SNinterface.capnp.h"
#include "message_buffer.h"
#include "servers/config.h"
#include "util/core_binding.h"
#include "util/event_channel.h"
#include "util/fast_random.h"
#include "util/timer.h"

// some helper macros
#define PRINT_MESSAGE(msg) (msg.toString().flatten().cStr())
// cast a piece of memory to capnp array, so that we can read the content of the message
#define CAST_CAPNP(data, size)                                                    \
    (kj::ArrayPtr<capnp::word const>(reinterpret_cast<capnp::word const *>(data), \
                                     ((size) / sizeof(capnp::word))))
// the RpcMessage is just a read pointer of kjArray, so we define this macro to
// help us remember not to pass RpcMessage around.
#define BUILD_MESSAGE_FROM_ARRAY(wordArray, ret)                    \
    auto _flat_array_ = ::capnp::FlatArrayMessageReader(wordArray); \
    auto ret = _flat_array_.getRoot<RpcMessage>();
#define BUILD_MESSAGE_FROM_BUFFER(msg, ret)   \
    BUILD_MESSAGE_FROM_ARRAY(CAST_CAPNP((msg).data(), (msg).size()), ret)
#define BUILD_MESSAGE_FROM_ZMQ(msg, ret)                      \
    void *__aligned_buffer_tmp = malloc(msg.size());          \
    memcpy(__aligned_buffer_tmp, (msg).data(), (msg).size()); \
    BUILD_MESSAGE_FROM_ARRAY(CAST_CAPNP(__aligned_buffer_tmp, msg.size()), ret)
#define BUILD_REQUEST(to)                                            \
    ::capnp::MallocMessageBuilder msgBuilder;                        \
    RpcMessage::Builder request = msgBuilder.initRoot<RpcMessage>(); \
    request.setSender(this->conf.get_my_id());                       \
    request.setReceiver(to)
#define BUILD_REQUEST1(to, size)                                            \
    ::capnp::MallocMessageBuilder msgBuilder(size);                        \
    RpcMessage::Builder request = msgBuilder.initRoot<RpcMessage>(); \
    request.setSender(this->conf.get_my_id());                       \
    request.setReceiver(to)
#define BUILD_REPLY(msg)                                           \
    ::capnp::MallocMessageBuilder msgBuilder;                      \
    RpcMessage::Builder reply = msgBuilder.initRoot<RpcMessage>(); \
    reply.setIsReply(true);                                        \
    reply.setSender(this->conf.get_my_id());                       \
    reply.setReceiver(msg.getSender());
#define BUILD_REPLY_NO_HASH(msg)                                   \
    ::capnp::MallocMessageBuilder msgBuilder;                      \
    RpcMessage::Builder reply = msgBuilder.initRoot<RpcMessage>(); \
    reply.setSender(this->conf.get_my_id());                       \
    reply.setReceiver(msg.getSender());

enum ServerStatus { INIT_OK = 0, START = 1, END = 2, REPORT =3 };

__thread thread_id_t socket_t_id = 0;
__thread uint64_t sent_size = 0, sent_times = 0;
volatile bool start_benchmarking_flag = false;

class zmqServer {
   private:
    zmq::socket_t socket;
    EventChannel **event_channels;
    EventChannel *logistics_channel;
    thread_id_t num_threads;
    thread_id_t beginning;

   public:
    blockingQueue<MsgBuffer> recv_queue;
    // blockingHash<MsgBuffer> recv_hash;

    zmqServer(zmq::context_t *context, uint port, EventChannel **event_channels,
              thread_id_t num_threads, thread_id_t beginning, EventChannel *logistics_channel)
        : socket(*context, ZMQ_PULL),
          event_channels(event_channels),
          logistics_channel(logistics_channel),
          num_threads(num_threads),
          beginning(beginning) {
        // bind address
        char address[100];
        snprintf(address, 100, "tcp://*:%d", port);
        // LOG(2) << beginning << " listening on " << address;
        socket.bind(address);
        ASSERT(num_threads == 1);
    };

    void run() {
        sigset_t mask;
        sigemptyset(&mask);
        sigaddset(&mask, SIGPROF);
        pthread_sigmask(SIG_BLOCK, &mask, NULL);
        if (event_channels) {
            uint64_t rr_count = beginning;
            while (true) {
                try {
                    zmq::message_t msg;
                    socket.recv(msg, zmq::recv_flags::none);

                    MsgBuffer msg_buf(msg);
                    BUILD_MESSAGE_FROM_BUFFER(msg_buf, capnp_msg);

                    auto data = capnp_msg.getData();
                    if (data.isReportargs() || data.isSyncargs()) {
                        // LOG(2) << "recv sync from: " << capnp_msg.getSender();
                        logistics_channel->push_rpc_event(msg);
                        continue;
                    }

                    if (data.isPrepareresponse() || data.isPrepareargs()) {
                        event_channels[rr_count]->push_prior_rpc_event_back(msg_buf);
                    } else {
                        event_channels[rr_count]->push_prior_rpc_event(msg_buf);
                    }
                    if (++rr_count == beginning + num_threads) {
                        rr_count = beginning;
                    }
                } catch (const zmq::error_t &e) {
                    LOG(2) << "error: " << e.what();
                    ASSERT(false);
                    continue;
                }
            }
        } else {
            ASSERT(false);
            while (true) {
                try {
                    zmq::message_t msg;
                    socket.recv(msg, zmq::recv_flags::none);
                    MsgBuffer msg_buf(msg);
                    recv_queue.push(msg_buf);
                } catch (const zmq::error_t &e) {
                    LOG(2) << e.what();
                    continue;
                }
            }
        }
    }

    void run_ts() {
        sigset_t mask;
        sigemptyset(&mask);
        sigaddset(&mask, SIGPROF);
        pthread_sigmask(SIG_BLOCK, &mask, NULL);
        ASSERT(event_channels);
        ASSERT(num_threads == 1);
        LOG(2) << VAR(beginning);
        while (true) {
            try {
                zmq::message_t msg;
                socket.recv(msg, zmq::recv_flags::none);
                MsgBuffer msg_buf(msg);
                event_channels[beginning]->push_prior_rpc_event(msg_buf);
            } catch (const zmq::error_t &e) {
                LOG(2) << "error: " << e.what();
                ASSERT(false);
                continue;
            }
        }
    }

};

class zmqClient {
   private:
    //  Prepare our context and socket
    zmq::socket_t socket;
    std::mutex send_mutex;  // TODO: zmqClient per thread;
    char address[100];

   public:
    zmqClient(zmq::context_t *context, const char *address) : socket(*context, ZMQ_PUSH) {
        strcpy(this->address, address);
        socket.connect(this->address);
    };

    zmqClient(zmq::context_t *context, std::string hostname, int port)
        : socket(*context, ZMQ_PUSH) {
        snprintf(address, sizeof(address), "tcp://%s:%d", hostname.c_str(), port);
        // LOG(2) << "connect to " << address;
        socket.connect(address);
    }

    // TODO: zero copy, zmqClient per thread
    void send(void *data, size_t size, zmq::free_fn *free = nullptr) {
        zmq::message_t m(data, size);
        while (true) {
            try {
                socket.send(m, zmq::send_flags::none);
                break;
            } catch (zmq::error_t &e) {
                // LOG(2) << e.what();
                ASSERT(false);
                continue;
            }
        }
        if (start_benchmarking_flag) {
            sent_size += size, sent_times += 1;
        }
    }

    void send_shared(void *data, size_t size, zmq::free_fn *free = nullptr) {
        std::lock_guard<std::mutex> guard(send_mutex);
        zmq::message_t m(data, size);
        while (true) {
            try {
                socket.send(m, zmq::send_flags::none);
                break;
            } catch (zmq::error_t &e) {
                LOG(2) << e.what();
                ASSERT(false);
                continue;
            }
        }
        if (start_benchmarking_flag) {
            sent_size += size, sent_times += 1;
        }
    }
};

class peerConnection {
   private:
    std::vector<zmqClient *> clients;

   public:
    peerConnection() = delete;
    peerConnection(zmq::context_t *context, std::string hostname, int port, int network_threads) {
        for (int i = 0; i < network_threads; i++) {
            clients.push_back(new zmqClient(context, hostname, port + i));
        }
    }
    ~peerConnection() {
        for (auto *client : clients) {
            delete client;
        }
    }

    void send(void *data, size_t size, uint32_t thread_num, zmq::free_fn *free = nullptr,
              bool rand = false, bool shared = false) {
        // uint32_t thread_num = thread_rand.next_u32() % clients.size();
        if (rand)
            thread_num = thread_rand.next_u32() % clients.size();
        else
            thread_num = thread_num % clients.size();
        if (shared) {
            clients[thread_num]->send_shared(data, size, free);
        } else {
            clients[thread_num]->send(data, size, free);
        }
    };
};

class Connections {
   private:
    const Configuration &conf;
    zmq::context_t *context;
    thread_id_t num_threads;
    thread_id_t network_threads;
    zmqServer **receivers;
    std::unordered_map<uint32_t, peerConnection *> peer_conns;
    thread_id_t target_ts_thread;

   public:
    Connections(const Configuration &conf, EventChannel **event_channels, thread_id_t num_threads,
                thread_id_t network_threads, EventChannel *logistics_channel,
                uint32_t io_threads = 1)
        : conf(conf),
          context(new zmq::context_t(io_threads)),
          num_threads(num_threads),
          network_threads(network_threads) {
        // if (conf.get_my_node_type() == Configuration::DB) {
        //     uint cores = std::thread::hardware_concurrency();
        //     for (uint i = 0; i < io_threads; i++) {
        //         zmq_ctx_set(context, ZMQ_THREAD_AFFINITY_CPU_ADD, cores - 1 - i);
        //     }
        // }
        receivers = new zmqServer *[this->network_threads];
        thread_id_t thread_per_server;
        if (conf.get_my_node_type() == Configuration::SN ||
            conf.get_my_node_type() == Configuration::DB) {
            ASSERT(num_threads == network_threads) << num_threads << " " << network_threads;
            thread_per_server = num_threads / network_threads;
        } else {
            thread_per_server = 1;
        }
        for (thread_id_t i = 0; i < this->network_threads; ++i) {
            receivers[i] =
                new zmqServer(context, conf.get_my_node().port + i, event_channels,
                              thread_per_server, thread_per_server * i, logistics_channel);
        }

        for (auto &node : conf.get_all_nodes()) {
            uint32_t num_conns = 1;
            if ((node.second->node_type == Configuration::SN && conf.get_my_node_type() == Configuration::DB) ||
                (node.second->node_type == Configuration::DB && conf.get_my_node_type() == Configuration::SN) ||
                (node.second->node_type == Configuration::DB && conf.get_my_node_type() == Configuration::TS) ||
                (node.second->node_type == Configuration::TS && conf.get_my_node_type() == Configuration::DB) ||
                (node.second->node_type == Configuration::DB && conf.get_my_node_type() == Configuration::CLIENT)) {
                num_conns = node.second->num_threads;
            } else if ((node.second->node_type == Configuration::DB && conf.get_my_node_type() == Configuration::DB 
                && !(conf.get_db_id(node.first) == 0 || conf.get_my_db_id() == 0))) {
                num_conns = 0;
            }
            peer_conns[node.first] =
                new peerConnection(context, node.second->host, node.second->port, num_conns);
        }

        target_ts_thread = conf.get_my_db_id() % conf.get_ts_node()->num_threads;
        LOG(3) << "Finish connecting " << VAR(target_ts_thread);
    }

    ~Connections() {
        for (thread_id_t i = 0; i < network_threads; ++i) {
            delete receivers[i];
        }
        for (auto iter : peer_conns) {
            delete iter.second;
        }
        delete[] receivers;
        delete context;
    }

    std::thread run(thread_id_t t_id = 0) {
        return std::thread(
            [=](thread_id_t t_id) {
                this->receivers[t_id]->run();
            },
            t_id);
    }

    std::thread run_ts(thread_id_t t_id = 0) {
        return std::thread(
            [=](thread_id_t t_id) {
                this->receivers[t_id]->run_ts();
            }, t_id);
    }
    
    inline void send(uint32_t nodeId, void *data, size_t size, zmq::free_fn *free) {
        if (conf.get_my_node_type() == Configuration::TS) {
            peer_conns.at(nodeId)->send(data, size, socket_t_id, free, true, true);
        } 
        else if (conf.get_node(nodeId).node_type == Configuration::SN) {
            peer_conns.at(nodeId)->send(data, size, socket_t_id, free, true, true);
        } 
        else if (conf.get_my_node_type() == Configuration::DB &&
                   conf.get_node(nodeId).node_type == Configuration::DB) {
            peer_conns.at(nodeId)->send(data, size, 0, free, false, true);
        } else if (conf.get_ts_node()->node_id == nodeId) {
            peer_conns.at(nodeId)->send(data, size, target_ts_thread, free, false, true);
        } else if (conf.get_my_node().num_threads > conf.get_node(nodeId).num_threads) {
            peer_conns.at(nodeId)->send(data, size, socket_t_id, free, false, true);
        } else {
            peer_conns.at(nodeId)->send(data, size, socket_t_id, free);
        }
    }

    inline MsgBuffer pop_recv_q(uint queue_id = 0) { return receivers[queue_id]->recv_queue.pop(); }
    inline std::vector<MsgBuffer> pop_recv_q_batch(int &pop_batch_size, uint queue_id = 0) {
        return receivers[queue_id]->recv_queue.pop_batch(pop_batch_size);
    }
    inline bool pop_recv_q_once(MsgBuffer &zmq_msg) {
        return receivers[0]->recv_queue.pop_once(zmq_msg);
    }
    // inline MsgBuffer recv_message(msg_id_t msgId) { return receivers[0]->recv_hash.get(msgId); }
    // inline bool recv_message_once(msg_id_t msgId, MsgBuffer &zmq_msg) {
    //     return receivers[0]->recv_hash.get_once(msgId, zmq_msg);
    // }
};

class RpcServerBase {
   public:
    const Configuration &conf;
    thread_id_t num_threads;
    EventChannel **event_channels;
    Connections *conn;
    EventChannel logistics_channel;
    bool is_sn;

   public:
    RpcServerBase(const Configuration &conf)
        : conf(conf),
          num_threads(conf.get_my_node().num_threads),
          event_channels(new EventChannel *[num_threads]) {
        // thread_id_t network_threads =
        //     conf.get_my_node_type() == Configuration::TS ? 1 : num_threads;
        thread_id_t network_threads = num_threads;
        thread_id_t io_threads = 1;
        is_sn = conf.get_my_node_type() == Configuration::SN;
        switch (conf.get_my_node_type()) {
            case Configuration::DB:
#ifdef BATCH_PREPARE
                io_threads = (network_threads + 3) / 4;
#else
                io_threads = 1;
#endif
                break;
            case Configuration::SN:
                io_threads = (network_threads + 1) / 2;
                break;
            case Configuration::TS:
                io_threads = (network_threads + 1) / 4;
                break;
            default:
                io_threads = 1;
                break;
        }
        for (thread_id_t i = 0; i < num_threads; ++i) {
            event_channels[i] = new EventChannel();
        }
        conn = new Connections(
            conf, event_channels, num_threads, network_threads, &logistics_channel, io_threads);

        std::vector<std::thread> conn_threads;
        for (thread_id_t i = 0; i < network_threads; ++i) {
            if (conf.get_my_node_type() != Configuration::TS) {
                conn_threads.emplace_back(conn->run(i));
            } else {
                conn_threads.emplace_back(conn->run_ts(i));
            }
            CPUBinder::bind(conn_threads[i].native_handle(),
                            std::thread::hardware_concurrency() - 1 - i);
        }
        for (thread_id_t i = 0; i < network_threads; ++i) {
            conn_threads[i].detach();
        }
    }

    ~RpcServerBase() {
        for (thread_id_t i = 0; i < num_threads; ++i) {
            delete event_channels[i];
        }
        delete[] event_channels;
    }
public:

    inline EventChannel *get_one_event_channel() {
        return event_channels[thread_rand.next_u32() % num_threads];
    }

    inline EventChannel *get_one_event_channel_startfrom(uint32_t thread_id) {
        return event_channels[thread_id + thread_rand.next_u32() % (num_threads - thread_id)];
    }

    inline EventChannel *get_specific_event_channel(thread_id_t thread_id) {
        return event_channels[thread_id];
    }

    inline EventChannel *get_free_event_channel(thread_id_t my_thread_id) {
        thread_id_t min_i = 0;
        size_t min_size = std::numeric_limits<size_t>::max();
        for (thread_id_t ii = 0; ii < num_threads; ++ii) {
            thread_id_t i = (ii + my_thread_id) % num_threads; 
            size_t i_size = event_channels[i]->size();
            if (i_size == 0) {
                return event_channels[i];
            } else if (i_size < min_size) {
                min_size = i_size;
                min_i = i;
            }
        }
        return event_channels[min_i];
    }

    inline void send(::capnp::MallocMessageBuilder &msgBuilder) {
        auto message = msgBuilder.getRoot<RpcMessage>();
        auto wordArray = capnp::messageToFlatArray(msgBuilder);
        uint32_t byteSize = wordArray.size() * sizeof(capnp::word);
        conn->send(message.getReceiver(), wordArray.begin(), byteSize, NULL);
    }

    inline Event pop_event_no_steal(thread_id_t worker_id) { 
        return event_channels[worker_id]->pop(); 
    }

    inline Event pop_event(thread_id_t worker_id) { 
        bool success;
        Event e = event_channels[worker_id]->pop_once(success);
        if (success) {
            return e;
        }
        e = steal_event(worker_id, success);
        if (success) {
            return e;
        }
        return event_channels[worker_id]->pop(); 
    }

    inline Event pop_event_without_steal(thread_id_t worker_id, thread_id_t except) { 
        bool success;
        Event e = event_channels[worker_id]->pop_once(success);
        if (success) {
            return e;
        }
        e = steal_event_except(worker_id, except, success);
        if (success) {
            return e;
        }
        return event_channels[worker_id]->pop(); 
    }

    inline Event steal_event_except(thread_id_t worker_id, thread_id_t except, bool &success) { 
        for (uint i = 0; i < num_threads - 1; ++i) {
            thread_id_t j = (worker_id + i + 1) % num_threads;
            if (is_sn && j == except) {
                continue;
            }
            Event e = event_channels[j]->pop_prior_once(success);
            if (success) {
                return e;
            }
        }
        for (uint i = 0; i < num_threads - 1; ++i) {
            thread_id_t j = (worker_id + i + 1) % num_threads;
            Event e = event_channels[j]->pop_normal_once(success);
            if (success) {
                return e;
            }
        }
        success = false;
        return Event();
    }

    inline Event steal_event(thread_id_t worker_id, bool &success) { 
        for (uint i = 0; i < num_threads - 1; ++i) {
            thread_id_t j = (worker_id + i + 1) % num_threads;
            Event e = event_channels[j]->pop_prior_once(success);
            if (success) {
                return e;
            }
        }
        for (uint i = 0; i < num_threads - 1; ++i) {
            thread_id_t j = (worker_id + i + 1) % num_threads;
            Event e = event_channels[j]->pop_normal_once(success);
            if (success) {
                return e;
            }
        }
        success = false;
        return Event();
    }
    inline Event pop_event_once(thread_id_t worker_id, bool &success) {
        return event_channels[worker_id]->pop_once(success);
    }
    inline MsgBuffer recv() { return conn->pop_recv_q(); }
    // inline bool recv_once(zmq::message_t &zmq_msg) { return conn->pop_recv_q_once(zmq_msg); }
    // inline zmq::message_t recv(msg_id_t msg_id) { return conn->recv_message(msg_id); }
    // inline bool recv_once(msg_id_t msg_id, zmq::message_t &zmq_msg) {
    //     return conn->recv_message_once(msg_id, zmq_msg);
    // }
    ServerStatus recv_sync() {
        Event e = logistics_channel.pop();
        MsgBuffer &msg = *reinterpret_cast<MsgBuffer *>(e.ptr);
        BUILD_MESSAGE_FROM_BUFFER(msg, args);
        return (ServerStatus)args.getData().getSyncargs().getStatus();
    }

    void send_sync_req(node_id_t node_id, ServerStatus status) {
        BUILD_REQUEST(node_id);
        auto sync = request.initData().initSyncargs();
        sync.setStatus(status);
        send(msgBuilder);
    }
};
