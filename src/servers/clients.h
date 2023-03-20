#pragma once

#include <unistd.h>

#include "config.h"
#include "rpc/rpc.h"
#include "util/macros.h"
#include "util/statistic.h"
#include "util/thread_safe_structures.h"
#include "util/timer.h"

struct Client {
    Key_t key = 0;
    Client(Key_t key) : key(key) {}
};
class ClientServer : public RpcServerBase {
    const BenchmarkInterface &bench;
    ThreadSafeQueue<Client *> free_clients;
    uint32_t num_clients;

   public:
    ClientServer(const Configuration &conf, const BenchmarkInterface &bench, uint32_t num_clients)
        : RpcServerBase(conf), bench(bench), num_clients(num_clients) {
        // TODO: bench.init_clients();
        for (uint i = 0; i < num_clients; ++i) {
            free_clients.enqueue(new Client(i));
        }
    }

    void send_thread(bool &sending, thread_id_t thread_id) {
        socket_t_id = thread_id;
        node_id_t count = 0;
        node_id_t sum = 0;
        while (sending) {
            Client *c = free_clients.dequeue();  // this is blocked by recv_thread
            node_id_t to_id = rr_db_id();
            // node_id_t to_id = (c->key % conf.numDB()) + conf.numSN() + conf.numTS();
            // node_id_t to_id = 2;
            sum += to_id;
            count++;
            BUILD_REQUEST(to_id);
            auto req = request.initData().initClientargs();
            req.setClientId((uint64_t)c);
            req.setData(c->key);
            req.setTxnType(thread_rand.randint(0, 99));
            send(msgBuilder);
        }
        LOG(2) << "send thread quit " << (double)sum / (double)count;
    }

    void recv_thread() {
        bool benchmarking = true;
        while (benchmarking || free_clients.size() != num_clients) {
            Event event = pop_event(0);
            if (event.event_type == Event::NONE) {
                benchmarking = false;
                // keep running until we receive all replies.
                LOG(2) << "recv thread waiting for all clients";
                continue;
            }
            ASSERT(event.event_type == Event::RPC);
            MsgBuffer &msg = *reinterpret_cast<MsgBuffer *>(event.ptr);
            BUILD_MESSAGE_FROM_BUFFER(msg, reply);
            ASSERT(reply.getData().isClientresponse());
            auto clientIds = reply.getData().getClientresponse().getClientIds();
            for (uint i = 0; i < clientIds.size(); ++i) {
                Client *c = (Client *)clientIds[i];
                ASSERT(c);
                free_clients.enqueue(c);
            }
        }
        LOG(2) << "recv thread quit";
        for (uint i = 0; i < conf.numSN(); ++i) {
            BUILD_REQUEST(i);
            auto req = request.initData().initSyncargs();
            req.setStatus(ServerStatus::END);
            send(msgBuilder);
        }
    }

    void run() {
        bool sending = true;
        ServerStatus status = recv_sync();
        ASSERT(status == ServerStatus::START);
        std::vector<std::thread *> workers;

        for (uint i = 0; i < conf.get_primary_db_node()->num_threads; i++) {
            std::thread *t =
                new std::thread([this, &sending](auto i) { this->send_thread(sending, i); }, i);
            workers.push_back(t);
        }
        std::thread t2([this]() { this->recv_thread(); });

        status = recv_sync();
        ASSERT(status == ServerStatus::END);
        sending = false;  // stop the send thread immediately
        // stop the recv thread when all requests are replied
        this->get_specific_event_channel(0)->push_null_event();
        for (auto t : workers) {
            t->join();
        }
        t2.join();
    }

    inline uint64_t random_db_id() {
        return conf.numSN() + conf.numTS() + thread_rand.randint(0, conf.numDB() - 1);
    }

    // std::atomic<int> rr;
    inline uint64_t rr_db_id() {
        thread_local uint64_t rr = 0;
        uint64_t ret = conf.numSN() + conf.numTS() + (rr % conf.numDB());
        rr++;
        return ret;
    }
};