#pragma once

#include <map>
#include <mutex>

#include "rpc/rpc.h"
#include "servers/config.h"
#include "storage/segment.h"
#include "util/event_channel.h"

class TimestampLayer {
   private:
    TxnDependencyWithLock *deps;
#ifdef TOTAL_ORDER_TS
    std::atomic<uint64_t> seq;
#endif
   public:
    TimestampLayer(Configuration &config) {
        deps = new TxnDependencyWithLock[config.numSegments()];
#ifdef TOTAL_ORDER_TS
        seq.store(0);
#endif
    }

    ~TimestampLayer() { delete[] deps; }

    void get_timestamp(GetTimestampArgs::Reader args, GetTimestampResponse::Builder resp) {
        resp.setTxnId(args.getTxnId());
#ifndef TOTAL_ORDER_TS
        auto rw_seg_ids = args.getRwSegIds();
        auto reply_deps = resp.initDeps(rw_seg_ids.size());
        uint32_t i = 0;
        std::vector<std::unique_lock<std::mutex>> locks;
        for (const seg_id_t &rw_seg_id : rw_seg_ids) {
            seg_id_t seg_id = SEG_GET_ID(rw_seg_id);
            locks.emplace_back(deps[seg_id].mtx);
            TxnDependency &dep = deps[seg_id].dep;

            auto reply_dep = reply_deps[i];
            reply_dep.setTimestamp(dep.ts);
            if (SEG_IS_WRITE(rw_seg_id)) {
                reply_dep.setNumReads(dep.num_reads);
                dep.ts++;
                dep.num_reads = 0;
            } else {
                reply_dep.setNumReads(dep.num_reads);
                dep.num_reads += 1;
            }
            ++i;
        }
#else
        uint64_t old_seq = seq.fetch_add(1);
        resp.setSeq(old_seq);
#endif
        resp.setOk(true);
    }
};

class TimestampRpcServer : public RpcServerBase {
   private:
    TimestampLayer *timeServer;
    std::atomic<int> thpt_counter;
    std::vector<ThreadSafeQueue<MsgBuffer *> *> requests_queues;

   public:
    TimestampRpcServer(const Configuration &conf, TimestampLayer *timeServer)
        : RpcServerBase(conf), timeServer(timeServer) {
        for (uint i = 0; i < conf.get_my_node().num_threads; i++) {
            requests_queues.emplace_back(new ThreadSafeQueue<MsgBuffer *>);
        }
    }

    ~TimestampRpcServer() {
        for (auto p : requests_queues) {
            delete p;
        }
    }

    void serve_one_request(EventChannel* channel, thread_id_t tid) {
        Event e = channel->pop();
        MsgBuffer &zmq_msg = *reinterpret_cast<MsgBuffer *>(e.ptr);

        BUILD_MESSAGE_FROM_BUFFER(zmq_msg, msg);
        BUILD_REPLY_NO_HASH(msg)
        if (msg.getData().isGettimestampargs()) {
            assert(msg.getReceiver() == conf.get_my_id());

            auto dat = msg.getData();
            auto args = dat.getGettimestampargs();
            auto data = reply.initData().initGettimestampresponse();
            timeServer->get_timestamp(args, data);
        } else {
            auto dat = msg.getData();
            auto args = dat.getBatchgettimestampargs().getGettimestampargs();
            auto data = reply.initData().initBatchgettimestampresponse();
            data.setLbatchId(dat.getBatchgettimestampargs().getLbatchId());
            auto resp = data.initGettimestampresponse(args.size());
            for (uint i = 0; i < args.size(); i++) {
                timeServer->get_timestamp(args[i], resp[i]);
            }
        }
        send(msgBuilder);


        // this->thpt_counter++;
    }

    void run() {
        auto worker_func = [this](thread_id_t tid) {
            socket_t_id = tid;
            while (1) {
                serve_one_request(event_channels[tid], tid);
            }
        };
        std::vector<std::thread> worker_threads;
        for (thread_id_t t_id = 0; t_id < num_threads; ++t_id) {
            worker_threads.push_back(std::thread(worker_func, t_id));
        }

        thpt_counter = 0;
        while (1) {
            thread_sleep(1);
            LOG(2) << "realtime thpt: " << thpt_counter << " reqs/s";
            thpt_counter = 0;

            // for (uint32_t i = 0; i < batch_ids.size(); ++i) {
            //     LOG(2) << "i: " << i << " " << std::hex << batch_ids[i];
            // }
        }

        for (auto &thread : worker_threads) {
            thread.join();
        }
    }
};
