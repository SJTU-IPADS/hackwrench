#pragma once
#include <atomic>
#include <mutex>

#include "rpc/rpc.h"
#include "servers/config.h"


// Forward declaration can't declare a nested class
struct TxnInfo {
    enum Status { NORMAL = 0, COUNT_BLOCKED_SEGS };
    TxnInfo(MsgBuffer &&_zmq_msg, const Configuration& conf)
        : zmq_msg(std::move(_zmq_msg)),
          flat_array(CAST_CAPNP(zmq_msg.data(), zmq_msg.size())),
          num_blocked_segs(0), 
          status(NORMAL), old_txn_info(nullptr) {
#ifdef FINE_VALIDATION
        num_blocked_locks.store(0);
#endif
        msg = flat_array.getRoot<RpcMessage>();
        req_db = msg.getSender();
        auto args =  msg.getData().getPrepareargs();
        txn_id = args.getTxnId();
        fastCommitEnabled = args.getFastPathEnabled();
        primary_sn_id = args.getPrimarySnId();
#if defined(TOTAL_ORDER_TS) || defined(DETERMINISTIC_VALIDATION)
        seq = args.getSeq();
#endif
        r_no = replica_no_of(conf.get_physical_sn_id(primary_sn_id), 
            conf.get_my_sn_id(), conf.numSN());
        copied = false;
    }

    TxnInfo(PrepareArgs::Reader args, node_id_t sender, 
            const Configuration& conf)
        : zmq_msg(), flat_array(CAST_CAPNP(zmq_msg.data(), zmq_msg.size())),
          num_blocked_segs(0), 
          status(NORMAL), old_txn_info(nullptr) {
#if defined(TOTAL_ORDER_TS) || defined(DETERMINISTIC_VALIDATION)
        seq = args.getSeq();
#endif
#ifdef FINE_VALIDATION
        num_blocked_locks.store(0);
#endif
        node_id_t receiver = conf.get_my_sn_id();
        txn_id = args.getTxnId();
        primary_sn_id = args.getPrimarySnId();
        fastCommitEnabled = args.getFastPathEnabled();
        req_db = sender;
        r_no = replica_no_of(conf.get_physical_sn_id(primary_sn_id), 
            conf.get_my_sn_id(), conf.numSN());

        RpcMessage::Builder request = msgBuilder.initRoot<RpcMessage>();
        request.setReceiver(receiver);
        request.setSender(sender);
        PrepareArgs::Builder prepareArgs = request.initData().initPrepareargs();
        prepareArgs.setTxnId(txn_id);
        prepareArgs.setSegments(args.getSegments());
        prepareArgs.setTxns(args.getTxns());
        prepareArgs.setTxnInputs(args.getTxnInputs());
        prepareArgs.setPrimarySnId(primary_sn_id);
        msg = msgBuilder.getRoot<RpcMessage>();
        copied = true;
    }

    ~TxnInfo() {
        if (old_txn_info) {
            delete old_txn_info;
        }
    }

    static uint32_t replica_no_of(node_id_t primary_sn_id, node_id_t my_sn_id, uint32_t num_sn) {
        uint32_t r_no = my_sn_id - primary_sn_id;
        if (r_no < 0) {
            r_no = my_sn_id + num_sn - primary_sn_id;
        }
        return r_no;
    }

    txn_id_t txn_id;
    uint64_t primary_sn_id;
    // set to corresponding msg_id, binding the client to certain worker thread...

    ::capnp::MallocMessageBuilder msgBuilder;
    MsgBuffer zmq_msg;
    RpcMessage::Reader msg;
    ::capnp::FlatArrayMessageReader flat_array;
    std::atomic<int32_t> num_blocked_segs;
    std::atomic<int32_t> msg_count;
    MultiThreadTimer timer;
    RdtscTimer rdtsc_timer; // TODO
    MultiThreadTimer total_timer;
    Status status;
    node_id_t req_db;
    uint32_t r_no;
    TxnInfo *old_txn_info;
    std::vector<void *> ordered_txns;
    bool fastCommitEnabled = false;
    // std::mutex mtx;

    bool copied;
    bool repaired = false;
    bool aborted = false;
    bool prepare_replicated = false;
    std::vector<uint> repaired_txns;
    uint64_t seq;
#ifdef FINE_VALIDATION
    std::atomic<int32_t> num_blocked_locks;
    bool lock_acquired = false;
#endif
    // std::vector<std::pair<seg_id_t, txn_id_t>> blocked_segs;
};