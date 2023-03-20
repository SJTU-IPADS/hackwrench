#pragma once

#include <atomic>
#include <set>
#include <thread>
#include <unordered_map>
#include <unordered_set>

#include "config.h"
#include "rpc/rpc.h"
#include "txn/db_txn.h"
#include "util/core_binding.h"
#include "util/event_channel.h"
#include "util/lock_free_queue.h"
#include "util/statistic.h"
#include "util/thread_safe_structures.h"
#include "storage/sequence_manager.h"
#include "storage/lock_manager.h"
#include "storage/occ_lock_manager.h"
#include "storage/storage.h"

extern std::unique_ptr<BenchmarkInterface> benchmark;

enum SnTaskType : uint32_t {
    UNBLOCK_TXN,
    TRY_PREPARE_TXN,
    WORKER_PREPARE,
    WORKER_COMMIT,
    LOCAL_NEW_TXN
};

class StorageNodeRpcServer : public RpcServerBase {
    using redo_logs_t = std::unordered_map<g_page_id_t, LogMeta>;

   private:
    volatile bool running = false;
    InMemSegments &segments;
    std::atomic<uint> prepares, commits, repairs;
    std::atomic<uint> temp1;
    std::atomic<uint> count;
#ifdef FINE_VALIDATION
    LockManager lock_manager;
#endif
#ifdef NO_TS
    OCCLockManager occ_lock_manager;
#endif
    thread_id_t num_threads;
    BatchThreadSafeMap<batch_id_t, TxnInfo *>* ongoing_txns[3];
#if defined(TOTAL_ORDER_TS) || defined(DETERMINISTIC_VALIDATION)
    SequenceManager** seq_managers;
#endif

   public:
    StorageNodeRpcServer(const Configuration &conf, InMemSegments &segments)
        : RpcServerBase(conf),
          segments(segments),
#ifdef FINE_VALIDATION
          lock_manager(segments),
#endif
#ifdef NO_TS
          occ_lock_manager(segments),
#endif
          num_threads(conf.get_my_node().num_threads) {
        for (uint32_t i = 0; i < conf.REPLICA_COUNT; ++i) {
            ongoing_txns[i] = new BatchThreadSafeMap<batch_id_t, TxnInfo *>(num_threads);
        }
#if defined(TOTAL_ORDER_TS) || defined(DETERMINISTIC_VALIDATION)
        seq_managers = new SequenceManager*[conf.numDB()];
        for (node_id_t i = 0; i < conf.numDB(); i ++) {
            seq_managers[i] = new SequenceManager(num_threads);
        }
#endif
    }

    ~StorageNodeRpcServer() {
        for (uint32_t i = 0; i < conf.REPLICA_COUNT; ++i) {
            if (ongoing_txns[i] != nullptr) {
                delete ongoing_txns[i];
            }
        }
#if defined(TOTAL_ORDER_TS) || defined(DETERMINISTIC_VALIDATION)
        for (node_id_t i = 0; i < conf.numDB(); i ++) {
            delete seq_managers[i];
        }
        delete [] seq_managers;
#endif
    }

    void worker_logic(uint32_t thread_id) {
        while (running) {
            auto event = pop_event(thread_id);
            switch (event.event_type) {
                case Event::RPC:
                    handle_rpc(event);
                    break;
                case Event::TASK:
                    handle_task(event);
                    break;
                case Event::NONE:
                    break;
                default:
                    ASSERT(false);
            }
        }
    }

    void run() {
        benchmark->init_storage(segments);
        segments.finalize_value_size();

        auto worker_func = [this](uint32_t thread_id) {
            while (!running) {
                COMPILER_MEMORY_FENCE();
            }
            socket_t_id = thread_id;
            LatencyLogger::worker_register(thread_id);
            LOG(2) << "Storage Node Thread Id: " << thread_id;
            // VALBinder::bind(thread_id + 1);
            worker_logic(thread_id);
        };

        count = 0;
        std::vector<std::thread> worker_threads;
        for (thread_id_t t_id = 0; t_id < num_threads; ++t_id) {
            worker_threads.push_back(std::thread(worker_func, t_id));
            CPUBinder::bind(worker_threads[t_id].native_handle(), std::thread::hardware_concurrency() - 1 - t_id);
        }

        node_id_t primary_db_id = conf.get_primary_db_node()->node_id;
        send_sync_req(primary_db_id, ServerStatus::INIT_OK);

        ServerStatus status = recv_sync();
        ASSERT(status == ServerStatus::START) << status;

        running = true;

        thread_sleep(5);
        LOG(3) << "start bench";
        prepares = 0;
        commits = 0;
        repairs = 0;
        sent_size = 0;
        temp1 = 0;

        uint prev_commits = commits;
        uint prev_rep = repairs;
        uint prev_sentSizes = sent_size;
        uint prev_temp1 = temp1;
        for (uint i = 0; i < benchmark->get_time_duration(); i++) {
            thread_sleep(1);
            uint this_commits = commits;
            uint this_rep = repairs;
            uint this_sentSizes = sent_size;
            uint this_temp1 = temp1;
            LOG(2) << "commits/s: " << this_commits - prev_commits
                   << ", repairs/s: " << this_rep - prev_rep
                   << ", networkTput/s: " << this_sentSizes - prev_sentSizes
                   << ", temp1/s: " << this_temp1 - prev_temp1;
            if (this_commits - prev_commits == 0) {
                for (uint32_t i = 0; i < conf.REPLICA_COUNT; i++) {
                    if (this->ongoing_txns[i] == nullptr) {
                        continue;
                    }
                    auto map = this->ongoing_txns[i]->collect();
                    for (auto& iter : map) {
                        LOG(2) << i << " exists: " << std::hex<< iter.first 
                            << " " << iter.second->num_blocked_segs
#ifdef FINE_VALIDATION
                            << " " << iter.second->num_blocked_locks
#endif
#ifdef TOTAL_ORDER_TS
                            << " " << iter.second->seq
#endif
                            << "";
                    }
                }
                // for (uint i = 0; i < num_threads; ++i) {
                //     get_specific_event_channel(i)->print();
                // }
            }
            prev_commits = this_commits;
            prev_rep = this_rep;
            prev_sentSizes = this_sentSizes;
            prev_temp1 = this_temp1;
        }

        uint thpt = commits;

        status = recv_sync();
        ASSERT(status == ServerStatus::END);
        thread_sleep(2);

        // A temp fix to stop SN threads
        running = false;
        for (uint32_t i = 0; i < num_threads; ++i) {
            event_channels[i]->push_null_event();
        }

        for (auto &thread : worker_threads) {
            thread.join();
        }

        LatencyLogger::report();
        Logger::report();

        // send_correctness_check();
        // LOG(2) << "report sent";

        LOG(2) << "SN throughput: " << ((double)thpt) / benchmark->get_time_duration() << " txs/s";
        LOG(2) << "prepares: " << prepares << ", commits: " << commits << ", repairs: " << repairs;
        LOG(2) << "count: " << count;
    }

   private:
    void handle_rpc(Event& event) {
        MsgBuffer &zmq_msg = *reinterpret_cast<MsgBuffer *>(event.ptr);
        sent_size += zmq_msg.size();
        BUILD_MESSAGE_FROM_BUFFER(zmq_msg, msg);
        // LOG(2) << PRINT_MESSAGE(msg);
        switch (msg.getData().which()) {
            case RpcMessage::Data::GETPAGEARGS:
                handle_get_page_req(msg);
                break;
            case RpcMessage::Data::GETPAGESARGS:
                handle_get_pages_req(msg);
                break;
            // case RpcMessage::Data::SETPAGEARGS:
            //     handle_set_page_req(msg);
            //     break;
            case RpcMessage::Data::PREPAREARGS:
                handle_prepare_req(zmq_msg);
                break;
            case RpcMessage::Data::BATCHPREPAREARGS:
                handle_prepare_req_lbatch(zmq_msg);
                break;
            case RpcMessage::Data::COMMITARGS:
                handle_commit_req(msg);
                break;
            case RpcMessage::Data::PREPARERESPONSE:
                handle_prepare_reply(msg);
                break;
            case RpcMessage::Data::COMMITRESPONSE:
                handle_commit_reply(msg);
                break;
            default:
                ASSERT(false) << "Server " << msg.getReceiver() << ": Unknown RPC "
                              << msg.getData().which() << " recevied" << PRINT_MESSAGE(msg);
        }
    }
    void handle_task(const Event &event) {
        switch (event.task_type) {
            case UNBLOCK_TXN:
                prepare(reinterpret_cast<TxnInfo *>(event.value) /*txn_info ptr*/);
                break;
            case TRY_PREPARE_TXN:
                try_prepare(reinterpret_cast<TxnInfo *>(event.value) /*txn_info ptr*/);
                break;
            default:
                ASSERT(false);
        }
    }

   private:
    // rpc_handlers

    void handle_get_page_req(RpcMessage::Reader msg) {
#ifdef SN_ECHO
        auto args = msg.getData().getGetpageargs();
        BUILD_REPLY_NO_HASH(msg)
        auto resp = reply.initData().initGetpageresponse();
        resp.setTxnId(args.getTxnId());
        send(msgBuilder);
#else
        auto args = msg.getData().getGetpageargs();
        g_page_id_t g_page_id = args.getGlobalPageId();

        // LOG(2) << PRINT_MESSAGE(msg);

        BUILD_REPLY_NO_HASH(msg)

        PageMeta *page_meta = segments.get_page_meta(g_page_id);

        auto resp = reply.initData().initGetpageresponse();
        auto resp_page = resp.initPage();
        resp_page.setGlobalPageId(g_page_id);
        resp_page.setCts(page_meta->get_cts());
        resp_page.setData(kj::arrayPtr(page_meta->get_data(), page_meta->cur_page_size));

        resp.setOk(true);
        resp.setTxnId(args.getTxnId());

        send(msgBuilder);
#endif
    }

    void handle_get_pages_req(RpcMessage::Reader msg) {
#ifdef SN_ECHO
        ASSERT(false);
#else
        auto args = msg.getData().getGetpagesargs();

        std::unordered_map<g_page_id_t, TxRedoLog*> redo_logs;
        auto pageReqs = args.getPageReqs();
        for(auto pageReq : pageReqs) {
            g_page_id_t g_page_id = pageReq.getGlobalPageId();
            PageMeta *page_meta = segments.get_page_meta(g_page_id);
            TxRedoLog* redo_log = redoLogArena.alloc(page_meta->get_value_size());
            uint8_t* data = page_meta->get_data();
            auto offsets = pageReq.getOffsets();
            for (offset_t offset : offsets) {
                redo_log->add_write_entry(offset, r_get_key_ptr(data + offset), 0);
            }
            redo_log->freeze();
            redo_logs[g_page_id] = redo_log;
        }

        BUILD_REPLY_NO_HASH(msg)
        auto resp = reply.initData().initGetpagesresponse();
        auto pages = resp.initPages(pageReqs.size());
        resp.setTxnId(args.getTxnId());

        uint32_t i = 0;
        for (auto& pair : redo_logs) {
            TxRedoLog* redo_log = pair.second;
            pages[i].setGlobalPageId(pair.first);
            pages[i].setRedoLog(kj::arrayPtr(redo_log->get_log(), redo_log->get_log_size()));
            ++i;
        }

        send(msgBuilder);
#endif
    }

    void handle_prepare_req_lbatch(MsgBuffer &zmq_msg) {
        BUILD_MESSAGE_FROM_BUFFER(zmq_msg, msg);
        auto req = msg.getData().getBatchprepareargs();
        auto args = req.getPrepareargs();
        for (uint i = 0; i < args.size(); i++) {
#ifdef SN_ECHO
            BUILD_REPLY_NO_HASH(msg);
            auto req = args[i];
            if (req.getTxns().size() != 0) {
                auto resp = reply.initData().initCommitresponse();
                resp.setTxnId(req.getTxnId());
                resp.setPrimarySnId(req.getPrimarySnId());
                resp.setOk(true);
                send(msgBuilder);
                continue;
            } else {
                auto resp = reply.initData().initPrepareresponse();
                resp.setTxnId(req.getTxnId());
                resp.setPrimarySnId(req.getPrimarySnId());
                resp.setOk(true);
                send(msgBuilder);
                continue;
            }
#endif
#if defined(NO_TS) && !defined(DETERMINISTIC_VALIDATION)
            auto req = args[i];
            if (conf.get_my_id() != req.getPrimarySnId() * conf.REPLICA_COUNT){
                continue;
            }
#endif
            TxnInfo *txn_info = new TxnInfo(args[i], msg.getSender(), conf);
            txn_info->rdtsc_timer.start();
            txn_info->total_timer.start();
#if defined(NO_TS) && !defined(DETERMINISTIC_VALIDATION)
            ongoing_txns[txn_info->r_no]->put(txn_info->txn_id, txn_info);
#endif
            get_one_event_channel()->push_prior_task_event(TRY_PREPARE_TXN, txn_info);
            // prepare(txn_info);
        }
    }

    void try_prepare(TxnInfo* txn_info) {
#if !defined(NO_TS) || defined(DETERMINISTIC_VALIDATION)
        bool ready = ready_to_prepare(txn_info);
        if (!ready) {
            return;
        }
#endif
        prepare(txn_info);
    }

    void handle_prepare_req(MsgBuffer &zmq_msg) {
#ifdef SN_ECHO
        BUILD_MESSAGE_FROM_BUFFER(zmq_msg, msg);
        auto txn_req = msg.getData().getPrepareargs();
        txn_id_t txn_id = txn_req.getTxnId();
        BUILD_REPLY_NO_HASH(msg);
        if (txn_req.getTxns().size() != 0) {
            auto resp = reply.initData().initCommitresponse();
            resp.setTxnId(txn_id);
            resp.setOk(true);
            resp.setPrimarySnId(txn_req.getPrimarySnId());
            send(msgBuilder);
            return;
        } else {
            auto resp = reply.initData().initPrepareresponse();
            resp.setTxnId(txn_id);
            resp.setPrimarySnId(txn_req.getPrimarySnId());
            resp.setOk(true);
            send(msgBuilder);
            return;
        }
#endif

        TxnInfo *txn_info = new TxnInfo(std::move(zmq_msg), conf);
        txn_info->rdtsc_timer.start();
        txn_info->total_timer.start();
#if defined(NO_TS) && !defined(DETERMINISTIC_VALIDATION)
        ongoing_txns[txn_info->r_no]->put(txn_info->txn_id, txn_info);
#else
        bool ready = ready_to_prepare(txn_info);
        if (!ready) {
            return;
        }
#endif
        // LOG(2) << "prepare: " << std::hex << txn_info->txn_id << " " << txn_info->msg.getSender();
        prepare(txn_info);
    }

    void handle_commit_req(RpcMessage::Reader msg) {
        auto args = msg.getData().getCommitargs();
#ifdef SN_ECHO
        BUILD_REPLY_NO_HASH(msg);
        auto resp = reply.initData().initCommitresponse();
        resp.setOk(true);
        resp.setTxnId(args.getTxnId());
        resp.setPrimarySnId(args.getPrimarySnId());
        send(msgBuilder);
#else
        int r_no = TxnInfo::replica_no_of(conf.get_physical_sn_id(args.getPrimarySnId()), 
            conf.get_my_sn_id(), conf.numSN());
        commit(args.getTxnId(), r_no, msg);
#endif
    }

    void handle_prepare_reply(RpcMessage::Reader msg) {
        auto args = msg.getData().getPrepareresponse();
        TxnInfo *txn_info = ongoing_txns[0]->get(args.getTxnId());

        int remaining = txn_info->msg_count.fetch_sub(1);
        // LOG(2) << "handle_prepare_reply: " << std::hex << txn_info->txn_id 
        //     << " " << remaining << " " << msg.getSender();
        if (remaining == 1) {
            send_prepare_resp(txn_info, true);
        }
    }

    void handle_commit_reply(RpcMessage::Reader msg) {
        auto args = msg.getData().getCommitresponse();
        TxnInfo *txn_info = ongoing_txns[0]->get(args.getTxnId());

        int remaining = txn_info->msg_count.fetch_sub(1);
        if (remaining == 1) {
            gc_txn_info(txn_info);
        }
    }

    void send_correctness_check() {
        // apply_all_logs();
        BUILD_REQUEST(conf.get_primary_db_node()->node_id);
        auto sync = request.initData().initSyncargs();
        sync.setStatus(ServerStatus::END);

        uint64_t correctness_result = 0;
#ifndef EVAL_MODE
        correctness_result = benchmark->check_correctness_map(segments);
#endif
        LOG(4) << "correctness_result: " << (int64_t)correctness_result;

        sync.setCount(correctness_result);
        send(msgBuilder);
    }

   private:

    bool ready_to_prepare(TxnInfo *txn_info) {
#if defined(NO_TS) && !defined(DETERMINISTIC_VALIDATION)
        if (txn_info->r_no != 0) {
            ongoing_txns[txn_info->r_no]->put(txn_info->txn_id, txn_info);
            return true;
        }
#endif
        auto txn_req = txn_info->msg.getData().getPrepareargs();
        txn_id_t txn_id = txn_req.getTxnId();

        TxnInfo *old_txn_info = nullptr;
        bool get_ok = ongoing_txns[txn_info->r_no]->try_get_then_put(txn_id, old_txn_info, txn_info);
        if (!get_ok) {
            // the first prepare request for certain transaction
            bool success = true;
            txn_info->timer.start();
            txn_info->status = TxnInfo::COUNT_BLOCKED_SEGS;
#if defined(TOTAL_ORDER_TS) || defined(DETERMINISTIC_VALIDATION)
            {
#ifdef TOTAL_ORDER_TS
                uint32_t seq_i = 0;
#else
                uint32_t seq_i = conf.get_db_id(txn_info->req_db);
#endif
                auto* seq_manager = seq_managers[seq_i];
                success = seq_manager->check_for_noblock(txn_info);
            }            
#else
            {
                int num_block_segs = 0;
                auto req_segs = txn_req.getSegments();
                // LOG(2) << "["<< latency_worker_id <<"] " << req_segs.size();
                // exclusively and atomically set txn.num_blocked_segs
                for (const auto &req_seg : req_segs) {
                    seg_id_t seg_id = req_seg.getSegId();
                    // LOG(2) << "---" << VAR(seg_id);
                    auto &segment = segments.get_segment(seg_id);
                    bool is_read = req_seg.getWritePages().size() == 0;

                    bool block = segment.check_if_txn_should_block(req_seg, txn_info,
                                                                   is_read, txn_info->req_db);
                    num_block_segs += block ? 1 : 0;
                }
                if (num_block_segs > 0) {
                    int remaining = txn_info->num_blocked_segs.fetch_add(num_block_segs);
                    // LOG(2) << std::hex << txn_info->txn_id << " " << remaining << " " << num_block_segs;
                    if (remaining == - num_block_segs) {
                        success = true;
                    } else {
                        success = false;
                    }
                }
                // txn_info->num_blocked_segs = num_block_segs;  // this is an atmoic op...
                // mem_fence?
            }
#endif
            COMPILER_MEMORY_FENCE();
            txn_info->status = TxnInfo::NORMAL;
            txn_info->rdtsc_timer.end(LatencyType::SN_CHECK_BLOCK);
#ifdef VALIDATION_NON_BLOCKING
            return true;
#else
            return success;
#endif
        } else {
            // This is the repare-prepare message.
            // LOG(2) << "alreay got?: " << txn_id;
#ifdef FINE_VALIDATION
            txn_info->lock_acquired = true;
#endif            
            txn_info->old_txn_info = old_txn_info;
            txn_info->repaired = true;

            return true;
        }
    }

    void prepare(TxnInfo *txn_info) {
#if defined(NO_TS) && !defined(DETERMINISTIC_VALIDATION)
        if(txn_info->r_no != 0) {
            prepare_backup(txn_info);
            return;
        }
#endif

        prepares++;
        auto txn_req = txn_info->msg.getData().getPrepareargs();
        // LOG(2) << "prepare: " << std::hex << txn_info->txn_id << " " << txn_info;
#ifdef FINE_DIFF
        std::unordered_map<PageMeta *, TxRedoLog*> diff_pages;
#else
        std::unordered_set<PageMeta *> diff_pages;
#endif
        auto req_segs = txn_req.getSegments();

        bool ok = true;
#if defined(NO_TS) && !defined(DETERMINISTIC_VALIDATION)
        std::vector<seg_id_t> should_lock;
        uint locked_seg_cnt = 0;
        bool lock_success = true;
        for (const auto req_seg : req_segs) {
            if (occ_lock_manager.aurora_lock(txn_info, req_seg)) {
                ++locked_seg_cnt;
            } else {
                lock_success = false;
                break;
            }
        }
        if (!lock_success) {
            // std::string segs;
            for (const auto &req_seg : req_segs) {
                if (locked_seg_cnt-- > 0) {
                    occ_lock_manager.aurora_unlock(txn_info, req_seg);
                    // segs += std::to_string(req_seg.getSegId()) + ", ";
                } else {
                    break;
                }
            }
            // LOG(3) << "txn_id: " << std::hex << txn_info->txn_id << " unlock_after_lock_abort: " + segs; 
            ok = false;
            send_prepare_resp(txn_info, ok);

            // get_one_event_channel()->push_prior_task_event_back(UNBLOCK_TXN, txn_info);
            return;
        }
#endif

#ifdef FINE_VALIDATION
        if (!txn_info->lock_acquired) {
            std::vector<TxnInfo *> txn_infos_to_unblock;
            // LatencyLogger::start(LatencyType::SN_TEST1);
            bool success = lock_manager.lock(txn_info, req_segs, txn_infos_to_unblock);
#if defined(TOTAL_ORDER_TS) || defined(DETERMINISTIC_VALIDATION)
#ifdef TOTAL_ORDER_TS
            uint32_t seq_i = 0;
#else
            uint32_t seq_i = conf.get_db_id(txn_info->req_db);
#endif
            auto* seq_manager = seq_managers[seq_i];
            TxnInfo* to_unblock = seq_manager->unblock(txn_info);
            if (to_unblock != nullptr) {
                push_unblock_txn(to_unblock);
            }
#else
            for (auto* txn_info : txn_infos_to_unblock) {
                push_unblock_txn(txn_info);
            }
#endif
            if (!success) {
                return;
            }
        }
        // ASSERT(txn_info->num_blocked_locks == 0) << txn_info->num_blocked_locks;
#endif

        if (!txn_info->repaired) {
            txn_info->timer.end(LatencyType::SN_BLOCKED);
            txn_info->rdtsc_timer.start();
        }

        // LOG(2) << "after: " << std::hex << txn_info->txn_id << " " << txn_info;


        if (txn_info->r_no == 0 || txn_info->fastCommitEnabled) {
            // at most 1 round of repair
            if (!txn_info->repaired) {
                LatencyLogger::start(LatencyType::SN_VALID);
                for (const auto &req_seg : req_segs) {
                    seg_id_t seg_id = req_seg.getSegId();

                    auto r_pages = req_seg.getReadPages();
                    auto w_pages = req_seg.getWritePages();

                    auto &segment = segments.get_segment(seg_id);

                    get_diff_pages(segment, r_pages, diff_pages);
                    get_diff_pages(segment, w_pages, diff_pages);
                }
                LatencyLogger::end(LatencyType::SN_VALID);
            }
        }

#ifndef ABORT_ON_FAIL
        if (txn_info->fastCommitEnabled) {
            // do sn repair
            if (diff_pages.size() > 0) {
                // need to repair
                repairs++;
                txn_info->repaired = true;
                uint i = 0;
                std::unordered_map<txn_id_t, bool> prev_txn_in_batch_is_repaired;
                batch_kv_t batch_kv = gather_write_set(txn_req);
                // LOG(2) << "Repair Batch: " << std::hex << txn_info->txn_id;
#ifdef BREAKDOWN_TRAVERSING
                uint64_t total_traversing_nsec = 0;
#endif
                for (auto txn : txn_req.getTxns()) {
                    static PageManager page_manager(conf);  // TODO: remove this
                    uint32_t txn_i = txn.getTxnI();
                    auto txn_input = txn_req.getTxnInputs()[txn_i];
                    const void *input = txn_input.getInput().begin();
                    size_t len = txn_input.getInput().asBytes().size();
                    char *input_copy = new char[len];
                    memcpy(input_copy, input, len);

                    DbTxn *sn_txn =
                        new DbTxn(page_manager, benchmark->get_txn_logic(txn_input.getTxnType()),
                                  input_copy, benchmark->get_partition_logic());
                    sn_txn->txn_seed = txn_input.getTxnType();
                    sn_txn->fastCommitEnabled = true;
                    sn_txn->sn_read_set = txn_req.getTxns()[i].getReadSet();
                    sn_txn->prev_txn_in_batch_is_repaired = &prev_txn_in_batch_is_repaired;
                    sn_txn->batch_kv = &batch_kv;
                    sn_txn->txn_id = txn_input.getTxnId();
                    sn_txn->primary_sn_id = txn_info->primary_sn_id;
                    txn_info->ordered_txns.push_back(sn_txn);
                    bool repaired = sn_txn->repair_logic(sn_txn, 0);
                    if (repaired) {
                        txn_info->repaired_txns.push_back(i);
                    }
#ifdef BREAKDOWN_TRAVERSING
                    total_traversing_nsec += sn_txn->total_traversing_nsec;
#endif
                    prev_txn_in_batch_is_repaired[sn_txn->txn_id] = repaired;
                    // merge sn_txn.write_set
                    delete[] input_copy;
                    i++;
                }
#ifdef BREAKDOWN_TRAVERSING
                _RdtscTimer::end_ns(LatencyType::TRAVERSING, total_traversing_nsec);
#endif 
            }
            commit_after_sn_repair(txn_info);
            // txn_info->rdtsc_timer.end(LatencyType::SN_PREPARE);
            return;
        }
#endif
#if !defined(NO_TS) || defined(DETERMINISTIC_VALIDATION)
#ifdef NORMAL_PATH_OPT
        if (txn_info->repaired) {
            // LOG(2) << "repaired: " << std::hex << txn_info->txn_id;
            commit(txn_info);
        } else {
            send_prepare_resp(txn_info, diff_pages, ok);
        }
#else
        send_prepare_resp(txn_info, diff_pages, ok);
#endif
#else
        if (diff_pages.size() > 0) {
            send_prepare_resp(txn_info, diff_pages, ok);
        } else {
            if (!conf.sn_replication) {
                send_prepare_resp(txn_info, diff_pages, ok);
            } else {
                send_backup_prepare_req(txn_info);
            }
        }
#endif

    }

    void send_prepare_resp(TxnInfo* txn_info, bool ok) {
        // LOG(2) << "send_prepare_resp: " << std::hex << txn_info->txn_id 
        //     << " " << (ok? "True" : "False");
        BUILD_REPLY_NO_HASH(txn_info->msg);
        auto resp = reply.initData().initPrepareresponse();
        resp.setTxnId(txn_info->txn_id);
        resp.setPrimarySnId(txn_info->primary_sn_id);
        resp.setOk(ok);
        send(msgBuilder);
    }

    void send_backup_prepare_req(TxnInfo* txn_info) {
        if (!conf.sn_replication) return;
        txn_info->msg_count = conf.REPLICA_COUNT - 1;
        txn_info->prepare_replicated = true;
        node_id_t my_id = conf.get_my_sn_id();

        // LOG(2) << "send_backup_prepare_req: " << std::hex << txn_info->txn_id;
        if (txn_info->copied == false) {
            RpcMessage::Builder request = txn_info->msgBuilder.initRoot<RpcMessage>();
            request.setSender(my_id);
            auto args = txn_info->msg.getData().getPrepareargs();
            PrepareArgs::Builder prepareArgs = request.initData().initPrepareargs();
            prepareArgs.setTxnId(txn_info->txn_id);
            prepareArgs.setSegments(args.getSegments());
            prepareArgs.setTxns(args.getTxns());
            prepareArgs.setPrimarySnId(txn_info->primary_sn_id);
            prepareArgs.setSeq(txn_info->seq);
            for (uint32_t i = 1; i < conf.REPLICA_COUNT; ++i) {
                request.setReceiver((my_id + i) % conf.numSN());
                send(txn_info->msgBuilder);
            }
        } else {
            RpcMessage::Builder request = txn_info->msgBuilder.getRoot<RpcMessage>();
            for (uint32_t i = 1; i < conf.REPLICA_COUNT; ++i) {
                request.setReceiver((my_id + i) % conf.numSN());
                send(txn_info->msgBuilder);
            }
        }
        // send(txn_info->msgBuilder);
    }

    void send_backup_commit_req(TxnInfo* txn_info) {
        if (!conf.sn_replication || !txn_info->prepare_replicated) {
            gc_txn_info(txn_info);
            return;
        }
        txn_info->msg_count = conf.REPLICA_COUNT - 1;
        node_id_t my_id = conf.get_my_sn_id();
        BUILD_REQUEST(0);
        auto req = request.initData().initCommitargs();
        req.setTxnId(txn_info->txn_id);
        req.setPrimarySnId(txn_info->primary_sn_id);
        req.setCommit(!txn_info->aborted);
        for (uint32_t i = 1; i < conf.REPLICA_COUNT; ++i) {
            request.setReceiver((my_id + i) % conf.numSN());
            send(msgBuilder);
        }
    }

    void prepare_backup(TxnInfo *txn_info) {
        // LOG(2) << "prepare_backup: " << std::hex << txn_info->txn_id;
        BUILD_REPLY_NO_HASH(txn_info->msg);
        auto resp = reply.initData().initPrepareresponse();
        resp.setTxnId(txn_info->txn_id);
        resp.setPrimarySnId(txn_info->primary_sn_id);
        resp.setOk(true);
        reply.setReceiver(conf.get_physical_sn_id(txn_info->primary_sn_id));
        send(msgBuilder);
    }

    batch_kv_t gather_write_set(PrepareArgs::Reader txn_req) {
        batch_kv_t batch_kv;
        auto req_segs = txn_req.getSegments();
        for (const auto& req_seg : req_segs) {
            if (!req_seg.hasWritePages()) {
                continue;
            }
            seg_id_t seg_id = req_seg.getSegId();
            Segment& seg = segments.get_segment(seg_id);
            auto w_pages = req_seg.getWritePages();
            for (const auto &page : w_pages) {
                if (!page.hasRedoLog()) {
                    continue;
                }
                auto bytes = page.getRedoLog().asBytes();
                RedoLogReader log_reader(bytes.begin(), bytes.size(), seg.value_size);
                while (log_reader.valid()) {
                    bool is_write = log_reader.is_write_entry();

                    if (is_write) {
                        txn_kv_t& txn_kv = batch_kv[{seg_id, log_reader.get_key_ref_as<Key_t>()}];
                        txn_kv[log_reader.get_version()] = log_reader.get_value_ptr();
                    }

                    log_reader.next();
                }
            }
        }
        return batch_kv;
    }


    void add_redo_log(redo_logs_t &redo_logs,
                      const capnp::List<Seg, capnp::Kind::STRUCT>::Reader &req_segs) {
        for (const auto &req_seg : req_segs) {
            auto write_pages = req_seg.getWritePages();
            auto &segment = segments.get_segment(req_seg.getSegId());
            for (const auto &page : write_pages) {
                if (page.hasRedoLog()) {
                    auto log_data = page.getRedoLog().asBytes();
                    g_page_id_t g_page_id = page.getGlobalPageId();
                    LogMeta& meta = redo_logs[g_page_id];
                    meta.log_size = log_data.size();
                    meta.log = new uint8_t[meta.log_size];
                    memcpy(meta.log, log_data.begin(), meta.log_size);
                    meta.cts = segment.get_cts();
                    meta.page_meta = segment.get_page_meta(GlobalPageId(g_page_id).page_id);
                }
            }
        }
    }

    void add_redo_log(redo_logs_t &redo_logs,
                      const capnp::List<Seg, capnp::Kind::STRUCT>::Reader &req_segs,
                      const capnp::List<Seg, capnp::Kind::STRUCT>::Reader &req_segs2) {
        std::unordered_map<g_page_id_t, kj::ArrayPtr<const kj::byte>> pages2;
        for (const auto &req_seg : req_segs2) {
#ifdef NORMAL_PATH_OPT
            if (!conf.isLocalSnSegment(req_seg.getSegId())) {
                continue;
            }
#endif
            auto write_pages = req_seg.getWritePages();
            for (const auto &page : write_pages) {
                if (page.hasRedoLog()) {
                    pages2[page.getGlobalPageId()] = page.getRedoLog().asBytes();
                }
            }
        }
        for (const auto &req_seg : req_segs) {
            auto write_pages = req_seg.getWritePages();
            auto &segment = segments.get_segment(req_seg.getSegId());
            for (const auto &page : write_pages) {
                g_page_id_t g_page_id = page.getGlobalPageId();
                LogMeta& meta = redo_logs[g_page_id];
                // check whether req_segs2 has this page
                auto bytes1 = page.getRedoLog().asBytes();
                auto iter2 = pages2.find(g_page_id);

                if (iter2 != pages2.end()) {
                    auto bytes2 = iter2->second;
                    meta.log_size = bytes1.size() + bytes2.size();
                    meta.log = new uint8_t[meta.log_size];
                    memcpy(meta.log, bytes1.begin(), bytes1.size());
                    append_redo_logs(meta.log, bytes2.begin(), bytes1.size(), bytes2.size());
                } else {
                    meta.log_size = bytes1.size();
                    meta.log = new uint8_t[meta.log_size];
                    memcpy(meta.log, bytes1.begin(), meta.log_size);
                }
                meta.cts = segment.get_cts();
                meta.page_meta = segment.get_page_meta(GlobalPageId(g_page_id).page_id);
            }
        }
    }

    bool commit_after_sn_repair(TxnInfo *txn_info) {
        LatencyLogger::start(LatencyType::TMP);
        ASSERT(txn_info->fastCommitEnabled);
        commits++;
        count++;

        auto txn_req = txn_info->msg.getData().getPrepareargs();

        auto req_segs = txn_req.getSegments();
        // TODO: unblock by txn_info only, don't use seg_id
        std::vector<TxnInfo *> txn_infos_to_unblock;

        // commit resp
        BUILD_REPLY_NO_HASH(txn_info->msg);
        auto resp = reply.initData().initCommitresponse();
        resp.setOk(!txn_info->aborted);
        resp.setIsBroadCast(false);
        resp.setTxnId(txn_info->txn_id);
        resp.setPrimarySnId(txn_info->primary_sn_id);
        auto ids = resp.initRepairedTxns(txn_info->repaired_txns.size());
        for (uint i = 0; i < txn_info->repaired_txns.size(); i++) {
            ids.set(i, txn_info->repaired_txns[i]);
        }

        // First apply the original redo-logs
        for (const auto &req_seg : req_segs) {
            seg_id_t seg_id = req_seg.getSegId();
            auto &segment = segments.get_segment(seg_id);
            ASSERT(!txn_info->aborted);

            auto write_pages = req_seg.getWritePages();
            if (write_pages.size() > 0) {
                ts_t cts = req_seg.getDep().getTimestamp();
                for (const auto &page : write_pages) {
                    if (!page.hasRedoLog()) {
                        continue;
                    }

                    g_page_id_t g_page_id = page.getGlobalPageId();
                    auto log_data = page.getRedoLog().asBytes();
                    uint32_t log_size = log_data.size();
                    auto log = reinterpret_cast<log_t>(const_cast<uint8_t *>(log_data.begin()));

                    // LOG(2) << "commit_redo_log[" << latency_worker_id << "] " 
                    //     << std::hex << g_page_id << " " << txn_info->txn_id << " " << (void*)log
                    //     << " " << segment.get_cts() << " " << cts;
                    commit_redo_log(segment, g_page_id, log, log_size, cts);
                }
            }
        }
        if (txn_info->repaired) {
            // set the new redo-logs and apply the repaired values
            std::unordered_map<g_page_id_t, std::pair<TxRedoLog *, ts_t>> redo_logs;
            for (auto *txn_p : txn_info->ordered_txns) {
                auto *txn = (DbTxn *)txn_p;
                txn->sn_local_commit();
                auto &w_set = txn->write_set;
                for (auto &record : w_set) {
                    seg_id_t seg_id = GlobalPageId(record.g_page_id).seg_id;
                    auto &segment = segments.get_segment(seg_id);

                    ts_t cts = segment.get_cts();
                    auto& pair = redo_logs[record.g_page_id];
                    TxRedoLog *&redo_log = pair.first;
                    if (redo_log == nullptr) {
                        redo_log = redoLogArena.alloc(record.value_size);
                        pair.second = cts;
                    }
                    redo_log->add_write_entry(record.record_page_offset, record.get_buf(),
                                        record.get_writer());
                }
            }
            // we only send back the repaired records
            auto resp_diff = resp.initDiff(redo_logs.size());
            uint i = 0;
            for (auto &iter : redo_logs) {
                TxRedoLog *redo_log = iter.second.first;
                redo_log->freeze();
                auto diff = resp_diff[i];
                diff.setGlobalPageId(iter.first);
                diff.setCts(iter.second.second);
                auto resp_logs = diff.initRedoLogs(1);
                resp_logs.set(0, kj::arrayPtr(redo_log->get_log(), redo_log->get_log_size()));

                redoLogArena.free(redo_log);
                i++;
            }
        }
#ifdef FINE_VALIDATION
        auto unlock_txn_func = [this](TxnInfo* t){push_unlocked_txn(t);};
        lock_manager.unlock(txn_info, req_segs, unlock_txn_func);
        for (auto* info : txn_infos_to_unblock) {
            push_unlocked_txn(info);
        }
#else
#if defined(TOTAL_ORDER_TS) || defined(DETERMINISTIC_VALIDATION)
        static_assert(false);
#endif
        for (const auto &req_seg : req_segs) {
            seg_id_t seg_id = req_seg.getSegId();
            auto &segment = segments.get_segment(seg_id);
            auto write_pages = req_seg.getWritePages();
            bool is_read = write_pages.size() == 0;
            segment.update_segment_dep(is_read, txn_infos_to_unblock, txn_info->req_db);
        }
        for (auto* txn_info : txn_infos_to_unblock) {
            push_unblock_txn(txn_info);
        }
#endif
        LatencyLogger::end(LatencyType::TMP);

#ifdef SN_BROADCAST
        // if (conf.sn_replication && (conf.get_my_id() % 3 == txn_info->txn_id % 3)) 
        if (txn_info->r_no == 0 && thread_rand.randint(1, 10) == 1) {
            std::unordered_map<g_page_id_t, std::unordered_map<Key_t, SimpleRecord>> bc_wset;
            std::unordered_map<g_page_id_t, uint32_t> page_value_sizes;
            for (const auto &req_seg : req_segs) {
                auto write_pages = req_seg.getWritePages();
                for (const auto &page : write_pages) {
                    if(!page.hasRedoLog()) {
                        ASSERT(false);
                    }
                    g_page_id_t g_page_id = page.getGlobalPageId();
                    PageMeta* page_meta = segments.get_page_meta(g_page_id);
                    if (!page_meta->need_async_update) {
                        continue;
                    }
                    uint32_t value_size = page_meta->get_value_size();
                    RedoLogReader reader(page.getRedoLog().asBytes(), value_size);
                    page_value_sizes[g_page_id] = value_size;

                    std::unordered_map<Key_t, SimpleRecord> &wset = bc_wset[g_page_id];
                    while (reader.valid()) {
                        if (!reader.is_write_entry()) {
                            // this is a read op
                            reader.next();
                            continue;
                        }
                        SimpleRecord &p = wset[*reader.get_key_ptr_as<Key_t>()];
                        p.v = reader.get_value_ptr();
                        p.writer = reader.get_version();
                        p.page_offset = reader.get_real_page_offset();
                        reader.next();
                    }
                }
            }
            if (txn_info->repaired) {
                for (auto *txn_p : txn_info->ordered_txns) {
                    auto *txn = (DbTxn *)txn_p;
                    auto &w_set = txn->write_set;
                    for (auto &record : w_set) {
                        // overwrite the data in broadcast message
                        auto iter = bc_wset.find(record.g_page_id);
                        if (iter == bc_wset.end()) {
                            continue;
                        }
                        SimpleRecord &p = iter->second[*(Key_t *)record.get_key_buf()];
                        p.v = record.value_buf();
                        p.writer = record.get_writer();
                        p.page_offset = record.record_page_offset;
                    }
                }
            }
            std::unordered_set<TxRedoLog *> bc_redo_logs;
            redo_logs_t bc_data;
            for (auto item : bc_wset) {
                uint32_t value_size = page_value_sizes[item.first];
                TxRedoLog *redo_log = redoLogArena.alloc(value_size);
                bc_redo_logs.insert(redo_log);
                for (auto &r : item.second) {
                    Key_t key = r.first;
                    SimpleRecord &sr = r.second;
                    redo_log->add_write_entry(sr.page_offset, &key, sr.v, sr.writer);
                }
                redo_log->freeze();

                auto& segment = segments.get_segment(GlobalPageId(item.first).seg_id);
                LogMeta& meta = bc_data[item.first];
                meta.log = redo_log->get_log();
                meta.cts = segment.get_cts();
                meta.log_size = redo_log->get_log_size();
            }

            broadcast_commit(bc_data, txn_info);

            for (auto log : bc_redo_logs) {
                redoLogArena.free(log);
            }
        }
#endif


        // LOG(2) << "commit: " << std::hex << txn_info->txn_id;
        send(msgBuilder);

        for (auto *txn_p : txn_info->ordered_txns) {
            auto *txn = (DbTxn *)txn_p;
            delete txn;
        }
        if (txn_info->old_txn_info) {
            txn_info->old_txn_info->rdtsc_timer.end(LatencyType::SN_COMMIT);
        } else {
            txn_info->rdtsc_timer.end(LatencyType::SN_COMMIT);
        }
        txn_info->total_timer.end(LatencyType::SN_PREPARE_COMMIT);
        gc_txn_info(txn_info);
        return true;
    }

    inline void commit(txn_id_t txn_id, int r_no, RpcMessage::Reader msg) {
        auto commit_req = msg.getData().getCommitargs();
        TxnInfo *txn_info = ongoing_txns[r_no]->get(txn_id);
        ASSERT(txn_info != nullptr) << std::hex << txn_id << " " << msg.getSender() << " " << msg.getReceiver();
        // if (!txn_info) {
        //     return true;
        // }
        txn_info->aborted = !commit_req.getCommit();
        commit(txn_info);
    }

    void commit(TxnInfo *txn_info) {
        // LOG(2) << "commit for " << std::hex << txn_info->txn_id << " " << txn_info;
#if defined(NO_TS) && !defined(DETERMINISTIC_VALIDATION)
        if(txn_info->r_no != 0) {
            commit_backup(txn_info);
            return;
        }
#endif
        if (txn_info->repaired) {
            LatencyLogger::start(LatencyType::SN_COMMIT_PHASE);
        }

        commits++;
        count++;

        auto txn_req = txn_info->msg.getData().getPrepareargs();

        auto req_segs = txn_req.getSegments();
        std::vector<TxnInfo *> txn_infos_to_unblock;
        redo_logs_t redo_logs;

        if (txn_info->repaired) {
            add_redo_log(redo_logs,
                         txn_info->old_txn_info->msg.getData().getPrepareargs().getSegments(),
                         req_segs);
            req_segs = txn_info->old_txn_info->msg.getData().getPrepareargs().getSegments();
        } else {
            add_redo_log(redo_logs, req_segs);
        }
#ifdef SN_BROADCAST
#ifndef EMULATE_AURORA
        redo_logs_t redo_logs_copy;
        // if (txn_info->r_no == 0) {
        if (txn_info->r_no == (txn_info->txn_id % conf.REPLICA_COUNT)) {
            // TODO: only send write logs
            for (auto iter : redo_logs) {
                PageMeta* page_meta = segments.get_page_meta(iter.first);
                if (!page_meta->need_async_update) {
                    continue;
                }
                LogMeta& meta = iter.second;
                
                LogMeta& meta2 = redo_logs_copy[iter.first];
                meta2.log = meta.log;
                meta2.cts = meta.cts;
                meta2.log_size = meta.log_size;
            }
        }
#endif
#endif
        if (!txn_info->aborted) {
            for (auto iter : redo_logs) {
                LogMeta& meta = iter.second;
                commit_redo_log(meta.page_meta, meta.log, meta.log_size, meta.cts);
            }
        }

#ifdef FINE_VALIDATION
        auto unlock_txn_func = [this](TxnInfo* t){push_unlocked_txn(t);};
        if (txn_info->repaired) {
            LatencyLogger::start(LatencyType::SN_COMMIT_PHASE2);
            lock_manager.unlock(txn_info->old_txn_info, req_segs, unlock_txn_func);
            LatencyLogger::end(LatencyType::SN_COMMIT_PHASE2);
            LatencyLogger::end(LatencyType::SN_COMMIT_PHASE);
        } else {
            lock_manager.unlock(txn_info, req_segs, unlock_txn_func);
        }
        // for (auto* info : txn_infos_to_unblock) {
        //     push_unlocked_txn(info);
        // }
#else
        for (const auto &req_seg : req_segs) {
#ifdef NO_TS
            occ_lock_manager.aurora_unlock(txn_info, req_seg);
#else
            seg_id_t seg_id = req_seg.getSegId();
            auto &segment = segments.get_segment(seg_id);
            auto write_pages = req_seg.getWritePages();
            bool is_read = write_pages.size() == 0;
            segment.update_segment_dep(is_read, txn_infos_to_unblock, txn_info->req_db);
#endif
        }

#ifndef NO_TS
        for (auto* txn_info : txn_infos_to_unblock) {
            push_unblock_txn(txn_info);
        }
#endif
#endif
        // LOG(2) << " recv commit for " << std::hex << txn_info->txn_id;

        // commit resp
        BUILD_REPLY_NO_HASH(txn_info->msg);
        auto resp = reply.initData().initCommitresponse();
        resp.setOk(!txn_info->aborted);
        resp.setIsBroadCast(false);
        resp.setTxnId(txn_info->txn_id);
        resp.setPrimarySnId(txn_info->primary_sn_id);
        send(msgBuilder);
#ifdef SN_BROADCAST
#ifdef EMULATE_AURORA
        resp.setIsBroadCast(true);
        for (uint i = 0; i < conf.numDB(); i++) {
            node_id_t to_id = conf.get_db_node(i)->node_id;
            if (to_id != txn_info->req_db)
                send(msgBuilder);
        }

#else
        if (redo_logs_copy.size() != 0) {
            broadcast_commit(redo_logs_copy, txn_info);
            for (auto iter : redo_logs_copy) {
                delete[] iter.second.log;
            }
        }
#endif
#endif
        if (txn_info->old_txn_info) {
            // LOG(2) << "gc2: " << std::hex << txn_info->txn_id << " " << txn_info << " " << txn_info->old_txn_info;
            txn_info->old_txn_info->rdtsc_timer.end(LatencyType::SN_COMMIT);
        } else {
            // LOG(2) << "gc2: " << std::hex << txn_info->txn_id << " " << txn_info;
            txn_info->rdtsc_timer.end(LatencyType::SN_COMMIT);
        }
        txn_info->total_timer.end(LatencyType::SN_PREPARE_COMMIT);

#if defined(NO_TS) && !defined(DETERMINISTIC_VALIDATION)
        send_backup_commit_req(txn_info);
#else
        gc_txn_info(txn_info);
#endif
    }

    void commit_backup(TxnInfo* txn_info) {
        auto txn_req = txn_info->msg.getData().getPrepareargs();
        auto req_segs = txn_req.getSegments();
        redo_logs_t redo_logs;
        add_redo_log(redo_logs, req_segs);

        if (!txn_info->aborted) {
            for (const auto &req_seg : req_segs) {
                seg_id_t seg_id = req_seg.getSegId();
                auto &segment = segments.get_segment(seg_id);
                auto write_pages = req_seg.getWritePages();
                bool is_write = write_pages.size() != 0;
                if (is_write) {
                    ts_t cts = req_seg.getDep().getTimestamp();
                    for (const auto &page : write_pages) {
                        g_page_id_t gpageid = page.getGlobalPageId();
                        LogMeta& meta = redo_logs[gpageid];
                        commit_redo_log(segment, gpageid, meta.log, meta.log_size, cts);
                    }
                }
            }
        }

        BUILD_REPLY_NO_HASH(txn_info->msg);
        auto resp = reply.initData().initCommitresponse();
        resp.setOk(!txn_info->aborted);
        resp.setIsBroadCast(false);
        resp.setTxnId(txn_info->txn_id);
        resp.setPrimarySnId(txn_info->primary_sn_id);
        reply.setReceiver(conf.get_physical_sn_id(txn_info->primary_sn_id));
        send(msgBuilder);
        gc_txn_info(txn_info);
    }

    void gc_txn_info(TxnInfo* txn_info) {
        // LOG(2) << "gc: " << std::hex << txn_info->txn_id << " " << txn_info;
        ongoing_txns[txn_info->r_no]->erase(txn_info->txn_id);
        delete txn_info;
    }

    void broadcast_commit(redo_logs_t &redo_logs, TxnInfo *txn_info) {
        BUILD_REQUEST(0);
        auto resp = request.initData().initCommitresponse();
        resp.setOk(true);
        resp.setIsBroadCast(true);
        resp.setTxnId(txn_info->txn_id);
        auto resp_diff = resp.initDiff(redo_logs.size());
        uint i = 0;
        for (auto &iter : redo_logs) {
            LogMeta& meta = iter.second;
            auto diff = resp_diff[i];
            diff.setGlobalPageId(iter.first);
            diff.setCts(meta.cts);
            auto resp_logs = diff.initRedoLogs(1);
            resp_logs.set(0, kj::arrayPtr(meta.log, meta.log_size));
            i++;
        }
        for (uint i = 0; i < conf.numDB(); ++i) {
            node_id_t to_id = conf.get_db_node(i)->node_id;
            if (to_id == txn_info->req_db) {
                continue;
            }
            request.setReceiver(to_id);
            send(msgBuilder);
        }
    }

   private:
    inline void push_unblock_txn(TxnInfo *txn_info) {
#ifdef VALIDATION_NON_BLOCKING
        return;
#endif
        while (txn_info->status == TxnInfo::COUNT_BLOCKED_SEGS) {
            // wait for a moment if the txn_info->num_blocked_segs is being added
            // This fence is necessary, otherwise it will not get out of the loop. (tested)
            COMPILER_MEMORY_FENCE();
        }

        int remaining = txn_info->num_blocked_segs.fetch_sub(1);
        if (remaining == 1) {  // atomic op
            get_one_event_channel()->push_prior_task_event_back(UNBLOCK_TXN, txn_info);
        }
    }

    inline void push_unlocked_txn(TxnInfo *txn_info) {
        get_one_event_channel()->push_prior_task_event_back(UNBLOCK_TXN, txn_info);
    }


#ifdef FINE_DIFF
    uint get_diff_pages(Segment &segment, capnp::List<Page, capnp::Kind::STRUCT>::Reader pages,
                        std::unordered_map<PageMeta *, TxRedoLog*> &diff) {
        uint num_diff_pages = 0;
        for (const auto &page : pages) {
            if (!page.hasRedoLog()) {
                continue;
            }
            GlobalPageId gp_id(page.getGlobalPageId());
            PageMeta *page_meta = segment.get_page_meta(gp_id.page_id);
            // std::lock_guard<std::mutex> lock(page_meta->mutex);

            TxRedoLog* redo_log = has_record_difference(page, page_meta);
            if (redo_log != nullptr) {
#ifndef VALIDATION_ALWAYS_SUCCESS
                diff[page_meta] = redo_log;
                num_diff_pages++;
#endif
                page_meta->require_async_update();
            }
        }
        return num_diff_pages;
    }

    TxRedoLog* has_record_difference(Page::Reader page, PageMeta *page_meta) {
        TxRedoLog* redo_log = nullptr;
        auto bytes = page.getRedoLog().asBytes();
        RedoLogReader log_reader(bytes, page_meta->get_value_size());
        uint8_t* data = page_meta->get_data();
#ifndef UNIQUE_REDO_LOG
        std::unordered_map<offset_t, bool> checked_offsets;
#endif
        while (log_reader.valid()) {
            offset_t page_offset = log_reader.get_real_page_offset();
            bool is_write = log_reader.is_write_entry();
#ifndef UNIQUE_REDO_LOG
            bool &checked = checked_offsets[page_offset];
#endif

            if (is_write) {
                // this is a write

                // avoid the first write of each batch is blind write,
                // and make subseqeunt key validation fail...
#ifndef UNIQUE_REDO_LOG
                checked = true;
#endif
                log_reader.next();
                continue;
            }
#ifndef UNIQUE_REDO_LOG
            if (checked) {
                log_reader.next();
                continue;
            } else {
                checked = true;
            }
#endif
            version_ts_t correct_ts = r_get_writer(data + page_offset);
            version_ts_t read_ts = log_reader.get_version();
            bool version_diff = (correct_ts != read_ts);
            if (version_diff) {
                // LOG(2) << std::hex << page_meta->gp_id.seg_id << " " << page_meta->gp_id.page_id 
                //     << " " << page_offset << " " << correct_ts << " " << read_ts;
                if (redo_log == nullptr) {
                    redo_log = redoLogArena.alloc(page_meta->get_value_size());
                }
                redo_log->add_write_entry(page_offset, r_get_key_ptr(data+page_offset), correct_ts);
            }

            log_reader.next();
        }
        if (redo_log) {
            redo_log->freeze();
        }
        return redo_log;
    }

    void send_prepare_resp(TxnInfo* txn_info, 
            const std::unordered_map<PageMeta *, TxRedoLog*>& diff_pages,
            bool ok) {
        BUILD_REPLY_NO_HASH(txn_info->msg);
        auto resp = reply.initData().initPrepareresponse();
        resp.setTxnId(txn_info->txn_id);
        resp.setPrimarySnId(txn_info->primary_sn_id);

        if (diff_pages.size() > 0) {
            // need to repair
            auto resp_diff_pages = resp.initDiff(diff_pages.size());
            uint64_t i = 0;
            for (auto& pair : diff_pages) {
                set_resp_redo_log(resp_diff_pages[i], pair.first, pair.second);
                ++i;
            }
            ok = false;
            Logger::count(CountType::PREPARE_DIFF_MSG_SIZE, msgBuilder.sizeInWords() * 8);
            Logger::count(CountType::NUM_DIFF_PAGES, diff_pages.size());
        }
        resp.setOk(ok);
        send(msgBuilder);
#ifdef TOTAL_ORDER_TS
        auto txn_req = txn_info->msg.getData().getPrepareargs();
        if (txn_req.getSegments().size() == 0 && !txn_info->repaired) {
            // LOG(2) << "gc1: " << std::hex << txn_info->txn_id << " " << txn_info;
            gc_txn_info(txn_info);
        }
#endif
    }

    void set_resp_redo_log(MultiLogPage::Builder resp_diff_page, PageMeta *page_meta, TxRedoLog* redo_log) {
        resp_diff_page.setGlobalPageId(page_meta->gp_id.g_page_id);
        std::lock_guard<std::mutex> lock(page_meta->mutex);
        auto resp_logs = resp_diff_page.initRedoLogs(1);
        resp_logs.set(0, kj::arrayPtr(redo_log->get_log(), redo_log->get_log_size()));
        redoLogArena.free(redo_log);
    }

#else
    uint get_diff_pages(Segment &segment, capnp::List<Page, capnp::Kind::STRUCT>::Reader pages,
                        std::unordered_set<PageMeta *> &diff) {
        uint num_diff_pages = 0;
        for (const auto &page : pages) {
            GlobalPageId gp_id(page.getGlobalPageId());

            PageMeta *page_meta = segment.get_page_meta(gp_id.page_id);
            // std::lock_guard<std::mutex> lock(page_meta->mutex);

            if (has_record_difference(page, page_meta)) {
#ifndef VALIDATION_ALWAYS_SUCCESS
                diff.insert(page_meta);
                num_diff_pages++;
#endif
                page_meta->require_async_update();
            }
        }
        return num_diff_pages;
    }

    bool has_record_difference(Page::Reader page, PageMeta *page_meta) {
        if (!page.hasRedoLog()) {
            return false;
        }
        auto bytes = page.getRedoLog().asBytes();
        std::unordered_map<offset_t, bool> checked_offsets;
        RedoLogReader log_reader(bytes, page_meta->get_value_size());
        uint8_t* data = page_meta->get_data();
        while (log_reader.valid()) {
            offset_t page_offset = log_reader.get_real_page_offset();
            bool is_write = log_reader.is_write_entry();
            bool &checked = checked_offsets[page_offset];

            // LOG(2) << "Key: " << *log_reader.get_key_ptr_as<uint64_t>() 
            //         << " read: " << (log_reader.get_page_offset() >> 63);
            // LOG(2) << "Value: " << *log_reader.get_value_ptr_as<Value_t>()
            //         << " " << *(Value_t *)(data + page_offset + sizeof(Key_t));

            if (is_write) {
                // this is a write

                // avoid the first write of each batch is blind write,
                // and make subseqeunt key validation fail...
                checked = true;
                log_reader.next();
                continue;
            }

            if (checked) {
                log_reader.next();
                continue;
            } else {
                checked = true;
            }
            version_ts_t correct_ts = r_get_writer(data + page_offset);
            version_ts_t read_ts = log_reader.get_version();
            bool version_diff = (correct_ts != read_ts);
            if (version_diff) {
                // debugging
                // LOG(2) << "version_diff: " << std::hex << correct_ts 
                //         << ", " << read_ts << ", " << version_diff;
                // LOG(2) << "Page Offset: " << page_meta->gp_id.seg_id 
                //     << " " << page_meta->gp_id.page_id 
                //     << " " << page_offset;
                // LOG(2) << "Key: " << log_reader.get_key_ref_as<Key_t>();
                // LOG(2) << "Value: " << *log_reader.get_value_ptr_as<Val_t>()
                //        << " " << *(Val_t *)(data + page_offset + sizeof(Key_t));
                return true;
            }

            log_reader.next();
        }
        return false;
    }

    void send_prepare_resp(TxnInfo* txn_info, 
            const std::unordered_set<PageMeta *>& diff_pages,
            bool ok) {
        BUILD_REPLY_NO_HASH(txn_info->msg);
        auto resp = reply.initData().initPrepareresponse();
        resp.setTxnId(txn_info->txn_id);
        resp.setPrimarySnId(txn_info->primary_sn_id);

        if (diff_pages.size() > 0) {
            // need to repair
            auto resp_diff_pages = resp.initDiff(diff_pages.size());
            uint64_t i = 0;
            for (PageMeta *page_meta : diff_pages) {
                set_resp_redo_log(resp_diff_pages[i], page_meta);
                ++i;
            }
            ok = false;
            Logger::count(CountType::PREPARE_DIFF_MSG_SIZE, msgBuilder.sizeInWords() * 8);
            Logger::count(CountType::NUM_DIFF_PAGES, diff_pages.size());
        }
        resp.setOk(ok);
        send(msgBuilder);
    }

    void set_resp_redo_log(MultiLogPage::Builder resp_diff_page, PageMeta *page_meta) {
        resp_diff_page.setGlobalPageId(page_meta->gp_id.g_page_id);
        std::lock_guard<std::mutex> lock(page_meta->mutex);
#ifndef EMULATE_AURORA
        resp_diff_page.setCts(page_meta->get_cts());
#endif
        resp_diff_page.setData(kj::arrayPtr(page_meta->get_data(), page_meta->cur_page_size));
    }
#endif

    void commit_redo_log(Segment &segment, g_page_id_t g_page_id, log_t log, 
            uint32_t log_size, ts_t cts) {
        page_id_t page_id = GlobalPageId(g_page_id).page_id;
        auto page_meta = segment.get_page_meta(page_id);
        commit_redo_log(page_meta, log, log_size, cts);
    }

    void commit_redo_log(PageMeta* page_meta, log_t log, uint32_t log_size, ts_t cts) {
        uint32_t value_size = page_meta->get_value_size();
        RedoLogReader log_reader(log, log_size, value_size);
    
        std::lock_guard<std::mutex> lock(page_meta->mutex);
        page_meta->set_cts(cts);
        while (log_reader.valid()) {
            if (log_reader.is_write_entry()) {
                uint8_t* record_ptr = page_meta->get_data() + log_reader.get_real_page_offset();
                set_key(record_ptr, log_reader.get_key_ptr());
                set_value(record_ptr, log_reader.get_value_ptr(), value_size);
                set_writer(record_ptr, log_reader.get_version());
            }
            log_reader.next();
        }
    }
};