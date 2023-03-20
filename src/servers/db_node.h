#pragma once

#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <stack>
#include <unordered_map>
#include <unordered_set>

#include "benchmarks/interface.h"
#include "config.h"
#include "rpc/rpc.h"
#include "storage/multi_ver_record.h"
#include "storage/page_manager.h"
#include "txn/pipeline_scheduler.h"
#include "util/macros.h"
#include "util/statistic.h"
#include "util/thread_safe_structures.h"
#include "util/timer.h"
#include "util/txn_lat.h"


class DatabaseRpcServer;
enum DbTaskType : uint32_t {
    NEW_TXN,
    RETRY_TXN,
    EXECUTE_TXN,
    SEND_PREPARE,
    COMMIT_BATCH,
    AFTER_BATCH,
    GC_BATCH,
    EMPTY,
    STOP
};

enum HandleOwnTxnTaskType : uint32_t { COMMIT, GC };

class DatabaseWorker {
   private:
    const Configuration &conf;
    txn_id_t next_txn_id;

   public:
    DatabaseRpcServer *server;
    thread_id_t worker_id;
    EventChannel *my_event_channel;
    std::map<batch_id_t, std::vector<DbTxn *>> blocked_txns;
    std::thread worker_thread;
    Profile profile;
    uint64_t sentSize;
#ifdef LOCK_FAIL_BLOCK
    uint64_t latest_batch_num = 0;
#endif
    uint8_t* lmsg_buf;
    kj::ArrayPtr<capnp::word> lmsg_ptr;

    bool stopped = false;  // for debug
    bool im_running = true;
    
    DatabaseWorker(const Configuration &, thread_id_t, DatabaseRpcServer *);
    ~DatabaseWorker();
    DbTxn *launch_new_txn(DbClient *c);
    DbTxn *retry_txn(DbTxn *txn);
    void worker_mainloop();
    void handle_rpc(Event& event);
    void handle_task(Event& event);
};

thread_local DatabaseWorker *current_worker;

class DatabaseNode {
    friend class DatabaseWorker;

   private:
    const Configuration &conf;
    RpcServerBase &rpcServer;
    BenchmarkInterface &benchmark;
    PageManager page_manager;

    node_id_t ts_node_id;
    const batch_id_t starting_batch_id;
public:
    BatchThreadSafeMap<batch_id_t, BatchTxn *> ongoing_batches;
    std::atomic<batch_id_t> next_lbatch_id;
    BatchThreadSafeMap<batch_id_t, LargeBatch *> ongoing_lbatches;

#ifdef LOCK_FREE_BATCH_QUEUE
    BatchQueueLockfree<DbTxn *> to_commit_queue;  // store executed txns, waiting for batch
#else
    BatchQueue<DbTxn *> to_commit_queue;  // store executed txns, waiting for batch
#endif
    PipelineScheduler scheduler;

   public:
    uint32_t batching_size;
    uint32_t split_batch_num;
    uint32_t cache_miss_ratio;
#ifdef DETERMINISTIC_VALIDATION
    std::atomic<uint64_t> my_seq[MAX_SN_NUM];
#endif
#ifdef EMULATE_AURORA
    BatchCache *curr_batch_cache;
#endif
#ifdef READ_COMMITTED
    BatchTxn *curr_batch;
    std::atomic<uint64_t> curr_gather_batch_num;
    uint curr_batch_size;
    std::mutex gather_batch_mutex;
#endif

    DatabaseNode(const Configuration &conf, BenchmarkInterface &benchmark, RpcServerBase &rpcServer,
                 uint32_t num_pages, uint32_t batching_size, uint32_t split_batch_num = 1,
                 uint32_t cache_miss_ratio = 0)
        : conf(conf),
          rpcServer(rpcServer),
          benchmark(benchmark),
          page_manager(conf),
          ts_node_id(conf.get_ts_node()->node_id),
          starting_batch_id( ((uint64_t)conf.get_my_db_id()) << 32),
          ongoing_batches(rpcServer.num_threads),
          next_lbatch_id(1),
          ongoing_lbatches(rpcServer.num_threads),
          to_commit_queue(batching_size, starting_batch_id),
          scheduler(0),
          batching_size(batching_size),
          split_batch_num(split_batch_num),
          cache_miss_ratio(cache_miss_ratio) {
#ifdef EMULATE_AURORA
        curr_batch_cache = new_batch_cache(rpcServer.num_threads);
#endif
#ifdef READ_COMMITTED
        curr_batch = NewBatchTxn(page_manager, rpcServer.num_threads, batching_size);
        curr_batch_size = 0;
        curr_gather_batch_num = 1;
#endif
#ifdef DETERMINISTIC_VALIDATION
        for (uint32_t i = 0; i < MAX_SN_NUM; ++i) {
            my_seq[i] = 0;
        }
#endif
    }

    ~DatabaseNode() {
#ifdef EMULATE_AURORA
        if (curr_batch_cache)
            delete curr_batch_cache;
#endif
    }

   public:
    inline void init_benchmark() { benchmark.init_database(*this); }

    inline void end_benchmark() {}

    void recv_client_args(ClientArgs::Reader req, DatabaseWorker &worker) {
#ifdef RPC_CLIENTS
        DbClient *c = new DbClient(req.getClientId(), req.getData(), req.getTxnType());
        worker.my_event_channel->push_task_event(NEW_TXN, (uint64_t)c);
#else
        ASSERT(false);
#endif
    }

    void recv_get_page_resp(GetPageResponse::Reader resp,
                                   DatabaseWorker &worker) {
        ASSERT(resp.getOk());
        ASSERT(resp.hasPage());
        auto resp_page = resp.getPage();
        // DbTxn *txn = ongoing_txns.get(txn_id);
        DbTxn *txn = reinterpret_cast<DbTxn *>(resp.getTxnId());
        ASSERT(txn);
        PageMeta *page_meta = page_manager.set_page_cache(resp_page);
        ASSERT(page_meta);
        execute_txn(txn, worker);
    }

    void recv_get_pages_resp(GetPagesResponse::Reader resp,
                                   DatabaseWorker &worker) {
        LatencyLogger::start(LatencyType::RECV_GET_PAGES);
        DbTxn *txn = reinterpret_cast<DbTxn *>(resp.getTxnId());
        bool ready = false;
        {
            std::lock_guard<std::mutex> guard(txn->mutex);

            auto pages = resp.getPages();
            for (auto page : pages) {
                g_page_id_t g_page_id = page.getGlobalPageId();
                // PageMeta *page_meta = page_manager.get_page_from_cache(g_page_id);

                uint32_t size = txn->pages_for_remote_read.erase(g_page_id);
                ASSERT(size == 1);
            }

            ready = txn->pages_for_remote_read.empty();
        }
        LatencyLogger::end(LatencyType::RECV_GET_PAGES);

        if (ready) {
            LatencyLogger::start(LatencyType::RECV_GET_PAGES1);
            execute_txn(txn, worker);
            LatencyLogger::end(LatencyType::RECV_GET_PAGES1);
        }
    }


    void recv_get_timestamp_resp_batch(BatchGetTimestampResponse::Reader resp,
                                       DatabaseWorker &worker) {
        LatencyLogger::start(LatencyType::PARSE_GETTS);
        batch_id_t lbatch_id = resp.getLbatchId();
        // LOG(2) << "recv gts batch: " << lbatch_id;
        LargeBatch *lbatch = nullptr;
        bool success = ongoing_lbatches.try_get(lbatch_id, lbatch);
        ASSERT(success) << lbatch_id;
#ifdef TEST_TS
        for (auto batch : lbatch->sub_batches) {
            after_commit_phase(batch, worker);
        }
        return;
#else
        auto sub_resps = resp.getGettimestampresponse();
        for (uint i = 0; i < sub_resps.size(); i++) {
            BatchTxn *batch = lbatch->sub_batches[i];
            batch->timer.end(LatencyType::DB_GETTS);

            // std::lock_guard<std::mutex> guard(batch->mutex);

            batch->parse_get_timestamp_resp(sub_resps[i]);
        }
        LatencyLogger::end(LatencyType::PARSE_GETTS);
        send_prepare(lbatch, worker);
#endif  // TEST_TS
    }

    void recv_get_timestamp_resp(GetTimestampResponse::Reader resp,
                                 DatabaseWorker &worker) {
        batch_id_t batch_id = resp.getTxnId();
        BatchTxn *batch = ongoing_batches.get(batch_id);
        ASSERT(batch != nullptr);
#ifdef TEST_TS
        after_commit_phase(batch, worker);
        return;
#else
        // std::lock_guard<std::mutex> guard(batch->mutex);

        batch->parse_get_timestamp_resp(resp);
        batch->timer.end(LatencyType::DB_GETTS);

#ifndef BATCH_PREPARE
        send_prepare(batch, worker);
        // after_commit_phase(batch, worker);
#else
        LargeBatch *lbatch = batch->lbatch;
        if (--lbatch->sub_count == 0) {
            lbatch->sub_count = lbatch->sub_batches.size();
            send_prepare_lbatch(lbatch, worker);
        }
#endif
#endif
    }

    void send_prepare(LargeBatch *lbatch, DatabaseWorker &worker) {
        LatencyLogger::start(LatencyType::PREPARE_MSG);
#ifndef BATCH_PREPARE
        for (BatchTxn* batch : lbatch->sub_batches) {
            send_prepare(batch, worker);
        }
#else
        send_prepare_lbatch(lbatch, worker);
#endif
        LatencyLogger::end(LatencyType::PREPARE_MSG);
    }
    
    void send_prepare(BatchTxn *batch, DatabaseWorker &worker) {
        // LOG(2) << "send prepare " << batch->cts;

#ifdef TEST_ECHO
        batch->msg_count = 1;
        batch->cur_phase_success = true;
        send_prepare_req(0, batch);
#else
        // start first prepare phase
        LatencyLogger::start(LatencyType::GENREDOLOG);
        batch->gen_redo_logs();
        LatencyLogger::end(LatencyType::GENREDOLOG);

        send_all_prepare_reqs(batch, worker.lmsg_ptr);
#endif
    }

    void send_prepare_lbatch(LargeBatch *lbatch, DatabaseWorker &worker) {
        for (auto batch : lbatch->sub_batches) {
            // start first prepare phase
            LatencyLogger::start(LatencyType::GENREDOLOG);
            batch->gen_redo_logs();
            LatencyLogger::end(LatencyType::GENREDOLOG);
            // LOG(2) << std::hex << VAR2(batch->batch_id, " ");
        }        
        send_all_prepare_reqs_lbatch(lbatch, worker.lmsg_ptr);
    }

    void recv_prepare_resp(node_id_t from_sn_id, PrepareResponse::Reader resp,
                           MsgBuffer& zmq_msg, DatabaseWorker &worker) {
        LatencyLogger::start(LatencyType::PREPARE_REPAIR);
        batch_id_t batch_id = resp.getTxnId();
        BatchTxn *batch = ongoing_batches.get(batch_id);
        ASSERT (batch != nullptr) << std::hex << batch_id;

        if (conf.get_physical_sn_id(resp.getPrimarySnId()) == from_sn_id) {
            batch->attach_msg_buffer(zmq_msg, resp);
            if (!resp.getOk()) {
#if !defined(ABORT_ON_FAIL) || defined(EMULATE_AURORA)
                LatencyLogger::start(LatencyType::APPLY_DIFF_PAGES);
                auto diff_pages = resp.getDiff();
                for (auto diff_page : diff_pages) {
                    apply_diff_page(diff_page);
                }
                for (auto diff_page : diff_pages) {
                    prepare_for_repair(batch, diff_page);
                }
                LatencyLogger::end(LatencyType::APPLY_DIFF_PAGES);
                // LOG(2) << "prepare_for_repair: " << std::hex << batch_id << " diff " << diff_pages.size();
#endif
                batch->cur_phase_success = false;
            }
        }
            // LOG(2) << "recv prepare for " << std::hex << batch_id << (resp.getOk() ? " ok" : " repair") << " from " << from_sn_id;

        while (batch->sending)
            ;
        int remaining = batch->msg_count.fetch_sub(1);
        // LOG(2) << "recv prepare " << std::hex << batch_id << " " << from_sn_id << " " << remaining;
        if (remaining == 1) {
            // start repair or commit phase, accordingly
            after_one_prepare_phase(batch, worker);
        }
    }

    void recv_commit_resp(node_id_t from_sn_id, CommitResponse::Reader resp,
                          DatabaseWorker &worker) {
        LatencyLogger::start(LatencyType::PARSE_COMMIT);
        if (resp.getIsBroadCast()) {
#ifdef SN_BROADCAST
            // ASSERT ((from_sn_id % 3) ==0);
            if (resp.getOk() && resp.getDiff().size()) {
                auto diff_pages = resp.getDiff();
                for (auto diff_page : diff_pages) {
                    try_apply_diff_page(diff_page);
                }
            }
            return;
#endif
            ASSERT(false);
        }
        batch_id_t batch_id = resp.getTxnId();
        BatchTxn *batch = ongoing_batches.get(batch_id);
        if (!batch) {
            LatencyLogger::end(LatencyType::PARSE_COMMIT);
            return;
        }
        while (batch->sending)
            ;
        ASSERT(batch->batch_id == batch_id) << std::hex << batch->batch_id << " " << batch_id << " " << batch;
        // Stop Timer after recv commit
        LatencyLogger::end(LatencyType::PARSE_COMMIT);

        // LOG(2) << "recv commit for " << std::hex << batch_id << " from " << from_sn_id;

        if (resp.hasDiff()) {
            LatencyLogger::start(LatencyType::APPLY_DIRTY);
            if (resp.getRepairedTxns().size() != 0) {
                uint64_t primary_sn_id = resp.getPrimarySnId();
                if (from_sn_id == primary_sn_id) {
                    batch->is_repaired = true;
                    auto& fast_txns = batch->sn_infos[primary_sn_id]->fast_txns;
                    for (auto i : resp.getRepairedTxns()) {
                        // LOG(2) << VAR2(i, " ");
                        fast_txns[i]->repaired = true;
                    }
                }
            }
            auto diff_pages = resp.getDiff();
            for (auto diff_page : diff_pages) {
                apply_diff_page(diff_page);
            }
            LatencyLogger::end(LatencyType::APPLY_DIRTY);
        }

        if (--batch->msg_count == 0) {
            ASSERT(!batch->is_aborted);
            after_commit_phase(batch, worker);
        }
    }

   public:
    inline PageMeta *init_empty_page(seg_id_t seg_id, page_id_t page_id) {
        return page_manager.init_page_cache(GlobalPageId(seg_id, page_id).g_page_id);
    }

    void get_remote_page_async(seg_id_t seg_id, page_id_t page_id, txn_id_t txn_id) {
        ASSERT(cache_miss_ratio != 0);
        send_get_page(seg_id, page_id, txn_id);
    }

   private:

#ifdef LOCK_FAIL_BLOCK
    void resume_txn(DatabaseWorker &worker) {
        while (true) {
            if(worker.blocked_txns.empty()) {
                return;
            }
            auto iter = worker.blocked_txns.begin();
            // LOG(2) << latency_worker_id << " resume: " << std::hex << iter->first
            //     << " " << &iter->second << " " << iter->second.size();
            // if (iter->first <= worker.latest_batch_num + 1) {
            std::vector<DbTxn*> vec;
            bool success = true;
            for (auto* txn : iter->second) {
                if (success) {
                    worker.retry_txn(txn);
                    success = execute_txn(txn, worker);
                    // LOG(2) << latency_worker_id << " success: " << std::hex << iter->first;
                } 
                if (!success) {
                    vec.push_back(txn);
                }
                // worker.my_event_channel->push_task_event(RETRY_TXN, txn);
            }
            if (success) {
                worker.blocked_txns.erase(iter);
            } else {
                vec.swap(iter->second);
                break;
            }
        }
    }
#endif

    bool execute_txn(DbTxn *txn, DatabaseWorker &worker) {
#ifdef LOCK_FAIL_BLOCK
        if (!txn->blocked) {
#elif defined(NON_CACHING) 
        if(txn->phase == 0) {
#else
        if(true) {
#endif
            LatencyLogger::start(LatencyType::PREV_EXECUTION);
            // uint32_t r = thread_rand.randint(0, 99);
#ifdef READ_COMMITTED
            {
                std::lock_guard<std::mutex> guard(gather_batch_mutex);
                if (curr_batch_size >= this->batching_size) {
                    curr_batch_size = 0;
                    curr_batch = NewBatchTxn(page_manager, rpcServer.num_threads, batching_size);
                    curr_gather_batch_num++;
#ifdef EMULATE_AURORA
                    curr_batch_cache = new_batch_cache(rpcServer.num_threads);
#endif
                }
                txn->batch = (void *)curr_batch;
#ifdef EMULATE_AURORA
                txn->batch_cache = curr_batch_cache;
#endif
                txn->gather_batch_num = curr_gather_batch_num;
                curr_batch_size++;
            }
#ifdef LOCK_FAIL_BLOCK
            auto rbegin = worker.blocked_txns.rbegin();
            if (rbegin != worker.blocked_txns.rend()) {
                if (txn->gather_batch_num >= rbegin->first) {
                    txn->blocked = true;
                    // worker.blocked_txns[e.b_id].push_back(txn);
                    worker.blocked_txns[txn->gather_batch_num].push_back(txn);
                    LatencyLogger::end(LatencyType::PREV_EXECUTION);
                    return true;
                }

            }
#endif
#endif
            LatencyLogger::end(LatencyType::PREV_EXECUTION);
        }

        return execute_txn_internal(txn, worker);
    }
        
    bool execute_txn_internal(DbTxn *txn, DatabaseWorker &worker) {
        txn->p_timer.start();
        try {
            bool finished = txn->execution_logic(txn, worker.worker_id);
            if (!finished) {
                txn->p_timer.end(LatencyType::RECV_EXE);
                return false;
            }
        } 
#ifdef LOCK_FAIL_BLOCK
        catch (const BatchLockException &e) {
            // LOG(2) << "batch failed: " << std::hex << txn->txn_id << " " << e.b_id;
            if (!txn->blocked) {
                txn->blocked = true;
                worker.blocked_txns[txn->gather_batch_num].push_back(txn);
            }
            return false;
        } 
#endif
        catch (const TxnAbortException &e) {
            txn->p_timer.end(LatencyType::LOCAL_ABORT_EXECUTION);
#ifdef READ_COMMITTED
            BatchTxn *batch = (BatchTxn *)txn->batch;
            {
                std::lock_guard<std::mutex> guard(batch->mutex);
                // TODO: can wo omit this line?
                batch->ordered_txns.push_back(nullptr);
                txn->release_locks();
                txn->release_batch_locks();
                if (batch->ordered_txns.size() == batching_size) {
                    gather_batch_split(worker, split_batch_num, batch);
                    delete_batch_txn(batch);
                    delete_batch_cache(txn->batch_cache);
                }
            }
#else
            txn->release_locks();
#endif

            end_txn(txn, false, worker);
            worker.my_event_channel->push_task_event(RETRY_TXN, txn);
            // LOG(2) << "aborted: " << std::hex << txn->txn_id;
            return false;
        }
        // LOG(2) << latency_worker_id << " success: " << std::hex << txn->txn_id << " " << txn->gather_batch_num;

#ifndef EMULATE_AURORA
        txn->local_commit();
#else
#ifdef READ_COMMITTED
        for (auto &record : txn->write_set) {
            Key_t key = *(Key_t *)record.get_key_buf();
            txn->batch_cache->put(key, {record.value_buf(), record.get_writer()});
        }
#endif
#endif
        txn->p_timer.end(LatencyType::EXECUTION);

#ifdef TEST_SINGLE_LOCAL_COMMIT
        txn->cur_phase_success = true;
        txn->release_locks();
        after_commit_phase(txn, worker);
        return true;
#endif

#ifdef READ_COMMITTED
        BatchTxn *batch = (BatchTxn *)txn->batch;
        std::lock_guard<std::mutex> guard(batch->mutex);
        batch->ordered_txns.push_back(txn);
        txn->release_locks();
        if (batch->ordered_txns.size() == batching_size) {
            LatencyLogger::start(LatencyType::POP_TXN_QUEUE);
            gather_batch_split(worker, split_batch_num, batch);
            delete_batch_txn(batch);
            delete_batch_cache(txn->batch_cache);
            LatencyLogger::end(LatencyType::POP_TXN_QUEUE);
        }
#else
        LatencyLogger::start(LatencyType::ENQUEUE_BATCH);
        to_commit_queue.enqueue_txn(txn);
        txn->release_locks();
        LatencyLogger::end(LatencyType::ENQUEUE_BATCH);
        LatencyLogger::start(LatencyType::POP_TXN_QUEUE);
        gather_batch_split(worker, split_batch_num);
        LatencyLogger::end(LatencyType::POP_TXN_QUEUE);
#endif
        return true;
    }


    // dummy: randomly add to a small batch
    void split_batch_random(const std::vector<DbTxn *> &ordered_txns, int split_number,
                            BatchTxn **batches) {
        for (DbTxn *txn : ordered_txns) {
            uint i = thread_rand.next_u32() % split_number;
            batches[i]->ordered_txns.push_back(txn);
        }
    }

    void split_batch_sequential(const std::vector<DbTxn *> &ordered_txns, int split_number,
                                std::vector<BatchTxn *> &batches) {
        uint64_t sub_batch_size = ordered_txns.size() / split_number;
        for (int i = 0; i < split_number; ++i) {
            for (uint64_t j = 0; j < sub_batch_size; ++j) {
                batches[i]->ordered_txns.push_back(ordered_txns[i * sub_batch_size + j]);
            }
        }
    }

    void split_batch_all(const std::vector<DbTxn *> &ordered_txns,
                         std::vector<BatchTxn *> &batches) {
            for (DbTxn *t : ordered_txns) {
                if (!t) {
                    continue;
                }
                batches[0]->ordered_txns.push_back(t);
            }
    }

    void split_batch_dep(const std::vector<DbTxn *> &ordered_txns, int split_number,
                         std::vector<BatchTxn *> &batches) {
        if (split_number == 1) {
            split_batch_all(ordered_txns, batches);
            return;
        }
        std::unordered_map<seg_id_t, uint> seg_access_count;

        // 1. build dependency graph in those txns
        std::vector<std::vector<uint>> deps(ordered_txns.size());  // dependency graph
        std::vector<bool> high_contention_txn(ordered_txns.size(), false);
        std::unordered_map<seg_id_t, uint> last_write;  // use this to build edges
        std::unordered_map<seg_id_t, std::vector<uint>> last_reads;

        for (uint i = 0; i < ordered_txns.size(); i++) {
            DbTxn *txn = ordered_txns[i];
            if (txn == nullptr)
                continue;
            for (auto &page_iter : txn->pages) {
                seg_id_t seg_id = GlobalPageId(page_iter.first).seg_id;
#ifdef SPLIT_ON_READ_WRITE
                if (page_iter.second.is_write) {
#else
                if (true) {
#endif
                    // write this segment
                    auto iter = last_write.find(seg_id);
                    if (iter != last_write.end()) {
                        deps[i].push_back(iter->second);
                        deps[iter->second].push_back(i);  // bi-directional graph
                    }
                    last_write[seg_id] = i;
#ifdef SPLIT_ON_READ_WRITE
                    for (uint read_i : last_reads[seg_id]) {
                        deps[read_i].push_back(i);
                        deps[i].push_back(read_i);
                    }
                    last_reads[seg_id].clear();
#endif
                } else if (page_iter.second.is_read) {
                    // read-only on this segment
                    auto iter = last_write.find(seg_id);
                    if (iter != last_write.end()) {
                        deps[i].push_back(iter->second);
                        deps[iter->second].push_back(i);  // bi-directional graph
                    }
                    last_reads[seg_id].push_back(i);
                }
            }
        }

        // 2. use DFS (or disjoint set) to cut connectted components
        std::vector<std::vector<uint>> ccs;
        std::vector<bool> visited(ordered_txns.size(), false);
        std::vector<uint> belong_to(ordered_txns.size(), 0xdeadbeef);
        std::stack<uint> dfs_stack;
        // iterate from the back
        for (int i = ordered_txns.size() - 1; i >= 0; i--) {
            auto txn = ordered_txns[i];
            if (txn == nullptr)
                continue;
            if (visited[i])
                continue;
            ccs.emplace_back();  // create a new CC
            // start DFS
            dfs_stack.push(i);
            while (!dfs_stack.empty()) {
                uint j = dfs_stack.top();
                dfs_stack.pop();
                // ASSERT(!visited[j] || belong_to[j] == ccs.size() - 1)
                //     << "txn appears in 2 ccs: " << belong_to[j] << " " << ccs.size() - 1;
                if (visited[j])
                    continue;
                visited[j] = true;
                belong_to[j] = ccs.size() - 1;
                // if (high_contention_txn[j]) {
                //     high_contention_cc[high_contention_cc.size() - 1] = true;
                // }
                ccs.back().push_back(j);
                for (uint k : deps[j]) {
                    dfs_stack.push(k);
                }
            }
        }

        // 3. add connected component to the smallest/random sub-batch
        std::vector<std::vector<uint>> sub_batches(split_number);
        for (uint i = 0; i < ccs.size(); i++) {
            auto& cc = ccs[i];
            sub_batches[i].insert(sub_batches[i].end(), cc.begin(), cc.end());
        }

        // 4. the order of txns in each CC should remain the same with commit order
        for (int i = 0; i < split_number; i++) {
            // vuint *sub_batch = sub_batches.top();
            // sub_batches.pop();
            std::vector<uint>& sub_batch = sub_batches[i];
            // LOG(2) << sub_batch->size();
            std::sort(sub_batch.begin(), sub_batch.end());
            for (uint j : sub_batch) {
                batches[i]->ordered_txns.push_back(ordered_txns[j]);
            }
        }
    }

    void gather_batch_split(DatabaseWorker &worker, int split_number, BatchTxn *batch = nullptr) {
        LatencyLogger::start(LatencyType::DEQUEUE_BATCH);
        std::vector<DbTxn *> ordered_txns;
        bool success;
        // get a batchsize of txns
        batch_id_t last_batch_id = 0;
        if (batch) {
            last_batch_id = to_commit_queue.new_batch_ids(split_number);
            ordered_txns = batch->ordered_txns;
            success = true;
        } else {
            last_batch_id = to_commit_queue.dequeue_batch(ordered_txns, split_number, success);
        }
        LatencyLogger::end(LatencyType::DEQUEUE_BATCH);
        if (!success) {
            return;
        }

        LatencyLogger::start(LatencyType::NEW_BATCH);
        batch_id_t lbatch_id = (last_batch_id - starting_batch_id) / split_number;
        LargeBatch *lbatch = new_large_batch(lbatch_id);

        // create some empty batches
        std::vector<BatchTxn *> batches(split_number, nullptr);
        for (int i = 0; i < split_number; i++) {
            BatchTxn* batch = NewBatchTxn(page_manager, rpcServer.num_threads, batching_size);
            batch->batch_id = last_batch_id - split_number + 1 + i;
            batch->lbatch = lbatch;
            batches[i] = batch;
        }
        LatencyLogger::end(LatencyType::NEW_BATCH);

        LatencyLogger::start(LatencyType::SPLIT_BATCH_LAT);
        // add txns to the sub-batches
        // split_batch_random(ordered_txns, split_number, batches);
        // split_batch_sequential(ordered_txns, split_number, batches);
#if defined(TEST_TS) || defined(BREAKDOWN_MSG_SIZE) 
        split_batch_all(ordered_txns, batches);
#else
        split_batch_dep(ordered_txns, split_number, batches);
#endif

#ifdef COUNT_BATCH_SIZE
        for (int i = 0; i < split_number; i++) {
            auto batch_size = batches[i]->ordered_txns.size();
            if (batch_size != 0) {
                Logger::count(CountType::BATCH_SIZE, batch_size);
            }
        }
#endif
        for (auto batch : batches) {
            if (batch != nullptr && batch->ordered_txns.size() != 0) {
                lbatch->sub_batches.push_back(batch);
                ongoing_batches.put(batch->batch_id, batch);
            } else {
                delete_batch_txn(batch);
            }
        }
        lbatch->sub_count = lbatch->sub_batches.size();
        if (lbatch->sub_count == 0) {
            // LOG(3) << "delete lbatch " << lbatch->id;
            delete_large_batch(lbatch);
            return;
        }
        LatencyLogger::end(LatencyType::SPLIT_BATCH_LAT);
        after_batch_gathering(lbatch, worker);
    }

    DbTxn *new_txn(txn_id_t txn_id, DbClient *c) {
        // LatencyLogger::start(LatencyType::GET_LOGIC);
        auto txn_logic = benchmark.get_txn_logic(c->txn_seed);
        auto part_logic = benchmark.get_partition_logic();
        // LatencyLogger::end(LatencyType::GET_LOGIC);

        // LatencyLogger::start(LatencyType::GET_INPUT);
        auto input = benchmark.get_input(c);
        if (input == nullptr) {
            return nullptr;
        }
        // LatencyLogger::end(LatencyType::GET_INPUT);

        DbTxn *txn = new DbTxn(page_manager, txn_logic, input, part_logic);
        txn->remote_read_logic = [this](DbTxn* txn, 
                std::unordered_map<g_page_id_t, std::vector<offset_t>> pages_for_remote_read) {
            this->send_get_pages(txn, pages_for_remote_read);
        };

        txn->txn_id = txn_id;
        return txn;
    }

    void end_txn(DbTxn *txn, bool commit, DatabaseWorker &worker) {

        // LOG(2) << "end_txn: " << std::hex << txn->txn_id;

        if (commit) {
            // txn->commit();
            if (start_benchmarking_flag) {
                worker.profile.commit();
            }
            txn->gc_logic(txn, worker.worker_id);            
            
            while (!txn->released) {
                COMPILER_MEMORY_FENCE();
            }
#ifndef RPC_CLIENTS
            delete txn->client;
#endif
            delete txn;

        } else {
            if (start_benchmarking_flag) {
                worker.profile.abort();
            }
#ifdef READ_COMMITTED
            txn->release_batch_locks();
#endif

            while (!txn->released) {
                COMPILER_MEMORY_FENCE();
            }

            txn->clear_all();
        }
    }

    void after_batch_gathering(LargeBatch *lbatch, DatabaseWorker &worker) {
        LatencyLogger::start(LatencyType::MERGE_RW_INFO);
        for (auto batch : lbatch->sub_batches) {
            batch->batch_manager.set_batch_id(batch->batch_id);
#ifdef EMULATE_AURORA
            batch->fastCommitEnabled = false;
#else
            for (auto &txn : batch->ordered_txns) {
                txn->p_timer.end(LatencyType::DB_EXE);
                if (!txn->fastCommitEnabled) {
                    batch->fastCommitEnabled = false;
                    break;
                }
            }
            if (!batch->fastCommitEnabled) {
                for (auto &txn : batch->ordered_txns) {
                    txn->fastCommitEnabled = false;
                }
            }
#endif
            batch->merge_rw_infos();

#ifdef TEST_LOCAL_COMMIT
            batch->cur_phase_success = true;

            // LatencyLogger::start(LatencyType::SEND_GETTS);
            // send_get_timestamp_req(batch);
            // LatencyLogger::end(LatencyType::SEND_GETTS);


            // LatencyLogger::start(LatencyType::LOCAL_COMMITMSG);
            // send_all_commit_reqs(batch);
            // LatencyLogger::end(LatencyType::LOCAL_COMMITMSG);

            // after_commit_phase(batch, worker);
            continue;
#endif
        }
        ongoing_lbatches.put(lbatch->id, lbatch);
        LatencyLogger::end(LatencyType::MERGE_RW_INFO);

#if !defined(NO_TS) || defined(DETERMINISTIC_VALIDATION)  
        LatencyLogger::start(LatencyType::SEND_GETTS);
#ifndef TEST_TS_KEYGEN 
        batch_id_t unblock_batch_id = scheduler.next_get_timestamp();
        bool success = false;
        do {
            success = send_get_timestamp_and_unblock(unblock_batch_id, worker);
            unblock_batch_id++;
        } while (success);
#else
        send_get_timestamp(lbatch, worker);
#endif
        LatencyLogger::end(LatencyType::SEND_GETTS);
#else
        ongoing_lbatches.put(lbatch->id, lbatch);
        send_prepare(lbatch, worker);
#endif

#ifdef TEST_LOCAL_COMMIT
        send_prepare(lbatch, worker);

        for (auto batch : lbatch->sub_batches) {
#ifdef READ_COMMITTED
            batch->release_aurora_locks();
#ifdef LOCK_FAIL_BLOCK
            uint64_t part_id = batch->ordered_txns[0]->part_id;
            rpcServer.get_specific_event_channel(part_id)->push_prior_task_event(DbTaskType::EMPTY, part_id);
#endif
#endif
            after_commit_phase(batch, worker);
        }
#endif
    }

    bool send_get_timestamp_and_unblock(batch_id_t b_id, DatabaseWorker &worker) {
        // LOG(2) << "try ts: " << std::hex << b_id;
        if (scheduler.next_get_timestamp() != b_id) {
            // LOG(2) << "try ts failed: " << std::hex << b_id << " " << scheduler.next_get_timestamp();
            return false;
        }
        LargeBatch *lbatch = nullptr;
        ongoing_lbatches.try_get(b_id, lbatch);
        if (!lbatch) {
            // LOG(2) << "try ts failed: " << std::hex << b_id;
            return false;
        }
        {
            // ensure a batch only send ts once
            std::lock_guard<std::mutex> guard(lbatch->mutex);
            if (lbatch->id != b_id) {
                // LOG(2) << "try ts failed: " << std::hex << b_id;
                return false;
            }
            if (lbatch->cur_phase == BatchTxn::BatchStatus::SEND_TIMESTAMP) {
                // has sent ts
                // LOG(2) << "try ts failed: " << std::hex << b_id;
                return false;
            }
            lbatch->cur_phase = BatchTxn::BatchStatus::SEND_TIMESTAMP;
        }
        // if (lbatch->id != b_id) {
        //     ASSERT(false) << std::hex << lbatch->id << " " << b_id << " " << scheduler.next_get_timestamp();
        //     return false;
        // }
        // ASSERT(lbatch->id == b_id) << std::hex << lbatch->id << " " << b_id 
        //     << " " << temp << " " << scheduler.next_get_timestamp();
        if (lbatch->sub_batches.size() != 0) {
#ifdef DETERMINISTIC_VALIDATION
            for (auto* batch: lbatch->sub_batches) {
                for (node_id_t i = 0; i < conf.numLogicalSN(); i++) {
                    if (batch->sn_infos[i]) {
                        batch->sn_infos[i]->seq = my_seq[i].fetch_add(1);
                    }
                }
            }
            rpcServer.get_one_event_channel()->push_prior_task_event_back(SEND_PREPARE, lbatch);
#else
            send_get_timestamp(lbatch, worker);
#endif
            scheduler.finish_get_timestamp(b_id);
        } else {
            scheduler.finish_get_timestamp(b_id);
            ongoing_lbatches.erase(b_id);
            delete_large_batch(lbatch);
        }
        return true;
    }

    void send_get_timestamp(LargeBatch *lbatch, DatabaseWorker &worker) {
#ifdef BATCH_GET_TS
        send_get_timestamp_req_batch(lbatch);
#else
        for (BatchTxn* batch : lbatch->sub_batches) {
            send_get_timestamp_req(batch);
        }
#endif
    }

    void handle_own_txns(std::vector<DbTxn *> *to_gc_txns, HandleOwnTxnTaskType type,
                         DatabaseWorker &worker) {
        for (auto txn : *to_gc_txns) {
            if (txn == nullptr)
                continue;
            switch (type) {
                case HandleOwnTxnTaskType::GC:
                    end_txn(txn, true, worker);
                    break;
                default:
                    ASSERT(false);
            }
        }
        if (type == HandleOwnTxnTaskType::GC) {
            delete to_gc_txns;
        }
    }

    void after_one_prepare_phase(BatchTxn *batch, DatabaseWorker &worker) {
        if (batch->is_repaired) {
            batch->timer.end(LatencyType::DB_PREPARE2);
        } else {
            batch->timer.end(LatencyType::DB_PREPARE);
        }
        // LOG(2) << "after_one_prepare_phase: " << 
        //     std::hex << batch->batch_id << " " << (batch->cur_phase_success ? "commit" : "abort");
        if (batch->cur_phase_success) {
#ifdef TEST_ECHO
            batch->msg_count = 1;
            send_commit_req(0, *batch);
#else
            LatencyLogger::start(LatencyType::LOCAL_COMMITMSG);
            send_all_commit_reqs(batch);
            LatencyLogger::end(LatencyType::LOCAL_COMMITMSG);
#endif
        } else {

#ifdef ABORT_ON_FAIL
            abort_phase(batch, worker);
            return;
#endif
            // Start second Prepare Phase
            batch->is_repaired = true;
            batch->clear_access_info();

            LatencyLogger::start(LatencyType::BATCH_REPAIR);
            uint num_repaired = batch->repair(worker.worker_id);
            LatencyLogger::end(LatencyType::BATCH_REPAIR);
            // LOG(2) << "repair for " << std::hex << batch->batch_id;
            LatencyLogger::start(LatencyType::PREPARE_AFTER_REPAIR);
            batch->merge_rw_infos_repair();
            LatencyLogger::end(LatencyType::PREPARE_AFTER_REPAIR);
            batch->free_redo_logs();
            batch->gen_redo_logs_repair();
            LatencyLogger::start(LatencyType::PREPARE_AFTER_REPAIR2);
#ifdef NORMAL_PATH_OPT
            send_all_prepare_reqs_repair(batch,worker.lmsg_ptr);
#else
#ifdef TOTAL_ORDER_TS
            send_all_prepare_reqs(batch,worker.lmsg_ptr, false);
#else
            send_all_prepare_reqs(batch,worker.lmsg_ptr);
#endif
#endif
            LatencyLogger::end(LatencyType::PREPARE_AFTER_REPAIR2);
            LatencyLogger::end(LatencyType::PREPARE_REPAIR);
            if (start_benchmarking_flag){
                worker.profile.db_batch_repair();
                worker.profile.db_repair(num_repaired);
            }
        }
    }

    inline void after_commit_phase(DbTxn *txn, DatabaseWorker &worker) {
        end_txn(txn, txn->cur_phase_success, worker);
#ifndef TEST_LOCAL_COMMIT
        worker.my_event_channel->push_task_event(NEW_TXN, NULL_TXN_ID);
#endif
    }

    void gc_batch(BatchTxn *batch) {
        // LatencyLogger::start(LatencyType::LOCAL_GC_DELETE_BATCH);
#ifndef INDEPENDENT_GC
        for (auto txn : batch->ordered_txns) {
            end_txn(txn, true, worker);
        }
#else
        std::vector<std::vector<DbTxn *> *> to_gc_txns;
        for (uint32_t i = 0; i < rpcServer.num_threads; ++i) {
            to_gc_txns.emplace_back(new std::vector<DbTxn *>);
        }

        for (auto txn : batch->ordered_txns) {
            TxnIdWrapper wrapper(txn->txn_id);
            to_gc_txns[wrapper.thread_id]->emplace_back(txn);
        }

        for (uint32_t channel_i = 0; channel_i < rpcServer.num_threads; ++channel_i) {
            auto worker_to_gc_txns = to_gc_txns[channel_i];
            if (worker_to_gc_txns->empty()) {
                delete worker_to_gc_txns;
            } else {
                rpcServer.get_specific_event_channel(channel_i)->push_prior_task_event(
                    GC_BATCH, worker_to_gc_txns);
            }
        }
#endif
        LargeBatch *lbatch = batch->lbatch;
        if (--lbatch->sub_count == 0) {
            ongoing_lbatches.erase(lbatch->id);
            delete_large_batch(lbatch);
        }
        delete_batch_txn(batch);
        // LatencyLogger::end(LatencyType::LOCAL_GC_DELETE_BATCH);
    }

#ifdef LOCK_FAIL_BLOCK
    void update_blocked_batch_num(uint64_t num) {
        // LOG(2) << "set blocked_batch_num: " << std::hex << blocked_batch_num;
        for (uint64_t thread_id = 0; thread_id < rpcServer.num_threads; ++thread_id) {
            rpcServer.get_specific_event_channel(thread_id)->push_task_event(DbTaskType::EMPTY, thread_id);
        }
    }
#endif

    void return_to_client(BatchTxn *batch) {
        {
            // make sure clients are only returned once
            if (batch->clients_returned) {
                return;
            }
            batch->clients_returned = true;
        }
#ifdef RPC_CLIENTS
        LatencyLogger::start(LatencyType::DB_REPLY_CLIENT);
        BUILD_REQUEST(conf.get_client_node_id(0));
        auto req = request.initData().initClientresponse();
        auto clientIds = req.initClientIds(batch->ordered_txns.size());
        for (uint i = 0; i < batch->ordered_txns.size(); ++i) {
            auto txn = batch->ordered_txns[i];
            ASSERT(txn->client);
            clientIds.set(i, txn->client->id);
            delete txn->client;
        }
        req.setSuccess(true);
        rpcServer.send(msgBuilder);
        LatencyLogger::end(LatencyType::DB_REPLY_CLIENT);
#else
        for (uint i = 0; i < batch->ordered_txns.size(); ++i) {
            rpcServer.get_one_event_channel()->push_task_event(NEW_TXN, NULL_TXN_ID);
        }
#endif
    }

    inline void after_commit_phase(BatchTxn *batch, DatabaseWorker &worker) {
        batch->timer.end(LatencyType::DB_COMMIT);
        LatencyLogger::start(LatencyType::LOCAL_GC);
        if (batch->fastCommitEnabled) {
            // sn repair
#ifdef READ_COMMITTED
            batch->release_aurora_locks();
#ifdef LOCK_FAIL_BLOCK
            uint64_t part_id = batch->ordered_txns[0]->part_id;
            rpcServer.get_specific_event_channel(part_id)->push_prior_task_event(DbTaskType::EMPTY, part_id);
#endif
#endif
            if (batch->is_repaired) {
                if (start_benchmarking_flag) {
                    worker.profile.sn_batch_repair();
                    for (auto txn : batch->ordered_txns) {
                        if (txn->repaired)
                            worker.profile.sn_repair();
                    }
                }
            }
        } else {
            batch->commit();
        }
#ifdef TXN_LAT_STAT
        // TODO: verify whether all txns committed pass this code path
        for(DbTxn* txn : batch->ordered_txns){
            // Stop timer of a transaction and record its latency
            txn->stopTimer();
        }
#endif

        return_to_client(batch);

        ongoing_batches.erase(batch->batch_id);

        gc_batch(batch);

        if (start_benchmarking_flag)
            worker.profile.commit_batch();
        LatencyLogger::end(LatencyType::LOCAL_GC);
    }

    void roll_back_batch(BatchTxn *batch) {
        for (int i = batch->ordered_txns.size() - 1; i >= 0; --i) {
            // roll back all written values
            auto txn = batch->ordered_txns[i];
            txn->roll_back();
        }
    }

    void abort_phase(BatchTxn *batch, DatabaseWorker &worker, bool send_commit = true) {
        batch->is_aborted = true;
#if defined(ABORT_ON_FAIL) && !defined(EMULATE_AURORA)
        int size = batch->ordered_txns.size();
        for (int i = size - 1; i >= 0; --i) {
            batch->ordered_txns[i]->roll_back();
        }
        // for (auto& resp : batch->resps) {
        for (auto& msg_buf : batch->msg_bufs) {
            BUILD_MESSAGE_FROM_BUFFER(msg_buf, msg);
            auto resp = msg.getData().getPrepareresponse();
            if(resp.hasDiff()) {
                auto diff_pages = resp.getDiff();
                // LOG(2) << VAR(diff_pages.size()) << " " << std::hex << batch->batch_id;
                for (auto diff_page : diff_pages) {
                    // apply_diff_page_print(diff_page);
                    apply_diff_page(diff_page);
                }
            }
        }
#endif

#ifdef READ_COMMITTED
        batch->release_aurora_locks();
#ifdef LOCK_FAIL_BLOCK
        ASSERT(false);
        uint64_t part_id = batch->ordered_txns[0]->part_id;
        rpcServer.get_specific_event_channel(part_id)->push_prior_task_event(DbTaskType::EMPTY, part_id);
#endif
#endif
        for (auto txn : batch->ordered_txns) {
            if (start_benchmarking_flag)
                worker.profile.remote_abort();
            end_txn(txn, false, worker);
            // TODOO: batch push
            rpcServer.get_one_event_channel()->push_task_event(RETRY_TXN, txn);
        }
        ongoing_batches.erase(batch->batch_id);
        if (send_commit) {
            send_all_commit_reqs(batch);
        }
        delete_batch_txn(batch);

        if (start_benchmarking_flag && send_commit)
            worker.profile.abort_batch();
    }

    // read kv for repair
    void prepare_for_repair(BatchTxn *batch, MultiLogPage::Reader diff_page) {
#ifdef EMULATE_AURORA
        return;
#endif
        auto array = diff_page.getData();
        auto byte_array = array.asBytes();

        // We always collect redo kvs, even if we don't need to apply diff page
        if (diff_page.hasData()) {
            // SN has just applied the page's redo logs
            // we have to repair_get data from redo page
            read_page_data_kv(batch, diff_page.getGlobalPageId(),
                              const_cast<uint8_t *>(byte_array.begin()));
        }
        // read kv in redo log
        read_redo_log_kv(batch, diff_page);
    }

    void apply_diff_page(MultiLogPage::Reader diff_page) {
        g_page_id_t g_page_id = diff_page.getGlobalPageId();
        PageMeta *page_meta = page_manager.get_page_from_cache(g_page_id);

        std::lock_guard<std::mutex> guard(page_meta->mutex);
#ifdef FINE_DIFF
        apply_diff_page(page_meta, diff_page);
#else
        ts_t ts = diff_page.getCts();
        if (page_meta->get_cts() > ts) {
            return;
        }
        apply_diff_page(page_meta, diff_page);
        page_meta->set_cts(ts);
#endif
    }


    void apply_diff_page_print(PageMeta *page_meta, MultiLogPage::Reader diff_page) {
        // First apply entire page if stale
        uint8_t* data = page_meta->get_data();
        if (diff_page.hasData()) {
            ASSERT(false);
            auto array = diff_page.getData();
            auto byte_array = array.asBytes();
            // memcpy(data, byte_array.begin(), byte_array.size());

            auto* meta = reinterpret_cast<LeafMeta* >((uint8_t *)byte_array.begin());
            auto* cache_meta = reinterpret_cast<LeafMeta* >(data);
            for (slot_t i = 0; i < meta->num_slots; ++i) {
                uint8_t* pair = meta->get_record_ptr(meta->get_offset(i));
                uint8_t* cache_pair = cache_meta->get_record_ptr(cache_meta->get_offset(i));
                r_copy(cache_pair, pair, meta->value_size);
            }
        }

        // Then apply redo logs
        auto redo_logs = diff_page.getRedoLogs();
        uint32_t num_log = redo_logs.size();
        for (uint32_t log_i = 0; log_i < num_log; ++log_i) {
            auto redo_log = redo_logs[log_i];
            if (redo_log.size() == 0) {
                continue;
            }
            RedoLogReader log_reader(redo_log.asBytes(), page_meta->get_value_size());
            while (log_reader.valid()) {
                if (!log_reader.is_write_entry()) {
                    // this is a read
                    log_reader.next();
                    continue;
                }
                uint64_t page_offset = log_reader.get_real_page_offset();
                uint8_t* record_ptr = data + page_offset;
                set_key(record_ptr, log_reader.get_key_ptr());
                set_value(record_ptr, log_reader.get_value_ptr(), log_reader.get_value_size());
                // clear local version
                set_writer(record_ptr, log_reader.get_version());

                LOG(2) << std::hex << page_meta->gp_id.seg_id << " " << page_meta->gp_id.page_id 
                    << " " << page_offset << " " << log_reader.get_version();

                // TODO: unref the pointer of the local version
                log_reader.next();
            }
        }
    }


    void try_apply_diff_page(MultiLogPage::Reader diff_page) {
        g_page_id_t g_page_id = diff_page.getGlobalPageId();
        PageMeta *page_meta = page_manager.try_get_page_from_cache(g_page_id);
        if (page_meta == nullptr) {
            return;
        }
        ts_t ts = diff_page.getCts();

        std::lock_guard<std::mutex> guard(page_meta->mutex);
        if (page_meta->get_cts() > ts) {
            return;
        }

        // 2. update the data.
        apply_diff_page(page_meta, diff_page);
        // At last, set ts
        page_meta->set_cts(ts);
    }

    void read_page_data_kv(BatchTxn *batch, g_page_id_t gp_id, uint8_t *data) {
        seg_id_t seg_id = GlobalPageId(gp_id).seg_id;

        // TODO: adapt to other kv types
        auto* meta = reinterpret_cast<LeafMeta* >(data);
        for (slot_t i = 0; i < meta->max_leaf_slots; ++i) {
            uint8_t* pair = meta->get_record_ptr(meta->get_offset(i));
            Key_t key = get_key(pair);
            uint8_t* val = r_get_value(pair);
            std::lock_guard<std::mutex> guard(batch->batch_manager.redo_log_mu);
            batch->batch_manager.redo_log_kv[{seg_id, key}] = {val, r_get_writer(pair)};
        }
    }

    void read_redo_log_kv(BatchTxn *batch, MultiLogPage::Reader diff_page) {
        auto redo_logs = diff_page.getRedoLogs();
        g_page_id_t g_page_id = diff_page.getGlobalPageId();
        seg_id_t seg_id = GlobalPageId(g_page_id).seg_id;
        PageMeta* page_meta = page_manager.get_page_from_cache(g_page_id);
        uint32_t num_log = redo_logs.size();
        for (uint32_t log_i = 0; log_i < num_log; ++log_i) {
            auto redo_log = redo_logs[log_i];
            RedoLogReader log_reader(redo_log.asBytes(), page_meta->get_value_size());
            while (log_reader.valid()) {
                if (!log_reader.is_write_entry()) {
                    log_reader.next();
                    continue;
                }
                Key_t key = log_reader.get_key_ref_as<Key_t>();
                std::lock_guard<std::mutex> guard(batch->batch_manager.redo_log_mu);
                batch->batch_manager.redo_log_kv[{seg_id, key}] = 
                    {log_reader.get_value_ptr(), log_reader.get_version()};
                log_reader.next();
            }
        }
    }

    void apply_diff_page(PageMeta *page_meta, MultiLogPage::Reader diff_page) {
        // First apply entire page if stale
        uint8_t* data = page_meta->get_data();
        if (diff_page.hasData()) {
            auto array = diff_page.getData();
            auto byte_array = array.asBytes();
            // memcpy(data, byte_array.begin(), byte_array.size());

            auto* meta = reinterpret_cast<LeafMeta* >((uint8_t *)byte_array.begin());
            auto* cache_meta = reinterpret_cast<LeafMeta* >(data);
            for (slot_t i = 0; i < meta->num_slots; ++i) {
                uint8_t* pair = meta->get_record_ptr(meta->get_offset(i));
                uint8_t* cache_pair = cache_meta->get_record_ptr(cache_meta->get_offset(i));
                r_copy(cache_pair, pair, meta->value_size);
            }
        }

        // Then apply redo logs
        auto redo_logs = diff_page.getRedoLogs();
        uint32_t num_log = redo_logs.size();
        for (uint32_t log_i = 0; log_i < num_log; ++log_i) {
            auto redo_log = redo_logs[log_i];
            if (redo_log.size() == 0) {
                continue;
            }
            RedoLogReader log_reader(redo_log.asBytes(), page_meta->get_value_size());
            while (log_reader.valid()) {
                if (!log_reader.is_write_entry()) {
                    // this is a read
                    log_reader.next();
                    continue;
                }
                uint64_t page_offset = log_reader.get_real_page_offset();
                uint8_t* record_ptr = data + page_offset;
                set_key(record_ptr, log_reader.get_key_ptr());
                set_value(record_ptr, log_reader.get_value_ptr(), log_reader.get_value_size());
                // clear local version
                set_writer(record_ptr, log_reader.get_version());

                // TODO: unref the pointer of the local version
                log_reader.next();
            }
        }
    }

   private:

    void send_get_pages(DbTxn* txn, std::unordered_map<g_page_id_t, std::vector<offset_t>> pages_for_remote_read) {
        std::unordered_map<node_id_t, std::vector<g_page_id_t>> sn_to_page_counts;
        Logger::count(CountType::REMOTE_READS, 1);

        for (auto& pair : pages_for_remote_read) {
            GlobalPageId gp_id(pair.first);
            node_id_t node_id = get_receiver_id(conf.segToSnID(gp_id.seg_id));
            std::vector<g_page_id_t>& g_page_ids = sn_to_page_counts[node_id];
            g_page_ids.push_back(gp_id.g_page_id);
        }

        for (auto& pair : sn_to_page_counts) {
            node_id_t node_id = pair.first;
            BUILD_REQUEST(node_id);

            auto args = request.initData().initGetpagesargs();
            std::vector<g_page_id_t>& g_page_ids = pair.second;
            uint32_t size = g_page_ids.size();
            args.setTxnId(reinterpret_cast<uint64_t>(txn));
            auto pageReqs = args.initPageReqs(size);
            for (uint32_t i = 0; i < size; i++) {
                g_page_id_t g_page_id = g_page_ids[i];
                pageReqs[i].setGlobalPageId(g_page_id);

                std::vector<offset_t>& offsets = pages_for_remote_read.at(g_page_id);
                uint32_t offsets_size = offsets.size();
                auto offsetsReq = pageReqs[i].initOffsets(offsets_size);
                for (uint32_t j = 0; j < offsets_size; j++) {
                    offsetsReq.set(j, offsets[j]);
                }
            }

            rpcServer.send(msgBuilder);
        }
    }

    void send_get_page(seg_id_t seg_id, page_id_t page_id, txn_id_t txn_id = 0) {
        node_id_t node_id = get_receiver_id(conf.segToSnID(seg_id));
        BUILD_REQUEST(node_id);

        auto args = request.initData().initGetpageargs();
        GlobalPageId gp_id(seg_id, page_id);
        args.setGlobalPageId(gp_id.g_page_id);
        args.setTxnId(txn_id);

        rpcServer.send(msgBuilder);
    }

    void send_alloc_page(PageMeta *page_meta) {
        node_id_t node_id = conf.segToSnID(page_meta->gp_id.seg_id);
        BUILD_REQUEST(node_id);
        auto args = request.initData().initSetpageargs();
        auto msg_page = args.initPage();
        msg_page.setGlobalPageId(page_meta->gp_id.g_page_id);

        rpcServer.send(msgBuilder);
    }

   private:
    // transaction related logic
    void send_get_timestamp_req_batch(LargeBatch *lbatch) {
        if (lbatch->sub_batches.size() == 1) {
            send_get_timestamp_req(lbatch->sub_batches[0]);
            return;
        }
        BUILD_REQUEST(ts_node_id);
        auto arg = request.initData().initBatchgettimestampargs();
        arg.setLbatchId(lbatch->id);
        auto args = arg.initGettimestampargs(lbatch->sub_batches.size());
        uint i = 0;

        for (auto batch : lbatch->sub_batches) {
            batch->pack_get_timestamp_req(args[i]);
            batch->timer.start();
            i++;
        }
#ifndef TEST_LOCAL_COMMIT
        rpcServer.send(msgBuilder);
#endif
        // Logger::count(CountType::GETTS_MSG_SIZE, msgBuilder.sizeInWords() * 8);
    }

    void send_get_timestamp_req(BatchTxn *batch) {
        BUILD_REQUEST(ts_node_id);
        auto args = request.initData().initGettimestampargs();
        batch->pack_get_timestamp_req(args);
        batch->timer.start();
#ifndef TEST_LOCAL_COMMIT
        // Logger::count(CountType::GETTS_MSG_SIZE, msgBuilder.sizeInWords() * 8);
        rpcServer.send(msgBuilder);
#endif
    }

    void send_all_prepare_reqs_lbatch(LargeBatch *lbatch, kj::ArrayPtr<capnp::word> lmsg_ptr) {
        if (lbatch->sub_batches.size() == 1) {
            send_all_prepare_reqs(lbatch->sub_batches[0], lmsg_ptr);
            return;
        }

        for (auto batch : lbatch->sub_batches) {
            batch->sending = true;
            batch->msg_count = conf.get_maximal_msg_count();
            // LOG(2) << "prepare_lbatch_start " << std::hex << batch->batch_id << " " << batch->msg_count;
        }
        // static uint8_t __thread buffer[20000000];
        // auto ptr = kj::arrayPtr(reinterpret_cast<capnp::word*>(buffer), 2500000);
        for (node_id_t i = 0; i < conf.numLogicalSN(); i++) {
            LatencyLogger::start(LatencyType::LOCAL_PREPAREMSG);
            node_id_t sn_id = i;
            node_id_t receiver = get_receiver_id(sn_id);

            std::vector<uint32_t> sub_batches_to_send;
            for (uint j = 0; j < lbatch->sub_batches.size(); j++) {
                auto batch = lbatch->sub_batches[j];
                bool skip;
#ifdef TOTAL_ORDER_TS
                skip = false;
#else
                skip = batch->sn_infos[i] == nullptr;
#endif
                if (skip) {
                    batch->msg_count -= conf.REPLICA_COUNT;
                } else {
                    sub_batches_to_send.push_back(j);
#if defined(NO_TS) && !defined(DETERMINISTIC_VALIDATION)
                    if (conf.sn_replication) {
                        batch->msg_count-=2;
                    }
#endif
                }
            }
            if (sub_batches_to_send.size() > 0) {
                // BUILD_REQUEST(receiver);
                BUILD_REQUEST1(receiver, lmsg_ptr);
                auto req = request.initData().initBatchprepareargs();
                req.setLbatchId(lbatch->id);
                auto args = req.initPrepareargs(sub_batches_to_send.size());
                uint32_t args_index = 0;
                for (uint j : sub_batches_to_send) {
                    auto batch = lbatch->sub_batches[j];

#ifdef BREAKDOWN_MSG_SIZE
                    batch->pack_prepare_req(args[args_index], sn_id, &msgBuilder);
#else
                    batch->pack_prepare_req(args[args_index], sn_id);
#endif
                    args_index++;
                }
                LatencyLogger::end(LatencyType::LOCAL_PREPAREMSG);
#ifndef TEST_LOCAL_COMMIT
                LatencyLogger::start(LatencyType::SEND_PREPAREMSG);
                rpcServer.send(msgBuilder);
                send_replicated_msg(receiver, msgBuilder, request);
                LatencyLogger::end(LatencyType::SEND_PREPAREMSG);
                // Logger::count(CountType::PREPARE_MSG_SIZE, msgBuilder.sizeInWords() * 8);
#endif    
            }
        }
        for (auto batch : lbatch->sub_batches) {
            // LOG(2) << "prepare_lbatch " << std::hex << batch->batch_id << " " << batch->msg_count;
            batch->timer.start();
            batch->sending = false;
            batch->cur_phase_success = true;
        }
    }

#ifdef TOTAL_ORDER_TS
    void send_all_prepare_reqs(BatchTxn *batch, kj::ArrayPtr<capnp::word> lmsg_ptr, bool broadcast_all = true) {
#else
    void send_all_prepare_reqs(BatchTxn *batch, kj::ArrayPtr<capnp::word> lmsg_ptr) {
#endif
        // LOG(2) << "send_all_prepare_reqs " << std::hex  << batch->batch_id;
        batch->sending = true;
        batch->msg_count = conf.get_maximal_msg_count();
        for (node_id_t i = 0; i < conf.numLogicalSN(); i++) {
            bool skip;
#ifdef TOTAL_ORDER_TS
            skip = broadcast_all ? false : (batch->sn_infos[i] == nullptr);
#else
            skip = batch->sn_infos[i] == nullptr; 
#endif
            if (skip) {
                batch->msg_count -= conf.REPLICA_COUNT;
                continue;
            } else {
#if defined(NO_TS) && !defined(DETERMINISTIC_VALIDATION)
                if (conf.sn_replication) {
                    batch->msg_count-=2;
                }
#endif
            }
            send_prepare_req(i, batch, lmsg_ptr);
        }

        batch->sending = false;
        batch->cur_phase_success = true;
    }

    void send_prepare_req(node_id_t sn_id, BatchTxn *batch, kj::ArrayPtr<capnp::word> lmsg_ptr) {
        LatencyLogger::start(LatencyType::LOCAL_PREPAREMSG);
        node_id_t receiver = get_receiver_id(sn_id);
        // static uint8_t __thread buffer[20000000];
        // auto ptr = kj::arrayPtr(reinterpret_cast<capnp::word*>(buffer), 2500000);

        BUILD_REQUEST1(receiver, lmsg_ptr);
        // BUILD_REQUEST(receiver);
        auto req = request.initData().initPrepareargs();
#ifdef BREAKDOWN_MSG_SIZE
        if (!batch->is_repaired) {
            batch->pack_prepare_req(req, sn_id, &msgBuilder);
        } else {
            batch->pack_prepare_req(req, sn_id);
            Logger::count(CountType::PREPARE_MSG_SIZE_DELTA, msgBuilder.sizeInWords() * 8);
        }
#else
        batch->pack_prepare_req(req, sn_id);
#endif
        LatencyLogger::end(LatencyType::LOCAL_PREPAREMSG);
        batch->timer.start();
#ifndef TEST_LOCAL_COMMIT
        LatencyLogger::start(LatencyType::SEND_PREPAREMSG);
        rpcServer.send(msgBuilder);
        LatencyLogger::end(LatencyType::SEND_PREPAREMSG);
        // Logger::count(CountType::PREPARE_MSG_SIZE, msgBuilder.sizeInWords() * 8);
        // LOG(2) << "send_prepare: " << std::hex << batch->batch_id << " " << receiver;
        send_replicated_msg(receiver, msgBuilder, request);
#endif
    }

    void send_all_prepare_reqs_repair(BatchTxn *batch, kj::ArrayPtr<capnp::word> lmsg_ptr) {
        // LOG(2) << "send_all_prepare_reqs " << std::hex  << batch->batch_id;
        batch->sending = true;
        batch->msg_count = conf.get_maximal_msg_count();
        BUILD_REQUEST1(0, lmsg_ptr);
        auto req = request.initData().initPrepareargs();
        batch->pack_prepare_req_repair(req);
#ifdef BREAKDOWN_MSG_SIZE
        Logger::count(CountType::PREPARE_MSG_SIZE_DELTA, msgBuilder.sizeInWords() * 8);
#endif

        for (node_id_t i = 0; i < conf.numLogicalSN(); i++) {
            auto sn_info = batch->sn_infos[i];
            if (sn_info == nullptr) {
                batch->msg_count -= conf.REPLICA_COUNT;
                continue;
            } else {
#if defined(NO_TS) && !defined(DETERMINISTIC_VALIDATION)
                if (conf.sn_replication) {
                    batch->msg_count-=2;
                }
#endif
            }
            int receiver = get_receiver_id(i);
            req.setPrimarySnId(i);
            request.setReceiver(receiver);
            rpcServer.send(msgBuilder);
            send_replicated_msg(receiver, msgBuilder, request);
        }
        batch->timer.start();
        batch->sending = false;
        batch->cur_phase_success = true;
    }

    int get_receiver_id(node_id_t sn_id) {
#ifndef NEW_REPLICATION
        sn_id *= conf.REPLICA_COUNT;
#endif
        return sn_id;
    }

    void send_replicated_msg(node_id_t receiver,
                            ::capnp::MallocMessageBuilder& msgBuilder, 
                            RpcMessage::Builder& request) {
#if !defined(NO_TS) || defined(DETERMINISTIC_VALIDATION)
        if (conf.sn_replication) {
            for (uint32_t i = 1; i < conf.REPLICA_COUNT; ++i) {
                request.setReceiver((receiver + i) % conf.numSN());
                rpcServer.send(msgBuilder);
            }
        }
#endif
    }

    void send_all_commit_reqs(BatchTxn *batch) {
        batch->msg_count = conf.get_maximal_msg_count();
        for (node_id_t i = 0; i < conf.numLogicalSN(); i++) {
            auto sn_info = batch->sn_infos[i];
            if (sn_info == nullptr) {
                batch->msg_count -= conf.REPLICA_COUNT;
            } else {
#if defined(NO_TS) && !defined(DETERMINISTIC_VALIDATION)
                if (conf.sn_replication) {
                    batch->msg_count-=2;
                }
#endif
            }
        }
        for (node_id_t i = 0; i < conf.numLogicalSN(); i++) {
            auto sn_info = batch->sn_infos[i];
            if (sn_info == nullptr) { 
                continue;
            }
            send_commit_req(i, *batch);
        }
        batch->timer.start();
        batch->cur_phase_success = true;
    }

    void send_commit_req(node_id_t sn_id, BatchTxn &batch) {
        node_id_t receiver = get_receiver_id(sn_id);
        BUILD_REQUEST(receiver);
        auto req = request.initData().initCommitargs();
        batch.pack_commit_req(req, sn_id);
#ifndef TEST_LOCAL_COMMIT
        rpcServer.send(msgBuilder);
        // LOG(2) << "send_commit: " << std::hex << batch.batch_id << " " << receiver;
        send_replicated_msg(receiver, msgBuilder, request);
#endif
    }
};

class DatabaseRpcServer : public RpcServerBase {
    friend class DatabaseWorker;
    volatile bool running;
    bool is_primary_db;
    DatabaseNode db;
    BenchmarkInterface &benchmark;
    uint32_t num_threads;
    std::vector<DatabaseWorker *> worker_threads;

   public:
    DatabaseRpcServer(const Configuration &conf, BenchmarkInterface &benchmark, uint32_t num_pages,
                      uint32_t batching_size = 100, uint32_t split_batch_num = 1, uint32_t cache_miss_ratio = 0)
        : RpcServerBase(conf),
          running(false),
          is_primary_db(conf.get_my_id() == conf.get_primary_db_node()->node_id),
          db(conf, benchmark, *this, num_pages, batching_size, split_batch_num, cache_miss_ratio),
          benchmark(benchmark),
          num_threads(conf.get_my_node().num_threads) {}

    ~DatabaseRpcServer() {}

    void run() {
        init_benchmark();

        for (thread_id_t thread_id = 0; thread_id < num_threads; ++thread_id) {
            worker_threads.emplace_back(new DatabaseWorker(conf, thread_id, this));
            CPUBinder::bind(worker_threads[thread_id]->worker_thread.native_handle(), thread_id);
        }
        start_benchmark();
        std::time_t tt = std::time(nullptr);
        LOG(2) << "benchmark started " << std::asctime(std::localtime(&tt));

        end_benchmark();
        for (thread_id_t thread_id = 0; thread_id < num_threads; ++thread_id) {
            get_specific_event_channel(thread_id)->push_task_event(DbTaskType::STOP, NULL_TXN_ID);
        }
        for (thread_id_t thread_id = 0; thread_id < num_threads; ++thread_id) {
            worker_threads[thread_id]->worker_thread.join();
            delete worker_threads[thread_id];
        }
    }

   private:
    void init_benchmark() {
        // As we have synchronized requests (get_page) in db.init_benchmark()
        // To avoid primary db's sync request(in start_benchmark()) confusing other dbs'
        // get_page
        // response We let: 1. other dbs init benchmark; 2. other dbs send sync(INIT_OK) to
        // primary
        // db
        // 3. primary db inits benchmark
        db.init_benchmark();
        if (!is_primary_db) {
            node_id_t primary_db_id = conf.get_primary_db_node()->node_id;
            send_sync_req(primary_db_id, ServerStatus::INIT_OK);
        } else {
            uint64_t msg_count = conf.numDB() - 1 + conf.numSN();
            while (msg_count != 0) {
                ServerStatus status = recv_sync();
                ASSERT(status == ServerStatus::INIT_OK);
                --msg_count;
            }
        }
    }
    void start_benchmark() {
        // sync the start time of all the databases
        LOG(2) << "Before Start";
        if (is_primary_db) {
            // the primary database runs in foreground, send start msg to the other servers
            sync_databases(ServerStatus::START);
            sync_storage_nodes(ServerStatus::START, true);
        } else {
            // the other databases receive the msg sent by primary database
            ServerStatus status = recv_sync();
            ASSERT(status == ServerStatus::START);
        }
        LOG(2) << "After Start";

#ifdef RPC_CLIENTS
        if (is_primary_db) {
            send_sync_req(conf.get_client_node_id(0), ServerStatus::START);
        }
#else
        // initialize: each client 1 ongoing transaction
        for (uint32_t i = 0; i < benchmark.get_num_clients(); i++) {
            get_specific_event_channel(i % num_threads)->push_task_event(NEW_TXN, NULL_TXN_ID);
        }
#endif
        // notify worker threads to start
        running = true;
    }

    void end_benchmark() {
        BenchmarkTimer timer;
#ifdef CHECK_CONTENTION
        start_benchmarking_flag = true;
        uint32_t sec = 0;
        while (true) {
            sec++;
            thread_sleep(1);
            Profile p = aggregate_profile();
            clear_profile();
            LOG(4) << "second " << sec << " throughput is " << p.commit_times
                   << " txn repair ratio is " << (double)p.repair_times / p.commit_times
                   << " batch repair ratio is "
                   << (double)p.batch_repair_times / p.batch_commit_times;
        }
#endif
        thread_sleep(5);
        timer.start();
        start_benchmarking_flag = true;
        LOG(3) << "start bench";
        uint64_t last_commits = 0;
        uint64_t last_aborts = 0;
        uint64_t last_retries = 0;
        uint64_t last_repairs = 0;
        uint64_t last_sentSizes = 0;
        for (uint i = 0; i < benchmark.get_time_duration(); i++) {
            thread_sleep(1);
            uint64_t commits = 0;
            uint64_t aborts = 0;
            uint64_t retries = 0;
            uint64_t repairs = 0;
            uint64_t sentSizes = 0;
            for (auto t : worker_threads) {
                commits += t->profile.commit_times;
                aborts += t->profile.remote_abort_times;
                retries += t->profile.local_abort_times;
                repairs += t->profile.num_repairs();
                sentSizes += t->sentSize;
            }
            uint64_t commit_tput = commits - last_commits;
            uint64_t abort_tput = aborts - last_aborts;
            uint64_t retry_tput = retries - last_retries;
            uint64_t repair_tput = repairs - last_repairs;
            uint64_t sentSizes_tput = sentSizes - last_sentSizes;
            if (commit_tput + abort_tput == 0) {
                db.ongoing_batches.print();
                // for (auto t : worker_threads) {
                //     LOG(2) << t->my_event_channel->size();
                // }
                ASSERT(false) << "no progress!";
            }
            LOG(2) << "commits/s: " << commit_tput << ", aborts/s: " << abort_tput
                   << ", local aborts/s: " << retry_tput << ", repairs/s: " << repair_tput
                   << ", networkTput/s: " << sentSizes_tput;;
            benchmark.print_debug_msg();
            last_commits = commits;
            last_aborts = aborts;
            last_retries = retries;
            last_repairs = repairs;
            last_sentSizes = sentSizes;
        }
        start_benchmarking_flag = false;
        for (auto t : worker_threads) {
            t->profile.time_duration = timer.double_passed_sec();
            LOG(2) << "worker[" << t->worker_id << "]: " << t->profile;
        }
        // stop the clients
#ifdef RPC_CLIENTS
        send_sync_req(conf.get_client_node_id(0), ServerStatus::END);
#endif

        thread_sleep(1);
        if (is_primary_db) {
            // the primary control the test duration of our evalution benchmark
            // after the duration, it sens end msg to the other databases

            // End myself first
            running = false;
            sync_databases(ServerStatus::END, false);
        }

        // wait for benchmark ending
        running = false;

        std::time_t tt = std::time(nullptr);
        LOG(2) << "benchmark ended" << std::asctime(std::localtime(&tt));
        // for (auto t : worker_threads) {
        //     t->worker_thread.detach();
        // }

        // all database send profiles to primary database
        node_id_t primary_db_id = conf.get_primary_db_node()->node_id;
        send_report(primary_db_id);
        LOG(2) << "report sent";

        if (is_primary_db) {
            // send END message to storage node
            sync_storage_nodes(ServerStatus::END);

            // receive REPORT from other databases
            double throughput = 0.0;
            std::vector<uint64_t> correctness_data;
            uint64_t run_times = 0, db_repair_times = 0, sn_repair_times = 0,
                     db_batch_repair_times = 0, sn_batch_repair_times = 0, batch_abort_times = 0,
                     batch_commit_times = 0, commit_times = 0, remote_times = 0, local_times = 0;
            // uint64_t msg_count = conf.numDB() + conf.numSN();
            uint64_t msg_count = conf.numDB();
            while (msg_count != 0) {
                auto event = logistics_channel.pop();
                if (event.event_type == Event::RPC) {
                    auto &zmq_msg = *reinterpret_cast<MsgBuffer *>(event.ptr);
                    BUILD_MESSAGE_FROM_BUFFER(zmq_msg, msg);
                    if (msg.getData().hasReportargs()) {
                        auto report = msg.getData().getReportargs();
                        run_times += report.getRunTimes();
                        commit_times += report.getCommitTimes();
                        remote_times += report.getRemoteAbortTimes();
                        LOG(2) << "commit_times[" << msg.getSender()
                               << "]: " << report.getCommitTimes();
                        local_times += report.getLocalAbortTimes();
                        throughput += report.getThroughput();
                        db_repair_times += report.getDbRepairTimes();
                        sn_repair_times += report.getSnRepairTimes();
                        db_batch_repair_times += report.getDbBatchRepairTimes();
                        sn_batch_repair_times += report.getSnBatchRepairTimes();
                        batch_abort_times += report.getBatchAbortTimes();
                        batch_commit_times += report.getBatchCommitTimes();
                        --msg_count;
                    } else if (msg.getData().hasSyncargs()) {
                        auto sync = msg.getData().getSyncargs();
                        correctness_data.push_back(sync.getCount());
                        --msg_count;
                    } else {
                        ASSERT(false) << "receive a msg: " << PRINT_MESSAGE(msg);
                    }
                    // LOG(2) << VAR2(msg.getSender(), " ") << VAR(msg_count);
                }
            }
            uint64_t repair_times = db_repair_times + sn_repair_times;
            uint64_t batch_repair_times = db_batch_repair_times + sn_batch_repair_times;
            LOG(4) << "Total " << VAR2(run_times, "");
            LOG(4) << "Total " << VAR2(commit_times, "");
            LOG(4) << "Total " << VAR2(repair_times, " ")
                   << "txn_repair_ratio: " << (double)repair_times / commit_times;
            LOG(4) << "Total remote_abort_times:" << remote_times
                   << " remote_abort_ratio: " << (double)remote_times / commit_times;
            LOG(4) << "Total local_abort_times:" << local_times
                   << " local_abort_ratio: " << (double)local_times / commit_times;
            LOG(4) << "batch_commit_times:" << batch_commit_times
                   << " batch_repair_times: " << batch_repair_times
                   << " batch_repair_ratio: " << (double)batch_repair_times / batch_commit_times;
            LOG(4) << "batch_abort_times: " << batch_abort_times
                   << " batch_abort_ratio: " << (double)batch_abort_times / batch_commit_times;
            LOG(4) << "db_repair_ratio: " << (double)db_repair_times / repair_times;
            LOG(4) << "db_batch_repair_ratio: "
                   << (double)db_batch_repair_times / batch_repair_times;

            LOG(4) << "Total " << VAR2(throughput, " txns/s");
            LatencyLogger::report();
            Logger::report();
            TxnLatencyReporter::report();

            benchmark.check_correctness_reduce(correctness_data);
        }

        db.end_benchmark();
        // ASSERT(false);
    }

   private:
    // sending messages
    void sync_storage_nodes(ServerStatus status,bool wait = false) {
        uint64_t num_sn = conf.numSN();
        for (uint32_t sn_i = 0; sn_i < num_sn; ++sn_i) {
            node_id_t other_sn_id = conf.get_sn_node(sn_i)->node_id;
            // LOG(2) << "send sync request to SN: " << other_sn_id << " " << status;
            send_sync_req(other_sn_id, status);
            // if(wait) {
            //     thread_sleep_micros(100000);
            // }
        }
    }

    void sync_databases(ServerStatus status, bool include_myself = false) {
        uint64_t num_db = conf.numDB();
        for (uint32_t db_i = 0; db_i < num_db; ++db_i) {
            node_id_t other_db_id = conf.get_db_node(db_i)->node_id;
            if (other_db_id != conf.get_my_id() || include_myself) {
                send_sync_req(other_db_id, status);
            }
        }
    }


    Profile aggregate_profile() {
        Profile p;
        for (auto &worker : worker_threads) {
            auto &profile = worker->profile;
            p.commit_times += profile.commit_times;
            p.run_times += profile.run_times;
            p.db_repair_times += profile.db_repair_times;
            p.sn_repair_times += profile.sn_repair_times;
            p.batch_commit_times += profile.batch_commit_times;
            p.db_batch_repair_times += profile.db_batch_repair_times;
            p.sn_batch_repair_times += profile.sn_batch_repair_times;
            p.batch_abort_times += profile.batch_abort_times;
            p.remote_abort_times += profile.remote_abort_times;
            p.local_abort_times += profile.local_abort_times;
            p.time_duration += profile.time_duration;
        }
        p.time_duration /= worker_threads.size();
        return p;
    }

    void clear_profile() {
        for (auto &worker : worker_threads) {
            worker->profile.clear();
        }
    }

    void send_report(node_id_t node_id) {
        BUILD_REQUEST(node_id);
        auto report = request.initData().initReportargs();
        Profile profile = aggregate_profile();
        report.setRunTimes(profile.run_times);
        report.setCommitTimes(profile.commit_times);
        report.setDbRepairTimes(profile.db_repair_times);
        report.setSnRepairTimes(profile.sn_repair_times);
        report.setBatchCommitTimes(profile.batch_commit_times);
        report.setDbBatchRepairTimes(profile.db_batch_repair_times);
        report.setSnBatchRepairTimes(profile.sn_batch_repair_times);
        report.setBatchAbortTimes(profile.batch_abort_times);
        report.setRemoteAbortTimes(profile.remote_abort_times);
        report.setLocalAbortTimes(profile.local_abort_times);
        report.setThroughput(profile.commit_times / profile.time_duration);
        send(msgBuilder);
    }
};

DatabaseWorker::DatabaseWorker(const Configuration &conf, thread_id_t worker_id,
                               DatabaseRpcServer *server)
    : conf(conf),
      server(server),
      worker_id(worker_id),
      my_event_channel(server->event_channels[worker_id]),
      worker_thread(&DatabaseWorker::worker_mainloop, this) {
    // A Wrapper for txn_id_t, used to encode & decode txn_id.
    // The txn_id count start from 1 to reserve NULL_TXN_ID(0) for specific usage in Database
    // and StorageNode.
    TxnIdWrapper wrapper(conf.get_my_id(), worker_id, 1);
    next_txn_id = wrapper.txn_id;
    lmsg_buf = new uint8_t[200000000];
    lmsg_ptr = kj::arrayPtr(reinterpret_cast<capnp::word*>(lmsg_buf), 25000000);
}
DatabaseWorker::~DatabaseWorker() {
    delete [] lmsg_buf;
}

inline DbTxn *DatabaseWorker::launch_new_txn(DbClient *c) {
    txn_id_t txn_id = next_txn_id++;
    auto &db = server->db;
    DbTxn *txn = db.new_txn(txn_id, c);
    if (txn != nullptr) {
        txn->client = c;
        txn->txn_seed = c->txn_seed;
    }
    return txn;
}

inline DbTxn *DatabaseWorker::retry_txn(DbTxn *txn) {
    txn->set_txn_id(next_txn_id++);
    return txn;
}

void DatabaseWorker::handle_rpc(Event& event) {
    LatencyLogger::start(LatencyType::RPCEVENT);
    MsgBuffer &zmq_msg = *reinterpret_cast<MsgBuffer *>(event.ptr);
    BUILD_MESSAGE_FROM_BUFFER(zmq_msg, msg);
    // LOG(2) << PRINT_MESSAGE(msg);
    switch (msg.getData().which()) {
        case RpcMessage::Data::GETPAGERESPONSE:
            server->db.recv_get_page_resp(msg.getData().getGetpageresponse(), *this);
            break;
        case RpcMessage::Data::GETPAGESRESPONSE:
            server->db.recv_get_pages_resp(msg.getData().getGetpagesresponse(), *this);
            break;
        case RpcMessage::Data::GETTIMESTAMPRESPONSE:
            server->db.recv_get_timestamp_resp(msg.getData().getGettimestampresponse(), *this);
            break;
        case RpcMessage::Data::BATCHGETTIMESTAMPRESPONSE:
            LatencyLogger::start(LatencyType::RECV_GETTS);
            server->db.recv_get_timestamp_resp_batch(msg.getData().getBatchgettimestampresponse(), *this);
            LatencyLogger::end(LatencyType::RECV_GETTS);
            break;
        case RpcMessage::Data::PREPARERESPONSE:
            LatencyLogger::start(LatencyType::RECV_PREPARE);
            server->db.recv_prepare_resp(msg.getSender(), msg.getData().getPrepareresponse(), zmq_msg, *this);
            event.event_type = NONE;
            LatencyLogger::end(LatencyType::RECV_PREPARE);
            break;
        case RpcMessage::Data::COMMITRESPONSE:
            LatencyLogger::start(LatencyType::RECV_COMMIT);
            server->db.recv_commit_resp(msg.getSender(), msg.getData().getCommitresponse(), *this);
            LatencyLogger::end(LatencyType::RECV_COMMIT);
            break;
        case RpcMessage::Data::CLIENTARGS:
            server->db.recv_client_args(msg.getData().getClientargs(), *this);
            break;
        default:
            ASSERT(false) << "Database " << msg.getReceiver() << ": Unknown RPC "
                            << msg.getData().which() << " recevied" << PRINT_MESSAGE(msg);
    }
    LatencyLogger::end(LatencyType::RPCEVENT);
}

void DatabaseWorker::handle_task(Event& event) {
    LatencyLogger::start(LatencyType::TASKEVENT);
    DbTxn *txn = nullptr;
    switch (event.task_type) {
        case NEW_TXN:
            LatencyLogger::start(LatencyType::LAUNCH_NEW_TXN);
#ifndef RPC_CLIENTS
            if (event.value == 0) {
                event.value = (uint64_t) new DbClient(0, 0, thread_rand.randint(0, 99));
            }
#endif
            txn = launch_new_txn((DbClient *)event.value);
            if (txn == nullptr) {
                my_event_channel->push_task_event(NEW_TXN, event.value);
            }
            LatencyLogger::end(LatencyType::LAUNCH_NEW_TXN);
            break;
#ifdef DETERMINISTIC_VALIDATION
        case SEND_PREPARE:
            server->db.send_prepare(reinterpret_cast<LargeBatch *>(event.value), *this);
            break;
#endif
        case RETRY_TXN:
            txn = retry_txn(reinterpret_cast<DbTxn *>(event.value));
            break;
        case GC_BATCH:
            LatencyLogger::start(LatencyType::LOCAL_INDEPENDENT_GC);
            server->db.handle_own_txns(
                reinterpret_cast<std::vector<DbTxn *> *>(event.value),
                HandleOwnTxnTaskType::GC, *this);
            LatencyLogger::end(LatencyType::LOCAL_INDEPENDENT_GC);
            break;
#ifdef LOCK_FAIL_BLOCK
        case EMPTY:
            if (worker_id != event.value) {
                server->get_specific_event_channel(event.value)->push_task_event(DbTaskType::EMPTY, event.value);
            } else {
                latest_batch_num += 1;
            }
            txn = nullptr;
            break;
#endif
        case STOP:
            im_running = false;
            break;
        default:
            ASSERT(false);
    }
    LatencyLogger::end(LatencyType::TASKEVENT);
#ifdef LOCK_FAIL_BLOCK
    server->db.resume_txn(*this);
#endif
    if (txn != nullptr) {
        LatencyLogger::start(LatencyType::EXEEVENT);
        server->db.execute_txn(txn, *this);
        LatencyLogger::end(LatencyType::EXEEVENT);
    }
    sentSize = sent_size;
}


void DatabaseWorker::worker_mainloop() {
    thread_rand.set_seed(worker_id);
    socket_t_id = worker_id;

    RdtscTimer timer;
    while (!server->running) {
        COMPILER_MEMORY_FENCE();
    }

    LOG(2) << "worker start: " << socket_t_id;

    current_worker = this;
    LatencyLogger::worker_register(worker_id);
    TxnLatencyReporter::worker_register(worker_id);

    BenchmarkTimer total_timer;
    if (worker_id == 0) {
        total_timer.start();
    }

    while (server->running && im_running) {
#ifdef TEST_SINGLE_LOCAL_COMMIT
        LatencyLogger::start(LatencyType::LAUNCH_NEW_TXN);
        DbTxn *txn = launch_new_txn(new DbClient(0, 0, thread_rand.randint(0, 99)));
        LatencyLogger::end(LatencyType::LAUNCH_NEW_TXN);
        server->db.execute_txn(txn, *this);
        continue;
#endif
        // LatencyLogger::start(LatencyType::WORKER_TOTAL);
        LatencyLogger::start(LatencyType::POPEVENT);
        auto event = server->pop_event(worker_id);
        LatencyLogger::end(LatencyType::POPEVENT);

        switch (event.event_type) {
            case Event::RPC:
                handle_rpc(event);
                break;
            case Event::TASK: {
                handle_task(event);
                break;
            }
            case Event::NONE:
                break;
            default:
                ASSERT(false);
        }
        // LatencyLogger::end(LatencyType::WORKER_TOTAL);
    }
    if (worker_id == 0) {
        LOG(2) << "total_timer: " << total_timer.passed_nsec();
    }
    clear_batch_txn();
    clear_large_batch();
}