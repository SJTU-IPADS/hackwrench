@0xcf5c69b17b3dd15f;

    # ------ interface for storage node ------

    struct Dependency {
        timestamp @0 : UInt64;
        numReads @1 : UInt32;
    }

    struct Page {
        globalPageId @0 :UInt64;
        redoLog @1 : Data;
    }

    struct PageReq {
        globalPageId @0 :UInt64;
        offsets @1 :List(UInt32);
    }

    struct MultiLogPage {
        globalPageId @0 :UInt64;
        cts @1 :UInt64;
        data @2 :Data;
        redoLogs @3 : List(Data);
    }

    struct Seg {
        segId @0 : UInt32;
        dep @1 : Dependency;

        readPages @2 :List(Page);
        writePages @3 :List(Page);
    }

    struct ReadSet {
        offset @0 :UInt32;
        writer @1 :UInt64;
    }

    struct TxnInput {
        txnId @0 :UInt64;
        input @1 :Data;
        txnType @2 :UInt32;
    }

    struct TxnReadSet {
        readSet @0 :List(ReadSet);
        txnI @1 :UInt32;
    }

    struct GetPageArgs {
        globalPageId @0 :UInt64;
        txnId @1 :UInt64;
    }

    struct GetPageResponse {
        ok @0 :Bool;
        page @1 : MultiLogPage;
        txnId @2 :UInt64;
    }

    struct GetPagesArgs {
        pageReqs @0 : List(PageReq);
        txnId @1 :UInt64;
    }

    struct GetPagesResponse {
        pages @0 : List(Page);
        txnId @1 :UInt64;
    }

    struct SetPageArgs {
        page @0 :Page;
    }

    struct SetPageResponse {
        ok @0 :Bool;
    }

    struct PrepareArgs {
        txnId @0 :UInt64;
        segments @1 :List(Seg);
        txns @2 :List(TxnReadSet);
        txnInputs @3 : List(TxnInput);
        timestamps @4 : List(Dependency);
        primarySnId @5 :UInt64;
        seq @6 :UInt64;
        fastPathEnabled @7 : Bool;
    }

    struct PrepareResponse {
        ok @0 :Bool;
        txnId @1 :UInt64;
        primarySnId @2 :UInt64;
        diff @3 :List(MultiLogPage);
    }

    struct CommitArgs {
        txnId @0 :UInt64;
        primarySnId @1 :UInt64;
        commit @2 :Bool; # false means abort
    }

    struct CommitResponse {
        ok @0 :Bool;
        isBroadCast @1 :Bool;
        txnId @2 :UInt64;
        primarySnId @3 :UInt64;
        diff @4 :List(MultiLogPage);
        repairedTxns @5 :List(UInt32);
    }

    # ------- interface for time server ------
    struct GetTimestampArgs {
        rwSegIds @0 :List(UInt32); # the 31th bit indicates whether this is write segment
        txnId @1 :UInt64;
    }

    struct GetTimestampResponse {
        ok @0 :Bool;
        deps @1 :List(Dependency);
        txnId @2 :UInt64;
        seq @3 :UInt64;
    }

    # ------- interface for server synchronization ------

    struct SyncArgs {
        status @0 :UInt32;
        count @1 :UInt64;
    }

    struct ReportArgs {
        runTimes @0 :UInt64;
        commitTimes @1 :UInt64;
        dbRepairTimes @2 :UInt64;
        snRepairTimes @3 :UInt64;
        batchCommitTimes @4 :UInt64;
        dbBatchRepairTimes @5 :UInt64;
        snBatchRepairTimes @6 :UInt64;
        batchAbortTimes @7 :UInt64;
        remoteAbortTimes @8 :UInt64;
        localAbortTimes @9 :UInt64;
        throughput @10 :Float64;
    }
    
    struct ClientArgs {
        clientId @0 :UInt64;
        txnType @1 :UInt32;
        data @2 :UInt64;
    }

    struct ClientResponse {
        clientIds @0 :List(UInt64);
        success @1 :Bool;
        data @2 :Data;
    }

    struct BatchPrepareArgs {
        prepareargs @0 :List(PrepareArgs);
        lbatchId @1 :UInt64;
    }
    
    struct BatchCommitArgs {
        commitargs @0 :List(CommitArgs);
        lbatchId @1 :UInt64;
    }

    struct BatchGetTimestampArgs {
        gettimestampargs @0 :List(GetTimestampArgs);
        lbatchId @1 :UInt64;
    }

    struct BatchGetTimestampResponse {
        gettimestampresponse @0 :List(GetTimestampResponse);
        lbatchId @1 :UInt64;
    }


    # ------ all messages in one struct ------
    struct RpcMessage {
        isReply @0 :Bool;
        receiver @1 :UInt32;
        sender @2 :UInt32;
        epoch @3 :UInt64; # an unique id to identify a conversation

        data :union {
            gettimestampargs @4 :GetTimestampArgs;
            gettimestampresponse @5 :GetTimestampResponse;
            getpageargs @6 :GetPageArgs;
            getpageresponse @7 :GetPageResponse;
            setpageargs @8 :SetPageArgs;
            setpageresponse @9 :SetPageResponse;
            prepareargs @10 :PrepareArgs;
            prepareresponse @11 :PrepareResponse;
            commitargs @12 :CommitArgs;
            commitresponse @13 :CommitResponse;
            syncargs @14 :SyncArgs;
            reportargs @15 :ReportArgs;
            clientargs @16 :ClientArgs;
            clientresponse @17 :ClientResponse;
            batchprepareargs @18 :BatchPrepareArgs;
            batchcommitargs @19 :BatchCommitArgs;
            batchgettimestampargs @20 :BatchGetTimestampArgs;
            batchgettimestampresponse @21 :BatchGetTimestampResponse;
            getpagesargs @22 :GetPagesArgs;
            getpagesresponse @23 :GetPagesResponse;
        }
        # version @16 :Text;
    }
