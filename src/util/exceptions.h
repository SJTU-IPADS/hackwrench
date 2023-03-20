#pragma once
#include <exception>

struct TxnRepairException : public std::exception {
    const char *what() const throw() { return "Transaction Repair Exception"; }
};

struct TxnAbortException : public TxnRepairException {
    const char *what() const throw() { return "The txn has aborted"; }
};

struct TxnRangeLockAbortException : public TxnAbortException {
    const char *what() const throw() { return "The record written is protected by range lock"; }
};

struct WorkloadException : public std::exception {
    const char *what() const throw() { return "Workload Exception"; }
};

struct WorkloadNoClientException : public WorkloadException {
    const char *what() const throw() { return "There is no client left"; }
};

struct TxnNoTsException : public std::exception {
    const char *what() const throw() { return "txn doesn't have timestamp yet"; }
};

struct BatchLockException : public TxnRepairException {
    BatchLockException(batch_id_t b_id) : b_id(b_id) {}
    batch_id_t b_id;

    const char *what() const throw() { return "Batch lock fails"; }
};

struct RecordLockException : public TxnRepairException {
    const char *what() const throw() { return "Record lock fails"; }
};