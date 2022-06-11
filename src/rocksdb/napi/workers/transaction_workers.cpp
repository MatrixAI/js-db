#define NAPI_VERSION 3

#include "transaction_workers.h"

#include <node/node_api.h>
#include <rocksdb/slice.h>

#include "../worker.h"
#include "../transaction.h"
#include "../iterator.h"
#include "../snapshot.h"
#include "../utils.h"

/**
 * Transaction commit
 */

TransactionCommitWorker::TransactionCommitWorker(napi_env env,
                                                 Transaction* tran,
                                                 napi_value callback)
    : BaseWorker(env, tran, callback, "rocksdb.transaction.commit") {}

TransactionCommitWorker::~TransactionCommitWorker() = default;

void TransactionCommitWorker::DoExecute() { SetStatus(transaction_->Commit()); }

void TransactionCommitWorker::DoFinally(napi_env env) {
  transaction_->Detach(env);
  BaseWorker::DoFinally(env);
}

/**
 * Transaction rollback
 */

TransactionRollbackWorker::TransactionRollbackWorker(napi_env env,
                                                     Transaction* tran,
                                                     napi_value callback)
    : BaseWorker(env, tran, callback, "rocksdb.transaction.rollback") {}

TransactionRollbackWorker::~TransactionRollbackWorker() = default;

void TransactionRollbackWorker::DoExecute() {
  SetStatus(transaction_->Rollback());
}

void TransactionRollbackWorker::DoFinally(napi_env env) {
  transaction_->Detach(env);
  BaseWorker::DoFinally(env);
}

/**
 * Transaction get
 */

TransactionGetWorker::TransactionGetWorker(napi_env env, Transaction* tran,
                                           napi_value callback,
                                           rocksdb::Slice key,
                                           const bool asBuffer,
                                           const bool fillCache,
                                           const TransactionSnapshot* snapshot)
    : PriorityWorker(env, tran, callback, "rocksdb.transaction.get"),
      key_(key),
      asBuffer_(asBuffer) {
  options_.fill_cache = fillCache;
  if (snapshot != nullptr) options_.snapshot = snapshot->snapshot();
}

TransactionGetWorker::~TransactionGetWorker() { DisposeSliceBuffer(key_); }

void TransactionGetWorker::DoExecute() {
  SetStatus(transaction_->Get(options_, key_, value_));
}

void TransactionGetWorker::HandleOKCallback(napi_env env, napi_value callback) {
  napi_value argv[2];
  napi_get_null(env, &argv[0]);
  Entry::Convert(env, &value_, asBuffer_, &argv[1]);
  CallFunction(env, callback, 2, argv);
}

/**
 * Transaction get for update
 */

TransactionGetForUpdateWorker::TransactionGetForUpdateWorker(
    napi_env env, Transaction* tran, napi_value callback, rocksdb::Slice key,
    const bool asBuffer, const bool fillCache,
    const TransactionSnapshot* snapshot, const bool exclusive)
    : PriorityWorker(env, tran, callback, "rocksdb.transaction.get_for_update"),
      key_(key),
      asBuffer_(asBuffer),
      exclusive_(exclusive) {
  options_.fill_cache = fillCache;
  if (snapshot != nullptr) options_.snapshot = snapshot->snapshot();
}

TransactionGetForUpdateWorker::~TransactionGetForUpdateWorker() {
  DisposeSliceBuffer(key_);
}

void TransactionGetForUpdateWorker::DoExecute() {
  SetStatus(transaction_->GetForUpdate(options_, key_, value_, exclusive_));
}

void TransactionGetForUpdateWorker::HandleOKCallback(napi_env env,
                                                     napi_value callback) {
  napi_value argv[2];
  napi_get_null(env, &argv[0]);
  Entry::Convert(env, &value_, asBuffer_, &argv[1]);
  CallFunction(env, callback, 2, argv);
}

/**
 * Transaction put
 */

TransactionPutWorker::TransactionPutWorker(napi_env env, Transaction* tran,
                                           napi_value callback,
                                           rocksdb::Slice key,
                                           rocksdb::Slice value)
    : PriorityWorker(env, tran, callback, "rocksdb.transaction.put"),
      key_(key),
      value_(value) {}

TransactionPutWorker::~TransactionPutWorker() {
  DisposeSliceBuffer(key_);
  DisposeSliceBuffer(value_);
}

void TransactionPutWorker::DoExecute() {
  SetStatus(transaction_->Put(key_, value_));
}

/**
 * Transaction del
 */

TransactionDelWorker::TransactionDelWorker(napi_env env, Transaction* tran,
                                           napi_value callback,
                                           rocksdb::Slice key)
    : PriorityWorker(env, tran, callback, "rocksdb.transaction.del"),
      key_(key) {}

TransactionDelWorker::~TransactionDelWorker() { DisposeSliceBuffer(key_); }

void TransactionDelWorker::DoExecute() { SetStatus(transaction_->Del(key_)); }
