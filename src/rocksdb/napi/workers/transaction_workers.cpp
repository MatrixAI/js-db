#define NAPI_VERSION 3

#include "transaction_workers.h"
#include <node_api.h>
#include <rocksdb/slice.h>
#include "../worker.h"
#include "../transaction.h"
#include "../iterator.h"
#include "../utils.h"

/**
 * Transaction commit
 */

TransactionCommitWorker::TransactionCommitWorker (
  napi_env env,
  Transaction* tran,
  napi_value callback
) :
  PriorityWorker(env, tran->database_, callback, "rocksdb.transaction.commit"),
  tran_(tran)
  {}

TransactionCommitWorker::~TransactionCommitWorker() {}

void TransactionCommitWorker::DoExecute () {
  SetStatus(tran_->Commit());
}

void TransactionCommitWorker::DoFinally (napi_env env) {
  tran_->Detach(env);
  PriorityWorker::DoFinally(env);
}

/**
 * Transaction rollback
 */

TransactionRollbackWorker::TransactionRollbackWorker(
  napi_env env,
  Transaction* tran,
  napi_value callback
) :
  PriorityWorker(env, tran->database_, callback, "rocksdb.transaction.rollback"),
  tran_(tran)
  {}

TransactionRollbackWorker::~TransactionRollbackWorker() {}

void TransactionRollbackWorker::DoExecute () {
  SetStatus(tran_->Rollback());
}

void TransactionRollbackWorker::DoFinally (napi_env env) {
  tran_->Detach(env);
  PriorityWorker::DoFinally(env);
}

/**
 * Transaction get
 */

TransactionGetWorker::TransactionGetWorker(
  napi_env env,
  Transaction* tran,
  napi_value callback,
  rocksdb::Slice key,
  const bool asBuffer,
  const bool fillCache
):
  PriorityWorker(env, tran->database_, callback, "rocksdb.transaction.get"),
  tran_(tran),
  key_(key),
  asBuffer_(asBuffer)
  {
  options_.fill_cache = fillCache;
}

TransactionGetWorker::~TransactionGetWorker () {
  DisposeSliceBuffer(key_);
}

void TransactionGetWorker::DoExecute () {
  SetStatus(tran_->Get(options_, key_, value_));
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
  napi_env env,
  Transaction* tran,
  napi_value callback,
  rocksdb::Slice key,
  const bool asBuffer,
  const bool fillCache,
  const bool exclusive
):
  PriorityWorker(env, tran->database_, callback, "rocksdb.transaction.get_for_update"),
  tran_(tran),
  key_(key),
  asBuffer_(asBuffer),
  exclusive_(exclusive)
{
  options_.fill_cache = fillCache;
}

TransactionGetForUpdateWorker::~TransactionGetForUpdateWorker () {
  DisposeSliceBuffer(key_);
}

void TransactionGetForUpdateWorker::DoExecute () {
  SetStatus(tran_->GetForUpdate(options_, key_, value_, exclusive_));
}

void TransactionGetForUpdateWorker::HandleOKCallback(napi_env env, napi_value callback) {
  napi_value argv[2];
  napi_get_null(env, &argv[0]);
  Entry::Convert(env, &value_, asBuffer_, &argv[1]);
  CallFunction(env, callback, 2, argv);
}

/**
 * Transaction put
 */

TransactionPutWorker::TransactionPutWorker (
  napi_env env,
  Transaction* tran,
  napi_value callback,
  rocksdb::Slice key,
  rocksdb::Slice value
):
  PriorityWorker(env, tran->database_, callback, "rocksdb.transaction.put"),
  tran_(tran),
  key_(key),
  value_(value)
  {}

TransactionPutWorker::~TransactionPutWorker () {
  DisposeSliceBuffer(key_);
  DisposeSliceBuffer(value_);
}

void TransactionPutWorker::DoExecute () {
  SetStatus(tran_->Put(key_, value_));
}

/**
 * Transaction del
 */

TransactionDelWorker::TransactionDelWorker(
  napi_env env,
  Transaction* tran,
  napi_value callback,
  rocksdb::Slice key
):
  PriorityWorker(env, tran->database_, callback, "rocksdb.transaction.del"),
  tran_(tran),
  key_(key)
  {}

TransactionDelWorker::~TransactionDelWorker () {
  DisposeSliceBuffer(key_);
}

void TransactionDelWorker::DoExecute () {
  SetStatus(tran_->Del(key_));
}
