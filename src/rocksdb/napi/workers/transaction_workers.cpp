#define NAPI_VERSION 3

#include "transaction_workers.h"

#include <string>
#include <vector>

#include <node_api.h>
#include <rocksdb/slice.h>
#include <rocksdb/status.h>

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
    const TransactionSnapshot* snapshot)
    : PriorityWorker(env, tran, callback, "rocksdb.transaction.get_for_update"),
      key_(key),
      asBuffer_(asBuffer) {
  options_.fill_cache = fillCache;
  if (snapshot != nullptr) options_.snapshot = snapshot->snapshot();
}

TransactionGetForUpdateWorker::~TransactionGetForUpdateWorker() {
  DisposeSliceBuffer(key_);
}

void TransactionGetForUpdateWorker::DoExecute() {
  SetStatus(transaction_->GetForUpdate(options_, key_, value_));
}

void TransactionGetForUpdateWorker::HandleOKCallback(napi_env env,
                                                     napi_value callback) {
  napi_value argv[2];
  napi_get_null(env, &argv[0]);
  Entry::Convert(env, &value_, asBuffer_, &argv[1]);
  CallFunction(env, callback, 2, argv);
}

/**
 * Transaction multi get
 */

TransactionMultiGetWorker::TransactionMultiGetWorker(
    napi_env env, Transaction* transaction,
    const std::vector<rocksdb::Slice>* keys, napi_value callback,
    const bool valueAsBuffer, const bool fillCache,
    const TransactionSnapshot* snapshot)
    : PriorityWorker(env, transaction, callback,
                     "rocksdb.transaction.multiget"),
      keys_(keys),
      valueAsBuffer_(valueAsBuffer) {
  options_.fill_cache = fillCache;
  if (snapshot) options_.snapshot = snapshot->snapshot();
}

TransactionMultiGetWorker::~TransactionMultiGetWorker() { delete keys_; }

void TransactionMultiGetWorker::DoExecute() {
  // NAPI requires a vector of string pointers
  // the nullptr can be used to represent `undefined`
  values_.reserve(keys_->size());
  // RocksDB requires just a vector of strings
  // these will be automatically deallocated
  std::vector<std::string> values(keys_->size());
  std::vector<rocksdb::Status> statuses =
      transaction_->MultiGet(options_, *keys_, values);
  for (size_t i = 0; i != statuses.size(); i++) {
    if (statuses[i].ok()) {
      std::string* value = new std::string(values[i]);
      values_.push_back(value);
    } else if (statuses[i].IsNotFound()) {
      values_.push_back(nullptr);
    } else {
      for (const std::string* value : values_) {
        if (value != NULL) delete value;
      }
      SetStatus(statuses[i]);
      break;
    }
  }
}

void TransactionMultiGetWorker::HandleOKCallback(napi_env env,
                                                 napi_value callback) {
  size_t size = values_.size();
  napi_value array;
  napi_create_array_with_length(env, size, &array);

  for (size_t idx = 0; idx < size; idx++) {
    std::string* value = values_[idx];
    napi_value element;
    Entry::Convert(env, value, valueAsBuffer_, &element);
    napi_set_element(env, array, static_cast<uint32_t>(idx), element);
    if (value != nullptr) delete value;
  }

  napi_value argv[2];
  napi_get_null(env, &argv[0]);
  argv[1] = array;
  CallFunction(env, callback, 2, argv);
}

/**
 * Transaction multi get for update
 */

TransactionMultiGetForUpdateWorker::TransactionMultiGetForUpdateWorker(
    napi_env env, Transaction* transaction,
    const std::vector<rocksdb::Slice>* keys, napi_value callback,
    const bool valueAsBuffer, const bool fillCache,
    const TransactionSnapshot* snapshot)
    : PriorityWorker(env, transaction, callback,
                     "rocksdb.transaction.multiget_for_update"),
      keys_(keys),
      valueAsBuffer_(valueAsBuffer) {
  options_.fill_cache = fillCache;
  if (snapshot) options_.snapshot = snapshot->snapshot();
}

TransactionMultiGetForUpdateWorker::~TransactionMultiGetForUpdateWorker() {
  delete keys_;
}

void TransactionMultiGetForUpdateWorker::DoExecute() {
  // NAPI requires a vector of string pointers
  // the nullptr can be used to represent `undefined`
  values_.reserve(keys_->size());
  // RocksDB requires just a vector of strings
  // these will be automatically deallocated
  std::vector<std::string> values(keys_->size());
  std::vector<rocksdb::Status> statuses =
      transaction_->MultiGetForUpdate(options_, *keys_, values);
  for (size_t i = 0; i != statuses.size(); i++) {
    if (statuses[i].ok()) {
      std::string* value = new std::string(values[i]);
      values_.push_back(value);
    } else if (statuses[i].IsNotFound()) {
      values_.push_back(nullptr);
    } else {
      for (const std::string* value : values_) {
        if (value != NULL) delete value;
      }
      SetStatus(statuses[i]);
      break;
    }
  }
}

void TransactionMultiGetForUpdateWorker::HandleOKCallback(napi_env env,
                                                          napi_value callback) {
  size_t size = values_.size();
  napi_value array;
  napi_create_array_with_length(env, size, &array);

  for (size_t idx = 0; idx < size; idx++) {
    std::string* value = values_[idx];
    napi_value element;
    Entry::Convert(env, value, valueAsBuffer_, &element);
    napi_set_element(env, array, static_cast<uint32_t>(idx), element);
    if (value != nullptr) delete value;
  }

  napi_value argv[2];
  napi_get_null(env, &argv[0]);
  argv[1] = array;
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
