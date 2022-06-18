#define NAPI_VERSION 3

#include "transaction.h"

#include <cassert>
#include <cstdint>

#include <node/node_api.h>
#include <napi-macros.h>
#include <rocksdb/slice.h>
#include <rocksdb/status.h>
#include <rocksdb/options.h>
#include <rocksdb/iterator.h>

#include "debug.h"
#include "database.h"
#include "iterator.h"

Transaction::Transaction(Database* database, const uint32_t id, const bool sync)
    : database_(database),
      id_(id),
      isCommitting_(false),
      hasCommitted_(false),
      isRollbacking_(false),
      hasRollbacked_(false),
      currentIteratorId_(0),
      closeWorker_(nullptr),
      pendingWork_(0),
      ref_(nullptr) {
  LOG_DEBUG("Transaction %d:Constructing from Database\n", id_);
  options_ = new rocksdb::WriteOptions();
  options_->sync = sync;
  tran_ = database->NewTransaction(*options_);
  LOG_DEBUG("Transaction %d:Constructed from Database\n", id_);
}

Transaction::~Transaction() {
  LOG_DEBUG("Transaction %d:Destroying\n", id_);
  assert(hasCommitted_ || hasRollbacked_);
  delete tran_;
  delete options_;
  LOG_DEBUG("Transaction %d:Destroyed\n", id_);
}

void Transaction::Attach(napi_env env, napi_value transaction_ref) {
  LOG_DEBUG("Transaction %d:Calling %s\n", id_, __func__);
  if (ref_ != nullptr) {
    LOG_DEBUG("Transaction %d:Called %s\n", id_, __func__);
    return;
  }
  NAPI_STATUS_THROWS_VOID(
      napi_create_reference(env, transaction_ref, 1, &ref_));
  database_->AttachTransaction(env, id_, this);
  LOG_DEBUG("Transaction %d:Called %s\n", id_, __func__);
}

void Transaction::Detach(napi_env env) {
  LOG_DEBUG("Transaction %d:Calling %s\n", id_, __func__);
  if (ref_ == nullptr) {
    LOG_DEBUG("Transaction %d:Called %s\n", id_, __func__);
    return;
  }
  database_->DetachTransaction(env, id_);
  NAPI_STATUS_THROWS_VOID(napi_delete_reference(env, ref_));
  ref_ = nullptr;
  LOG_DEBUG("Transaction %d:Called %s\n", id_, __func__);
}

rocksdb::Status Transaction::Commit() {
  LOG_DEBUG("Transaction %d:Calling %s\n", id_, __func__);
  assert(!hasRollbacked_);
  if (hasCommitted_) {
    LOG_DEBUG("Transaction %d:Called %s\n", id_, __func__);
    return rocksdb::Status::OK();
  }
  hasCommitted_ = true;
  rocksdb::Status status = tran_->Commit();
  // Early deletion
  delete tran_;
  tran_ = nullptr;
  delete options_;
  options_ = nullptr;
  LOG_DEBUG("Transaction %d:Called %s\n", id_, __func__);
  return status;
}

rocksdb::Status Transaction::Rollback() {
  LOG_DEBUG("Transaction %d:Calling %s\n", id_, __func__);
  assert(!hasCommitted_);
  if (hasRollbacked_) {
    LOG_DEBUG("Transaction %d:Called %s\n", id_, __func__);
    return rocksdb::Status::OK();
  }
  hasRollbacked_ = true;
  rocksdb::Status status = tran_->Rollback();
  // Early deletion
  delete tran_;
  tran_ = nullptr;
  delete options_;
  options_ = nullptr;
  LOG_DEBUG("Transaction %d:Called %s\n", id_, __func__);
  return status;
}

void Transaction::SetSnapshot() {
  assert(!hasCommitted_ && !hasRollbacked_);
  return tran_->SetSnapshot();
}

const rocksdb::Snapshot* Transaction::GetSnapshot() {
  assert(!hasCommitted_ && !hasRollbacked_);
  return tran_->GetSnapshot();
}

rocksdb::Iterator* Transaction::GetIterator(
    const rocksdb::ReadOptions& options) {
  assert(!hasCommitted_ && !hasRollbacked_);
  return tran_->GetIterator(options);
}

rocksdb::Status Transaction::Get(const rocksdb::ReadOptions& options,
                                 rocksdb::Slice key, std::string& value) {
  assert(!hasCommitted_ && !hasRollbacked_);
  return tran_->Get(options, key, &value);
}

rocksdb::Status Transaction::GetForUpdate(const rocksdb::ReadOptions& options,
                                          rocksdb::Slice key,
                                          std::string& value, bool exclusive) {
  assert(!hasCommitted_ && !hasRollbacked_);
  return tran_->GetForUpdate(options, key, &value, exclusive);
}

rocksdb::Status Transaction::Put(rocksdb::Slice key, rocksdb::Slice value) {
  assert(!hasCommitted_ && !hasRollbacked_);
  return tran_->Put(key, value);
}

rocksdb::Status Transaction::Del(rocksdb::Slice key) {
  assert(!hasCommitted_ && !hasRollbacked_);
  return tran_->Delete(key);
}

std::vector<rocksdb::Status> Transaction::MultiGet(
    const rocksdb::ReadOptions& options,
    const std::vector<rocksdb::Slice>& keys, std::vector<std::string>& values) {
  assert(!hasCommitted_ && !hasRollbacked_);
  return tran_->MultiGet(options, keys, &values);
}

std::vector<rocksdb::Status> Transaction::MultiGetForUpdate(
    const rocksdb::ReadOptions& options,
    const std::vector<rocksdb::Slice>& keys, std::vector<std::string>& values) {
  assert(!hasCommitted_ && !hasRollbacked_);
  return tran_->MultiGetForUpdate(options, keys, &values);
}

void Transaction::AttachIterator(napi_env env, uint32_t id,
                                 Iterator* iterator) {
  assert(!hasCommitted_ && !hasRollbacked_);
  iterators_[id] = iterator;
  IncrementPendingWork(env);
}

void Transaction::DetachIterator(napi_env env, uint32_t id) {
  iterators_.erase(id);
  DecrementPendingWork(env);
}

void Transaction::IncrementPendingWork(napi_env env) {
  assert(!hasCommitted_ && !hasRollbacked_);
  // The initial JS reference count starts at 1
  // therefore the `pendingWork_` will start at 1
  napi_reference_ref(env, ref_, &pendingWork_);
}

void Transaction::DecrementPendingWork(napi_env env) {
  napi_reference_unref(env, ref_, &pendingWork_);
  // If the `closeWorker_` is set, then the closing operation
  // is waiting until all pending work is completed
  // Remember that the `pendingWork_` starts at 1
  // so when there's no pending work, `pendingWork_` will be 1
  if (closeWorker_ != nullptr && pendingWork_ == 1) {
    closeWorker_->Queue(env);
    closeWorker_ = nullptr;
  }
}

bool Transaction::HasPendingWork() const {
  // Remember that the `pendingWork_` starts at 1
  // so when there's no pending work, `pendingWork_` will be 1
  return pendingWork_ > 1;
}
