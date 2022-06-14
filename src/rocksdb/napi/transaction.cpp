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
  LOG_DEBUG("Transaction:Constructing Transaction %d\n", id_);
  options_ = new rocksdb::WriteOptions();
  options_->sync = sync;
  tran_ = database->NewTransaction(*options_);
  LOG_DEBUG("Transaction:Constructed Transaction %d\n", id_);
}

Transaction::~Transaction() {
  LOG_DEBUG("Transaction:Destroying Transaction %d\n", id_);
  assert(hasCommitted_ || hasRollbacked_);
  delete tran_;
  delete options_;
  LOG_DEBUG("Transaction:Destroyed Transaction %d\n", id_);
}

void Transaction::Attach(napi_env env, napi_value transaction_ref) {
  if (ref_ != nullptr) return;
  NAPI_STATUS_THROWS_VOID(
      napi_create_reference(env, transaction_ref, 1, &ref_));
  database_->AttachTransaction(env, id_, this);
}

void Transaction::Detach(napi_env env) {
  if (ref_ == nullptr) return;
  database_->DetachTransaction(env, id_);
  NAPI_STATUS_THROWS_VOID(napi_delete_reference(env, ref_));
  ref_ = nullptr;
}

rocksdb::Status Transaction::Commit() {
  LOG_DEBUG("Transaction:Committing Transaction %d\n", id_);
  assert(!hasRollbacked_);
  if (hasCommitted_) return rocksdb::Status::OK();
  hasCommitted_ = true;
  rocksdb::Status status = tran_->Commit();
  // Early deletion
  delete tran_;
  tran_ = nullptr;
  delete options_;
  options_ = nullptr;
  LOG_DEBUG("Transaction:Committed Transaction %d\n", id_);
  return status;
}

rocksdb::Status Transaction::Rollback() {
  LOG_DEBUG("Transaction:Rollbacking Transaction %d\n", id_);
  assert(!hasCommitted_);
  if (hasRollbacked_) return rocksdb::Status::OK();
  hasRollbacked_ = true;
  rocksdb::Status status = tran_->Rollback();
  // Early deletion
  delete tran_;
  tran_ = nullptr;
  delete options_;
  options_ = nullptr;
  LOG_DEBUG("Transaction:Rollbacked Transaction %d\n", id_);
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

void Transaction::AttachIterator(napi_env env, uint32_t id,
                                 Iterator* iterator) {
  assert(!hasCommitted_ && !hasRollbacked_);
  iterators_[id] = iterator;
  IncrementPendingWork(env);
}

void Transaction::DetachIterator(napi_env env, uint32_t id) {
  assert(!hasCommitted_ && !hasRollbacked_);
  iterators_.erase(id);
  DecrementPendingWork(env);
}

void Transaction::IncrementPendingWork(napi_env env) {
  assert(!hasCommitted_ && !hasRollbacked_);
  napi_reference_ref(env, ref_, &pendingWork_);
}

void Transaction::DecrementPendingWork(napi_env env) {
  assert(!hasCommitted_ && !hasRollbacked_);
  napi_reference_unref(env, ref_, &pendingWork_);
  // If the `closeWorker_` is set, then the closing operation
  // is waiting until all pending work is completed
  if (closeWorker_ != nullptr && pendingWork_ == 0) {
    closeWorker_->Queue(env);
    closeWorker_ = nullptr;
  }
}

bool Transaction::HasPendingWork() const {
  // Initial JS reference count starts at 1
  return pendingWork_ > 1;
}
