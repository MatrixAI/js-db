#define NAPI_VERSION 3

#include "transaction.h"

#include <cassert>
#include <cstdint>

#include <node/node_api.h>
#include <rocksdb/slice.h>
#include <rocksdb/status.h>
#include <rocksdb/options.h>

#include "database.h"

Transaction::Transaction(Database* database, const uint32_t id, const bool sync)
    : database_(database),
      id_(id),
      isCommitting_(false),
      hasCommitted_(false),
      isRollbacking_(false),
      hasRollbacked_(false),
      pendingCloseWorker_(nullptr),
      ref_(nullptr),
      priorityWork_(0) {
  options_ = new rocksdb::WriteOptions();
  options_->sync = sync;
  dbTransaction_ = database->NewTransaction(options_);
}

Transaction::~Transaction() {
  assert(hasCommitted_ || hasRollbacked_);
  delete options_;
}

void Transaction::Attach(napi_env env, napi_value tran_ref) {
  napi_create_reference(env, tran_ref, 1, &ref_);
  database_->AttachTransaction(env, id_, this);
}

void Transaction::Detach(napi_env env) {
  database_->DetachTransaction(env, id_);
  if (ref_ != NULL) napi_delete_reference(env, ref_);
}

rocksdb::Status Transaction::Commit() {
  if (hasCommitted_) {
    return rocksdb::Status::OK();
  }
  hasCommitted_ = true;
  rocksdb::Status status = dbTransaction_->Commit();
  delete dbTransaction_;
  dbTransaction_ = NULL;
  // TODO: release snapshot?
  // database_->ReleaseSnapshot(options_->snapshot);
  return status;
}

rocksdb::Status Transaction::Rollback() {
  if (hasRollbacked_) {
    return rocksdb::Status::OK();
  }
  hasRollbacked_ = true;
  rocksdb::Status status = dbTransaction_->Rollback();
  delete dbTransaction_;
  dbTransaction_ = NULL;
  // TODO: release snapshot?
  // database_->ReleaseSnapshot(options_->snapshot);
  return status;
}

rocksdb::Status Transaction::Get(const rocksdb::ReadOptions& options,
                                 rocksdb::Slice key, std::string& value) {
  return dbTransaction_->Get(options, key, &value);
}

rocksdb::Status Transaction::GetForUpdate(const rocksdb::ReadOptions& options,
                                          rocksdb::Slice key,
                                          std::string& value, bool exclusive) {
  return dbTransaction_->GetForUpdate(options, key, &value, exclusive);
}

rocksdb::Status Transaction::Put(rocksdb::Slice key, rocksdb::Slice value) {
  return dbTransaction_->Put(key, value);
}

rocksdb::Status Transaction::Del(rocksdb::Slice key) {
  return dbTransaction_->Delete(key);
}

void Transaction::SetSnapshot() { return dbTransaction_->SetSnapshot(); }

const rocksdb::Snapshot* Transaction::GetSnapshot() {
  return dbTransaction_->GetSnapshot();
}

void Transaction::IncrementPriorityWork(napi_env env) {
  napi_reference_ref(env, ref_, &priorityWork_);
}

void Transaction::DecrementPriorityWork(napi_env env) {
  napi_reference_unref(env, ref_, &priorityWork_);

  if (priorityWork_ == 0 && pendingCloseWorker_ != NULL) {
    pendingCloseWorker_->Queue(env);
    pendingCloseWorker_ = NULL;
  }
}

bool Transaction::HasPriorityWork() const {
  // The initial ref count for transaction starts at 1
  // to prevent `tran_ref` from being GCed by JS
  return priorityWork_ > 1;
}
