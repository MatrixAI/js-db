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
      ref_(NULL) {
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
