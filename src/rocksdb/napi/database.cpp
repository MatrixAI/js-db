#define NAPI_VERSION 3

#include "database.h"
#include <node_api.h>
#include <rocksdb/db.h>
#include <rocksdb/status.h>
#include <rocksdb/slice.h>
#include <rocksdb/options.h>
#include <rocksdb/utilities/optimistic_transaction_db.h>
#include "worker.h"

Database::Database():
  db_(NULL),
  currentIteratorId_(0),
  currentTransactionId_(0),
  pendingCloseWorker_(NULL),
  ref_(NULL),
  priorityWork_(0) {}

Database::~Database() {
  if (db_ != NULL) {
    delete db_;
    db_ = NULL;
  }
}

rocksdb::Status Database::Open (
  const rocksdb::Options& options,
  const char* location
) {
  return rocksdb::OptimisticTransactionDB::Open(options, location, &db_);
}

void Database::CloseDatabase () {
  delete db_;
  db_ = NULL;
}

rocksdb::Status Database::Put (
  const rocksdb::WriteOptions& options,
  rocksdb::Slice key,
  rocksdb::Slice value
) {
  return db_->Put(options, key, value);
}

rocksdb::Status Database::Get (
  const rocksdb::ReadOptions& options,
  rocksdb::Slice key,
  std::string& value
) {
  return db_->Get(options, key, &value);
}

rocksdb::Status Database::Del (
  const rocksdb::WriteOptions& options,
  rocksdb::Slice key
) {
  return db_->Delete(options, key);
}

rocksdb::Status Database::WriteBatch (
  const rocksdb::WriteOptions& options,
  rocksdb::WriteBatch* batch
) {
  return db_->Write(options, batch);
}

uint64_t Database::ApproximateSize (const rocksdb::Range* range) {
  uint64_t size = 0;
  db_->GetApproximateSizes(range, 1, &size);
  return size;
}

void Database::CompactRange (
  const rocksdb::Slice* start,
  const rocksdb::Slice* end
) {
  rocksdb::CompactRangeOptions options;
  db_->CompactRange(options, start, end);
}

void Database::GetProperty (const rocksdb::Slice& property, std::string* value) {
  db_->GetProperty(property, value);
}

const rocksdb::Snapshot* Database::NewSnapshot () {
  return db_->GetSnapshot();
}

rocksdb::Iterator* Database::NewIterator (rocksdb::ReadOptions* options) {
  return db_->NewIterator(*options);
}

rocksdb::Transaction* Database::NewTransaction (rocksdb::WriteOptions* options) {
  return db_->BeginTransaction(*options);
}

void Database::ReleaseSnapshot (const rocksdb::Snapshot* snapshot) {
  return db_->ReleaseSnapshot(snapshot);
}

void Database::AttachIterator (napi_env env, uint32_t id, Iterator* iterator) {
  iterators_[id] = iterator;
  IncrementPriorityWork(env);
}

void Database::DetachIterator (napi_env env, uint32_t id) {
  iterators_.erase(id);
  DecrementPriorityWork(env);
}

void Database::AttachTransaction (napi_env env, uint32_t id, Transaction* transaction) {
  transactions_[id] = transaction;
  IncrementPriorityWork(env);
}

void Database::DetachTransaction (napi_env env, uint32_t id) {
  transactions_.erase(id);
  DecrementPriorityWork(env);
}

void Database::IncrementPriorityWork (napi_env env) {
  napi_reference_ref(env, ref_, &priorityWork_);
}

void Database::DecrementPriorityWork (napi_env env) {
  napi_reference_unref(env, ref_, &priorityWork_);

  if (priorityWork_ == 0 && pendingCloseWorker_ != NULL) {
    pendingCloseWorker_->Queue(env);
    pendingCloseWorker_ = NULL;
  }
}

bool Database::HasPriorityWork () const {
  return priorityWork_ > 0;
}
