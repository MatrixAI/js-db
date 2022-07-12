#define NAPI_VERSION 3

#include "database.h"

#include <string>
#include <vector>

#include <napi-macros.h>
#include <node_api.h>
#include <rocksdb/db.h>
#include <rocksdb/status.h>
#include <rocksdb/slice.h>
#include <rocksdb/options.h>
#include <rocksdb/snapshot.h>
#include <rocksdb/utilities/optimistic_transaction_db.h>

#include "debug.h"
#include "worker.h"

Database::Database()
    : db_(nullptr),
      isClosing_(false),
      hasClosed_(false),
      currentIteratorId_(0),
      currentTransactionId_(0),
      closeWorker_(nullptr),
      ref_(nullptr),
      pendingWork_(0) {
  LOG_DEBUG("Database:Constructing Database\n");
  LOG_DEBUG("Database:Constructed Database\n");
}

Database::~Database() {
  LOG_DEBUG("Database:Destroying Database\n");
  assert(hasClosed_);
  delete db_;
  LOG_DEBUG("Database:Destroyed Database\n");
}

void Database::Attach(napi_env env, napi_value database_ref) {
  if (ref_ != nullptr) return;
  NAPI_STATUS_THROWS_VOID(napi_create_reference(env, database_ref, 0, &ref_));
}

void Database::Detach(napi_env env) {
  if (ref_ == nullptr) return;
  NAPI_STATUS_THROWS_VOID(napi_delete_reference(env, ref_));
  ref_ = nullptr;
}

rocksdb::Status Database::Open(const rocksdb::Options& options,
                               const char* location) {
  return rocksdb::OptimisticTransactionDB::Open(options, location, &db_);
}

void Database::Close() {
  LOG_DEBUG("Database:Calling %s\n", __func__);
  if (hasClosed_) return;
  hasClosed_ = true;
  delete db_;
  db_ = nullptr;
  LOG_DEBUG("Database:Called %s\n", __func__);
}

rocksdb::Status Database::Put(const rocksdb::WriteOptions& options,
                              rocksdb::Slice key, rocksdb::Slice value) {
  assert(!hasClosed_);
  return db_->Put(options, key, value);
}

rocksdb::Status Database::Get(const rocksdb::ReadOptions& options,
                              rocksdb::Slice key, std::string& value) {
  assert(!hasClosed_);
  return db_->Get(options, key, &value);
}

std::vector<rocksdb::Status> Database::MultiGet(
    const rocksdb::ReadOptions& options,
    const std::vector<rocksdb::Slice>& keys, std::vector<std::string>& values) {
  assert(!hasClosed_);
  return db_->MultiGet(options, keys, &values);
}

rocksdb::Status Database::Del(const rocksdb::WriteOptions& options,
                              rocksdb::Slice key) {
  assert(!hasClosed_);
  return db_->Delete(options, key);
}

rocksdb::Status Database::WriteBatch(const rocksdb::WriteOptions& options,
                                     rocksdb::WriteBatch* batch) {
  assert(!hasClosed_);
  return db_->Write(options, batch);
}

uint64_t Database::ApproximateSize(const rocksdb::Range* range) {
  assert(!hasClosed_);
  uint64_t size = 0;
  db_->GetApproximateSizes(range, 1, &size);
  return size;
}

void Database::CompactRange(const rocksdb::Slice* start,
                            const rocksdb::Slice* end) {
  assert(!hasClosed_);
  rocksdb::CompactRangeOptions options;
  db_->CompactRange(options, start, end);
}

void Database::GetProperty(const rocksdb::Slice& property, std::string* value) {
  assert(!hasClosed_);
  db_->GetProperty(property, value);
}

const rocksdb::Snapshot* Database::NewSnapshot() {
  assert(!hasClosed_);
  return db_->GetSnapshot();
}

rocksdb::Iterator* Database::NewIterator(rocksdb::ReadOptions& options) {
  assert(!hasClosed_);
  return db_->NewIterator(options);
}

rocksdb::Transaction* Database::NewTransaction(rocksdb::WriteOptions& options) {
  assert(!hasClosed_);
  return db_->BeginTransaction(options);
}

void Database::ReleaseSnapshot(const rocksdb::Snapshot* snapshot) {
  assert(!hasClosed_);
  return db_->ReleaseSnapshot(snapshot);
}

void Database::AttachSnapshot(napi_env env, uint32_t id, Snapshot* snapshot) {
  assert(!hasClosed_);
  snapshots_[id] = snapshot;
  IncrementPendingWork(env);
}

void Database::DetachSnapshot(napi_env env, uint32_t id) {
  snapshots_.erase(id);
  DecrementPendingWork(env);
}

void Database::AttachIterator(napi_env env, uint32_t id, Iterator* iterator) {
  assert(!hasClosed_);
  iterators_[id] = iterator;
  IncrementPendingWork(env);
}

void Database::DetachIterator(napi_env env, uint32_t id) {
  iterators_.erase(id);
  DecrementPendingWork(env);
}

void Database::AttachTransaction(napi_env env, uint32_t id,
                                 Transaction* transaction) {
  assert(!hasClosed_);
  transactions_[id] = transaction;
  IncrementPendingWork(env);
}

void Database::DetachTransaction(napi_env env, uint32_t id) {
  transactions_.erase(id);
  DecrementPendingWork(env);
}

void Database::IncrementPendingWork(napi_env env) {
  assert(!hasClosed_);
  napi_reference_ref(env, ref_, &pendingWork_);
}

void Database::DecrementPendingWork(napi_env env) {
  napi_reference_unref(env, ref_, &pendingWork_);
  // If the `closeWorker_` is set, then the closing operation
  // is waiting until all pending work is completed
  if (closeWorker_ != nullptr && pendingWork_ == 0) {
    closeWorker_->Queue(env);
    closeWorker_ = nullptr;
  }
}

bool Database::HasPendingWork() const {
  // Initial JS reference count starts at 0
  return pendingWork_ > 0;
}
