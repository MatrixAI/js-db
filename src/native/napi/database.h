#pragma once

#ifndef NAPI_VERSION
#define NAPI_VERSION 3
#endif

#include <string>
#include <map>
#include <vector>

#include <node_api.h>
#include <rocksdb/db.h>
#include <rocksdb/status.h>
#include <rocksdb/slice.h>
#include <rocksdb/options.h>
#include <rocksdb/utilities/optimistic_transaction_db.h>

/**
 * Forward declarations
 */
struct Iterator;
struct Transaction;
struct Snapshot;
struct BaseWorker;

/**
 * Owns the RocksDB storage, cache, filter policy and iterators.
 */
struct Database {
  /**
   * Constructs database
   */
  Database();

  /**
   * Destroys transaction
   * Call `Database::Close()` beforehand
   */
  ~Database();

  /**
   * Creates JS reference count at 0
   * This is a weak reference, it will be GCed once the
   * db reference is no longer live
   * Repeating this call is idempotent
   */
  void Attach(napi_env env, napi_value database_ref);

  /**
   * Deletes JS reference count to allow GC of this object
   * Even though this object starts out as a weak reference,
   * this should still be called when the object is GCed
   * Repeating this call is idempotent
   */
  void Detach(napi_env env);

  rocksdb::Status Open(const rocksdb::Options& options, const char* location);

  /**
   * Close the database
   * Repeating this call is idempotent
   */
  void Close();

  rocksdb::Status Put(const rocksdb::WriteOptions& options, rocksdb::Slice key,
                      rocksdb::Slice value);

  rocksdb::Status Get(const rocksdb::ReadOptions& options, rocksdb::Slice key,
                      std::string& value);

  std::vector<rocksdb::Status> MultiGet(const rocksdb::ReadOptions& options,
                                        const std::vector<rocksdb::Slice>& keys,
                                        std::vector<std::string>& values);

  rocksdb::Status Del(const rocksdb::WriteOptions& options, rocksdb::Slice key);

  rocksdb::Status WriteBatch(const rocksdb::WriteOptions& options,
                             rocksdb::WriteBatch* batch);

  uint64_t ApproximateSize(const rocksdb::Range* range);

  void CompactRange(const rocksdb::Slice* start, const rocksdb::Slice* end);

  void GetProperty(const rocksdb::Slice& property, std::string* value);

  const rocksdb::Snapshot* NewSnapshot();

  rocksdb::Iterator* NewIterator(rocksdb::ReadOptions& options);

  rocksdb::Transaction* NewTransaction(rocksdb::WriteOptions& options);

  void ReleaseSnapshot(const rocksdb::Snapshot* snapshot);

  void AttachIterator(napi_env env, uint32_t id, Iterator* iterator);

  void DetachIterator(napi_env env, uint32_t id);

  void AttachTransaction(napi_env env, uint32_t id, Transaction* transaction);

  void DetachTransaction(napi_env env, uint32_t id);

  void AttachSnapshot(napi_env env, uint32_t id, Snapshot* snapshot);

  void DetachSnapshot(napi_env, uint32_t id);

  void IncrementPendingWork(napi_env env);

  void DecrementPendingWork(napi_env env);

  bool HasPendingWork() const;

  rocksdb::OptimisticTransactionDB* db_;
  bool isClosing_;
  bool hasClosed_;
  uint32_t currentIteratorId_;
  uint32_t currentTransactionId_;
  uint32_t currentSnapshotId_;
  std::map<uint32_t, Iterator*> iterators_;
  std::map<uint32_t, Transaction*> transactions_;
  std::map<uint32_t, Snapshot*> snapshots_;
  BaseWorker* closeWorker_;
  napi_ref ref_;

 private:
  uint32_t pendingWork_;
};
