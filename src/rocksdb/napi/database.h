#pragma once

#ifndef NAPI_VERSION
#define NAPI_VERSION 3
#endif

#include <string>
#include <map>
#include <vector>

#include <node/node_api.h>
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
  Database();

  ~Database();

  rocksdb::Status Open(const rocksdb::Options& options, const char* location);

  void CloseDatabase();

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

  rocksdb::Iterator* NewIterator(rocksdb::ReadOptions* options);

  rocksdb::Transaction* NewTransaction(rocksdb::WriteOptions* options);

  void ReleaseSnapshot(const rocksdb::Snapshot* snapshot);

  void AttachIterator(napi_env env, uint32_t id, Iterator* iterator);

  void DetachIterator(napi_env env, uint32_t id);

  void AttachTransaction(napi_env env, uint32_t id, Transaction* transaction);

  void DetachTransaction(napi_env env, uint32_t id);

  void AttachSnapshot(napi_env env, uint32_t id, Snapshot* snapshot);

  void DetachSnapshot(napi_env, uint32_t id);

  void IncrementPriorityWork(napi_env env);

  void DecrementPriorityWork(napi_env env);

  bool HasPriorityWork() const;

  rocksdb::OptimisticTransactionDB* db_;
  uint32_t currentIteratorId_;
  uint32_t currentTransactionId_;
  uint32_t currentSnapshotId_;
  BaseWorker* pendingCloseWorker_;
  std::map<uint32_t, Iterator*> iterators_;
  std::map<uint32_t, Transaction*> transactions_;
  std::map<uint32_t, Snapshot*> snapshots_;
  napi_ref ref_;

 private:
  uint32_t priorityWork_;
};
