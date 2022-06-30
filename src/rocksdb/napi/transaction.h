#pragma once

#ifndef NAPI_VERSION
#define NAPI_VERSION 3
#endif

#include <cstdint>
#include <map>
#include <vector>
#include <string>

#include <node/node_api.h>
#include <rocksdb/slice.h>
#include <rocksdb/status.h>
#include <rocksdb/options.h>
#include <rocksdb/iterator.h>
#include <rocksdb/utilities/transaction.h>

#include "database.h"

/**
 * Forward declarations
 */
struct Iterator;
struct BaseWorker;

/**
 * Transaction object managed from JS
 */
struct Transaction final {
  /**
   * Constructs transaction from database
   * Call `Transaction::Attach` afterwards
   */
  Transaction(Database* database, const uint32_t id, const bool sync);

  /**
   * Destroys transaction
   * Call `Transaction::Rollback()` or `Transaction::Commit()`
   * then `Transaction::Detach` beforehand
   */
  ~Transaction();

  /**
   * Creates JS reference count at 1 to prevent GC of this object
   * Attaches this `Transaction` to the `Database`
   * Repeating this call is idempotent
   */
  void Attach(napi_env env, napi_value transaction_ref);

  /**
   * Deletes JS reference count to allow GC of this object
   * Detaches this `Transaction` from the `Database`
   * Repeating this call is idempotent
   */
  void Detach(napi_env env);

  /**
   * Commit the transaction
   * Repeating this call is idempotent
   */
  rocksdb::Status Commit();

  /**
   * Rollback the transaction
   * Repeating this call is idempotent
   */
  rocksdb::Status Rollback();

  /**
   * Set the snapshot for the transaction
   * This only affects write consistency
   * It does not affect whether reads are consistent
   */
  void SetSnapshot();

  /**
   * Get the snapshot that was set for the transaction
   * If you don't set the snapshot prior, this will return `nullptr`
   * This snapshot must not be manually released, it will
   * be automatically released when this `Transaction` is deleted
   */
  const rocksdb::Snapshot* GetSnapshot();

  /**
   * Get an iterator for this transaction
   * The caller is responsible for deleting the iterator
   * By default it will read any value set in the transaction overlay
   * before default to the underlying DB value, this includes deleted values
   * Setting a read snapshot only affects what is read from the DB
   */
  rocksdb::Iterator* GetIterator(const rocksdb::ReadOptions& options);

  /**
   * Get a value
   * This will read from the transaction overlay and default to the underlying
   * db Use a snapshot for consistent reads
   */
  rocksdb::Status Get(const rocksdb::ReadOptions& options, rocksdb::Slice key,
                      std::string& value);

  /**
   * Get a value for update
   * This will read from the transaction overlay and default to the underlying
   * db Use this to solve write skews, and to for read-write conflicts Use a
   * snapshot for consistent reads
   */
  rocksdb::Status GetForUpdate(const rocksdb::ReadOptions& options,
                               rocksdb::Slice key, std::string& value,
                               bool exclusive = true);

  /**
   * Get multiple values
   */
  std::vector<rocksdb::Status> MultiGet(const rocksdb::ReadOptions& options,
                                        const std::vector<rocksdb::Slice>& keys,
                                        std::vector<std::string>& values);

  /**
   * Get multiple values for update
   */
  std::vector<rocksdb::Status> MultiGetForUpdate(
      const rocksdb::ReadOptions& options,
      const std::vector<rocksdb::Slice>& keys,
      std::vector<std::string>& values);

  /**
   * Put a key value
   * This will write to the transaction overlay
   * Writing to the same key after this put operation will cause a conflict
   * If a snapshot is applied to the transaction, writing to keys after the
   * snapshot is set that is also written to by this transaction, will cause a
   * conflict
   */
  rocksdb::Status Put(rocksdb::Slice key, rocksdb::Slice value);

  /**
   * Delete a key value
   * This will write to the transaction overlay
   * Writing to the same key after this put operation will cause a conflict
   * If a snapshot is applied to the transaction, writing to keys after the
   * snapshot is set that is also written to by this transaction, will cause a
   * conflict
   */
  rocksdb::Status Del(rocksdb::Slice key);

  /**
   * Attach `Iterator` to be managed by this `Transaction`
   * Iterators attached will be closed automatically if not detached
   */
  void AttachIterator(napi_env env, uint32_t id, Iterator* iterator);

  /**
   * Detach `Iterator` from this `Transaction`
   * It is assumed the caller will have closed or will be closing the iterator
   */
  void DetachIterator(napi_env env, uint32_t id);

  /**
   * Increment pending work count to delay concurrent close operation
   * This also increments the JS reference count which prevents GC
   * Pending work can be priority asynchronous operations
   * or they can be sub-objects like iterators
   */
  void IncrementPendingWork(napi_env env);

  /**
   * Decrement pending work count
   * When count reaches 0, it will run the `closeWorker_` if it set
   */
  void DecrementPendingWork(napi_env env);

  /**
   * Check if it has any pending work
   */
  bool HasPendingWork() const;

  Database* database_;
  const uint32_t id_;
  /**
   * This is managed by workers
   * It is used to indicate whether commit is asynchronously scheduled
   */
  bool isCommitting_;
  bool hasCommitted_;
  /**
   * This is managed by workers
   * It is used to indicate whether rollback is asynchronously scheduled
   */
  bool isRollbacking_;
  bool hasRollbacked_;
  uint32_t currentIteratorId_;
  std::map<uint32_t, Iterator*> iterators_;
  BaseWorker* closeWorker_;

 private:
  rocksdb::WriteOptions* options_;
  rocksdb::Transaction* tran_;
  uint32_t pendingWork_;
  napi_ref ref_;
};
