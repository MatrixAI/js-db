#pragma once

#ifndef NAPI_VERSION
#define NAPI_VERSION 3
#endif

#include <cstdint>
#include <string>

#include <node/node_api.h>
#include <rocksdb/status.h>
#include <rocksdb/options.h>
#include <rocksdb/slice.h>
#include <rocksdb/iterator.h>

#include "database.h"
#include "transaction.h"
#include "snapshot.h"
#include "worker.h"

/**
 * Whether to yield entries, keys or values.
 */
enum Mode { entries, keys, values };

/**
 * Helper struct for caching and converting a key-value pair to napi_values.
 */
struct Entry {
  Entry(const rocksdb::Slice* key, const rocksdb::Slice* value);

  void ConvertByMode(napi_env env, Mode mode, const bool keyAsBuffer,
                     const bool valueAsBuffer, napi_value* result);

  static void Convert(napi_env env, const std::string* s, const bool asBuffer,
                      napi_value* result);

 private:
  std::string key_;
  std::string value_;
};

/**
 * Iterator wrapper used internally
 * Lifecycle controlled manually in C++
 */
struct BaseIterator {
  /**
   * Constructs iterator from database
   */
  BaseIterator(Database* database, const bool reverse, std::string* lt,
               std::string* lte, std::string* gt, std::string* gte,
               const int limit, const bool fillCache,
               const Snapshot* snapshot = nullptr);

  /**
   * Constructs iterator from transaction
   */
  BaseIterator(Transaction* transaction, const bool reverse, std::string* lt,
               std::string* lte, std::string* gt, std::string* gte,
               const int limit, const bool fillCache,
               const TransactionSnapshot* snapshot = nullptr);

  /**
   * Destroy iterator
   * Call `BaseIterator::Close` beforehand
   */
  virtual ~BaseIterator();

  /**
   * Closes the iterator
   * Repeating this call is idempotent
   */
  virtual void Close();

  bool DidSeek() const;

  /**
   * Seek to the first relevant key based on range options.
   */
  void SeekToRange();

  /**
   * Seek manually (during iteration).
   */
  void Seek(rocksdb::Slice& target);

  bool Valid() const;

  bool Increment();

  void Next();

  void SeekToFirst();

  void SeekToLast();

  void SeekToEnd();

  rocksdb::Slice CurrentKey() const;

  rocksdb::Slice CurrentValue() const;

  rocksdb::Status Status() const;

  bool OutOfRange(const rocksdb::Slice& target) const;

  Database* database_;
  Transaction* transaction_;
  bool hasClosed_;

 private:
  rocksdb::Iterator* iter_;
  bool didSeek_;
  const bool reverse_;
  std::string* lt_;
  std::string* lte_;
  std::string* gt_;
  std::string* gte_;
  const int limit_;
  int count_;
  rocksdb::ReadOptions* options_;
};

/**
 * Iterator object managed from JS
 * Lifecycle controlled by JS
 */
struct Iterator final : public BaseIterator {
  /**
   * Constructs iterator from database
   * Call `Iterator::Attach` afterwards
   */
  Iterator(Database* database, const uint32_t id, const bool reverse,
           const bool keys, const bool values, const int limit, std::string* lt,
           std::string* lte, std::string* gt, std::string* gte,
           const bool fillCache, const bool keyAsBuffer,
           const bool valueAsBuffer, const uint32_t highWaterMarkBytes,
           const Snapshot* snapshot = nullptr);

  /**
   * Constructs iterator from transaction
   * Call `Iterator::Attach` afterwards
   */
  Iterator(Transaction* transaction, const uint32_t id, const bool reverse,
           const bool keys, const bool values, const int limit, std::string* lt,
           std::string* lte, std::string* gt, std::string* gte,
           const bool fillCache, const bool keyAsBuffer,
           const bool valueAsBuffer, const uint32_t highWaterMarkBytes,
           const TransactionSnapshot* snapshot = nullptr);

  ~Iterator() override;

  /**
   * Creates JS reference count at 1 to prevent GC of this object
   * Attaches this `Iterator` to the `Database` or `Transaction`
   * Repeating this call is idempotent
   * Call this after `Iterator::Iterator`
   */
  void Attach(napi_env env, napi_value iterator_ref);

  /**
   * Deletes JS reference count to allow GC of this object
   * Detaches this `Transaction` from the `Database`
   * Repeating this call is idempotent
   * Call this after `BaseIterator::Close` but before
   * `BaseIterator::~BaseIterator`
   */
  void Detach(napi_env env);

  void Close() override;

  bool ReadMany(uint32_t size);

  const uint32_t id_;
  const bool keys_;
  const bool values_;
  const bool keyAsBuffer_;
  const bool valueAsBuffer_;
  const uint32_t highWaterMarkBytes_;
  bool first_;
  bool nexting_;
  /**
   * This is managed by workers
   * It is used to indicate whether close is asynchronously scheduled
   */
  bool isClosing_;
  BaseWorker* closeWorker_;
  std::vector<Entry> cache_;

 private:
  napi_ref ref_;
};
