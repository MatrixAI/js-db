#pragma once

#ifndef NAPI_VERSION
#define NAPI_VERSION 3
#endif

#include <cstdint>
#include <string>
#include <node_api.h>
#include <rocksdb/status.h>
#include <rocksdb/options.h>
#include <rocksdb/slice.h>
#include <rocksdb/iterator.h>
#include "database.h"

/**
 * Whether to yield entries, keys or values.
 */
enum Mode {
  entries,
  keys,
  values
};

/**
 * Helper struct for caching and converting a key-value pair to napi_values.
 */
struct Entry {
  Entry(const rocksdb::Slice* key, const rocksdb::Slice* value);

  void ConvertByMode(napi_env env, Mode mode, const bool keyAsBuffer, const bool valueAsBuffer, napi_value* result);

  static void Convert(napi_env env, const std::string* s, const bool asBuffer, napi_value* result);

private:
  std::string key_;
  std::string value_;
};

/**
 * Owns a rocksdb iterator.
 */
struct BaseIterator {
  BaseIterator(
    Database* database,
    const bool reverse,
    std::string* lt,
    std::string* lte,
    std::string* gt,
    std::string* gte,
    const int limit,
    const bool fillCache
  );

  virtual ~BaseIterator ();

  bool DidSeek () const;

  /**
   * Seek to the first relevant key based on range options.
   */
  void SeekToRange ();

  /**
   * Seek manually (during iteration).
   */
  void Seek (rocksdb::Slice& target);

  void Close ();

  bool Valid () const;

  bool Increment ();

  void Next ();

  void SeekToFirst ();

  void SeekToLast ();

  void SeekToEnd ();

  rocksdb::Slice CurrentKey () const;

  rocksdb::Slice CurrentValue () const;

  rocksdb::Status Status () const;

  bool OutOfRange (const rocksdb::Slice& target) const;

  Database* database_;
  bool hasClosed_;

private:
  rocksdb::Iterator* dbIterator_;
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
 * Extends BaseIterator for reading it from JS land.
 */
struct Iterator final : public BaseIterator {
  Iterator (Database* database,
            const uint32_t id,
            const bool reverse,
            const bool keys,
            const bool values,
            const int limit,
            std::string* lt,
            std::string* lte,
            std::string* gt,
            std::string* gte,
            const bool fillCache,
            const bool keyAsBuffer,
            const bool valueAsBuffer,
            const uint32_t highWaterMarkBytes);

  ~Iterator ();

  void Attach (napi_env env, napi_value context);

  void Detach (napi_env env);

  bool ReadMany (uint32_t size);

  const uint32_t id_;
  const bool keys_;
  const bool values_;
  const bool keyAsBuffer_;
  const bool valueAsBuffer_;
  const uint32_t highWaterMarkBytes_;
  bool first_;
  bool nexting_;
  bool isClosing_;
  BaseWorker* closeWorker_;
  std::vector<Entry> cache_;

private:
  napi_ref ref_;
};
