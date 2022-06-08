#pragma once

#ifndef NAPI_VERSION
#define NAPI_VERSION 3
#endif

#include <rocksdb/status.h>
#include <rocksdb/slice.h>
#include <rocksdb/write_batch.h>
#include "database.h"

/**
 * Owns a WriteBatch.
 */
struct Batch {
  Batch (Database* database);

  ~Batch ();

  void Put (rocksdb::Slice key, rocksdb::Slice value);

  void Del (rocksdb::Slice key);

  void Clear ();

  rocksdb::Status Write (bool sync);

  Database* database_;
  rocksdb::WriteBatch* batch_;
  bool hasData_;
};
