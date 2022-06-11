#pragma once

#ifndef NAPI_VERSION
#define NAPI_VERSION 3
#endif

#include <cstdint>

#include <node/node_api.h>
#include <rocksdb/slice.h>
#include <rocksdb/status.h>
#include <rocksdb/options.h>
#include <rocksdb/utilities/transaction.h>

#include "database.h"

/**
 * Transaction to be used from JS land.
 */
struct Transaction final {
  Transaction(Database* database, const uint32_t id, const bool sync);

  ~Transaction();

  /**
   * Creates reference to `napi_external` of this `Transaction`
   * to prevent garbage collection
   * Tracks this `Transaction` in the `Database` for cleanup
   */
  void Attach(napi_env env, napi_value tran_ref);

  /**
   * Deletes references to `napi_external` of this `Transaction`
   * to allow garbage collection
   * Untracks this `Transaction` in the `Database` for cleanup
   */
  void Detach(napi_env env);

  /**
   * Commit the transaction
   * Synchronous operation
   */
  rocksdb::Status Commit();

  /**
   * Rollback the transaction
   * Synchronous operation
   */
  rocksdb::Status Rollback();

  rocksdb::Status Get(const rocksdb::ReadOptions& options, rocksdb::Slice key,
                      std::string& value);

  rocksdb::Status GetForUpdate(const rocksdb::ReadOptions& options,
                               rocksdb::Slice key, std::string& value,
                               bool exclusive = true);

  rocksdb::Status Put(rocksdb::Slice key, rocksdb::Slice value);

  rocksdb::Status Del(rocksdb::Slice key);

  Database* database_;
  const uint32_t id_;
  bool isCommitting_;
  bool hasCommitted_;
  bool isRollbacking_;
  bool hasRollbacked_;

 private:
  rocksdb::Transaction* dbTransaction_;
  rocksdb::WriteOptions* options_;
  napi_ref ref_;
};
