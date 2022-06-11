#pragma once

#ifndef NAPI_VERSION
#define NAPI_VERSION 3
#endif

#include <cstdint>

#include <node/node_api.h>
#include <rocksdb/snapshot.h>

#include "database.h"
#include "transaction.h"

/**
 * Snapshot to be used from JS land
 */
struct Snapshot final {
  Snapshot(Database* database, const uint32_t id);

  virtual ~Snapshot();

  void Release();

  void Attach(napi_env env, napi_value snap_ref);

  void Detach(napi_env env);

  const rocksdb::Snapshot* snapshot() const;

  Database* database_;
  const uint32_t id_;
  bool isReleasing_;
  bool hasReleased_;

 private:
  const rocksdb::Snapshot* dbSnapshot_;
  napi_ref ref_;
};

/**
 * Snapshot to be used from JS land
 * This is only for transactions
 */
struct TransactionSnapshot final {
  /**
   * Constructs a snapshot for a transaction
   * This sets then gets the snapshot for the transaction
   */
  TransactionSnapshot(Transaction* transaction);

  virtual ~TransactionSnapshot();

  const rocksdb::Snapshot* snapshot() const;

 private:
  const rocksdb::Snapshot* dbSnapshot_;
};
