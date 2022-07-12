#pragma once

#ifndef NAPI_VERSION
#define NAPI_VERSION 3
#endif

#include <cstdint>

#include <node_api.h>
#include <rocksdb/snapshot.h>

#include "database.h"
#include "transaction.h"

/**
 * Snapshot object managed from JS
 */
struct Snapshot final {
  /**
   * Constructs snapshot from database
   * Call `Snapshot::Attach` afterwards
   */
  Snapshot(Database* database, const uint32_t id);

  /**
   * Destroys snapshot
   * Call `Snapshot::Release()` then `Snapshot::Detach` beforehand
   */
  virtual ~Snapshot();

  /**
   * Creates JS reference count at 1 to prevent GC of this object
   * Attaches this `Snapshot` to the `Database`
   * Repeating this call is idempotent
   */
  void Attach(napi_env env, napi_value snapshot_ref);

  /**
   * Deletes JS reference count to allow GC of this object
   * Detaches this `Snapshot` from the `Database`
   * Repeating this call is idempotent
   */
  void Detach(napi_env env);

  /**
   * Release the snapshot
   * Repeating this call is idempotent
   */
  void Release();

  const rocksdb::Snapshot* snapshot() const;

  Database* database_;
  const uint32_t id_;
  /**
   * This is managed by workers
   * It is used to indicate whether release is asynchronously scheduled
   */
  bool isReleasing_;
  bool hasReleased_;

 private:
  const rocksdb::Snapshot* snap_;
  napi_ref ref_;
};

/**
 * Snapshot to be used from JS land
 * This is only for transactions
 * These snapshots must not be manually released
 * because transactions will automatically release snapshots
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
  const rocksdb::Snapshot* snap_;
};
