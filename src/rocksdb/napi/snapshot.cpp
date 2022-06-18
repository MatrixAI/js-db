#define NAPI_VERSION 3

#include "snapshot.h"

#include <cassert>
#include <cstdint>

#include <node/node_api.h>
#include <napi-macros.h>
#include <rocksdb/snapshot.h>

#include "database.h"
#include "transaction.h"
#include "debug.h"

Snapshot::Snapshot(Database* database, const uint32_t id)
    : database_(database),
      id_(id),
      isReleasing_(false),
      hasReleased_(false),
      ref_(NULL) {
  LOG_DEBUG("Snapshot %d:Constructing Snapshot from Database\n", id_);
  snap_ = database->NewSnapshot();
  LOG_DEBUG("Snapshot %d:Constructed Snapshot from Database\n", id_);
}

Snapshot::~Snapshot() {
  LOG_DEBUG("Snapshot %d:Destroying\n", id_);
  assert(hasReleased_);
  // Cannot delete `snap_` because it is already deleted by `ReleaseSnapshot`
  LOG_DEBUG("Snapshot %d:Destroyed\n", id_);
}

void Snapshot::Attach(napi_env env, napi_value snapshot_ref) {
  LOG_DEBUG("Snapshot %d:Calling %s\n", id_, __func__);
  if (ref_ != nullptr) {
    LOG_DEBUG("Snapshot %d:Called %s\n", id_, __func__);
    return;
  }
  NAPI_STATUS_THROWS_VOID(napi_create_reference(env, snapshot_ref, 1, &ref_));
  database_->AttachSnapshot(env, id_, this);
  LOG_DEBUG("Snapshot %d:Called %s\n", id_, __func__);
}

void Snapshot::Detach(napi_env env) {
  LOG_DEBUG("Snapshot %d:Calling %s\n", id_, __func__);
  if (ref_ == nullptr) {
    LOG_DEBUG("Snapshot %d:Called %s\n", id_, __func__);
    return;
  }
  database_->DetachSnapshot(env, id_);
  NAPI_STATUS_THROWS_VOID(napi_delete_reference(env, ref_));
  ref_ = nullptr;
  LOG_DEBUG("Snapshot %d:Called %s\n", id_, __func__);
}

void Snapshot::Release() {
  LOG_DEBUG("Snapshot %d:Calling %s\n", id_, __func__);
  if (hasReleased_) {
    LOG_DEBUG("Snapshot %d:Called %s\n", id_, __func__);
    return;
  }
  hasReleased_ = true;
  // This deletes also deletes `rocksdb::Snapshot`
  database_->ReleaseSnapshot(snap_);
  LOG_DEBUG("Snapshot %d:Called %s\n", id_, __func__);
}

const rocksdb::Snapshot* Snapshot::snapshot() const { return snap_; }

TransactionSnapshot::TransactionSnapshot(Transaction* transaction) {
  LOG_DEBUG(
      "TransactionSnapshot:Constructing TransactionSnapshot from Transaction "
      "%d\n",
      transaction->id_);
  // This ensures that the transaction has consistent writes
  transaction->SetSnapshot();
  // Use this snapshot to get consistent reads
  snap_ = transaction->GetSnapshot();
  LOG_DEBUG(
      "TransactionSnapshot:Constructed TransactionSnapshot from Transaction "
      "%d\n",
      transaction->id_);
}

TransactionSnapshot::~TransactionSnapshot() {
  LOG_DEBUG("TransactionSnapshot:Destroying\n");
  LOG_DEBUG("TransactionSnapshot:Destroyed\n");
}

const rocksdb::Snapshot* TransactionSnapshot::snapshot() const { return snap_; }
