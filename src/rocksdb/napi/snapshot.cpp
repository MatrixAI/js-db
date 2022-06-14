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
  LOG_DEBUG("Snapshot:Constructing Snapshot %d\n", id_);
  snap_ = database->NewSnapshot();
  LOG_DEBUG("Snapshot:Constructed Snapshot %d\n", id_);
}

Snapshot::~Snapshot() {
  LOG_DEBUG("Snapshot:Destroying Snapshot %d\n", id_);
  assert(hasReleased_);
  // Cannot delete `snap_` because it is already deleted by `ReleaseSnapshot`
  LOG_DEBUG("Snapshot:Destroyed Snapshot %d\n", id_);
}

void Snapshot::Attach(napi_env env, napi_value snapshot_ref) {
  if (ref_ != nullptr) return;
  NAPI_STATUS_THROWS_VOID(napi_create_reference(env, snapshot_ref, 1, &ref_));
  database_->AttachSnapshot(env, id_, this);
}

void Snapshot::Detach(napi_env env) {
  if (ref_ == nullptr) return;
  database_->DetachSnapshot(env, id_);
  NAPI_STATUS_THROWS_VOID(napi_delete_reference(env, ref_));
  ref_ = nullptr;
}

void Snapshot::Release() {
  LOG_DEBUG("Snapshot:Releasing Snapshot %d\n", id_);
  if (hasReleased_) return;
  hasReleased_ = true;
  // This deletes also deletes `rocksdb::Snapshot`
  database_->ReleaseSnapshot(snap_);
  LOG_DEBUG("Snapshot:Released Snapshot %d\n", id_);
}

const rocksdb::Snapshot* Snapshot::snapshot() const { return snap_; }

TransactionSnapshot::TransactionSnapshot(Transaction* transaction) {
  LOG_DEBUG("TransactionSnapshot:Constructing Snapshot from Transaction\n");
  // This ensures that the transaction has consistent writes
  transaction->SetSnapshot();
  // Use this snapshot to get consistent reads
  snap_ = transaction->GetSnapshot();
  LOG_DEBUG("TransactionSnapshot:Constructed Snapshot from Transaction\n");
}

TransactionSnapshot::~TransactionSnapshot() {
  LOG_DEBUG("TransactionSnapshot:Destroying Snapshot from Transaction\n");
  LOG_DEBUG("TransactionSnapshot:Destoryed Snapshot from Transaction\n");
}

const rocksdb::Snapshot* TransactionSnapshot::snapshot() const { return snap_; }
