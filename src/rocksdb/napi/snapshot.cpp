#define NAPI_VERSION 3

#include "snapshot.h"

#include <cassert>
#include <cstdint>

#include <node/node_api.h>
#include <rocksdb/snapshot.h>

#include "database.h"
#include "transaction.h"

Snapshot::Snapshot(Database* database, const uint32_t id)
    : database_(database),
      id_(id),
      isReleasing_(false),
      hasReleased_(false),
      ref_(NULL) {
  dbSnapshot_ = database->NewSnapshot();
}

Snapshot::~Snapshot() { assert(hasReleased_); }

void Snapshot::Attach(napi_env env, napi_value snap_ref) {
  napi_create_reference(env, snap_ref, 1, &ref_);
  database_->AttachSnapshot(env, id_, this);
}

void Snapshot::Detach(napi_env env) {
  database_->DetachSnapshot(env, id_);
  if (ref_ != nullptr) napi_delete_reference(env, ref_);
}

void Snapshot::Release() {
  if (!hasReleased_) {
    hasReleased_ = true;
    database_->ReleaseSnapshot(dbSnapshot_);
  }
}

const rocksdb::Snapshot* Snapshot::snapshot() const { return dbSnapshot_; }

TransactionSnapshot::TransactionSnapshot(Transaction* transaction) {
  // This ensures that the transaction has consistent writes
  transaction->SetSnapshot();
  // Use this snapshot to get consistent reads
  dbSnapshot_ = transaction->GetSnapshot();
}

TransactionSnapshot::~TransactionSnapshot() = default;

const rocksdb::Snapshot* TransactionSnapshot::snapshot() const {
  return dbSnapshot_;
}
