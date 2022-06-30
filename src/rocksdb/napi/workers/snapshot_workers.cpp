#define NAPI_VERSION 3

#include "snapshot_workers.h"

#include <node/node_api.h>

#include "../worker.h"
#include "../snapshot.h"

SnapshotReleaseWorker::SnapshotReleaseWorker(napi_env env, Snapshot* snapshot,
                                             napi_value callback)
    : PriorityWorker(env, snapshot->database_, callback,
                     "rocksdb.snapshot.release"),
      snapshot_(snapshot) {}

SnapshotReleaseWorker::~SnapshotReleaseWorker() = default;

void SnapshotReleaseWorker::DoExecute() { snapshot_->Release(); };

void SnapshotReleaseWorker::DoFinally(napi_env env) {
  snapshot_->Detach(env);
  PriorityWorker::DoFinally(env);
};
