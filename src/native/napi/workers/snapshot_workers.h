#pragma once

#ifndef NAPI_VERSION
#define NAPI_VERSION 3
#endif

#include <node_api.h>

#include "../worker.h"
#include "../snapshot.h"

/**
 * Worker class for closing a snapshot
 */
struct SnapshotReleaseWorker final : public PriorityWorker {
  SnapshotReleaseWorker(napi_env env, Snapshot* snapshot, napi_value callback);

  ~SnapshotReleaseWorker();

  void DoExecute() override;

  void DoFinally(napi_env env) override;

 private:
  Snapshot* snapshot_;
};
