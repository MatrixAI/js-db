#pragma once

#ifndef NAPI_VERSION
#define NAPI_VERSION 3
#endif

#include <node_api.h>
#include <rocksdb/options.h>
#include <rocksdb/write_batch.h>

#include "../worker.h"
#include "../database.h"
#include "../batch.h"

/**
 * Worker class for batch write operation.
 */
struct BatchWorker final : public PriorityWorker {
  BatchWorker(napi_env env, Database* database, napi_value callback,
              rocksdb::WriteBatch* batch, const bool sync, const bool hasData);

  ~BatchWorker();

  void DoExecute() override;

 private:
  rocksdb::WriteOptions options_;
  rocksdb::WriteBatch* batch_;
  const bool hasData_;
};

/**
 * Worker class for batch write operation.
 */
struct BatchWriteWorker final : public PriorityWorker {
  BatchWriteWorker(napi_env env, napi_value context, Batch* batch,
                   napi_value callback, const bool sync);

  ~BatchWriteWorker();

  void DoExecute() override;

  void DoFinally(napi_env env) override;

 private:
  Batch* batch_;
  const bool sync_;
  napi_ref contextRef_;
};
