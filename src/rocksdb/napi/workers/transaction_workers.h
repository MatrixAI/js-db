#pragma once

#ifndef NAPI_VERSION
#define NAPI_VERSION 3
#endif

#include <node_api.h>
#include <rocksdb/options.h>
#include <rocksdb/slice.h>
#include "../worker.h"
#include "../transaction.h"

/**
 * Transaction commit worker
 */
struct TransactionCommitWorker final: public PriorityWorker {
  TransactionCommitWorker (
    napi_env env,
    Transaction* tran,
    napi_value callback
  );

  ~TransactionCommitWorker();

  void DoExecute () override;

  void DoFinally (napi_env env) override;

private:
  Transaction* tran_;
};

/**
 * Rollback commit worker
 */
struct TransactionRollbackWorker final: public PriorityWorker {
  TransactionRollbackWorker (napi_env env,
                             Transaction* tran,
                             napi_value callback);

  ~TransactionRollbackWorker();

  void DoExecute () override;

  void DoFinally (napi_env env) override;

private:
  Transaction* tran_;
};

/**
 * Worker for transaction get
 */
struct TransactionGetWorker final: public PriorityWorker {
  TransactionGetWorker(
    napi_env env,
    Transaction* tran,
    napi_value callback,
    rocksdb::Slice key,
    const bool asBuffer,
    const bool fillCache
  );

  ~TransactionGetWorker();

  void DoExecute () override;

  void HandleOKCallback (napi_env env, napi_value callback) override;

private:
  Transaction* tran_;
  rocksdb::ReadOptions options_;
  rocksdb::Slice key_;
  std::string value_;
  const bool asBuffer_;
};

/**
 * Worker for transaction get for update
 */
struct TransactionGetForUpdateWorker final: public PriorityWorker {

  TransactionGetForUpdateWorker(
    napi_env env,
    Transaction* tran,
    napi_value callback,
    rocksdb::Slice key,
    const bool asBuffer,
    const bool fillCache,
    const bool exclusive = true
  );

  ~TransactionGetForUpdateWorker();

  void DoExecute () override;

  void HandleOKCallback (napi_env env, napi_value callback) override;

private:
  Transaction* tran_;
  rocksdb::ReadOptions options_;
  rocksdb::Slice key_;
  std::string value_;
  const bool asBuffer_;
  const bool exclusive_;
};

/**
 * Worker for transaction put
 */
struct TransactionPutWorker final: public PriorityWorker {
  TransactionPutWorker(
    napi_env env,
    Transaction* tran,
    napi_value callback,
    rocksdb::Slice key,
    rocksdb::Slice value
  );

  ~TransactionPutWorker();

  void DoExecute () override;

private:
  Transaction* tran_;
  rocksdb::Slice key_;
  rocksdb::Slice value_;
};

/**
 * Worker for transaction del
 */
struct TransactionDelWorker final: public PriorityWorker {

  TransactionDelWorker(
    napi_env env,
    Transaction* tran,
    napi_value callback,
    rocksdb::Slice key
  );

  ~TransactionDelWorker();

  void DoExecute () override;

private:
  Transaction* tran_;
  rocksdb::Slice key_;
};
