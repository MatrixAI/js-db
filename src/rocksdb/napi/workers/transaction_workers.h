#pragma once

#ifndef NAPI_VERSION
#define NAPI_VERSION 3
#endif

#include <string>
#include <vector>

#include <node/node_api.h>
#include <rocksdb/options.h>
#include <rocksdb/slice.h>
#include <rocksdb/snapshot.h>

#include "../worker.h"
#include "../transaction.h"
#include "../snapshot.h"

/**
 * Transaction commit worker
 */
struct TransactionCommitWorker final : public BaseWorker {
  TransactionCommitWorker(napi_env env, Transaction* tran, napi_value callback);

  ~TransactionCommitWorker();

  void DoExecute() override;

  void DoFinally(napi_env env) override;
};

/**
 * Rollback commit worker
 */
struct TransactionRollbackWorker final : public BaseWorker {
  TransactionRollbackWorker(napi_env env, Transaction* tran,
                            napi_value callback);

  ~TransactionRollbackWorker();

  void DoExecute() override;

  void DoFinally(napi_env env) override;
};

/**
 * Worker for transaction get
 */
struct TransactionGetWorker final : public PriorityWorker {
  TransactionGetWorker(napi_env env, Transaction* tran, napi_value callback,
                       rocksdb::Slice key, const bool asBuffer,
                       const bool fillCache,
                       const TransactionSnapshot* snapshot = nullptr);

  ~TransactionGetWorker();

  void DoExecute() override;

  void HandleOKCallback(napi_env env, napi_value callback) override;

 private:
  rocksdb::ReadOptions options_;
  rocksdb::Slice key_;
  std::string value_;
  const bool asBuffer_;
};

/**
 * Worker for transaction get for update
 */
struct TransactionGetForUpdateWorker final : public PriorityWorker {
  TransactionGetForUpdateWorker(napi_env env, Transaction* tran,
                                napi_value callback, rocksdb::Slice key,
                                const bool asBuffer, const bool fillCache,
                                const TransactionSnapshot* snapshot = nullptr);

  ~TransactionGetForUpdateWorker();

  void DoExecute() override;

  void HandleOKCallback(napi_env env, napi_value callback) override;

 private:
  rocksdb::ReadOptions options_;
  rocksdb::Slice key_;
  std::string value_;
  const bool asBuffer_;
};

struct TransactionMultiGetWorker final : public PriorityWorker {
  TransactionMultiGetWorker(napi_env env, Transaction* transaction,
                            const std::vector<rocksdb::Slice>* keys,
                            napi_value callback, const bool valueAsBuffer,
                            const bool fillCache,
                            const TransactionSnapshot* snapshot = nullptr);

  ~TransactionMultiGetWorker();

  void DoExecute() override;

  void HandleOKCallback(napi_env env, napi_value callback) override;

 private:
  rocksdb::ReadOptions options_;
  const std::vector<rocksdb::Slice>* keys_;
  std::vector<std::string*> values_;
  const bool valueAsBuffer_;
};

struct TransactionMultiGetForUpdateWorker final : public PriorityWorker {
  TransactionMultiGetForUpdateWorker(
      napi_env env, Transaction* transaction,
      const std::vector<rocksdb::Slice>* keys, napi_value callback,
      const bool valueAsBuffer, const bool fillCache,
      const TransactionSnapshot* snapshot = nullptr);

  ~TransactionMultiGetForUpdateWorker();

  void DoExecute() override;

  void HandleOKCallback(napi_env env, napi_value callback) override;

 private:
  rocksdb::ReadOptions options_;
  const std::vector<rocksdb::Slice>* keys_;
  std::vector<std::string*> values_;
  const bool valueAsBuffer_;
};

/**
 * Worker for transaction put
 */
struct TransactionPutWorker final : public PriorityWorker {
  TransactionPutWorker(napi_env env, Transaction* tran, napi_value callback,
                       rocksdb::Slice key, rocksdb::Slice value);

  ~TransactionPutWorker();

  void DoExecute() override;

 private:
  rocksdb::Slice key_;
  rocksdb::Slice value_;
};

/**
 * Worker for transaction del
 */
struct TransactionDelWorker final : public PriorityWorker {
  TransactionDelWorker(napi_env env, Transaction* tran, napi_value callback,
                       rocksdb::Slice key);

  ~TransactionDelWorker();

  void DoExecute() override;

 private:
  rocksdb::Slice key_;
};
