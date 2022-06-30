#pragma once

#ifndef NAPI_VERSION
#define NAPI_VERSION 3
#endif

#include <cstdint>
#include <string>

#include <node/node_api.h>
#include <rocksdb/options.h>

#include "../worker.h"
#include "../database.h"
#include "../iterator.h"
#include "../transaction.h"
#include "../snapshot.h"

/**
 * Worker class for closing an iterator
 */
struct IteratorCloseWorker final : public BaseWorker {
  IteratorCloseWorker(napi_env env, Iterator* iterator, napi_value callback);

  ~IteratorCloseWorker();

  void DoExecute() override;

  void DoFinally(napi_env env) override;

 private:
  Iterator* iterator_;
};

/**
 * Worker class for nexting an iterator.
 */
struct IteratorNextWorker final : public BaseWorker {
  IteratorNextWorker(napi_env env, Iterator* iterator, uint32_t size,
                     napi_value callback);

  ~IteratorNextWorker();

  void DoExecute() override;

  void HandleOKCallback(napi_env env, napi_value callback) override;

  void DoFinally(napi_env env) override;

 private:
  Iterator* iterator_;
  uint32_t size_;
  bool ok_;
};

/**
 * Worker class for deleting a range from a database.
 */
struct IteratorClearWorker final : public PriorityWorker {
  IteratorClearWorker(napi_env env, Database* database, napi_value callback,
                      const int limit, std::string* lt, std::string* lte,
                      std::string* gt, std::string* gte, const bool sync,
                      const Snapshot* snapshot = nullptr);

  IteratorClearWorker(napi_env env, Transaction* transaction,
                      napi_value callback, const int limit, std::string* lt,
                      std::string* lte, std::string* gt, std::string* gte,
                      const TransactionSnapshot* snapshot = nullptr);

  ~IteratorClearWorker();

  void DoExecute() override;

 private:
  BaseIterator* iterator_;
  rocksdb::WriteOptions* writeOptions_;
};

struct IteratorCountWorker final : public PriorityWorker {
  IteratorCountWorker(napi_env env, Database* database, napi_value callback,
                      const int limit, std::string* lt, std::string* lte,
                      std::string* gt, std::string* gte,
                      const Snapshot* snapshot = nullptr);

  IteratorCountWorker(napi_env env, Transaction* transaction,
                      napi_value callback, const int limit, std::string* lt,
                      std::string* lte, std::string* gt, std::string* gte,
                      const TransactionSnapshot* snapshot = nullptr);

  ~IteratorCountWorker();

  void DoExecute() override;

  void HandleOKCallback(napi_env env, napi_value callback) override;

 private:
  BaseIterator* iterator_;
  uint32_t count_ = 0;
};
