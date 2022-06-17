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
struct CloseIteratorWorker final : public BaseWorker {
  CloseIteratorWorker(napi_env env, Iterator* iterator, napi_value callback);

  ~CloseIteratorWorker();

  void DoExecute() override;

  void DoFinally(napi_env env) override;

 private:
  Iterator* iterator_;
};

/**
 * Worker class for nexting an iterator.
 */
struct NextWorker final : public BaseWorker {
  NextWorker(napi_env env, Iterator* iterator, uint32_t size,
             napi_value callback);

  ~NextWorker();

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
struct ClearWorker final : public PriorityWorker {
  ClearWorker(napi_env env, Database* database, napi_value callback,
              const bool reverse, const int limit, std::string* lt,
              std::string* lte, std::string* gt, std::string* gte,
              const bool sync, const Snapshot* snapshot = nullptr);

  ClearWorker(napi_env env, Transaction* transaction, napi_value callback,
              const bool reverse, const int limit, std::string* lt,
              std::string* lte, std::string* gt, std::string* gte,
              const TransactionSnapshot* snapshot = nullptr);

  ~ClearWorker();

  void DoExecute() override;

 private:
  BaseIterator* iterator_;
  rocksdb::WriteOptions* writeOptions_;
};
