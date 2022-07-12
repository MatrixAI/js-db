#pragma once

#ifndef NAPI_VERSION
#define NAPI_VERSION 3
#endif

#include <node_api.h>
#include <rocksdb/status.h>

#include "database.h"
#include "transaction.h"

/**
 * Asynchronous worker queues operations into the Node.js libuv thread pool
 * Use this to make synchronous operations asynchronous so you don't block
 * the main Node.js thread
 *
 * Derived classes can override the following virtual methods
 * (listed in the order in which they're called):
 *
 * - DoExecute (abstract, worker pool thread): main work
 * - HandleOKCallback (main thread): call JS callback on success
 * - HandleErrorCallback (main thread): call JS callback on error
 * - DoFinally (main thread): do cleanup regardless of success
 *
 * Note: storing env is discouraged as we'd end up using it in unsafe places.
 */
struct BaseWorker {
  BaseWorker(napi_env env, Database* database, napi_value callback,
             const char* resourceName);

  BaseWorker(napi_env env, Transaction* transaction, napi_value callback,
             const char* resourceName);

  virtual ~BaseWorker();

  static void Execute(napi_env env, void* data);

  bool SetStatus(rocksdb::Status status);

  void SetErrorMessage(const char* msg);

  virtual void DoExecute() = 0;

  static void Complete(napi_env env, napi_status status, void* data);

  void DoComplete(napi_env env);

  virtual void HandleOKCallback(napi_env env, napi_value callback);

  virtual void HandleErrorCallback(napi_env env, napi_value callback);

  virtual void DoFinally(napi_env env);

  void Queue(napi_env env);

  Database* database_;
  Transaction* transaction_;

 private:
  napi_ref callbackRef_;
  napi_async_work asyncWork_;
  rocksdb::Status status_;
  char* errMsg_;
};

/**
 * Priority worker represents asynchronous operations that delays concurrent
 * close operations
 */
struct PriorityWorker : public BaseWorker {
  PriorityWorker(napi_env env, Database* database, napi_value callback,
                 const char* resourceName);

  PriorityWorker(napi_env env, Transaction* transaction, napi_value callback,
                 const char* resourceName);

  virtual ~PriorityWorker();

  void DoFinally(napi_env env) override;
};
