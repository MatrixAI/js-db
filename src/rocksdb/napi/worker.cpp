#define NAPI_VERSION 3

#include "worker.h"

#include <napi-macros.h>
#include <node_api.h>
#include <rocksdb/status.h>

#include "database.h"
#include "utils.h"

BaseWorker::BaseWorker(napi_env env, Database* database, napi_value callback,
                       const char* resourceName)
    : database_(database), transaction_(nullptr), errMsg_(nullptr) {
  NAPI_STATUS_THROWS_VOID(
      napi_create_reference(env, callback, 1, &callbackRef_));
  napi_value asyncResourceName;
  NAPI_STATUS_THROWS_VOID(napi_create_string_utf8(
      env, resourceName, NAPI_AUTO_LENGTH, &asyncResourceName));
  NAPI_STATUS_THROWS_VOID(napi_create_async_work(
      env, callback, asyncResourceName, BaseWorker::Execute,
      BaseWorker::Complete, this, &asyncWork_));
}

BaseWorker::BaseWorker(napi_env env, Transaction* transaction,
                       napi_value callback, const char* resourceName)
    : database_(nullptr), transaction_(transaction), errMsg_(nullptr) {
  NAPI_STATUS_THROWS_VOID(
      napi_create_reference(env, callback, 1, &callbackRef_));
  napi_value asyncResourceName;
  NAPI_STATUS_THROWS_VOID(napi_create_string_utf8(
      env, resourceName, NAPI_AUTO_LENGTH, &asyncResourceName));
  NAPI_STATUS_THROWS_VOID(napi_create_async_work(
      env, callback, asyncResourceName, BaseWorker::Execute,
      BaseWorker::Complete, this, &asyncWork_));
}

BaseWorker::~BaseWorker() { delete[] errMsg_; }

void BaseWorker::Execute(napi_env env, void* data) {
  BaseWorker* self = (BaseWorker*)data;
  // Don't pass env to DoExecute() because use of Node-API
  // methods should generally be avoided in async work.
  self->DoExecute();
}

bool BaseWorker::SetStatus(rocksdb::Status status) {
  status_ = status;
  if (!status.ok()) {
    SetErrorMessage(status.ToString().c_str());
    return false;
  }
  return true;
}

void BaseWorker::SetErrorMessage(const char* msg) {
  delete[] errMsg_;
  size_t size = strlen(msg) + 1;
  errMsg_ = new char[size];
  memcpy(errMsg_, msg, size);
}

void BaseWorker::Complete(napi_env env, napi_status status, void* data) {
  BaseWorker* self = (BaseWorker*)data;

  self->DoComplete(env);
  self->DoFinally(env);
}

void BaseWorker::DoComplete(napi_env env) {
  napi_value callback;
  napi_get_reference_value(env, callbackRef_, &callback);

  if (status_.ok()) {
    HandleOKCallback(env, callback);
  } else {
    HandleErrorCallback(env, callback);
  }
}

void BaseWorker::HandleOKCallback(napi_env env, napi_value callback) {
  napi_value argv;
  napi_get_null(env, &argv);
  CallFunction(env, callback, 1, &argv);
}

void BaseWorker::HandleErrorCallback(napi_env env, napi_value callback) {
  napi_value argv;

  if (status_.IsNotFound()) {
    argv = CreateCodeError(env, "NOT_FOUND", errMsg_);
  } else if (status_.IsCorruption()) {
    argv = CreateCodeError(env, "CORRUPTION", errMsg_);
  } else if (status_.IsIOError()) {
    if (strlen(errMsg_) > 15 &&
        strncmp("IO error: lock ", errMsg_, 15) == 0) {  // fs_posix.cc
      argv = CreateCodeError(env, "LOCKED", errMsg_);
    } else if (strlen(errMsg_) > 32 &&
               strncmp("IO error: Failed to create lock ", errMsg_, 32) ==
                   0) {  // env_win.cc
      argv = CreateCodeError(env, "LOCKED", errMsg_);
    } else {
      argv = CreateCodeError(env, "IO_ERROR", errMsg_);
    }
  } else if (status_.IsBusy()) {
    argv = CreateCodeError(env, "TRANSACTION_CONFLICT", errMsg_);
  } else {
    argv = CreateError(env, errMsg_);
  }

  CallFunction(env, callback, 1, &argv);
}

void BaseWorker::DoFinally(napi_env env) {
  napi_delete_reference(env, callbackRef_);
  napi_delete_async_work(env, asyncWork_);
  // Because the worker is executed asynchronously
  // cleanup must be done by itself
  delete this;
}

void BaseWorker::Queue(napi_env env) { napi_queue_async_work(env, asyncWork_); }

PriorityWorker::PriorityWorker(napi_env env, Database* database,
                               napi_value callback, const char* resourceName)
    : BaseWorker(env, database, callback, resourceName) {
  database_->IncrementPendingWork(env);
}

PriorityWorker::PriorityWorker(napi_env env, Transaction* transaction,
                               napi_value callback, const char* resourceName)
    : BaseWorker(env, transaction, callback, resourceName) {
  transaction_->IncrementPendingWork(env);
}

PriorityWorker::~PriorityWorker() = default;

void PriorityWorker::DoFinally(napi_env env) {
  assert(database_ != nullptr || transaction_ != nullptr);
  if (database_ != nullptr) {
    database_->DecrementPendingWork(env);
  } else if (transaction_ != nullptr) {
    transaction_->DecrementPendingWork(env);
  }
  BaseWorker::DoFinally(env);
}
