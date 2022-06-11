#pragma once

#ifndef NAPI_VERSION
#define NAPI_VERSION 3
#endif

#include <cstdint>

#include <node/node_api.h>

#include "../worker.h"
#include "../iterator.h"

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
