#define NAPI_VERSION 3

#include "batch_workers.h"
#include <node_api.h>
#include <rocksdb/write_batch.h>
#include "../worker.h"
#include "../database.h"
#include "../batch.h"
#include "../utils.h"

BatchWorker::BatchWorker (napi_env env,
              Database* database,
              napi_value callback,
              rocksdb::WriteBatch* batch,
              const bool sync,
              const bool hasData)
  : PriorityWorker(env, database, callback, "rocksdb.batch.do"),
    batch_(batch), hasData_(hasData) {
  options_.sync = sync;
}

BatchWorker::~BatchWorker () {
  delete batch_;
}

void BatchWorker::DoExecute () {
  if (hasData_) {
    SetStatus(database_->WriteBatch(options_, batch_));
  }
}

BatchWriteWorker::BatchWriteWorker (napi_env env,
                  napi_value context,
                  Batch* batch,
                  napi_value callback,
                  const bool sync)
  : PriorityWorker(env, batch->database_, callback, "rocksdb.batch.write"),
    batch_(batch),
    sync_(sync) {
      // Prevent GC of batch object before we execute
      NAPI_STATUS_THROWS_VOID(napi_create_reference(env, context, 1, &contextRef_));
    }

BatchWriteWorker::~BatchWriteWorker () {}

void BatchWriteWorker::DoExecute () {
  if (batch_->hasData_) {
    SetStatus(batch_->Write(sync_));
  }
}

void BatchWriteWorker::DoFinally (napi_env env) {
  napi_delete_reference(env, contextRef_);
  PriorityWorker::DoFinally(env);
}
