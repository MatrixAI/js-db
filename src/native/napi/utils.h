#pragma once

#ifndef NAPI_VERSION
#define NAPI_VERSION 3
#endif

#include <string>
#include <vector>

#include <napi-macros.h>
#include <node_api.h>
#include <rocksdb/env.h>
#include <rocksdb/slice.h>

#include "database.h"
#include "iterator.h"
#include "transaction.h"
#include "batch.h"
#include "snapshot.h"

/**
 * Macros
 */

#define NAPI_DB_CONTEXT()    \
  Database* database = NULL; \
  NAPI_STATUS_THROWS(napi_get_value_external(env, argv[0], (void**)&database));

#define NAPI_ITERATOR_CONTEXT() \
  Iterator* iterator = NULL;    \
  NAPI_STATUS_THROWS(napi_get_value_external(env, argv[0], (void**)&iterator));

#define NAPI_TRANSACTION_CONTEXT() \
  Transaction* transaction = NULL; \
  NAPI_STATUS_THROWS(              \
      napi_get_value_external(env, argv[0], (void**)&transaction));

#define NAPI_BATCH_CONTEXT() \
  Batch* batch = NULL;       \
  NAPI_STATUS_THROWS(napi_get_value_external(env, argv[0], (void**)&batch));

#define NAPI_SNAPSHOT_CONTEXT() \
  Snapshot* snapshot = NULL;    \
  NAPI_STATUS_THROWS(napi_get_value_external(env, argv[0], (void**)&snapshot));

#define NAPI_RETURN_UNDEFINED() return 0;

#define NAPI_UTF8_NEW(name, val)                                   \
  size_t name##_size = 0;                                          \
  NAPI_STATUS_THROWS(                                              \
      napi_get_value_string_utf8(env, val, NULL, 0, &name##_size)) \
  char* name = new char[name##_size + 1];                          \
  NAPI_STATUS_THROWS(napi_get_value_string_utf8(                   \
      env, val, name, name##_size + 1, &name##_size))              \
  name[name##_size] = '\0';

#define NAPI_ARGV_UTF8_NEW(name, i) NAPI_UTF8_NEW(name, argv[i])

#define LD_STRING_OR_BUFFER_TO_COPY(env, from, to)                         \
  char* to##Ch_ = 0;                                                       \
  size_t to##Sz_ = 0;                                                      \
  if (IsString(env, from)) {                                               \
    napi_get_value_string_utf8(env, from, NULL, 0, &to##Sz_);              \
    to##Ch_ = new char[to##Sz_ + 1];                                       \
    napi_get_value_string_utf8(env, from, to##Ch_, to##Sz_ + 1, &to##Sz_); \
    to##Ch_[to##Sz_] = '\0';                                               \
  } else if (IsBuffer(env, from)) {                                        \
    char* buf = 0;                                                         \
    napi_get_buffer_info(env, from, (void**)&buf, &to##Sz_);               \
    to##Ch_ = new char[to##Sz_];                                           \
    memcpy(to##Ch_, buf, to##Sz_);                                         \
  }

#define ASSERT_TRANSACTION_READY_CB(env, transaction, callback)              \
  if (transaction->isCommitting_ || transaction->hasCommitted_) {            \
    napi_value callback_error = CreateCodeError(                             \
        env, "TRANSACTION_COMMITTED", "Transaction is already committed");   \
    NAPI_STATUS_THROWS(CallFunction(env, callback, 1, &callback_error));     \
    NAPI_RETURN_UNDEFINED();                                                 \
  }                                                                          \
  if (transaction->isRollbacking_ || transaction->hasRollbacked_) {          \
    napi_value callback_error = CreateCodeError(                             \
        env, "TRANSACTION_ROLLBACKED", "Transaction is already rollbacked"); \
    NAPI_STATUS_THROWS(CallFunction(env, callback, 1, &callback_error));     \
    NAPI_RETURN_UNDEFINED();                                                 \
  }

#define ASSERT_TRANSACTION_READY(env, transaction)                  \
  if (transaction->isCommitting_ || transaction->hasCommitted_) {   \
    napi_throw_error(env, "TRANSACTION_COMMITTED",                  \
                     "Transaction is already committed");           \
    NAPI_RETURN_UNDEFINED();                                        \
  }                                                                 \
  if (transaction->isRollbacking_ || transaction->hasRollbacked_) { \
    napi_throw_error(env, "TRANSACTION_ROLLBACKED",                 \
                     "Transaction is already rollbacked");          \
    NAPI_RETURN_UNDEFINED();                                        \
  }

/**
 * NAPI_EXPORT_FUNCTION does not export the name of the function
 * To ensure that this overrides napi-macros.h, make sure to include this
 * header after you include <napi-macros.h>
 */
#undef NAPI_EXPORT_FUNCTION
#define NAPI_EXPORT_FUNCTION(name)                                             \
  {                                                                            \
    napi_value name##_fn;                                                      \
    NAPI_STATUS_THROWS_VOID(napi_create_function(env, #name, NAPI_AUTO_LENGTH, \
                                                 name, NULL, &name##_fn))      \
    NAPI_STATUS_THROWS_VOID(                                                   \
        napi_set_named_property(env, exports, #name, name##_fn))               \
  }

class NullLogger : public rocksdb::Logger {
 public:
  using rocksdb::Logger::Logv;
  virtual void Logv(const char* format, va_list ap) override;
  virtual size_t GetLogFileSize() const override;
};

/**
 * Helper functions
 */

/**
 * Returns true if 'value' is a string.
 */
bool IsString(napi_env env, napi_value value);

/**
 * Returns true if 'value' is a buffer.
 */
bool IsBuffer(napi_env env, napi_value value);

/**
 * Returns true if 'value' is an object.
 */
bool IsObject(napi_env env, napi_value value);

/**
 * Returns true if 'value' is an undefined.
 */
bool IsUndefined(napi_env env, napi_value value);

/**
 * Returns true if 'value' is an null.
 */
bool IsNull(napi_env env, napi_value value);

/**
 * Returns true if 'value' is an external.
 */
bool IsExternal(napi_env env, napi_value value);

/**
 * Create an error object.
 */
napi_value CreateError(napi_env env, const char* str);

napi_value CreateCodeError(napi_env env, const char* code, const char* msg);

/**
 * Returns true if 'obj' has a property 'key'.
 */
bool HasProperty(napi_env env, napi_value obj, const char* key);

/**
 * Returns a property in napi_value form.
 */
napi_value GetProperty(napi_env env, napi_value obj, const char* key);

/**
 * Returns a boolean property 'key' from 'obj'.
 * Returns 'DEFAULT' if the property doesn't exist.
 */
bool BooleanProperty(napi_env env, napi_value obj, const char* key,
                     bool DEFAULT);

/**
 * Returns true if the options object contains an encoding option that is
 * "buffer"
 */
bool EncodingIsBuffer(napi_env env, napi_value options, const char* option);

/**
 * Returns a uint32 property 'key' from 'obj'.
 * Returns 'DEFAULT' if the property doesn't exist.
 */
uint32_t Uint32Property(napi_env env, napi_value obj, const char* key,
                        uint32_t DEFAULT);

/**
 * Returns a int32 property 'key' from 'obj'.
 * Returns 'DEFAULT' if the property doesn't exist.
 */
int Int32Property(napi_env env, napi_value obj, const char* key, int DEFAULT);

/**
 * Returns a string property 'key' from 'obj'.
 * Returns empty string if the property doesn't exist.
 */
std::string StringProperty(napi_env env, napi_value obj, const char* key);

/**
 * Returns a snapshot property 'key' from 'obj'.
 * Returns `nullptr` if the property doesn't exist.
 */
const Snapshot* SnapshotProperty(napi_env env, napi_value obj, const char* key);

/**
 * Returns a transaction snapshot property 'key' from 'obj'.
 * Returns `nullptr` if the property doesn't exist.
 */
const TransactionSnapshot* TransactionSnapshotProperty(napi_env env,
                                                       napi_value obj,
                                                       const char* key);

void DisposeSliceBuffer(rocksdb::Slice slice);

/**
 * Convert a napi_value to a rocksdb::Slice.
 */
rocksdb::Slice ToSlice(napi_env env, napi_value from);

/**
 * Returns length of string or buffer
 */
size_t StringOrBufferLength(napi_env env, napi_value value);

/**
 * Takes a Buffer or string property 'name' from 'opts'.
 * Returns null if the property does not exist or is zero-length.
 */
std::string* RangeOption(napi_env env, napi_value opts, const char* name);

/**
 * Converts an array containing Buffer or string keys to a vector.
 */
std::vector<rocksdb::Slice>* KeyArray(napi_env env, napi_value arr);

/**
 * Calls a function.
 */
napi_status CallFunction(napi_env env, napi_value callback, const int argc,
                         napi_value* argv);

napi_value noop_callback(napi_env env, napi_callback_info info);
