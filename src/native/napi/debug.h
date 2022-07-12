#pragma once

#include <cstdio>

#define LOG_DEBUG(...)              \
  do {                              \
    if (is_log_debug_enabled) {     \
      fprintf(stderr, __VA_ARGS__); \
    }                               \
  } while (0)

extern bool is_log_debug_enabled;

void CheckNodeDebugNative();
