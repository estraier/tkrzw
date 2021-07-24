/*************************************************************************************************
 * C interface feature checker
 *
 * Copyright 2020 Google LLC
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License.  You may obtain a copy of the License at
 *     https://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied.  See the License for the specific language governing permissions
 * and limitations under the License.
 *************************************************************************************************/

#include <stdbool.h>
#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>

#include "tkrzw_langc.h"

void print_usage() {
  fprintf(stderr, "usage: tkrzw_lang_check [path [params [iters]]]\n");
}

void print_error(const char* func) {
  TkrzwStatus status = tkrzw_get_last_status();
  fprintf(stderr, "%s failed: %s: %s\n",
          func, tkrzw_status_code_name(status.code), status.message);
}

void put_char(int32_t c) {
  putchar(c);
  fflush(stdout);
}

int32_t process(const char* path, const char* params, int32_t num_iters) {
  const int64_t start_mem_rss = tkrzw_get_memory_usage();
  TkrzwDBM* dbm = tkrzw_dbm_open(path, true, params);
  if (dbm == NULL) {
    print_error("open");
    return 1;
  }
  bool has_error = false;
  int32_t dot_mod = num_iters / 1000;
  if (dot_mod == 0) {
    dot_mod = 1;
  }
  int32_t fold_mod = num_iters / 20;
  if (fold_mod == 0) {
    fold_mod = 1;
  }
  {
    char buf[32];
    printf("Setting records: path=%s params=%s num_iterations=%d\n",
           path, params, num_iters);
    const double start_time = tkrzw_get_wall_time();
    bool midline = false;;
    for (int32_t i = 0; !has_error && i < num_iters; i++) {
      const size_t buf_size = sprintf(buf, "%08d", i);
      if (!tkrzw_dbm_set(dbm, buf, buf_size, buf, buf_size, true)) {
        print_error("set");
        has_error = true;
      }
      if ((i + 1) % dot_mod == 0) {
        put_char('.');
        midline = true;
        if ((i + 1) % fold_mod == 0) {
          printf(" (%08d)\n", i + 1);
          midline = false;
        }
      }
    }
    if (midline) {
      printf(" (%08d)\n", num_iters);
    }
    printf("Synchronizing: ... ");
    const double sync_start_time = tkrzw_get_wall_time();
    if (!tkrzw_dbm_synchronize(dbm, false, NULL, NULL, "")) {
      print_error("synchronize");
      has_error = true;
    }
    const double sync_end_time = tkrzw_get_wall_time();
    printf("done (elapsed=%.6f)\n", sync_end_time - sync_start_time);
    const double end_time = tkrzw_get_wall_time();
    const double elapsed_time = end_time - start_time;
    const int64_t mem_usage = tkrzw_get_memory_usage() - start_mem_rss;
    printf("Setting done: elapsed_time=%.6f, mem=%lld\n", elapsed_time, (long long)mem_usage);
    puts("");
  }
  {
    char key_buf[32];
    printf("Getting records: path=%s params=%s num_iterations=%d\n",
           path, params, num_iters);
    const double start_time = tkrzw_get_wall_time();
    bool midline = false;;
    for (int32_t i = 0; !has_error && i < num_iters; i++) {
      const size_t key_size = sprintf(key_buf, "%08d", i);
      int32_t value_size = 0;
      char* value_ptr = tkrzw_dbm_get(dbm, key_buf, key_size, &value_size);
      if (value_ptr) {
        free(value_ptr);
      } else {
        print_error("get");
        has_error = true;
      }
      if ((i + 1) % dot_mod == 0) {
        put_char('.');
        midline = true;
        if ((i + 1) % fold_mod == 0) {
          printf(" (%08d)\n", i + 1);
          midline = false;
        }
      }
    }
    if (midline) {
      printf(" (%08d)\n", num_iters);
    }
    const double end_time = tkrzw_get_wall_time();
    const double elapsed_time = end_time - start_time;
    const int64_t mem_usage = tkrzw_get_memory_usage() - start_mem_rss;
    printf("Getting done: elapsed_time=%.6f, mem=%lld\n", elapsed_time, (long long)mem_usage);
    puts("");
  }
  {
    printf("Iterating records: path=%s params=%s num_iterations=%d\n",
           path, params, num_iters);
    const double start_time = tkrzw_get_wall_time();
    bool midline = false;;
    TkrzwDBMIter* iter = tkrzw_dbm_make_iterator(dbm);
    if (!tkrzw_dbm_iter_first(iter)) {
      print_error("iter_first");
      has_error = true;
    }
    for (int32_t i = 0; !has_error && true; i++) {
      char* key_ptr = NULL;
      int32_t key_size = 0;
      char* value_ptr = NULL;
      int32_t value_size = 0;
      if (!tkrzw_dbm_iter_get(iter, &key_ptr, &key_size, &value_ptr, &value_size)) {
        if (tkrzw_get_last_status_code() != TKRZW_STATUS_NOT_FOUND_ERROR) {
          print_error("iter_get");
          has_error = true;
        }
        break;
      }
      free(key_ptr);
      free(value_ptr);
      if (!tkrzw_dbm_iter_next(iter)) {
        print_error("iter_next");
        has_error = true;
      }
      if ((i + 1) % dot_mod == 0) {
        put_char('.');
        midline = true;
        if ((i + 1) % fold_mod == 0) {
          printf(" (%08d)\n", i + 1);
          midline = false;
        }
      }
    }
    if (midline) {
      printf(" (%08d)\n", num_iters);
    }
    tkrzw_dbm_iter_free(iter);
    const double end_time = tkrzw_get_wall_time();
    const double elapsed_time = end_time - start_time;
    const int64_t mem_usage = tkrzw_get_memory_usage() - start_mem_rss;
    printf("Iterating done: elapsed_time=%.6f, mem=%lld\n", elapsed_time, (long long)mem_usage);
    puts("");
  }
  {
    char buf[32];
    printf("Removing records: path=%s params=%s num_iterations=%d\n",
           path, params, num_iters);
    const double start_time = tkrzw_get_wall_time();
    bool midline = false;;
    for (int32_t i = 0; !has_error && i < num_iters; i++) {
      const size_t buf_size = sprintf(buf, "%08d", i);
      if (!tkrzw_dbm_remove(dbm, buf, buf_size)) {
        print_error("set");
        has_error = true;
      }
      if ((i + 1) % dot_mod == 0) {
        put_char('.');
        midline = true;
        if ((i + 1) % fold_mod == 0) {
          printf(" (%08d)\n", i + 1);
          midline = false;
        }
      }
    }
    if (midline) {
      printf(" (%08d)\n", num_iters);
    }
    printf("Synchronizing: ... ");
    const double sync_start_time = tkrzw_get_wall_time();
    if (!tkrzw_dbm_synchronize(dbm, false, NULL, NULL, "")) {
      print_error("synchronize");
      has_error = true;
    }
    const double sync_end_time = tkrzw_get_wall_time();
    printf("done (elapsed=%.6f)\n", sync_end_time - sync_start_time);
    const double end_time = tkrzw_get_wall_time();
    const double elapsed_time = end_time - start_time;
    const int64_t mem_usage = tkrzw_get_memory_usage() - start_mem_rss;
    printf("Removing done: elapsed_time=%.6f, mem=%lld\n", elapsed_time, (long long)mem_usage);
    puts("");
  }
  if (!tkrzw_dbm_close(dbm)) {
    print_error("close");
    has_error = true;
  }
  return has_error ? 1 : 0;
}

int main(int argc, char** argv) {
  if (argc < 2 || argc > 4) {
    print_usage();
    return 1;
  }
  const char* path = argv[1];
  const char* params = argc > 2 ? argv[2] : "";
  const char* iters_expr = argc > 3 ? argv[3] : "10000";
  const int32_t num_iters = atoi(iters_expr);
  if (num_iters < 1) {
    fprintf(stderr, "invalid iters: %s\n", iters_expr);
    print_usage();
    return 1;
  }
  return process(path, params, num_iters);
}
