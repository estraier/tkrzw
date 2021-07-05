/*************************************************************************************************
 * Tests for tkrzw_langc.h
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

#include "tkrzw_sys_config.h"

#include <stdlib.h>
#include <stdio.h>
#include <string.h>

#include "gtest/gtest.h"
#include "gmock/gmock.h"

#include "tkrzw_file_util.h"
#include "tkrzw_langc.h"
#include "tkrzw_str_util.h"

using namespace testing;

// Main routine
int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

TEST(LangCTest, Constants) {
  EXPECT_GT(std::strlen(TKRZW_PACKAGE_VERSION), 0);
  EXPECT_GT(std::strlen(TKRZW_LIBRARY_VERSION), 0);
  EXPECT_EQ(tkrzw::INT64MIN, TKRZW_INT64MIN);
  EXPECT_EQ(tkrzw::INT64MAX, TKRZW_INT64MAX);
  EXPECT_EQ(tkrzw::Status::SUCCESS, TKRZW_STATUS_SUCCESS);
  EXPECT_EQ(tkrzw::Status::UNKNOWN_ERROR, TKRZW_STATUS_UNKNOWN_ERROR);
  EXPECT_EQ(tkrzw::Status::SYSTEM_ERROR, TKRZW_STATUS_SYSTEM_ERROR);
  EXPECT_EQ(tkrzw::Status::NOT_IMPLEMENTED_ERROR, TKRZW_STATUS_NOT_IMPLEMENTED_ERROR);
  EXPECT_EQ(tkrzw::Status::PRECONDITION_ERROR, TKRZW_STATUS_PRECONDITION_ERROR);
  EXPECT_EQ(tkrzw::Status::INVALID_ARGUMENT_ERROR, TKRZW_STATUS_INVALID_ARGUMENT_ERROR);
  EXPECT_EQ(tkrzw::Status::CANCELED_ERROR, TKRZW_STATUS_CANCELED_ERROR);
  EXPECT_EQ(tkrzw::Status::NOT_FOUND_ERROR, TKRZW_STATUS_NOT_FOUND_ERROR);
  EXPECT_EQ(tkrzw::Status::PERMISSION_ERROR, TKRZW_STATUS_PERMISSION_ERROR);
  EXPECT_EQ(tkrzw::Status::INFEASIBLE_ERROR, TKRZW_STATUS_INFEASIBLE_ERROR);
  EXPECT_EQ(tkrzw::Status::DUPLICATION_ERROR, TKRZW_STATUS_DUPLICATION_ERROR);
  EXPECT_EQ(tkrzw::Status::BROKEN_DATA_ERROR, TKRZW_STATUS_BROKEN_DATA_ERROR);
  EXPECT_EQ(tkrzw::Status::APPLICATION_ERROR, TKRZW_STATUS_APPLICATION_ERROR);
  EXPECT_STREQ("SUCCESS", tkrzw_status_code_name(TKRZW_STATUS_SUCCESS));
  EXPECT_STREQ("UNKNOWN_ERROR", tkrzw_status_code_name(TKRZW_STATUS_UNKNOWN_ERROR));
  EXPECT_STREQ("APPLICATION_ERROR", tkrzw_status_code_name(TKRZW_STATUS_APPLICATION_ERROR));
};

TEST(LangCTest, Utils) {
  EXPECT_EQ(TKRZW_STATUS_SUCCESS, tkrzw_last_status_code());
  EXPECT_STREQ("", tkrzw_last_status_message());
  EXPECT_GT(tkrzw_get_wall_time(), 0);
  EXPECT_GT(tkrzw_get_memory_capacity(), 0);
  EXPECT_GT(tkrzw_get_memory_usage(), 0);
  EXPECT_EQ(39025, tkrzw_primary_hash("foobar", -1, 65536));
  EXPECT_EQ(39025, tkrzw_primary_hash("foobar", 6, 65536));
  EXPECT_EQ(8012, tkrzw_secondary_hash("foobar", -1, 65536));
  EXPECT_EQ(8012, tkrzw_secondary_hash("foobar", 6, 65536));
}

void file_proc_check(void* arg, const char* path) {
  std::string* path_str = (std::string*)arg;
  *path_str = path;
}

TEST(LangCTest, Basic) {
  tkrzw::TemporaryDirectory tmp_dir(true, "tkrzw-");
  const std::string file_path = tmp_dir.MakeUniquePath("casket-", ".tkh");
  const std::string copy_path = tmp_dir.MakeUniquePath("casket-copy-", ".tkh");
  TkrzwDBM* dbm = tkrzw_dbm_open("casket", true, "");
  EXPECT_EQ(nullptr, dbm);
  EXPECT_EQ(TKRZW_STATUS_INVALID_ARGUMENT_ERROR, tkrzw_last_status_code());
  EXPECT_STREQ("unknown DBM class", tkrzw_last_status_message());
  dbm = tkrzw_dbm_open(file_path.c_str(), true, "truncate=true,num_buckets=10");
  ASSERT_NE(nullptr, dbm);
  EXPECT_EQ(nullptr, tkrzw_dbm_get(dbm, "", 0, nullptr));
  EXPECT_TRUE(tkrzw_dbm_set(dbm, "one", 3, "first", 5, false));
  EXPECT_FALSE(tkrzw_dbm_set(dbm, "one", 3, "1", 1, false));
  EXPECT_EQ(TKRZW_STATUS_DUPLICATION_ERROR, tkrzw_last_status_code());
  EXPECT_TRUE(tkrzw_dbm_remove(dbm, "one", 3));
  EXPECT_FALSE(tkrzw_dbm_remove(dbm, "one", 3));
  EXPECT_EQ(TKRZW_STATUS_NOT_FOUND_ERROR, tkrzw_last_status_code());
  EXPECT_TRUE(tkrzw_dbm_set(dbm, "one", 3, "first", 5, true));
  EXPECT_TRUE(tkrzw_dbm_set(dbm, "two", 3, "second", -1, true));
  EXPECT_TRUE(tkrzw_dbm_set(dbm, "three", -1, "third", -1, true));
  EXPECT_TRUE(tkrzw_dbm_append(dbm, "more", 4, "hop", 3, ":", 1));
  EXPECT_TRUE(tkrzw_dbm_append(dbm, "more", 4, "step", -1, ":", 1));
  EXPECT_TRUE(tkrzw_dbm_append(dbm, "more", -1, "jump", -1, ":", 1));
  int32_t value_size = 0;
  char* value_ptr = tkrzw_dbm_get(dbm, "one", 3, &value_size);
  ASSERT_NE(nullptr, value_ptr);
  EXPECT_EQ(5, value_size);
  EXPECT_STREQ("first", value_ptr);
  std::free(value_ptr);
  value_ptr = tkrzw_dbm_get(dbm, "more", 4, &value_size);
  ASSERT_NE(nullptr, value_ptr);
  EXPECT_EQ(13, value_size);
  EXPECT_STREQ("hop:step:jump", value_ptr);
  std::free(value_ptr);
  EXPECT_FALSE(tkrzw_dbm_compare_exchange(dbm, "color", -1, "red", -1, "green", -1));
  EXPECT_TRUE(tkrzw_dbm_compare_exchange(dbm, "color", -1, nullptr, 0, "red", -1));
  EXPECT_TRUE(tkrzw_dbm_compare_exchange(dbm, "color", -1, "red", -1, "green", -1));
  value_ptr = tkrzw_dbm_get(dbm, "color", -1, &value_size);
  ASSERT_NE(nullptr, value_ptr);
  EXPECT_STREQ("green", value_ptr);
  std::free(value_ptr);
  EXPECT_TRUE(tkrzw_dbm_compare_exchange(dbm, "color", -1, "green", -1, nullptr, 0));
  value_ptr = tkrzw_dbm_get(dbm, "color", -1, &value_size);
  EXPECT_EQ(nullptr, value_ptr);
  TkrzwKeyValuePair expected[2];
  expected[0].key_ptr = "color1";
  expected[0].key_size = -1;
  expected[0].value_ptr = nullptr;
  expected[0].value_size = -1;
  expected[1].key_ptr = "one";
  expected[1].key_size = 3;
  expected[1].value_ptr = "first";
  expected[1].value_size = 5;
  TkrzwKeyValuePair desired[2];
  desired[0].key_ptr = "color1";
  desired[0].key_size = 6;
  desired[0].value_ptr = "blue";
  desired[0].value_size = 4;
  desired[1].key_ptr = "color2";
  desired[1].key_size = -1;
  desired[1].value_ptr = "purple";
  desired[1].value_size = -1;
  EXPECT_TRUE(tkrzw_dbm_compare_exchange_multi(dbm, expected, 2, desired, 2));
  EXPECT_EQ(6, tkrzw_dbm_count(dbm));
  EXPECT_FALSE(tkrzw_dbm_compare_exchange_multi(dbm, expected, 2, desired, 2));
  expected[0].key_ptr = "color1";
  expected[0].key_size = -1;
  expected[0].value_ptr = "blue";
  expected[0].value_size = -1;
  expected[1].key_ptr = "color2";
  expected[1].key_size = -1;
  expected[1].value_ptr = "purple";
  expected[1].value_size = 6;
  desired[0].key_ptr = "color1";
  desired[0].key_size = -1;
  desired[0].value_ptr = nullptr;
  desired[0].value_size = -1;
  desired[1].key_ptr = "color2";
  desired[1].key_size = -1;
  desired[1].value_ptr = nullptr;
  desired[1].value_size = -1;
  EXPECT_TRUE(tkrzw_dbm_compare_exchange_multi(dbm, expected, 2, desired, 2));
  EXPECT_EQ(4, tkrzw_dbm_count(dbm));
  EXPECT_EQ(-1, tkrzw_dbm_increment(dbm, "num", -1, TKRZW_INT64MIN, -1));
  EXPECT_EQ(4, tkrzw_dbm_increment(dbm, "num", -1, 1, 3));
  EXPECT_EQ(6, tkrzw_dbm_increment(dbm, "num", -1, 2, 0));
  EXPECT_EQ(6, tkrzw_dbm_increment(dbm, "num", -1, TKRZW_INT64MIN, 0));
  value_ptr = tkrzw_dbm_get(dbm, "num", -1, &value_size);
  ASSERT_NE(nullptr, value_ptr);
  EXPECT_EQ(8, value_size);
  EXPECT_EQ(std::string_view("\x00\x00\x00\x00\x00\x00\x00\x06", 8),
            std::string_view(value_ptr, 8));
  std::free(value_ptr);
  EXPECT_TRUE(tkrzw_dbm_remove(dbm, "num", -1));
  EXPECT_EQ(4, tkrzw_dbm_count(dbm));
  EXPECT_GT(tkrzw_dbm_get_file_size(dbm), 0);
  char* path_ptr = tkrzw_dbm_get_file_path(dbm);
  ASSERT_NE(nullptr, path_ptr);
  EXPECT_EQ(file_path, path_ptr);
  std::free(path_ptr);
  for (int32_t i = 1; i <= 20; i++) {
    const std::string expr = tkrzw::ToString(i);
    EXPECT_TRUE(tkrzw_dbm_set(dbm, expr.data(), expr.size(), expr.data(), expr.size(), false));
  }
  EXPECT_TRUE(tkrzw_dbm_should_be_rebuilt(dbm));
  EXPECT_TRUE(tkrzw_dbm_rebuild(dbm, ""));
  EXPECT_FALSE(tkrzw_dbm_should_be_rebuilt(dbm));
  std::string path_str;
  EXPECT_TRUE(tkrzw_dbm_synchronize(dbm, false, file_proc_check, &path_str, ""));
  EXPECT_EQ(file_path, path_str);
  EXPECT_TRUE(tkrzw_dbm_copy_file_data(dbm, copy_path.c_str()));
  EXPECT_TRUE(tkrzw_dbm_clear(dbm));
  EXPECT_EQ(0, tkrzw_dbm_count(dbm));
  EXPECT_TRUE(tkrzw_dbm_close(dbm));
  dbm = tkrzw_dbm_open(copy_path.c_str(), true, "");
  ASSERT_NE(nullptr, dbm);
  EXPECT_TRUE(tkrzw_dbm_is_healthy(dbm));
  EXPECT_TRUE(tkrzw_dbm_is_writable(dbm));
  EXPECT_FALSE(tkrzw_dbm_is_ordered(dbm));
  EXPECT_EQ(24, tkrzw_dbm_count(dbm));
  char* inspect_ptr = tkrzw_dbm_inspect(dbm);
  ASSERT_NE(nullptr, inspect_ptr);
  EXPECT_NE(nullptr, strstr(inspect_ptr, "num_records"));
  EXPECT_NE(nullptr, strstr(inspect_ptr, "file_size"));
  std::free(inspect_ptr);
  TkrzwDBM* orig_dbm = tkrzw_dbm_open(file_path.c_str(), true, "no_create=true");
  ASSERT_NE(nullptr, orig_dbm);
  EXPECT_TRUE(tkrzw_dbm_is_healthy(orig_dbm));
  EXPECT_TRUE(tkrzw_dbm_is_writable(orig_dbm));
  EXPECT_EQ(0, tkrzw_dbm_count(orig_dbm));
  EXPECT_TRUE(tkrzw_dbm_export(dbm, orig_dbm));
  EXPECT_EQ(24, tkrzw_dbm_count(orig_dbm));
  EXPECT_TRUE(tkrzw_dbm_close(orig_dbm));
  EXPECT_TRUE(tkrzw_dbm_close(dbm));
}

const char* proc_increment(void* arg, const char* key_ptr, int32_t key_size,
                           const char* value_ptr, int32_t value_size, int32_t* new_value_size) {
  std::string* new_value = (std::string*)arg;
  int64_t num_value = 0;
  if (value_ptr != nullptr) {
    num_value = tkrzw::StrToInt(std::string_view(value_ptr, value_size));
  }
  *new_value = tkrzw::ToString(num_value + 1);
  *new_value_size = new_value->size();
  return new_value->data();
}

const char* proc_get(void* arg, const char* key_ptr, int32_t key_size,
                     const char* value_ptr, int32_t value_size, int32_t* new_value_size) {
  std::string* value = (std::string*)arg;
  if (value_ptr == nullptr) {
    *value = "*";
  } else {
    *value = std::string(value_ptr, value_size);
  }
  return TKRZW_REC_PROC_NOOP;
}

const char* proc_remove(void* arg, const char* key_ptr, int32_t key_size,
                        const char* value_ptr, int32_t value_size, int32_t* new_value_size) {
  return TKRZW_REC_PROC_REMOVE;
}

TEST(LangCTest, Process) {
  tkrzw::TemporaryDirectory tmp_dir(true, "tkrzw-");
  const std::string file_path = tmp_dir.MakeUniquePath("casket-", ".tkt");
  TkrzwDBM* dbm = tkrzw_dbm_open(file_path.c_str(), true, "truncate=true,num_buckets=100");
  ASSERT_NE(nullptr, dbm);
  std::string num_value;
  EXPECT_TRUE(tkrzw_dbm_process(dbm, "foo", -1, proc_increment, &num_value, true));
  EXPECT_EQ("1", num_value);
  EXPECT_TRUE(tkrzw_dbm_process(dbm, "foo", -1, proc_increment, &num_value, true));
  EXPECT_EQ("2", num_value);
  int32_t value_size = 0;
  char* value_ptr = tkrzw_dbm_get(dbm, "foo", -1, &value_size);
  ASSERT_NE(nullptr, value_ptr);
  EXPECT_EQ(1, value_size);
  EXPECT_STREQ("2", value_ptr);
  std::free(value_ptr);
  EXPECT_TRUE(tkrzw_dbm_process(dbm, "bar", -1, proc_get, &num_value, false));
  EXPECT_EQ("*", num_value);
  EXPECT_TRUE(tkrzw_dbm_process(dbm, "foo", -1, proc_get, &num_value, false));
  EXPECT_EQ("2", num_value);
  EXPECT_TRUE(tkrzw_dbm_process(dbm, "foo", -1, proc_remove, nullptr, true));
  EXPECT_TRUE(tkrzw_dbm_process(dbm, "foo", -1, proc_get, &num_value, false));
  EXPECT_EQ("*", num_value);
  TkrzwKeyProcPair kp_pairs[2];
  kp_pairs[0].key_ptr = "num1";
  kp_pairs[0].key_size = 4;
  kp_pairs[0].proc = proc_increment;
  kp_pairs[0].proc_arg = &num_value;
  kp_pairs[1].key_ptr = "num2";
  kp_pairs[1].key_size = -1;
  kp_pairs[1].proc = proc_increment;
  kp_pairs[1].proc_arg = &num_value;
  EXPECT_TRUE(tkrzw_dbm_process_multi(dbm, kp_pairs, 2, true));
  EXPECT_TRUE(tkrzw_dbm_process_multi(dbm, kp_pairs, 1, true));
  EXPECT_TRUE(tkrzw_dbm_process_each(dbm, proc_increment, &num_value, true));
  value_ptr = tkrzw_dbm_get(dbm, "num1", -1, &value_size);
  ASSERT_NE(nullptr, value_ptr);
  EXPECT_EQ(1, value_size);
  EXPECT_STREQ("3", value_ptr);
  std::free(value_ptr);
  value_ptr = tkrzw_dbm_get(dbm, "num2", -1, &value_size);
  ASSERT_NE(nullptr, value_ptr);
  EXPECT_EQ(1, value_size);
  EXPECT_STREQ("2", value_ptr);
  std::free(value_ptr);
  EXPECT_TRUE(tkrzw_dbm_close(dbm));
}

TEST(LangCTest, Iterator) {
  tkrzw::TemporaryDirectory tmp_dir(true, "tkrzw-");
  const std::string file_path = tmp_dir.MakeUniquePath("casket-", ".tkt");
  TkrzwDBM* dbm = tkrzw_dbm_open(file_path.c_str(), true, "truncate=true,num_buckets=100");
  ASSERT_NE(nullptr, dbm);
  EXPECT_TRUE(tkrzw_dbm_is_ordered(dbm));
  for (int i = 1; i <= 10; i++) {
    const std::string key = tkrzw::SPrintF("%04d", i);
    const std::string value = tkrzw::ToString(i * i);
    EXPECT_TRUE(tkrzw_dbm_set(dbm, key.c_str(), -1, value.c_str(), -1, false));
  }
  EXPECT_EQ(10, tkrzw_dbm_count(dbm));
  TkrzwDBMIter* iter = tkrzw_dbm_make_iterator(dbm);
  ASSERT_NE(nullptr, iter);
  EXPECT_TRUE(tkrzw_dbm_iter_first(iter));
  int32_t count = 0;
  while (true) {
    char* key_ptr = nullptr;
    int32_t key_size = 0;
    char* value_ptr = nullptr;
    int32_t value_size = 0;
    if (!tkrzw_dbm_iter_get(iter, &key_ptr, &key_size, &value_ptr, &value_size)) {
      EXPECT_EQ(TKRZW_STATUS_NOT_FOUND_ERROR, tkrzw_last_status_code());
      break;
    }
    count++;
    const std::string key = tkrzw::SPrintF("%04d", count);
    const std::string value = tkrzw::ToString(count * count);
    EXPECT_EQ(key, std::string_view(key_ptr, key_size));
    EXPECT_EQ(value, std::string_view(value_ptr, value_size));
    std::free(value_ptr);
    std::free(key_ptr);
    ASSERT_TRUE(tkrzw_dbm_iter_get(iter, &key_ptr, &key_size, nullptr, nullptr));
    EXPECT_EQ(key, std::string_view(key_ptr, key_size));
    std::free(key_ptr);
    ASSERT_TRUE(tkrzw_dbm_iter_get(iter, nullptr, nullptr, &value_ptr, &value_size));
    EXPECT_EQ(value, std::string_view(value_ptr, value_size));
    std::free(value_ptr);
    EXPECT_TRUE(tkrzw_dbm_iter_get(iter, nullptr, nullptr, nullptr, nullptr));
    key_ptr = tkrzw_dbm_iter_get_key(iter, &key_size);
    EXPECT_EQ(key, std::string_view(key_ptr, key_size));
    std::free(key_ptr);
    std::string num_value;
    EXPECT_TRUE(tkrzw_dbm_iter_process(iter, proc_increment, &num_value, true));
    const std::string inc_value = tkrzw::ToString(count * count + 1);
    value_ptr = tkrzw_dbm_iter_get_value(iter, &value_size);
    EXPECT_EQ(inc_value, std::string_view(value_ptr, value_size));
    std::free(value_ptr);
    EXPECT_TRUE(tkrzw_dbm_iter_next(iter));
  }
  TkrzwDBMIter* jump_iter = tkrzw_dbm_make_iterator(dbm);
  EXPECT_TRUE(tkrzw_dbm_iter_last(iter));
  count = tkrzw_dbm_count(dbm);
  while (true) {
    char* key_ptr = nullptr;
    int32_t key_size = 0;
    char* value_ptr = nullptr;
    int32_t value_size = 0;
    if (!tkrzw_dbm_iter_get(iter, &key_ptr, &key_size, &value_ptr, &value_size)) {
      EXPECT_EQ(TKRZW_STATUS_NOT_FOUND_ERROR, tkrzw_last_status_code());
      break;
    }
    const std::string key = tkrzw::SPrintF("%04d", count);
    const std::string value = tkrzw::ToString(count * count + 1);
    EXPECT_EQ(key, std::string_view(key_ptr, key_size));
    EXPECT_EQ(value, std::string_view(value_ptr, value_size));
    std::free(value_ptr);
    std::free(key_ptr);
    EXPECT_TRUE(tkrzw_dbm_iter_jump_lower(jump_iter, key.data(), key.size()));
    if (count > 1) {
      key_ptr = tkrzw_dbm_iter_get_key(jump_iter, &key_size);
      ASSERT_NE(nullptr, key_ptr);
      EXPECT_EQ(tkrzw::SPrintF("%04d", count - 1), std::string_view(key_ptr, key_size));
      free(key_ptr);
    } else {
      EXPECT_EQ(nullptr, tkrzw_dbm_iter_get_key(jump_iter, &key_size));
    }
    EXPECT_TRUE(tkrzw_dbm_iter_jump_upper(jump_iter, key.data(), key.size()));
    if (count < tkrzw_dbm_count(dbm)) {
      key_ptr = tkrzw_dbm_iter_get_key(jump_iter, &key_size);
      ASSERT_NE(nullptr, key_ptr);
      EXPECT_EQ(tkrzw::SPrintF("%04d", count + 1), std::string_view(key_ptr, key_size));
      free(key_ptr);
    } else {
      EXPECT_EQ(nullptr, tkrzw_dbm_iter_get_key(jump_iter, &key_size));
    }
    const std::string new_value = tkrzw::ToString(count * count);
    EXPECT_TRUE(tkrzw_dbm_iter_set(iter, new_value.data(), new_value.size()));
    count--;
    EXPECT_TRUE(tkrzw_dbm_iter_previous(iter));
  }
  EXPECT_EQ(0, count);
  tkrzw_dbm_iter_free(jump_iter);
  EXPECT_TRUE(tkrzw_dbm_iter_first(iter));
  count = 1;
  while (true) {
    char* key_ptr = nullptr;
    int32_t key_size = 0;
    char* value_ptr = nullptr;
    int32_t value_size = 0;
    if (!tkrzw_dbm_iter_get(iter, &key_ptr, &key_size, &value_ptr, &value_size)) {
      EXPECT_EQ(TKRZW_STATUS_NOT_FOUND_ERROR, tkrzw_last_status_code());
      break;
    }
    const std::string key = tkrzw::SPrintF("%04d", count);
    const std::string value = tkrzw::ToString(count * count);
    EXPECT_EQ(key, std::string_view(key_ptr, key_size));
    EXPECT_EQ(value, std::string_view(value_ptr, value_size));
    std::free(value_ptr);
    std::free(key_ptr);
    count++;
    EXPECT_TRUE(tkrzw_dbm_iter_remove(iter));
  }
  EXPECT_EQ(11, count);
  EXPECT_EQ(0, tkrzw_dbm_count(dbm));
  tkrzw_dbm_iter_free(iter);
  EXPECT_TRUE(tkrzw_dbm_close(dbm));
}

TEST(LangCTest, Search) {
  tkrzw::TemporaryDirectory tmp_dir(true, "tkrzw-");
  const std::string file_path = tmp_dir.MakeUniquePath("casket-", ".tkt");
  TkrzwDBM* dbm = tkrzw_dbm_open(file_path.c_str(), true, "truncate=true,num_buckets=100");
  ASSERT_NE(nullptr, dbm);
  for (int32_t i = 1; i <= 100; i++) {
    const std::string key = tkrzw::ToString(i);
    EXPECT_TRUE(tkrzw_dbm_set(dbm, key.c_str(), key.size(), key.c_str(), key.size(), false));
  }
  {
    int32_t num_keys = 0;
    TkrzwStr* keys = tkrzw_dbm_search(dbm, "contain", "1", 1, -1, false, &num_keys);
    ASSERT_NE(nullptr, keys);
    EXPECT_EQ(20, num_keys);
    tkrzw_free_str_array(keys, num_keys);
  }
  {
    int32_t num_keys = 0;
    TkrzwStr* keys = tkrzw_dbm_search(dbm, "contain", "1", -1, 10, false, &num_keys);
    ASSERT_NE(nullptr, keys);
    EXPECT_EQ(10, num_keys);
    tkrzw_free_str_array(keys, num_keys);
  }
  {
    int32_t num_keys = 0;
    TkrzwStr* keys = tkrzw_dbm_search(dbm, "edit", "10", -1, 3, false, &num_keys);
    ASSERT_NE(nullptr, keys);
    ASSERT_EQ(3, num_keys);
    EXPECT_STREQ(keys[0].ptr, "10");
    EXPECT_STREQ(keys[1].ptr, "1");
    EXPECT_STREQ(keys[2].ptr, "100");
    tkrzw_free_str_array(keys, num_keys);
  }
  EXPECT_TRUE(tkrzw_dbm_close(dbm));
}

TEST(LangCTest, RestoreDatabase) {
  tkrzw::TemporaryDirectory tmp_dir(true, "tkrzw-");
  const std::string file_path = tmp_dir.MakeUniquePath("casket-", ".tkt");
  const std::string restored_file_path = tmp_dir.MakeUniquePath("casket-restored-", ".tkt");
  constexpr int32_t num_records = 100;
  TkrzwDBM* dbm = tkrzw_dbm_open(file_path.c_str(), true, "");
  ASSERT_NE(nullptr, dbm);
  for (int32_t i = 1; i <= num_records; i++) {
    const std::string key = tkrzw::ToString(i);
    EXPECT_TRUE(tkrzw_dbm_set(dbm, key.c_str(), key.size(), key.c_str(), key.size(), false));
  }
  EXPECT_TRUE(tkrzw_dbm_close(dbm));
  EXPECT_TRUE(tkrzw_dbm_restore_database(
      file_path.c_str(), restored_file_path.c_str(), NULL, -1));
  dbm = tkrzw_dbm_open(restored_file_path.c_str(), false, "");
  ASSERT_NE(nullptr, dbm);
  EXPECT_TRUE(tkrzw_dbm_is_healthy(dbm));
  EXPECT_EQ(num_records, tkrzw_dbm_count(dbm));
  EXPECT_TRUE(tkrzw_dbm_close(dbm));
  EXPECT_EQ(tkrzw::Status::SUCCESS, tkrzw::RemoveFile(restored_file_path));
  EXPECT_EQ(tkrzw::Status::SUCCESS, tkrzw::RemoveFile(file_path));
  dbm = tkrzw_dbm_open(file_path.c_str(), true, "num_shards=3");
  ASSERT_NE(nullptr, dbm);
  for (int32_t i = 1; i <= num_records; i++) {
    const std::string key = tkrzw::ToString(i);
    EXPECT_TRUE(tkrzw_dbm_set(dbm, key.c_str(), key.size(), key.c_str(), key.size(), false));
  }
  EXPECT_TRUE(tkrzw_dbm_close(dbm));
  EXPECT_TRUE(tkrzw_dbm_restore_database(
      file_path.c_str(), restored_file_path.c_str(), NULL, -1));
  dbm = tkrzw_dbm_open(restored_file_path.c_str(), false, "num_shards=0");
  ASSERT_NE(nullptr, dbm);
  EXPECT_TRUE(tkrzw_dbm_is_healthy(dbm));
  EXPECT_EQ(num_records, tkrzw_dbm_count(dbm));
  for (int32_t i = 1; i <= num_records; i++) {
    const std::string key = tkrzw::ToString(i);
    int32_t value_size = 0;
    char* value_ptr = tkrzw_dbm_get(dbm, key.c_str(), key.size(), &value_size);
    ASSERT_NE(nullptr, value_ptr);
    free(value_ptr);
  }
  EXPECT_TRUE(tkrzw_dbm_close(dbm));
}

// END OF FILE
