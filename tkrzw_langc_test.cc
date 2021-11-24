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
  EXPECT_GT(std::strlen(TKRZW_OS_NAME), 0);
  EXPECT_EQ(tkrzw::PAGE_SIZE, TKRZW_PAGE_SIZE);
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
  TkrzwStatus status = tkrzw_get_last_status();
  EXPECT_EQ(TKRZW_STATUS_SUCCESS, status.code);
  EXPECT_STREQ("", status.message);
  EXPECT_EQ(TKRZW_STATUS_SUCCESS, tkrzw_get_last_status_code());
  EXPECT_STREQ("", tkrzw_get_last_status_message());
  tkrzw_set_last_status(TKRZW_STATUS_NOT_FOUND_ERROR, NULL);
  status = tkrzw_get_last_status();
  EXPECT_EQ(TKRZW_STATUS_NOT_FOUND_ERROR, status.code);
  EXPECT_STREQ("", status.message);
  tkrzw_set_last_status(TKRZW_STATUS_APPLICATION_ERROR, "error");
  status = tkrzw_get_last_status();
  EXPECT_EQ(TKRZW_STATUS_APPLICATION_ERROR, status.code);
  EXPECT_STREQ("error", status.message);
  EXPECT_GT(tkrzw_get_wall_time(), 0);
  if (std::strcmp(TKRZW_OS_NAME, "Linux") == 0) {
    EXPECT_GT(tkrzw_get_memory_capacity(), 0);
    EXPECT_GT(tkrzw_get_memory_usage(), 0);
  }
  EXPECT_EQ(39025, tkrzw_primary_hash("foobar", -1, 65536));
  EXPECT_EQ(39025, tkrzw_primary_hash("foobar", 6, 65536));
  EXPECT_EQ(8012, tkrzw_secondary_hash("foobar", -1, 65536));
  EXPECT_EQ(8012, tkrzw_secondary_hash("foobar", 6, 65536));
  EXPECT_EQ(-1, tkrzw_str_search_regex("", "B"));
  EXPECT_EQ(-2, tkrzw_str_search_regex("", "*"));
  EXPECT_EQ(2, tkrzw_str_search_regex("ABCDEF", "CD"));
  char* result = tkrzw_str_replace_regex("ABCDEF", "CD", "XYZ");
  EXPECT_STREQ("ABXYZEF", result);
  free(result);
  result = tkrzw_str_replace_regex("ABCDEF", "123", "XYZ");
  EXPECT_STREQ("ABCDEF", result);
  free(result);
  EXPECT_EQ(2, tkrzw_str_edit_distance_lev("ABC", "B", true));
  EXPECT_EQ(1, tkrzw_str_edit_distance_lev("あいう", "あう", true));
  EXPECT_EQ(2, tkrzw_str_edit_distance_lev("ABC", "B", false));
  EXPECT_EQ(3, tkrzw_str_edit_distance_lev("あいう", "あう", false));
  int32_t esc_size = 0;
  char* esc_ptr = tkrzw_str_escape_c("a\tb\n", 4, false, &esc_size);
  EXPECT_STREQ("a\\tb\\n", esc_ptr);
  EXPECT_EQ(6, esc_size);
  int32_t unesc_size = 0;
  char* unesc_ptr = tkrzw_str_unescape_c(esc_ptr, esc_size, &unesc_size);
  EXPECT_STREQ("a\tb\n", unesc_ptr);
  EXPECT_EQ(4, unesc_size);
  free(unesc_ptr);
  free(esc_ptr);
  esc_ptr = tkrzw_str_escape_c("aBあ", -1, true, NULL);
  EXPECT_STREQ("aB\\xe3\\x81\\x82", esc_ptr);
  unesc_ptr = tkrzw_str_unescape_c(esc_ptr, -1, NULL);
  EXPECT_STREQ("aBあ", unesc_ptr);
  free(unesc_ptr);
  free(esc_ptr);
  char* append_str = tkrzw_str_append(NULL, "abc");
  EXPECT_STREQ("abc", append_str);
  append_str = tkrzw_str_append(append_str, ":");
  append_str = tkrzw_str_append(append_str, "defg");
  append_str = tkrzw_str_append(append_str, ":hijklmn");
  EXPECT_STREQ("abc:defg:hijklmn", append_str);
  free(append_str);
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
  EXPECT_EQ(TKRZW_STATUS_INVALID_ARGUMENT_ERROR, tkrzw_get_last_status_code());
  EXPECT_STREQ("unknown DBM class", tkrzw_get_last_status_message());
  dbm = tkrzw_dbm_open(file_path.c_str(), true, "truncate=true,num_buckets=10");
  ASSERT_NE(nullptr, dbm);
  EXPECT_EQ(nullptr, tkrzw_dbm_get(dbm, "", 0, nullptr));
  EXPECT_TRUE(tkrzw_dbm_set(dbm, "one", 3, "first", 5, false));
  EXPECT_FALSE(tkrzw_dbm_set(dbm, "one", 3, "1", 1, false));
  EXPECT_EQ(TKRZW_STATUS_DUPLICATION_ERROR, tkrzw_get_last_status_code());
  EXPECT_TRUE(tkrzw_dbm_check(dbm, "one", 3));
  EXPECT_TRUE(tkrzw_dbm_remove(dbm, "one", 3));
  EXPECT_FALSE(tkrzw_dbm_remove(dbm, "one", 3));
  EXPECT_FALSE(tkrzw_dbm_check(dbm, "one", -1));
  EXPECT_EQ(TKRZW_STATUS_NOT_FOUND_ERROR, tkrzw_get_last_status_code());
  TkrzwStatus status = tkrzw_get_last_status();
  EXPECT_EQ(TKRZW_STATUS_NOT_FOUND_ERROR, status.code);
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
  free(value_ptr);
  value_ptr = tkrzw_dbm_get(dbm, "more", 4, &value_size);
  ASSERT_NE(nullptr, value_ptr);
  EXPECT_EQ(13, value_size);
  EXPECT_STREQ("hop:step:jump", value_ptr);
  free(value_ptr);
  value_ptr = tkrzw_dbm_set_and_get(dbm, "zero", -1, "nil", -1, false, NULL);
  EXPECT_EQ(nullptr, value_ptr);
  EXPECT_EQ(TKRZW_STATUS_SUCCESS, tkrzw_get_last_status_code());
  value_ptr = tkrzw_dbm_set_and_get(dbm, "zero", 4, "nothing", 7, false, &value_size);
  ASSERT_NE(nullptr, value_ptr);
  EXPECT_STREQ("nil", value_ptr);
  EXPECT_EQ(3, value_size);
  free(value_ptr);
  EXPECT_EQ(TKRZW_STATUS_DUPLICATION_ERROR, tkrzw_get_last_status_code());
  value_ptr = tkrzw_dbm_remove_and_get(dbm, "zero", -1, &value_size);
  ASSERT_NE(nullptr, value_ptr);
  EXPECT_STREQ("nil", value_ptr);
  EXPECT_EQ(3, value_size);
  free(value_ptr);
  EXPECT_EQ(TKRZW_STATUS_SUCCESS, tkrzw_get_last_status_code());
  value_ptr = tkrzw_dbm_remove_and_get(dbm, "zero", -1, NULL);
  ASSERT_EQ(nullptr, value_ptr);
  EXPECT_EQ(TKRZW_STATUS_NOT_FOUND_ERROR, tkrzw_get_last_status_code());
  TkrzwKeyValuePair set_records[2];
  set_records[0].key_ptr = "brother";
  set_records[0].key_size = 7;
  set_records[0].value_ptr = "niisan";
  set_records[0].value_size = 6;
  set_records[1].key_ptr = "sister";
  set_records[1].key_size = -1;
  set_records[1].value_ptr = "neesan";
  set_records[1].value_size = -1;
  EXPECT_TRUE(tkrzw_dbm_set_multi(dbm, set_records, 2, true));
  EXPECT_TRUE(tkrzw_dbm_append_multi(dbm, set_records, 2, ":", -1));
  TkrzwStr get_keys[3];
  get_keys[0].ptr = "brother";
  get_keys[0].size = 7;
  get_keys[1].ptr = "sister";
  get_keys[1].size = -1;
  get_keys[2].ptr = "pompom";
  get_keys[2].size = -1;
  int32_t num_get_keys = 0;
  TkrzwKeyValuePair* get_records = tkrzw_dbm_get_multi(dbm, get_keys, 3, &num_get_keys);
  ASSERT_NE(nullptr, get_records);
  ASSERT_EQ(2, num_get_keys);
  EXPECT_STREQ("brother", get_records[0].key_ptr);
  EXPECT_EQ(7, get_records[0].key_size);
  EXPECT_STREQ("niisan:niisan", get_records[0].value_ptr);
  EXPECT_EQ(13, get_records[0].value_size);
  EXPECT_STREQ("sister", get_records[1].key_ptr);
  EXPECT_EQ(6, get_records[1].key_size);
  EXPECT_STREQ("neesan:neesan", get_records[1].value_ptr);
  EXPECT_EQ(13, get_records[1].value_size);
  tkrzw_free_str_map(get_records, num_get_keys);
  EXPECT_TRUE(tkrzw_dbm_remove_multi(dbm, get_keys, 2));
  EXPECT_FALSE(tkrzw_dbm_compare_exchange(dbm, "color", -1, "red", -1, "green", -1));
  EXPECT_TRUE(tkrzw_dbm_compare_exchange(dbm, "color", -1, nullptr, 0, "red", -1));
  EXPECT_TRUE(tkrzw_dbm_compare_exchange(dbm, "color", -1, "red", -1, "green", -1));
  value_ptr = tkrzw_dbm_get(dbm, "color", -1, &value_size);
  ASSERT_NE(nullptr, value_ptr);
  EXPECT_STREQ("green", value_ptr);
  free(value_ptr);
  EXPECT_TRUE(tkrzw_dbm_compare_exchange(dbm, "color", -1, "green", -1, nullptr, 0));
  value_ptr = tkrzw_dbm_get(dbm, "color", -1, &value_size);
  EXPECT_EQ(nullptr, value_ptr);
  EXPECT_FALSE(tkrzw_dbm_compare_exchange(dbm, "xyz", 3, TKRZW_ANY_DATA, 0, "abc", -1));
  EXPECT_TRUE(tkrzw_dbm_compare_exchange(dbm, "xyz", 3, nullptr, 0, "abc", -1));
  EXPECT_TRUE(tkrzw_dbm_compare_exchange(dbm, "xyz", 3, TKRZW_ANY_DATA, 0, "def", -1));
  EXPECT_TRUE(tkrzw_dbm_compare_exchange(dbm, "xyz", 3, TKRZW_ANY_DATA, -1, TKRZW_ANY_DATA, -1));
  value_ptr = tkrzw_dbm_get(dbm, "xyz", -1, &value_size);
  ASSERT_NE(nullptr, value_ptr);
  EXPECT_STREQ("def", value_ptr);
  free(value_ptr);
  EXPECT_TRUE(tkrzw_dbm_compare_exchange(dbm, "xyz", 3, TKRZW_ANY_DATA, 0, nullptr, 0));
  value_ptr = tkrzw_dbm_get(dbm, "xyz", -1, &value_size);
  EXPECT_EQ(nullptr, value_ptr);
  int32_t actual_size = 0;
  char* actual_ptr = tkrzw_dbm_compare_exchange_and_get(
      dbm, "xyz", -1, nullptr, 0, "123", -1, &actual_size);
  EXPECT_EQ(nullptr, actual_ptr);
  EXPECT_EQ(TKRZW_STATUS_SUCCESS, tkrzw_get_last_status_code());
  actual_ptr = tkrzw_dbm_compare_exchange_and_get(
      dbm, "xyz", -1, "123", -1, TKRZW_ANY_DATA, 0, &actual_size);
  EXPECT_STREQ("123", actual_ptr);
  EXPECT_EQ(3, actual_size);
  free(actual_ptr);
  EXPECT_EQ(TKRZW_STATUS_SUCCESS, tkrzw_get_last_status_code());
  actual_ptr = tkrzw_dbm_compare_exchange_and_get(
      dbm, "xyz", -1, TKRZW_ANY_DATA, 0, nullptr, 0, &actual_size);
  EXPECT_STREQ("123", actual_ptr);
  EXPECT_EQ(3, actual_size);
  free(actual_ptr);
  EXPECT_EQ(TKRZW_STATUS_SUCCESS, tkrzw_get_last_status_code());
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
  expected[0].key_ptr = "xyz";
  expected[0].key_size = -1;
  expected[0].value_ptr = TKRZW_ANY_DATA;
  expected[0].value_size = -1;
  desired[0].key_ptr = "xyz";
  desired[0].key_size = -1;
  desired[0].value_ptr = "abc";
  desired[0].value_size = -1;
  EXPECT_FALSE(tkrzw_dbm_compare_exchange_multi(dbm, expected, 1, desired, 1));
  expected[0].value_ptr = nullptr;
  EXPECT_TRUE(tkrzw_dbm_compare_exchange_multi(dbm, expected, 1, desired, 1));
  expected[0].value_ptr = TKRZW_ANY_DATA;
  desired[0].value_ptr = "def";
  EXPECT_TRUE(tkrzw_dbm_compare_exchange_multi(dbm, expected, 1, desired, 1));
  value_ptr = tkrzw_dbm_get(dbm, "xyz", -1, &value_size);
  ASSERT_NE(nullptr, value_ptr);
  EXPECT_STREQ("def", value_ptr);
  free(value_ptr);
  desired[0].value_ptr = nullptr;
  EXPECT_TRUE(tkrzw_dbm_compare_exchange_multi(dbm, expected, 1, desired, 1));
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
  free(value_ptr);
  EXPECT_TRUE(tkrzw_dbm_remove(dbm, "num", -1));
  EXPECT_EQ(4, tkrzw_dbm_count(dbm));
  EXPECT_GT(tkrzw_dbm_get_file_size(dbm), 0);
  char* path_ptr = tkrzw_dbm_get_file_path(dbm);
  EXPECT_GT(tkrzw_dbm_get_timestamp(dbm), 0);
  ASSERT_NE(nullptr, path_ptr);
  EXPECT_EQ(file_path, path_ptr);
  free(path_ptr);
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
  EXPECT_TRUE(tkrzw_dbm_copy_file_data(dbm, copy_path.c_str(), false));
  EXPECT_TRUE(tkrzw_dbm_clear(dbm));
  EXPECT_EQ(0, tkrzw_dbm_count(dbm));
  EXPECT_TRUE(tkrzw_dbm_close(dbm));
  dbm = tkrzw_dbm_open(copy_path.c_str(), true, "");
  ASSERT_NE(nullptr, dbm);
  EXPECT_TRUE(tkrzw_dbm_is_healthy(dbm));
  EXPECT_TRUE(tkrzw_dbm_is_writable(dbm));
  EXPECT_FALSE(tkrzw_dbm_is_ordered(dbm));
  EXPECT_EQ(24, tkrzw_dbm_count(dbm));
  int32_t num_insp_records = 0;
  TkrzwKeyValuePair* insp_records = tkrzw_dbm_inspect(dbm, &num_insp_records);
  EXPECT_GE(num_insp_records, 2);
  TkrzwKeyValuePair* insp_elem =
      tkrzw_search_str_map(insp_records, num_insp_records, "num_records", -1);
  ASSERT_NE(nullptr, insp_elem);
  EXPECT_STREQ("num_records", insp_elem->key_ptr);
  EXPECT_STREQ("24", insp_elem->value_ptr);
  insp_elem = tkrzw_search_str_map(insp_records, num_insp_records, "class", 5);
  ASSERT_NE(nullptr, insp_elem);
  EXPECT_STREQ("class", insp_elem->key_ptr);
  EXPECT_STREQ("HashDBM", insp_elem->value_ptr);
  tkrzw_free_str_map(insp_records, num_insp_records);
  EXPECT_TRUE(tkrzw_dbm_set(dbm, "zero", 4, "foo", 3, false));
  EXPECT_TRUE(tkrzw_dbm_rekey(dbm, "zero", 4, "one", 3, true, false));
  EXPECT_FALSE(tkrzw_dbm_rekey(dbm, "zero", 4, "one", 3, true, false));
  value_ptr = tkrzw_dbm_get(dbm, "one", 3, &value_size);
  ASSERT_NE(nullptr, value_ptr);
  EXPECT_EQ(3, value_size);
  EXPECT_STREQ("foo", value_ptr);
  free(value_ptr);
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
  free(value_ptr);
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
  free(value_ptr);
  value_ptr = tkrzw_dbm_get(dbm, "num2", -1, &value_size);
  ASSERT_NE(nullptr, value_ptr);
  EXPECT_EQ(1, value_size);
  EXPECT_STREQ("2", value_ptr);
  free(value_ptr);
  int32_t count = tkrzw_dbm_count(dbm);
  while (tkrzw_dbm_process_first(dbm, proc_remove, nullptr, true)) {
    count--;
  }
  EXPECT_EQ(0, count);
  EXPECT_EQ(0, tkrzw_dbm_count(dbm));
  EXPECT_TRUE(tkrzw_dbm_close(dbm));
}

TEST(LangCTest, Iterator) {
  tkrzw::TemporaryDirectory tmp_dir(true, "tkrzw-");
  const std::string file_path = tmp_dir.MakeUniquePath("casket-", ".tkt");
  TkrzwDBM* dbm = tkrzw_dbm_open(file_path.c_str(), true, "truncate=true,num_buckets=100");
  ASSERT_NE(nullptr, dbm);
  EXPECT_TRUE(tkrzw_dbm_is_ordered(dbm));
  for (int32_t i = 1; i <= 10; i++) {
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
      EXPECT_EQ(TKRZW_STATUS_NOT_FOUND_ERROR, tkrzw_get_last_status_code());
      break;
    }
    count++;
    const std::string key = tkrzw::SPrintF("%04d", count);
    const std::string value = tkrzw::ToString(count * count);
    EXPECT_EQ(key, std::string_view(key_ptr, key_size));
    EXPECT_EQ(value, std::string_view(value_ptr, value_size));
    free(value_ptr);
    free(key_ptr);
    ASSERT_TRUE(tkrzw_dbm_iter_get(iter, &key_ptr, &key_size, nullptr, nullptr));
    EXPECT_EQ(key, std::string_view(key_ptr, key_size));
    free(key_ptr);
    ASSERT_TRUE(tkrzw_dbm_iter_get(iter, nullptr, nullptr, &value_ptr, &value_size));
    EXPECT_EQ(value, std::string_view(value_ptr, value_size));
    free(value_ptr);
    EXPECT_TRUE(tkrzw_dbm_iter_get(iter, nullptr, nullptr, nullptr, nullptr));
    key_ptr = tkrzw_dbm_iter_get_key(iter, &key_size);
    EXPECT_EQ(key, std::string_view(key_ptr, key_size));
    free(key_ptr);
    std::string num_value;
    EXPECT_TRUE(tkrzw_dbm_iter_process(iter, proc_increment, &num_value, true));
    const std::string inc_value = tkrzw::ToString(count * count + 1);
    value_ptr = tkrzw_dbm_iter_get_value(iter, &value_size);
    EXPECT_EQ(inc_value, std::string_view(value_ptr, value_size));
    free(value_ptr);
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
      EXPECT_EQ(TKRZW_STATUS_NOT_FOUND_ERROR, tkrzw_get_last_status_code());
      break;
    }
    const std::string key = tkrzw::SPrintF("%04d", count);
    const std::string value = tkrzw::ToString(count * count + 1);
    EXPECT_EQ(key, std::string_view(key_ptr, key_size));
    EXPECT_EQ(value, std::string_view(value_ptr, value_size));
    free(value_ptr);
    free(key_ptr);
    EXPECT_TRUE(tkrzw_dbm_iter_jump_lower(jump_iter, key.data(), key.size(), false));
    if (count > 1) {
      key_ptr = tkrzw_dbm_iter_get_key(jump_iter, &key_size);
      ASSERT_NE(nullptr, key_ptr);
      EXPECT_EQ(tkrzw::SPrintF("%04d", count - 1), std::string_view(key_ptr, key_size));
      free(key_ptr);
    } else {
      EXPECT_EQ(nullptr, tkrzw_dbm_iter_get_key(jump_iter, &key_size));
    }
    EXPECT_TRUE(tkrzw_dbm_iter_jump_upper(jump_iter, key.data(), key.size(), false));
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
      EXPECT_EQ(TKRZW_STATUS_NOT_FOUND_ERROR, tkrzw_get_last_status_code());
      break;
    }
    const std::string key = tkrzw::SPrintF("%04d", count);
    const std::string value = tkrzw::ToString(count * count);
    EXPECT_EQ(key, std::string_view(key_ptr, key_size));
    EXPECT_EQ(value, std::string_view(value_ptr, value_size));
    free(value_ptr);
    free(key_ptr);
    count++;
    EXPECT_TRUE(tkrzw_dbm_iter_remove(iter));
  }
  EXPECT_EQ(11, count);
  EXPECT_EQ(0, tkrzw_dbm_count(dbm));
  EXPECT_TRUE(tkrzw_dbm_set(dbm, "one", -1, "first", -1, false));
  EXPECT_TRUE(tkrzw_dbm_set(dbm, "two", -1, "second", -1, false));
  EXPECT_TRUE(tkrzw_dbm_set(dbm, "three", -1, "third", -1, false));
  int32_t step_count = 0;
  EXPECT_TRUE(tkrzw_dbm_iter_first(iter));
  while (true) {
    char* key_ptr = nullptr;
    int32_t key_size = 0;
    char* value_ptr = nullptr;
    int32_t value_size = 0;
    if (!tkrzw_dbm_iter_step(iter, &key_ptr, &key_size, &value_ptr, &value_size)) {
      EXPECT_EQ(TKRZW_STATUS_NOT_FOUND_ERROR, tkrzw_get_last_status_code());
      break;
    }
    EXPECT_EQ(TKRZW_STATUS_SUCCESS, tkrzw_get_last_status_code());
    free(value_ptr);
    free(key_ptr);
    step_count++;
  }
  EXPECT_EQ(tkrzw_dbm_count(dbm), step_count);
  int32_t pop_count = 0;
  while (true) {
    char* key_ptr = nullptr;
    int32_t key_size = 0;
    char* value_ptr = nullptr;
    int32_t value_size = 0;
    if (!tkrzw_dbm_pop_first(dbm, &key_ptr, &key_size, &value_ptr, &value_size)) {
      EXPECT_EQ(TKRZW_STATUS_NOT_FOUND_ERROR, tkrzw_get_last_status_code());
      break;
    }
    EXPECT_EQ(TKRZW_STATUS_SUCCESS, tkrzw_get_last_status_code());
    free(value_ptr);
    free(key_ptr);
    pop_count++;
  }
  EXPECT_EQ(step_count, pop_count);
  EXPECT_EQ(0, tkrzw_dbm_count(dbm));


  EXPECT_TRUE(tkrzw_dbm_push_last(dbm, "foo", -1, 0));

  char* key_ptr = nullptr;
  int32_t key_size = 0;
  char* value_ptr = nullptr;
  int32_t value_size = 0;
  EXPECT_TRUE(tkrzw_dbm_pop_first(dbm, &key_ptr, &key_size, &value_ptr, &value_size));
  EXPECT_EQ(TKRZW_STATUS_SUCCESS, tkrzw_get_last_status_code());

  EXPECT_EQ(std::string_view("\0\0\0\0\0\0\0\0", 8), std::string_view(key_ptr, key_size));


  free(value_ptr);
  free(key_ptr);






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
    TkrzwStr* keys = tkrzw_dbm_search(dbm, "contain", "1", 1, -1, &num_keys);
    ASSERT_NE(nullptr, keys);
    EXPECT_EQ(20, num_keys);
    tkrzw_free_str_array(keys, num_keys);
  }
  {
    int32_t num_keys = 0;
    TkrzwStr* keys = tkrzw_dbm_search(dbm, "contain", "1", -1, 10, &num_keys);
    ASSERT_NE(nullptr, keys);
    EXPECT_EQ(10, num_keys);
    tkrzw_free_str_array(keys, num_keys);
  }
  {
    int32_t num_keys = 0;
    TkrzwStr* keys = tkrzw_dbm_search(dbm, "edit", "10", -1, 3, &num_keys);
    ASSERT_NE(nullptr, keys);
    ASSERT_EQ(3, num_keys);
    EXPECT_STREQ(keys[0].ptr, "10");
    EXPECT_STREQ(keys[1].ptr, "1");
    EXPECT_STREQ(keys[2].ptr, "100");
    tkrzw_free_str_array(keys, num_keys);
  }
  {
    int32_t num_keys = 0;
    TkrzwStr* keys = tkrzw_dbm_search(dbm, "upperinc", "5", -1, 3, &num_keys);
    ASSERT_NE(nullptr, keys);
    ASSERT_EQ(3, num_keys);
    EXPECT_STREQ(keys[0].ptr, "5");
    EXPECT_STREQ(keys[1].ptr, "50");
    EXPECT_STREQ(keys[2].ptr, "51");
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

TEST(LangCTest, Sharding) {
  constexpr int32_t num_shards = 3;
  constexpr int32_t num_records = 20;
  tkrzw::TemporaryDirectory tmp_dir(true, "tkrzw-");
  const std::string file_path = tmp_dir.MakeUniquePath("casket-", ".tkh");
  const std::string restored_file_path = tmp_dir.MakeUniquePath("casket-restored-", ".tkh");
  std::string params = "truncate=true";
  params += ",num_shards=" + tkrzw::ToString(num_shards);
  TkrzwDBM* dbm = tkrzw_dbm_open(file_path.c_str(), true, params.c_str());
  ASSERT_NE(nullptr, dbm);
  EXPECT_TRUE(tkrzw_dbm_is_healthy(dbm));
  EXPECT_TRUE(tkrzw_dbm_is_writable(dbm));
  for (int32_t i = 0; i < num_shards; i++) {
    const std::string shard_path = tkrzw::SPrintF(
        "%s-%05d-of-%05d", file_path.c_str(),i, num_shards);
    EXPECT_TRUE(tkrzw::PathIsFile(shard_path));
  }
  for (int32_t i = 0; i < num_records; i++) {
    const std::string key = tkrzw::ToString(i);
    EXPECT_TRUE(tkrzw_dbm_set(dbm, key.data(), key.size(), key.data(), key.size(), true));
  }
  EXPECT_EQ(num_records, tkrzw_dbm_count(dbm));
  EXPECT_TRUE(tkrzw_dbm_close(dbm));
  EXPECT_TRUE(tkrzw_dbm_restore_database(
      file_path.c_str(), restored_file_path.c_str(), NULL, -1));
  for (int32_t i = 0; i < num_shards; i++) {
    const std::string shard_path = tkrzw::SPrintF(
        "%s-%05d-of-%05d", restored_file_path.c_str(),i, num_shards);
    EXPECT_TRUE(tkrzw::PathIsFile(shard_path));
  }
  dbm = tkrzw_dbm_open(restored_file_path.c_str(), false, params.c_str());
  ASSERT_NE(nullptr, dbm);
  for (int32_t i = 0; i < num_records; i++) {
    const std::string key = tkrzw::ToString(i);
    int32_t value_size = 0;
    char* value_ptr = tkrzw_dbm_get(dbm, key.data(), key.size(), &value_size);
    ASSERT_NE(nullptr, value_ptr);
    EXPECT_EQ(key, std::string_view(value_ptr, value_size));
    free(value_ptr);
  }
  EXPECT_EQ(num_records, tkrzw_dbm_count(dbm));
  EXPECT_TRUE(tkrzw_dbm_close(dbm));
}

TEST(LangCTest, Export) {
  tkrzw::TemporaryDirectory tmp_dir(true, "tkrzw-");
  const std::string file_path = tmp_dir.MakeUniquePath("casket-", ".tkh");
  const std::string copy_path = tmp_dir.MakeUniquePath("casket-copy-", ".tkh");
  TkrzwDBM* dbm = tkrzw_dbm_open(file_path.c_str(), true, "truncate=true");
  ASSERT_NE(nullptr, dbm);
  EXPECT_TRUE(tkrzw_dbm_set(dbm, "ichi", -1, "first", -1, false));
  EXPECT_TRUE(tkrzw_dbm_set(dbm, "ni", -1, "second", -1, false));
  TkrzwFile* file = tkrzw_file_open(copy_path.c_str(), true, "truncate=true");
  ASSERT_NE(nullptr, file);
  EXPECT_TRUE(tkrzw_dbm_export_to_flat_records(dbm, file));
  EXPECT_EQ(25, tkrzw_file_get_size(file));
  EXPECT_TRUE(tkrzw_dbm_clear(dbm));
  EXPECT_EQ(0, tkrzw_dbm_count(dbm));
  EXPECT_TRUE(tkrzw_dbm_import_from_flat_records(dbm, file));
  EXPECT_EQ(2, tkrzw_dbm_count(dbm));
  EXPECT_TRUE(tkrzw_file_close(file));
  file = tkrzw_file_open(copy_path.c_str(), true, "truncate=true");
  ASSERT_NE(nullptr, file);
  EXPECT_TRUE(tkrzw_dbm_export_keys_as_lines(dbm, file));
  EXPECT_TRUE(tkrzw_file_close(file));
  std::string content;
  EXPECT_EQ(tkrzw::Status::SUCCESS, tkrzw::ReadFile(copy_path, &content));
  EXPECT_EQ("ichi\nni\n", content);
  EXPECT_TRUE(tkrzw_dbm_close(dbm));
}

TEST(LangCTest, Async) {
  tkrzw::TemporaryDirectory tmp_dir(true, "tkrzw-");
  const std::string file_path = tmp_dir.MakeUniquePath("casket-", ".tkh");
  const std::string copy_path = tmp_dir.MakeUniquePath("casket-copy-", ".tkh");
  TkrzwDBM* dbm = tkrzw_dbm_open(file_path.c_str(), true, "truncate=true,num_buckets=100");
  ASSERT_NE(nullptr, dbm);
  TkrzwAsyncDBM* async = tkrzw_async_dbm_new(dbm, 4);
  for (int32_t i = 1; i <= 100; i++) {
    const std::string key = tkrzw::SPrintF("%04d", i);
    const std::string value = tkrzw::ToString(i * i);
    TkrzwFuture* set_future = tkrzw_async_dbm_set(
        async, key.data(), key.size(), value.data(), value.size(), true);
    if (i % 3 == 0) {
      EXPECT_TRUE(tkrzw_future_wait(set_future, -1));
    }
    tkrzw_future_get(set_future);
    EXPECT_EQ(TKRZW_STATUS_SUCCESS, tkrzw_get_last_status_code());
    tkrzw_future_free(set_future);
    TkrzwFuture* get_future = tkrzw_async_dbm_get(async, key.data(), key.size());
    if (i % 3 == 0) {
      EXPECT_TRUE(tkrzw_future_wait(get_future, -1));
    }
    int32_t value_size = 0;
    char* value_ptr = tkrzw_future_get_str(get_future, &value_size);
    EXPECT_EQ(TKRZW_STATUS_SUCCESS, tkrzw_get_last_status_code());
    EXPECT_STREQ(value.c_str(), value_ptr);
    EXPECT_EQ(value.size(), value_size);
    free(value_ptr);
    tkrzw_future_free(get_future);
    if (i % 2 == 0) {
      TkrzwFuture* remove_future = tkrzw_async_dbm_remove(async, key.data(), key.size());
      if (i % 3 == 0) {
        EXPECT_TRUE(tkrzw_future_wait(remove_future, -1));
      }
      tkrzw_future_get(remove_future);
      EXPECT_EQ(TKRZW_STATUS_SUCCESS, tkrzw_get_last_status_code());
      tkrzw_future_free(remove_future);
      TkrzwFuture* incr_future = tkrzw_async_dbm_increment(async, key.data(), key.size(), 1, 100);
      if (i % 3 == 0) {
        EXPECT_TRUE(tkrzw_future_wait(incr_future, -1));
      }
      const int64_t num = tkrzw_future_get_int(incr_future);
      EXPECT_EQ(TKRZW_STATUS_SUCCESS, tkrzw_get_last_status_code());
      EXPECT_EQ(101, num);
      tkrzw_future_free(incr_future);
    }
  }
  EXPECT_EQ(100, tkrzw_dbm_count(dbm));
  TkrzwKeyValuePair set_records[2];
  set_records[0].key_ptr = "tako";
  set_records[0].key_size = 4;
  set_records[0].value_ptr = "ika";
  set_records[0].value_size = 3;
  set_records[1].key_ptr = "uni";
  set_records[1].key_size = -1;
  set_records[1].value_ptr = "kani";
  set_records[1].value_size = -1;
  TkrzwFuture* set_future = tkrzw_async_dbm_set_multi(async, set_records, 2, true);
  EXPECT_TRUE(tkrzw_future_wait(set_future, -1));
  tkrzw_future_get(set_future);
  EXPECT_EQ(TKRZW_STATUS_SUCCESS, tkrzw_get_last_status_code());
  tkrzw_future_free(set_future);
  TkrzwFuture* append_future = tkrzw_async_dbm_append_multi(async, set_records, 2, ":", -1);
  EXPECT_TRUE(tkrzw_future_wait(append_future, -1));
  tkrzw_future_get(append_future);
  EXPECT_EQ(TKRZW_STATUS_SUCCESS, tkrzw_get_last_status_code());
  tkrzw_future_free(append_future);
  TkrzwStr get_keys[2];
  get_keys[0].ptr = "tako";
  get_keys[0].size = -1;
  get_keys[1].ptr = "uni";
  get_keys[1].size = -3;
  TkrzwFuture* get_future = tkrzw_async_dbm_get_multi(async, get_keys, 2);
  EXPECT_TRUE(tkrzw_future_wait(get_future, -1));
  int32_t num_get_records = 0;
  TkrzwKeyValuePair* get_records = tkrzw_future_get_str_map(get_future, &num_get_records);
  EXPECT_EQ(TKRZW_STATUS_SUCCESS, tkrzw_get_last_status_code());
  EXPECT_EQ(2, num_get_records);
  TkrzwKeyValuePair* rec1 = tkrzw_search_str_map(get_records, num_get_records, "tako", -1);
  ASSERT_NE(nullptr, rec1);
  EXPECT_STREQ("ika:ika", rec1->value_ptr);
  tkrzw_free_str_map(get_records, num_get_records);
  tkrzw_future_free(get_future);
  TkrzwFuture* remove_future = tkrzw_async_dbm_remove_multi(async, get_keys, 2);
  EXPECT_TRUE(tkrzw_future_wait(remove_future, -1));
  tkrzw_future_get(remove_future);
  EXPECT_EQ(TKRZW_STATUS_SUCCESS, tkrzw_get_last_status_code());
  tkrzw_future_free(remove_future);
  EXPECT_EQ(100, tkrzw_dbm_count(dbm));
  append_future = tkrzw_async_dbm_append(async, "ebi", -1, "ikura", -1, ":", -1);
  EXPECT_TRUE(tkrzw_future_wait(append_future, -1));
  tkrzw_future_get(append_future);
  EXPECT_EQ(TKRZW_STATUS_SUCCESS, tkrzw_get_last_status_code());
  tkrzw_future_free(append_future);
  append_future = tkrzw_async_dbm_append(async, "ebi", 3, "hoya", 4, ":", 1);
  EXPECT_TRUE(tkrzw_future_wait(append_future, -1));
  tkrzw_future_get(append_future);
  EXPECT_EQ(TKRZW_STATUS_SUCCESS, tkrzw_get_last_status_code());
  tkrzw_future_free(append_future);
  TkrzwFuture* cas_future = tkrzw_async_dbm_compare_exchange(
      async, "ebi", -1, "ikura:hoya", -1, "sushi", -1);
  tkrzw_future_get(cas_future);
  EXPECT_EQ(TKRZW_STATUS_SUCCESS, tkrzw_get_last_status_code());
  tkrzw_future_free(cas_future);
  cas_future = tkrzw_async_dbm_compare_exchange(async, "ebi", 3, "poke", 4, "sushi", 4);
  tkrzw_future_get(cas_future);
  EXPECT_EQ(TKRZW_STATUS_INFEASIBLE_ERROR, tkrzw_get_last_status_code());
  tkrzw_future_free(cas_future);
  cas_future = tkrzw_async_dbm_compare_exchange(
      async, "xyz", -1, TKRZW_ANY_DATA, 0, TKRZW_ANY_DATA, -1);
  tkrzw_future_get(cas_future);
  EXPECT_EQ(TKRZW_STATUS_INFEASIBLE_ERROR, tkrzw_get_last_status_code());
  tkrzw_future_free(cas_future);
  cas_future = tkrzw_async_dbm_compare_exchange(
      async, "xyz", -1, nullptr, 0, "abc", -1);
  tkrzw_future_get(cas_future);
  EXPECT_EQ(TKRZW_STATUS_SUCCESS, tkrzw_get_last_status_code());
  tkrzw_future_free(cas_future);
  cas_future = tkrzw_async_dbm_compare_exchange(
      async, "xyz", -1, TKRZW_ANY_DATA, 0, "def", -1);
  tkrzw_future_get(cas_future);
  EXPECT_EQ(TKRZW_STATUS_SUCCESS, tkrzw_get_last_status_code());
  tkrzw_future_free(cas_future);
  cas_future = tkrzw_async_dbm_compare_exchange(
      async, "xyz", -1, TKRZW_ANY_DATA, 0, nullptr, 0);
  tkrzw_future_get(cas_future);
  EXPECT_EQ(TKRZW_STATUS_SUCCESS, tkrzw_get_last_status_code());
  tkrzw_future_free(cas_future);
  TkrzwKeyValuePair expected[2];
  expected[0].key_ptr = "tako";
  expected[0].key_size = -1;
  expected[0].value_ptr = nullptr;
  expected[0].value_size = -1;
  expected[1].key_ptr = "uni";
  expected[1].key_size = -1;
  expected[1].value_ptr = nullptr;
  expected[1].value_size = -1;
  TkrzwKeyValuePair desired[2];
  desired[0].key_ptr = "tako";
  desired[0].key_size = 4;
  desired[0].value_ptr = "ika";
  desired[0].value_size = 3;
  desired[1].key_ptr = "uni";
  desired[1].key_size = 3;
  desired[1].value_ptr = "kani";
  desired[1].value_size = 4;
  cas_future = tkrzw_async_dbm_compare_exchange_multi(
      async, expected, 2, desired, 2);
  tkrzw_future_get(cas_future);
  EXPECT_EQ(TKRZW_STATUS_SUCCESS, tkrzw_get_last_status_code());
  tkrzw_future_free(cas_future);
  expected[0].key_ptr = "xyz";
  expected[0].key_size = -1;
  expected[0].value_ptr = TKRZW_ANY_DATA;
  expected[0].value_size = 0;
  desired[0].key_ptr = "xyz";
  desired[0].key_size = -1;
  desired[0].value_ptr = TKRZW_ANY_DATA;
  desired[0].value_size = 0;
  cas_future = tkrzw_async_dbm_compare_exchange_multi(async, expected, 1, desired, 1);
  tkrzw_future_get(cas_future);
  EXPECT_EQ(TKRZW_STATUS_INFEASIBLE_ERROR, tkrzw_get_last_status_code());
  tkrzw_future_free(cas_future);
  expected[0].key_ptr = "xyz";
  expected[0].key_size = -1;
  expected[0].value_ptr = nullptr;
  expected[0].value_size = 0;
  desired[0].key_ptr = "xyz";
  desired[0].key_size = -1;
  desired[0].value_ptr = "abc";
  desired[0].value_size = -1;
  cas_future = tkrzw_async_dbm_compare_exchange_multi(async, expected, 1, desired, 1);
  tkrzw_future_get(cas_future);
  EXPECT_EQ(TKRZW_STATUS_SUCCESS, tkrzw_get_last_status_code());
  tkrzw_future_free(cas_future);
  expected[0].key_ptr = "xyz";
  expected[0].key_size = -1;
  expected[0].value_ptr = TKRZW_ANY_DATA;
  expected[0].value_size = 0;
  desired[0].key_ptr = "xyz";
  desired[0].key_size = -1;
  desired[0].value_ptr = "def";
  desired[0].value_size = -1;
  cas_future = tkrzw_async_dbm_compare_exchange_multi(async, expected, 1, desired, 1);
  tkrzw_future_get(cas_future);
  EXPECT_EQ(TKRZW_STATUS_SUCCESS, tkrzw_get_last_status_code());
  tkrzw_future_free(cas_future);
  expected[0].key_ptr = "xyz";
  expected[0].key_size = -1;
  expected[0].value_ptr = TKRZW_ANY_DATA;
  expected[0].value_size = 0;
  desired[0].key_ptr = "xyz";
  desired[0].key_size = -1;
  desired[0].value_ptr = nullptr;
  desired[0].value_size = -1;
  cas_future = tkrzw_async_dbm_compare_exchange_multi(async, expected, 1, desired, 1);
  tkrzw_future_get(cas_future);
  EXPECT_EQ(TKRZW_STATUS_SUCCESS, tkrzw_get_last_status_code());
  tkrzw_future_free(cas_future);
  TkrzwFuture* search_future = tkrzw_async_dbm_search(async, "regex", "^(tako|uni)$", -1, 0);
  int32_t num_search_keys = 0;
  TkrzwStr* search_keys = tkrzw_future_get_str_array(search_future, &num_search_keys);
  EXPECT_EQ(TKRZW_STATUS_SUCCESS, tkrzw_get_last_status_code());
  EXPECT_EQ(2, num_search_keys);
  tkrzw_free_str_array(search_keys, num_search_keys);
  tkrzw_future_free(search_future);
  TkrzwFuture* rebuild_future = tkrzw_async_dbm_rebuild(async, "");
  tkrzw_future_get(rebuild_future);
  EXPECT_EQ(TKRZW_STATUS_SUCCESS, tkrzw_get_last_status_code());
  tkrzw_future_free(rebuild_future);
  TkrzwFuture* sync_future = tkrzw_async_dbm_synchronize(async, false, "");
  tkrzw_future_get(sync_future);
  EXPECT_EQ(TKRZW_STATUS_SUCCESS, tkrzw_get_last_status_code());
  tkrzw_future_free(sync_future);
  EXPECT_EQ(103, tkrzw_dbm_count(dbm));
  TkrzwFuture* copy_future = tkrzw_async_dbm_copy_file_data(async, copy_path.c_str(), false);
  tkrzw_future_get(copy_future);
  EXPECT_EQ(TKRZW_STATUS_SUCCESS, tkrzw_get_last_status_code());
  tkrzw_future_free(copy_future);
  TkrzwDBM* copy_dbm = tkrzw_dbm_open(copy_path.c_str(), true, "num_buckets=100");
  ASSERT_NE(nullptr, copy_dbm);
  EXPECT_EQ(103, tkrzw_dbm_count(copy_dbm));
  EXPECT_TRUE(tkrzw_dbm_clear(copy_dbm));
  EXPECT_EQ(0, tkrzw_dbm_count(copy_dbm));
  TkrzwFuture* export_future = tkrzw_async_dbm_export(async, copy_dbm);
  tkrzw_future_get(export_future);
  EXPECT_EQ(TKRZW_STATUS_SUCCESS, tkrzw_get_last_status_code());
  tkrzw_future_free(export_future);
  EXPECT_EQ(103, tkrzw_dbm_count(copy_dbm));
  EXPECT_EQ(tkrzw::Status::SUCCESS, tkrzw::RemoveFile(copy_path));
  EXPECT_TRUE(tkrzw_dbm_close(copy_dbm));
  TkrzwFile* copy_file = tkrzw_file_open(copy_path.c_str(), true, "truncate=true");
  TkrzwFuture* expflat_future = tkrzw_async_dbm_export_to_flat_records(async, copy_file);
  tkrzw_future_get(expflat_future);
  EXPECT_EQ(TKRZW_STATUS_SUCCESS, tkrzw_get_last_status_code());
  tkrzw_future_free(expflat_future);
  TkrzwFuture* clear_future = tkrzw_async_dbm_clear(async);
  tkrzw_future_get(clear_future);
  EXPECT_EQ(TKRZW_STATUS_SUCCESS, tkrzw_get_last_status_code());
  tkrzw_future_free(clear_future);
  EXPECT_EQ(0, tkrzw_dbm_count(dbm));
  TkrzwFuture* impflat_future = tkrzw_async_dbm_import_from_flat_records(async, copy_file);
  tkrzw_future_get(impflat_future);
  EXPECT_EQ(TKRZW_STATUS_SUCCESS, tkrzw_get_last_status_code());
  tkrzw_future_free(impflat_future);
  EXPECT_EQ(103, tkrzw_dbm_count(dbm));
  EXPECT_TRUE(tkrzw_dbm_clear(dbm));
  EXPECT_TRUE(tkrzw_dbm_set(dbm, "abc", -1, "1234", -1, false));
  TkrzwFuture* pop_future = tkrzw_async_dbm_pop_first(async);
  TkrzwKeyValuePair* pop_rec = tkrzw_future_get_str_pair(pop_future);
  EXPECT_EQ(TKRZW_STATUS_SUCCESS, tkrzw_get_last_status_code());
  EXPECT_STREQ("abc", pop_rec->key_ptr);
  EXPECT_EQ(3, pop_rec->key_size);
  EXPECT_STREQ("1234", pop_rec->value_ptr);
  EXPECT_EQ(4, pop_rec->value_size);
  free(pop_rec);
  tkrzw_future_free(pop_future);
  pop_future = tkrzw_async_dbm_pop_first(async);
  pop_rec = tkrzw_future_get_str_pair(pop_future);
  EXPECT_EQ(TKRZW_STATUS_NOT_FOUND_ERROR, tkrzw_get_last_status_code());
  EXPECT_STREQ("", pop_rec->key_ptr);
  EXPECT_EQ(0, pop_rec->key_size);
  EXPECT_STREQ("", pop_rec->value_ptr);
  EXPECT_EQ(0, pop_rec->value_size);
  free(pop_rec);
  tkrzw_future_free(pop_future);
  EXPECT_EQ(0, tkrzw_dbm_count(dbm));
  TkrzwFuture* push_future = tkrzw_async_dbm_push_last(async, "foo", -1, 0);
  tkrzw_future_get(push_future);
  EXPECT_EQ(TKRZW_STATUS_SUCCESS, tkrzw_get_last_status_code());
  tkrzw_future_free(push_future);
  pop_future = tkrzw_async_dbm_pop_first(async);
  pop_rec = tkrzw_future_get_str_pair(pop_future);
  EXPECT_EQ(TKRZW_STATUS_SUCCESS, tkrzw_get_last_status_code());
  EXPECT_EQ(std::string_view("\0\0\0\0\0\0\0\0", 8),
            std::string_view(pop_rec->key_ptr, pop_rec->key_size));
  EXPECT_STREQ("foo", pop_rec->value_ptr);
  free(pop_rec);
  tkrzw_future_free(pop_future);
  EXPECT_TRUE(tkrzw_file_close(copy_file));
  tkrzw_async_dbm_free(async);
  EXPECT_TRUE(tkrzw_dbm_close(dbm));
}

TEST(LangCTest, File) {
  tkrzw::TemporaryDirectory tmp_dir(true, "tkrzw-");
  const std::string file_path = tmp_dir.MakeUniquePath("casket-", ".txt");
  TkrzwFile* file = tkrzw_file_open(
      file_path.c_str(), true,
      "truncate=true,file=pos-atom,block_size=512,access_options=padding:pagecache");
  ASSERT_NE(file, nullptr);
  EXPECT_TRUE(tkrzw_file_write(file, 5, "12345", 5));
  EXPECT_TRUE(tkrzw_file_write(file, 0, "ABCDE", 5));
  int64_t new_off = 0;
  EXPECT_TRUE(tkrzw_file_append(file, "FGH", 3, &new_off));
  EXPECT_EQ(10, new_off);
  EXPECT_TRUE(tkrzw_file_append(file, "IJ", 2, &new_off));
  EXPECT_EQ(13, new_off);
  EXPECT_EQ(15, tkrzw_file_get_size(file));
  EXPECT_TRUE(tkrzw_file_truncate(file, 12));
  EXPECT_EQ(12, tkrzw_file_get_size(file));
  EXPECT_TRUE(tkrzw_file_synchronize(file, false, 0, 0));
  EXPECT_EQ(12, tkrzw_file_get_size(file));
  char* path = tkrzw_file_get_path(file);
  ASSERT_NE(path, nullptr);
  EXPECT_NE(std::string_view::npos, std::string_view(path).find("casket-"));
  free(path);
  char buf[16];
  EXPECT_TRUE(tkrzw_file_read(file, 0, buf, 12));
  EXPECT_EQ(0, std::memcmp("ABCDE12345FG", buf, 12));
  EXPECT_TRUE(tkrzw_file_read(file, 3, buf, 5));
  EXPECT_EQ(0, std::memcmp("DE123", buf, 5));
  EXPECT_FALSE(tkrzw_file_read(file, 1024, buf, 10));
  EXPECT_EQ(TKRZW_STATUS_INFEASIBLE_ERROR, tkrzw_get_last_status_code());
  EXPECT_TRUE(tkrzw_file_close(file));
  file = tkrzw_file_open(file_path.c_str(), file, "");
  EXPECT_EQ(512, tkrzw_file_get_size(file));
  EXPECT_TRUE(tkrzw_file_read(file, 4, buf, 7));
  EXPECT_EQ(0, std::memcmp("E12345F", buf, 7));
  EXPECT_TRUE(tkrzw_file_truncate(file, 0));
  for (int32_t i = 1; i <= 100; i++) {
    const std::string line = tkrzw::SPrintF("%08d\n", i);
    EXPECT_TRUE(tkrzw_file_append(file, line.data(), line.size(), NULL));
  }
  {
    int32_t num_lines = 0;
    TkrzwStr* lines = tkrzw_file_search(file, "contain", "9", 1, -1, &num_lines);
    ASSERT_NE(nullptr, lines);
    EXPECT_EQ(19, num_lines);
    tkrzw_free_str_array(lines, num_lines);
  }
  {
    int32_t num_lines = 0;
    TkrzwStr* lines = tkrzw_file_search(file, "contain", "100", -1, 100, &num_lines);
    ASSERT_NE(nullptr, lines);
    EXPECT_EQ(1, num_lines);
    tkrzw_free_str_array(lines, num_lines);
  }
  EXPECT_TRUE(tkrzw_file_close(file));
}

// END OF FILE
