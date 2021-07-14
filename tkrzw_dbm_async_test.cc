/*************************************************************************************************
 * Tests for tkrzw_dbm_async.h
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

#include "tkrzw_dbm.h"
#include "tkrzw_dbm_async.h"
#include "tkrzw_dbm_poly.h"
#include "tkrzw_file_util.h"
#include "tkrzw_lib_common.h"
#include "tkrzw_str_util.h"

using namespace testing;

// Main routine
int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

TEST(AsyncDBMTest, Basic) {
  tkrzw::TemporaryDirectory tmp_dir(true, "tkrzw-");
  std::string file_path = tmp_dir.MakeUniquePath("casket-", ".tkh");
  tkrzw::PolyDBM dbm;
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm.OpenAdvanced(
      file_path, true, tkrzw::File::OPEN_TRUNCATE, {{"num_buckets", "100"}}));
  std::vector<std::future<std::pair<tkrzw::Status, std::string>>> get_results;
  {
    tkrzw::AsyncDBM async_dbm(&dbm, 4);
    for (int32_t i = 1; i <= 100; i++) {
      const std::string key = tkrzw::ToString(i);
      const std::string value = tkrzw::ToString(i * i);
      EXPECT_EQ(tkrzw::Status::SUCCESS, async_dbm.Set(key, value, false).get());
      const auto& get_result = async_dbm.Get(key).get();
      EXPECT_EQ(tkrzw::Status::SUCCESS, get_result.first);
      EXPECT_EQ(value, get_result.second);
      EXPECT_EQ(tkrzw::Status::DUPLICATION_ERROR, async_dbm.Set(key, value, false).get());
      if (i % 2 == 0) {
        EXPECT_EQ(tkrzw::Status::SUCCESS, async_dbm.Remove(key).get());
        EXPECT_EQ(tkrzw::Status::NOT_FOUND_ERROR, async_dbm.Remove(key).get());
      }
    }
    EXPECT_EQ(50, dbm.CountSimple());
    for (int32_t i = 1; i <= 100; i++) {
      const std::string key = tkrzw::ToString(i);
      const std::string value = tkrzw::ToString(i * i);
      async_dbm.Set(key, value, false);
    }
    for (int32_t i = 1; i <= 100; i++) {
      const std::string key = tkrzw::ToString(i);
      async_dbm.Append(key, "0", ":");
    }
    for (int32_t i = 1; i <= 100; i++) {
      const std::string key = tkrzw::ToString(i);
      get_results.emplace_back(async_dbm.Get(key));
    }
  }
  EXPECT_EQ(100, dbm.CountSimple());
  ASSERT_EQ(100, get_results.size());
  for (int32_t i = 1; i <= 100; i++) {
    const std::string value = tkrzw::ToString(i * i) + ":0";
    const auto& rv = get_results[i-1].get();
    EXPECT_EQ(tkrzw::Status::SUCCESS, rv.first);
    EXPECT_EQ(value, rv.second);
  }
  {
    tkrzw::AsyncDBM async_dbm(&dbm, 4);
    EXPECT_EQ(tkrzw::Status::SUCCESS, async_dbm.Synchronize(false).get());
    EXPECT_EQ(tkrzw::Status::SUCCESS, async_dbm.Rebuild().get());
    EXPECT_EQ(100, dbm.CountSimple());
    EXPECT_EQ(tkrzw::Status::SUCCESS, async_dbm.Clear().get());
    EXPECT_EQ(0, dbm.CountSimple());
    EXPECT_EQ(tkrzw::Status::SUCCESS,
              async_dbm.CompareExchange("a", std::string_view(), "123").get());
    EXPECT_EQ(tkrzw::Status::SUCCESS,
              async_dbm.CompareExchange("a", "123", "4567").get());
    EXPECT_EQ("4567", async_dbm.Get("a").get().second);
    EXPECT_EQ(tkrzw::Status::SUCCESS,
              async_dbm.CompareExchange("a", "4567", std::string_view()).get());
    EXPECT_EQ(tkrzw::Status::SUCCESS, async_dbm.Increment("b", 2, 100).get().first);
    EXPECT_EQ(105, async_dbm.Increment("b", 3, 100).get().second);
    const std::map<std::string_view, std::string_view> records = {{"a", "A"}, {"b", "BB"}};
    EXPECT_EQ(tkrzw::Status::SUCCESS, async_dbm.SetMulti(records, true).get());
    const std::vector<std::string_view> get_keys = {"a", "b", "c", "d"};
    auto get_results = async_dbm.GetMulti(get_keys).get();
    EXPECT_EQ(2, get_results.size());
    EXPECT_EQ("A", get_results["a"]);
    EXPECT_EQ("BB", get_results["b"]);
    const std::vector<std::string_view> remove_keys = {"a", "b"};
    EXPECT_EQ(tkrzw::Status::SUCCESS, async_dbm.RemoveMulti(remove_keys).get());
    async_dbm.Set("1", "10");
    async_dbm.Set("2", "20");
    async_dbm.Set("3", "30");
    typedef std::vector<std::pair<std::string_view, std::string_view>> kv_list;
    EXPECT_EQ(tkrzw::Status::SUCCESS, async_dbm.CompareExchangeMulti(
        kv_list({{"1", "10"}, {"2", "20"}}), kv_list({{"1", "100"}, {"2", "200"}})).get());
    EXPECT_EQ(tkrzw::Status::INFEASIBLE_ERROR, async_dbm.CompareExchangeMulti(
        kv_list({{"1", "10"}, {"2", "20"}}), kv_list({{"1", "xxx"}, {"2", "yyy"}})).get());
    EXPECT_EQ("100", dbm.GetSimple("1"));
    EXPECT_EQ("200", dbm.GetSimple("2"));
    EXPECT_EQ(tkrzw::Status::INFEASIBLE_ERROR, async_dbm.CompareExchangeMulti(
        kv_list({{"1", "100"}, {"2", std::string_view()}}),
        kv_list({{"1", "xx"}, {"2", "yyy"}})).get());
    EXPECT_EQ(tkrzw::Status::SUCCESS, async_dbm.CompareExchangeMulti(
        kv_list({{"1", "100"}, {"2", "200"}}),
        kv_list({{"1", "xx"}, {"2", std::string_view()}})).get());
    EXPECT_EQ("xx", dbm.GetSimple("1"));
    EXPECT_EQ(tkrzw::Status::NOT_FOUND_ERROR, dbm.Get("2"));
    EXPECT_EQ(tkrzw::Status::SUCCESS, async_dbm.CompareExchangeMulti(
      kv_list({{"1", "xx"}, {"3", "30"}}),
      kv_list({{"1", std::string_view()}, {"3", std::string_view()}, {"4", "hello"}})).get());
    EXPECT_EQ(tkrzw::Status::SUCCESS, async_dbm.CompareExchangeMulti(
        kv_list({{"4", "hello"}}), kv_list({{"4", std::string_view()}})).get());
  }
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm.Close());
}

// END OF FILE
