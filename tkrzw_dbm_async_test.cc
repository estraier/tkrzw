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
    tkrzw::AsyncDBM async(&dbm, 4);
    for (int32_t i = 1; i <= 100; i++) {
      const std::string key = tkrzw::ToString(i);
      const std::string value = tkrzw::ToString(i * i);
      EXPECT_EQ(tkrzw::Status::SUCCESS, async.Set(key, value, false).get());
      const auto& get_result = async.Get(key).get();
      EXPECT_EQ(tkrzw::Status::SUCCESS, get_result.first);
      EXPECT_EQ(value, get_result.second);
      EXPECT_EQ(tkrzw::Status::DUPLICATION_ERROR, async.Set(key, value, false).get());
      if (i % 2 == 0) {
        EXPECT_EQ(tkrzw::Status::SUCCESS, async.Remove(key).get());
        EXPECT_EQ(tkrzw::Status::NOT_FOUND_ERROR, async.Remove(key).get());
      }
    }
    EXPECT_EQ(50, dbm.CountSimple());
    for (int32_t i = 1; i <= 100; i++) {
      const std::string key = tkrzw::ToString(i);
      const std::string value = tkrzw::ToString(i * i);
      async.Set(key, value, false);
    }
    for (int32_t i = 1; i <= 100; i++) {
      const std::string key = tkrzw::ToString(i);
      async.Append(key, "0", ":");
    }
    for (int32_t i = 1; i <= 100; i++) {
      const std::string key = tkrzw::ToString(i);
      get_results.emplace_back(async.Get(key));
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
    tkrzw::AsyncDBM async(&dbm, 4);
    EXPECT_EQ(tkrzw::Status::SUCCESS, async.Synchronize(false).get());
    EXPECT_EQ(tkrzw::Status::SUCCESS, async.Rebuild().get());
    EXPECT_EQ(100, dbm.CountSimple());
    EXPECT_EQ(tkrzw::Status::SUCCESS, async.Clear().get());
    EXPECT_EQ(0, dbm.CountSimple());
    EXPECT_EQ(tkrzw::Status::SUCCESS,
              async.CompareExchange("a", std::string_view(), "123").get());
    EXPECT_EQ(tkrzw::Status::SUCCESS,
              async.CompareExchange("a", "123", "4567").get());
    EXPECT_EQ("4567", async.Get("a").get().second);
    EXPECT_EQ(tkrzw::Status::SUCCESS,
              async.CompareExchange("a", "4567", std::string_view()).get());
    EXPECT_EQ(tkrzw::Status::SUCCESS, async.Increment("b", 2, 100).get().first);
    EXPECT_EQ(105, async.Increment("b", 3, 100).get().second);
    const std::map<std::string_view, std::string_view> records = {{"a", "A"}, {"b", "BB"}};
    EXPECT_EQ(tkrzw::Status::SUCCESS, async.SetMulti(records, true).get());
    EXPECT_EQ(tkrzw::Status::SUCCESS, async.AppendMulti(records, ":").get());
    const std::vector<std::string_view> get_keys = {"a", "b", "c", "d"};
    auto get_results = async.GetMulti(get_keys).get();
    EXPECT_EQ(2, get_results.second.size());
    EXPECT_EQ("A:A", get_results.second["a"]);
    EXPECT_EQ("BB:BB", get_results.second["b"]);
    const std::vector<std::string_view> remove_keys = {"a", "b"};
    EXPECT_EQ(tkrzw::Status::SUCCESS, async.RemoveMulti(remove_keys).get());
    async.Set("1", "10");
    async.Set("2", "20");
    async.Set("3", "30");
    typedef std::vector<std::pair<std::string_view, std::string_view>> kv_list;
    EXPECT_EQ(tkrzw::Status::SUCCESS, async.CompareExchangeMulti(
        kv_list({{"1", "10"}, {"2", "20"}}), kv_list({{"1", "100"}, {"2", "200"}})).get());
    EXPECT_EQ(tkrzw::Status::INFEASIBLE_ERROR, async.CompareExchangeMulti(
        kv_list({{"1", "10"}, {"2", "20"}}), kv_list({{"1", "xxx"}, {"2", "yyy"}})).get());
    EXPECT_EQ("100", dbm.GetSimple("1"));
    EXPECT_EQ("200", dbm.GetSimple("2"));
    EXPECT_EQ(tkrzw::Status::INFEASIBLE_ERROR, async.CompareExchangeMulti(
        kv_list({{"1", "100"}, {"2", std::string_view()}}),
        kv_list({{"1", "xx"}, {"2", "yyy"}})).get());
    EXPECT_EQ(tkrzw::Status::SUCCESS, async.CompareExchangeMulti(
        kv_list({{"1", "100"}, {"2", "200"}}),
        kv_list({{"1", "xx"}, {"2", std::string_view()}})).get());
    EXPECT_EQ("xx", dbm.GetSimple("1"));
    EXPECT_EQ(tkrzw::Status::NOT_FOUND_ERROR, dbm.Get("2"));
    EXPECT_EQ(tkrzw::Status::SUCCESS, async.CompareExchangeMulti(
      kv_list({{"1", "xx"}, {"3", "30"}}),
      kv_list({{"1", std::string_view()}, {"3", std::string_view()}, {"4", "hello"}})).get());
    EXPECT_EQ(tkrzw::Status::SUCCESS, async.CompareExchangeMulti(
        kv_list({{"4", "hello"}}), kv_list({{"4", std::string_view()}})).get());
  }
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm.Close());
}

TEST(AsyncDBMTest, SearchModal) {
  tkrzw::TemporaryDirectory tmp_dir(true, "tkrzw-");
  std::string file_path = tmp_dir.MakeUniquePath("casket-", ".tkt");
  tkrzw::PolyDBM dbm;
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm.OpenAdvanced(
      file_path, true, tkrzw::File::OPEN_TRUNCATE, {{"num_buckets", "100"}}));
  std::vector<std::future<std::pair<tkrzw::Status, std::string>>> get_results;
  {
    tkrzw::AsyncDBM async(&dbm, 4);
    std::vector<std::future<tkrzw::Status>> set_results;
    for (int32_t i = 1; i <= 50; i++) {
      const std::string key = tkrzw::SPrintF("%04d", i);
      const std::string value = tkrzw::ToString(i * i);
      set_results.emplace_back(async.Set(key, value, true));
    }
    for (auto& set_result : set_results) {
      EXPECT_EQ(tkrzw::Status::SUCCESS, set_result.get());
    }
    std::map<std::string, std::string> records;
    for (int32_t i = 51; i <= 100; i++) {
      std::string key = tkrzw::SPrintF("%04d", i);
      std::string value = tkrzw::ToString(i * i);
      records.emplace(std::make_pair(std::move(key), std::move(value)));
    }
    EXPECT_EQ(tkrzw::Status::SUCCESS, async.SetMulti(records, false).get());
    EXPECT_EQ(100, dbm.CountSimple());
    std::map<std::string, std::string> all_records;
    std::string last_key = "";
    while (true) {
      auto [search_status, keys] = async.SearchModal("upper", last_key, 10).get();
      if (search_status != tkrzw::Status::SUCCESS || keys.empty()) {
        break;
      }
      const auto [get_status, records] = async.GetMulti(keys).get();
      for (const auto& record : records) {
        all_records.emplace(record);
      }
      last_key = keys.back();
    }
    EXPECT_EQ(100, all_records.size());
  }
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm.Close());
}

// END OF FILE
