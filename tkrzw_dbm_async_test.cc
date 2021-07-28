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
#include "tkrzw_file_poly.h"
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
  std::string copy_path = tmp_dir.MakeUniquePath("casket-copy-", ".tkh");
  tkrzw::PolyDBM dbm;
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm.OpenAdvanced(
      file_path, true, tkrzw::File::OPEN_TRUNCATE, {{"num_buckets", "100"}}));
  std::vector<std::future<std::pair<tkrzw::Status, std::string>>> get_results;
  std::map<std::string, int32_t> status_counter;
  std::mutex postproc_mutex;
  auto postproc = [&](const char* name, const tkrzw::Status& status) {
                    std::lock_guard lock(postproc_mutex);
                    status_counter[tkrzw::StrCat(
                        name, ":", tkrzw::Status::CodeName(status.GetCode()))]++;
                  };
  {
    tkrzw::AsyncDBM async(&dbm, 4);
    async.SetCommonPostprocessor(postproc);
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
  {
    std::lock_guard lock(postproc_mutex);
    EXPECT_EQ(150, status_counter["Set:SUCCESS"]);
    EXPECT_EQ(150, status_counter["Set:DUPLICATION_ERROR"]);
    EXPECT_EQ(200, status_counter["Get:SUCCESS"]);
    EXPECT_EQ(50, status_counter["Remove:SUCCESS"]);
    EXPECT_EQ(50, status_counter["Remove:NOT_FOUND_ERROR"]);
    EXPECT_EQ(100, status_counter["Append:SUCCESS"]);
    status_counter.clear();
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
    async.SetCommonPostprocessor(postproc);
    EXPECT_EQ(tkrzw::Status::SUCCESS, async.Synchronize(false).get());
    EXPECT_EQ(tkrzw::Status::SUCCESS, async.Rebuild().get());
    EXPECT_EQ(100, dbm.CountSimple());
    EXPECT_EQ(tkrzw::Status::SUCCESS, async.CopyFileData(copy_path).get());
    {
      tkrzw::PolyDBM copy_dbm;
      EXPECT_EQ(tkrzw::Status::SUCCESS, copy_dbm.OpenAdvanced(
          copy_path, true, tkrzw::File::OPEN_DEFAULT, {{"num_buckets", "100"}}));
      EXPECT_EQ(100, copy_dbm.CountSimple());
      EXPECT_EQ(tkrzw::Status::SUCCESS, copy_dbm.Clear());
      EXPECT_EQ(0, copy_dbm.CountSimple());
      EXPECT_EQ(tkrzw::Status::SUCCESS, async.Export(&copy_dbm).get());
      EXPECT_EQ(100, copy_dbm.CountSimple());
      EXPECT_EQ(tkrzw::Status::SUCCESS, copy_dbm.Close());
    }
    {
      tkrzw::PolyFile copy_file;
      EXPECT_EQ(tkrzw::Status::SUCCESS, copy_file.Open(
          copy_path, true, tkrzw::File::OPEN_TRUNCATE));
      EXPECT_EQ(tkrzw::Status::SUCCESS, async.ExportRecordsToFlatRecords(&copy_file).get());
      EXPECT_EQ(tkrzw::Status::SUCCESS, dbm.Clear());
      EXPECT_EQ(0, dbm.CountSimple());
      EXPECT_EQ(tkrzw::Status::SUCCESS, async.ImportRecordsFromFlatRecords(&copy_file).get());
      EXPECT_EQ(100, dbm.CountSimple());
      EXPECT_EQ(tkrzw::Status::SUCCESS, copy_file.Close());
    }
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
  {
    std::lock_guard lock(postproc_mutex);
    EXPECT_EQ(1, status_counter["Synchronize:SUCCESS"]);
    EXPECT_EQ(1, status_counter["Rebuild:SUCCESS"]);
    EXPECT_EQ(1, status_counter["Clear:SUCCESS"]);
    EXPECT_EQ(1, status_counter["GetMulti:NOT_FOUND_ERROR"]);
    EXPECT_EQ(1, status_counter["SetMulti:SUCCESS"]);
    EXPECT_EQ(1, status_counter["AppendMulti:SUCCESS"]);
    EXPECT_EQ(1, status_counter["RemoveMulti:SUCCESS"]);
  }
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm.Close());
}

TEST(AsyncDBMTest, Process) {
  tkrzw::TemporaryDirectory tmp_dir(true, "tkrzw-");
  std::string file_path = tmp_dir.MakeUniquePath("casket-", ".tkt");
  tkrzw::PolyDBM dbm;
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm.OpenAdvanced(
      file_path, true, tkrzw::File::OPEN_TRUNCATE, {{"num_buckets", "100"}}));
  {
    tkrzw::AsyncDBM async(&dbm, 4);
    class Setter : public tkrzw::AsyncDBM::RecordProcessor {
     public:
      Setter(std::string_view new_value) : new_value_(new_value), old_value_() {}
      std::string_view ProcessFull(std::string_view key, std::string_view value) override {
        old_value_ = value;
        return new_value_;
      }
      std::string_view ProcessEmpty(std::string_view key) override {
        return new_value_;
      }
      const std::string& GetOldValue() const {
        return old_value_;
      }
     private:
      std::string new_value_;
      std::string old_value_;
    };
    auto r1 = async.Process("a", std::make_unique<Setter>("one"), true).get();
    EXPECT_EQ(tkrzw::Status::SUCCESS, r1.first);
    EXPECT_EQ("", r1.second->GetOldValue());
    auto r2 = async.Process("a", std::make_unique<Setter>("two"), true).get();
    EXPECT_EQ(tkrzw::Status::SUCCESS, r2.first);
    EXPECT_EQ("one", r2.second->GetOldValue());
    std::string old_value;
    auto r3 = async.Process("b", [&](std::string_view key, std::string_view value) {
                                   if (value.data() != tkrzw::DBM::RecordProcessor::NOOP.data()) {
                                     old_value = value;
                                   }
                                   return "uno";
                                 }, true).get();
    EXPECT_EQ(tkrzw::Status::SUCCESS, r3);
    EXPECT_EQ("", old_value);
    auto r4 = async.Process("b", [&](std::string_view key, std::string_view value) {
                                   if (value.data() != tkrzw::DBM::RecordProcessor::NOOP.data()) {
                                     old_value = value;
                                   }
                                   return "dos";
                                 }, true).get();
    EXPECT_EQ(tkrzw::Status::SUCCESS, r4);
    EXPECT_EQ("uno", old_value);
    class Bracketter : public tkrzw::AsyncDBM::RecordProcessor {
     public:
      Bracketter() {}
      std::string_view ProcessFull(std::string_view key, std::string_view value) override {
        if (value.data() == tkrzw::DBM::RecordProcessor::NOOP.data()) {
          return tkrzw::DBM::RecordProcessor::NOOP;
        }
        new_value_ = tkrzw::StrCat("[", value, "]");
        return new_value_;
      }
     private:
      std::string new_value_;
    };
    auto r5 = async.ProcessEach(std::make_unique<Bracketter>(), true).get();
    EXPECT_EQ(tkrzw::Status::SUCCESS, r5.first);
    std::string new_value;
    auto r6 = async.ProcessEach([&](
        std::string_view key, std::string_view value) -> std::string_view {
                                  if (value.data() == tkrzw::DBM::RecordProcessor::NOOP.data()) {
                                    return tkrzw::DBM::RecordProcessor::NOOP;
                                  }
                                  new_value = tkrzw::StrCat("(", value, ")");
                                  return new_value;
                                }, true).get();
    EXPECT_EQ("([two])", dbm.GetSimple("a"));
    EXPECT_EQ("([dos])", dbm.GetSimple("b"));
    std::vector<std::pair<std::string_view, std::shared_ptr<Setter>>> key_proc_pairs;
    key_proc_pairs.emplace_back(std::make_pair("a", std::make_unique<Setter>("three")));
    key_proc_pairs.emplace_back(std::make_pair("b", std::make_unique<Setter>("tres")));
    auto r7 = async.ProcessMulti(key_proc_pairs, true).get();
    EXPECT_EQ(tkrzw::Status::SUCCESS, r7.first);
    EXPECT_EQ("three", dbm.GetSimple("a"));
    EXPECT_EQ("tres", dbm.GetSimple("b"));
    ASSERT_EQ(2, r7.second.size());
    EXPECT_EQ("([two])", r7.second[0]->GetOldValue());
    EXPECT_EQ("([dos])", r7.second[1]->GetOldValue());
    std::vector<std::pair<std::string_view, tkrzw::DBM::RecordLambdaType>> key_lambda_pairs;
    key_lambda_pairs.emplace_back(std::make_pair(
        "a", [&](std::string_view key, std::string_view value) { return "four"; }));
    key_lambda_pairs.emplace_back(std::make_pair(
        "b", [&](std::string_view key, std::string_view value) { return "cuatro"; }));
    auto r8 = async.ProcessMulti(key_lambda_pairs, true).get();
    EXPECT_EQ(tkrzw::Status::SUCCESS, r8);
    EXPECT_EQ("four", dbm.GetSimple("a"));
    EXPECT_EQ("cuatro", dbm.GetSimple("b"));
  }
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm.Close());
}

TEST(AsyncDBMTest, SearchModal) {
  tkrzw::TemporaryDirectory tmp_dir(true, "tkrzw-");
  std::string file_path = tmp_dir.MakeUniquePath("casket-", ".tkt");
  tkrzw::PolyDBM dbm;
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm.OpenAdvanced(
      file_path, true, tkrzw::File::OPEN_TRUNCATE, {{"num_buckets", "100"}}));
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

TEST(AsyncDBMTest, StatusFeature) {
  tkrzw::TemporaryDirectory tmp_dir(true, "tkrzw-");
  std::string file_path = tmp_dir.MakeUniquePath("casket-", ".tkh");
  tkrzw::PolyDBM dbm;
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm.OpenAdvanced(
      file_path, true, tkrzw::File::OPEN_TRUNCATE, {{"num_buckets", "100"}}));
  {
    tkrzw::AsyncDBM async(&dbm, 4);
    for (int32_t i = 1; i <= 100; i++) {
      const std::string key = tkrzw::SPrintF("%04d", i);
      const std::string value = tkrzw::ToString(i * i);
      tkrzw::StatusFuture set_future(async.Set(key, value));
      if (i % 3 == 0) {
        EXPECT_TRUE(set_future.Wait());
      }
      const auto& set_result = set_future.Get();
      EXPECT_EQ(tkrzw::Status::SUCCESS, set_result);
      tkrzw::StatusFuture get_future(async.Get(key));
      if (i % 3 == 0) {
        EXPECT_TRUE(get_future.Wait());
      }
      const auto& get_result = get_future.GetString();
      EXPECT_EQ(tkrzw::Status::SUCCESS, get_result.first);
      EXPECT_EQ(value, get_result.second);
      if (i % 2 == 0) {
        tkrzw::StatusFuture remove_future(async.Remove(key));
        if (i % 3 == 0) {
          EXPECT_TRUE(remove_future.Wait());
        }
        const auto& remove_result = remove_future.Get();
        EXPECT_EQ(tkrzw::Status::SUCCESS, remove_result);
        tkrzw::StatusFuture incr_future(async.Increment(key, 1, 100));
        if (i % 3 == 0) {
          EXPECT_TRUE(incr_future.Wait());
        }
        const auto& incr_result = incr_future.GetInteger();
        EXPECT_EQ(tkrzw::Status::SUCCESS, incr_result.first);
        EXPECT_EQ(101, incr_result.second);
      }
    }
    EXPECT_EQ(100, dbm.CountSimple());
    {
      tkrzw::StatusFuture search_future(async.SearchModal("regex", "[123]7", 0));
      search_future.Wait(0);
      EXPECT_TRUE(search_future.Wait());
      const auto& search_result = search_future.GetStringVector();
      EXPECT_EQ(tkrzw::Status::SUCCESS, search_result.first);
      EXPECT_THAT(search_result.second, UnorderedElementsAre("0017", "0027", "0037"));
    }
    for (int32_t i = 0; i < 10; i++) {
      std::map<std::string, std::string> records;
      std::vector<std::string> keys;
      for (int32_t j = 1; j <= 10; j++) {
        const int32_t num = i * 10 + j;
        const std::string key = tkrzw::SPrintF("%04d", num);
        const std::string value = tkrzw::ToString(num * num);
        records.emplace(key, value);
        keys.emplace_back(key);
      }
      tkrzw::StatusFuture set_future(async.SetMulti(records));
      if (i % 3 == 0) {
        EXPECT_TRUE(set_future.Wait());
      }
      const auto& set_result = set_future.Get();
      EXPECT_EQ(tkrzw::Status::SUCCESS, set_result);
      tkrzw::StatusFuture get_future(async.GetMulti(keys));
      if (i % 3 == 0) {
        EXPECT_TRUE(get_future.Wait());
      }
      const auto& get_result = get_future.GetStringMap();
      EXPECT_EQ(tkrzw::Status::SUCCESS, get_result.first);
      EXPECT_EQ(10, get_result.second.size());
      for (const auto& key : keys) {
        EXPECT_NE(get_result.second.end(), get_result.second.find(key));
      }
      if (i == 0) {
        tkrzw::StatusFuture remove_future(async.RemoveMulti(keys));
        if (i % 3 == 0) {
          EXPECT_TRUE(remove_future.Wait());
        }
        const auto& remove_result = remove_future.Get();
        EXPECT_EQ(tkrzw::Status::SUCCESS, remove_result);
      }
    }
  }
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm.Close());
}

// END OF FILE
