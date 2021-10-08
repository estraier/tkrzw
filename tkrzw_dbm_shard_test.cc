/*************************************************************************************************
 * Tests for tkrzw_dbm_shard.h
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
#include "tkrzw_dbm_poly.h"
#include "tkrzw_dbm_shard.h"
#include "tkrzw_dbm_test_common.h"
#include "tkrzw_dbm_ulog.h"
#include "tkrzw_file.h"
#include "tkrzw_file_mmap.h"
#include "tkrzw_file_pos.h"
#include "tkrzw_file_std.h"
#include "tkrzw_file_util.h"
#include "tkrzw_lib_common.h"
#include "tkrzw_str_util.h"

using namespace testing;

// Main routine
int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

class ShardDBMTest : public CommonDBMTest {};

TEST_F(ShardDBMTest, Basic) {
  tkrzw::TemporaryDirectory tmp_dir(true, "tkrzw-");
  std::string file_path = tmp_dir.MakeUniquePath("casket-", ".tkh");
  tkrzw::ShardDBM dbm;
  std::map<std::string, std::string> params = {{"num_shards", "5"}, {"num_buckets", "2000"}};
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm.OpenAdvanced(
      file_path, true, tkrzw::File::OPEN_TRUNCATE, params));
  BasicTest(&dbm);
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm.Close());
  file_path = tmp_dir.MakeUniquePath("casket-", ".tkt");
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm.OpenAdvanced(
      file_path, true, tkrzw::File::OPEN_TRUNCATE, params));
  BasicTest(&dbm);
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm.Close());
}

TEST_F(ShardDBMTest, Sequence) {
  tkrzw::TemporaryDirectory tmp_dir(true, "tkrzw-");
  std::string file_path = tmp_dir.MakeUniquePath("casket-", ".tkh");
  tkrzw::ShardDBM dbm;
  std::map<std::string, std::string> params = {{"num_shards", "5"}, {"num_buckets", "2000"}};
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm.OpenAdvanced(
      file_path, true, tkrzw::File::OPEN_TRUNCATE, params));
  SequenceTest(&dbm);
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm.Close());
  file_path = tmp_dir.MakeUniquePath("casket-", ".tkt");
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm.OpenAdvanced(
      file_path, true, tkrzw::File::OPEN_TRUNCATE, params));
  SequenceTest(&dbm);
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm.Close());
}

TEST_F(ShardDBMTest, Append) {
  tkrzw::TemporaryDirectory tmp_dir(true, "tkrzw-");
  std::string file_path = tmp_dir.MakeUniquePath("casket-", ".tkh");
  tkrzw::ShardDBM dbm;
  std::map<std::string, std::string> params = {{"num_shards", "5"}, {"num_buckets", "2000"}};
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm.OpenAdvanced(
      file_path, true, tkrzw::File::OPEN_TRUNCATE, params));
  AppendTest(&dbm);
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm.Close());
  file_path = tmp_dir.MakeUniquePath("casket-", ".tkt");
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm.OpenAdvanced(
      file_path, true, tkrzw::File::OPEN_TRUNCATE, params));
  AppendTest(&dbm);
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm.Close());
}

TEST_F(ShardDBMTest, Process) {
  tkrzw::TemporaryDirectory tmp_dir(true, "tkrzw-");
  std::string file_path = tmp_dir.MakeUniquePath("casket-", ".tkh");
  tkrzw::ShardDBM dbm;
  std::map<std::string, std::string> params = {{"num_shards", "5"}, {"num_buckets", "2000"}};
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm.OpenAdvanced(
      file_path, true, tkrzw::File::OPEN_TRUNCATE, params));
  ProcessTest(&dbm);
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm.Close());
  file_path = tmp_dir.MakeUniquePath("casket-", ".tkt");
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm.OpenAdvanced(
      file_path, true, tkrzw::File::OPEN_TRUNCATE, params));
  ProcessTest(&dbm);
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm.Close());
}

TEST_F(ShardDBMTest, ProcessEach) {
  tkrzw::TemporaryDirectory tmp_dir(true, "tkrzw-");
  std::string file_path = tmp_dir.MakeUniquePath("casket-", ".tkh");
  tkrzw::ShardDBM dbm;
  std::map<std::string, std::string> params = {{"num_shards", "5"}, {"num_buckets", "2000"}};
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm.OpenAdvanced(
      file_path, true, tkrzw::File::OPEN_TRUNCATE, params));
  ProcessEachTest(&dbm);
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm.Close());
  file_path = tmp_dir.MakeUniquePath("casket-", ".tkt");
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm.OpenAdvanced(
      file_path, true, tkrzw::File::OPEN_TRUNCATE, params));
  ProcessEachTest(&dbm);
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm.Close());
}

TEST_F(ShardDBMTest, ProcessMulti) {
  tkrzw::TemporaryDirectory tmp_dir(true, "tkrzw-");
  std::string file_path = tmp_dir.MakeUniquePath("casket-", ".tkh");
  tkrzw::ShardDBM dbm;
  std::map<std::string, std::string> params = {{"num_shards", "5"}, {"num_buckets", "2000"}};
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm.OpenAdvanced(
      file_path, true, tkrzw::File::OPEN_TRUNCATE, params));
  ProcessEachTest(&dbm);
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm.Close());
  file_path = tmp_dir.MakeUniquePath("casket-", ".tkt");
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm.OpenAdvanced(
      file_path, true, tkrzw::File::OPEN_TRUNCATE, params));
  ProcessMultiTest(&dbm);
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm.Close());
}

TEST_F(ShardDBMTest, ShardBasic) {
  tkrzw::TemporaryDirectory tmp_dir(true, "tkrzw-");
  struct Config final {
    std::string class_name;
    std::string path;
    std::map<std::string, std::string> open_params;
    std::map<std::string, std::string> synchronize_params;
    std::map<std::string, std::string> rebuild_params;
  };
  const std::vector<Config> configs = {
    {"HashDBM", "casket",
     {{"dbm", "hash"}, {"num_buckets", "50"}}, {}, {{"offset_width", "3"}}},
    {"HashDBM", "casket.tkh",
     {{"update_mode", "update_appending"}, {"offset_width", "3"},
      {"align_pow", "1"}, {"num_buckets", "50"}}, {}, {}},
    {"TreeDBM", "casket",
     {{"dbm", "tree"}, {"key_comparator", "decimal"}}, {}, {{"max_page_size", "512"}}},
    {"TreeDBM", "casket.tkt",
     {{"update_mode", "update_appending"}, {"key_comparator", "realnumber"}}, {}, {}},
    {"SkipDBM", "casket",
     {{"dbm", "skip"}, {"step_unit", "3"}}, {{"reducer", "last"}}, {{"max_level", "5"}}},
    {"SkipDBM", "casket.tks",
     {{"insert_in_order", "true"}, {"step_unit", "8"}}, {}, {}},
    {"TinyDBM", "",
     {{"dbm", "tiny"}, {"num_buckets", "50"}}, {}, {{"num_buckets", "30"}}},
    {"TinyDBM", "casket.tiny",
     {{"num_buckets", "50"}}, {}, {}},
    {"BabyDBM", "",
     {{"dbm", "baby"}, {"key_comparator", "decimal"}}, {}, {}},
    {"BabyDBM", "casket.baby",
     {{"key_comparator", "realnumber"}}, {}, {}},
    {"StdHashDBM", "",
     {{"dbm", "stdhash"}, {"num_buckets", "50"}}, {}, {}},
    {"StdHashDBM", "casket.stdhash",
     {{"num_buckets", "50"}}, {}, {}},
    {"StdTreeDBM", "",
     {{"dbm", "stdtree"}}, {}, {}},
    {"StdTreeDBM", "casket.stdtree",
     {}, {}, {}},
  };
  for (const auto& config : configs) {
    tkrzw::ShardDBM dbm;
    std::string path;
    if (!config.path.empty()) {
      path = tkrzw::JoinPath(tmp_dir.Path(), config.path);
    }
    auto shard_open_params = config.open_params;
    shard_open_params.emplace("num_shards", "4");
    EXPECT_EQ(tkrzw::Status::SUCCESS, dbm.OpenAdvanced(
        path, true, tkrzw::File::OPEN_TRUNCATE, shard_open_params));
    const auto inspect = dbm.Inspect();
    const std::map<std::string, std::string> inspect_map(inspect.begin(), inspect.end());
    EXPECT_EQ(config.class_name, tkrzw::SearchMap(inspect_map, "class", ""));
    const std::string type_name(dbm.GetInternalDBM()->GetType().name());
    EXPECT_NE(std::string::npos, type_name.find(config.class_name));
    EXPECT_TRUE(dbm.IsHealthy());
    for (int32_t i = 1; i <= 100; i++) {
      const std::string key = tkrzw::SPrintF("%08d", i);
      const std::string value = tkrzw::ToString(i);
      EXPECT_EQ(tkrzw::Status::SUCCESS, dbm.Set(key, value));
    }
    EXPECT_EQ(tkrzw::Status::SUCCESS, dbm.SynchronizeAdvanced(
        false, nullptr, config.synchronize_params));
    EXPECT_EQ(100, dbm.CountSimple());
    for (int32_t i = 1; i <= 100; i++) {
      const std::string key = tkrzw::SPrintF("%08d", i);
      std::string value;
      EXPECT_EQ(tkrzw::Status::SUCCESS, dbm.Get(key, &value));
      EXPECT_EQ(tkrzw::SPrintF("%08d", i), key);
      EXPECT_EQ(tkrzw::ToString(i), value);
    }
    for (int32_t i = 1; i <= 30; i++) {
      const std::string key = tkrzw::SPrintF("%08d", i);
      EXPECT_EQ(tkrzw::Status::SUCCESS, dbm.Remove(key));
    }
    for (int32_t i = 71; i <= 100; i++) {
      const std::string key = tkrzw::SPrintF("%08d", i);
      EXPECT_EQ(tkrzw::Status::SUCCESS, dbm.Remove(key));
    }
    EXPECT_EQ(tkrzw::Status::SUCCESS, dbm.SynchronizeAdvanced(
        false, nullptr, config.synchronize_params));
    EXPECT_EQ(40, dbm.CountSimple());
    std::map<std::string, std::string> records;
    for (int32_t i = 31; i <= 70; i++) {
      const std::string key = tkrzw::SPrintF("%08d", i);
      std::string value;
      EXPECT_EQ(tkrzw::Status::SUCCESS, dbm.Get(key, &value));
      records.emplace(key, value);
    }
    auto iter = dbm.MakeIterator();
    EXPECT_EQ(tkrzw::Status::SUCCESS, iter->First());
    while (true) {
      std::string key, value;
      const tkrzw::Status status = iter->Get(&key, &value);
      if (status != tkrzw::Status::SUCCESS) {
        EXPECT_EQ(tkrzw::Status::NOT_FOUND_ERROR, status);
        break;
      }
      EXPECT_EQ(tkrzw::SearchMap(records, key, ""), value);
      records.erase(key);
      EXPECT_EQ(tkrzw::Status::SUCCESS, iter->Next());
    }
    EXPECT_TRUE(records.empty());
    EXPECT_EQ(tkrzw::Status::SUCCESS, dbm.RebuildAdvanced(config.rebuild_params));
    EXPECT_EQ(tkrzw::Status::SUCCESS, dbm.Close());
    if (!path.empty()) {
      EXPECT_EQ(tkrzw::Status::SUCCESS, dbm.OpenAdvanced(
          path, false, tkrzw::File::OPEN_DEFAULT, config.open_params));
      EXPECT_TRUE(dbm.IsHealthy());
      EXPECT_EQ(40, dbm.CountSimple());
      for (int32_t i = 31; i <= 70; i++) {
        const std::string key = tkrzw::SPrintF("%08d", i);
        EXPECT_EQ(tkrzw::ToString(i), dbm.GetSimple(key));
      }
      const std::string dest_path =
          tkrzw::JoinPath(tmp_dir.Path(), tkrzw::StrCat("copy-", config.path));
      EXPECT_EQ(tkrzw::Status::SUCCESS, dbm.CopyFileData(dest_path));
      EXPECT_EQ(tkrzw::Status::SUCCESS, dbm.Close());
      EXPECT_EQ(tkrzw::Status::SUCCESS, dbm.OpenAdvanced(
          dest_path, false, tkrzw::File::OPEN_DEFAULT, config.open_params));
      EXPECT_TRUE(dbm.IsOpen());
      EXPECT_FALSE(dbm.IsWritable());
      EXPECT_TRUE(dbm.IsHealthy());
      EXPECT_EQ(40, dbm.CountSimple());
      EXPECT_EQ(tkrzw::Status::SUCCESS, dbm.Close());
      if (config.class_name == "HashDBM" || config.class_name == "TreeDBM" ||
          config.class_name == "SkipDBM") {
        const std::string restore_path = tmp_dir.MakeUniquePath("restore-", config.path);
        EXPECT_EQ(tkrzw::Status::SUCCESS, tkrzw::ShardDBM::RestoreDatabase(
            path, restore_path, config.class_name, -1));
        EXPECT_EQ(tkrzw::Status::SUCCESS, dbm.OpenAdvanced(
            restore_path, false, tkrzw::File::OPEN_DEFAULT, config.open_params));
        EXPECT_TRUE(dbm.IsHealthy());
        EXPECT_EQ(40, dbm.CountSimple());
        EXPECT_EQ(tkrzw::Status::SUCCESS, dbm.Close());
      }
    }
  }
}

TEST_F(ShardDBMTest, ShardLargeRecord) {
  tkrzw::TemporaryDirectory tmp_dir(true, "tkrzw-");
  struct Config final {
    std::string path;
    std::map<std::string, std::string> open_params;
  };
  const std::vector<Config> configs = {
    {"casket.tkh", {{"align_pow", "0"}, {"num_buckets", "1000"}}},
    {"casket.tkt", {{"align_pow", "0"}, {"max_page_size", "200"}, {"max_cached_pages", "256"}}},
    {"casket.tks", {{"step_unit", "3"}, {"max_level", "4"}}},
    {"casket.tiny", {{"num_buckets", "1000"}}},
    {"casket.baby", {{"key_comparator", "lexical"}}},
  };
  for (const auto& config : configs) {
    tkrzw::ShardDBM dbm;
    const std::string path = tkrzw::JoinPath(tmp_dir.Path(), config.path);
    EXPECT_EQ(tkrzw::Status::SUCCESS, dbm.OpenAdvanced(
        path, true, tkrzw::File::OPEN_TRUNCATE, config.open_params));
    LargeRecordTest(&dbm);
    EXPECT_EQ(tkrzw::Status::SUCCESS, dbm.Close());
  }
}

TEST_F(ShardDBMTest, ShardRebuildRandom) {
  tkrzw::TemporaryDirectory tmp_dir(true, "tkrzw-");
  struct Config final {
    std::string path;
    std::map<std::string, std::string> open_params;
  };
  const std::vector<Config> configs = {
    {"casket.tkh", {{"align_pow", "0"}, {"num_buckets", "1000"}}},
    {"casket.tkt", {{"align_pow", "0"}, {"max_page_size", "200"}, {"max_cached_pages", "256"}}},
  };
  for (const auto& config : configs) {
    tkrzw::ShardDBM dbm;
    const std::string path = tkrzw::JoinPath(tmp_dir.Path(), config.path);
    auto shard_open_params = config.open_params;
    shard_open_params.emplace("num_shards", "2");
    EXPECT_EQ(tkrzw::Status::SUCCESS, dbm.OpenAdvanced(
        path, true, tkrzw::File::OPEN_TRUNCATE, shard_open_params));
    RebuildRandomTest(&dbm);
    EXPECT_EQ(tkrzw::Status::SUCCESS, dbm.Close());
  }
}

TEST_F(ShardDBMTest, ShardBackIterator) {
  tkrzw::TemporaryDirectory tmp_dir(true, "tkrzw-");
  struct Config final {
    std::string path;
    std::map<std::string, std::string> open_params;
  };
  const std::vector<Config> configs = {
    {"casket.tree", {{"max_page_size", "1"}, {"max_branches", "2"}}},
    {"casket.skip", {{"step_unit", "2"}}},
    {"casket.baby", {}},
    {"casket.stdtree", {}},
  };
  for (const auto& config : configs) {
    tkrzw::ShardDBM dbm;
    const std::string path = tkrzw::JoinPath(tmp_dir.Path(), config.path);
    auto shard_open_params = config.open_params;
    shard_open_params.emplace("num_shards", "4");
    EXPECT_EQ(tkrzw::Status::SUCCESS, dbm.OpenAdvanced(
        path, true, tkrzw::File::OPEN_TRUNCATE, shard_open_params));
    BackIteratorTest(&dbm);
    EXPECT_EQ(tkrzw::Status::SUCCESS, dbm.Close());
  }
}

TEST_F(ShardDBMTest, ShardIteratorBound) {
  tkrzw::TemporaryDirectory tmp_dir(true, "tkrzw-");
  struct Config final {
    std::string path;
    std::map<std::string, std::string> open_params;
  };
  const std::vector<Config> configs = {
    {"casket.tree", {{"max_page_size", "1"}, {"max_branches", "2"}}},
    {"casket.skip", {{"step_unit", "2"}}},
    {"casket.baby", {}},
    {"casket.stdtree", {}},
  };
  for (const auto& config : configs) {
    tkrzw::ShardDBM dbm;
    const std::string path = tkrzw::JoinPath(tmp_dir.Path(), config.path);
    auto shard_open_params = config.open_params;
    shard_open_params.emplace("num_shards", "4");
    EXPECT_EQ(tkrzw::Status::SUCCESS, dbm.OpenAdvanced(
        path, true, tkrzw::File::OPEN_TRUNCATE, shard_open_params));
    IteratorBoundTest(&dbm);
    EXPECT_EQ(tkrzw::Status::SUCCESS, dbm.Close());
  }
}

TEST_F(ShardDBMTest, UpdateLogger) {
  tkrzw::TemporaryDirectory tmp_dir(true, "tkrzw-");
  struct Config final {
    std::string path;
    std::map<std::string, std::string> open_params;
  };
  const std::vector<Config> configs = {
    {"casket.hash", {{"num_buckets", "100"}}},
    {"casket.tree", {{"max_page_size", "1"}, {"max_branches", "2"}}},
    {"casket.tiny", {{"num_buckets", "100"}}},
    {"casket.baby", {}},
  };
  for (const auto& config : configs) {
    tkrzw::ShardDBM dbm;
    const std::string path = tkrzw::JoinPath(tmp_dir.Path(), config.path);
    auto shard_open_params = config.open_params;
    shard_open_params.emplace("num_shards", "4");
    EXPECT_EQ(tkrzw::Status::SUCCESS, dbm.OpenAdvanced(
        path, true, tkrzw::File::OPEN_TRUNCATE, config.open_params));
    UpdateLoggerTest(&dbm);
    EXPECT_EQ(tkrzw::Status::SUCCESS, dbm.Close());
  }
}

TEST_F(ShardDBMTest, UpdateLoggerMQ) {
  tkrzw::TemporaryDirectory tmp_dir(true, "tkrzw-");
  struct Config final {
    std::string path;
    std::map<std::string, std::string> open_params;
  };
  const std::vector<Config> configs = {
    {"casket.hash", {{"num_buckets", "100"}}},
    {"casket.tree", {{"max_page_size", "1"}, {"max_branches", "2"}}},
    {"casket.tiny", {{"num_buckets", "100"}}},
    {"casket.baby", {}},
  };
  for (const auto& config : configs) {
    const std::string ulog_prefix = tmp_dir.MakeUniquePath("casket-", "-ulog");
    tkrzw::ShardDBM dbm;
    const std::string path = tkrzw::JoinPath(tmp_dir.Path(), config.path);
    auto shard_open_params = config.open_params;
    shard_open_params.emplace("num_shards", "4");
    shard_open_params["ulog_prefix"] = ulog_prefix;
    shard_open_params["ulog_max_file_size"] = "1024";
    shard_open_params["ulog_server_id"] = "1234";
    shard_open_params["ulog_dbm_index"] = "56789";
    EXPECT_EQ(tkrzw::Status::SUCCESS, dbm.OpenAdvanced(
        path, true, tkrzw::File::OPEN_TRUNCATE, shard_open_params));
    for (int32_t i = 1; i <= 100; i++) {
      const std::string key = tkrzw::ToString(i);
      const std::string value = tkrzw::ToString(i * i);
      EXPECT_EQ(tkrzw::Status::SUCCESS, dbm.Set(key, value));
    }
    EXPECT_EQ(tkrzw::Status::SUCCESS, dbm.Synchronize(false));
    tkrzw::StdTreeDBM dbm_restored;
    EXPECT_EQ(tkrzw::Status::SUCCESS, tkrzw::DBMUpdateLoggerMQ::ApplyUpdateLogFromFiles(
        &dbm_restored, ulog_prefix, 0, 1234, 56789));
    EXPECT_EQ(dbm.CountSimple(), dbm_restored.CountSimple());
    for (int32_t i = 1; i <= 100; i++) {
      const std::string key = tkrzw::ToString(i);
      const std::string value = tkrzw::ToString(i * i);
      EXPECT_EQ(value, dbm_restored.GetSimple(key));
    }
    EXPECT_EQ(tkrzw::Status::SUCCESS, dbm.Close());
  }
}

TEST_F(ShardDBMTest, ShardRestoreAndRename) {
  tkrzw::TemporaryDirectory tmp_dir(true, "tkrzw-");
  const std::string file_path = tmp_dir.MakeUniquePath("casket-", ".tkh");
  const std::string new_file_path = tmp_dir.MakeUniquePath("casket-", ".tkh");
  tkrzw::ShardDBM dbm;
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm.OpenAdvanced(
      file_path, true, tkrzw::File::OPEN_TRUNCATE,
      {{"num_shards", "3"}, {"num_buckets", "10"}}));
  for (int32_t i = 1; i <= 100; i++) {
    const std::string key = tkrzw::SPrintF("%08d", i);
    const std::string value = tkrzw::ToString(i);
    EXPECT_EQ(tkrzw::Status::SUCCESS, dbm.Set(key, value));
  }
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm.Close());
  int32_t num_shards = 0;
  EXPECT_EQ(tkrzw::Status::SUCCESS, tkrzw::ShardDBM::GetNumberOfShards(file_path, &num_shards));
  EXPECT_EQ(3, num_shards);
  EXPECT_EQ(tkrzw::Status::SUCCESS, tkrzw::ShardDBM::RestoreDatabase(file_path, new_file_path));
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm.Open(new_file_path, false));
  for (int32_t i = 1; i <= 100; i++) {
    const std::string key = tkrzw::SPrintF("%08d", i);
    EXPECT_EQ(tkrzw::Status::SUCCESS, dbm.Get(key));
  }
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm.Close());
  const std::string zero_file_path = file_path + "-00000-of-00003";
  EXPECT_EQ(tkrzw::Status::SUCCESS, tkrzw::RemoveFile(zero_file_path));
  EXPECT_EQ(tkrzw::Status::SUCCESS, tkrzw::ShardDBM::RenameDatabase(new_file_path, file_path));
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm.Open(file_path, false));
  for (int32_t i = 1; i <= 100; i++) {
    const std::string key = tkrzw::SPrintF("%08d", i);
    EXPECT_EQ(tkrzw::Status::SUCCESS, dbm.Get(key));
  }
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm.Close());
}

// END OF FILE
