/*************************************************************************************************
 * Tests for tkrzw_dbm_cache.h
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

#include "tkrzw_file.h"
#include "tkrzw_file_mmap.h"
#include "tkrzw_file_pos.h"
#include "tkrzw_file_std.h"
#include "tkrzw_file_util.h"
#include "tkrzw_dbm.h"
#include "tkrzw_dbm_cache.h"
#include "tkrzw_dbm_test_common.h"
#include "tkrzw_dbm_ulog.h"
#include "tkrzw_lib_common.h"
#include "tkrzw_str_util.h"

using namespace testing;

// Main routine
int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

class CacheDBMTest : public CommonDBMTest {};

TEST_F(CacheDBMTest, File) {
  tkrzw::TemporaryDirectory tmp_dir(true, "tkrzw-");
  const std::string file_path = tmp_dir.MakeUniquePath();
  tkrzw::CacheDBM dbm;
  FileTest(&dbm, file_path);
}

TEST_F(CacheDBMTest, LargeRecord) {
  tkrzw::CacheDBM dbm;
  LargeRecordTest(&dbm);
}

TEST_F(CacheDBMTest, Basic) {
  tkrzw::CacheDBM dbm;
  BasicTest(&dbm);
}

TEST_F(CacheDBMTest, Sequence) {
  tkrzw::CacheDBM dbm;
  SequenceTest(&dbm);
}

TEST_F(CacheDBMTest, Append) {
  tkrzw::CacheDBM dbm;
  AppendTest(&dbm);
}

TEST_F(CacheDBMTest, Process) {
  tkrzw::CacheDBM dbm;
  ProcessTest(&dbm);
}

TEST_F(CacheDBMTest, ProcessMulti) {
  tkrzw::CacheDBM dbm;
  ProcessMultiTest(&dbm);
}

TEST_F(CacheDBMTest, Random) {
  tkrzw::CacheDBM dbm;
  RandomTest(&dbm, 1);
}

TEST_F(CacheDBMTest, RandomThread) {
  tkrzw::CacheDBM dbm;
  RandomTestThread(&dbm);
}

TEST_F(CacheDBMTest, RebuildRandom) {
  tkrzw::CacheDBM dbm;
  RebuildRandomTest(&dbm);
}

TEST_F(CacheDBMTest, RecordMigration) {
  tkrzw::TemporaryDirectory tmp_dir(true, "tkrzw-");
  const std::string flat_file_path = tmp_dir.MakeUniquePath();
  tkrzw::CacheDBM dbm;
  tkrzw::PositionalParallelFile file;
  EXPECT_EQ(tkrzw::Status::SUCCESS, file.Open(flat_file_path, true));
  RecordMigrationTest(&dbm, &file);
  EXPECT_EQ(tkrzw::Status::SUCCESS, file.Close());
}

TEST_F(CacheDBMTest, LRURemove) {
  tkrzw::CacheDBM dbm(1024);
  for (int32_t i = 0; i < 4096; i++) {
    const std::string key = tkrzw::ToString(i);
    const std::string value = tkrzw::ToString(i * i);
    EXPECT_EQ(tkrzw::Status::SUCCESS, dbm.Set(key, value, false));
  }
  int64_t count = dbm.CountSimple();
  EXPECT_GE(count, 1024);
  EXPECT_LT(count, 2000);
  EXPECT_GT(dbm.GetEffectiveDataSize(), count * 2);
  EXPECT_GT(dbm.GetMemoryUsage(), count * 2 + 1024 * sizeof(char*));
  for (int32_t i = 3500; i < 4096; i++) {
    const std::string key = tkrzw::ToString(i);
    EXPECT_EQ(tkrzw::ToString(i * i), dbm.GetSimple(key));
  }
  for (int32_t i = 0; i < 4096; i++) {
    const std::string key = tkrzw::ToString(i);
    const std::string value = tkrzw::ToString(i * i);
    EXPECT_EQ(tkrzw::Status::SUCCESS, dbm.Set(key, value, false));
    const int32_t small = i % 256;
    const std::string small_key = tkrzw::ToString(small);
    EXPECT_EQ(tkrzw::ToString(small * small), dbm.GetSimple(small_key));
  }
  for (int32_t i = 0; i < 4096; i++) {
    const std::string key = tkrzw::ToString(i);
    const std::string value = tkrzw::ToString(static_cast<int64_t>(std::pow(i, i % 4)));
    EXPECT_EQ(tkrzw::Status::SUCCESS, dbm.Set(key, value));
  }
  for (int32_t i = 0; i < 4096; i += 7) {
    const std::string key = tkrzw::ToString(i);
    const tkrzw::Status status = dbm.Remove(key);
    EXPECT_TRUE(status == tkrzw::Status::SUCCESS || status == tkrzw::Status::NOT_FOUND_ERROR);
  }
  auto iter = dbm.MakeIterator();
  EXPECT_EQ(tkrzw::Status::SUCCESS, iter->First());
  int64_t eff_data_size = 0;
  std::string key, value;
  while (iter->Get(&key, &value).IsOK()) {
    eff_data_size += key.size() + value.size();
    EXPECT_EQ(tkrzw::Status::SUCCESS, iter->Next());
  }
  EXPECT_EQ(eff_data_size, dbm.GetEffectiveDataSize());
  EXPECT_EQ(tkrzw::Status::SUCCESS, iter->First());
  count = dbm.CountSimple();
  while (iter->Remove().IsOK()) {
    count--;
  }
  EXPECT_EQ(0, count);
  EXPECT_EQ(0, dbm.CountSimple());
  EXPECT_EQ(0, dbm.GetEffectiveDataSize());
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm.RebuildAdvanced(10000, tkrzw::INT32MAX));
  for (int32_t i = 0; i < 4096; i++) {
    const std::string key = tkrzw::ToString(i);
    const std::string value = tkrzw::ToString(i * i);
    EXPECT_EQ(tkrzw::Status::SUCCESS, dbm.Set(key, value, false));
  }
  EXPECT_EQ(4096, dbm.CountSimple());
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm.RebuildAdvanced(2048));
  count = dbm.CountSimple();
  EXPECT_GE(count, 2048);
  EXPECT_LT(count, 3000);
  for (int32_t i = 0; i < 4096; i++) {
    const std::string key = tkrzw::ToString(i);
    const std::string value = tkrzw::ToString(i * i);
    EXPECT_EQ(tkrzw::Status::SUCCESS, dbm.Set(key, value, true));
  }
  EXPECT_EQ(count, dbm.CountSimple());
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm.RebuildAdvanced(10000, tkrzw::INT32MAX));
  EXPECT_EQ(count, dbm.CountSimple());
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm.RebuildAdvanced(10000, dbm.GetMemoryUsage() - 20000));
  EXPECT_LT(dbm.CountSimple(), count);
  for (int32_t i = 0; i < 4096; i++) {
    const std::string key = tkrzw::ToString(i);
    const std::string value = tkrzw::ToString(i * i);
    EXPECT_EQ(tkrzw::Status::SUCCESS, dbm.Set(key, value, true));
  }
  EXPECT_LT(dbm.CountSimple(), count);
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm.Clear());
  EXPECT_EQ(0, dbm.CountSimple());
  EXPECT_EQ(0, dbm.GetEffectiveDataSize());
}

TEST_F(CacheDBMTest, GetInternalFile) {
  tkrzw::TemporaryDirectory tmp_dir(true, "tkrzw-");
  const std::string file_path = tmp_dir.MakeUniquePath();
  tkrzw::CacheDBM dbm(1024);
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm.Open(file_path, true, tkrzw::File::OPEN_TRUNCATE));
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm.Set("one", "first"));
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm.Set("two", "second"));
  tkrzw::File* file = dbm.GetInternalFile();
  EXPECT_EQ(0, file->GetSizeSimple());
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm.Close());
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm.Open(file_path, true));
  EXPECT_EQ(2, dbm.CountSimple());
  EXPECT_EQ("first", dbm.GetSimple("one"));
  EXPECT_EQ("second", dbm.GetSimple("two"));
  const int64_t file_size = file->GetSizeSimple();
  EXPECT_GT(file_size, 0);
  EXPECT_EQ(dbm.GetFileSizeSimple(), file_size);
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm.Close());
}

TEST_F(CacheDBMTest, UpdateLogger) {
  tkrzw::CacheDBM dbm(1024);
  UpdateLoggerTest(&dbm);
}

// END OF FILE
