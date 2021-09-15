/*************************************************************************************************
 * Tests for tkrzw_dbm_tiny.h
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
#include "tkrzw_dbm_test_common.h"
#include "tkrzw_dbm_tiny.h"
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

class TinyDBMTest : public CommonDBMTest {};

TEST_F(TinyDBMTest, File) {
  tkrzw::TemporaryDirectory tmp_dir(true, "tkrzw-");
  const std::string file_path = tmp_dir.MakeUniquePath();
  tkrzw::TinyDBM dbm(10);
  FileTest(&dbm, file_path);
}

TEST_F(TinyDBMTest, LargeRecord) {
  tkrzw::TinyDBM dbm(10);
  LargeRecordTest(&dbm);
}

TEST_F(TinyDBMTest, Basic) {
  tkrzw::TinyDBM dbm(1);
  BasicTest(&dbm);
}

TEST_F(TinyDBMTest, Sequence) {
  tkrzw::TinyDBM dbm(1000);
  SequenceTest(&dbm);
}

TEST_F(TinyDBMTest, Append) {
  tkrzw::TinyDBM dbm(100);
  AppendTest(&dbm);
}

TEST_F(TinyDBMTest, Process) {
  tkrzw::TinyDBM dbm(1000);
  ProcessTest(&dbm);
}

TEST_F(TinyDBMTest, ProcessMulti) {
  tkrzw::TinyDBM dbm(5000);
  ProcessMultiTest(&dbm);
}

TEST_F(TinyDBMTest, Random) {
  tkrzw::TinyDBM dbm(10000);
  RandomTest(&dbm, 1);
}

TEST_F(TinyDBMTest, RandomThread) {
  tkrzw::TinyDBM dbm(10000);
  RandomTestThread(&dbm);
}

TEST_F(TinyDBMTest, RebuildRandom) {
  tkrzw::TinyDBM dbm(2000);
  RebuildRandomTest(&dbm);
}

TEST_F(TinyDBMTest, RecordMigration) {
  tkrzw::TemporaryDirectory tmp_dir(true, "tkrzw-");
  const std::string flat_file_path = tmp_dir.MakeUniquePath();
  tkrzw::TinyDBM dbm(100);
  tkrzw::PositionalParallelFile file;
  EXPECT_EQ(tkrzw::Status::SUCCESS, file.Open(flat_file_path, true));
  RecordMigrationTest(&dbm, &file);
  EXPECT_EQ(tkrzw::Status::SUCCESS, file.Close());
}

TEST_F(TinyDBMTest, GetInternalFile) {
  tkrzw::TemporaryDirectory tmp_dir(true, "tkrzw-");
  const std::string file_path = tmp_dir.MakeUniquePath();
  tkrzw::TinyDBM dbm(100);
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

TEST_F(TinyDBMTest, UpdateLogger) {
  tkrzw::TinyDBM dbm(100);
  UpdateLoggerTest(&dbm);
}

// END OF FILE
