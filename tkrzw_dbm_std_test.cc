/*************************************************************************************************
 * Tests for tkrzw_dbm_std.h
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
#include "tkrzw_dbm_std.h"
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

class StdHashDBMTest : public CommonDBMTest {};

TEST_F(StdHashDBMTest, File) {
  tkrzw::TemporaryDirectory tmp_dir(true, "tkrzw-");
  const std::string file_path = tmp_dir.MakeUniquePath();
  tkrzw::StdHashDBM dbm(10);
  FileTest(&dbm, file_path);
}

TEST_F(StdHashDBMTest, LargeRecord) {
  tkrzw::StdHashDBM dbm(10);
  LargeRecordTest(&dbm);
}

TEST_F(StdHashDBMTest, Basic) {
  tkrzw::StdHashDBM dbm(1);
  BasicTest(&dbm);
}

TEST_F(StdHashDBMTest, Sequence) {
  tkrzw::StdHashDBM dbm(1000);
  SequenceTest(&dbm);
}

TEST_F(StdHashDBMTest, Append) {
  tkrzw::StdHashDBM dbm(100);
  AppendTest(&dbm);
}

TEST_F(StdHashDBMTest, Process) {
  tkrzw::StdHashDBM dbm(1000);
  ProcessTest(&dbm);
}

TEST_F(StdHashDBMTest, ProcessMulti) {
  tkrzw::StdHashDBM dbm(5000);
  ProcessMultiTest(&dbm);
}

TEST_F(StdHashDBMTest, ProcessEach) {
  tkrzw::StdHashDBM dbm(1000);
  ProcessEachTest(&dbm);
}

TEST_F(StdHashDBMTest, Random) {
  tkrzw::StdHashDBM dbm(10000);
  RandomTest(&dbm, 1);
}

TEST_F(StdHashDBMTest, RandomThread) {
  tkrzw::StdHashDBM dbm(10000);
  RandomTestThread(&dbm);
}

TEST_F(StdHashDBMTest, RebuildRandom) {
  tkrzw::StdHashDBM dbm(2000);
  RebuildRandomTest(&dbm);
}

TEST_F(StdHashDBMTest, RecordMigration) {
  tkrzw::TemporaryDirectory tmp_dir(true, "tkrzw-");
  const std::string flat_file_path = tmp_dir.MakeUniquePath();
  tkrzw::StdHashDBM dbm;
  tkrzw::MemoryMapParallelFile file;
  EXPECT_EQ(tkrzw::Status::SUCCESS, file.Open(flat_file_path, true));
  RecordMigrationTest(&dbm, &file);
  EXPECT_EQ(tkrzw::Status::SUCCESS, file.Close());
}

TEST_F(StdHashDBMTest, GetInternalFile) {
  tkrzw::TemporaryDirectory tmp_dir(true, "tkrzw-");
  const std::string file_path = tmp_dir.MakeUniquePath();
  tkrzw::StdHashDBM dbm(100);
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

TEST_F(StdHashDBMTest, UpdateLogger) {
  tkrzw::StdHashDBM dbm(100);
  UpdateLoggerTest(&dbm);
}

class StdTreeDBMTest : public CommonDBMTest {};

TEST_F(StdTreeDBMTest, File) {
  tkrzw::TemporaryDirectory tmp_dir(true, "tkrzw-");
  const std::string file_path = tmp_dir.MakeUniquePath();
  tkrzw::StdTreeDBM dbm;
  FileTest(&dbm, file_path);
}

TEST_F(StdTreeDBMTest, LargeRecord) {
  tkrzw::StdTreeDBM dbm;
  LargeRecordTest(&dbm);
}

TEST_F(StdTreeDBMTest, Basic) {
  tkrzw::StdTreeDBM dbm;
  BasicTest(&dbm);
}

TEST_F(StdTreeDBMTest, Sequence) {
  tkrzw::StdTreeDBM dbm;
  SequenceTest(&dbm);
}

TEST_F(StdTreeDBMTest, Append) {
  tkrzw::StdTreeDBM dbm;
  AppendTest(&dbm);
}

TEST_F(StdTreeDBMTest, Process) {
  tkrzw::StdTreeDBM dbm;
  ProcessTest(&dbm);
}

TEST_F(StdTreeDBMTest, ProcessMulti) {
  tkrzw::StdTreeDBM dbm;
  ProcessMultiTest(&dbm);
}

TEST_F(StdTreeDBMTest, ProcessEach) {
  tkrzw::StdTreeDBM dbm;
  ProcessEachTest(&dbm);
}

TEST_F(StdTreeDBMTest, Random) {
  tkrzw::StdTreeDBM dbm;
  RandomTest(&dbm, 1);
}

TEST_F(StdTreeDBMTest, RandomThread) {
  tkrzw::StdTreeDBM dbm;
  RandomTestThread(&dbm);
}

TEST_F(StdTreeDBMTest, RebuildRandom) {
  tkrzw::StdTreeDBM dbm;
  RebuildRandomTest(&dbm);
}

TEST_F(StdTreeDBMTest, RecordMigration) {
  tkrzw::TemporaryDirectory tmp_dir(true, "tkrzw-");
  const std::string flat_file_path = tmp_dir.MakeUniquePath();
  tkrzw::StdTreeDBM dbm;
  tkrzw::PositionalParallelFile file;
  EXPECT_EQ(tkrzw::Status::SUCCESS, file.Open(flat_file_path, true));
  RecordMigrationTest(&dbm, &file);
  EXPECT_EQ(tkrzw::Status::SUCCESS, file.Close());
}

TEST_F(StdTreeDBMTest, BackIterator) {
  tkrzw::StdTreeDBM dbm;
  BackIteratorTest(&dbm);
}

TEST_F(StdTreeDBMTest, IteratorBound) {
  tkrzw::StdTreeDBM dbm;
  IteratorBoundTest(&dbm);
}

TEST_F(StdTreeDBMTest, GetInternalFile) {
  tkrzw::TemporaryDirectory tmp_dir(true, "tkrzw-");
  const std::string file_path = tmp_dir.MakeUniquePath();
  tkrzw::StdHashDBM dbm;
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

TEST_F(StdTreeDBMTest, UpdateLogger) {
  tkrzw::StdTreeDBM dbm;
  UpdateLoggerTest(&dbm);
}

// END OF FILE
