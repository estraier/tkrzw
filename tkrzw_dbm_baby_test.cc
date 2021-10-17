/*************************************************************************************************
 * Tests for tkrzw_dbm_baby.h
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
#include "tkrzw_dbm_baby.h"
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

class BabyDBMTest : public CommonDBMTest {};

TEST_F(BabyDBMTest, File) {
  tkrzw::TemporaryDirectory tmp_dir(true, "tkrzw-");
  const std::string file_path = tmp_dir.MakeUniquePath();
  tkrzw::BabyDBM dbm;
  FileTest(&dbm, file_path);
}

TEST_F(BabyDBMTest, LargeRecord) {
  tkrzw::BabyDBM dbm;
  LargeRecordTest(&dbm);
}

TEST_F(BabyDBMTest, Basic) {
  tkrzw::BabyDBM dbm;
  BasicTest(&dbm);
}

TEST_F(BabyDBMTest, Sequence) {
  tkrzw::BabyDBM dbm;
  SequenceTest(&dbm);
}

TEST_F(BabyDBMTest, Append) {
  tkrzw::BabyDBM dbm;
  AppendTest(&dbm);
}

TEST_F(BabyDBMTest, Process) {
  tkrzw::BabyDBM dbm;
  ProcessTest(&dbm);
}

TEST_F(BabyDBMTest, ProcessMulti) {
  tkrzw::BabyDBM dbm;
  ProcessMultiTest(&dbm);
}

TEST_F(BabyDBMTest, Random) {
  tkrzw::BabyDBM dbm;
  RandomTest(&dbm, 1);
}

TEST_F(BabyDBMTest, RandomThread) {
  tkrzw::BabyDBM dbm;
  RandomTestThread(&dbm);
}

TEST_F(BabyDBMTest, RebuildRandom) {
  tkrzw::BabyDBM dbm;
  RebuildRandomTest(&dbm);
}

TEST_F(BabyDBMTest, RecordMigration) {
  tkrzw::TemporaryDirectory tmp_dir(true, "tkrzw-");
  const std::string flat_file_path = tmp_dir.MakeUniquePath();
  tkrzw::BabyDBM dbm;
  tkrzw::PositionalParallelFile file;
  EXPECT_EQ(tkrzw::Status::SUCCESS, file.Open(flat_file_path, true));
  RecordMigrationTest(&dbm, &file);
  EXPECT_EQ(tkrzw::Status::SUCCESS, file.Close());
}

TEST_F(BabyDBMTest, BackIterator) {
  tkrzw::BabyDBM dbm;
  BackIteratorTest(&dbm);
}

TEST_F(BabyDBMTest, IteratorBound) {
  tkrzw::BabyDBM dbm;
  IteratorBoundTest(&dbm);
}

TEST_F(BabyDBMTest, Queue) {
  tkrzw::BabyDBM dbm;
  QueueTest(&dbm);
}

TEST_F(BabyDBMTest, Iterator) {
  tkrzw::BabyDBM dbm;
  std::vector<std::unique_ptr<tkrzw::DBM::Iterator>> iters;
  for (int32_t i = 1; i <= 100; i++) {
    const std::string key = tkrzw::SPrintF("%08d", i);
    const std::string value = tkrzw::ToString(i);
    EXPECT_EQ(tkrzw::Status::SUCCESS, dbm.Set(key, value));
    auto iter = dbm.MakeIterator();
    EXPECT_EQ(tkrzw::Status::SUCCESS, iter->Jump(key));
    std::string iter_key, iter_value;
    EXPECT_EQ(tkrzw::Status::SUCCESS, iter->Get(&iter_key, &iter_value));
    EXPECT_EQ(key, iter_key);
    EXPECT_EQ(value, iter_value);
    iters.emplace_back(std::move(iter));
  }
  EXPECT_EQ(100, dbm.CountSimple());
  auto iter = dbm.MakeIterator();
  EXPECT_EQ(tkrzw::Status::SUCCESS, iter->First());
  int32_t count = 0;
  while (true) {
    std::string iter_key, iter_value;
    const tkrzw::Status status = iter->Get(&iter_key, &iter_value);
    if (status != tkrzw::Status::SUCCESS) {
      EXPECT_EQ(tkrzw::Status::NOT_FOUND_ERROR, status);
      break;
    }
    EXPECT_EQ(count + 1, tkrzw::StrToInt(iter_key));
    EXPECT_EQ(count + 1, tkrzw::StrToInt(iter_value));
    count++;
    EXPECT_EQ(tkrzw::Status::SUCCESS, iter->Next());
  }
  EXPECT_EQ(tkrzw::Status::SUCCESS, iter->First());
  EXPECT_EQ("00000001", iter->GetKey());
  EXPECT_EQ("1", iter->GetValue());
  for (int32_t i = 1; i <= 33; i++) {
    const std::string key = tkrzw::SPrintF("%08d", i);
    EXPECT_EQ(tkrzw::Status::SUCCESS, dbm.Remove(key));
    std::string iter_key, iter_value;
    EXPECT_EQ(tkrzw::Status::SUCCESS, iter->Get(&iter_key, &iter_value));
    EXPECT_EQ(i + 1, tkrzw::StrToInt(iter_key));
    EXPECT_EQ(i + 1, tkrzw::StrToInt(iter_value));
  }
  EXPECT_EQ(67, dbm.CountSimple());
  for (int32_t i = 99; i >= 66; i--) {
    const std::string key = tkrzw::SPrintF("%08d", i);
    EXPECT_EQ(tkrzw::Status::SUCCESS, dbm.Remove(key));
  }
  EXPECT_EQ(33, dbm.CountSimple());
  EXPECT_EQ("00000034", iter->GetKey());
  EXPECT_EQ("34", iter->GetValue());
  for (size_t i = 0; i < iters.size(); i++) {
    std::string iter_key, iter_value;
    EXPECT_EQ(tkrzw::Status::SUCCESS, iters[i]->Get(&iter_key, &iter_value));
    if (i <= 33) {
      EXPECT_EQ(34, tkrzw::StrToInt(iter_key));
    } else if (i >= 65) {
      EXPECT_EQ(100, tkrzw::StrToInt(iter_key));
    } else {
      EXPECT_EQ(i + 1, tkrzw::StrToInt(iter_key));
    }
  }
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm.Clear());
  EXPECT_EQ(0, dbm.CountSimple());
  EXPECT_EQ("*", iter->GetKey("*"));
  EXPECT_EQ("*", iter->GetValue("*"));
  iters.clear();
  for (int32_t i = 0; i <= 100; i++) {
    const std::string key = tkrzw::ToString(i * i);
    const std::string value = tkrzw::ToString(i);
    EXPECT_EQ(tkrzw::Status::SUCCESS, dbm.Set(key, value));
    auto iter = dbm.MakeIterator();
    EXPECT_EQ(tkrzw::Status::SUCCESS, iter->Jump(key));
    std::string iter_key, iter_value;
    EXPECT_EQ(tkrzw::Status::SUCCESS, iter->Get(&iter_key, &iter_value));
    EXPECT_EQ(key, iter_key);
    EXPECT_EQ(value, iter_value);
    iters.emplace_back(std::move(iter));
  }
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm.Set("z", "zzz"));
  for (int32_t i = 0; i <= 50; i++) {
    const std::string key = tkrzw::ToString(i * i);
    EXPECT_EQ(tkrzw::Status::SUCCESS, dbm.Remove(key));
  }
  EXPECT_EQ(51, dbm.CountSimple());
  for (size_t i = 0; i < iters.size(); i++) {
    std::string iter_key, iter_value;
    EXPECT_EQ(tkrzw::Status::SUCCESS, iters[i]->Get(&iter_key, &iter_value));
    if (i > 50) {
      EXPECT_EQ(i * i, tkrzw::StrToInt(iter_key));
      EXPECT_EQ(i, tkrzw::StrToInt(iter_value));
    }
  }
}

TEST_F(BabyDBMTest, Comparator) {
  {
    tkrzw::BabyDBM dbm(tkrzw::LexicalCaseKeyComparator);
    EXPECT_EQ(tkrzw::LexicalCaseKeyComparator, dbm.GetKeyComparator());
    EXPECT_EQ(tkrzw::Status::SUCCESS, dbm.Set("a", "first"));
    EXPECT_EQ(tkrzw::Status::SUCCESS, dbm.Set("A", "first2"));
    EXPECT_EQ(tkrzw::Status::SUCCESS, dbm.Set("B", "second"));
    EXPECT_EQ(tkrzw::Status::SUCCESS, dbm.Set("b", "second2"));
    EXPECT_EQ(tkrzw::Status::SUCCESS, dbm.Set("c", "third"));
    EXPECT_EQ(3, dbm.CountSimple());
    EXPECT_EQ("first2", dbm.GetSimple("a"));
    EXPECT_EQ("second2", dbm.GetSimple("B"));
    EXPECT_EQ("third", dbm.GetSimple("C"));
  }
  {
    tkrzw::BabyDBM dbm(tkrzw::DecimalKeyComparator);
    EXPECT_EQ(tkrzw::DecimalKeyComparator, dbm.GetKeyComparator());
    EXPECT_EQ(tkrzw::Status::SUCCESS, dbm.Set("1", "first"));
    EXPECT_EQ(tkrzw::Status::SUCCESS, dbm.Set("001", "first2"));
    EXPECT_EQ(tkrzw::Status::SUCCESS, dbm.Set("002", "second"));
    EXPECT_EQ(tkrzw::Status::SUCCESS, dbm.Set("2", "second2"));
    EXPECT_EQ(tkrzw::Status::SUCCESS, dbm.Set("  3  ", "third"));
    EXPECT_EQ(3, dbm.CountSimple());
    EXPECT_EQ("first2", dbm.GetSimple("1"));
    EXPECT_EQ("second2", dbm.GetSimple("0002"));
    EXPECT_EQ("third", dbm.GetSimple("3"));
  }
}

TEST_F(BabyDBMTest, GetInternalFile) {
  tkrzw::TemporaryDirectory tmp_dir(true, "tkrzw-");
  const std::string file_path = tmp_dir.MakeUniquePath();
  tkrzw::BabyDBM dbm;
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

TEST_F(BabyDBMTest, UpdateLogger) {
  tkrzw::BabyDBM dbm;
  UpdateLoggerTest(&dbm);
}

// END OF FILE
