/*************************************************************************************************
 * Tests for tkrzw_file_mmap.h
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

#include "gtest/gtest.h"
#include "gmock/gmock.h"

#include "tkrzw_file.h"
#include "tkrzw_file_test_common.h"
#include "tkrzw_file_mmap.h"
#include "tkrzw_file_util.h"
#include "tkrzw_lib_common.h"
#include "tkrzw_sys_config.h"

using namespace testing;

// Main routine
int main(int argc, char** argv) {
  InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

template <class FILE>
class MemoryMapFileTest : public CommonFileTest<FILE> {
 protected:
  void ZoneTest();
  void LockMemoryTest();
};

template <class FILE>
void MemoryMapFileTest<FILE>::ZoneTest() {
  tkrzw::TemporaryDirectory tmp_dir(true, "tkrzw-");
  const std::string file_path = tmp_dir.MakeUniquePath();
  FILE file;
  EXPECT_EQ(tkrzw::Status::SUCCESS, file.Open(file_path, true));
  {
    std::unique_ptr<typename FILE::Zone> zone;
    ASSERT_EQ(tkrzw::Status::SUCCESS, file.MakeZone(true, 0, 4, &zone));
    std::memcpy(zone->Pointer(), "abcd", 4);
  }
  {
    std::unique_ptr<typename FILE::Zone> zone;
    ASSERT_EQ(tkrzw::Status::SUCCESS, file.MakeZone(false, 0, tkrzw::INT32MAX, &zone));
    EXPECT_EQ(4, zone->Size());
    EXPECT_EQ("abcd", std::string(zone->Pointer(), zone->Size()));
  }
  {
    std::unique_ptr<typename FILE::Zone> zone;
    ASSERT_EQ(tkrzw::Status::SUCCESS, file.MakeZone(true, -1, 3, &zone));
    std::memcpy(zone->Pointer(), "xyz", 3);
  }
  {
    std::unique_ptr<typename FILE::Zone> zone;
    ASSERT_EQ(tkrzw::Status::SUCCESS, file.MakeZone(false, 0, tkrzw::INT32MAX, &zone));
    EXPECT_EQ(7, zone->Size());
    EXPECT_EQ("abcdxyz", std::string(zone->Pointer(), zone->Size()));
  }
  {
    std::unique_ptr<typename FILE::Zone> zone;
    ASSERT_EQ(tkrzw::Status::SUCCESS, file.MakeZone(true, 3, 2, &zone));
    std::memcpy(zone->Pointer(), "00", 2);
  }
  {
    std::unique_ptr<typename FILE::Zone> zone;
    ASSERT_EQ(tkrzw::Status::SUCCESS, file.MakeZone(false, 2, 4, &zone));
    EXPECT_EQ(4, zone->Size());
    EXPECT_EQ("c00y", std::string(zone->Pointer(), zone->Size()));
  }
  EXPECT_EQ(tkrzw::Status::SUCCESS, file.Close());
  std::string content;
  EXPECT_EQ(tkrzw::Status::SUCCESS, tkrzw::ReadFile(file_path, &content));
  EXPECT_EQ("abc00yz", content);
}

template <class FILE>
void MemoryMapFileTest<FILE>::LockMemoryTest() {
  tkrzw::TemporaryDirectory tmp_dir(true, "tkrzw-");
  const std::string file_path = tmp_dir.MakeUniquePath();
  FILE file;
  EXPECT_EQ(tkrzw::Status::SUCCESS, file.SetAllocationStrategy(1, 2));
  EXPECT_EQ(tkrzw::Status::SUCCESS, file.Open(file_path, true));
  const std::string data(4096, 'z');
  EXPECT_EQ(tkrzw::Status::SUCCESS, file.Append(data.data(), data.size()));
  EXPECT_EQ(tkrzw::Status::SUCCESS, file.LockMemory(4096));
  EXPECT_EQ(tkrzw::Status::SUCCESS, file.Append(data.data(), data.size()));
  EXPECT_EQ(tkrzw::Status::SUCCESS, file.Append(data.data(), data.size()));
  EXPECT_EQ(tkrzw::Status::SUCCESS, file.Append(data.data(), data.size()));
  EXPECT_EQ(tkrzw::Status::SUCCESS, file.Append(data.data(), data.size()));
  EXPECT_EQ(tkrzw::Status::SUCCESS, file.LockMemory(8192));
  EXPECT_EQ(tkrzw::Status::SUCCESS, file.Append(data.data(), data.size()));
  EXPECT_EQ(tkrzw::Status::SUCCESS, file.Append(data.data(), data.size()));
  EXPECT_EQ(tkrzw::Status::SUCCESS, file.Append(data.data(), data.size()));
  EXPECT_EQ(tkrzw::Status::SUCCESS, file.Truncate(4096));
  EXPECT_EQ(tkrzw::Status::SUCCESS, file.LockMemory(4096));
  EXPECT_EQ(tkrzw::Status::SUCCESS, file.Append(data.data(), data.size()));
  EXPECT_EQ(tkrzw::Status::SUCCESS, file.Append(data.data(), data.size()));
  EXPECT_EQ(tkrzw::Status::SUCCESS, file.Close());
}

class MemoryMapParallelFileTest : public MemoryMapFileTest<tkrzw::MemoryMapParallelFile> {};

TEST_F(MemoryMapParallelFileTest, Attributes) {
  tkrzw::MemoryMapParallelFile file;
  EXPECT_TRUE(file.IsMemoryMapping());
  EXPECT_FALSE(file.IsAtomic());
}

TEST_F(MemoryMapParallelFileTest, EmptyFile) {
  EmptyFileTest();
}

TEST_F(MemoryMapParallelFileTest, SimpleRead) {
  SimpleReadTest();
}

TEST_F(MemoryMapParallelFileTest, SimpleWrite) {
  SimpleWriteTest();
}

TEST_F(MemoryMapParallelFileTest, ReallocWrite) {
  ReallocWriteTest();
}

TEST_F(MemoryMapParallelFileTest, ImplicitClose) {
  ImplicitCloseTest();
}

TEST_F(MemoryMapParallelFileTest, OpenOptions) {
  OpenOptionsTest();
}

TEST_F(MemoryMapParallelFileTest, OrderedThread) {
  OrderedThreadTest();
}

TEST_F(MemoryMapParallelFileTest, RandomThread) {
  RandomThreadTest();
}

TEST_F(MemoryMapParallelFileTest, FileReader) {
  FileReaderTest();
}

TEST_F(MemoryMapParallelFileTest, FlatRecord) {
  FlatRecordTest();
}

TEST_F(MemoryMapParallelFileTest, Zone) {
  ZoneTest();
}

TEST_F(MemoryMapParallelFileTest, LockMemory) {
  LockMemoryTest();
}

class MemoryMapAtomicFileTest : public MemoryMapFileTest<tkrzw::MemoryMapAtomicFile> {};

TEST_F(MemoryMapAtomicFileTest, Attributes) {
  tkrzw::MemoryMapAtomicFile file;
  EXPECT_TRUE(file.IsMemoryMapping());
  EXPECT_TRUE(file.IsAtomic());
}

TEST_F(MemoryMapAtomicFileTest, EmptyFile) {
  EmptyFileTest();
}

TEST_F(MemoryMapAtomicFileTest, SimpleRead) {
  SimpleReadTest();
}

TEST_F(MemoryMapAtomicFileTest, SimpleWrite) {
  SimpleWriteTest();
}

TEST_F(MemoryMapAtomicFileTest, ReallocWrite) {
  ReallocWriteTest();
}

TEST_F(MemoryMapAtomicFileTest, ImplicitClose) {
  ImplicitCloseTest();
}

TEST_F(MemoryMapAtomicFileTest, OpenOptions) {
  OpenOptionsTest();
}

TEST_F(MemoryMapAtomicFileTest, OrderedThread) {
  OrderedThreadTest();
}

TEST_F(MemoryMapAtomicFileTest, RandomThread) {
  RandomThreadTest();
}

TEST_F(MemoryMapAtomicFileTest, FileReader) {
  FileReaderTest();
}

TEST_F(MemoryMapAtomicFileTest, FlatRecord) {
  FlatRecordTest();
}

TEST_F(MemoryMapAtomicFileTest, Zone) {
  ZoneTest();
}

TEST_F(MemoryMapAtomicFileTest, LockMemory) {
  LockMemoryTest();
}

// END OF FILE
