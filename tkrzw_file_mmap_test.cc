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

#include "tkrzw_sys_config.h"

#include "gtest/gtest.h"
#include "gmock/gmock.h"

#include "tkrzw_file.h"
#include "tkrzw_file_mmap.h"
#include "tkrzw_file_test_common.h"
#include "tkrzw_file_util.h"
#include "tkrzw_lib_common.h"

using namespace testing;

// Main routine
int main(int argc, char** argv) {
  InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

template <class FILEIMPL>
class MemoryMapFileTest : public CommonFileTest {
 protected:
  void ZoneTest(FILEIMPL* file);
};

template <class FILEIMPL>
void MemoryMapFileTest<FILEIMPL>::ZoneTest(FILEIMPL* file) {
  tkrzw::TemporaryDirectory tmp_dir(true, "tkrzw-");
  const std::string file_path = tmp_dir.MakeUniquePath();
  EXPECT_EQ(tkrzw::Status::SUCCESS, file->Open(file_path, true));
  {
    std::unique_ptr<typename FILEIMPL::Zone> zone;
    ASSERT_EQ(tkrzw::Status::SUCCESS, file->MakeZone(true, 0, 4, &zone));
    std::memcpy(zone->Pointer(), "abcd", 4);
  }
  {
    std::unique_ptr<typename FILEIMPL::Zone> zone;
    ASSERT_EQ(tkrzw::Status::SUCCESS, file->MakeZone(false, 0, tkrzw::INT32MAX, &zone));
    EXPECT_EQ(4, zone->Size());
    EXPECT_EQ("abcd", std::string(zone->Pointer(), zone->Size()));
  }
  {
    std::unique_ptr<typename FILEIMPL::Zone> zone;
    ASSERT_EQ(tkrzw::Status::SUCCESS, file->MakeZone(true, -1, 3, &zone));
    std::memcpy(zone->Pointer(), "xyz", 3);
  }
  {
    std::unique_ptr<typename FILEIMPL::Zone> zone;
    ASSERT_EQ(tkrzw::Status::SUCCESS, file->MakeZone(false, 0, tkrzw::INT32MAX, &zone));
    EXPECT_EQ(7, zone->Size());
    EXPECT_EQ("abcdxyz", std::string(zone->Pointer(), zone->Size()));
  }
  {
    std::unique_ptr<typename FILEIMPL::Zone> zone;
    ASSERT_EQ(tkrzw::Status::SUCCESS, file->MakeZone(true, 3, 2, &zone));
    std::memcpy(zone->Pointer(), "00", 2);
  }
  {
    std::unique_ptr<typename FILEIMPL::Zone> zone;
    ASSERT_EQ(tkrzw::Status::SUCCESS, file->MakeZone(false, 2, 4, &zone));
    EXPECT_EQ(4, zone->Size());
    EXPECT_EQ("c00y", std::string(zone->Pointer(), zone->Size()));
  }
  EXPECT_EQ(tkrzw::Status::SUCCESS, file->Close());
  std::string content;
  EXPECT_EQ(tkrzw::Status::SUCCESS, tkrzw::ReadFile(file_path, &content));
  EXPECT_EQ("abc00yz", content);
}

class MemoryMapParallelFileTest : public MemoryMapFileTest<tkrzw::MemoryMapParallelFile> {};

TEST_F(MemoryMapParallelFileTest, Attributes) {
  tkrzw::MemoryMapParallelFile file;
  EXPECT_TRUE(file.IsMemoryMapping());
  EXPECT_FALSE(file.IsAtomic());
}

TEST_F(MemoryMapParallelFileTest, EmptyFile) {
  tkrzw::MemoryMapParallelFile file;
  EmptyFileTest(&file);
}

TEST_F(MemoryMapParallelFileTest, SmallFile) {
  tkrzw::MemoryMapParallelFile file;
  SmallFileTest(&file);
}

TEST_F(MemoryMapParallelFileTest, SimpleRead) {
  tkrzw::MemoryMapParallelFile file;
  SimpleReadTest(&file);
}

TEST_F(MemoryMapParallelFileTest, SimpleWrite) {
  tkrzw::MemoryMapParallelFile file;
  SimpleWriteTest(&file);
}

TEST_F(MemoryMapParallelFileTest, ReallocWrite) {
  tkrzw::MemoryMapParallelFile file;
  ReallocWriteTest(&file);
}

TEST_F(MemoryMapParallelFileTest, Truncate) {
  tkrzw::MemoryMapParallelFile file;
  TruncateTest(&file);
}

TEST_F(MemoryMapParallelFileTest, Synchronize) {
  tkrzw::MemoryMapParallelFile file;
  SynchronizeTest(&file);
}

TEST_F(MemoryMapParallelFileTest, ImplicitClose) {
  tkrzw::MemoryMapParallelFile file;
  ImplicitCloseTest(&file);
}

TEST_F(MemoryMapParallelFileTest, OpenOptions) {
  tkrzw::MemoryMapParallelFile file;
  OpenOptionsTest(&file);
}

TEST_F(MemoryMapParallelFileTest, OrderedThread) {
  tkrzw::MemoryMapParallelFile file;
  OrderedThreadTest(&file);
}

TEST_F(MemoryMapParallelFileTest, RandomThread) {
  tkrzw::MemoryMapParallelFile file;
  RandomThreadTest(&file);
}

TEST_F(MemoryMapParallelFileTest, FileReader) {
  tkrzw::MemoryMapParallelFile file;
  FileReaderTest(&file);
}

TEST_F(MemoryMapParallelFileTest, FlatRecord) {
  tkrzw::MemoryMapParallelFile file;
  FlatRecordTest(&file);
}

TEST_F(MemoryMapParallelFileTest, Rename) {
  tkrzw::MemoryMapParallelFile file;
  RenameTest(&file);
}

TEST_F(MemoryMapParallelFileTest, Zone) {
  tkrzw::MemoryMapParallelFile file;
  ZoneTest(&file);
}

class MemoryMapAtomicFileTest : public MemoryMapFileTest<tkrzw::MemoryMapAtomicFile> {};

TEST_F(MemoryMapAtomicFileTest, Attributes) {
  tkrzw::MemoryMapAtomicFile file;
  EXPECT_TRUE(file.IsMemoryMapping());
  EXPECT_TRUE(file.IsAtomic());
}

TEST_F(MemoryMapAtomicFileTest, EmptyFile) {
  tkrzw::MemoryMapAtomicFile file;
  EmptyFileTest(&file);
}

TEST_F(MemoryMapAtomicFileTest, SmallFile) {
  tkrzw::MemoryMapAtomicFile file;
  SmallFileTest(&file);
}

TEST_F(MemoryMapAtomicFileTest, SimpleRead) {
  tkrzw::MemoryMapAtomicFile file;
  SimpleReadTest(&file);
}

TEST_F(MemoryMapAtomicFileTest, SimpleWrite) {
  tkrzw::MemoryMapAtomicFile file;
  SimpleWriteTest(&file);
}

TEST_F(MemoryMapAtomicFileTest, ReallocWrite) {
  tkrzw::MemoryMapAtomicFile file;
  ReallocWriteTest(&file);
}

TEST_F(MemoryMapAtomicFileTest, Truncate) {
  tkrzw::MemoryMapAtomicFile file;
  TruncateTest(&file);
}

TEST_F(MemoryMapAtomicFileTest, Synchronize) {
  tkrzw::MemoryMapAtomicFile file;
  SynchronizeTest(&file);
}

TEST_F(MemoryMapAtomicFileTest, ImplicitClose) {
  tkrzw::MemoryMapAtomicFile file;
  ImplicitCloseTest(&file);
}

TEST_F(MemoryMapAtomicFileTest, OpenOptions) {
  tkrzw::MemoryMapAtomicFile file;
  OpenOptionsTest(&file);
}

TEST_F(MemoryMapAtomicFileTest, OrderedThread) {
  tkrzw::MemoryMapAtomicFile file;
  OrderedThreadTest(&file);
}

TEST_F(MemoryMapAtomicFileTest, RandomThread) {
  tkrzw::MemoryMapAtomicFile file;
  RandomThreadTest(&file);
}

TEST_F(MemoryMapAtomicFileTest, FileReader) {
  tkrzw::MemoryMapAtomicFile file;
  FileReaderTest(&file);
}

TEST_F(MemoryMapAtomicFileTest, FlatRecord) {
  tkrzw::MemoryMapAtomicFile file;
  FlatRecordTest(&file);
}

TEST_F(MemoryMapAtomicFileTest, Rename) {
  tkrzw::MemoryMapAtomicFile file;
  RenameTest(&file);
}

TEST_F(MemoryMapAtomicFileTest, Zone) {
  tkrzw::MemoryMapAtomicFile file;
  ZoneTest(&file);
}

// END OF FILE
