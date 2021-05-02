/*************************************************************************************************
 * Tests for tkrzw_file_pos.h
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
#include "tkrzw_file_test_common.h"
#include "tkrzw_file_pos.h"
#include "tkrzw_file_util.h"
#include "tkrzw_lib_common.h"

using namespace testing;

// Main routine
int main(int argc, char** argv) {
  InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

template <class FILE>
class PositionalFileTest : public CommonFileTest<FILE> {};

class PositionalParallelFileTest : public PositionalFileTest<tkrzw::PositionalParallelFile> {};

TEST_F(PositionalParallelFileTest, Attributes) {
  tkrzw::PositionalParallelFile file;
  EXPECT_FALSE(file.IsMemoryMapping());
  EXPECT_FALSE(file.IsAtomic());
}

TEST_F(PositionalParallelFileTest, EmptyFile) {
  EmptyFileTest();
}

TEST_F(PositionalParallelFileTest, SimpleRead) {
  SimpleReadTest();
}

TEST_F(PositionalParallelFileTest, SimpleWrite) {
  SimpleWriteTest();
}

TEST_F(PositionalParallelFileTest, ReallocWrite) {
  ReallocWriteTest();
}

TEST_F(PositionalParallelFileTest, ImplicitClose) {
  ImplicitCloseTest();
}

TEST_F(PositionalParallelFileTest, OpenOptions) {
  OpenOptionsTest();
}

TEST_F(PositionalParallelFileTest, OrderedThread) {
  OrderedThreadTest();
}

TEST_F(PositionalParallelFileTest, RandomThread) {
  RandomThreadTest();
}

TEST_F(PositionalParallelFileTest, FileReader) {
  FileReaderTest();
}

TEST_F(PositionalParallelFileTest, FlatRecord) {
  FlatRecordTest();
}

TEST_F(PositionalParallelFileTest, Rename) {
  RenameTest();
}

class PositionalAtomicFileTest : public PositionalFileTest<tkrzw::PositionalAtomicFile> {};

TEST_F(PositionalAtomicFileTest, Attributes) {
  tkrzw::PositionalAtomicFile file;
  EXPECT_FALSE(file.IsMemoryMapping());
  EXPECT_TRUE(file.IsAtomic());
}

TEST_F(PositionalAtomicFileTest, EmptyFile) {
  EmptyFileTest();
}

TEST_F(PositionalAtomicFileTest, SimpleRead) {
  SimpleReadTest();
}

TEST_F(PositionalAtomicFileTest, SimpleWrite) {
  SimpleWriteTest();
}

TEST_F(PositionalAtomicFileTest, ReallocWrite) {
  ReallocWriteTest();
}

TEST_F(PositionalAtomicFileTest, ImplicitClose) {
  ImplicitCloseTest();
}

TEST_F(PositionalAtomicFileTest, OpenOptions) {
  OpenOptionsTest();
}

TEST_F(PositionalAtomicFileTest, OrderedThread) {
  OrderedThreadTest();
}

TEST_F(PositionalAtomicFileTest, RandomThread) {
  RandomThreadTest();
}

TEST_F(PositionalAtomicFileTest, FileReader) {
  FileReaderTest();
}

TEST_F(PositionalAtomicFileTest, FlatRecord) {
  FlatRecordTest();
}

TEST_F(PositionalAtomicFileTest, Rename) {
  RenameTest();
}

// END OF FILE
