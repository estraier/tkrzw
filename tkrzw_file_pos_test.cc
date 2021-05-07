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

class PositionalParallelFileTest : public CommonFileTest {};

TEST_F(PositionalParallelFileTest, Attributes) {
  tkrzw::PositionalParallelFile file;
  EXPECT_FALSE(file.IsMemoryMapping());
  EXPECT_FALSE(file.IsAtomic());
}

TEST_F(PositionalParallelFileTest, EmptyFile) {
  tkrzw::PositionalParallelFile file;
  EmptyFileTest(&file);
}

TEST_F(PositionalParallelFileTest, SimpleRead) {
  tkrzw::PositionalParallelFile file;
  SimpleReadTest(&file);
}

TEST_F(PositionalParallelFileTest, SimpleWrite) {
  tkrzw::PositionalParallelFile file;
  SimpleWriteTest(&file);
}

TEST_F(PositionalParallelFileTest, ReallocWrite) {
  tkrzw::PositionalParallelFile file;
  ReallocWriteTest(&file);
}

TEST_F(PositionalParallelFileTest, ImplicitClose) {
  tkrzw::PositionalParallelFile file;
  ImplicitCloseTest(&file);
}

TEST_F(PositionalParallelFileTest, OpenOptions) {
  tkrzw::PositionalParallelFile file;
  OpenOptionsTest(&file);
}

TEST_F(PositionalParallelFileTest, OrderedThread) {
  tkrzw::PositionalParallelFile file;
  OrderedThreadTest(&file);
}

TEST_F(PositionalParallelFileTest, RandomThread) {
  tkrzw::PositionalParallelFile file;
  RandomThreadTest(&file);
}

TEST_F(PositionalParallelFileTest, FileReader) {
  tkrzw::PositionalParallelFile file;
  FileReaderTest(&file);
}

TEST_F(PositionalParallelFileTest, FlatRecord) {
  tkrzw::PositionalParallelFile file;
  FlatRecordTest(&file);
}

TEST_F(PositionalParallelFileTest, Rename) {
  tkrzw::PositionalParallelFile file;
  RenameTest(&file);
}

class PositionalAtomicFileTest : public CommonFileTest {};

TEST_F(PositionalAtomicFileTest, Attributes) {
  tkrzw::PositionalAtomicFile file;
  EXPECT_FALSE(file.IsMemoryMapping());
  EXPECT_TRUE(file.IsAtomic());
}

TEST_F(PositionalAtomicFileTest, EmptyFile) {
  tkrzw::PositionalAtomicFile file;
  EmptyFileTest(&file);
}

TEST_F(PositionalAtomicFileTest, SimpleRead) {
  tkrzw::PositionalAtomicFile file;
  SimpleReadTest(&file);
}

TEST_F(PositionalAtomicFileTest, SimpleWrite) {
  tkrzw::PositionalAtomicFile file;
  SimpleWriteTest(&file);
}

TEST_F(PositionalAtomicFileTest, ReallocWrite) {
  tkrzw::PositionalAtomicFile file;
  ReallocWriteTest(&file);
}

TEST_F(PositionalAtomicFileTest, ImplicitClose) {
  tkrzw::PositionalAtomicFile file;
  ImplicitCloseTest(&file);
}

TEST_F(PositionalAtomicFileTest, OpenOptions) {
  tkrzw::PositionalAtomicFile file;
  OpenOptionsTest(&file);
}

TEST_F(PositionalAtomicFileTest, OrderedThread) {
  tkrzw::PositionalAtomicFile file;
  OrderedThreadTest(&file);
}

TEST_F(PositionalAtomicFileTest, RandomThread) {
  tkrzw::PositionalAtomicFile file;
  RandomThreadTest(&file);
}

TEST_F(PositionalAtomicFileTest, FileReader) {
  tkrzw::PositionalAtomicFile file;
  FileReaderTest(&file);
}

TEST_F(PositionalAtomicFileTest, FlatRecord) {
  tkrzw::PositionalAtomicFile file;
  FlatRecordTest(&file);
}

TEST_F(PositionalAtomicFileTest, Rename) {
  tkrzw::PositionalAtomicFile file;
  RenameTest(&file);
}

// END OF FILE
