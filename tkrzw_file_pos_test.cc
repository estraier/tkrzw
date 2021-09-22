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
#include "tkrzw_file_pos.h"
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
class PositionalFileTest : public CommonFileTest {
 protected:
  void BlockIOTest(FILEIMPL* file, bool with_pagecache);
  void DirectIOTest(FILEIMPL* file, bool with_pagecache);
};

template <class FILEIMPL>
void PositionalFileTest<FILEIMPL>::BlockIOTest(FILEIMPL* file, bool with_pagecache) {
  tkrzw::TemporaryDirectory tmp_dir(true, "tkrzw-");
  const std::string file_path = tmp_dir.MakeUniquePath();
  EXPECT_EQ(tkrzw::Status::SUCCESS,
            tkrzw::WriteFile(file_path, "012345678901234567890123456789"));
  EXPECT_EQ(1, file->GetBlockSize());
  int32_t access_options = tkrzw::PositionalFile::ACCESS_DEFAULT;
  if (with_pagecache) {
    access_options |= tkrzw::PositionalFile::ACCESS_PAGECACHE;
  }
  EXPECT_EQ(tkrzw::Status::SUCCESS, file->SetAccessStrategy(8, access_options));
  EXPECT_EQ(8, file->GetBlockSize());
  EXPECT_FALSE(file->IsDirectIO());
  EXPECT_EQ(tkrzw::Status::SUCCESS, file->Open(file_path, true, tkrzw::File::OPEN_DEFAULT));
  EXPECT_EQ(8, file->GetBlockSize());
  EXPECT_EQ(tkrzw::Status::SUCCESS, file->SetHeadBuffer(6));
  EXPECT_EQ(tkrzw::Status::SUCCESS, file->Write(0, "ab", 2));
  EXPECT_EQ(tkrzw::Status::SUCCESS, file->Write(3, "cde", 3));
  EXPECT_EQ(tkrzw::Status::SUCCESS, file->Write(5, "EFG", 3));
  EXPECT_EQ(tkrzw::Status::SUCCESS, file->Write(7, "gh", 2));
  EXPECT_EQ(tkrzw::Status::SUCCESS, file->Write(9, "ij", 2));
  char* aligned = static_cast<char*>(tkrzw::xmallocaligned(8, 8));
  std::memset(aligned, 'Z', 8);
  EXPECT_EQ(tkrzw::Status::SUCCESS, file->Write(16, aligned, 8));
  tkrzw::xfreealigned(aligned);
  EXPECT_EQ(tkrzw::Status::SUCCESS, file->Write(24, "xx", 2));
  EXPECT_EQ(tkrzw::Status::SUCCESS, file->Write(30, "zz", 2));
  char buf[256];
  EXPECT_EQ(tkrzw::Status::SUCCESS, file->Read(0, buf, 4));
  EXPECT_EQ("ab2c", std::string_view(buf, 4));
  EXPECT_EQ(tkrzw::Status::SUCCESS, file->Read(4, buf, 4));
  EXPECT_EQ("dEFg", std::string_view(buf, 4));
  EXPECT_EQ(tkrzw::Status::SUCCESS, file->Read(6, buf, 4));
  EXPECT_EQ("Fghi", std::string_view(buf, 4));
  EXPECT_EQ(tkrzw::Status::SUCCESS, file->Read(8, buf, 4));
  EXPECT_EQ("hij1", std::string_view(buf, 4));
  EXPECT_EQ(tkrzw::Status::SUCCESS, file->Read(8, buf, 8));
  EXPECT_EQ("hij12345", std::string_view(buf, 8));
  EXPECT_EQ(tkrzw::Status::SUCCESS, file->Read(12, buf, 8));
  EXPECT_EQ("2345ZZZZ", std::string_view(buf, 8));
  EXPECT_EQ(tkrzw::Status::SUCCESS, file->Read(0, buf, 32));
  EXPECT_EQ("ab2cdEFghij12345ZZZZZZZZxx6789zz", std::string_view(buf, 32));
  EXPECT_EQ(tkrzw::Status::SUCCESS, file->Read(0, buf + 1, 32));
  EXPECT_EQ("ab2cdEFghij12345ZZZZZZZZxx6789zz", std::string_view(buf + 1, 32));
  EXPECT_EQ(tkrzw::Status::SUCCESS, file->Read(0, buf + 1, 32));
  EXPECT_EQ(tkrzw::Status::SUCCESS, file->Read(31, buf, 1));
  EXPECT_EQ("z", std::string_view(buf, 1));
  EXPECT_EQ(tkrzw::Status::INFEASIBLE_ERROR, file->Read(24, buf, 32));
  EXPECT_EQ(tkrzw::Status::SUCCESS, file->Close());
  std::string content;
  EXPECT_EQ(tkrzw::Status::SUCCESS, tkrzw::ReadFile(file_path, &content));
  EXPECT_EQ("ab2cdEFghij12345ZZZZZZZZxx6789zz", content);
  EXPECT_EQ(tkrzw::Status::SUCCESS, file->Open(file_path, true, tkrzw::File::OPEN_TRUNCATE));
  int64_t off = -1;
  EXPECT_EQ(tkrzw::Status::SUCCESS, file->Append("012", 3, &off));
  EXPECT_EQ(0, off);
  EXPECT_EQ(tkrzw::Status::SUCCESS, file->Append("34", 2, &off));
  EXPECT_EQ(3, off);
  EXPECT_EQ(tkrzw::Status::SUCCESS, file->Append("5678901", 7, &off));
  EXPECT_EQ(5, off);
  EXPECT_EQ(tkrzw::Status::SUCCESS, file->Append("2345", 4, &off));
  EXPECT_EQ(12, off);
  aligned = static_cast<char*>(tkrzw::xmallocaligned(16, 8));
  std::memset(aligned, 'X', 8);
  EXPECT_EQ(tkrzw::Status::SUCCESS, file->Append(aligned, 8, &off));
  EXPECT_EQ(16, off);
  EXPECT_EQ(tkrzw::Status::SUCCESS, file->Append(aligned, 4, &off));
  EXPECT_EQ(24, off);
  tkrzw::xfreealigned(aligned);
  EXPECT_EQ(tkrzw::Status::SUCCESS, file->Close());
  EXPECT_EQ(tkrzw::Status::SUCCESS, tkrzw::ReadFile(file_path, &content));
  EXPECT_EQ("0123456789012345XXXXXXXXXXXX", content);
  EXPECT_EQ(28, tkrzw::GetFileSize(file_path));
  EXPECT_EQ(tkrzw::Status::SUCCESS, file->Open(file_path, false));
  EXPECT_EQ(tkrzw::Status::SUCCESS, file->Read(0, buf, 28));
  EXPECT_EQ("0123456789012345XXXXXXXXXXXX", std::string_view(buf, 28));
  EXPECT_EQ(tkrzw::Status::SUCCESS, file->Read(6, buf, 10));
  EXPECT_EQ("6789012345", std::string_view(buf, 10));
  EXPECT_EQ(tkrzw::Status::SUCCESS, file->Read(14, buf, 10));
  EXPECT_EQ("45XXXXXXXX", std::string_view(buf, 10));
  EXPECT_EQ(tkrzw::Status::SUCCESS, file->Read(16, buf, 12));
  EXPECT_EQ("XXXXXXXXXXXX", std::string_view(buf, 12));
  EXPECT_EQ(tkrzw::Status::SUCCESS, file->Close());
  EXPECT_EQ(28,  tkrzw::GetFileSize(file_path));
  EXPECT_EQ(tkrzw::Status::SUCCESS, file->Open(file_path, true));
  EXPECT_EQ(tkrzw::Status::SUCCESS, file->Write(58, "ABCD", 4));
  EXPECT_EQ(tkrzw::Status::SUCCESS, file->Read(58, buf, 4));
  EXPECT_EQ("ABCD", std::string_view(buf, 4));
  EXPECT_EQ(tkrzw::Status::SUCCESS, file->Close());
  EXPECT_EQ(62,  tkrzw::GetFileSize(file_path));
  EXPECT_EQ(tkrzw::Status::SUCCESS, file->Open(file_path, true));
  EXPECT_EQ(tkrzw::Status::SUCCESS, file->Write(5, "ABCDEFGHIJ", 10));
  EXPECT_EQ(tkrzw::Status::SUCCESS, file->Truncate(10));
  EXPECT_EQ(tkrzw::Status::SUCCESS, file->Synchronize(false));
  EXPECT_EQ(10, file->GetSizeSimple());
  const int64_t file_size = tkrzw::GetFileSize(file_path);
  EXPECT_TRUE(file_size == 16 || file_size == 10);
  EXPECT_EQ(tkrzw::Status::SUCCESS, file->Read(5, buf, 5));
  EXPECT_EQ("ABCDE", std::string_view(buf, 5));
  EXPECT_EQ(tkrzw::Status::SUCCESS, file->Close());
  EXPECT_EQ(10, tkrzw::GetFileSize(file_path));
  EXPECT_EQ(tkrzw::Status::SUCCESS,
            file->SetAccessStrategy(128, tkrzw::PositionalFile::ACCESS_PADDING));
  EXPECT_EQ(128, file->GetBlockSize());
  EXPECT_EQ(tkrzw::Status::SUCCESS, file->Open(file_path, true));
  EXPECT_EQ(tkrzw::Status::SUCCESS, file->Read(5, buf, 5));
  EXPECT_EQ("ABCDE", std::string_view(buf, 5));
  EXPECT_EQ(tkrzw::Status::SUCCESS, file->Write(10, "XYZ", 3));
  EXPECT_EQ(tkrzw::Status::SUCCESS, file->Read(5, buf, 8));
  EXPECT_EQ("ABCDEXYZ", std::string_view(buf, 8));
  EXPECT_EQ(13, file->GetSizeSimple());
  EXPECT_EQ(tkrzw::Status::SUCCESS, file->Close());
  EXPECT_EQ(128, tkrzw::GetFileSize(file_path));
}

template <class FILEIMPL>
void PositionalFileTest<FILEIMPL>::DirectIOTest(FILEIMPL* file, bool with_pagecache) {
  tkrzw::TemporaryDirectory tmp_dir(true, "tkrzw-");
  const std::string file_path = tmp_dir.MakeUniquePath();
  constexpr int64_t block_size = 512;
  constexpr int64_t head_buffer_size = block_size * 10;
  constexpr int32_t file_size = block_size * 200;
  constexpr int32_t max_record_size = 1024;
  char* write_buf = static_cast<char*>(tkrzw::xmallocaligned(512, max_record_size));
  char* read_buf = static_cast<char*>(tkrzw::xmallocaligned(512, max_record_size));
  std::mt19937 mt(1);
  std::uniform_int_distribution<int32_t> size_dist(1, max_record_size);
  std::uniform_int_distribution<int32_t> append_dist(0, 1);
  EXPECT_EQ(tkrzw::Status::SUCCESS,
            tkrzw::WriteFile(file_path, "012345678901234567890123456789"));
  int32_t access_options = tkrzw::PositionalFile::ACCESS_DIRECT;
  if (with_pagecache) {
    access_options |= tkrzw::PositionalFile::ACCESS_PAGECACHE;
  }
  EXPECT_EQ(tkrzw::Status::SUCCESS, file->SetAccessStrategy(block_size, access_options));
  EXPECT_EQ(block_size, file->GetBlockSize());
  EXPECT_TRUE(file->IsDirectIO());
  EXPECT_EQ(tkrzw::Status::SUCCESS, file->Open(file_path, true, tkrzw::File::OPEN_TRUNCATE));
  EXPECT_EQ(tkrzw::Status::SUCCESS, file->SetHeadBuffer(head_buffer_size));
  int64_t pos = 0;
  while (pos < file_size + max_record_size * 2) {
    const int32_t record_size = size_dist(mt);
    for (int32_t i = 0; i < record_size; i++) {
      write_buf[i] = 'a' + (pos + i) % 26;
    }
    if (append_dist(mt) == 0) {
      EXPECT_EQ(tkrzw::Status::SUCCESS, file->Write(pos, write_buf, record_size));
    } else {
      EXPECT_EQ(tkrzw::Status::SUCCESS, file->Append(write_buf, record_size));
    }
    pos += record_size;
  }
  pos = 0;
  while (pos < file_size + max_record_size) {
    const int32_t record_size = size_dist(mt);
    for (int32_t i = 0; i < record_size; i++) {
      write_buf[i] = 'A' + (pos + i) % 26;
    }
    EXPECT_EQ(tkrzw::Status::SUCCESS, file->Write(pos, write_buf, record_size));
    pos += record_size + size_dist(mt);
  }
  pos = 0;
  while (pos < file_size + max_record_size) {
    const int32_t record_size = size_dist(mt);
    for (int32_t i = 0; i < record_size; i++) {
      write_buf[i] = 'A' + (pos + i) % 26;
    }
    EXPECT_EQ(tkrzw::Status::SUCCESS, file->Read(pos, read_buf, record_size));
    EXPECT_EQ(tkrzw::StrLowerCase(std::string_view(write_buf, record_size)),
              tkrzw::StrLowerCase(std::string_view(read_buf, record_size)));
    pos += record_size;
  }
  const int64_t final_file_size = file->GetSizeSimple();
  EXPECT_EQ(tkrzw::Status::SUCCESS, file->Close());
  EXPECT_EQ(final_file_size, tkrzw::GetFileSize(file_path));
  tkrzw::xfreealigned(read_buf);
  tkrzw::xfreealigned(write_buf);
  auto tmp_file = file->MakeFile();
  auto* pos_file = dynamic_cast<tkrzw::PositionalFile*>(tmp_file.get());
  EXPECT_NE(nullptr, pos_file);
  EXPECT_EQ(1, pos_file->GetBlockSize());
  EXPECT_FALSE(pos_file->IsDirectIO());
  EXPECT_EQ(tkrzw::Status::SUCCESS, file->CopyProperties(tmp_file.get()));
  EXPECT_TRUE(pos_file->IsDirectIO());
  EXPECT_EQ(block_size, pos_file->GetBlockSize());
  EXPECT_EQ(tkrzw::Status::SUCCESS, file->SetAccessStrategy(
      512, tkrzw::PositionalFile::ACCESS_DIRECT | tkrzw::PositionalFile::ACCESS_PADDING));
  EXPECT_EQ(tkrzw::Status::SUCCESS, file->Open(file_path, true, tkrzw::File::OPEN_TRUNCATE));
  EXPECT_EQ(tkrzw::Status::SUCCESS, file->Write(510, "0123", 4));
  EXPECT_EQ(514, file->GetSizeSimple());
  char buf[256];
  EXPECT_EQ(tkrzw::Status::SUCCESS, file->Read(510, buf, 4));
  EXPECT_EQ("0123", std::string_view(buf, 4));
  EXPECT_EQ(tkrzw::Status::SUCCESS, file->Close());
  EXPECT_EQ(1024, tkrzw::GetFileSize(file_path));
  EXPECT_EQ(tkrzw::Status::SUCCESS, file->Open(file_path, false));
  EXPECT_EQ(tkrzw::Status::SUCCESS, file->TruncateFakely(514));
  EXPECT_EQ(tkrzw::Status::SUCCESS, file->Read(510, buf, 4));
  EXPECT_EQ("0123", std::string_view(buf, 4));
  EXPECT_EQ(tkrzw::Status::SUCCESS, file->Close());
}

class PositionalParallelFileTest : public PositionalFileTest<tkrzw::PositionalParallelFile> {};

TEST_F(PositionalParallelFileTest, Attributes) {
  tkrzw::PositionalParallelFile file;
  EXPECT_FALSE(file.IsMemoryMapping());
  EXPECT_FALSE(file.IsAtomic());
}

TEST_F(PositionalParallelFileTest, EmptyFile) {
  tkrzw::PositionalParallelFile file;
  EmptyFileTest(&file);
}

TEST_F(PositionalParallelFileTest, SmallFile) {
  tkrzw::PositionalParallelFile file;
  SmallFileTest(&file);
}

TEST_F(PositionalParallelFileTest, SimpleRead) {
  tkrzw::PositionalParallelFile file;
  SimpleReadTest(&file);
}

TEST_F(PositionalParallelFileTest, SimpleReadBlock) {
  tkrzw::PositionalParallelFile file;
  EXPECT_EQ(tkrzw::Status::SUCCESS,
            file.SetAccessStrategy(8, tkrzw::PositionalFile::ACCESS_DEFAULT));
  SimpleReadTest(&file);
}

TEST_F(PositionalParallelFileTest, SimpleWrite) {
  tkrzw::PositionalParallelFile file;
  SimpleWriteTest(&file);
}

TEST_F(PositionalParallelFileTest, SimpleWriteBlock) {
  tkrzw::PositionalParallelFile file;
  EXPECT_EQ(tkrzw::Status::SUCCESS,
            file.SetAccessStrategy(8, tkrzw::PositionalFile::ACCESS_DEFAULT));
  SimpleWriteTest(&file);
}

TEST_F(PositionalParallelFileTest, ReallocWrite) {
  tkrzw::PositionalParallelFile file;
  ReallocWriteTest(&file);
}

TEST_F(PositionalParallelFileTest, ReallocWriteBlock) {
  tkrzw::PositionalParallelFile file;
  EXPECT_EQ(tkrzw::Status::SUCCESS,
            file.SetAccessStrategy(8, tkrzw::PositionalFile::ACCESS_DEFAULT));
  ReallocWriteTest(&file);
}

TEST_F(PositionalParallelFileTest, Truncate) {
  tkrzw::PositionalParallelFile file;
  TruncateTest(&file);
}

TEST_F(PositionalParallelFileTest, Synchronize) {
  tkrzw::PositionalParallelFile file;
  SynchronizeTest(&file);
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

TEST_F(PositionalParallelFileTest, OrderedThreadBlock) {
  tkrzw::PositionalParallelFile file;
  EXPECT_EQ(tkrzw::Status::SUCCESS,
            file.SetAccessStrategy(128, tkrzw::PositionalFile::ACCESS_DEFAULT));
  OrderedThreadTest(&file);
}

TEST_F(PositionalParallelFileTest, RandomThread) {
  tkrzw::PositionalParallelFile file;
  RandomThreadTest(&file);
}

TEST_F(PositionalParallelFileTest, RandomThreadBlock) {
  tkrzw::PositionalParallelFile file;
  EXPECT_EQ(tkrzw::Status::SUCCESS,
            file.SetAccessStrategy(32, tkrzw::PositionalFile::ACCESS_DEFAULT));
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

TEST_F(PositionalParallelFileTest, BlockIO) {
  tkrzw::PositionalParallelFile file;
  BlockIOTest(&file, false);
}

TEST_F(PositionalParallelFileTest, BlockIOPage) {
  tkrzw::PositionalParallelFile file;
  BlockIOTest(&file, true);
}

TEST_F(PositionalParallelFileTest, DirectIO) {
  tkrzw::PositionalParallelFile file;
  DirectIOTest(&file, false);
}

TEST_F(PositionalParallelFileTest, DirectIOPage) {
  tkrzw::PositionalParallelFile file;
  DirectIOTest(&file, true);
}

class PositionalAtomicFileTest : public PositionalFileTest<tkrzw::PositionalAtomicFile> {};

TEST_F(PositionalAtomicFileTest, Attributes) {
  tkrzw::PositionalAtomicFile file;
  EXPECT_FALSE(file.IsMemoryMapping());
  EXPECT_TRUE(file.IsAtomic());
}

TEST_F(PositionalAtomicFileTest, EmptyFile) {
  tkrzw::PositionalAtomicFile file;
  EmptyFileTest(&file);
}

TEST_F(PositionalAtomicFileTest, SmallFile) {
  tkrzw::PositionalAtomicFile file;
  SmallFileTest(&file);
}

TEST_F(PositionalAtomicFileTest, SimpleRead) {
  tkrzw::PositionalAtomicFile file;
  SimpleReadTest(&file);
}

TEST_F(PositionalAtomicFileTest, SimpleReadBlock) {
  tkrzw::PositionalAtomicFile file;
  EXPECT_EQ(tkrzw::Status::SUCCESS,
            file.SetAccessStrategy(8, tkrzw::PositionalFile::ACCESS_DEFAULT));
  SimpleReadTest(&file);
}

TEST_F(PositionalAtomicFileTest, SimpleWrite) {
  tkrzw::PositionalAtomicFile file;
  SimpleWriteTest(&file);
}

TEST_F(PositionalAtomicFileTest, SimpleWriteBlock) {
  tkrzw::PositionalAtomicFile file;
  EXPECT_EQ(tkrzw::Status::SUCCESS,
            file.SetAccessStrategy(8, tkrzw::PositionalFile::ACCESS_DEFAULT));
  SimpleWriteTest(&file);
}

TEST_F(PositionalAtomicFileTest, ReallocWrite) {
  tkrzw::PositionalAtomicFile file;
  ReallocWriteTest(&file);
}

TEST_F(PositionalAtomicFileTest, ReallocWriteBlock) {
  tkrzw::PositionalAtomicFile file;
  EXPECT_EQ(tkrzw::Status::SUCCESS,
            file.SetAccessStrategy(8, tkrzw::PositionalFile::ACCESS_DEFAULT));
  ReallocWriteTest(&file);
}

TEST_F(PositionalAtomicFileTest, Truncate) {
  tkrzw::PositionalAtomicFile file;
  TruncateTest(&file);
}

TEST_F(PositionalAtomicFileTest, Synchronize) {
  tkrzw::PositionalAtomicFile file;
  SynchronizeTest(&file);
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

TEST_F(PositionalAtomicFileTest, OrderedThreadBlock) {
  tkrzw::PositionalAtomicFile file;
  EXPECT_EQ(tkrzw::Status::SUCCESS,
            file.SetAccessStrategy(128, tkrzw::PositionalFile::ACCESS_DEFAULT));
  OrderedThreadTest(&file);
}

TEST_F(PositionalAtomicFileTest, RandomThread) {
  tkrzw::PositionalAtomicFile file;
  RandomThreadTest(&file);
}

TEST_F(PositionalAtomicFileTest, RandomThreadBlock) {
  tkrzw::PositionalAtomicFile file;
  EXPECT_EQ(tkrzw::Status::SUCCESS,
            file.SetAccessStrategy(32, tkrzw::PositionalFile::ACCESS_DEFAULT));
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

TEST_F(PositionalAtomicFileTest, BlockIO) {
  tkrzw::PositionalAtomicFile file;
  BlockIOTest(&file, false);
}

TEST_F(PositionalAtomicFileTest, BlockIOPage) {
  tkrzw::PositionalAtomicFile file;
  BlockIOTest(&file, true);
}

TEST_F(PositionalAtomicFileTest, DirectIO) {
  tkrzw::PositionalAtomicFile file;
  DirectIOTest(&file, false);
}

TEST_F(PositionalAtomicFileTest, DirectIOPage) {
  tkrzw::PositionalAtomicFile file;
  DirectIOTest(&file, true);
}

// END OF FILE
