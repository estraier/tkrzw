/*************************************************************************************************
 * Common tests for File implementations
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
#include "tkrzw_file_util.h"
#include "tkrzw_lib_common.h"
#include "tkrzw_str_util.h"

using namespace testing;

class CommonFileTest : public Test {
 protected:
  void EmptyFileTest(tkrzw::File* file);
  void SmallFileTest(tkrzw::File* file);
  void SimpleReadTest(tkrzw::File* file);
  void SimpleWriteTest(tkrzw::File* file);
  void ReallocWriteTest(tkrzw::File* file);
  void TruncateTest(tkrzw::File* file);
  void ImplicitCloseTest(tkrzw::File* file);
  void SynchronizeTest(tkrzw::File* file);
  void OpenOptionsTest(tkrzw::File* file);
  void OrderedThreadTest(tkrzw::File* file);
  void RandomThreadTest(tkrzw::File* file);
  void FileReaderTest(tkrzw::File* file);
  void FlatRecordTest(tkrzw::File* file);
  void RenameTest(tkrzw::File* file);
};

void CommonFileTest::EmptyFileTest(tkrzw::File* file) {
  tkrzw::TemporaryDirectory tmp_dir(true, "tkrzw-");
  const std::string file_path = tmp_dir.MakeUniquePath();
  EXPECT_FALSE(file->IsOpen());
  EXPECT_EQ(tkrzw::Status::SUCCESS, file->SetAllocationStrategy(1, 1.2));
  EXPECT_EQ(tkrzw::Status::SUCCESS, file->Open(file_path, true, tkrzw::File::OPEN_DEFAULT));
  EXPECT_TRUE(file->IsOpen());
  int64_t file_size = -1;
  EXPECT_EQ(tkrzw::Status::SUCCESS, file->GetSize(&file_size));
  EXPECT_EQ(0, file_size);
  EXPECT_EQ(tkrzw::Status::SUCCESS, file->Close());
  EXPECT_EQ(0, tkrzw::GetFileSize(file_path));
  EXPECT_EQ(tkrzw::Status::SUCCESS, file->Open(file_path, true, tkrzw::File::OPEN_DEFAULT));
  EXPECT_EQ(tkrzw::Status::SUCCESS, file->GetSize(&file_size));
  EXPECT_EQ(0, file_size);
  EXPECT_EQ(tkrzw::Status::SUCCESS, file->Synchronize(false));
  EXPECT_EQ(0, tkrzw::GetFileSize(file_path));
  EXPECT_EQ(tkrzw::Status::SUCCESS, file->Close());
  EXPECT_EQ(0, tkrzw::GetFileSize(file_path));
  EXPECT_EQ(tkrzw::Status::SUCCESS, file->Open(file_path, false));
  file_size = -1;
  EXPECT_EQ(tkrzw::Status::SUCCESS, file->GetSize(&file_size));
  EXPECT_EQ(0, file_size);
  EXPECT_EQ(tkrzw::Status::SUCCESS, file->Close());
  EXPECT_EQ(0, tkrzw::GetFileSize(file_path));
  auto tmp_file = file->MakeFile();
  EXPECT_EQ(file->GetType(), tmp_file->GetType());
  EXPECT_EQ(tkrzw::Status::SUCCESS, file->CopyProperties(tmp_file.get()));
  EXPECT_EQ(tkrzw::Status::SUCCESS, tmp_file->Open(file_path, true, tkrzw::File::OPEN_TRUNCATE));
  EXPECT_EQ(tkrzw::Status::SUCCESS, tmp_file->Close());
}

void CommonFileTest::SmallFileTest(tkrzw::File* file) {
  tkrzw::TemporaryDirectory tmp_dir(true, "tkrzw-");
  const std::string file_path = tmp_dir.MakeUniquePath();
  EXPECT_EQ(tkrzw::Status::SUCCESS, file->SetAllocationStrategy(0, 0));
  EXPECT_EQ(tkrzw::Status::SUCCESS, file->Open(file_path, true, tkrzw::File::OPEN_DEFAULT));
  std::string total_data;
  for (int32_t i = 0; i < 20; i++) {
    const std::string data(i % 3 + 1, 'a' + i % 3);
    if (i % 2 == 0) {
      EXPECT_EQ(tkrzw::Status::SUCCESS, file->Write(total_data.size(), data.data(), data.size()));
    } else {
      int64_t new_off = 0;
      EXPECT_EQ(tkrzw::Status::SUCCESS, file->Append(data.data(), data.size(), &new_off));
      EXPECT_EQ(total_data.size(), new_off);
    }
    total_data += data;
    if (file->IsMemoryMapping()) {
      EXPECT_EQ(tkrzw::AlignNumber(total_data.size(), tkrzw::PAGE_SIZE),
                tkrzw::GetFileSize(file_path));
    } else {
      EXPECT_EQ(total_data.size(), tkrzw::GetFileSize(file_path));
    }
    char buf[5];
    EXPECT_EQ(tkrzw::Status::SUCCESS,
              file->Read(total_data.size() - data.size(), buf, data.size()));
    EXPECT_EQ(data, std::string_view(buf, data.size()));
  }
  EXPECT_EQ(tkrzw::Status::SUCCESS, file->Close());
  EXPECT_EQ(total_data.size(), tkrzw::GetFileSize(file_path));
  EXPECT_EQ(tkrzw::Status::SUCCESS, file->Open(file_path, true, tkrzw::File::OPEN_DEFAULT));
  char buf[8192];
  EXPECT_EQ(tkrzw::Status::SUCCESS, file->Read(0, buf, total_data.size()));
  EXPECT_EQ(total_data, std::string_view(buf, total_data.size()));
  EXPECT_EQ(tkrzw::Status::SUCCESS, file->Truncate(5));
  if (file->IsMemoryMapping()) {
    EXPECT_EQ(tkrzw::PAGE_SIZE, tkrzw::GetFileSize(file_path));
  } else {
    EXPECT_EQ(5, tkrzw::GetFileSize(file_path));
  }
  EXPECT_EQ(tkrzw::Status::SUCCESS, file->Close());
  EXPECT_EQ(5, tkrzw::GetFileSize(file_path));
  EXPECT_EQ(tkrzw::Status::SUCCESS, file->SetAllocationStrategy(1, 2));
  EXPECT_EQ(tkrzw::Status::SUCCESS, file->Open(file_path, true, tkrzw::File::OPEN_DEFAULT));
  EXPECT_EQ(tkrzw::Status::SUCCESS, file->Write(0, "0123456789", 10));
  EXPECT_EQ(tkrzw::Status::SUCCESS, file->Read(0, buf, 10));
  EXPECT_EQ("0123456789", std::string_view(buf, 10));
  EXPECT_EQ(tkrzw::Status::SUCCESS, file->Truncate(5));
  EXPECT_EQ(tkrzw::Status::SUCCESS, file->Synchronize(false));
  EXPECT_EQ(5, tkrzw::GetFileSize(file_path));
  EXPECT_EQ(tkrzw::Status::SUCCESS, file->Close());
  EXPECT_EQ(5, tkrzw::GetFileSize(file_path));
}

void CommonFileTest::SimpleReadTest(tkrzw::File* file) {
  tkrzw::TemporaryDirectory tmp_dir(true, "tkrzw-");
  const std::string file_path = tmp_dir.MakeUniquePath();
  EXPECT_EQ(tkrzw::Status::SUCCESS, tkrzw::WriteFile(file_path, "0123456789"));
  EXPECT_EQ(tkrzw::Status::SUCCESS, file->Open(file_path, false));
  char buf[10];
  EXPECT_EQ(tkrzw::Status::SUCCESS, file->Read(0, buf, 10));
  EXPECT_EQ("0123456789", std::string(buf, 10));
  EXPECT_EQ(tkrzw::Status::SUCCESS, file->Read(5, buf, 3));
  EXPECT_EQ("567", std::string(buf, 3));
  EXPECT_EQ("567", file->ReadSimple(5, 3));
  EXPECT_EQ(tkrzw::Status::INFEASIBLE_ERROR, file->Read(11, buf, 3));
  EXPECT_EQ(tkrzw::Status::INFEASIBLE_ERROR, file->Read(9, buf, 3));
  EXPECT_EQ("", file->ReadSimple(11, 3));
  EXPECT_EQ("", file->ReadSimple(9, 3));
  EXPECT_EQ(tkrzw::Status::SUCCESS, file->Close());
}

void CommonFileTest::SimpleWriteTest(tkrzw::File* file) {
  tkrzw::TemporaryDirectory tmp_dir(true, "tkrzw-");
  const std::string file_path = tmp_dir.MakeUniquePath();
  EXPECT_EQ(tkrzw::Status::SUCCESS, file->Open(file_path, true));
  int64_t off = -1;
  EXPECT_EQ(tkrzw::Status::SUCCESS, file->Append("01234", 5, &off));
  EXPECT_EQ(0, off);
  EXPECT_EQ(tkrzw::Status::SUCCESS, file->Append("56789", 5, &off));
  EXPECT_EQ(5, off);
  EXPECT_EQ(tkrzw::Status::SUCCESS, file->Write(3, "XYZ", 3));
  EXPECT_TRUE(file->WriteSimple(8, "ABCDEF"));
  EXPECT_EQ(14, file->AppendSimple("GHI"));
  EXPECT_EQ(tkrzw::Status::SUCCESS, file->Expand(2, &off));
  EXPECT_EQ(17, off);
  EXPECT_EQ(19, file->ExpandSimple(2));
  EXPECT_EQ(tkrzw::Status::SUCCESS, file->Write(off, "JKLMN", 5));
  EXPECT_EQ(tkrzw::Status::SUCCESS, file->Close());
  std::string content;
  EXPECT_EQ(tkrzw::Status::SUCCESS, tkrzw::ReadFile(file_path, &content));
  EXPECT_EQ("012XYZ67ABCDEFGHIJKLMN", content);
}

void CommonFileTest::ReallocWriteTest(tkrzw::File* file) {
  tkrzw::TemporaryDirectory tmp_dir(true, "tkrzw-");
  const std::string file_path = tmp_dir.MakeUniquePath();
  EXPECT_EQ(tkrzw::Status::SUCCESS, file->SetAllocationStrategy(1, 1.2));
  EXPECT_EQ(tkrzw::Status::SUCCESS, file->Open(file_path, true));
  for (int32_t i = 0; i < 10000; i++) {
    EXPECT_EQ(tkrzw::Status::SUCCESS, file->Append("0123456789", 10));
  }
  int64_t size = 0;
  EXPECT_EQ(tkrzw::Status::SUCCESS, file->GetSize(&size));
  EXPECT_EQ(100000, size);
  EXPECT_EQ(100000, file->GetSizeSimple());
  for (int32_t i = 0; i < 10000; i++) {
    char buf[10];
    EXPECT_EQ(tkrzw::Status::SUCCESS, file->Read(i * 10, buf, 10));
    EXPECT_EQ("0123456789", std::string(buf, 10));
  }
  EXPECT_EQ(tkrzw::Status::SUCCESS, file->Close());
  EXPECT_EQ(100000, tkrzw::GetFileSize(file_path));
}

void CommonFileTest::TruncateTest(tkrzw::File* file) {
  tkrzw::TemporaryDirectory tmp_dir(true, "tkrzw-");
  const std::string file_path = tmp_dir.MakeUniquePath();
  EXPECT_EQ(tkrzw::Status::SUCCESS, file->SetAllocationStrategy(1, 1.2));
  char buf[256];
  int64_t offset = 0;
  EXPECT_EQ(tkrzw::Status::SUCCESS,
            file->Open(file_path, true, tkrzw::File::OPEN_TRUNCATE));
  EXPECT_EQ(0, file->GetSizeSimple());
  EXPECT_EQ(tkrzw::Status::SUCCESS, file->Truncate(8192));
  EXPECT_EQ(8192, file->GetSizeSimple());
  file->Read(4096, buf, 3);
  EXPECT_EQ(tkrzw::Status::SUCCESS, file->Append("abc", 3, &offset));
  EXPECT_EQ(8192, offset);
  EXPECT_EQ(8195, file->GetSizeSimple());
  EXPECT_EQ(tkrzw::Status::SUCCESS, file->Read(8192, buf, 3));
  EXPECT_EQ("abc", std::string_view(buf, 3));
  EXPECT_EQ(tkrzw::Status::SUCCESS, file->Truncate(1024));
  EXPECT_EQ(1024, file->GetSizeSimple());
  EXPECT_EQ(tkrzw::Status::SUCCESS, file->Append("abc", 3, &offset));
  EXPECT_EQ(1027, file->GetSizeSimple());
  EXPECT_EQ(tkrzw::Status::SUCCESS, file->Read(1024, buf, 3));
  EXPECT_EQ("abc", std::string_view(buf, 3));
  EXPECT_EQ(tkrzw::Status::SUCCESS, file->Close());
  EXPECT_EQ(1027, tkrzw::GetFileSize(file_path));
  EXPECT_EQ(tkrzw::Status::SUCCESS,
            file->Open(file_path, true, tkrzw::File::OPEN_TRUNCATE));
  EXPECT_EQ(0, file->GetSizeSimple());
  EXPECT_EQ(tkrzw::Status::SUCCESS, file->Truncate(65536));
  EXPECT_EQ(tkrzw::Status::SUCCESS, file->TruncateFakely(8192));
  EXPECT_EQ(8192, file->GetSizeSimple());
  EXPECT_EQ(tkrzw::Status::SUCCESS, file->Read(4096, buf, 3));
  EXPECT_EQ(tkrzw::Status::SUCCESS, file->Append("abc", 3, &offset));
  EXPECT_EQ(8192, offset);
  EXPECT_EQ(8195, file->GetSizeSimple());
  EXPECT_EQ(tkrzw::Status::SUCCESS, file->Read(8192, buf, 3));
  EXPECT_EQ("abc", std::string_view(buf, 3));
  EXPECT_EQ(tkrzw::Status::SUCCESS, file->TruncateFakely(1024));
  EXPECT_EQ(1024, file->GetSizeSimple());
  EXPECT_EQ(tkrzw::Status::SUCCESS, file->Append("abc", 3, &offset));
  EXPECT_EQ(1027, file->GetSizeSimple());
  EXPECT_EQ(tkrzw::Status::SUCCESS, file->Read(1024, buf, 3));
  EXPECT_EQ("abc", std::string_view(buf, 3));
  EXPECT_EQ(tkrzw::Status::SUCCESS, file->Close());
  EXPECT_EQ(1027, tkrzw::GetFileSize(file_path));
  EXPECT_EQ(tkrzw::Status::SUCCESS, file->Open(file_path, false));
  EXPECT_EQ(1027, file->GetSizeSimple());
  EXPECT_EQ(tkrzw::Status::SUCCESS, file->Read(1024, buf, 3));
  EXPECT_EQ("abc", std::string_view(buf, 3));
  EXPECT_EQ(tkrzw::Status::SUCCESS, file->TruncateFakely(512));
  EXPECT_EQ(512, file->GetSizeSimple());
  EXPECT_EQ(tkrzw::Status::INFEASIBLE_ERROR, file->TruncateFakely(65536));
  EXPECT_EQ(tkrzw::Status::SUCCESS, file->Close());
}

void CommonFileTest::SynchronizeTest(tkrzw::File* file) {
  tkrzw::TemporaryDirectory tmp_dir(true, "tkrzw-");
  const std::string file_path = tmp_dir.MakeUniquePath();
  EXPECT_EQ(tkrzw::Status::SUCCESS, file->SetAllocationStrategy(1, 1.2));
  char buf[1024];
  std::memset(buf, 0, std::size(buf));
  EXPECT_EQ(tkrzw::Status::SUCCESS,
            file->Open(file_path, true, tkrzw::File::OPEN_TRUNCATE));
  for (int32_t i = 0; i < 8; i++) {
    EXPECT_EQ(tkrzw::Status::SUCCESS, file->Append(buf, std::size(buf)));
    EXPECT_EQ(tkrzw::Status::SUCCESS, file->Append(buf, std::size(buf)));
  }
  const int64_t file_size = file->GetSizeSimple();
  for (int64_t off = 0; off < file_size; off += 256) {
    EXPECT_EQ(tkrzw::Status::SUCCESS, file->Synchronize(true, off, 256));
  }
  EXPECT_EQ(tkrzw::Status::SUCCESS, file->Synchronize(true, 0, 0));
  EXPECT_EQ(tkrzw::Status::SUCCESS, file->Synchronize(true, 1, 0));
  EXPECT_EQ(tkrzw::Status::SUCCESS, file->Synchronize(true, 0, 8192));
  EXPECT_EQ(tkrzw::Status::SUCCESS, file->Synchronize(true, 8192, 0));
  EXPECT_EQ(tkrzw::Status::SUCCESS, file->Close());
}

void CommonFileTest::ImplicitCloseTest(tkrzw::File* file) {
  tkrzw::TemporaryDirectory tmp_dir(true, "tkrzw-");
  const std::string file_path = tmp_dir.MakeUniquePath();
  {
    auto tmp_file = file->MakeFile();
    EXPECT_EQ(tkrzw::Status::SUCCESS, tmp_file->Open(file_path, true));
    EXPECT_EQ(tkrzw::Status::SUCCESS, tmp_file->Append("0123456789", 10));
  }
  EXPECT_EQ(10, tkrzw::GetFileSize(file_path));
}

void CommonFileTest::OpenOptionsTest(tkrzw::File* file) {
  tkrzw::TemporaryDirectory tmp_dir(true, "tkrzw-");
  const std::string file_path = tmp_dir.MakeUniquePath();
  EXPECT_EQ(tkrzw::Status::NOT_FOUND_ERROR,
            file->Open(file_path, true, tkrzw::File::OPEN_NO_CREATE));
  EXPECT_EQ(tkrzw::Status::SUCCESS, tkrzw::WriteFile(file_path, "abc"));
  EXPECT_EQ(tkrzw::Status::SUCCESS, file->Open(file_path, true, tkrzw::File::OPEN_NO_CREATE));
  EXPECT_EQ(tkrzw::Status::SUCCESS, file->Close());
  EXPECT_EQ(tkrzw::Status::SUCCESS, file->Open(file_path, true, tkrzw::File::OPEN_NO_LOCK));
  EXPECT_EQ(tkrzw::Status::SUCCESS, file->Append("def", 3));
  EXPECT_EQ(tkrzw::Status::SUCCESS, file->Close());
  EXPECT_EQ(6, tkrzw::GetFileSize(file_path));
  EXPECT_EQ(tkrzw::Status::SUCCESS, file->Open(file_path, true, tkrzw::File::OPEN_NO_WAIT));
  EXPECT_EQ(tkrzw::Status::SUCCESS, file->Append("efg", 3));
  EXPECT_EQ(tkrzw::Status::SUCCESS, file->Close());
  EXPECT_EQ(9, tkrzw::GetFileSize(file_path));
  EXPECT_EQ(tkrzw::Status::SUCCESS, file->Open(file_path, true, tkrzw::File::OPEN_TRUNCATE));
  EXPECT_EQ(tkrzw::Status::SUCCESS, file->Close());
  EXPECT_EQ(0, tkrzw::GetFileSize(file_path));
}

void CommonFileTest::OrderedThreadTest(tkrzw::File* file) {
  constexpr int32_t num_threads = 10;
  constexpr int32_t num_iterations = 10000;
  constexpr int32_t record_size = 128;
  tkrzw::TemporaryDirectory tmp_dir(true, "tkrzw-");
  const std::string file_path = tmp_dir.MakeUniquePath();
  EXPECT_EQ(tkrzw::Status::SUCCESS, file->SetAllocationStrategy(1, 1.2));
  EXPECT_EQ(tkrzw::Status::SUCCESS, file->Open(file_path, true));
  auto write_func = [&](int32_t id) {
    char* write_buf = new char[record_size];
    std::memset(write_buf, '0' + id, record_size);
    char* read_buf = new char[record_size];
    std::memset(read_buf, 0, record_size);
    for (int32_t i = 0; i < num_iterations; i++) {
      const int32_t off = (i * num_threads + id) * record_size;
      EXPECT_EQ(tkrzw::Status::SUCCESS, file->Write(off, write_buf, record_size));
      if (i % 2 == 0) {
        std::this_thread::yield();
      }
      EXPECT_EQ(tkrzw::Status::SUCCESS, file->Read(off, read_buf, record_size));
      EXPECT_EQ(0, std::memcmp(read_buf, write_buf, record_size));
    }
    delete[] read_buf;
    delete[] write_buf;
  };
  std::vector<std::thread> write_threads;
  for (int32_t i = 0; i < num_threads; i++) {
    write_threads.emplace_back(std::thread(write_func, i));
  }
  for (auto& write_thread : write_threads) {
    write_thread.join();
  }
  EXPECT_EQ(tkrzw::Status::SUCCESS, file->Close());
  EXPECT_EQ(num_iterations * num_threads * record_size, tkrzw::GetFileSize(file_path));
  EXPECT_EQ(tkrzw::Status::SUCCESS, file->Open(file_path, false));
  auto read_func = [&](int32_t id) {
    char* expected_buf = new char[record_size];
    std::memset(expected_buf, '0' + id, record_size);
    char* read_buf = new char[record_size];
    std::memset(read_buf, 0, record_size);
    for (int32_t i = 0; i < num_iterations; i++) {
      const int32_t off = (i * num_threads + id) * record_size;
      EXPECT_EQ(tkrzw::Status::SUCCESS, file->Read(off, read_buf, record_size));
      EXPECT_EQ(0, std::memcmp(read_buf, expected_buf, record_size));
    }
    delete[] read_buf;
    delete[] expected_buf;
  };
  std::vector<std::thread> read_threads;
  for (int32_t i = 0; i < num_threads; i++) {
    read_threads.emplace_back(std::thread(read_func, i));
  }
  for (auto& read_thread : read_threads) {
    read_thread.join();
  }
  EXPECT_EQ(tkrzw::Status::SUCCESS, file->Close());
}

void CommonFileTest::RandomThreadTest(tkrzw::File* file) {
  constexpr int32_t num_threads = 10;
  constexpr int32_t num_iterations = 10000;
  constexpr int32_t file_size = 100000;
  constexpr int32_t record_size = 256;
  tkrzw::TemporaryDirectory tmp_dir(true, "tkrzw-");
  const std::string file_path = tmp_dir.MakeUniquePath();
  EXPECT_EQ(tkrzw::Status::SUCCESS, file->SetAllocationStrategy(1, 1.2));
  EXPECT_EQ(tkrzw::Status::SUCCESS, file->Open(file_path, true));
  auto func = [&](int32_t seed) {
    std::mt19937 mt(seed);
    std::uniform_int_distribution<int32_t> op_dist(0, 4);
    std::uniform_int_distribution<int32_t> write_buf_dist(0, 1);
    std::uniform_int_distribution<int32_t> off_dist(0, file_size - 1);
    std::uniform_int_distribution<int32_t> append_dist(0, 4);
    std::uniform_int_distribution<int32_t> size_dist(0, record_size);
    std::uniform_int_distribution<int32_t> sync_dist(0, num_iterations - 1);
    std::uniform_int_distribution<int32_t> trunc_dist(0, num_iterations - 1);
    char* write_buf = new char[record_size];
    std::memset(write_buf, '0' + seed, record_size);
    char* read_buf = new char[record_size];
    std::memcpy(read_buf, write_buf, record_size);
    for (int32_t i = 0; i < num_iterations; i++) {
      int32_t off = off_dist(mt);
      const int32_t size = std::min(size_dist(mt), file_size - off);
      if (file->IsAtomic() && trunc_dist(mt) == 0) {
        EXPECT_EQ(tkrzw::Status::SUCCESS, file->Truncate(off));
      } else if (file->IsAtomic() && sync_dist(mt) == 0) {
        EXPECT_EQ(tkrzw::Status::SUCCESS, file->Synchronize(true));
      } else if (op_dist(mt) == 0) {
        const char* buf = write_buf_dist(mt) == 0 ? write_buf : read_buf;
        if (append_dist(mt) == 0) {
          int64_t new_off = -1;
          EXPECT_EQ(tkrzw::Status::SUCCESS, file->Append(buf, size, &new_off));
          EXPECT_GE(new_off, 0);
        } else {
          EXPECT_EQ(tkrzw::Status::SUCCESS, file->Write(off, buf, size));
        }
      } else {
        const tkrzw::Status status = file->Read(off, read_buf, size);
        EXPECT_TRUE(status == tkrzw::Status::SUCCESS ||
                    status == tkrzw::Status::INFEASIBLE_ERROR);
      }
    }
    delete[] read_buf;
    delete[] write_buf;
  };
  std::vector<std::thread> threads;
  for (int32_t i = 0; i < num_threads; i++) {
    threads.emplace_back(std::thread(func, i));
  }
  for (auto& thread : threads) {
    thread.join();
  }
  EXPECT_EQ(tkrzw::Status::SUCCESS, file->Close());
}

void CommonFileTest::FileReaderTest(tkrzw::File* file) {
  tkrzw::TemporaryDirectory tmp_dir(true, "tkrzw-");
  const std::string file_path = tmp_dir.MakeUniquePath();
  {
    EXPECT_EQ(tkrzw::Status::SUCCESS, tkrzw::WriteFile(file_path, "1\n22\n333\n"));
    EXPECT_EQ(tkrzw::Status::SUCCESS, file->Open(file_path, false));
    tkrzw::FileReader reader(file);
    std::string line;
    EXPECT_EQ(tkrzw::Status::SUCCESS, reader.ReadLine(&line));
    EXPECT_EQ("1\n", line);
    EXPECT_EQ(tkrzw::Status::SUCCESS, reader.ReadLine(&line));
    EXPECT_EQ("22\n", line);
    EXPECT_EQ(tkrzw::Status::SUCCESS, reader.ReadLine(&line));
    EXPECT_EQ("333\n", line);
    EXPECT_EQ(tkrzw::Status::NOT_FOUND_ERROR, reader.ReadLine(&line));
    EXPECT_EQ(tkrzw::Status::SUCCESS, file->Close());
  }
  {
    EXPECT_EQ(tkrzw::Status::SUCCESS, tkrzw::WriteFile(file_path, "1\n\n22"));
    EXPECT_EQ(tkrzw::Status::SUCCESS, file->Open(file_path, false));
    tkrzw::FileReader reader(file);
    std::string line;
    EXPECT_EQ(tkrzw::Status::SUCCESS, reader.ReadLine(&line));
    EXPECT_EQ("1\n", line);
    EXPECT_EQ(tkrzw::Status::SUCCESS, reader.ReadLine(&line));
    EXPECT_EQ("\n", line);
    EXPECT_EQ(tkrzw::Status::SUCCESS, reader.ReadLine(&line));
    EXPECT_EQ("22", line);
    EXPECT_EQ(tkrzw::Status::SUCCESS, file->Close());
  }
  {
    std::string expected_line(99, 'x');
    expected_line.append(1, '\n');
    std::string content;
    for (int32_t i = 0; i < 100; i++) {
      content.append(expected_line);
    }
    EXPECT_EQ(tkrzw::Status::SUCCESS, tkrzw::WriteFile(file_path, content));
    EXPECT_EQ(tkrzw::Status::SUCCESS, file->Open(file_path, false));
    tkrzw::FileReader reader(file);
    for (int32_t i = 0; i < 100; i++) {
      std::string line;
      EXPECT_EQ(tkrzw::Status::SUCCESS, reader.ReadLine(&line));
      EXPECT_EQ(expected_line, line);
    }
    std::string line;
    EXPECT_EQ(tkrzw::Status::NOT_FOUND_ERROR, reader.ReadLine(&line));
    EXPECT_EQ(tkrzw::Status::SUCCESS, file->Close());
  }
}

void CommonFileTest::FlatRecordTest(tkrzw::File* file) {
  tkrzw::TemporaryDirectory tmp_dir(true, "tkrzw-");
  const std::string file_path = tmp_dir.MakeUniquePath();
  EXPECT_EQ(tkrzw::Status::SUCCESS, file->Open(file_path, true));
  const std::vector<int32_t> data_sizes = {
    0, 1, 2, 4, 8, 15, 16, 17, 30, 31, 32, 33, 62, 63, 64, 65, 126, 127, 128, 129,
    254, 255, 256, 257, 16384, 16385, 16386, 16387, 32766, 32767, 32768, 32769,
    0, 32769, 1, 32769, 2, 32767, 4, 32766, 8, 16387, 15, 16386, 16, 16385, 17, 16384,
    30, 257, 31, 256, 32, 255, 33, 254, 129, 62, 128, 63, 127, 64, 126};
  std::vector<size_t> reader_buffer_sizes;
  for (size_t size = 8; size <= 65536; size = size * 1.1 + 1) {
    reader_buffer_sizes.emplace_back(size);
  }
  reader_buffer_sizes.emplace_back(0);
  tkrzw::FlatRecord rec(file);
  for (size_t i = 0; i < data_sizes.size(); i++) {
    const auto& data_size = data_sizes[i];
    std::string data(data_size, 'v');
    if (data_size > 0) {
      data.front() = 'A';
    }
    if (data_size > 1) {
      data.back() = 'Z';
    }
    const tkrzw::FlatRecord::RecordType rec_type =
        i % 2 == 0 ? tkrzw::FlatRecord::RECORD_NORMAL : tkrzw::FlatRecord::RECORD_METADATA;
    EXPECT_EQ(tkrzw::Status::SUCCESS, rec.Write(data, rec_type));
    const int64_t offset = rec.GetOffset();
    EXPECT_EQ(tkrzw::Status::SUCCESS, rec.Read(offset));
    EXPECT_EQ(data, rec.GetData());
    EXPECT_EQ(rec_type, rec.GetRecordType());
  }
  const int64_t end_offset = file->GetSizeSimple();
  int64_t offset = 0;
  size_t index = 0;
  while (offset < end_offset) {
    EXPECT_EQ(tkrzw::Status::SUCCESS, rec.Read(offset));
    std::string data(data_sizes[index], 'v');
    if (data.size() > 0) {
      data.front() = 'A';
    }
    if (data.size() > 1) {
      data.back() = 'Z';
    }
    EXPECT_EQ(data, rec.GetData());
    if (index % 2 == 0) {
      EXPECT_EQ(tkrzw::FlatRecord::RECORD_NORMAL, rec.GetRecordType());
    } else {
      EXPECT_EQ(tkrzw::FlatRecord::RECORD_METADATA, rec.GetRecordType());
    }
    offset += rec.GetWholeSize();
    index++;
  }
  EXPECT_EQ(data_sizes.size(), index);
  for (const size_t reader_buffer_size : reader_buffer_sizes) {
    std::vector<int32_t> actual_sizes;
    tkrzw::FlatRecordReader reader(file, reader_buffer_size);
    while (true) {
      std::string_view data;
      tkrzw::FlatRecord::RecordType rec_type;
      const tkrzw::Status status = reader.Read(&data, &rec_type);
      if (status != tkrzw::Status::SUCCESS) {
        EXPECT_EQ(tkrzw::Status::NOT_FOUND_ERROR, status);
        break;
      }
      if (data.size() > 0) {
        EXPECT_EQ('A', data.front());
      }
      if (data.size() > 1) {
        EXPECT_EQ('Z', data.back());
      }
      if (actual_sizes.size() % 2 == 0) {
        EXPECT_EQ(tkrzw::FlatRecord::RECORD_NORMAL, rec_type);
      } else {
        EXPECT_EQ(tkrzw::FlatRecord::RECORD_METADATA, rec_type);
      }
      actual_sizes.emplace_back(data.size());
    }
    EXPECT_THAT(actual_sizes, ElementsAreArray(data_sizes));
  }
  EXPECT_EQ(tkrzw::Status::SUCCESS, file->Close());
}

void CommonFileTest::RenameTest(tkrzw::File* file) {
  tkrzw::TemporaryDirectory tmp_dir(true, "tkrzw-");
  const std::string file_path = tmp_dir.MakeUniquePath();
  EXPECT_EQ(tkrzw::Status::SUCCESS, file->Open(file_path, true, tkrzw::File::OPEN_TRUNCATE));
  EXPECT_EQ(file_path, file->GetPathSimple());
  EXPECT_EQ(tkrzw::Status::SUCCESS, file->Truncate(5));
  EXPECT_EQ(5, file->GetSizeSimple());
  EXPECT_EQ(tkrzw::Status::SUCCESS, file->Synchronize(false));
  const std::string rename_file_path = tmp_dir.MakeUniquePath();
  EXPECT_EQ(tkrzw::Status::SUCCESS, file->Rename(rename_file_path));
  auto rename_file = file->MakeFile();
  EXPECT_EQ(tkrzw::Status::NOT_FOUND_ERROR, rename_file->Open(file_path, false));
  EXPECT_EQ(tkrzw::Status::SUCCESS, rename_file->Open(rename_file_path, false));
  EXPECT_EQ(rename_file_path, rename_file->GetPathSimple());
  EXPECT_EQ(5, rename_file->GetSizeSimple());
  EXPECT_EQ(tkrzw::Status::SUCCESS, rename_file->Close());
  EXPECT_EQ(tkrzw::Status::SUCCESS, file->Append("abc", 3));
  EXPECT_EQ(8, file->GetSizeSimple());
  EXPECT_EQ("abc", file->ReadSimple(5, 3));
  EXPECT_EQ(tkrzw::Status::SUCCESS, file->DisablePathOperations());
  std::string found_path;
  EXPECT_EQ(tkrzw::Status::PRECONDITION_ERROR, file->GetPath(&found_path));
  EXPECT_EQ(tkrzw::Status::PRECONDITION_ERROR, file->Rename(file_path));
  EXPECT_EQ(tkrzw::Status::SUCCESS, file->Close());
}

// END OF FILE
