/*************************************************************************************************
 * Tests for tkrzw_message_queue.h
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

#include "tkrzw_file_mmap.h"
#include "tkrzw_file_util.h"
#include "tkrzw_lib_common.h"
#include "tkrzw_message_queue.h"
#include "tkrzw_str_util.h"

using namespace testing;

// Main routine
int main(int argc, char** argv) {
  InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

TEST(MessageQueueTest, Basic) {
  tkrzw::TemporaryDirectory tmp_dir(true, "tkrzw-");
  const std::string prefix = tmp_dir.MakeUniquePath("casket-", "-mq");
  tkrzw::MessageQueue mq;
  EXPECT_EQ(-1, mq.GetTimestamp());
  EXPECT_EQ(tkrzw::Status::SUCCESS, mq.Open(prefix, 50, tkrzw::MessageQueue::OPEN_TRUNCATE));
  EXPECT_EQ(0, mq.GetTimestamp());
  EXPECT_EQ(tkrzw::Status::SUCCESS, mq.Close());
  int64_t file_id = 0;
  int64_t timestamp = 0;
  int64_t file_size = 0;
  EXPECT_EQ(tkrzw::Status::SUCCESS, tkrzw::MessageQueue::ReadFileMetadata(
      prefix + ".0000000000", &file_id, &timestamp, &file_size));
  EXPECT_EQ(0, file_id);
  EXPECT_EQ(0, timestamp);
  EXPECT_EQ(32, file_size);
  EXPECT_EQ(tkrzw::Status::SUCCESS, mq.Open(prefix, 50));
  EXPECT_EQ(0, mq.GetTimestamp());
  tkrzw::MemoryMapParallelFile file;
  EXPECT_EQ(tkrzw::Status::SUCCESS, file.Open(prefix + ".0000000000", false));
  int64_t file_offset = 0;
  std::string message;
  EXPECT_EQ(tkrzw::Status::CANCELED_ERROR, tkrzw::MessageQueue::ReadNextMessage(
      &file, &file_offset, &timestamp, &message));
  EXPECT_EQ(tkrzw::Status::SUCCESS, mq.Write(10, "one"));
  EXPECT_EQ(10, mq.GetTimestamp());
  file_offset = 0;
  EXPECT_EQ(tkrzw::Status::SUCCESS, tkrzw::MessageQueue::ReadNextMessage(
      &file, &file_offset, &timestamp, &message));
  EXPECT_EQ(10, timestamp);
  EXPECT_EQ("one", message);
  EXPECT_EQ(tkrzw::Status::SUCCESS, tkrzw::MessageQueue::ReadFileMetadata(
      prefix + ".0000000000", &file_id, &timestamp, &file_size));
  EXPECT_EQ(0, file_id);
  EXPECT_EQ(0, timestamp);
  EXPECT_EQ(32, file_size);
  EXPECT_EQ(tkrzw::Status::SUCCESS, mq.Synchronize(false));
  EXPECT_EQ(tkrzw::Status::SUCCESS, tkrzw::MessageQueue::ReadFileMetadata(
      prefix + ".0000000000", &file_id, &timestamp, &file_size));
  EXPECT_EQ(0, file_id);
  EXPECT_EQ(10, timestamp);
  EXPECT_EQ(47, file_size);
  EXPECT_EQ(tkrzw::Status::SUCCESS, mq.Write(5, "two"));
  EXPECT_EQ(10, mq.GetTimestamp());
  EXPECT_EQ(tkrzw::Status::SUCCESS, mq.Write(20, std::string(50, 'x')));
  EXPECT_EQ(20, mq.GetTimestamp());
  EXPECT_EQ(tkrzw::Status::SUCCESS, mq.Write(30, "four"));
  EXPECT_EQ(30, mq.GetTimestamp());
  EXPECT_EQ(tkrzw::Status::SUCCESS, mq.Write(40, "five"));
  EXPECT_EQ(40, mq.GetTimestamp());
  EXPECT_EQ(tkrzw::Status::SUCCESS, file.Close());
  EXPECT_EQ(tkrzw::Status::SUCCESS, mq.Close());
  EXPECT_EQ(tkrzw::Status::SUCCESS, tkrzw::MessageQueue::ReadFileMetadata(
      prefix + ".0000000000", &file_id, &timestamp, &file_size));
  EXPECT_EQ(0, file_id);
  EXPECT_EQ(10, timestamp);
  EXPECT_EQ(tkrzw::GetFileSize(prefix + ".0000000000"), file_size);
  EXPECT_EQ(tkrzw::Status::SUCCESS, tkrzw::MessageQueue::ReadFileMetadata(
      prefix + ".0000000001", &file_id, &timestamp, &file_size));
  EXPECT_EQ(1, file_id);
  EXPECT_EQ(20, timestamp);
  EXPECT_EQ(tkrzw::GetFileSize(prefix + ".0000000001"), file_size);
  EXPECT_EQ(tkrzw::Status::SUCCESS, tkrzw::MessageQueue::ReadFileMetadata(
      prefix + ".0000000002", &file_id, &timestamp, &file_size));
  EXPECT_EQ(2, file_id);
  EXPECT_EQ(40, timestamp);
  EXPECT_EQ(tkrzw::GetFileSize(prefix + ".0000000002"), file_size);
  EXPECT_EQ(tkrzw::Status::SUCCESS, mq.Open(prefix, 0, tkrzw::MessageQueue::OPEN_READ_ONLY));
  auto reader = mq.MakeReader(0);
  EXPECT_EQ(-1, reader->GetTimestamp());
  std::vector<std::pair<int64_t, std::string>> records;
  while (true) {
    std::string message;
    const tkrzw::Status status = reader->Read(&timestamp, &message, 0);
    if (status != tkrzw::Status::SUCCESS) {
      EXPECT_EQ(tkrzw::Status::NOT_FOUND_ERROR, status);
      break;
    }
    EXPECT_EQ(timestamp, reader->GetTimestamp());
    records.emplace_back(std::make_pair(timestamp, message));
  }
  EXPECT_THAT(records, ElementsAre(
      std::pair<uint64_t, std::string>{10, "one"},
      std::pair<uint64_t, std::string>{10, "two"},
      std::pair<uint64_t, std::string>{20, std::string(50, 'x')},
      std::pair<uint64_t, std::string>{30, "four"},
      std::pair<uint64_t, std::string>{40, "five"}));
  reader = mq.MakeReader(10);
  EXPECT_EQ(tkrzw::Status::SUCCESS, reader->Wait(0));
  EXPECT_EQ(tkrzw::Status::SUCCESS, reader->Read(&timestamp, &message, 0));
  EXPECT_EQ("one", message);
  reader = mq.MakeReader(20);
  EXPECT_EQ(tkrzw::Status::SUCCESS, reader->Wait(-1));
  EXPECT_EQ(tkrzw::Status::SUCCESS, reader->Read(&timestamp, &message, -1));
  EXPECT_EQ(std::string(50, 'x'), message);
  reader = mq.MakeReader(29);
  EXPECT_EQ(tkrzw::Status::SUCCESS, reader->Read(&timestamp, &message, 0));
  EXPECT_EQ("four", message);
  reader = mq.MakeReader(30);
  EXPECT_EQ(tkrzw::Status::SUCCESS, reader->Read(&timestamp, &message, 0));
  EXPECT_EQ("four", message);
  reader = mq.MakeReader(39);
  EXPECT_EQ(tkrzw::Status::SUCCESS, reader->Wait(0));
  EXPECT_EQ(tkrzw::Status::SUCCESS, reader->Read(&timestamp, &message, 0));
  EXPECT_EQ("five", message);
  reader = mq.MakeReader(40);
  EXPECT_EQ(tkrzw::Status::SUCCESS, reader->Wait(0));
  EXPECT_EQ(tkrzw::Status::SUCCESS, reader->Read(&timestamp, &message, 0));
  EXPECT_EQ("five", message);
  reader = mq.MakeReader(41);
  EXPECT_EQ(tkrzw::Status::NOT_FOUND_ERROR, reader->Read(&timestamp, &message, 0));
  EXPECT_EQ(tkrzw::Status::SUCCESS, file.Open(prefix + ".0000000002", false));
  file_offset = 0;
  EXPECT_EQ(tkrzw::Status::SUCCESS, tkrzw::MessageQueue::ReadNextMessage(
      &file, &file_offset, &timestamp, &message));
  EXPECT_EQ(30, timestamp);
  EXPECT_EQ("four", message);
  EXPECT_EQ(tkrzw::Status::SUCCESS, tkrzw::MessageQueue::ReadNextMessage(
      &file, &file_offset, &timestamp, &message));
  EXPECT_EQ(40, timestamp);
  EXPECT_EQ("five", message);
  EXPECT_EQ(tkrzw::Status::SUCCESS, file.Close());
  std::vector<std::string> paths;
  EXPECT_EQ(tkrzw::Status::SUCCESS, tkrzw::MessageQueue::RemoveOldFiles(prefix, 10));
  EXPECT_EQ(tkrzw::Status::SUCCESS, tkrzw::MessageQueue::FindFiles(prefix, &paths));
  EXPECT_EQ(3, paths.size());
  EXPECT_EQ(tkrzw::Status::SUCCESS, tkrzw::MessageQueue::RemoveOldFiles(prefix, 20));
  EXPECT_EQ(tkrzw::Status::SUCCESS, tkrzw::MessageQueue::FindFiles(prefix, &paths));
  EXPECT_EQ(2, paths.size());
  EXPECT_EQ(tkrzw::Status::SUCCESS, tkrzw::MessageQueue::RemoveOldFiles(prefix, 30));
  EXPECT_EQ(tkrzw::Status::SUCCESS, tkrzw::MessageQueue::FindFiles(prefix, &paths));
  EXPECT_EQ(1, paths.size());
  EXPECT_EQ(tkrzw::Status::SUCCESS, tkrzw::MessageQueue::RemoveOldFiles(prefix, 40));
  EXPECT_EQ(tkrzw::Status::SUCCESS, tkrzw::MessageQueue::FindFiles(prefix, &paths));
  EXPECT_THAT(paths, ElementsAre(prefix + ".0000000002"));
  EXPECT_EQ(tkrzw::Status::SUCCESS, mq.Close());
  EXPECT_EQ(tkrzw::Status::SUCCESS, mq.Open(prefix, 0));
  EXPECT_EQ(tkrzw::Status::SUCCESS, mq.Write(10, "six"));
  EXPECT_EQ(tkrzw::Status::SUCCESS, mq.Write(70, "seven"));
  EXPECT_EQ(tkrzw::Status::SUCCESS, mq.Write(65, ""));
  reader = mq.MakeReader(10);
  records.clear();
  while (true) {
    std::string message;
    const tkrzw::Status status = reader->Read(&timestamp, &message, 0);
    if (status != tkrzw::Status::SUCCESS) {
      EXPECT_EQ(tkrzw::Status::INFEASIBLE_ERROR, status);
      EXPECT_EQ(70, timestamp);
      break;
    }
    records.emplace_back(std::make_pair(timestamp, message));
  }
  EXPECT_THAT(records, ElementsAre(
      std::pair<uint64_t, std::string>{30, "four"},
      std::pair<uint64_t, std::string>{40, "five"},
      std::pair<uint64_t, std::string>{40, "six"},
      std::pair<uint64_t, std::string>{70, "seven"},
      std::pair<uint64_t, std::string>{70, ""}));
  EXPECT_EQ(70, mq.GetTimestamp());
  EXPECT_EQ(tkrzw::Status::SUCCESS, mq.UpdateTimestamp(10));
  EXPECT_EQ(70, mq.GetTimestamp());
  EXPECT_EQ(tkrzw::Status::SUCCESS, mq.UpdateTimestamp(75));
  EXPECT_EQ(75, mq.GetTimestamp());
  EXPECT_EQ(tkrzw::Status::SUCCESS, mq.Close());
  EXPECT_EQ(tkrzw::Status::SUCCESS, tkrzw::TruncateFile(prefix + ".0000000002", 65536));
  EXPECT_EQ(tkrzw::Status::SUCCESS, file.Open(prefix + ".0000000002", false));
  file_offset = 0;
  records.clear();
  while (true) {
    std::string message;
    const tkrzw::Status status =
        tkrzw::MessageQueue::ReadNextMessage(&file, &file_offset, &timestamp, &message, 40);
    if (status != tkrzw::Status::SUCCESS) {
      EXPECT_EQ(tkrzw::Status::CANCELED_ERROR, status);
      break;
    }
    records.emplace_back(std::make_pair(timestamp, message));
  }
  EXPECT_THAT(records, ElementsAre(
      std::pair<uint64_t, std::string>{30, ""},
      std::pair<uint64_t, std::string>{40, "five"}));
  EXPECT_EQ(tkrzw::Status::SUCCESS, file.Close());
  EXPECT_EQ(tkrzw::Status::SUCCESS, tkrzw::TruncateFile(prefix + ".0000000005", 65536));
  EXPECT_EQ(tkrzw::Status::SUCCESS, mq.Open(prefix, 50, tkrzw::MessageQueue::OPEN_READ_ONLY));
  EXPECT_EQ(75, mq.GetTimestamp());
  EXPECT_EQ(tkrzw::Status::SUCCESS, mq.Close());
  EXPECT_EQ(tkrzw::Status::SUCCESS, tkrzw::TruncateFile(prefix + ".0000000005", 65536));
  EXPECT_EQ(tkrzw::Status::SUCCESS, mq.Open(prefix, 65536));
  EXPECT_EQ(75, mq.GetTimestamp());
  EXPECT_EQ(tkrzw::Status::SUCCESS, mq.Write(80, "eight"));
  EXPECT_EQ(80, mq.GetTimestamp());
  reader = mq.MakeReader(70);
  records.clear();
  while (true) {
    std::string message;
    const tkrzw::Status status = reader->Read(&timestamp, &message, 0);
    if (status != tkrzw::Status::SUCCESS) {
      EXPECT_EQ(tkrzw::Status::INFEASIBLE_ERROR, status);
      break;
    }
    EXPECT_EQ(timestamp, reader->GetTimestamp());
    records.emplace_back(std::make_pair(timestamp, message));
  }
  EXPECT_THAT(records, ElementsAre(
      std::pair<uint64_t, std::string>{70, "seven"},
      std::pair<uint64_t, std::string>{70, ""},
      std::pair<uint64_t, std::string>{80, "eight"}));
  reader = mq.MakeReader(70);
  EXPECT_EQ(tkrzw::Status::SUCCESS, mq.CancelReaders());
  EXPECT_EQ(tkrzw::Status::CANCELED_ERROR, reader->Read(&timestamp, &message, 0));
  EXPECT_EQ(tkrzw::Status::SUCCESS, mq.Close());
  EXPECT_EQ(tkrzw::Status::SUCCESS, tkrzw::MessageQueue::RemoveOldFiles(prefix, 10000, true));
  EXPECT_EQ(tkrzw::Status::SUCCESS, tkrzw::MessageQueue::FindFiles(prefix, &paths));
  EXPECT_EQ(1, paths.size());
  EXPECT_EQ(tkrzw::Status::SUCCESS, tkrzw::MessageQueue::RemoveOldFiles(prefix, 10000, false));
  EXPECT_EQ(tkrzw::Status::SUCCESS, tkrzw::MessageQueue::FindFiles(prefix, &paths));
  EXPECT_TRUE(paths.empty());
}

TEST(MessageQueueTest, AutoRestore) {
  tkrzw::TemporaryDirectory tmp_dir(true, "tkrzw-");
  const std::string prefix = tmp_dir.MakeUniquePath("casket-", "-mq");
  tkrzw::MessageQueue mq;
  EXPECT_EQ(tkrzw::Status::SUCCESS, mq.Open(prefix, 8192, tkrzw::MessageQueue::OPEN_TRUNCATE));
  EXPECT_EQ(tkrzw::Status::SUCCESS, mq.Write(1, "one"));
  EXPECT_EQ(tkrzw::Status::SUCCESS, mq.Write(2, "two"));
  EXPECT_EQ(tkrzw::Status::SUCCESS, mq.Write(3, "three"));
  EXPECT_EQ(tkrzw::Status::SUCCESS, mq.Close());
  EXPECT_EQ(tkrzw::Status::SUCCESS, tkrzw::TruncateFile(prefix + ".0000000000", 65536));
  EXPECT_EQ(tkrzw::Status::SUCCESS, mq.Open(prefix, 8192));
  auto reader = mq.MakeReader(0);
  std::vector<std::pair<int64_t, std::string>> records;
  while (true) {
    int64_t timestamp = 0;
    std::string message;
    const tkrzw::Status status = reader->Read(&timestamp, &message, 0);
    if (status != tkrzw::Status::SUCCESS) {
      EXPECT_EQ(tkrzw::Status::INFEASIBLE_ERROR, status);
      break;
    }
    EXPECT_EQ(timestamp, reader->GetTimestamp());
    records.emplace_back(std::make_pair(timestamp, message));
  }
  EXPECT_THAT(records, ElementsAre(
      std::pair<uint64_t, std::string>{1, "one"},
      std::pair<uint64_t, std::string>{2, "two"},
      std::pair<uint64_t, std::string>{3, "three"}));
  EXPECT_EQ(tkrzw::Status::SUCCESS, mq.Close());
  tkrzw::MemoryMapParallelFile file;
  EXPECT_EQ(tkrzw::Status::SUCCESS, file.Open(prefix + ".0000000000", true));
  EXPECT_EQ(tkrzw::Status::SUCCESS, file.Write(22, std::string(6, '\0').data(), 6));
  EXPECT_EQ(tkrzw::Status::SUCCESS, file.Close());
  EXPECT_EQ(tkrzw::Status::SUCCESS, mq.Open(prefix, 8192));
  reader = mq.MakeReader(0);
  records.clear();
  while (true) {
    int64_t timestamp = 0;
    std::string message;
    const tkrzw::Status status = reader->Read(&timestamp, &message, 0);
    if (status != tkrzw::Status::SUCCESS) {
      EXPECT_EQ(tkrzw::Status::INFEASIBLE_ERROR, status);
      break;
    }
    EXPECT_EQ(timestamp, reader->GetTimestamp());
    records.emplace_back(std::make_pair(timestamp, message));
  }
  EXPECT_THAT(records, ElementsAre(
      std::pair<uint64_t, std::string>{1, "one"},
      std::pair<uint64_t, std::string>{2, "two"},
      std::pair<uint64_t, std::string>{3, "three"}));
  EXPECT_EQ(tkrzw::Status::SUCCESS, mq.Close());
}

TEST(MessageQueueTest, Serial) {
  tkrzw::TemporaryDirectory tmp_dir(true, "tkrzw-");
  const std::string prefix = tmp_dir.MakeUniquePath("casket-", "-mq");
  constexpr int32_t num_messages = 100000;
  tkrzw::MessageQueue mq;
  EXPECT_EQ(tkrzw::Status::SUCCESS,
            mq.Open(prefix, 10000000, tkrzw::MessageQueue::OPEN_TRUNCATE));
  tkrzw::WaitCounter wc(1);
  auto read_task =
      [&]() {
        auto reader = mq.MakeReader(0);
        int32_t count = num_messages;
        while (true) {
          double wait_time = 0;
          switch (count % 3) {
            case 0: wait_time = -1; break;
            case 1: wait_time = 0.0001; break;
          }
          int64_t timestamp = 0;
          std::string message;
          const tkrzw::Status status = reader->Read(&timestamp, &message, wait_time);
          if (status != tkrzw::Status::SUCCESS) {
            if (status == tkrzw::Status::INFEASIBLE_ERROR) {
              continue;
            }
            EXPECT_EQ(tkrzw::Status::CANCELED_ERROR, status);
            break;
          }
          if (--count == 0) {
            wc.Done();
          }
        }
      };
  std::thread th = std::thread(read_task);
  for (int32_t i = 0; i < num_messages; i++) {
    const std::string message = tkrzw::SPrintF("%020d", i);
    EXPECT_EQ(tkrzw::Status::SUCCESS, mq.Write(i, message));
    if (i % 10 == 0) {
      std::this_thread::yield();
    }
  }
  EXPECT_TRUE(wc.Wait());
  EXPECT_EQ(tkrzw::Status::SUCCESS, mq.Close());
  th.join();
}

TEST(MessageQueueTest, Parallel) {
  tkrzw::TemporaryDirectory tmp_dir(true, "tkrzw-");
  const std::string prefix = tmp_dir.MakeUniquePath("casket-", "-mq");
  constexpr int32_t num_threads = 10;
  constexpr int32_t num_messages = 1000;
  std::vector<std::pair<int64_t, std::string>> results[num_threads];
  tkrzw::MessageQueue mq;
  EXPECT_EQ(tkrzw::Status::SUCCESS, mq.Open(prefix, 50, tkrzw::MessageQueue::OPEN_TRUNCATE));
  tkrzw::WaitCounter wc(num_threads);
  auto read_task =
      [&](int32_t id, std::vector<std::pair<int64_t, std::string>>* result) {
        auto reader = mq.MakeReader(0);
        int32_t count = num_messages;
        while (true) {
          double wait_time = 0;
          switch (count % 3) {
            case 0: wait_time = -1; break;
            case 1: wait_time = 0.0001; break;
          }


          bool ready = false;
          tkrzw::Status status = reader->Wait(0);
          if (status == tkrzw::Status::SUCCESS) {
            ready = true;
          } else {
            EXPECT_TRUE(status == tkrzw::Status::INFEASIBLE_ERROR ||
                        status == tkrzw::Status::CANCELED_ERROR);
          }





          int64_t timestamp = 0;
          std::string message;
          status = reader->Read(&timestamp, &message, wait_time);
          if (status != tkrzw::Status::SUCCESS) {
            if (status == tkrzw::Status::INFEASIBLE_ERROR) {
              continue;
            }
            EXPECT_EQ(tkrzw::Status::CANCELED_ERROR, status);
            EXPECT_FALSE(ready);
            break;
          }
          result->emplace_back(std::make_pair(timestamp, message));
          if (--count == 0) {
            wc.Done();
          }
        }
      };
  std::vector<std::thread> threads;
  for (int32_t i = 0; i < num_threads; i++) {
    threads.emplace_back(std::thread(read_task, i, &results[i]));
  }
  for (int32_t i = 0; i < num_messages; i++) {
    const std::string message = tkrzw::ToString(i);
    EXPECT_EQ(tkrzw::Status::SUCCESS, mq.Write(i * 100, message));
  }
  EXPECT_TRUE(wc.Wait());
  EXPECT_EQ(tkrzw::Status::SUCCESS, mq.Close());
  for (auto& thread : threads) {
    thread.join();
  }
  for (const auto& result : results) {
    EXPECT_EQ(num_messages, result.size());
    for (size_t i = 0; i < result.size(); i++) {
      EXPECT_EQ(i * 100, result[i].first);
      EXPECT_EQ(tkrzw::ToString(i), result[i].second);
    }
  }
}

TEST(MessageQueueTest, ParseTimestamp) {
  EXPECT_EQ(0, tkrzw::MessageQueue::ParseTimestamp("", 100000));
  EXPECT_EQ(1234, tkrzw::MessageQueue::ParseTimestamp(" 1234 ", 100000));
  EXPECT_EQ(101234, tkrzw::MessageQueue::ParseTimestamp("+1234", 100000));
  EXPECT_EQ(98766, tkrzw::MessageQueue::ParseTimestamp("-1234", 100000));
  EXPECT_EQ(43200000, tkrzw::MessageQueue::ParseTimestamp(" 0.5 d ", 0));
  EXPECT_EQ(43200000, tkrzw::MessageQueue::ParseTimestamp("+0.5D", 0));
  EXPECT_EQ(-43200000, tkrzw::MessageQueue::ParseTimestamp("-0.5D", 0));
  EXPECT_EQ(1800000, tkrzw::MessageQueue::ParseTimestamp(" 0.5 h ", 0));
  EXPECT_EQ(1800000, tkrzw::MessageQueue::ParseTimestamp("+0.5H", 0));
  EXPECT_EQ(-1800000, tkrzw::MessageQueue::ParseTimestamp("-0.5H", 0));
  EXPECT_EQ(30000, tkrzw::MessageQueue::ParseTimestamp( "0.5 m ", 0));
  EXPECT_EQ(30000, tkrzw::MessageQueue::ParseTimestamp("+0.5M", 0));
  EXPECT_EQ(-30000, tkrzw::MessageQueue::ParseTimestamp("-0.5M", 0));
  EXPECT_EQ(500, tkrzw::MessageQueue::ParseTimestamp( "0.5 s ", 0));
  EXPECT_EQ(500, tkrzw::MessageQueue::ParseTimestamp("+0.5S", 0));
  EXPECT_EQ(-500, tkrzw::MessageQueue::ParseTimestamp("-0.5S", 0));
  EXPECT_EQ(1631947423000, tkrzw::MessageQueue::ParseTimestamp("-7D", 1632552223000));
  EXPECT_EQ(1632554023000, tkrzw::MessageQueue::ParseTimestamp("+0.5H", 1632552223000));
}

// END OF FILE
