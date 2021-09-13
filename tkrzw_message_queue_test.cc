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
  //tkrzw::TemporaryDirectory tmp_dir(true, "tkrzw-");
  //const std::string prefix = tmp_dir.MakeUniquePath("casket-", "-mq");

  const std::string prefix = "casket-mq";

  tkrzw::MessageQueue mq;
  EXPECT_EQ(tkrzw::Status::SUCCESS, mq.Open(prefix, 50, tkrzw::MessageQueue::OPEN_TRUNCATE));
  EXPECT_EQ(tkrzw::Status::SUCCESS, mq.Close());
  int64_t file_id = 0;
  int64_t timestamp = 0;
  int64_t file_size = 0;
  EXPECT_EQ(tkrzw::Status::SUCCESS, tkrzw::MessageQueue::ReadFileMetadata(
      "casket-mq.0000000000", &file_id, &timestamp, &file_size));
  EXPECT_EQ(0, file_id);
  EXPECT_EQ(0, timestamp);
  EXPECT_EQ(32, file_size);
  EXPECT_EQ(tkrzw::Status::SUCCESS, mq.Open(prefix, 50));
  EXPECT_EQ(tkrzw::Status::SUCCESS, mq.Write(10, "one"));
  EXPECT_EQ(tkrzw::Status::SUCCESS, mq.Write(10, "two"));
  EXPECT_EQ(tkrzw::Status::SUCCESS, mq.Write(20, std::string(50, 'x')));
  EXPECT_EQ(tkrzw::Status::SUCCESS, mq.Write(30, "four"));
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
  EXPECT_EQ(30, timestamp);
  EXPECT_EQ(tkrzw::GetFileSize(prefix + ".0000000002"), file_size);







}

// END OF FILE
