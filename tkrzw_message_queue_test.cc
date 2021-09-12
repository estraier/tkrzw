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
  tkrzw::TemporaryDirectory tmp_dir(true, "tkrzw-");
  const std::string prefix = tmp_dir.MakeUniquePath("casket-", "-ulog");

  //const std::string prefix = "casket-mq";

  tkrzw::MessageQueue mq;
  EXPECT_EQ(tkrzw::Status::SUCCESS, mq.Open(prefix, 50, 10));
  EXPECT_EQ(tkrzw::Status::SUCCESS, mq.Write(0, "one"));
  EXPECT_EQ(tkrzw::Status::SUCCESS, mq.Write(1, "two"));
  EXPECT_EQ(tkrzw::Status::SUCCESS, mq.Write(10, std::string(50, 'x')));
  EXPECT_EQ(tkrzw::Status::SUCCESS, mq.Write(5, std::string(50, 'y')));
  EXPECT_EQ(tkrzw::Status::SUCCESS, mq.Write(12, "hop"));
  EXPECT_EQ(tkrzw::Status::SUCCESS, mq.Close());

  std::vector<std::string> paths;
  EXPECT_EQ(tkrzw::Status::SUCCESS, tkrzw::MessageQueue::FindFiles(prefix, &paths));
  EXPECT_EQ(3, paths.size());
  bool has_first = false;
  bool has_second = false;
  bool has_third = false;
  for (const auto& path : paths) {
    if (tkrzw::StrEndsWith(path, "010")) {
      has_first = true;
    }
    if (tkrzw::StrEndsWith(path, "010-00001")) {
      has_second = true;
    }
    if (tkrzw::StrEndsWith(path, "012")) {
      has_third = true;
    }
  }
  EXPECT_TRUE(has_first);
  EXPECT_TRUE(has_second);
  EXPECT_TRUE(has_third);

  EXPECT_EQ(tkrzw::Status::SUCCESS, mq.Open(prefix, 50, 13));
  EXPECT_EQ(tkrzw::Status::SUCCESS, mq.Write(13, "step"));
  EXPECT_EQ(tkrzw::Status::SUCCESS, mq.Write(14, std::string(50, 'z')));
  EXPECT_EQ(tkrzw::Status::SUCCESS, mq.Write(15, "jump"));
  EXPECT_EQ(tkrzw::Status::SUCCESS, mq.Close());


  EXPECT_EQ(tkrzw::Status::SUCCESS, tkrzw::MessageQueue::FindFiles(prefix, &paths));
  EXPECT_EQ(4, paths.size());
  bool has_fourth = false;
  for (const auto& path : paths) {
    if (tkrzw::StrEndsWith(path, "015")) {
      has_fourth = true;
    }
  }
  EXPECT_TRUE(has_fourth);



  std::cout << "HELLO" << std::endl;
}

// END OF FILE
