/*************************************************************************************************
 * Tests for tkrzw_dbm_ulog.h
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

#include "tkrzw_dbm.h"
#include "tkrzw_dbm_ulog.h"
#include "tkrzw_dbm_std.h"
#include "tkrzw_file.h"
#include "tkrzw_file_mmap.h"
#include "tkrzw_file_pos.h"
#include "tkrzw_file_std.h"
#include "tkrzw_file_util.h"
#include "tkrzw_lib_common.h"
#include "tkrzw_str_util.h"

using namespace testing;

// Main routine
int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

TEST(DBMUpdateLoggerTest, DBMUpdateLoggerStrDeque) {
  tkrzw::DBMUpdateLoggerStrDeque ulog(" ");
  EXPECT_EQ(0, ulog.GetSize());
  EXPECT_EQ(tkrzw::Status::SUCCESS, ulog.WriteSet("one", "hop"));
  EXPECT_EQ(tkrzw::Status::SUCCESS, ulog.WriteSet("two", "step"));
  EXPECT_EQ(tkrzw::Status::SUCCESS, ulog.WriteSet("three", "jump"));
  EXPECT_EQ(tkrzw::Status::SUCCESS, ulog.WriteRemove("two"));
  EXPECT_EQ(tkrzw::Status::SUCCESS, ulog.WriteClear());
  EXPECT_EQ(5, ulog.GetSize());
  std::string text;
  EXPECT_TRUE(ulog.PopFront(&text));
  EXPECT_EQ("SET one hop", text);
  EXPECT_TRUE(ulog.PopFront(&text));
  EXPECT_EQ("SET two step", text);
  EXPECT_TRUE(ulog.PopBack(&text));
  EXPECT_EQ("CLEAR", text);
  EXPECT_TRUE(ulog.PopBack(&text));
  EXPECT_EQ("REMOVE two", text);
  EXPECT_EQ(1, ulog.GetSize());
  ulog.Clear();
  EXPECT_EQ(0, ulog.GetSize());
  EXPECT_FALSE(ulog.PopFront(&text));
  EXPECT_FALSE(ulog.PopBack(&text));
}

TEST(DBMUpdateLoggerTest, DBMUpdateLoggerDBM) {
  tkrzw::StdHashDBM dbm(10);
  tkrzw::DBMUpdateLoggerDBM ulog(&dbm);
  EXPECT_EQ(tkrzw::Status::SUCCESS, ulog.WriteSet("one", "hop"));
  EXPECT_EQ("hop", dbm.GetSimple("one"));
  EXPECT_EQ(tkrzw::Status::SUCCESS, ulog.WriteSet("two", "step"));
  EXPECT_EQ("step", dbm.GetSimple("two"));
  EXPECT_EQ(tkrzw::Status::SUCCESS, ulog.WriteRemove("two"));
  EXPECT_EQ("", dbm.GetSimple("two"));
  EXPECT_EQ(tkrzw::Status::SUCCESS, ulog.WriteRemove("two"));
  EXPECT_EQ(1, dbm.CountSimple());
  EXPECT_EQ(tkrzw::Status::SUCCESS, ulog.WriteClear());
  EXPECT_EQ(0, dbm.CountSimple());
}

TEST(DBMUpdateLoggerTest, DBMUpdateLoggerSecondShard) {
  tkrzw::StdHashDBM dbm(10);
  tkrzw::DBMUpdateLoggerStrDeque ulog_core(" ");
  tkrzw::DBMUpdateLoggerSecondShard ulog(&ulog_core);
  EXPECT_EQ(tkrzw::Status::SUCCESS, ulog.WriteSet("one", "hop"));
  EXPECT_EQ(tkrzw::Status::SUCCESS, ulog.WriteRemove("two"));
  EXPECT_EQ(tkrzw::Status::SUCCESS, ulog.WriteClear());
  EXPECT_EQ(tkrzw::Status::SUCCESS, ulog.WriteSet("two", "step"));
  EXPECT_EQ(3, ulog_core.GetSize());
  std::string text;
  EXPECT_TRUE(ulog_core.PopFront(&text));
  EXPECT_EQ("SET one hop", text);
  EXPECT_TRUE(ulog_core.PopFront(&text));
  EXPECT_EQ("REMOVE two", text);
  EXPECT_TRUE(ulog_core.PopFront(&text));
  EXPECT_EQ("SET two step", text);
}

// END OF FILE
