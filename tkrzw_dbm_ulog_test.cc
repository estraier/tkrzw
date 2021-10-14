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
#include "tkrzw_thread_util.h"

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
  EXPECT_EQ(tkrzw::Status::SUCCESS, ulog.Synchronize(false));
}

TEST(DBMUpdateLoggerTest, DBMUpdateLoggerSecondShard) {
  tkrzw::StdHashDBM dbm(10);
  tkrzw::DBMUpdateLoggerStrDeque ulog_core(" ");
  tkrzw::DBMUpdateLoggerSecondShard ulog(&ulog_core);
  EXPECT_EQ(tkrzw::Status::SUCCESS, ulog.WriteSet("one", "hop"));
  EXPECT_EQ(tkrzw::Status::SUCCESS, ulog.WriteRemove("two"));
  EXPECT_EQ(tkrzw::Status::SUCCESS, ulog.WriteClear());
  EXPECT_EQ(tkrzw::Status::SUCCESS, ulog.WriteSet("two", "step"));
  EXPECT_EQ(tkrzw::Status::SUCCESS, ulog.Synchronize(false));
  EXPECT_EQ(3, ulog_core.GetSize());
  std::string text;
  EXPECT_TRUE(ulog_core.PopFront(&text));
  EXPECT_EQ("SET one hop", text);
  EXPECT_TRUE(ulog_core.PopFront(&text));
  EXPECT_EQ("REMOVE two", text);
  EXPECT_TRUE(ulog_core.PopFront(&text));
  EXPECT_EQ("SET two step", text);
}

TEST(DBMUpdateLoggerTest, MQWrite) {
  tkrzw::TemporaryDirectory tmp_dir(true, "tkrzw-");
  const std::string prefix = tmp_dir.MakeUniquePath("casket-", "-mq");
  tkrzw::MessageQueue mq;
  tkrzw::DBMUpdateLoggerMQ ulog(&mq, 1, 2);
  tkrzw::StdHashDBM dbm(10);
  EXPECT_EQ(tkrzw::Status::SUCCESS, mq.Open(prefix, 10000, tkrzw::MessageQueue::OPEN_TRUNCATE));
  dbm.SetUpdateLogger(&ulog);
  const int64_t begin_ts = tkrzw::GetWallTime() * 1000;
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm.Set("one", "first"));
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm.Set("two", "second"));
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm.Remove("two"));
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm.Clear());
  const int64_t end_ts = tkrzw::GetWallTime() * 1000;
  auto reader = mq.MakeReader(begin_ts);
  int64_t timestamp = 0;
  std::string message;
  EXPECT_EQ(tkrzw::Status::SUCCESS, reader->Read(&timestamp, &message, 0));
  EXPECT_GE(timestamp, begin_ts);
  EXPECT_LE(timestamp, end_ts);
  EXPECT_EQ(std::string("\xA1\x01\x02\x03\x05onefirst", 13), message);
  EXPECT_EQ(tkrzw::Status::SUCCESS, reader->Read(&timestamp, &message, 0));
  EXPECT_GE(timestamp, begin_ts);
  EXPECT_LE(timestamp, end_ts);
  EXPECT_EQ(std::string("\xA1\x01\x02\x03\x06twosecond", 14), message);
  EXPECT_EQ(tkrzw::Status::SUCCESS, reader->Read(&timestamp, &message, 0));
  EXPECT_GE(timestamp, begin_ts);
  EXPECT_LE(timestamp, end_ts);
  EXPECT_EQ(std::string("\xA2\x01\x02\x03two", 7), message);
  EXPECT_EQ(tkrzw::Status::SUCCESS, reader->Read(&timestamp, &message, 0));
  EXPECT_GE(timestamp, begin_ts);
  EXPECT_LE(timestamp, end_ts);
  EXPECT_EQ(std::string("\xA3\x01\x02", 3), message);
  EXPECT_EQ(tkrzw::Status::INFEASIBLE_ERROR, reader->Read(&timestamp, &message, 0));
  EXPECT_EQ(tkrzw::Status::SUCCESS, mq.Close());
}

TEST(DBMUpdateLoggerTest, MQParseUpdateLog) {
  tkrzw::DBMUpdateLoggerMQ::UpdateLog op;
  EXPECT_EQ(tkrzw::Status::BROKEN_DATA_ERROR,
            tkrzw::DBMUpdateLoggerMQ::ParseUpdateLog("", &op));
  EXPECT_EQ(tkrzw::Status::BROKEN_DATA_ERROR,
            tkrzw::DBMUpdateLoggerMQ::ParseUpdateLog(std::string_view(
                "\xA1\x00\x00\x0F\x0F", 5), &op));
  EXPECT_EQ(tkrzw::Status::BROKEN_DATA_ERROR,
            tkrzw::DBMUpdateLoggerMQ::ParseUpdateLog(std::string_view(
                "\xA1", 1), &op));
  EXPECT_EQ(tkrzw::Status::BROKEN_DATA_ERROR,
            tkrzw::DBMUpdateLoggerMQ::ParseUpdateLog(std::string_view(
                "\xA1\x00\x00", 3), &op));
  EXPECT_EQ(tkrzw::Status::BROKEN_DATA_ERROR,
            tkrzw::DBMUpdateLoggerMQ::ParseUpdateLog(std::string_view(
                "\xA1\x00\x00\x0F", 4), &op));
  EXPECT_EQ(tkrzw::Status::BROKEN_DATA_ERROR,
            tkrzw::DBMUpdateLoggerMQ::ParseUpdateLog(std::string_view(
                "\xFF\x00\x00", 3), &op));
  EXPECT_EQ(tkrzw::Status::SUCCESS,
            tkrzw::DBMUpdateLoggerMQ::ParseUpdateLog(std::string_view(
                "\xA1\x01\x02\x03\x05onefirst", 13), &op));
  EXPECT_EQ(tkrzw::DBMUpdateLoggerMQ::OP_SET, op.op_type);
  EXPECT_EQ("one", op.key);
  EXPECT_EQ("first", op.value);
  EXPECT_EQ(1, op.server_id);
  EXPECT_EQ(2, op.dbm_index);
  EXPECT_EQ(tkrzw::Status::SUCCESS,
            tkrzw::DBMUpdateLoggerMQ::ParseUpdateLog(std::string_view(
                "\xA1\x81\x00\x81\x80\x00\x03\x06twosecond", 17), &op));
  EXPECT_EQ(tkrzw::DBMUpdateLoggerMQ::OP_SET, op.op_type);
  EXPECT_EQ("two", op.key);
  EXPECT_EQ("second", op.value);
  EXPECT_EQ(128, op.server_id);
  EXPECT_EQ(16384, op.dbm_index);
  EXPECT_EQ(tkrzw::Status::SUCCESS,
            tkrzw::DBMUpdateLoggerMQ::ParseUpdateLog(std::string_view(
                "\xA2\x01\x02\x03two", 7), &op));
  EXPECT_EQ(tkrzw::DBMUpdateLoggerMQ::OP_REMOVE, op.op_type);
  EXPECT_EQ("two", op.key);
  EXPECT_EQ("", op.value);
  EXPECT_EQ(tkrzw::Status::SUCCESS,
            tkrzw::DBMUpdateLoggerMQ::ParseUpdateLog(std::string_view(
                "\xA3\x01\x02", 3), &op));
  EXPECT_EQ(tkrzw::DBMUpdateLoggerMQ::OP_CLEAR, op.op_type);
  EXPECT_EQ("", op.key);
  EXPECT_EQ("", op.value);
}

TEST(DBMUpdateLoggerTest, MQApplyUpdateLog) {
  tkrzw::StdHashDBM dbm(10);
  EXPECT_EQ(tkrzw::Status::BROKEN_DATA_ERROR,
            tkrzw::DBMUpdateLoggerMQ::ApplyUpdateLog(&dbm, ""));
  EXPECT_EQ(tkrzw::Status::INFEASIBLE_ERROR,
            tkrzw::DBMUpdateLoggerMQ::ApplyUpdateLog(&dbm, std::string_view(
                "\xA3\x00\x00", 3), 1, 0));
  EXPECT_EQ(tkrzw::Status::INFEASIBLE_ERROR,
            tkrzw::DBMUpdateLoggerMQ::ApplyUpdateLog(&dbm, std::string_view(
                "\xA3\x01\x00", 3), -1, 0));

  EXPECT_EQ(tkrzw::Status::INFEASIBLE_ERROR,
            tkrzw::DBMUpdateLoggerMQ::ApplyUpdateLog(&dbm, std::string_view(
                "\xA3\x00\x00", 3), 0, 1));
  EXPECT_EQ(tkrzw::Status::INFEASIBLE_ERROR,
            tkrzw::DBMUpdateLoggerMQ::ApplyUpdateLog(&dbm, std::string_view(
                "\xA3\x00\x01", 3), 0, -1));
  EXPECT_EQ(tkrzw::Status::SUCCESS,
            tkrzw::DBMUpdateLoggerMQ::ApplyUpdateLog(&dbm, std::string_view(
                "\xA1\x00\x00\x03\x05onefirst", 13), -128, 0));
  EXPECT_EQ("first", dbm.GetSimple("one"));
  EXPECT_EQ(tkrzw::Status::SUCCESS,
            tkrzw::DBMUpdateLoggerMQ::ApplyUpdateLog(&dbm, std::string_view(
                "\xA1\x00\x00\x03\x06twosecond", 14), 0, -128));
  EXPECT_EQ("second", dbm.GetSimple("two"));
  EXPECT_EQ(tkrzw::Status::SUCCESS,
            tkrzw::DBMUpdateLoggerMQ::ApplyUpdateLog(&dbm, std::string_view(
                "\xA2\x00\x00\x03two", 7)));
  EXPECT_EQ("", dbm.GetSimple("two"));
  EXPECT_EQ(1, dbm.CountSimple());
  EXPECT_EQ(tkrzw::Status::SUCCESS,
            tkrzw::DBMUpdateLoggerMQ::ApplyUpdateLog(&dbm, std::string_view(
                "\xA3\x00\x00", 3)));
  EXPECT_EQ(0, dbm.CountSimple());
}

TEST(DBMUpdateLoggerTest, MQIntegrate) {
  tkrzw::TemporaryDirectory tmp_dir(true, "tkrzw-");
  const std::string prefix = tmp_dir.MakeUniquePath("casket-", "-mq");
  constexpr int32_t num_iterations = 10000;
  tkrzw::MessageQueue mq;
  tkrzw::DBMUpdateLoggerMQ ulog(&mq, 333333, 999), ulog_dummy(&mq, 333, 999);
  tkrzw::StdTreeDBM src_dbm, dest_dbm1, dest_dbm2, dummy_dbm1, dummy_dbm2;
  EXPECT_EQ(tkrzw::Status::SUCCESS,
            mq.Open(prefix, 100000, tkrzw::MessageQueue::OPEN_TRUNCATE));
  src_dbm.SetUpdateLogger(&ulog);
  dummy_dbm1.SetUpdateLogger(&ulog);
  dummy_dbm2.SetUpdateLogger(&ulog_dummy);
  tkrzw::WaitCounter wc(2);
  auto copier =
      [&](tkrzw::DBM* dest) {
        auto reader = mq.MakeReader(0);
        int32_t count = num_iterations;
        while (true) {
          int64_t timestamp = 0;
          std::string message;
          tkrzw::Status status = reader->Read(&timestamp, &message, 0.001);
          if (status != tkrzw::Status::SUCCESS) {
            if (status == tkrzw::Status::INFEASIBLE_ERROR) {
              continue;
            }
            EXPECT_EQ(tkrzw::Status::CANCELED_ERROR, status);
            break;
          }
          tkrzw::DBMUpdateLoggerMQ::UpdateLog op;
          EXPECT_EQ(tkrzw::Status::SUCCESS,
                    tkrzw::DBMUpdateLoggerMQ::ParseUpdateLog(message, &op));
          status = tkrzw::DBMUpdateLoggerMQ::ApplyUpdateLog(dest, message, 333333, 999);
          if (status == tkrzw::Status::SUCCESS) {
            EXPECT_EQ(333333, op.server_id);
            if (--count == 0) {
              wc.Done();
            }
          } else {
            EXPECT_TRUE(op.server_id == 333 || op.server_id == 888);
            EXPECT_EQ(tkrzw::Status::INFEASIBLE_ERROR, status);
          }
        }
        wc.Done();
      };
  auto th1 = std::thread(copier, &dest_dbm1);
  auto th2 = std::thread(copier, &dest_dbm2);
  std::mt19937 mt(1);
  std::uniform_int_distribution<int32_t> key_num_dist(1, num_iterations);
  std::uniform_int_distribution<int32_t> op_dist(0, 3);
  constexpr int32_t clear_pos = num_iterations / 8;
  for (int32_t i = 1; i <= num_iterations; i++) {
    const int32_t key_num = key_num_dist(mt);
    const std::string key = tkrzw::ToString(key_num);
    const std::string value = tkrzw::ToString(i * i);
    if (i == clear_pos) {
      EXPECT_EQ(tkrzw::Status::SUCCESS, src_dbm.Clear());
    } else {
      switch (op_dist(mt)) {
        case 0:
          EXPECT_EQ(tkrzw::Status::SUCCESS, src_dbm.Set(key, value));
          tkrzw::DBMUpdateLoggerMQ::OverwriteThreadServerID(tkrzw::INT32MIN);
          dummy_dbm1.Set(key, "DUMMY");
          tkrzw::DBMUpdateLoggerMQ::OverwriteThreadServerID(888);
          dummy_dbm2.Set(key, "DUMMY");
          tkrzw::DBMUpdateLoggerMQ::OverwriteThreadServerID(-1);
          dummy_dbm2.Set(key, "DUMMY");
          break;
        case 1:
          EXPECT_EQ(tkrzw::Status::SUCCESS, src_dbm.Append(key, value, ":"));
          break;
        case 2: {
          const tkrzw::Status status = src_dbm.Remove(key);
          if (status != tkrzw::Status::SUCCESS) {
            EXPECT_EQ(tkrzw::Status::NOT_FOUND_ERROR, status);
            EXPECT_EQ(tkrzw::Status::SUCCESS, src_dbm.Set(key, value));
          }
          tkrzw::DBMUpdateLoggerMQ::OverwriteThreadServerID(tkrzw::INT32MIN);
          dummy_dbm1.Remove(key);
          tkrzw::DBMUpdateLoggerMQ::OverwriteThreadServerID(888);
          dummy_dbm2.Remove(key);
          tkrzw::DBMUpdateLoggerMQ::OverwriteThreadServerID(-1);
          dummy_dbm2.Remove(key);
          break;
        }
        default: {
          const tkrzw::Status status = src_dbm.Set(key, value, false);
          if (status != tkrzw::Status::SUCCESS) {
            EXPECT_EQ(tkrzw::Status::DUPLICATION_ERROR, status);
            EXPECT_EQ(tkrzw::Status::SUCCESS, src_dbm.Remove(key));
          }
          tkrzw::DBMUpdateLoggerMQ::OverwriteThreadServerID(tkrzw::INT32MIN);
          dummy_dbm1.Set(key, "DUMMY");
          tkrzw::DBMUpdateLoggerMQ::OverwriteThreadServerID(888);
          dummy_dbm2.Set(key, "DUMMY");
          tkrzw::DBMUpdateLoggerMQ::OverwriteThreadServerID(-1);
          dummy_dbm2.Set(key, "DUMMY");
          break;
        }
      }
    }
  }
  wc.Wait();
  EXPECT_EQ(tkrzw::Status::SUCCESS, mq.Close());
  EXPECT_EQ(src_dbm.CountSimple(), dest_dbm1.CountSimple());
  EXPECT_EQ(src_dbm.CountSimple(), dest_dbm2.CountSimple());
  std::string key, value;
  auto iter = src_dbm.MakeIterator();
  EXPECT_EQ(tkrzw::Status::SUCCESS, iter->First());
  while (iter->Get(&key, &value).IsOK()) {
    EXPECT_EQ(value, dest_dbm1.GetSimple(key));
    EXPECT_EQ(value, dest_dbm2.GetSimple(key));
    EXPECT_EQ(tkrzw::Status::SUCCESS, iter->Next());
  }
  th2.join();
  th1.join();
}

TEST(DBMUpdateLoggerTest, ApplyUpdateLogFromFiles) {
  tkrzw::TemporaryDirectory tmp_dir(true, "tkrzw-");
  const std::string prefix = tmp_dir.MakeUniquePath("casket-", "-mq");
  constexpr int32_t num_iterations = 10000;
  tkrzw::MessageQueue mq;
  tkrzw::DBMUpdateLoggerMQ ulog(&mq, 44444, 888);
  tkrzw::StdTreeDBM src_dbm, dest_dbm;
  EXPECT_EQ(tkrzw::Status::SUCCESS,
            mq.Open(prefix, 100000, tkrzw::MessageQueue::OPEN_TRUNCATE));
  src_dbm.SetUpdateLogger(&ulog);
  EXPECT_EQ(tkrzw::Status::SUCCESS, src_dbm.Set("hello", "world"));
  EXPECT_EQ(tkrzw::Status::SUCCESS, src_dbm.Append("hello", "world"));
  EXPECT_EQ(tkrzw::Status::SUCCESS, src_dbm.Clear());
  for (int32_t i = 1; i <= num_iterations; i++) {
    const std::string key = tkrzw::ToString(i);
    const std::string value = tkrzw::ToString(i * i);
    EXPECT_EQ(tkrzw::Status::SUCCESS, src_dbm.Set(key, value));
    if (i % 2 == 0) {
      EXPECT_EQ(tkrzw::Status::SUCCESS, src_dbm.Remove(key));
    }
  }
  EXPECT_EQ(tkrzw::Status::SUCCESS, mq.Close());
  EXPECT_EQ(tkrzw::Status::SUCCESS, tkrzw::DBMUpdateLoggerMQ::ApplyUpdateLogFromFiles(
      &dest_dbm, prefix, 0, 44444, 888));
  EXPECT_EQ(src_dbm.CountSimple(), dest_dbm.CountSimple());
  for (int32_t i = 1; i <= num_iterations; i++) {
    const std::string key = tkrzw::ToString(i);
    const std::string value = tkrzw::ToString(i * i);
    if (i % 2 == 0) {
      EXPECT_EQ(tkrzw::Status::NOT_FOUND_ERROR, dest_dbm.Get(key));
    } else {
      EXPECT_EQ(value, dest_dbm.GetSimple(key));
    }
  }
}

// END OF FILE
