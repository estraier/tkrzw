/*************************************************************************************************
 * Tests for tkrzw_time_util.h
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

#include "tkrzw_lib_common.h"
#include "tkrzw_str_util.h"
#include "tkrzw_time_util.h"

using namespace testing;

// Main routine
int main(int argc, char** argv) {
  InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

TEST(ThreadUtilTest, GetWallTime) {
  const double start_time = tkrzw::GetWallTime();
  EXPECT_GT(start_time, 0);
  std::this_thread::sleep_for(std::chrono::microseconds(static_cast<int64_t>(1000)));
  const double end_time = tkrzw::GetWallTime();
  EXPECT_GT(end_time, start_time);
}

TEST(ThreadUtilTest, GetCalendar) {
  struct std::tm lts;
  tkrzw::GetLocalCalendar(72000, &lts);
  EXPECT_EQ(70, lts.tm_year);
  EXPECT_EQ(0, lts.tm_mon);
  EXPECT_TRUE(lts.tm_mday == 1 || lts.tm_mday == 2);
  EXPECT_TRUE(lts.tm_wday == 4 || lts.tm_wday == 5);
  EXPECT_TRUE(lts.tm_yday == 0 || lts.tm_yday == 1);
  struct std::tm uts;
  tkrzw::GetUniversalCalendar(72000, &uts);
  EXPECT_EQ(70, uts.tm_year);
  EXPECT_EQ(0, uts.tm_mon);
  EXPECT_EQ(1, uts.tm_mday);
  EXPECT_EQ(20, uts.tm_hour);
  EXPECT_EQ(0, uts.tm_min);
  EXPECT_EQ(0, uts.tm_sec);
  EXPECT_EQ(4, uts.tm_wday);
  EXPECT_EQ(0, uts.tm_yday);
}

TEST(ThreadUtilTest, MakeUniversalTime) {
  struct std::tm uts;
  std::memset(&uts, 0, sizeof(uts));
  uts.tm_year = 70;
  uts.tm_mon = 0;
  uts.tm_mday = 1;
  EXPECT_EQ(0, tkrzw::MakeUniversalTime(uts));
  tkrzw::GetUniversalCalendar(1234567890, &uts);
  EXPECT_EQ(1234567890, tkrzw::MakeUniversalTime(uts));
}

TEST(ThreadUtilTest, GetLocalTimeDifference) {
  const int32_t td = tkrzw::GetLocalTimeDifference(true);
  EXPECT_TRUE(td > -86400 && td < 86400);
  EXPECT_EQ(td, tkrzw::GetLocalTimeDifference(true));
  EXPECT_EQ(td, tkrzw::GetLocalTimeDifference(false));
  if (td % 3600 == 0) {
    struct std::tm lts;
    tkrzw::GetLocalCalendar(0, &lts);
    EXPECT_EQ(td / 3600, lts.tm_hour);
    EXPECT_EQ(0, lts.tm_min);
    EXPECT_EQ(0, lts.tm_sec);
  }
}

TEST(ThreadUtilTest, GetDayOfWeek) {
  EXPECT_EQ(4, tkrzw::GetDayOfWeek(1970, 1, 1));
  EXPECT_EQ(5, tkrzw::GetDayOfWeek(1970, 1, 2));
  EXPECT_EQ(6, tkrzw::GetDayOfWeek(1978, 2, 11));
  EXPECT_EQ(0, tkrzw::GetDayOfWeek(1978, 2, 12));
  EXPECT_EQ(0, tkrzw::GetDayOfWeek(2000, 12, 31));
  EXPECT_EQ(1, tkrzw::GetDayOfWeek(2001, 1, 1));
}

TEST(ThreadUtilTest, FormatDateSimple) {
  char result[48];
  size_t size = tkrzw::FormatDateSimple(result, 0, 0);
  EXPECT_STREQ("1970/01/01 00:00:00", result);
  EXPECT_EQ(size, std::strlen(result));
  size = tkrzw::FormatDateSimpleWithFrac(result, 0, 0, 0);
  EXPECT_STREQ("1970/01/01 00:00:00", result);
  EXPECT_EQ(size, std::strlen(result));
  size = tkrzw::FormatDateSimple(result, 256037405, 32400);
  EXPECT_STREQ("1978/02/11 18:30:05", result);
  EXPECT_EQ(size, std::strlen(result));
  size = tkrzw::FormatDateSimpleWithFrac(result, 256037405.25, 32400, 4);
  EXPECT_STREQ("1978/02/11 18:30:05.2500", result);
  EXPECT_EQ(size, std::strlen(result));
  if (tkrzw::GetLocalTimeDifference() == 32400) {
    size = tkrzw::FormatDateSimple(result, 1234567890);
    EXPECT_STREQ("2009/02/14 08:31:30", result);
    EXPECT_EQ(size, std::strlen(result));
    size = tkrzw::FormatDateSimpleWithFrac(result, 1234567890.5);
    EXPECT_STREQ("2009/02/14 08:31:30.500000", result);
    EXPECT_EQ(size, std::strlen(result));
  }
  EXPECT_EQ(19, tkrzw::FormatDateSimple(result));
  EXPECT_EQ('2', result[0]);
  EXPECT_EQ(26, tkrzw::FormatDateSimpleWithFrac(result));
  EXPECT_EQ('2', result[0]);
}

TEST(ThreadUtilTest, FormatDateW3CDTF) {
  char result[48];
  size_t size = tkrzw::FormatDateW3CDTF(result, 0, 0);
  EXPECT_STREQ("1970-01-01T00:00:00Z", result);
  EXPECT_EQ(size, std::strlen(result));
  size = tkrzw::FormatDateW3CDTFWithFrac(result, 0, 0, 0);
  EXPECT_STREQ("1970-01-01T00:00:00Z", result);
  EXPECT_EQ(size, std::strlen(result));
  size = tkrzw::FormatDateW3CDTF(result, 256037405, 32400);
  EXPECT_STREQ("1978-02-11T18:30:05+09:00", result);
  EXPECT_EQ(size, std::strlen(result));
  size = tkrzw::FormatDateW3CDTFWithFrac(result, 256037405.25, 32400, 4);
  EXPECT_STREQ("1978-02-11T18:30:05.2500+09:00", result);
  EXPECT_EQ(size, std::strlen(result));
  if (tkrzw::GetLocalTimeDifference() == 32400) {
    size = tkrzw::FormatDateW3CDTF(result, 1234567890);
    EXPECT_STREQ("2009-02-14T08:31:30+09:00", result);
    EXPECT_EQ(size, std::strlen(result));
    size = tkrzw::FormatDateW3CDTFWithFrac(result, 1234567890.5);
    EXPECT_STREQ("2009-02-14T08:31:30.500000+09:00", result);
    EXPECT_EQ(size, std::strlen(result));
  }
  tkrzw::FormatDateW3CDTF(result);
  EXPECT_EQ('2', result[0]);
  tkrzw::FormatDateW3CDTFWithFrac(result);
  EXPECT_EQ('2', result[0]);
}

TEST(ThreadUtilTest, FormatDateRFC1123) {
  char result[48];
  size_t size = tkrzw::FormatDateRFC1123(result, 0, 0);
  EXPECT_STREQ("Thu, 01 Jan 1970 00:00:00 GMT", result);
  EXPECT_EQ(size, std::strlen(result));
  size = tkrzw::FormatDateRFC1123(result, 256037405, 32400);
  EXPECT_STREQ("Sat, 11 Feb 1978 18:30:05 +0900", result);
  EXPECT_EQ(size, std::strlen(result));
  if (tkrzw::GetLocalTimeDifference() == 32400) {
    size = tkrzw::FormatDateRFC1123(result, 1234567890);
    EXPECT_STREQ("Sat, 14 Feb 2009 08:31:30 +0900", result);
    EXPECT_EQ(size, std::strlen(result));
  }
  tkrzw::FormatDateRFC1123(result);
  EXPECT_GE(strlen(result), 29);
}

TEST(ThreadUtilTest, ParseDateStr) {
  EXPECT_TRUE(std::isnan(tkrzw::ParseDateStr("")));
  EXPECT_EQ(0x123, tkrzw::ParseDateStr("0x123"));
  EXPECT_EQ(0xABCD, tkrzw::ParseDateStr(" 0XABCD"));
  EXPECT_EQ(-123, tkrzw::ParseDateStr(" -0123"));
  EXPECT_EQ(10, tkrzw::ParseDateStr("10 s "));
  EXPECT_EQ(-600, tkrzw::ParseDateStr("-10 m"));
  EXPECT_EQ(37800, tkrzw::ParseDateStr("10.5h"));
  EXPECT_EQ(864000, tkrzw::ParseDateStr("10d"));
  EXPECT_DOUBLE_EQ(0, tkrzw::ParseDateStr("1970-01-01T00:00:00"));
  EXPECT_DOUBLE_EQ(36610, tkrzw::ParseDateStr("1970-01-01T10:10:10"));
  EXPECT_DOUBLE_EQ(10, tkrzw::ParseDateStr("1970-01-01T09:10:10+09:10"));
  EXPECT_DOUBLE_EQ(-45000, tkrzw::ParseDateStr("1970-01-01T00:00:00+12:30"));
  EXPECT_DOUBLE_EQ(45000, tkrzw::ParseDateStr("1970-01-01T00:00:00-12:30"));
  EXPECT_DOUBLE_EQ(3666.75, tkrzw::ParseDateStr("1970/01/01 01:01:06.75"));
  EXPECT_DOUBLE_EQ(256037405.25, (tkrzw::ParseDateStr("1978/02/11 18:30:05.2500+09:00")));
  EXPECT_DOUBLE_EQ(1234567890.12345, tkrzw::ParseDateStr("2009:02:14 08:31:30.12345+09:00"));
  EXPECT_DOUBLE_EQ(0, tkrzw::ParseDateStr("Thu, 01 Jan 1970"));
  EXPECT_DOUBLE_EQ(256003200, tkrzw::ParseDateStr("Sat, 11 Feb 1978"));
  EXPECT_DOUBLE_EQ(0, tkrzw::ParseDateStr("Thu, 01 Jan 1970 00:00:00 GMT"));
  EXPECT_DOUBLE_EQ(-32400, tkrzw::ParseDateStr("Thu, 01 Jan 1970 00:00:00 JST"));
  EXPECT_DOUBLE_EQ(28800, tkrzw::ParseDateStr("Thu, 01 Jan 1970 00:00:00 PST"));
  EXPECT_DOUBLE_EQ(0, tkrzw::ParseDateStr("Thu, 01 Jan 1970 09:00:00 JST"));
  EXPECT_DOUBLE_EQ(256037405, tkrzw::ParseDateStr("Sat, 11 Feb 1978 18:30:05 +0900"));
  EXPECT_DOUBLE_EQ(1234567890, tkrzw::ParseDateStr("Sat, 14 Feb 2009 08:31:30 +0900"));
  EXPECT_TRUE(std::isnan(tkrzw::ParseDateStr("hoge")));
  EXPECT_TRUE(std::isnan(tkrzw::ParseDateStr("2001-")));
  EXPECT_TRUE(std::isnan(tkrzw::ParseDateStr("2001-11-")));
  EXPECT_TRUE(std::isnan(tkrzw::ParseDateStr("Thu, ")));
  EXPECT_TRUE(std::isnan(tkrzw::ParseDateStr("Thu, 01 xyz 1970")));
}

TEST(ThreadUtilTest, ParseDateStrYYYYMMDD) {
  EXPECT_TRUE(std::isnan(tkrzw::ParseDateStrYYYYMMDD("")));
  EXPECT_TRUE(std::isnan(tkrzw::ParseDateStrYYYYMMDD("197")));
  EXPECT_EQ(0, tkrzw::ParseDateStrYYYYMMDD("  1970-01-01  ", 0));
  EXPECT_EQ(0, tkrzw::ParseDateStrYYYYMMDD("19700101", 0));
  EXPECT_EQ(3, tkrzw::ParseDateStrYYYYMMDD("1970-01-01 00:00:03", 0));
  EXPECT_EQ(180, tkrzw::ParseDateStrYYYYMMDD("1970-01-01 00:03:00", 0));
  EXPECT_EQ(10800, tkrzw::ParseDateStrYYYYMMDD("1970-01-01 03:00:00", 0));
  EXPECT_EQ(0, tkrzw::ParseDateStrYYYYMMDD("1970-01-01 03:00:00", 10800));
  EXPECT_EQ(256037435, tkrzw::ParseDateStrYYYYMMDD("19780211183035", 32400));
}

TEST(ThreadUtilTest, MakeRelativeTimeExpr) {
  EXPECT_EQ("0.0 seconds", tkrzw::MakeRelativeTimeExpr(0));
  EXPECT_EQ("1.2 seconds", tkrzw::MakeRelativeTimeExpr(1.2));
  EXPECT_EQ("-2.8 seconds", tkrzw::MakeRelativeTimeExpr(-2.8));
  EXPECT_EQ("1.0 minutes", tkrzw::MakeRelativeTimeExpr(60));
  EXPECT_EQ("-2.0 minutes", tkrzw::MakeRelativeTimeExpr(60 * -2));
  EXPECT_EQ("3.5 minutes", tkrzw::MakeRelativeTimeExpr(60 * 3.5));
  EXPECT_EQ("1.0 hours", tkrzw::MakeRelativeTimeExpr(60 * 60));
  EXPECT_EQ("-2.0 hours", tkrzw::MakeRelativeTimeExpr(60 * 60 * -2));
  EXPECT_EQ("3.5 hours", tkrzw::MakeRelativeTimeExpr(60 * 60 * 3.5));
  EXPECT_EQ("1.0 days", tkrzw::MakeRelativeTimeExpr(60 * 60 * 24));
  EXPECT_EQ("-2.0 days", tkrzw::MakeRelativeTimeExpr(60 * 60 * 24 * -2));
  EXPECT_EQ("3.5 days", tkrzw::MakeRelativeTimeExpr(60 * 60 * 24 * 3.5));
}

// END OF FILE
