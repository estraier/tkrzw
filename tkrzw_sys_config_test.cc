/*************************************************************************************************
 * Tests for tkrzw_sys_config.h
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

using namespace testing;

// Main routine
int main(int argc, char** argv) {
  InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

TEST(SysConfigTest, HexDump) {
  const char* buf = "\x00\x12\x21\x9A\xA9\xEF\xFE\xFF";
  EXPECT_EQ("", tkrzw::HexDump(buf, 0));
  EXPECT_EQ("00 12 21 9A", tkrzw::HexDump(buf, 4));
  EXPECT_EQ("00 12 21 9A A9 EF FE FF", tkrzw::HexDump(buf, 8));
  EXPECT_EQ("00 12 21\n9A A9 EF\nFE FF", tkrzw::HexDump(buf, 8, 3));
  EXPECT_EQ("00 12 21 9A\nA9 EF FE FF", tkrzw::HexDump(buf, 8, 4));
  EXPECT_EQ("61 62\n63 64", tkrzw::HexDumpStr("abcd", 2));
}

TEST(SysConfigTest, AlignNumber) {
  EXPECT_EQ(0, tkrzw::AlignNumber(0, 10));
  EXPECT_EQ(10, tkrzw::AlignNumber(1, 10));
  EXPECT_EQ(10, tkrzw::AlignNumber(10, 10));
  EXPECT_EQ(20, tkrzw::AlignNumber(11, 10));
  EXPECT_EQ(0, tkrzw::AlignNumber(0, 512));
  EXPECT_EQ(512, tkrzw::AlignNumber(1, 512));
  EXPECT_EQ(512, tkrzw::AlignNumber(512, 512));
}

TEST(SysConfigTest, AlignNumberPowTwo) {
  EXPECT_EQ(1, tkrzw::AlignNumberPowTwo(1));
  EXPECT_EQ(2, tkrzw::AlignNumberPowTwo(2));
  EXPECT_EQ(4, tkrzw::AlignNumberPowTwo(3));
  EXPECT_EQ(4, tkrzw::AlignNumberPowTwo(4));
  EXPECT_EQ(8, tkrzw::AlignNumberPowTwo(5));
  EXPECT_EQ(8, tkrzw::AlignNumberPowTwo(7));
  EXPECT_EQ(8, tkrzw::AlignNumberPowTwo(8));
  EXPECT_EQ(16, tkrzw::AlignNumberPowTwo(9));
}

TEST(SysConfigTest, ByteOrders) {
  if (tkrzw::IS_BIG_ENDIAN) {
    EXPECT_EQ(0x1122, tkrzw::HostToNet16(0x1122));
    EXPECT_EQ(0x1122, tkrzw::NetToHost16(0x1122));
    EXPECT_EQ(0x11223344, tkrzw::HostToNet32(0x11223344));
    EXPECT_EQ(0x11223344, tkrzw::NetToHost32(0x11223344));
    EXPECT_EQ(0x1122334455667788, tkrzw::HostToNet64(0x1122334455667788));
    EXPECT_EQ(0x1122334455667788, tkrzw::NetToHost64(0x1122334455667788));
  } else {
    EXPECT_EQ(0x2211, tkrzw::HostToNet16(0x1122));
    EXPECT_EQ(0x2211, tkrzw::NetToHost16(0x1122));
    EXPECT_EQ(0x44332211, tkrzw::HostToNet32(0x11223344));
    EXPECT_EQ(0x44332211, tkrzw::NetToHost32(0x11223344));
    EXPECT_EQ(0x8877665544332211, tkrzw::HostToNet64(0x1122334455667788));
    EXPECT_EQ(0x8877665544332211, tkrzw::NetToHost64(0x1122334455667788));
  }
}

TEST(SysConfigTest, FixNumStatic) {
  char buf[8];
  tkrzw::WriteFixNum(buf, 0x1122334455667788, 8);
  EXPECT_EQ(0, std::memcmp(buf, "\x11\x22\x33\x44\x55\x66\x77\x88", 8));
  EXPECT_EQ(0x1122334455667788, tkrzw::ReadFixNum(buf, 8));
  tkrzw::WriteFixNum(buf, 0x1122334455667788, 6);
  EXPECT_EQ(0, std::memcmp(buf, "\x33\x44\x55\x66\x77\x88", 6));
  EXPECT_EQ(0x334455667788, tkrzw::ReadFixNum(buf, 6));
  tkrzw::WriteFixNum(buf, 0x1122334455667788, 4);
  EXPECT_EQ(0, std::memcmp(buf, "\x55\x66\x77\x88", 4));
  EXPECT_EQ(0x55667788, tkrzw::ReadFixNum(buf, 4));
  tkrzw::WriteFixNum(buf, 0x1122334455667788, 2);
  EXPECT_EQ(0, std::memcmp(buf, "\x77\x88", 2));
  EXPECT_EQ(0x7788, tkrzw::ReadFixNum(buf, 2));
}

TEST(SysConfigTest, FixNumRandom) {
  constexpr int32_t num_iterations = 10;
  constexpr uint64_t max_number = 1ULL << 60;
  std::mt19937 mt(1);
  std::uniform_int_distribution<int32_t> dist(0, 10);
  for (int32_t i = 0; i < num_iterations; i++) {
    uint64_t num = 0;
    while (num <= max_number) {
      char buf[8];
      for (int32_t width = 1; width <= 8; width++) {
        uint64_t mask = 0xFFFFFFFFFFFFFFFF;
        if (width != 8) {
          mask = ~((mask >> (width * 8)) << (width * 8));
        }
        const uint64_t masked_num = num & mask;
        tkrzw::WriteFixNum(buf, num, width);
        const uint64_t read_num = tkrzw::ReadFixNum(buf, width);
        EXPECT_EQ(masked_num, read_num);
      }
      if (dist(mt) == 0) {
        num *= 2;
      }
      num += dist(mt) + 1;
    }
  }
}

TEST(SysConfigTest, VarNumStatic) {
  char buf[tkrzw::NUM_BUFFER_SIZE];
  uint64_t num = 0;
  EXPECT_EQ(1, tkrzw::SizeVarNum(0ULL));
  EXPECT_EQ(1, tkrzw::WriteVarNum(buf, 0ULL));
  EXPECT_EQ(0, std::memcmp(buf, "\x00", 1));
  EXPECT_EQ(1, tkrzw::ReadVarNum(buf, 1, &num));
  EXPECT_EQ(0ULL, num);
  EXPECT_EQ(1, tkrzw::ReadVarNum(buf, &num));
  EXPECT_EQ(0ULL, num);
  EXPECT_EQ(1, tkrzw::SizeVarNum(1ULL));
  EXPECT_EQ(1, tkrzw::WriteVarNum(buf, 1ULL));
  EXPECT_EQ(0, std::memcmp(buf, "\x01", 1));
  EXPECT_EQ(1, tkrzw::ReadVarNum(buf, 1, &num));
  EXPECT_EQ(1ULL, num);
  EXPECT_EQ(1, tkrzw::ReadVarNum(buf, &num));
  EXPECT_EQ(1ULL, num);
  EXPECT_EQ(1, tkrzw::SizeVarNum(127ULL));
  EXPECT_EQ(1, tkrzw::WriteVarNum(buf, 127ULL));
  EXPECT_EQ(0, std::memcmp(buf, "\x7F", 1));
  EXPECT_EQ(1, tkrzw::ReadVarNum(buf, 1, &num));
  EXPECT_EQ(127ULL, num);
  EXPECT_EQ(1, tkrzw::ReadVarNum(buf, &num));
  EXPECT_EQ(127ULL, num);
  EXPECT_EQ(2, tkrzw::SizeVarNum(128ULL));
  EXPECT_EQ(2, tkrzw::WriteVarNum(buf, 128ULL));
  EXPECT_EQ(0, std::memcmp(buf, "\x81\x00", 2));
  EXPECT_EQ(2, tkrzw::ReadVarNum(buf, 2, &num));
  EXPECT_EQ(128ULL, num);
  EXPECT_EQ(2, tkrzw::ReadVarNum(buf, &num));
  EXPECT_EQ(128ULL, num);
  EXPECT_EQ(2, tkrzw::SizeVarNum(16383ULL));
  EXPECT_EQ(2, tkrzw::WriteVarNum(buf, 16383ULL));
  EXPECT_EQ(0, std::memcmp(buf, "\xFF\x7F", 2));
  EXPECT_EQ(2, tkrzw::ReadVarNum(buf, 2, &num));
  EXPECT_EQ(16383ULL, num);
  EXPECT_EQ(2, tkrzw::ReadVarNum(buf, &num));
  EXPECT_EQ(16383ULL, num);
  EXPECT_EQ(3, tkrzw::SizeVarNum(16384ULL));
  EXPECT_EQ(3, tkrzw::WriteVarNum(buf, 16384ULL));
  EXPECT_EQ(0, std::memcmp(buf, "\x81\x80\x00", 3));
  EXPECT_EQ(3, tkrzw::ReadVarNum(buf, 3, &num));
  EXPECT_EQ(16384ULL, num);
  EXPECT_EQ(3, tkrzw::ReadVarNum(buf, &num));
  EXPECT_EQ(16384ULL, num);
  EXPECT_EQ(3, tkrzw::SizeVarNum(2097151ULL));
  EXPECT_EQ(3, tkrzw::WriteVarNum(buf, 2097151ULL));
  EXPECT_EQ(0, std::memcmp(buf, "\xFF\xFF\x7F", 3));
  EXPECT_EQ(3, tkrzw::ReadVarNum(buf, 3, &num));
  EXPECT_EQ(2097151ULL, num);
  EXPECT_EQ(3, tkrzw::ReadVarNum(buf, &num));
  EXPECT_EQ(2097151ULL, num);
  EXPECT_EQ(4, tkrzw::SizeVarNum(2097152ULL));
  EXPECT_EQ(4, tkrzw::WriteVarNum(buf, 2097152ULL));
  EXPECT_EQ(0, std::memcmp(buf, "\x81\x80\x80\x00", 4));
  EXPECT_EQ(4, tkrzw::ReadVarNum(buf, 4, &num));
  EXPECT_EQ(2097152ULL, num);
  EXPECT_EQ(4, tkrzw::ReadVarNum(buf, &num));
  EXPECT_EQ(2097152ULL, num);
  EXPECT_EQ(4, tkrzw::SizeVarNum(268435455ULL));
  EXPECT_EQ(4, tkrzw::WriteVarNum(buf, 268435455ULL));
  EXPECT_EQ(0, std::memcmp(buf, "\xFF\xFF\xFF\x7F", 4));
  EXPECT_EQ(4, tkrzw::ReadVarNum(buf, 5, &num));
  EXPECT_EQ(268435455ULL, num);
  EXPECT_EQ(4, tkrzw::ReadVarNum(buf, &num));
  EXPECT_EQ(268435455ULL, num);
  EXPECT_EQ(5, tkrzw::SizeVarNum(268435456ULL));
  EXPECT_EQ(5, tkrzw::WriteVarNum(buf, 268435456ULL));
  EXPECT_EQ(0, std::memcmp(buf, "\x81\x80\x80\x80\x00", 5));
  EXPECT_EQ(5, tkrzw::ReadVarNum(buf, 5, &num));
  EXPECT_EQ(268435456ULL, num);
  EXPECT_EQ(5, tkrzw::ReadVarNum(buf, &num));
  EXPECT_EQ(268435456ULL, num);
  EXPECT_EQ(5, tkrzw::SizeVarNum(34359738367ULL));
  EXPECT_EQ(5, tkrzw::WriteVarNum(buf, 34359738367ULL));
  EXPECT_EQ(0, std::memcmp(buf, "\xFF\xFF\xFF\xFF\x7F", 5));
  EXPECT_EQ(5, tkrzw::ReadVarNum(buf, 5, &num));
  EXPECT_EQ(34359738367ULL, num);
  EXPECT_EQ(5, tkrzw::ReadVarNum(buf, &num));
  EXPECT_EQ(34359738367ULL, num);
}

TEST(SysConfigTest, VarNumRandom) {
  constexpr int32_t num_iterations = 40;
  constexpr uint64_t max_number = 1ULL << 60;
  std::mt19937 mt(1);
  std::uniform_int_distribution<int32_t> dist(0, 10);
  for (int32_t i = 0; i < num_iterations; i++) {
    uint64_t num = 0;
    while (num <= max_number) {
      char buf[tkrzw::NUM_BUFFER_SIZE];
      const int32_t width = tkrzw::SizeVarNum(num);
      EXPECT_GT(width, 0);
      EXPECT_EQ(width, tkrzw::WriteVarNum(buf, num));
      uint64_t read_num = 0;
      EXPECT_EQ(width, tkrzw::ReadVarNum(buf, width, &read_num));
      if (dist(mt) == 0) {
        num *= 2;
      }
      num += dist(mt) + 1;
    }
  }
}

// END OF FILE
