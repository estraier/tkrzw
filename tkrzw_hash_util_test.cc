/*************************************************************************************************
 * Tests for the common library features
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

#include "tkrzw_hash_util.h"

using namespace testing;

// Main routine
int main(int argc, char** argv) {
  InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

TEST(LibCommonTest, HashMurmur) {
  EXPECT_EQ(0x15941D6097FA1378ULL, tkrzw::HashMurmur("Hello World", 19780211));
  EXPECT_EQ(0x4C6A0FFD2F090C3AULL, tkrzw::HashMurmur("こんにちは世界", 19780211));
}

TEST(LibCommonTest, HashFNV) {
  EXPECT_EQ(0x9AA143013F1E405FULL, tkrzw::HashFNV("Hello World"));
  EXPECT_EQ(0x8609C402DAD8A1EFULL, tkrzw::HashFNV("こんにちは世界"));
}

TEST(LibCommonTest, HashCRC8) {
  EXPECT_EQ(0x92, tkrzw::HashCRC8("hello"));
  EXPECT_EQ(0x25, tkrzw::HashCRC8("Hello World"));
  EXPECT_EQ(0xB7, tkrzw::HashCRC8("こんにちは世界"));
  uint32_t crc = tkrzw::HashCRC8Continuous("Hello", 5, false);
  crc = tkrzw::HashCRC8Continuous(" ", 1, false, crc);
  crc = tkrzw::HashCRC8Continuous("World", 5, true, crc);
  EXPECT_EQ(0x25, crc);
}

TEST(LibCommonTest, HashCRC16) {
  EXPECT_EQ(0xD26E, tkrzw::HashCRC16("hello"));
  EXPECT_EQ(0x4D25, tkrzw::HashCRC16("Hello World"));
  EXPECT_EQ(0xCFDB, tkrzw::HashCRC16("こんにちは世界"));
  uint32_t crc = tkrzw::HashCRC16Continuous("Hello", 5, false);
  crc = tkrzw::HashCRC16Continuous(" ", 1, false, crc);
  crc = tkrzw::HashCRC16Continuous("World", 5, true, crc);
  EXPECT_EQ(0x4D25, crc);
}

TEST(LibCommonTest, HashCRC32) {
  EXPECT_EQ(0x3610A686U, tkrzw::HashCRC32("hello"));
  EXPECT_EQ(0x4A17B156U, tkrzw::HashCRC32("Hello World"));
  EXPECT_EQ(0x75197186U, tkrzw::HashCRC32("こんにちは世界"));
  uint32_t crc = tkrzw::HashCRC32Continuous("Hello", 5, false);
  crc = tkrzw::HashCRC32Continuous(" ", 1, false, crc);
  crc = tkrzw::HashCRC32Continuous("World", 5, true, crc);
  EXPECT_EQ(0x4A17B156U, crc);
}

// END OF FILE
