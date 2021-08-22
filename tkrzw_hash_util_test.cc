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

std::string MakeCyclicString(size_t size = 256) {
  std::string str;
  str.reserve(size);
  for (size_t i = 0; i < size; i++) {
    str.append(1, i);
  }
  return str;
}

TEST(LibCommonTest, HashMurmur) {
  EXPECT_EQ(0x15941D6097FA1378ULL, tkrzw::HashMurmur("Hello World", 19780211));
  EXPECT_EQ(0x4C6A0FFD2F090C3AULL, tkrzw::HashMurmur("こんにちは世界", 19780211));
  EXPECT_EQ(0xD247B93561BD1053ULL, tkrzw::HashMurmur(MakeCyclicString(), 19780211));
  EXPECT_EQ(0x7D20AA9F76F60EC0ULL, tkrzw::HashMurmur(MakeCyclicString(100000), 19780211));
}

TEST(LibCommonTest, HashFNV) {
  EXPECT_EQ(0x9AA143013F1E405FULL, tkrzw::HashFNV("Hello World"));
  EXPECT_EQ(0x8609C402DAD8A1EFULL, tkrzw::HashFNV("こんにちは世界"));
  EXPECT_EQ(0x2F8C4ED90D46DE25ULL, tkrzw::HashFNV(MakeCyclicString()));
  EXPECT_EQ(0xB117046EFB9CE805ULL, tkrzw::HashFNV(MakeCyclicString(100000)));
}

TEST(LibCommonTest, HashChecksum6) {
  EXPECT_EQ(0x2C, tkrzw::HashChecksum6("hello"));
  EXPECT_EQ(0x0F, tkrzw::HashChecksum6("Hello World"));
  EXPECT_EQ(0x04, tkrzw::HashChecksum6("こんにちは世界"));
  EXPECT_EQ(0x05, tkrzw::HashChecksum6(MakeCyclicString()));
  EXPECT_EQ(0x1E, tkrzw::HashChecksum6(MakeCyclicString(100000)));
  uint32_t crc = tkrzw::HashChecksum6Continuous("Hello", 5, false);
  crc = tkrzw::HashChecksum6Continuous(" ", 1, false, crc);
  crc = tkrzw::HashChecksum6Continuous("World", 5, true, crc);
  EXPECT_EQ(0x0F, crc);
  crc = tkrzw::HashChecksum6Continuous("こんにちは", 15, false);
  crc = tkrzw::HashChecksum6Continuous("世界", 6, true, crc);
  EXPECT_EQ(0x04, crc);
  EXPECT_EQ(0x0F, tkrzw::HashChecksum6Pair("Hello", 5, " World", 6));
  EXPECT_EQ(0x04, tkrzw::HashChecksum6Pair("こんにちは", 15, "世界", 6));
  for (int32_t i = 0; i < 256; i++) {
    const std::string key = MakeCyclicString(i);
    EXPECT_LT(tkrzw::HashChecksum6(key), 61);
  }
}

TEST(LibCommonTest, HashChecksum8) {
  EXPECT_EQ(0x1E, tkrzw::HashChecksum8("hello"));
  EXPECT_EQ(0x30, tkrzw::HashChecksum8("Hello World"));
  EXPECT_EQ(0x96, tkrzw::HashChecksum8("こんにちは世界"));
  EXPECT_EQ(0x0A, tkrzw::HashChecksum8(MakeCyclicString()));
  EXPECT_EQ(0x36, tkrzw::HashChecksum8(MakeCyclicString(100000)));
  uint32_t crc = tkrzw::HashChecksum8Continuous("Hello", 5, false);
  crc = tkrzw::HashChecksum8Continuous(" ", 1, false, crc);
  crc = tkrzw::HashChecksum8Continuous("World", 5, true, crc);
  EXPECT_EQ(0x30, crc);
  crc = tkrzw::HashChecksum8Continuous("こんにちは", 15, false);
  crc = tkrzw::HashChecksum8Continuous("世界", 6, true, crc);
  EXPECT_EQ(0x96, crc);
  EXPECT_EQ(0x30, tkrzw::HashChecksum8Pair("Hello", 5, " World", 6));
  EXPECT_EQ(0x96, tkrzw::HashChecksum8Pair("こんにちは", 15, "世界", 6));
  for (int32_t i = 0; i < 256; i++) {
    const std::string key = MakeCyclicString(i);
    EXPECT_LT(tkrzw::HashChecksum8(key), 251);
  }
}

TEST(LibCommonTest, HashAdler6) {
  EXPECT_EQ(0x29, tkrzw::HashAdler6("hello"));
  EXPECT_EQ(0x13, tkrzw::HashAdler6("Hello World"));
  EXPECT_EQ(0x14, tkrzw::HashAdler6("こんにちは世界"));
  EXPECT_EQ(0x00, tkrzw::HashAdler6(MakeCyclicString()));
  EXPECT_EQ(0x34, tkrzw::HashAdler6(MakeCyclicString(100000)));
  uint32_t crc = tkrzw::HashAdler6Continuous("Hello", 5, false);
  crc = tkrzw::HashAdler6Continuous(" ", 1, false, crc);
  crc = tkrzw::HashAdler6Continuous("World", 5, true, crc);
  EXPECT_EQ(0x13, crc);
  crc = tkrzw::HashAdler6Continuous("こんにちは", 15, false);
  crc = tkrzw::HashAdler6Continuous("世界", 6, true, crc);
  EXPECT_EQ(0x14, crc);
  for (int32_t i = 0; i < 256; i++) {
    const std::string key = MakeCyclicString(i);
    EXPECT_LT(tkrzw::HashAdler6(key), 64);
  }
}

TEST(LibCommonTest, HashAdler8) {
  EXPECT_EQ(0x70, tkrzw::HashAdler8("hello"));
  EXPECT_EQ(0x60, tkrzw::HashAdler8("Hello World"));
  EXPECT_EQ(0x2C, tkrzw::HashAdler8("こんにちは世界"));
  EXPECT_EQ(0xCB, tkrzw::HashAdler8(MakeCyclicString()));
  EXPECT_EQ(0x17, tkrzw::HashAdler8(MakeCyclicString(100000)));
  uint32_t crc = tkrzw::HashAdler8Continuous("Hello", 5, false);
  crc = tkrzw::HashAdler8Continuous(" ", 1, false, crc);
  crc = tkrzw::HashAdler8Continuous("World", 5, true, crc);
  EXPECT_EQ(0x60, crc);
  crc = tkrzw::HashAdler8Continuous("こんにちは", 15, false);
  crc = tkrzw::HashAdler8Continuous("世界", 6, true, crc);
  EXPECT_EQ(0x2C, crc);
  for (int32_t i = 0; i < 256; i++) {
    const std::string key = MakeCyclicString(i);
    EXPECT_LT(tkrzw::HashAdler8(key), 256);
  }
}

TEST(LibCommonTest, HashAdler16) {
  EXPECT_EQ(0x4A1F, tkrzw::HashAdler16("hello"));
  EXPECT_EQ(0x8331, tkrzw::HashAdler16("Hello World"));
  EXPECT_EQ(0x9B97, tkrzw::HashAdler16("こんにちは世界"));
  EXPECT_EQ(0x190B, tkrzw::HashAdler16(MakeCyclicString()));
  EXPECT_EQ(0xC337, tkrzw::HashAdler16(MakeCyclicString(100000)));
  uint32_t crc = tkrzw::HashAdler16Continuous("Hello", 5, false);
  crc = tkrzw::HashAdler16Continuous(" ", 1, false, crc);
  crc = tkrzw::HashAdler16Continuous("World", 5, true, crc);
  EXPECT_EQ(0x8331, crc);
  crc = tkrzw::HashAdler16Continuous("こんにちは", 15, false);
  crc = tkrzw::HashAdler16Continuous("世界", 6, true, crc);
  EXPECT_EQ(0x9B97, crc);
  for (int32_t i = 0; i < 256; i++) {
    const std::string key = MakeCyclicString(i);
    EXPECT_LT(tkrzw::HashAdler16(key), 65536);
  }
}

TEST(LibCommonTest, HashAdler32) {
  EXPECT_EQ(0x062C0215U, tkrzw::HashAdler32("hello"));
  EXPECT_EQ(0x180B041DU, tkrzw::HashAdler32("Hello World"));
  EXPECT_EQ(0x9D7B0E51U, tkrzw::HashAdler32("こんにちは世界"));
  EXPECT_EQ(0xADF67F81U, tkrzw::HashAdler32(MakeCyclicString()));
  EXPECT_EQ(0x61657A0FU, tkrzw::HashAdler32(MakeCyclicString(100000)));
  uint32_t crc = tkrzw::HashAdler32Continuous("Hello", 5, false);
  crc = tkrzw::HashAdler32Continuous(" ", 1, false, crc);
  crc = tkrzw::HashAdler32Continuous("World", 5, true, crc);
  EXPECT_EQ(0x180B041DU, crc);
  crc = tkrzw::HashAdler32Continuous("こんにちは", 15, false);
  crc = tkrzw::HashAdler32Continuous("世界", 6, true, crc);
  EXPECT_EQ(0x9D7B0E51U, crc);
}

TEST(LibCommonTest, HashCRC4) {
  EXPECT_EQ(0xD, tkrzw::HashCRC4("hello"));
  EXPECT_EQ(0x9, tkrzw::HashCRC4("Hello World"));
  EXPECT_EQ(0xE, tkrzw::HashCRC4("こんにちは世界"));
  EXPECT_EQ(0x5, tkrzw::HashCRC4(MakeCyclicString()));
  EXPECT_EQ(0x3, tkrzw::HashCRC4(MakeCyclicString(100000)));
  uint32_t crc = tkrzw::HashCRC4Continuous("Hello", 5, false);
  crc = tkrzw::HashCRC4Continuous(" ", 1, false, crc);
  crc = tkrzw::HashCRC4Continuous("World", 5, true, crc);
  EXPECT_EQ(0x9, crc);
  crc = tkrzw::HashCRC4Continuous("こんにちは", 15, false);
  crc = tkrzw::HashCRC4Continuous("世界", 6, true, crc);
  EXPECT_EQ(0xE, crc);
  for (int32_t i = 0; i < 256; i++) {
    const std::string key = MakeCyclicString(i);
    EXPECT_LT(tkrzw::HashCRC4(key), 16);
  }
}

TEST(LibCommonTest, HashCRC8) {
  EXPECT_EQ(0x92, tkrzw::HashCRC8("hello"));
  EXPECT_EQ(0x25, tkrzw::HashCRC8("Hello World"));
  EXPECT_EQ(0xB7, tkrzw::HashCRC8("こんにちは世界"));
  EXPECT_EQ(0x14, tkrzw::HashCRC8(MakeCyclicString()));
  EXPECT_EQ(0xB8, tkrzw::HashCRC8(MakeCyclicString(100000)));
  uint32_t crc = tkrzw::HashCRC8Continuous("Hello", 5, false);
  crc = tkrzw::HashCRC8Continuous(" ", 1, false, crc);
  crc = tkrzw::HashCRC8Continuous("World", 5, true, crc);
  EXPECT_EQ(0x25, crc);
  crc = tkrzw::HashCRC8Continuous("こんにちは", 15, false);
  crc = tkrzw::HashCRC8Continuous("世界", 6, true, crc);
  EXPECT_EQ(0xB7, crc);
  for (int32_t i = 0; i < 256; i++) {
    const std::string key = MakeCyclicString(i);
    EXPECT_LT(tkrzw::HashCRC8(key), 256);
  }
}

TEST(LibCommonTest, HashCRC16) {
  EXPECT_EQ(0xC362, tkrzw::HashCRC16("hello"));
  EXPECT_EQ(0xC362, tkrzw::HashCRC16(std::string_view("12hello" + 2, 5)));
  EXPECT_EQ(0x992A, tkrzw::HashCRC16("Hello World"));
  EXPECT_EQ(0xF802, tkrzw::HashCRC16("こんにちは世界"));
  EXPECT_EQ(0x7E55, tkrzw::HashCRC16(MakeCyclicString()));
  EXPECT_EQ(0x96E2, tkrzw::HashCRC16(MakeCyclicString(100000)));
  uint32_t crc = tkrzw::HashCRC16Continuous("Hello", 5, false);
  crc = tkrzw::HashCRC16Continuous(" ", 1, false, crc);
  crc = tkrzw::HashCRC16Continuous("World", 5, true, crc);
  EXPECT_EQ(0x992A, crc);
  crc = tkrzw::HashCRC16Continuous("こんにちは", 15, false);
  crc = tkrzw::HashCRC16Continuous("世界", 6, true, crc);
  EXPECT_EQ(0xF802, crc);
  for (int32_t i = 0; i < 256; i++) {
    const std::string key = MakeCyclicString(i);
    EXPECT_LT(tkrzw::HashCRC16(key), 65536);
  }
}

TEST(LibCommonTest, HashCRC32) {
  EXPECT_EQ(0x3610A686U, tkrzw::HashCRC32("hello"));
  EXPECT_EQ(0x3610A686U, tkrzw::HashCRC32(std::string_view("12hello" + 2, 5)));
  EXPECT_EQ(0x4A17B156U, tkrzw::HashCRC32("Hello World"));
  EXPECT_EQ(0x75197186U, tkrzw::HashCRC32("こんにちは世界"));
  EXPECT_EQ(0x29058C73U, tkrzw::HashCRC32(MakeCyclicString()));
  EXPECT_EQ(0xAACF4FC9U, tkrzw::HashCRC32(MakeCyclicString(100000)));
  uint32_t crc = tkrzw::HashCRC32Continuous("Hello", 5, false);
  crc = tkrzw::HashCRC32Continuous(" ", 1, false, crc);
  crc = tkrzw::HashCRC32Continuous("World", 5, true, crc);
  EXPECT_EQ(0x4A17B156U, crc);
  crc = tkrzw::HashCRC32Continuous("こんにちは", 15, false);
  crc = tkrzw::HashCRC32Continuous("世界", 6, true, crc);
  EXPECT_EQ(0x75197186U, crc);
}

// END OF FILE
