/*************************************************************************************************
 * Tests for tkrzw_compress.h
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

#include "tkrzw_compress.h"
#include "tkrzw_lib_common.h"

using namespace testing;

// Main routine
int main(int argc, char** argv) {
  InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

class CompressorTest : public Test {
 protected:
  void BasicTest(tkrzw::Compressor* compressor);
};

void CompressorTest::BasicTest(tkrzw::Compressor* compressor) {
  if (!compressor->IsSupported()) {
    size_t size = 0;
    EXPECT_EQ(nullptr, compressor->Compress("", 0, &size));
    EXPECT_EQ(nullptr, compressor->Decompress("", 0, &size));
    return;
  }
  std::vector<std::string> inputs = {"", "a", "abc", "aaaaa", "a\0b\0c\xFF"};
  for (int32_t size = 16; size <= 262144; size *= 2) {
    inputs.emplace_back(std::string(size, 'z'));
    std::string cycle_str;
    for (int32_t i = 0; i < size; i++) {
      cycle_str.append(1, 'a' + i % 26);
    }
    inputs.emplace_back(cycle_str);
    std::string random_str;
    std::mt19937 mt(size);
    std::uniform_int_distribution<int32_t> dist(0, tkrzw::UINT8MAX);
    for (int32_t i = 0; i < size; i++) {
      random_str.append(1, dist(mt));
    }
    inputs.emplace_back(random_str);
  }
  for (const auto& input : inputs) {
    size_t comp_size = -1;
    char* comp_data = compressor->Compress(input.data(), input.size(), &comp_size);
    EXPECT_NE(nullptr, comp_data);
    EXPECT_GE(comp_size, 0);
    size_t decomp_size = 0;
    char* decomp_data = compressor->Decompress(comp_data, comp_size, &decomp_size);
    EXPECT_NE(nullptr, decomp_data);
    EXPECT_EQ(input, std::string_view(decomp_data, decomp_size));
    tkrzw::xfree(decomp_data);
    tkrzw::xfree(comp_data);
  }
  auto copy_compressor = compressor->MakeCompressor();
  EXPECT_EQ(compressor->GetType(), copy_compressor->GetType());
}

TEST_F(CompressorTest, DummyCompressorDefault) {
  tkrzw::DummyCompressor compressor;
  BasicTest(&compressor);
}

TEST_F(CompressorTest, DummyCompressorChecksum) {
  tkrzw::DummyCompressor compressor(true);
  BasicTest(&compressor);
}

TEST_F(CompressorTest, ZlibCompressoDefault) {
  tkrzw::ZLibCompressor compressor;
  BasicTest(&compressor);
}

TEST_F(CompressorTest, ZlibCompressorNoop) {
  tkrzw::ZLibCompressor compressor(0, tkrzw::ZLibCompressor::METADATA_NONE);
  BasicTest(&compressor);
}

TEST_F(CompressorTest, ZlibCompressorFast) {
  tkrzw::ZLibCompressor compressor(1, tkrzw::ZLibCompressor::METADATA_ADLER32);
  BasicTest(&compressor);
}

TEST_F(CompressorTest, ZlibCompressorSlow) {
  tkrzw::ZLibCompressor compressor(9, tkrzw::ZLibCompressor::METADATA_CRC32);
  BasicTest(&compressor);
}

TEST_F(CompressorTest, ZStdCompressorDefault) {
  tkrzw::ZStdCompressor compressor;
  BasicTest(&compressor);
}

TEST_F(CompressorTest, ZStdCompressorFast) {
  tkrzw::ZStdCompressor compressor(0);
  BasicTest(&compressor);
}

TEST_F(CompressorTest, ZStdCompressorSlow) {
  tkrzw::ZStdCompressor compressor(10);
  BasicTest(&compressor);
}

TEST_F(CompressorTest, LZ4CompressorDefault) {
  tkrzw::LZ4Compressor compressor;
  BasicTest(&compressor);
}

TEST_F(CompressorTest, LZ4CompressorFast) {
  tkrzw::LZ4Compressor compressor(10);
  BasicTest(&compressor);
}

TEST_F(CompressorTest, LZMACompressorDefault) {
  tkrzw::LZMACompressor compressor(0, tkrzw::LZMACompressor::METADATA_NONE);
  BasicTest(&compressor);
}

TEST_F(CompressorTest, LZMACompressorFast) {
  tkrzw::LZMACompressor compressor(1, tkrzw::LZMACompressor::METADATA_CRC32);
  BasicTest(&compressor);
}

TEST_F(CompressorTest, LZMACompressorSlow) {
  tkrzw::LZMACompressor compressor(9, tkrzw::LZMACompressor::METADATA_SHA256);
  BasicTest(&compressor);
}

// END OF FILE
