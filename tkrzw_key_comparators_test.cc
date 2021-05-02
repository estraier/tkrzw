/*************************************************************************************************
 * Tests for tkrzw_key_comparators.h
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

#include "tkrzw_key_comparators.h"
#include "tkrzw_lib_common.h"
#include "tkrzw_str_util.h"

using namespace testing;

// Main routine
int main(int argc, char** argv) {
  InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

TEST(KeyComparatorTest, LexicalKeyComparator) {
  EXPECT_EQ(0, tkrzw::LexicalKeyComparator("", ""));
  EXPECT_LT(tkrzw::LexicalKeyComparator("", "a"), 0);
  EXPECT_GT(tkrzw::LexicalKeyComparator("a", ""), 0);
  EXPECT_EQ(0, tkrzw::LexicalKeyComparator("aa", "aa"));
  EXPECT_LT(tkrzw::LexicalKeyComparator("a", "aa"), 0);
  EXPECT_GT(tkrzw::LexicalKeyComparator("aa", "a"), 0);
  EXPECT_LT(tkrzw::LexicalKeyComparator("aa", "ab"), 0);
  EXPECT_GT(tkrzw::LexicalKeyComparator("ab", "aa"), 0);
  EXPECT_LT(tkrzw::LexicalKeyComparator("aaa", "あ"), 0);
  EXPECT_GT(tkrzw::LexicalKeyComparator("あ", "aaa"), 0);
  EXPECT_LT(tkrzw::LexicalKeyComparator(
      tkrzw::IntToStrBigEndian(0), tkrzw::IntToStrBigEndian(0xFFFFFFFF)), 0);
  EXPECT_GT(tkrzw::LexicalKeyComparator(
      tkrzw::IntToStrBigEndian(0xFFFFFFFF), tkrzw::IntToStrBigEndian(0)), 0);
}

TEST(KeyComparatorTest, LexicalCaseKeyComparator) {
  EXPECT_EQ(0, tkrzw::LexicalCaseKeyComparator("", ""));
  EXPECT_LT(tkrzw::LexicalCaseKeyComparator("", "a"), 0);
  EXPECT_GT(tkrzw::LexicalCaseKeyComparator("a", ""), 0);
  EXPECT_EQ(0, tkrzw::LexicalCaseKeyComparator("aA", "Aa"));
  EXPECT_LT(tkrzw::LexicalCaseKeyComparator("a", "Aa"), 0);
  EXPECT_GT(tkrzw::LexicalCaseKeyComparator("Aa", "a"), 0);
  EXPECT_LT(tkrzw::LexicalCaseKeyComparator("aa", "Ab"), 0);
  EXPECT_GT(tkrzw::LexicalCaseKeyComparator("Ab", "aa"), 0);
  EXPECT_LT(tkrzw::LexicalCaseKeyComparator("aAa", "あ"), 0);
  EXPECT_GT(tkrzw::LexicalCaseKeyComparator("あ", "aAa"), 0);
}

TEST(KeyComparatorTest, DecimalKeyComparator) {
  EXPECT_EQ(0, tkrzw::DecimalKeyComparator("", ""));
  EXPECT_EQ(tkrzw::DecimalKeyComparator("a", "b"), 0);
  EXPECT_GT(tkrzw::DecimalKeyComparator("1", ""), 0);
  EXPECT_LT(tkrzw::DecimalKeyComparator("", "1"), 0);
  EXPECT_EQ(0, tkrzw::DecimalKeyComparator("1", "1a"));
  EXPECT_LT(tkrzw::DecimalKeyComparator("-1", "1"), 0);
  EXPECT_GT(tkrzw::DecimalKeyComparator("100", "99"), 0);
  EXPECT_EQ(0, tkrzw::DecimalKeyComparator("100", "100"));
}

TEST(KeyComparatorTest, HexadecimalKeyComparator) {
  EXPECT_EQ(0, tkrzw::HexadecimalKeyComparator("", ""));
  EXPECT_LT(tkrzw::HexadecimalKeyComparator("a", "b"), 0);
  EXPECT_GT(tkrzw::HexadecimalKeyComparator("F", "0"), 0);
  EXPECT_GT(tkrzw::HexadecimalKeyComparator("1", ""), 0);
  EXPECT_LT(tkrzw::HexadecimalKeyComparator("", "1"), 0);
  EXPECT_LT(tkrzw::HexadecimalKeyComparator("1", "1a"), 0);
  EXPECT_GT(tkrzw::HexadecimalKeyComparator("FF", "A"), 0);
  EXPECT_GT(tkrzw::HexadecimalKeyComparator("0xFF", "0xAA"), 0);
  EXPECT_EQ(0, tkrzw::HexadecimalKeyComparator("8E", "8e"));
}

TEST(KeyComparatorTest, RealNumberKeyComparator) {
  EXPECT_EQ(0, tkrzw::RealNumberKeyComparator("", ""));
  EXPECT_LT(tkrzw::RealNumberKeyComparator("1.0", "1.1"), 0);
  EXPECT_GT(tkrzw::RealNumberKeyComparator("102.1", "30.5"), 0);
  EXPECT_GT(tkrzw::RealNumberKeyComparator("0.5", ""), 0);
  EXPECT_LT(tkrzw::RealNumberKeyComparator("", "0.5"), 0);
  EXPECT_LT(tkrzw::RealNumberKeyComparator("1.5", "1.50001"), 0);
  EXPECT_GT(tkrzw::RealNumberKeyComparator("-2.5", "-3.8"), 0);
  EXPECT_GT(tkrzw::RealNumberKeyComparator("99", "-100"), 0);
  EXPECT_EQ(0, tkrzw::RealNumberKeyComparator("123", "123.0"));
}

TEST(KeyComparatorTest, PairLexicalKeyComparator) {
  EXPECT_EQ(0, tkrzw::PairLexicalKeyComparator(
      tkrzw::SerializeStrPair("", ""), tkrzw::SerializeStrPair("", "")));
  EXPECT_EQ(0, tkrzw::PairLexicalKeyComparator(
      tkrzw::SerializeStrPair("a", "a"), tkrzw::SerializeStrPair("a", "a")));
  EXPECT_LT(tkrzw::PairLexicalKeyComparator(
      tkrzw::SerializeStrPair("a", ""), tkrzw::SerializeStrPair("b", "")), 0);
  EXPECT_LT(tkrzw::PairLexicalKeyComparator(
      tkrzw::SerializeStrPair("a", "b"), tkrzw::SerializeStrPair("b", "a")), 0);
  EXPECT_LT(tkrzw::PairLexicalKeyComparator(
      tkrzw::SerializeStrPair("a", "a"), tkrzw::SerializeStrPair("a", "b")), 0);
  EXPECT_GT(tkrzw::PairLexicalKeyComparator(
      tkrzw::SerializeStrPair("ab", "a"), tkrzw::SerializeStrPair("a", "b")), 0);
  EXPECT_GT(tkrzw::PairLexicalKeyComparator(
      tkrzw::SerializeStrPair("あ", "a"), tkrzw::SerializeStrPair("a", "b")), 0);
}

TEST(KeyComparatorTest, PairLexicalCaseKeyComparator) {
  EXPECT_EQ(0, tkrzw::PairLexicalCaseKeyComparator(
      tkrzw::SerializeStrPair("", ""), tkrzw::SerializeStrPair("", "")));
  EXPECT_EQ(0, tkrzw::PairLexicalCaseKeyComparator(
      tkrzw::SerializeStrPair("a", "a"), tkrzw::SerializeStrPair("A", "a")));
  EXPECT_LT(tkrzw::PairLexicalCaseKeyComparator(
      tkrzw::SerializeStrPair("a", ""), tkrzw::SerializeStrPair("B", "")), 0);
  EXPECT_LT(tkrzw::PairLexicalCaseKeyComparator(
      tkrzw::SerializeStrPair("a", "b"), tkrzw::SerializeStrPair("B", "a")), 0);
  EXPECT_LT(tkrzw::PairLexicalCaseKeyComparator(
      tkrzw::SerializeStrPair("a", "a"), tkrzw::SerializeStrPair("B", "b")), 0);
  EXPECT_GT(tkrzw::PairLexicalCaseKeyComparator(
      tkrzw::SerializeStrPair("Ab", "a"), tkrzw::SerializeStrPair("a", "b")), 0);
  EXPECT_GT(tkrzw::PairLexicalCaseKeyComparator(
      tkrzw::SerializeStrPair("あ", "a"), tkrzw::SerializeStrPair("a", "b")), 0);
}

TEST(KeyComparatorTest, PairDecimalKeyComparator) {
  EXPECT_EQ(0, tkrzw::PairDecimalKeyComparator(
      tkrzw::SerializeStrPair("", ""), tkrzw::SerializeStrPair("", "")));
  EXPECT_EQ(0, tkrzw::PairDecimalKeyComparator(
      tkrzw::SerializeStrPair("1", "a"), tkrzw::SerializeStrPair(" 1 ", "a")));
  EXPECT_LT(tkrzw::PairDecimalKeyComparator(
      tkrzw::SerializeStrPair("1", ""), tkrzw::SerializeStrPair("2", "")), 0);
  EXPECT_LT(tkrzw::PairDecimalKeyComparator(
      tkrzw::SerializeStrPair("2", "b"), tkrzw::SerializeStrPair("100", "a")), 0);
  EXPECT_LT(tkrzw::PairDecimalKeyComparator(
      tkrzw::SerializeStrPair("-1", "a"), tkrzw::SerializeStrPair("1", "b")), 0);
  EXPECT_GT(tkrzw::PairDecimalKeyComparator(
      tkrzw::SerializeStrPair("101", "a"), tkrzw::SerializeStrPair("100", "b")), 0);
  EXPECT_GT(tkrzw::PairDecimalKeyComparator(
      tkrzw::SerializeStrPair("100", "b"), tkrzw::SerializeStrPair("\n100\n", "a")), 0);
}

TEST(KeyComparatorTest, PairHexadecimalKeyComparator) {
  EXPECT_EQ(0, tkrzw::PairHexadecimalKeyComparator(
      tkrzw::SerializeStrPair("", ""), tkrzw::SerializeStrPair("", "")));
  EXPECT_EQ(0, tkrzw::PairHexadecimalKeyComparator(
      tkrzw::SerializeStrPair("1", "a"), tkrzw::SerializeStrPair(" 1 ", "a")));
  EXPECT_LT(tkrzw::PairHexadecimalKeyComparator(
      tkrzw::SerializeStrPair("aa", ""), tkrzw::SerializeStrPair("8bf", "")), 0);
  EXPECT_LT(tkrzw::PairHexadecimalKeyComparator(
      tkrzw::SerializeStrPair("CC", "b"), tkrzw::SerializeStrPair("0xDD", "a")), 0);
  EXPECT_LT(tkrzw::PairHexadecimalKeyComparator(
      tkrzw::SerializeStrPair("0", "a"), tkrzw::SerializeStrPair("f", "b")), 0);
  EXPECT_GT(tkrzw::PairHexadecimalKeyComparator(
      tkrzw::SerializeStrPair("abc", "a"), tkrzw::SerializeStrPair("abb", "b")), 0);
  EXPECT_GT(tkrzw::PairHexadecimalKeyComparator(
      tkrzw::SerializeStrPair("ff", "b"), tkrzw::SerializeStrPair("FF", "a")), 0);
}

TEST(KeyComparatorTest, PairRealNumberKeyComparator) {
  EXPECT_EQ(0, tkrzw::PairRealNumberKeyComparator(
      tkrzw::SerializeStrPair("", ""), tkrzw::SerializeStrPair("", "")));
  EXPECT_EQ(0, tkrzw::PairRealNumberKeyComparator(
      tkrzw::SerializeStrPair("1", "a"), tkrzw::SerializeStrPair(" 1 ", "a")));
  EXPECT_LT(tkrzw::PairRealNumberKeyComparator(
      tkrzw::SerializeStrPair("12.3", ""), tkrzw::SerializeStrPair("12.4", "")), 0);
  EXPECT_LT(tkrzw::PairRealNumberKeyComparator(
      tkrzw::SerializeStrPair("-10", "b"), tkrzw::SerializeStrPair("2", "a")), 0);
  EXPECT_LT(tkrzw::PairRealNumberKeyComparator(
      tkrzw::SerializeStrPair("0", "a"), tkrzw::SerializeStrPair("100", "b")), 0);
  EXPECT_GT(tkrzw::PairRealNumberKeyComparator(
      tkrzw::SerializeStrPair("52.3", "a"), tkrzw::SerializeStrPair("-52.3", "b")), 0);
  EXPECT_GT(tkrzw::PairRealNumberKeyComparator(
      tkrzw::SerializeStrPair("100.0", "b"), tkrzw::SerializeStrPair("100", "a")), 0);
}

// END OF FILE
