/*************************************************************************************************
 * Tests for tkrzw_str_util.h
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

using namespace testing;

// Main routine
int main(int argc, char** argv) {
  InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

// Make a text of random characters.
std::string MakeRandomCharacterText(int32_t length, uint8_t first_char, uint8_t last_char) {
  static std::mt19937 mt(19780211);
  std::uniform_int_distribution<int32_t> dist(0, tkrzw::INT32MAX);
  std::string text;
  const int32_t range = last_char - first_char + 1;
  text.resize(length);
  for (int32_t i = 0; i < length; i++) {
    text[i] = dist(mt) % range + first_char;
  }
  return text;
}

TEST(StrUtilTest, StrToInt) {
  EXPECT_EQ(0, tkrzw::StrToInt(""));
  EXPECT_EQ(0, tkrzw::StrToInt("a"));
  EXPECT_EQ(0, tkrzw::StrToInt("0"));
  EXPECT_EQ(1, tkrzw::StrToInt("1"));
  EXPECT_EQ(1, tkrzw::StrToInt("01"));
  EXPECT_EQ(1, tkrzw::StrToInt("+1"));
  EXPECT_EQ(-1, tkrzw::StrToInt("-1"));
  EXPECT_EQ(-10, tkrzw::StrToInt(" - 10 "));
  EXPECT_EQ(1230, tkrzw::StrToInt("1230"));
  EXPECT_EQ(1230, tkrzw::StrToInt("1230abc"));
  EXPECT_EQ(1230, tkrzw::StrToInt("1230.5"));
  EXPECT_EQ(-123, tkrzw::StrToInt("hoge", -123));
  EXPECT_EQ(999, tkrzw::StrToInt(std::string("999")));
  EXPECT_EQ(-123, tkrzw::StrToInt(std::string("hoge"), -123));
}

TEST(StrUtilTest, StrToIntMetric) {
  EXPECT_EQ(0, tkrzw::StrToIntMetric(""));
  EXPECT_EQ(0, tkrzw::StrToIntMetric("a"));
  EXPECT_EQ(0, tkrzw::StrToIntMetric("0"));
  EXPECT_EQ(1, tkrzw::StrToIntMetric("1"));
  EXPECT_EQ(1, tkrzw::StrToIntMetric("01"));
  EXPECT_EQ(1, tkrzw::StrToIntMetric("+1"));
  EXPECT_EQ(-1, tkrzw::StrToIntMetric("-1"));
  EXPECT_EQ(-10, tkrzw::StrToIntMetric("-10"));
  EXPECT_EQ(1230, tkrzw::StrToIntMetric("1230"));
  EXPECT_EQ(1000LL, tkrzw::StrToIntMetric("1k"));
  EXPECT_EQ(1LL << 10, tkrzw::StrToIntMetric("1ki"));
  EXPECT_EQ(1000000LL, tkrzw::StrToIntMetric("1m"));
  EXPECT_EQ(1LL << 20, tkrzw::StrToIntMetric("1mi"));
  EXPECT_EQ(1000000000LL, tkrzw::StrToIntMetric("1g"));
  EXPECT_EQ(1LL << 30, tkrzw::StrToIntMetric("1gi"));
  EXPECT_EQ(1000000000000LL, tkrzw::StrToIntMetric("1t"));
  EXPECT_EQ(1LL << 40, tkrzw::StrToIntMetric("1ti"));
  EXPECT_EQ(1000000000000000LL, tkrzw::StrToIntMetric("1p"));
  EXPECT_EQ(1LL << 50, tkrzw::StrToIntMetric("1pi"));
  EXPECT_EQ(1000000000000000000LL, tkrzw::StrToIntMetric("1e"));
  EXPECT_EQ(1LL << 60, tkrzw::StrToIntMetric("1ei"));
  EXPECT_EQ(500, tkrzw::StrToIntMetric("0.5k"));
  EXPECT_EQ(-250, tkrzw::StrToIntMetric("-.25k"));
  EXPECT_EQ(-123, tkrzw::StrToIntMetric("hoge", -123));
  EXPECT_EQ(999, tkrzw::StrToIntMetric(std::string("999")));
  EXPECT_EQ(-123, tkrzw::StrToIntMetric(std::string("hoge"), -123));
}

TEST(StrUtilTest, StrToIntOct) {
  EXPECT_EQ(0, tkrzw::StrToIntOct(""));
  EXPECT_EQ(0, tkrzw::StrToIntOct("0"));
  EXPECT_EQ(1, tkrzw::StrToIntOct("1"));
  EXPECT_EQ(7, tkrzw::StrToIntOct("7"));
  EXPECT_EQ(0, tkrzw::StrToIntOct("8"));
  EXPECT_EQ(8, tkrzw::StrToIntOct("10"));
  EXPECT_EQ(9, tkrzw::StrToIntOct("11"));
  EXPECT_EQ(64, tkrzw::StrToIntOct("100"));
  EXPECT_EQ(65, tkrzw::StrToIntOct("101"));
  EXPECT_EQ(73, tkrzw::StrToIntOct("111"));
}

TEST(StrUtilTest, StrToIntHex) {
  EXPECT_EQ(0x0, tkrzw::StrToIntHex(""));
  EXPECT_EQ(0x0, tkrzw::StrToIntHex("0"));
  EXPECT_EQ(0x1, tkrzw::StrToIntHex("1"));
  EXPECT_EQ(0x9, tkrzw::StrToIntHex("9"));
  EXPECT_EQ(0xA, tkrzw::StrToIntHex("a"));
  EXPECT_EQ(0xA0, tkrzw::StrToIntHex(" A0 "));
  EXPECT_EQ(0xABCDEFAB1230, tkrzw::StrToIntHex("ABCDEFab1230"));
  EXPECT_EQ(0xFF, tkrzw::StrToIntHex("hoge", 0xFF));
  EXPECT_EQ(0xEEE, tkrzw::StrToIntHex(std::string("EEE")));
  EXPECT_EQ(0xFF, tkrzw::StrToIntHex(std::string("hoge"), 0xFF));
}

TEST(StrUtilTest, StrToIntBigEndian) {
  EXPECT_EQ(0x0, tkrzw::StrToIntBigEndian(std::string_view("", 0)));
  EXPECT_EQ(0x00, tkrzw::StrToIntBigEndian(std::string_view("\x00", 1)));
  EXPECT_EQ(0x12, tkrzw::StrToIntBigEndian(std::string_view("\x12", 1)));
  EXPECT_EQ(0x12AB, tkrzw::StrToIntBigEndian(std::string_view("\x12\xAB", 2)));
  EXPECT_EQ(0x1234AB, tkrzw::StrToIntBigEndian(std::string_view("\x12\x34\xAB", 3)));
  EXPECT_EQ(0x1234ABCD, tkrzw::StrToIntBigEndian(std::string_view("\x12\x34\xAB\xCD", 4)));
  EXPECT_EQ(0xABCD1234ABCD, tkrzw::StrToIntBigEndian(
      std::string_view("\xAB\xCD\x12\x34\xAB\xCD", 6)));
  EXPECT_EQ(0xABCD1234ABCD1234, tkrzw::StrToIntBigEndian(
      std::string_view("\xAB\xCD\x12\x34\xAB\xCD\x12\x34", 8)));
  EXPECT_EQ(0xFFFFFFFFFFFFFFFF, tkrzw::StrToIntBigEndian(
      std::string_view("\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF", 8)));
}

TEST(StrUtilTest, StrToDouble) {
  EXPECT_DOUBLE_EQ(0.0, tkrzw::StrToDouble(""));
  EXPECT_DOUBLE_EQ(1.0, tkrzw::StrToDouble("1"));
  EXPECT_DOUBLE_EQ(23.4, tkrzw::StrToDouble("23.4"));
  EXPECT_DOUBLE_EQ(-56.7, tkrzw::StrToDouble("-56.7"));
  EXPECT_DOUBLE_EQ(-0.125, tkrzw::StrToDouble(" - .125 "));
  EXPECT_TRUE(std::isinf(tkrzw::StrToDouble("inf")));
  EXPECT_TRUE(std::isnan(tkrzw::StrToDouble("nan")));
  EXPECT_DOUBLE_EQ(500.0, tkrzw::StrToDouble("5e2"));
  EXPECT_DOUBLE_EQ(0.05, tkrzw::StrToDouble("5e-2"));
  EXPECT_DOUBLE_EQ(-123.0, tkrzw::StrToDouble("hoge", -123.0));
  EXPECT_DOUBLE_EQ(1.25, tkrzw::StrToDouble(std::string("1.25")));
  EXPECT_DOUBLE_EQ(-123.0, tkrzw::StrToDouble(std::string("hoge"), -123.0));
}

TEST(StrUtilTest, StrToBool) {
  EXPECT_TRUE(tkrzw::StrToBool("", true));
  EXPECT_TRUE(tkrzw::StrToBool("abc", true));
  EXPECT_FALSE(tkrzw::StrToBool("", false));
  EXPECT_FALSE(tkrzw::StrToBool("abc", false));
  EXPECT_TRUE(tkrzw::StrToBool("true", false));
  EXPECT_TRUE(tkrzw::StrToBool(" TRUE ", false));
  EXPECT_TRUE(tkrzw::StrToBool("t", false));
  EXPECT_TRUE(tkrzw::StrToBool("yes", false));
  EXPECT_TRUE(tkrzw::StrToBool("y", false));
  EXPECT_TRUE(tkrzw::StrToBool(" 1 ", false));
  EXPECT_FALSE(tkrzw::StrToBool("false", true));
  EXPECT_FALSE(tkrzw::StrToBool(" FALSE ", true));
  EXPECT_FALSE(tkrzw::StrToBool("f", true));
  EXPECT_FALSE(tkrzw::StrToBool("no", true));
  EXPECT_FALSE(tkrzw::StrToBool("n", true));
  EXPECT_FALSE(tkrzw::StrToBool(" 0 ", true));
}

TEST(StrUtilTest, StrToIntOrBool) {
  EXPECT_EQ(-1, tkrzw::StrToIntOrBool("", -1));
  EXPECT_EQ(1, tkrzw::StrToIntOrBool("true", -1));
  EXPECT_EQ(1, tkrzw::StrToIntOrBool(" yes ", -1));
  EXPECT_EQ(0, tkrzw::StrToIntOrBool("false", -1));
  EXPECT_EQ(0, tkrzw::StrToIntOrBool("NO", -1));
  EXPECT_EQ(100, tkrzw::StrToIntOrBool("100", -1));
  EXPECT_EQ(-99, tkrzw::StrToIntOrBool("-99", -1));
  EXPECT_EQ(0, tkrzw::StrToIntOrBool("0000", -1));
}

TEST(StrUtilTest, SPrintF) {
  EXPECT_EQ("", tkrzw::SPrintF(""));
  EXPECT_EQ("a", tkrzw::SPrintF("a"));
  EXPECT_EQ("123", tkrzw::SPrintF("%d", 123));
  EXPECT_EQ("-123", tkrzw::SPrintF("%d", -123));
  EXPECT_EQ("-  123-", tkrzw::SPrintF("-%5d-", 123));
  EXPECT_EQ("-123  -", tkrzw::SPrintF("-%-5d-", 123));
  EXPECT_EQ("a-123-b", tkrzw::SPrintF("a-%u-b", 123U));
  EXPECT_EQ("a-123-b", tkrzw::SPrintF("a-%zu-b", static_cast<size_t>(123U)));
  EXPECT_EQ("a-123-b", tkrzw::SPrintF("a-%ld-b", 123L));
  EXPECT_EQ("a-123-b", tkrzw::SPrintF("a-%lld-b", 123LL));
  EXPECT_EQ("a-777-b", tkrzw::SPrintF("a-%llo-b", 0777));
  EXPECT_EQ("a-fff-b", tkrzw::SPrintF("a-%llx-b", 0xFFF));
  EXPECT_EQ("a-FFF-b", tkrzw::SPrintF("a-%llX-b", 0xFFF));
  EXPECT_EQ("a-0123-b", tkrzw::SPrintF("a-%04d-b", 123));
  EXPECT_EQ("-9223372036854775808",
            tkrzw::SPrintF("%lld", static_cast<long long>(tkrzw::INT64MIN)));
  EXPECT_EQ("ffffffffffffffff",
            tkrzw::SPrintF("%llx", static_cast<unsigned long long>(tkrzw::UINT64MAX)));
  EXPECT_EQ("FFFFFFFFFFFFFFFF",
            tkrzw::SPrintF("%llX", static_cast<unsigned long long>(tkrzw::UINT64MAX)));
  EXPECT_EQ("a-123.4560-b", tkrzw::SPrintF("a-%.4f-b", 123.456));
  EXPECT_EQ("-012.45600", tkrzw::SPrintF("%010.5f", -12.456));
  EXPECT_EQ("[-12.456   ]", tkrzw::SPrintF("[%-10.3f]", -12.456));
  EXPECT_EQ("0.000000", tkrzw::SPrintF("%f", tkrzw::DOUBLEMIN));
  EXPECT_GE(tkrzw::SPrintF("%f", tkrzw::DOUBLEMAX).size(), tkrzw::NUM_BUFFER_SIZE);
  const std::string& ldmaxstr = tkrzw::SPrintF("%Lf", std::numeric_limits<long double>::max());
  if (ldmaxstr != "inf") {
    EXPECT_GE(tkrzw::SPrintF("%Lf", std::numeric_limits<long double>::max()).size(),
              tkrzw::NUM_BUFFER_SIZE);
  }
  EXPECT_EQ("-abc-", tkrzw::SPrintF("-%s-", "abc"));
  EXPECT_EQ("[  abc]", tkrzw::SPrintF("[%5s]", "abc"));
  EXPECT_EQ("[abc  ]", tkrzw::SPrintF("[%-5s]", "abc"));
  EXPECT_FALSE(tkrzw::SPrintF("%p", nullptr).empty());
  EXPECT_EQ("\t[100%]\n", tkrzw::SPrintF("\t[100%%]\n"));
}

TEST(StrUtilTest, ToString) {
  EXPECT_EQ("-123", tkrzw::ToString(-123));
  EXPECT_EQ("123", tkrzw::ToString(123U));
  EXPECT_EQ("-123", tkrzw::ToString(-123L));
  EXPECT_EQ("123", tkrzw::ToString(123LU));
  EXPECT_EQ("-123", tkrzw::ToString(-123LL));
  EXPECT_EQ("123", tkrzw::ToString(123LLU));
  EXPECT_EQ("-123", tkrzw::ToString(int8_t(-123)));
  EXPECT_EQ("123", tkrzw::ToString(uint8_t(123)));
  EXPECT_EQ("-123", tkrzw::ToString(int16_t(-123)));
  EXPECT_EQ("123", tkrzw::ToString(uint16_t(123)));
  EXPECT_EQ("-123", tkrzw::ToString(int32_t(-123)));
  EXPECT_EQ("123", tkrzw::ToString(uint32_t(123)));
  EXPECT_EQ("-123", tkrzw::ToString(int64_t(-123)));
  EXPECT_EQ("123", tkrzw::ToString(uint64_t(123)));
  EXPECT_EQ("0.123", tkrzw::ToString(0.123));
  EXPECT_EQ("12.34", tkrzw::ToString(12.34));
  EXPECT_EQ("12.34", tkrzw::ToString(12.34F));
  EXPECT_EQ("12.34", tkrzw::ToString(12.34L));
  EXPECT_EQ("0", tkrzw::ToString(0.0));
  EXPECT_EQ("true", tkrzw::ToString(true));
  EXPECT_EQ("false", tkrzw::ToString(false));
  EXPECT_EQ("X", tkrzw::ToString('X'));
  EXPECT_EQ("abc", tkrzw::ToString("abc"));
  EXPECT_EQ("def", tkrzw::ToString(std::string_view("def")));
  EXPECT_EQ("ghi", tkrzw::ToString(std::string("ghi")));
}

TEST(StrUtilTest, IntToStrBigEndian) {
  EXPECT_EQ(std::string ("\x00\x00\x00\x00\x00\x00\x00\x00", 8),
            tkrzw::IntToStrBigEndian(0));
  EXPECT_EQ(std::string ("\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF", 8),
            tkrzw::IntToStrBigEndian(-1));
  EXPECT_EQ(std::string ("\xBE\xAF", 2),
            tkrzw::IntToStrBigEndian(0xDEADBEAF, 2));
  EXPECT_EQ(std::string ("\xDE\xAD\xBE\xAF", 4),
            tkrzw::IntToStrBigEndian(0xDEADBEAF, 4));
  EXPECT_EQ(std::string ("\x00\x00\xDE\xAD\xBE\xAF", 6),
            tkrzw::IntToStrBigEndian(0xDEADBEAF, 6));
  EXPECT_EQ(std::string ("\xAB\xCD\x12\x34\x56\x78\xAB\xCD", 8),
            tkrzw::IntToStrBigEndian(0xABCD12345678ABCD));
  EXPECT_EQ(std::string ("\x12", 1), tkrzw::IntToStrBigEndian(0x12, 1));
  EXPECT_EQ("", tkrzw::IntToStrBigEndian(0x12, 0));
}

TEST(StrUtilTest, StrJoin) {
  EXPECT_EQ("", tkrzw::StrJoin(std::vector<std::string>(), ","));
  EXPECT_EQ("a", tkrzw::StrJoin(std::vector<std::string>({"a"}), ","));
  EXPECT_EQ("a,b,c", tkrzw::StrJoin(std::vector<std::string>({"a", "b", "c"}), ","));
  EXPECT_EQ("a,b,c", tkrzw::StrJoin(std::vector<const char*>({"a", "b", "c"}), ","));
  EXPECT_EQ("a,b,c", tkrzw::StrJoin(std::vector<char>({'a', 'b', 'c'}), ","));
  EXPECT_EQ("12,34", tkrzw::StrJoin(std::vector<int32_t>({12, 34}), ","));
  EXPECT_EQ("12.34,56.78", tkrzw::StrJoin(std::vector<double>({12.34, 56.78}), ","));
  EXPECT_EQ("true,false", tkrzw::StrJoin(std::vector<bool>({true, false}), ","));
}

TEST(StrUtilTest, StrCat) {
  EXPECT_EQ("", tkrzw::StrCat());
  EXPECT_EQ("abc", tkrzw::StrCat("abc"));
  EXPECT_EQ("1234", tkrzw::StrCat(1, 2, 3, 4));
  EXPECT_EQ("abcd12345.6true", tkrzw::StrCat("a", "b", "c", 'd', 1, 2, 3, 4, 5.6, "true"));
}

TEST(StrUtilTest, StrSplit) {
  EXPECT_THAT(tkrzw::StrSplit("", ',', true), ElementsAre());
  EXPECT_THAT(tkrzw::StrSplit("", ','), ElementsAre(""));
  EXPECT_THAT(tkrzw::StrSplit(",", ',', true), ElementsAre());
  EXPECT_THAT(tkrzw::StrSplit(",", ',', false), ElementsAre("", ""));
  EXPECT_THAT(tkrzw::StrSplit("aa,bb,cc", ',', true), ElementsAre("aa", "bb", "cc"));
  EXPECT_THAT(tkrzw::StrSplit("aa,bb,cc", ',', false), ElementsAre("aa", "bb", "cc"));
  EXPECT_THAT(tkrzw::StrSplit(",a,b,c,", ','), ElementsAre("", "a", "b", "c", ""));
  EXPECT_THAT(tkrzw::StrSplit("", ",", true), ElementsAre());
  EXPECT_THAT(tkrzw::StrSplit("", ","), ElementsAre(""));
  EXPECT_THAT(tkrzw::StrSplit(",", ",", true), ElementsAre());
  EXPECT_THAT(tkrzw::StrSplit(",", ",", false), ElementsAre("", ""));
  EXPECT_THAT(tkrzw::StrSplit("aa,bb,cc", ",", true), ElementsAre("aa", "bb", "cc"));
  EXPECT_THAT(tkrzw::StrSplit("aa,bb,cc", ",", false), ElementsAre("aa", "bb", "cc"));
  EXPECT_THAT(tkrzw::StrSplit(",a,b,c,", ","), ElementsAre("", "a", "b", "c", ""));
  EXPECT_THAT(tkrzw::StrSplit("||a||b||c||", "||"), ElementsAre("", "a", "b", "c", ""));
  EXPECT_THAT(tkrzw::StrSplit("", ""), ElementsAre());
  EXPECT_THAT(tkrzw::StrSplit("abcde", ""), ElementsAre("a", "b", "c", "d", "e"));
  EXPECT_THAT(tkrzw::StrSplitAny("", ",:", true), ElementsAre());
  EXPECT_THAT(tkrzw::StrSplitAny("", ",:"), ElementsAre(""));
  EXPECT_THAT(tkrzw::StrSplitAny(",:", ",:", true), ElementsAre());
  EXPECT_THAT(tkrzw::StrSplitAny(",:", ",:", false), ElementsAre("", "", ""));
  EXPECT_THAT(tkrzw::StrSplitAny("aa:bb,cc", ",:", true), ElementsAre("aa", "bb", "cc"));
  EXPECT_THAT(tkrzw::StrSplitAny("aa:bb,cc", ",:", false), ElementsAre("aa", "bb", "cc"));
  EXPECT_THAT(tkrzw::StrSplitAny(":a,b,c:", ",:"), ElementsAre("", "a", "b", "c", ""));
  {
    auto map = tkrzw::StrSplitIntoMap("x;ab=c;;c=de;fg==hi;x", ";", "=");
    EXPECT_EQ(3, map.size());
    EXPECT_EQ("c", map["ab"]);
    EXPECT_EQ("de", map["c"]);
    EXPECT_EQ("=hi", map["fg"]);
  }
}

TEST(StrUtilTest, StrUpperCase) {
  EXPECT_EQ("[I LOVE YOU.]", tkrzw::StrUpperCase("[i love you.]"));
  EXPECT_EQ("AあいうえおZ", tkrzw::StrUpperCase("aあいうえおz"));
  std::string str = "[i love you.]";
  tkrzw::StrUpperCase(&str);
  EXPECT_EQ("[I LOVE YOU.]", str);
  str = "aあいうえおz";
  tkrzw::StrUpperCase(&str);
  EXPECT_EQ("AあいうえおZ", str);
}

TEST(StrUtilTest, StrLowerCase) {
  EXPECT_EQ("[i love you.]", tkrzw::StrLowerCase("[I LOVE YOU.]"));
  EXPECT_EQ("aあいうえおz", tkrzw::StrLowerCase("AあいうえおZ"));
  std::string str = "[I LOVE YOU.]";
  tkrzw::StrLowerCase(&str);
  EXPECT_EQ("[i love you.]", str);
  str = "AあいうえおZ";
  tkrzw::StrLowerCase(&str);
  EXPECT_EQ("aあいうえおz", str);
}

TEST(StrUtilTest, StrReplace) {
  EXPECT_EQ("bcbc", tkrzw::StrReplace("abcabc", "a", ""));
  EXPECT_EQ("xbcxbc", tkrzw::StrReplace("abcabc", "a", "x"));
  EXPECT_EQ("xybcxybc", tkrzw::StrReplace("abcabc", "a", "xy"));
  EXPECT_EQ("xcxc", tkrzw::StrReplace("abcabc", "ab", "x"));
  EXPECT_EQ("abxabx", tkrzw::StrReplace("abcabc", "c", "x"));
  EXPECT_EQ("axax", tkrzw::StrReplace("abcabc", "bc", "x"));
  EXPECT_EQ("xx", tkrzw::StrReplace("abcabc", "abc", "x"));
  EXPECT_EQ("abcabc", tkrzw::StrReplace("abcabc", "abcdefg", "x"));
  EXPECT_EQ("abcabc", tkrzw::StrReplace("abcabc", "", "x"));
  EXPECT_EQ("あいxえお", tkrzw::StrReplace("あいうえお", "う", "x"));
}

TEST(StrUtilTest, StrReplaceCharacters) {
  std::string str;
  tkrzw::StrReplaceCharacters(&str, "", "");
  EXPECT_EQ("", str);
  str = "abcde";
  tkrzw::StrReplaceCharacters(&str, "bcd", "BCD");
  EXPECT_EQ("aBCDe", str);
  str = "abcde";
  tkrzw::StrReplaceCharacters(&str, "bcd", "BD");
  EXPECT_EQ("aBDe", str);
  str = "あAいBうCえDお";
  tkrzw::StrReplaceCharacters(&str, "ABCD", "ab");
  EXPECT_EQ("あaいbうえお", str);
}

TEST(StrUtilTest, StrContains) {
  EXPECT_TRUE(tkrzw::StrContains("", ""));
  EXPECT_TRUE(tkrzw::StrContains("abc", ""));
  EXPECT_TRUE(tkrzw::StrContains("abc", "a"));
  EXPECT_TRUE(tkrzw::StrContains("abc", "ab"));
  EXPECT_TRUE(tkrzw::StrContains("abc", "abc"));
  EXPECT_TRUE(tkrzw::StrContains("abcd", "bc"));
  EXPECT_TRUE(tkrzw::StrContains("abcd", "cd"));
  EXPECT_TRUE(tkrzw::StrContains("abcd", "d"));
  EXPECT_FALSE(tkrzw::StrContains("abc", "abcd"));
  EXPECT_FALSE(tkrzw::StrContains("abc", "xa"));
  EXPECT_FALSE(tkrzw::StrContains("abc", "ac"));
}

TEST(StrUtilTest, StrBeginsWith) {
  EXPECT_TRUE(tkrzw::StrBeginsWith("", ""));
  EXPECT_TRUE(tkrzw::StrBeginsWith("abc", ""));
  EXPECT_TRUE(tkrzw::StrBeginsWith("abc", "a"));
  EXPECT_TRUE(tkrzw::StrBeginsWith("abc", "ab"));
  EXPECT_TRUE(tkrzw::StrBeginsWith("abc", "abc"));
  EXPECT_FALSE(tkrzw::StrBeginsWith("abc", "abcd"));
  EXPECT_FALSE(tkrzw::StrBeginsWith("abc", "xa"));
  EXPECT_FALSE(tkrzw::StrBeginsWith("abc", "ac"));
}

TEST(StrUtilTest, StrEndsWith) {
  EXPECT_TRUE(tkrzw::StrEndsWith("", ""));
  EXPECT_TRUE(tkrzw::StrEndsWith("abc", ""));
  EXPECT_TRUE(tkrzw::StrEndsWith("abc", "c"));
  EXPECT_TRUE(tkrzw::StrEndsWith("abc", "bc"));
  EXPECT_TRUE(tkrzw::StrEndsWith("abc", "abc"));
  EXPECT_FALSE(tkrzw::StrEndsWith("abc", "xabc"));
  EXPECT_FALSE(tkrzw::StrEndsWith("abc", "cx"));
  EXPECT_FALSE(tkrzw::StrEndsWith("abc", "ac"));
}

TEST(StrUtilTest, StrCaseCompare) {
  EXPECT_EQ(0, tkrzw::StrCaseCompare("", ""));
  EXPECT_EQ(0, tkrzw::StrCaseCompare("a", "A"));
  EXPECT_EQ(0, tkrzw::StrCaseCompare("A", "a"));
  EXPECT_EQ(0, tkrzw::StrCaseCompare("[aA]", "[Aa]"));
  EXPECT_LT(tkrzw::StrCaseCompare("a", "aa"), 0);
  EXPECT_GT(tkrzw::StrCaseCompare("aa", "a"), 0);
  EXPECT_LT(tkrzw::StrCaseCompare("aa", "ab"), 0);
  EXPECT_GT(tkrzw::StrCaseCompare("ab", "aa"), 0);
  EXPECT_LT(tkrzw::StrCaseCompare("aaa", "あ"), 0);
  EXPECT_GT(tkrzw::StrCaseCompare("あ", "aaa"), 0);
}

class StrSearchStaticTest :
    public TestWithParam<int32_t (*)(std::string_view, std::string_view)> {
};

TEST_P(StrSearchStaticTest, StrSearchStatic) {
  const auto search = GetParam();
  EXPECT_EQ(0, search("ABABABABC", ""));
  EXPECT_EQ(0, search("ABABABABC", "A"));
  EXPECT_EQ(0, search("ABABABABC", "AB"));
  EXPECT_EQ(0, search("ABABABABC", "ABA"));
  EXPECT_EQ(4, search("ABABABABC", "ABABC"));
  EXPECT_EQ(-1, search("ABABABABC", "ABABCX"));
  EXPECT_EQ(2, search("ABCDE", "CDE"));
  EXPECT_EQ(-1, search("ABCDE", "X"));
  EXPECT_EQ(0, search("あいあいうえお", "あ"));
  EXPECT_EQ(3, search("あいあいうえお", "い"));
  EXPECT_EQ(9, search("あいあいうえお", "いう"));
  EXPECT_EQ(9, search("あいあいうえお", "いうえお"));
  EXPECT_EQ(-1, search("", "A"));
  EXPECT_EQ(-1, search("AB", "ABC"));
}

INSTANTIATE_TEST_SUITE_P(StrSearchStatic, StrSearchStaticTest, Values(
    tkrzw::StrSearch, tkrzw::StrSearchDoubleLoop, tkrzw::StrSearchMemchr,
    tkrzw::StrSearchMemmem, tkrzw::StrSearchKMP, tkrzw::StrSearchBM,
    tkrzw::StrSearchRK, tkrzw::StrSearchZ));

class StrSearchRandomTest :
    public TestWithParam<int32_t (*)(std::string_view, std::string_view)> {
};

TEST_P(StrSearchRandomTest, StrSearchRandom) {
  const auto search = GetParam();
  const std::vector<std::pair<char, char>> charsets =
    {{'a', 'b'}, {'a', 'c'}, {'a', 'd'}, {'a', 'h'}, {'a', 'z'}};
  constexpr int32_t num_iterations = 3;
  const std::vector<int> text_sizes = {1, 4, 16, 64, 256, 1024};
  const std::vector<int> pattern_sizes = {1, 2, 3, 4, 5, 6};
  for (const auto& charset : charsets) {
    for (int32_t i = 0; i < num_iterations; i++) {
      for (const int32_t text_size : text_sizes) {
        const std::string& text = MakeRandomCharacterText(
            text_size, charset.first, charset.second);
        for (const int32_t pattern_size : pattern_sizes) {
          const std::string& pattern = MakeRandomCharacterText(
              pattern_size, charset.first, charset.second);
          const int32_t base_result = tkrzw::StrSearch(text, pattern);
          const int32_t test_result = search(text, pattern);
          EXPECT_EQ(base_result, test_result);
        }
      }
    }
  }
}

INSTANTIATE_TEST_SUITE_P(StrSearchRandom, StrSearchRandomTest, Values(
    tkrzw::StrSearchDoubleLoop, tkrzw::StrSearchMemchr,
    tkrzw::StrSearchMemmem, tkrzw::StrSearchKMP, tkrzw::StrSearchBM,
    tkrzw::StrSearchRK, tkrzw::StrSearchZ));

class StrSearchWholeStaticTest :
    public TestWithParam<std::vector<int32_t> (*)(std::string_view, std::string_view, size_t)> {
};

TEST_P(StrSearchWholeStaticTest, StrSearchWholeStatic) {
  const auto search = GetParam();
  EXPECT_THAT(search("abc", "", 0), ElementsAre(0, 1, 2));
  EXPECT_THAT(search("abcabcabcd", "x", 0), ElementsAre());
  EXPECT_THAT(search("abcabcabcd", "a", 0), ElementsAre(0, 3, 6));
  EXPECT_THAT(search("abcabcabcd", "ab", 0), ElementsAre(0, 3, 6));
  EXPECT_THAT(search("abcabcabcd", "abc", 0), ElementsAre(0, 3, 6));
  EXPECT_THAT(search("abcabcabcd", "abcd", 0), ElementsAre(6));
  EXPECT_THAT(search("abbbc", "b", 0), ElementsAre(1, 2, 3));
  EXPECT_THAT(search("abcbcbcd", "bc", 0), ElementsAre(1, 3, 5));
  EXPECT_THAT(search("aaaaa", "aa", 0), ElementsAre(0, 1, 2, 3));
  EXPECT_THAT(search("あいうとい", "いう", 0), ElementsAre(3));
  EXPECT_THAT(search("あいうという", "いう", 0), ElementsAre(3, 12));
  EXPECT_THAT(search("abcde", "", 3), ElementsAre(0, 1, 2));
  EXPECT_THAT(search("aabbaaaaaa", "a", 3), ElementsAre(0, 1, 4));
  EXPECT_THAT(search("aabbaaaaaa", "aa", 3), ElementsAre(0, 4, 5));
}

INSTANTIATE_TEST_SUITE_P(StrSearchWholeStatic, StrSearchWholeStaticTest, Values(
    tkrzw::StrSearchWhole,
    tkrzw::StrSearchWholeKMP, tkrzw::StrSearchWholeBM, tkrzw::StrSearchWholeRK));

class StrSearchWholeRandomTest :
    public TestWithParam<std::vector<int32_t> (*)(std::string_view, std::string_view, size_t)> {
};

TEST_P(StrSearchWholeRandomTest, StrSearchWholeRandom) {
  const auto search = GetParam();
  const std::vector<std::pair<char, char>> charsets =
    {{'a', 'b'}, {'a', 'c'}, {'a', 'd'}, {'a', 'h'}, {'a', 'z'}};
  constexpr int32_t num_iterations = 3;
  const std::vector<int> text_sizes = {1, 4, 16, 64, 256, 1024};
  const std::vector<int> pattern_sizes = {1, 2, 3, 4, 5, 6};
  for (const auto& charset : charsets) {
    for (int32_t i = 0; i < num_iterations; i++) {
      for (const int32_t text_size : text_sizes) {
        const std::string& text = MakeRandomCharacterText(
            text_size, charset.first, charset.second);
        for (const int32_t pattern_size : pattern_sizes) {
          const std::string& pattern = MakeRandomCharacterText(
              pattern_size, charset.first, charset.second);
          const auto base_result = tkrzw::StrSearchWhole(text, pattern);
          const auto test_result = search(text, pattern, 0);
          EXPECT_EQ(base_result.size(), test_result.size());
        }
      }
    }
  }
}

INSTANTIATE_TEST_SUITE_P(StrSearchWholeRandom, StrSearchWholeRandomTest, Values(
    tkrzw::StrSearchWholeKMP, tkrzw::StrSearchWholeBM,tkrzw::StrSearchWholeRK));

class StrSearchBatchStaticTest : public TestWithParam<
  std::vector<std::vector<int32_t>> (*)(
      std::string_view, const std::vector<std::string>&, size_t)> {
};

TEST_P(StrSearchBatchStaticTest, StrSearchBatchStatic) {
  const auto search = GetParam();
  EXPECT_TRUE(search("", {}, 0).empty());
  {
    const auto result = search("abc", {""}, 0);
    EXPECT_EQ(1, result.size());
    EXPECT_THAT(result[0], ElementsAre(0, 1, 2));
  }
  {
    const auto result = search("abcabc", {"abc", "ab", "bc", "c", "d"}, 0);
    EXPECT_EQ(5, result.size());
    EXPECT_THAT(result[0], ElementsAre(0, 3));
    EXPECT_THAT(result[1], ElementsAre(0, 3));
    EXPECT_THAT(result[2], ElementsAre(1, 4));
    EXPECT_THAT(result[3], ElementsAre(2, 5));
    EXPECT_THAT(result[4], ElementsAre());
  }
  {
    const auto result = search("aaaa", {"a", "aa", "aaa", "aaaa"}, 0);
    EXPECT_EQ(4, result.size());
    EXPECT_THAT(result[0], ElementsAre(0, 1, 2, 3));
    EXPECT_THAT(result[1], ElementsAre(0, 1, 2));
    EXPECT_THAT(result[2], ElementsAre(0, 1));
    EXPECT_THAT(result[3], ElementsAre(0));
  }
  {
    const auto result =
        search("もももすももも", {"もも", "すも", "も", "す", "すもも"}, 0);
    EXPECT_EQ(5, result.size());
    EXPECT_THAT(result[0], ElementsAre(0, 3, 12, 15));
    EXPECT_THAT(result[1], ElementsAre(9));
    EXPECT_THAT(result[2], ElementsAre(0, 3, 6, 12, 15, 18));
    EXPECT_THAT(result[3], ElementsAre(9));
    EXPECT_THAT(result[4], ElementsAre(9));
  }
  {
    const auto result = search("aabbaaaaaa", {"a", "aa"}, 3);
    EXPECT_EQ(2, result.size());
    EXPECT_THAT(result[0], ElementsAre(0, 1, 4));
    EXPECT_THAT(result[1], ElementsAre(0, 4, 5));
  }
}

INSTANTIATE_TEST_SUITE_P(StrSearchBatchStatic, StrSearchBatchStaticTest, Values(
    tkrzw::StrSearchBatch,
    tkrzw::StrSearchBatchKMP, tkrzw::StrSearchBatchBM, tkrzw::StrSearchBatchRK));

class StrSearchBatchRandomTest : public TestWithParam<
  std::vector<std::vector<int32_t>> (*)(
      std::string_view, const std::vector<std::string>&, size_t)> {
};

TEST_P(StrSearchBatchRandomTest, StrSearchBatchRandom) {
  const auto search = GetParam();
  const std::vector<std::pair<char, char>> charsets =
    {{'a', 'b'}, {'a', 'c'}, {'a', 'd'}, {'a', 'h'}, {'a', 'z'}};
  constexpr int32_t num_iterations = 3;
  const std::vector<int> text_sizes = {1, 4, 16, 64, 256, 1024};
  const std::vector<int> pattern_sizes = {1, 2, 3, 4, 5, 6};
  constexpr int32_t batch_size_factor = 2;
  for (const auto& charset : charsets) {
    for (int32_t i = 0; i < num_iterations; i++) {
      for (const int32_t text_size : text_sizes) {
        const std::string& text = MakeRandomCharacterText(
            text_size, charset.first, charset.second);
        std::vector<std::string> patterns;
        for (const int32_t pattern_size : pattern_sizes) {
          for (int32_t j = 0; j < batch_size_factor; ++j) {
            patterns.emplace_back(MakeRandomCharacterText(
                pattern_size, charset.first, charset.second));
          }
        }
        const auto base_result = tkrzw::StrSearchBatch(text, patterns);
        const auto test_result = search(text, patterns, 0);
        for (size_t j = 0; j < base_result.size(); j++) {
          EXPECT_EQ(base_result[j].size(), test_result[j].size());
        }
      }
    }
  }
}

INSTANTIATE_TEST_SUITE_P(StrSearchBatchRandom, StrSearchBatchRandomTest, Values(
    tkrzw::StrSearchBatchKMP, tkrzw::StrSearchBatchBM, tkrzw::StrSearchBatchRK));

TEST(StrUtilTest, StrStripSpace) {
  EXPECT_EQ("", tkrzw::StrStripSpace(""));
  EXPECT_EQ("ABC", tkrzw::StrStripSpace("  ABC  "));
  EXPECT_EQ("A  B  C", tkrzw::StrStripSpace(" A  B  C  "));
  EXPECT_EQ("あいう", tkrzw::StrStripSpace("  あいう  "));
}

TEST(StrUtilTest, StrStripLine) {
  EXPECT_EQ("", tkrzw::StrStripLine(""));
  EXPECT_EQ("ABC", tkrzw::StrStripLine("ABC\r\n"));
  EXPECT_EQ("ABC", tkrzw::StrStripLine("ABC\n"));
  EXPECT_EQ("ABC", tkrzw::StrStripLine("ABC\r"));
  EXPECT_EQ(" ABC ", tkrzw::StrStripLine(" ABC \r"));
  EXPECT_EQ("", tkrzw::StrStripLine("\n\r"));
  std::string str(" hello \r\n\r\n");
  tkrzw::StrStripLine(&str);
  EXPECT_EQ(" hello ", str);
  str = "\r\n";
  tkrzw::StrStripLine(&str);
  EXPECT_EQ("", str);
}

TEST(StrUtilTest, StrSqueezeAndStripSpace) {
  EXPECT_EQ("", tkrzw::StrSqueezeAndStripSpace(""));
  EXPECT_EQ("ABC", tkrzw::StrSqueezeAndStripSpace("  ABC  "));
  EXPECT_EQ("A B C", tkrzw::StrSqueezeAndStripSpace(" A  B  C  "));
  EXPECT_EQ("あ い う", tkrzw::StrSqueezeAndStripSpace("  あ   い   う  "));
}

TEST(StrUtilTest, StrTrimForTSV) {
  EXPECT_EQ("", tkrzw::StrTrimForTSV(""));
  EXPECT_EQ(" ", tkrzw::StrTrimForTSV(std::string("\x00", 1)));
  EXPECT_EQ("  ABC  ", tkrzw::StrTrimForTSV("\n\rABC\t\x7F"));
  EXPECT_EQ("A  B C", tkrzw::StrTrimForTSV("A\n\tB C"));
  EXPECT_EQ("A \tB C", tkrzw::StrTrimForTSV("A\n\tB C", true));
  EXPECT_EQ("あ  い  う", tkrzw::StrTrimForTSV("あ\t\tい\r\nう"));
  EXPECT_EQ("あ\t\tい  う", tkrzw::StrTrimForTSV("あ\t\tい\r\nう", true));
}

TEST(StrUtilTest, StrEscapeC) {
  EXPECT_EQ("", tkrzw::StrEscapeC(""));
  EXPECT_EQ("a\\nb\\tc", tkrzw::StrEscapeC("a\nb\tc"));
  EXPECT_EQ(" a b c ", tkrzw::StrEscapeC(" a b c "));
  EXPECT_EQ("\\\\", tkrzw::StrEscapeC("\\"));
  EXPECT_EQ("あいう", tkrzw::StrEscapeC("あいう"));
  EXPECT_EQ("\\xe3\\x81\\x82\\xe3\\x81\\x84\\xe3\\x81\\x86", tkrzw::StrEscapeC("あいう", true));
  EXPECT_EQ("a\0\a\b\t\n\v\f\r\\\x7Fz", tkrzw::StrEscapeC("a\0\a\b\t\n\v\f\r\\\x7Fz"));
  EXPECT_EQ("a\\x01\\x02\\x03z", tkrzw::StrEscapeC("a\x01\x02\x03z"));
  EXPECT_EQ("\\0\\0", tkrzw::StrEscapeC(std::string("\0\0", 2)));
  EXPECT_EQ("a\\0\\0b", tkrzw::StrEscapeC(std::string("a\0\0b", 4)));
  EXPECT_EQ("\\x000", tkrzw::StrEscapeC(std::string("\0000", 2)));
  EXPECT_EQ("\\08", tkrzw::StrEscapeC(std::string("\0008", 2)));
  EXPECT_EQ(" \\0 ", tkrzw::StrEscapeC(std::string(" \000 ", 3)));
}

TEST(StrUtilTest, StrUnescapeC) {
  EXPECT_EQ("", tkrzw::StrUnescapeC(""));
  EXPECT_EQ(std::string("\0\0", 2), tkrzw::StrUnescapeC("\\0\\0"));
  EXPECT_EQ(std::string("a\0\0b", 4), tkrzw::StrUnescapeC("a\\0\\0b"));
  EXPECT_EQ("a", tkrzw::StrUnescapeC("\\141"));
  EXPECT_EQ("a1", tkrzw::StrUnescapeC("\\1411"));
  EXPECT_EQ("abc", tkrzw::StrUnescapeC("a\\142c"));
  EXPECT_EQ("\\", tkrzw::StrUnescapeC("\\\\"));
  EXPECT_EQ("", tkrzw::StrUnescapeC("\\"));
  EXPECT_EQ("a", tkrzw::StrUnescapeC("a\\"));
  EXPECT_EQ("a", tkrzw::StrUnescapeC("\\x61"));
  EXPECT_EQ("a1", tkrzw::StrUnescapeC("\\x611"));
  EXPECT_EQ("abc", tkrzw::StrUnescapeC("a\\x62c"));
  EXPECT_EQ("あいう", tkrzw::StrUnescapeC("あいう"));
  EXPECT_EQ("あいう", tkrzw::StrUnescapeC("\\xe3\\x81\\x82\\xe3\\x81\\x84\\xe3\\x81\\x86"));
  EXPECT_EQ("a\0\a\b\t\n\v\f\r\\\x7Fz", tkrzw::StrUnescapeC("a\0\a\b\t\n\v\f\r\\\x7Fz"));
  EXPECT_EQ("a\x01\x02\x03z", tkrzw::StrUnescapeC("a\\x01\\x02\\x03z"));
  for (int32_t c1 = tkrzw::INT8MIN; c1 <= tkrzw::INT8MAX; c1++) {
    std::string str(1, c1);
    tkrzw::StrUnescapeC(str);
    EXPECT_EQ(str, tkrzw::StrUnescapeC(tkrzw::StrEscapeC(str)));
  }
  for (int32_t c2 = 0; c2 <= tkrzw::INT8MAX; c2++) {
    std::string str("\\");
    str.push_back(c2);
    tkrzw::StrUnescapeC(str);
    EXPECT_EQ(str, tkrzw::StrUnescapeC(tkrzw::StrEscapeC(str)));
    for (int32_t c3 = '0'; c3 <= '9'; c3++) {
      str.resize(2);
      str.append(3, c3);
      tkrzw::StrUnescapeC(str);
      EXPECT_EQ(str, tkrzw::StrUnescapeC(tkrzw::StrEscapeC(str)));
    }
  }
}

TEST(StrUtilTest, StrEncodeBase64) {
  EXPECT_EQ("", tkrzw::StrEncodeBase64(""));
  EXPECT_EQ("YQ==", tkrzw::StrEncodeBase64("a"));
  EXPECT_EQ("YWI=", tkrzw::StrEncodeBase64("ab"));
  EXPECT_EQ("YWJj", tkrzw::StrEncodeBase64("abc"));
  EXPECT_EQ("YWJjZA==", tkrzw::StrEncodeBase64("abcd"));
  EXPECT_EQ("YWJjZGU=", tkrzw::StrEncodeBase64("abcde"));
  EXPECT_EQ("YWJjZGVm", tkrzw::StrEncodeBase64("abcdef"));
  EXPECT_EQ("QUJDzrHOss6z44GC44GE44GG", tkrzw::StrEncodeBase64("ABCαβγあいう"));
}

TEST(StrUtilTest, StrDecodeBase64) {
  EXPECT_EQ("", tkrzw::StrDecodeBase64(""));
  EXPECT_EQ("a", tkrzw::StrDecodeBase64("YQ=="));
  EXPECT_EQ("ab", tkrzw::StrDecodeBase64("YWI="));
  EXPECT_EQ("abc", tkrzw::StrDecodeBase64("YWJj"));
  EXPECT_EQ("abcd", tkrzw::StrDecodeBase64("YWJjZA=="));
  EXPECT_EQ("abcde", tkrzw::StrDecodeBase64("YWJjZGU="));
  EXPECT_EQ("abcdef", tkrzw::StrDecodeBase64("YWJjZGVm"));
  EXPECT_EQ("ABCαβγあいう", tkrzw::StrDecodeBase64("QUJDzrHOss6z44GC44GE44GG"));
  uint8_t c = 0;
  for (int32_t len = 0; len <= 64; len++) {
    std::string str;
    for (int32_t i = 0; i < len; i++) {
      str.push_back(c++);
    }
    EXPECT_EQ(str, tkrzw::StrDecodeBase64(tkrzw::StrEncodeBase64(str)));
  }
}

TEST(StrUtilTest, StrEncodeURL) {
  EXPECT_EQ("", tkrzw::StrEncodeURL(""));
  EXPECT_EQ("abc", tkrzw::StrEncodeURL("abc"));
  EXPECT_EQ("%60%21%40%23%24%25%5e%26%2a%28%29-_%2b%3d",
            tkrzw::StrEncodeURL("`!@#$%^&*()-_+="));
  EXPECT_EQ("%7b%7d%5c%7c%3a%3b%22%27%3c%3e%2f%3f%2c.",
            tkrzw::StrEncodeURL("{}\\|:;\"'<>/?,."));
  EXPECT_EQ("%e3%81%82%e3%81%84%e3%81%86%20%ce%b1%ce%b2%ce%b3",
            tkrzw::StrEncodeURL("あいう αβγ"));
}

TEST(StrUtilTest, StrDecodeURL) {
  EXPECT_EQ("", tkrzw::StrDecodeURL(""));
  EXPECT_EQ("abc", tkrzw::StrDecodeURL("abc"));
  EXPECT_EQ("`!@#$%^&*()-_+=",
            tkrzw::StrDecodeURL("%60%21%40%23%24%25%5e%26%2a%28%29-_%2b%3d"));
  EXPECT_EQ("{}\\|:;\"'<>/?,.",
            tkrzw::StrDecodeURL("%7b%7d%5c%7c%3a%3b%22%27%3c%3e%2f%3f%2c."));
  EXPECT_EQ("あいう αβγ",
            tkrzw::StrDecodeURL("%e3%81%82%e3%81%84%e3%81%86%20%ce%b1%ce%b2%ce%b3"));
  uint8_t c = 0;
  for (int32_t len = 0; len <= 64; len++) {
    std::string str;
    for (int32_t i = 0; i < len; i++) {
      str.push_back(c++);
    }
    EXPECT_EQ(str, tkrzw::StrDecodeURL(tkrzw::StrEncodeURL(str)));
  }
}

TEST(StrUtilTest, StrSearchRegex) {
  EXPECT_EQ(0, tkrzw::StrSearchRegex("", ""));
  EXPECT_EQ(-1, tkrzw::StrSearchRegex("", "a"));
  EXPECT_EQ(-2, tkrzw::StrSearchRegex("", "*"));
  EXPECT_EQ(0, tkrzw::StrSearchRegex("abc", ""));
  EXPECT_EQ(0, tkrzw::StrSearchRegex("abc", "a"));
  EXPECT_EQ(0, tkrzw::StrSearchRegex("abc", "ab"));
  EXPECT_EQ(1, tkrzw::StrSearchRegex("abc", "b"));
  EXPECT_EQ(1, tkrzw::StrSearchRegex("abc", "bc"));
  EXPECT_EQ(2, tkrzw::StrSearchRegex("abc", "c"));
  EXPECT_EQ(-1, tkrzw::StrSearchRegex("abc", "d"));
  EXPECT_EQ(2, tkrzw::StrSearchRegex("abcdabcd", "c"));
  EXPECT_EQ(3, tkrzw::StrSearchRegex("あいうえお", "いうえ"));
}

TEST(StrUtilTest, StrReplaceRegex) {
  EXPECT_EQ("", tkrzw::StrReplaceRegex("", "", ""));
  EXPECT_EQ("", tkrzw::StrReplaceRegex("", "a", ""));
  EXPECT_EQ("", tkrzw::StrReplaceRegex("", "*", ""));
  EXPECT_EQ("bcd", tkrzw::StrReplaceRegex("abcd", "a", ""));
  EXPECT_EQ("a[BC]d", tkrzw::StrReplaceRegex("abcd", "bc", "[BC]"));
  EXPECT_EQ("a[bc]d", tkrzw::StrReplaceRegex("abcd", "bc", "[$&]"));
  EXPECT_EQ("a[]d", tkrzw::StrReplaceRegex("abcd", "bc", "[$3]"));
  EXPECT_EQ("a[bc]D[ef]g", tkrzw::StrReplaceRegex("abcdefg", "(bc)d(ef)", "[$1]D[$2]"));
  EXPECT_EQ("あ[いう]エ[おか]き", tkrzw::StrReplaceRegex(
      "あいうえおかき", "(いう)え(おか)", "[$1]エ[$2]"));
}

TEST(StrUtilTest, ConvertUTF8AndUCS4) {
  for (const std::string utf : {"", "abc", "αβγ", "あいう"}) {
    const std::vector<uint32_t>& ucs = tkrzw::ConvertUTF8ToUCS4(utf);
    const std::string& utf_restored = tkrzw::ConvertUCS4ToUTF8(ucs);
    EXPECT_EQ(utf, utf_restored);
  }
  std::vector<uint32_t> ucs;
  for (uint32_t c = 1; c < 1U << 31; c *= 2) {
    ucs.emplace_back(c - 1);
    ucs.emplace_back(c);
    ucs.emplace_back(c + 1);
  }
  const std::string& utf = tkrzw::ConvertUCS4ToUTF8(ucs);
  const std::vector<uint32_t>& ucs_restored = tkrzw::ConvertUTF8ToUCS4(utf);
  EXPECT_THAT(ucs_restored, ElementsAreArray(ucs));
}

TEST(StrUtilTest, ConvertUTF8AndWide) {
  for (const std::string utf : {"", "abc", "αβγ", "あいう"}) {
    const std::wstring wstr = tkrzw::ConvertUTF8ToWide(utf);
    const std::string& utf_restored = tkrzw::ConvertWideToUTF8(wstr);
    EXPECT_EQ(utf, utf_restored);
  }
  std::wstring wstr;
  for (uint32_t c = 1; c < 1U << 31; c *= 2) {
    wstr.push_back(c - 1);
    wstr.push_back(c);
    wstr.push_back(c + 1);
  }
  const std::string& utf = tkrzw::ConvertWideToUTF8(wstr);
  const std::wstring& wstr_restored = tkrzw::ConvertUTF8ToWide(utf);
  EXPECT_EQ(wstr, wstr_restored);
}

TEST(StrUtilTest, EditDistanceLev) {
  struct TestCase final {
    std::string a;
    std::string b;
    int32_t expected_dist;
  };
  const std::vector<TestCase> test_cases = {
    {"", "", 0},
    {"abc", "abc", 0},
    {"abc", "abxc", 1},
    {"abc", "ac", 1},
    {"abc", "axc", 1},
    {"abcde", "axcxe", 2},
    {"abcdef", "abcf", 2},
    {"あいう", "あいう", 0},
    {std::string(100, 'x'), std::string(100, 'x'), 0},
    {"", std::string(100, 'x'), 100},
    {std::string(100, 'x'), std::string(100, 'y'), 100},
    {std::string(100, 'x'), std::string(50, 'x'), 50},
    {std::string(100, 'x'), std::string(50, 'y'), 100},
  };
  for (const auto& test_case : test_cases) {
    EXPECT_EQ(test_case.expected_dist, tkrzw::EditDistanceLev(test_case.a, test_case.b));
    EXPECT_EQ(test_case.expected_dist, tkrzw::EditDistanceLev(test_case.b, test_case.a));
    const std::vector<uint32_t>& a_ucs = tkrzw::ConvertUTF8ToUCS4(test_case.a);
    const std::vector<uint32_t>& b_ucs = tkrzw::ConvertUTF8ToUCS4(test_case.b);
    EXPECT_EQ(test_case.expected_dist, tkrzw::EditDistanceLev(a_ucs, b_ucs));
    EXPECT_EQ(test_case.expected_dist, tkrzw::EditDistanceLev(b_ucs, a_ucs));
  }
}

TEST(StrUtilTest, SerializeStrPair) {
  struct TestCase final {
    std::string first;
    std::string second;
    size_t expected_size;
  };
  const std::vector<TestCase> test_cases = {
    {"", "", 2},
    {"a", "bb", 5},
    {"aaa", "bb", 7},
    {std::string(127, 'a'), std::string(127, 'b'), 1 + 127 + 1 + 127},
    {std::string(128, 'a'), std::string(128, 'b'), 2 + 128 + 2 + 128},
    {std::string(16383, 'a'), std::string(16383, 'b'), 2 + 16383 + 2 + 16383},
    {std::string(16384, 'a'), std::string(16384, 'b'), 3 + 16384 + 3 + 16384},
  };
  for (const auto& test_case : test_cases) {
    const std::string serialized = tkrzw::SerializeStrPair(test_case.first, test_case.second);
    EXPECT_EQ(test_case.expected_size, serialized.size());
    std::string_view first, second;
    tkrzw::DeserializeStrPair(serialized, &first, &second);
    EXPECT_EQ(test_case.first, first);
    EXPECT_EQ(test_case.second, second);
    EXPECT_EQ(test_case.first, tkrzw::GetFirstFromSerializedStrPair(serialized));
  }
}

TEST(StrUtilTest, SerializeStrVector) {
  struct TestCase final {
    std::vector<std::string> values;
    size_t expected_size;
  };
  const std::vector<TestCase> test_cases = {
    {{""}, 1},
    {{"", ""}, 2},
    {{"a", "bb"}, 5},
    {{"a", "bb", "ccc"}, 9},
    {{std::string(127, 'a'), std::string(127, 'b')}, 1 + 127 + 1 + 127},
    {{std::string(128, 'a'), std::string(128, 'b')}, 2 + 128 + 2 + 128},
  };
  for (const auto& test_case : test_cases) {
    const std::string serialized = tkrzw::SerializeStrVector(test_case.values);
    EXPECT_EQ(test_case.expected_size, serialized.size());
    const std::vector<std::string> values = tkrzw::DeserializeStrVector(serialized);
    EXPECT_THAT(values, ElementsAreArray(test_case.values));
  }
}

TEST(StrUtilTest, MakeStrVectorFromViews) {
  const std::vector<std::string_view> views = {"one", "two", "three"};
  const std::vector<std::string>& values = tkrzw::MakeStrVectorFromViews(views);
  EXPECT_THAT(values, ElementsAreArray(views));
  const std::vector<std::string_view>& restored = tkrzw::MakeStrViewVectorFromValues(values);
  ASSERT_EQ(values.size(), restored.size());
  for (size_t i = 0; i < restored.size(); i++) {
    EXPECT_EQ(values[i], restored[i]);
  }
}

TEST(StrUtilTest, SerializeStrMap) {
  struct TestCase final {
    std::map<std::string, std::string> records;
    size_t expected_size;
  };
  const std::vector<TestCase> test_cases = {
    {{{"", ""}}, 2},
    {{{"a", "bb"}}, 5},
    {{{"a", "bb"}, {"ccc", "dddd"}}, 14},
    {{{std::string(127, 'a'), std::string(127, 'b')}}, 1 + 127 + 1 + 127},
    {{{std::string(128, 'a'), std::string(128, 'b')}}, 2 + 128 + 2 + 128},
  };
  for (const auto& test_case : test_cases) {
    const std::string serialized = tkrzw::SerializeStrMap(test_case.records);
    EXPECT_EQ(test_case.expected_size, serialized.size());
    const std::map<std::string, std::string> records = tkrzw::DeserializeStrMap(serialized);
    EXPECT_THAT(records, ElementsAreArray(test_case.records));
  }
}

TEST(StrUtilTest, MakeStrMapFromViews) {
  const std::map<std::string_view, std::string_view> views = {
    {"one", "hop"}, {"two", "step"}, {"three", "jump"}};
  std::map<std::string, std::string> records = tkrzw::MakeStrMapFromViews(views);
  EXPECT_EQ(3, records.size());
  EXPECT_EQ("hop", records["one"]);
  EXPECT_EQ("step", records["two"]);
  EXPECT_EQ("jump", records["three"]);
  std::map<std::string_view, std::string_view> restored =
      tkrzw::MakeStrViewMapFromRecords(records);
  EXPECT_EQ(3, restored.size());
  EXPECT_EQ("hop", restored["one"]);
  EXPECT_EQ("step", restored["two"]);
  EXPECT_EQ("jump", restored["three"]);
}

TEST(StrUtilTest, ScopedStringView) {
  {
    char* buf = static_cast<char*>(tkrzw::xmalloc(5));
    std::memcpy(buf, "12345", 5);
    tkrzw::ScopedStringView scoped_view(buf, 5);
    EXPECT_EQ(std::string_view("12345", 5), scoped_view.Get());
    buf = static_cast<char*>(tkrzw::xmalloc(6));
    std::memcpy(buf, "ABCDEF", 6);
    scoped_view.Set(buf, 6);
    EXPECT_EQ(std::string_view("ABCDEF", 6), scoped_view.Get());
  }
  {
    char* buf = static_cast<char*>(tkrzw::xmalloc(6));
    std::memcpy(buf, "ABCDEF", 6);
    tkrzw::ScopedStringView scoped_view(std::string_view(buf, 6));
    EXPECT_EQ(std::string_view("ABCDEF", 6), scoped_view.Get());
    buf = static_cast<char*>(tkrzw::xmalloc(5));
    std::memcpy(buf, "12345", 5);
    scoped_view.Set(std::string_view(buf, 5));
    EXPECT_EQ(std::string_view("12345", 5), scoped_view.Get());
  }
  {
    tkrzw::ScopedStringView scoped_view;
  }
}

// END OF FILE
