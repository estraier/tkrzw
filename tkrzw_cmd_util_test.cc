/*************************************************************************************************
 * Tests for tkrzw_cmd_util.h
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
#include "gtest/internal/gtest-port.h"
#include "gmock/gmock.h"

#include "tkrzw_cmd_util.h"
#include "tkrzw_lib_common.h"
#include "tkrzw_str_util.h"

using namespace testing;

// Main routine
int main(int argc, char** argv) {
  InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

TEST(CmdUtilTest, Print) {
  testing::internal::CaptureStdout();
  tkrzw::Print("a", "b", "c", 1, 2, 3.4, " ", tkrzw::Status(tkrzw::Status::SUCCESS), "\n");
  EXPECT_EQ("abc123.4 SUCCESS\n", testing::internal::GetCapturedStdout());
}

TEST(CmdUtilTest, PrintL) {
  testing::internal::CaptureStdout();
  tkrzw::PrintL("a", "b", "c", 1, 2, 3.4, " ", tkrzw::Status(tkrzw::Status::SUCCESS));
  EXPECT_EQ("abc123.4 SUCCESS\n", testing::internal::GetCapturedStdout());
}

TEST(CmdUtilTest, PrintF) {
  testing::internal::CaptureStdout();
  tkrzw::PrintF("%s %06d %06.2f\n", "Hello", 123, 4.5);
  EXPECT_EQ("Hello 000123 004.50\n", testing::internal::GetCapturedStdout());
}

TEST(CmdUtilTest, PutChar) {
  testing::internal::CaptureStdout();
  tkrzw::PutChar('.');
  tkrzw::PutChar('.');
  tkrzw::PutChar('.');
  EXPECT_EQ("...", testing::internal::GetCapturedStdout());
}

TEST(CmdUtilTest, EPrint) {
  testing::internal::CaptureStderr();
  tkrzw::EPrint("a", "b", "c", 1, 2, 3.4, " ", tkrzw::Status(tkrzw::Status::SUCCESS), "\n");
  EXPECT_EQ("abc123.4 SUCCESS\n", testing::internal::GetCapturedStderr());
}

TEST(CmdUtilTest, EPrintL) {
  testing::internal::CaptureStderr();
  tkrzw::EPrintL("a", "b", "c", 1, 2, 3.4, " ", tkrzw::Status(tkrzw::Status::SUCCESS));
  EXPECT_EQ("abc123.4 SUCCESS\n", testing::internal::GetCapturedStderr());
}

TEST(CmdUtilTest, EPrintF) {
  testing::internal::CaptureStderr();
  tkrzw::EPrintF("%s %06d %06.2f\n", "Hello", 123, 4.5);
  EXPECT_EQ("Hello 000123 004.50\n", testing::internal::GetCapturedStderr());
}

TEST(CmdUtilTest, EPutChar) {
  testing::internal::CaptureStderr();
  tkrzw::EPutChar('.');
  tkrzw::EPutChar('.');
  tkrzw::EPutChar('.');
  EXPECT_EQ("...", testing::internal::GetCapturedStderr());
}

TEST(CmdUtilTest, ParseCommandArguments) {
  {
    const char* argv[] = {"cmd", "one", "two"};
    constexpr int32_t argc = std::extent<decltype(argv), 0>::value;
    const std::map<std::string, int32_t> configs = {};
    std::map<std::string, std::vector<std::string>> result;
    std::string error_message;
    EXPECT_TRUE(tkrzw::ParseCommandArguments(argc, argv, configs, &result, &error_message));
    EXPECT_THAT(result[""], ElementsAre("one", "two"));
  }
  {
    const char* argv[] = {"cmd", "-v", "-i", "123", "-p", "45", "67", "--", "one", "two", "-3"};
    constexpr int32_t argc = std::extent<decltype(argv), 0>::value;
    const std::map<std::string, int32_t> configs = {{"-v", 0}, {"-i", 1}, {"-p", 2}};
    std::map<std::string, std::vector<std::string>> result;
    std::string error_message;
    EXPECT_TRUE(tkrzw::ParseCommandArguments(argc, argv, configs, &result, &error_message));
    EXPECT_THAT(result[""], ElementsAre("one", "two", "-3"));
    EXPECT_THAT(result["-v"], ElementsAre());
    EXPECT_THAT(result["-i"], ElementsAre("123"));
    EXPECT_THAT(result["-p"], ElementsAre("45", "67"));
  }
  {
    const char* argv[] = {"cmd", "-v"};
    constexpr int32_t argc = std::extent<decltype(argv), 0>::value;
    const std::map<std::string, int32_t> configs = {};
    std::map<std::string, std::vector<std::string>> result;
    std::string error_message;
    EXPECT_FALSE(tkrzw::ParseCommandArguments(argc, argv, configs, &result, &error_message));
    EXPECT_EQ("invalid option: -v", error_message);
  }
  {
    const char* argv[] = {"cmd", "-v", "-v"};
    constexpr int32_t argc = std::extent<decltype(argv), 0>::value;
    const std::map<std::string, int32_t> configs = {{"-v", 0}};
    std::map<std::string, std::vector<std::string>> result;
    std::string error_message;
    EXPECT_FALSE(tkrzw::ParseCommandArguments(argc, argv, configs, &result, &error_message));
    EXPECT_EQ("duplicated option: -v", error_message);
  }
  {
    const char* argv[] = {"cmd", "-v"};
    constexpr int32_t argc = std::extent<decltype(argv), 0>::value;
    const std::map<std::string, int32_t> configs = {{"-v", 1}};
    std::map<std::string, std::vector<std::string>> result;
    std::string error_message;
    EXPECT_FALSE(tkrzw::ParseCommandArguments(argc, argv, configs, &result, &error_message));
    EXPECT_EQ("fewer arguments: -v", error_message);
  }
  {
    const char* argv[] = {"cmd", "1", "2"};
    constexpr int32_t argc = std::extent<decltype(argv), 0>::value;
    const std::map<std::string, int32_t> configs = {{"", 1}};
    std::map<std::string, std::vector<std::string>> result;
    std::string error_message;
    EXPECT_FALSE(tkrzw::ParseCommandArguments(argc, argv, configs, &result, &error_message));
    EXPECT_EQ("too many arguments", error_message);
  }
  {
    const char* argv[] = {"cmd", "1", "2"};
    constexpr int32_t argc = std::extent<decltype(argv), 0>::value;
    const std::map<std::string, int32_t> configs = {{"", 3}};
    std::map<std::string, std::vector<std::string>> result;
    std::string error_message;
    EXPECT_FALSE(tkrzw::ParseCommandArguments(argc, argv, configs, &result, &error_message));
    EXPECT_EQ("too few arguments", error_message);
  }
}

TEST(CmdUtilTest, GetArgument) {
  const std::map<std::string, std::vector<std::string>> args = {
    {"-s", {"one", "two"}},
    {"-i", {"1", "2"}},
    {"-d", {"1.5", "2.5"}},
  };
  EXPECT_EQ("one", tkrzw::GetStringArgument(args, "-s", 0, "foo"));
  EXPECT_EQ("two", tkrzw::GetStringArgument(args, "-s", 1, "foo"));
  EXPECT_EQ("foo", tkrzw::GetStringArgument(args, "-s", 2, "foo"));
  EXPECT_EQ("foo", tkrzw::GetStringArgument(args, "-x", 0, "foo"));
  EXPECT_EQ(1, tkrzw::GetIntegerArgument(args, "-i", 0, -1));
  EXPECT_EQ(2, tkrzw::GetIntegerArgument(args, "-i", 1, -1));
  EXPECT_EQ(-1, tkrzw::GetIntegerArgument(args, "-i", 2, -1));
  EXPECT_EQ(-1, tkrzw::GetIntegerArgument(args, "-x", 0, -1));
  EXPECT_DOUBLE_EQ(1.5, tkrzw::GetDoubleArgument(args, "-d", 0, -1.0));
  EXPECT_DOUBLE_EQ(2.5, tkrzw::GetDoubleArgument(args, "-d", 1, -1.0));
  EXPECT_DOUBLE_EQ(-1.0, tkrzw::GetDoubleArgument(args, "-d", 2, -1.0));
  EXPECT_DOUBLE_EQ(-1.0, tkrzw::GetDoubleArgument(args, "-x", 0, -1.0));
}

TEST(CmdUtilTest, Die) {
  EXPECT_THROW({
      tkrzw::Die("foo");
    }, tkrzw::StatusException);
  try {
      tkrzw::Die("bar");
  } catch (const tkrzw::StatusException& e) {
    EXPECT_EQ(tkrzw::Status::APPLICATION_ERROR, e.GetStatus());
    EXPECT_EQ("bar", e.GetStatus().GetMessage());
  }
}

TEST(CmdUtilTest, MakeFileOrDie) {
  EXPECT_EQ(typeid(tkrzw::MemoryMapParallelFile),
            tkrzw::MakeFileOrDie("mmap-para", 0, 0)->GetType());
  EXPECT_EQ(typeid(tkrzw::MemoryMapParallelFile),
            tkrzw::MakeFileOrDie("mmap-para", 1 << 10, 2)->GetType());
  EXPECT_EQ(typeid(tkrzw::MemoryMapAtomicFile),
            tkrzw::MakeFileOrDie("mmap-atom", 1 << 10, 2)->GetType());
  EXPECT_EQ(typeid(tkrzw::PositionalParallelFile),
            tkrzw::MakeFileOrDie("pos-para", 1 << 10, 2)->GetType());
  EXPECT_EQ(typeid(tkrzw::PositionalAtomicFile),
            tkrzw::MakeFileOrDie("pos-atom", 1 << 10, 2)->GetType());
  EXPECT_THROW({
      tkrzw::MakeFileOrDie("foo", 0, 0);
    }, tkrzw::StatusException);
}

TEST(CmdUtilTest, PrintDBMRecordsInTSV) {
  testing::internal::CaptureStdout();
  tkrzw::StdTreeDBM dbm;
  dbm.Set("\taa", "\nA\tA\x7f");
  dbm.Set("bb\n", std::string(" B\tB\x00", 5));
  tkrzw::PrintDBMRecordsInTSV(&dbm);
  EXPECT_EQ(" aa\t A A \nbb \t B B \n", testing::internal::GetCapturedStdout());
}

TEST(CmdUtilTest, MakeCyclishText) {
  int32_t seed = 0;
  for (int32_t size = 1; size < 1024; size *= 2) {
    const std::string text = tkrzw::MakeCyclishText(size, seed++);
    EXPECT_EQ(size, text.size());
    for (auto c : text) {
      EXPECT_TRUE(c == ' ' || (c >= 'a' && c <= 'z'));
    }
  }
}

TEST(CmdUtilTest, MakeNaturalishText) {
  int32_t seed = 0;
  for (int32_t size = 1; size < 1024; size *= 2) {
    const std::string text = tkrzw::MakeNaturalishText(size, seed++);
    EXPECT_EQ(size, text.size());
    for (auto c : text) {
      EXPECT_TRUE(c == ' ' || (c >= 'a' && c <= 'z'));
    }
  }
}

// END OF FILE
