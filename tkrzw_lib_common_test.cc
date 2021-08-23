/*************************************************************************************************
 * Tests for tkrzw_lib_common.h
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

TEST(LibCommonTest, Constants) {
  EXPECT_EQ(-128, tkrzw::INT8MIN);
  EXPECT_EQ(127, tkrzw::INT8MAX);
  EXPECT_EQ(0xFF, tkrzw::UINT8MAX);
  EXPECT_EQ(-32768, tkrzw::INT16MIN);
  EXPECT_EQ(32767, tkrzw::INT16MAX);
  EXPECT_EQ(0xFFFF, tkrzw::UINT16MAX);
  EXPECT_EQ(-2147483648, tkrzw::INT32MIN);
  EXPECT_EQ(2147483647, tkrzw::INT32MAX);
  EXPECT_EQ(0xFFFFFFFF, tkrzw::UINT32MAX);
  EXPECT_EQ(static_cast<int64_t>(-9223372036854775808ULL), tkrzw::INT64MIN);
  EXPECT_EQ(9223372036854775807, tkrzw::INT64MAX);
  EXPECT_EQ(0xFFFFFFFFFFFFFFFF, tkrzw::UINT64MAX);
  EXPECT_GT(tkrzw::FLOATMIN, 0.0);
  EXPECT_LT(tkrzw::FLOATMIN, 0.000001);
  EXPECT_GT(tkrzw::FLOATMAX, tkrzw::INT32MAX);
  EXPECT_GT(tkrzw::DOUBLEMIN, 0.0);
  EXPECT_LT(tkrzw::DOUBLEMIN, 0.000001);
  EXPECT_GT(tkrzw::DOUBLEMAX, tkrzw::INT64MAX);
  EXPECT_TRUE(std::isnan(tkrzw::DOUBLENAN));
  EXPECT_TRUE(std::isinf(tkrzw::DOUBLEINF));
  EXPECT_GE(tkrzw::NUM_BUFFER_SIZE, 22);
  EXPECT_GE(tkrzw::MAX_MEMORY_SIZE, 1LL << 32);
  EXPECT_GE(tkrzw::PAGE_SIZE, 256);
  EXPECT_GT(std::strlen(tkrzw::PACKAGE_VERSION), 0);
  EXPECT_GT(std::strlen(tkrzw::LIBRARY_VERSION), 0);
  EXPECT_GT(std::strlen(tkrzw::OS_NAME), 0);
}

TEST(LibCommonTest, ByteOrder) {
  const uint32_t num = 0xDEADBEAF;
  const uint8_t* const bytes = reinterpret_cast<const uint8_t*>(&num);
  if (tkrzw::IS_BIG_ENDIAN) {
    EXPECT_EQ(0xDE, bytes[0]);
    EXPECT_EQ(0xAD, bytes[1]);
    EXPECT_EQ(0xBE, bytes[2]);
    EXPECT_EQ(0xAF, bytes[3]);
  } else {
    EXPECT_EQ(0xAF, bytes[0]);
    EXPECT_EQ(0xBE, bytes[1]);
    EXPECT_EQ(0xAD, bytes[2]);
    EXPECT_EQ(0xDE, bytes[3]);
  }
}

TEST(LibCommonTest, XMalloc) {
  for (size_t size = 1; size <= 8192; size *= 2) {
    const std::string str(size, 'z');
    void* ptr = tkrzw::xmalloc(size);
    EXPECT_NE(nullptr, ptr);
    std::memset(ptr, 'z', size);
    ptr = tkrzw::xrealloc(ptr, size + 1);
    EXPECT_NE(nullptr, ptr);
    EXPECT_EQ(0, std::memcmp(ptr, str.data(), size));
    tkrzw::xfree(ptr);
  }
  for (size_t size = 1; size <= 8192; size *= 2) {
    const std::string str(size, 0);
    void* ptr = tkrzw::xcalloc(1, size);
    EXPECT_NE(nullptr, ptr);
    ptr = tkrzw::xrealloc(ptr, size + 1);
    EXPECT_NE(nullptr, ptr);
    EXPECT_EQ(0, std::memcmp(ptr, str.data(), size));
    tkrzw::xfree(ptr);
  }
  void* ptr = tkrzw::xreallocappend(nullptr, 1);
  EXPECT_NE(nullptr, ptr);
  for (size_t size = 1; size <= 8192; size *= 2) {
    ptr = tkrzw::xreallocappend(ptr, size);
    EXPECT_NE(nullptr, ptr);
  }
  tkrzw::xfree(ptr);
  for (size_t size = sizeof(void*); size <= 8192; size *= 2) {
    ptr = tkrzw::xmallocaligned(512, std::max<int32_t>(size, 512));
    EXPECT_NE(nullptr, ptr);
    EXPECT_EQ(0, reinterpret_cast<intptr_t>(ptr) % 512);
    std::memset(ptr, 0, size);
    tkrzw::xfreealigned(ptr);
  }
}

TEST(LibCommonTest, CheckSet) {
  const std::set<int32_t> int_set = {1, 2, 3};
  EXPECT_TRUE(tkrzw::CheckSet(int_set, 2));
  EXPECT_TRUE(tkrzw::CheckSet(int_set, 3));
  EXPECT_FALSE(tkrzw::CheckSet(int_set, 4));
  const std::set<std::string> str_set = {"a", "b", "c"};
  EXPECT_TRUE(tkrzw::CheckSet(str_set, "b"));
  EXPECT_TRUE(tkrzw::CheckSet(str_set, "c"));
  EXPECT_FALSE(tkrzw::CheckSet(str_set, "d"));
}

TEST(LibCommonTest, CheckMap) {
  const std::map<int32_t, int32_t> int_map = {{1, 11}, {2, 22}, {3, 33}};
  EXPECT_TRUE(tkrzw::CheckMap(int_map, 2));
  EXPECT_TRUE(tkrzw::CheckMap(int_map, 3));
  EXPECT_FALSE(tkrzw::CheckMap(int_map, 4));
  const std::map<std::string, std::string> str_map = {{"a", "AA"}, {"b", "BB"}, {"c", "CC"}};
  EXPECT_TRUE(tkrzw::CheckMap(str_map, "b"));
  EXPECT_TRUE(tkrzw::CheckMap(str_map, "c"));
  EXPECT_FALSE(tkrzw::CheckMap(str_map, "d"));
}

TEST(LibCommonTest, SearchMap) {
  const std::map<int32_t, int32_t> int_map = {{1, 11}, {2, 22}, {3, 33}};
  EXPECT_EQ(22, tkrzw::SearchMap(int_map, 2, -1));
  EXPECT_EQ(33, tkrzw::SearchMap(int_map, 3, -1));
  EXPECT_EQ(-1, tkrzw::SearchMap(int_map, 4, -1));
  const std::map<std::string, std::string> str_map = {{"a", "AA"}, {"b", "BB"}, {"c", "CC"}};
  EXPECT_EQ("BB", tkrzw::SearchMap(str_map, "b", "*"));
  EXPECT_EQ("CC", tkrzw::SearchMap(str_map, "c", "*"));
  EXPECT_EQ("*", tkrzw::SearchMap(str_map, "d", "*"));
  const std::map<std::string, int32_t> str_int_map = {{"a", 11}, {"b", 22}, {"c", 33}};
  EXPECT_EQ(11, tkrzw::SearchMap(str_int_map, "a", 0));
  EXPECT_EQ(33, tkrzw::SearchMap(str_int_map, "c", 0));
  EXPECT_EQ(0, tkrzw::SearchMap(str_int_map, "d", 0));
}

TEST(LibCommonTest, GetProcessID) {
  const int64_t pid = tkrzw::GetProcessID();
  EXPECT_GT(pid, 0);
  EXPECT_EQ(pid, tkrzw::GetProcessID());
}

TEST(LibCommonTest, GetSystemInfo) {
  const auto& info = tkrzw::GetSystemInfo();
  EXPECT_TRUE(tkrzw::CheckMap(info, "mem_size"));
  EXPECT_GT(tkrzw::GetMemoryCapacity(), 0);
  EXPECT_GT(tkrzw::GetMemoryUsage(), 0);
}

TEST(LibCommonTest, Status) {
  tkrzw::Status s1;
  EXPECT_EQ(tkrzw::Status::SUCCESS, s1.GetCode());
  EXPECT_EQ("", s1.GetMessage());
  EXPECT_FALSE(s1.HasMessage());
  char* cstr = s1.MakeMessageC();
  EXPECT_STREQ("", cstr);
  tkrzw::xfree(cstr);
  EXPECT_EQ(tkrzw::Status::SUCCESS, s1);
  EXPECT_EQ(s1, tkrzw::Status::SUCCESS);
  EXPECT_EQ("SUCCESS", static_cast<std::string>(s1));
  EXPECT_TRUE(s1.IsOK());
  tkrzw::Status s2(s1);
  EXPECT_EQ(s1, s2);
  s2.Set(tkrzw::Status::UNKNOWN_ERROR, "hello");
  EXPECT_EQ(tkrzw::Status::UNKNOWN_ERROR, s2.GetCode());
  EXPECT_EQ("hello", s2.GetMessage());
  EXPECT_TRUE(s2.HasMessage());
  cstr = s2.MakeMessageC();
  EXPECT_STREQ("hello", cstr);
  tkrzw::xfree(cstr);
  EXPECT_NE(s1, s2);
  EXPECT_NE(s2, s1);
  EXPECT_FALSE(s2.IsOK());
  tkrzw::Status s3;
  s3 = s2;
  EXPECT_EQ(s3, s2);
  s3 = s3 = s3;
  EXPECT_EQ(s3, s2);
  tkrzw::Status s4(std::move(s3));
  EXPECT_EQ(s4, s2);
  tkrzw::Status s5;
  s5 = std::move(s4);
  EXPECT_EQ(s4, s2);
  std::vector<tkrzw::Status> statuses;
  statuses.emplace_back(tkrzw::Status(tkrzw::Status::NOT_FOUND_ERROR, "not found 2"));
  statuses.emplace_back(tkrzw::Status(tkrzw::Status::NOT_FOUND_ERROR, "not found 1"));
  statuses.emplace_back(tkrzw::Status(tkrzw::Status::NOT_IMPLEMENTED_ERROR, "not implemented"));
  statuses.emplace_back(tkrzw::Status(tkrzw::Status::SYSTEM_ERROR, "system"));
  statuses.emplace_back(tkrzw::Status(tkrzw::Status::SUCCESS, "success"));
  std::sort(statuses.begin(), statuses.end());
  EXPECT_EQ("SUCCESS: success", std::string(statuses[0]));
  EXPECT_EQ("SYSTEM_ERROR: system", std::string(statuses[1]));
  EXPECT_EQ("NOT_IMPLEMENTED_ERROR: not implemented", std::string(statuses[2]));
  EXPECT_EQ("NOT_FOUND_ERROR: not found 1", std::string(statuses[3]));
  EXPECT_EQ("NOT_FOUND_ERROR: not found 2", ToString(statuses[4]));
  tkrzw::Status s6(tkrzw::Status::SUCCESS, "s6");
  tkrzw::Status s7(tkrzw::Status::SUCCESS, "s7");
  tkrzw::Status s8(tkrzw::Status::SYSTEM_ERROR, "s8");
  s6 |= s6;
  EXPECT_EQ("SUCCESS: s6", std::string(s6));
  s6 |= s7;
  EXPECT_EQ("SUCCESS: s6", std::string(s6));
  s6 |= s8;
  EXPECT_EQ("SYSTEM_ERROR: s8", std::string(s6));
  s6 |= tkrzw::Status(tkrzw::Status::INFEASIBLE_ERROR, "move 1");
  EXPECT_EQ("SYSTEM_ERROR: s8", std::string(s6));
  s6.Set(tkrzw::Status::SUCCESS, "ok");
  s6 |= tkrzw::Status(tkrzw::Status::INFEASIBLE_ERROR, "move 1");
  EXPECT_EQ("INFEASIBLE_ERROR: move 1", std::string(s6));
}

TEST(LibCommonTest, StatusException) {
  EXPECT_NO_THROW({
      EXPECT_EQ(tkrzw::Status::SUCCESS, tkrzw::Status(tkrzw::Status::SUCCESS).OrDie());
    });
  EXPECT_THROW({
      tkrzw::Status(tkrzw::Status::SYSTEM_ERROR, "foo").OrDie();
    }, tkrzw::StatusException);
  try {
    tkrzw::Status(tkrzw::Status::SYSTEM_ERROR, "foo").OrDie();
  } catch (const tkrzw::StatusException& e) {
    EXPECT_EQ(tkrzw::Status::SYSTEM_ERROR, e.GetStatus());
    EXPECT_EQ("foo", e.GetStatus().GetMessage());
    EXPECT_EQ("SYSTEM_ERROR: foo", std::string(e));
  }
  try {
    throw tkrzw::StatusException(tkrzw::Status(tkrzw::Status::UNKNOWN_ERROR, "bar"));
  } catch (const tkrzw::StatusException& e) {
    EXPECT_EQ(tkrzw::Status::UNKNOWN_ERROR, e.GetStatus());
    EXPECT_EQ("bar", e.GetStatus().GetMessage());
    EXPECT_EQ("UNKNOWN_ERROR: bar", std::string(e));
  }
}

// END OF FILE
