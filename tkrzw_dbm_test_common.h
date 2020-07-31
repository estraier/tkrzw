/*************************************************************************************************
 * Common tests for DBM implementations
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

#include "gtest/gtest.h"
#include "gmock/gmock.h"

#include "tkrzw_dbm.h"
#include "tkrzw_dbm_common_impl.h"
#include "tkrzw_file.h"
#include "tkrzw_file_util.h"
#include "tkrzw_lib_common.h"
#include "tkrzw_str_util.h"
#include "tkrzw_sys_config.h"

using namespace testing;

class CommonDBMTest : public Test {
 protected:
  void FileTest(tkrzw::DBM* dbm, const std::string& path);
  void LargeRecordTest(tkrzw::DBM* dbm);
  void BasicTest(tkrzw::DBM* dbm);
  void SequenceTest(tkrzw::DBM* dbm);
  void AppendTest(tkrzw::DBM* dbm);
  void ProcessTest(tkrzw::DBM* dbm);
  void ProcessEachTest(tkrzw::DBM* dbm);
  void RandomTest(tkrzw::DBM* dbm, int32_t seed);
  void RandomTestThread(tkrzw::DBM* dbm);
  void RebuildRandomTest(tkrzw::DBM* dbm);
  void RecordMigrationTest(tkrzw::DBM* dbm, tkrzw::File* file);
  void BackIteratorTest(tkrzw::DBM* dbm);
  void IteratorBoundTest(tkrzw::DBM* dbm);
};

inline void CommonDBMTest::FileTest(tkrzw::DBM* dbm, const std::string& path) {
  const std::string copy_path = path + ".copy";
  EXPECT_FALSE(dbm->IsOpen());
  EXPECT_FALSE(dbm->IsWritable());
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Open(path, true));
  EXPECT_TRUE(dbm->IsOpen());
  EXPECT_TRUE(dbm->IsWritable());
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Set("a", "AA"));
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Set("bb", "BBB"));
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Set("ccc", "CCCC"));
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Synchronize(false));
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Remove("bb"));
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Close());
  EXPECT_GT(tkrzw::GetFileSize(path), 0);
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Open(path, true));
  EXPECT_EQ(2, dbm->CountSimple());
  EXPECT_EQ("AA", dbm->GetSimple("a"));
  EXPECT_EQ("CCCC", dbm->GetSimple("ccc"));
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Close());
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Open(path, true));
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Set("dddd", "DDDDD"));
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Synchronize(false));
  EXPECT_GT(dbm->GetFileSizeSimple(), 0);
  EXPECT_EQ(3, dbm->CountSimple());
  EXPECT_EQ("AA", dbm->GetSimple("a"));
  EXPECT_EQ("CCCC", dbm->GetSimple("ccc"));
  EXPECT_EQ("DDDDD", dbm->GetSimple("dddd"));
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Set("eeeee", "EEEEEE"));
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->CopyFile(copy_path));
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Close());
  EXPECT_EQ(tkrzw::GetFileSize(path), tkrzw::GetFileSize(copy_path));
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Open(copy_path, true, tkrzw::File::OPEN_NO_CREATE));
  EXPECT_EQ(4, dbm->CountSimple());
  EXPECT_EQ("AA", dbm->GetSimple("a"));
  EXPECT_EQ("CCCC", dbm->GetSimple("ccc"));
  EXPECT_EQ("DDDDD", dbm->GetSimple("dddd"));
  EXPECT_EQ("EEEEEE", dbm->GetSimple("eeeee"));
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Set("x", "XX"));
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Synchronize(true));
  EXPECT_EQ("XX", dbm->GetSimple("x"));
  const std::string export_path = path + ".export";
  auto new_dbm = dbm->MakeDBM();
  EXPECT_EQ(tkrzw::Status::SUCCESS, new_dbm->Open(export_path, true));
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Export(new_dbm.get()));
  EXPECT_EQ(5, dbm->CountSimple());
  EXPECT_EQ("AA", dbm->GetSimple("a"));
  EXPECT_EQ("CCCC", dbm->GetSimple("ccc"));
  EXPECT_EQ("DDDDD", dbm->GetSimple("dddd"));
  EXPECT_EQ("EEEEEE", dbm->GetSimple("eeeee"));
  EXPECT_EQ("XX", dbm->GetSimple("x"));
  EXPECT_EQ(tkrzw::Status::SUCCESS, new_dbm->Close());
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Close());
}

inline void CommonDBMTest::LargeRecordTest(tkrzw::DBM* dbm) {
  constexpr int32_t max_key_size = 128 * 128;
  constexpr int32_t max_value_size = 128 * 128 * 128;
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Set("", ""));
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Synchronize(false));
  EXPECT_EQ(1, dbm->CountSimple());
  int32_t count = 0;
  for (int32_t key_size = 1; key_size <= max_key_size; key_size *= 2) {
    const std::string key_prefix(key_size, 'k');
    for (int32_t value_size = 1; value_size <= max_value_size; value_size *= 2) {
      const std::string key = tkrzw::StrCat(key_prefix, ":", count * count);
      const std::string value(value_size, 'v');
      EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Set(key, value));
      count++;
    }
  }
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Synchronize(false));
  EXPECT_EQ(count + 1, dbm->CountSimple());
  count = 0;
  for (int32_t key_size = 1; key_size <= max_key_size; key_size *= 2) {
    const std::string key_prefix(key_size, 'k');
    for (int32_t value_size = 1; value_size <= max_value_size; value_size *= 2) {
      const std::string key = tkrzw::StrCat(key_prefix, ":", count * count);
      std::string value;
      EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Get(key, &value));
      EXPECT_EQ(std::string(value_size, 'v'), value);
      count++;
    }
  }
  count = 0;
  for (int32_t key_size = 1; key_size <= max_key_size; key_size *= 2) {
    const std::string key_prefix(key_size, 'k');
    for (int32_t value_size = 1; value_size <= max_value_size; value_size *= 2) {
      const std::string key = tkrzw::StrCat(key_prefix, ":", count * count);
      EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Remove(key));
      count++;
    }
  }
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Synchronize(false));
  EXPECT_EQ(1, dbm->CountSimple());
}

inline void CommonDBMTest::BasicTest(tkrzw::DBM* dbm) {
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Set("one", "ichi"));
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Set("two", "ni"));
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Set("three", "san"));
  EXPECT_EQ(tkrzw::Status::DUPLICATION_ERROR, dbm->Set("three", "SAN", false));
  int64_t count = 0;
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Count(&count));
  EXPECT_EQ(3, count);
  std::string key, value;
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Get("one", &value));
  EXPECT_EQ("ichi", value);
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Get("two", &value));
  EXPECT_EQ("ni", value);
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Get("three", &value));
  EXPECT_EQ("san", value);
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Get("three"));
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Remove("three"));
  EXPECT_EQ(tkrzw::Status::NOT_FOUND_ERROR, dbm->Remove("four"));
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Count(&count));
  EXPECT_EQ(2, count);
  EXPECT_EQ(tkrzw::Status::NOT_FOUND_ERROR, dbm->Get("three", &value));
  EXPECT_EQ(tkrzw::Status::NOT_FOUND_ERROR, dbm->Get("three"));
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Set("three", "SANSAN"));
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Get("three", &value));
  EXPECT_EQ("SANSAN", value);
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Set("three", "SANSANSAN"));
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Get("three", &value));
  EXPECT_EQ("SANSANSAN", value);
  EXPECT_EQ("SANSANSAN", dbm->GetSimple("three"));
  EXPECT_EQ("*", dbm->GetSimple("foobar", "*"));
  EXPECT_EQ(3, dbm->CountSimple());
  auto iter = dbm->MakeIterator();
  EXPECT_EQ(tkrzw::Status::SUCCESS, iter->First());
  std::vector<std::string> keys, values;
  while (true) {
    const tkrzw::Status status = iter->Get(&key, &value);
    if (status != tkrzw::Status::SUCCESS) {
      EXPECT_EQ(tkrzw::Status::NOT_FOUND_ERROR, status);
      EXPECT_EQ(tkrzw::Status::NOT_FOUND_ERROR, iter->Get());
      EXPECT_EQ("*", iter->GetKey("*"));
      EXPECT_EQ("*", iter->GetValue("*"));
      break;
    }
    keys.emplace_back(key);
    values.emplace_back(value);
    EXPECT_EQ(key, iter->GetKey());
    EXPECT_EQ(value, iter->GetValue());
    EXPECT_EQ(tkrzw::Status::SUCCESS, iter->Get());
    EXPECT_EQ(tkrzw::Status::SUCCESS, iter->Next());
  }
  EXPECT_THAT(keys, UnorderedElementsAre("one", "two", "three"));
  EXPECT_THAT(values, UnorderedElementsAre("ichi", "ni", "SANSANSAN"));
  auto iter1 = dbm->MakeIterator();
  EXPECT_EQ(tkrzw::Status::SUCCESS, iter1->Jump("one"));
  EXPECT_EQ("one", iter1->GetKey());
  auto iter2 = dbm->MakeIterator();
  EXPECT_EQ(tkrzw::Status::SUCCESS, iter2->Jump("two"));
  EXPECT_EQ("two", iter2->GetKey());
  auto iter3 = dbm->MakeIterator();
  EXPECT_EQ(tkrzw::Status::SUCCESS, iter3->Jump("three"));
  EXPECT_EQ("three", iter3->GetKey());
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Remove("one"));
  auto status = iter1->Get();
  EXPECT_TRUE(status == tkrzw::Status::SUCCESS || status == tkrzw::Status::NOT_FOUND_ERROR);
  status = iter2->Get();
  EXPECT_TRUE(status == tkrzw::Status::SUCCESS || status == tkrzw::Status::NOT_FOUND_ERROR);
  status = iter3->Get();
  EXPECT_TRUE(status == tkrzw::Status::SUCCESS || status == tkrzw::Status::NOT_FOUND_ERROR);
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Remove("two"));
  status = iter1->Get();
  EXPECT_TRUE(status == tkrzw::Status::SUCCESS || status == tkrzw::Status::NOT_FOUND_ERROR);
  status = iter2->Get();
  EXPECT_TRUE(status == tkrzw::Status::SUCCESS || status == tkrzw::Status::NOT_FOUND_ERROR);
  status = iter3->Get();
  EXPECT_TRUE(status == tkrzw::Status::SUCCESS || status == tkrzw::Status::NOT_FOUND_ERROR);
  EXPECT_EQ(tkrzw::Status::SUCCESS, iter->Jump("three"));
  EXPECT_EQ(tkrzw::Status::SUCCESS, iter->Set("hello"));
  EXPECT_EQ("three", iter->GetKey());
  EXPECT_EQ("hello", iter->GetValue());
  EXPECT_EQ(tkrzw::Status::SUCCESS, iter->Remove());
  EXPECT_EQ(tkrzw::Status::NOT_FOUND_ERROR, iter->Remove());
  EXPECT_EQ(tkrzw::Status::NOT_FOUND_ERROR, iter->Get());
  status = iter3->Get();
  EXPECT_TRUE(status == tkrzw::Status::SUCCESS || status == tkrzw::Status::NOT_FOUND_ERROR);
  EXPECT_EQ(0, dbm->CountSimple());
  for (size_t size = 1; size <= 32768; size *= 2) {
    const std::string value(size, '1');
    EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Set("1", value));
    EXPECT_EQ(value, dbm->GetSimple("1"));
  }
  for (size_t size = 32768; size >= 1; size /= 2) {
    const std::string value(size, '2');
    EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Set("2", value));
    EXPECT_EQ(value, dbm->GetSimple("2"));
  }
  for (size_t size = 1; size <= 32768; size *= 2) {
    const std::string value(size, '3');
    EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Set("3", value));
    EXPECT_EQ(value, dbm->GetSimple("3"));
    EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Remove("3"));
  }
  for (size_t size = 32768; size >= 1; size /= 2) {
    const std::string value(size, '4');
    EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Set("4", value));
    EXPECT_EQ(value, dbm->GetSimple("4"));
    EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Remove("4"));
  }
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Remove("1"));
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Set("1", "first"));
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Set("2", "second"));
  EXPECT_EQ(2, dbm->CountSimple());
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Rebuild());
  EXPECT_EQ("first", dbm->GetSimple("1"));
  EXPECT_EQ("second", dbm->GetSimple("2"));
  EXPECT_EQ(2, dbm->CountSimple());
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Clear());
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->SetMulti(
      {{"one", "first"}, {"two", "second"}, {"three", "third"}, {"three", "foo"}}, true));
  std::map<std::string, std::string> multi_records =
      {{"four", "fourth"}, {"five", "fifth"}};
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->SetMulti(multi_records, true));
  EXPECT_EQ(5, dbm->CountSimple());
  EXPECT_EQ("first", dbm->GetSimple("one"));
  EXPECT_EQ("second", dbm->GetSimple("two"));
  EXPECT_EQ("foo", dbm->GetSimple("three"));
  EXPECT_EQ("fifth", dbm->GetSimple("five"));
  multi_records = dbm->GetMulti({"one", "two", "three", "four", "five"});
  EXPECT_EQ(5, multi_records.size());
  EXPECT_EQ("first", multi_records["one"]);
  EXPECT_EQ("second", multi_records["two"]);
  EXPECT_EQ("foo", multi_records["three"]);
  EXPECT_EQ("fifth", multi_records["five"]);
  const std::vector<std::string> multi_keys = {"one", "two", "three", "four", "five"};
  multi_records = dbm->GetMulti(multi_keys);
  EXPECT_EQ(5, multi_records.size());
  EXPECT_EQ("first", multi_records["one"]);
  EXPECT_EQ("second", multi_records["two"]);
  EXPECT_EQ("foo", multi_records["three"]);
  EXPECT_EQ("fifth", multi_records["five"]);
}

inline void CommonDBMTest::SequenceTest(tkrzw::DBM* dbm) {
  constexpr int32_t num_var_records = 100;
  constexpr int32_t num_seq_records = 100;
  for (int32_t i = 0; i < num_var_records; i++) {
    const std::string key(i, 'k');
    const std::string value(i, 'v');
    EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Set(key, value));
    std::string got_value;
    EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Get(key, &got_value));
    EXPECT_EQ(value, got_value);
  }
  EXPECT_EQ(num_var_records, dbm->CountSimple());
  for (int32_t i = 1; i <= num_seq_records; i++) {
    const std::string key = tkrzw::ToString(i);
    EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Set(key, key));
    std::string got_value;
    EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Get(key, &got_value));
    EXPECT_EQ(key, got_value);
  }
  EXPECT_EQ(num_var_records + num_seq_records, dbm->CountSimple());
  std::map<std::string, std::string> expected;
  for (int32_t i = 0; i < num_var_records; i++) {
    const std::string key(i, 'k');
    const std::string value(num_var_records - i - 1, 'v');
    if (i % 2 == 0) {
      const std::string value(num_var_records - i - 1, 'v');
      EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Set(key, value));
      std::string got_value;
      EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Get(key, &got_value));
      expected.emplace(key, got_value);
    } else {
      EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Remove(key));
      EXPECT_EQ(tkrzw::Status::NOT_FOUND_ERROR, dbm->Get(key, nullptr));
    }
  }
  for (int32_t i = 1; i <= num_seq_records; i++) {
    const std::string key = tkrzw::ToString(i);
    if (i % 2 == 0) {
      EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Set(key, "*"));
      std::string got_value;
      EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Get(key, &got_value));
      EXPECT_EQ("*", got_value);
      expected.emplace(key, got_value);
    } else {
      EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Remove(key));
      EXPECT_EQ(tkrzw::Status::NOT_FOUND_ERROR, dbm->Get(key, nullptr));
    }
  }
  EXPECT_EQ(expected.size(), dbm->CountSimple());
  auto iter = dbm->MakeIterator();
  EXPECT_EQ(tkrzw::Status::SUCCESS, iter->First());
  int32_t count = 0;
  while (true) {
    std::string key, value;
    const tkrzw::Status status = iter->Get(&key, &value);
    if (status != tkrzw::Status::SUCCESS) {
      EXPECT_EQ(tkrzw::Status::NOT_FOUND_ERROR, status);
      break;
    }
    EXPECT_EQ(tkrzw::SearchMap(expected, key, "-"), value);
    count++;
    EXPECT_EQ(tkrzw::Status::SUCCESS, iter->Next());
  }
  EXPECT_EQ(count, expected.size());
  EXPECT_EQ(tkrzw::Status::SUCCESS, iter->First());
  while (true) {
    const tkrzw::Status status = iter->Remove();
    if (status != tkrzw::Status::SUCCESS) {
      EXPECT_EQ(tkrzw::Status::NOT_FOUND_ERROR, status);
      break;
    }
  }
  EXPECT_EQ(0, dbm->CountSimple());
}

inline void CommonDBMTest::AppendTest(tkrzw::DBM* dbm) {
  constexpr int64_t num_iterations = 300;
  constexpr int64_t num_records = 100;
  std::string even_value, odd_value;
  for (int32_t i = 1; i <= num_iterations; i++) {
    const std::string value = tkrzw::ToString(i * i);
    for (int32_t key_num = 1; key_num <= num_records; key_num++) {
      const std::string key = tkrzw::ToString(key_num * key_num);
      if (key_num % 2 == 0) {
        EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Append(key, value, ","));
      } else {
        EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Append(key, value));
      }
    }
    if (!even_value.empty()) {
      even_value.append(",");
    }
    even_value.append(value);
    odd_value.append(value);
  }
  for (int32_t key_num = 1; key_num <= num_records; key_num++) {
    const std::string key = tkrzw::ToString(key_num * key_num);
    if (key_num % 2 == 0) {
      EXPECT_EQ(even_value, dbm->GetSimple(key));
    } else {
      EXPECT_EQ(odd_value, dbm->GetSimple(key));
    }
  }
}

inline void CommonDBMTest::ProcessTest(tkrzw::DBM* dbm) {
  class ConcatProc : public tkrzw::DBM::RecordProcessor {
   public:
    ConcatProc(std::string_view value, bool append) : value_(value), append_(append) {}
    std::string_view ProcessFull(std::string_view key, std::string_view value) override {
      if (append_) {
        new_value_ = std::string(value);
        new_value_.append(value_);
      } else {
        new_value_ = std::string(value_);
        new_value_.append(value);
      }
      return new_value_;
    }
    std::string_view ProcessEmpty(std::string_view key) override {
      return value_;
    }
   private:
    std::string_view value_;
    std::string new_value_;
    bool append_;
  };
  {
    ConcatProc append_proc("0", true);
    EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Process("a", &append_proc, true));
  }
  {
    ConcatProc append_proc("1", true);
    EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Process("a", &append_proc, true));
  }
  {
    ConcatProc append_proc("2", true);
    EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Process("a", &append_proc, true));
  }
  EXPECT_EQ("012", dbm->GetSimple("a"));
  {
    ConcatProc prepend_proc("0", false);
    EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Process("p", &prepend_proc, true));
  }
  {
    ConcatProc prepend_proc("1", false);
    EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Process("p", &prepend_proc, true));
  }
  {
    ConcatProc prepend_proc("2", false);
    EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Process("p", &prepend_proc, true));
  }
  EXPECT_EQ("210", dbm->GetSimple("p"));
  auto iter = dbm->MakeIterator();
  EXPECT_EQ(tkrzw::Status::SUCCESS, iter->First());
  while (true) {
    const std::string key = iter->GetKey();
    if (key.empty()) {
      break;
    }
    ConcatProc append_proc("Z", true);
    EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Process(key, &append_proc, true));
    EXPECT_EQ(tkrzw::Status::SUCCESS, iter->Next());
  }
  EXPECT_EQ("012Z", dbm->GetSimple("a"));
  EXPECT_EQ("210Z", dbm->GetSimple("p"));
  class SizeProc : public tkrzw::DBM::RecordProcessor {
   public:
    SizeProc() : size_(-1) {}
    std::string_view ProcessFull(std::string_view key, std::string_view value) override {
      size_ = value.size();
      return NOOP;
    }
    int32_t Size() {
      return size_;
    }
   private:
    int32_t size_;
  };
  {
    SizeProc size_proc;
    EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Process("x", &size_proc, false));
    EXPECT_EQ(-1, size_proc.Size());
  }
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Set("x", "abcde"));
  {
    SizeProc size_proc;
    EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Process("x", &size_proc, false));
    EXPECT_EQ(5, size_proc.Size());
  }
  EXPECT_EQ(tkrzw::Status::SUCCESS, iter->First());
  std::vector<int32_t> sizes;
  while (true) {
    const std::string key = iter->GetKey();
    if (key.empty()) {
      break;
    }
    SizeProc size_proc;
    const tkrzw::Status status = dbm->Process(key, &size_proc, false);
    if (status != tkrzw::Status::SUCCESS) {
      EXPECT_EQ(tkrzw::Status::NOT_FOUND_ERROR, status);
      break;
    }
    sizes.emplace_back(size_proc.Size());
    EXPECT_EQ(tkrzw::Status::SUCCESS, iter->Next());
  }
  EXPECT_EQ("210Z", dbm->GetSimple("p"));
  EXPECT_THAT(sizes, UnorderedElementsAre(4, 4, 5));
  EXPECT_EQ(3, dbm->CountSimple());
  class RemoveProc : public tkrzw::DBM::RecordProcessor {
   public:
    std::string_view ProcessFull(std::string_view key, std::string_view value) override {
      return REMOVE;
    }
    std::string_view ProcessEmpty(std::string_view key) override {
      return REMOVE;
    }
  };
  {
    RemoveProc remove_proc;
    EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Process("p", &remove_proc, true));
  }
  EXPECT_EQ(2, dbm->CountSimple());
  {
    RemoveProc remove_proc;
    EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Process("p", &remove_proc, true));
  }
  EXPECT_EQ(tkrzw::Status::SUCCESS, iter->First());
  while (true) {
    ConcatProc prepend_proc("BEGIN:", false);
    const tkrzw::Status status = iter->Process(&prepend_proc, true);
    if (status != tkrzw::Status::SUCCESS) {
      EXPECT_EQ(tkrzw::Status::NOT_FOUND_ERROR, status);
      break;
    }
    EXPECT_EQ(tkrzw::Status::SUCCESS, iter->Next());
  }
  EXPECT_EQ(tkrzw::Status::SUCCESS, iter->First());
  while (true) {
    std::string key, value;
    tkrzw::DBM::RecordProcessorIteratorGet get_proc(&key, &value);
    tkrzw::Status status = iter->Process(&get_proc, false);
    if (status != tkrzw::Status::SUCCESS) {
      EXPECT_EQ(tkrzw::Status::NOT_FOUND_ERROR, status);
      break;
    }
    EXPECT_TRUE(tkrzw::StrBeginsWith(value, "BEGIN:"));
    tkrzw::DBM::RecordProcessorIteratorSet set_proc(key);
    EXPECT_EQ(tkrzw::Status::SUCCESS, iter->Process(&set_proc, true));
    EXPECT_EQ(tkrzw::Status::SUCCESS, iter->Next());
  }
  auto second_iter = dbm->MakeIterator();
  EXPECT_EQ(tkrzw::Status::SUCCESS, iter->First());
  EXPECT_EQ(tkrzw::Status::SUCCESS, second_iter->First());
  int32_t count = 0;
  while (true) {
    std::string key, value;
    tkrzw::DBM::RecordProcessorIteratorGet get_proc(&key, &value);
    tkrzw::Status status = iter->Process(&get_proc, false);
    if (status == tkrzw::Status::SUCCESS) {
      EXPECT_EQ(key, value);
      count++;
    } else {
      EXPECT_EQ(tkrzw::Status::NOT_FOUND_ERROR, status);
    }
    tkrzw::DBM::RecordProcessorIteratorRemove remove_proc;
    status = iter->Process(&remove_proc, true);
    if (status != tkrzw::Status::SUCCESS) {
      EXPECT_EQ(tkrzw::Status::NOT_FOUND_ERROR, status);
      break;
    }
  }
  EXPECT_EQ(2, count);
  EXPECT_EQ(0, dbm->CountSimple());
  EXPECT_EQ(tkrzw::Status::NOT_FOUND_ERROR, iter->Get());
  const tkrzw::Status status = second_iter->Get();
  EXPECT_TRUE(status == tkrzw::Status::SUCCESS || status == tkrzw::Status::NOT_FOUND_ERROR);
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Append("1234", "foo", ","));
  EXPECT_EQ("foo", dbm->GetSimple("1234"));
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Append("1234", "bar", ","));
  EXPECT_EQ("foo,bar", dbm->GetSimple("1234"));
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Append("1234", "baz"));
  EXPECT_EQ("foo,barbaz", dbm->GetSimple("1234"));
  EXPECT_EQ(tkrzw::Status::NOT_FOUND_ERROR, dbm->CompareExchange("a", "foo", "bar"));
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Set("a", "foo"));
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->CompareExchange("a", "foo", "bar"));
  std::string actual;
  EXPECT_EQ(tkrzw::Status::DUPLICATION_ERROR, dbm->CompareExchange("a", "foo", "baz", &actual));
  EXPECT_EQ("bar", actual);
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->CompareExchange("a", "bar", "qux", &actual));
  EXPECT_EQ("bar", actual);
  int64_t current = 0;
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Increment("b", -1, &current, -9));
  EXPECT_EQ(-10, current);
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Increment("b", tkrzw::INT64MIN, &current, 100));
  EXPECT_EQ(-10, current);
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Increment("b", 110));
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Increment("b", 1, &current));
  EXPECT_EQ(101, current);
  EXPECT_EQ(102, dbm->IncrementSimple("b"));
  EXPECT_EQ(0x7FFF000088880066, dbm->IncrementSimple("b", 0x7FFF000088880000));
  EXPECT_EQ(std::string("\x7F\xFF\x00\x00\x88\x88\x00\x66", 8), dbm->GetSimple("b"));
  EXPECT_EQ(100, dbm->IncrementSimple("ccc", tkrzw::INT64MIN, 100));
  EXPECT_EQ(tkrzw::Status::NOT_FOUND_ERROR, dbm->Get("ccc"));
}

inline void CommonDBMTest::ProcessEachTest(tkrzw::DBM* dbm) {
  constexpr int64_t num_records = 1000;
  for (int64_t i = 1; i <= num_records; ++i) {
    const std::string& expr = tkrzw::ToString(i * i);
    EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Set(expr, expr, false));
  }
  auto iter1 = dbm->MakeIterator();
  EXPECT_EQ(tkrzw::Status::SUCCESS, iter1->First());
  for (int64_t i = 1; i <= num_records; i += 4) {
    const std::string& expr = tkrzw::ToString(i * i);
    EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Remove(expr));
    EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Set(expr, expr, false));
  }
  auto iter2 = dbm->MakeIterator();
  EXPECT_EQ(tkrzw::Status::SUCCESS, iter1->Jump("1"));
  class EvenRemoveProc : public tkrzw::DBM::RecordProcessor {
   public:
    std::string_view ProcessFull(std::string_view key, std::string_view value) override {
      full_count_++;
      if (tkrzw::StrToInt(key) % 2 == 0) {
        return REMOVE;
      }
      return NOOP;
    }
    std::string_view ProcessEmpty(std::string_view key) {
      empty_count_++;
      return NOOP;
    }
    int32_t GetFullCount() const {
      return full_count_;
    }
    int32_t GetEmptyCount() const {
      return empty_count_;
    }
   private:
    int32_t full_count_ = 0;
    int32_t empty_count_ = 0;
  } remove_proc;
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->ProcessEach(&remove_proc, true));
  EXPECT_EQ(num_records, remove_proc.GetFullCount());
  EXPECT_EQ(2, remove_proc.GetEmptyCount());
  EXPECT_EQ(num_records / 2, dbm->CountSimple());
  tkrzw::Status status = iter1->Get();
  EXPECT_TRUE(status == tkrzw::Status::SUCCESS || status == tkrzw::Status::NOT_FOUND_ERROR);
  status = iter1->Next();
  EXPECT_TRUE(status == tkrzw::Status::SUCCESS || status == tkrzw::Status::NOT_FOUND_ERROR);
  class IncrementProc : public tkrzw::DBM::RecordProcessor {
   public:
    std::string_view ProcessFull(std::string_view key, std::string_view value) override {
      full_count_++;
      value_ = tkrzw::ToString(tkrzw::StrToInt(value) + 1);
      return value_;
    }
    std::string_view ProcessEmpty(std::string_view key) {
      empty_count_++;
      return NOOP;
    }
    int32_t GetFullCount() const {
      return full_count_;
    }
    int32_t GetEmptyCount() const {
      return empty_count_;
    }
   private:
    std::string value_;
    int32_t full_count_ = 0;
    int32_t empty_count_ = 0;
  } increment_proc;
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->ProcessEach(&increment_proc, true));
  EXPECT_EQ(num_records / 2, increment_proc.GetFullCount());
  EXPECT_EQ(2, increment_proc.GetEmptyCount());
  EXPECT_EQ(num_records / 2, dbm->CountSimple());
  status = iter1->Get();
  EXPECT_TRUE(status == tkrzw::Status::SUCCESS || status == tkrzw::Status::NOT_FOUND_ERROR);
  status = iter1->Next();
  EXPECT_TRUE(status == tkrzw::Status::SUCCESS || status == tkrzw::Status::NOT_FOUND_ERROR);
  class CheckProc : public tkrzw::DBM::RecordProcessor {
   public:
    std::string_view ProcessFull(std::string_view key, std::string_view value) override {
      full_count_++;
      EXPECT_EQ(tkrzw::ToString(tkrzw::StrToInt(key) + 1), value);
      return NOOP;
    }
    std::string_view ProcessEmpty(std::string_view key) {
      empty_count_++;
      return NOOP;
    }
    int32_t GetFullCount() const {
      return full_count_;
    }
    int32_t GetEmptyCount() const {
      return empty_count_;
    }
   private:
    int32_t full_count_ = 0;
    int32_t empty_count_ = 0;
  } check_proc;
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->ProcessEach(&check_proc, false));
  EXPECT_EQ(num_records / 2, check_proc.GetFullCount());
  EXPECT_EQ(2, check_proc.GetEmptyCount());
  EXPECT_EQ(num_records / 2, dbm->CountSimple());
  status = iter1->Get();
  EXPECT_TRUE(status == tkrzw::Status::SUCCESS || status == tkrzw::Status::NOT_FOUND_ERROR);
  status = iter1->Next();
  EXPECT_TRUE(status == tkrzw::Status::SUCCESS || status == tkrzw::Status::NOT_FOUND_ERROR);
  status = iter2->Get();
  EXPECT_TRUE(status == tkrzw::Status::SUCCESS || status == tkrzw::Status::NOT_FOUND_ERROR);
  status = iter2->Next();
  EXPECT_TRUE(status == tkrzw::Status::SUCCESS || status == tkrzw::Status::NOT_FOUND_ERROR);
}

inline void CommonDBMTest::RandomTest(tkrzw::DBM* dbm, int32_t seed) {
  constexpr int32_t num_iterations = 10000;
  constexpr int32_t num_iterators = 3;
  std::vector<std::shared_ptr<tkrzw::DBM::Iterator>> iterators;
  for (int32_t i = 0; i < num_iterators; i++) {
    iterators.emplace_back(dbm->MakeIterator());
  }
  std::mt19937 mt(seed);
  std::uniform_int_distribution<int32_t> rec_dist(1, num_iterations);
  std::uniform_int_distribution<int32_t> op_dist(0, tkrzw::INT32MAX);
  std::uniform_int_distribution<int32_t> it_dist(0, iterators.size() - 1);
  tkrzw::Status s;
  for (int32_t i = 0; i < num_iterations; i++) {
    const std::string& key = tkrzw::ToString(rec_dist(mt));
    const std::string& value = tkrzw::ToString(i + 1);
    if (op_dist(mt) % 4 == 0) {
      auto* iterator = iterators[it_dist(mt)].get();
      switch (op_dist(mt) % 100) {
      case 0: {
        s = iterator->First();
        EXPECT_TRUE(s == tkrzw::Status::SUCCESS || s == tkrzw::Status::NOT_FOUND_ERROR);
        break;
      }
      case 1: {
        s = iterator->Jump(key);
        EXPECT_TRUE(s == tkrzw::Status::SUCCESS || s == tkrzw::Status::NOT_FOUND_ERROR);
        break;
      }
      case 2: {
        auto new_iterator = dbm->MakeIterator();
        if (op_dist(mt) % 2 == 0) {
          s = new_iterator->First();
        } else {
          s = new_iterator->Jump(key);
        }
        EXPECT_TRUE(s == tkrzw::Status::SUCCESS || s == tkrzw::Status::NOT_FOUND_ERROR);
        s = iterator->Get();
        EXPECT_TRUE(s == tkrzw::Status::SUCCESS || s == tkrzw::Status::NOT_FOUND_ERROR);
        if (op_dist(mt) % 10 == 0) {
          s = iterator->Remove();
          EXPECT_TRUE(s == tkrzw::Status::SUCCESS || s == tkrzw::Status::NOT_FOUND_ERROR);
        }
        break;
      }
      default: {
        switch (op_dist(mt) % 4) {
        case 0:
          s = iterator->Get();
          EXPECT_TRUE(s == tkrzw::Status::SUCCESS || s == tkrzw::Status::NOT_FOUND_ERROR);
          break;
        case 1:
          s = iterator->Remove();
          EXPECT_TRUE(s == tkrzw::Status::SUCCESS || s == tkrzw::Status::NOT_FOUND_ERROR);
          break;
        default:
          s = iterator->Next();
          EXPECT_TRUE(s == tkrzw::Status::SUCCESS || s == tkrzw::Status::NOT_FOUND_ERROR);
          break;
        }
        break;
      }
      }
    } else {
      switch (op_dist(mt) % 5) {
      case 0:
        s = dbm->Get(key);
        EXPECT_TRUE(s == tkrzw::Status::SUCCESS || s == tkrzw::Status::NOT_FOUND_ERROR);
        break;
      case 1:
        s = dbm->Remove(key);
        EXPECT_TRUE(s == tkrzw::Status::SUCCESS || s == tkrzw::Status::NOT_FOUND_ERROR);
        break;
      case 3:
        s = dbm->Set(key, value, true);
        EXPECT_TRUE(s == tkrzw::Status::SUCCESS);
        break;
      default:
        s = dbm->Set(key, value, false);
        EXPECT_TRUE(s == tkrzw::Status::SUCCESS || s == tkrzw::Status::DUPLICATION_ERROR);
        break;
      }
    }
  }
}

inline void CommonDBMTest::RandomTestThread(tkrzw::DBM* dbm) {
  constexpr int32_t num_threads = 5;
  std::vector<std::thread> threads;
  for (int32_t i = 0; i < num_threads; i++) {
    threads.emplace_back(std::thread([&]() { this->RandomTest(dbm, i); }));
  }
  for (auto& thread : threads) {
    thread.join();
  }
}

inline void CommonDBMTest::RebuildRandomTest(tkrzw::DBM* dbm) {
  constexpr int32_t num_threads = 5;
  constexpr int32_t num_iterations = 5000;
  auto task = [&](int32_t id, std::unordered_map<std::string, std::string>* map) {
    std::mt19937 mt(id);
    std::uniform_int_distribution<int32_t> key_dist(1, num_iterations);
    std::uniform_int_distribution<int32_t> value_dist(1, 6);
    std::uniform_int_distribution<int32_t> op_dist(0, tkrzw::INT32MAX);
    for (int32_t i = 0; i < num_iterations; i++) {
      int32_t key_num = key_dist(mt);
      key_num = key_num - key_num % num_threads + id;
      const std::string& key = tkrzw::ToString(key_num);
      if (op_dist(mt) % (num_iterations / 2) == 0) {
        EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Rebuild());
      } else if (op_dist(mt) % 10 == 0) {
        auto iter = dbm->MakeIterator();
        tkrzw::Status status = iter->Jump(key);
        EXPECT_TRUE(status == tkrzw::Status::SUCCESS || status == tkrzw::Status::NOT_FOUND_ERROR);
        status = iter->Get();
        EXPECT_TRUE(status == tkrzw::Status::SUCCESS || status == tkrzw::Status::NOT_FOUND_ERROR);
        status = iter->Next();
        EXPECT_TRUE(status == tkrzw::Status::SUCCESS || status == tkrzw::Status::NOT_FOUND_ERROR);
      } else if (op_dist(mt) % 5 == 0) {
        const tkrzw::Status status = dbm->Get(key);
        EXPECT_TRUE(status == tkrzw::Status::SUCCESS || status == tkrzw::Status::NOT_FOUND_ERROR);
      } else if (op_dist(mt) % 3 == 0) {
        const tkrzw::Status status = dbm->Remove(key);
        EXPECT_TRUE(status == tkrzw::Status::SUCCESS || status == tkrzw::Status::NOT_FOUND_ERROR);
        map->erase(key);
      } else {
        const int32_t value_size = value_dist(mt) * value_dist(mt);
        const std::string value(value_size, 'x');
        EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Set(key, value));
        (*map)[key] = value;
      }
    }
  };
  std::vector<std::thread> threads;
  std::vector<std::unordered_map<std::string, std::string>> maps(num_threads);
  for (int32_t i = 0; i < num_threads; i++) {
    threads.emplace_back(std::thread(task, i, &maps[i]));
  }
  for (auto& thread : threads) {
    thread.join();
  }
  int32_t num_records = 0;
  for (const auto& map :maps) {
    num_records += map.size();
    for (const auto& rec : map) {
      EXPECT_EQ(rec.second, dbm->GetSimple(rec.first));
    }
  }
  EXPECT_EQ(num_records, dbm->CountSimple());
}

inline void CommonDBMTest::RecordMigrationTest(tkrzw::DBM* dbm, tkrzw::File* file) {
  constexpr int32_t num_records = 100;
  for (int32_t i = 1; i <= num_records; i++) {
    const std::string key = tkrzw::SPrintF("%08d\t%08d", i, i);
    const std::string value = tkrzw::StrCat((i * i), "\n", (i * i));
    EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Set(key, value));
  }
  EXPECT_EQ(num_records, dbm->CountSimple());
  std::vector<std::string> back_keys;
  EXPECT_EQ(tkrzw::Status::SUCCESS, tkrzw::SearchDBM(
      dbm, "0", &back_keys, 0, tkrzw::StrEndsWith));
  EXPECT_EQ(10, back_keys.size());
  std::vector<std::string> forward_keys;
  EXPECT_EQ(tkrzw::Status::SUCCESS, tkrzw::SearchDBMForwardMatch(dbm, "0000001", &forward_keys));
  EXPECT_EQ(10, forward_keys.size());
  std::vector<std::string> regex_keys;
  EXPECT_EQ(tkrzw::Status::INVALID_ARGUMENT_ERROR, tkrzw::SearchDBMRegex(dbm, "[", &regex_keys));
  EXPECT_EQ(tkrzw::Status::SUCCESS, SearchDBMRegex(dbm, "\\d+0\\s+\\d+", &regex_keys));
  EXPECT_EQ(10, regex_keys.size());
  EXPECT_EQ(tkrzw::Status::SUCCESS, SearchDBMRegex(dbm, "\\d+0\\s+\\d+", &regex_keys, 0, true));
  EXPECT_EQ(10, regex_keys.size());
  std::vector<std::string> similar_keys;
  EXPECT_EQ(tkrzw::Status::SUCCESS, SearchDBMEditDistance(
      dbm, "00000100 00000100", &similar_keys, 1));
  EXPECT_THAT(similar_keys, UnorderedElementsAre("00000100\t00000100"));
  EXPECT_EQ(tkrzw::Status::SUCCESS, SearchDBMEditDistance(
      dbm, "00000100 00000100", &similar_keys, 1, true));
  EXPECT_THAT(similar_keys, UnorderedElementsAre("00000100\t00000100"));
  EXPECT_EQ(tkrzw::Status::SUCCESS, tkrzw::ExportDBMRecordsToFlatRecords(dbm, file));
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Clear());
  EXPECT_EQ(0, dbm->CountSimple());
  EXPECT_EQ(tkrzw::Status::SUCCESS, tkrzw::ImportDBMRecordsFromFlatRecords(dbm, file));
  for (int32_t i = 1; i <= num_records; i++) {
    const std::string key = tkrzw::SPrintF("%08d\t%08d", i, i);
    const std::string value = tkrzw::StrCat((i * i), "\n", (i * i));
    EXPECT_EQ(value, dbm->GetSimple(key));
  }
  EXPECT_EQ(num_records, dbm->CountSimple());
  EXPECT_EQ(tkrzw::Status::SUCCESS, tkrzw::ExportDBMRecordsToTSV(dbm, file, true));
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Clear());
  EXPECT_EQ(0, dbm->CountSimple());
  EXPECT_EQ(tkrzw::Status::SUCCESS, tkrzw::ImportDBMRecordsFromTSV(dbm, file, true));
  for (int32_t i = 1; i <= num_records; i++) {
    const std::string key = tkrzw::SPrintF("%08d\t%08d", i, i);
    const std::string value = tkrzw::StrCat((i * i), "\n", (i * i));
    EXPECT_EQ(value, dbm->GetSimple(key));
  }
  EXPECT_EQ(num_records, dbm->CountSimple());
  EXPECT_EQ(tkrzw::Status::SUCCESS, tkrzw::ExportDBMRecordsToTSV(dbm, file));
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Clear());
  EXPECT_EQ(0, dbm->CountSimple());
  EXPECT_EQ(tkrzw::Status::SUCCESS, tkrzw::ImportDBMRecordsFromTSV(dbm, file));
  std::vector<std::string> keys;
  for (int32_t i = 1; i <= num_records; i++) {
    const std::string key = tkrzw::SPrintF("%08d %08d", i, i);
    const std::string value = tkrzw::StrCat((i * i), " ", (i * i));
    EXPECT_EQ(value, dbm->GetSimple(key));
    keys.emplace_back(key);
  }
  EXPECT_EQ(num_records, dbm->CountSimple());
  EXPECT_EQ(tkrzw::Status::SUCCESS, tkrzw::ExportDBMKeysToFlatRecords(dbm, file));
  tkrzw::FlatRecordReader flat_reader(file);
  std::vector<std::string> flat_keys;
  std::string_view rec;
  while (flat_reader.Read(&rec) == tkrzw::Status::SUCCESS) {
    flat_keys.emplace_back(rec);
  }
  EXPECT_THAT(flat_keys, UnorderedElementsAreArray(keys));
  EXPECT_EQ(tkrzw::Status::SUCCESS, tkrzw::ExportDBMKeysAsLines(dbm, file));
  tkrzw::FileReader file_reader(file);
  std::vector<std::string> text_keys;
  std::string line;
  while (file_reader.ReadLine(&line) == tkrzw::Status::SUCCESS) {
    std::string_view key(
        line.data(), line.empty() || line.back() != '\n' ? line.size() : line.size() - 1);
    text_keys.emplace_back(key);
  }
  EXPECT_THAT(text_keys, UnorderedElementsAreArray(keys));
  std::vector<std::string> text_middle_keys;
  EXPECT_EQ(tkrzw::Status::SUCCESS, SearchTextFile(file, "0 0", &text_middle_keys));
  EXPECT_EQ(10, text_middle_keys.size());
  std::vector<std::string> text_regex_keys;
  EXPECT_EQ(tkrzw::Status::INVALID_ARGUMENT_ERROR,
            SearchTextFileRegex(file, "[", &text_regex_keys));
  EXPECT_EQ(tkrzw::Status::SUCCESS, SearchTextFileRegex(file, "\\d+0 +0\\d+", &text_regex_keys));
  EXPECT_EQ(10, text_regex_keys.size());
  EXPECT_EQ(tkrzw::Status::SUCCESS, SearchTextFileRegex(
      file, "\\d+0 +0\\d+", &text_regex_keys, 0, true));
  EXPECT_EQ(10, text_regex_keys.size());
  std::vector<std::string> text_similar_keys;
  EXPECT_EQ(tkrzw::Status::SUCCESS, SearchTextFileEditDistance(
      file, "00000100 00000100", &text_similar_keys, 1));
  EXPECT_THAT(text_similar_keys, UnorderedElementsAre("00000100 00000100"));
  EXPECT_EQ(tkrzw::Status::SUCCESS, SearchTextFileEditDistance(
      file, "00000100 00000100", &text_similar_keys, 1, true));
  EXPECT_THAT(text_similar_keys, UnorderedElementsAre("00000100 00000100"));
}

inline void CommonDBMTest::BackIteratorTest(tkrzw::DBM* dbm) {
  for (int32_t i = 1; i <= 300; i++) {
    const std::string key = tkrzw::ToString(i * i);
    const std::string value = tkrzw::ToString(i);
    EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Set(key, value));
  }
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Synchronize(false));
  for (int32_t i = 1; i <= 100; i++) {
    const std::string key = tkrzw::ToString(i * i);
    EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Remove(key));
  }
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Synchronize(false));
  for (int32_t i = 201; i <= 300; i++) {
    const std::string key = tkrzw::ToString(i * i);
    EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Remove(key));
  }
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Synchronize(false));
  for (int32_t i = 1; i <= 100; i++) {
    const std::string key = tkrzw::ToString(i * i);
    const std::string value = tkrzw::ToString(i);
    EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Set(key, value));
  }
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Synchronize(false));
  for (int32_t i = 101; i <= 200; i++) {
    const std::string key = tkrzw::ToString(i * i);
    EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Remove(key));
  }
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Synchronize(false));
  EXPECT_EQ(100, dbm->CountSimple());
  auto iter = dbm->MakeIterator();
  std::vector<std::pair<std::string, std::string>> forward_records;
  EXPECT_EQ(tkrzw::Status::SUCCESS, iter->First());
  while (true) {
    std::string key, value;
    const tkrzw::Status status = iter->Get(&key, &value);
    if (status != tkrzw::Status::SUCCESS) {
      EXPECT_EQ(tkrzw::Status::NOT_FOUND_ERROR, status);
      break;
    }
    forward_records.emplace_back(std::make_pair(key, value));
    EXPECT_EQ(tkrzw::Status::SUCCESS, iter->Next());
  }
  std::vector<std::pair<std::string, std::string>> backward_records;
  EXPECT_EQ(tkrzw::Status::SUCCESS, iter->Last());
  while (true) {
    std::string key, value;
    const tkrzw::Status status = iter->Get(&key, &value);
    if (status != tkrzw::Status::SUCCESS) {
      EXPECT_EQ(tkrzw::Status::NOT_FOUND_ERROR, status);
      break;
    }
    backward_records.emplace_back(std::make_pair(key, value));
    EXPECT_EQ(tkrzw::Status::SUCCESS, iter->Previous());
  }
  std::reverse(backward_records.begin(), backward_records.end());
  EXPECT_THAT(backward_records, ElementsAreArray(forward_records));
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Set("a", "AA"));
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Set("b", "BB"));
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Set("c", "CC"));
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Synchronize(false));
  EXPECT_EQ(tkrzw::Status::SUCCESS, iter->Jump("1"));
  EXPECT_EQ("1", iter->GetKey());
  EXPECT_EQ(tkrzw::Status::SUCCESS, iter->Previous());
  EXPECT_EQ(tkrzw::Status::NOT_FOUND_ERROR, iter->Get());
  EXPECT_EQ(tkrzw::Status::NOT_FOUND_ERROR, iter->Previous());
  EXPECT_EQ(tkrzw::Status::SUCCESS, iter->Jump("c"));
  EXPECT_EQ("c", iter->GetKey());
  EXPECT_EQ("CC", iter->GetValue());
  EXPECT_EQ(tkrzw::Status::SUCCESS, iter->Previous());
  EXPECT_EQ("b", iter->GetKey());
  EXPECT_EQ("BB", iter->GetValue());
  EXPECT_EQ(tkrzw::Status::SUCCESS, iter->Previous());
  EXPECT_EQ("a", iter->GetKey());
  EXPECT_EQ("AA", iter->GetValue());
}

inline void CommonDBMTest::IteratorBoundTest(tkrzw::DBM* dbm) {
  auto iter = dbm->MakeIterator();
  EXPECT_EQ(tkrzw::Status::SUCCESS, iter->First());
  EXPECT_EQ(tkrzw::Status::SUCCESS, iter->Last());
  EXPECT_EQ(tkrzw::Status::SUCCESS, iter->Jump(""));
  EXPECT_EQ(tkrzw::Status::SUCCESS, iter->JumpLower("", true));
  EXPECT_EQ(tkrzw::Status::SUCCESS, iter->JumpLower("", false));
  EXPECT_EQ(tkrzw::Status::SUCCESS, iter->JumpUpper("", true));
  EXPECT_EQ(tkrzw::Status::SUCCESS, iter->JumpUpper("", false));
  for (int32_t i = 1; i <= 8; i++) {
    const std::string key = tkrzw::ToString(i);
    const std::string value = tkrzw::ToString(i * i);
    EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Set(key, value, false));
  }
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Synchronize(false));
  EXPECT_EQ(8, dbm->CountSimple());
  EXPECT_EQ(tkrzw::Status::SUCCESS, iter->First());
  EXPECT_EQ("1", iter->GetKey());
  EXPECT_EQ(tkrzw::Status::SUCCESS, iter->Next());
  EXPECT_EQ("2", iter->GetKey());
  EXPECT_EQ(tkrzw::Status::SUCCESS, iter->Last());
  EXPECT_EQ("8", iter->GetKey());
  EXPECT_EQ(tkrzw::Status::SUCCESS, iter->Previous());
  EXPECT_EQ("7", iter->GetKey());
  for (int32_t i = 1; i <= 8; i++) {
    const std::string key = tkrzw::ToString(i);
    EXPECT_EQ(tkrzw::Status::SUCCESS, iter->Jump(key));
    EXPECT_EQ(key, iter->GetKey());
    EXPECT_EQ(tkrzw::Status::SUCCESS, iter->JumpLower(key, true));
    EXPECT_EQ(key, iter->GetKey());
    EXPECT_EQ(tkrzw::Status::SUCCESS, iter->JumpUpper(key, true));
    EXPECT_EQ(key, iter->GetKey());
  }
  EXPECT_EQ(tkrzw::Status::SUCCESS, iter->Jump("0"));
  EXPECT_EQ("1", iter->GetKey());
  EXPECT_EQ(tkrzw::Status::SUCCESS, iter->Previous());
  EXPECT_EQ("", iter->GetKey());
  EXPECT_EQ(tkrzw::Status::SUCCESS, iter->Jump("9"));
  EXPECT_EQ("", iter->GetKey());
  EXPECT_EQ(tkrzw::Status::SUCCESS, iter->JumpLower("0", true));
  EXPECT_EQ("", iter->GetKey());
  EXPECT_EQ(tkrzw::Status::SUCCESS, iter->JumpLower("0", false));
  EXPECT_EQ("", iter->GetKey());
  EXPECT_EQ(tkrzw::Status::SUCCESS, iter->JumpLower("1", true));
  EXPECT_EQ("1", iter->GetKey());
  EXPECT_EQ(tkrzw::Status::SUCCESS, iter->JumpLower("1", false));
  EXPECT_EQ("", iter->GetKey());
  EXPECT_EQ(tkrzw::Status::SUCCESS, iter->JumpLower("4", true));
  EXPECT_EQ("4", iter->GetKey());
  EXPECT_EQ(tkrzw::Status::SUCCESS, iter->JumpLower("4", false));
  EXPECT_EQ("3", iter->GetKey());
  EXPECT_EQ(tkrzw::Status::SUCCESS, iter->Previous());
  EXPECT_EQ("2", iter->GetKey());
  EXPECT_EQ(tkrzw::Status::SUCCESS, iter->Next());
  EXPECT_EQ("3", iter->GetKey());
  EXPECT_EQ(tkrzw::Status::SUCCESS, iter->JumpLower("4A", true));
  EXPECT_EQ("4", iter->GetKey());
  EXPECT_EQ(tkrzw::Status::SUCCESS, iter->Next());
  EXPECT_EQ("5", iter->GetKey());
  EXPECT_EQ(tkrzw::Status::SUCCESS, iter->JumpLower("4A", false));
  EXPECT_EQ("4", iter->GetKey());
  EXPECT_EQ(tkrzw::Status::SUCCESS, iter->Previous());
  EXPECT_EQ("3", iter->GetKey());
  EXPECT_EQ(tkrzw::Status::SUCCESS, iter->Next());
  EXPECT_EQ("4", iter->GetKey());
  EXPECT_EQ(tkrzw::Status::SUCCESS, iter->JumpLower("9", true));
  EXPECT_EQ("8", iter->GetKey());
  EXPECT_EQ(tkrzw::Status::SUCCESS, iter->JumpLower("9", false));
  EXPECT_EQ("8", iter->GetKey());
  EXPECT_EQ(tkrzw::Status::SUCCESS, iter->Previous());
  EXPECT_EQ("7", iter->GetKey());
  EXPECT_EQ(tkrzw::Status::SUCCESS, iter->Previous());
  EXPECT_EQ("6", iter->GetKey());
  EXPECT_EQ(tkrzw::Status::SUCCESS, iter->Next());
  EXPECT_EQ("7", iter->GetKey());
  EXPECT_EQ(tkrzw::Status::SUCCESS, iter->JumpUpper("0", true));
  EXPECT_EQ("1", iter->GetKey());
  EXPECT_EQ(tkrzw::Status::SUCCESS, iter->JumpUpper("0", false));
  EXPECT_EQ("1", iter->GetKey());
  EXPECT_EQ(tkrzw::Status::SUCCESS, iter->Next());
  EXPECT_EQ("2", iter->GetKey());
  EXPECT_EQ(tkrzw::Status::SUCCESS, iter->Next());
  EXPECT_EQ("3", iter->GetKey());
  EXPECT_EQ(tkrzw::Status::SUCCESS, iter->Previous());
  EXPECT_EQ("2", iter->GetKey());
  EXPECT_EQ(tkrzw::Status::SUCCESS, iter->JumpUpper("4", true));
  EXPECT_EQ("4", iter->GetKey());
  EXPECT_EQ(tkrzw::Status::SUCCESS, iter->JumpUpper("4", false));
  EXPECT_EQ("5", iter->GetKey());
  EXPECT_EQ(tkrzw::Status::SUCCESS, iter->Next());
  EXPECT_EQ("6", iter->GetKey());
  EXPECT_EQ(tkrzw::Status::SUCCESS, iter->Previous());
  EXPECT_EQ("5", iter->GetKey());
  EXPECT_EQ(tkrzw::Status::SUCCESS, iter->JumpUpper("4A", true));
  EXPECT_EQ("5", iter->GetKey());
  EXPECT_EQ(tkrzw::Status::SUCCESS, iter->JumpUpper("4A", false));
  EXPECT_EQ("5", iter->GetKey());
  EXPECT_EQ(tkrzw::Status::SUCCESS, iter->Next());
  EXPECT_EQ("6", iter->GetKey());
  EXPECT_EQ(tkrzw::Status::SUCCESS, iter->Next());
  EXPECT_EQ("7", iter->GetKey());
  EXPECT_EQ(tkrzw::Status::SUCCESS, iter->JumpUpper("8", true));
  EXPECT_EQ("8", iter->GetKey());
  EXPECT_EQ(tkrzw::Status::SUCCESS, iter->Previous());
  EXPECT_EQ("7", iter->GetKey());
  EXPECT_EQ(tkrzw::Status::SUCCESS, iter->JumpUpper("8", false));
  EXPECT_EQ("", iter->GetKey());
  EXPECT_EQ(tkrzw::Status::SUCCESS, iter->JumpUpper("9", true));
  EXPECT_EQ("", iter->GetKey());
  EXPECT_EQ(tkrzw::Status::SUCCESS, iter->JumpUpper("9", false));
  EXPECT_EQ("", iter->GetKey());
}

// END OF FILE
