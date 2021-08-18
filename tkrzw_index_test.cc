/*************************************************************************************************
 * Tests for tkrzw_index.h
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

#include "tkrzw_dbm_tree.h"
#include "tkrzw_file.h"
#include "tkrzw_file_util.h"
#include "tkrzw_lib_common.h"
#include "tkrzw_index.h"
#include "tkrzw_thread_util.h"
#include "tkrzw_str_util.h"

using namespace testing;

// Main routine
int main(int argc, char** argv) {
  InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

template<typename INDEXTYPE>
void CommonIndexBasicTest(INDEXTYPE& index) {
  constexpr int32_t num_keys = 10;
  constexpr int32_t num_values = 3;
  std::set<std::pair<std::string, std::string>> records;
  for (int32_t key_index = 1; key_index <= num_keys; key_index++) {
    for (int32_t value_index = 1; value_index <= num_values; value_index++) {
      const std::string key = tkrzw::ToString(key_index);
      const std::string value = tkrzw::SPrintF("%08d", value_index);
      EXPECT_FALSE(index.Check(key, value));
      index.Add(key, value);
      EXPECT_TRUE(index.Check(key, value));
      EXPECT_FALSE(index.Check(key, ""));
      records.emplace(std::make_pair(key, value));
    }
  }
  EXPECT_EQ(num_keys * num_values, index.Count());
  std::vector<std::pair<std::string, std::string>> forward_records;
  auto iter = index.MakeIterator();
  iter->First();
  std::string key, value;
  while (iter->Get(&key, &value)) {
    records.erase(std::make_pair(key, value));
    forward_records.emplace_back(std::make_pair(key, value));
    iter->Next();
  }
  EXPECT_EQ(0, records.size());
  std::vector<std::pair<std::string, std::string>> backward_records;
  iter->Last();
  while (iter->Get(&key, &value)) {
    backward_records.emplace_back(std::make_pair(key, value));
    iter->Previous();
  }
  std::reverse(backward_records.begin(), backward_records.end());
  EXPECT_THAT(backward_records, ElementsAreArray(forward_records));
  std::vector<std::string> values;
  for (int32_t value_index = 1; value_index <= num_values; value_index++) {
    values.emplace_back(tkrzw::SPrintF("%08d", value_index));
  }
  for (int32_t key_index = 1; key_index <= num_keys; key_index++) {
    const std::string key = tkrzw::ToString(key_index);
    iter->Jump(key);
    std::vector<std::string> rec_values;
    std::string rec_key, rec_value;
    while (iter->Get(&rec_key, &rec_value)) {
      if (rec_key != key) {
        break;
      }
      rec_values.emplace_back(rec_value);
      iter->Next();
    }
    EXPECT_THAT(rec_values, ElementsAreArray(values));
    EXPECT_THAT(index.GetValues(key), ElementsAreArray(values));
    EXPECT_THAT(index.GetValues(key, 1), ElementsAre("00000001"));
  }
  for (int32_t key_index = 1; key_index <= num_keys; key_index++) {
    const std::string key = tkrzw::ToString(key_index);
    const std::string value = "00000001";
    EXPECT_TRUE(index.Check(key, value));
    index.Remove(key, value);
    EXPECT_FALSE(index.Check(key, value));
    EXPECT_EQ(num_values - 1, index.GetValues(key).size());
  }
  index.Clear();
  EXPECT_EQ(0, index.Count());
}

template<typename INDEXTYPE>
void CommonIndexDecimalTest(INDEXTYPE& index) {
  constexpr int32_t num_keys = 10;
  constexpr int32_t num_values = 3;
  std::set<std::pair<std::string, std::string>> records;
  for (int32_t key = 1; key <= num_keys; key++) {
    for (int32_t value = 1; value <= num_values; value++) {
      const std::string key_str = tkrzw::ToString(key);
      const std::string value_str = tkrzw::ToString(value);
      EXPECT_FALSE(index.Check(key_str, value_str));
      index.Add(key_str, value_str);
      EXPECT_TRUE(index.Check(key_str, value_str));
      EXPECT_FALSE(index.Check(key_str, ""));
      records.emplace(std::make_pair(key_str, value_str));
    }
  }
  EXPECT_EQ(num_keys * num_values, index.Count());
  std::vector<std::pair<std::string, std::string>> forward_records;
  auto iter = index.MakeIterator();
  iter->First();
  std::string key, value;
  while (iter->Get(&key, &value)) {
    records.erase(std::make_pair(key, value));
    forward_records.emplace_back(std::make_pair(key, value));
    iter->Next();
  }
  EXPECT_EQ(0, records.size());
  std::vector<std::pair<std::string, std::string>> backward_records;
  iter->Last();
  while (iter->Get(&key, &value)) {
    backward_records.emplace_back(std::make_pair(key, value));
    iter->Previous();
  }
  std::reverse(backward_records.begin(), backward_records.end());
  EXPECT_THAT(backward_records, ElementsAreArray(forward_records));
  std::vector<std::string> values;
  for (int32_t value_index = 1; value_index <= num_values; value_index++) {
    values.emplace_back(tkrzw::ToString(value_index));
  }
  for (int32_t key = 1; key <= num_keys; key++) {
    const std::string key_str = tkrzw::ToString(key);
    iter->Jump(key_str);
    std::vector<std::string> rec_values;
    std::string rec_key, rec_value;
    while (iter->Get(&rec_key, &rec_value)) {
      if (rec_key != key_str) {
        break;
      }
      rec_values.emplace_back(rec_value);
      iter->Next();
    }
    EXPECT_THAT(rec_values, ElementsAreArray(values));
    EXPECT_THAT(index.GetValues(key_str), ElementsAreArray(values));
    EXPECT_THAT(index.GetValues(key_str, 1), ElementsAre("1"));
  }
  for (int32_t key = 1; key <= num_keys; key++) {
    const std::string key_str = tkrzw::ToString(key);
    const std::string value_str = "1";
    EXPECT_TRUE(index.Check(key_str, value_str));
    index.Remove(key_str, value_str);
    EXPECT_FALSE(index.Check(key_str, value_str));
    EXPECT_EQ(num_values - 1, index.GetValues(key_str).size());
  }
  index.Clear();
  EXPECT_EQ(0, index.Count());
}

template<typename INDEXTYPE>
void CommonIndexThreadTest(INDEXTYPE& index) {
  constexpr int32_t num_threads = 4;
  constexpr int32_t num_iterations = 1000;
  constexpr int32_t num_values = 3;
  auto task = [&](int32_t id) {
    for (int32_t i = 1; i <= num_iterations; ++i) {
      const int32_t key_num = i * num_threads + id;
      const std::string key = tkrzw::ToString(key_num);
      for (int32_t value_index = 1; value_index <= num_values; ++value_index) {
        const std::string value = tkrzw::ToString(value_index);
        index.Add(key, value);
      }
      for (int32_t value_index = 1; value_index <= num_values; ++value_index) {
        const std::string value = tkrzw::ToString(value_index);
        EXPECT_TRUE(index.Check(key, value));
      }
      if (i % 10 == 0) {
        auto iter = index.MakeIterator();
        iter->First();
        iter->Get();
        iter->Next();
        iter->Jump(key);
        std::vector<std::string> rec_values;
        std::string rec_key, rec_value;
        while (iter->Get(&rec_key, &rec_value)) {
          if (rec_key != key) {
            break;
          }
          rec_values.emplace_back(rec_value);
          iter->Next();
        }
        EXPECT_EQ(num_values, rec_values.size());
        EXPECT_THAT(index.GetValues(key), ElementsAreArray(rec_values));
      }
      for (int32_t value_index = 1; value_index <= num_values; ++value_index) {
        const std::string value = tkrzw::ToString(value_index);
        index.Remove(key, value);
      }
      for (int32_t value_index = 1; value_index <= num_values; ++value_index) {
        const std::string value = tkrzw::ToString(value_index);
        EXPECT_FALSE(index.Check(key, value));
      }
    }
  };
  std::vector<std::thread> threads;
  for (int32_t i = 0; i < num_threads; i++) {
    threads.emplace_back(std::thread(task, i));
  }
  for (auto& thread : threads) {
    thread.join();
  }
}

template<typename INDEXTYPE>
void StdIndexBasicIntTest() {
  INDEXTYPE index;
  constexpr int32_t num_keys = 10;
  constexpr int32_t num_values = 3;
  std::set<std::pair<int32_t, int32_t>> records;
  for (int32_t key = 1; key <= num_keys; key++) {
    for (int32_t value = 1; value <= num_values; value++) {
      EXPECT_FALSE(index.Check(key, value));
      index.Add(key, value);
      EXPECT_TRUE(index.Check(key, value));
      EXPECT_FALSE(index.Check(key, int32_t()));
      records.emplace(std::make_pair(key, value));
    }
  }
  EXPECT_EQ(num_keys * num_values, index.Count());
  std::vector<std::pair<int32_t, int32_t>> forward_records;
  auto iter = index.MakeIterator();
  iter->First();
  int32_t key, value;
  while (iter->Get(&key, &value)) {
    records.erase(std::make_pair(key, value));
    forward_records.emplace_back(std::make_pair(key, value));
    iter->Next();
  }
  EXPECT_EQ(0, records.size());
  std::vector<std::pair<int32_t, int32_t>> backward_records;
  iter->Last();
  while (iter->Get(&key, &value)) {
    backward_records.emplace_back(std::make_pair(key, value));
    iter->Previous();
  }
  std::reverse(backward_records.begin(), backward_records.end());
  EXPECT_THAT(backward_records, ElementsAreArray(forward_records));
  std::vector<int32_t> values;
  for (int32_t value_index = 1; value_index <= num_values; value_index++) {
    values.emplace_back(value_index);
  }
  for (int32_t key = 1; key <= num_keys; key++) {
    iter->Jump(key);
    std::vector<int32_t> rec_values;
    int32_t rec_key, rec_value;
    while (iter->Get(&rec_key, &rec_value)) {
      if (rec_key != key) {
        break;
      }
      rec_values.emplace_back(rec_value);
      iter->Next();
    }
    EXPECT_THAT(rec_values, ElementsAreArray(values));
    EXPECT_THAT(index.GetValues(key), ElementsAreArray(values));
    EXPECT_THAT(index.GetValues(key, 1), ElementsAre(1));
  }
  for (int32_t key = 1; key <= num_keys; key++) {
    const int32_t value = 1;
    EXPECT_TRUE(index.Check(key, value));
    index.Remove(key, value);
    EXPECT_FALSE(index.Check(key, value));
    EXPECT_EQ(num_values - 1, index.GetValues(key).size());
  }
  index.Clear();
  EXPECT_EQ(0, index.Count());
}

template<typename INDEXTYPE>
void StdIndexBasicStrTest() {
  INDEXTYPE index;
  constexpr int32_t num_keys = 10;
  constexpr int32_t num_values = 3;
  std::set<std::pair<std::string, std::string>> records;
  for (int32_t key_index = 1; key_index <= num_keys; key_index++) {
    for (int32_t value_index = 1; value_index <= num_values; value_index++) {
      const std::string key = tkrzw::ToString(key_index);
      const std::string value = tkrzw::SPrintF("%08d", value_index);
      EXPECT_FALSE(index.Check(key, value));
      index.Add(key, value);
      EXPECT_TRUE(index.Check(key, value));
      EXPECT_FALSE(index.Check(key, ""));
      records.emplace(std::make_pair(key, value));
    }
  }
  EXPECT_EQ(num_keys * num_values, index.Count());
  std::vector<std::pair<std::string, std::string>> forward_records;
  auto iter = index.MakeIterator();
  iter->First();
  std::string key, value;
  while (iter->Get(&key, &value)) {
    records.erase(std::make_pair(key, value));
    forward_records.emplace_back(std::make_pair(key, value));
    iter->Next();
  }
  EXPECT_EQ(0, records.size());
  std::vector<std::pair<std::string, std::string>> backward_records;
  iter->Last();
  while (iter->Get(&key, &value)) {
    backward_records.emplace_back(std::make_pair(key, value));
    iter->Previous();
  }
  std::reverse(backward_records.begin(), backward_records.end());
  EXPECT_THAT(backward_records, ElementsAreArray(forward_records));
  std::vector<std::string> values;
  for (int32_t value_index = 1; value_index <= num_values; value_index++) {
    values.emplace_back(tkrzw::SPrintF("%08d", value_index));
  }
  for (int32_t key_index = 1; key_index <= num_keys; key_index++) {
    const std::string key = tkrzw::ToString(key_index);
    iter->Jump(key);
    std::vector<std::string> rec_values;
    std::string rec_key, rec_value;
    while (iter->Get(&rec_key, &rec_value)) {
      if (rec_key != key) {
        break;
      }
      rec_values.emplace_back(rec_value);
      iter->Next();
    }
    EXPECT_THAT(rec_values, ElementsAreArray(values));
    EXPECT_THAT(index.GetValues(key), ElementsAreArray(values));
    EXPECT_THAT(index.GetValues(key, 1), ElementsAre("00000001"));
  }
  for (int32_t key_index = 1; key_index <= num_keys; key_index++) {
    const std::string key = tkrzw::ToString(key_index);
    const std::string value = "00000001";
    EXPECT_TRUE(index.Check(key, value));
    index.Remove(key, value);
    EXPECT_FALSE(index.Check(key, value));
    EXPECT_EQ(num_values - 1, index.GetValues(key).size());
  }
  index.Clear();
  EXPECT_EQ(0, index.Count());
}

template<typename INDEXTYPE>
void StdIndexThreadIntTest() {
  INDEXTYPE index;
  constexpr int32_t num_threads = 4;
  constexpr int32_t num_iterations = 1000;
  constexpr int32_t num_values = 3;
  auto task = [&](int32_t id) {
    for (int32_t i = 1; i <= num_iterations; ++i) {
      const int32_t key = i * num_threads + id;
      for (int32_t value = 1; value <= num_values; ++value) {
        index.Add(key, value);
      }
      for (int32_t value = 1; value <= num_values; ++value) {
        EXPECT_TRUE(index.Check(key, value));
      }
      if (i % 10 == 0) {
        auto iter = index.MakeIterator();
        iter->First();
        iter->Get();
        iter->Next();
        iter->Jump(key);
        std::vector<int32_t> rec_values;
        int32_t rec_key, rec_value;
        while (iter->Get(&rec_key, &rec_value)) {
          if (rec_key != key) {
            break;
          }
          rec_values.emplace_back(rec_value);
          iter->Next();
        }
        EXPECT_EQ(num_values, rec_values.size());
        EXPECT_THAT(index.GetValues(key), ElementsAreArray(rec_values));
      }
      for (int32_t value = 1; value <= num_values; ++value) {
        index.Remove(key, value);
      }
      for (int32_t value = 1; value <= num_values; ++value) {
        EXPECT_FALSE(index.Check(key, value));
      }
    }
  };
  std::vector<std::thread> threads;
  for (int32_t i = 0; i < num_threads; i++) {
    threads.emplace_back(std::thread(task, i));
  }
  for (auto& thread : threads) {
    thread.join();
  }
}

template<typename INDEXTYPE>
void StdIndexThreadStrTest() {
  INDEXTYPE index;
  constexpr int32_t num_threads = 4;
  constexpr int32_t num_iterations = 1000;
  constexpr int32_t num_values = 3;
  auto task = [&](int32_t id) {
    for (int32_t i = 1; i <= num_iterations; ++i) {
      const int32_t key_num = i * num_threads + id;
      const std::string key = tkrzw::ToString(key_num);
      for (int32_t value_index = 1; value_index <= num_values; ++value_index) {
        const std::string value = tkrzw::ToString(value_index);
        index.Add(key, value);
      }
      for (int32_t value_index = 1; value_index <= num_values; ++value_index) {
        const std::string value = tkrzw::ToString(value_index);
        EXPECT_TRUE(index.Check(key, value));
      }
      if (i % 10 == 0) {
        auto iter = index.MakeIterator();
        iter->First();
        iter->Get();
        iter->Next();
        iter->Jump(key);
        std::vector<std::string> rec_values;
        std::string rec_key, rec_value;
        while (iter->Get(&rec_key, &rec_value)) {
          if (rec_key != key) {
            break;
          }
          rec_values.emplace_back(rec_value);
          iter->Next();
        }
        EXPECT_EQ(num_values, rec_values.size());
        EXPECT_THAT(index.GetValues(key), ElementsAreArray(rec_values));
      }
      for (int32_t value_index = 1; value_index <= num_values; ++value_index) {
        const std::string value = tkrzw::ToString(value_index);
        index.Remove(key, value);
      }
      for (int32_t value_index = 1; value_index <= num_values; ++value_index) {
        const std::string value = tkrzw::ToString(value_index);
        EXPECT_FALSE(index.Check(key, value));
      }
    }
  };
  std::vector<std::thread> threads;
  for (int32_t i = 0; i < num_threads; i++) {
    threads.emplace_back(std::thread(task, i));
  }
  for (auto& thread : threads) {
    thread.join();
  }
}

template <typename KEYTYPE, typename VALUETYPE>
struct KeyDescendingComparator final {
  bool operator()(const std::pair<KEYTYPE, VALUETYPE>& lhs,
                  const std::pair<KEYTYPE, VALUETYPE>& rhs) const {
    if (std::greater<KEYTYPE>()(lhs.first, rhs.first)) {
      return true;
    }
    if (std::greater<KEYTYPE>()(rhs.first, lhs.first)) {
      return false;
    }
    return lhs.second < rhs.second;
  }
};

TEST(FileIndexTest, Basic) {
  tkrzw::TemporaryDirectory tmp_dir(true, "tkrzw-");
  const std::string file_path = tmp_dir.MakeUniquePath();
  tkrzw::FileIndex index;
  tkrzw::TreeDBM::TuningParameters tuning_params;
  tuning_params.num_buckets = 100;
  tuning_params.max_page_size = 1000;
  tuning_params.max_branches = 4;
  EXPECT_EQ(tkrzw::Status::SUCCESS,
            index.Open(file_path, true, tkrzw::File::OPEN_DEFAULT, tuning_params));
  CommonIndexBasicTest<tkrzw::FileIndex>(index);
  EXPECT_TRUE(index.IsOpen());
  EXPECT_TRUE(index.IsWritable());
  EXPECT_EQ(tkrzw::Status::SUCCESS, index.Rebuild());
  EXPECT_EQ(tkrzw::Status::SUCCESS, index.Synchronize(false));
  tkrzw::TreeDBM* tree_dbm = index.GetInternalDBM();
  EXPECT_EQ(index.Count(), tree_dbm->CountSimple());
  EXPECT_EQ(tkrzw::Status::SUCCESS, index.Close());
  EXPECT_FALSE(index.IsOpen());
  EXPECT_FALSE(index.IsWritable());
}

TEST(FileIndexTest, Decimal) {
  tkrzw::TemporaryDirectory tmp_dir(true, "tkrzw-");
  const std::string file_path = tmp_dir.MakeUniquePath();
  tkrzw::FileIndex index;
  tkrzw::TreeDBM::TuningParameters tuning_params;
  tuning_params.num_buckets = 100;
  tuning_params.max_page_size = 1000;
  tuning_params.max_branches = 4;
  tuning_params.key_comparator = tkrzw::PairDecimalKeyComparator;
  EXPECT_EQ(tkrzw::Status::SUCCESS,
            index.Open(file_path, true, tkrzw::File::OPEN_DEFAULT, tuning_params));
  CommonIndexDecimalTest<tkrzw::FileIndex>(index);
  EXPECT_EQ(tkrzw::Status::SUCCESS, index.Close());
}

TEST(FileIndexTest, Thread) {
  tkrzw::TemporaryDirectory tmp_dir(true, "tkrzw-");
  const std::string file_path = tmp_dir.MakeUniquePath();
  tkrzw::FileIndex index;
  tkrzw::TreeDBM::TuningParameters tuning_params;
  tuning_params.num_buckets = 100;
  tuning_params.max_page_size = 1000;
  tuning_params.max_branches = 4;
  EXPECT_EQ(tkrzw::Status::SUCCESS,
            index.Open(file_path, true, tkrzw::File::OPEN_DEFAULT, tuning_params));
  CommonIndexThreadTest<tkrzw::FileIndex>(index);
  EXPECT_EQ(tkrzw::Status::SUCCESS, index.Close());
}

TEST(MemIndexTest, Basic) {
  tkrzw::MemIndex index;
  CommonIndexBasicTest<tkrzw::MemIndex>(index);
}

TEST(MemIndexTest, Decimal) {
  tkrzw::MemIndex index(tkrzw::PairDecimalKeyComparator);
  CommonIndexDecimalTest<tkrzw::MemIndex>(index);
}

TEST(MemIndexTest, Thread) {
  tkrzw::MemIndex index(tkrzw::PairDecimalKeyComparator);
  CommonIndexDecimalTest<tkrzw::MemIndex>(index);
}

TEST(StdIndexTest, GenericIntBasic) {
  StdIndexBasicIntTest<tkrzw::StdIndex<int32_t, int32_t>>();
}

TEST(StdIndexTest, GenericIntBasicDesc) {
  StdIndexBasicIntTest<tkrzw::StdIndex<
    int32_t, int32_t, KeyDescendingComparator<int32_t, int32_t>>>();
}

TEST(StdIndexTest, GenericStrBasic) {
  StdIndexBasicStrTest<tkrzw::StdIndex<std::string, std::string>>();
}

TEST(StdIndexTest, GenericStrBasicDesc) {
  StdIndexBasicStrTest<tkrzw::StdIndex<
    std::string, std::string, KeyDescendingComparator<std::string, std::string>>>();
}

TEST(StdIndexTest, SpecificStrBasic) {
  StdIndexBasicStrTest<tkrzw::StdIndexStr>();
}

TEST(StdIndexTest, GenericIntThread) {
  StdIndexThreadIntTest<tkrzw::StdIndex<int32_t, int32_t>>();
}

TEST(StdIndexTest, GenericStrThread) {
  StdIndexThreadStrTest<tkrzw::StdIndex<std::string, std::string>>();
}

TEST(StdIndexTest, SpecificStrThread) {
  StdIndexThreadStrTest<tkrzw::StdIndexStr>();
}

// END OF FILE
