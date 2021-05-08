/*************************************************************************************************
 * Tests for tkrzw_dbm_common_impl.h
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

#include "tkrzw_dbm.h"
#include "tkrzw_dbm_common_impl.h"
#include "tkrzw_file.h"
#include "tkrzw_file_mmap.h"
#include "tkrzw_file_pos.h"
#include "tkrzw_file_std.h"
#include "tkrzw_file_util.h"
#include "tkrzw_lib_common.h"
#include "tkrzw_str_util.h"

using namespace testing;

// Main routine
int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

TEST(DBMCommonImplTest, PrimaryHash) {
  EXPECT_EQ(16973900370012003622ULL, tkrzw::PrimaryHash("abc", tkrzw::UINT64MAX));
  EXPECT_EQ(3042090208ULL, tkrzw::PrimaryHash("abc", tkrzw::UINT32MAX));
  constexpr int32_t num_records = 1000000;
  constexpr int32_t num_buckets = 1000;
  constexpr double mean_count = num_records * 1.0 / num_buckets;
  std::vector<int32_t> buckets(num_buckets, 0);
  for (int32_t i = 0; i < num_records; i++) {
    const int64_t bucket_index = tkrzw::PrimaryHash(tkrzw::ToString(i), num_buckets);
    EXPECT_LT(bucket_index, num_buckets);
    buckets[bucket_index]++;
  }
  int32_t min_count = tkrzw::INT32MAX;
  int32_t max_count = 0;
  double variance = 0;
  for (const auto count : buckets) {
    min_count = std::min(min_count, count);
    max_count = std::max(max_count, count);
    variance += std::pow(count - mean_count, 2);
  }
  variance /= num_buckets;
  const double stddev = std::sqrt(variance);
  EXPECT_GT(min_count, mean_count * 0.8);
  EXPECT_LT(max_count, mean_count / 0.8);
  EXPECT_LT(stddev, mean_count * 0.1);
}

TEST(DBMCommonImplTest, SecondaryHash) {
  EXPECT_EQ(1765794342254572867ULL, tkrzw::SecondaryHash("abc", tkrzw::UINT64MAX));
  EXPECT_EQ(702176507ULL, tkrzw::SecondaryHash("abc", tkrzw::UINT32MAX));
  constexpr int32_t num_records = 1000000;
  constexpr int32_t num_shards = 1000;
  constexpr double mean_count = num_records * 1.0 / num_shards;
  std::vector<int32_t> shards(num_shards, 0);
  for (int32_t i = 0; i < num_records; i++) {
    const int64_t shard_index = tkrzw::SecondaryHash(tkrzw::ToString(i), num_shards);
    EXPECT_LT(shard_index, num_shards);
    shards[shard_index]++;
  }
  int32_t min_count = tkrzw::INT32MAX;
  int32_t max_count = 0;
  double variance = 0;
  for (const auto count : shards) {
    min_count = std::min(min_count, count);
    max_count = std::max(max_count, count);
    variance += std::pow(count - mean_count, 2);
  }
  variance /= num_shards;
  const double stddev = std::sqrt(variance);
  EXPECT_GT(min_count, mean_count * 0.8);
  EXPECT_LT(max_count, mean_count / 0.8);
  EXPECT_LT(stddev, mean_count * 0.1);
}

TEST(DBMCommonImplTest, IsPrimeNumber) {
  const std::vector<std::pair<uint64_t, bool>> samples = {
    {0, false}, {1, false}, {2, true}, {3, true}, {4, false},
    {5, true}, {6, false}, {7, true}, {8, false}, {9, false},
    {10, false}, {11, true}, {12, false}, {13, true}, {14, false},
    {4294967279, true}, {4294967296, false}, {4294967333, false},
  };
  for (const auto& sample : samples) {
    EXPECT_EQ(sample.second, tkrzw::IsPrimeNumber(sample.first));
  }
}

TEST(DBMCommonImplTest, GetHashBucketSize) {
  EXPECT_EQ(1, tkrzw::GetHashBucketSize(0));
  EXPECT_EQ(1, tkrzw::GetHashBucketSize(1));
  EXPECT_EQ(10, tkrzw::GetHashBucketSize(10));
  EXPECT_EQ(100, tkrzw::GetHashBucketSize(100));
  EXPECT_EQ(257, tkrzw::GetHashBucketSize(256));
  EXPECT_EQ(65537, tkrzw::GetHashBucketSize(65536));
  EXPECT_EQ(16777259ULL, tkrzw::GetHashBucketSize(16777216ULL));
  EXPECT_EQ(68719476767ULL, tkrzw::GetHashBucketSize(68719476736ULL));
}

// END OF FILE
