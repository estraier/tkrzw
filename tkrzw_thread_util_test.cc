/*************************************************************************************************
 * Tests for tkrzw_thread_util.h
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
#include "tkrzw_thread_util.h"
#include "tkrzw_str_util.h"

using namespace testing;

// Main routine
int main(int argc, char** argv) {
  InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

TEST(ThreadUtilTest, GetWallTimeAndSleep) {
  const double start_time = tkrzw::GetWallTime();
  EXPECT_GT(start_time, 0);
  tkrzw::Sleep(0.001);
  const double end_time = tkrzw::GetWallTime();
  EXPECT_GT(end_time, start_time);
}

TEST(ThreadUtilTest, SlottedMutex) {
  constexpr int32_t num_threads = 5;
  constexpr int32_t num_iterations = 20000;
  constexpr int32_t num_slots = 10;
  tkrzw::SlottedMutex mutex(num_slots);
  std::vector<uint8_t> values(num_slots, false);
  auto func = [&](int32_t id) {
    std::mt19937 mt(id);
    std::uniform_int_distribution<int32_t> all_dist(0, mutex.GetNumSlots() * 10 - 1);
    std::uniform_int_distribution<int32_t> slot_index_dist(0, mutex.GetNumSlots() - 1);
    std::uniform_int_distribution<int32_t> writable_dist(0, 3);
    std::uniform_int_distribution<int32_t> mode_dist(0, 2);
    for (int32_t i = 0; i < num_iterations; i++) {
      const bool all = all_dist(mt) == 0;
      const int32_t slot_index = slot_index_dist(mt);
      const bool writable = writable_dist(mt) == 0;
      if (mode_dist(mt) == 0) {
        tkrzw::ScopedSlottedLock lock(mutex, all ? -1 : slot_index, writable);
        if (all) {
          for (int32_t j = 0; j < num_slots; j++) {
            EXPECT_FALSE(values[j]);
            if (writable) {
              values[j] = true;
            }
          }
        } else {
          EXPECT_FALSE(values[slot_index]);
          if (writable) {
            values[slot_index] = true;
          }
        }
        std::this_thread::yield();
        if (all) {
          for (int32_t j = 0; j < num_slots; j++) {
            if (writable) {
              EXPECT_TRUE(values[j]);
              values[j] = false;
            } else {
              EXPECT_FALSE(values[j]);
            }
          }
        } else {
          if (writable) {
            EXPECT_TRUE(values[slot_index]);
            values[slot_index] = false;
          } else {
            EXPECT_FALSE(values[slot_index]);
          }
        }
      } else {
        if (all) {
          if (writable) {
            mutex.LockAll();
            for (int32_t j = 0; j < num_slots; j++) {
              EXPECT_FALSE(values[j]);
              values[j] = true;
            }
            std::this_thread::yield();
            for (int32_t j = 0; j < num_slots; j++) {
              EXPECT_TRUE(values[j]);
              values[j] = false;
            }
            mutex.UnlockAll();
          } else {
            mutex.LockAllShared();
            for (int32_t j = 0; j < num_slots; j++) {
              EXPECT_FALSE(values[j]);
            }
            std::this_thread::yield();
            for (int32_t j = 0; j < num_slots; j++) {
              EXPECT_FALSE(values[j]);
            }
            mutex.UnlockAllShared();
          }
        } else {
          if (writable) {
            mutex.LockOne(slot_index);
            EXPECT_FALSE(values[slot_index]);
            values[slot_index] = true;
            std::this_thread::yield();
            EXPECT_TRUE(values[slot_index]);
            values[slot_index] = false;
            mutex.UnlockOne(slot_index);
          } else {
            mutex.LockOneShared(slot_index);
            EXPECT_FALSE(values[slot_index]);
            std::this_thread::yield();
            EXPECT_FALSE(values[slot_index]);
            mutex.UnlockOneShared(slot_index);
          }
        }
      }
    }
  };
  std::vector<std::thread> threads;
  for (int32_t i = 0; i < num_threads; i++) {
    threads.emplace_back(std::thread(func, i));
  }
  for (auto& thread : threads) {
    thread.join();
  }
}

TEST(ThreadUtilTest, HashMutex) {
  constexpr int32_t num_threads = 5;
  constexpr int32_t num_iterations = 20000;
  constexpr int32_t num_slots = 10;
  constexpr int32_t max_buckets = 100;
  auto hash_func = [](std::string_view data, uint64_t num_buckets) -> uint64_t {
    return tkrzw::HashMurmur(data, 2147483647) % num_buckets;
  };
  tkrzw::HashMutex mutex(num_slots, max_buckets, hash_func);
  std::vector<uint8_t> values(max_buckets, false);
  auto func = [&](int32_t id) {
    std::mt19937 mt(id);
    std::uniform_int_distribution<int32_t> rehash_dist(0, num_iterations / 10 - 1);
    std::uniform_int_distribution<int32_t> mode_dist(0, 2);
    std::uniform_int_distribution<int32_t> all_dist(0, mutex.GetNumSlots() * 10 - 1);
    std::uniform_int_distribution<int32_t> writable_dist(0, 3);
    std::uniform_int_distribution<int32_t> iter_dist(0, 5);
    std::uniform_int_distribution<int32_t> record_dist(0, tkrzw::INT32MAX);
    std::uniform_int_distribution<int32_t> num_buckets_dist(1, max_buckets);
    for (int32_t i = 0; i < num_iterations; i++) {
      const bool writable = writable_dist(mt) == 0;
      if (rehash_dist(mt) == 0) {
        mutex.LockAll();
        mutex.Rehash(num_buckets_dist(mt));
        mutex.UnlockAll();
      } else if (mode_dist(mt) == 0) {
        if (all_dist(mt) == 0) {
          tkrzw::ScopedHashLock lock(mutex, writable);
          const int64_t num_buckets = mutex.GetNumBuckets();
          if (writable) {
            for (int32_t j = 0; j < num_buckets; j++) {
              EXPECT_FALSE(values[j]);
              values[j] = true;
            }
            std::this_thread::yield();
            for (int32_t j = 0; j < num_buckets; j++) {
              EXPECT_TRUE(values[j]);
              values[j] = false;
            }
          } else {
            for (int32_t j = 0; j < num_buckets; j++) {
              EXPECT_FALSE(values[j]);
            }
            std::this_thread::yield();
            for (int32_t j = 0; j < num_buckets; j++) {
              EXPECT_FALSE(values[j]);
            }
          }
        } else if (iter_dist(mt) == 0) {
          int64_t bucket_index = num_buckets_dist(mt) - 1;
          tkrzw::ScopedHashLock lock(mutex, bucket_index, writable);
          bucket_index = lock.GetBucketIndex();
          if (bucket_index >= 0) {
            if (writable) {
              EXPECT_FALSE(values[bucket_index]);
              values[bucket_index] = true;
              std::this_thread::yield();
              EXPECT_TRUE(values[bucket_index]);
              values[bucket_index] = false;
            } else {
              EXPECT_FALSE(values[bucket_index]);
              std::this_thread::yield();
              EXPECT_FALSE(values[bucket_index]);
            }
          }
        } else {
          const std::string& key = tkrzw::ToString(record_dist(mt));
          tkrzw::ScopedHashLock lock(mutex, key, writable);
          const int64_t bucket_index = lock.GetBucketIndex();
          EXPECT_FALSE(values[bucket_index]);
          if (writable) {
            EXPECT_FALSE(values[bucket_index]);
            values[bucket_index] = true;
            std::this_thread::yield();
            EXPECT_TRUE(values[bucket_index]);
            values[bucket_index] = false;
          } else {
            EXPECT_FALSE(values[bucket_index]);
            std::this_thread::yield();
            EXPECT_FALSE(values[bucket_index]);
          }
        }
      } else {
        if (all_dist(mt) == 0) {
          if (writable) {
            mutex.LockAll();
            const int64_t num_buckets = mutex.GetNumBuckets();
            for (int32_t j = 0; j < num_buckets; j++) {
              EXPECT_FALSE(values[j]);
              values[j] = true;
            }
            std::this_thread::yield();
            for (int32_t j = 0; j < num_buckets; j++) {
              EXPECT_TRUE(values[j]);
              values[j] = false;
            }
            mutex.UnlockAll();
          } else {
            mutex.LockAllShared();
            const int64_t num_buckets = mutex.GetNumBuckets();
            for (int32_t j = 0; j < num_buckets; j++) {
              EXPECT_FALSE(values[j]);
            }
            std::this_thread::yield();
            for (int32_t j = 0; j < num_buckets; j++) {
              EXPECT_FALSE(values[j]);
            }
            mutex.UnlockAllShared();
          }
        } else if (iter_dist(mt) == 0) {
          const int64_t bucket_index = num_buckets_dist(mt) - 1;
          if (writable) {
            if (mutex.LockOne(bucket_index)) {
              EXPECT_FALSE(values[bucket_index]);
              values[bucket_index] = true;
              std::this_thread::yield();
              EXPECT_TRUE(values[bucket_index]);
              values[bucket_index] = false;
              mutex.UnlockOne(bucket_index);
            }
          } else {
            if (mutex.LockOneShared(bucket_index)) {
              EXPECT_FALSE(values[bucket_index]);
              std::this_thread::yield();
              EXPECT_FALSE(values[bucket_index]);
              mutex.UnlockOneShared(bucket_index);
            }
          }
        } else {
          const std::string& key = tkrzw::ToString(record_dist(mt));
          if (writable) {
            const int64_t bucket_index = mutex.LockOne(key);
            EXPECT_FALSE(values[bucket_index]);
            values[bucket_index] = true;
            if (i % 3 == 0) {
              EXPECT_EQ(bucket_index, mutex.GetBucketIndex(key));
            } else {
              std::this_thread::yield();
            }
            EXPECT_TRUE(values[bucket_index]);
            values[bucket_index] = false;
            mutex.UnlockOne(bucket_index);
          } else {
            const int64_t bucket_index = mutex.LockOneShared(key);
            EXPECT_FALSE(values[bucket_index]);
            if (i % 3 == 0) {
              EXPECT_EQ(bucket_index, mutex.GetBucketIndex(key));
            } else {
              std::this_thread::yield();
            }
            EXPECT_FALSE(values[bucket_index]);
            mutex.UnlockOneShared(bucket_index);
          }
        }
      }
    }
  };
  std::vector<std::thread> threads;
  for (int32_t i = 0; i < num_threads; i++) {
    threads.emplace_back(std::thread(func, i));
  }
  for (auto& thread : threads) {
    thread.join();
  }
}

// END OF FILE
