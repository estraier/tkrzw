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
#include "tkrzw_hash_util.h"
#include "tkrzw_str_util.h"
#include "tkrzw_thread_util.h"
#include "tkrzw_time_util.h"

using namespace testing;

// Main routine
int main(int argc, char** argv) {
  InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

TEST(ThreadUtilTest, SleepThread) {
  const double start_time = tkrzw::GetWallTime();
  tkrzw::SleepThread(0.001);
  const double end_time = tkrzw::GetWallTime();
  EXPECT_GT(end_time, start_time);
}

TEST(ThreadUtilTest, SpinMutex) {
  constexpr int32_t num_threads = 5;
  constexpr int32_t num_iterations = 500000;
  tkrzw::SpinMutex mutex;
  int64_t count = 0;
  auto func = [&]() {
                for (int32_t i = 0; i < num_iterations; i++) {
                  if (i % 3== 0) {
                    while (!mutex.try_lock()) {
                      std::this_thread::yield();
                    }
                  } else {
                    mutex.lock();
                  }
                  volatile int64_t my_count = count;
                  my_count += 1;
                  if (i % 100 == 0) {
                    std::this_thread::yield();
                  }
                  count = my_count;
                  mutex.unlock();
                }
              };
  std::vector<std::thread> threads;
  for (int32_t i = 0; i < num_threads; i++) {
    threads.emplace_back(std::thread(func));
  }
  for (auto& thread : threads) {
    thread.join();
  }
  EXPECT_EQ(num_threads * num_iterations, count);
}

TEST(ThreadUtilTest, SpinSharedMutex) {
  constexpr int32_t num_threads = 5;
  constexpr int32_t num_iterations = 100000;
  tkrzw::SpinSharedMutex mutex;
  int64_t count = 0;
  std::atomic_uint32_t count_writers(0);
  std::atomic_uint32_t count_readers(0);
  auto func = [&]() {
                for (int32_t i = 0; i < num_iterations; i++) {
                  if (i % 3 == 0) {
                    while (!mutex.try_lock()) {
                      std::this_thread::yield();
                    }
                  } else {
                    mutex.lock();
                  }
                  EXPECT_EQ(0, count_writers.fetch_add(1));
                  EXPECT_EQ(0, count_readers.load());
                  volatile int64_t my_count = count;
                  my_count += 1;
                  if (i % 100 == 0) {
                    std::this_thread::yield();
                  }
                  count = my_count;
                  EXPECT_EQ(1, count_writers.fetch_sub(1));
                  EXPECT_EQ(0, count_readers.load());
                  mutex.unlock();
                  for (int32_t j = 0; j < 4; j++) {
                    if (i % 3 == 0) {
                      while (!mutex.try_lock_shared()) {
                        std::this_thread::yield();
                      }
                    } else {
                      mutex.lock_shared();
                    }
                    EXPECT_EQ(0, count_writers.load());
                    count_readers.fetch_add(1);
                    if (i % 100 == 0) {
                      std::this_thread::yield();
                    }
                    EXPECT_EQ(0, count_writers.load());
                    count_readers.fetch_sub(1);
                    if (i % 5 == 0 && mutex.try_upgrade(i % 7 != 0)) {
                      EXPECT_EQ(0, count_writers.fetch_add(1));
                      EXPECT_EQ(0, count_readers.load());
                      if (i % 8 == 0) {
                        std::this_thread::yield();
                      }
                      EXPECT_EQ(1, count_writers.fetch_sub(1));
                      EXPECT_EQ(0, count_readers.load());
                      if (i % 2 == 0) {
                        mutex.downgrade();
                        if (i % 100 == 0) {
                          std::this_thread::yield();
                        }
                        mutex.unlock_shared();
                      } else {
                        mutex.unlock();
                      }
                    } else {
                      mutex.unlock_shared();
                    }
                  }
                }
              };
  std::vector<std::thread> threads;
  for (int32_t i = 0; i < num_threads; i++) {
    threads.emplace_back(std::thread(func));
  }
  for (auto& thread : threads) {
    thread.join();
  }
  EXPECT_EQ(num_threads * num_iterations, count);
}

TEST(ThreadUtilTest, SpinWPSharedMutex) {
  constexpr int32_t num_threads = 5;
  constexpr int32_t num_iterations = 100000;
  tkrzw::SpinWPSharedMutex mutex;
  int64_t count = 0;
  std::atomic_uint32_t count_writers(0);
  std::atomic_uint32_t count_readers(0);
  auto func = [&]() {
                for (int32_t i = 0; i < num_iterations; i++) {
                  if (i % 3 == 0) {
                    while (!mutex.try_lock()) {
                      std::this_thread::yield();
                    }
                  } else {
                    mutex.lock();
                  }
                  EXPECT_EQ(0, count_writers.fetch_add(1));
                  EXPECT_EQ(0, count_readers.load());
                  volatile int64_t my_count = count;
                  my_count += 1;
                  if (i % 100 == 0) {
                    std::this_thread::yield();
                  }
                  count = my_count;
                  EXPECT_EQ(1, count_writers.fetch_sub(1));
                  EXPECT_EQ(0, count_readers.load());
                  mutex.unlock();
                  for (int32_t j = 0; j < 4; j++) {
                    if (i % 3 == 0) {
                      while (!mutex.try_lock_shared()) {
                        std::this_thread::yield();
                      }
                    } else {
                      mutex.lock_shared();
                    }
                    EXPECT_EQ(0, count_writers.load());
                    count_readers.fetch_add(1);
                    if (i % 100 == 0) {
                      std::this_thread::yield();
                    }
                    EXPECT_EQ(0, count_writers.load());
                    count_readers.fetch_sub(1);
                    if (i % 5 == 0 && mutex.try_upgrade(i % 7 != 0)) {
                      EXPECT_EQ(0, count_writers.fetch_add(1));
                      EXPECT_EQ(0, count_readers.load());
                      if (i % 8 == 0) {
                        std::this_thread::yield();
                      }
                      EXPECT_EQ(1, count_writers.fetch_sub(1));
                      EXPECT_EQ(0, count_readers.load());
                      if (i % 2 == 0) {
                        mutex.downgrade();
                        if (i % 100 == 0) {
                          std::this_thread::yield();
                        }
                        mutex.unlock_shared();
                      } else {
                        mutex.unlock();
                      }
                    } else {
                      mutex.unlock_shared();
                    }
                  }
                }
              };
  std::vector<std::thread> threads;
  for (int32_t i = 0; i < num_threads; i++) {
    threads.emplace_back(std::thread(func));
  }
  for (auto& thread : threads) {
    thread.join();
  }
  EXPECT_EQ(num_threads * num_iterations, count);
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
  constexpr int32_t num_iterations = 30000;
  constexpr int32_t num_slots = 10;
  constexpr int32_t max_buckets = 100;
  auto hash_func = [](std::string_view data, uint64_t num_buckets) -> uint64_t {
    return tkrzw::HashMurmur(data, 2147483647) % num_buckets;
  };
  tkrzw::HashMutex mutex(num_slots, max_buckets, hash_func);
  std::vector<uint8_t> values(max_buckets, false);
  auto func = [&](int32_t id) {
    std::mt19937 mt(id);
    std::uniform_int_distribution<int32_t> writable_dist(0, 3);
    std::uniform_int_distribution<int32_t> rehash_dist(0, num_iterations / 10 - 1);
    std::uniform_int_distribution<int32_t> mode_dist(0, 2);
    std::uniform_int_distribution<int32_t> all_dist(0, mutex.GetNumSlots() * 10 - 1);
    std::uniform_int_distribution<int32_t> iter_dist(0, 5);
    std::uniform_int_distribution<int32_t> record_dist(0, tkrzw::INT32MAX);
    std::uniform_int_distribution<int32_t> multi_dist(0, 5);
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
        } else if (multi_dist(mt) == 0) {
          std::vector<std::string> keys;
          std::vector<std::string_view> key_views;
          for (int32_t j = 0; j < 3; j++) {
            keys.emplace_back(tkrzw::ToString(record_dist(mt)));
          }
          for (const auto& key : keys) {
            key_views.emplace_back(key);
          }
          if (writable) {
            tkrzw::ScopedHashLockMulti lock(mutex, key_views, true);
            const std::vector<int64_t>& bucket_indices = lock.GetBucketIndices();
            const std::set<int64_t> uniq_indices(bucket_indices.begin(), bucket_indices.end());
            for (int64_t bucket_index : uniq_indices) {
              EXPECT_FALSE(values[bucket_index]);
              values[bucket_index] = true;
            }
            if (i % 3 != 0) {
              std::this_thread::yield();
            }
            for (int64_t bucket_index : uniq_indices) {
              EXPECT_TRUE(values[bucket_index]);
              values[bucket_index] = false;
            }
          } else {
            tkrzw::ScopedHashLockMulti lock(mutex, key_views, false);
            const std::vector<int64_t>& bucket_indices = lock.GetBucketIndices();
            const std::set<int64_t> uniq_indices(bucket_indices.begin(), bucket_indices.end());
            for (int64_t bucket_index : uniq_indices) {
              EXPECT_FALSE(values[bucket_index]);
            }
            if (i % 3 != 0) {
              std::this_thread::yield();
            }
            for (int64_t bucket_index : uniq_indices) {
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
        } else if (multi_dist(mt) == 0) {
          std::vector<std::string> keys;
          std::vector<std::string_view> key_views;
          for (int32_t j = 0; j < 3; j++) {
            keys.emplace_back(tkrzw::ToString(record_dist(mt)));
          }
          for (const auto& key : keys) {
            key_views.emplace_back(key);
          }
          if (writable) {
            const std::vector<int64_t> bucket_indices = mutex.LockMulti(key_views);
            const std::set<int64_t> uniq_indices(bucket_indices.begin(), bucket_indices.end());
            for (int64_t bucket_index : uniq_indices) {
              EXPECT_FALSE(values[bucket_index]);
              values[bucket_index] = true;
            }
            if (i % 3 != 0) {
              std::this_thread::yield();
            }
            for (int64_t bucket_index : uniq_indices) {
              EXPECT_TRUE(values[bucket_index]);
              values[bucket_index] = false;
            }
            mutex.UnlockMulti(bucket_indices);
          } else {
            const std::vector<int64_t> bucket_indices = mutex.LockMultiShared(key_views);
            const std::set<int64_t> uniq_indices(bucket_indices.begin(), bucket_indices.end());
            for (int64_t bucket_index : uniq_indices) {
              EXPECT_FALSE(values[bucket_index]);
            }
            if (i % 3 != 0) {
              std::this_thread::yield();
            }
            for (int64_t bucket_index : uniq_indices) {
              EXPECT_FALSE(values[bucket_index]);
            }
            mutex.UnlockMultiShared(bucket_indices);
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

TEST(ThreadUtilTest, TaskQueue) {
  constexpr int32_t num_tasks = 10000;
  std::atomic_int32_t count = 0;
  tkrzw::TaskQueue queue;
  queue.Start(10);
  for (int32_t i = 0; i < num_tasks; i++) {
    auto task = [&](int32_t id) {
                  count.fetch_add(1);
                  if (id % 10 == 0) {
                    std::this_thread::yield();
                  }
                };
    queue.Add(std::bind(task, i));
    if (i % 10 == 0) {
      std::this_thread::yield();
    }
  }
  queue.Stop(tkrzw::INT32MAX);
  EXPECT_EQ(0, queue.GetSize());
  EXPECT_EQ(num_tasks, count.load());
}

TEST(ThreadUtilTest, TaskQueueFuture) {
  constexpr int32_t num_tasks = 4000;
  std::atomic_int32_t count = 0;
  tkrzw::TaskQueue queue;
  queue.Start(10);
  for (int32_t i = 0; i < num_tasks; i++) {
    std::promise<void> p;
    std::future<void> f = p.get_future();
    auto task = [&]() {
                  count.fetch_add(1);
                  p.set_value();
                };
    queue.Add(task);
    f.wait();
  }
  EXPECT_EQ(0, queue.GetSize());
  queue.Stop(0.0);
  EXPECT_EQ(num_tasks, count.load());
}

TEST(ThreadUtilTest, WaitCounter) {
  tkrzw::WaitCounter wc1;
  EXPECT_EQ(0, wc1.Get());
  wc1.Add(2);
  EXPECT_EQ(2, wc1.Get());
  EXPECT_FALSE(wc1.Wait(0));
  wc1.Done(2);
  EXPECT_TRUE(wc1.Wait(-1));
  EXPECT_TRUE(wc1.Wait(0));
  EXPECT_TRUE(wc1.Wait(0.00001));
  constexpr int32_t num_threads = 10;
  constexpr int32_t num_loops = 10000;
  tkrzw::WaitCounter wc2(num_threads);
  std::atomic_int32_t count(0);
  auto task = [&]() {
                for (int32_t i = 0; i < num_loops; i++) {
                  std::this_thread::yield();
                  count.fetch_add(1);
                }
                wc2.Done();
              };
  std::vector<std::thread> threads;
  for (int32_t i = 0; i < num_threads; i++) {
    threads.emplace_back(std::thread(task));
  }
  wc2.Wait(3);
  EXPECT_EQ(0, wc2.Get());
  EXPECT_EQ(num_threads * num_loops, count.load());
  for (auto& thread : threads) {
    thread.join();
  }
}

TEST(ThreadUtilTest, SignalBroker) {
  constexpr int32_t num_threads = 5;
  constexpr int32_t num_loops = 1000;
  for (const bool broadcast : {true, false}) {
    tkrzw::WaitCounter wc(num_threads);
    tkrzw::SignalBroker broker;
    auto task = [&](int32_t id) {
      for (int32_t i = 0; i < num_loops; i++) {
        tkrzw::SignalBroker::Waiter waiter(&broker);
        if (broadcast && i % 2 == 0) {
          EXPECT_TRUE(waiter.Wait());
        } else {
          while (!waiter.Wait(0.00001)) {
          }
        }
        if ((id + i) % 3 == 0) {
          std::this_thread::yield();
        }
      }
      wc.Done();
    };
    std::vector<std::thread> threads;
    for (int32_t i = 0; i < num_threads; i++) {
      threads.emplace_back(std::thread(task, i));
    }
    int32_t count = 0;
    while (!wc.Wait(0)) {
      for (int32_t i = 0; i < num_threads; i++) {
        if (broker.Send(broadcast || (count + i) % 5 > 0)) {
          count++;
        }
        if ((count + i) % 3 == 0) {
          std::this_thread::yield();
        }
      }
    }
    for (auto& thread : threads) {
      thread.join();
    }
  }
}

TEST(ThreadUtilTest, KeySignalBroker) {
  constexpr int32_t num_threads = 5;
  constexpr int32_t num_loops = 1000;
  for (const bool broadcast : {true, false}) {
    tkrzw::WaitCounter wc(num_threads);
    tkrzw::KeySignalBroker<int32_t> broker;
    auto task = [&](int32_t id) {
      for (int32_t i = 0; i < num_loops; i++) {
        tkrzw::KeySignalBroker<int32_t>::Waiter waiter(&broker, id);
        if (broadcast && i % 2 == 0) {
          EXPECT_TRUE(waiter.Wait());
        } else {
          while (!waiter.Wait(0.00001)) {
          }
        }
        if ((id + i) % 3 == 0) {
          std::this_thread::yield();
        }
      }
      wc.Done();
    };
    std::vector<std::thread> threads;
    for (int32_t i = 0; i < num_threads; i++) {
      threads.emplace_back(std::thread(task, i));
    }
    int32_t count = 0;
    while (!wc.Wait(0)) {
      for (int32_t i = 0; i < num_threads; i++) {
        if (broker.Send(i, broadcast || (count + i) % 5 > 0)) {
          count++;
        }
        if ((count + i) % 3 == 0) {
          std::this_thread::yield();
        }
      }
    }
    for (auto& thread : threads) {
      thread.join();
    }
    EXPECT_EQ(num_threads * num_loops, count);
  }
}

TEST(ThreadUtilTest, SlottedKeySignalBroker) {
  constexpr int32_t num_threads = 10;
  constexpr int32_t num_loops = 1000;
  for (const bool broadcast : {true, false}) {
    tkrzw::WaitCounter wc(num_threads);
    tkrzw::SlottedKeySignalBroker<int32_t> broker(4);
    auto task = [&](int32_t id) {
      for (int32_t i = 0; i < num_loops; i++) {
        tkrzw::SlottedKeySignalBroker<int32_t>::Waiter waiter(&broker, id);
        if (broadcast && i % 2 == 0) {
          EXPECT_TRUE(waiter.Wait());
        } else {
          while (!waiter.Wait(0.00001)) {
          }
        }
        if ((id + i) % 3 == 0) {
          std::this_thread::yield();
        }
      }
      wc.Done();
    };
    std::vector<std::thread> threads;
    for (int32_t i = 0; i < num_threads; i++) {
      threads.emplace_back(std::thread(task, i));
    }
    int32_t count = 0;
    while (!wc.Wait(0)) {
      for (int32_t i = 0; i < num_threads; i++) {
        if (broker.Send(i, broadcast || (count + i) % 5 > 0)) {
          count++;
        }
        if ((count + i) % 3 == 0) {
          std::this_thread::yield();
        }
      }
    }
    for (auto& thread : threads) {
      thread.join();
    }
    EXPECT_EQ(num_threads * num_loops, count);
  }
}

TEST(ThreadUtilTest, ScopedCounter) {
  std::atomic_int32_t count(0);
  {
    tkrzw::ScopedCounter sc(&count);
    EXPECT_EQ(1, count.load());
  }
  EXPECT_EQ(0, count.load());
  {
    tkrzw::ScopedCounter sc(&count, 10);
    EXPECT_EQ(10, count.load());
  }
  EXPECT_EQ(0, count.load());
}

// END OF FILE
