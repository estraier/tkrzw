/*************************************************************************************************
 * Threading utilities
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

#ifndef _TKRZW_THREAD_UTIL_H
#define _TKRZW_THREAD_UTIL_H

#include <atomic>
#include <shared_mutex>
#include <string>
#include <string_view>
#include <vector>

#include <cinttypes>

#include "tkrzw_lib_common.h"

namespace tkrzw {

/**
 * Gets the number of seconds since the UNIX epoch.
 * @return The number of seconds since the UNIX epoch with microsecond precision.
 */
double GetWallTime();

/**
 * Sleeps the current thread.
 * @param sec The duration in seconds to sleep for.
 */
void Sleep(double sec);

/**
 * Spin lock mutex.
 */
class SpinLock final {
 public:
  /**
   * Constructor.
   */
  SpinLock() {}

  /**
   * Copy and assignment are disabled.
   */
  explicit SpinLock(const SpinLock& rhs) = delete;
  SpinLock& operator =(const SpinLock& rhs) = delete;

  /**
   * Gets exclusive ownership of the lock.
   */
  void lock() {
    while (lock_.test_and_set(std::memory_order_acquire)) {
      std::this_thread::yield();
    }
  }

  /**
   * Releases exclusive ownership of the lock.
   */
  void unlock() {
    lock_.clear(std::memory_order_release);
  }

 private:
  /** Atomic flat of locked state. */
  std::atomic_flag lock_ = ATOMIC_FLAG_INIT;
};

/**
 * Slotted shared mutex.
 */
class SlottedMutex final {
 public:
  /**
   * Constructor.
   * @param num_slots The number of slots.
   */
  explicit SlottedMutex(int32_t num_slots);

  /**
   * Destructor.
   */
  ~SlottedMutex();

  /**
   * Copy and assignment are disabled.
   */
  explicit SlottedMutex(const SlottedMutex& rhs) = delete;
  SlottedMutex& operator =(const SlottedMutex& rhs) = delete;

  /**
   * Gets the number of the slots.
   * @ return the number of the slots.
   */
  int32_t GetNumSlots() const;

  /**
   * Gets exclusive ownership of a slot.
   * @param index The index of the slot to lock.
   */
  void LockOne(int32_t index);

  /**
   * Releases exclusive ownership of a slot.
   * @param index The index of the slot to unlock.
   */
  void UnlockOne(int32_t index);

  /**
   * Gets exclusive ownership of all slots.
   */
  void LockAll();

  /**
   * Releases exclusive ownership of all slots.
   */
  void UnlockAll();

  /**
   * Gets shared ownership of a slot.
   * @param index The index of the slot to lock.
   */
  void LockOneShared(int32_t index);

  /**
   * Releases shared ownership of a slot.
   * @param index The index of the slot to unlock.
   */
  void UnlockOneShared(int32_t index);

  /**
   * Gets shared ownership of all slots.
   */
  void LockAllShared();

  /**
   * Releases exclusive ownership of all slots.
   */
  void UnlockAllShared();

 private:
  /** The number of the slots. */
  int32_t num_slots_;
  /** The array of the slots. */
  std::shared_timed_mutex* slots_;
};

/**
 * Scoped lock with a slotted shared mutex.
 */
class ScopedSlottedLock final {
 public:
  /**
   * Constructor.
   * @param mutex A slotted shared mutex.
   * @param index The index of a slot.  Negative means all slots.
   * @param writable True for exclusive lock.  False for shared lock.
   */
  ScopedSlottedLock(SlottedMutex& mutex, int32_t index, bool writable);

  /**
   * Destructor.
   */
  ~ScopedSlottedLock();

  /**
   * Copy and assignment are disabled.
   */
  explicit ScopedSlottedLock(const ScopedSlottedLock& rhs) = delete;
  ScopedSlottedLock& operator =(const ScopedSlottedLock& rhs) = delete;

 private:
  /** The slotted mutex. */
  SlottedMutex& mutex_;
  /** The index of the locked slot. */
  int32_t index_;
  /** Whether it is an exclusive lock. */
  bool writable_;
};

/**
 * Mutex for a hash table.
 */
class HashMutex final {
public:
  /**
   * Constructor.
   * @param num_slots The number of slots.
   * @param num_buckets The number of buckets.
   * @param hash_func A hash function which takes a string view object and a bucket number and
   * returns a bucket index.
   */
  HashMutex(int32_t num_slots, int64_t num_buckets,
            uint64_t (*hash_func)(std::string_view, uint64_t));

  /**
   * Destructor.
   */
  ~HashMutex();

  /**
   * Copy and assignment are disabled.
   */
  explicit HashMutex(const HashMutex& rhs) = delete;
  HashMutex& operator =(const HashMutex& rhs) = delete;

  /**
   * Gets the number of the slots.
   * @ return the number of the slots.
   */
  int32_t GetNumSlots() const;

  /**
   * Gets the number of the buckets.
   * @return The number of the buckets.
   */
  int64_t GetNumBuckets() const;

  /**
   * Modifies the number of buckets.
   * @param num_buckets The new number of the buckets.
   * @details Precondition: The thread must have called the LockOne method.
   */
  void Rehash(int64_t num_buckets);

  /**
   * Gets the index of the bucket of data.
   * @param data The data set in the hash table.
   * @return The index of the bucket which the data belongs to.
   * @details Precondition: The thread must have locked the bucket or all buckets.
   */
  int64_t GetBucketIndex(std::string_view data);

  /**
   * Gets exclusive ownership of a slot of a bucket.
   * @param data The data to be set in the hash table.
   * @return The index of the bucket which the data should belong to.
   */
  int64_t LockOne(std::string_view data);

  /**
   * Gets exclusive ownership of a slot by a bucket index.
   * @param bucket_index The index of the bucket to lock.
   * @return True on success or False on failure.
   */
  bool LockOne(int64_t bucket_index);

  /**
   * Releases exclusive ownership of a slot of a bucket.
   * @param bucket_index The index of the bucket to unlock.
   */
  void UnlockOne(int64_t bucket_index);

  /**
   * Gets shared ownership of a slot by a bucket index.
   * @param data The data to be set in the hash table.
   * @return The index of the bucket which the data should belong to.
   */
  int64_t LockOneShared(std::string_view data);

  /**
   * Gets shared ownership of a slot of a bucket.
   * @param bucket_index The index of the bucket to lock.
   * @return True on success or False on failure.
   */
  bool LockOneShared(int64_t bucket_index);

  /**
   * Releases shared ownership of a slot of a bucket.
   * @param bucket_index The index of the bucket to unlock.
   */
  void UnlockOneShared(int64_t bucket_index);

  /**
   * Gets exclusive ownership of all slots.
   */
  void LockAll();

  /**
   * Releases exclusive ownership of all slots.
   */
  void UnlockAll();

  /**
   * Gets shared ownership of all slots.
   */
  void LockAllShared();

  /**
   * Releases shared ownership of all slots.
   */
  void UnlockAllShared();

  /**
   * Gets exclusive ownership of slots of multiple buckets.
   * @param data_list The data list to be set in the hash table.
   * @return The indices of the buckets which the data list should belong to.
   */
  std::vector<int64_t> LockMulti(const std::vector<std::string_view>& data_list);

  /**
   * Releases exclusive ownership of slots of multiple buckets.
   * @param bucket_indices The indices of the buckets to unlock.
   */
  void UnlockMulti(const std::vector<int64_t>& bucket_indices);

  /**
   * Gets shared ownership of slots of multiple buckets.
   * @param data_list The data list to be set in the hash table.
   * @return The indices of the buckets which the data list should belong to.
   */
  std::vector<int64_t> LockMultiShared(const std::vector<std::string_view>& data_list);

  /**
   * Releases shared ownership of slots of multiple buckets.
   * @param bucket_indices The indices of the buckets to unlock.
   */
  void UnlockMultiShared(const std::vector<int64_t>& bucket_indices);

 private:
  /** The number of the slots. */
  int32_t num_slots_;
  /** The number of the buckets. */
  std::atomic_int64_t num_buckets_;
  /** The hash function. */
  uint64_t (*hash_func_)(std::string_view, uint64_t);
  /** The array of the slots. */
  std::shared_timed_mutex* slots_;
};

/**
 * Scoped lock with a mutex for a hash table.
 */
class ScopedHashLock final {
 public:
  /**
   * Constructor to lock one bucket.
   * @param mutex A hash mutex.
   * @param data The data to be set in the hash table.
   * @param writable True for exclusive lock or false for shared lock.
   */
  ScopedHashLock(HashMutex& mutex, std::string_view data, bool writable);

  /**
   * Constructro to lock all buckets.
   * @param mutex A hash mutex.
   * @param writable True for exclusive lock or false for shared lock.
   */
  ScopedHashLock(HashMutex& mutex, bool writable);

  /**
   * Constructor to lock the bucket specific to an index.
   * @param mutex A hash mutex.
   * @param bucket_index The index of the bucket to lock.
   * @param writable True for exclusive lock or false for shared lock.
   * @details Only this constructor can fail.  It is because of rehashing.
   */
  ScopedHashLock(HashMutex& mutex, int64_t bucket_index, bool writable);

  /**
   * Destructor.
   */
  ~ScopedHashLock();

  /**
   * Copy and assignment are disabled.
   */
  explicit ScopedHashLock(const ScopedHashLock& rhs) = delete;
  ScopedHashLock& operator =(const ScopedHashLock& rhs) = delete;

  /**
   * Gets the index of the bucket.
   * @return The index of the bucket which the data should belong to.  The return value is always
   * INT64MIN if all bucket is locked.  The return value is -1 if the constructor taking a bucket
   * index is called and it fails to lock the bucket because of rehashing.
   */
  int64_t GetBucketIndex() const;

 private:
  /** The slotted mutex. */
  HashMutex& mutex_;
  /** The index of the bucket or -1 for all buckets. */
  int64_t bucket_index_;
  /** Whether it is an exclusive lock. */
  bool writable_;
};

/**
 * Scoped lock with multiple mutexes for a hash table.
 */
class ScopedHashLockMulti final {
 public:
  /**
   * Constructor to lock multiple buckets.
   * @param mutex A hash mutex.
   * @param data_list The data list to be set in the hash table.
   * @param writable True for exclusive lock or false for shared lock.
   */
  ScopedHashLockMulti(HashMutex& mutex, std::vector<std::string_view> data_list, bool writable);

  /**
   * Destructor.
   */
  ~ScopedHashLockMulti();

  /**
   * Copy and assignment are disabled.
   */
  explicit ScopedHashLockMulti(const ScopedHashLockMulti& rhs) = delete;
  ScopedHashLockMulti& operator =(const ScopedHashLockMulti& rhs) = delete;

  /**
   * Gets the indices of the buckets.
   * @return The indices of the buckets which the data should belong to.
   */
  const std::vector<int64_t> GetBucketIndices() const;

 private:
  /** The slotted mutex. */
  HashMutex& mutex_;
  /** The indices of the buckets. */
  std::vector<int64_t> bucket_indices_;
  /** Whether it is an exclusive lock. */
  bool writable_;
};

}  // namespace tkrzw

#endif  // _TKRZW_THREAD_UTIL_H

// END OF FILE
