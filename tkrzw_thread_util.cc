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

#include "tkrzw_lib_common.h"
#include "tkrzw_sys_config.h"
#include "tkrzw_thread_util.h"

namespace tkrzw {

double GetWallTime() {
  const auto epoch = std::chrono::time_point<std::chrono::system_clock>();
  const auto current = std::chrono::system_clock::now();
  const auto elapsed = std::chrono::duration_cast<std::chrono::microseconds>(current - epoch);
  return elapsed.count() / 1000000.0;
}

void Sleep(double sec) {
  std::this_thread::sleep_for(std::chrono::microseconds(static_cast<int64_t>(sec * 1000000)));
}

SlottedMutex::SlottedMutex(int32_t num_slots) : num_slots_(num_slots) {
  assert(num_slots > 0);
  slots_ = new std::shared_timed_mutex[num_slots];
}

SlottedMutex::~SlottedMutex() {
  delete[] slots_;
}

int32_t SlottedMutex::GetNumSlots() const {
  return num_slots_;
}

void SlottedMutex::LockOne(int32_t index) {
  assert(index < num_slots_);
  slots_[index].lock();
}

void SlottedMutex::UnlockOne(int32_t index) {
  assert(index < num_slots_);
  slots_[index].unlock();
}

void SlottedMutex::LockAll() {
  for (int32_t i = 0; i < num_slots_; i++) {
    slots_[i].lock();
  }
}

void SlottedMutex::UnlockAll() {
  for (int32_t i = num_slots_ - 1; i >= 0; i--) {
    slots_[i].unlock();
  }
}

void SlottedMutex::LockOneShared(int32_t index) {
  assert(index < num_slots_);
  slots_[index].lock_shared();
}

void SlottedMutex::UnlockOneShared(int32_t index) {
  assert(index < num_slots_);
  slots_[index].unlock_shared();
}

void SlottedMutex::LockAllShared() {
  for (int32_t i = 0; i < num_slots_; i++) {
    slots_[i].lock_shared();
  }
}

void SlottedMutex::UnlockAllShared() {
  for (int32_t i = num_slots_ - 1; i >= 0; i--) {
    slots_[i].unlock_shared();
  }
}

ScopedSlottedLock::ScopedSlottedLock(
    SlottedMutex& mutex, int32_t index, bool writable)
    : mutex_(mutex), index_(index), writable_(writable) {
  if (index_ < 0) {
    if (writable_) {
      mutex_.LockAll();
    } else {
      mutex_.LockAllShared();
    }
  } else {
    if (writable_) {
      mutex_.LockOne(index_);
    } else {
      mutex_.LockOneShared(index_);
    }
  }
}

ScopedSlottedLock::~ScopedSlottedLock() {
  if (index_ < 0) {
    if (writable_) {
      mutex_.UnlockAll();
    } else {
      mutex_.UnlockAllShared();
    }
  } else {
    if (writable_) {
      mutex_.UnlockOne(index_);
    } else {
      mutex_.UnlockOneShared(index_);
    }
  }
}

HashMutex::HashMutex(int32_t num_slots, int64_t num_buckets,
                     uint64_t (*hash_func)(std::string_view, uint64_t))
    : num_slots_(num_slots), num_buckets_(num_buckets), hash_func_(hash_func) {
  assert(num_slots > 0 && num_buckets > 0);
  slots_ = new std::shared_timed_mutex[num_slots];
}

HashMutex::~HashMutex() {
  delete[] slots_;
}

int32_t HashMutex::GetNumSlots() const {
  return num_slots_;
}

int64_t HashMutex::GetNumBuckets() const {
  return num_buckets_.load();
}

void HashMutex::Rehash(int64_t num_buckets) {
  num_buckets_.store(num_buckets);
}

int64_t HashMutex::GetBucketIndex(std::string_view data) {
  return hash_func_(data, num_buckets_.load());
}

int64_t HashMutex::LockOne(std::string_view data) {
  while (true) {
    const int64_t old_num_buckets = num_buckets_.load();
    const uint64_t bucket_index = hash_func_(data, old_num_buckets);
    const int32_t slot_index = bucket_index % num_slots_;
    slots_[slot_index].lock();
    if (num_buckets_.load() == old_num_buckets) {
      return bucket_index;
    }
    slots_[slot_index].unlock();
  }
  return -1;
}

bool HashMutex::LockOne(int64_t bucket_index) {
  const int64_t old_num_buckets = num_buckets_.load();
  if (bucket_index >= old_num_buckets) {
    return false;
  }
  const int32_t slot_index = bucket_index % num_slots_;
  slots_[slot_index].lock();
  if (num_buckets_.load() == old_num_buckets) {
    return true;
  }
  slots_[slot_index].unlock();
  return false;
}

void HashMutex::UnlockOne(int64_t bucket_index) {
  const int32_t slot_index = bucket_index % num_slots_;
  slots_[slot_index].unlock();
}

int64_t HashMutex::LockOneShared(std::string_view data) {
  while (true) {
    const int64_t old_num_buckets = num_buckets_.load();
    const uint64_t bucket_index = hash_func_(data, old_num_buckets);
    const int32_t slot_index = bucket_index % num_slots_;
    slots_[slot_index].lock_shared();
    if (num_buckets_.load() == old_num_buckets) {
      return bucket_index;
    }
    slots_[slot_index].unlock_shared();
  }
  return -1;
}

bool HashMutex::LockOneShared(int64_t bucket_index) {
  const int64_t old_num_buckets = num_buckets_.load();
  if (bucket_index >= old_num_buckets) {
    return false;
  }
  const int32_t slot_index = bucket_index % num_slots_;
  slots_[slot_index].lock();
  if (num_buckets_.load() == old_num_buckets) {
    return true;
  }
  slots_[slot_index].unlock();
  return false;
}

void HashMutex::UnlockOneShared(int64_t bucket_index) {
  const int32_t slot_index = bucket_index % num_slots_;
  slots_[slot_index].unlock_shared();
}

void HashMutex::LockAll() {
  for (int32_t i = 0; i < num_slots_; i++) {
    slots_[i].lock();
  }
}

void HashMutex::UnlockAll() {
  for (int32_t i = num_slots_ - 1; i >= 0; i--) {
    slots_[i].unlock();
  }
}

void HashMutex::LockAllShared() {
  for (int32_t i = 0; i < num_slots_; i++) {
    slots_[i].lock_shared();
  }
}

void HashMutex::UnlockAllShared() {
  for (int32_t i = num_slots_ - 1; i >= 0; i--) {
    slots_[i].unlock_shared();
  }
}

ScopedHashLock::ScopedHashLock(HashMutex& mutex, std::string_view data, bool writable)
    : mutex_(mutex), bucket_index_(0), writable_(writable) {
  if (writable_) {
    bucket_index_ = mutex_.LockOne(data);
  } else {
    bucket_index_ = mutex_.LockOneShared(data);
  }
}

ScopedHashLock::ScopedHashLock(HashMutex& mutex, bool writable)
    : mutex_(mutex), bucket_index_(INT64MIN), writable_(writable) {
  if (writable_) {
    mutex_.LockAll();
  } else {
    mutex_.LockAllShared();
  }
}

ScopedHashLock::ScopedHashLock(HashMutex& mutex, int64_t bucket_index, bool writable)
    : mutex_(mutex), bucket_index_(0), writable_(writable) {
  if (writable_) {
    bucket_index_ = mutex_.LockOne(bucket_index) ? bucket_index : -1;
  } else {
    bucket_index_ = mutex_.LockOneShared(bucket_index) ? bucket_index : -1;
  }
}

ScopedHashLock::~ScopedHashLock() {
  if (bucket_index_ == INT64MIN) {
    if (writable_) {
      mutex_.UnlockAll();
    } else {
      mutex_.UnlockAllShared();
    }
  } else if (bucket_index_ != -1) {
    if (writable_) {
      mutex_.UnlockOne(bucket_index_);
    } else {
      mutex_.UnlockOneShared(bucket_index_);
    }
  }
}

int64_t ScopedHashLock::GetBucketIndex() const {
  return bucket_index_;
}

}  // namespace tkrzw

// END OF FILE
