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
#include <chrono>
#include <condition_variable>
#include <functional>
#include <queue>
#include <set>
#include <shared_mutex>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include <cinttypes>

#include "tkrzw_lib_common.h"
#include "tkrzw_time_util.h"

namespace tkrzw {

/**
 * Sleeps the current thread.
 * @param sec The duration in seconds to sleep for.
 */
void SleepThread(double sec);

/**
 * Spin lock mutex.
 */
class SpinMutex final {
 public:
  /**
   * Constructor.
   */
  SpinMutex() {}

  /**
   * Copy and assignment are disabled.
   */
  explicit SpinMutex(const SpinMutex& rhs) = delete;
  SpinMutex& operator =(const SpinMutex& rhs) = delete;

  /**
   * Gets exclusive ownership of the lock.
   * @details Precondition: The thread doesn't have the exclusive ownership.
   */
  void lock() {
    while (lock_.test_and_set(std::memory_order_acquire)) {
      std::this_thread::yield();
    }
  }

  /**
   * Tries to get exclusive ownership of the lock.
   * @return True if successful or false on failure.
   * @details Precondition: The thread doesn't have the exclusive ownership.
   */
  bool try_lock() {
    return !lock_.test_and_set(std::memory_order_acquire);
  }

  /**
   * Releases exclusive ownership of the lock.
   * @details Precondition: The thread has the exclusive ownership.
   */
  void unlock() {
    lock_.clear(std::memory_order_release);
  }

 private:
  /** Atomic flat of locked state. */
  std::atomic_flag lock_ = ATOMIC_FLAG_INIT;
};

/**
 * Spin lock shared mutex.
 */
class SpinSharedMutex final {
 public:
  /**
   * Constructor.
   */
  SpinSharedMutex() : count_(0) {}

  /**
   * Copy and assignment are disabled.
   */
  explicit SpinSharedMutex(const SpinSharedMutex& rhs) = delete;
  SpinSharedMutex& operator =(const SpinSharedMutex& rhs) = delete;

  /**
   * Gets exclusive ownership of the lock.
   * @details Precondition: The thread doesn't have any ownership.
   */
  void lock() {
    uint32_t old_value = 0;
    while (!count_.compare_exchange_weak(old_value, INT32MAX)) {
      old_value = 0;
      if (count_.compare_exchange_strong(old_value, INT32MAX)) {
        break;
      }
      std::this_thread::yield();
      old_value = 0;
    }
  }

  /**
   * Tries to get exclusive ownership of the lock.
   * @return True if successful or false on failure.
   * @details Precondition: The thread doesn't have any ownership.
   */
  bool try_lock() {
    uint32_t old_value = 0;
    return count_.compare_exchange_strong(old_value, INT32MAX);
  }

  /**
   * Releases exclusive ownership of the lock.
   * @details Precondition: The thread has the exclusive ownership.
   */
  void unlock() {
    count_.store(0, std::memory_order_release);
  }

  /**
   * Gets shared ownership of the lock.
   * @details Precondition: The thread doesn't have any ownership.
   */
  void lock_shared() {
    while (count_.fetch_add(1) >= INT32MAX) {
      uint32_t old_value = count_.load();
      if (old_value > INT32MAX) {
        count_.compare_exchange_weak(old_value, INT32MAX);
      }
      std::this_thread::yield();
    }
  }

  /**
   * Tries to get shared ownership of the lock.
   * @return True if successful or false on failure.
   * @details Precondition: The thread doesn't have any ownership.
   */
  bool try_lock_shared() {
    if (count_.fetch_add(1) < INT32MAX) {
      return true;
    }
    uint32_t old_value = count_.load();
    if (old_value > INT32MAX) {
      count_.compare_exchange_weak(old_value, INT32MAX);
    }
    return false;
  }

  /**
   * Releases shared ownership of the lock.
   * @details Precondition: The thread has the shared ownership.
   */
  void unlock_shared() {
    count_.fetch_sub(1, std::memory_order_release);
  }

  /**
   * Tries to upgrade shared ownership to exclusive ownership.
   * @param wait If true, waits while there's possibility to get exclusive ownership.
   * @return True if successful or false on failure.
   * @details Precondition: The thread has the shared ownership.
   */
  bool try_upgrade(bool wait = false) {
    while (true) {
      uint32_t old_value = 1;
      if (count_.compare_exchange_strong(old_value, INT32MAX)) {
        return true;
      }
      if (old_value < INT32MAX) {
        return false;
      }
      std::this_thread::yield();
    }
  }

  /**
   * Downgrades exclusive ownership.to shared ownership.
   * @details Precondition: The thread has the exclusive ownership.
   */
  void downgrade() {
    count_.store(1, std::memory_order_release);
  }

 private:
  /** The count of threads sharing the lock. */
  std::atomic_uint32_t count_;
};

/**
 * Spin lock shared mutex, with write-preferring policy.
 */
class SpinWPSharedMutex final {
 public:
  /**
   * Constructor.
   */
  SpinWPSharedMutex() : count_(0), wannabe_count_(0) {}

  /**
   * Copy and assignment are disabled.
   */
  explicit SpinWPSharedMutex(const SpinWPSharedMutex& rhs) = delete;
  SpinWPSharedMutex& operator =(const SpinWPSharedMutex& rhs) = delete;

  /**
   * Gets exclusive ownership of the lock.
   * @details Precondition: The thread doesn't have any ownership.
   */
  void lock() {
    wannabe_count_.fetch_add(1);
    uint32_t old_value = 0;
    while (!count_.compare_exchange_weak(old_value, INT32MAX)) {
      old_value = 0;
      if (count_.compare_exchange_strong(old_value, INT32MAX)) {
        break;
      }
      std::this_thread::yield();
      old_value = 0;
    }
    wannabe_count_.fetch_sub(1);
  }

  /**
   * Tries to get exclusive ownership of the lock.
   * @return True if successful or false on failure.
   * @details Precondition: The thread doesn't have any ownership.
   */
  bool try_lock() {
    uint32_t old_value = 0;
    return count_.compare_exchange_strong(old_value, INT32MAX);
  }

  /**
   * Releases exclusive ownership of the lock.
   * @details Precondition: The thread has the exclusive ownership.
   */
  void unlock() {
    count_.store(0, std::memory_order_release);
  }

  /**
   * Gets shared ownership of the lock.
   * @details Precondition: The thread doesn't have any ownership.
   */
  void lock_shared() {
    while (wannabe_count_.load() != 0 || count_.fetch_add(1) >= INT32MAX) {
      uint32_t old_value = count_.load();
      if (old_value > INT32MAX) {
        count_.compare_exchange_weak(old_value, INT32MAX);
      }
      std::this_thread::yield();
    }
  }

  /**
   * Tries to get shared ownership of the lock.
   * @return True if successful or false on failure.
   * @details Precondition: The thread doesn't have any ownership.
   */
  bool try_lock_shared() {
    if (wannabe_count_.load() != 0) {
      return false;
    }
    if (count_.fetch_add(1) < INT32MAX) {
      return true;
    }
    uint32_t old_value = count_.load();
    if (old_value > INT32MAX) {
      count_.compare_exchange_weak(old_value, INT32MAX);
    }
    return false;
  }

  /**
   * Releases shared ownership of the lock.
   * @details Precondition: The thread has the shared ownership.
   */
  void unlock_shared() {
    count_.fetch_sub(1, std::memory_order_release);
  }

  /**
   * Tries to upgrade shared ownership to exclusive ownership.
   * @param wait If true, waits while there's possibility to get exclusive ownership.
   * @return True if successful or false on failure.
   * @details Precondition: The thread has the shared ownership.
   */
  bool try_upgrade(bool wait = false) {
    wannabe_count_.fetch_add(1);
    while (true) {
      uint32_t old_value = 1;
      if (count_.compare_exchange_strong(old_value, INT32MAX)) {
        wannabe_count_.fetch_sub(1);
        return true;
      }
      if (!wait || (old_value < INT32MAX && wannabe_count_.load() > 1)) {
        wannabe_count_.fetch_sub(1);
        return false;
      }
      std::this_thread::yield();
    }
  }

  /**
   * Downgrades exclusive ownership.to shared ownership.
   * @details Precondition: The thread has the exclusive ownership.
   */
  void downgrade() {
    count_.store(1, std::memory_order_release);
  }

 private:
  /** The count of threads sharing the lock. */
  std::atomic_uint32_t count_;
  /** The count of threads trying to get exclusive ownership. */
  std::atomic_uint32_t wannabe_count_;
};

/**
 * Slotted shared mutex.
 */
template<typename SHAREDMUTEX = std::shared_mutex>
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
  SHAREDMUTEX* slots_;
};

/**
 * Scoped lock with a slotted shared mutex.
 */
template<typename SHAREDMUTEX = std::shared_mutex>
class ScopedSlottedLock final {
 public:
  /**
   * Constructor.
   * @param mutex A slotted shared mutex.
   * @param index The index of a slot.  Negative means all slots.
   * @param writable True for exclusive lock.  False for shared lock.
   */
  ScopedSlottedLock(SlottedMutex<SHAREDMUTEX>& mutex, int32_t index, bool writable);

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
  SlottedMutex<SHAREDMUTEX>& mutex_;
  /** The index of the locked slot. */
  int32_t index_;
  /** Whether it is an exclusive lock. */
  bool writable_;
};

/**
 * Mutex for a hash table.
 */
template<typename SHAREDMUTEX = std::shared_mutex>
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
  SHAREDMUTEX* slots_;
};

/**
 * Scoped lock with a mutex for a hash table.
 */
template<typename SHAREDMUTEX = std::shared_mutex>
class ScopedHashLock final {
 public:
  /**
   * Constructor to lock one bucket.
   * @param mutex A hash mutex.
   * @param data The data to be set in the hash table.
   * @param writable True for exclusive lock or false for shared lock.
   */
  ScopedHashLock(HashMutex<SHAREDMUTEX>& mutex, std::string_view data, bool writable);

  /**
   * Constructro to lock all buckets.
   * @param mutex A hash mutex.
   * @param writable True for exclusive lock or false for shared lock.
   */
  ScopedHashLock(HashMutex<SHAREDMUTEX>& mutex, bool writable);

  /**
   * Constructor to lock the bucket specific to an index.
   * @param mutex A hash mutex.
   * @param bucket_index The index of the bucket to lock.
   * @param writable True for exclusive lock or false for shared lock.
   * @details Only this constructor can fail.  It is because of rehashing.
   */
  ScopedHashLock(HashMutex<SHAREDMUTEX>& mutex, int64_t bucket_index, bool writable);

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
  HashMutex<SHAREDMUTEX>& mutex_;
  /** The index of the bucket or -1 for all buckets. */
  int64_t bucket_index_;
  /** Whether it is an exclusive lock. */
  bool writable_;
};

/**
 * Scoped lock with multiple mutexes for a hash table.
 */
template<typename SHAREDMUTEX = std::shared_mutex>
class ScopedHashLockMulti final {
 public:
  /**
   * Constructor to lock multiple buckets.
   * @param mutex A hash mutex.
   * @param data_list The data list to be set in the hash table.
   * @param writable True for exclusive lock or false for shared lock.
   */
  ScopedHashLockMulti(
      HashMutex<SHAREDMUTEX>& mutex, std::vector<std::string_view> data_list, bool writable);

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
  HashMutex<SHAREDMUTEX>& mutex_;
  /** The indices of the buckets. */
  std::vector<int64_t> bucket_indices_;
  /** Whether it is an exclusive lock. */
  bool writable_;
};

/**
 * Task queue with a thread pool
 */
class TaskQueue final {
 public:
  /**
   * Interface of a task.
   */
  class Task {
   public:
    /**
     * Destructor.
     */
    virtual ~Task() = default;

    /**
     * Do the task.
     */
    virtual void Do() = 0;
  };

  /**
   * Lambda function type to do a task.
   */
  typedef std::function<void()> TaskLambdaType;

  /**
   * Task implementation with a lambda function.
   */
  class TaskWithLambda final : public Task {
   public:
    /**
     * Constructor.
     * @param lambda A lambda function to process a task.
     */
    explicit TaskWithLambda(TaskLambdaType lambda) : lambda_(lambda) {}

    /**
     * Do the task.
     */
    void Do() override {
      return lambda_();
    }

   private:
    // Lambda function to process a task.
    TaskLambdaType lambda_;
  };

  /**
   * Default constructor.
   */
  TaskQueue();

  /**
   * Destructor.
   */
  ~TaskQueue();

  /**
   * Starts worker threads.
   * @param num_worker_threads The number of worker threads.
   */
  void Start(int32_t num_worker_threads);

  /**
   * Stops worker threads.
   * @param timeout The timeout in seconds to wait for all tasks in the queue to be done.
   */
  void Stop(double timeout);

  /**
   * Adds a task to the queue.
   * @param task The task object.
   */
  void Add(std::unique_ptr<Task> task);

  /**
   * Adds a task to the queue.
   * @param task The lambda function to do the task.
   */
  void Add(TaskLambdaType task);

  /**
   * Get the number of tasks in the queue.
   * @return The number of tasks in the queue.
   */
  int32_t GetSize();

 private:
  /** The task queue. */
  std::queue<std::shared_ptr<Task>> queue_;
  /** The number of tasks. */
  std::atomic_int32_t num_tasks_;
  /** The worker threads. */
  std::vector<std::thread> threads_;
  /** Whether the worker is running or not. */
  std::atomic_bool running_;
  /** The mutex to guard the task queue. */
  std::mutex mutex_;
  /** The conditional variable to notify the change. */
  std::condition_variable cond_;
};

/**
 * Wait counter for monitoring other threads.
 */
class WaitCounter final {
 public:
  /**
   * Constructor.
   * @param initial The initial value of the counter.
   */
  explicit WaitCounter(int32_t initial = 0);

  /**
   * Adds a value to the counter.
   * @param increment The incremental value.  Use a nagative for decrement.
   */
  void Add(int32_t increment = 1);

  /**
   * Gets the current value of the counter.
   * @return The current value of the counter.
   */
  int32_t Get() const;

  /**
   * Decrements a value from the counter and notify if the result value is zero or less.
   * @param decrement The decremental value.
   */
  void Done(int32_t decrement = 1);

  /**
   * Waits for the counter value to be zero or less.
   * @param timeout The timeout in seconds to wait.   Zero means no wait.  Negative means
   * unlimited.
   * @return True if successful or false on timeout.
   */
  bool Wait(double timeout = -1);

 private:
  /** The counter of events. */
  std::atomic_int32_t count_;
  /** The mutex to guard the counter. */
  std::mutex mutex_;
  /** The conditional variable to notify the change. */
  std::condition_variable cond_;
};

/**
 * Broker to send a signal to another thread.
 */
class SignalBroker final {
  friend class Waiter;
 public:
  /**
   * Handler to wait for the signal.
   * @details The constructor should be called before checking the resource.  Then, the Wait
   * method should be called if the resource state doesn't satisfy the required condition.
   */
  class Waiter {
   public:
    /**
     * Constructor.
     * @param broker The broker object.
     */
    explicit Waiter(SignalBroker* broker);

    /**
     * Destructor.
     */
    ~Waiter();

    /**
     * Waits for a signal to happen.
     * @param timeout The timeout in seconds to wait.   Zero means no wait.  Negative means
     * unlimited.
     * @return True if successful or false on timeout.
     * @details This detects only signals which happen during this method is running.  In other
     * words, signals which were sent before the call are not ignored.
     **/
    bool Wait(double timeout = -1);

   private:
    /** The broker object. */
    SignalBroker* broker_;
  };

  /**
   * Default constructor.
   */
  SignalBroker();

  /**
   * Sends a signal identified by a key.
   * @param all If true, notification is sent to all waiting threads.  If false, it is sent to
   * only one waiting thread.
   * @return True if the signal is received by an waiting thread, or false if not.
   */
  bool Send(bool all = true);

 private:
  /** The number of waiting threads. */
  int32_t wait_count_;
  /** The mutex to guard the counter. */
  std::mutex mutex_;
  /** The conditional variable to notify the change. */
  std::condition_variable cond_;
  /** The mutex to guard notification. */
  SpinSharedMutex notify_mutex_;
};

/**
 * Broker to send a signal associated with a key to another thread.
 */
template<typename KEYTYPE>
class KeySignalBroker final {
  friend class Waiter;
 public:
  /**
   * Handler to wait for the signal.
   * @details The constructor should be called before checking the resource.  Then, the Wait
   * method should be called if the resource state doesn't satisfy the required condition.
   */
  class Waiter {
   public:
    /**
     * Constructor.
     * @param broker The broker object.
     * @param key The key of the signal.
     */
    Waiter(KeySignalBroker* broker, const KEYTYPE& key);

    /**
     * Destructor.
     */
    ~Waiter();

    /**
     * Waits for a signal to happen.
     * @param timeout The timeout in seconds to wait.   Zero means no wait.  Negative means
     * unlimited.
     * @return True if successful or false on timeout.
     * @details This detects only signals which happen during this method is running.  In other
     * words, signals which were sent before the call are not ignored.
     **/
    bool Wait(double timeout = -1);

   private:
    /** The broker object. */
    KeySignalBroker* broker_;
    /** The key of the signal. */
    KEYTYPE key_;
  };

  /**
   * Default constructor.
   */
  KeySignalBroker();

  /**
   * Sends a signal identified by a key.
   * @param key The key of the signal.
   * @param all If true, notification is sent to all waiting threads.  If false, it is sent to
   * only one waiting thread.
   * @return True if the signal is received by an waiting thread, or false if not.
   */
  bool Send(const KEYTYPE& key, bool all = true);

 private:
  /** The set of event keys and the wait count. */
  std::map<KEYTYPE, int32_t> counts_;
  /** The mutex to guard the counter. */
  std::mutex mutex_;
  /** The conditional variable to notify the change. */
  std::condition_variable cond_;
  /** The mutex to guard notification. */
  SpinSharedMutex notify_mutex_;
};

/**
 * Slotted broker to send a signal associated with a key to another thread.
 */
template<typename KEYTYPE>
class SlottedKeySignalBroker final {
  friend class Waiter;
 public:
  /**
   * Handler to wait for the signal.
   * @details The constructor should be called before checking the resource.  Then, the Wait
   * method should be called if the resource state doesn't satisfy the required condition.
   */
  class Waiter {
   public:
    /**
     * Constructor.
     * @param broker The broker object.
     * @param key The key of the signal.
     */
    Waiter(SlottedKeySignalBroker<KEYTYPE>* broker, const KEYTYPE& key);

    /**
     * Waits for a signal to happen.
     * @param timeout The timeout in seconds to wait.   Zero means no wait.  Negative means
     * unlimited.
     * @return True if successful or false on timeout.
     * @details This detects only signals which happen during this method is running.  In other
     * words, signals which were sent before the call are not ignored.
     **/
    bool Wait(double timeout = -1);

   private:
    /** The waiter of the key slot. */
    typename KeySignalBroker<KEYTYPE>::Waiter slot_waiter_;
  };

  /**
   * Default constructor.
   * @param num_slots The number of slots.
   */
  explicit SlottedKeySignalBroker(int32_t num_slots);

  /**
   * Default constructor.
   * @param num_slots The number of slots.
   */
  ~SlottedKeySignalBroker();

  /**
   * Sends a signal identified by a key.
   * @param key The key of the signal.
   * @param all If true, notification is sent to all waiting threads.  If false, it is sent to
   * only one waiting thread.
   * @return True if the signal is received by an waiting thread, or false if not.
   */
  bool Send(const KEYTYPE& key, bool all = true);

 private:
  /** The number of the slots. */
  int32_t num_slots_;
  /** The array of the slots. */
  KeySignalBroker<KEYTYPE>* slots_;
};

/**
 * Scoped counter for auto increment and decrement.
 */
template<typename T>
class ScopedCounter final {
 public:
  /**
   * Constructor.
   * @param count The pointer to the counter object.
   * @param increment The value to add in the constructor and subtract in the destructor.
   */
  explicit ScopedCounter(T* count, int32_t increment = 1);

  /**
   * Destructor.
   */
  ~ScopedCounter();

  /**
   * Copy and assignment are disabled.
   */
  explicit ScopedCounter(const ScopedCounter& rhs) = delete;
  ScopedCounter& operator =(const ScopedCounter& rhs) = delete;

 private:
  /** The pointer to the counter object. */
  T* count_;
  /** The value to add and subtract. */
  T increment_;
};

inline void SleepThread(double sec) {
  std::this_thread::sleep_for(std::chrono::microseconds(static_cast<int64_t>(sec * 1000000)));
}

template<typename SHAREDMUTEX>
inline SlottedMutex<SHAREDMUTEX>::SlottedMutex(int32_t num_slots) : num_slots_(num_slots) {
  slots_ = new SHAREDMUTEX[num_slots];
}

template<typename SHAREDMUTEX>
inline SlottedMutex<SHAREDMUTEX>::~SlottedMutex() {
  delete[] slots_;
}

template<typename SHAREDMUTEX>
inline int32_t SlottedMutex<SHAREDMUTEX>::GetNumSlots() const {
  return num_slots_;
}

template<typename SHAREDMUTEX>
inline void SlottedMutex<SHAREDMUTEX>::LockOne(int32_t index) {
  slots_[index].lock();
}

template<typename SHAREDMUTEX>
inline void SlottedMutex<SHAREDMUTEX>::UnlockOne(int32_t index) {
  slots_[index].unlock();
}

template<typename SHAREDMUTEX>
inline void SlottedMutex<SHAREDMUTEX>::LockAll() {
  for (int32_t i = 0; i < num_slots_; i++) {
    slots_[i].lock();
  }
}

template<typename SHAREDMUTEX>
inline void SlottedMutex<SHAREDMUTEX>::UnlockAll() {
  for (int32_t i = num_slots_ - 1; i >= 0; i--) {
    slots_[i].unlock();
  }
}

template<typename SHAREDMUTEX>
inline void SlottedMutex<SHAREDMUTEX>::LockOneShared(int32_t index) {
  slots_[index].lock_shared();
}

template<typename SHAREDMUTEX>
inline void SlottedMutex<SHAREDMUTEX>::UnlockOneShared(int32_t index) {
  slots_[index].unlock_shared();
}

template<typename SHAREDMUTEX>
inline void SlottedMutex<SHAREDMUTEX>::LockAllShared() {
  for (int32_t i = 0; i < num_slots_; i++) {
    slots_[i].lock_shared();
  }
}

template<typename SHAREDMUTEX>
inline void SlottedMutex<SHAREDMUTEX>::UnlockAllShared() {
  for (int32_t i = num_slots_ - 1; i >= 0; i--) {
    slots_[i].unlock_shared();
  }
}

template<typename SHAREDMUTEX>
inline ScopedSlottedLock<SHAREDMUTEX>::ScopedSlottedLock(
    SlottedMutex<SHAREDMUTEX>& mutex, int32_t index, bool writable)
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

template<typename SHAREDMUTEX>
inline ScopedSlottedLock<SHAREDMUTEX>::~ScopedSlottedLock() {
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

template<typename SHAREDMUTEX>
inline HashMutex<SHAREDMUTEX>::HashMutex(int32_t num_slots, int64_t num_buckets,
                            uint64_t (*hash_func)(std::string_view, uint64_t))
    : num_slots_(num_slots), num_buckets_(num_buckets), hash_func_(hash_func) {
  slots_ = new SHAREDMUTEX[num_slots];
}

template<typename SHAREDMUTEX>
inline HashMutex<SHAREDMUTEX>::~HashMutex() {
  delete[] slots_;
}

template<typename SHAREDMUTEX>
inline int32_t HashMutex<SHAREDMUTEX>::GetNumSlots() const {
  return num_slots_;
}

template<typename SHAREDMUTEX>
inline int64_t HashMutex<SHAREDMUTEX>::GetNumBuckets() const {
  return num_buckets_.load();
}

template<typename SHAREDMUTEX>
inline void HashMutex<SHAREDMUTEX>::Rehash(int64_t num_buckets) {
  num_buckets_.store(num_buckets);
}

template<typename SHAREDMUTEX>
inline int64_t HashMutex<SHAREDMUTEX>::GetBucketIndex(std::string_view data) {
  return hash_func_(data, num_buckets_.load());
}

template<typename SHAREDMUTEX>
inline int64_t HashMutex<SHAREDMUTEX>::LockOne(std::string_view data) {
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

template<typename SHAREDMUTEX>
inline bool HashMutex<SHAREDMUTEX>::LockOne(int64_t bucket_index) {
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

template<typename SHAREDMUTEX>
inline void HashMutex<SHAREDMUTEX>::UnlockOne(int64_t bucket_index) {
  const int32_t slot_index = bucket_index % num_slots_;
  slots_[slot_index].unlock();
}

template<typename SHAREDMUTEX>
inline int64_t HashMutex<SHAREDMUTEX>::LockOneShared(std::string_view data) {
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

template<typename SHAREDMUTEX>
inline bool HashMutex<SHAREDMUTEX>::LockOneShared(int64_t bucket_index) {
  const int64_t old_num_buckets = num_buckets_.load();
  if (bucket_index >= old_num_buckets) {
    return false;
  }
  const int32_t slot_index = bucket_index % num_slots_;
  slots_[slot_index].lock_shared();
  if (num_buckets_.load() == old_num_buckets) {
    return true;
  }
  slots_[slot_index].unlock_shared();
  return false;
}

template<typename SHAREDMUTEX>
inline void HashMutex<SHAREDMUTEX>::UnlockOneShared(int64_t bucket_index) {
  const int32_t slot_index = bucket_index % num_slots_;
  slots_[slot_index].unlock_shared();
}

template<typename SHAREDMUTEX>
inline void HashMutex<SHAREDMUTEX>::LockAll() {
  for (int32_t i = 0; i < num_slots_; i++) {
    slots_[i].lock();
  }
}

template<typename SHAREDMUTEX>
inline void HashMutex<SHAREDMUTEX>::UnlockAll() {
  for (int32_t i = num_slots_ - 1; i >= 0; i--) {
    slots_[i].unlock();
  }
}

template<typename SHAREDMUTEX>
inline void HashMutex<SHAREDMUTEX>::LockAllShared() {
  for (int32_t i = 0; i < num_slots_; i++) {
    slots_[i].lock_shared();
  }
}

template<typename SHAREDMUTEX>
inline void HashMutex<SHAREDMUTEX>::UnlockAllShared() {
  for (int32_t i = num_slots_ - 1; i >= 0; i--) {
    slots_[i].unlock_shared();
  }
}

template<typename SHAREDMUTEX>
inline std::vector<int64_t> HashMutex<SHAREDMUTEX>::LockMulti(
    const std::vector<std::string_view>& data_list) {
  slots_[0].lock();
  std::vector<int64_t> bucket_indices;
  bucket_indices.reserve(data_list.size());
  const int64_t num_buckets = num_buckets_.load();
  std::set<int32_t> slot_indices;
  for (const auto& data : data_list) {
    const uint64_t bucket_index = hash_func_(data, num_buckets);
    bucket_indices.emplace_back(bucket_index);
    const int32_t slot_index = bucket_index % num_slots_;
    slot_indices.emplace(slot_index);
  }
  bool has_zero = false;
  for (int64_t slot_index : slot_indices) {
    if (slot_index == 0) {
      has_zero = true;
    } else {
      slots_[slot_index].lock();
    }
  }
  if (!has_zero) {
    slots_[0].unlock();
  }
  return bucket_indices;
}

template<typename SHAREDMUTEX>
inline void HashMutex<SHAREDMUTEX>::UnlockMulti(const std::vector<int64_t>& bucket_indices) {
  std::set<int32_t> slot_indices;
  for (int64_t bucket_index : bucket_indices) {
    const int32_t slot_index = bucket_index % num_slots_;
    slot_indices.emplace(slot_index);
  }
  for (auto it = slot_indices.rbegin(); it != slot_indices.rend(); it++) {
    slots_[*it].unlock();
  }
}

template<typename SHAREDMUTEX>
inline std::vector<int64_t> HashMutex<SHAREDMUTEX>::LockMultiShared(
    const std::vector<std::string_view>& data_list) {
  slots_[0].lock_shared();
  std::vector<int64_t> bucket_indices;
  bucket_indices.reserve(data_list.size());
  const int64_t num_buckets = num_buckets_.load();
  std::set<int32_t> slot_indices;
  for (const auto& data : data_list) {
    const uint64_t bucket_index = hash_func_(data, num_buckets);
    bucket_indices.emplace_back(bucket_index);
    const int32_t slot_index = bucket_index % num_slots_;
    slot_indices.emplace(slot_index);
  }
  bool has_zero = false;
  for (int64_t slot_index : slot_indices) {
    if (slot_index == 0) {
      has_zero = true;
    } else {
      slots_[slot_index].lock_shared();
    }
  }
  if (!has_zero) {
    slots_[0].unlock_shared();
  }
  return bucket_indices;
}

template<typename SHAREDMUTEX>
inline void HashMutex<SHAREDMUTEX>::UnlockMultiShared(
    const std::vector<int64_t>& bucket_indices) {
  std::set<int32_t> slot_indices;
  for (int64_t bucket_index : bucket_indices) {
    const int32_t slot_index = bucket_index % num_slots_;
    slot_indices.emplace(slot_index);
  }
  for (auto it = slot_indices.rbegin(); it != slot_indices.rend(); it++) {
    slots_[*it].unlock_shared();
  }
}

template<typename SHAREDMUTEX>
inline ScopedHashLock<SHAREDMUTEX>::ScopedHashLock(
    HashMutex<SHAREDMUTEX>& mutex, std::string_view data, bool writable)
    : mutex_(mutex), bucket_index_(0), writable_(writable) {
  if (writable_) {
    bucket_index_ = mutex_.LockOne(data);
  } else {
    bucket_index_ = mutex_.LockOneShared(data);
  }
}

template<typename SHAREDMUTEX>
inline ScopedHashLock<SHAREDMUTEX>::ScopedHashLock(HashMutex<SHAREDMUTEX>& mutex, bool writable)
    : mutex_(mutex), bucket_index_(INT64MIN), writable_(writable) {
  if (writable_) {
    mutex_.LockAll();
  } else {
    mutex_.LockAllShared();
  }
}

template<typename SHAREDMUTEX>
inline ScopedHashLock<SHAREDMUTEX>::ScopedHashLock(
    HashMutex<SHAREDMUTEX>& mutex, int64_t bucket_index, bool writable)
    : mutex_(mutex), bucket_index_(0), writable_(writable) {
  if (writable_) {
    bucket_index_ = mutex_.LockOne(bucket_index) ? bucket_index : -1;
  } else {
    bucket_index_ = mutex_.LockOneShared(bucket_index) ? bucket_index : -1;
  }
}

template<typename SHAREDMUTEX>
inline ScopedHashLock<SHAREDMUTEX>::~ScopedHashLock() {
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

template<typename SHAREDMUTEX>
inline int64_t ScopedHashLock<SHAREDMUTEX>::GetBucketIndex() const {
  return bucket_index_;
}

template<typename SHAREDMUTEX>
inline ScopedHashLockMulti<SHAREDMUTEX>::ScopedHashLockMulti(
    HashMutex<SHAREDMUTEX>& mutex, std::vector<std::string_view> data_list, bool writable)
    : mutex_(mutex), bucket_indices_(), writable_(writable) {
  if (writable_) {
    bucket_indices_ = mutex_.LockMulti(data_list);
  } else {
    bucket_indices_ = mutex_.LockMultiShared(data_list);
  }
}

template<typename SHAREDMUTEX>
inline ScopedHashLockMulti<SHAREDMUTEX>::~ScopedHashLockMulti() {
  if (writable_) {
    mutex_.UnlockMulti(bucket_indices_);
  } else {
    mutex_.UnlockMultiShared(bucket_indices_);
  }
}

template<typename SHAREDMUTEX>
inline const std::vector<int64_t> ScopedHashLockMulti<SHAREDMUTEX>::GetBucketIndices() const {
  return bucket_indices_;
}

inline TaskQueue::TaskQueue() :
    queue_(), num_tasks_(0), threads_(), running_(false), mutex_(), cond_() {}

inline TaskQueue::~TaskQueue() {
  if (running_.load()) {
    Stop(0.1);
  }
}

inline void TaskQueue::Start(int32_t num_worker_threads) {
  threads_.reserve(num_worker_threads);
  running_ = true;
  for (int32_t i = 0; i < num_worker_threads; i++) {
    auto worker =
        [&]() {
          while (running_.load()) {
            std::shared_ptr<Task> task(nullptr);
            {
              std::unique_lock<std::mutex> lock(mutex_);
              if (queue_.empty()) {
                cond_.wait(lock, [&]() { return !running_.load() || !queue_.empty(); });
              } else {
                task = queue_.front();
                queue_.pop();
                num_tasks_.store(queue_.size());
              }
            }
            if (task != nullptr) {
              task->Do();
            }
          }
        };
    threads_.emplace_back(std::thread(worker));
  }
}

inline void TaskQueue::Stop(double timeout) {
  const double end_time = GetWallTime() + timeout;
  while (GetWallTime() < end_time) {
    {
      std::lock_guard<std::mutex> lock(mutex_);
      if (queue_.empty()) {
        break;
      }
    }
    cond_.notify_all();
    SleepThread(0.01);
  }
  running_.store(false);
  cond_.notify_all();
  for (auto& thread : threads_) {
    thread.join();
  }
  threads_.clear();
  num_tasks_.store(0);
}

inline void TaskQueue::Add(std::unique_ptr<Task> task) {
  {
    std::lock_guard<std::mutex> lock(mutex_);
    queue_.push(std::move(task));
    num_tasks_.store(queue_.size());
  }
  cond_.notify_one();
}

inline void TaskQueue::Add(TaskLambdaType task) {
  Add(std::make_unique<TaskWithLambda>(task));
}

inline int32_t TaskQueue::GetSize() {
  return num_tasks_.load();
}

inline WaitCounter::WaitCounter(int32_t initial) : count_(initial) {}

inline void WaitCounter::Add(int32_t increment) {
  count_.fetch_add(increment);
}

inline int32_t WaitCounter::Get() const {
  return count_;
}

inline void WaitCounter::Done(int32_t decrement) {
  if (count_.fetch_sub(decrement) <= decrement) {
    cond_.notify_all();
  }
}

inline bool WaitCounter::Wait(double timeout) {
  if (timeout == 0) {
    return count_.load() <= 0;
  }
  if (timeout < 0) {
    timeout = UINT32MAX;
  }
  const auto deadline = std::chrono::steady_clock::now() +
      std::chrono::microseconds(static_cast<int64_t>(timeout * 1000000));
  std::unique_lock<std::mutex> lock(mutex_);
  while (count_.load() > 0) {
    if (cond_.wait_until(lock, deadline) == std::cv_status::timeout) {
      return count_.load() <= 0;
    }
  }
  return true;
}

inline SignalBroker::SignalBroker() : wait_count_(false), mutex_(), cond_(), notify_mutex_() {}

inline bool SignalBroker::Send(bool all) {
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (wait_count_ == 0) {
      return false;
    }
    wait_count_ = 0;
  }
  std::lock_guard lock(notify_mutex_);
  if (all) {
    cond_.notify_all();
  } else {
    cond_.notify_one();
  }
  return true;
}

inline SignalBroker::Waiter::Waiter(SignalBroker* broker) : broker_(broker) {
  broker_->notify_mutex_.lock_shared();
}

inline SignalBroker::Waiter::~Waiter() {
  broker_->notify_mutex_.unlock_shared();
}

inline bool SignalBroker::Waiter::Wait(double timeout) {
  if (timeout < 0) {
    timeout = UINT32MAX;
  }
  const auto deadline = std::chrono::steady_clock::now() +
      std::chrono::microseconds(static_cast<int64_t>(timeout * 1000000));
  std::unique_lock<std::mutex> lock(broker_->mutex_);
  broker_->wait_count_++;
  while (broker_->wait_count_ != 0) {
    broker_->notify_mutex_.unlock_shared();
    auto wait_rv = broker_->cond_.wait_until(lock, deadline);
    broker_->notify_mutex_.lock_shared();
    if (wait_rv == std::cv_status::timeout) {
      if (broker_->wait_count_ == 0) {
        return true;
      }
      broker_->wait_count_--;
      return false;
    }
  }
  return true;
}

template<typename KEYTYPE>
inline KeySignalBroker<KEYTYPE>::KeySignalBroker()
    : counts_(), mutex_(), cond_(), notify_mutex_() {}

template<typename KEYTYPE>
inline bool KeySignalBroker<KEYTYPE>::Send(const KEYTYPE& key, bool all) {
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (counts_.erase(key) == 0) {
      return false;
    }
  }
  std::lock_guard lock(notify_mutex_);
  if (all) {
    cond_.notify_all();
  } else {
    cond_.notify_one();
  }
  return true;
}

template<typename KEYTYPE>
inline KeySignalBroker<KEYTYPE>::Waiter::Waiter(KeySignalBroker* broker, const KEYTYPE& key)
    : broker_(broker), key_(key) {
  broker_->notify_mutex_.lock_shared();
}

template<typename KEYTYPE>
inline KeySignalBroker<KEYTYPE>::Waiter::~Waiter() {
  broker_->notify_mutex_.unlock_shared();
}

template<typename KEYTYPE>
inline bool KeySignalBroker<KEYTYPE>::Waiter::Wait(double timeout) {
  if (timeout < 0) {
    timeout = UINT32MAX;
  }
  const auto deadline = std::chrono::steady_clock::now() +
      std::chrono::microseconds(static_cast<int64_t>(timeout * 1000000));
  std::unique_lock<std::mutex> lock(broker_->mutex_);
  broker_->counts_[key_]++;
  while (broker_->counts_.find(key_) != broker_->counts_.end()) {
    broker_->notify_mutex_.unlock_shared();
    auto wait_rv = broker_->cond_.wait_until(lock, deadline);
    broker_->notify_mutex_.lock_shared();
    if (wait_rv == std::cv_status::timeout) {
      auto it = broker_->counts_.find(key_);
      if (it == broker_->counts_.end()) {
        return true;
      }
      if (it->second > 1) {
        it->second--;
      } else {
        broker_->counts_.erase(it);
      }
      return false;
    }
  }
  return true;
}

template<typename KEYTYPE>
inline SlottedKeySignalBroker<KEYTYPE>::SlottedKeySignalBroker(int32_t num_slots)
    : num_slots_(num_slots) {
  slots_ = new KeySignalBroker<KEYTYPE>[num_slots];
}

template<typename KEYTYPE>
inline SlottedKeySignalBroker<KEYTYPE>::~SlottedKeySignalBroker() {
  delete[] slots_;
}

template<typename KEYTYPE>
inline bool SlottedKeySignalBroker<KEYTYPE>::Send(const KEYTYPE& key, bool all) {
  const int32_t bucket_index = static_cast<uint32_t>(std::hash<KEYTYPE>{}(key)) % num_slots_;
  return slots_[bucket_index].Send(key, all);
}

template<typename KEYTYPE>
inline SlottedKeySignalBroker<KEYTYPE>::Waiter::Waiter(
    SlottedKeySignalBroker<KEYTYPE>* broker, const KEYTYPE& key)
    : slot_waiter_(&broker->slots_[
          static_cast<uint32_t>(std::hash<KEYTYPE>{}(key)) % broker->num_slots_], key) {}

template<typename KEYTYPE>
inline bool SlottedKeySignalBroker<KEYTYPE>::Waiter::Wait(double timeout) {
  return slot_waiter_.Wait(timeout);
}

template<typename T>
inline ScopedCounter<T>::ScopedCounter(T* count, int32_t increment)
    : count_(count), increment_(increment) {
  *count_ += increment_;
}

template<typename T>
inline ScopedCounter<T>::~ScopedCounter() {
  *count_ -= increment_;
}

}  // namespace tkrzw

#endif  // _TKRZW_THREAD_UTIL_H

// END OF FILE
