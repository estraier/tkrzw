/*************************************************************************************************
 * Asynchronous database manager adapter
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

#ifndef _TKRZW_DBM_ASYNC_H
#define _TKRZW_DBM_ASYNC_H

#include <functional>
#include <future>
#include <initializer_list>
#include <map>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <cinttypes>

#include "tkrzw_dbm.h"
#include "tkrzw_thread_util.h"

namespace tkrzw {

/**
 * Asynchronous database manager adapter.
 * @details This class is a wrapper of DBM for asynchronous operations.  A task queue with a
 * thread pool is used inside.  Every methods except for the constructor and the destructor are
 * run by a thread in the thread pool and the result is set in the feature oject of the return
 * value.  The caller can ignore the feature object if it is not necessary.  The destructor of
 * this asynchronous database manager waits for all tasks to be done.  Therefore, the destructor
 * should be called before the database is closed.
 */
class AsyncDBM final {
 public:
  /**
   * Constructor.
   * @param dbm A database object which has been opened.  The ownership is not taken.
   * @param num_worker_threads The number of threads in the internal thread pool.
   */
  AsyncDBM(DBM* dbm, int32_t num_worker_threads);

  /**
   * Destructor.
   */
  ~AsyncDBM();

  /**
   * Processes a record with a processor.
   * @param key The key of the record.
   * @param proc The processor object derived from DBM::RecordProcessor.  The ownership is taken.
   * @param writable True if the processor can edit the record.
   * @return The result status and the same processor object as the parameter.
   * @details If the specified record exists, the ProcessFull of the processor is called.
   * Otherwise, the ProcessEmpty of the processor is called.
   */
  template <typename PROC>
  std::future<std::pair<Status, std::unique_ptr<PROC>>> Process(
      std::string_view key, std::unique_ptr<PROC> proc, bool writable);

  /**
   * Processes a record with a lambda function.
   * @param key The key of the record.
   * @param rec_lambda The lambda function to process a record.  The first parameter is the key
   * of the record.  The second parameter is the value of the existing record, or NOOP if it the
   * record doesn't exist.  The return value is a string reference to NOOP, REMOVE, or the new
   * record value.
   * @param writable True if the processor can edit the record.
   * @return The result status.
   */
  std::future<Status> Process(std::string_view key, DBM::RecordLambdaType rec_lambda,
                              bool writable);

  /**
   * Gets the value of a record of a key.
   * @param key The key of the record.
   * @return The result status and the result value.  If there's no matching record,
   * NOT_FOUND_ERROR is returned.
   */
  std::future<std::pair<Status, std::string>> Get(std::string_view key);

  /**
   * Gets the values of multiple records of keys, with a string view vector.
   * @param keys The keys of records to retrieve.
   * @return The result status and a map of retrieved records.  Keys which don't match existing
   * records are ignored.  If all records of the given keys are found, SUCCESS is returned.
   * If one or more records are missing, NOT_FOUND_ERROR is returned.  Thus, even with an error
   * code, the result map can have elements.
   */
  std::future<std::pair<Status, std::map<std::string, std::string>>> GetMulti(
      const std::vector<std::string_view>& keys);

  /**
   * Gets the values of multiple records of keys, with a string vector.
   * @param keys The keys of records to retrieve.
   * @return The result status and a map of retrieved records.  Keys which don't match existing
   * records are ignored.  If all records of the given keys are found, SUCCESS is returned.
   * If one or more records are missing, NOT_FOUND_ERROR is returned.  Thus, even with an error
   * code, the result map can have elements.
   */
  std::future<std::pair<Status, std::map<std::string, std::string>>> GetMulti(
      const std::vector<std::string>& keys) {
    return GetMulti(MakeStrViewVectorFromValues(keys));
  }

  /**
   * Sets a record of a key and a value.
   * @param key The key of the record.
   * @param value The value of the record.
   * @param overwrite Whether to overwrite the existing value if there's a record with the same
   * key.  If true, the existing value is overwritten by the new value.  If false, the operation
   * is given up and an error status is returned.
   * @return The result status.  If overwriting is abandoned, DUPLICATION_ERROR is returned.
   */
  std::future<Status> Set(std::string_view key, std::string_view value, bool overwrite = true);

  /**
   * Sets multiple records, with a map of string views.
   * @param records The records to store.
   * @param overwrite Whether to overwrite the existing value if there's a record with the same
   * key.  If true, the existing value is overwritten by the new value.  If false, the operation
   * is given up and an error status is returned.
   * @return The result status.
   */
  std::future<Status> SetMulti(
      const std::map<std::string_view, std::string_view>& records, bool overwrite = true);

  /**
   * Sets multiple records, with a map of strings.
   * @param records The records to store.
   * @param overwrite Whether to overwrite the existing value if there's a record with the same
   * key.  If true, the existing value is overwritten by the new value.  If false, the operation
   * is given up and an error status is returned.
   * @return The result status.
   */
  std::future<Status> SetMulti(
      const std::map<std::string, std::string>& records, bool overwrite = true) {
    return SetMulti(MakeStrViewMapFromRecords(records));
  }

  /**
   * Removes a record of a key.
   * @param key The key of the record.
   * @return The result status.  If there's no matching record, NOT_FOUND_ERROR is returned.
   */
  std::future<Status> Remove(std::string_view key);

  /**
   * Removes records of keys, with a string view vector.
   * @param keys The keys of records to remove.
   * @return The result status.  If there are missing records, NOT_FOUND_ERROR is returned.
   */
  std::future<Status> RemoveMulti(const std::vector<std::string_view>& keys);

  /**
   * Removes records of keys, with a string vector.
   * @param keys The keys of records to remove.
   * @return The result status.  If there are missing records, NOT_FOUND_ERROR is returned.
   */
  std::future<Status> RemoveMulti(const std::vector<std::string>& keys) {
    return RemoveMulti(MakeStrViewVectorFromValues(keys));
  }

  /**
   * Appends data at the end of a record of a key.
   * @param key The key of the record.
   * @param value The value to append.
   * @param delim The delimiter to put after the existing record.
   * @return The result status.
   * @details If there's no existing record, the value is set without the delimiter.
   */
  std::future<Status> Append(
      std::string_view key, std::string_view value, std::string_view delim = "");

  /**
   * Appends data to multiple records, with a map of strings.
   * @param records The records to append.
   * @param delim The delimiter to put after the existing record.
   * @return The result status.
   * @details If there's no existing record, the value is set without the delimiter.
   */
  std::future<Status> AppendMulti(
      const std::map<std::string_view, std::string_view>& records, std::string_view delim = "");

  /**
   * Compares the value of a record and exchanges if the condition meets.
   * @param key The key of the record.
   * @param expected The expected value.  If the data is nullptr, no existing record is expected.
   * @param desired The desired value.  If the data is nullptr, the record is to be removed.
   * @return The result status.  If the condition doesn't meet, INFEASIBLE_ERROR is returned.
   */
  std::future<Status> CompareExchange(std::string_view key, std::string_view expected,
                                      std::string_view desired);

  /**
   * Compares the values of records and exchanges if the condition meets.
   * @param expected The record keys and their expected values.  If the value is nullptr, no
   * existing record is expected.
   * @param desired The record keys and their desired values.  If the value is nullptr, the
   * record is to be removed.
   * @return The result status.  If the condition doesn't meet, INFEASIBLE_ERROR is returned.
   */
  std::future<Status> CompareExchangeMulti(
      const std::vector<std::pair<std::string_view, std::string_view>>& expected,
      const std::vector<std::pair<std::string_view, std::string_view>>& desired);

  /**
   * Increments the numeric value of a record.
   * @param key The key of the record.
   * @param increment The incremental value.  If it is INT64MIN, the current value is not changed
   * and a new record is not created.
   * @param initial The initial value.
   * @return The result status and the current value.
   * @details The record value is stored as an 8-byte big-endian integer.  Negative is also
   * supported.
   */
  std::future<std::pair<Status, int64_t>> Increment(
      std::string_view key, int64_t increment = 1, int64_t initial = 0);

  /**
   * Processes multiple records with processors.
   * @param key_proc_pairs Pairs of the keys and their processor objects derived from
   * DBM::RecordProcessor.  The ownership is taken.
   * @param writable True if the processors can edit the records.
   * @return The result status and a vector of the same object as the parameter.
   * @details If the specified record exists, the ProcessFull of the processor is called.
   * Otherwise, the ProcessEmpty of the processor is called.
   */
  template <typename PROC>
  std::future<std::pair<Status, std::vector<std::shared_ptr<PROC>>>> ProcessMulti(
      const std::vector<std::pair<std::string_view, std::shared_ptr<PROC>>>& key_proc_pairs,
      bool writable);

  /**
   * Processes multiple records with lambda functions.
   * @param key_lambda_pairs Pairs of the keys and their lambda functions.  The first parameter of
   * the lambda functions is the key of the record, or NOOP if it the record doesn't exist.  The
   * return value is a string reference to NOOP, REMOVE, or the new record value.
   * @param writable True if the processors can edit the records.
   * @return The result status.
   */
  std::future<Status> ProcessMulti(
      const std::vector<std::pair<std::string_view, DBM::RecordLambdaType>>& key_lambda_pairs,
      bool writable);

  /**
   * Processes each and every record in the database with a processor.
   * @param proc The processor object derived from DBM::RecordProcessor.  The ownership is taken.
   * @param writable True if the processor can edit the record.
   * @return The result status and the same processor object as the parameter.
   * @details The ProcessFull of the processor is called repeatedly for each record.  The
   * ProcessEmpty of the processor is called once before the iteration and once after the
   * iteration.
   */
  template <typename PROC>
  std::future<std::pair<Status, std::unique_ptr<PROC>>> ProcessEach(
      std::unique_ptr<PROC> proc, bool writable);

  /**
   * Processes each and every record in the database with a lambda function.
   * @param rec_lambda The lambda function to process a record.  The first parameter is the key
   * of the record.  The second parameter is the value of the existing record, or NOOP if it the
   * record doesn't exist.  The return value is a string reference to NOOP, REMOVE, or the new
   * record value.
   * @param writable True if the processor can edit the record.
   * @return The result status.
   * @details The lambda function is called repeatedly for each record.  It is also called once
   * before the iteration and once after the iteration with both the key and the value being NOOP.
   */
  std::future<Status> ProcessEach(DBM::RecordLambdaType rec_lambda, bool writable);

  /**
   * Removes all records.
   * @return The result status.
   */
  std::future<Status> Clear();

  /**
   * Rebuilds the entire database.
   * @return The result status.
   */
  std::future<Status> Rebuild();

  /**
   * Synchronizes the content of the database to the file system.
   * @param hard True to do physical synchronization with the hardware or false to do only
   * logical synchronization with the file system.
   * @param proc The file processor object, whose Process method is called while the content of
   * the file is synchronized.  The ownership is taken. If it is nullptr, it is ignored.
   * If it is nullptr, it is not used.
   * @return The result status.
   */
  std::future<Status> Synchronize(bool hard, std::unique_ptr<DBM::FileProcessor> proc = nullptr);

  /**
   * Searches the database and get keys which match a pattern, according to a mode expression.
   * @param mode The search mode.  "contain" extracts keys containing the pattern.  "begin"
   * extracts keys beginning with the pattern.  "end" extracts keys ending with the pattern.
   * "regex" extracts keys partially matches the pattern of a regular expression.  "edit"
   * extracts keys whose edit distance to the UTF-8 pattern is the least.  "editbin" extracts
   * keys whose edit distance to the binary pattern is the least.  Ordered databases support
   * "upper" and "lower" which extract keys whose positions are upper/lower than the pattern.
   * "upperinc" and "lowerinc" are their inclusive versions.
   * @param pattern The pattern for matching.
   * @param capacity The maximum records to obtain.  0 means unlimited.
   * @return The result status and the result keys.
   */
  std::future<std::pair<Status, std::vector<std::string>>> SearchModal(
      std::string_view mode, std::string_view pattern, size_t capacity = 0);

  /**
   * Gets the internal task queue.
   * @return The pointer to the internal task queue.  The ownership is not moved.
   */
  TaskQueue* GetTaskQueue() {
    return &queue_;
  }

 private:
  /** The database object. */
  DBM* dbm_;
  /** The task queue. */
  TaskQueue queue_;
};

template <typename PROC>
inline std::future<std::pair<Status, std::unique_ptr<PROC>>> AsyncDBM::Process(
    std::string_view key, std::unique_ptr<PROC> proc, bool writable) {
  struct ProcessTask : public TaskQueue::Task {
    DBM* dbm;
    std::string key;
    std::unique_ptr<PROC> proc;
    bool writable;
    std::promise<std::pair<Status, std::unique_ptr<PROC>>> promise;
    void Do() override {
      Status status = dbm->Process(key, proc.get(), writable);
      promise.set_value(std::make_pair(std::move(status), std::move(proc)));
    }
  };
  auto task = std::make_unique<ProcessTask>();
  task->dbm = dbm_;
  task->key = key;
  task->proc = std::move(proc);
  task->writable = writable;
  auto future = task->promise.get_future();
  queue_.Add(std::move(task));
  return future;
}

inline std::future<Status> AsyncDBM::Process(
    std::string_view key, DBM::RecordLambdaType rec_lambda, bool writable) {
  struct ProcessTask : public TaskQueue::Task {
    DBM* dbm;
    std::string key;
    DBM::RecordLambdaType rec_lambda;
    bool writable;
    std::promise<Status> promise;
    void Do() override {
      Status status = dbm->Process(key, rec_lambda, writable);
      promise.set_value(std::move(status));
    }
  };
  auto task = std::make_unique<ProcessTask>();
  task->dbm = dbm_;
  task->key = key;
  task->rec_lambda = rec_lambda;
  task->writable = writable;
  auto future = task->promise.get_future();
  queue_.Add(std::move(task));
  return future;
}

template <typename PROC>
inline std::future<std::pair<Status, std::unique_ptr<PROC>>> AsyncDBM::ProcessEach(
    std::unique_ptr<PROC> proc, bool writable) {
  struct ProcessEachTask : public TaskQueue::Task {
    DBM* dbm;
    std::unique_ptr<PROC> proc;
    bool writable;
    std::promise<std::pair<Status, std::unique_ptr<PROC>>> promise;
    void Do() override {
      Status status = dbm->ProcessEach(proc.get(), writable);
      promise.set_value(std::make_pair(std::move(status), std::move(proc)));
    }
  };
  auto task = std::make_unique<ProcessEachTask>();
  task->dbm = dbm_;
  task->proc = std::move(proc);
  task->writable = writable;
  auto future = task->promise.get_future();
  queue_.Add(std::move(task));
  return future;
}

inline std::future<Status> AsyncDBM::ProcessEach(
    DBM::RecordLambdaType rec_lambda, bool writable) {
  struct ProcessEachTask : public TaskQueue::Task {
    DBM* dbm;
    DBM::RecordLambdaType rec_lambda;
    bool writable;
    std::promise<Status> promise;
    void Do() override {
      Status status = dbm->ProcessEach(rec_lambda, writable);
      promise.set_value(std::move(status));
    }
  };
  auto task = std::make_unique<ProcessEachTask>();
  task->dbm = dbm_;
  task->rec_lambda = rec_lambda;
  task->writable = writable;
  auto future = task->promise.get_future();
  queue_.Add(std::move(task));
  return future;
}

template <typename PROC>
inline std::future<std::pair<Status, std::vector<std::shared_ptr<PROC>>>> AsyncDBM::ProcessMulti(
    const std::vector<std::pair<std::string_view, std::shared_ptr<PROC>>>& key_proc_pairs,
    bool writable) {
  struct ProcessMultiTask : public TaskQueue::Task {
    DBM* dbm;
    std::vector<std::pair<std::string, std::shared_ptr<PROC>>> key_proc_pairs;
    bool writable;
    std::promise<std::pair<Status, std::vector<std::shared_ptr<PROC>>>> promise;
    void Do() override {
      std::vector<std::pair<std::string_view, DBM::RecordProcessor*>> tmp_pairs;
      tmp_pairs.reserve(key_proc_pairs.size());
      std::vector<std::shared_ptr<PROC>> procs;
      procs.reserve(key_proc_pairs.size());
      for (auto& key_proc : key_proc_pairs) {
        tmp_pairs.emplace_back(std::make_pair(
            std::string_view(key_proc.first), key_proc.second.get()));
        procs.emplace_back(key_proc.second);
      }
      Status status = dbm->ProcessMulti(tmp_pairs, writable);
      promise.set_value(std::make_pair(std::move(status), std::move(procs)));
    }
  };
  auto task = std::make_unique<ProcessMultiTask>();
  task->dbm = dbm_;
  task->key_proc_pairs.reserve(key_proc_pairs.size());
  for (auto& key_proc : key_proc_pairs) {
    task->key_proc_pairs.emplace_back(std::make_pair(
        std::string(key_proc.first), key_proc.second));
  }
  task->writable = writable;
  auto future = task->promise.get_future();
  queue_.Add(std::move(task));
  return future;
}

inline std::future<Status> AsyncDBM::ProcessMulti(
    const std::vector<std::pair<std::string_view, DBM::RecordLambdaType>>& key_lambda_pairs,
    bool writable) {
  struct ProcessMultiTask : public TaskQueue::Task {
    DBM* dbm;
    std::vector<std::pair<std::string, DBM::RecordLambdaType>> key_lambda_pairs;
    bool writable;
    std::promise<Status> promise;
    void Do() override {
      std::vector<std::pair<std::string_view, DBM::RecordProcessor*>> key_proc_pairs;
      key_proc_pairs.reserve(key_lambda_pairs.size());
      std::vector<DBM::RecordProcessorLambda> procs;
      procs.reserve(key_lambda_pairs.size());
      for (auto& key_lambda : key_lambda_pairs) {
        procs.emplace_back(key_lambda.second);
        key_proc_pairs.emplace_back(std::make_pair(
            std::string_view(key_lambda.first), &procs.back()));
      }
      Status status = dbm->ProcessMulti(key_proc_pairs, writable);
      promise.set_value(std::move(status));
    }
  };
  auto task = std::make_unique<ProcessMultiTask>();
  task->dbm = dbm_;
  task->key_lambda_pairs.reserve(key_lambda_pairs.size());
  for (auto& key_lambda : key_lambda_pairs) {
    task->key_lambda_pairs.emplace_back(std::make_pair(
        std::string(key_lambda.first), key_lambda.second));
  }
  task->writable = writable;
  auto future = task->promise.get_future();
  queue_.Add(std::move(task));
  return future;
}

}  // namespace tkrzw

#endif  // _TKRZW_DBM_ASYNC_H

// END OF FILE
