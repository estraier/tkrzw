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
#include <typeinfo>
#include <utility>
#include <vector>

#include <cinttypes>

#include "tkrzw_dbm.h"
#include "tkrzw_thread_util.h"

namespace tkrzw {

/**
 * Asynchronous database manager adapter.
 * @details This class is a wrapper of DBM for asynchronous operations.  A task queue with a
 * thread pool is used inside.  Every method except for the constructor and the destructor is
 * run by a thread in the thread pool and the result is set in the future oject of the return
 * value.  The caller can ignore the future object if it is not necessary.  The destructor of
 * this asynchronous database manager waits for all tasks to be done.  Therefore, the destructor
 * should be called before the database is closed.
 */
class AsyncDBM final {
 public:
  /**
   * Interface of a common post processor for a record.
   */
  class CommonPostprocessor {
   public:
    /**
     * Destructor.
     */
    virtual ~CommonPostprocessor() = default;

    /**
     * Processes the status of a database operation.
     * @param name The method name of the database operation.
     * @param status The status of the database operation.
     */
    virtual void Postprocess(const char* name, const Status& status) {}
  };

  /**
   * Lambda function type to postprocess a database operation.
   * @details The first parameter is the method name of the database operation.  The second
   * parameter is the status of the database operation.
   */
  typedef std::function<void(const char*, const Status&)> CommonPostLambdaType;

  /**
   * Interface of asynchronous processor for a record.
   */
  class RecordProcessor : public DBM::RecordProcessor {
   public:
    /**
     * Destructor.
     */
    virtual ~RecordProcessor() = default;

    /**
     * Processes the status of the database operation.
     * @param status The status of the database operation.
     * @details This is called just after the database operation.
     */
    virtual void ProcessStatus(const Status& status) {}
  };

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
   * Copy and assignment are disabled.
   */
  explicit AsyncDBM(const AsyncDBM& rhs) = delete;
  AsyncDBM& operator =(const AsyncDBM& rhs) = delete;

  /**
   * Set the common post processor.
   * @param proc the common post processor.  If it is nullptr, no postprocess is done.  The
   * ownership is taken.
   * @details By default or if nullptr is set, no postprocess is done.  If a processor is set,
   * its Postprocess method is called every time in parallel after every method is called.
   */
  void SetCommonPostprocessor(std::unique_ptr<CommonPostprocessor> proc);

  /**
   * Set the common post processor with a lambda function.
   * @param post_lambda the lambda function of the common post processor.
   * @details By default, no postprocess is done.  If a processor is set, it is called every time
   * in parallel after every method is called.
   */
  void SetCommonPostprocessor(CommonPostLambdaType post_lambda) {
    class PostprocessorLambda : public CommonPostprocessor {
     public:
      explicit PostprocessorLambda(CommonPostLambdaType post_lambda)
          : post_lambda_(post_lambda) {}
      void Postprocess(const char* name, const Status& status) override {
        post_lambda_(name, status);
      }
     private:
      CommonPostLambdaType post_lambda_;
    };
    SetCommonPostprocessor(std::make_unique<PostprocessorLambda>(post_lambda));
  }

  /**
   * Processes a record with a processor.
   * @param key The key of the record.
   * @param proc The processor object derived from RecordProcessor.  The ownership is taken.
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
   * of the record.  The second parameter is the value of the existing record, or
   * RecordProcessor::NOOP if it the record doesn't exist.  The return value is a string
   * reference to RecordProcessor::NOOP, RecordProcessor::REMOVE, or the new record value.
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
   * Appends data to multiple records, with a map of string views.
   * @param records The records to append.
   * @param delim The delimiter to put after the existing record.
   * @return The result status.
   * @details If there's no existing record, the value is set without the delimiter.
   */
  std::future<Status> AppendMulti(
      const std::map<std::string_view, std::string_view>& records, std::string_view delim = "");

  /**
   * Appends data to multiple records, with a map of strings.
   * @param records The records to append.
   * @param delim The delimiter to put after the existing record.
   * @return The result status.
   * @details If there's no existing record, the value is set without the delimiter.
   */
  std::future<Status> AppendMulti(
      const std::map<std::string, std::string>& records, std::string_view delim = "") {
    return AppendMulti(MakeStrViewMapFromRecords(records), delim);
  }

  /**
   * Compares the value of a record and exchanges if the condition meets.
   * @param key The key of the record.
   * @param expected The expected value.  If the data is nullptr, no existing record is expected.
   * If it is DBM::ANY_DATA, an existing record with any value is expacted.
   * @param desired The desired value.  If the data is nullptr, the record is to be removed.
   * If it is DBM::ANY_DATA, no update is done.
   * @return The result status.  If the condition doesn't meet, INFEASIBLE_ERROR is returned.
   */
  std::future<Status> CompareExchange(std::string_view key, std::string_view expected,
                                      std::string_view desired);

  /**
   * Compares the values of records and exchanges if the condition meets.
   * @param expected The record keys and their expected values.  If the value is nullptr, no
   * existing record is expected.  If the value is DBM::ANY_DATA, an existing record with any
   * value is expacted.
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
   * Changes the key of a record.
   * @param old_key The old key of the record.
   * @param new_key The new key of the record.
   * @param overwrite Whether to overwrite the existing record of the new key.
   * @param copying Whether to retain the record of the old key.
   * @return The result status.  If there's no matching record to the old key, NOT_FOUND_ERROR
   * is returned.  If the overwrite flag is false and there is an existing record of the new key,
   * DUPLICATION ERROR is returned.
   * @details This method is done atomically by ProcessMulti.  The other threads observe that the
   * record has either the old key or the new value.  No intermediate states are observed.
   */
  std::future<Status> Rekey(std::string_view old_key, std::string_view new_key,
                            bool overwrite = true, bool copying = false);

  /**
   * Processes the first record with a processor.
   * @param proc The processor object derived from RecordProcessor.  The ownership is taken.
   * @param writable True if the processor can edit the record.
   * @return The result status and the same processor object as the parameter.
   * @details If the first record exists, the ProcessFull of the processor is called.
   * Otherwise, this method fails and no method of the processor is called.  Whereas ordered
   * databases have efficient implementations of this method, unordered databases have
   * inefficient implementations.
   */
  template <typename PROC>
  std::future<std::pair<Status, std::unique_ptr<PROC>>> ProcessFirst(
      std::unique_ptr<PROC> proc, bool writable);

  /**
   * Processes the first record with a lambda function.
   * @param rec_lambda The lambda function to process a record.  The first parameter is the key
   * of the record.  The second parameter is the value of the record.  The return value is a
   * string reference to RecordProcessor::NOOP, RecordProcessor::REMOVE, or the new record value.
   * @param writable True if the processor can edit the record.
   * @return The result status.
   */
  std::future<Status> ProcessFirst(DBM::RecordLambdaType rec_lambda, bool writable);

  /**
   * Gets the first record and removes it.
   * @param key The pointer to a string object to contain the key of the first record.  If it
   * is nullptr, it is ignored.
   * @param value The pointer to a string object to contain the value of the first record.  If
   * it is nullptr, it is ignored.
   * @return The result status and a pair of the key and the value of the first record.
   */
  std::future<std::pair<Status, std::pair<std::string, std::string>>> PopFirst();

  /**
   * Adds a record with a key of the current timestamp.
   * @param value The value of the record.
   * @param wtime The current wall time used to generate the key.  If it is negative, the system
   * clock is used.
   * @return The result status.
   * @details The key is generated as an 8-bite big-endian binary string of the timestamp.  If
   * there is an existing record matching the generated key, the key is regenerated and the
   * attempt is repeated until it succeeds.
   */
  std::future<Status> PushLast(std::string_view value, double wtime = -1);

  /**
   * Processes multiple records with processors.
   * @param key_proc_pairs Pairs of the keys and their processor objects derived from
   * RecordProcessor.  The ownership is taken.
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
   * the lambda functions is the key of the record, or RecordProcessor::NOOP if it the record
   * doesn't exist.  The return value is a string reference to RecordProcessor::NOOP,
   * RecordProcessor::REMOVE, or the new record value.
   * @param writable True if the processors can edit the records.
   * @return The result status.
   */
  std::future<Status> ProcessMulti(
      const std::vector<std::pair<std::string_view, DBM::RecordLambdaType>>& key_lambda_pairs,
      bool writable);

  /**
   * Processes each and every record in the database with a processor.
   * @param proc The processor object derived from RecordProcessor.  The ownership is taken.
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
   * of the record.  The second parameter is the value of the existing record, or
   * RecordProcessor::NOOP if it the record doesn't exist.  The return value is a string
   * reference to RecordProcessor::NOOP, RecordProcessor::REMOVE, or the new record value.
   * @param writable True if the processor can edit the record.
   * @return The result status.
   * @details The lambda function is called repeatedly for each record.  It is also called once
   * before the iteration and once after the iteration with both the key and the value being
   * RecordProcessor::NOOP.
   */
  std::future<Status> ProcessEach(DBM::RecordLambdaType rec_lambda, bool writable);

  /**
   * Removes all records.
   * @return The result status.
   */
  std::future<Status> Clear();

  /**
   * Rebuilds the entire database.
   * @param params Optional parameters.
   * @return The result status.
   * @details The parameters work in the same way as with PolyDBM::RebuildAdvanced.
   */
  std::future<Status> Rebuild(const std::map<std::string, std::string>& params = {});

  /**
   * Synchronizes the content of the database to the file system.
   * @param hard True to do physical synchronization with the hardware or false to do only
   * logical synchronization with the file system.
   * @param proc The file processor object, whose Process method is called while the content of
   * the file is synchronized.  The ownership is taken. If it is nullptr, it is ignored.
   * If it is nullptr, it is not used.
   * @param params Optional parameters.
   * @return The result status.
   * @details The parameters work in the same way as with PolyDBM::Synchronize.
   */
  std::future<Status> Synchronize(bool hard, std::unique_ptr<DBM::FileProcessor> proc = nullptr,
                                  const std::map<std::string, std::string>& params = {});

  /**
   * Copies the content of the database file to another file.
   * @param dest_path A path to the destination file.
   * @param sync_hard True to do physical synchronization with the hardware.
   * @return The result status.
   * @details Copying is done while the content is synchronized and stable.  So, this method is
   * suitable for making a backup file while running a database service.
   */
  std::future<Status> CopyFileData(const std::string& dest_path, bool sync_hard = false);

  /**
   * Exports all records to another database.
   * @param dbm The pointer to the destination database.  The lefetime of the database object
   * must last until the task finishes.
   * @return The result status.
   */
  std::future<Status> Export(DBM* dbm);

  /**
   * Exports all records of a database to a flat record file.
   * @param dest_file The file object to write records in.  The lefetime of the file object
   * must last until the task finishes.
   * @return The result status.
   * @details A flat record file contains a sequence of binary records without any high level
   * structure so it is useful as a intermediate file for data migration.
   */
  std::future<Status> ExportToFlatRecords(File* dest_file);

  /**
   * Imports records to a database from a flat record file.
   * @param src_file The file object to read records from.  The lefetime of the file object
   * must last until the task finishes.
   * @return The result status.
   */
  std::future<Status> ImportFromFlatRecords(File* src_file);

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
  /** The postprocessor. */
  std::unique_ptr<CommonPostprocessor> postproc_;
};

/**
 * Wrapper of std::future containing a status object and extra data.
 */
class StatusFuture final {
 public:
  /**
   * Constructor for a status object.
   * @param future a future object.  The ownership is taken.
   */
  explicit StatusFuture(std::future<Status>&& future);

  /**
   * Constructor for a status object and a string.
   * @param future a future object.  The ownership is taken.
   */
  explicit StatusFuture(std::future<std::pair<Status, std::string>>&& future);

  /**
   * Constructor for a status object and a string pair.
   * @param future a future object.  The ownership is taken.
   */
  explicit StatusFuture(std::future<std::pair<
                        Status, std::pair<std::string, std::string>>>&& future);

  /**
   * Constructor for a status object and a string vector.
   * @param future a future object.  The ownership is taken.
   */
  explicit StatusFuture(std::future<std::pair<Status, std::vector<std::string>>>&& future);

  /**
   * Constructor for a status object and a string map.
   * @param future a future object.  The ownership is taken.
   */
  explicit StatusFuture(std::future<std::pair<
                        Status, std::map<std::string, std::string>>>&& future);

  /**
   * Constructor for a status object and an integer.
   * @param future a future object.  The ownership is taken.
   */
  explicit StatusFuture(std::future<std::pair<Status, int64_t>>&& future);

  /**
   * Move constructor.
   * @param rhs The right-hand-side object.
   */
  StatusFuture(StatusFuture&& rhs);

  /**
   * Destructor.
   */
  ~StatusFuture();

  /**
   * Copy and assignment are disabled.
   */
  explicit StatusFuture(const StatusFuture& rhs) = delete;
  StatusFuture& operator =(const StatusFuture& rhs) = delete;

  /**
   * Waits for the operation to be done.
   * @param timeout The waiting time in seconds.  If it is negative, no timeout is set.
   * @return True if the operation has done.  False if timeout occurs.
   */
  bool Wait(double timeout = -1);

  /**
   * Waits for the operation to be done and gets the result status.
   * @return The result status.
   * @details Either one of the Get method family can be called only once.
   */
  Status Get();

  /**
   * Waits for the operation to be done and gets the status and the extra string.
   * @return The result status and the extra string.
   * @details Either one of the Get method family can be called only once.
   */
  std::pair<Status, std::string> GetString();

  /**
   * Waits for the operation to be done and gets the status and the extra string pair.
   * @return The result status and the extra string pair.
   * @details Either one of the Get method family can be called only once.
   */
  std::pair<Status, std::pair<std::string, std::string>> GetStringPair();

  /**
   * Waits for the operation to be done and gets the status and the extra string vector.
   * @return The result status and the extra string vector.
   * @details Either one of the Get method family can be called only once.
   */
  std::pair<Status, std::vector<std::string>> GetStringVector();

  /**
   * Waits for the operation to be done and gets the status and the extra string map.
   * @return The result status and the extra string map.
   * @details Either one of the Get method family can be called only once.
   */
  std::pair<Status, std::map<std::string, std::string>> GetStringMap();

  /**
   * Waits for the operation to be done and gets the status and the extra integer.
   * @return The result status and the extra integer.
   * @details Either one of the Get method family can be called only once.
   */
  std::pair<Status, int64_t> GetInteger();

  /**
   * Gets the type information of the extra data.
   * @return The type information of the extra data.
   */
  const std::type_info& GetExtraType();

 private:
  void* future_;
  const std::type_info& type_;
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
      proc->ProcessStatus(status);
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
inline std::future<std::pair<Status, std::unique_ptr<PROC>>> AsyncDBM::ProcessFirst(
    std::unique_ptr<PROC> proc, bool writable) {
  struct ProcessEachTask : public TaskQueue::Task {
    DBM* dbm;
    std::unique_ptr<PROC> proc;
    bool writable;
    std::promise<std::pair<Status, std::unique_ptr<PROC>>> promise;
    void Do() override {
      Status status = dbm->ProcessFirst(proc.get(), writable);
      proc->ProcessStatus(status);
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

inline std::future<Status> AsyncDBM::ProcessFirst(
    DBM::RecordLambdaType rec_lambda, bool writable) {
  struct ProcessEachTask : public TaskQueue::Task {
    DBM* dbm;
    DBM::RecordLambdaType rec_lambda;
    bool writable;
    std::promise<Status> promise;
    void Do() override {
      Status status = dbm->ProcessFirst(rec_lambda, writable);
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
inline std::future<std::pair<Status, std::unique_ptr<PROC>>> AsyncDBM::ProcessEach(
    std::unique_ptr<PROC> proc, bool writable) {
  struct ProcessEachTask : public TaskQueue::Task {
    DBM* dbm;
    std::unique_ptr<PROC> proc;
    bool writable;
    std::promise<std::pair<Status, std::unique_ptr<PROC>>> promise;
    void Do() override {
      Status status = dbm->ProcessEach(proc.get(), writable);
      proc->ProcessStatus(status);
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
      for (auto& proc : procs) {
        proc->ProcessStatus(status);
      }
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
