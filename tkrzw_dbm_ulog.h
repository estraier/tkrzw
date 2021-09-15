/*************************************************************************************************
 * DBM update logger implementations
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

#ifndef _TKRZW_DBM_ULOG_H
#define _TKRZW_DBM_ULOG_H

#include <deque>
#include <string>
#include <string_view>

#include <cinttypes>

#include "tkrzw_dbm.h"
#include "tkrzw_lib_common.h"
#include "tkrzw_thread_util.h"

namespace tkrzw {

/**
 * DBM update logger to store logs into a string deque.
 */
class DBMUpdateLoggerStrDeque final : public DBM::UpdateLogger {
 public:
  /**
   * Constructor.
   * @param delimiter The delimiter put between fields in a log.
   */
  explicit DBMUpdateLoggerStrDeque(const std::string& delim = "\t");

  /**
   * Writes a log for modifying an existing record or adding a new record.
   * @param key The key of the record.
   * @param value The new value of the record.
   * @return The result status.
   */
  Status WriteSet(std::string_view key, std::string_view value) override;

  /**
   * Writes a log for removing an existing record.
   * @param key The key of the record.
   * @return The result status.
   */
  Status WriteRemove(std::string_view key) override;

  /**
   * Writes a log for removing all records.
   * @return The result status.
   */
  Status WriteClear() override;

  /**
   * Gets the number of logs.
   * @return The number of logs.
   */
  int64_t GetSize();

  /**
   * Gets the first log in the queue and removes it.
   * @param text The pointer to a string object to store the result.  If it is nullptr,
   * assignment is not done.
   * @return True on success or false if there's no log in the queue.
   */
  bool PopFront(std::string* text);

  /**
   * Gets the last log in the queue and removes it.
   * @param text The pointer to a string object to store the result.  If it is nullptr,
   * assignment is not done.
   * @return True on success or false if there's no log in the queue.
   */
  bool PopBack(std::string* text);

  /**
   * Removes all logs.
   */
  void Clear();

 private:
  /** The delimiter string. */
  std::string delim_;
  /** Log strings. */
  std::deque<std::string> logs_;
  /** Mutex to guard the logs. */
  SpinMutex mutex_;
};

/**
 * DBM update logger to replicate updates in another DBM.
 */
class DBMUpdateLoggerDBM final : public DBM::UpdateLogger {
 public:
  /**
   * Constructor.
   * @param dbm A DBM object to store logs in.  The ownership is not taken.
   */
  explicit DBMUpdateLoggerDBM(DBM* dbm);

  /**
   * Writes a log for modifying an existing record or adding a new record.
   * @param key The key of the record.
   * @param value The new value of the record.
   * @return The result status.
   */
  Status WriteSet(std::string_view key, std::string_view value) override;

  /**
   * Writes a log for removing an existing record.
   * @param key The key of the record.
   * @return The result status.
   */
  Status WriteRemove(std::string_view key) override;

  /**
   * Writes a log for removing all records.
   * @return The result status.
   */
  Status WriteClear() override;

 private:
  /** A DBM object to store logs in. */
  DBM* dbm_;
};

/**
 * Update logger adapter for the second shard and later.
 */
class DBMUpdateLoggerSecondShard final : public DBM::UpdateLogger {
 public:
  /**
   * Default constructor.
   */
  DBMUpdateLoggerSecondShard() : ulog_(nullptr) {}

  /**
   * Constructor.
   * @param logger The logger to do actual logging.
   */
  explicit DBMUpdateLoggerSecondShard(DBM::UpdateLogger* ulog) : ulog_(ulog) {}

  /**
   * Set the update logger to do actual logging.
   * @param logger The update logger to do actual logging.
   */
  void SetUpdateLogger(DBM::UpdateLogger* ulog) {
    ulog_ = ulog;
  }

  /**
   * Writes a log for modifying an existing record or adding a new record.
   * @param key The key of the record.
   * @param value The new value of the record.
   * @return The result status.
   */
  Status WriteSet(std::string_view key, std::string_view value) override {
    return ulog_->WriteSet(key, value);
  }

  /**
   * Writes a log for removing an existing record.
   * @param key The key of the record.
   * @return The result status.
   */
  Status WriteRemove(std::string_view key) override {
    return ulog_->WriteRemove(key);
  }

  /**
   * Writes a log for removing all records.
   * @return The result status.
   * @details This does no operation.
   */
  Status WriteClear() override {
    return Status(Status::SUCCESS);
  }

 public:
  DBM::UpdateLogger* ulog_;
};

}  // namespace tkrzw

#endif  // _TKRZW_DBM_ULOG_H

// END OF FILE
