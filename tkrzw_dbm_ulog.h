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
#include "tkrzw_message_queue.h"
#include "tkrzw_thread_util.h"

namespace tkrzw {

/**
 * DBM update logger to store logs into a string deque.
 */
class DBMUpdateLoggerStrDeque final : public DBM::UpdateLogger {
 public:
  /**
   * Constructor.
   * @param delim The delimiter put between fields in a log.
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

  /**
   * Synchronizes the metadata and content to the file system.
   * @param hard True to do physical synchronization with the hardware or false to do only
   * logical synchronization with the file system.
   * @return The result status.
   */
  Status Synchronize(bool hard) override;

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
   * @param ulog The logger to do actual logging.
   */
  explicit DBMUpdateLoggerSecondShard(DBM::UpdateLogger* ulog) : ulog_(ulog) {}

  /**
   * Set the update logger to do actual logging.
   * @param ulog The update logger to do actual logging.
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
  /** The internal update logger. */
  DBM::UpdateLogger* ulog_;
};

/**
 * DBM update logger with a message queue.
 */
class DBMUpdateLoggerMQ final : public DBM::UpdateLogger {
 public:
  /**
   * Enumeration for operation types.
   */
  enum OpType : int32_t {
    /** Invalid operation. */
    OP_VOID = 0,
    /** To modify or add a record. */
    OP_SET = 1,
    /** To remove a record. */
    OP_REMOVE = 2,
    /** To remove all records. */
    OP_CLEAR = 3,
  };

  /**
   * Common structure of an update log.
   */
  struct UpdateLog {
    /** The operation type. */
    OpType op_type;
    /** The server ID. */
    int32_t server_id;
    /** The DBM index. */
    int32_t dbm_index;
    /** The key of the record. */
    std::string_view key;
    /** The value of the record. */
    std::string_view value;
  };

  /**
   * Constructor.
   * @param mq The message queue object to store update logs.  The ownership is not taken.
   * @param server_id The server ID of the process.
   * @param dbm_index The index of the DBM on the server.
   * @param fixed_timestamp If not negative, the timestamp is fixed to the value.
   */
  explicit DBMUpdateLoggerMQ(MessageQueue* mq, int32_t server_id = 0, int32_t dbm_index = 0,
                             int64_t fixed_timestamp = -1);

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
   * Synchronizes the metadata and content to the file system.
   * @param hard True to do physical synchronization with the hardware or false to do only
   * logical synchronization with the file system.
   * @return The result status.
   */
  Status Synchronize(bool hard) override;

  /**
   * Overwrites the server ID of the current thread.
   * @param server_id The server ID of the process.  If it is negative, the thread local setting
   * is undone.  If it is INT32MIN, logging of the current thread is disable.
   * @details This affects logging of only the current thread regardless of the logger instance.
   */
  static void OverwriteThreadServerID(int32_t server_id);

  /**
   * Parses an update log message.
   * @param message The update log message.
   * @param op The pointer to the update log object to store the result.  The life duration of
   * the key and the value fields is the same as the given message.
   * @return The result status.
   */
  static Status ParseUpdateLog(std::string_view message, UpdateLog* op);

  /**
   * Applys the operation in an update log to a database.
   * @param dbm The DBM object of the database.
   * @param message The update log message.
   * @param server_id The server ID to focus on.  A negative applies a filter which ignores the
   * message if the server ID mathces the absolute value.  Zero or a positive applies a filter
   * which adopts the message if the server ID matches the value.
   * @param dbm_index The DBM index to focus on.  A negative applies a filter which ignores the
   * message if the DBM index mathces the absolute value.  Zero or a positive applies a filter
   * which adopts the message if the DBM index matches the value.
   * @return The result status.  If the log is ignored due to the filter, INFEASIBLE_ERROR is
   * returned.
   */
  static Status ApplyUpdateLog(
      DBM* dbm, std::string_view message,
      int32_t server_id = INT32MIN + 1, int32_t dbm_index = INT32MIN + 1);

  /**
   * Applys the operations in the message queue files.
   * @param dbm The DBM object of the database.
   * @param prefix The prefix for the message queue file names.
   * @param min_timestamp The minimum timestamp in milliseconds of messages to read.
   * @param server_id The server ID to focus on.  A negative applies a filter which ignores the
   * message if the server ID mathces the absolute value.  Zero or a positive applies a filter
   * which adopts the message if the server ID matches the value.
   * @param dbm_index The DBM index to focus on.  A negative applies a filter which ignores the
   * message if the DBM index mathces the absolute value.  Zero or a positive applies a filter
   * which adopts the message if the DBM index matches the value.
   * @return The result status.
   */
  static Status ApplyUpdateLogFromFiles(
      DBM* dbm, const std::string& prefix, double min_timestamp = 0,
      int32_t server_id = INT32MIN + 1, int32_t dbm_index = INT32MIN + 1);

 private:
  /** The thread local server ID. */
  static thread_local int32_t thread_local_server_id_;
  /** The message queue. */
  MessageQueue* mq_;
  /** The server ID of the process. */
  int32_t server_id_;
  /** The index of the DBM on the server. */
  int32_t dbm_index_;
  /** The fixed timestamp. */
  int64_t fixed_timestamp_;
};

}  // namespace tkrzw

#endif  // _TKRZW_DBM_ULOG_H

// END OF FILE
