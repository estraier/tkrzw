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

#include <string>
#include <string_view>

#include <cinttypes>

#include "tkrzw_dbm.h"
#include "tkrzw_lib_common.h"

namespace tkrzw {

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
   * Constructor.
   * @param logger The logger to do actual logging.
   */
  explicit DBMUpdateLoggerSecondShard(DBM::UpdateLogger* logger) : logger_(logger) {}

  /**
   * Writes a log for modifying an existing record or adding a new record.
   * @param key The key of the record.
   * @param value The new value of the record.
   * @return The result status.
   */
  Status WriteSet(std::string_view key, std::string_view value) override {
    return logger_->WriteSet(key, value);
  }

  /**
   * Writes a log for removing an existing record.
   * @param key The key of the record.
   * @return The result status.
   */
  Status WriteRemove(std::string_view key) override {
    return logger_->WriteRemove(key);
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
  DBM::UpdateLogger* logger_;
};

}  // namespace tkrzw

#endif  // _TKRZW_DBM_ULOG_H

// END OF FILE
