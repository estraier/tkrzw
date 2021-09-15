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

#include "tkrzw_sys_config.h"

#include "tkrzw_dbm.h"
#include "tkrzw_dbm_ulog.h"
#include "tkrzw_file_util.h"
#include "tkrzw_hash_util.h"
#include "tkrzw_lib_common.h"
#include "tkrzw_str_util.h"
#include "tkrzw_time_util.h"

namespace tkrzw {


DBMUpdateLoggerStrDeque::DBMUpdateLoggerStrDeque(const std::string& delim) : delim_(delim) {}

Status DBMUpdateLoggerStrDeque::WriteSet(std::string_view key, std::string_view value) {
  std::lock_guard lock(mutex_);
  logs_.emplace_back(StrCat("SET", delim_, key, delim_, value));
  return Status(Status::SUCCESS);
}

Status DBMUpdateLoggerStrDeque::WriteRemove(std::string_view key) {
  std::lock_guard lock(mutex_);
  logs_.emplace_back(StrCat("REMOVE", delim_, key));
  return Status(Status::SUCCESS);
}

Status DBMUpdateLoggerStrDeque::WriteClear() {
  std::lock_guard lock(mutex_);
  logs_.emplace_back("CLEAR");
  return Status(Status::SUCCESS);
}

int64_t DBMUpdateLoggerStrDeque::GetSize() {
  std::lock_guard lock(mutex_);
  return logs_.size();
}

bool DBMUpdateLoggerStrDeque::PopFront(std::string* text) {
  std::lock_guard lock(mutex_);
  if (logs_.empty()) {
    return false;
  }
  if (text != nullptr) {
    *text = logs_.front();
  }
  logs_.pop_front();
  return true;
}

bool DBMUpdateLoggerStrDeque::PopBack(std::string* text) {
  std::lock_guard lock(mutex_);
  if (logs_.empty()) {
    return false;
  }
  if (text != nullptr) {
    *text = logs_.back();
  }
  logs_.pop_back();
  return true;
}

void DBMUpdateLoggerStrDeque::Clear() {
  std::lock_guard lock(mutex_);
  logs_.clear();
}

DBMUpdateLoggerDBM::DBMUpdateLoggerDBM(DBM* dbm) : dbm_(dbm) {}

Status DBMUpdateLoggerDBM::WriteSet(std::string_view key, std::string_view value) {
  return dbm_->Set(key, value);
}

Status DBMUpdateLoggerDBM::WriteRemove(std::string_view key) {
  const Status status = dbm_->Remove(key);
  if (status != Status::SUCCESS && status != Status::NOT_FOUND_ERROR) {
    return status;
  }
  return Status(Status::SUCCESS);
}

Status DBMUpdateLoggerDBM::WriteClear() {
  return dbm_->Clear();
}

}  // namespace tkrzw

// END OF FILE
