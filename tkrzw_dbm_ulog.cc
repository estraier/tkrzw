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
#include "tkrzw_file_mmap.h"
#include "tkrzw_file_util.h"
#include "tkrzw_hash_util.h"
#include "tkrzw_lib_common.h"
#include "tkrzw_message_queue.h"
#include "tkrzw_str_util.h"
#include "tkrzw_time_util.h"

namespace tkrzw {

static constexpr size_t WRITE_BUFFER_SIZE = 4096;
static constexpr uint8_t OP_MAGIC_SET = 0xA1;
static constexpr uint8_t OP_MAGIC_REMOVE = 0xA2;
static constexpr uint8_t OP_MAGIC_CLEAR = 0xA3;

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

Status DBMUpdateLoggerDBM::Synchronize(bool hard) {
  return dbm_->Synchronize(hard);
}

thread_local int32_t DBMUpdateLoggerMQ::thread_local_server_id_ = -1;

DBMUpdateLoggerMQ::DBMUpdateLoggerMQ(
    MessageQueue* mq, int32_t server_id, int32_t dbm_index, int64_t fixed_timestamp)
    : mq_(mq), server_id_(std::max(server_id, 0)), dbm_index_(std::max(dbm_index, 0)),
      fixed_timestamp_(fixed_timestamp) {}

Status DBMUpdateLoggerMQ::WriteSet(std::string_view key, std::string_view value) {
  int32_t rec_server_id = server_id_;
  if (thread_local_server_id_ >= 0) {
    rec_server_id = thread_local_server_id_;
  } else if (thread_local_server_id_ == INT32MIN) {
    return Status(Status::SUCCESS);
  }
  const int64_t timestamp = fixed_timestamp_ < 0 ? GetWallTime() * 1000 : fixed_timestamp_;
  char stack[WRITE_BUFFER_SIZE];
  const size_t est_size = 20 + key.size() + value.size();
  char* write_buf = est_size > sizeof(stack) ? static_cast<char*>(xmalloc(est_size)) : stack;
  char* wp = write_buf;
  *(wp++) = OP_MAGIC_SET;
  wp += WriteVarNum(wp, rec_server_id);
  wp += WriteVarNum(wp, dbm_index_);
  wp += WriteVarNum(wp, key.size());
  wp += WriteVarNum(wp, value.size());
  std::memcpy(wp, key.data(), key.size());
  wp += key.size();
  std::memcpy(wp, value.data(), value.size());
  wp += value.size();
  const Status status = mq_->Write(timestamp, std::string_view(write_buf, wp - write_buf));
  if (write_buf != stack) {
    xfree(write_buf);
  }
  return status;
}

Status DBMUpdateLoggerMQ::WriteRemove(std::string_view key) {
  int32_t rec_server_id = server_id_;
  if (thread_local_server_id_ >= 0) {
    rec_server_id = thread_local_server_id_;
  } else if (thread_local_server_id_ == INT32MIN) {
    return Status(Status::SUCCESS);
  }
  const int64_t timestamp = fixed_timestamp_ < 0 ? GetWallTime() * 1000 : fixed_timestamp_;
  char stack[WRITE_BUFFER_SIZE];
  const size_t est_size = 20 + key.size();
  char* write_buf = est_size > sizeof(stack) ? static_cast<char*>(xmalloc(est_size)) : stack;
  char* wp = write_buf;
  *(wp++) = OP_MAGIC_REMOVE;
  wp += WriteVarNum(wp, rec_server_id);
  wp += WriteVarNum(wp, dbm_index_);
  wp += WriteVarNum(wp, key.size());
  std::memcpy(wp, key.data(), key.size());
  wp += key.size();
  const Status status = mq_->Write(timestamp, std::string_view(write_buf, wp - write_buf));
  if (write_buf != stack) {
    xfree(write_buf);
  }
  return status;
}

Status DBMUpdateLoggerMQ::WriteClear() {
  int32_t rec_server_id = server_id_;
  if (thread_local_server_id_ >= 0) {
    rec_server_id = thread_local_server_id_;
  } else if (thread_local_server_id_ == INT32MIN) {
    return Status(Status::SUCCESS);
  }
  const int64_t timestamp = fixed_timestamp_ < 0 ? GetWallTime() * 1000 : fixed_timestamp_;
  char stack[WRITE_BUFFER_SIZE];
  char* write_buf = stack;
  char* wp = write_buf;
  *(wp++) = OP_MAGIC_CLEAR;
  wp += WriteVarNum(wp, rec_server_id);
  wp += WriteVarNum(wp, dbm_index_);
  return mq_->Write(timestamp, std::string_view(write_buf, wp - write_buf));
}

Status DBMUpdateLoggerMQ::Synchronize(bool hard) {
  return mq_->Synchronize(hard);
}

void DBMUpdateLoggerMQ::OverwriteThreadServerID(int32_t server_id) {
  thread_local_server_id_ = server_id;
}

Status DBMUpdateLoggerMQ::ParseUpdateLog(std::string_view message, UpdateLog* op) {
  assert(op != nullptr);
  op->op_type = OP_VOID;
  op->server_id = 0;
  op->dbm_index = 0;
  op->key = std::string_view();
  op->value = std::string_view();
  const char* rp = message.data();
  int32_t record_size = message.size();
  if (record_size < 3) {
    return Status(Status::BROKEN_DATA_ERROR, "too short message");
  }
  const uint32_t magic = *(uint8_t*)rp;
  rp++;
  record_size--;
  uint64_t num = 0;
  int32_t step = ReadVarNum(rp, record_size, &num);
  if (step < 1) {
    return Status(Status::BROKEN_DATA_ERROR, "invalid server ID");
  }
  op->server_id = num;
  rp += step;
  record_size -= step;
  if (record_size < 1) {
    return Status(Status::BROKEN_DATA_ERROR, "no DBM index");
  }
  step = ReadVarNum(rp, record_size, &num);
  if (step < 1) {
    return Status(Status::BROKEN_DATA_ERROR, "invalid DBM index");
  }
  op->dbm_index = num;
  rp += step;
  record_size -= step;
  switch (magic) {
    case OP_MAGIC_SET: {
      if (record_size < 1) {
        return Status(Status::BROKEN_DATA_ERROR, "no key");
      }
      step = ReadVarNum(rp, record_size, &num);
      if (step < 1) {
        return Status(Status::BROKEN_DATA_ERROR, "invalid key size");
      }
      const int32_t key_size = num;
      rp += step;
      record_size -= step;
      if (record_size < 1) {
        return Status(Status::BROKEN_DATA_ERROR, "no value");
      }
      step = ReadVarNum(rp, record_size, &num);
      if (step < 1) {
        return Status(Status::BROKEN_DATA_ERROR, "invalid value size");
      }
      const int32_t value_size = num;
      rp += step;
      record_size -= step;
      if (key_size + value_size != record_size) {
        return Status(Status::BROKEN_DATA_ERROR, "inconsistent data size");
      }
      op->op_type = OP_SET;
      op->key = std::string_view(rp, key_size);
      op->value = std::string_view(rp + key_size, value_size);
      return Status(Status::SUCCESS);
    }
    case OP_MAGIC_REMOVE: {
      if (record_size < 1) {
        return Status(Status::BROKEN_DATA_ERROR, "no key");
      }
      step = ReadVarNum(rp, record_size, &num);
      if (step < 1) {
        return Status(Status::BROKEN_DATA_ERROR, "invalid key size");
      }
      const int32_t key_size = num;
      rp += step;
      record_size -= step;
      if (key_size != record_size) {
        return Status(Status::BROKEN_DATA_ERROR, "inconsistent data size");
      }
      op->op_type = OP_REMOVE;
      op->key = std::string_view(rp, key_size);
      return Status(Status::SUCCESS);
    }
    case OP_MAGIC_CLEAR: {
      op->op_type = OP_CLEAR;
      return Status(Status::SUCCESS);
    }
    default:
      break;
  }
  return Status(Status::BROKEN_DATA_ERROR, "invalid opertion magic data");
}

Status DBMUpdateLoggerMQ::ApplyUpdateLog(
    DBM* dbm, std::string_view message, int32_t server_id, int32_t dbm_index) {
  assert(dbm != nullptr);
  UpdateLog op;
  Status status = ParseUpdateLog(message, &op);
  if (status != Status::SUCCESS) {
    return status;
  }
  if (server_id < 0) {
    if (op.server_id == -server_id) {
      return Status(Status::INFEASIBLE_ERROR);
    }
  } else {
    if (op.server_id != server_id) {
      return Status(Status::INFEASIBLE_ERROR);
    }
  }
  if (dbm_index < 0) {
    if (op.dbm_index == -dbm_index) {
      return Status(Status::INFEASIBLE_ERROR);
    }
  } else {
    if (op.dbm_index != dbm_index) {
      return Status(Status::INFEASIBLE_ERROR);
    }
  }
  switch (op.op_type) {
    case OP_SET:
      status = dbm->Set(op.key, op.value);
      break;
    case OP_REMOVE:
      status = dbm->Remove(op.key);
      break;
    case OP_CLEAR:
      status = dbm->Clear();
      break;
    default:
      break;
  }
  return Status(Status::SUCCESS);
}

Status DBMUpdateLoggerMQ::ApplyUpdateLogFromFiles(
      DBM* dbm, const std::string& prefix, double min_timestamp,
      int32_t server_id, int32_t dbm_index) {
  std::vector<std::string> paths;
  Status status = tkrzw::MessageQueue::FindFiles(prefix, &paths);
  if (status != Status::SUCCESS) {
    return status;
  }
  for (const auto& path : paths) {
    int64_t file_id = 0;
    int64_t timestamp = 0;
    int64_t file_size = 0;
    status = MessageQueue::ReadFileMetadata(path, &file_id, &timestamp, &file_size);
    if (status != Status::SUCCESS) {
      return status;
    }
    if (timestamp < min_timestamp) {
      continue;
    }
    tkrzw::MemoryMapParallelFile file;
    status = file.Open(path, false);
    if (status != Status::SUCCESS) {
      return status;
    }
    file_size = std::min(file_size, file.GetSizeSimple());
    int64_t file_offset = 0;
    std::string message;
    while (file_offset < file_size) {
      status = tkrzw::MessageQueue::ReadNextMessage(
          &file, &file_offset, &timestamp, &message);
      if (status != Status::SUCCESS) {
        if (status == Status::CANCELED_ERROR) {
          continue;
        }
        return status;
      }
      if (timestamp < min_timestamp) {
        continue;
      }
      status = ApplyUpdateLog(dbm, message, server_id, dbm_index);
      if (status != Status::SUCCESS && status != Status::INFEASIBLE_ERROR) {
        return status;
      }
    }
    status = file.Close();
    if (status != Status::SUCCESS) {
      return status;
    }
  }
  return Status(Status::SUCCESS);
}

}  // namespace tkrzw

// END OF FILE
