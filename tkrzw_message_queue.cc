/*************************************************************************************************
 * Message queue on the file stream
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

#include "tkrzw_file_util.h"
#include "tkrzw_hash_util.h"
#include "tkrzw_lib_common.h"
#include "tkrzw_message_queue.h"
#include "tkrzw_str_util.h"
#include "tkrzw_thread_util.h"
#include "tkrzw_time_util.h"

namespace tkrzw {

static constexpr size_t WRITE_BUFFER_SIZE = 4096;
static constexpr uint32_t MAGIC_DATA = 0xFF;


class MessageQueueImpl final {
  friend class MessageQueueReaderImpl;
  typedef std::list<MessageQueueReaderImpl*> ReaderList;
 public:
  MessageQueueImpl();
  ~MessageQueueImpl();
  Status Open(
      const std::string& prefix, int64_t max_file_size, double timestamp, bool sync_hard);
  Status Close();
  Status Write(double timestamp, std::string_view message);


 private:
  std::string MakeDatePath();

  std::string prefix_;
  int64_t max_file_size_;
  bool sync_hard_;
  std::fstream file_;
  int64_t file_size_;
  uint64_t timestamp_;


  ReaderList readers_;


  SpinSharedMutex mutex_;
};

class MessageQueueReaderImpl final {
 public:

  MessageQueueReaderImpl(MessageQueueImpl* queue, double min_timestamp);
  ~MessageQueueReaderImpl();

 private:
  MessageQueueImpl* queue_;
  uint64_t min_timestamp_;
};

MessageQueueImpl::MessageQueueImpl()
    : prefix_(), max_file_size_(0), sync_hard_(false),
      file_(), file_size_(), timestamp_(0), readers_(), mutex_() {}

MessageQueueImpl::~MessageQueueImpl() {
  if (file_.is_open()) {
    Close();
  }
}

Status MessageQueueImpl::Open(
    const std::string& prefix, int64_t max_file_size, double timestamp, bool sync_hard) {
  std::lock_guard lock(mutex_);
  std::vector<std::string> file_paths;
  Status status = MessageQueue::FindFiles(prefix, &file_paths);
  if (status != Status::SUCCESS) {
    return status;
  }
  prefix_ = prefix;
  max_file_size_ = max_file_size;
  timestamp_ = timestamp * 100;
  sync_hard_ = sync_hard;

  std::string last_file_path;
  if (file_paths.empty()) {
    last_file_path = MakeDatePath();
  } else {
    last_file_path = file_paths.back();
  }
  const std::ios_base::openmode mode =
      std::ios_base::out | std::ios_base::binary | std::ios_base::app | std::ios_base::ate;
  file_.open(last_file_path, mode);
  if (!file_.good()) {
    return Status(Status::SYSTEM_ERROR, "open failed");
  }
  file_size_ = file_.tellp();
  if (!file_.good() || file_size_ < 0) {
    return Status(Status::SYSTEM_ERROR, "tellp failed");
  }
  return Status(Status::SUCCESS);
}

Status MessageQueueImpl::Close() {
  std::lock_guard lock(mutex_);
  file_.close();
  if (!file_.good()) {
    return Status(Status::SYSTEM_ERROR, "close failed");
  }


  // Notification here.

  return Status(Status::SUCCESS);
}

Status MessageQueueImpl::Write(double timestamp, std::string_view message) {
  std::lock_guard lock(mutex_);
  timestamp_ = std::max(static_cast<uint64_t>(timestamp * 100), timestamp_);

  if (file_size_ >= max_file_size_) {
    file_.close();
    if (!file_.good()) {
      return Status(Status::SYSTEM_ERROR, "close failed");
    }
    const std::string new_file_path = MakeDatePath();
    const std::ios_base::openmode mode =
        std::ios_base::out | std::ios_base::binary | std::ios_base::trunc;
    file_.open(new_file_path, mode);
    if (!file_.good()) {
      return Status(Status::SYSTEM_ERROR, "open failed");
    }
    file_size_ = 0;
  }

  const size_t est_size = 11 + message.size();
  // 1 + 5 + 4 + 1


  char stack[WRITE_BUFFER_SIZE];
  char* write_buf = est_size > sizeof(stack) ? static_cast<char*>(xmalloc(est_size)) : stack;
  char* wp = write_buf;
  *(wp++) = MAGIC_DATA;
  WriteFixNum(wp, timestamp_, 5);
  wp += 5;

  WriteFixNum(wp, message.size(), 4);
  wp += 4;

  std::memcpy(wp, message.data(), message.size());
  wp += message.size();
  const uint32_t checksum = HashChecksum8(wp, wp - write_buf) + 1;
  *(wp++) = checksum;

  const int64_t act_size = wp - write_buf;
  if (est_size != act_size) {
    abort();
  }

  Status status(Status::SUCCESS);
  file_.write(write_buf, est_size);
  file_size_ += est_size;

  if (!file_.good()) {
    status = Status(Status::SYSTEM_ERROR, "write failed");
  }
  if (write_buf != stack) {
    xfree(write_buf);
  }

  // Notification here.


  return status;
}

std::string MessageQueueImpl::MakeDatePath() {
  struct std::tm lts;
  GetLocalCalendar(timestamp_ / 100, &lts);
  lts.tm_year += 1900;
  lts.tm_mon += 1;
  const std::string base_path = SPrintF("%s.%04d%02d%02d%02d%02d%02d", prefix_.c_str(),
                                        lts.tm_year, lts.tm_mon, lts.tm_mday,
                                        lts.tm_hour, lts.tm_min, lts.tm_sec);
  if (!PathIsFile(base_path)) {
    return base_path;
  }
  for (int32_t i = 1; i <= 99999; i++) {
    const std::string extra_path = base_path + SPrintF("-%05d", i);
    if (!PathIsFile(extra_path)) {
      return extra_path;
    }
  }
  return base_path + "-xxxxx";
}


MessageQueueReaderImpl::MessageQueueReaderImpl(MessageQueueImpl* queue, double min_timestamp)
    : queue_(queue), min_timestamp_(min_timestamp * 100) {

    /*
      , bucket_index_(-1), keys_() {
  std::lock_guard<SpinSharedMutex> lock(dbm_->mutex_);
  dbm_->iterators_.emplace_back(this);
  */
}

MessageQueueReaderImpl::~MessageQueueReaderImpl() {
  /*
  if (dbm_ != nullptr) {
    std::lock_guard<SpinSharedMutex> lock(dbm_->mutex_);
    dbm_->iterators_.remove(this);
  }
  */
}





MessageQueue::MessageQueue() {
  impl_ = new MessageQueueImpl;
}

MessageQueue::~MessageQueue() {
  delete impl_;
}

Status MessageQueue::Open(
    const std::string& prefix, int64_t max_file_size, double timestamp, bool sync_hard) {
  return impl_->Open(prefix, max_file_size, timestamp, sync_hard);
}

Status MessageQueue::Close() {
  return impl_->Close();
}

Status MessageQueue::Write(double timestamp, std::string_view message) {
  return impl_->Write(timestamp, message);
}

std::unique_ptr<MessageQueue::Reader> MessageQueue::MakeReader(double min_timestamp) {
  std::unique_ptr<MessageQueue::Reader> reader(new MessageQueue::Reader(impl_, min_timestamp));
  return reader;
}

Status MessageQueue::FindFiles(const std::string& prefix, std::vector<std::string>* paths) {
  assert(paths != nullptr);
  paths->clear();
  const std::string dir_path = PathToDirectoryName(prefix);
  if (dir_path.empty()) {
    return Status(Status::INVALID_ARGUMENT_ERROR, "empty directory name");
  }
  const std::string prefix_base = PathToBaseName(prefix);
  if (prefix_base.empty()) {
    return Status(Status::INVALID_ARGUMENT_ERROR, "empty file name");
  }
  std::vector<std::string> children;
  Status status = ReadDirectory(dir_path, &children);
  if (status != Status::SUCCESS) {
    return status;
  }
  for (const auto& child : children) {
    if (StrBeginsWith(child, prefix_base)) {
      const std::string suffix = child.substr(prefix_base.size());
      if (StrSearchRegex(suffix, "^\\.\\d{14}($|-)") >= 0) {
        paths->emplace_back(JoinPath(dir_path, child));
      }
    }
  }
  std::sort(paths->begin(), paths->end());
  return Status(Status::SUCCESS);
}



MessageQueue::Reader::Reader(MessageQueueImpl* queue_impl, double min_timestamp) {
  impl_ = new MessageQueueReaderImpl(queue_impl, min_timestamp);
}

MessageQueue::Reader::~Reader() {
  delete impl_;
}

}  // namespace tkrzw

// END OF FILE
