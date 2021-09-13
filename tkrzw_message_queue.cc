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

#include "tkrzw_file.h"
#include "tkrzw_file_mmap.h"
#include "tkrzw_file_pos.h"
#include "tkrzw_file_util.h"
#include "tkrzw_hash_util.h"
#include "tkrzw_lib_common.h"
#include "tkrzw_message_queue.h"
#include "tkrzw_str_util.h"
#include "tkrzw_thread_util.h"
#include "tkrzw_time_util.h"

namespace tkrzw {

static constexpr int32_t METADATA_SIZE = 32;
static constexpr char META_MAGIC_DATA[] = "TkrzwMQX\n";
static constexpr int32_t META_OFFSET_CYCLIC_MAGIC_FRONT = 9;
static constexpr int32_t META_OFFSET_FILE_ID = 10;
static constexpr int32_t META_OFFSET_TIMESTAMP = 16;
static constexpr int32_t META_OFFSET_FILE_SIZE = 22;
static constexpr int32_t META_OFFSET_CYCLIC_MAGIC_BACK = 31;
static constexpr size_t WRITE_BUFFER_SIZE = 10240;
static constexpr uint32_t RECORD_MAGIC_DATA = 0xFF;


class MessageQueueImpl final {
  friend class MessageQueueReaderImpl;
 public:
  MessageQueueImpl();
  ~MessageQueueImpl();
  Status Open(const std::string& prefix, int64_t max_file_size, int32_t options);
  Status Close();
  Status Write(int64_t timestamp, std::string_view message);
  static Status SaveMetadata(
      File* file, int32_t cyclic_magic, int64_t file_id,  int64_t timestamp);
  static Status LoadMetadata(
      File* file, int32_t* cyclic_magic, int64_t* file_id, int64_t* timestamp,
      int64_t* file_size);

 private:
  std::string MakeFilePath(int64_t file_id);
  Status Synchronize();
  Status WriteImpl(int64_t timestamp, std::string_view message);

  std::string prefix_;
  int64_t max_file_size_;
  bool sync_hard_;
  bool read_only_;
  std::unique_ptr<File> last_file_;
  int32_t cyclic_magic_;
  int64_t last_id_;
  int64_t timestamp_;
  std::mutex mutex_;
  std::condition_variable cond_;
};

class MessageQueueReaderImpl final {
 public:
  MessageQueueReaderImpl(MessageQueueImpl* queue, int64_t min_timestamp);
  ~MessageQueueReaderImpl();
  Status Read(double timeout, int64_t* timestamp, std::string* message);

 private:
  Status OpenNextFile();
  Status ReadNextMessage(int64_t* timestamp, std::string* message);

  MessageQueueImpl* queue_;
  int64_t min_timestamp_;
  std::unique_ptr<File> file_;
  int64_t file_id_;
};

MessageQueueImpl::MessageQueueImpl()
    : prefix_(), max_file_size_(0), sync_hard_(false), read_only_(false),
      last_file_(nullptr), cyclic_magic_(0), last_id_(0), timestamp_(0), mutex_(), cond_() {}

MessageQueueImpl::~MessageQueueImpl() {}

Status MessageQueueImpl::Open(
    const std::string& prefix, int64_t max_file_size, int32_t options) {
  std::lock_guard lock(mutex_);
  if (last_file_ != nullptr) {
    return Status(Status::PRECONDITION_ERROR, "opened message queue");
  }
  if ((options & MessageQueue::OPEN_TRUNCATE) && !(options & MessageQueue::OPEN_READ_ONLY)) {
    std::vector<std::string> file_paths;
    if (MessageQueue::FindFiles(prefix, &file_paths) == Status::SUCCESS) {
      for (const auto& path : file_paths) {
        RemoveFile(path);
      }
    }
  }
  std::vector<std::string> file_paths;
  Status status = MessageQueue::FindFiles(prefix, &file_paths);
  if (status != Status::SUCCESS) {
    return status;
  }
  prefix_ = prefix;
  max_file_size_ = max_file_size;
  sync_hard_ = options & MessageQueue::OPEN_SYNC_HARD;
  read_only_ = options & MessageQueue::OPEN_READ_ONLY;
  last_file_ = std::make_unique<PositionalParallelFile>();
  if (file_paths.empty()) {
    last_id_ = 0;
  } else {
    last_id_ = MessageQueue::GetFileID(file_paths.back());
  }
  const std::string last_path = MakeFilePath(last_id_);
  status = last_file_->Open(last_path, !read_only_);
  if (status != Status::SUCCESS) {
    last_file_.reset(nullptr);
    return status;
  }
  if (file_paths.empty()) {
    cyclic_magic_ = 0;
    timestamp_ = 0;
    status = Synchronize();
    if (status != Status::SUCCESS) {
      last_file_.reset(nullptr);
      return status;
    }
  }
  int64_t check_last_id = 0;
  int64_t check_file_size = 0;
  status = LoadMetadata(last_file_.get(), &cyclic_magic_, &check_last_id, &timestamp_,
                        &check_file_size);
  if (check_last_id != last_id_) {
    status |= Status(Status::BROKEN_DATA_ERROR, "inconsistent file ID");
  }
  if (cyclic_magic_ < 0) {
    status |= Status(Status::BROKEN_DATA_ERROR, "inconsistent cyclic magic data");
  }
  if (check_file_size != last_file_->GetSizeSimple()) {
    status |= Status(Status::BROKEN_DATA_ERROR, "inconsistent file size");
  }
  if (status != Status::SUCCESS) {
    last_file_.reset(nullptr);
    return status;
  }
  return Status(Status::SUCCESS);
}

Status MessageQueueImpl::Close() {
  std::lock_guard lock(mutex_);
  if (last_file_ == nullptr) {
    return Status(Status::PRECONDITION_ERROR, "not opened message queue");
  }
  Status status(Status::SUCCESS);
  status |= Synchronize();
  status |= last_file_->Close();
  last_file_.reset(nullptr);
  return status;
}

Status MessageQueueImpl::Write(int64_t timestamp, std::string_view message) {
  if (timestamp < 0) {
    timestamp = GetWallTime();
  } else if (timestamp >= ((1LL << 48) - 1)) {
    return Status(Status::INVALID_ARGUMENT_ERROR, "out-of-range timestamp");
  }
  {
    std::lock_guard lock(mutex_);
    if (last_file_ == nullptr) {
      return Status(Status::PRECONDITION_ERROR, "not opened message queue");
    }
    const Status status = WriteImpl(timestamp, message);
    if (status != Status::SUCCESS) {
      return status;
    }
  }
  cond_.notify_all();
  return Status(Status::SUCCESS);
}

Status MessageQueueImpl::SaveMetadata(
    File* file, int32_t cyclic_magic, int64_t file_id,  int64_t timestamp) {
  const int64_t file_size = std::max<int64_t>(file->GetSizeSimple(), METADATA_SIZE);
  char meta[METADATA_SIZE];
  std::memset(meta, 0, METADATA_SIZE);
  std::memcpy(meta, META_MAGIC_DATA, sizeof(META_MAGIC_DATA) - 1);
  WriteFixNum(meta + META_OFFSET_CYCLIC_MAGIC_FRONT, cyclic_magic, 1);
  WriteFixNum(meta + META_OFFSET_FILE_ID, file_id, 6);
  WriteFixNum(meta + META_OFFSET_TIMESTAMP, timestamp, 6);
  WriteFixNum(meta + META_OFFSET_FILE_SIZE, file_size, 6);
  WriteFixNum(meta + META_OFFSET_CYCLIC_MAGIC_BACK, cyclic_magic, 1);
  return file->Write(0, meta, METADATA_SIZE);
}

Status MessageQueueImpl::LoadMetadata(
      File* file, int32_t* cyclic_magic, int64_t* file_id, int64_t* timestamp,
      int64_t* file_size) {
  char meta[METADATA_SIZE];
  const Status status = file->Read(0, meta, METADATA_SIZE);
  if (status != Status::SUCCESS) {
    return status;
  }
  if (std::memcmp(meta, META_MAGIC_DATA, sizeof(META_MAGIC_DATA) - 1) != 0) {
    return Status(Status::BROKEN_DATA_ERROR, "bad magic data");
  }
  *cyclic_magic = ReadFixNum(meta + META_OFFSET_CYCLIC_MAGIC_FRONT, 1);
  *file_id = ReadFixNum(meta + META_OFFSET_FILE_ID, 6);
  *timestamp = ReadFixNum(meta + META_OFFSET_TIMESTAMP, 6);
  *file_size = ReadFixNum(meta + META_OFFSET_FILE_SIZE, 6);
  const int32_t cyclic_magic_back = ReadFixNum(meta + META_OFFSET_CYCLIC_MAGIC_BACK, 1);
  if (*cyclic_magic != cyclic_magic_back) {
    *cyclic_magic = -1;
  }
  return Status(Status::SUCCESS);
}

std::string MessageQueueImpl::MakeFilePath(int64_t file_id) {
  char numbuf[32];
  std::sprintf(numbuf, ".%010lld", (long long)file_id);
  return StrCat(prefix_, numbuf);
}

Status MessageQueueImpl::Synchronize() {
  cyclic_magic_ = cyclic_magic_ % 255 + 1;
  return SaveMetadata(last_file_.get(), cyclic_magic_, last_id_, timestamp_);
}

Status MessageQueueImpl::WriteImpl(int64_t timestamp, std::string_view message) {
  Status status(Status::SUCCESS);
  if (last_file_->GetSizeSimple() >= max_file_size_) {
    status = Synchronize();
    status |= last_file_->Close();
    if (status != Status::SUCCESS) {
      return status;
    }
    last_id_++;
    const std::string new_file_path = MakeFilePath(last_id_);
    status = last_file_->Open(new_file_path, true, MessageQueue::OPEN_TRUNCATE);
    status |= Synchronize();
    if (status != Status::SUCCESS) {
      return status;
    }
  }
  timestamp_ = std::max(timestamp, timestamp_);
  const size_t est_size = 12 + message.size();
  char stack[WRITE_BUFFER_SIZE];
  char* write_buf = est_size > sizeof(stack) ? static_cast<char*>(xmalloc(est_size)) : stack;
  char* wp = write_buf;
  *(wp++) = RECORD_MAGIC_DATA;
  WriteFixNum(wp, timestamp_, 6);
  wp += 6;
  WriteFixNum(wp, message.size(), 4);
  wp += 4;
  std::memcpy(wp, message.data(), message.size());
  wp += message.size();
  const uint32_t checksum = HashChecksum8(write_buf, wp - write_buf) + 1;
  *(wp++) = checksum;
  status = last_file_->Append(write_buf, est_size);
  if (write_buf != stack) {
    xfree(write_buf);
  }
  return status;
}




MessageQueueReaderImpl::MessageQueueReaderImpl(MessageQueueImpl* queue, int64_t min_timestamp)
    : queue_(queue), min_timestamp_(min_timestamp), file_(), file_id_(0) {}

MessageQueueReaderImpl::~MessageQueueReaderImpl() {}

Status MessageQueueReaderImpl::Read(double timeout, int64_t* timestamp, std::string* message) {
  return Status(Status::SUCCESS);
}

Status MessageQueueReaderImpl::OpenNextFile() {
  return Status(Status::SUCCESS);
}

Status MessageQueueReaderImpl::ReadNextMessage(int64_t* timestamp, std::string* message) {
  return Status(Status::SUCCESS);
}

MessageQueue::MessageQueue() {
  impl_ = new MessageQueueImpl;
}

MessageQueue::~MessageQueue() {
  delete impl_;
}

Status MessageQueue::Open(
    const std::string& prefix, int64_t max_file_size, int32_t options) {
  return impl_->Open(prefix, max_file_size, options);
}

Status MessageQueue::Close() {
  return impl_->Close();
}

Status MessageQueue::Write(int64_t timestamp, std::string_view message) {
  return impl_->Write(timestamp, message);
}

std::unique_ptr<MessageQueue::Reader> MessageQueue::MakeReader(int64_t min_timestamp) {
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
      if (StrSearchRegex(suffix, "^\\.\\d{10}") >= 0) {
        paths->emplace_back(JoinPath(dir_path, child));
      }
    }
  }
  std::sort(paths->begin(), paths->end());
  return Status(Status::SUCCESS);
}

uint64_t MessageQueue::GetFileID(const std::string& path) {
  const size_t pos = path.rfind('.');
  if (pos == std::string::npos) {
    return UINT64MAX;
  }
  return StrToInt(std::string_view(path).substr(pos + 1));
}

Status MessageQueue::ReadFileMetadata(
    const std::string& path, int64_t *file_id, int64_t* timestamp, int64_t* file_size) {
  assert(file_id != nullptr && timestamp != nullptr && file_size != nullptr);
  PositionalParallelFile file;
  Status status = file.Open(path, false);
  if (status != Status::SUCCESS) {
    return status;
  }
  int32_t cyclic_magic = 0;
  status |= MessageQueueImpl::LoadMetadata(&file, &cyclic_magic, file_id, timestamp, file_size);
  status |= file.Close();
  return status;
}

MessageQueue::Reader::Reader(MessageQueueImpl* queue_impl, uint64_t min_timestamp) {
  impl_ = new MessageQueueReaderImpl(queue_impl, min_timestamp);
}

MessageQueue::Reader::~Reader() {
  delete impl_;
}

Status MessageQueue::Reader::Read(double timeout, int64_t* timestamp, std::string* message) {
  assert(timestamp != nullptr && message != nullptr);
  return impl_->Read(timeout, timestamp, message);
}

}  // namespace tkrzw

// END OF FILE
