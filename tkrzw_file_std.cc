/*************************************************************************************************
 * File implementations by the C++ standard file stream
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

#include "tkrzw_file.h"
#include "tkrzw_file_std.h"
#include "tkrzw_file_util.h"
#include "tkrzw_lib_common.h"
#include "tkrzw_str_util.h"
#include "tkrzw_sys_config.h"
#include "tkrzw_thread_util.h"

namespace tkrzw {

class StdFileImpl final {
 public:
  StdFileImpl();
  ~StdFileImpl();
  Status Open(const std::string& path, bool writable, int32_t options);
  Status Close();
  Status Read(int64_t off, void* buf, size_t size);
  Status Write(int64_t off, const void* buf, size_t size);
  Status Append(const void* buf, size_t size, int64_t* off);
  Status Expand(size_t inc_size, int64_t* old_size);
  Status Truncate(int64_t size);
  Status Synchronize(bool hard);
  Status GetSize(int64_t* size);
  Status SetAllocationStrategy(int64_t init_size, double inc_factor);

 private:
  std::unique_ptr<std::fstream> file_;
  std::string path_;
  bool writable_;
  int32_t open_options_;
  int64_t file_size_;
  std::mutex mutex_;
};

StdFileImpl::StdFileImpl()
    : file_(), writable_(false), open_options_(0), file_size_(0), mutex_() {}

StdFileImpl::~StdFileImpl() {
  if (file_ != nullptr) {
    Close();
  }
}

Status StdFileImpl::Open(const std::string& path, bool writable, int32_t options) {
  std::lock_guard<std::mutex> lock(mutex_);
  if (file_ != nullptr) {
    return Status(Status::PRECONDITION_ERROR, "opened file");
  }
  std::ios_base::openmode mode = std::ios_base::in | std::ios_base::binary;
  if (writable) {
    mode |= std::ios_base::out;
    if (!PathIsFile(path)) {
      if (options & File::OPEN_NO_CREATE) {
        return Status(Status::NOT_FOUND_ERROR, "no create");
      }
      mode |= std::ios_base::trunc;
    }
    if (options & File::OPEN_TRUNCATE) {
      mode |= std::ios_base::trunc;
    }
  }
  file_ = std::make_unique<std::fstream>();
  file_->rdbuf()->pubsetbuf(nullptr, 0);
  file_->open(path, mode);
  if (!file_->good()) {
    file_.reset(nullptr);
    return Status(Status::SYSTEM_ERROR, "open failed");
  }
  file_->rdbuf()->pubsetbuf(nullptr, 0);
  file_->clear();
  int64_t size = 0;
  if (writable) {
    file_->seekp(0, std::ios_base::end);
    if (!file_->good()) {
      file_.reset(nullptr);
      return Status(Status::SYSTEM_ERROR, "seekp failed");
    }
    size = file_->tellp();
    if (!file_->good() || size < 0) {
      file_.reset(nullptr);
      return Status(Status::SYSTEM_ERROR, "tellp failed");
    }
  } else {
    file_->seekg(0, std::ios_base::end);
    if (!file_->good() || size < 0) {
      file_.reset(nullptr);
      return Status(Status::SYSTEM_ERROR, "seekg failed");
    }
    size = file_->tellg();
    if (!file_->good() || size < 0) {
      file_.reset(nullptr);
      return Status(Status::SYSTEM_ERROR, "tellg failed");
    }
  }
  path_ = path;
  file_size_ = size;
  return Status(Status::SUCCESS);
}

Status StdFileImpl::Close() {
  std::lock_guard<std::mutex> lock(mutex_);
  if (file_ == nullptr) {
    return Status(Status::PRECONDITION_ERROR, "not opened file");
  }
  file_->clear();
  file_->close();
  Status status(Status::SUCCESS);
  if (!file_->good()) {
    status.Set(Status::SYSTEM_ERROR, "close failed");
  }
  file_.reset(nullptr);
  return status;
}

Status StdFileImpl::Read(int64_t off, void* buf, size_t size) {
  std::lock_guard<std::mutex> lock(mutex_);
  if (file_ == nullptr) {
    return Status(Status::PRECONDITION_ERROR, "not opened file");
  }
  if (static_cast<int64_t>(off + size) > file_size_) {
    return Status(Status::INFEASIBLE_ERROR, "excessive size");
  }
  file_->clear();
  file_->seekg(off);
  if (!file_->good()) {
    return Status(Status::SYSTEM_ERROR, "seekg failed");
  }
  file_->read(static_cast<char*>(buf), size);
  if (!file_->good()) {
    return Status(Status::SYSTEM_ERROR, "read failed");
  }
  return Status(Status::SUCCESS);
}

Status StdFileImpl::Write(int64_t off, const void* buf, size_t size) {
  std::lock_guard<std::mutex> lock(mutex_);
  if (file_ == nullptr) {
    return Status(Status::PRECONDITION_ERROR, "not opened file");
  }
  file_->clear();
  file_->seekp(off);
  if (!file_->good()) {
    return Status(Status::SYSTEM_ERROR, "seekp failed");
  }
  file_->write(static_cast<const char*>(buf), size);
  if (!file_->good()) {
    return Status(Status::SYSTEM_ERROR, "write failed");
  }
  file_size_ = std::max<int64_t>(file_size_, off + size);
  return Status(Status::SUCCESS);
}

Status StdFileImpl::Append(const void* buf, size_t size, int64_t* off) {
  std::lock_guard<std::mutex> lock(mutex_);
  if (file_ == nullptr) {
    return Status(Status::PRECONDITION_ERROR, "not opened file");
  }
  file_->clear();
  file_->seekp(file_size_);
  if (!file_->good()) {
    return Status(Status::SYSTEM_ERROR, "seekp failed");
  }
  file_->write(static_cast<const char*>(buf), size);
  if (!file_->good()) {
    return Status(Status::SYSTEM_ERROR, "write failed");
  }
  if (off != nullptr) {
    *off = file_size_;
  }
  file_size_ += size;
  return Status(Status::SUCCESS);
}

Status StdFileImpl::Expand(size_t inc_size, int64_t* old_size) {
  std::lock_guard<std::mutex> lock(mutex_);
  if (file_ == nullptr) {
    return Status(Status::PRECONDITION_ERROR, "not opened file");
  }
  if (inc_size == 0) {
    return Status(Status::SUCCESS);
  }
  file_->clear();
  file_->seekp(file_size_ + inc_size - 1);
  if (!file_->good()) {
    return Status(Status::SYSTEM_ERROR, "seekp failed");
  }
  char buf[1];
  buf[0] = 0;
  file_->write(buf, 1);
  if (!file_->good()) {
    return Status(Status::SYSTEM_ERROR, "write failed");
  }
  if (old_size != nullptr) {
    *old_size = file_size_;
  }
  file_size_ += inc_size;
  return Status(Status::SUCCESS);
}

Status StdFileImpl::Truncate(int64_t size) {
  std::lock_guard<std::mutex> lock(mutex_);
  if (file_ == nullptr) {
    return Status(Status::PRECONDITION_ERROR, "not opened file");
  }
  std::filesystem::resize_file(path_, size);
  file_->seekp(0, std::ios_base::end);
  if (!file_->good()) {
    return Status(Status::SYSTEM_ERROR, "seekp failed");
  }
  const int64_t new_size = file_->tellp();
  if (!file_->good()) {
    return Status(Status::SYSTEM_ERROR, "tellp failed");
  }
  if (new_size != size) {
    return Status(Status::SYSTEM_ERROR, "resize_file failed");
  }
  file_size_ = size;
  return Status(Status::SUCCESS);
}

Status StdFileImpl::Synchronize(bool hard) {
  std::lock_guard<std::mutex> lock(mutex_);
  if (file_ == nullptr) {
    return Status(Status::PRECONDITION_ERROR, "not opened file");
  }
  return Status(Status::SUCCESS);
}

Status StdFileImpl::GetSize(int64_t* size) {
  std::lock_guard<std::mutex> lock(mutex_);
  if (file_ == nullptr) {
    return Status(Status::PRECONDITION_ERROR, "not opened file");
  }
  *size = file_size_;
  return Status(Status::SUCCESS);
}

Status StdFileImpl::SetAllocationStrategy(int64_t init_size, double inc_factor) {
  return Status(Status::SUCCESS);
}

StdFile::StdFile() {
  impl_ = new StdFileImpl();
}

StdFile::~StdFile() {
  delete impl_;
}

Status StdFile::Open(const std::string& path, bool writable, int32_t options) {
  return impl_->Open(path, writable, options);
}

Status StdFile::Close() {
  return impl_->Close();
}

Status StdFile::Read(int64_t off, void* buf, size_t size) {
  assert(off >= 0 && buf != nullptr);
  return impl_->Read(off, buf, size);
}

Status StdFile::Write(int64_t off, const void* buf, size_t size) {
  assert(off >= 0 && buf != nullptr && size <= MAX_MEMORY_SIZE);
  return impl_->Write(off, buf, size);
}

Status StdFile::Append(const void* buf, size_t size, int64_t* off) {
  assert(buf != nullptr && size <= MAX_MEMORY_SIZE);
  return impl_->Append(buf, size, off);
}

Status StdFile::Expand(size_t inc_size, int64_t* old_size) {
  assert(inc_size <= MAX_MEMORY_SIZE);
  return impl_->Expand(inc_size, old_size);
}

Status StdFile::Truncate(int64_t size) {
  assert(size >= 0 && size <= MAX_MEMORY_SIZE);
  return impl_->Truncate(size);
}

Status StdFile::Synchronize(bool hard) {
  return impl_->Synchronize(hard);
}

Status StdFile::GetSize(int64_t* size) {
  assert(size != nullptr);
  return impl_->GetSize(size);
}

Status StdFile::SetAllocationStrategy(int64_t init_size, double inc_factor) {
  assert(init_size > 0 && inc_factor > 0);
  return impl_->SetAllocationStrategy(init_size, inc_factor);
}

}  // namespace tkrzw

// END OF FILE
