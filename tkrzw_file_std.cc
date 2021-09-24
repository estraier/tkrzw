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

#include "tkrzw_sys_config.h"

#include "tkrzw_file.h"
#include "tkrzw_file_std.h"
#include "tkrzw_file_util.h"
#include "tkrzw_lib_common.h"
#include "tkrzw_str_util.h"
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
  Status TruncateFakely(int64_t size);
  Status Synchronize(bool hard, int64_t off, int64_t size);
  Status GetSize(int64_t* size);
  Status SetAllocationStrategy(int64_t init_size, double inc_factor);
  Status CopyProperties(File* file);
  Status GetPath(std::string* path);
  Status Rename(const std::string& new_path);
  Status DisablePathOperations();
  int64_t Lock();
  int64_t Unlock();
  Status ReadInCriticalSection(int64_t off, void* buf, size_t size);
  Status WriteInCriticalSection(int64_t off, const void* buf, size_t size);
  bool IsOpen();

 private:
  Status OpenImpl(const std::string& path, bool writable, int32_t options);
  Status CloseImpl();
  Status ReadImpl(int64_t off, void* buf, size_t size);
  Status WriteImpl(int64_t off, const void* buf, size_t size);
  Status AppendImpl(const void* buf, size_t size, int64_t* off);
  Status ExpandImpl(size_t inc_size, int64_t* old_size);

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
  return OpenImpl(path, writable, options);
}

Status StdFileImpl::Close() {
  std::lock_guard<std::mutex> lock(mutex_);
  if (file_ == nullptr) {
    return Status(Status::PRECONDITION_ERROR, "not opened file");
  }
  return CloseImpl();
}

Status StdFileImpl::Read(int64_t off, void* buf, size_t size) {
  std::lock_guard<std::mutex> lock(mutex_);
  if (file_ == nullptr) {
    return Status(Status::PRECONDITION_ERROR, "not opened file");
  }
  return ReadImpl(off, buf, size);
}

Status StdFileImpl::Write(int64_t off, const void* buf, size_t size) {
  std::lock_guard<std::mutex> lock(mutex_);
  if (file_ == nullptr) {
    return Status(Status::PRECONDITION_ERROR, "not opened file");
  }
  if (!writable_) {
    return Status(Status::PRECONDITION_ERROR, "not writable file");
  }
  return WriteImpl(off, buf, size);
}

Status StdFileImpl::Append(const void* buf, size_t size, int64_t* off) {
  std::lock_guard<std::mutex> lock(mutex_);
  if (file_ == nullptr) {
    return Status(Status::PRECONDITION_ERROR, "not opened file");
  }
  if (!writable_) {
    return Status(Status::PRECONDITION_ERROR, "not writable file");
  }
  return AppendImpl(buf, size, off);
}

Status StdFileImpl::Expand(size_t inc_size, int64_t* old_size) {
  std::lock_guard<std::mutex> lock(mutex_);
  if (file_ == nullptr) {
    return Status(Status::PRECONDITION_ERROR, "not opened file");
  }
  if (!writable_) {
    return Status(Status::PRECONDITION_ERROR, "not writable file");
  }
  return ExpandImpl(inc_size, old_size);
}

Status StdFileImpl::Truncate(int64_t size) {
  std::lock_guard<std::mutex> lock(mutex_);
  if (file_ == nullptr) {
    return Status(Status::PRECONDITION_ERROR, "not opened file");
  }
  if (!writable_) {
    return Status(Status::PRECONDITION_ERROR, "not writable file");
  }
  if (size == file_size_) {
    return Status(Status::SUCCESS);
  }
  if (size > file_size_) {
    file_->clear();
    file_->seekp(size - 1);
    if (!file_->good()) {
      return Status(Status::SYSTEM_ERROR, "seekp failed");
    }
    file_->write("", 1);
    if (!file_->good()) {
      return Status(Status::SYSTEM_ERROR, "write failed");
    }
    file_size_ = size;
    return Status(Status::SUCCESS);
  }
  if (!PathIsFile(path_)) {
    return Status(Status::INFEASIBLE_ERROR, "missing file");
  }
  const std::string old_path = path_;
  const int32_t options = open_options_;
  Status status = CloseImpl();
  if (status != Status::SUCCESS) {
    return status;
  }
  status |= TruncateFile(old_path, size);
  status |= OpenImpl(old_path, true, options);
  return status;
}

Status StdFileImpl::TruncateFakely(int64_t size) {
  std::lock_guard<std::mutex> lock(mutex_);
  if (file_ == nullptr) {
    return Status(Status::PRECONDITION_ERROR, "not opened file");
  }
  if (size > file_size_) {
    return Status(Status::INFEASIBLE_ERROR, "unable to increase the file size");
  }
  file_size_ = size;
  return Status(Status::SUCCESS);
}

Status StdFileImpl::Synchronize(bool hard, int64_t off, int64_t size) {
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

Status StdFileImpl::CopyProperties(File* file) {
  return Status(Status::SUCCESS);
}

Status StdFileImpl::GetPath(std::string* path) {
  std::lock_guard<std::mutex> lock(mutex_);
  if (file_ == nullptr) {
    return Status(Status::PRECONDITION_ERROR, "not opened file");
  }
  if (path_.empty()) {
    return Status(Status::PRECONDITION_ERROR, "disabled path operatione");
  }
  *path = path_;
  return Status(Status::SUCCESS);
}

Status StdFileImpl::Rename(const std::string& new_path) {
  std::lock_guard<std::mutex> lock(mutex_);
  if (file_ == nullptr) {
    return Status(Status::PRECONDITION_ERROR, "not opened file");
  }
  if (path_.empty()) {
    return Status(Status::PRECONDITION_ERROR, "disabled path operatione");
  }
  const std::string old_path = path_;
  const bool writable = writable_;
  const int32_t options = open_options_;
  Status status = CloseImpl();
  if (status != Status::SUCCESS) {
    return status;
  }
  status = RenameFile(old_path, new_path);
  if (status != Status::SUCCESS) {
    OpenImpl(new_path, writable, options);
    return status;
  }
  return OpenImpl(new_path, writable, options);
}

Status StdFileImpl::DisablePathOperations() {
  std::lock_guard<std::mutex> lock(mutex_);
  if (file_ == nullptr) {
    return Status(Status::PRECONDITION_ERROR, "not opened file");
  }
  path_.clear();
  return Status(Status::SUCCESS);
}

int64_t StdFileImpl::Lock() {
  mutex_.lock();
  return file_ == nullptr ? -1 : file_size_;
}

int64_t StdFileImpl::Unlock() {
  const int64_t file_size = file_ == nullptr ? -1 : file_size_;
  mutex_.unlock();
  return file_size;
}

Status StdFileImpl::ReadInCriticalSection(int64_t off, void* buf, size_t size) {
  if (file_ == nullptr) {
    return Status(Status::PRECONDITION_ERROR, "not opened file");
  }
  return ReadImpl(off, buf, size);
}

Status StdFileImpl::WriteInCriticalSection(int64_t off, const void* buf, size_t size) {
  if (file_ == nullptr) {
    return Status(Status::PRECONDITION_ERROR, "not opened file");
  }
  if (!writable_) {
    return Status(Status::PRECONDITION_ERROR, "not writable file");
  }
  return WriteImpl(off, buf, size);
}

bool StdFileImpl::IsOpen() {
  std::lock_guard<std::mutex> lock(mutex_);
  return file_ != nullptr;
}

Status StdFileImpl::OpenImpl(const std::string& path, bool writable, int32_t options) {
  std::ios_base::openmode mode = std::ios_base::in | std::ios_base::binary;
  const bool has_existed = PathIsFile(path);
  if (writable) {
    mode |= std::ios_base::out;
    if (!has_existed) {
      if (options & File::OPEN_NO_CREATE) {
        return Status(Status::NOT_FOUND_ERROR, "no such file");
      }
      mode |= std::ios_base::trunc;
    }
    if (options & File::OPEN_TRUNCATE) {
      mode |= std::ios_base::trunc;
    }
  } else if (!has_existed) {
    return Status(Status::NOT_FOUND_ERROR, "no such file");
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
  writable_ = writable;
  open_options_ = options & ~File::OPEN_TRUNCATE;
  file_size_ = size;
  return Status(Status::SUCCESS);
}

Status StdFileImpl::CloseImpl() {
  Status status(Status::SUCCESS);
  file_->clear();
  file_->close();
  if (!file_->good()) {
    status |= Status(Status::SYSTEM_ERROR, "close failed");
  }
  if (writable_ && !path_.empty() && PathIsFile(path_)) {
    status |= TruncateFile(path_, file_size_);
  }
  file_.reset(nullptr);
  path_.clear();
  writable_ = false;
  open_options_ = 0;
  return status;
}

Status StdFileImpl::ReadImpl(int64_t off, void* buf, size_t size) {
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

Status StdFileImpl::WriteImpl(int64_t off, const void* buf, size_t size) {
  if (size == 0) {
    if (off > file_size_) {
      off--;
      buf = static_cast<const void*>("");
      size = 1;
    } else {
      return Status(Status::SUCCESS);
    }
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

Status StdFileImpl::AppendImpl(const void* buf, size_t size, int64_t* off) {
  if (size == 0) {
    if (off != nullptr) {
      *off = file_size_;
    }
    return Status(Status::SUCCESS);
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

Status StdFileImpl::ExpandImpl(size_t inc_size, int64_t* old_size) {
  if (inc_size == 0) {
    return Status(Status::SUCCESS);
  }
  file_->clear();
  file_->seekp(file_size_ + inc_size - 1);
  if (!file_->good()) {
    return Status(Status::SYSTEM_ERROR, "seekp failed");
  }
  file_->write("", 1);
  if (!file_->good()) {
    return Status(Status::SYSTEM_ERROR, "write failed");
  }
  if (old_size != nullptr) {
    *old_size = file_size_;
  }
  file_size_ += inc_size;
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

Status StdFile::TruncateFakely(int64_t size) {
  assert(size >= 0 && size <= MAX_MEMORY_SIZE);
  return impl_->TruncateFakely(size);
}

Status StdFile::Synchronize(bool hard, int64_t off, int64_t size) {
  assert(off >= 0 && size >= 0);
  return impl_->Synchronize(hard, off, size);
}

Status StdFile::GetSize(int64_t* size) {
  assert(size != nullptr);
  return impl_->GetSize(size);
}

Status StdFile::SetAllocationStrategy(int64_t init_size, double inc_factor) {
  assert(init_size >= 0 && inc_factor >= 0);
  return impl_->SetAllocationStrategy(init_size, inc_factor);
}

Status StdFile::CopyProperties(File* file) {
  assert(file != nullptr);
  return impl_->CopyProperties(file);
}

Status StdFile::GetPath(std::string* path) {
  assert(path != nullptr);
  return impl_->GetPath(path);
}

Status StdFile::Rename(const std::string& new_path) {
  return impl_->Rename(new_path);
}

Status StdFile::DisablePathOperations() {
  return impl_->DisablePathOperations();
}

int64_t StdFile::Lock() {
  return impl_->Lock();
}

int64_t StdFile::Unlock() {
  return impl_->Unlock();
}

Status StdFile::ReadInCriticalSection(int64_t off, void* buf, size_t size) {
  assert(off >= 0 && buf != nullptr);
  return impl_->ReadInCriticalSection(off, buf, size);
}

Status StdFile::WriteInCriticalSection(int64_t off, const void* buf, size_t size) {
  assert(off >= 0 && buf != nullptr && size <= MAX_MEMORY_SIZE);
  return impl_->WriteInCriticalSection(off, buf, size);
}

bool StdFile::IsOpen() const {
  return impl_->IsOpen();
}

}  // namespace tkrzw

// END OF FILE
