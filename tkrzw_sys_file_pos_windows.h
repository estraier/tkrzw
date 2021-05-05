/*************************************************************************************************
 * Implementations for positional access file on Windows
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

#ifndef _TKRZW_SYS_FILE_POS_WINDOWS_H
#define _TKRZW_SYS_FILE_POS_WINDOWS_H

#include "tkrzw_sys_config.h"

#include <memory>
#include <string>
#include <vector>

#include <cinttypes>

#include "tkrzw_file.h"
#include "tkrzw_file_pos.h"
#include "tkrzw_file_std.h"    // Remove me
#include "tkrzw_file_util.h"
#include "tkrzw_lib_common.h"
#include "tkrzw_str_util.h"
#include "tkrzw_sys_util_windows.h"
#include "tkrzw_thread_util.h"

namespace tkrzw {

class PositionalParallelFileImpl final {
 public:
  PositionalParallelFileImpl();
  ~PositionalParallelFileImpl();
  Status Open(const std::string& path, bool writable, int32_t options);
  Status Close();
  Status Read(int64_t off, void* buf, size_t size);
  Status Write(int64_t off, const void* buf, size_t size);
  Status Append(const void* buf, size_t size, int64_t* off);
  Status Truncate(int64_t size);
  Status Synchronize(bool hard);
  Status GetSize(int64_t* size);
  Status SetAllocationStrategy(int64_t init_size, double inc_factor);
  Status GetPath(std::string* path);
  Status Rename(const std::string& new_path);

 private:
  Status AllocateSpace(int64_t min_size);

  HANDLE file_handle_;
  std::string path_;
  std::atomic_int64_t file_size_;
  std::atomic_int64_t trunc_size_;
  bool writable_;
  int32_t open_options_;
  int64_t alloc_init_size_;
  double alloc_inc_factor_;
  std::mutex mutex_;
};

PositionalParallelFileImpl::PositionalParallelFileImpl()
    : file_handle_(nullptr), file_size_(0), trunc_size_(0),
      writable_(false), open_options_(0),
      alloc_init_size_(File::DEFAULT_ALLOC_INIT_SIZE),
      alloc_inc_factor_(File::DEFAULT_ALLOC_INC_FACTOR), mutex_() {}

PositionalParallelFileImpl::~PositionalParallelFileImpl() {
  if (file_handle_ != nullptr) {
    Close();
  }
}

Status PositionalParallelFileImpl::Open(const std::string& path, bool writable, int32_t options) {
  if (file_handle_ != nullptr) {
    return Status(Status::PRECONDITION_ERROR, "opened file");
  }

  // Opens the file.
  DWORD amode = GENERIC_READ;
  DWORD smode = FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE;
  DWORD cmode = OPEN_EXISTING;
  DWORD flags = FILE_FLAG_RANDOM_ACCESS;
  if (writable) {
    amode |= GENERIC_WRITE;
    if (options & File::OPEN_NO_CREATE) {
      if (options & File::OPEN_TRUNCATE) {
        cmode = TRUNCATE_EXISTING;
      }
    } else {
      cmode = OPEN_ALWAYS;
      if (options & File::OPEN_TRUNCATE) {
        cmode = CREATE_ALWAYS;
      }
    }
  }
  HANDLE file_handle = CreateFile(path.c_str(), amode, smode, nullptr, cmode, flags, nullptr);
  if (file_handle == nullptr || file_handle == INVALID_HANDLE_VALUE) {
    return GetSysErrorStatus("CreateFile", GetLastError());
  }

  // Locks the file.
  if (!(options & File::OPEN_NO_LOCK)) {
    DWORD lmode = writable ? LOCKFILE_EXCLUSIVE_LOCK : 0;
    if (options & File::OPEN_NO_WAIT) {
      lmode |= LOCKFILE_FAIL_IMMEDIATELY;
    }
    OVERLAPPED ol;
    ol.Offset = INT32MAX;
    ol.OffsetHigh = 0;
    ol.hEvent = 0;
    if (!LockFileEx(file_handle, lmode, 0, 1, 0, &ol)) {
      const Status status = GetSysErrorStatus("LockFileEx", GetLastError());
      CloseHandle(file_handle);
      return status;
    }
  }

  // Checks the file size and type.
  LARGE_INTEGER sbuf;
  if (!GetFileSizeEx(file_handle, &sbuf)) {
    const Status status = GetSysErrorStatus("GetFileSizeEx", GetLastError());
    CloseHandle(file_handle);
    return status;
  }
  const int64_t file_size = sbuf.QuadPart;
  if (file_size > MAX_MEMORY_SIZE) {
    CloseHandle(file_handle);
    return Status(Status::INFEASIBLE_ERROR, "too large file");
  }

  // Truncates the file.
  int64_t trunc_size = file_size;
  if (writable) {
    trunc_size = std::max(trunc_size, alloc_init_size_);
    const int64_t diff = trunc_size % PAGE_SIZE;
    if (diff > 0) {
      trunc_size += PAGE_SIZE - diff;
    }
    const Status status = TruncateFile(file_handle, trunc_size);
    if (status != Status::SUCCESS) {
      CloseHandle(file_handle);
      return status;
    }
  }

  // Updates the internal data.
  file_handle_ = file_handle;
  path_ = path;
  file_size_.store(file_size);
  trunc_size_.store(trunc_size);
  writable_ = writable;
  open_options_ = options;

  return Status(Status::SUCCESS);
}

Status PositionalParallelFileImpl::Close() {
  if (file_handle_ == nullptr) {
    return Status(Status::PRECONDITION_ERROR, "not opened file");
  }
  Status status(Status::SUCCESS);

  // Truncates the file.
  if (writable_) {
    status |= TruncateFile(file_handle_, file_size_.load());
  }

  // Unlocks the file.
  if (!(open_options_ & File::OPEN_NO_LOCK)) {
    OVERLAPPED ol;
    ol.Offset = INT32MAX;
    ol.OffsetHigh = 0;
    ol.hEvent = 0;
    if (!UnlockFileEx(file_handle_, 0, 1, 0, &ol)) {
      status |= GetSysErrorStatus("UnlockFileEx", GetLastError());
    }
  }

  // Close the file.
  if (!CloseHandle(file_handle_)) {
    status |= GetSysErrorStatus("CloseHandle", GetLastError());
  }

  // Updates the internal data.
  file_handle_ = nullptr;
  path_.clear();
  file_size_.store(0);
  trunc_size_.store(0);
  writable_ = false;
  open_options_ = 0;

  return status;
}

Status PositionalParallelFileImpl::Read(int64_t off, void* buf, size_t size) {
  if (file_handle_ == nullptr) {
    return Status(Status::PRECONDITION_ERROR, "not opened file");
  }
  char* wp = static_cast<char*>(buf);
  while (size > 0) {
    const int32_t rsiz = PositionalReadFile(file_handle_, wp, size, off);
    if (rsiz < 0) {
      return GetSysErrorStatus("ReadFile", GetLastError());
    }
    if (rsiz == 0) {
      return Status(Status::INFEASIBLE_ERROR, "excessive region");
    }
    off += rsiz;
    wp += rsiz;
    size -= rsiz;
  }
  return Status(Status::SUCCESS);
}

Status PositionalParallelFileImpl::Write(int64_t off, const void* buf, size_t size) {
  if (file_handle_ == nullptr) {
    return Status(Status::PRECONDITION_ERROR, "not opened file");
  }
  if (!writable_) {
    return Status(Status::PRECONDITION_ERROR, "not writable file");
  }
  const int64_t end_position = off + size;
  const Status status = AllocateSpace(end_position);
  if (status != Status::SUCCESS) {
    return status;
  }
  while (true) {
    int64_t old_file_size = file_size_.load();
    if (end_position <= old_file_size ||
        file_size_.compare_exchange_weak(old_file_size, end_position)) {
      break;
    }
  }
  const char* rp = static_cast<const char*>(buf);
  while (size > 0) {
    const int32_t rsiz = PositionalWriteFile(file_handle_, rp, size, off);
    if (rsiz < 0) {
      return GetSysErrorStatus("WriteFile", GetLastError());
    }
    off += rsiz;
    rp += rsiz;
    size -= rsiz;
  }
  return Status(Status::SUCCESS);
}

Status PositionalParallelFileImpl::Append(const void* buf, size_t size, int64_t* off) {
  if (file_handle_ == nullptr) {
    return Status(Status::PRECONDITION_ERROR, "not opened file");
  }
  if (!writable_) {
    return Status(Status::PRECONDITION_ERROR, "not writable file");
  }
  int64_t position = 0;
  while (true) {
    position = file_size_.load();
    const int64_t end_position = position + size;
    const Status status = AllocateSpace(end_position);
    if (status != Status::SUCCESS) {
      return status;
    }
    if (file_size_.compare_exchange_weak(position, end_position)) {
      break;
    }
  }
  if (off != nullptr) {
    *off = position;
  }
  if (buf != nullptr) {
    const char* rp = static_cast<const char*>(buf);
    while (size > 0) {
      const int32_t rsiz = PositionalWriteFile(file_handle_, rp, size, position);
      if (rsiz < 0) {
        return GetSysErrorStatus("WriteFile", GetLastError());
      }
      position += rsiz;
      rp += rsiz;
      size -= rsiz;
    }
  }
  return Status(Status::SUCCESS);
}

Status PositionalParallelFileImpl::Truncate(int64_t size) {
  if (file_handle_ == nullptr) {
    return Status(Status::PRECONDITION_ERROR, "not opened file");
  }
  if (!writable_) {
    return Status(Status::PRECONDITION_ERROR, "not writable file");
  }
  int64_t new_trunc_size =
      std::max(std::max(size, static_cast<int64_t>(PAGE_SIZE)), alloc_init_size_);
  const int64_t diff = new_trunc_size % PAGE_SIZE;
  if (diff > 0) {
    new_trunc_size += PAGE_SIZE - diff;
  }
  const Status status = TruncateFile(file_handle_, new_trunc_size);
  if (status != Status::SUCCESS) {
    return status;
  }
  file_size_.store(size);
  trunc_size_.store(new_trunc_size);
  return Status(Status::SUCCESS);
}

Status PositionalParallelFileImpl::Synchronize(bool hard) {
  if (file_handle_ == nullptr) {
    return Status(Status::PRECONDITION_ERROR, "not opened file");
  }
  if (!writable_) {
    return Status(Status::PRECONDITION_ERROR, "not writable file");
  }
  Status status(Status::SUCCESS);
  trunc_size_.store(file_size_.load());
  status |= TruncateFile(file_handle_, trunc_size_.load());
  if (hard && !FlushFileBuffers(file_handle_)) {
    status |= GetSysErrorStatus("FlushFileBuffers", GetLastError());
  }
  return status;
}

Status PositionalParallelFileImpl::GetSize(int64_t* size) {
  if (file_handle_ == nullptr) {
    return Status(Status::PRECONDITION_ERROR, "not opened file");
  }
  *size = file_size_.load();
  return Status(Status::SUCCESS);
}

Status PositionalParallelFileImpl::SetAllocationStrategy(int64_t init_size, double inc_factor) {
  if (file_handle_ == nullptr) {
    return Status(Status::PRECONDITION_ERROR, "alread opened file");
  }
  alloc_init_size_ = std::max<int64_t>(1, init_size);
  alloc_inc_factor_ = std::max<double>(1.1, inc_factor);
  return Status(Status::SUCCESS);
}

Status PositionalParallelFileImpl::GetPath(std::string* path) {
  if (file_handle_ == nullptr) {
    return Status(Status::PRECONDITION_ERROR, "not opened file");
  }
  *path = path_;
  return Status(Status::SUCCESS);
}

Status PositionalParallelFileImpl::Rename(const std::string& new_path) {
  if (file_handle_ == nullptr) {
    return Status(Status::PRECONDITION_ERROR, "not opened file");
  }
  Status status = RenameFile(path_, new_path);
  if (status == Status::SUCCESS) {
    path_ = new_path;
  }
  return status;
}

Status PositionalParallelFileImpl::AllocateSpace(int64_t min_size) {
  if (min_size <= trunc_size_.load()) {
    return Status(Status::SUCCESS);
  }
  std::lock_guard<std::mutex> lock(mutex_);
  if (min_size <= trunc_size_.load()) {
    return Status(Status::SUCCESS);
  }
  int64_t new_trunc_size =
      std::max(min_size, static_cast<int64_t>(trunc_size_.load() * alloc_inc_factor_));
  const int64_t diff = new_trunc_size % PAGE_SIZE;
  if (diff > 0) {
    new_trunc_size += PAGE_SIZE - diff;
  }
  if (PositionalWriteFile(file_handle_, "", 1, new_trunc_size - 1) != 1) {
    return GetSysErrorStatus("WriteFile", GetLastError());
  }
  trunc_size_.store(new_trunc_size);
  return Status(Status::SUCCESS);
}

PositionalParallelFile::PositionalParallelFile() {
  impl_ = new PositionalParallelFileImpl();
}

PositionalParallelFile::~PositionalParallelFile() {
  delete impl_;
}

Status PositionalParallelFile::Open(const std::string& path, bool writable, int32_t options) {
  return impl_->Open(path, writable, options);
}

Status PositionalParallelFile::Close() {
  return impl_->Close();
}

Status PositionalParallelFile::Read(int64_t off, void* buf, size_t size) {
  assert(off >= 0 && buf != nullptr);
  return impl_->Read(off, buf, size);
}

Status PositionalParallelFile::Write(int64_t off, const void* buf, size_t size) {
  assert(off >= 0 && buf != nullptr && size <= MAX_MEMORY_SIZE);
  return impl_->Write(off, buf, size);
}

Status PositionalParallelFile::Append(const void* buf, size_t size, int64_t* off) {
  assert(buf != nullptr && size <= MAX_MEMORY_SIZE);
  return impl_->Append(buf, size, off);
}

Status PositionalParallelFile::Expand(size_t inc_size, int64_t* old_size) {
  assert(inc_size <= MAX_MEMORY_SIZE);
  return impl_->Append(nullptr, inc_size, old_size);
}

Status PositionalParallelFile::Truncate(int64_t size) {
  assert(size >= 0 && size <= MAX_MEMORY_SIZE);
  return impl_->Truncate(size);
}

Status PositionalParallelFile::Synchronize(bool hard) {
  return impl_->Synchronize(hard);
}

Status PositionalParallelFile::GetSize(int64_t* size) {
  assert(size != nullptr);
  return impl_->GetSize(size);
}

Status PositionalParallelFile::SetAllocationStrategy(int64_t init_size, double inc_factor) {
  assert(init_size > 0 && inc_factor > 0);
  return impl_->SetAllocationStrategy(init_size, inc_factor);
}

Status PositionalParallelFile::GetPath(std::string* path) {
  assert(path != nullptr);
  return impl_->GetPath(path);
}

Status PositionalParallelFile::Rename(const std::string& new_path) {
  return impl_->Rename(new_path);
}

class PositionalAtomicFileImpl final {
 public:
  PositionalAtomicFileImpl();
  ~PositionalAtomicFileImpl();
  Status Open(const std::string& path, bool writable, int32_t options);
  Status Close();
  Status Read(int64_t off, void* buf, size_t size);
  Status Write(int64_t off, const void* buf, size_t size);
  Status Append(const void* buf, size_t size, int64_t* off);
  Status Truncate(int64_t size);
  Status Synchronize(bool hard);
  Status GetSize(int64_t* size);
  Status SetAllocationStrategy(int64_t init_size, double inc_factor);
  Status GetPath(std::string* path);
  Status Rename(const std::string& new_path);

 private:
  Status AllocateSpace(int64_t min_size);

  HANDLE file_handle_;
  std::string path_;
  int64_t file_size_;
  int64_t trunc_size_;
  bool writable_;
  int32_t open_options_;
  int64_t alloc_init_size_;
  double alloc_inc_factor_;
  std::shared_timed_mutex mutex_;
};

PositionalAtomicFileImpl::PositionalAtomicFileImpl()
    : file_handle_(nullptr), file_size_(0), trunc_size_(0),
      writable_(false), open_options_(0),
      alloc_init_size_(File::DEFAULT_ALLOC_INIT_SIZE),
      alloc_inc_factor_(File::DEFAULT_ALLOC_INC_FACTOR), mutex_() {}

PositionalAtomicFileImpl::~PositionalAtomicFileImpl() {
  if (file_handle_ != nullptr) {
    Close();
  }
}

Status PositionalAtomicFileImpl::Open(const std::string& path, bool writable, int32_t options) {
  std::lock_guard<std::shared_timed_mutex> lock(mutex_);
  if (file_handle_ != nullptr) {
    return Status(Status::PRECONDITION_ERROR, "opened file");
  }

  // Opens the file.
  DWORD amode = GENERIC_READ;
  DWORD smode = FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE;
  DWORD cmode = OPEN_EXISTING;
  DWORD flags = FILE_FLAG_RANDOM_ACCESS;
  if (writable) {
    amode |= GENERIC_WRITE;
    if (options & File::OPEN_NO_CREATE) {
      if (options & File::OPEN_TRUNCATE) {
        cmode = TRUNCATE_EXISTING;
      }
    } else {
      cmode = OPEN_ALWAYS;
      if (options & File::OPEN_TRUNCATE) {
        cmode = CREATE_ALWAYS;
      }
    }
  }
  HANDLE file_handle = CreateFile(path.c_str(), amode, smode, nullptr, cmode, flags, nullptr);
  if (file_handle == nullptr || file_handle == INVALID_HANDLE_VALUE) {
    return GetSysErrorStatus("CreateFile", GetLastError());
  }

  // Locks the file.
  if (!(options & File::OPEN_NO_LOCK)) {
    DWORD lmode = writable ? LOCKFILE_EXCLUSIVE_LOCK : 0;
    if (options & File::OPEN_NO_WAIT) {
      lmode |= LOCKFILE_FAIL_IMMEDIATELY;
    }
    OVERLAPPED ol;
    ol.Offset = INT32MAX;
    ol.OffsetHigh = 0;
    ol.hEvent = 0;
    if (!LockFileEx(file_handle, lmode, 0, 1, 0, &ol)) {
      const Status status = GetSysErrorStatus("LockFileEx", GetLastError());
      CloseHandle(file_handle);
      return status;
    }
  }

  // Checks the file size and type.
  LARGE_INTEGER sbuf;
  if (!GetFileSizeEx(file_handle, &sbuf)) {
    const Status status = GetSysErrorStatus("GetFileSizeEx", GetLastError());
    CloseHandle(file_handle);
    return status;
  }
  const int64_t file_size = sbuf.QuadPart;
  if (file_size > MAX_MEMORY_SIZE) {
    CloseHandle(file_handle);
    return Status(Status::INFEASIBLE_ERROR, "too large file");
  }

  // Truncates the file.
  int64_t trunc_size = file_size;
  if (writable) {
    trunc_size = std::max(trunc_size, alloc_init_size_);
    const int64_t diff = trunc_size % PAGE_SIZE;
    if (diff > 0) {
      trunc_size += PAGE_SIZE - diff;
    }
    const Status status = TruncateFile(file_handle, trunc_size);
    if (status != Status::SUCCESS) {
      CloseHandle(file_handle);
      return status;
    }
  }

  // Updates the internal data.
  file_handle_ = file_handle;
  path_ = path;
  file_size_ = file_size;
  trunc_size_ = trunc_size;
  writable_ = writable;
  open_options_ = options;

  return Status(Status::SUCCESS);
}

Status PositionalAtomicFileImpl::Close() {
  std::lock_guard<std::shared_timed_mutex> lock(mutex_);
  if (file_handle_ == nullptr) {
    return Status(Status::PRECONDITION_ERROR, "not opened file");
  }
  Status status(Status::SUCCESS);

  // Truncates the file.
  if (writable_) {
    status |= TruncateFile(file_handle_, file_size_);
  }

  // Unlocks the file.
  if (!(open_options_ & File::OPEN_NO_LOCK)) {
    OVERLAPPED ol;
    ol.Offset = INT32MAX;
    ol.OffsetHigh = 0;
    ol.hEvent = 0;
    if (!UnlockFileEx(file_handle_, 0, 1, 0, &ol)) {
      status |= GetSysErrorStatus("UnlockFileEx", GetLastError());
    }
  }

  // Close the file.
  if (!CloseHandle(file_handle_)) {
    status |= GetSysErrorStatus("CloseHandle", GetLastError());
  }

  // Updates the internal data.
  file_handle_ = nullptr;
  path_.clear();
  file_size_ = 0;
  trunc_size_ = 0;
  writable_ = false;
  open_options_ = 0;

  return status;
}

Status PositionalAtomicFileImpl::Read(int64_t off, void* buf, size_t size) {
  std::shared_lock<std::shared_timed_mutex> lock(mutex_);
  if (file_handle_ == nullptr) {
    return Status(Status::PRECONDITION_ERROR, "not opened file");
  }
  char* wp = static_cast<char*>(buf);
  while (size > 0) {
    const int32_t rsiz = PositionalReadFile(file_handle_, wp, size, off);
    if (rsiz < 0) {
      return GetSysErrorStatus("ReadFile", GetLastError());
    }
    if (rsiz == 0) {
      return Status(Status::INFEASIBLE_ERROR, "excessive region");
    }
    off += rsiz;
    wp += rsiz;
    size -= rsiz;
  }
  return Status(Status::SUCCESS);
}

Status PositionalAtomicFileImpl::Write(int64_t off, const void* buf, size_t size) {
  std::lock_guard<std::shared_timed_mutex> lock(mutex_);
  if (file_handle_ == nullptr) {
    return Status(Status::PRECONDITION_ERROR, "not opened file");
  }
  if (!writable_) {
    return Status(Status::PRECONDITION_ERROR, "not writable file");
  }
  const int64_t end_position = off + size;
  if (end_position > trunc_size_) {
    const Status status = AllocateSpace(end_position);
    if (status != Status::SUCCESS) {
      return status;
    }
  }
  file_size_ = std::max(file_size_, end_position);
  const char* rp = static_cast<const char*>(buf);
  while (size > 0) {
    const int32_t rsiz = PositionalWriteFile(file_handle_, rp, size, off);
    if (rsiz < 0) {
      return GetSysErrorStatus("WriteFile", GetLastError());
    }
    off += rsiz;
    rp += rsiz;
    size -= rsiz;
  }
  return Status(Status::SUCCESS);
}

Status PositionalAtomicFileImpl::Append(const void* buf, size_t size, int64_t* off) {
  std::lock_guard<std::shared_timed_mutex> lock(mutex_);
  if (file_handle_ == nullptr) {
    return Status(Status::PRECONDITION_ERROR, "not opened file");
  }
  if (!writable_) {
    return Status(Status::PRECONDITION_ERROR, "not writable file");
  }
  int64_t position = file_size_;
  const int64_t end_position = position + size;
  if (end_position > trunc_size_) {
    const Status status = AllocateSpace(end_position);
    if (status != Status::SUCCESS) {
      return status;
    }
  }
  file_size_ = end_position;
  if (off != nullptr) {
    *off = position;
  }
  if (buf != nullptr) {
    const char* rp = static_cast<const char*>(buf);
    while (size > 0) {
      const int32_t rsiz = PositionalWriteFile(file_handle_, rp, size, position);
      if (rsiz < 0) {
        return GetSysErrorStatus("WriteFile", GetLastError());
      }
      position += rsiz;
      rp += rsiz;
      size -= rsiz;
    }
  }
  return Status(Status::SUCCESS);
}

Status PositionalAtomicFileImpl::Truncate(int64_t size) {
  std::lock_guard<std::shared_timed_mutex> lock(mutex_);
  if (file_handle_ == nullptr) {
    return Status(Status::PRECONDITION_ERROR, "not opened file");
  }
  if (!writable_) {
    return Status(Status::PRECONDITION_ERROR, "not writable file");
  }
  int64_t new_trunc_size =
      std::max(std::max(size, static_cast<int64_t>(PAGE_SIZE)), alloc_init_size_);
  const int64_t diff = new_trunc_size % PAGE_SIZE;
  if (diff > 0) {
    new_trunc_size += PAGE_SIZE - diff;
  }
  const Status status = TruncateFile(file_handle_, new_trunc_size);
  if (status != Status::SUCCESS) {
    return status;
  }
  file_size_ = size;
  trunc_size_ = new_trunc_size;
  return Status(Status::SUCCESS);
}

Status PositionalAtomicFileImpl::Synchronize(bool hard) {
  std::lock_guard<std::shared_timed_mutex> lock(mutex_);
  if (file_handle_ == nullptr) {
    return Status(Status::PRECONDITION_ERROR, "not opened file");
  }
  if (!writable_) {
    return Status(Status::PRECONDITION_ERROR, "not writable file");
  }
  Status status(Status::SUCCESS);
  trunc_size_ = file_size_;
  status |= TruncateFile(file_handle_, trunc_size_);
  if (hard && !FlushFileBuffers(file_handle_)) {
    status |= GetSysErrorStatus("FlushFileBuffers", GetLastError());
  }
  return status;
}

Status PositionalAtomicFileImpl::GetSize(int64_t* size) {
  std::shared_lock<std::shared_timed_mutex> lock(mutex_);
  if (file_handle_ == nullptr) {
    return Status(Status::PRECONDITION_ERROR, "not opened file");
  }
  *size = file_size_;
  return Status(Status::SUCCESS);
}

Status PositionalAtomicFileImpl::SetAllocationStrategy(int64_t init_size, double inc_factor) {
  std::lock_guard<std::shared_timed_mutex> lock(mutex_);
  if (file_handle_ == nullptr) {
    return Status(Status::PRECONDITION_ERROR, "alread opened file");
  }
  alloc_init_size_ = std::max<int64_t>(1, init_size);
  alloc_inc_factor_ = std::max<double>(1.1, inc_factor);
  return Status(Status::SUCCESS);
}

Status PositionalAtomicFileImpl::GetPath(std::string* path) {
  std::shared_lock<std::shared_timed_mutex> lock(mutex_);
  if (file_handle_ == nullptr) {
    return Status(Status::PRECONDITION_ERROR, "not opened file");
  }
  *path = path_;
  return Status(Status::SUCCESS);
}

Status PositionalAtomicFileImpl::Rename(const std::string& new_path) {
  std::lock_guard<std::shared_timed_mutex> lock(mutex_);
  if (file_handle_ == nullptr) {
    return Status(Status::PRECONDITION_ERROR, "not opened file");
  }
  Status status = RenameFile(path_, new_path);
  if (status == Status::SUCCESS) {
    path_ = new_path;
  }
  return status;
}

Status PositionalAtomicFileImpl::AllocateSpace(int64_t min_size) {
  int64_t new_trunc_size =
      std::max(min_size, static_cast<int64_t>(trunc_size_ * alloc_inc_factor_));
  const int64_t diff = new_trunc_size % PAGE_SIZE;
  if (diff > 0) {
    new_trunc_size += PAGE_SIZE - diff;
  }
  if (PositionalWriteFile(file_handle_, "", 1, new_trunc_size - 1) != 1) {
    return GetSysErrorStatus("WriteFile", GetLastError());
  }
  trunc_size_ = new_trunc_size;
  return Status(Status::SUCCESS);
}

PositionalAtomicFile::PositionalAtomicFile() {
  impl_ = new PositionalAtomicFileImpl();
}

PositionalAtomicFile::~PositionalAtomicFile() {
  delete impl_;
}

Status PositionalAtomicFile::Open(const std::string& path, bool writable, int32_t options) {
  return impl_->Open(path, writable, options);
}

Status PositionalAtomicFile::Close() {
  return impl_->Close();
}

Status PositionalAtomicFile::Read(int64_t off, void* buf, size_t size) {
  assert(off >= 0 && buf != nullptr);
  return impl_->Read(off, buf, size);
}

Status PositionalAtomicFile::Write(int64_t off, const void* buf, size_t size) {
  assert(off >= 0 && buf != nullptr && size <= MAX_MEMORY_SIZE);
  return impl_->Write(off, buf, size);
}

Status PositionalAtomicFile::Append(const void* buf, size_t size, int64_t* off) {
  assert(buf != nullptr && size <= MAX_MEMORY_SIZE);
  return impl_->Append(buf, size, off);
}

Status PositionalAtomicFile::Expand(size_t inc_size, int64_t* old_size) {
  assert(inc_size <= MAX_MEMORY_SIZE);
  return impl_->Append(nullptr, inc_size, old_size);
}

Status PositionalAtomicFile::Truncate(int64_t size) {
  assert(size >= 0 && size <= MAX_MEMORY_SIZE);
  return impl_->Truncate(size);
}

Status PositionalAtomicFile::Synchronize(bool hard) {
  return impl_->Synchronize(hard);
}

Status PositionalAtomicFile::GetSize(int64_t* size) {
  assert(size != nullptr);
  return impl_->GetSize(size);
}

Status PositionalAtomicFile::SetAllocationStrategy(int64_t init_size, double inc_factor) {
  assert(init_size > 0 && inc_factor > 0);
  return impl_->SetAllocationStrategy(init_size, inc_factor);
}

Status PositionalAtomicFile::GetPath(std::string* path) {
  assert(path != nullptr);
  return impl_->GetPath(path);
}

Status PositionalAtomicFile::Rename(const std::string& new_path) {
  return impl_->Rename(new_path);
}

}  // namespace tkrzw

#endif  // _TKRZW_SYS_FILE_POS_WINDOWS_H

// END OF FILE
