/*************************************************************************************************
 * Dummy implementations for memory mapping file with the standard library
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

#ifndef _TKRZW_SYS_FILE_MMAP_STD_H
#define _TKRZW_SYS_FILE_MMAP_STD_H

#include "tkrzw_sys_config.h"

#include <memory>
#include <string>
#include <vector>

#include <cinttypes>

#include "tkrzw_file.h"
#include "tkrzw_file_mmap.h"
#include "tkrzw_file_std.h"
#include "tkrzw_file_util.h"
#include "tkrzw_lib_common.h"
#include "tkrzw_str_util.h"
#include "tkrzw_thread_util.h"

namespace tkrzw {

class MemoryMapParallelFileImpl final {
 public:
  StdFile file;
};

class MemoryMapParallelFileZoneImpl final {
 public:
  StdFile* file;
  Status* status;
  char* buf;
};

MemoryMapParallelFile::MemoryMapParallelFile() {
  impl_ = new MemoryMapParallelFileImpl();
}

MemoryMapParallelFile::~MemoryMapParallelFile() {
  delete impl_;
}

Status MemoryMapParallelFile::Open(
    const std::string& path, bool writable, int32_t options) {
  return impl_->file.Open(path, writable, options);
}

Status MemoryMapParallelFile::Close() {
  return impl_->file.Close();
}

Status MemoryMapParallelFile::MakeZone(
    bool writable, int64_t off, size_t size, std::unique_ptr<Zone>* zone) {
  Status status(Status::SUCCESS);
  zone->reset(new Zone(impl_, writable, off, size, &status));
  return status;
}

Status MemoryMapParallelFile::Read(int64_t off, void* buf, size_t size) {
  return impl_->file.Read(off, buf, size);
}

std::string MemoryMapParallelFile::ReadSimple(int64_t off, size_t size) {
  std::string data(size, 0);
  if (Read(off, const_cast<char*>(data.data()), size) != Status::SUCCESS) {
    data.clear();
  }
  return data;
}

Status MemoryMapParallelFile::Write(int64_t off, const void* buf, size_t size) {
  return impl_->file.Write(off, buf, size);
}

Status MemoryMapParallelFile::Append(const void* buf, size_t size, int64_t* off) {
  return impl_->file.Append(buf, size, off);
}

Status MemoryMapParallelFile::Expand(size_t inc_size, int64_t* old_size) {
  return impl_->file.Expand(inc_size, old_size);
}

Status MemoryMapParallelFile::Truncate(int64_t size) {
  return impl_->file.Truncate(size);
}

Status MemoryMapParallelFile::TruncateFakely(int64_t size) {
  return impl_->file.TruncateFakely(size);
}

Status MemoryMapParallelFile::Synchronize(bool hard, int64_t off, int64_t size) {
  return impl_->file.Synchronize(hard, off, size);
}

Status MemoryMapParallelFile::GetSize(int64_t* size) {
  return impl_->file.GetSize(size);
}

Status MemoryMapParallelFile::SetAllocationStrategy(int64_t init_size, double inc_factor) {
  return impl_->file.SetAllocationStrategy(init_size, inc_factor);
}

Status MemoryMapParallelFile::CopyProperties(File* file) {
  return impl_->file.CopyProperties(file);
}

Status MemoryMapParallelFile::LockMemory(size_t size) {
  return Status(Status::SUCCESS);
}

Status MemoryMapParallelFile::GetPath(std::string* path) {
  assert(path != nullptr);
  return impl_->file.GetPath(path);
}

Status MemoryMapParallelFile::Rename(const std::string& new_path) {
  return impl_->file.Rename(new_path);
}

Status MemoryMapParallelFile::DisablePathOperations() {
  return impl_->file.DisablePathOperations();
}

MemoryMapParallelFile::Zone::Zone(
    MemoryMapParallelFileImpl* fileimpl, bool writable, int64_t off, size_t size, Status* status)
    : file_(nullptr), off_(0), size_(0), writable_(false) {
  const int64_t file_size = fileimpl->file.Lock();
  if (off < 0) {
    off = file_size;
  }
  const int64_t readable_size = std::max<int64_t>(0, std::min<int64_t>(file_size - off, size));
  if (!writable) {
    size = readable_size;
  }
  auto* impl = new MemoryMapParallelFileZoneImpl;
  impl->file = &fileimpl->file;
  impl->status = status;
  impl->buf = new char[size + 1];
  if (readable_size > 0) {
    *impl->status |= impl->file->ReadInCriticalSection(off, impl->buf, readable_size);
  }
  if (static_cast<int64_t>(size) > readable_size) {
    std::memset(impl->buf + readable_size, 0, size - readable_size);
  }
  off_ = off;
  size_ = size;
  writable_ = writable;
  file_ = reinterpret_cast<MemoryMapParallelFileImpl*>(impl);
}

MemoryMapParallelFile::Zone::~Zone() {
  auto* impl = reinterpret_cast<MemoryMapParallelFileZoneImpl*>(file_);
  StdFile* file = impl->file;
  if (writable_) {
    *impl->status |= file->WriteInCriticalSection(off_, impl->buf, size_);
  }
  delete[] impl->buf;
  delete impl;
  file->Unlock();
}

int64_t MemoryMapParallelFile::Zone::Offset() const {
  return off_;
}

char* MemoryMapParallelFile::Zone::Pointer() const {
  auto* impl = reinterpret_cast<MemoryMapParallelFileZoneImpl*>(file_);
  return impl->buf;
}

size_t MemoryMapParallelFile::Zone::Size() const {
  return size_;
}

class MemoryMapAtomicFileImpl final {
 public:
  StdFile file;
};

class MemoryMapAtomicFileZoneImpl final {
 public:
  StdFile* file;
  Status* status;
  char* buf;
};

MemoryMapAtomicFile::MemoryMapAtomicFile() {
  impl_ = new MemoryMapAtomicFileImpl();
}

MemoryMapAtomicFile::~MemoryMapAtomicFile() {
  delete impl_;
}

Status MemoryMapAtomicFile::Open(
    const std::string& path, bool writable, int32_t options) {
  return impl_->file.Open(path, writable, options);
}

Status MemoryMapAtomicFile::Close() {
  return impl_->file.Close();
}

Status MemoryMapAtomicFile::MakeZone(
    bool writable, int64_t off, size_t size, std::unique_ptr<Zone>* zone) {
  Status status(Status::SUCCESS);
  zone->reset(new Zone(impl_, writable, off, size, &status));
  return status;
}

Status MemoryMapAtomicFile::Read(int64_t off, void* buf, size_t size) {
  return impl_->file.Read(off, buf, size);
}

std::string MemoryMapAtomicFile::ReadSimple(int64_t off, size_t size) {
  std::string data(size, 0);
  if (Read(off, const_cast<char*>(data.data()), size) != Status::SUCCESS) {
    data.clear();
  }
  return data;
}

Status MemoryMapAtomicFile::Write(int64_t off, const void* buf, size_t size) {
  return impl_->file.Write(off, buf, size);
}

Status MemoryMapAtomicFile::Append(const void* buf, size_t size, int64_t* off) {
  return impl_->file.Append(buf, size, off);
}

Status MemoryMapAtomicFile::Expand(size_t inc_size, int64_t* old_size) {
  return impl_->file.Expand(inc_size, old_size);
}

Status MemoryMapAtomicFile::Truncate(int64_t size) {
  return impl_->file.Truncate(size);
}

Status MemoryMapAtomicFile::TruncateFakely(int64_t size) {
  return impl_->file.TruncateFakely(size);
}

Status MemoryMapAtomicFile::Synchronize(bool hard, int64_t off, int64_t size) {
  return impl_->file.Synchronize(hard, off, size);
}

Status MemoryMapAtomicFile::GetSize(int64_t* size) {
  return impl_->file.GetSize(size);
}

Status MemoryMapAtomicFile::SetAllocationStrategy(int64_t init_size, double inc_factor) {
  return impl_->file.SetAllocationStrategy(init_size, inc_factor);
}

Status MemoryMapAtomicFile::CopyProperties(File* file) {
  return impl_->file.CopyProperties(file);
}

Status MemoryMapAtomicFile::LockMemory(size_t size) {
  return Status(Status::SUCCESS);
}

Status MemoryMapAtomicFile::GetPath(std::string* path) {
  assert(path != nullptr);
  return impl_->file.GetPath(path);
}

Status MemoryMapAtomicFile::Rename(const std::string& new_path) {
  return impl_->file.Rename(new_path);
}

Status MemoryMapAtomicFile::DisablePathOperations() {
  return impl_->file.DisablePathOperations();
}

MemoryMapAtomicFile::Zone::Zone(
    MemoryMapAtomicFileImpl* fileimpl, bool writable, int64_t off, size_t size, Status* status) {
  const int64_t file_size = fileimpl->file.Lock();
  if (off < 0) {
    off = file_size;
  }
  const int64_t readable_size = std::max<int64_t>(0, std::min<int64_t>(file_size - off, size));
  if (!writable) {
    size = readable_size;
  }
  auto* impl = new MemoryMapAtomicFileZoneImpl;
  impl->file = &fileimpl->file;
  impl->status = status;
  impl->buf = new char[size + 1];
  if (readable_size > 0) {
    *impl->status |= impl->file->ReadInCriticalSection(off, impl->buf, readable_size);
  }
  if (static_cast<int64_t>(size) > readable_size) {
    std::memset(impl->buf + readable_size, 0, size - readable_size);
  }
  writable_ = writable;
  off_ = off;
  size_ = size;
  file_ = reinterpret_cast<MemoryMapAtomicFileImpl*>(impl);
}

MemoryMapAtomicFile::Zone::~Zone() {
  auto* impl = reinterpret_cast<MemoryMapAtomicFileZoneImpl*>(file_);
  StdFile* file = impl->file;
  if (writable_) {
    *impl->status |= file->WriteInCriticalSection(off_, impl->buf, size_);
  }
  delete[] impl->buf;
  delete impl;
  file->Unlock();
}

int64_t MemoryMapAtomicFile::Zone::Offset() const {
  return off_;
}

char* MemoryMapAtomicFile::Zone::Pointer() const {
  auto* impl = reinterpret_cast<MemoryMapAtomicFileZoneImpl*>(file_);
  return impl->buf;
}

size_t MemoryMapAtomicFile::Zone::Size() const {
  return size_;
}

}  // namespace tkrzw

#endif  // _TKRZW_SYS_FILE_MMAP_STD_H

// END OF FILE
