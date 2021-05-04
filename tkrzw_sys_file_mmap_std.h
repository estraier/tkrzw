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
  bool writable;
  int64_t off;
  size_t size;
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

Status MemoryMapParallelFile::Synchronize(bool hard) {
  return impl_->file.Synchronize(hard);
}

Status MemoryMapParallelFile::GetSize(int64_t* size) {
  return impl_->file.GetSize(size);
}

Status MemoryMapParallelFile::SetAllocationStrategy(int64_t init_size, double inc_factor) {
  return impl_->file.SetAllocationStrategy(init_size, inc_factor);
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

MemoryMapParallelFile::Zone::Zone(
    MemoryMapParallelFileImpl* fileimpl, bool writable, int64_t off, size_t size, Status* status) {
  const int64_t file_size = fileimpl->file.Lock();
  if (off < 0) {
    off = file_size;
  }
  const int64_t readable_size = std::max<int64_t>(0, std::min<int64_t>(file_size - off, size));
  if (!writable) {
    size = readable_size;
  }
  impl_ = new MemoryMapParallelFileZoneImpl;
  impl_->file = &fileimpl->file;
  impl_->writable = writable;
  impl_->off = off;
  impl_->size = size;
  impl_->status = status;
  impl_->buf = new char[size + 1];
  if (readable_size > 0) {
    *impl_->status |= impl_->file->ReadInCriticalSection(off, impl_->buf, readable_size);
  }
  if (static_cast<int64_t>(size) > readable_size) {
    std::memset(impl_->buf + readable_size, 0, size - readable_size);
  }
}

MemoryMapParallelFile::Zone::~Zone() {
  StdFile* file = impl_->file;
  if (impl_->writable) {
    *impl_->status |= file->WriteInCriticalSection(impl_->off, impl_->buf, impl_->size);
  }
  delete[] impl_->buf;
  delete impl_;
  file->Unlock();
}

int64_t MemoryMapParallelFile::Zone::Offset() const {
  return impl_->off;
}

char* MemoryMapParallelFile::Zone::Pointer() const {
  return impl_->buf;
}

size_t MemoryMapParallelFile::Zone::Size() const {
  return impl_->size;
}

class MemoryMapAtomicFileImpl final {
 public:
  StdFile file;
};

class MemoryMapAtomicFileZoneImpl final {
 public:
  StdFile* file;
  bool writable;
  int64_t off;
  size_t size;
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

Status MemoryMapAtomicFile::Synchronize(bool hard) {
  return impl_->file.Synchronize(hard);
}

Status MemoryMapAtomicFile::GetSize(int64_t* size) {
  return impl_->file.GetSize(size);
}

Status MemoryMapAtomicFile::SetAllocationStrategy(int64_t init_size, double inc_factor) {
  return impl_->file.SetAllocationStrategy(init_size, inc_factor);
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
  impl_ = new MemoryMapAtomicFileZoneImpl;
  impl_->file = &fileimpl->file;
  impl_->writable = writable;
  impl_->off = off;
  impl_->size = size;
  impl_->status = status;
  impl_->buf = new char[size + 1];
  if (readable_size > 0) {
    *impl_->status |= impl_->file->ReadInCriticalSection(off, impl_->buf, readable_size);
  }
  if (static_cast<int64_t>(size) > readable_size) {
    std::memset(impl_->buf + readable_size, 0, size - readable_size);
  }
}

MemoryMapAtomicFile::Zone::~Zone() {
  StdFile* file = impl_->file;
  if (impl_->writable) {
    *impl_->status |= file->WriteInCriticalSection(impl_->off, impl_->buf, impl_->size);
  }
  delete[] impl_->buf;
  delete impl_;
  file->Unlock();
}

int64_t MemoryMapAtomicFile::Zone::Offset() const {
  return impl_->off;
}

char* MemoryMapAtomicFile::Zone::Pointer() const {
  return impl_->buf;
}

size_t MemoryMapAtomicFile::Zone::Size() const {
  return impl_->size;
}

}  // namespace tkrzw

#endif  // _TKRZW_SYS_FILE_MMAP_STD_H

// END OF FILE
