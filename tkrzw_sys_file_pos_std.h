/*************************************************************************************************
 * Dummy implementations for positional access file with the standard library
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

#ifndef _TKRZW_SYS_FILE_POS_STD_H
#define _TKRZW_SYS_FILE_POS_STD_H

#include "tkrzw_sys_config.h"

#include <memory>
#include <string>
#include <vector>

#include <cinttypes>

#include "tkrzw_file.h"
#include "tkrzw_file_pos.h"
#include "tkrzw_file_std.h"
#include "tkrzw_file_util.h"
#include "tkrzw_lib_common.h"
#include "tkrzw_str_util.h"
#include "tkrzw_thread_util.h"

namespace tkrzw {

class PositionalParallelFileImpl final {
 public:
  StdFile file;
};

PositionalParallelFile::PositionalParallelFile() {
  impl_ = new PositionalParallelFileImpl();
}

PositionalParallelFile::~PositionalParallelFile() {
  delete impl_;
}

Status PositionalParallelFile::Open(
    const std::string& path, bool writable, int32_t options) {
  return impl_->file.Open(path, writable, options);
}

Status PositionalParallelFile::Close() {
  return impl_->file.Close();
}

Status PositionalParallelFile::Read(int64_t off, void* buf, size_t size) {
  return impl_->file.Read(off, buf, size);
}

Status PositionalParallelFile::Write(int64_t off, const void* buf, size_t size) {
  return impl_->file.Write(off, buf, size);
}

Status PositionalParallelFile::Append(const void* buf, size_t size, int64_t* off) {
  return impl_->file.Append(buf, size, off);
}

Status PositionalParallelFile::Expand(size_t inc_size, int64_t* old_size) {
  return impl_->file.Expand(inc_size, old_size);
}

Status PositionalParallelFile::Truncate(int64_t size) {
  return impl_->file.Truncate(size);
}

Status PositionalParallelFile::Synchronize(bool hard) {
  return impl_->file.Synchronize(hard);
}

Status PositionalParallelFile::GetSize(int64_t* size) {
  return impl_->file.GetSize(size);
}

Status PositionalParallelFile::SetAllocationStrategy(int64_t init_size, double inc_factor) {
  return impl_->file.SetAllocationStrategy(init_size, inc_factor);
}

Status PositionalParallelFile::GetPath(std::string* path) {
  assert(path != nullptr);
  return impl_->file.GetPath(path);
}

Status PositionalParallelFile::Rename(const std::string& new_path) {
  return impl_->file.Rename(new_path);
}

class PositionalAtomicFileImpl final {
 public:
  StdFile file;
};

PositionalAtomicFile::PositionalAtomicFile() {
  impl_ = new PositionalAtomicFileImpl();
}

PositionalAtomicFile::~PositionalAtomicFile() {
  delete impl_;
}

Status PositionalAtomicFile::Open(
    const std::string& path, bool writable, int32_t options) {
  return impl_->file.Open(path, writable, options);
}

Status PositionalAtomicFile::Close() {
  return impl_->file.Close();
}

Status PositionalAtomicFile::Read(int64_t off, void* buf, size_t size) {
  return impl_->file.Read(off, buf, size);
}

Status PositionalAtomicFile::Write(int64_t off, const void* buf, size_t size) {
  return impl_->file.Write(off, buf, size);
}

Status PositionalAtomicFile::Append(const void* buf, size_t size, int64_t* off) {
  return impl_->file.Append(buf, size, off);
}

Status PositionalAtomicFile::Expand(size_t inc_size, int64_t* old_size) {
  return impl_->file.Expand(inc_size, old_size);
}

Status PositionalAtomicFile::Truncate(int64_t size) {
  return impl_->file.Truncate(size);
}

Status PositionalAtomicFile::Synchronize(bool hard) {
  return impl_->file.Synchronize(hard);
}

Status PositionalAtomicFile::GetSize(int64_t* size) {
  return impl_->file.GetSize(size);
}

Status PositionalAtomicFile::SetAllocationStrategy(int64_t init_size, double inc_factor) {
  return impl_->file.SetAllocationStrategy(init_size, inc_factor);
}

Status PositionalAtomicFile::GetPath(std::string* path) {
  assert(path != nullptr);
  return impl_->file.GetPath(path);
}

Status PositionalAtomicFile::Rename(const std::string& new_path) {
  return impl_->file.Rename(new_path);
}

}  // namespace tkrzw

#endif  // _TKRZW_SYS_FILE_POS_STD_H

// END OF FILE
