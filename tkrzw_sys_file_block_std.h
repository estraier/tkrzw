/*************************************************************************************************
 * Dummy implementations for block-aligned direct access file with the standard library
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

#ifndef _TKRZW_SYS_FILE_POS_BLOCK_H
#define _TKRZW_SYS_FILE_POS_BLOCK_H

#include "tkrzw_sys_config.h"

#include <memory>
#include <string>
#include <vector>

#include <cinttypes>

#include "tkrzw_file.h"
#include "tkrzw_file_block.h"
#include "tkrzw_file_std.h"
#include "tkrzw_file_util.h"
#include "tkrzw_lib_common.h"
#include "tkrzw_str_util.h"
#include "tkrzw_thread_util.h"

namespace tkrzw {

class BlockParallelFileImpl final {
 public:
  StdFile file;
};

BlockParallelFile::BlockParallelFile() {
  impl_ = new BlockParallelFileImpl();
}

BlockParallelFile::~BlockParallelFile() {
  delete impl_;
}

Status BlockParallelFile::Open(
    const std::string& path, bool writable, int32_t options) {
  return impl_->file.Open(path, writable, options);
}

Status BlockParallelFile::Close() {
  return impl_->file.Close();
}

Status BlockParallelFile::Read(int64_t off, void* buf, size_t size) {
  return impl_->file.Read(off, buf, size);
}

Status BlockParallelFile::Write(int64_t off, const void* buf, size_t size) {
  return impl_->file.Write(off, buf, size);
}

Status BlockParallelFile::Append(const void* buf, size_t size, int64_t* off) {
  return impl_->file.Append(buf, size, off);
}

Status BlockParallelFile::Expand(size_t inc_size, int64_t* old_size) {
  return impl_->file.Expand(inc_size, old_size);
}

Status BlockParallelFile::Truncate(int64_t size) {
  return impl_->file.Truncate(size);
}

Status BlockParallelFile::Synchronize(bool hard) {
  return impl_->file.Synchronize(hard);
}

Status BlockParallelFile::GetSize(int64_t* size) {
  return impl_->file.GetSize(size);
}

Status BlockParallelFile::SetHeadBuffer(int64_t size) {
  return Status(Status::SUCCESS);
}

Status BlockParallelFile::SetAccessStrategy(int64_t block_size, int32_t options) {
  return Status(Status::SUCCESS);
}

Status BlockParallelFile::SetAllocationStrategy(int64_t init_size, double inc_factor) {
  return impl_->file.SetAllocationStrategy(init_size, inc_factor);
}

Status BlockParallelFile::GetPath(std::string* path) {
  assert(path != nullptr);
  return impl_->file.GetPath(path);
}

Status BlockParallelFile::Rename(const std::string& new_path) {
  return impl_->file.Rename(new_path);
}

class BlockAtomicFileImpl final {
 public:
  StdFile file;
};

BlockAtomicFile::BlockAtomicFile() {
  impl_ = new BlockAtomicFileImpl();
}

BlockAtomicFile::~BlockAtomicFile() {
  delete impl_;
}

Status BlockAtomicFile::Open(
    const std::string& path, bool writable, int32_t options) {
  return impl_->file.Open(path, writable, options);
}

Status BlockAtomicFile::Close() {
  return impl_->file.Close();
}

Status BlockAtomicFile::Read(int64_t off, void* buf, size_t size) {
  return impl_->file.Read(off, buf, size);
}

Status BlockAtomicFile::Write(int64_t off, const void* buf, size_t size) {
  return impl_->file.Write(off, buf, size);
}

Status BlockAtomicFile::Append(const void* buf, size_t size, int64_t* off) {
  return impl_->file.Append(buf, size, off);
}

Status BlockAtomicFile::Expand(size_t inc_size, int64_t* old_size) {
  return impl_->file.Expand(inc_size, old_size);
}

Status BlockAtomicFile::Truncate(int64_t size) {
  return impl_->file.Truncate(size);
}

Status BlockAtomicFile::Synchronize(bool hard) {
  return impl_->file.Synchronize(hard);
}

Status BlockAtomicFile::GetSize(int64_t* size) {
  return impl_->file.GetSize(size);
}

Status BlockAtomicFile::SetHeadBuffer(int64_t size) {
  return Status(Status::SUCCESS);
}

Status BlockAtomicFile::SetAccessStrategy(int64_t block_size, int32_t options) {
  return Status(Status::SUCCESS);
}

Status BlockAtomicFile::SetAllocationStrategy(int64_t init_size, double inc_factor) {
  return impl_->file.SetAllocationStrategy(init_size, inc_factor);
}

Status BlockAtomicFile::GetPath(std::string* path) {
  assert(path != nullptr);
  return impl_->file.GetPath(path);
}

Status BlockAtomicFile::Rename(const std::string& new_path) {
  return impl_->file.Rename(new_path);
}

}  // namespace tkrzw

#endif  // _TKRZW_SYS_FILE_BLOCK_STD_H

// END OF FILE
