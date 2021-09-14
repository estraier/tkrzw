/*************************************************************************************************
 * Polymorphic file adapter
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
#include "tkrzw_file_poly.h"
#include "tkrzw_file_pos.h"
#include "tkrzw_file_std.h"
#include "tkrzw_file_util.h"
#include "tkrzw_lib_common.h"
#include "tkrzw_str_util.h"

namespace tkrzw {

PolyFile::PolyFile() : file_(nullptr) {}


Status PolyFile::OpenAdvanced(
    const std::string& path, bool writable, int32_t options,
    const std::map<std::string, std::string>& params) {
  if (file_ != nullptr) {
    return Status(Status::PRECONDITION_ERROR, "opened file");
  }
  std::map<std::string, std::string> tmp_params = params;
  file_ = MakeFileInstance(&tmp_params);
  const Status status = file_->Open(path, writable, options);
  if (status != Status::SUCCESS) {
    file_->Close();
    file_.reset(nullptr);
    return status;
  }
  return Status(Status::SUCCESS);
}

Status PolyFile::Close() {
  if (file_ == nullptr) {
    return Status(Status::PRECONDITION_ERROR, "not opened file");
  }
  Status status(Status::SUCCESS);
  status |= file_->Close();
  file_.reset(nullptr);
  return status;
}

Status PolyFile::Read(int64_t off, void* buf, size_t size) {
  if (file_ == nullptr) {
    return Status(Status::PRECONDITION_ERROR, "not opened file");
  }
  return file_->Read(off, buf, size);
}

Status PolyFile::Write(int64_t off, const void* buf, size_t size) {
  if (file_ == nullptr) {
    return Status(Status::PRECONDITION_ERROR, "not opened file");
  }
  return file_->Write(off, buf, size);
}

Status PolyFile::Append(const void* buf, size_t size, int64_t* off) {
  if (file_ == nullptr) {
    return Status(Status::PRECONDITION_ERROR, "not opened file");
  }
  return file_->Append(buf, size, off);
}

Status PolyFile::Expand(size_t inc_size, int64_t* old_size) {
  if (file_ == nullptr) {
    return Status(Status::PRECONDITION_ERROR, "not opened file");
  }
  return file_->Expand(inc_size, old_size);
}

Status PolyFile::Truncate(int64_t size) {
  if (file_ == nullptr) {
    return Status(Status::PRECONDITION_ERROR, "not opened file");
  }
  return file_->Truncate(size);
}

Status PolyFile::TruncateFakely(int64_t size) {
  if (file_ == nullptr) {
    return Status(Status::PRECONDITION_ERROR, "not opened file");
  }
  return file_->TruncateFakely(size);
}

Status PolyFile::Synchronize(bool hard, int64_t off, int64_t size) {
  if (file_ == nullptr) {
    return Status(Status::PRECONDITION_ERROR, "not opened file");
  }
  return file_->Synchronize(hard, off, size);
}

Status PolyFile::GetSize(int64_t* size) {
  if (file_ == nullptr) {
    return Status(Status::PRECONDITION_ERROR, "not opened file");
  }
  return file_->GetSize(size);
}

Status PolyFile::SetAllocationStrategy(int64_t init_size, double inc_factor) {
  if (file_ == nullptr) {
    return Status(Status::PRECONDITION_ERROR, "not opened file");
  }
  return file_->SetAllocationStrategy(init_size, inc_factor);
}

Status PolyFile::CopyProperties(File* file) {
  if (file_ == nullptr) {
    return Status(Status::PRECONDITION_ERROR, "not opened file");
  }
  return file_->CopyProperties(file);
}

Status PolyFile::GetPath(std::string* path) {
  if (file_ == nullptr) {
    return Status(Status::PRECONDITION_ERROR, "not opened file");
  }
  return file_->GetPath(path);
}

Status PolyFile::Rename(const std::string& new_path) {
  if (file_ == nullptr) {
    return Status(Status::PRECONDITION_ERROR, "not opened file");
  }
  return file_->Rename(new_path);
}

Status PolyFile::DisablePathOperations() {
  if (file_ == nullptr) {
    return Status(Status::PRECONDITION_ERROR, "not opened file");
  }
  return file_->DisablePathOperations();
}

bool PolyFile::IsOpen() const {
  if (file_ == nullptr) {
    return false;
  }
  return file_->IsOpen();
}

bool PolyFile::IsMemoryMapping() const {
  if (file_ == nullptr) {
    return false;
  }
  return file_->IsMemoryMapping();
}

bool PolyFile::IsAtomic() const {
  if (file_ == nullptr) {
    return false;
  }
  return file_->IsAtomic();
}

std::unique_ptr<File> PolyFile::MakeFile() const {
  return std::make_unique<PolyFile>();
}

File* PolyFile::GetInternalFile() const {
  return file_.get();
}

std::unique_ptr<File> PolyFile::MakeFileInstance(std::map<std::string, std::string>* params) {
  const std::string file_class = StrLowerCase(SearchMap(*params, "file", ""));
  std::unique_ptr<File> file;
  if (file_class == "stdfile" || file_class == "std") {
    file = std::make_unique<StdFile>();
  } else if (file_class == "" ||
             file_class == "memorymapparallelfile" || file_class == "mmap-para") {
    file = std::make_unique<MemoryMapParallelFile>();
  } else if (file_class == "memorymapatomicfile" || file_class == "mmap-atom") {
    file = std::make_unique<MemoryMapAtomicFile>();
  } else if (file_class == "positionalparallelfile" || file_class == "pos-para") {
    file = std::make_unique<PositionalParallelFile>();
  } else if (file_class == "positionalatomicfile" || file_class == "pos-atom") {
    file = std::make_unique<PositionalAtomicFile>();
  }
  params->erase("file");
  if (file == nullptr) {
    return nullptr;
  }
  auto* pos_file = dynamic_cast<PositionalFile*>(file.get());
  if (pos_file) {
    const int64_t block_size =
        std::max<int64_t>(StrToInt(SearchMap(*params, "block_size", "-1")), 1);
    int32_t options = PositionalFile::ACCESS_DEFAULT;
    for (const auto& expr : StrSplit(SearchMap(*params, "access_options", ""), ':')) {
      const std::string norm_expr = StrLowerCase(StrStripSpace(expr));
      if (norm_expr == "direct") {
        options |= PositionalFile::ACCESS_DIRECT;
      }
      if (norm_expr == "sync") {
        options |= PositionalFile::ACCESS_SYNC;
      }
      if (norm_expr == "padding") {
        options |= PositionalFile::ACCESS_PADDING;
      }
      if (norm_expr == "pagecache") {
        options |= PositionalFile::ACCESS_PAGECACHE;
      }
    }
    pos_file->SetAccessStrategy(block_size, options);
    params->erase("block_size");
    params->erase("access_options");
  }
  return file;
}

}  // namespace tkrzw

// END OF FILE
