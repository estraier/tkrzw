#include "tkrzw_file_std.h"

namespace tkrzw {

class MemoryMapParallelFileImpl final {
 public:
  StdFile file_;
};

MemoryMapParallelFile::MemoryMapParallelFile() {
  impl_ = new MemoryMapParallelFileImpl();
}

MemoryMapParallelFile::~MemoryMapParallelFile() {
  delete impl_;
}

Status MemoryMapParallelFile::Open(const std::string& path, bool writable, int32_t options) {
  return impl_->file_.Open(path, writable, options);
}

Status MemoryMapParallelFile::Close() {
  return impl_->file_.Close();
}

Status MemoryMapParallelFile::MakeZone(
    bool writable, int64_t off, size_t size, std::unique_ptr<Zone>* zone) {
  return Status(Status::NOT_IMPLEMENTED_ERROR);
}

Status MemoryMapParallelFile::Read(int64_t off, void* buf, size_t size) {
  return impl_->file_.Read(off, buf, size);
}

std::string MemoryMapParallelFile::ReadSimple(int64_t off, size_t size) {
  std::string data(size, 0);
  if (Read(off, const_cast<char*>(data.data()), size) != Status::SUCCESS) {
    data.clear();
  }
  return data;
}

Status MemoryMapParallelFile::Write(int64_t off, const void* buf, size_t size) {
  return impl_->file_.Write(off, buf, size);
}

Status MemoryMapParallelFile::Append(const void* buf, size_t size, int64_t* off) {
  return impl_->file_.Append(buf, size, off);
}

Status MemoryMapParallelFile::Expand(size_t inc_size, int64_t* old_size) {
  return impl_->file_.Expand(inc_size, old_size);
}

Status MemoryMapParallelFile::Truncate(int64_t size) {
  return impl_->file_.Truncate(size);
}

Status MemoryMapParallelFile::Synchronize(bool hard) {
  return impl_->file_.Synchronize(hard);
}

Status MemoryMapParallelFile::GetSize(int64_t* size) {
  return impl_->file_.GetSize(size);
}

Status MemoryMapParallelFile::SetAllocationStrategy(int64_t init_size, double inc_factor) {
  return impl_->file_.SetAllocationStrategy(init_size, inc_factor);
}

Status MemoryMapParallelFile::LockMemory(size_t size) {
  return Status(Status::SUCCESS);
}

class MemoryMapAtomicFileImpl final {
 public:
  StdFile file_;
};

MemoryMapAtomicFile::MemoryMapAtomicFile() {
  impl_ = new MemoryMapAtomicFileImpl();
}

MemoryMapAtomicFile::~MemoryMapAtomicFile() {
  delete impl_;
}

Status MemoryMapAtomicFile::Open(const std::string& path, bool writable, int32_t options) {
  return impl_->file_.Open(path, writable, options);
}

Status MemoryMapAtomicFile::Close() {
  return impl_->file_.Close();
}

Status MemoryMapAtomicFile::MakeZone(
    bool writable, int64_t off, size_t size, std::unique_ptr<Zone>* zone) {
  return Status(Status::NOT_IMPLEMENTED_ERROR);
}

Status MemoryMapAtomicFile::Read(int64_t off, void* buf, size_t size) {
  return impl_->file_.Read(off, buf, size);
}

std::string MemoryMapAtomicFile::ReadSimple(int64_t off, size_t size) {
  std::string data(size, 0);
  if (Read(off, const_cast<char*>(data.data()), size) != Status::SUCCESS) {
    data.clear();
  }
  return data;
}

Status MemoryMapAtomicFile::Write(int64_t off, const void* buf, size_t size) {
  return impl_->file_.Write(off, buf, size);
}

Status MemoryMapAtomicFile::Append(const void* buf, size_t size, int64_t* off) {
  return impl_->file_.Append(buf, size, off);
}

Status MemoryMapAtomicFile::Expand(size_t inc_size, int64_t* old_size) {
  return impl_->file_.Expand(inc_size, old_size);
}

Status MemoryMapAtomicFile::Truncate(int64_t size) {
  return impl_->file_.Truncate(size);
}

Status MemoryMapAtomicFile::Synchronize(bool hard) {
  return impl_->file_.Synchronize(hard);
}

Status MemoryMapAtomicFile::GetSize(int64_t* size) {
  return impl_->file_.GetSize(size);
}

Status MemoryMapAtomicFile::SetAllocationStrategy(int64_t init_size, double inc_factor) {
  return impl_->file_.SetAllocationStrategy(init_size, inc_factor);
}

Status MemoryMapAtomicFile::LockMemory(size_t size) {
  return Status(Status::SUCCESS);
}

}  // namespace tkrzw
