/*************************************************************************************************
 * File implementations by memory mapping
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

#if defined(_TKRZW_STDONLY)

#include "tkrzw_sys_file_mmap_std.h"

#elif defined(_SYS_WINDOWS_)

#include "tkrzw_sys_file_mmap_windows.h"

#else

#include "tkrzw_file.h"
#include "tkrzw_file_mmap.h"
#include "tkrzw_file_util.h"
#include "tkrzw_lib_common.h"
#include "tkrzw_str_util.h"
#include "tkrzw_sys_util_posix.h"
#include "tkrzw_thread_util.h"

namespace tkrzw {

inline void AdviseMemoryRandomAccessPattern(void* addr, size_t len) {
#if defined(_SYS_LINUX_) || defined(_SYS_MACOSX_)
  posix_madvise(addr, len, POSIX_MADV_RANDOM);
#endif
}

inline void* tkrzw_mremap(
    void *old_address, size_t old_size, size_t new_size, int32_t fd) {
#if defined(_SYS_LINUX_)
  return mremap(old_address, std::max<int64_t>(1, old_size),
                std::max<int64_t>(1, new_size), MREMAP_MAYMOVE);
#else
  if (munmap(old_address, std::max<int64_t>(1, old_size)) != 0) {
    return MAP_FAILED;
  }
  return mmap(0, std::max<int64_t>(1, new_size), PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
#endif
}

class MemoryMapParallelFileImpl final {
  friend class MemoryMapParallelFile::Zone;
 public:
  MemoryMapParallelFileImpl();
  ~MemoryMapParallelFileImpl();
  Status Open(const std::string& path, bool writable, int32_t options);
  Status Close();
  Status Truncate(int64_t size);
  Status TruncateFakely(int64_t size);
  Status Synchronize(bool hard, int64_t off, int64_t size);
  Status GetSize(int64_t* size);
  Status SetAllocationStrategy(int64_t init_size, double inc_factor);
  Status CopyProperties(File* file);
  Status GetPath(std::string* path);
  Status Rename(const std::string& new_path);
  Status DisablePathOperations();
  bool IsOpen();

 private:
  Status AllocateSpace(int64_t min_size);

  std::atomic_int32_t fd_;
  std::string path_;
  std::atomic_int64_t file_size_;
  char* map_;
  std::atomic_int64_t map_size_;
  bool writable_;
  int32_t open_options_;
  int64_t alloc_init_size_;
  double alloc_inc_factor_;
  SpinSharedMutex mutex_;
};

MemoryMapParallelFileImpl::MemoryMapParallelFileImpl() :
    fd_(-1), file_size_(-0), map_(nullptr), map_size_(0),
    writable_(false), open_options_(0),
    alloc_init_size_(File::DEFAULT_ALLOC_INIT_SIZE),
    alloc_inc_factor_(File::DEFAULT_ALLOC_INC_FACTOR), mutex_() {}

MemoryMapParallelFileImpl::~MemoryMapParallelFileImpl() {
  if (fd_ >= 0) {
    Close();
  }
}

Status MemoryMapParallelFileImpl::Open(
    const std::string& path, bool writable, int32_t options) {
  if (fd_ >= 0) {
    return Status(Status::PRECONDITION_ERROR, "opened file");
  }

  // Opens the file.
  int32_t oflags = O_RDONLY;
  if (writable) {
    oflags = O_RDWR;
    if (!(options & File::OPEN_NO_CREATE)) {
      oflags |= O_CREAT;
    }
    if (options & File::OPEN_TRUNCATE) {
      oflags |= O_TRUNC;
    }
  }
  const int32_t fd = open(path.c_str(), oflags, FILEPERM);
  if (fd < 0) {
    return GetErrnoStatus("open", errno);
  }

  // Locks the file.
  if (!(options & File::OPEN_NO_LOCK)) {
    struct flock flbuf;
    std::memset(&flbuf, 0, sizeof(flbuf));
    flbuf.l_type = writable ? F_WRLCK : F_RDLCK;
    flbuf.l_whence = SEEK_SET;
    flbuf.l_start = 0;
    flbuf.l_len = 0;
    flbuf.l_pid = 0;
    const int32_t flcmd = options & File::OPEN_NO_WAIT ? F_SETLK : F_SETLKW;
    if (fcntl(fd, flcmd, &flbuf) != 0) {
      const Status status = GetErrnoStatus("fcntl-lock", errno);
      close(fd);
      return status;
    }
  }

  // Checks the file size and type.
  struct stat sbuf;
  if (fstat(fd, &sbuf) != 0) {
    const Status status = GetErrnoStatus("fstat", errno);
    close(fd);
    return status;
  }
  if (!S_ISREG(sbuf.st_mode)) {
    close(fd);
    return Status(Status::INFEASIBLE_ERROR, "not a regular file");
  }
  const int64_t file_size = sbuf.st_size;
  if (file_size > MAX_MEMORY_SIZE) {
    close(fd);
    return Status(Status::INFEASIBLE_ERROR, "too large file");
  }

  // Maps the memory.
  int64_t map_size = file_size;
  int32_t mprot = PROT_READ;
  if (writable) {
    map_size = std::max<int64_t>(1, std::max(map_size, alloc_init_size_));
    map_size = AlignNumber(map_size, PAGE_SIZE);
    mprot |= PROT_WRITE;
    const Status status = TruncateFile(fd, map_size);
    if (status != Status::SUCCESS) {
      close(fd);
      return status;
    }
  } else {
    map_size = std::max(map_size, static_cast<int64_t>(PAGE_SIZE));
  }
  void* map = mmap(0, std::max<int64_t>(1, map_size), mprot, MAP_SHARED, fd, 0);
  if (map == MAP_FAILED) {
    const Status status = GetErrnoStatus("mmap", errno);
    close(fd);
    return status;
  }
  AdviseMemoryRandomAccessPattern(map, map_size);

  // Updates the internal data.
  fd_ = fd;
  path_ = path;
  file_size_.store(file_size);
  map_ = static_cast<char*>(map);
  map_size_.store(map_size);
  writable_ = writable;
  open_options_ = options;

  return Status(Status::SUCCESS);
}

Status MemoryMapParallelFileImpl::Close() {
  if (fd_ < 0) {
    return Status(Status::PRECONDITION_ERROR, "not opened file");
  }
  Status status(Status::SUCCESS);

  // Unmaps the memory.
  const int64_t unmap_size = std::max(map_size_.load(), static_cast<int64_t>(PAGE_SIZE));
  if (writable_ && (open_options_ & File::OPEN_SYNC_HARD) && unmap_size > 0 &&
      msync(map_, unmap_size, MS_SYNC) != 0) {
    status |= GetErrnoStatus("msync", errno);
  }
  if (munmap(map_, std::max<int64_t>(1, unmap_size)) != 0) {
    status |= GetErrnoStatus("munmap", errno);
  }

  // Truncates the file.
  if (writable_) {
    status |= TruncateFile(fd_, file_size_.load());
  }

  // Unlocks the file.
  if (!(open_options_ & File::OPEN_NO_LOCK)) {
    struct flock flbuf;
    std::memset(&flbuf, 0, sizeof(flbuf));
    flbuf.l_type = F_UNLCK;
    flbuf.l_whence = SEEK_SET;
    flbuf.l_start = 0;
    flbuf.l_len = 0;
    flbuf.l_pid = 0;
    if (fcntl(fd_, F_SETLKW, &flbuf) != 0) {
      status |= GetErrnoStatus("fcntl-unlock", errno);
    }
  }

  // Close the file.
  if (close(fd_) != 0) {
    status |= GetErrnoStatus("close", errno);
  }

  // Updates the internal data.
  fd_ = -1;
  path_.clear();
  file_size_ .store(0);
  map_ = nullptr;
  map_size_.store(0);
  writable_ = false;
  open_options_ = 0;

  return status;
}

Status MemoryMapParallelFileImpl::Truncate(int64_t size) {
  if (fd_ < 0) {
    return Status(Status::PRECONDITION_ERROR, "not opened file");
  }
  if (!writable_) {
    return Status(Status::PRECONDITION_ERROR, "not writable file");
  }
  int64_t new_map_size =
      std::max(std::max(size, static_cast<int64_t>(PAGE_SIZE)), alloc_init_size_);
  new_map_size = AlignNumber(new_map_size, PAGE_SIZE);
  void* new_map = tkrzw_mremap(map_, map_size_.load(), new_map_size, fd_);
  if (new_map == MAP_FAILED) {
    const Status status = GetErrnoStatus("mremap", errno);
    map_ = nullptr;
    close(fd_);
    fd_ = -1;
    return status;
  }
  AdviseMemoryRandomAccessPattern(new_map, new_map_size);
  map_ = static_cast<char*>(new_map);
  map_size_.store(new_map_size);
  const Status status = TruncateFile(fd_, new_map_size);
  if (status != Status::SUCCESS) {
    return status;
  }
  file_size_.store(size);
  return Status(Status::SUCCESS);
}

Status MemoryMapParallelFileImpl::TruncateFakely(int64_t size) {
  if (fd_ < 0) {
    return Status(Status::PRECONDITION_ERROR, "not opened file");
  }
  if (size > map_size_.load()) {
    return Status(Status::INFEASIBLE_ERROR, "unable to increase the file size");
  }
  file_size_.store(size);
  return Status(Status::SUCCESS);
}

Status MemoryMapParallelFileImpl::Synchronize(bool hard, int64_t off, int64_t size) {
  if (fd_ < 0) {
    return Status(Status::PRECONDITION_ERROR, "not opened file");
  }
  if (!writable_) {
    return Status(Status::PRECONDITION_ERROR, "not writable file");
  }
  std::lock_guard<SpinSharedMutex> lock(mutex_);
  if (file_size_.load() != map_size_.load()) {
    const int64_t new_map_size = file_size_.load();
    void* new_map = tkrzw_mremap(map_, map_size_.load(), new_map_size, fd_);
    if (new_map == MAP_FAILED) {
      const Status status = GetErrnoStatus("mremap", errno);
      map_ = nullptr;
      close(fd_);
      fd_ = -1;
      return status;
    }
    AdviseMemoryRandomAccessPattern(new_map, new_map_size);
    map_ = static_cast<char*>(new_map);
    map_size_.store(new_map_size);
  }
  Status status = TruncateFile(fd_, file_size_.load());
  if (hard) {
    if (size == 0) {
      size = map_size_;
    }
    const int64_t end = off + size;
    off -= off % PAGE_SIZE;
    size = end - off;
    off = std::min<int64_t>(map_size_, off);
    size = std::min<int64_t>(map_size_ - off, size);
    if (size > 0 && msync(map_ + off, size, MS_SYNC) != 0) {
      status |= GetErrnoStatus("msync", errno);
    }
  }
  return status;
}

Status MemoryMapParallelFileImpl::GetSize(int64_t* size) {
  if (fd_ < 0) {
    return Status(Status::PRECONDITION_ERROR, "not opened file");
  }
  *size = file_size_.load();
  return Status(Status::SUCCESS);
}

Status MemoryMapParallelFileImpl::SetAllocationStrategy(int64_t init_size, double inc_factor) {
  if (fd_ >= 0) {
    return Status(Status::PRECONDITION_ERROR, "alread opened file");
  }
  alloc_init_size_ = init_size;
  alloc_inc_factor_ = inc_factor;
  return Status(Status::SUCCESS);
}

Status MemoryMapParallelFileImpl::CopyProperties(File* file) {
  return file->SetAllocationStrategy(alloc_init_size_, alloc_inc_factor_);
}

Status MemoryMapParallelFileImpl::GetPath(std::string* path) {
  if (fd_ < 0) {
    return Status(Status::PRECONDITION_ERROR, "not opened file");
  }
  if (path_.empty()) {
    return Status(Status::PRECONDITION_ERROR, "disabled path operatione");
  }
  *path = path_;
  return Status(Status::SUCCESS);
}

Status MemoryMapParallelFileImpl::Rename(const std::string& new_path) {
  if (fd_ < 0) {
    return Status(Status::PRECONDITION_ERROR, "not opened file");
  }
  if (path_.empty()) {
    return Status(Status::PRECONDITION_ERROR, "disabled path operatione");
  }
  Status status = RenameFile(path_, new_path);
  if (status == Status::SUCCESS) {
    path_ = new_path;
  }
  return status;
}

Status MemoryMapParallelFileImpl::DisablePathOperations() {
  if (fd_ < 0) {
    return Status(Status::PRECONDITION_ERROR, "not opened file");
  }
  path_.clear();
  return Status(Status::SUCCESS);
}

bool MemoryMapParallelFileImpl::IsOpen() {
  return fd_ >= 0;
}

Status MemoryMapParallelFileImpl::AllocateSpace(int64_t min_size) {
  if (min_size <= map_size_.load()) {
    return Status(Status::SUCCESS);
  }
  std::lock_guard<SpinSharedMutex> lock(mutex_);
  if (min_size <= map_size_.load()) {
    return Status(Status::SUCCESS);
  }
  int64_t new_map_size =
      std::max(std::max(min_size, static_cast<int64_t>(
          map_size_.load() * alloc_inc_factor_)), static_cast<int64_t>(PAGE_SIZE));
  new_map_size = AlignNumber(new_map_size, PAGE_SIZE);
  Status status = TruncateFile(fd_, new_map_size);
  if (status != Status::SUCCESS) {
    return status;
  }
  void* new_map = tkrzw_mremap(map_, map_size_.load(), new_map_size, fd_);
  if (new_map == MAP_FAILED) {
    status = GetErrnoStatus("mremap", errno);
    map_ = nullptr;
    close(fd_);
    fd_ = -1;
    return status;
  }
  AdviseMemoryRandomAccessPattern(new_map, new_map_size);
  map_ = static_cast<char*>(new_map);
  map_size_.store(new_map_size);
  return Status(Status::SUCCESS);
}

MemoryMapParallelFile::MemoryMapParallelFile() {
  impl_ = new MemoryMapParallelFileImpl();
}

MemoryMapParallelFile::~MemoryMapParallelFile() {
  delete impl_;
}

Status MemoryMapParallelFile::Open(const std::string& path, bool writable, int32_t options) {
  return impl_->Open(path, writable, options);
}

Status MemoryMapParallelFile::Close() {
  return impl_->Close();
}

Status MemoryMapParallelFile::MakeZone(
    bool writable, int64_t off, size_t size, std::unique_ptr<Zone>* zone) {
  Status status(Status::SUCCESS);
  zone->reset(new Zone(impl_, writable, off, size, &status));
  return status;
}

Status MemoryMapParallelFile::Read(int64_t off, void* buf, size_t size) {
  assert(off >= 0 && buf != nullptr);
  Status status(Status::SUCCESS);
  Zone zone(impl_, false, off, size, &status);
  if (status != Status::SUCCESS) {
    return status;
  }
  if (zone.Size() != size) {
    return Status(Status::INFEASIBLE_ERROR, "excessive size");
  }
  std::memcpy(buf, zone.Pointer(), zone.Size());
  return Status(Status::SUCCESS);
}

std::string MemoryMapParallelFile::ReadSimple(int64_t off, size_t size) {
  assert(off >= 0);
  Status status(Status::SUCCESS);
  Zone zone(impl_, false, off, size, &status);
  if (status != Status::SUCCESS || zone.Size() != size) {
    return "";
  }
  std::string result(zone.Pointer(), size);
  return result;
}

Status MemoryMapParallelFile::Write(int64_t off, const void* buf, size_t size) {
  assert(off >= 0 && buf != nullptr && size <= MAX_MEMORY_SIZE);
  Status status(Status::SUCCESS);
  Zone zone(impl_, true, off, size, &status);
  if (status != Status::SUCCESS) {
    return status;
  }
  std::memcpy(zone.Pointer(), buf, zone.Size());
  return Status(Status::SUCCESS);
}

Status MemoryMapParallelFile::Append(const void* buf, size_t size, int64_t* off) {
  assert(buf != nullptr && size <= MAX_MEMORY_SIZE);
  Status status(Status::SUCCESS);
  Zone zone(impl_, true, -1, size, &status);
  if (status != Status::SUCCESS) {
    return status;
  }
  std::memcpy(zone.Pointer(), buf, zone.Size());
  if (off != nullptr) {
    *off = zone.Offset();
  }
  return Status(Status::SUCCESS);
}

Status MemoryMapParallelFile::Expand(size_t inc_size, int64_t* old_size) {
  assert(inc_size <= MAX_MEMORY_SIZE);
  Status status(Status::SUCCESS);
  Zone zone(impl_, true, -1, inc_size, &status);
  if (status != Status::SUCCESS) {
    return status;
  }
  if (old_size != nullptr) {
    *old_size = zone.Offset();
  }
  return Status(Status::SUCCESS);
}

Status MemoryMapParallelFile::Truncate(int64_t size) {
  assert(size >= 0 && size <= MAX_MEMORY_SIZE);
  return impl_->Truncate(size);
}

Status MemoryMapParallelFile::TruncateFakely(int64_t size) {
  assert(size >= 0 && size <= MAX_MEMORY_SIZE);
  return impl_->TruncateFakely(size);
}

Status MemoryMapParallelFile::Synchronize(bool hard, int64_t off, int64_t size) {
  assert(off >= 0 && size >= 0);
  return impl_->Synchronize(hard, off, size);
}

Status MemoryMapParallelFile::GetSize(int64_t* size) {
  assert(size != nullptr);
  return impl_->GetSize(size);
}

Status MemoryMapParallelFile::SetAllocationStrategy(int64_t init_size, double inc_factor) {
  assert(init_size >= 0 && inc_factor >= 0);
  return impl_->SetAllocationStrategy(init_size, inc_factor);
}

Status MemoryMapParallelFile::CopyProperties(File* file) {
  assert(file != nullptr);
  return impl_->CopyProperties(file);
}

Status MemoryMapParallelFile::GetPath(std::string* path) {
  assert(path != nullptr);
  return impl_->GetPath(path);
}

Status MemoryMapParallelFile::Rename(const std::string& new_path) {
  return impl_->Rename(new_path);
}

Status MemoryMapParallelFile::DisablePathOperations() {
  return impl_->DisablePathOperations();
}

bool MemoryMapParallelFile::IsOpen() const {
  return impl_->IsOpen();
}

MemoryMapParallelFile::Zone::Zone(
    MemoryMapParallelFileImpl* file, bool writable, int64_t off, size_t size,
    Status* status)
    : file_(nullptr), off_(-1), size_(0) {
  if (file->fd_ < 0) {
    status->Set(Status::PRECONDITION_ERROR, "not opened file");
    return;
  }
  if (writable) {
    if (!file->writable_) {
      status->Set(Status::PRECONDITION_ERROR, "not writable file");
      return;
    }
    if (off < 0) {
      int64_t old_file_size = 0;
      while (true) {
        old_file_size = file->file_size_.load();
        const int64_t end_position = old_file_size + size;
        const Status adjust_status = file->AllocateSpace(end_position);
        if (adjust_status != Status::SUCCESS) {
          *status = adjust_status;
          return;
        }
        if (file->file_size_.compare_exchange_weak(old_file_size, end_position)) {
          break;
        }
      }
      off = old_file_size;
    } else {
      const int64_t end_position = off + size;
      const Status adjust_status = file->AllocateSpace(end_position);
      if (adjust_status != Status::SUCCESS) {
        *status = adjust_status;
        return;
      }
      while (true) {
        int64_t old_file_size = file->file_size_.load();
        if (end_position <= old_file_size ||
            file->file_size_.compare_exchange_weak(old_file_size, end_position)) {
          break;
        }
      }
    }
  } else {
    if (off < 0) {
      status->Set(Status::PRECONDITION_ERROR, "negative offset");
      return;
    }
    if (off > file->file_size_.load()) {
      status->Set(Status::INFEASIBLE_ERROR, "excessive offset");
      return;
    }
    size = std::min(static_cast<int64_t>(size), file->file_size_.load() - off);
  }
  file_ = file;
  file_->mutex_.lock_shared();
  off_ = off;
  size_ = size;
}

MemoryMapParallelFile::Zone::~Zone() {
  if (file_ != nullptr) {
    file_->mutex_.unlock_shared();
  }
}

int64_t MemoryMapParallelFile::Zone::Offset() const {
  return off_;
}

char* MemoryMapParallelFile::Zone::Pointer() const {
  return file_->map_ + off_;
}

size_t MemoryMapParallelFile::Zone::Size() const {
  return size_;
}

class MemoryMapAtomicFileImpl final {
  friend class MemoryMapAtomicFile::Zone;
 public:
  MemoryMapAtomicFileImpl();
  ~MemoryMapAtomicFileImpl();
  Status Open(const std::string& path, bool writable, int32_t options);
  Status Close();
  Status Truncate(int64_t size);
  Status TruncateFakely(int64_t size);
  Status Synchronize(bool hard, int64_t off, int64_t size);
  Status GetSize(int64_t* size);
  Status SetAllocationStrategy(int64_t init_size, double inc_factor);
  Status CopyProperties(File* file);
  Status GetPath(std::string* path);
  Status Rename(const std::string& new_path);
  Status DisablePathOperations();
  bool IsOpen();

 private:
  Status AllocateSpace(int64_t min_size);

  int32_t fd_;
  std::string path_;
  int64_t file_size_;
  char* map_;
  int64_t map_size_;
  bool writable_;
  int32_t open_options_;
  int64_t alloc_init_size_;
  double alloc_inc_factor_;
  SpinSharedMutex mutex_;
};

MemoryMapAtomicFileImpl::MemoryMapAtomicFileImpl() :
    fd_(-1), file_size_(0), map_(nullptr), map_size_(0),
    writable_(false), open_options_(0),
    alloc_init_size_(File::DEFAULT_ALLOC_INIT_SIZE),
    alloc_inc_factor_(File::DEFAULT_ALLOC_INC_FACTOR), mutex_() {}

MemoryMapAtomicFileImpl::~MemoryMapAtomicFileImpl() {
  if (fd_ >= 0) {
    Close();
  }
}

Status MemoryMapAtomicFileImpl::Open(
    const std::string& path, bool writable, int32_t options) {
  std::lock_guard<SpinSharedMutex> lock(mutex_);
  if (fd_ >= 0) {
    return Status(Status::PRECONDITION_ERROR, "opened file");
  }

  // Opens the file.
  int32_t oflags = O_RDONLY;
  if (writable) {
    oflags = O_RDWR;
    if (!(options & File::OPEN_NO_CREATE)) {
      oflags |= O_CREAT;
    }
    if (options & File::OPEN_TRUNCATE) {
      oflags |= O_TRUNC;
    }
  }
  const int32_t fd = open(path.c_str(), oflags, FILEPERM);
  if (fd < 0) {
    return GetErrnoStatus("open", errno);
  }

  // Locks the file.
  if (!(options & File::OPEN_NO_LOCK)) {
    struct flock flbuf;
    std::memset(&flbuf, 0, sizeof(flbuf));
    flbuf.l_type = writable ? F_WRLCK : F_RDLCK;
    flbuf.l_whence = SEEK_SET;
    flbuf.l_start = 0;
    flbuf.l_len = 0;
    flbuf.l_pid = 0;
    const int32_t flcmd = options & File::OPEN_NO_WAIT ? F_SETLK : F_SETLKW;
    if (fcntl(fd, flcmd, &flbuf) != 0) {
      const Status status = GetErrnoStatus("fcntl-lock", errno);
      close(fd);
      return status;
    }
  }

  // Checks the file size and type.
  struct stat sbuf;
  if (fstat(fd, &sbuf) != 0) {
    const Status status = GetErrnoStatus("fstat", errno);
    close(fd);
    return status;
  }
  if (!S_ISREG(sbuf.st_mode)) {
    close(fd);
    return Status(Status::INFEASIBLE_ERROR, "not a regular file");
  }
  const int64_t file_size = sbuf.st_size;
  if (file_size > MAX_MEMORY_SIZE) {
    close(fd);
    return Status(Status::INFEASIBLE_ERROR, "too large file");
  }

  // Maps the memory.
  int64_t map_size = file_size;
  int32_t mprot = PROT_READ;
  if (writable) {
    map_size = std::max<int64_t>(1, std::max(map_size, alloc_init_size_));
    map_size = AlignNumber(map_size, PAGE_SIZE);
    mprot |= PROT_WRITE;
    const Status status = TruncateFile(fd, map_size);
    if (status != Status::SUCCESS) {
      close(fd);
      return status;
    }
  } else {
    map_size = std::max(map_size, static_cast<int64_t>(PAGE_SIZE));
  }
  void* map = mmap(0, std::max<int64_t>(1, map_size), mprot, MAP_SHARED, fd, 0);
  if (map == MAP_FAILED) {
    const Status status = GetErrnoStatus("mmap", errno);
    close(fd);
    return status;
  }
  AdviseMemoryRandomAccessPattern(map, map_size);

  // Updates the internal data.
  fd_ = fd;
  path_ = path;
  file_size_ = file_size;
  map_ = static_cast<char*>(map);
  map_size_ = map_size;
  writable_ = writable;
  open_options_ = options;

  return Status(Status::SUCCESS);
}

Status MemoryMapAtomicFileImpl::Close() {
  std::lock_guard<SpinSharedMutex> lock(mutex_);
  if (fd_ < 0) {
    return Status(Status::PRECONDITION_ERROR, "not opened file");
  }
  Status status(Status::SUCCESS);

  // Unmaps the memory.
  const int64_t unmap_size = std::max(map_size_, static_cast<int64_t>(PAGE_SIZE));
  if (writable_ && (open_options_ & File::OPEN_SYNC_HARD) && unmap_size > 0 &&
      msync(map_, unmap_size, MS_SYNC) != 0) {
    status |= GetErrnoStatus("msync", errno);
  }
  if (munmap(map_, std::max<int64_t>(1, unmap_size)) != 0) {
    status |= GetErrnoStatus("munmap", errno);
  }

  // Truncates the file.
  if (writable_) {
    status |= TruncateFile(fd_, file_size_);
  }

  // Unlocks the file.
  if (!(open_options_ & File::OPEN_NO_LOCK)) {
    struct flock flbuf;
    std::memset(&flbuf, 0, sizeof(flbuf));
    flbuf.l_type = F_UNLCK;
    flbuf.l_whence = SEEK_SET;
    flbuf.l_start = 0;
    flbuf.l_len = 0;
    flbuf.l_pid = 0;
    if (fcntl(fd_, F_SETLKW, &flbuf) != 0) {
      status |= GetErrnoStatus("fcntl-unlock", errno);
    }
  }

  // Close the file.
  if (close(fd_) != 0) {
    status |= GetErrnoStatus("close", errno);
  }

  // Updates the internal data.
  fd_ = -1;
  path_.clear();
  file_size_ = 0;
  map_ = nullptr;
  map_size_ = 0;
  writable_ = false;
  open_options_ = 0;

  return status;
}

Status MemoryMapAtomicFileImpl::Truncate(int64_t size) {
  std::lock_guard<SpinSharedMutex> lock(mutex_);
  if (fd_ < 0) {
    return Status(Status::PRECONDITION_ERROR, "not opened file");
  }
  if (!writable_) {
    return Status(Status::PRECONDITION_ERROR, "not writable file");
  }
  int64_t new_map_size =
      std::max(std::max(size, static_cast<int64_t>(PAGE_SIZE)), alloc_init_size_);
  new_map_size = AlignNumber(new_map_size, PAGE_SIZE);
  void* new_map = tkrzw_mremap(map_, map_size_, new_map_size, fd_);
  if (new_map == MAP_FAILED) {
    const Status status = GetErrnoStatus("mremap", errno);
    map_ = nullptr;
    close(fd_);
    fd_ = -1;
    return status;
  }
  map_ = static_cast<char*>(new_map);
  map_size_ = new_map_size;
  const Status status = TruncateFile(fd_, new_map_size);
  if (status != Status::SUCCESS) {
    return status;
  }
  file_size_ = size;
  return Status(Status::SUCCESS);
}

Status MemoryMapAtomicFileImpl::TruncateFakely(int64_t size) {
  std::lock_guard<SpinSharedMutex> lock(mutex_);
  if (fd_ < 0) {
    return Status(Status::PRECONDITION_ERROR, "not opened file");
  }
  if (size > map_size_) {
    return Status(Status::INFEASIBLE_ERROR, "unable to increase the file size");
  }
  file_size_ = size;
  return Status(Status::SUCCESS);
}

Status MemoryMapAtomicFileImpl::Synchronize(bool hard, int64_t off, int64_t size) {
  std::lock_guard<SpinSharedMutex> lock(mutex_);
  if (fd_ < 0) {
    return Status(Status::PRECONDITION_ERROR, "not opened file");
  }
  if (!writable_) {
    return Status(Status::PRECONDITION_ERROR, "not writable file");
  }
  if (file_size_ != map_size_) {
    const int64_t new_map_size = file_size_;
    void* new_map = tkrzw_mremap(map_, map_size_, new_map_size, fd_);
    if (new_map == MAP_FAILED) {
      const Status status = GetErrnoStatus("mremap", errno);
      map_ = nullptr;
      close(fd_);
      fd_ = -1;
      return status;
    }
    AdviseMemoryRandomAccessPattern(new_map, new_map_size);
    map_ = static_cast<char*>(new_map);
    map_size_ = new_map_size;
  }
  Status status = TruncateFile(fd_, file_size_);
  if (hard) {
    if (size == 0) {
      size = map_size_;
    }
    const int64_t end = off + size;
    off -= off % PAGE_SIZE;
    size = end - off;
    off = std::min<int64_t>(map_size_, off);
    size = std::min<int64_t>(map_size_ - off, size);
    if (size > 0 && msync(map_ + off, size, MS_SYNC) != 0) {
      status |= GetErrnoStatus("msync", errno);
    }
  }
  return status;
}

Status MemoryMapAtomicFileImpl::GetSize(int64_t* size) {
  std::shared_lock<SpinSharedMutex> lock(mutex_);
  if (fd_ < 0) {
    return Status(Status::PRECONDITION_ERROR, "not opened file");
  }
  *size = file_size_;
  return Status(Status::SUCCESS);
}

Status MemoryMapAtomicFileImpl::SetAllocationStrategy(int64_t init_size, double inc_factor) {
  std::lock_guard<SpinSharedMutex> lock(mutex_);
  if (fd_ >= 0) {
    return Status(Status::PRECONDITION_ERROR, "alread opened file");
  }
  alloc_init_size_ = init_size;
  alloc_inc_factor_ = inc_factor;
  return Status(Status::SUCCESS);
}

Status MemoryMapAtomicFileImpl::CopyProperties(File* file) {
  std::shared_lock<SpinSharedMutex> lock(mutex_);
  return file->SetAllocationStrategy(alloc_init_size_, alloc_inc_factor_);
}

Status MemoryMapAtomicFileImpl::GetPath(std::string* path) {
  std::shared_lock<SpinSharedMutex> lock(mutex_);
  if (fd_ < 0) {
    return Status(Status::PRECONDITION_ERROR, "not opened file");
  }
  if (path_.empty()) {
    return Status(Status::PRECONDITION_ERROR, "disabled path operatione");
  }
  *path = path_;
  return Status(Status::SUCCESS);
}

Status MemoryMapAtomicFileImpl::Rename(const std::string& new_path) {
  std::lock_guard<SpinSharedMutex> lock(mutex_);
  if (fd_ < 0) {
    return Status(Status::PRECONDITION_ERROR, "not opened file");
  }
  if (path_.empty()) {
    return Status(Status::PRECONDITION_ERROR, "disabled path operatione");
  }
  Status status = RenameFile(path_, new_path);
  if (status == Status::SUCCESS) {
    path_ = new_path;
  }
  return status;
}

Status MemoryMapAtomicFileImpl::DisablePathOperations() {
  std::lock_guard<SpinSharedMutex> lock(mutex_);
  if (fd_ < 0) {
    return Status(Status::PRECONDITION_ERROR, "not opened file");
  }
  path_.clear();
  return Status(Status::SUCCESS);
}

bool MemoryMapAtomicFileImpl::IsOpen() {
  std::shared_lock<SpinSharedMutex> lock(mutex_);
  return fd_ >= 0;
}

Status MemoryMapAtomicFileImpl::AllocateSpace(int64_t min_size) {
  if (min_size <= map_size_) {
    return Status(Status::SUCCESS);
  }
  int64_t new_map_size =
      std::max(std::max(min_size, static_cast<int64_t>(
          map_size_ * alloc_inc_factor_)), static_cast<int64_t>(PAGE_SIZE));
  new_map_size = AlignNumber(new_map_size, PAGE_SIZE);
  Status status = TruncateFile(fd_, new_map_size);
  if (status != Status::SUCCESS) {
    return status;
  }
  void* new_map = tkrzw_mremap(map_, map_size_, new_map_size, fd_);
  if (new_map == MAP_FAILED) {
    status = GetErrnoStatus("mremap", errno);
    map_ = nullptr;
    close(fd_);
    fd_ = -1;
    return status;
  }
  map_ = static_cast<char*>(new_map);
  map_size_ = new_map_size;
  return Status(Status::SUCCESS);
}

MemoryMapAtomicFile::MemoryMapAtomicFile() {
  impl_ = new MemoryMapAtomicFileImpl();
}

MemoryMapAtomicFile::~MemoryMapAtomicFile() {
  delete impl_;
}

Status MemoryMapAtomicFile::Open(const std::string& path, bool writable, int32_t options) {
  return impl_->Open(path, writable, options);
}

Status MemoryMapAtomicFile::Close() {
  return impl_->Close();
}

Status MemoryMapAtomicFile::MakeZone(
    bool writable, int64_t off, size_t size, std::unique_ptr<Zone>* zone) {
  Status status(Status::SUCCESS);
  zone->reset(new Zone(impl_, writable, off, size, &status));
  return status;
}

Status MemoryMapAtomicFile::Read(int64_t off, void* buf, size_t size) {
  assert(off >= 0 && buf != nullptr);
  Status status(Status::SUCCESS);
  Zone zone(impl_, false, off, size, &status);
  if (status != Status::SUCCESS) {
    return status;
  }
  if (zone.Size() != size) {
    return Status(Status::INFEASIBLE_ERROR, "excessive size");
  }
  std::memcpy(buf, zone.Pointer(), zone.Size());
  return Status(Status::SUCCESS);
}

std::string MemoryMapAtomicFile::ReadSimple(int64_t off, size_t size) {
  assert(off >= 0);
  Status status(Status::SUCCESS);
  Zone zone(impl_, false, off, size, &status);
  if (status != Status::SUCCESS || zone.Size() != size) {
    return "";
  }
  std::string result(zone.Pointer(), size);
  return result;
}

Status MemoryMapAtomicFile::Write(int64_t off, const void* buf, size_t size) {
  assert(off >= 0 && buf != nullptr && size <= MAX_MEMORY_SIZE);
  Status status(Status::SUCCESS);
  Zone zone(impl_, true, off, size, &status);
  if (status != Status::SUCCESS) {
    return status;
  }
  std::memcpy(zone.Pointer(), buf, zone.Size());
  return Status(Status::SUCCESS);
}

Status MemoryMapAtomicFile::Append(const void* buf, size_t size, int64_t* off) {
  assert(buf != nullptr && size <= MAX_MEMORY_SIZE);
  Status status(Status::SUCCESS);
  Zone zone(impl_, true, -1, size, &status);
  if (status != Status::SUCCESS) {
    return status;
  }
  std::memcpy(zone.Pointer(), buf, zone.Size());
  if (off != nullptr) {
    *off = zone.Offset();
  }
  return Status(Status::SUCCESS);
}

Status MemoryMapAtomicFile::Expand(size_t inc_size, int64_t* old_size) {
  assert(inc_size <= MAX_MEMORY_SIZE);
  Status status(Status::SUCCESS);
  Zone zone(impl_, true, -1, inc_size, &status);
  if (status != Status::SUCCESS) {
    return status;
  }
  if (old_size != nullptr) {
    *old_size = zone.Offset();
  }
  return Status(Status::SUCCESS);
}

Status MemoryMapAtomicFile::Truncate(int64_t size) {
  assert(size >= 0 && size <= MAX_MEMORY_SIZE);
  return impl_->Truncate(size);
}

Status MemoryMapAtomicFile::TruncateFakely(int64_t size) {
  assert(size >= 0 && size <= MAX_MEMORY_SIZE);
  return impl_->TruncateFakely(size);
}

Status MemoryMapAtomicFile::Synchronize(bool hard, int64_t off, int64_t size) {
  assert(off >= 0 && size >= 0);
  return impl_->Synchronize(hard, off, size);
}

Status MemoryMapAtomicFile::GetSize(int64_t* size) {
  assert(size != nullptr);
  return impl_->GetSize(size);
}

Status MemoryMapAtomicFile::SetAllocationStrategy(int64_t init_size, double inc_factor) {
  assert(init_size >= 0 && inc_factor >= 0);
  return impl_->SetAllocationStrategy(init_size, inc_factor);
}

Status MemoryMapAtomicFile::CopyProperties(File* file) {
  assert(file != nullptr);
  return impl_->CopyProperties(file);
}

Status MemoryMapAtomicFile::GetPath(std::string* path) {
  assert(path != nullptr);
  return impl_->GetPath(path);
}

Status MemoryMapAtomicFile::Rename(const std::string& new_path) {
  return impl_->Rename(new_path);
}

Status MemoryMapAtomicFile::DisablePathOperations() {
  return impl_->DisablePathOperations();
}

bool MemoryMapAtomicFile::IsOpen() const {
  return impl_->IsOpen();
}

MemoryMapAtomicFile::Zone::Zone(
    MemoryMapAtomicFileImpl* file, bool writable, int64_t off, size_t size, Status* status)
    : file_(file), off_(-1), size_(0), writable_(writable) {
  if (writable) {
    file_->mutex_.lock();
  } else {
    file_->mutex_.lock_shared();
  }
  if (file_->fd_ < 0) {
    status->Set(Status::PRECONDITION_ERROR, "not opened file");
    return;
  }
  if (writable) {
    if (!file_->writable_) {
      status->Set(Status::PRECONDITION_ERROR, "not writable file");
      return;
    }
    if (off < 0) {
      off = file_->file_size_;
    }
    const int64_t end_position = off + size;
    const Status adjust_status = file->AllocateSpace(end_position);
    if (adjust_status != Status::SUCCESS) {
      *status = adjust_status;
      return;
    }
    file_->file_size_ = std::max(file_->file_size_, end_position);
  } else {
    if (off < 0) {
      status->Set(Status::PRECONDITION_ERROR, "negative offset");
      return;
    }
    if (off > file_->file_size_) {
      status->Set(Status::INFEASIBLE_ERROR, "excessive offset");
      return;
    }
    size = std::min(static_cast<int64_t>(size), file_->file_size_ - off);
  }
  off_ = off;
  size_ = size;
}

MemoryMapAtomicFile::Zone::~Zone() {
  if (writable_) {
    file_->mutex_.unlock();
  } else {
    file_->mutex_.unlock_shared();
  }
}

int64_t MemoryMapAtomicFile::Zone::Offset() const {
  return off_;
}

char* MemoryMapAtomicFile::Zone::Pointer() const {
  return file_->map_ + off_;
}

size_t MemoryMapAtomicFile::Zone::Size() const {
  return size_;
}

}  // namespace tkrzw

#endif

// END OF FILE
