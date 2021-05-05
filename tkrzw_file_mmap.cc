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
#include "tkrzw_thread_util.h"

namespace tkrzw {

#if defined(_SYS_LINUX_)

inline void* tkrzw_mremap(
    void *old_address, size_t old_size, size_t new_size, int fd) {
  return mremap(old_address, old_size, new_size, MREMAP_MAYMOVE);
}

#else

inline void* tkrzw_mremap(
    void *old_address, size_t old_size, size_t new_size, int fd) {
  if (munmap(old_address, old_size) != 0) {
    return MAP_FAILED;
  }
  return mmap(0, new_size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
}

#endif

class MemoryMapParallelFileImpl final {
  friend class MemoryMapParallelFileZoneImpl;
 public:
  MemoryMapParallelFileImpl();
  ~MemoryMapParallelFileImpl();
  Status Open(const std::string& path, bool writable, int32_t options);
  Status Close();
  Status Truncate(int64_t size);
  Status Synchronize(bool hard);
  Status GetSize(int64_t* size);
  Status SetAllocationStrategy(int64_t init_size, double inc_factor);
  Status LockMemory(size_t size);
  Status GetPath(std::string* path);
  Status Rename(const std::string& new_path);

 private:
  Status AllocateSpace(int64_t min_size);

  int32_t fd_;
  std::string path_;
  std::atomic_int64_t file_size_;
  char* map_;
  std::atomic_int64_t map_size_;
  std::atomic_int64_t lock_size_;
  bool writable_;
  int32_t open_options_;
  int64_t alloc_init_size_;
  double alloc_inc_factor_;
  std::shared_timed_mutex mutex_;
};

class MemoryMapParallelFileZoneImpl final {
 public:
  MemoryMapParallelFileZoneImpl(
      MemoryMapParallelFileImpl* file, bool writable, int64_t off, size_t size, Status* status);
  ~MemoryMapParallelFileZoneImpl();
  void SetLockedMutex(std::shared_timed_mutex* mutex);
  int64_t Offset() const;
  char* Pointer() const;
  size_t Size() const;

 private:
  MemoryMapParallelFileImpl* file_;
  int64_t off_;
  size_t size_;
  bool writable_;
};

MemoryMapParallelFileImpl::MemoryMapParallelFileImpl() :
    fd_(-1), file_size_(-0), map_(nullptr), map_size_(0), lock_size_(0),
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
    map_size = std::max(map_size, alloc_init_size_);
    const int64_t diff = map_size % PAGE_SIZE;
    if (diff > 0) {
      map_size += PAGE_SIZE - diff;
    }
    mprot |= PROT_WRITE;
    if (ftruncate(fd, map_size) != 0) {
      const Status status = GetErrnoStatus("ftruncate", errno);
      close(fd);
      return status;
    }
  } else {
    map_size = std::max(map_size, static_cast<int64_t>(PAGE_SIZE));
  }
  void* map = mmap(0, map_size, mprot, MAP_SHARED, fd, 0);
  if (map == MAP_FAILED) {
    const Status status = GetErrnoStatus("mmap", errno);
    close(fd);
    return status;
  }

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
  if (munmap(map_, unmap_size) != 0) {
    status |= GetErrnoStatus("munmap", errno);
  }

  // Truncates the file.
  if (writable_ && ftruncate(fd_, file_size_.load()) != 0) {
    status |= GetErrnoStatus("ftruncate", errno);
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
  lock_size_.store(0);
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
  if (lock_size_.load() > 0 && munlock(map_, lock_size_.load()) != 0) {
    return GetErrnoStatus("munlock", errno);
  }
  lock_size_.store(0);
  int64_t new_map_size =
      std::max(std::max(size, static_cast<int64_t>(PAGE_SIZE)), alloc_init_size_);
  const int64_t diff = new_map_size % PAGE_SIZE;
  if (diff > 0) {
    new_map_size += PAGE_SIZE - diff;
  }
  void* new_map = tkrzw_mremap(map_, map_size_.load(), new_map_size, fd_);
  if (new_map == MAP_FAILED) {
    const Status status = GetErrnoStatus("mremap", errno);
    map_ = nullptr;
    close(fd_);
    fd_ = -1;
    return status;
  }
  map_ = static_cast<char*>(new_map);
  map_size_.store(new_map_size);
  if (ftruncate(fd_, new_map_size) != 0) {
    return GetErrnoStatus("ftruncate", errno);
  }
  file_size_.store(size);
  return Status(Status::SUCCESS);
}

Status MemoryMapParallelFileImpl::Synchronize(bool hard) {
  if (fd_ < 0) {
    return Status(Status::PRECONDITION_ERROR, "not opened file");
  }
  if (!writable_) {
    return Status(Status::PRECONDITION_ERROR, "not writable file");
  }
  std::lock_guard<std::shared_timed_mutex> lock(mutex_);
  Status status(Status::SUCCESS);
  map_size_.store(file_size_.load());
  if (ftruncate(fd_, map_size_.load()) != 0) {
    status |= GetErrnoStatus("ftruncate", errno);
  }
  if (hard) {
    if (msync(map_, map_size_, MS_SYNC) != 0) {
      status |= GetErrnoStatus("msync", errno);
    }
    if (fsync(fd_) != 0) {
      status |= GetErrnoStatus("fsync", errno);
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
  alloc_init_size_ = std::max<int64_t>(1, init_size);
  alloc_inc_factor_ = std::max<int64_t>(1.1, inc_factor);
  return Status(Status::SUCCESS);
}

Status MemoryMapParallelFileImpl::LockMemory(size_t size) {
  if (fd_ < 0) {
    return Status(Status::PRECONDITION_ERROR, "not opened file");
  }
  if (lock_size_.load() > 0 && munlock(map_, lock_size_.load()) != 0) {
    return GetErrnoStatus("munlock", errno);
  }
  lock_size_.store(0);
  if (size > 0 && mlock(map_, size) != 0) {
    return GetErrnoStatus("mlock", errno);
  }
  lock_size_.store(size);
  return Status(Status::SUCCESS);
}

Status MemoryMapParallelFileImpl::GetPath(std::string* path) {
  if (fd_ < 0) {
    return Status(Status::PRECONDITION_ERROR, "not opened file");
  }
  *path = path_;
  return Status(Status::SUCCESS);
}

Status MemoryMapParallelFileImpl::Rename(const std::string& new_path) {
  if (fd_ < 0) {
    return Status(Status::PRECONDITION_ERROR, "not opened file");
  }
  Status status = RenameFile(path_, new_path);
  if (status == Status::SUCCESS) {
    path_ = new_path;
  }
  return status;
}

Status MemoryMapParallelFileImpl::AllocateSpace(int64_t min_size) {
  if (min_size <= map_size_.load()) {
    return Status(Status::SUCCESS);
  }
  std::lock_guard<std::shared_timed_mutex> lock(mutex_);
  if (min_size <= map_size_.load()) {
    return Status(Status::SUCCESS);
  }
  if (lock_size_.load() > 0 && munlock(map_, lock_size_.load()) != 0) {
    return GetErrnoStatus("munlock", errno);
  }
  int64_t new_map_size =
      std::max(std::max(min_size, static_cast<int64_t>(
          map_size_.load() * alloc_inc_factor_)), static_cast<int64_t>(PAGE_SIZE));
  const int64_t diff = new_map_size % PAGE_SIZE;
  if (diff > 0) {
    new_map_size += PAGE_SIZE - diff;
  }
  if (ftruncate(fd_, new_map_size) != 0) {
    return GetErrnoStatus("ftruncate", errno);
  }
  void* new_map = tkrzw_mremap(map_, map_size_.load(), new_map_size, fd_);
  if (new_map == MAP_FAILED) {
    const Status status = GetErrnoStatus("mremap", errno);
    map_ = nullptr;
    close(fd_);
    fd_ = -1;
    return status;
  }
  map_ = static_cast<char*>(new_map);
  map_size_.store(new_map_size);
  if (lock_size_.load() > 0 && mlock(map_, lock_size_.load()) != 0) {
    lock_size_.store(0);
    return GetErrnoStatus("mlock", errno);
  }
  return Status(Status::SUCCESS);
}

MemoryMapParallelFileZoneImpl::MemoryMapParallelFileZoneImpl(
    MemoryMapParallelFileImpl* file, bool writable, int64_t off, size_t size, Status* status)
    : file_(nullptr), off_(-1), size_(0), writable_(writable) {
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

MemoryMapParallelFileZoneImpl::~MemoryMapParallelFileZoneImpl() {
  if (file_ != nullptr) {
    file_->mutex_.unlock_shared();
  }
}

int64_t MemoryMapParallelFileZoneImpl::Offset() const {
  return off_;
}

char* MemoryMapParallelFileZoneImpl::Pointer() const {
  return file_->map_ + off_;
}

size_t MemoryMapParallelFileZoneImpl::Size() const {
  return size_;
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
  std::unique_ptr<Zone> zone;
  Status status = MakeZone(false, off, size, &zone);
  if (status != Status::SUCCESS) {
    return status;
  }
  if (zone->Size() != size) {
    return Status(Status::INFEASIBLE_ERROR, "excessive size");
  }
  std::memcpy(buf, zone->Pointer(), zone->Size());
  return Status(Status::SUCCESS);
}

std::string MemoryMapParallelFile::ReadSimple(int64_t off, size_t size) {
  assert(off >= 0);
  std::unique_ptr<Zone> zone;
  Status status = MakeZone(false, off, size, &zone);
  if (status != Status::SUCCESS || zone->Size() != size) {
    return "";
  }
  std::string result(zone->Pointer(), size);
  return result;
}

Status MemoryMapParallelFile::Write(int64_t off, const void* buf, size_t size) {
  assert(off >= 0 && buf != nullptr && size <= MAX_MEMORY_SIZE);
  std::unique_ptr<Zone> zone;
  Status status = MakeZone(true, off, size, &zone);
  if (status != Status::SUCCESS) {
    return status;
  }
  std::memcpy(zone->Pointer(), buf, zone->Size());
  return Status(Status::SUCCESS);
}

Status MemoryMapParallelFile::Append(const void* buf, size_t size, int64_t* off) {
  assert(buf != nullptr && size <= MAX_MEMORY_SIZE);
  std::unique_ptr<Zone> zone;
  Status status = MakeZone(true, -1, size, &zone);
  if (status != Status::SUCCESS) {
    return status;
  }
  std::memcpy(zone->Pointer(), buf, zone->Size());
  if (off != nullptr) {
    *off = zone->Offset();
  }
  return Status(Status::SUCCESS);
}

Status MemoryMapParallelFile::Expand(size_t inc_size, int64_t* old_size) {
  assert(inc_size <= MAX_MEMORY_SIZE);
  std::unique_ptr<Zone> zone;
  Status status = MakeZone(true, -1, inc_size, &zone);
  if (status != Status::SUCCESS) {
    return status;
  }
  if (old_size != nullptr) {
    *old_size = zone->Offset();
  }
  return Status(Status::SUCCESS);
}

Status MemoryMapParallelFile::Truncate(int64_t size) {
  assert(size >= 0 && size <= MAX_MEMORY_SIZE);
  return impl_->Truncate(size);
}

Status MemoryMapParallelFile::Synchronize(bool hard) {
  return impl_->Synchronize(hard);
}

Status MemoryMapParallelFile::GetSize(int64_t* size) {
  assert(size != nullptr);
  return impl_->GetSize(size);
}

Status MemoryMapParallelFile::SetAllocationStrategy(int64_t init_size, double inc_factor) {
  assert(init_size > 0 && inc_factor > 0);
  return impl_->SetAllocationStrategy(init_size, inc_factor);
}

Status MemoryMapParallelFile::LockMemory(size_t size) {
  assert(size <= MAX_MEMORY_SIZE);
  return impl_->LockMemory(size);
}

Status MemoryMapParallelFile::GetPath(std::string* path) {
  assert(path != nullptr);
  return impl_->GetPath(path);
}

Status MemoryMapParallelFile::Rename(const std::string& new_path) {
  return impl_->Rename(new_path);
}

MemoryMapParallelFile::Zone::Zone(
    MemoryMapParallelFileImpl* file_impl, bool writable, int64_t off, size_t size,
    Status* status) {
  impl_ = new MemoryMapParallelFileZoneImpl(file_impl, writable, off, size, status);
}

MemoryMapParallelFile::Zone::~Zone() {
  delete impl_;
}

int64_t MemoryMapParallelFile::Zone::Offset() const {
  return impl_->Offset();
}

char* MemoryMapParallelFile::Zone::Pointer() const {
  return impl_->Pointer();
}

size_t MemoryMapParallelFile::Zone::Size() const {
  return impl_->Size();
}

class MemoryMapAtomicFileImpl final {
  friend class MemoryMapAtomicFileZoneImpl;
 public:
  MemoryMapAtomicFileImpl();
  ~MemoryMapAtomicFileImpl();
  Status Open(const std::string& path, bool writable, int32_t options);
  Status Close();
  Status Truncate(int64_t size);
  Status Synchronize(bool hard);
  Status GetSize(int64_t* size);
  Status SetAllocationStrategy(int64_t init_size, double inc_factor);
  Status LockMemory(size_t size);
  Status GetPath(std::string* path);
  Status Rename(const std::string& new_path);

 private:
  Status AllocateSpace(int64_t min_size);

  int32_t fd_;
  std::string path_;
  int64_t file_size_;
  char* map_;
  int64_t map_size_;
  int64_t lock_size_;
  bool writable_;
  int32_t open_options_;
  int64_t alloc_init_size_;
  double alloc_inc_factor_;
  std::shared_timed_mutex mutex_;
};

class MemoryMapAtomicFileZoneImpl final {
 public:
  MemoryMapAtomicFileZoneImpl(
      MemoryMapAtomicFileImpl* file, bool writable, int64_t off, size_t size, Status* status);
  ~MemoryMapAtomicFileZoneImpl();
  void SetLockedMutex(std::shared_timed_mutex* mutex);
  int64_t Offset() const;
  char* Pointer() const;
  size_t Size() const;

 private:
  MemoryMapAtomicFileImpl* file_;
  int64_t off_;
  size_t size_;
  bool writable_;
};

MemoryMapAtomicFileImpl::MemoryMapAtomicFileImpl() :
    fd_(-1), file_size_(0), map_(nullptr), map_size_(0), lock_size_(0),
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
  std::lock_guard<std::shared_timed_mutex> lock(mutex_);
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
    map_size = std::max(map_size, alloc_init_size_);
    const int64_t diff = map_size % PAGE_SIZE;
    if (diff > 0) {
      map_size += PAGE_SIZE - diff;
    }
    mprot |= PROT_WRITE;
    if (ftruncate(fd, map_size) != 0) {
      const Status status = GetErrnoStatus("ftruncate", errno);
      close(fd);
      return status;
    }
  } else {
    map_size = std::max(map_size, static_cast<int64_t>(PAGE_SIZE));
  }
  void* map = mmap(0, map_size, mprot, MAP_SHARED, fd, 0);
  if (map == MAP_FAILED) {
    const Status status = GetErrnoStatus("mmap", errno);
    close(fd);
    return status;
  }

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
  std::lock_guard<std::shared_timed_mutex> lock(mutex_);
  if (fd_ < 0) {
    return Status(Status::PRECONDITION_ERROR, "not opened file");
  }
  Status status(Status::SUCCESS);

  // Unmaps the memory.
  const int64_t unmap_size = std::max(map_size_, static_cast<int64_t>(PAGE_SIZE));
  if (munmap(map_, unmap_size) != 0) {
    status |= GetErrnoStatus("munmap", errno);
  }

  // Truncates the file.
  if (writable_ && ftruncate(fd_, file_size_) != 0) {
    status |= GetErrnoStatus("ftruncate", errno);
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
  lock_size_ = 0;
  writable_ = false;
  open_options_ = 0;

  return status;
}

Status MemoryMapAtomicFileImpl::Truncate(int64_t size) {
  std::lock_guard<std::shared_timed_mutex> lock(mutex_);
  if (fd_ < 0) {
    return Status(Status::PRECONDITION_ERROR, "not opened file");
  }
  if (!writable_) {
    return Status(Status::PRECONDITION_ERROR, "not writable file");
  }
  if (lock_size_ > 0 && munlock(map_, lock_size_) != 0) {
    return GetErrnoStatus("munlock", errno);
  }
  lock_size_ = 0;
  int64_t new_map_size =
      std::max(std::max(size, static_cast<int64_t>(PAGE_SIZE)), alloc_init_size_);
  const int64_t diff = new_map_size % PAGE_SIZE;
  if (diff > 0) {
    new_map_size += PAGE_SIZE - diff;
  }
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
  if (ftruncate(fd_, new_map_size) != 0) {
    return GetErrnoStatus("ftruncate", errno);
  }
  file_size_ = size;
  return Status(Status::SUCCESS);
}

Status MemoryMapAtomicFileImpl::Synchronize(bool hard) {
  std::lock_guard<std::shared_timed_mutex> lock(mutex_);
  if (fd_ < 0) {
    return Status(Status::PRECONDITION_ERROR, "not opened file");
  }
  if (!writable_) {
    return Status(Status::PRECONDITION_ERROR, "not writable file");
  }
  Status status(Status::SUCCESS);
  map_size_ = file_size_;
  if (ftruncate(fd_, map_size_) != 0) {
    status |= GetErrnoStatus("ftruncate", errno);
  }
  if (hard) {
    if (msync(map_, map_size_, MS_SYNC) != 0) {
      status |= GetErrnoStatus("msync", errno);
    }
    if (fsync(fd_) != 0) {
      status |= GetErrnoStatus("fsync", errno);
    }
  }
  return status;
}

Status MemoryMapAtomicFileImpl::GetSize(int64_t* size) {
  std::shared_lock<std::shared_timed_mutex> lock(mutex_);
  if (fd_ < 0) {
    return Status(Status::PRECONDITION_ERROR, "not opened file");
  }
  *size = file_size_;
  return Status(Status::SUCCESS);
}

Status MemoryMapAtomicFileImpl::SetAllocationStrategy(int64_t init_size, double inc_factor) {
  std::lock_guard<std::shared_timed_mutex> lock(mutex_);
  if (fd_ >= 0) {
    return Status(Status::PRECONDITION_ERROR, "alread opened file");
  }
  alloc_init_size_ = std::max<int64_t>(1, init_size);
  alloc_inc_factor_ = std::max<double>(1.1, inc_factor);
  return Status(Status::SUCCESS);
}

Status MemoryMapAtomicFileImpl::GetPath(std::string* path) {
  std::shared_lock<std::shared_timed_mutex> lock(mutex_);
  if (fd_ < 0) {
    return Status(Status::PRECONDITION_ERROR, "not opened file");
  }
  *path = path_;
  return Status(Status::SUCCESS);
}

Status MemoryMapAtomicFileImpl::Rename(const std::string& new_path) {
  std::lock_guard<std::shared_timed_mutex> lock(mutex_);
  if (fd_ < 0) {
    return Status(Status::PRECONDITION_ERROR, "not opened file");
  }
  Status status = RenameFile(path_, new_path);
  if (status == Status::SUCCESS) {
    path_ = new_path;
  }
  return status;
}

Status MemoryMapAtomicFileImpl::LockMemory(size_t size) {
  std::lock_guard<std::shared_timed_mutex> lock(mutex_);
  if (fd_ < 0) {
    return Status(Status::PRECONDITION_ERROR, "not opened file");
  }
  if (lock_size_ > 0 && munlock(map_, lock_size_) != 0) {
    return GetErrnoStatus("munlock", errno);
  }
  lock_size_ = 0;
  if (size > 0 && mlock(map_, size) != 0) {
    return GetErrnoStatus("mlock", errno);
  }
  lock_size_ = size;
  return Status(Status::SUCCESS);
}

Status MemoryMapAtomicFileImpl::AllocateSpace(int64_t min_size) {
  if (min_size <= map_size_) {
    return Status(Status::SUCCESS);
  }
  if (lock_size_ > 0 && munlock(map_, lock_size_) != 0) {
    return GetErrnoStatus("munlock", errno);
  }
  int64_t new_map_size =
      std::max(std::max(min_size, static_cast<int64_t>(
          map_size_ * alloc_inc_factor_)), static_cast<int64_t>(PAGE_SIZE));
  const int64_t diff = new_map_size % PAGE_SIZE;
  if (diff > 0) {
    new_map_size += PAGE_SIZE - diff;
  }
  if (ftruncate(fd_, new_map_size) != 0) {
    return GetErrnoStatus("ftruncate", errno);
  }
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
  if (lock_size_ > 0 && mlock(map_, lock_size_) != 0) {
    lock_size_ = 0;
    return GetErrnoStatus("mlock", errno);
  }
  return Status(Status::SUCCESS);
}

MemoryMapAtomicFileZoneImpl::MemoryMapAtomicFileZoneImpl(
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

MemoryMapAtomicFileZoneImpl::~MemoryMapAtomicFileZoneImpl() {
  if (writable_) {
    file_->mutex_.unlock();
  } else {
    file_->mutex_.unlock_shared();
  }
}

int64_t MemoryMapAtomicFileZoneImpl::Offset() const {
  return off_;
}

char* MemoryMapAtomicFileZoneImpl::Pointer() const {
  return file_->map_ + off_;
}

size_t MemoryMapAtomicFileZoneImpl::Size() const {
  return size_;
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
  std::unique_ptr<Zone> zone;
  Status status = MakeZone(false, off, size, &zone);
  if (status != Status::SUCCESS) {
    return status;
  }
  if (zone->Size() != size) {
    return Status(Status::INFEASIBLE_ERROR, "excessive size");
  }
  std::memcpy(buf, zone->Pointer(), zone->Size());
  return Status(Status::SUCCESS);
}

std::string MemoryMapAtomicFile::ReadSimple(int64_t off, size_t size) {
  assert(off >= 0);
  std::unique_ptr<Zone> zone;
  Status status = MakeZone(false, off, size, &zone);
  if (status != Status::SUCCESS || zone->Size() != size) {
    return "";
  }
  std::string result(zone->Pointer(), size);
  return result;
}

Status MemoryMapAtomicFile::Write(int64_t off, const void* buf, size_t size) {
  assert(off >= 0 && buf != nullptr && size <= MAX_MEMORY_SIZE);
  std::unique_ptr<Zone> zone;
  Status status = MakeZone(true, off, size, &zone);
  if (status != Status::SUCCESS) {
    return status;
  }
  std::memcpy(zone->Pointer(), buf, zone->Size());
  return Status(Status::SUCCESS);
}

Status MemoryMapAtomicFile::Append(const void* buf, size_t size, int64_t* off) {
  assert(buf != nullptr && size <= MAX_MEMORY_SIZE);
  std::unique_ptr<Zone> zone;
  Status status = MakeZone(true, -1, size, &zone);
  if (status != Status::SUCCESS) {
    return status;
  }
  std::memcpy(zone->Pointer(), buf, zone->Size());
  if (off != nullptr) {
    *off = zone->Offset();
  }
  return Status(Status::SUCCESS);
}

Status MemoryMapAtomicFile::Expand(size_t inc_size, int64_t* old_size) {
  assert(inc_size <= MAX_MEMORY_SIZE);
  std::unique_ptr<Zone> zone;
  Status status = MakeZone(true, -1, inc_size, &zone);
  if (status != Status::SUCCESS) {
    return status;
  }
  if (old_size != nullptr) {
    *old_size = zone->Offset();
  }
  return Status(Status::SUCCESS);
}

Status MemoryMapAtomicFile::Truncate(int64_t size) {
  assert(size >= 0 && size <= MAX_MEMORY_SIZE);
  return impl_->Truncate(size);
}

Status MemoryMapAtomicFile::Synchronize(bool hard) {
  return impl_->Synchronize(hard);
}

Status MemoryMapAtomicFile::GetSize(int64_t* size) {
  assert(size != nullptr);
  return impl_->GetSize(size);
}

Status MemoryMapAtomicFile::SetAllocationStrategy(int64_t init_size, double inc_factor) {
  assert(init_size > 0 && inc_factor > 0);
  return impl_->SetAllocationStrategy(init_size, inc_factor);
}

Status MemoryMapAtomicFile::LockMemory(size_t size) {
  assert(size <= MAX_MEMORY_SIZE);
  return impl_->LockMemory(size);
}

Status MemoryMapAtomicFile::GetPath(std::string* path) {
  assert(path != nullptr);
  return impl_->GetPath(path);
}

Status MemoryMapAtomicFile::Rename(const std::string& new_path) {
  return impl_->Rename(new_path);
}

MemoryMapAtomicFile::Zone::Zone(
    MemoryMapAtomicFileImpl* file_impl, bool writable, int64_t off, size_t size, Status* status) {
  impl_ = new MemoryMapAtomicFileZoneImpl(file_impl, writable, off, size, status);
}

MemoryMapAtomicFile::Zone::~Zone() {
  delete impl_;
}

int64_t MemoryMapAtomicFile::Zone::Offset() const {
  return impl_->Offset();
}

char* MemoryMapAtomicFile::Zone::Pointer() const {
  return impl_->Pointer();
}

size_t MemoryMapAtomicFile::Zone::Size() const {
  return impl_->Size();
}

}  // namespace tkrzw

#endif

// END OF FILE
