/*************************************************************************************************
 * File implementations by block-aligned direct access
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

#include "tkrzw_sys_file_block_std.h"

#elif defined(_SYS_WINDOWS_)

#include "tkrzw_sys_file_block_std.h"

#else

#include "tkrzw_file.h"
#include "tkrzw_file_block.h"
#include "tkrzw_file_util.h"
#include "tkrzw_lib_common.h"
#include "tkrzw_str_util.h"
#include "tkrzw_sys_util_posix.h"
#include "tkrzw_thread_util.h"

namespace tkrzw {

inline void SetDirectAccessToOpenFlag(int32_t* oflags) {
#if defined(_SYS_LINUX_)
  *oflags |= O_DIRECT;
#endif
}


class BlockParallelFileImpl final {
 public:
  BlockParallelFileImpl();
  ~BlockParallelFileImpl();
  Status Open(const std::string& path, bool writable, int32_t options);
  Status Close();
  Status Read(int64_t off, void* buf, size_t size);
  Status Write(int64_t off, const void* buf, size_t size);
  Status Append(const void* buf, size_t size, int64_t* off);
  Status Truncate(int64_t size);
  Status Synchronize(bool hard);
  Status GetSize(int64_t* size);
  Status SetHeadBuffer(int64_t size);
  Status SetAccessStrategy(int64_t block_size, int32_t options);
  Status SetAllocationStrategy(int64_t init_size, double inc_factor);
  Status GetPath(std::string* path);
  Status Rename(const std::string& new_path);

 private:
  Status AllocateSpace(int64_t min_size);
  Status ReadImpl(int64_t off, char* buf, size_t size);
  Status WriteImpl(int64_t off, const char* buf, size_t size);

  std::atomic_int32_t fd_;
  char* head_buffer_;
  std::atomic_int64_t head_buffer_size_;
  std::string path_;
  std::atomic_int64_t file_size_;
  std::atomic_int64_t trunc_size_;
  bool writable_;
  int32_t open_options_;
  int64_t block_size_;
  int32_t access_options_;
  int64_t alloc_init_size_;
  double alloc_inc_factor_;
  std::mutex mutex_;
};

BlockParallelFileImpl::BlockParallelFileImpl()
    : fd_(-1), head_buffer_(nullptr), head_buffer_size_(0),
      file_size_(0), trunc_size_(0), writable_(false), open_options_(0),
      block_size_(BlockParallelFile::DEFAULT_BLOCK_SIZE), access_options_(0),
      alloc_init_size_(File::DEFAULT_ALLOC_INIT_SIZE),
      alloc_inc_factor_(File::DEFAULT_ALLOC_INC_FACTOR), mutex_() {}

BlockParallelFileImpl::~BlockParallelFileImpl() {
  if (fd_ >= 0) {
    Close();
  }
}

Status BlockParallelFileImpl::Open(const std::string& path, bool writable, int32_t options) {
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
  if (access_options_ & BlockParallelFile::ACCESS_DIRECT) {
    SetDirectAccessToOpenFlag(&oflags);
  }
  if (access_options_ & BlockParallelFile::ACCESS_SYNC) {
    oflags |= O_SYNC;
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

  // Truncates and synchronizes the file.
  int64_t trunc_size = file_size;
  if (writable) {
    trunc_size = std::max(trunc_size, alloc_init_size_);
    const int64_t alignment = std::max<int64_t>(PAGE_SIZE, block_size_);
    const int64_t diff = trunc_size % alignment;
    if (diff > 0) {
      trunc_size += alignment - diff;
    }
    if (ftruncate(fd, trunc_size) != 0) {
      const Status status = GetErrnoStatus("ftruncate", errno);
      close(fd);
      return status;
    }
  }

  // Updates the internal data.
  fd_ = fd;
  head_buffer_ = nullptr;
  head_buffer_size_.store(0);
  path_ = path;
  file_size_.store(file_size);
  trunc_size_.store(trunc_size);
  writable_ = writable;
  open_options_ = options;

  return Status(Status::SUCCESS);
}

Status BlockParallelFileImpl::Close() {
  if (fd_ < 0) {
    return Status(Status::PRECONDITION_ERROR, "not opened file");
  }
  Status status(Status::SUCCESS);

  // Truncates the file.
  if (writable_) {
    if (head_buffer_ != nullptr) {
      status |= PWriteSequence(fd_, 0, head_buffer_, head_buffer_size_.load());
    }
    if (file_size_.load() % block_size_ == 0) {
      if (ftruncate(fd_, file_size_.load()) != 0) {
        status |= GetErrnoStatus("ftruncate", errno);
      }
    } else {
      if (truncate(path_.c_str(), file_size_.load()) != 0) {
        status |= GetErrnoStatus("truncate", errno);
      }
    }
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

  // Releases the head buffer.
  if (head_buffer_ != nullptr) {
    xfreealigned(head_buffer_);
  }

  // Close the file.
  if (close(fd_) != 0) {
    status |= GetErrnoStatus("close", errno);
  }

  // Updates the internal data.
  fd_ = -1;
  head_buffer_ = nullptr;
  head_buffer_size_.store(0);
  path_.clear();
  file_size_.store(0);
  trunc_size_.store(0);
  writable_ = false;
  open_options_ = 0;

  return status;
}

Status BlockParallelFileImpl::Read(int64_t off, void* buf, size_t size) {
  if (fd_ < 0) {
    return Status(Status::PRECONDITION_ERROR, "not opened file");
  }
  return ReadImpl(off, static_cast<char*>(buf), size);
}

Status BlockParallelFileImpl::Write(int64_t off, const void* buf, size_t size) {
  if (fd_ < 0) {
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
  return WriteImpl(off, static_cast<const char*>(buf), size);
}

Status BlockParallelFileImpl::Append(const void* buf, size_t size, int64_t* off) {
  if (fd_ < 0) {
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
  if (buf == nullptr) {
    return Status(Status::SUCCESS);
  }
  return WriteImpl(position, static_cast<const char*>(buf), size);
}

Status BlockParallelFileImpl::Truncate(int64_t size) {
  if (fd_ < 0) {
    return Status(Status::PRECONDITION_ERROR, "not opened file");
  }
  if (!writable_) {
    return Status(Status::PRECONDITION_ERROR, "not writable file");
  }
  int64_t new_trunc_size =
      std::max(std::max(size, static_cast<int64_t>(PAGE_SIZE)), alloc_init_size_);
  const int64_t alignment = std::max<int64_t>(PAGE_SIZE, block_size_);
  const int64_t diff = new_trunc_size % alignment;
  if (diff > 0) {
    new_trunc_size += alignment - diff;
  }
  if (ftruncate(fd_, new_trunc_size) != 0) {
    return GetErrnoStatus("ftruncate", errno);
  }
  file_size_.store(size);
  trunc_size_.store(new_trunc_size);
  return Status(Status::SUCCESS);
}

Status BlockParallelFileImpl::Synchronize(bool hard) {
  if (fd_ < 0) {
    return Status(Status::PRECONDITION_ERROR, "not opened file");
  }
  if (!writable_) {
    return Status(Status::PRECONDITION_ERROR, "not writable file");
  }
  Status status(Status::SUCCESS);
  if (head_buffer_ != nullptr) {
    status |= PWriteSequence(fd_, 0, head_buffer_, head_buffer_size_.load());
  }
  trunc_size_.store(file_size_.load());
  if (trunc_size_.load() % block_size_ == 0) {
    if (ftruncate(fd_, trunc_size_.load()) != 0) {
      status |= GetErrnoStatus("ftruncate", errno);
    }
  } else {
    if (truncate(path_.c_str(), trunc_size_.load()) != 0) {
      status |= GetErrnoStatus("truncate", errno);
    }
  }
  if (hard && fsync(fd_) != 0) {
    status |= GetErrnoStatus("fsync", errno);
  }
  return status;
}

Status BlockParallelFileImpl::GetSize(int64_t* size) {
  if (fd_ < 0) {
    return Status(Status::PRECONDITION_ERROR, "not opened file");
  }
  *size = file_size_.load();
  return Status(Status::SUCCESS);
}

Status BlockParallelFileImpl::SetHeadBuffer(int64_t size) {
  if (fd_ < 0) {
    return Status(Status::PRECONDITION_ERROR, "not opened file");
  }
  Status status(Status::SUCCESS);
  if (head_buffer_ != nullptr) {
    status |= PWriteSequence(fd_, 0, head_buffer_, head_buffer_size_.load());
    xfreealigned(head_buffer_);
  }
  if (size < 1) {
    head_buffer_size_.store(0);
    head_buffer_ = nullptr;
    return status;
  }
  const int64_t diff = size % block_size_;
  if (diff > 0) {
    size += block_size_ - diff;
  }
  head_buffer_size_.store(size);
  head_buffer_ = static_cast<char*>(xmallocaligned(block_size_, head_buffer_size_.load()));
  std::memset(head_buffer_, 0, head_buffer_size_.load());
  const int64_t read_size = std::min<int64_t>(head_buffer_size_.load(), file_size_.load());
  status |= PReadSequence(fd_, 0, head_buffer_, read_size);
  return status;
}

Status BlockParallelFileImpl::SetAccessStrategy(int64_t block_size, int32_t options) {
  if (fd_ >= 0) {
    return Status(Status::PRECONDITION_ERROR, "alread opened file");
  }
  block_size_ = block_size;
  access_options_ = options;
  return Status(Status::SUCCESS);
}

Status BlockParallelFileImpl::SetAllocationStrategy(int64_t init_size, double inc_factor) {
  if (fd_ >= 0) {
    return Status(Status::PRECONDITION_ERROR, "alread opened file");
  }
  alloc_init_size_ = init_size;
  alloc_inc_factor_ = inc_factor;
  return Status(Status::SUCCESS);
}

Status BlockParallelFileImpl::GetPath(std::string* path) {
  if (fd_ < 0) {
    return Status(Status::PRECONDITION_ERROR, "not opened file");
  }
  *path = path_;
  return Status(Status::SUCCESS);
}

Status BlockParallelFileImpl::Rename(const std::string& new_path) {
  if (fd_ < 0) {
    return Status(Status::PRECONDITION_ERROR, "not opened file");
  }
  Status status = RenameFile(path_, new_path);
  if (status == Status::SUCCESS) {
    path_ = new_path;
  }
  return status;
}

Status BlockParallelFileImpl::AllocateSpace(int64_t min_size) {
  int64_t diff = min_size % block_size_;
  if (diff > 0) {
    min_size += block_size_ - diff;
  }
  if (min_size <= trunc_size_.load()) {
    return Status(Status::SUCCESS);
  }
  std::lock_guard<std::mutex> lock(mutex_);
  if (min_size <= trunc_size_.load()) {
    return Status(Status::SUCCESS);
  }
  int64_t new_trunc_size =
      std::max(min_size, static_cast<int64_t>(trunc_size_.load() * alloc_inc_factor_));
  const int64_t alignment = std::max<int64_t>(PAGE_SIZE, block_size_);
  diff = new_trunc_size % alignment;
  if (diff > 0) {
    new_trunc_size += alignment - diff;
  }
  if (ftruncate(fd_, new_trunc_size) != 0) {
    return GetErrnoStatus("ftruncate", errno);
  }
  trunc_size_.store(new_trunc_size);
  return Status(Status::SUCCESS);
}

Status BlockParallelFileImpl::ReadImpl(int64_t off, char* buf, size_t size) {
  if (static_cast<int64_t>(off + size) > file_size_.load()) {
    return Status(Status::INFEASIBLE_ERROR, "excessive size");
  }
  if (off < head_buffer_size_.load()) {
    const int64_t prefix_size = std::min<int64_t>(size, head_buffer_size_.load() - off);
    std::memcpy(buf, head_buffer_ + off, prefix_size);
    buf += prefix_size;
    off = head_buffer_size_.load();
    size -= prefix_size;
  }
  if (size < 1) {
    return Status(Status::SUCCESS);
  }
  if (block_size_ == 1) {
    return PReadSequence(fd_, off, buf, size);
  }
  const int64_t end_position = off + size;
  Status status(Status::SUCCESS);
  const int64_t off_rem = off % block_size_;
  const int64_t end_rem = end_position % block_size_;
  if (off_rem > 0 || end_rem > 0) {
    const int64_t mod_off = off - off_rem;
    int64_t end_len = end_rem > 0 ? block_size_ - end_rem : 0;
    if (end_position + end_len > file_size_.load()) {
      end_len = file_size_.load() - end_position;
    }
    const int64_t mod_end = end_position + end_len;
    const int64_t mod_len = mod_end - mod_off;
    char* aligned = static_cast<char*>(xmallocaligned(block_size_, mod_len));
    status |= PReadSequence(fd_, mod_off, aligned, mod_len);
    std::memcpy(buf, aligned + off_rem, size);
    xfreealigned(aligned);
  } else if (reinterpret_cast<intptr_t>(buf) % block_size_ == 0) {
    status |= PReadSequence(fd_, off, buf, size);
  } else {
    char* aligned = static_cast<char*>(xmallocaligned(block_size_, size));
    status |= PReadSequence(fd_, off, aligned, size);
    std::memcpy(buf, aligned, size);
    xfreealigned(aligned);
  }
  return status;
}

Status BlockParallelFileImpl::WriteImpl(int64_t off, const char* buf, size_t size) {
  if (off < head_buffer_size_.load()) {
    const int64_t prefix_size = std::min<int64_t>(size, head_buffer_size_.load() - off);
    std::memcpy(head_buffer_ + off, buf, prefix_size);
    buf += prefix_size;
    off = head_buffer_size_.load();
    size -= prefix_size;
  }
  if (size < 1) {
    return Status(Status::SUCCESS);
  }
  if (block_size_ == 1) {
    return PWriteSequence(fd_, off, buf, size);
  }
  const int64_t end_position = off + size;
  Status status(Status::SUCCESS);
  const int64_t off_rem = off % block_size_;
  const int64_t end_rem = end_position % block_size_;
  if (off_rem > 0 || end_rem > 0) {
    const int64_t mod_off = off - off_rem;
    const int64_t end_len = end_rem > 0 ? block_size_ - end_rem : 0;
    const int64_t mod_end = end_position + end_len;
    const int64_t mod_len = mod_end - mod_off;
    char* aligned = static_cast<char*>(xmallocaligned(block_size_, mod_len));
    char* block = static_cast<char*>(xmallocaligned(block_size_, block_size_));
    if (off_rem > 0) {
      status |= PReadSequence(fd_, mod_off, block, block_size_);
      std::memcpy(aligned, block, off_rem);
    }
    std::memcpy(aligned + off_rem, buf, size);
    if (end_len > 0) {
      status |= PReadSequence(fd_, mod_end - block_size_, block, block_size_);
      std::memcpy(aligned + off_rem + size, block + end_rem, end_len);
    }
    status |= PWriteSequence(fd_, mod_off, aligned, off_rem + size + end_len);
    xfreealigned(block);
    xfreealigned(aligned);
  } else if (reinterpret_cast<intptr_t>(buf) % block_size_ == 0) {
    status |= PWriteSequence(fd_, off, buf, size);
  } else {
    char* aligned = static_cast<char*>(xmallocaligned(block_size_, size));
    std::memcpy(aligned, buf, size);
    status |= PWriteSequence(fd_, off, aligned, size);
    xfreealigned(aligned);
  }
  return status;
}

BlockParallelFile::BlockParallelFile() {
  impl_ = new BlockParallelFileImpl();
}

BlockParallelFile::~BlockParallelFile() {
  delete impl_;
}

Status BlockParallelFile::Open(const std::string& path, bool writable, int32_t options) {
  return impl_->Open(path, writable, options);
}

Status BlockParallelFile::Close() {
  return impl_->Close();
}

Status BlockParallelFile::Read(int64_t off, void* buf, size_t size) {
  assert(off >= 0 && buf != nullptr);
  return impl_->Read(off, buf, size);
}

Status BlockParallelFile::Write(int64_t off, const void* buf, size_t size) {
  assert(off >= 0 && buf != nullptr && size <= MAX_MEMORY_SIZE);
  return impl_->Write(off, buf, size);
}

Status BlockParallelFile::Append(const void* buf, size_t size, int64_t* off) {
  assert(buf != nullptr && size <= MAX_MEMORY_SIZE);
  return impl_->Append(buf, size, off);
}

Status BlockParallelFile::Expand(size_t inc_size, int64_t* old_size) {
  assert(inc_size <= MAX_MEMORY_SIZE);
  return impl_->Append(nullptr, inc_size, old_size);
}

Status BlockParallelFile::Truncate(int64_t size) {
  assert(size >= 0 && size <= MAX_MEMORY_SIZE);
  return impl_->Truncate(size);
}

Status BlockParallelFile::Synchronize(bool hard) {
  return impl_->Synchronize(hard);
}

Status BlockParallelFile::GetSize(int64_t* size) {
  assert(size != nullptr);
  return impl_->GetSize(size);
}

Status BlockParallelFile::SetHeadBuffer(int64_t size) {
  return impl_->SetHeadBuffer(size);
}

Status BlockParallelFile::SetAccessStrategy(int64_t block_size, int32_t options) {
  assert(block_size > 0);
  return impl_->SetAccessStrategy(block_size, options);
}

Status BlockParallelFile::SetAllocationStrategy(int64_t init_size, double inc_factor) {
  assert(init_size > 0 && inc_factor > 0);
  return impl_->SetAllocationStrategy(init_size, inc_factor);
}

Status BlockParallelFile::GetPath(std::string* path) {
  assert(path != nullptr);
  return impl_->GetPath(path);
}

Status BlockParallelFile::Rename(const std::string& new_path) {
  return impl_->Rename(new_path);
}

class BlockAtomicFileImpl final {
 public:
  BlockAtomicFileImpl();
  ~BlockAtomicFileImpl();
  Status Open(const std::string& path, bool writable, int32_t options);
  Status Close();
  Status Read(int64_t off, void* buf, size_t size);
  Status Write(int64_t off, const void* buf, size_t size);
  Status Append(const void* buf, size_t size, int64_t* off);
  Status Truncate(int64_t size);
  Status Synchronize(bool hard);
  Status GetSize(int64_t* size);
  Status SetHeadBuffer(int64_t size);
  Status SetAccessStrategy(int64_t block_size, int32_t options);
  Status SetAllocationStrategy(int64_t init_size, double inc_factor);
  Status GetPath(std::string* path);
  Status Rename(const std::string& new_path);

 private:
  Status AllocateSpace(int64_t min_size);
  Status ReadImpl(int64_t off, char* buf, size_t size);
  Status WriteImpl(int64_t off, const char* buf, size_t size);

  int32_t fd_;
  char* head_buffer_;
  int64_t head_buffer_size_;
  std::string path_;
  int64_t file_size_;
  int64_t trunc_size_;
  bool writable_;
  int32_t open_options_;
  int64_t block_size_;
  int32_t access_options_;
  int64_t alloc_init_size_;
  double alloc_inc_factor_;
  std::shared_timed_mutex mutex_;
};

BlockAtomicFileImpl::BlockAtomicFileImpl()
    : fd_(-1), head_buffer_(nullptr), head_buffer_size_(0),
      file_size_(0), trunc_size_(0), writable_(false), open_options_(0),
      block_size_(BlockAtomicFile::DEFAULT_BLOCK_SIZE), access_options_(0),
      alloc_init_size_(File::DEFAULT_ALLOC_INIT_SIZE),
      alloc_inc_factor_(File::DEFAULT_ALLOC_INC_FACTOR), mutex_() {}

BlockAtomicFileImpl::~BlockAtomicFileImpl() {
  if (fd_ >= 0) {
    Close();
  }
}

Status BlockAtomicFileImpl::Open(const std::string& path, bool writable, int32_t options) {
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
  if (access_options_ & BlockAtomicFile::ACCESS_DIRECT) {
    SetDirectAccessToOpenFlag(&oflags);
  }
  if (access_options_ & BlockAtomicFile::ACCESS_SYNC) {
    oflags |= O_SYNC;
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

  // Truncates the file.
  int64_t trunc_size = file_size;
  if (writable) {
    trunc_size = std::max(trunc_size, alloc_init_size_);
    const int64_t alignment = std::max<int64_t>(PAGE_SIZE, block_size_);
    const int64_t diff = trunc_size % alignment;
    if (diff > 0) {
      trunc_size += alignment - diff;
    }
    if (ftruncate(fd, trunc_size) != 0) {
      const Status status = GetErrnoStatus("ftruncate", errno);
      close(fd);
      return status;
    }
  }

  // Updates the internal data.
  fd_ = fd;
  head_buffer_ = nullptr;
  head_buffer_size_ = 0;
  path_ = path;
  file_size_ = file_size;
  trunc_size_ = trunc_size;
  writable_ = writable;
  open_options_ = options;

  return Status(Status::SUCCESS);
}

Status BlockAtomicFileImpl::Close() {
  std::lock_guard<std::shared_timed_mutex> lock(mutex_);
  if (fd_ < 0) {
    return Status(Status::PRECONDITION_ERROR, "not opened file");
  }
  Status status(Status::SUCCESS);

  // Truncates and synchronizes the file.
  if (writable_) {
    if (head_buffer_ != nullptr) {
      status |= PWriteSequence(fd_, 0, head_buffer_, head_buffer_size_);
    }
    if (file_size_ % block_size_ == 0) {
      if (ftruncate(fd_, file_size_) != 0) {
        status |= GetErrnoStatus("ftruncate", errno);
      }
    } else {
      if (truncate(path_.c_str(), file_size_) != 0) {
        status |= GetErrnoStatus("truncate", errno);
      }
    }
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

  // Releases the head buffer.
  if (head_buffer_ != nullptr) {
    xfreealigned(head_buffer_);
  }

  // Close the file.
  if (close(fd_) != 0) {
    status |= GetErrnoStatus("close", errno);
  }

  // Updates the internal data.
  fd_ = -1;
  head_buffer_ = nullptr;
  head_buffer_size_ = 0;
  path_.clear();
  file_size_ = 0;
  trunc_size_ = 0;
  writable_ = false;
  open_options_ = 0;

  return status;
}

Status BlockAtomicFileImpl::Read(int64_t off, void* buf, size_t size) {
  std::shared_lock<std::shared_timed_mutex> lock(mutex_);
  if (fd_ < 0) {
    return Status(Status::PRECONDITION_ERROR, "not opened file");
  }
  return ReadImpl(off, static_cast<char*>(buf), size);
}

Status BlockAtomicFileImpl::Write(int64_t off, const void* buf, size_t size) {
  std::lock_guard<std::shared_timed_mutex> lock(mutex_);
  if (fd_ < 0) {
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
  return WriteImpl(off, static_cast<const char*>(buf), size);
}

Status BlockAtomicFileImpl::Append(const void* buf, size_t size, int64_t* off) {
  std::lock_guard<std::shared_timed_mutex> lock(mutex_);
  if (fd_ < 0) {
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
  if (buf == nullptr) {
    return Status(Status::SUCCESS);
  }
  return WriteImpl(position, static_cast<const char*>(buf), size);
}

Status BlockAtomicFileImpl::Truncate(int64_t size) {
  std::lock_guard<std::shared_timed_mutex> lock(mutex_);
  if (fd_ < 0) {
    return Status(Status::PRECONDITION_ERROR, "not opened file");
  }
  if (!writable_) {
    return Status(Status::PRECONDITION_ERROR, "not writable file");
  }
  int64_t new_trunc_size =
      std::max(std::max(size, static_cast<int64_t>(PAGE_SIZE)), alloc_init_size_);
  const int64_t alignment = std::max<int64_t>(PAGE_SIZE, block_size_);
  const int64_t diff = new_trunc_size % alignment;
  if (diff > 0) {
    new_trunc_size += PAGE_SIZE - diff;
  }
  if (ftruncate(fd_, new_trunc_size) != 0) {
    return GetErrnoStatus("ftruncate", errno);
  }
  file_size_ = size;
  trunc_size_ = new_trunc_size;
  return Status(Status::SUCCESS);
}

Status BlockAtomicFileImpl::Synchronize(bool hard) {
  std::lock_guard<std::shared_timed_mutex> lock(mutex_);
  if (fd_ < 0) {
    return Status(Status::PRECONDITION_ERROR, "not opened file");
  }
  if (!writable_) {
    return Status(Status::PRECONDITION_ERROR, "not writable file");
  }
  Status status(Status::SUCCESS);
  if (head_buffer_ != nullptr) {
    status |= PWriteSequence(fd_, 0, head_buffer_, head_buffer_size_);
  }
  trunc_size_ = file_size_;
  if (trunc_size_ % block_size_ == 0) {
    if (ftruncate(fd_, trunc_size_) != 0) {
      status |= GetErrnoStatus("ftruncate", errno);
    }
  } else {
    if (truncate(path_.c_str(), trunc_size_) != 0) {
      status |= GetErrnoStatus("truncate", errno);
    }
  }
  if (hard && fsync(fd_) != 0) {
    status |= GetErrnoStatus("fsync", errno);
  }
  return status;
}

Status BlockAtomicFileImpl::GetSize(int64_t* size) {
  std::shared_lock<std::shared_timed_mutex> lock(mutex_);
  if (fd_ < 0) {
    return Status(Status::PRECONDITION_ERROR, "not opened file");
  }
  *size = file_size_;
  return Status(Status::SUCCESS);
}

Status BlockAtomicFileImpl::SetHeadBuffer(int64_t size) {
  std::lock_guard<std::shared_timed_mutex> lock(mutex_);
  if (fd_ < 0) {
    return Status(Status::PRECONDITION_ERROR, "not opened file");
  }
  Status status(Status::SUCCESS);
  if (head_buffer_ != nullptr) {
    status |= PWriteSequence(fd_, 0, head_buffer_, head_buffer_size_);
    xfreealigned(head_buffer_);
  }
  if (size < 1) {
    head_buffer_size_ = 0;
    head_buffer_ = nullptr;
    return status;
  }
  const int64_t diff = size % block_size_;
  if (diff > 0) {
    size += block_size_ - diff;
  }
  head_buffer_size_ = size;
  head_buffer_ = static_cast<char*>(xmallocaligned(block_size_, head_buffer_size_));
  std::memset(head_buffer_, 0, head_buffer_size_);
  const int64_t read_size = std::min<int64_t>(head_buffer_size_, file_size_);
  status |= PReadSequence(fd_, 0, head_buffer_, read_size);
  return status;
}

Status BlockAtomicFileImpl::SetAccessStrategy(int64_t block_size, int32_t options) {
  std::lock_guard<std::shared_timed_mutex> lock(mutex_);
  if (fd_ >= 0) {
    return Status(Status::PRECONDITION_ERROR, "alread opened file");
  }
  block_size_ = block_size;
  access_options_ = options;
  return Status(Status::SUCCESS);
}

Status BlockAtomicFileImpl::SetAllocationStrategy(int64_t init_size, double inc_factor) {
  std::lock_guard<std::shared_timed_mutex> lock(mutex_);
  if (fd_ >= 0) {
    return Status(Status::PRECONDITION_ERROR, "alread opened file");
  }
  alloc_init_size_ = init_size;
  alloc_inc_factor_ = inc_factor;
  return Status(Status::SUCCESS);
}

Status BlockAtomicFileImpl::GetPath(std::string* path) {
  std::shared_lock<std::shared_timed_mutex> lock(mutex_);
  if (fd_ < 0) {
    return Status(Status::PRECONDITION_ERROR, "not opened file");
  }
  *path = path_;
  return Status(Status::SUCCESS);
}

Status BlockAtomicFileImpl::Rename(const std::string& new_path) {
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

Status BlockAtomicFileImpl::AllocateSpace(int64_t min_size) {
  int64_t new_trunc_size =
      std::max(min_size, static_cast<int64_t>(trunc_size_ * alloc_inc_factor_));
  const int64_t alignment = std::max<int64_t>(PAGE_SIZE, block_size_);
  const int64_t diff = new_trunc_size % alignment;
  if (diff > 0) {
    new_trunc_size += PAGE_SIZE - diff;
  }
  if (ftruncate(fd_, new_trunc_size) != 0) {
    return GetErrnoStatus("ftruncate", errno);
  }
  trunc_size_ = new_trunc_size;
  return Status(Status::SUCCESS);
}

Status BlockAtomicFileImpl::ReadImpl(int64_t off, char* buf, size_t size) {
  if (static_cast<int64_t>(off + size) > file_size_) {
    return Status(Status::INFEASIBLE_ERROR, "excessive size");
  }
  if (off < head_buffer_size_) {
    const int64_t prefix_size = std::min<int64_t>(size, head_buffer_size_ - off);
    std::memcpy(buf, head_buffer_ + off, prefix_size);
    buf += prefix_size;
    off = head_buffer_size_;
    size -= prefix_size;
  }
  if (size < 1) {
    return Status(Status::SUCCESS);
  }
  if (block_size_ == 1) {
    return PReadSequence(fd_, off, buf, size);
  }
  const int64_t end_position = off + size;
  Status status(Status::SUCCESS);
  const int64_t off_rem = off % block_size_;
  const int64_t end_rem = end_position % block_size_;
  if (off_rem > 0 || end_rem > 0) {
    const int64_t mod_off = off - off_rem;
    int64_t end_len = end_rem > 0 ? block_size_ - end_rem : 0;
    if (end_position + end_len > file_size_) {
      end_len = file_size_ - end_position;
    }
    const int64_t mod_end = end_position + end_len;
    const int64_t mod_len = mod_end - mod_off;
    char* aligned = static_cast<char*>(xmallocaligned(block_size_, mod_len));
    status |= PReadSequence(fd_, mod_off, aligned, mod_len);
    std::memcpy(buf, aligned + off_rem, size);
    xfreealigned(aligned);
  } else if (reinterpret_cast<intptr_t>(buf) % block_size_ == 0) {
    status |= PReadSequence(fd_, off, buf, size);
  } else {
    char* aligned = static_cast<char*>(xmallocaligned(block_size_, size));
    status |= PReadSequence(fd_, off, aligned, size);
    std::memcpy(buf, aligned, size);
    xfreealigned(aligned);
  }
  return status;
}

Status BlockAtomicFileImpl::WriteImpl(int64_t off, const char* buf, size_t size) {
  if (off < head_buffer_size_) {
    const int64_t prefix_size = std::min<int64_t>(size, head_buffer_size_ - off);
    std::memcpy(head_buffer_ + off, buf, prefix_size);
    buf += prefix_size;
    off = head_buffer_size_;
    size -= prefix_size;
  }
  if (size < 1) {
    return Status(Status::SUCCESS);
  }
  if (block_size_ == 1) {
    return PWriteSequence(fd_, off, buf, size);
  }
  const int64_t end_position = off + size;
  Status status(Status::SUCCESS);
  const int64_t off_rem = off % block_size_;
  const int64_t end_rem = end_position % block_size_;
  if (off_rem > 0 || end_rem > 0) {
    const int64_t mod_off = off - off_rem;
    const int64_t end_len = end_rem > 0 ? block_size_ - end_rem : 0;
    const int64_t mod_end = end_position + end_len;
    const int64_t mod_len = mod_end - mod_off;
    char* aligned = static_cast<char*>(xmallocaligned(block_size_, mod_len));
    char* block = static_cast<char*>(xmallocaligned(block_size_, block_size_));
    if (off_rem > 0) {
      status |= PReadSequence(fd_, mod_off, block, block_size_);
      std::memcpy(aligned, block, off_rem);
    }
    std::memcpy(aligned + off_rem, buf, size);
    if (end_len > 0) {
      status |= PReadSequence(fd_, mod_end - block_size_, block, block_size_);
      std::memcpy(aligned + off_rem + size, block + end_rem, end_len);
    }
    status |= PWriteSequence(fd_, mod_off, aligned, off_rem + size + end_len);
    xfreealigned(block);
    xfreealigned(aligned);
  } else if (reinterpret_cast<intptr_t>(buf) % block_size_ == 0) {
    status |= PWriteSequence(fd_, off, buf, size);
  } else {
    char* aligned = static_cast<char*>(xmallocaligned(block_size_, size));
    std::memcpy(aligned, buf, size);
    status |= PWriteSequence(fd_, off, aligned, size);
    xfreealigned(aligned);
  }
  return status;
}

BlockAtomicFile::BlockAtomicFile() {
  impl_ = new BlockAtomicFileImpl();
}

BlockAtomicFile::~BlockAtomicFile() {
  delete impl_;
}

Status BlockAtomicFile::Open(const std::string& path, bool writable, int32_t options) {
  return impl_->Open(path, writable, options);
}

Status BlockAtomicFile::Close() {
  return impl_->Close();
}

Status BlockAtomicFile::Read(int64_t off, void* buf, size_t size) {
  assert(off >= 0 && buf != nullptr);
  return impl_->Read(off, buf, size);
}

Status BlockAtomicFile::Write(int64_t off, const void* buf, size_t size) {
  assert(off >= 0 && buf != nullptr && size <= MAX_MEMORY_SIZE);
  return impl_->Write(off, buf, size);
}

Status BlockAtomicFile::Append(const void* buf, size_t size, int64_t* off) {
  assert(buf != nullptr && size <= MAX_MEMORY_SIZE);
  return impl_->Append(buf, size, off);
}

Status BlockAtomicFile::Expand(size_t inc_size, int64_t* old_size) {
  assert(inc_size <= MAX_MEMORY_SIZE);
  return impl_->Append(nullptr, inc_size, old_size);
}

Status BlockAtomicFile::Truncate(int64_t size) {
  assert(size >= 0 && size <= MAX_MEMORY_SIZE);
  return impl_->Truncate(size);
}

Status BlockAtomicFile::Synchronize(bool hard) {
  return impl_->Synchronize(hard);
}

Status BlockAtomicFile::GetSize(int64_t* size) {
  assert(size != nullptr);
  return impl_->GetSize(size);
}

Status BlockAtomicFile::SetHeadBuffer(int64_t size) {
  return impl_->SetHeadBuffer(size);
}

Status BlockAtomicFile::SetAccessStrategy(int64_t block_size, int32_t options) {
  assert(block_size > 0);
  return impl_->SetAccessStrategy(block_size, options);
}

Status BlockAtomicFile::SetAllocationStrategy(int64_t init_size, double inc_factor) {
  assert(init_size > 0 && inc_factor > 0);
  return impl_->SetAllocationStrategy(init_size, inc_factor);
}

Status BlockAtomicFile::GetPath(std::string* path) {
  assert(path != nullptr);
  return impl_->GetPath(path);
}

Status BlockAtomicFile::Rename(const std::string& new_path) {
  return impl_->Rename(new_path);
}

}  // namespace tkrzw

#endif

// END OF FILE
