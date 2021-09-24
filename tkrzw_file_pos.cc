/*************************************************************************************************
 * File implementations by positional access
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

#include "tkrzw_sys_file_pos_std.h"

#elif defined(_SYS_WINDOWS_)

#include "tkrzw_sys_file_pos_windows.h"

#else

#include "tkrzw_file.h"
#include "tkrzw_file_pos.h"
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

inline void SetDirectAccessToFileDescriptor(int32_t fd) {
#if defined(_SYS_MACOSX_)
  fcntl(fd, F_NOCACHE, 1);
#endif
}

inline void AdviseFileRandomAccessPattern(int32_t fd) {
#if defined(_SYS_LINUX_)
  posix_fadvise(fd, 0, 0, POSIX_FADV_RANDOM);
#endif
}

inline int32_t tkrzw_fsync_range(int32_t fd, int64_t off, int64_t size) {
#if defined(_SYS_LINUX_)
  constexpr int32_t flags =
      SYNC_FILE_RANGE_WAIT_BEFORE | SYNC_FILE_RANGE_WRITE | SYNC_FILE_RANGE_WAIT_AFTER;
  return sync_file_range(fd, off, size, flags);
#else
  return fsync(fd);
#endif
}

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
  Status TruncateFakely(int64_t size);
  Status Synchronize(bool hard, int64_t off, int64_t size);
  Status GetSize(int64_t* size);
  Status SetHeadBuffer(int64_t size);
  Status SetAccessStrategy(int64_t block_size, int32_t options);
  Status SetAllocationStrategy(int64_t init_size, double inc_factor);
  Status CopyProperties(File* file);
  Status GetPath(std::string* path);
  Status Rename(const std::string& new_path);
  Status DisablePathOperations();
  bool IsOpen();
  int64_t GetBlockSize();
  bool IsDirectIO();

 private:
  Status AllocateSpace(int64_t min_size);
  Status ReadImpl(int64_t off, char* buf, size_t size);
  Status WriteImpl(int64_t off, const char* buf, size_t size);

  int32_t fd_;
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
  std::unique_ptr<PageCache> page_cache_;
  std::mutex mutex_;
};

PositionalParallelFileImpl::PositionalParallelFileImpl()
    : fd_(-1), head_buffer_(nullptr), head_buffer_size_(0),
      file_size_(0), trunc_size_(0), writable_(false), open_options_(0),
      block_size_(1), access_options_(0),
      alloc_init_size_(File::DEFAULT_ALLOC_INIT_SIZE),
      alloc_inc_factor_(File::DEFAULT_ALLOC_INC_FACTOR),
      page_cache_(nullptr), mutex_() {}

PositionalParallelFileImpl::~PositionalParallelFileImpl() {
  if (fd_ >= 0) {
    Close();
  }
}

Status PositionalParallelFileImpl::Open(const std::string& path, bool writable, int32_t options) {
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
  if (access_options_ & PositionalFile::ACCESS_DIRECT) {
    SetDirectAccessToOpenFlag(&oflags);
  }
  if (access_options_ & PositionalFile::ACCESS_SYNC) {
    oflags |= O_SYNC;
  }
  const int32_t fd = open(path.c_str(), oflags, FILEPERM);
  if (fd < 0) {
    return GetErrnoStatus("open", errno);
  }
  if (access_options_ & PositionalFile::ACCESS_DIRECT) {
    SetDirectAccessToFileDescriptor(fd);
  }
  AdviseFileRandomAccessPattern(fd);

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
    trunc_size = AlignNumber(trunc_size, std::max<int64_t>(PAGE_SIZE, block_size_));
    const Status status = TruncateFile(fd, trunc_size);
    if (status != Status::SUCCESS) {
      close(fd);
      return status;
    }
  } else {
    trunc_size = file_size;
  }

  // Updates the internal data.
  fd_ = fd;
  head_buffer_ = nullptr;
  head_buffer_size_ = 0;
  path_ = path;
  file_size_.store(file_size);
  trunc_size_.store(trunc_size);
  writable_ = writable;
  open_options_ = options;

  // Builds the page cache.
  if (access_options_ & PositionalFile::ACCESS_PAGECACHE) {
    auto reader = [&](int64_t off, void* buf, size_t size) {
                    return PReadSequence(fd_, off, buf, size);
                  };
    auto writer = [&](int64_t off, const void* buf, size_t size) {
                    return PWriteSequence(fd_, off, buf, size);
                  };
    page_cache_ = std::make_unique<PageCache>(block_size_, 256, reader, writer);
    page_cache_->SetRegionSize(trunc_size);
  } else {
    page_cache_.reset(nullptr);
  }

  return Status(Status::SUCCESS);
}

Status PositionalParallelFileImpl::Close() {
  if (fd_ < 0) {
    return Status(Status::PRECONDITION_ERROR, "not opened file");
  }
  Status status(Status::SUCCESS);

  // Finishes and truncates the file.
  if (writable_) {
    if (page_cache_ != nullptr) {
      status |= page_cache_->Flush();
    }
    if (head_buffer_ != nullptr) {
      status |= PWriteSequence(fd_, 0, head_buffer_, head_buffer_size_);
    }
    int64_t trunc_size = file_size_.load();
    if (access_options_ & PositionalFile::ACCESS_PADDING) {
      trunc_size = AlignNumber(trunc_size, block_size_);
    }
    status |= TruncateFile(fd_, trunc_size);
    if ((open_options_ & File::OPEN_SYNC_HARD) && fsync(fd_) != 0) {
      status |= GetErrnoStatus("fsync", errno);
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
  file_size_.store(0);
  trunc_size_.store(0);
  writable_ = false;
  open_options_ = 0;
  page_cache_.reset(nullptr);

  return status;
}

Status PositionalParallelFileImpl::Read(int64_t off, void* buf, size_t size) {
  if (fd_ < 0) {
    return Status(Status::PRECONDITION_ERROR, "not opened file");
  }
  return ReadImpl(off, static_cast<char*>(buf), size);
}

Status PositionalParallelFileImpl::Write(int64_t off, const void* buf, size_t size) {
  if (fd_ < 0) {
    return Status(Status::PRECONDITION_ERROR, "not opened file");
  }
  if (!writable_) {
    return Status(Status::PRECONDITION_ERROR, "not writable file");
  }
  const int64_t end_position = off + size;
  const Status status = AllocateSpace(AlignNumber(end_position, block_size_));
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

Status PositionalParallelFileImpl::Append(const void* buf, size_t size, int64_t* off) {
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
    const Status status = AllocateSpace(AlignNumber(end_position, block_size_));
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

Status PositionalParallelFileImpl::Truncate(int64_t size) {
  if (fd_ < 0) {
    return Status(Status::PRECONDITION_ERROR, "not opened file");
  }
  if (!writable_) {
    return Status(Status::PRECONDITION_ERROR, "not writable file");
  }
  Status status(Status::SUCCESS);
  if (page_cache_ != nullptr) {
    status |= page_cache_->Flush(0, size);
    page_cache_->Clear();
  }
  int64_t new_trunc_size = 0;
  if (alloc_inc_factor_ <= 0) {
    new_trunc_size = std::max(size, alloc_init_size_);
  } else {
    new_trunc_size = std::max(std::max(size, static_cast<int64_t>(PAGE_SIZE)), alloc_init_size_);
    new_trunc_size = AlignNumber(new_trunc_size, std::max<int64_t>(PAGE_SIZE, block_size_));
  }
  status |= TruncateFile(fd_, new_trunc_size);
  if (status == Status::SUCCESS) {
    file_size_.store(size);
    trunc_size_.store(new_trunc_size);
  }
  if (page_cache_ != nullptr) {
    page_cache_->SetRegionSize(trunc_size_.load());
  }
  return status;
}

Status PositionalParallelFileImpl::TruncateFakely(int64_t size) {
  if (fd_ < 0) {
    return Status(Status::PRECONDITION_ERROR, "not opened file");
  }
  if (size > trunc_size_.load()) {
    return Status(Status::INFEASIBLE_ERROR, "unable to increase the file size");
  }
  file_size_.store(size);
  return Status(Status::SUCCESS);
}

Status PositionalParallelFileImpl::Synchronize(bool hard, int64_t off, int64_t size) {
  if (fd_ < 0) {
    return Status(Status::PRECONDITION_ERROR, "not opened file");
  }
  if (!writable_) {
    return Status(Status::PRECONDITION_ERROR, "not writable file");
  }
  Status status(Status::SUCCESS);
  if (page_cache_ != nullptr) {
    status |= page_cache_->Flush(off, size);
  }
  if (head_buffer_ != nullptr) {
    status |= PWriteSequence(fd_, 0, head_buffer_, head_buffer_size_);
  }
  trunc_size_.store(AlignNumber(file_size_.load(), block_size_));
  status |= TruncateFile(fd_, trunc_size_.load());
  if (hard) {
    if (size == 0) {
      size = trunc_size_;
    }
    const int64_t end = off + size;
    off -= off % PAGE_SIZE;
    size = end - off;
    off = std::min<int64_t>(trunc_size_, off);
    size = std::min<int64_t>(trunc_size_ - off, size);
    if (tkrzw_fsync_range(fd_, off, size) != 0) {
      status |= GetErrnoStatus("fsync", errno);
    }
  }
  if (page_cache_ != nullptr) {
    page_cache_->SetRegionSize(trunc_size_.load());
  }
  return status;
}

Status PositionalParallelFileImpl::GetSize(int64_t* size) {
  if (fd_ < 0) {
    return Status(Status::PRECONDITION_ERROR, "not opened file");
  }
  *size = file_size_.load();
  return Status(Status::SUCCESS);
}

Status PositionalParallelFileImpl::SetHeadBuffer(int64_t size) {
  if (fd_ < 0) {
    return Status(Status::PRECONDITION_ERROR, "not opened file");
  }
  Status status(Status::SUCCESS);
  if (page_cache_ != nullptr) {
    if (writable_) {
      status |= page_cache_->Flush();
    }
    page_cache_->Clear();
  }
  if (head_buffer_ != nullptr) {
    status |= PWriteSequence(fd_, 0, head_buffer_, head_buffer_size_);
    xfreealigned(head_buffer_);
  }
  if (size < 1) {
    head_buffer_size_ = 0;
    head_buffer_ = nullptr;
    return status;
  }
  head_buffer_size_ = std::max<int64_t>(sizeof(void*), AlignNumber(size, block_size_));
  head_buffer_ = static_cast<char*>(xmallocaligned(
      std::max<int64_t>(sizeof(void*), block_size_), head_buffer_size_));
  std::memset(head_buffer_, 0, head_buffer_size_);
  const int64_t read_size = std::min<int64_t>(head_buffer_size_, file_size_.load());
  status |= PReadSequence(fd_, 0, head_buffer_, read_size);
  return status;
}

Status PositionalParallelFileImpl::SetAccessStrategy(int64_t block_size, int32_t options) {
  if (fd_ >= 0) {
    return Status(Status::PRECONDITION_ERROR, "alread opened file");
  }
  block_size_ = block_size > 1 ?
      AlignNumberPowTwo(std::max<int64_t>(sizeof(void*), block_size)) : 1;
  access_options_ = options;
  return Status(Status::SUCCESS);
}

Status PositionalParallelFileImpl::SetAllocationStrategy(int64_t init_size, double inc_factor) {
  if (fd_ >= 0) {
    return Status(Status::PRECONDITION_ERROR, "alread opened file");
  }
  alloc_init_size_ = init_size;
  alloc_inc_factor_ = inc_factor;
  return Status(Status::SUCCESS);
}

Status PositionalParallelFileImpl::CopyProperties(File* file) {
  Status status = file->SetAllocationStrategy(alloc_init_size_, alloc_inc_factor_);
  auto* pos_file = dynamic_cast<PositionalFile*>(file);
  if (pos_file != nullptr) {
    status |= pos_file->SetAccessStrategy(block_size_, access_options_);
  }
  return status;
}

Status PositionalParallelFileImpl::GetPath(std::string* path) {
  if (fd_ < 0) {
    return Status(Status::PRECONDITION_ERROR, "not opened file");
  }
  if (path_.empty()) {
    return Status(Status::PRECONDITION_ERROR, "disabled path operatione");
  }
  *path = path_;
  return Status(Status::SUCCESS);
}

Status PositionalParallelFileImpl::Rename(const std::string& new_path) {
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

Status PositionalParallelFileImpl::DisablePathOperations() {
  if (fd_ < 0) {
    return Status(Status::PRECONDITION_ERROR, "not opened file");
  }
  path_.clear();
  return Status(Status::SUCCESS);
}

bool PositionalParallelFileImpl::IsOpen() {
  return fd_ >= 0;
}

int64_t PositionalParallelFileImpl::GetBlockSize() {
  return block_size_;
}

bool PositionalParallelFileImpl::IsDirectIO() {
  return access_options_ & PositionalFile::ACCESS_DIRECT;
}

Status PositionalParallelFileImpl::AllocateSpace(int64_t min_size) {
  if (min_size <= trunc_size_.load()) {
    return Status(Status::SUCCESS);
  }
  std::lock_guard<std::mutex> lock(mutex_);
  if (min_size <= trunc_size_.load()) {
    return Status(Status::SUCCESS);
  }
  if (alloc_inc_factor_ <= 0) {
    trunc_size_.store(min_size);
    return Status(Status::SUCCESS);
  }
  int64_t new_trunc_size =
      std::max(min_size, static_cast<int64_t>(trunc_size_.load() * alloc_inc_factor_));
  new_trunc_size = AlignNumber(new_trunc_size, std::max<int64_t>(PAGE_SIZE, block_size_));
  const Status status = TruncateFile(fd_, new_trunc_size);
  if (status != Status::SUCCESS) {
    return status;
  }
  trunc_size_.store(new_trunc_size);
  if (page_cache_ != nullptr) {
    page_cache_->SetRegionSize(trunc_size_.load());
  }
  return Status(Status::SUCCESS);
}

Status PositionalParallelFileImpl::ReadImpl(int64_t off, char* buf, size_t size) {
  if (static_cast<int64_t>(off + size) > file_size_.load()) {
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
  if (page_cache_ != nullptr) {
    return page_cache_->Read(off, buf, size);
  }
  const int64_t end_position = off + size;
  Status status(Status::SUCCESS);
  const int64_t off_rem = off % block_size_;
  const int64_t end_rem = end_position % block_size_;
  if (off_rem > 0 || end_rem > 0) {
    const int64_t mod_off = off - off_rem;
    int64_t end_len = end_rem > 0 ? block_size_ - end_rem : 0;
    const int64_t alloc_size = trunc_size_.load();
    if (end_position + end_len > alloc_size) {
      end_len = alloc_size - end_position;
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

Status PositionalParallelFileImpl::WriteImpl(int64_t off, const char* buf, size_t size) {
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
  if (page_cache_ != nullptr) {
    return page_cache_->Write(off, buf, size);
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
      const int64_t mod_end_off = mod_end - block_size_;
      if (off_rem == 0 || mod_end_off != mod_off) {
        status |= PReadSequence(fd_, mod_end_off, block, block_size_);
      }
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

Status PositionalParallelFile::TruncateFakely(int64_t size) {
  assert(size >= 0 && size <= MAX_MEMORY_SIZE);
  return impl_->TruncateFakely(size);
}

Status PositionalParallelFile::Synchronize(bool hard, int64_t off, int64_t size) {
  assert(off >= 0 && size >= 0);
  return impl_->Synchronize(hard, off, size);
}

Status PositionalParallelFile::GetSize(int64_t* size) {
  assert(size != nullptr);
  return impl_->GetSize(size);
}

Status PositionalParallelFile::SetHeadBuffer(int64_t size) {
  return impl_->SetHeadBuffer(size);
}

Status PositionalParallelFile::SetAccessStrategy(int64_t block_size, int32_t options) {
  assert(block_size > 0);
  return impl_->SetAccessStrategy(block_size, options);
}

Status PositionalParallelFile::SetAllocationStrategy(int64_t init_size, double inc_factor) {
  assert(init_size >= 0 && inc_factor >= 0);
  return impl_->SetAllocationStrategy(init_size, inc_factor);
}

Status PositionalParallelFile::CopyProperties(File* file) {
  assert(file != nullptr);
  return impl_->CopyProperties(file);
}

Status PositionalParallelFile::GetPath(std::string* path) {
  assert(path != nullptr);
  return impl_->GetPath(path);
}

Status PositionalParallelFile::Rename(const std::string& new_path) {
  return impl_->Rename(new_path);
}

Status PositionalParallelFile::DisablePathOperations() {
  return impl_->DisablePathOperations();
}

bool PositionalParallelFile::IsOpen() const {
  return impl_->IsOpen();
}

int64_t PositionalParallelFile::GetBlockSize() const {
  return impl_->GetBlockSize();
}

bool PositionalParallelFile::IsDirectIO() const {
  return impl_->IsDirectIO();
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
  Status TruncateFakely(int64_t size);
  Status Synchronize(bool hard, int64_t off, int64_t size);
  Status GetSize(int64_t* size);
  Status SetHeadBuffer(int64_t size);
  Status SetAccessStrategy(int64_t block_size, int32_t options);
  Status SetAllocationStrategy(int64_t init_size, double inc_factor);
  Status CopyProperties(File* file);
  Status GetPath(std::string* path);
  Status Rename(const std::string& new_path);
  Status DisablePathOperations();
  bool IsOpen();
  int64_t GetBlockSize();
  bool IsDirectIO();

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
  std::unique_ptr<PageCache> page_cache_;
  std::shared_mutex mutex_;
};

PositionalAtomicFileImpl::PositionalAtomicFileImpl()
    : fd_(-1), head_buffer_(nullptr), head_buffer_size_(0),
      file_size_(0), trunc_size_(0), writable_(false), open_options_(0),
      block_size_(1), access_options_(0),
      alloc_init_size_(File::DEFAULT_ALLOC_INIT_SIZE),
      alloc_inc_factor_(File::DEFAULT_ALLOC_INC_FACTOR),
      page_cache_(nullptr), mutex_() {}

PositionalAtomicFileImpl::~PositionalAtomicFileImpl() {
  if (fd_ >= 0) {
    Close();
  }
}

Status PositionalAtomicFileImpl::Open(const std::string& path, bool writable, int32_t options) {
  std::lock_guard<std::shared_mutex> lock(mutex_);
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
  if (access_options_ & PositionalFile::ACCESS_DIRECT) {
    SetDirectAccessToOpenFlag(&oflags);
  }
  if (access_options_ & PositionalFile::ACCESS_SYNC) {
    oflags |= O_SYNC;
  }
  const int32_t fd = open(path.c_str(), oflags, FILEPERM);
  if (fd < 0) {
    return GetErrnoStatus("open", errno);
  }
  AdviseFileRandomAccessPattern(fd);

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
    trunc_size = AlignNumber(trunc_size, std::max<int64_t>(PAGE_SIZE, block_size_));
    const Status status = TruncateFile(fd, trunc_size);
    if (status != Status::SUCCESS) {
      close(fd);
      return status;
    }
  } else {
    trunc_size = file_size;
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

  // Builds the page cache.
  if (access_options_ & PositionalFile::ACCESS_PAGECACHE) {
    auto reader = [&](int64_t off, void* buf, size_t size) {
                    return PReadSequence(fd_, off, buf, size);
                  };
    auto writer = [&](int64_t off, const void* buf, size_t size) {
                    return PWriteSequence(fd_, off, buf, size);
                  };
    page_cache_ = std::make_unique<PageCache>(block_size_, 256, reader, writer);
    page_cache_->SetRegionSize(trunc_size);
  } else {
    page_cache_.reset(nullptr);
  }

  return Status(Status::SUCCESS);
}

Status PositionalAtomicFileImpl::Close() {
  std::lock_guard<std::shared_mutex> lock(mutex_);
  if (fd_ < 0) {
    return Status(Status::PRECONDITION_ERROR, "not opened file");
  }
  Status status(Status::SUCCESS);

  // Finishes and truncates the file.
  if (writable_) {
    if (page_cache_ != nullptr) {
      status |= page_cache_->Flush();
    }
    if (head_buffer_ != nullptr) {
      status |= PWriteSequence(fd_, 0, head_buffer_, head_buffer_size_);
    }
    int64_t trunc_size = file_size_;
    if (access_options_ & PositionalFile::ACCESS_PADDING) {
      trunc_size = AlignNumber(trunc_size, block_size_);
    }
    status |= TruncateFile(fd_, trunc_size);
    if ((open_options_ & File::OPEN_SYNC_HARD) && fsync(fd_) != 0) {
      status |= GetErrnoStatus("fsync", errno);
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
  page_cache_.reset(nullptr);

  return status;
}

Status PositionalAtomicFileImpl::Read(int64_t off, void* buf, size_t size) {
  std::shared_lock<std::shared_mutex> lock(mutex_);
  if (fd_ < 0) {
    return Status(Status::PRECONDITION_ERROR, "not opened file");
  }
  return ReadImpl(off, static_cast<char*>(buf), size);
}

Status PositionalAtomicFileImpl::Write(int64_t off, const void* buf, size_t size) {
  std::lock_guard<std::shared_mutex> lock(mutex_);
  if (fd_ < 0) {
    return Status(Status::PRECONDITION_ERROR, "not opened file");
  }
  if (!writable_) {
    return Status(Status::PRECONDITION_ERROR, "not writable file");
  }
  const int64_t end_position = off + size;
  const Status status = AllocateSpace(AlignNumber(end_position, block_size_));
  if (status != Status::SUCCESS) {
    return status;
  }
  file_size_ = std::max(file_size_, end_position);
  return WriteImpl(off, static_cast<const char*>(buf), size);
}

Status PositionalAtomicFileImpl::Append(const void* buf, size_t size, int64_t* off) {
  std::lock_guard<std::shared_mutex> lock(mutex_);
  if (fd_ < 0) {
    return Status(Status::PRECONDITION_ERROR, "not opened file");
  }
  if (!writable_) {
    return Status(Status::PRECONDITION_ERROR, "not writable file");
  }
  int64_t position = file_size_;
  const int64_t end_position = position + size;
  const Status status = AllocateSpace(AlignNumber(end_position, block_size_));
  if (status != Status::SUCCESS) {
    return status;
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

Status PositionalAtomicFileImpl::Truncate(int64_t size) {
  std::lock_guard<std::shared_mutex> lock(mutex_);
  if (fd_ < 0) {
    return Status(Status::PRECONDITION_ERROR, "not opened file");
  }
  if (!writable_) {
    return Status(Status::PRECONDITION_ERROR, "not writable file");
  }
  Status status(Status::SUCCESS);
  if (page_cache_ != nullptr) {
    status |= page_cache_->Flush(0, size);
    page_cache_->Clear();
  }
  int64_t new_trunc_size = 0;
  if (alloc_inc_factor_ <= 0) {
    new_trunc_size = std::max(size, alloc_init_size_);
  } else {
    new_trunc_size = std::max(std::max(size, static_cast<int64_t>(PAGE_SIZE)), alloc_init_size_);
    new_trunc_size = AlignNumber(new_trunc_size, std::max<int64_t>(PAGE_SIZE, block_size_));
  }
  status |= TruncateFile(fd_, new_trunc_size);
  if (status == Status::SUCCESS) {
    file_size_ = size;
    trunc_size_ = new_trunc_size;
  }
  if (page_cache_ != nullptr) {
    page_cache_->SetRegionSize(trunc_size_);
  }
  return status;
}

Status PositionalAtomicFileImpl::TruncateFakely(int64_t size) {
  std::lock_guard<std::shared_mutex> lock(mutex_);
  if (fd_ < 0) {
    return Status(Status::PRECONDITION_ERROR, "not opened file");
  }
  if (size > trunc_size_) {
    return Status(Status::INFEASIBLE_ERROR, "unable to increase the file size");
  }
  file_size_ = size;
  return Status(Status::SUCCESS);
}

Status PositionalAtomicFileImpl::Synchronize(bool hard, int64_t off, int64_t size) {
  std::lock_guard<std::shared_mutex> lock(mutex_);
  if (fd_ < 0) {
    return Status(Status::PRECONDITION_ERROR, "not opened file");
  }
  if (!writable_) {
    return Status(Status::PRECONDITION_ERROR, "not writable file");
  }
  Status status(Status::SUCCESS);
  if (page_cache_ != nullptr) {
    status |= page_cache_->Flush(off, size);
  }
  if (head_buffer_ != nullptr) {
    status |= PWriteSequence(fd_, 0, head_buffer_, head_buffer_size_);
  }
  trunc_size_ = AlignNumber(file_size_, block_size_);
  status |= TruncateFile(fd_, trunc_size_);
  if (hard) {
    if (size == 0) {
      size = trunc_size_;
    }
    const int64_t end = off + size;
    off -= off % PAGE_SIZE;
    size = end - off;
    off = std::min<int64_t>(trunc_size_, off);
    size = std::min<int64_t>(trunc_size_ - off, size);
    if (tkrzw_fsync_range(fd_, off, size) != 0) {
      status |= GetErrnoStatus("fsync", errno);
    }
  }
  if (page_cache_ != nullptr) {
    page_cache_->SetRegionSize(trunc_size_);
  }
  return status;
}

Status PositionalAtomicFileImpl::GetSize(int64_t* size) {
  std::shared_lock<std::shared_mutex> lock(mutex_);
  if (fd_ < 0) {
    return Status(Status::PRECONDITION_ERROR, "not opened file");
  }
  *size = file_size_;
  return Status(Status::SUCCESS);
}

Status PositionalAtomicFileImpl::SetHeadBuffer(int64_t size) {
  std::lock_guard<std::shared_mutex> lock(mutex_);
  if (fd_ < 0) {
    return Status(Status::PRECONDITION_ERROR, "not opened file");
  }
  Status status(Status::SUCCESS);
  if (page_cache_ != nullptr) {
    if (writable_) {
      status |= page_cache_->Flush();
    }
    page_cache_->Clear();
  }
  if (head_buffer_ != nullptr) {
    status |= PWriteSequence(fd_, 0, head_buffer_, head_buffer_size_);
    xfreealigned(head_buffer_);
  }
  if (size < 1) {
    head_buffer_size_ = 0;
    head_buffer_ = nullptr;
    return status;
  }
  head_buffer_size_ = std::max<int64_t>(sizeof(void*), AlignNumber(size, block_size_));
  head_buffer_ = static_cast<char*>(xmallocaligned(block_size_, head_buffer_size_));
  std::memset(head_buffer_, 0, head_buffer_size_);
  const int64_t read_size = std::min<int64_t>(head_buffer_size_, file_size_);
  status |= PReadSequence(fd_, 0, head_buffer_, read_size);
  return status;
}

Status PositionalAtomicFileImpl::SetAccessStrategy(int64_t block_size, int32_t options) {
  std::lock_guard<std::shared_mutex> lock(mutex_);
  if (fd_ >= 0) {
    return Status(Status::PRECONDITION_ERROR, "alread opened file");
  }
  block_size_ = block_size > 1 ?
      AlignNumberPowTwo(std::max<int64_t>(sizeof(void*), block_size)) : 1;
  access_options_ = options;
  return Status(Status::SUCCESS);
}

Status PositionalAtomicFileImpl::SetAllocationStrategy(int64_t init_size, double inc_factor) {
  std::lock_guard<std::shared_mutex> lock(mutex_);
  if (fd_ >= 0) {
    return Status(Status::PRECONDITION_ERROR, "alread opened file");
  }
  alloc_init_size_ = init_size;
  alloc_inc_factor_ = inc_factor;
  return Status(Status::SUCCESS);
}

Status PositionalAtomicFileImpl::CopyProperties(File* file) {
  std::shared_lock<std::shared_mutex> lock(mutex_);
  Status status = file->SetAllocationStrategy(alloc_init_size_, alloc_inc_factor_);
  auto* pos_file = dynamic_cast<PositionalFile*>(file);
  if (pos_file != nullptr) {
    status |= pos_file->SetAccessStrategy(block_size_, access_options_);
  }
  return status;
}

Status PositionalAtomicFileImpl::GetPath(std::string* path) {
  std::shared_lock<std::shared_mutex> lock(mutex_);
  if (fd_ < 0) {
    return Status(Status::PRECONDITION_ERROR, "not opened file");
  }
  if (path_.empty()) {
    return Status(Status::PRECONDITION_ERROR, "disabled path operatione");
  }
  *path = path_;
  return Status(Status::SUCCESS);
}

Status PositionalAtomicFileImpl::Rename(const std::string& new_path) {
  std::lock_guard<std::shared_mutex> lock(mutex_);
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

Status PositionalAtomicFileImpl::DisablePathOperations() {
  std::lock_guard<std::shared_mutex> lock(mutex_);
  if (fd_ < 0) {
    return Status(Status::PRECONDITION_ERROR, "not opened file");
  }
  path_.clear();
  return Status(Status::SUCCESS);
}

bool PositionalAtomicFileImpl::IsOpen() {
  std::shared_lock<std::shared_mutex> lock(mutex_);
  return fd_ >= 0;
}

int64_t PositionalAtomicFileImpl::GetBlockSize() {
  std::shared_lock<std::shared_mutex> lock(mutex_);
  return block_size_;
}

bool PositionalAtomicFileImpl::IsDirectIO() {
  std::shared_lock<std::shared_mutex> lock(mutex_);
  return access_options_ & PositionalFile::ACCESS_DIRECT;
}

Status PositionalAtomicFileImpl::AllocateSpace(int64_t min_size) {
  if (min_size <= trunc_size_) {
    return Status(Status::SUCCESS);
  }
  if (alloc_inc_factor_ <= 0) {
    trunc_size_ = min_size;
    return Status(Status::SUCCESS);
  }
  int64_t new_trunc_size =
      std::max(min_size, static_cast<int64_t>(trunc_size_ * alloc_inc_factor_));
  new_trunc_size = AlignNumber(new_trunc_size, std::max<int64_t>(PAGE_SIZE, block_size_));
  const Status status = TruncateFile(fd_, new_trunc_size);
  if (status != Status::SUCCESS) {
    return status;
  }
  trunc_size_ = new_trunc_size;
  if (page_cache_ != nullptr) {
    page_cache_->SetRegionSize(trunc_size_);
  }
  return Status(Status::SUCCESS);
}

Status PositionalAtomicFileImpl::ReadImpl(int64_t off, char* buf, size_t size) {
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
  if (page_cache_ != nullptr) {
    return page_cache_->Read(off, buf, size);
  }
  const int64_t end_position = off + size;
  Status status(Status::SUCCESS);
  const int64_t off_rem = off % block_size_;
  const int64_t end_rem = end_position % block_size_;
  if (off_rem > 0 || end_rem > 0) {
    const int64_t mod_off = off - off_rem;
    int64_t end_len = end_rem > 0 ? block_size_ - end_rem : 0;
    if (end_position + end_len > trunc_size_) {
      end_len = trunc_size_ - end_position;
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

Status PositionalAtomicFileImpl::WriteImpl(int64_t off, const char* buf, size_t size) {
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
  if (page_cache_ != nullptr) {
    return page_cache_->Write(off, buf, size);
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
      const int64_t mod_end_off = mod_end - block_size_;
      if (off_rem == 0 || mod_end_off != mod_off) {
        status |= PReadSequence(fd_, mod_end_off, block, block_size_);
      }
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

Status PositionalAtomicFile::TruncateFakely(int64_t size) {
  assert(size >= 0 && size <= MAX_MEMORY_SIZE);
  return impl_->TruncateFakely(size);
}

Status PositionalAtomicFile::Synchronize(bool hard, int64_t off, int64_t size) {
  assert(off >= 0 && size >= 0);
  return impl_->Synchronize(hard, off, size);
}

Status PositionalAtomicFile::GetSize(int64_t* size) {
  assert(size != nullptr);
  return impl_->GetSize(size);
}

Status PositionalAtomicFile::SetHeadBuffer(int64_t size) {
  return impl_->SetHeadBuffer(size);
}

Status PositionalAtomicFile::SetAccessStrategy(int64_t block_size, int32_t options) {
  assert(block_size > 0);
  return impl_->SetAccessStrategy(block_size, options);
}

Status PositionalAtomicFile::SetAllocationStrategy(int64_t init_size, double inc_factor) {
  assert(init_size >= 0 && inc_factor >= 0);
  return impl_->SetAllocationStrategy(init_size, inc_factor);
}

Status PositionalAtomicFile::CopyProperties(File* file) {
  assert(file != nullptr);
  return impl_->CopyProperties(file);
}

Status PositionalAtomicFile::GetPath(std::string* path) {
  assert(path != nullptr);
  return impl_->GetPath(path);
}

Status PositionalAtomicFile::Rename(const std::string& new_path) {
  return impl_->Rename(new_path);
}

Status PositionalAtomicFile::DisablePathOperations() {
  return impl_->DisablePathOperations();
}

bool PositionalAtomicFile::IsOpen() const {
  return impl_->IsOpen();
}

int64_t PositionalAtomicFile::GetBlockSize() const {
  return impl_->GetBlockSize();
}

bool PositionalAtomicFile::IsDirectIO() const {
  return impl_->IsDirectIO();
}

}  // namespace tkrzw

#endif

// END OF FILE
