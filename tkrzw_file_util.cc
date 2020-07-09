/*************************************************************************************************
 * File system utilities
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

#include "tkrzw_file.h"
#include "tkrzw_file_util.h"
#include "tkrzw_lib_common.h"
#include "tkrzw_str_util.h"
#include "tkrzw_sys_config.h"
#include "tkrzw_thread_util.h"

namespace tkrzw {

std::string MakeTemporaryName() {
  static std::atomic_uint32_t count(0);
  const uint32_t current_count = count++;
  int32_t pid = getpid();
  double wtime = GetWallTime();
  std::unique_ptr<int> ptr(new int(0));
  const std::string& joined = SPrintF("%x:%x:%f:%p", current_count, pid, wtime, ptr.get());
  const std::string& hashed =
      SPrintF("%04d%016llx", count % 10000, static_cast<unsigned long long>(HashFNV(joined)));
  return hashed;
}

std::string JoinPath(const std::string& base_path, const std::string& child_name) {
  if (child_name.empty()) {
    return base_path;
  }
  if (child_name.front() == '/') {
    return child_name;
  }
  std::string joined_path = base_path;
  if (joined_path.empty()) {
    joined_path = "/";
  }
  if (joined_path.back() != '/') {
    joined_path += "/";
  }
  joined_path += child_name;
  return joined_path;
}

std::string NormalizePath(const std::string& path) {
  if (path.empty()) {
    return "";
  }
  const bool is_absolute = path.front() == '/';
  const auto& input_elems = StrSplit(path, '/', true);
  std::vector<std::string> output_elems;
  for (const auto& elem : input_elems) {
    if (elem == ".") {
      continue;
    }
    if (elem == "..") {
      if (!output_elems.empty()) {
        output_elems.pop_back();
      }
      continue;
    }
    output_elems.emplace_back(elem);
  }
  std::string norm_path;
  if (is_absolute) {
    norm_path += "/";
  }
  for (int32_t i = 0; i < static_cast<int32_t>(output_elems.size()); i++) {
    norm_path += output_elems[i];
    if (i != static_cast<int32_t>(output_elems.size()) - 1) {
      norm_path += "/";
    }
  }
  return norm_path;
}

std::string PathToBaseName(const std::string& path) {
  size_t size = path.size();
  while (size > 1 && path[size - 1] == '/') {
    size--;
  }
  const std::string tmp_path = path.substr(0, size);
  if (tmp_path == "/") {
    return tmp_path;
  }
  const size_t pos = tmp_path.rfind('/');
  if (pos == std::string::npos) {
    return tmp_path;
  }
  return tmp_path.substr(pos + 1);
}

std::string PathToDirectoryName(const std::string& path) {
  size_t size = path.size();
  while (size > 1 && path[size - 1] == '/') {
    size--;
  }
  const std::string tmp_path = path.substr(0, size);
  if (tmp_path == "/") {
    return tmp_path;
  }
  const size_t pos = tmp_path.rfind('/');
  if (pos == std::string::npos) {
    return ".";
  }
  return tmp_path.substr(0, pos);
}

std::string PathToExtension(const std::string& path) {
  const std::string& base_name = PathToBaseName(path);
  const size_t pos = base_name.rfind('.');
  if (pos == std::string::npos) {
    return "";
  }
  return base_name.substr(pos + 1);
}

Status GetRealPath(const std::string& path, std::string* real_path) {
  assert(real_path != nullptr);
  real_path->clear();
  char buf[PATH_MAX];
  if (realpath(path.c_str(), buf) == nullptr) {
    return GetErrnoStatus("realpath", errno);
  }
  *real_path = std::string(buf);
  return Status(Status::SUCCESS);
}

Status ReadFileStatus(const std::string& path, FileStatus* fstats) {
  assert(fstats != nullptr);
  struct stat sbuf;
  if (lstat(path.c_str(), &sbuf) != 0) {
    return GetErrnoStatus("lstat", errno);
  }
  fstats->is_file = S_ISREG(sbuf.st_mode);
  fstats->is_directory = S_ISDIR(sbuf.st_mode);
  fstats->file_size = sbuf.st_size;
  fstats->modified_time = sbuf.st_mtime;
  return Status(Status::SUCCESS);
}

bool PathIsFile(const std::string& path) {
  FileStatus fstats;
  return ReadFileStatus(path, &fstats) == Status::SUCCESS && fstats.is_file;
}

bool PathIsDirectory(const std::string& path) {
  FileStatus fstats;
  return ReadFileStatus(path, &fstats) == Status::SUCCESS && fstats.is_directory;
}

int64_t GetFileSize(const std::string& path) {
  FileStatus fstats;
  return ReadFileStatus(path, &fstats) == Status::SUCCESS && fstats.is_file ?
      fstats.file_size : -1;
}

std::string GetPathToTemporaryDirectory() {
  static const char* tmp_candidates[] = {
    "/tmp", "/temp", "/var/tmp", "/",
  };
  static std::string tmp_path;
  static std::once_flag tmp_path_once_flag;
  std::call_once(tmp_path_once_flag, [&]() {
      for (const auto& tmp_candidate : tmp_candidates) {
        if (PathIsDirectory(tmp_candidate)) {
          tmp_path = tmp_candidate;
          break;
        }
      }
    });
  return tmp_path;
}

Status WriteFile(const std::string& path, std::string_view content) {
  const int32_t oflags = O_RDWR | O_CREAT | O_TRUNC;
  const int32_t fd = open(path.c_str(), oflags, FILEPERM);
  if (fd < 0) {
    return GetErrnoStatus("open", errno);
  }
  Status status(Status::SUCCESS);
  const char* rp = content.data();
  size_t size = content.size();
  while (size > 0) {
    int32_t step = write(fd, rp, size);
    if (step < 0) {
      status = GetErrnoStatus("write", errno);
      break;
    }
    rp += step;
    size -= step;
  }
  if (close(fd) != 0) {
    status |= GetErrnoStatus("close", errno);
  }
  return status;
}

Status ReadFile(const std::string& path, std::string* content) {
  assert(content != nullptr);
  content->clear();
  const int32_t fd = open(path.c_str(), O_RDONLY);
  if (fd < 0) {
    return GetErrnoStatus("open", errno);
  }
  Status status(Status::SUCCESS);
  constexpr size_t bufsiz = 8192;
  char buf[bufsiz];
  while (true) {
    int32_t size = read(fd, buf, bufsiz);
    if (size <= 0) {
      if (size < 0) {
        status |= GetErrnoStatus("read", errno);
      }
      break;
    }
    content->append(buf, size);
  }
  if (close(fd) != 0) {
    status |= GetErrnoStatus("close", errno);
  }
  return status;
}

Status RemoveFile(const std::string& path) {
  if (unlink(path.c_str()) != 0) {
    return GetErrnoStatus("unlink", errno);
  }
  return Status(Status::SUCCESS);
}

Status RenameFile(const std::string& src_path, const std::string& dest_path) {
  if (rename(src_path.c_str(), dest_path.c_str()) != 0) {
    return GetErrnoStatus("rename", errno);
  }
  return Status(Status::SUCCESS);
}

Status CopyFile(const std::string& src_path, const std::string& dest_path) {
  // TODO: use sendfile on Linux.
  const int32_t src_fd = open(src_path.c_str(), O_RDONLY);
  if (src_fd < 0) {
    return GetErrnoStatus("open", errno);
  }
  const int32_t dest_fd = open(dest_path.c_str(), O_RDWR | O_CREAT | O_TRUNC, FILEPERM);
  if (dest_fd < 0) {
    const Status status = GetErrnoStatus("open", errno);
    close(src_fd);
    return status;
  }
  Status status(Status::SUCCESS);
  constexpr size_t bufsiz = 8192;
  char buf[bufsiz];
  while (true) {
    int32_t size = read(src_fd, buf, bufsiz);
    if (size <= 0) {
      if (size < 0) {
        status |= GetErrnoStatus("read", errno);
      }
      break;
    }
    const char* rp = buf;
    while (size > 0) {
      int32_t step = write(dest_fd, rp, size);
      if (step < 0) {
        status = GetErrnoStatus("write", errno);
        break;
      }
      rp += step;
      size -= step;
    }
  }
  if (close(dest_fd) != 0) {
    status |= GetErrnoStatus("close", errno);
  }
  if (close(src_fd) != 0) {
    status |= GetErrnoStatus("close", errno);
  }
  return status;
}

Status ReadDirectory(const std::string& path, std::vector<std::string>* children) {
  assert(children != nullptr);
  children->clear();
  DIR* dir = opendir(path.c_str());
  if (dir == nullptr) {
    return GetErrnoStatus("opendir", errno);
  }
  struct dirent *dp;
  while ((dp = readdir(dir)) != nullptr) {
    if (std::strcmp(dp->d_name, ".") && std::strcmp(dp->d_name, "..")) {
      children->emplace_back(dp->d_name);
    }
  }
  if (closedir(dir) != 0) {
    return GetErrnoStatus("closedir", errno);
  }
  return Status(Status::SUCCESS);
}

Status MakeDirectory(const std::string& path, bool recursive) {
  if (recursive) {
    const std::string& norm_path = NormalizePath(path);
    if (norm_path.empty()) {
      return Status(Status::PRECONDITION_ERROR, "invalid path");
    }
    std::string concat_path ;
    if (norm_path.front() == '/') {
      concat_path += "/";
    }
    const auto& elems = StrSplit(norm_path, '/');
    for (size_t i = 0; i < elems.size(); i++) {
      if (i > 0) {
        concat_path += "/";
      }
      concat_path += elems[i];
      if (PathIsDirectory(concat_path)) {
        continue;
      }
      if (mkdir(concat_path.c_str(), DIRPERM) == 0) {
        continue;
      }
      return GetErrnoStatus("mkdir", errno);
    }
  } else {
    if (mkdir(path.c_str(), DIRPERM) != 0) {
      return GetErrnoStatus("mkdir", errno);
    }
  }
  return Status(Status::SUCCESS);
}

Status RemoveDirectory(const std::string& path, bool recursive) {
  if (rmdir(path.c_str()) == 0) {
    return Status(Status::SUCCESS);
  }
  if (!recursive || errno != ENOTEMPTY) {
    return GetErrnoStatus("rmdir", errno);
  }
  Status status(Status::SUCCESS);
  std::vector<std::string> stack;
  stack.emplace_back(path);
  while (!stack.empty()) {
    const std::string cur_path = stack.back();
    stack.pop_back();
    if (unlink(cur_path.c_str()) == 0) {
      continue;
    }
    if (errno != EISDIR) {
      status |= GetErrnoStatus("unlink", errno);
      continue;
    }
    if (rmdir(cur_path.c_str()) == 0) {
      continue;
    }
    if (errno != ENOTEMPTY) {
      status |= GetErrnoStatus("rmdir", errno);
      continue;
    }
    std::vector<std::string> child_names;
    Status tmp_status = ReadDirectory(cur_path, &child_names);
    if (tmp_status != Status::SUCCESS) {
      status |= tmp_status;
      continue;
    }
    if (child_names.empty()) {
      continue;
    }
    stack.emplace_back(cur_path);
    for (const auto& child_name : child_names) {
      stack.emplace_back(JoinPath(cur_path, child_name));
    }
  }
  return status;
}

TemporaryDirectory::TemporaryDirectory(
    bool cleanup, const std::string& prefix, const std::string& base_dir)
    : cleanup_(cleanup) {
  tmp_dir_path_ = JoinPath(base_dir.empty() ? GetPathToTemporaryDirectory() : base_dir,
                           prefix + MakeTemporaryName());
  creation_status_ = MakeDirectory(tmp_dir_path_);
}

TemporaryDirectory::~TemporaryDirectory() {
  if (cleanup_) {
    RemoveDirectory(tmp_dir_path_, true);
  }
}

Status TemporaryDirectory::CreationStatus() const {
  return creation_status_;
}

std::string TemporaryDirectory::Path() const {
  return tmp_dir_path_;
}

std::string TemporaryDirectory::MakeUniquePath(
    const std::string& prefix, const std::string& suffix) const {
  return JoinPath(tmp_dir_path_, StrCat(prefix, MakeTemporaryName(), suffix));
}

Status TemporaryDirectory::CleanUp() const {
  std::vector<std::string> child_names;
  Status status = ReadDirectory(tmp_dir_path_, &child_names);
  if (status != Status::SUCCESS) {
    return status;
  }
  for (const auto& child_name : child_names) {
    const std::string& child_path = JoinPath(tmp_dir_path_, child_name);
    status |= PathIsDirectory(child_path) ?
        RemoveDirectory(child_path, true) : RemoveFile(child_path);
  }
  return status;
}

FileReader::FileReader(File* file) : file_(file), offset_(0), data_size_(0), index_(0) {}

Status FileReader::ReadLine(std::string* str, size_t max_size) {
  assert(str != nullptr);
  str->clear();
  while (true) {
    while (index_ < data_size_) {
      const int32_t c = buffer_[index_++];
      str->push_back(c);
      if (c == '\n' || (max_size > 0 && str->size() >= max_size)) {
        return Status(Status::SUCCESS);
      }
    }
    int64_t file_size = 0;
    Status status = file_->GetSize(&file_size);
    if (status != Status::SUCCESS) {
      return status;
    }
    int64_t remaining = file_size - offset_;
    if (remaining < 1) {
      if (str->empty()) {
        break;
      }
      return Status(Status::SUCCESS);
    }
    remaining = std::min(remaining, static_cast<int64_t>(BUFFER_SIZE));
    status = file_->Read(offset_, buffer_, remaining);
    if (status != Status::SUCCESS) {
      return status;
    }
    offset_ += remaining;
    data_size_ = remaining;
    index_ = 0;
  }
  return Status(Status::NOT_FOUND_ERROR);
}

FlatRecord::FlatRecord(File* file) :
    file_(file), offset_(0), whole_size_(0),
    data_ptr_(nullptr), data_size_(0), body_buf_(nullptr) {}

FlatRecord::~FlatRecord() {
  delete[] body_buf_;
}

Status FlatRecord::Read(int64_t offset) {
  offset_ = offset;
  constexpr int64_t min_record_size = sizeof(uint8_t) * 2;
  int64_t record_size = file_->GetSizeSimple() - offset;
  if (record_size > static_cast<int64_t>(READ_BUFFER_SIZE)) {
    record_size = READ_BUFFER_SIZE;
  } else {
    if (record_size < min_record_size) {
      return Status(Status::BROKEN_DATA_ERROR, "too short record data");
    }
  }
  Status status = file_->Read(offset, buffer_, record_size);
  if (status != Status::SUCCESS) {
    return status;
  }
  const char* rp = buffer_;
  if (*(uint8_t*)rp != RECORD_MAGIC) {
    return Status(Status::BROKEN_DATA_ERROR, "invalid record magic number");
  }
  rp++;
  record_size--;
  uint64_t num = 0;
  size_t step = ReadVarNum(rp, record_size, &num);
  if (step < 1) {
    return Status(Status::BROKEN_DATA_ERROR, "invalid record size");
  }
  data_size_ = num;
  rp += step;
  record_size -= step;
  const size_t header_size = rp - buffer_;
  whole_size_ = header_size + data_size_;
  data_ptr_ = nullptr;
  if (record_size >= static_cast<int64_t>(data_size_)) {
    data_ptr_ = rp;
  } else {
    const int64_t body_offset = offset + header_size;
    delete[] body_buf_;
    body_buf_ = new char[data_size_];
    const Status status = file_->Read(body_offset, body_buf_, data_size_);
    if (status != Status::SUCCESS) {
      return status;
    }
    data_ptr_ = body_buf_;
  }
  return Status(Status::SUCCESS);
}

std::string_view FlatRecord::GetData() {
  return std::string_view(data_ptr_, data_size_);
}

size_t FlatRecord::GetOffset() const {
  return offset_;
}

size_t FlatRecord::GetWholeSize() const {
  return whole_size_;
}

Status FlatRecord::Write(std::string_view data) {
  whole_size_ = sizeof(uint8_t) + SizeVarNum(data.size()) + data.size();
  char stack[WRITE_BUFFER_SIZE];
  char* write_buf = whole_size_ > sizeof(stack) ? new char[whole_size_] : stack;
  char* wp = write_buf;
  *(wp++) = RECORD_MAGIC;
  wp += WriteVarNum(wp, data.size());
  std::memcpy(wp, data.data(), data.size());
  const Status status = file_->Append(write_buf, whole_size_, &offset_);
  if (write_buf != stack) {
    delete[] write_buf;
  }
  return status;
}

FlatRecordReader::FlatRecordReader(File* file, size_t buffer_size)
    : file_(file), offset_(0), buffer_(nullptr), buffer_size_(0), data_size_(0), index_(0) {
  if (buffer_size < 1) {
    buffer_size_ = DEFAULT_BUFFER_SIZE;
  } else {
    buffer_size_ = std::max(buffer_size, sizeof(uint64_t));
  }
  buffer_ = new char[buffer_size_];
}

FlatRecordReader::~FlatRecordReader() {
  delete[] buffer_;
}

Status FlatRecordReader::Read(std::string_view* str) {
  constexpr int64_t min_record_size = sizeof(uint8_t) * 2;
  constexpr size_t max_header_size = sizeof(uint8_t) + sizeof(uint64_t);
  if (index_ + max_header_size <= data_size_) {
    size_t record_size = data_size_ - index_;
    const char* rp = buffer_ + index_;
    if (*(uint8_t*)rp != FlatRecord::RECORD_MAGIC) {
      return Status(Status::BROKEN_DATA_ERROR, "invalid record magic number");
    }
    rp++;
    record_size--;
    uint64_t value_size = 0;
    size_t step = ReadVarNum(rp, record_size, &value_size);
    if (step < 1) {
      return Status(Status::BROKEN_DATA_ERROR, "invalid record size");
    }
    rp += step;
    record_size -= step;
    if (record_size >= value_size) {
      *str = std::string_view(rp, value_size);
      index_ = rp - buffer_ + value_size;
      offset_ += sizeof(uint8_t) + step + value_size;
      return Status(Status::SUCCESS);
    }
  }
  int64_t file_size = 0;
  Status status = file_->GetSize(&file_size);
  if (status != Status::SUCCESS) {
    return status;
  }
  if (file_size <= offset_ + min_record_size) {
    return Status(Status::NOT_FOUND_ERROR);
  }
  data_size_ = std::min(static_cast<int64_t>(buffer_size_), file_size - offset_);
  if (data_size_ < min_record_size) {
    return Status(Status::NOT_FOUND_ERROR);
  }
  status = file_->Read(offset_, buffer_, data_size_);
  if (status != Status::SUCCESS) {
    return status;
  }
  size_t record_size = data_size_;
  const char* rp = buffer_;
  if (*(uint8_t*)rp != FlatRecord::RECORD_MAGIC) {
    return Status(Status::BROKEN_DATA_ERROR, "invalid record magic number");
  }
  rp++;
  record_size--;
  uint64_t value_size = 0;
  size_t step = ReadVarNum(rp, record_size, &value_size);
  if (step < 1) {
    return Status(Status::BROKEN_DATA_ERROR, "invalid record size");
  }
  rp += step;
  record_size -= step;
  if (record_size >= value_size) {
    *str = std::string_view(rp, value_size);
    index_ = rp - buffer_ + value_size;
    offset_ += sizeof(uint8_t) + step + value_size;
    return Status(Status::SUCCESS);
  }
  data_size_ = 0;
  index_ = 0;
  delete[] buffer_;
  buffer_size_ += value_size - record_size;
  buffer_ = new char[buffer_size_];
  status = file_->Read(offset_, buffer_, buffer_size_);
  if (status != Status::SUCCESS) {
    return status;
  }
  record_size = buffer_size_;
  rp = buffer_;
  if (*(uint8_t*)rp != FlatRecord::RECORD_MAGIC) {
    return Status(Status::BROKEN_DATA_ERROR, "invalid record magic number");
  }
  rp++;
  record_size--;
  value_size = 0;
  step = ReadVarNum(rp, record_size, &value_size);
  if (step < 1) {
    return Status(Status::BROKEN_DATA_ERROR, "invalid record size");
  }
  rp += step;
  record_size -= step;
  if (record_size < value_size) {
    return Status(Status::BROKEN_DATA_ERROR, "too short record data");
  }
  *str = std::string_view(rp, value_size);
  offset_ += sizeof(uint8_t) + step + value_size;
  return Status(Status::SUCCESS);
}

}  // namespace tkrzw

// END OF FILE
