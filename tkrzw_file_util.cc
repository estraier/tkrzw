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

#include "tkrzw_sys_config.h"

#include "tkrzw_file.h"
#include "tkrzw_file_util.h"
#include "tkrzw_hash_util.h"
#include "tkrzw_lib_common.h"
#include "tkrzw_str_util.h"
#include "tkrzw_thread_util.h"
#include "tkrzw_time_util.h"

#if defined(_SYS_WINDOWS_)
#include "tkrzw_sys_util_windows.h"
#define open _open
#define close _close
#define read _read
#define write _write
#define unlink _unlink
#define mkdir(a,b) _mkdir(a)
#define rmdir _rmdir
const char* const TMP_DIR_CANDIDATES[] = {
  "\\tmp", "\\temp", "\\var\\tmp", "\\",
};
#else
#include "tkrzw_sys_util_posix.h"
const char* const TMP_DIR_CANDIDATES[] = {
  "/tmp", "/temp", "/var/tmp", "/",
};
#endif

#if defined(_SYS_LINUX_)
#include <sys/sendfile.h>
#include <linux/fs.h>
#endif

namespace tkrzw {

#if defined(_SYS_WINDOWS_)
const char DIR_SEP_CHR = '\\';
const char* const DIR_SEP_STR = "\\";
const char EXT_SEP_CHR = '.';
const char* const EXT_SEP_STR = ".";
const char* const CURRENT_DIR_NAME = ".";
const char* const PARENT_DIR_NAME = "..";
#else
const char DIR_SEP_CHR = '/';
const char* const DIR_SEP_STR = "/";
const char EXT_SEP_CHR = '.';
const char* const EXT_SEP_STR = ".";
const char* const CURRENT_DIR_NAME = ".";
const char* const PARENT_DIR_NAME = "..";
#endif

std::string MakeTemporaryName() {
  static std::atomic_uint32_t count(0);
  const uint32_t current_count = count++;
  const uint32_t pid = GetProcessID();
  const double wtime = GetWallTime();
  const std::string& joined = SPrintF("%x:%x:%f", current_count, pid, wtime);
  const std::string& hashed =
      SPrintF("%04d%016llx", count % 10000, static_cast<unsigned long long>(HashFNV(joined)));
  return hashed;
}

std::string JoinPath(const std::string& base_path, const std::string& child_name) {
  if (child_name.empty()) {
    return base_path;
  }
  if (child_name.front() == DIR_SEP_CHR) {
    return child_name;
  }
  std::string joined_path = base_path;
  if (joined_path.empty()) {
    joined_path = DIR_SEP_STR;
  }
  if (joined_path.back() != DIR_SEP_CHR) {
    joined_path += DIR_SEP_STR;
  }
  joined_path += child_name;
  return joined_path;
}

std::string NormalizePath(const std::string& path) {
  if (path.empty()) {
    return "";
  }
  std::string norm_path;
  if (!IS_POSIX && DIR_SEP_CHR == '\\') {
    norm_path = StrReplace(path, "/", DIR_SEP_STR);
  } else {
    norm_path = path;
  }
  const bool is_absolute = path.front() == DIR_SEP_CHR;
  const auto& input_elems = StrSplit(path, DIR_SEP_CHR, true);
  std::vector<std::string> output_elems;
  for (const auto& elem : input_elems) {
    if (elem == CURRENT_DIR_NAME) {
      continue;
    }
    if (elem == PARENT_DIR_NAME) {
      if (output_elems.empty() && is_absolute) {
      } else if (output_elems.empty() || output_elems.back() == PARENT_DIR_NAME) {
        output_elems.emplace_back(PARENT_DIR_NAME);
      } else {
        output_elems.pop_back();
      }
      continue;
    }
    output_elems.emplace_back(elem);
  }
  norm_path.clear();
  if (is_absolute) {
    norm_path += DIR_SEP_STR;
  }
  for (int32_t i = 0; i < static_cast<int32_t>(output_elems.size()); i++) {
    norm_path += output_elems[i];
    if (i != static_cast<int32_t>(output_elems.size()) - 1) {
      norm_path += DIR_SEP_STR;
    }
  }
  return norm_path;
}

std::string PathToBaseName(const std::string& path) {
  size_t size = path.size();
  while (size > 1 && path[size - 1] == DIR_SEP_CHR) {
    size--;
  }
  const std::string tmp_path = path.substr(0, size);
  if (tmp_path == DIR_SEP_STR) {
    return tmp_path;
  }
  const size_t pos = tmp_path.rfind(DIR_SEP_CHR);
  if (pos == std::string::npos) {
    return tmp_path;
  }
  return tmp_path.substr(pos + 1);
}

std::string PathToDirectoryName(const std::string& path) {
  size_t size = path.size();
  while (size > 1 && path[size - 1] == DIR_SEP_CHR) {
    size--;
  }
  const std::string tmp_path = path.substr(0, size);
  if (tmp_path == DIR_SEP_STR) {
    return tmp_path;
  }
  const size_t pos = tmp_path.rfind(DIR_SEP_CHR);
  if (pos == std::string::npos) {
    return CURRENT_DIR_NAME;
  }
  if (pos == 0) {
    return DIR_SEP_STR;
  }
  return tmp_path.substr(0, pos);
}

std::string PathToExtension(const std::string& path) {
  const std::string& base_name = PathToBaseName(path);
  const size_t pos = base_name.rfind(EXT_SEP_CHR);
  if (pos == std::string::npos) {
    return "";
  }
  return base_name.substr(pos + 1);
}

Status GetRealPath(const std::string& path, std::string* real_path) {
#if defined(_SYS_WINDOWS_)
  assert(real_path != nullptr);
  char buf[4096];
  DWORD size = GetFullPathName(path.c_str(), sizeof(buf), buf, nullptr);
  if (size < 1) {
    return GetErrnoStatus("GetFullPathName", errno);
  }
  if (size < sizeof(buf)) {
    *real_path = std::string(buf);
    return Status(Status::SUCCESS);
  }
  char* lbuf = new char[size];
  DWORD nsiz = GetFullPathName(path.c_str(), size, lbuf, nullptr);
  if (nsiz < 1 || nsiz >= size) {
    delete[] lbuf;
    return GetErrnoStatus("GetFullPathName", errno);
  }
  *real_path = std::string(lbuf);
  delete[] lbuf;
  return Status(Status::SUCCESS);
#else
  assert(real_path != nullptr);
  real_path->clear();
  char buf[PATH_MAX];
  if (realpath(path.c_str(), buf) == nullptr) {
    return GetErrnoStatus("realpath", errno);
  }
  *real_path = std::string(buf);
  return Status(Status::SUCCESS);
#endif
}

Status ReadFileStatus(const std::string& path, FileStatus* fstats) {
#if defined(_SYS_WINDOWS_)
  assert(fstats != nullptr);
  WIN32_FILE_ATTRIBUTE_DATA ibuf;
  if (!GetFileAttributesEx(path.c_str(), GetFileExInfoStandard, &ibuf)) {
    return GetErrnoStatus("GetFileAttributesEx", errno);
  }
  if (ibuf.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY) {
    fstats->is_file = false;
    fstats->is_directory = true;
  } else {
    fstats->is_file = true;
    fstats->is_directory = false;
  }
  LARGE_INTEGER li;
  li.LowPart = ibuf.nFileSizeLow;
  li.HighPart = ibuf.nFileSizeHigh;
  fstats->file_size = li.QuadPart;
  li.LowPart = ibuf.ftLastWriteTime.dwLowDateTime;
  li.HighPart = ibuf.ftLastWriteTime.dwHighDateTime;
  fstats->modified_time = li.QuadPart;
  return Status(Status::SUCCESS);
#else
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
#endif
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
  static std::string tmp_path;
  static std::once_flag tmp_path_once_flag;
  std::call_once(tmp_path_once_flag, [&]() {
      for (const auto& tmp_candidate : TMP_DIR_CANDIDATES) {
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

Status WriteFileAtomic(const std::string& path, std::string_view content,
                       const std::string& tmp_path) {
  const std::string& mod_tmp_path = tmp_path.empty() ? path + ".tmp" : tmp_path;
  Status status = WriteFile(mod_tmp_path, content);
  if (status != Status::SUCCESS) {
    return status;
  }
  status = RenameFile(mod_tmp_path, path);
  if (status != Status::SUCCESS) {
    RemoveFile(mod_tmp_path);
    return status;
  }
  return Status(Status::SUCCESS);
}

Status ReadFile(const std::string& path, std::string* content, int64_t max_size) {
  assert(content != nullptr);
  content->clear();
  const int32_t fd = open(path.c_str(), O_RDONLY);
  if (fd < 0) {
    return GetErrnoStatus("open", errno);
  }
  Status status(Status::SUCCESS);
  constexpr size_t bufsiz = 8192;
  char buf[bufsiz];
  while (static_cast<int64_t>(content->size()) < max_size) {
    int32_t size = read(fd, buf, std::min<int64_t>(bufsiz, max_size - content->size()));
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

std::string ReadFileSimple(const std::string& path, std::string_view default_value,
                           int64_t max_size) {
  std::string content;
  return ReadFile(path, &content, max_size) == Status::SUCCESS ?
      content : std::string(default_value);
}

Status TruncateFile(const std::string& path, int64_t size) {
#if defined(_SYS_WINDOWS_)
  assert(size >= 0);
  HANDLE file_handle = CreateFile(path.c_str(), GENERIC_WRITE, 0, 0, OPEN_EXISTING, 0, 0);
  if (file_handle == nullptr || file_handle == INVALID_HANDLE_VALUE) {
    return Status(Status::SYSTEM_ERROR, "CreateFile failed");
  }
  Status status(Status::SUCCESS);
  LARGE_INTEGER li;
  li.QuadPart = size;
  if (!SetFilePointerEx(file_handle, li, nullptr, FILE_BEGIN)) {
    status |= Status(Status::SYSTEM_ERROR, "SetFilePointerEx failed");
  } else if (!SetEndOfFile(file_handle)) {
    status |= Status(Status::SYSTEM_ERROR, "SetEndOfFile failed");
  }
  if (!CloseHandle(file_handle)) {
    status |= Status(Status::SYSTEM_ERROR, "CloseHandle failed");
  }
  return status;
#else
  assert(size >= 0);
  if (truncate(path.c_str(), size) != 0) {
    return GetErrnoStatus("truncate", errno);
  }
  return Status(Status::SUCCESS);
#endif
}

Status RemoveFile(const std::string& path) {
  if (unlink(path.c_str()) != 0) {
    return GetErrnoStatus("unlink", errno);
  }
  return Status(Status::SUCCESS);
}

Status RenameFile(const std::string& src_path, const std::string& dest_path) {
  if (!IS_POSIX) {
    FileStatus src_stats;
    Status status = ReadFileStatus(src_path, &src_stats);
    if (status != Status::SUCCESS) {
      return status;
    }
    FileStatus dest_stats;
    status = ReadFileStatus(dest_path, &dest_stats);
    if (status == Status::SUCCESS) {
      if (src_stats.is_file && dest_stats.is_file) {
        status = RemoveFile(dest_path);
        if (status != Status::SUCCESS) {
          return status;
        }
      }
      if (src_stats.is_directory && dest_stats.is_directory) {
        status = RemoveDirectory(dest_path, false);
        if (status != Status::SUCCESS) {
          return status;
        }
      }
    }
  }
  if (rename(src_path.c_str(), dest_path.c_str()) != 0) {
    return GetErrnoStatus("rename", errno);
  }
  return Status(Status::SUCCESS);
}

Status CopyFileData(const std::string& src_path, const std::string& dest_path) {
#if defined(_SYS_LINUX_)
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
  int64_t file_size = lseek(src_fd, 0, SEEK_END);
  if (file_size < 0) {
    const Status status = GetErrnoStatus("lseek", errno);
    close(dest_fd);
    close(src_fd);
    return status;
  }
  if (lseek(src_fd, 0, SEEK_SET) != 0) {
    const Status status = GetErrnoStatus("lseek", errno);
    close(dest_fd);
    close(src_fd);
    return status;
  }
  Status status = TruncateFile(dest_fd, 0);
  if (status != Status::SUCCESS) {
    close(src_fd);
    close(dest_fd);
    return status;
  }
  if (ioctl(dest_fd, FICLONE, src_fd) != 0) {
    while (file_size > 0) {
      const int64_t sent_size =
          sendfile(dest_fd, src_fd, nullptr, std::min<int64_t>(file_size, 65536));
      if (sent_size < 0) {
        status |= GetErrnoStatus("read", errno);
        break;
      }
      file_size -= sent_size;
    }
  }
  if (close(dest_fd) != 0) {
    status |= GetErrnoStatus("close", errno);
  }
  if (close(src_fd) != 0) {
    status |= GetErrnoStatus("close", errno);
  }
  return status;
#else
  const int32_t src_fd = open(src_path.c_str(), O_RDONLY);
  if (src_fd < 0) {
    return GetErrnoStatus("open", errno);
  }
  RemoveFile(dest_path);
  const int32_t dest_fd = open(dest_path.c_str(), O_RDWR | O_CREAT | O_TRUNC, FILEPERM);
  if (dest_fd < 0) {
    const Status status = GetErrnoStatus("open", errno);
    close(src_fd);
    return status;
  }
  Status status(Status::SUCCESS);
  constexpr size_t bufsiz = 65536;
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
#endif
}

Status ReadDirectory(const std::string& path, std::vector<std::string>* children) {
#if defined(_SYS_WINDOWS_)
  assert(children != nullptr);
  std::string dpath = path;
  if (path.empty() || path.back() != DIR_SEP_CHR) {
    dpath.append(DIR_SEP_STR);
  }
  dpath.append("*");
  WIN32_FIND_DATA fbuf;
  HANDLE dh = FindFirstFile(dpath.c_str(), &fbuf);
  if (!dh || dh == INVALID_HANDLE_VALUE) {
    return GetErrnoStatus("FindFirstFile", errno);
  }
  if (std::strcmp(fbuf.cFileName, CURRENT_DIR_NAME) &&
      std::strcmp(fbuf.cFileName, PARENT_DIR_NAME)) {
    children->push_back(fbuf.cFileName);
  }
  while (FindNextFile(dh, &fbuf)) {
    if (std::strcmp(fbuf.cFileName, CURRENT_DIR_NAME) &&
        std::strcmp(fbuf.cFileName, PARENT_DIR_NAME)) {
      children->push_back(fbuf.cFileName);
    }
  }
  if (!FindClose(dh)) {
    return GetErrnoStatus("FindClose", errno);
  }
  return Status(Status::SUCCESS);
#else
  assert(children != nullptr);
  children->clear();
  DIR* dir = opendir(path.c_str());
  if (dir == nullptr) {
    return GetErrnoStatus("opendir", errno);
  }
  struct dirent *dp;
  while ((dp = readdir(dir)) != nullptr) {
    if (std::strcmp(dp->d_name, CURRENT_DIR_NAME) && std::strcmp(dp->d_name, PARENT_DIR_NAME)) {
      children->emplace_back(dp->d_name);
    }
  }
  if (closedir(dir) != 0) {
    return GetErrnoStatus("closedir", errno);
  }
  return Status(Status::SUCCESS);
#endif
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

Status SynchronizeFile(const std::string& path) {
#if defined(_SYS_WINDOWS_)
  if (PathIsDirectory(path)) {
    return Status(Status::SUCCESS);
  }
  const DWORD amode = GENERIC_READ;
  const DWORD smode = FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE;
  const DWORD cmode = OPEN_EXISTING;
  const DWORD flags = FILE_FLAG_RANDOM_ACCESS;
  const HANDLE file_handle =
      CreateFile(path.c_str(), amode, smode, nullptr, cmode, flags, nullptr);
  if (file_handle == nullptr || file_handle == INVALID_HANDLE_VALUE) {
    return GetSysErrorStatus("CreateFile", GetLastError());
  }
  Status status(Status::SUCCESS);
  if (!FlushFileBuffers(file_handle)) {
    status |= GetSysErrorStatus("FlushFileBuffers", GetLastError());
  }
  if (!CloseHandle(file_handle)) {
    status |= GetSysErrorStatus("CloseHandle", GetLastError());
  }
  return status;
#else
  const int32_t fd = open(path.c_str(), O_RDONLY);
  if (fd < 0) {
    return GetErrnoStatus("open", errno);
  }
  Status status(Status::SUCCESS);
  if (fsync(fd) != 0) {
    status = GetErrnoStatus("fsync", errno);
  }
  if (close(fd) != 0) {
    status |= GetErrnoStatus("close", errno);
  }
  return status;
#endif
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

PageCache::PageCache(
    int64_t page_size, int64_t capacity, ReadType read_func, WriteType write_func) :
    page_size_(page_size), read_func_(read_func), write_func_(write_func),
    slot_capacity_(std::max<int64_t>(capacity / NUM_SLOTS, 1)),
    slots_(), region_size_(0) {
  for (auto& slot : slots_) {
    slot.pages = new LinkedHashMap<int64_t, Page>(slot_capacity_ * 4 + 1);
  }
}

PageCache::~PageCache() {
  for (auto& slot : slots_) {
    auto& pages = *slot.pages;
    for (auto it = pages.begin(); it != pages.end(); ++it) {
      const int64_t off = it->key;
      Page& page = it->value;
      if (page.dirty) {
        WritePages(&slot, off, &page);
      }
      xfreealigned(page.buf);
    }
    delete slot.pages;
  }
}

Status PageCache::Read(int64_t off, void* buf, size_t size) {
  PageCache::PageRequest requests_buf[PAGE_REQUEST_BUFFER_SIZE];
  size_t num_requests = 0;
  PageCache::PageRequest* requests = MakePageRequest(off, size, requests_buf, &num_requests);
  char* wp = static_cast<char*>(buf);
  size_t batch_start_index = 0;
  for (size_t request_index = 0; request_index < num_requests; request_index++) {
    const auto& request = requests[request_index];
    const int32_t slot_index = GetSlotIndex(request.offset);
    auto& slot = slots_[slot_index];
    std::lock_guard<std::mutex> lock(slot.mutex);
    Page* page = FindPage(&slot, request.offset);
    if (page != nullptr && page->size >= static_cast<int64_t>(request.data_size)) {
      std::memcpy(wp, page->buf + request.prefix_size, request.copy_size);
      wp += request.copy_size;
      off += request.copy_size;
      size -= request.copy_size;
      batch_start_index = request_index + 1;
    } else {
      break;
    }
  }
  Status status(Status::SUCCESS);
  if (batch_start_index + 1 == num_requests) {
    const auto& request = requests[batch_start_index];
    const int32_t slot_index = GetSlotIndex(request.offset);
    auto& slot = slots_[slot_index];
    std::lock_guard<std::mutex> lock(slot.mutex);
    Page* page = nullptr;
    status = PreparePage(&slot, request.offset, request.data_size, true, nullptr, &page);
    if (status == Status::SUCCESS) {
      std::memcpy(wp, page->buf + request.prefix_size, request.copy_size);
      wp += request.copy_size;
      off += request.copy_size;
      size -= request.copy_size;
      status = ReduceCache(&slot);
    }
  } else if (batch_start_index != num_requests) {
    int32_t slot_indices[NUM_SLOTS];
    const int32_t num_slots =
        LockSlots(requests + batch_start_index, num_requests - batch_start_index, slot_indices);
    const auto& first_req = requests[batch_start_index];
    const auto& last_req = requests[num_requests - 1];
    const int64_t area_off = first_req.offset;
    const int64_t area_size = last_req.offset + last_req.data_size - first_req.offset;
    char* area_buf = static_cast<char*>(xmallocaligned(page_size_, area_size));
    status |= read_func_(area_off, area_buf, area_size);
    char* arp = area_buf;
    for (size_t request_index = batch_start_index;
         status == Status::SUCCESS && request_index < num_requests; request_index++) {
      const auto& request = requests[request_index];
      const int32_t slot_index = GetSlotIndex(request.offset);
      auto& slot = slots_[slot_index];
      Page* page = nullptr;
      status |= PreparePage(&slot, request.offset, request.data_size, false, arp, &page);
      if (status != Status::SUCCESS) {
        break;
      }
      arp += request.data_size;
      std::memcpy(wp, page->buf + request.prefix_size, request.copy_size);
      wp += request.copy_size;
      off += request.copy_size;
      size -= request.copy_size;
    }
    for (int32_t i = 0; status == Status::SUCCESS && i < num_slots; ++i) {
      const int32_t slot_index = slot_indices[i];
      auto& slot = slots_[slot_index];
      status |= ReduceCache(&slot);
    }
    xfreealigned(area_buf);
    UnlockSlots(slot_indices, num_slots);
  }
  if (requests != requests_buf) {
    delete[] requests;
  }
  return status;
}

Status PageCache::Write(int64_t off, const void* buf, size_t size) {
  PageCache::PageRequest requests_buf[PAGE_REQUEST_BUFFER_SIZE];
  size_t num_requests = 0;
  PageCache::PageRequest* requests = MakePageRequest(off, size, requests_buf, &num_requests);
  const char* rp = static_cast<const char*>(buf);
  size_t batch_start_index = 0;
  for (size_t request_index = 0; request_index < num_requests; request_index++) {
    const auto& request = requests[request_index];
    const int32_t slot_index = GetSlotIndex(request.offset);
    auto& slot = slots_[slot_index];
    std::lock_guard<std::mutex> lock(slot.mutex);
    Page* page = FindPage(&slot, request.offset);
    if (page != nullptr && page->size >= static_cast<int64_t>(request.data_size)) {
      std::memcpy(page->buf + request.prefix_size, rp, request.copy_size);
      page->dirty = true;
      rp += request.copy_size;
      off += request.copy_size;
      size -= request.copy_size;
      batch_start_index = request_index + 1;
    } else {
      break;
    }
  }
  Status status(Status::SUCCESS);
  if (batch_start_index + 1 == num_requests) {
    const auto& request = requests[batch_start_index];
    const int32_t slot_index = GetSlotIndex(request.offset);
    auto& slot = slots_[slot_index];
    std::lock_guard<std::mutex> lock(slot.mutex);
    Page* page = nullptr;
    const bool do_load = request.copy_size != request.data_size;
    status = PreparePage(&slot, request.offset, request.data_size, do_load, nullptr, &page);
    if (status == Status::SUCCESS) {
      std::memcpy(page->buf + request.prefix_size, rp, request.copy_size);
      page->dirty = true;
      rp += request.copy_size;
      off += request.copy_size;
      size -= request.copy_size;
      status = ReduceCache(&slot);
    }
  } else if (batch_start_index != num_requests) {
    int32_t slot_indices[NUM_SLOTS];
    const int32_t num_slots =
        LockSlots(requests + batch_start_index, num_requests - batch_start_index, slot_indices);
    const auto& first_req = requests[batch_start_index];
    const auto& last_req = requests[num_requests - 1];
    int64_t area_off = first_req.offset;
    int64_t area_size = last_req.offset + last_req.data_size - first_req.offset;
    char* area_buf = static_cast<char*>(xmallocaligned(page_size_, area_size));
    if (off % page_size_ != 0 || size % page_size_ != 0) {
      char* awp = area_buf;
      for (size_t request_index = batch_start_index;
           request_index < num_requests; request_index++) {
        const auto& request = requests[request_index];
        if (request.copy_size == page_size_ &&
            area_size >= page_size_) {
          area_off += page_size_;
          area_size -= page_size_;
          awp += page_size_;
        } else {
          break;
        }
      }
      if (area_size > 0) {
        status |= read_func_(area_off, awp, area_size);
      }
    }
    char* arp = area_buf;
    for (size_t request_index = batch_start_index;
         status == Status::SUCCESS && request_index < num_requests; request_index++) {
      const auto& request = requests[request_index];
      const int32_t slot_index = GetSlotIndex(request.offset);
      auto& slot = slots_[slot_index];
      Page* page = nullptr;
      status |= PreparePage(&slot, request.offset, request.data_size, false, arp, &page);
      if (status != Status::SUCCESS) {
        break;
      }
      arp += request.data_size;
      std::memcpy(page->buf + request.prefix_size, rp, request.copy_size);
      page->dirty = true;
      rp += request.copy_size;
      off += request.copy_size;
      size -= request.copy_size;
    }
    for (int32_t i = 0; status == Status::SUCCESS && i < num_slots; ++i) {
      const int32_t slot_index = slot_indices[i];
      auto& slot = slots_[slot_index];
      status |= ReduceCache(&slot);
    }
    xfreealigned(area_buf);
    UnlockSlots(slot_indices, num_slots);
  }
  while (true) {
    int64_t region_size = region_size_.load();
    if (off <= region_size) break;
    if (region_size_.compare_exchange_weak(region_size, off)) {
      break;
    }
  }
  if (requests != requests_buf) {
    delete[] requests;
  }
  return status;
}

Status PageCache::Flush(int64_t off, int64_t size) {
  Status status(Status::SUCCESS);
  if (size == 0) {
    size = std::max<int64_t>(0, region_size_.load() - off);
  }
  int64_t end_position = AlignNumber(off + size, page_size_);
  off -= off % page_size_;
  size = end_position - off;
  for (auto& slot : slots_) {
    std::lock_guard lock(slot.mutex);
    auto& pages = *slot.pages;
    for (auto it = pages.begin(); it != pages.end(); ++it) {
      const int64_t page_off = it->key;
      Page& page = it->value;
      if (page.dirty && page_off >= off && page_off < end_position) {
        status |= WritePages(&slot, page_off, &page);
      }
    }
  }
  return status;
}

void PageCache::Clear() {
  for (auto& slot : slots_) {
    std::lock_guard lock(slot.mutex);
    auto& pages = *slot.pages;
    for (auto it = pages.begin(); it != pages.end(); ++it) {
      Page& page = it->value;
      xfreealigned(page.buf);
    }
    pages.clear();
  }
}

int64_t PageCache::GetRegionSize() {
  return region_size_.load();
}

void PageCache::SetRegionSize(int64_t size) {
  return region_size_.store(size);
}

PageCache::PageRequest* PageCache::MakePageRequest(
    int64_t off, int64_t size, PageCache::PageRequest* buf, size_t* num_requests) {
  assert(off >= 0 && size >= 0 && buf != nullptr && num_requests != nullptr);
  const size_t num_possible_requests = size / page_size_ + 2;
  PageRequest* requests = num_possible_requests > PAGE_REQUEST_BUFFER_SIZE ?
      new PageRequest[num_possible_requests] : buf;
  const int64_t off_rem = off % page_size_;
  *num_requests = 0;
  if (off_rem != 0) {
    const int64_t page_off = off - off_rem;
    const int64_t copy_size = std::min<int64_t>(size, page_size_ - off_rem);
    requests[(*num_requests)++] = PageRequest({page_off, off_rem, page_size_, copy_size});
    off += copy_size;
    size -= copy_size;
  }
  while (size > 0) {
    const int64_t copy_size = std::min<int64_t>(size, page_size_);
    const int64_t data_size =
        std::max(copy_size, std::min(region_size_.load() - off, page_size_));
    requests[(*num_requests)++] = PageRequest({off, 0, data_size, copy_size});
    off += copy_size;
    size -= copy_size;
  }
  return requests;
}

int32_t PageCache::GetSlotIndex(int64_t off) {
  return (off / page_size_ / 32) % NUM_SLOTS;
}

PageCache::Page* PageCache::FindPage(Slot* slot, int64_t off) {
  auto *rec = slot->pages->Get(off, LinkedHashMap<int64_t, Page>::MOVE_LAST);
  if (rec == nullptr) {
    return nullptr;
  }
  return &rec->value;
}

int32_t PageCache::LockSlots(
    const PageRequest* requests, size_t num_requests, int32_t* slot_indices) {
  int32_t num_slots = 0;
  for (size_t request_index = 0; request_index < num_requests; request_index++) {
    const auto& request = requests[request_index];
    const int32_t slot_index = GetSlotIndex(request.offset);
    bool hit = false;
    for (int32_t i = 0; i < num_slots; i++) {
      if (slot_indices[i] == slot_index) {
        hit = true;
        break;
      }
    }
    if (!hit) {
      slot_indices[num_slots++] = slot_index;
    }
  }
  std::sort(slot_indices, slot_indices + num_slots);
  for (int32_t i = 0; i < num_slots; i++) {
    slots_[slot_indices[i]].mutex.lock();
  }
  return num_slots;
}

void PageCache::UnlockSlots(int32_t* slot_indices, int32_t num_slots) {
  for (int32_t i = num_slots - 1; i >= 0; i--) {
    slots_[slot_indices[i]].mutex.unlock();
  }
}

Status PageCache::PreparePage(
    Slot* slot, int64_t off, int64_t size, bool do_load, const char* batch_ptr, Page** result) {
  auto& pages = *slot->pages;
  auto *rec = pages.Get(off, LinkedHashMap<int64_t, Page>::MOVE_LAST);
  if (rec == nullptr) {
    char* buf = static_cast<char*>(xmallocaligned(page_size_, size));
    if (batch_ptr == nullptr) {
      if (do_load) {
        const Status status = read_func_(off, buf, size);
        if (status != Status::SUCCESS) {
          xfreealigned(buf);
          return status;
        }
      }
    } else {
      std::memcpy(buf, batch_ptr, size);
    }
    *result = &pages.Set(off, Page({buf, size, false}))->value;
  } else {
    Page* page = &rec->value;
    if (page->size < size) {
      char* buf = static_cast<char*>(xmallocaligned(page_size_, size));
      if (batch_ptr == nullptr) {
        if (do_load) {
          const Status status = read_func_(off, buf, size);
          if (status != Status::SUCCESS) {
            xfreealigned(buf);
            return status;
          }
        }
      } else {
        std::memcpy(buf, batch_ptr, size);
      }
      if (page->dirty) {
        std::memcpy(buf, page->buf, page->size);
      }
      xfreealigned(page->buf);
      page->buf = buf;
      page->size = size;
      *result = page;
    }
    *result = page;
  }
  return Status(Status::SUCCESS);
}

Status PageCache::ReduceCache(Slot* slot) {
  Status status(Status::SUCCESS);
  auto& pages = *slot->pages;
  int32_t num_excessives = pages.size() - slot_capacity_;
  while (num_excessives > 0) {
    auto& rec = pages.front();
    const int64_t off = rec.key;
    auto& page = rec.value;
    if (page.dirty) {
      status |= WritePages(slot, off, &page);
    }
    xfreealigned(page.buf);
    pages.Remove(rec.key);
    num_excessives--;
  }
  return status;
}

Status PageCache::WritePages(Slot* slot, int64_t off, Page* page) {
  Status status(Status::SUCCESS);
  auto& pages = *slot->pages;
  Page* prev_pages[16];
  int32_t num_prev_pages = 0;
  int64_t off_begin = off;
  for (int64_t cur_off = off - page_size_;
       num_prev_pages < static_cast<int32_t>(std::size(prev_pages)); cur_off -= page_size_) {
    auto *rec = pages.Get(cur_off);
    if (rec == nullptr) {
      break;
    }
    Page& prev_page = rec->value;
    if (!prev_page.dirty || prev_page.size != page_size_) {
      break;
    }
    prev_pages[num_prev_pages++] = &prev_page;
    off_begin = cur_off;
  }
  Page* next_pages[16];
  int32_t num_next_pages = 0;
  int64_t off_end = off + page->size;
  if (page->size == page_size_) {
    for (int64_t cur_off = off + page_size_;
         num_next_pages < static_cast<int32_t>(std::size(next_pages)); cur_off += page_size_) {
      auto *rec = pages.Get(cur_off);
      if (rec == nullptr) {
        break;
      }
      Page& next_page = rec->value;
      if (!next_page.dirty) {
        break;
      }
      next_pages[num_next_pages++] = &next_page;
      off_end = cur_off + next_page.size;
      if (next_page.size != page_size_) {
        break;
      }
    }
  }
  if (num_prev_pages > 0 || num_next_pages > 0) {
    const int64_t area_size = off_end - off_begin;
    char* area_buf = static_cast<char*>(xmallocaligned(page_size_, area_size));
    char* wp = area_buf;
    for (int32_t i = num_prev_pages - 1; i >= 0; i--) {
      Page& prev_page = *prev_pages[i];
      std::memcpy(wp, prev_page.buf, prev_page.size);
      prev_page.dirty = false;
      wp += prev_page.size;
    }
    std::memcpy(wp, page->buf, page->size);
    page->dirty = false;
    wp += page->size;
    for (int32_t i = 0; i < num_next_pages; i++) {
      Page& next_page = *next_pages[i];
      std::memcpy(wp, next_page.buf, next_page.size);
      next_page.dirty = false;
      wp += next_page.size;
    }
    status |= write_func_(off_begin, area_buf, area_size);
    xfreealigned(area_buf);
  } else {
    page->dirty = false;
    status |= write_func_(off, page->buf, page->size);
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
    data_ptr_(nullptr), data_size_(0), body_buf_(nullptr),
    rec_type_(RECORD_NORMAL) {}

FlatRecord::~FlatRecord() {
  delete[] body_buf_;
}

Status FlatRecord::Read(int64_t offset) {
  offset_ = offset;
  constexpr int64_t min_record_size = sizeof(uint8_t) * 2;
  const int64_t max_read_size = std::min(file_->GetSizeSimple() - offset, MAX_MEMORY_SIZE);
  int64_t record_size = max_read_size;
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
  if (*(uint8_t*)rp == RECORD_MAGIC_NORMAL) {
    rec_type_ = RECORD_NORMAL;
  } else if (*(uint8_t*)rp == RECORD_MAGIC_METADATA) {
    rec_type_ = RECORD_METADATA;
  } else {
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
  if (static_cast<int64_t>(data_size_) > max_read_size) {
    return Status(Status::BROKEN_DATA_ERROR, "too large record size");
  }
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

std::string_view FlatRecord::GetData() const {
  return std::string_view(data_ptr_, data_size_);
}

FlatRecord::RecordType FlatRecord::GetRecordType() const {
  return rec_type_;
}

size_t FlatRecord::GetOffset() const {
  return offset_;
}

size_t FlatRecord::GetWholeSize() const {
  return whole_size_;
}

Status FlatRecord::Write(std::string_view data, RecordType rec_type) {
  whole_size_ = sizeof(uint8_t) + SizeVarNum(data.size()) + data.size();
  char stack[WRITE_BUFFER_SIZE];
  char* write_buf = whole_size_ > sizeof(stack) ? new char[whole_size_] : stack;
  char* wp = write_buf;
  switch (rec_type) {
    case RECORD_METADATA:
      *(wp++) = RECORD_MAGIC_METADATA;
      break;
    default:
      *(wp++) = RECORD_MAGIC_NORMAL;
      break;
  }
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

Status FlatRecordReader::Read(std::string_view* str, FlatRecord::RecordType* rec_type) {
  constexpr int64_t min_record_size = sizeof(uint8_t) * 2;
  constexpr size_t max_header_size = sizeof(uint8_t) + sizeof(uint64_t);
  if (index_ + max_header_size <= data_size_) {
    size_t record_size = data_size_ - index_;
    const char* rp = buffer_ + index_;
    if (*(uint8_t*)rp == FlatRecord::RECORD_MAGIC_NORMAL) {
      if (rec_type != nullptr) {
        *rec_type = FlatRecord::RECORD_NORMAL;
      }
    } else if (*(uint8_t*)rp == FlatRecord::RECORD_MAGIC_METADATA) {
      if (rec_type != nullptr) {
        *rec_type = FlatRecord::RECORD_METADATA;
      }
    } else {
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
  if (*(uint8_t*)rp == FlatRecord::RECORD_MAGIC_NORMAL) {
    if (rec_type != nullptr) {
      *rec_type = FlatRecord::RECORD_NORMAL;
    }
  } else if (*(uint8_t*)rp == FlatRecord::RECORD_MAGIC_METADATA) {
    if (rec_type != nullptr) {
      *rec_type = FlatRecord::RECORD_METADATA;
    }
  } else {
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
  if (*(uint8_t*)rp == FlatRecord::RECORD_MAGIC_NORMAL) {
    if (rec_type != nullptr) {
      *rec_type = FlatRecord::RECORD_NORMAL;
    }
  } else if (*(uint8_t*)rp == FlatRecord::RECORD_MAGIC_METADATA) {
    if (rec_type != nullptr) {
      *rec_type = FlatRecord::RECORD_METADATA;
    }
  } else {
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
