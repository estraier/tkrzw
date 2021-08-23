/*************************************************************************************************
 * Common library features
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

#include "tkrzw_lib_common.h"
#include "tkrzw_str_util.h"

namespace tkrzw {

#if defined(_SYS_WINDOWS_)

const int32_t PAGE_SIZE = 4096;
const char* const PACKAGE_VERSION = _TKRZW_PKG_VERSION;;
const char* const LIBRARY_VERSION = _TKRZW_LIB_VERSION;;
const char* const OS_NAME = _TKRZW_OSNAME;
const bool IS_POSIX = _IS_POSIX;
const bool IS_BIG_ENDIAN = _IS_BIG_ENDIAN;
constexpr int32_t EDQUOT = 10001;

#else

const int32_t PAGE_SIZE = sysconf(_SC_PAGESIZE);
const char* const PACKAGE_VERSION = _TKRZW_PKG_VERSION;;
const char* const LIBRARY_VERSION = _TKRZW_LIB_VERSION;;
const char* const OS_NAME = _TKRZW_OSNAME;
const bool IS_POSIX = _IS_POSIX;
const bool IS_BIG_ENDIAN = _IS_BIG_ENDIAN;

#endif

void* xmallocaligned(size_t alignment, size_t size) {
#if defined(_SYS_LINUX_)
  assert(alignment >= sizeof(void*));
  size = AlignNumber(size, alignment);
  void* ptr = std::aligned_alloc(alignment, size);
  if (ptr == nullptr) {
    throw std::bad_alloc();
  }
  return ptr;
#elif defined(_SYS_POSIX_)
  assert(alignment >= sizeof(void*));
  void* ptr = nullptr;
  if (posix_memalign(&ptr, alignment, size) != 0) {
    throw std::bad_alloc();
  }
  return ptr;
#elif defined(_SYS_WINDOWS_)
  assert(alignment >= sizeof(void*));
  size = AlignNumber(size, alignment);
  void* ptr = _aligned_malloc(size, alignment);
  if (ptr == nullptr) {
    throw std::bad_alloc();
  }
  return ptr;
#else
  assert(alignment >= sizeof(void*));
  size = AlignNumber(size, alignment);
  void* ptr = xmalloc(size + sizeof(void*) + alignment);
  char* aligned = (char*)ptr + sizeof(void*);
  aligned += alignment - (intptr_t)aligned % alignment;
  std::memcpy(aligned - sizeof(void*), &ptr, sizeof(void*));
  return aligned;
#endif
}

void xfreealigned(void* ptr) {
#if defined(_SYS_LINUX_)
  assert(ptr != nullptr);
  std::free(ptr);
#elif defined(_SYS_POSIX_)
  assert(ptr != nullptr);
  std::free(ptr);
#elif defined(_SYS_WINDOWS_)
  assert(ptr != nullptr);
  _aligned_free(ptr);
#else
  assert(ptr != nullptr);
  void* orig = nullptr;
  std::memcpy(&orig, (char*)ptr - sizeof(void*), sizeof(void*));
  std::free(orig);
#endif
}

int64_t GetProcessID() {
#if defined(_SYS_WINDOWS_)
  return GetCurrentProcessId();
#else
  return getpid();
#endif
}

std::map<std::string, std::string> GetSystemInfo() {
  std::map<std::string, std::string> info;
#if defined(_SYS_LINUX_)
  info["proc_id"] = SPrintF("%lld", (long long)GetProcessID());
  struct rusage rbuf;
  std::memset(&rbuf, 0, sizeof(rbuf));
  if (getrusage(RUSAGE_SELF, &rbuf) == 0) {
    info["ru_utime"] = SPrintF("%0.6f",
                               rbuf.ru_utime.tv_sec + rbuf.ru_utime.tv_usec / 1000000.0);
    info["ru_stime"] = SPrintF("%0.6f",
                               rbuf.ru_stime.tv_sec + rbuf.ru_stime.tv_usec / 1000000.0);
    if (rbuf.ru_maxrss > 0) {
      int64_t size = rbuf.ru_maxrss * 1024LL;
      info["mem_peak"] = SPrintF("%lld", (long long)size);
      info["mem_size"] = SPrintF("%lld", (long long)size);
      info["mem_rss"] = SPrintF("%lld", (long long)size);
    }
  }
  std::ifstream ifs;
  ifs.open("/proc/self/status", std::ios_base::in | std::ios_base::binary);
  if (ifs) {
    std::string line;
    while (getline(ifs, line)) {
      size_t index = line.find(':');
      if (index != std::string::npos) {
        const std::string& name = line.substr(0, index);
        index++;
        while (index < line.size() && line[index] >= '\0' && line[index] <= ' ') {
          index++;
        }
        const std::string& value = line.substr(index);
        if (name == "VmPeak") {
          int64_t size = StrToIntMetric(value.c_str());
          if (size > 0) info["mem_peak"] = SPrintF("%lld", (long long)size);
        } else if (name == "VmSize") {
          int64_t size = StrToIntMetric(value.c_str());
          if (size > 0) info["mem_size"] = SPrintF("%lld", (long long)size);
        } else if (name == "VmRSS") {
          int64_t size = StrToIntMetric(value.c_str());
          if (size > 0) info["mem_rss"] = SPrintF("%lld", (long long)size);
        }
      }
    }
    ifs.close();
  }
  ifs.open("/proc/meminfo", std::ios_base::in | std::ios_base::binary);
  if (ifs) {
    std::string line;
    while (getline(ifs, line)) {
      size_t index = line.find(':');
      if (index != std::string::npos) {
        const std::string& name = line.substr(0, index);
        index++;
        while (index < line.size() && line[index] >= '\0' && line[index] <= ' ') {
          index++;
        }
        const std::string& value = line.substr(index);
        if (name == "MemTotal") {
          int64_t size = StrToIntMetric(value.c_str());
          if (size > 0) info["mem_total"] = SPrintF("%lld", (long long)size);
        } else if (name == "MemFree") {
          int64_t size = StrToIntMetric(value.c_str());
          if (size > 0) info["mem_free"] = SPrintF("%lld", (long long)size);
        } else if (name == "Cached") {
          int64_t size = StrToIntMetric(value.c_str());
          if (size > 0) info["mem_cached"] = SPrintF("%lld", (long long)size);
        }
      }
    }
    ifs.close();
  }
#elif defined(_SYS_POSIX_)
  info["proc_id"] = SPrintF("%lld", (long long)GetProcessID());
  struct rusage rbuf;
  std::memset(&rbuf, 0, sizeof(rbuf));
  if (getrusage(RUSAGE_SELF, &rbuf) == 0) {
    info["ru_utime"] = SPrintF("%0.6f",
                               rbuf.ru_utime.tv_sec + rbuf.ru_utime.tv_usec / 1000000.0);
    info["ru_stime"] = SPrintF("%0.6f",
                               rbuf.ru_stime.tv_sec + rbuf.ru_stime.tv_usec / 1000000.0);
    if (rbuf.ru_maxrss > 0) {
      int64_t size = rbuf.ru_maxrss * 1024LL;
      info["mem_peak"] = SPrintF("%lld", (long long)size);
      info["mem_size"] = SPrintF("%lld", (long long)size);
      info["mem_rss"] = SPrintF("%lld", (long long)size);
    }
  }
#elif defined(_SYS_WINDOWS_)
  const DWORD proc_id = GetCurrentProcessId();
  info["proc_id"] = SPrintF("%lld", (long long)proc_id);
  HANDLE proc_handle =
      OpenProcess(PROCESS_QUERY_INFORMATION | PROCESS_VM_READ, false, proc_id);
  if (proc_handle != nullptr) {
    PROCESS_MEMORY_COUNTERS pmc;
    if (GetProcessMemoryInfo(proc_handle, &pmc, sizeof(pmc))) {
      info["mem_peak"] = SPrintF("%lld", (long long)pmc.PeakWorkingSetSize);
      info["mem_size"] = SPrintF("%lld", (long long)pmc.QuotaPagedPoolUsage);
      info["mem_rss"] = SPrintF("%lld", (long long)pmc.WorkingSetSize);
    }
    CloseHandle(proc_handle);
  }
  MEMORYSTATUSEX stat;
  std::memset(&stat, 0, sizeof(stat));
  stat.dwLength = sizeof(stat);
  if (GlobalMemoryStatusEx(&stat)) {
    info["mem_total"] = ToString(stat.ullTotalPhys);
    info["mem_free"] = ToString(stat.ullAvailPageFile);
    const int64_t cached = stat.ullTotalPhys * (stat.dwMemoryLoad / 100.0);
    info["mem_cached"] = ToString(cached);
  }
#endif
  return info;
}

int64_t GetMemoryCapacity() {
  const std::map<std::string, std::string> records = tkrzw::GetSystemInfo();
  return StrToInt(tkrzw::SearchMap(records, "mem_total", "-1"));
}

int64_t GetMemoryUsage() {
  const std::map<std::string, std::string> records = tkrzw::GetSystemInfo();
  return StrToInt(tkrzw::SearchMap(records, "mem_rss", "-1"));
}

const Status& Status::OrDie() const {
  if (code_ != SUCCESS) {
    throw StatusException(*this);
  }
  return *this;
}

Status GetErrnoStatus(const char* call_name, int32_t sys_err_num) {
  auto msg = [&](const char* message) {
    return std::string(call_name) + ": " + message;
  };
  switch (sys_err_num) {
    case EAGAIN: return Status(Status::SYSTEM_ERROR, msg("temporarily unavailable"));
    case EINTR: return Status(Status::SYSTEM_ERROR, msg("interrupted by a signal"));
    case EACCES: return Status(Status::PERMISSION_ERROR, msg("permission denied"));
    case ENOENT: return Status(Status::NOT_FOUND_ERROR, msg("no such file"));
    case ENOTDIR: return Status(Status::NOT_FOUND_ERROR, msg("not a directory"));
    case EISDIR: return Status(Status::INFEASIBLE_ERROR, msg("duplicated directory"));
    case ELOOP: return Status(Status::INFEASIBLE_ERROR, msg("looped path"));
    case EFBIG: return Status(Status::INFEASIBLE_ERROR, msg("too big file"));
    case ENOSPC: return Status(Status::INFEASIBLE_ERROR, msg("no enough space"));
    case ENOMEM: return Status(Status::INFEASIBLE_ERROR, msg("no enough memory"));
    case EEXIST: return Status(Status::DUPLICATION_ERROR, msg("already exist"));
    case ENOTEMPTY: return Status(Status::INFEASIBLE_ERROR, msg("not empty"));
    case EXDEV: return Status(Status::INFEASIBLE_ERROR, msg("cross device move"));
    case EBADF: return Status(Status::SYSTEM_ERROR, msg("bad file descriptor"));
    case EINVAL: return Status(Status::SYSTEM_ERROR, msg("invalid file descriptor"));
    case EIO: return Status(Status::SYSTEM_ERROR, msg("low-level I/O error"));
    case EFAULT: return Status(Status::SYSTEM_ERROR, msg("fault buffer address"));
    case EDQUOT: return Status(Status::INFEASIBLE_ERROR, msg("exhausted quota"));
    case EMFILE: return Status(Status::INFEASIBLE_ERROR, msg("exceeding process limit"));
    case ENFILE: return Status(Status::INFEASIBLE_ERROR, msg("exceeding system-wide limit"));
    case ENAMETOOLONG: return Status(Status::INFEASIBLE_ERROR, msg("too long name"));
    case ETXTBSY: return Status(Status::INFEASIBLE_ERROR, msg("busy file"));
    case EOVERFLOW: return Status(Status::INFEASIBLE_ERROR, msg("size overflow"));
  }
  return Status(Status::SYSTEM_ERROR, msg("unknown error: ") + std::to_string(sys_err_num));
}

}  // namespace tkrzw

// END OF FILE
