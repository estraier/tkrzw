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
  assert(alignment > 0);
  void* ptr = std::aligned_alloc(alignment, size);
  if (ptr == nullptr) {
    throw std::bad_alloc();
  }
  return ptr;
#elif defined(_SYS_WINDOWS_)
  assert(alignment > 0);
  void* ptr = _aligned_malloc(size, alignment);
  if (ptr == nullptr) {
    throw std::bad_alloc();
  }
  return ptr;
#else
  assert(alignment > 0);
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
