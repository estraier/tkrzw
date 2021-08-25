/*************************************************************************************************
 * Implementation utilities for POSIX systems
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

#ifndef _TKRZW_SYS_UTIL_POSIX_H
#define _TKRZW_SYS_UTIL_POSIX_H

#include "tkrzw_sys_config.h"

#include <memory>
#include <string>
#include <vector>

#include <cinttypes>

#include "tkrzw_lib_common.h"
#include "tkrzw_str_util.h"
#include "tkrzw_thread_util.h"

namespace tkrzw {

/**
 * Reads data from a file descriptor at a position.
 * @param fd The file descriptor.
 * @param off The offset of a source region.
 * @param buf The pointer to the destination buffer.
 * @param size The size of the data to be read.
 * @return The result status.
 */
inline Status PReadSequence(int32_t fd, int64_t off, void* buf, size_t size) {
  char* wp = static_cast<char*>(buf);
  while (size > 0) {
    const int32_t rsiz = pread(fd, wp, size, off);
    if (rsiz < 0) {
      if (errno == EINTR) {
        continue;
      }
      return GetErrnoStatus("pread", errno);
    }
    if (rsiz == 0) {
      return Status(Status::INFEASIBLE_ERROR, "excessive region");
    }
    off += rsiz;
    wp += rsiz;
    size -= rsiz;
  }
  return Status(Status::SUCCESS);
}

/**
 * Writes data to a file descriptor at a position.
 * @param fd The file descriptor.
 * @param off The offset of the destination region.
 * @param buf The pointer to the source buffer.
 * @param size The size of the data to be written.
 * @return The result status.
 */
inline Status PWriteSequence(int32_t fd, int64_t off, const void* buf, size_t size) {
  const char* rp = static_cast<const char*>(buf);
  while (size > 0) {
    const int32_t rsiz = pwrite(fd, rp, size, off);
    if (rsiz < 0) {
      if (errno == EINTR) {
        continue;
      }
      return GetErrnoStatus("pwrite", errno);
    }
    off += rsiz;
    rp += rsiz;
    size -= rsiz;
  }
  return Status(Status::SUCCESS);
}

/**
 * Truncates the file of a file descriptor.
 * @param fd The file descriptor.
 * @param length The new length of the file.
 * @return The result status.
 */
inline Status TruncateFile(int32_t fd, int64_t length) {
  while (ftruncate(fd, length) != 0) {
    if (errno != EINTR) {
      return GetErrnoStatus("ftruncate", errno);
    }
  }
  return Status(Status::SUCCESS);
}

}  // namespace tkrzw

#endif  // _TKRZW_SYS_UTIL_POSIX_H

// END OF FILE
