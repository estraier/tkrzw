/*************************************************************************************************
 * Implementation utilities for Windows
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

#ifndef _TKRZW_SYS_UTIL_WINDOWS_H
#define _TKRZW_SYS_UTIL_WINDOWS_H

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
 * Get the message string of a system error code.
 * @param error_code The system error code, typically gotton with GetLastError.
 * @return The message string of the error code.
 */
inline std::string GetSysErrorString(int32_t error_code) {
  LPVOID msg_buf;
  const size_t msg_size = FormatMessageA(
      FORMAT_MESSAGE_ALLOCATE_BUFFER | FORMAT_MESSAGE_FROM_SYSTEM | FORMAT_MESSAGE_IGNORE_INSERTS,
      nullptr, error_code, MAKELANGID(LANG_NEUTRAL, SUBLANG_DEFAULT),
      (LPTSTR)&msg_buf, 0, nullptr);
  std::string msg_str(static_cast<char*>(msg_buf), msg_size);
  LocalFree(msg_buf);
  return msg_str;
}

/**
 * Gets a status according to a system error code of a system call.
 * @param call_name The name of the system call.
 * @param error_code The system error code, typically gotton with GetLastError.
 * @return The status object.
 */
inline Status GetSysErrorStatus(std::string_view call_name, int32_t error_code) {
  Status::Code status_code = Status::Code::SYSTEM_ERROR;
  switch (error_code) {
    case ERROR_FILE_NOT_FOUND: status_code = Status::Code::NOT_FOUND_ERROR; break;
    case ERROR_PATH_NOT_FOUND: status_code = Status::Code::NOT_FOUND_ERROR; break;
    case ERROR_INVALID_DRIVE: status_code = Status::Code::NOT_FOUND_ERROR; break;
    case ERROR_BAD_PATHNAME: status_code = Status::Code::NOT_FOUND_ERROR; break;
    case ERROR_TOO_MANY_OPEN_FILES: status_code = Status::Code::INFEASIBLE_ERROR; break;
    case ERROR_NOT_ENOUGH_MEMORY: status_code = Status::Code::INFEASIBLE_ERROR; break;
    case ERROR_OUTOFMEMORY: status_code = Status::Code::INFEASIBLE_ERROR; break;
    case ERROR_HANDLE_DISK_FULL: status_code = Status::Code::INFEASIBLE_ERROR; break;
    case ERROR_CURRENT_DIRECTORY: status_code = Status::Code::INFEASIBLE_ERROR; break;
    case ERROR_DIR_NOT_EMPTY: status_code = Status::Code::INFEASIBLE_ERROR; break;
    case ERROR_ACCESS_DENIED: status_code = Status::Code::PERMISSION_ERROR; break;
    case ERROR_WRITE_PROTECT: status_code = Status::Code::PERMISSION_ERROR; break;
    case ERROR_CANNOT_MAKE: status_code = Status::Code::PERMISSION_ERROR; break;
    case ERROR_NO_SUCH_PRIVILEGE: status_code = Status::Code::PERMISSION_ERROR; break;
    case ERROR_BUSY: status_code = Status::Code::INFEASIBLE_ERROR; break;
    case ERROR_ALREADY_EXISTS: status_code = Status::Code::DUPLICATION_ERROR; break;
    case ERROR_FILE_EXISTS: status_code = Status::Code::DUPLICATION_ERROR; break;
    default: break;
  }
  const std::string message = StrCat(call_name, ": ", std::to_string(error_code),
                                     ": ", GetSysErrorString(error_code));
  return Status(status_code, message);
}

/**
 * Truncates a file.
 * @param file_handle The file to truncate.
 * @param length The new length of the file.
 * @return The result status.
 */
inline Status TruncateFileInternally(HANDLE file_handle, int64_t length) {
  LARGE_INTEGER li;
  li.QuadPart = length;
  if (!SetFilePointerEx(file_handle, li, nullptr, FILE_BEGIN)) {
    return GetSysErrorStatus("SetFilePointerEx", GetLastError());
  }
  if (!SetEndOfFile(file_handle) && GetLastError() != 1224) {
    return GetSysErrorStatus("SetEndOfFile", GetLastError());
  }
  return Status(Status::SUCCESS);
}

/**
 * Truncates a file externally.
 * @param path The path of the file to truncate.
 * @param length The new length of the file.
 * @return The result status.
 */
inline Status TruncateFileExternally(const std::string& path, int64_t length) {
  const DWORD amode = GENERIC_WRITE;;
  const DWORD smode = FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE;
  const DWORD cmode = OPEN_EXISTING;
  const DWORD flags = FILE_FLAG_RANDOM_ACCESS;
  HANDLE file_handle = CreateFile(path.c_str(), amode, smode, nullptr, cmode, flags, nullptr);
  if (file_handle == nullptr || file_handle == INVALID_HANDLE_VALUE) {
    return GetSysErrorStatus("CreateFile", GetLastError());
  }
  Status status = TruncateFileInternally(file_handle, length);
  if (!CloseHandle(file_handle)) {
    status |= GetSysErrorStatus("CloseHandle", GetLastError());
  }
  return status;
}

/**
 * Remaps a memory map.
 * @param file_handle The handle of the data file.
 * @param map_size The new size of the mapping.
 * @param map_handle The pointer to the map handle, which is modified as the new map handle.
 * @param map The pointer to the map buffer, which is modified as the new map buffer.
 * @return The result status.
 */
inline Status RemapMemory(
    HANDLE file_handle, int64_t map_size, HANDLE* map_handle, char** map) {
  if (!UnmapViewOfFile(*map)) {
    return GetSysErrorStatus("UnmapViewOfFile", GetLastError());
  }
  if (!CloseHandle(*map_handle)) {
    return GetSysErrorStatus("CloseHandle", GetLastError());
  }
  LARGE_INTEGER sbuf;
  sbuf.QuadPart = map_size;
  HANDLE new_map_handle = CreateFileMapping(
      file_handle, nullptr, PAGE_READWRITE, sbuf.HighPart, sbuf.LowPart, nullptr);
  if (new_map_handle == nullptr || new_map_handle == INVALID_HANDLE_VALUE) {
    return GetSysErrorStatus("CreateFileMapping", GetLastError());
  }
  void* new_map = MapViewOfFile(new_map_handle, FILE_MAP_WRITE, 0, 0, 0);
  if (new_map == nullptr) {
    return GetSysErrorStatus("MapViewOfFile", GetLastError());
  }
  *map_handle = new_map_handle;
  *map = static_cast<char*>(new_map);
  return Status(Status::SUCCESS);
}

/**
 * Read from a file at a given offset.
 * @param file_handle The handle of the file to read from.
 * @param buf The buffer to store the read data.
 * @param count The number of bytes to read.
 * @param offset The offset position to read at.
 * @return The number of bytes acctuary read or -1 on failure.
 */
inline int32_t PositionalReadFile(
    HANDLE file_handle, void* buf, size_t count, int64_t offset) {
  OVERLAPPED obuf;
  obuf.Internal = 0;
  obuf.InternalHigh = 0;
  obuf.Offset = offset;
  obuf.OffsetHigh = offset >> 32;
  obuf.hEvent = nullptr;
  DWORD read_size = 0;
  if (ReadFile(file_handle, buf, count, &read_size, &obuf)) {
    return read_size;
  }
  return -1;
}

/**
 * Write to a file at a given offset.
 * @param file_handle The handle of the file to write to.
 * @param buf The buffer of the data to write.
 * @param count The number of bytes to write.
 * @param offset The offset position to write at.
 * @return The number of bytes acctuary written or -1 on failure.
 */
inline int32_t PositionalWriteFile(
    HANDLE file_handle, const void* buf, size_t count, int64_t offset) {
  OVERLAPPED obuf;
  obuf.Internal = 0;
  obuf.InternalHigh = 0;
  obuf.Offset = offset;
  obuf.OffsetHigh = offset >> 32;
  obuf.hEvent = nullptr;
  DWORD wrote_size = 0;
  if (WriteFile(file_handle, buf, count, &wrote_size, &obuf)) {
    return wrote_size;
  }
  return -1;
}

/**
 * Reads data from a file handle at a position.
 * @param fd The file handle.
 * @param off The offset of a source region.
 * @param buf The pointer to the destination buffer.
 * @param size The size of the data to be read.
 * @return The result status.
 */
inline Status PReadSequence(
    HANDLE file_handle, int64_t off, void* buf, size_t size) {
  char* wp = static_cast<char*>(buf);
  while (size > 0) {
    const int32_t rsiz = PositionalReadFile(file_handle, wp, size, off);
    if (rsiz < 0) {
      return GetSysErrorStatus("ReadFile", GetLastError());
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
 * Writes data to a file handle at a position.
 * @param fd The file handle.
 * @param off The offset of the destination region.
 * @param buf The pointer to the source buffer.
 * @param size The size of the data to be written.
 * @return The result status.
 */
inline Status PWriteSequence(
    HANDLE file_handle, int64_t off, const void* buf, size_t size) {
  const char* rp = static_cast<const char*>(buf);
  while (size > 0) {
    const int32_t rsiz = PositionalWriteFile(file_handle, rp, size, off);
    if (rsiz < 0) {
      return GetSysErrorStatus("WriteFile", GetLastError());
    }
    off += rsiz;
    rp += rsiz;
    size -= rsiz;
  }
  return Status(Status::SUCCESS);
}

}  // namespace tkrzw

#endif  // _TKRZW_SYS_UTIL_WINDOWS_H

// END OF FILE
