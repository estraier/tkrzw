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

inline Status TruncateFile(HANDLE file_handle, int64_t length) {
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

}  // namespace tkrzw

#endif  // _TKRZW_SYS_UTIL_WINDOWS_H

// END OF FILE
