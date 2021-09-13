/*************************************************************************************************
 * System-dependent configurations
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

#ifndef _TKRZW_SYS_CONFIG_H
#define _TKRZW_SYS_CONFIG_H

#if __cplusplus < 201412L
#error expecting C++17 standard
#endif

#if defined(__linux__)

#define _SYS_LINUX_
#define _SYS_POSIX_
#define _TKRZW_OSNAME     "Linux"

#elif defined(__FreeBSD__)

#define _SYS_FREEBSD_
#define _SYS_POSIX_
#define _TKRZW_OSNAME     "FreeBSD"

#elif defined(__NetBSD__)

#define _SYS_NETBSD_
#define _SYS_POSIX_
#define _TKRZW_OSNAME     "NetBSD"

#elif defined(__OpenBSD__)

#define _SYS_OPENBSD_
#define _SYS_POSIX_
#define _TKRZW_OSNAME     "OpenBSD"

#elif defined(__sun__) || defined(__sun)

#define _SYS_SUNOS_
#define _SYS_POSIX_
#define _TKRZW_OSNAME     "SunOS"

#elif defined(__hpux)

#define _SYS_HPUX_
#define _SYS_POSIX_
#define _TKRZW_OSNAME     "HP-UX"

#elif defined(__osf)

#define _SYS_TRU64_
#define _SYS_POSIX_
#define _TKRZW_OSNAME     "Tru64"

#elif defined(_AIX)

#define _SYS_AIX_
#define _SYS_POSIX_
#define _TKRZW_OSNAME     "AIX"

#elif defined(__APPLE__) && defined(__MACH__)

#define _SYS_MACOSX_
#define _SYS_POSIX_
#define _TKRZW_OSNAME     "Mac OS X"

#elif defined(_MSC_VER)

#define _SYS_MSVC_
#define _SYS_WINDOWS_
#define _TKRZW_OSNAME     "Windows (VC++)"

#elif defined(_WIN32)

#define _SYS_MINGW_
#define _SYS_WINDOWS_
#define _TKRZW_OSNAME     "Windows (MinGW)"

#elif defined(__CYGWIN__)

#define _SYS_CYGWIN_
#define _SYS_WINDOWS_
#define _TKRZW_OSNAME     "Windows (Cygwin)"

#else

#define _SYS_GENERIC_
#define _TKRZW_OSNAME     "Generic"

#endif

#if !defined(_TKRZW_PREFIX)
#define _TKRZW_PREFIX       "/"
#endif
#if !defined(_TKRZW_INCLUDEDIR)
#define _TKRZW_INCLUDEDIR   "/"
#endif
#if !defined(_TKRZW_LIBDIR)
#define _TKRZW_LIBDIR       "/"
#endif
#if !defined(_TKRZW_BINDIR)
#define _TKRZW_BINDIR       "/"
#endif
#if !defined(_TKRZW_LIBEXECDIR)
#define _TKRZW_LIBEXECDIR   "/"
#endif
#if !defined(_TKRZW_APPINC)
#define _TKRZW_APPINC       ""
#endif
#if !defined(_TKRZW_APPLIBS)
#define _TKRZW_APPLIBS      ""
#endif
#if !defined(_TKRZW_PKG_VERSION)
#define _TKRZW_PKG_VERSION  "0.0.0"
#endif
#if !defined(_TKRZW_LIB_VERSION)
#define _TKRZW_LIB_VERSION  "0.0.0"
#endif

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <fstream>
#include <functional>
#include <future>
#include <initializer_list>
#include <limits>
#include <list>
#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <random>
#include <regex>
#include <set>
#include <shared_mutex>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <tuple>
#include <typeinfo>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include <cassert>
#include <cerrno>
#include <cmath>
#include <cstdarg>
#include <cstddef>
#include <cstdio>
#include <cstdlib>
#include <cstdint>
#include <cstring>
#include <ctime>

#if defined(_SYS_POSIX_)

extern "C" {
#include <unistd.h>
#include <sys/ioctl.h>
#include <sys/param.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <sys/time.h>
#include <sys/times.h>
#include <sys/wait.h>
#include <sys/resource.h>
#include <fcntl.h>
#include <dirent.h>
#include <time.h>
}  // extern "C"

#endif

#if defined(_SYS_WINDOWS_)

#define NOMINMAX
#include <windows.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <direct.h>
#include <io.h>
#include <process.h>
#include <processthreadsapi.h>
#include <psapi.h>
#include <sysinfoapi.h>

#endif

namespace tkrzw {

/**
 * Makes a hexadecimal expression of a buffer.
 * @param buf The soruce buffer.
 * @param size The size of the source buffer.
 * @param folding The number of columns in one line.  0 means no folding.
 * @return The hexadecimal string.
 */
std::string HexDump(const void* buf, size_t size, int32_t folding = 0);

/**
 * Makes a hexadecimal expression of a string.
 * @param str The string.
 * @param folding The number of columns in one line.  0 means no folding.
 * @return The hexadecimal string.
 */
std::string HexDumpStr(const std::string_view& str, int32_t folding = 0);

/**
 * Aligns a number to a multiple of another number.
 * @param num The number to align.
 * @param alignment The alignment number.
 * @return The aligned number.
 */
int64_t AlignNumber(int64_t num, int64_t alignment);

/**
 * Aligns a number to a power of two.
 * @param num The number to align.
 * @return The aligned number.
 */
int64_t AlignNumberPowTwo(int64_t num);

/**
 * Normalizes a 16-bit number in the native order into the network byte order.
 * @param num The 16-bit number in the native order.
 * @return The number in the network byte order.
 */
uint16_t HostToNet16(uint16_t num);

/**
 * Normalizes a 32-bit number in the native order into the network byte order.
 * @param num The 32-bit number in the native order.
 * @return The number in the network byte order.
 */
uint32_t HostToNet32(uint32_t num);

/**
 * Normalizes a 64-bit number in the native order into the network byte order.
 * @param num The 64-bit number in the native order.
 * @return The number in the network byte order.
 */
uint64_t HostToNet64(uint64_t num);

/**
 * Denormalizes a 16-bit number in the network byte order into the native order.
 * @param num The 16-bit number in the network byte order.
 * @return The converted number in the native order.
 */
uint16_t NetToHost16(uint16_t num);

/**
 * Denormalizes a 32-bit number in the network byte order into the native order.
 * @param num The 32-bit number in the network byte order.
 * @return The converted number in the native order.
 */
uint32_t NetToHost32(uint32_t num);

/**
 * Denormalizes a 64-bit number in the network byte order into the native order.
 * @param num The 64-bit number in the network byte order.
 * @return The converted number in the native order.
 */
uint64_t NetToHost64(uint64_t num);

/**
 * Writes a number in fixed length format into a buffer.
 * @param buf The desitination buffer.
 * @param num The number.
 * @param width The width of bytes from the LSB.
 */
void WriteFixNum(void* buf, uint64_t num, size_t width);

/**
 * Reads a number in fixed length format from a buffer.
 * @param buf The source buffer.
 * @param width The width of bytes from the LSB.
 * @return The read number.
 */
uint64_t ReadFixNum(const void* buf, size_t width);

/**
 * Writes a number in variable length format into a buffer.
 * @param buf The desitination buffer.
 * @param num The number.
 * @return The length of the written region.
 */
size_t WriteVarNum(void* buf, uint64_t num);

/**
 * Reads a number in variable length format from a buffer.
 * @param buf The source buffer.
 * @param size The size of the source buffer.
 * @param np The pointer to the variable into which the read number is assigned.
 * @return The length of the read region, or 0 on failure.
 */
size_t ReadVarNum(const void* buf, size_t size, uint64_t* np);

/**
 * Reads a number in variable length format from a buffer, without size checking.
 * @param buf The source buffer.
 * @param np The pointer to the variable into which the read number is assigned.
 * @return The length of the read region.
 */
size_t ReadVarNum(const void* buf, uint64_t* np);

/**
 * Checks the size of variable length format of a number.
 * @return The size of variable length format.
 */
size_t SizeVarNum(uint64_t num);

#if defined(_SYS_POSIX_)
constexpr bool _IS_POSIX = true;
#else
constexpr bool _IS_POSIX = false;
#endif

#if defined(_TKRZW_BIGEND)
constexpr bool _IS_BIG_ENDIAN = true;
#else
constexpr bool _IS_BIG_ENDIAN = false;
#endif

constexpr int32_t FILEPERM = 00644;
constexpr int32_t DIRPERM = 00755;

inline std::string HexDump(const void* buf, size_t size, int32_t folding) {
  const unsigned char* rp = (const unsigned char*)buf;
  const unsigned char* ep = rp + size;
  std::string str;
  int32_t nc = 0;
  while (rp < ep) {
    if (rp > buf) {
      if (++nc == folding) {
        str += "\n";
        nc = 0;
      } else {
        str += " ";
      }
    }
    const int32_t upper = (*rp & 0xF0) >> 4;
    if (upper < 10) {
      str.append(1, upper + '0');
    } else {
      str.append(1, upper - 10 + 'A');
    }
    const int32_t lower = *rp & 0x0F;
    if (lower < 10) {
      str.append(1, lower + '0');
    } else {
      str.append(1, lower - 10 + 'A');
    }
    rp++;
  }
  return str;
}

inline std::string HexDumpStr(const std::string_view& str, int32_t folding) {
  return HexDump(str.data(), str.size(), folding);
}

inline int64_t AlignNumber(int64_t num, int64_t alignment) {
  const int64_t diff = num % alignment;
  if (diff > 0) {
    num += alignment - diff;
  }
  return num;
}

inline int64_t AlignNumberPowTwo(int64_t num) {
  int64_t remainder = num - 1;
  num = 1;
  while (remainder != 0) {
    num *= 2;
    remainder /= 2;
  }
  return num;
}

inline uint16_t HostToNet16(uint16_t num) {
  if (_IS_BIG_ENDIAN) return num;
  return ((num & 0x00ffU) << 8) | ((num & 0xff00U) >> 8);
}

inline uint32_t HostToNet32(uint32_t num) {
  if (_IS_BIG_ENDIAN) return num;
  return ((num & 0x000000ffUL) << 24) | ((num & 0x0000ff00UL) << 8) | \
      ((num & 0x00ff0000UL) >> 8) | ((num & 0xff000000UL) >> 24);
}

inline uint64_t HostToNet64(uint64_t num) {
  if (_IS_BIG_ENDIAN) return num;
  return ((num & 0x00000000000000ffULL) << 56) | ((num & 0x000000000000ff00ULL) << 40) |
      ((num & 0x0000000000ff0000ULL) << 24) | ((num & 0x00000000ff000000ULL) << 8) |
      ((num & 0x000000ff00000000ULL) >> 8) | ((num & 0x0000ff0000000000ULL) >> 24) |
      ((num & 0x00ff000000000000ULL) >> 40) | ((num & 0xff00000000000000ULL) >> 56);
}

inline uint16_t NetToHost16(uint16_t num) {
  return HostToNet16(num);
}

inline uint32_t NetToHost32(uint32_t num) {
  return HostToNet32(num);
}

inline uint64_t NetToHost64(uint64_t num) {
  return HostToNet64(num);
}

inline void WriteFixNum(void* buf, uint64_t num, size_t width) {
  assert(buf != nullptr && width <= sizeof(int64_t));
  num = HostToNet64(num);
  std::memcpy(buf, (const char*)&num + sizeof(num) - width, width);
}

inline uint64_t ReadFixNum(const void* buf, size_t width) {
  assert(buf != nullptr && width <= sizeof(int64_t));
  uint64_t num = 0;
  std::memcpy(&num, buf, width);
  return NetToHost64(num) >> ((sizeof(num) - width) * 8);
}

inline size_t WriteVarNum(void* buf, uint64_t num) {
  assert(buf != nullptr);
  unsigned char* wp = (unsigned char*)buf;
  if (num < (1ULL << 7)) {
    *(wp++) = num;
  } else if (num < (1ULL << 14)) {
    *(wp++) = (num >> 7) | 0x80;
    *(wp++) = num & 0x7f;
  } else if (num < (1ULL << 21)) {
    *(wp++) = (num >> 14) | 0x80;
    *(wp++) = ((num >> 7) & 0x7f) | 0x80;
    *(wp++) = num & 0x7f;
  } else if (num < (1ULL << 28)) {
    *(wp++) = (num >> 21) | 0x80;
    *(wp++) = ((num >> 14) & 0x7f) | 0x80;
    *(wp++) = ((num >> 7) & 0x7f) | 0x80;
    *(wp++) = num & 0x7f;
  } else if (num < (1ULL << 35)) {
    *(wp++) = (num >> 28) | 0x80;
    *(wp++) = ((num >> 21) & 0x7f) | 0x80;
    *(wp++) = ((num >> 14) & 0x7f) | 0x80;
    *(wp++) = ((num >> 7) & 0x7f) | 0x80;
    *(wp++) = num & 0x7f;
  } else if (num < (1ULL << 42)) {
    *(wp++) = (num >> 35) | 0x80;
    *(wp++) = ((num >> 28) & 0x7f) | 0x80;
    *(wp++) = ((num >> 21) & 0x7f) | 0x80;
    *(wp++) = ((num >> 14) & 0x7f) | 0x80;
    *(wp++) = ((num >> 7) & 0x7f) | 0x80;
    *(wp++) = num & 0x7f;
  } else if (num < (1ULL << 49)) {
    *(wp++) = (num >> 42) | 0x80;
    *(wp++) = ((num >> 35) & 0x7f) | 0x80;
    *(wp++) = ((num >> 28) & 0x7f) | 0x80;
    *(wp++) = ((num >> 21) & 0x7f) | 0x80;
    *(wp++) = ((num >> 14) & 0x7f) | 0x80;
    *(wp++) = ((num >> 7) & 0x7f) | 0x80;
    *(wp++) = num & 0x7f;
  } else if (num < (1ULL << 56)) {
    *(wp++) = (num >> 49) | 0x80;
    *(wp++) = ((num >> 42) & 0x7f) | 0x80;
    *(wp++) = ((num >> 35) & 0x7f) | 0x80;
    *(wp++) = ((num >> 28) & 0x7f) | 0x80;
    *(wp++) = ((num >> 21) & 0x7f) | 0x80;
    *(wp++) = ((num >> 14) & 0x7f) | 0x80;
    *(wp++) = ((num >> 7) & 0x7f) | 0x80;
    *(wp++) = num & 0x7f;
  } else if (num < (1ULL << 63)) {
    *(wp++) = (num >> 56) | 0x80;
    *(wp++) = ((num >> 49) & 0x7f) | 0x80;
    *(wp++) = ((num >> 42) & 0x7f) | 0x80;
    *(wp++) = ((num >> 35) & 0x7f) | 0x80;
    *(wp++) = ((num >> 28) & 0x7f) | 0x80;
    *(wp++) = ((num >> 21) & 0x7f) | 0x80;
    *(wp++) = ((num >> 14) & 0x7f) | 0x80;
    *(wp++) = ((num >> 7) & 0x7f) | 0x80;
    *(wp++) = num & 0x7f;
  } else {
    *(wp++) = (num >> 63) | 0x80;
    *(wp++) = ((num >> 56) & 0x7f) | 0x80;
    *(wp++) = ((num >> 49) & 0x7f) | 0x80;
    *(wp++) = ((num >> 42) & 0x7f) | 0x80;
    *(wp++) = ((num >> 35) & 0x7f) | 0x80;
    *(wp++) = ((num >> 28) & 0x7f) | 0x80;
    *(wp++) = ((num >> 21) & 0x7f) | 0x80;
    *(wp++) = ((num >> 14) & 0x7f) | 0x80;
    *(wp++) = ((num >> 7) & 0x7f) | 0x80;
    *(wp++) = num & 0x7f;
  }
  return wp - (unsigned char*)buf;
}

inline size_t ReadVarNum(const void* buf, size_t size, uint64_t* np) {
  assert(buf != nullptr && size <= (1 << 30) && np != nullptr);
  const unsigned char* rp = (const unsigned char*)buf;
  const unsigned char* ep = rp + size;
  uint64_t num = 0;
  uint32_t c;
  do {
    if (rp >= ep) {
      *np = 0;
      return 0;
    }
    c = *rp;
    num = (num << 7) + (c & 0x7f);
    rp++;
  } while (c >= 0x80);
  *np = num;
  return rp - (const unsigned char*)buf;
}

inline size_t ReadVarNum(const void* buf, uint64_t* np) {
  assert(buf != nullptr && np != nullptr);
  const unsigned char* rp = (const unsigned char*)buf;
  uint64_t num = 0;
  uint32_t c;
  do {
    c = *rp;
    num = (num << 7) + (c & 0x7f);
    rp++;
  } while (c >= 0x80);
  *np = num;
  return rp - (const unsigned char*)buf;
}

inline size_t SizeVarNum(uint64_t num) {
  if (num < (1ULL << 7)) return 1;
  if (num < (1ULL << 14)) return 2;
  if (num < (1ULL << 21)) return 3;
  if (num < (1ULL << 28)) return 4;
  if (num < (1ULL << 35)) return 5;
  if (num < (1ULL << 42)) return 6;
  if (num < (1ULL << 49)) return 7;
  if (num < (1ULL << 56)) return 8;
  if (num < (1ULL << 63)) return 9;
  return 10;
}

}  // namespace tkrzw

#endif  // _TKRZW_SYS_CONFIG_H

// END OF FILE
