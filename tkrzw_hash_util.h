/*************************************************************************************************
 * Hash utilities
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

#ifndef _TKRZW_HASH_UTIL_H
#define _TKRZW_HASH_UTIL_H

#include <map>
#include <string>
#include <vector>

#include <cinttypes>

#include "tkrzw_lib_common.h"

namespace tkrzw {

/**
 * Gets the hash value by Murmur hashing.
 * @param buf The source buffer.
 * @param size The size of the source buffer.
 * @param seed The seed value.
 * @return The hash value.
 */
uint64_t HashMurmur(const void* buf, size_t size, uint64_t seed);

/**
 * Gets the hash value by Murmur hashing.
 * @see HashMurmur
 */
inline uint64_t HashMurmur(std::string_view str, uint64_t seed) {
  return HashMurmur(str.data(), str.size(), seed);
}

/**
 * Gets the hash value by FNV hashing.
 * @param buf The source buffer.
 * @param size The size of the source buffer.
 * @return The hash value.
 */
uint64_t HashFNV(const void* buf, size_t size);

/**
 * Gets the hash value by FNV hashing.
 * @see HashFNV
 */
inline uint64_t HashFNV(std::string_view str) {
  return HashFNV(str.data(), str.size());
}

/**
 * Gets the hash value by Checksum-6, in a continuous way.
 * @param buf The source buffer.
 * @param size The size of the source buffer.
 * @param finish True if the cycle is to be finished.
 * @param seed A seed value.  This should be 0 for the frist call of the cycle.
 * @return The hash value.
 */
uint32_t HashChecksum6Continuous(
    const void* buf, size_t size, bool finish, uint32_t seed = 0);

/**
 * Gets the hash value by Checksum-6.
 * @param buf The source buffer.
 * @param size The size of the source buffer.
 * @return The hash value.
 */
inline uint32_t HashChecksum6(const void* buf, size_t size) {
  return HashChecksum6Continuous(buf, size, true);
}

/**
 * Gets the hash value by Checksum-6.
 * @see HashChecksum6
 */
inline uint32_t HashChecksum6(std::string_view str) {
  return HashChecksum6Continuous(str.data(), str.size(), true);
}

/**
 * Gets the hash value by Checksum-6.
 * @see HashChecksum6
 */
inline uint32_t HashChecksum6Pair(
    const void* first_buf, size_t first_size,
    const void* second_buf, size_t second_size, uint32_t seed = 0) {
  constexpr uint32_t modulo = 61;
  constexpr uint32_t batch_cap = 1U << 23;
  if (first_size + second_size < batch_cap) {
    const unsigned char* rp = (const unsigned char*)first_buf;
    while (first_size) {
      seed += *rp++;
      first_size--;
    }
    rp = (const unsigned char*)second_buf;
    while (second_size) {
      seed += *rp++;
      second_size--;
    }
    return seed % modulo;
  }
  return HashChecksum6Continuous(
      second_buf, second_size, true, HashChecksum6Continuous(first_buf, first_size, false, seed));
}

/**
 * Gets the hash value by checksum-8, in a continuous way.
 * @param buf The source buffer.
 * @param size The size of the source buffer.
 * @param finish True if the cycle is to be finished.
 * @param seed A seed value.  This should be 0 for the frist call of the cycle.
 * @return The hash value.
 */
uint32_t HashChecksum8Continuous(
    const void* buf, size_t size, bool finish, uint32_t seed = 0);

/**
 * Gets the hash value by checksum-8.
 * @param buf The source buffer.
 * @param size The size of the source buffer.
 * @return The hash value.
 */
inline uint32_t HashChecksum8(const void* buf, size_t size) {
  return HashChecksum8Continuous(buf, size, true);
}

/**
 * Gets the hash value by checksum-8.
 * @see HashChecksum8
 */
inline uint32_t HashChecksum8(std::string_view str) {
  return HashChecksum8Continuous(str.data(), str.size(), true);
}

/**
 * Gets the hash value by Checksum-8.
 * @see HashChecksum8
 */
inline uint32_t HashChecksum8Pair(
    const void* first_buf, size_t first_size,
    const void* second_buf, size_t second_size, uint32_t seed = 0) {
  constexpr uint32_t modulo = 251;
  constexpr uint32_t batch_cap = 1U << 23;
  if (first_size + second_size < batch_cap) {
    const unsigned char* rp = (const unsigned char*)first_buf;
    while (first_size) {
      seed += *rp++;
      first_size--;
    }
    rp = (const unsigned char*)second_buf;
    while (second_size) {
      seed += *rp++;
      second_size--;
    }
    return seed % modulo;
  }
  return HashChecksum8Continuous(
      second_buf, second_size, true, HashChecksum8Continuous(first_buf, first_size, false, seed));
}

/**
 * Gets the hash value by Adler-6, in a continuous way.
 * @param buf The source buffer.
 * @param size The size of the source buffer.
 * @param finish True if the cycle is to be finished.
 * @param seed A seed value.  This should be 1 for the frist call of the cycle.
 * @return The hash value.
 */
uint32_t HashAdler6Continuous(
    const void* buf, size_t size, bool finish, uint32_t seed = 1);

/**
 * Gets the hash value by Adler-6.
 * @param buf The source buffer.
 * @param size The size of the source buffer.
 * @return The hash value.
 */
inline uint32_t HashAdler6(const void* buf, size_t size) {
  return HashAdler6Continuous(buf, size, true);
}

/**
 * Gets the hash value by Adler-6.
 * @see HashAdler6
 */
inline uint32_t HashAdler6(std::string_view str) {
  return HashAdler6Continuous(str.data(), str.size(), true);
}

/**
 * Gets the hash value by adler-8, in a continuous way.
 * @param buf The source buffer.
 * @param size The size of the source buffer.
 * @param finish True if the cycle is to be finished.
 * @param seed A seed value.  This should be 1 for the frist call of the cycle.
 * @return The hash value.
 */
uint32_t HashAdler8Continuous(
    const void* buf, size_t size, bool finish, uint32_t seed = 1);

/**
 * Gets the hash value by adler-8.
 * @param buf The source buffer.
 * @param size The size of the source buffer.
 * @return The hash value.
 */
inline uint32_t HashAdler8(const void* buf, size_t size) {
  return HashAdler8Continuous(buf, size, true);
}

/**
 * Gets the hash value by adler-8.
 * @see HashAdler8
 */
inline uint32_t HashAdler8(std::string_view str) {
  return HashAdler8Continuous(str.data(), str.size(), true);
}

/**
 * Gets the hash value by Adler-16, in a continuous way.
 * @param buf The source buffer.
 * @param size The size of the source buffer.
 * @param finish True if the cycle is to be finished.
 * @param seed A seed value.  This should be 1 for the frist call of the cycle.
 * @return The hash value.
 */
uint32_t HashAdler16Continuous(
    const void* buf, size_t size, bool finish, uint32_t seed = 1);

/**
 * Gets the hash value by Adler-16.
 * @param buf The source buffer.
 * @param size The size of the source buffer.
 * @return The hash value.
 */
inline uint32_t HashAdler16(const void* buf, size_t size) {
  return HashAdler16Continuous(buf, size, true);
}

/**
 * Gets the hash value by Adler-16.
 * @see HashAdler16
 */
inline uint32_t HashAdler16(std::string_view str) {
  return HashAdler16Continuous(str.data(), str.size(), true);
}

/**
 * Gets the hash value by Adler-32, in a continuous way.
 * @param buf The source buffer.
 * @param size The size of the source buffer.
 * @param finish True if the cycle is to be finished.
 * @param seed A seed value.  This should be 1 for the frist call of the cycle.
 * @return The hash value.
 */
uint32_t HashAdler32Continuous(
    const void* buf, size_t size, bool finish, uint32_t seed = 1);

/**
 * Gets the hash value by Adler-32.
 * @param buf The source buffer.
 * @param size The size of the source buffer.
 * @return The hash value.
 */
inline uint32_t HashAdler32(const void* buf, size_t size) {
  return HashAdler32Continuous(buf, size, true);
}

/**
 * Gets the hash value by Adler-32.
 * @see HashAdler32
 */
inline uint32_t HashAdler32(std::string_view str) {
  return HashAdler32Continuous(str.data(), str.size(), true);
}

/**
 * Gets the hash value by CRC-4, in a continuous way.
 * @param buf The source buffer.
 * @param size The size of the source buffer.
 * @param finish True if the cycle is to be finished.
 * @param seed A seed value.  This should be 0 for the frist call of the cycle.
 * @return The hash value.
 */
uint32_t HashCRC4Continuous(
    const void* buf, size_t size, bool finish, uint32_t seed = 0);

/**
 * Gets the hash value by CRC-4.
 * @param buf The source buffer.
 * @param size The size of the source buffer.
 * @return The hash value.
 */
inline uint32_t HashCRC4(const void* buf, size_t size) {
  return HashCRC4Continuous(buf, size, true);
}

/**
 * Gets the hash value by CRC-4.
 * @see HashCRC4
 */
inline uint32_t HashCRC4(std::string_view str) {
  return HashCRC4Continuous(str.data(), str.size(), true);
}

/**
 * Gets the hash value by CRC-8, in a continuous way.
 * @param buf The source buffer.
 * @param size The size of the source buffer.
 * @param finish True if the cycle is to be finished.
 * @param seed A seed value.  This should be 0 for the frist call of the cycle.
 * @return The hash value.
 */
uint32_t HashCRC8Continuous(
    const void* buf, size_t size, bool finish, uint32_t seed = 0);

/**
 * Gets the hash value by CRC-8.
 * @param buf The source buffer.
 * @param size The size of the source buffer.
 * @return The hash value.
 */
inline uint32_t HashCRC8(const void* buf, size_t size) {
  return HashCRC8Continuous(buf, size, true);
}

/**
 * Gets the hash value by CRC-8.
 * @see HashCRC8
 */
inline uint32_t HashCRC8(std::string_view str) {
  return HashCRC8Continuous(str.data(), str.size(), true);
}

/**
 * Gets the hash value by CRC-16, in a continuous way.
 * @param buf The source buffer.
 * @param size The size of the source buffer.
 * @param finish True if the cycle is to be finished.
 * @param seed A seed value.  This should be 0 for the frist call of the cycle.
 * @return The hash value.
 */
uint32_t HashCRC16Continuous(
    const void* buf, size_t size, bool finish, uint32_t seed = 0);

/**
 * Gets the hash value by CRC-16.
 * @param buf The source buffer.
 * @param size The size of the source buffer.
 * @return The hash value.
 */
inline uint32_t HashCRC16(const void* buf, size_t size) {
  return HashCRC16Continuous(buf, size, true);
}

/**
 * Gets the hash value by CRC-16.
 * @see HashCRC8
 */
inline uint32_t HashCRC16(std::string_view str) {
  return HashCRC16Continuous(str.data(), str.size(), true);
}

/**
 * Gets the hash value by CRC-32, in a continuous way.
 * @param buf The source buffer.
 * @param size The size of the source buffer.
 * @param finish True if the cycle is to be finished.
 * @param seed A seed value.  This should be 0xFFFFFFFF for the frist call of the cycle.
 * @return The hash value.
 */
uint32_t HashCRC32Continuous(
    const void* buf, size_t size, bool finish, uint32_t seed = 0xFFFFFFFF);

/**
 * Gets the hash value by CRC-32.
 * @param buf The source buffer.
 * @param size The size of the source buffer.
 * @return The hash value.
 */
inline uint32_t HashCRC32(const void* buf, size_t size) {
  return HashCRC32Continuous(buf, size, true);
}

/**
 * Gets the hash value by CRC-32.
 * @see HashCRC32
 */
inline uint32_t HashCRC32(std::string_view str) {
  return HashCRC32Continuous(str.data(), str.size(), true);
}

}  // namespace tkrzw

#endif  // _TKRZW_HASH_UTIL_H

// END OF FILE
