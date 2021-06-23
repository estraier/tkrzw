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

#include "tkrzw_sys_config.h"

#include "tkrzw_hash_util.h"
#include "tkrzw_lib_common.h"

#if _TKRZW_COMP_ZLIB
extern "C" {
#include <zlib.h>
}
#endif

namespace tkrzw {

uint64_t HashMurmur(const void* buf, size_t size, uint64_t seed) {
  assert(buf != nullptr && size <= MAX_MEMORY_SIZE);
  const uint64_t mul = 0xc6a4a7935bd1e995ULL;
  const int32_t rtt = 47;
  uint64_t hash = seed ^ (size * mul);
  const unsigned char* rp = (const unsigned char*)buf;
  while (size >= sizeof(uint64_t)) {
    uint64_t num = ((uint64_t)rp[0] << 0) | ((uint64_t)rp[1] << 8) |
        ((uint64_t)rp[2] << 16) | ((uint64_t)rp[3] << 24) |
        ((uint64_t)rp[4] << 32) | ((uint64_t)rp[5] << 40) |
        ((uint64_t)rp[6] << 48) | ((uint64_t)rp[7] << 56);
    num *= mul;
    num ^= num >> rtt;
    num *= mul;
    hash *= mul;
    hash ^= num;
    rp += sizeof(uint64_t);
    size -= sizeof(uint64_t);
  }
  switch (size) {
    case 7: hash ^= (uint64_t)rp[6] << 48;  // fall through
    case 6: hash ^= (uint64_t)rp[5] << 40;  // fall through
    case 5: hash ^= (uint64_t)rp[4] << 32;  // fall through
    case 4: hash ^= (uint64_t)rp[3] << 24;  // fall through
    case 3: hash ^= (uint64_t)rp[2] << 16;  // fall through
    case 2: hash ^= (uint64_t)rp[1] << 8;   // fall through
    case 1: hash ^= (uint64_t)rp[0]; hash *= mul;  // fall through
  };
  hash ^= hash >> rtt;
  hash *= mul;
  hash ^= hash >> rtt;
  return hash;
}

uint64_t HashFNV(const void* buf, size_t size) {
  assert(buf != nullptr && size <= MAX_MEMORY_SIZE);
  uint64_t hash = 14695981039346656037ULL;
  const unsigned char* rp = (const unsigned char*)buf;
  while (size) {
    hash = (hash ^ *rp++) * 109951162811ULL;
    size--;
  }
  return hash;
}

uint32_t HashChecksum6Continuous(const void* buf, size_t size, bool finish, uint32_t seed) {
  assert(buf != nullptr && size <= MAX_MEMORY_SIZE);
  constexpr uint32_t modulo = 61;
  constexpr uint32_t batch_cap = 1U << 23;
  const unsigned char* rp = (const unsigned char*)buf;
  while (size) {
    size_t batch_size = std::min<size_t>(batch_cap, size);
    size -= batch_size;
    do {
      seed += *rp++;
    } while (--batch_size);
    seed %= modulo;
  }
  return seed;
}

uint32_t HashChecksum8Continuous(const void* buf, size_t size, bool finish, uint32_t seed) {
  assert(buf != nullptr && size <= MAX_MEMORY_SIZE);
  constexpr uint32_t modulo = 251;
  constexpr uint32_t batch_cap = 1U << 23;
  const unsigned char* rp = (const unsigned char*)buf;
  while (size) {
    size_t batch_size = std::min<size_t>(batch_cap, size);
    size -= batch_size;
    do {
      seed += *rp++;
    } while (--batch_size);
    seed %= modulo;
  }
  return seed;
}

uint32_t HashAdler6Continuous(const void* buf, size_t size, bool finish, uint32_t seed) {
  assert(buf != nullptr && size <= MAX_MEMORY_SIZE);
  constexpr uint32_t modulo = 7;
  constexpr uint32_t batch_cap = 4096;
  const unsigned char* rp = (const unsigned char*)buf;
  uint32_t sum = seed >> 3;
  seed &= 0x7;
  while (size) {
    size_t batch_size = std::min<size_t>(batch_cap, size);
    size -= batch_size;
    do {
      seed += *rp++;
      sum += seed;
    } while (--batch_size);
    seed %= modulo;
    sum %= modulo;
  }
  return (sum << 3) | seed;
}

uint32_t HashAdler8Continuous(const void* buf, size_t size, bool finish, uint32_t seed) {
  assert(buf != nullptr && size <= MAX_MEMORY_SIZE);
  constexpr uint32_t modulo = 13;
  constexpr uint32_t batch_cap = 4096;
  const unsigned char* rp = (const unsigned char*)buf;
  uint32_t sum = seed >> 4;
  seed &= 0xF;
  while (size) {
    size_t batch_size = std::min<size_t>(batch_cap, size);
    size -= batch_size;
    do {
      seed += *rp++;
      sum += seed;
    } while (--batch_size);
    seed %= modulo;
    sum %= modulo;
  }
  return (sum << 4) | seed;
}

uint32_t HashAdler16Continuous(const void* buf, size_t size, bool finish, uint32_t seed) {
  assert(buf != nullptr && size <= MAX_MEMORY_SIZE);
  constexpr uint32_t modulo = 251;
  constexpr uint32_t batch_cap = 4096;
  const unsigned char* rp = (const unsigned char*)buf;
  uint32_t sum = seed >> 8;
  seed &= 0xFF;
  while (size) {
    size_t batch_size = std::min<size_t>(batch_cap, size);
    size -= batch_size;
    do {
      seed += *rp++;
      sum += seed;
    } while (--batch_size);
    seed %= modulo;
    sum %= modulo;
  }
  return (sum << 8) | seed;
}

uint32_t HashAdler32Continuous(const void* buf, size_t size, bool finish, uint32_t seed) {
#if _TKRZW_COMP_ZLIB && !defined(_TKRZW_STDONLY)
  assert(buf != nullptr && size <= MAX_MEMORY_SIZE);
  return adler32(seed, (Bytef*)buf, size);
#else
  assert(buf != nullptr && size <= MAX_MEMORY_SIZE);
  constexpr uint32_t modulo = 65521;
  constexpr uint32_t batch_cap = 4096;
  const unsigned char* rp = (const unsigned char*)buf;
  uint32_t sum = seed >> 16;
  seed &= 0xFFFF;
  while (size) {
    size_t batch_size = std::min<size_t>(batch_cap, size);
    size -= batch_size;
    do {
      seed += *rp++;
      sum += seed;
    } while (--batch_size);
    seed %= modulo;
    sum %= modulo;
  }
  return (sum << 16) | seed;
#endif
}

uint32_t HashCRC4Continuous(const void* buf, size_t size, bool finish, uint32_t seed) {
#if !defined(_TKRZW_BIGEND) && !defined(_TKRZW_STDONLY)
  assert(buf != nullptr && size <= MAX_MEMORY_SIZE);
  static std::unique_ptr<uint32_t[]> uniq_table0, uniq_table1, uniq_table2, uniq_table3;
  static uint32_t* table0 = nullptr;
  static uint32_t* table1 = nullptr;
  static uint32_t* table2 = nullptr;
  static uint32_t* table3 = nullptr;
  static std::once_flag table_once_flag;
  std::call_once(table_once_flag, [&]() {
    uniq_table0.reset(new uint32_t[256]);
    table0 = uniq_table0.get();
    uniq_table1.reset(new uint32_t[256]);
    table1 = uniq_table1.get();
    uniq_table2.reset(new uint32_t[256]);
    table2 = uniq_table2.get();
    uniq_table3.reset(new uint32_t[256]);
    table3 = uniq_table3.get();
    for (uint32_t i = 0; i < 256; i++) {
      uint32_t c = i;
      for (int32_t j = 0; j < 8; j++) {
        c = c & 1 ? (c >> 1) ^ 0x0C : c >> 1;
      }
      table0[i] = c;
    }
    for (uint32_t i = 0; i < 256; i++) {
      table1[i] = (table0[i] >> 8) ^ table0[table0[i] & 0xFF];
      table2[i] = (table1[i] >> 8) ^ table0[table1[i] & 0xFF];
      table3[i] = (table2[i] >> 8) ^ table0[table2[i] & 0xFF];
    }
  });
  uint32_t crc = seed;
  const uint8_t* rp = (uint8_t*)buf;
  while (size && (intptr_t)rp & 3) {
    crc = (crc >> 8) ^ table0[(crc & 0xFF) ^ *rp++];
    size--;
  }
  const uint32_t* irp = (uint32_t*)rp;
  while (size >= 4) {
    crc ^= *(irp++);
    crc = table3[crc & 0xFF] ^ table2[(crc >> 8) & 0xFF] ^
        table1[(crc >> 16) & 0xFF] ^ table0[crc >> 24];
    size -= 4;
  }
  rp = (uint8_t*)irp;
  while (size--) {
    crc = (crc >> 8) ^ table0[(crc & 0xFF) ^ *rp++];
  }
  return crc;
#else
  assert(buf != nullptr && size <= MAX_MEMORY_SIZE);
  static std::unique_ptr<uint8_t[]> uniq_table;
  static uint8_t* table = nullptr;
  static std::once_flag table_once_flag;
  std::call_once(table_once_flag, [&]() {
    uniq_table.reset(new uint8_t[256]);
    table = uniq_table.get();
    for (int32_t i = 0; i < 256; i++) {
      uint32_t c = i;
      for(int32_t j = 0; j < 8; j++) {
        c = c & 1 ? (c >> 1) ^ 0x0C : c >> 1;
      }
      table[i] = c;
    }
  });
  uint32_t crc = seed;
  const uint8_t* rp = (uint8_t*)buf;
  while (size--) {
    crc = table[(crc ^ *rp++)];
  }
  return crc;
#endif
}

uint32_t HashCRC8Continuous(const void* buf, size_t size, bool finish, uint32_t seed) {
#if !defined(_TKRZW_BIGEND) && !defined(_TKRZW_STDONLY)
  assert(buf != nullptr && size <= MAX_MEMORY_SIZE);
  static std::unique_ptr<uint32_t[]> uniq_table0, uniq_table1, uniq_table2, uniq_table3;
  static uint32_t* table0 = nullptr;
  static uint32_t* table1 = nullptr;
  static uint32_t* table2 = nullptr;
  static uint32_t* table3 = nullptr;
  static std::once_flag table_once_flag;
  std::call_once(table_once_flag, [&]() {
    uniq_table0.reset(new uint32_t[256]);
    table0 = uniq_table0.get();
    uniq_table1.reset(new uint32_t[256]);
    table1 = uniq_table1.get();
    uniq_table2.reset(new uint32_t[256]);
    table2 = uniq_table2.get();
    uniq_table3.reset(new uint32_t[256]);
    table3 = uniq_table3.get();
    for (uint32_t i = 0; i < 256; i++) {
      uint8_t c = i;
      for (uint32_t j = 0; j < 8; j++) {
        c = (c << 1) ^ ((c & 0x80) ? 0x07 : 0);
      }
      table0[i] = c;
    }
    for (uint32_t i = 0; i < 256; i++) {
      table1[i] = (table0[i] >> 8) ^ table0[table0[i] & 0xFF];
      table2[i] = (table1[i] >> 8) ^ table0[table1[i] & 0xFF];
      table3[i] = (table2[i] >> 8) ^ table0[table2[i] & 0xFF];
    }
  });
  uint32_t crc = seed;
  const uint8_t* rp = (uint8_t*)buf;
  while (size && (intptr_t)rp & 3) {
    crc = (crc >> 8) ^ table0[(crc & 0xFF) ^ *rp++];
    size--;
  }
  const uint32_t* irp = (uint32_t*)rp;
  while (size >= 4) {
    crc ^= *(irp++);
    crc = table3[crc & 0xFF] ^ table2[(crc >> 8) & 0xFF] ^
        table1[(crc >> 16) & 0xFF] ^ table0[crc >> 24];
    size -= 4;
  }
  rp = (uint8_t*)irp;
  while (size--) {
    crc = (crc >> 8) ^ table0[(crc & 0xFF) ^ *rp++];
  }
  return crc;
#else
  assert(buf != nullptr && size <= MAX_MEMORY_SIZE);
  static std::unique_ptr<uint8_t[]> uniq_table;
  static uint8_t* table = nullptr;
  static std::once_flag table_once_flag;
  std::call_once(table_once_flag, [&]() {
    uniq_table.reset(new uint8_t[256]);
    table = uniq_table.get();
    for (uint32_t i = 0; i < 256; i++) {
      uint8_t c = i;
      for (uint32_t j = 0; j < 8; j++) {
        c = (c << 1) ^ ((c & 0x80) ? 0x07 : 0);
      }
      table[i] = c;
    }
  });
  uint32_t crc = seed;
  const uint8_t* rp = (uint8_t*)buf;
  while (size--) {
    crc = table[(crc ^ *rp++)];
  }
  return crc;
#endif
}

uint32_t HashCRC16Continuous(const void* buf, size_t size, bool finish, uint32_t seed) {
  assert(buf != nullptr && size <= MAX_MEMORY_SIZE);
  static std::unique_ptr<uint16_t[]> uniq_table;
  static uint16_t* table = nullptr;
  static std::once_flag table_once_flag;
  std::call_once(table_once_flag, [&]() {
    uniq_table.reset(new uint16_t[256]);
    table = uniq_table.get();
    for (uint32_t i = 0; i < 256; i++) {
      uint16_t c = i << 8;
      for (uint32_t j = 0; j < 8; j++) {
        c = (c & 0x8000) ? (0x1021 ^ (c << 1)) : (c << 1);
      }
      table[i] = c;
    }
  });
  uint32_t crc = seed;
  const uint8_t* rp = (uint8_t*)buf;
  while (size--) {
    crc = table[((crc >> 8) ^ *rp++) & 0xFF] ^ (crc << 8);
  }
  return crc & 0xFFFF;
}

uint32_t HashCRC32Continuous(const void* buf, size_t size, bool finish, uint32_t seed) {
#if _TKRZW_COMP_ZLIB && !defined(_TKRZW_STDONLY)
  assert(buf != nullptr && size <= MAX_MEMORY_SIZE);
  uint32_t crc = crc32(seed ^ 0xFFFFFFFF, (Bytef*)buf, size);
  if (!finish) {
    crc ^= 0xFFFFFFFF;
  }
  return crc;
#elif !defined(_TKRZW_BIGEND) && !defined(_TKRZW_STDONLY)
  assert(buf != nullptr && size <= MAX_MEMORY_SIZE);
  static std::unique_ptr<uint32_t[]> uniq_table0, uniq_table1, uniq_table2, uniq_table3;
  static uint32_t* table0 = nullptr;
  static uint32_t* table1 = nullptr;
  static uint32_t* table2 = nullptr;
  static uint32_t* table3 = nullptr;
  static std::once_flag table_once_flag;
  std::call_once(table_once_flag, [&]() {
    uniq_table0.reset(new uint32_t[256]);
    table0 = uniq_table0.get();
    uniq_table1.reset(new uint32_t[256]);
    table1 = uniq_table1.get();
    uniq_table2.reset(new uint32_t[256]);
    table2 = uniq_table2.get();
    uniq_table3.reset(new uint32_t[256]);
    table3 = uniq_table3.get();
    for (uint32_t i = 0; i < 256; i++) {
      uint32_t c = i;
      for (int32_t j = 0; j < 8; j++) {
        c = (c & 1) ? (0xEDB88320 ^ (c >> 1)) : (c >> 1);
      }
      table0[i] = c;
    }
    for (uint32_t i = 0; i < 256; i++) {
      table1[i] = (table0[i] >> 8) ^ table0[table0[i] & 0xFF];
      table2[i] = (table1[i] >> 8) ^ table0[table1[i] & 0xFF];
      table3[i] = (table2[i] >> 8) ^ table0[table2[i] & 0xFF];
    }
  });
  uint32_t crc = seed;
  const uint8_t* rp = (uint8_t*)buf;
  while (size && (intptr_t)rp & 3) {
    crc = (crc >> 8) ^ table0[(crc & 0xFF) ^ *rp++];
    size--;
  }
  const uint32_t* irp = (uint32_t*)rp;
  while (size >= 4) {
    crc ^= *(irp++);
    crc = table3[crc & 0xFF] ^ table2[(crc >> 8) & 0xFF] ^
        table1[(crc >> 16) & 0xFF] ^ table0[crc >> 24];
    size -= 4;
  }
  rp = (uint8_t*)irp;
  while (size--) {
    crc = (crc >> 8) ^ table0[(crc & 0xFF) ^ *rp++];
  }
  if (finish) {
    crc ^= 0xFFFFFFFF;
  }
  return crc;
#else
  assert(buf != nullptr && size <= MAX_MEMORY_SIZE);
  static std::unique_ptr<uint32_t[]> uniq_table;
  static uint32_t* table = nullptr;
  static std::once_flag table_once_flag;
  std::call_once(table_once_flag, [&]() {
    uniq_table.reset(new uint32_t[256]);
    table = uniq_table.get();
    for (uint32_t i = 0; i < 256; i++) {
      uint32_t c = i;
      for (int32_t j = 0; j < 8; j++) {
        c = (c & 1) ? (0xEDB88320 ^ (c >> 1)) : (c >> 1);
      }
      table[i] = c;
    }
  });
  uint32_t crc = seed;
  const uint8_t* rp = (uint8_t*)buf;
  while (size--) {
    crc = (crc >> 8) ^ table[(crc & 0xFF) ^ *rp++];
  }
  if (finish) {
    crc ^= 0xFFFFFFFF;
  }
  return crc;
#endif
}

}  // namespace tkrzw

// END OF FILE
