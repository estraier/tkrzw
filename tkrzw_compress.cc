/*************************************************************************************************
 * Data compression functions
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

#include "tkrzw_compress.h"
#include "tkrzw_hash_util.h"
#include "tkrzw_lib_common.h"
#include "tkrzw_sys_compress_aes.h"
#include "tkrzw_thread_util.h"

#if _TKRZW_COMP_ZLIB
extern "C" {
#include <zlib.h>
}
#endif

#if _TKRZW_COMP_ZSTD
extern "C" {
#include <zstd.h>
}
#endif

#if _TKRZW_COMP_LZ4
extern "C" {
#include <lz4.h>
}
#endif

#if _TKRZW_COMP_LZMA
extern "C" {
#include <lzma.h>
}
#endif

namespace tkrzw {

DummyCompressor::DummyCompressor(bool checksum) : checksum_(checksum) {}

DummyCompressor::~DummyCompressor() {}

bool DummyCompressor::IsSupported() const {
  return true;
}

char* DummyCompressor::Compress(const void* buf, size_t size, size_t* sp) const {
  if (checksum_) {
    const uint32_t crc = HashCRC32(buf, size);
    char* zbuf = static_cast<char*>(xmalloc(sizeof(uint32_t) + size));
    WriteFixNum(zbuf, crc, sizeof(crc));
    std::memcpy(zbuf + sizeof(crc), buf, size);
    *sp = sizeof(crc) + size;
    return zbuf;
  }
  char* zbuf = static_cast<char*>(xmalloc(size + 1));
  std::memcpy(zbuf, buf, size);
  *sp = size;
  return zbuf;
}

char* DummyCompressor::Decompress(const void* buf, size_t size, size_t* sp) const {
  if (checksum_) {
    if (size < sizeof(uint32_t)) {
      return nullptr;
    }
    const char* rp = static_cast<const char*>(buf);
    const uint32_t crc = ReadFixNum(rp, sizeof(uint32_t));
    rp += sizeof(crc);
    size -= sizeof(crc);
    if (HashCRC32(rp, size) != crc) {
      return nullptr;
    }
    char* zbuf = static_cast<char*>(xmalloc(size + 1));
    std::memcpy(zbuf, rp, size);
    *sp = size;
    return zbuf;
  }
  char* zbuf = static_cast<char*>(xmalloc(size + 1));
  std::memcpy(zbuf, buf, size);
  *sp = size;
  return zbuf;
}

std::unique_ptr<Compressor> DummyCompressor::MakeCompressor() const {
  return std::make_unique<DummyCompressor>(checksum_);
}

ZLibCompressor::ZLibCompressor(int32_t level, MetadataMode metadata_mode)
    : level_(level), metadata_mode_(metadata_mode) {
  assert(level >= 0 && level <= 9);
}

ZLibCompressor::~ZLibCompressor() {}

bool ZLibCompressor::IsSupported() const {
#if _TKRZW_COMP_ZLIB
  return true;
#else
  return false;
#endif
}

char* ZLibCompressor::Compress(const void* buf, size_t size, size_t* sp) const {
#if _TKRZW_COMP_ZLIB
  assert(buf != nullptr && size <= MAX_MEMORY_SIZE && sp != nullptr);
  z_stream zs;
  zs.zalloc = Z_NULL;
  zs.zfree = Z_NULL;
  zs.opaque = Z_NULL;
  switch (metadata_mode_) {
    default:
      if (deflateInit2(&zs, level_, Z_DEFLATED, -15, 9, Z_DEFAULT_STRATEGY) != Z_OK) {
        return nullptr;
      }
      break;
    case METADATA_ADLER32:
      if (deflateInit2(&zs, level_, Z_DEFLATED, 15, 9, Z_DEFAULT_STRATEGY) != Z_OK) {
        return nullptr;
      }
      break;
    case METADATA_CRC32:
      if (deflateInit2(&zs, level_, Z_DEFLATED, 15 + 16, 9, Z_DEFAULT_STRATEGY) != Z_OK) {
        return nullptr;
      }
      break;
  }
  const char* rp = (const char*)buf;
  size_t zsiz = size + size / 8 + 32;
  char* zbuf = static_cast<char*>(xmalloc(zsiz));
  char* wp = zbuf;
  zs.next_in = (Bytef*)rp;
  zs.avail_in = size;
  zs.next_out = (Bytef*)wp;
  zs.avail_out = zsiz;
  if (deflate(&zs, Z_FINISH) != Z_STREAM_END) {
    xfree(zbuf);
    deflateEnd(&zs);
    return nullptr;
  }
  deflateEnd(&zs);
  zsiz -= zs.avail_out;
  *sp = zsiz;
  return zbuf;
#else
  return nullptr;
#endif
}

char* ZLibCompressor::Decompress(const void* buf, size_t size, size_t* sp) const {
#if _TKRZW_COMP_ZLIB
  assert(buf != nullptr && size <= MAX_MEMORY_SIZE && sp != nullptr);
  size_t zsiz = size * 8 + 32;
  char* zbuf = static_cast<char*>(xmalloc(zsiz));
  while (true) {
    z_stream zs;
    zs.zalloc = Z_NULL;
    zs.zfree = Z_NULL;
    zs.opaque = Z_NULL;
    switch (metadata_mode_) {
      default:
        if (inflateInit2(&zs, -15) != Z_OK) {
          xfree(zbuf);
          return nullptr;
        }
        break;
      case METADATA_ADLER32:
        if (inflateInit2(&zs, 15) != Z_OK) {
          xfree(zbuf);
          return nullptr;
        }
        break;
      case METADATA_CRC32:
        if (inflateInit2(&zs, 15 + 16) != Z_OK) {
          xfree(zbuf);
          return nullptr;
        }
        break;
    }
    zs.next_in = (Bytef*)buf;
    zs.avail_in = size;
    zs.next_out = (Bytef*)zbuf;
    zs.avail_out = zsiz;
    int32_t rv = inflate(&zs, Z_FINISH);
    inflateEnd(&zs);
    if (rv == Z_STREAM_END) {
      zsiz -= zs.avail_out;
      *sp = zsiz;
      return zbuf;
    } else if (rv == Z_BUF_ERROR) {
      if (zsiz >= INT32MAX / 2) {
        break;
      }
      zsiz *= 2;
      zbuf = static_cast<char*>(xrealloc(zbuf, zsiz));
    } else {
      break;
    }
  }
  xfree(zbuf);
  return nullptr;
#else
  return nullptr;
#endif
}

std::unique_ptr<Compressor> ZLibCompressor::MakeCompressor() const {
  return std::make_unique<ZLibCompressor>(level_, metadata_mode_);
}

ZStdCompressor::ZStdCompressor(int32_t level) : level_(level) {
  assert(level >= -1 && level <= 19);
}

ZStdCompressor::~ZStdCompressor() {
}

bool ZStdCompressor::IsSupported() const {
#if _TKRZW_COMP_ZSTD
  return true;
#else
  return false;
#endif
}

char* ZStdCompressor::Compress(const void* buf, size_t size, size_t* sp) const {
#if _TKRZW_COMP_ZSTD
  int32_t zsiz = ZSTD_compressBound(size);
  char* zbuf = static_cast<char*>(xmalloc(zsiz));
  const size_t rsiz = ZSTD_compress(zbuf, zsiz, static_cast<const char*>(buf), size, level_);
  if (ZSTD_isError(rsiz)) {
    xfree(zbuf);
    return zbuf;
  }
  *sp = rsiz;
  return zbuf;
#else
  return nullptr;
#endif
}

char* ZStdCompressor::Decompress(const void* buf, size_t size, size_t* sp) const {
#if _TKRZW_COMP_ZSTD
  int32_t zsiz = size * 8 + 32;
  char* zbuf = static_cast<char*>(xmalloc(zsiz));
  while (true) {
    const size_t rsiz = ZSTD_decompress(zbuf, zsiz, static_cast<const char*>(buf), size);
    if (!ZSTD_isError(rsiz)) {
      *sp = rsiz;
      return zbuf;
    }
    if (zsiz >= INT32MAX / 2) {
      break;
    }
    zsiz *= 2;
    zbuf = static_cast<char*>(xrealloc(zbuf, zsiz));
  }
  xfree(zbuf);
  return nullptr;
#else
  return nullptr;
#endif
}

std::unique_ptr<Compressor> ZStdCompressor::MakeCompressor() const {
  return std::make_unique<ZStdCompressor>(level_);
}

LZ4Compressor::LZ4Compressor(int32_t accelaration) : acceleration_(accelaration) {
  assert(accelaration > 0);
}

LZ4Compressor::~LZ4Compressor() {
}

bool LZ4Compressor::IsSupported() const {
#if _TKRZW_COMP_LZ4
  return true;
#else
  return false;
#endif
}

char* LZ4Compressor::Compress(const void* buf, size_t size, size_t* sp) const {
#if _TKRZW_COMP_LZ4
  int32_t zsiz = LZ4_compressBound(size);
  char* zbuf = static_cast<char*>(xmalloc(zsiz));
  const int32_t rsiz =
      LZ4_compress_fast(static_cast<const char*>(buf), zbuf, size, zsiz, acceleration_);
  if (rsiz < 1) {
    xfree(zbuf);
    return zbuf;
  }
  *sp = rsiz;
  return zbuf;
#else
  return nullptr;
#endif
}

char* LZ4Compressor::Decompress(const void* buf, size_t size, size_t* sp) const {
#if _TKRZW_COMP_LZ4
  int32_t zsiz = size * 4 + 32;
  char* zbuf = static_cast<char*>(xmalloc(zsiz));
  while (true) {
    const int32_t rsiz = LZ4_decompress_safe(static_cast<const char*>(buf), zbuf, size, zsiz);
    if (rsiz >= 0) {
      *sp = rsiz;
      return zbuf;
    }
    if (zsiz >= INT32MAX / 2) {
      break;
    }
    zsiz *= 2;
    zbuf = static_cast<char*>(xrealloc(zbuf, zsiz));
  }
  xfree(zbuf);
  return nullptr;
#else
  return nullptr;
#endif
}

std::unique_ptr<Compressor> LZ4Compressor::MakeCompressor() const {
  return std::make_unique<LZ4Compressor>(acceleration_);
}

LZMACompressor::LZMACompressor(int32_t level, MetadataMode metadata_mode)
    : level_(level), metadata_mode_(metadata_mode) {
  assert(level >= 0 && level <= 9);
}

LZMACompressor::~LZMACompressor() {}

bool LZMACompressor::IsSupported() const {
#if _TKRZW_COMP_LZMA
  return true;
#else
  return false;
#endif
}

char* LZMACompressor::Compress(const void* buf, size_t size, size_t* sp) const {
#if _TKRZW_COMP_LZMA
  assert(buf != nullptr && size <= MAX_MEMORY_SIZE && sp != nullptr);
  lzma_stream zs = LZMA_STREAM_INIT;
  const char* rp = (const char*)buf;
  size_t zsiz = size + 1024;
  char* zbuf = static_cast<char*>(xmalloc(zsiz));
  char* wp = zbuf;
  zs.next_in = (const uint8_t*)rp;
  zs.avail_in = size;
  zs.next_out = (uint8_t*)wp;
  zs.avail_out = zsiz;
  switch (metadata_mode_) {
    default: {
      if (lzma_easy_encoder(&zs, level_, LZMA_CHECK_NONE) != LZMA_OK) {
        xfree(zbuf);
        return nullptr;
      }
      break;
    }
    case METADATA_CRC32: {
      if (lzma_easy_encoder(&zs, level_, LZMA_CHECK_CRC32) != LZMA_OK) {
        xfree(zbuf);
        return nullptr;
      }
      break;
    }
    case METADATA_SHA256: {
      if (lzma_easy_encoder(&zs, level_, LZMA_CHECK_SHA256) != LZMA_OK) {
        xfree(zbuf);
        return nullptr;
      }
      break;
    }
  }
  if (lzma_code(&zs, LZMA_FINISH) != LZMA_STREAM_END) {
    xfree(zbuf);
    lzma_end(&zs);
    return nullptr;
  }
  lzma_end(&zs);
  zsiz -= zs.avail_out;
  *sp = zsiz;
  return zbuf;
#else
  return nullptr;
#endif
}

char* LZMACompressor::Decompress(const void* buf, size_t size, size_t* sp) const {
#if _TKRZW_COMP_LZMA
  assert(buf != nullptr && size <= MAX_MEMORY_SIZE && sp != nullptr);
  size_t zsiz = size * 12 + 32;
  char* zbuf = static_cast<char*>(xmalloc(zsiz));
  while (true) {
    lzma_stream zs = LZMA_STREAM_INIT;
    const char* rp = (const char*)buf;
    char* wp = zbuf;
    zs.next_in = (const uint8_t*)rp;
    zs.avail_in = size;
    zs.next_out = (uint8_t*)wp;
    zs.avail_out = zsiz;
    if (lzma_auto_decoder(&zs, 1ULL << 30, 0) != LZMA_OK) {
      xfree(zbuf);
      return nullptr;
    }
    int32_t rv = lzma_code(&zs, LZMA_FINISH);
    lzma_end(&zs);
    if (rv == LZMA_STREAM_END) {
      zsiz -= zs.avail_out;
      *sp = zsiz;
      return zbuf;
    } else if (rv == LZMA_OK) {
      if (zsiz >= INT32MAX / 2) {
        break;
      }
      zsiz *= 2;
      zbuf = static_cast<char*>(xrealloc(zbuf, zsiz));
    } else {
      break;
    }
  }
  xfree(zbuf);
  return nullptr;
#else
  return nullptr;
#endif
}

std::unique_ptr<Compressor> LZMACompressor::MakeCompressor() const {
  return std::make_unique<LZMACompressor>(level_, metadata_mode_);
}

RC4Compressor::RC4Compressor(std::string_view key, uint32_t rnd_seed) {
  if (key.empty()) {
    key = std::string_view("\0", 1);
  }
  key_ = key;
  rnd_seed_ = rnd_seed;
  if (rnd_seed == 0) {
    rnd_seed = std::random_device()();
  }
  rnd_gen_ = new std::mt19937(rnd_seed);
  rnd_dist_ = new std::uniform_int_distribution<uint64_t>;
  rnd_mutex_ = new SpinMutex;
}

RC4Compressor::~RC4Compressor() {
  delete (SpinMutex*)rnd_mutex_;
  delete (std::uniform_int_distribution<uint64_t>*)rnd_dist_;
  delete (std::mt19937*)rnd_gen_;
}

bool RC4Compressor::IsSupported() const {
  return true;
}

char* RC4Compressor::Compress(const void* buf, size_t size, size_t* sp) const {
  assert(buf != nullptr && size <= MAX_MEMORY_SIZE && sp != nullptr);
  ((SpinMutex*)rnd_mutex_)->lock();
  uint64_t iv = (*((std::uniform_int_distribution<uint64_t>*)rnd_dist_))(
      *(std::mt19937*)rnd_gen_);
  ((SpinMutex*)rnd_mutex_)->unlock();
  constexpr size_t ivsize = 6;
  char* res_buf = (char*)xmalloc(size + ivsize);
  char* wp = res_buf;
  WriteFixNum(wp, iv, ivsize);
  const char* ivp = wp;
  wp += ivsize;
  const size_t vkey_size = key_.size() + ivsize;
  uint32_t sbox[0x100], kbox[0x100];
  for (int32_t i = 0; i < 0x100; i++) {
    sbox[i] = i;
    const int32_t vidx = i % vkey_size;
    if (vidx < key_.size()) {
      kbox[i] = ((uint8_t*)key_.data())[vidx];
    } else {
      kbox[i] = *(ivp + vidx - key_.size());
    }
  }
  uint32_t sidx = 0;
  for (int32_t i = 0; i < 0x100; i++) {
    sidx = (sidx + sbox[i] + kbox[i]) & 0xff;
    const uint32_t swap = sbox[i];
    sbox[i] = sbox[sidx];
    sbox[sidx] = swap;
  }
  const char* rp = (const char*)buf;
  const char* ep = rp + size;
  uint32_t x = 0;
  uint32_t y = 0;
  while (rp < ep) {
    x = (x + 1) & 0xff;
    y = (y + sbox[x]) & 0xff;
    const uint32_t swap = sbox[x];
    sbox[x] = sbox[y];
    sbox[y] = swap;
    *(wp++) = *(rp++) ^ sbox[(sbox[x] + sbox[y]) & 0xff];
  }
  *sp = wp - res_buf;
  return res_buf;
}

char* RC4Compressor::Decompress(const void* buf, size_t size, size_t* sp) const {
  assert(buf != nullptr && size <= MAX_MEMORY_SIZE && sp != nullptr);
  constexpr size_t ivsize = 6;
  if (size < ivsize) {
    return nullptr;
  }
  const char* rp = (const char*)buf;
  const char* ivp = rp;
  rp += ivsize;
  size -= ivsize;
  const size_t vkey_size = key_.size() + ivsize;
  uint32_t sbox[0x100], kbox[0x100];
  for (int32_t i = 0; i < 0x100; i++) {
    sbox[i] = i;
    const int32_t vidx = i % vkey_size;
    if (vidx < key_.size()) {
      kbox[i] = ((uint8_t*)key_.data())[vidx];
    } else {
      kbox[i] = *(ivp + vidx - key_.size());
    }
  }
  uint32_t sidx = 0;
  for (int32_t i = 0; i < 0x100; i++) {
    sidx = (sidx + sbox[i] + kbox[i]) & 0xff;
    const uint32_t swap = sbox[i];
    sbox[i] = sbox[sidx];
    sbox[sidx] = swap;
  }
  char* res_buf = (char*)xmalloc(size + 1);
  char* wp = res_buf;
  const char* ep = rp + size;
  uint32_t x = 0;
  uint32_t y = 0;
  while (rp < ep) {
    x = (x + 1) & 0xff;
    y = (y + sbox[x]) & 0xff;
    const uint32_t swap = sbox[x];
    sbox[x] = sbox[y];
    sbox[y] = swap;
    *(wp++) = *(rp++) ^ sbox[(sbox[x] + sbox[y]) & 0xff];
  }
  *sp = wp - res_buf;
  return res_buf;
}

std::unique_ptr<Compressor> RC4Compressor::MakeCompressor() const {
  return std::make_unique<RC4Compressor>(key_, rnd_seed_);
}

AESCompressor::AESCompressor(std::string_view key, uint32_t rnd_seed) {
  key_ = key;
  char bin_key[32];
  size_t key_size = 0;
  std::memset(bin_key, 0, 32);
  if (key.size() <= 32) {
    std::memset(bin_key, 0, 32);
    std::memcpy(bin_key, key.data(), key.size());
    const size_t rem_size = key.size() % 16;
    if (rem_size > 0) {
      key_size = key.size() + 16 - rem_size;
    }
    if (key_size < 16) {
      key_size = 16;
    }
  } else {
    for (size_t i = 0; i < key.size(); i++) {
      bin_key[i % 32] ^= *(unsigned char*)(key.data() + i);
    }
    key_size = 32;
  }
  rnd_seed_ = rnd_seed;
  if (rnd_seed == 0) {
    rnd_seed = std::random_device()();
  }
  rnd_gen_ = new std::mt19937(rnd_seed);
  rnd_dist_ = new std::uniform_int_distribution<uint64_t>;
  rnd_mutex_ = new SpinMutex;
  enc_rk_ = new uint32_t[64];
  dec_rk_ = new uint32_t[64];
  enc_rounds_ = AESKeySetupEnc(enc_rk_, (uint8_t*)bin_key, key_size * 8);
  dec_rounds_ = AESKeySetupDec(dec_rk_, (uint8_t*)bin_key, key_size * 8);
}

AESCompressor::~AESCompressor() {
  delete[] dec_rk_;
  delete[] enc_rk_;
  delete (SpinMutex*)rnd_mutex_;
  delete (std::uniform_int_distribution<uint64_t>*)rnd_dist_;
  delete (std::mt19937*)rnd_gen_;
}

bool AESCompressor::IsSupported() const {
  return true;
}

char* AESCompressor::Compress(const void* buf, size_t size, size_t* sp) const {
  assert(buf != nullptr && size <= MAX_MEMORY_SIZE && sp != nullptr);
  ((SpinMutex*)rnd_mutex_)->lock();
  uint64_t iv1 = (*((std::uniform_int_distribution<uint64_t>*)rnd_dist_))(
      *(std::mt19937*)rnd_gen_);
  uint64_t iv2 = (*((std::uniform_int_distribution<uint64_t>*)rnd_dist_))(
      *(std::mt19937*)rnd_gen_);
  ((SpinMutex*)rnd_mutex_)->unlock();
  char* res_buf = (char*)xmalloc(size + 32);
  char* wp = res_buf;
  const char* rp = (const char*)buf;
  char ivbuf[16];
  WriteFixNum(ivbuf, iv1, 8);
  WriteFixNum(ivbuf + 8, iv2, 8);
  AESEncrypt(enc_rk_, enc_rounds_, (const uint8_t*)ivbuf, (uint8_t*)wp);
  wp += 16;
  char xbuf[16];
  int32_t round = 0;
  while (size >= 16) {
    const char* pp = round > 0 ? rp - 16 : ivbuf;
    AESBlockXOR(rp, pp, xbuf);
    AESEncrypt(enc_rk_, enc_rounds_, (const uint8_t*)xbuf, (uint8_t*)wp);
    wp += 16;
    rp += 16;
    size -= 16;
    round++;
  }
  std::memset(xbuf, size, 16);
  const char* pp = round > 0 ? rp - 16 : ivbuf;
  AESBlockXORSized(rp, pp, xbuf, size);
  AESEncrypt(enc_rk_, enc_rounds_, (const uint8_t*)xbuf, (uint8_t*)wp);
  wp += 16;
  *sp = wp - res_buf;
  return res_buf;
}

char* AESCompressor::Decompress(const void* buf, size_t size, size_t* sp) const {
  assert(buf != nullptr && size <= MAX_MEMORY_SIZE && sp != nullptr);
  if (size < 32 || size % 16 != 0) {
    return nullptr;
  }
  const char* rp = (const char*)buf;
  char ivbuf[16];
  AESDecrypt(dec_rk_, dec_rounds_, (const uint8_t*)rp, (uint8_t*)ivbuf);
  rp += 16;
  size -= 16;
  char* res_buf = (char*)xmalloc(size + 1);
  char* wp = res_buf;
  int32_t round = 0;
  size_t rem_size = 0;
  while (size >= 16) {
    char xbuf[16];
    AESDecrypt(dec_rk_, dec_rounds_, (const uint8_t*)rp, (uint8_t*)xbuf);
    rem_size = xbuf[15];
    const char* pp = round > 0 ? wp - 16 : ivbuf;
    AESBlockXOR(xbuf, pp, wp);
    wp += 16;
    rp += 16;
    size -= 16;
    rem_size = wp[-1] ^ pp[15];
    round++;
  }
  if (rem_size > 16) {
    xfree(res_buf);
    return nullptr;
  }
  wp -= 16 - rem_size;
  *sp = wp - res_buf;
  return res_buf;
}

std::unique_ptr<Compressor> AESCompressor::MakeCompressor() const {
  return std::make_unique<AESCompressor>(key_, rnd_seed_);
}

}  // namespace tkrzw

// END OF FILE
