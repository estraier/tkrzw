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

char* DummyCompressor::Compress(const void* buf, size_t size, size_t* sp) {
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

char* DummyCompressor::Decompress(const void* buf, size_t size, size_t* sp) {
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

char* ZLibCompressor::Compress(const void* buf, size_t size, size_t* sp) {
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

char* ZLibCompressor::Decompress(const void* buf, size_t size, size_t* sp) {
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

char* ZStdCompressor::Compress(const void* buf, size_t size, size_t* sp) {
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

char* ZStdCompressor::Decompress(const void* buf, size_t size, size_t* sp) {
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

char* LZ4Compressor::Compress(const void* buf, size_t size, size_t* sp) {
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

char* LZ4Compressor::Decompress(const void* buf, size_t size, size_t* sp) {
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

char* LZMACompressor::Compress(const void* buf, size_t size, size_t* sp) {
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

char* LZMACompressor::Decompress(const void* buf, size_t size, size_t* sp) {
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

}  // namespace tkrzw

// END OF FILE
