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

#ifndef _TKRZW_COMPRESS_H
#define _TKRZW_COMPRESS_H

#include <map>
#include <memory>
#include <string>
#include <vector>

#include <cinttypes>

#include "tkrzw_lib_common.h"

namespace tkrzw {

/**
 * Interfrace of data compression and decompression.
 */
class Compressor {
 public:
  /**
   * Destructor.
   */
  virtual ~Compressor() = default;

  /**
   * Checks whether the implementation is actually supported.
   * @return True if the implementation is actually supported.
   */
  virtual bool IsSupported() const = 0;

  /**
   * Compresses a serial data.
   * @param buf the input buffer.
   * @param size the size of the input buffer.
   * @param sp the pointer to the variable into which the size of the region of the return
   * value is assigned.
   * @return The pointer to the result data, or nullptr on failure.
   * @details Because the region of the return value is allocated with the xmalloc function,
   * it should be released with the xfree function.
   */
  virtual char* Compress(const void* buf, size_t size, size_t* sp) = 0;

  /**
   * Decompresses a serial data.
   * @param buf the input buffer.
   * @param size the size of the input buffer.
   * @param sp the pointer to the variable into which the size of the region of the return
   * value is assigned.
   * @return The pointer to the result data, or nullptr on failure.
   * @details Because the region of the return value is allocated with the xmalloc function,
   * it should be released with the xfree function.
   */
  virtual char* Decompress(const void* buf, size_t size, size_t* sp) = 0;

  /**
   * Makes a new Compressor object of the same concrete class.
   * @return The new Compressor object.
   */
  virtual std::unique_ptr<Compressor> MakeCompressor() const = 0;

  /**
   * Gets the type information of the actual class.
   * @return The type information of the actual class.
   */
  const std::type_info& GetType() const {
    const auto& entity = *this;
    return typeid(entity);
  }
};

/**
 * Dummy compressor implemetation.
 */
class DummyCompressor : public Compressor {
 public:
  /**
   * Constructor.
   * @param checksum If true, a checksum is added.
   */
  explicit DummyCompressor(bool checksum = false);

  /**
   * Destructor.
   */
  virtual ~DummyCompressor();

  /**
   * Checks whether the implementation is actually supported.
   * @return True if the implementation is actually supported.
   */
  bool IsSupported() const override;

  /**
   * Compresses a serial data.
   * @param buf the input buffer.
   * @param size the size of the input buffer.
   * @param sp the pointer to the variable into which the size of the region of the return
   * value is assigned.
   * @return The pointer to the result data, or nullptr on failure.
   * @details Because the region of the return value is allocated with the xmalloc function,
   * it should be released with the xfree function.
   */
  char* Compress(const void* buf, size_t size, size_t* sp) override;

  /**
   * Decompresses a serial data.
   * @param buf the input buffer.
   * @param size the size of the input buffer.
   * @param sp the pointer to the variable into which the size of the region of the return
   * value is assigned.
   * @return The pointer to the result data, or nullptr on failure.
   * @details Because the region of the return value is allocated with the xmalloc function,
   * it should be released with the xfree function.
   */
  char* Decompress(const void* buf, size_t size, size_t* sp) override;

  /**
   * Makes a new Compressor object of the same concrete class.
   * @return The new Compressor object.
   */
  std::unique_ptr<Compressor> MakeCompressor() const override;

 private:
  /** Whether to add a checksum. */
  bool checksum_;
};

/**
 * Complessor implemeted with ZLib.
 */
class ZLibCompressor final : public Compressor {
 public:
  /**
   * Enumeration for metadata modes.
   */
  enum MetadataMode : int32_t {
    /** Without any checksum. */
    METADATA_NONE = 0,
    /** With Adler-32 checksum, compatible with deflate. */
    METADATA_ADLER32 = 1,
    /** With CRC-32 checksum, compatible with gzip. */
    METADATA_CRC32 = 2,
  };

  /**
   * Constructor.
   * @param level The compression level between 0 and 9.  Higher means slower but better
   * compression.  0 means no compression.
   * @param metadata_mode The mode for the metadata added to the result.
   */
  explicit ZLibCompressor(int32_t level = 6, MetadataMode metadata_mode = METADATA_NONE);

  /**
   * Destructor.
   */
  virtual ~ZLibCompressor();

  /**
   * Checks whether the implementation is actually supported.
   * @return True if the implementation is actually supported.
   */
  bool IsSupported() const override;

  /**
   * Compresses a serial data.
   * @param buf the input buffer.
   * @param size the size of the input buffer.
   * @param sp the pointer to the variable into which the size of the region of the return
   * value is assigned.
   * @return The pointer to the result data, or nullptr on failure.
   * @details Because the region of the return value is allocated with the xmalloc function,
   * it should be released with the xfree function.
   */
  char* Compress(const void* buf, size_t size, size_t* sp) override;

  /**
   * Decompresses a serial data.
   * @param buf the input buffer.
   * @param size the size of the input buffer.
   * @param sp the pointer to the variable into which the size of the region of the return
   * value is assigned.
   * @return The pointer to the result data, or nullptr on failure.
   * @details Because the region of the return value is allocated with the xmalloc function,
   * it should be released with the xfree function.
   */
  char* Decompress(const void* buf, size_t size, size_t* sp) override;

  /**
   * Makes a new Compressor object of the same concrete class.
   * @return The new Compressor object.
   */
  std::unique_ptr<Compressor> MakeCompressor() const override;

 private:
  /** The compression level. */
  int32_t level_;
  /** The metadata mode. */
  MetadataMode metadata_mode_;
};

/**
 * Complessor implemeted with ZStd.
 */
class ZStdCompressor final : public Compressor {
 public:
  /**
   * Constructor.
   * @param level The compression level between -1 and 19.  Higher means slower but better
   * compression.  0 is a special value for adaptive settings.  -1 is a special value for ultra
   * fast settings.
   */
  explicit ZStdCompressor(int32_t level = 3);

  /**
   * Destructor.
   */
  virtual ~ZStdCompressor();

  /**
   * Checks whether the implementation is actually supported.
   * @return True if the implementation is actually supported.
   */
  bool IsSupported() const override;

  /**
   * Compresses a serial data.
   * @param buf the input buffer.
   * @param size the size of the input buffer.
   * @param sp the pointer to the variable into which the size of the region of the return
   * value is assigned.
   * @return The pointer to the result data, or nullptr on failure.
   * @details Because the region of the return value is allocated with the xmalloc function,
   * it should be released with the xfree function.
   */
  char* Compress(const void* buf, size_t size, size_t* sp) override;

  /**
   * Decompresses a serial data.
   * @param buf the input buffer.
   * @param size the size of the input buffer.
   * @param sp the pointer to the variable into which the size of the region of the return
   * value is assigned.
   * @return The pointer to the result data, or nullptr on failure.
   * @details Because the region of the return value is allocated with the xmalloc function,
   * it should be released with the xfree function.
   */
  char* Decompress(const void* buf, size_t size, size_t* sp) override;

  /**
   * Makes a new Compressor object of the same concrete class.
   * @return The new Compressor object.
   */
  std::unique_ptr<Compressor> MakeCompressor() const override;

 private:
  /** The compression level. */
  int32_t level_;
};

/**
 * Complessor implemeted with LZ4.
 */
class LZ4Compressor final : public Compressor {
 public:
  /**
   * Constructor.
   * @param acceleration The accelaration level which is 1 or more.  Increasing it by 1 means
   * 3-4% speed boost with less compression ratio.
   */
  explicit LZ4Compressor(int32_t acceleration = 1);

  /**
   * Destructor.
   */
  virtual ~LZ4Compressor();

  /**
   * Checks whether the implementation is actually supported.
   * @return True if the implementation is actually supported.
   */
  bool IsSupported() const override;

  /**
   * Compresses a serial data.
   * @param buf the input buffer.
   * @param size the size of the input buffer.
   * @param sp the pointer to the variable into which the size of the region of the return
   * value is assigned.
   * @return The pointer to the result data, or nullptr on failure.
   * @details Because the region of the return value is allocated with the xmalloc function,
   * it should be released with the xfree function.
   */
  char* Compress(const void* buf, size_t size, size_t* sp) override;

  /**
   * Decompresses a serial data.
   * @param buf the input buffer.
   * @param size the size of the input buffer.
   * @param sp the pointer to the variable into which the size of the region of the return
   * value is assigned.
   * @return The pointer to the result data, or nullptr on failure.
   * @details Because the region of the return value is allocated with the xmalloc function,
   * it should be released with the xfree function.
   */
  char* Decompress(const void* buf, size_t size, size_t* sp) override;

  /**
   * Makes a new Compressor object of the same concrete class.
   * @return The new Compressor object.
   */
  std::unique_ptr<Compressor> MakeCompressor() const override;

 private:
  /** The acceleration level. */
  int32_t acceleration_;
};

/**
 * Complessor implemeted with LZMA.
 */
class LZMACompressor final : public Compressor {
 public:
  /**
   * Enumeration for metadata modes.
   */
  enum MetadataMode : int32_t {
    /** Without any checksum. */
    METADATA_NONE = 0,
    /** With CRC-32 checksum. */
    METADATA_CRC32 = 2,
    /** With SHA-256 checksum. */
    METADATA_SHA256 = 1,
  };

  /**
   * Constructor.
   * @param level The compression level between 0 and 9.  Higher means slower but better
   * compression.  0 means no compression.
   * @param metadata_mode The mode for the metadata added to the result.
   */
  explicit LZMACompressor(int32_t level = 6, MetadataMode metadata_mode = METADATA_NONE);

  /**
   * Destructor.
   */
  virtual ~LZMACompressor();

  /**
   * Checks whether the implementation is actually supported.
   * @return True if the implementation is actually supported.
   */
  bool IsSupported() const override;

  /**
   * Compresses a serial data.
   * @param buf the input buffer.
   * @param size the size of the input buffer.
   * @param sp the pointer to the variable into which the size of the region of the return
   * value is assigned.
   * @return The pointer to the result data, or nullptr on failure.
   * @details Because the region of the return value is allocated with the xmalloc function,
   * it should be released with the xfree function.
   */
  char* Compress(const void* buf, size_t size, size_t* sp) override;

  /**
   * Decompresses a serial data.
   * @param buf the input buffer.
   * @param size the size of the input buffer.
   * @param sp the pointer to the variable into which the size of the region of the return
   * value is assigned.
   * @return The pointer to the result data, or nullptr on failure.
   * @details Because the region of the return value is allocated with the xmalloc function,
   * it should be released with the xfree function.
   */
  char* Decompress(const void* buf, size_t size, size_t* sp) override;

  /**
   * Makes a new Compressor object of the same concrete class.
   * @return The new Compressor object.
   */
  std::unique_ptr<Compressor> MakeCompressor() const override;

 private:
  /** The compression level. */
  int32_t level_;
  /** The metadata mode. */
  MetadataMode metadata_mode_;
};

}  // namespace tkrzw

#endif  // _TKRZW_COMPRESS_H

// END OF FILE
