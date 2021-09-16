/*************************************************************************************************
 * Implementation components for the hash database manager
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

#ifndef _TKRZW_DBM_HASH_IMPL_H
#define _TKRZW_DBM_HASH_IMPL_H

#include <iostream>
#include <limits>
#include <mutex>
#include <set>
#include <string>
#include <string_view>
#include <vector>

#include <cinttypes>
#include <cstdarg>

#include "tkrzw_compress.h"
#include "tkrzw_dbm.h"
#include "tkrzw_file.h"
#include "tkrzw_hash_util.h"
#include "tkrzw_lib_common.h"
#include "tkrzw_str_util.h"
#include "tkrzw_thread_util.h"

namespace tkrzw {

/**
 * Key and value record structure in the file hash database.
 */
class HashRecord final {
 public:
  /** The maximum key size. */
  static constexpr int64_t MAX_KEY_SIZE = 8 * (1LL << 20) - 1;
  /** The maximum value size. */
  static constexpr int64_t MAX_VALUE_SIZE = 2 * (1LL << 30) - 1;
  /** The minimum size to read the meta data. */
  static constexpr int64_t META_MIN_READ_SIZE = 48;

  /**
   * Enumeration for operation types.
   */
  enum OperationType : int32_t {
    /** Operation to do nothing. */
    OP_VOID = 0,
    /** Operation to set the value of an existing record. */
    OP_SET = 1,
    /** Operation to remove an existing record. */
    OP_REMOVE = 2,
    /** Operation to add a new record. */
    OP_ADD = 3,
  };

  /**
   * Constructor.
   * @param file The pointer to the file object.
   * @param crc_width The width of the CRC value.
   * @param offset_width The width of the offset data.
   * @param align_pow The alignment power.
   */
  HashRecord(File* file, int32_t crc_width, int32_t offset_width, int32_t align_pow);

  /**
   * Destructor.
   */
  ~HashRecord();

  /**
   * Gets the operation type of the record.
   * @return The operation type of the record.
   */
  OperationType GetOperationType() const;

  /**
   * Gets the key data.
   * @return The key data.
   */
  std::string_view GetKey() const;

  /**
   * Gets the value data.
   * @return The value data.  The data might be nullptr if the body hasn't been read.
   */
  std::string_view GetValue() const;

  /**
   * Gets the offset of the child record.
   * @return The offset of the child record.
   */
  int64_t GetChildOffset() const;

  /**
   * Gets the whole size of the record.
   * @return The whole size of the record.  It might be zero if the body hasn't been read.
   */
  int32_t GetWholeSize() const;

  /**
   * Reads the metadata and the key.
   * @param offset The offset of the record.
   * @param min_read_size The minimum reading size.
   * @return The result status.
   * @details If successful, the key data is always read.  However the value data and the whole
   * size is not always read.  To read them, call ReadBody.
   */
  Status ReadMetadataKey(int64_t offset, int32_t min_read_size);

  /**
   * Reads the body data and fill all the properties.
   * @return The result status.
   */
  Status ReadBody();

  /**
   * Sets the body data with the value filled with null codes.
   */
  void SetBodyWithNullValue();

  /**
   * Check the CRC.
   * @return The result status.
   */
  Status CheckCRC();

  /**
   * Sets the actual data of the record.
   * @param type An operation type.
   * @param ideal_whole_size The ideal size of the storage space.
   * @param key_ptr The pointer to a key buffer.
   * @param key_size The size of the key buffer.
   * @param value_ptr The pointer to a value buffer.
   * @param value_size The size of the value buffer.
   * @param child_offset The offset of the child record or zero for nothing.
   */
  void SetData(OperationType type, int32_t ideal_whole_size,
               const char* key_ptr, int32_t key_size,
               const char* value_ptr, int32_t value_size,
               int64_t child_offset);

  /**
   * Writes the record in the file.
   * @param offset The offset of the record.  If it is negative, the data is appended at the end
   * of the file.
   * @param new_offset The pointer to an integer to store the offset of the appended data.  If can
   * be nullptr if the given offset is not negative.
   * @return The result status.
   */
  Status Write(int64_t offset, int64_t* new_offset) const;

  /**
   * Writes the child offset of the record in the file.
   * @param offset The offset of the record to update.
   * @param child_offset The offset of the child record.
   * @return The result status.
   */
  Status WriteChildOffset(int64_t offset, int64_t child_offset);

  /**
   * Finds the next record offset by heuristics.
   * @param offset The current offset.
   * @param min_read_size The minimum reading size.
   * @param next_offset The pointer to an integer to store the next offset.
   * @return The result status.
   */
  Status FindNextOffset(int64_t offset, int32_t min_read_size, int64_t* next_offset);

  /**
   * Replays operations applied on a hash database file.
   * @param file A file object having opened the database file.
   * @param proc The pointer to the processor object.
   * @param record_base The record base offset.
   * @param crc_width The width of the CRC value.
   * @param compressor The compressor object.
   * @param offset_width The offset width.
   * @param align_pow The alignment power.
   * @param min_read_size The minimum reading size.
   * @param skip_broken_records If true, the operation continues even if there are broken records
   * which can be skipped.
   * @param end_offset The exclusive end offset of records to read.  Negative means unlimited.
   * @return The result status.
   * @details For each setting operation, ProcessFull of the processer is called.  For each
   * removing operation, ProcessEmpty of the processor is called.  If they return a value other
   * than NOOP, the iteration is cancelled.
   */
  static Status ReplayOperations(
      File* file, DBM::RecordProcessor* proc,
      int64_t record_base, int32_t crc_width, Compressor* compressor,
      int32_t offset_width, int32_t align_pow,
      int32_t min_read_size, bool skip_broken_records, int64_t end_offset);

  /**
   * Extracts a sequence of offsets from a file.
   * @param in_file A file object having opened the input database file.
   * @param out_file A file object having opened as a writer to store the output.
   * @param record_base The record base offset.
   * @param crc_width The width of the CRC value.
   * @param offset_width The offset width.
   * @param align_pow The alignment power.
   * @param skip_broken_records If true, the operation continues even if there are broken records
   * which can be skipped.
   * @param end_offset The exclusive end offset of records to read.  Negative means unlimited.
   * @return The result status.
   */
  static Status ExtractOffsets(
      File* in_file, File* out_file,
      int64_t record_base, int32_t crc_width, int32_t offset_width, int32_t align_pow,
      bool skip_broken_records, int64_t end_offset);

 private:
  /** The size of the stack buffer to read the meta data. */
  static constexpr int32_t META_BUFFER_SIZE = 512;
  /** The size of the stack buffer to write the record. */
  static constexpr int32_t WRITE_BUFFER_SIZE = 4096;
  /** The magic number at the top of the void record. */
  static constexpr uint8_t RECORD_MAGIC_VOID = 0xC0;
  /** The magic number at the top of the setting record. */
  static constexpr uint8_t RECORD_MAGIC_SET = 0x80;
  /** The magic number at the top of the removing record. */
  static constexpr uint8_t RECORD_MAGIC_REMOVE = 0x40;
  /** The magic number at the padding size. */
  static constexpr uint8_t PADDING_SIZE_MAGIC = 0xEE;
  /** The magic number at the top of the padding. */
  static constexpr uint8_t PADDING_TOP_MAGIC = 0xDD;
  /** The file object, unowned. */
  File* file_;
  /** The width of the CRC function. */
  int32_t crc_width_;
  /** The width of the offset data. */
  int32_t offset_width_;
  /** The alignment power. */
  int32_t align_pow_;
  /** The stack buffer to read the meta data. */
  char meta_buf_[META_BUFFER_SIZE];
  /** The extended buffer to read the meta data. */
  char* ext_meta_buf_;
  /** The type of operation. */
  OperationType type_;
  /** The whole size of the record. */
  uint32_t whole_size_;
  /** The header size of the record. */
  uint32_t header_size_;
  /** The size of the key. */
  uint32_t key_size_;
  /** The size of the value */
  uint32_t value_size_;
  /** The size of the padding */
  uint32_t padding_size_;
  /** The offset of the child record. */
  int64_t child_offset_;
  /** The magic checksum value. */
  uint32_t magic_checksum_;
  /** The extra CRC value. */
  uint32_t crc_value_;
  /** The pointer to the key region. */
  const char* key_ptr_;
  /** The pointer to the value region. */
  const char* value_ptr_;
  /** The offset of the body region. */
  int64_t body_offset_;
  /** The buffer for the body data. */
  char* body_buf_;
};

/**
 * Reader of a sequence of offsets.
 */
class OffsetReader final {
 public:
  /**
   * Constructor.
   * @param file A file object containing offsets.
   * @param offset_width The offset width.
   * @param align_pow The alignment power.
   * @param reversal The flag for reverse order.
   */
  OffsetReader(File* file, int32_t offset_width, int32_t align_pow, bool reversal);

  /**
   * Read an offset.
   * @param offset The pointer to an integer to store the result.
   * @return The result status.
   */
  Status ReadOffset(int64_t* offset);

 private:
  /** The size of the stack buffer to read the record. */
  static constexpr int32_t READ_BUFFER_SIZE = 4096;
  /** The file object, unowned. */
  File* file_;
  /** The width of the offset data. */
  int32_t offset_width_;
  /** The alignment power. */
  int32_t align_pow_;
  /** The flag for reverse order. */
  bool reversal_;
  /** The stack buffer with the consant size. */
  char buffer_[READ_BUFFER_SIZE];
  /** The current pointer. */
  char* current_ptr_;
  /** The end pointer. */
  char* end_ptr_;
  /** The current offset. */
  int64_t current_offset_;
};

/**
 * Free block structure.
 */
struct FreeBlock final {
  /** The offset in the file. */
  int64_t offset;
  /** The size of the block. */
  int32_t size;

  /** Constructor. */
  FreeBlock() {}

  /**
   * Constructor.
   * @param offset The offset in the file.
   * @param size The size of the block
   */
  FreeBlock(int64_t offset, int32_t size) : offset(offset), size(size) {}

  /**
   * Comparator to get the minimum free block.
   * @param rhs The other object to compare with.
   * @return True if the self record is smaller than the oher record.
   */
  bool operator <(const FreeBlock& rhs) const {
    if (size != rhs.size) {
      return size < rhs.size;
    }
    return offset < rhs.offset;
  }
};

/**
 * Registry of free blocks.
 */
class FreeBlockPool final {
 public:
  /**
   * Constructor.
   * @param capacity The capacity of the pool.
   */
  FreeBlockPool(int32_t capacity);

  /**
   * Sets the capacity.
   * @param capacity The capacity of the pool.
   */
  void SetCapacity(int32_t capacity);

  /**
   * Removes all records.
   */
  void Clear();

  /**
   * Inserts a free block.
   * @param offset The offset in the file.
   * @param size The size of the block
   */
  void InsertFreeBlock(int64_t offset, int32_t size);

  /**
   * Fetchs the minimum free block meeting the record size to fit it in.
   * @param min_size The minimum size of the block to fetch.
   * @param res The pointer to a free block object to store the result.
   * @return True on success or false on failure.
   */
  bool FetchFreeBlock(int32_t min_size, FreeBlock* res);

  /**
   * Gets the current number of free blocks.
   * @return The current number of free blocks.
   */
  int32_t Size();

  /**
   * Serializes records into a string.
   * @param offset_width The offset width.
   * @param align_pow The alignment power.
   * @param size The size of the result string.
   * @return The result string.
   */
  std::string Serialize(int32_t offset_width, int32_t align_pow, int32_t size);

  /**
   * Deserializes a string to set records.
   * @param str The string to deserialize.
   * @param offset_width The offset width.
   * @param align_pow The alignment power.
   */
  void Deserialize(std::string_view str, int32_t offset_width, int32_t align_pow);

 private:
  /** The maximum number of free blocks to keep. */
  int32_t capacity_ = std::numeric_limits<int32_t>::max();
  /** The set of free blocks in order of the size. */
  std::set<FreeBlock> data_;
  /** Mutex for the data set. */
  SpinMutex mutex_;
};

/**
 * Calculates the checksum of the record data to store in the record magic data.
 * @param key_buf The key buffer.
 * @param key_size The key size.
 * @param value_buf The value buffer.
 * @param value_size The value size.
 * @return The checksum which uses the lower 6 bits only.
 */
inline uint32_t MagicChecksum(const char* key_buf, size_t key_size,
                              const char* value_buf, size_t value_size) {
  return HashChecksum6Pair(key_buf, key_size, value_buf, value_size, 11) + 3;
}

/**
 * Calls the ProcessFull method of a record processor with compression and decompression.
 * @param proc The record processor, which takes a decompressed value.
 * @param key The key of a record.
 * @param old_value The old value of the record.
 * @param new_value_orig The pointer to store the original new value before compression.
 * @param compressor The compressor object for compression and decompression.  nullptr is OK.
 * @param comp_data_placeholder The praceholder to manage the compression data.
 * @return The value returned from the record processor.  If it is a normal value, compression is
 * done implicitly.
 */

std::string_view CallRecordProcessFull(
    DBM::RecordProcessor* proc, std::string_view key, std::string_view old_value,
    std::string_view* new_value_orig,
    Compressor* compressor, ScopedStringView* comp_data_placeholder);

/**
 * Calls the ProcessEmpty method of a record processor with compression.
 * @param proc The record processor.
 * @param key The key of a record.
 * @param new_value_orig The pointer to store the original new value before compression.
 * @param compressor The compressor object for compression and decompression.  nullptr is OK.
 * @param comp_data_placeholder The praceholder to manage the compression data.
 * @return The value returned from the record processor.  If it is a normal value, compression is
 * done implicitly.
 */
std::string_view CallRecordProcessEmpty(
    DBM::RecordProcessor* proc, std::string_view key,
    std::string_view* new_value_orig,
    Compressor* compressor, ScopedStringView* comp_data_placeholder);

}  // namespace tkrzw

#endif  // _TKRZW_DBM_HASH_IMPL_H

// END OF FILE
