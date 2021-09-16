/*************************************************************************************************
 * Implementation components for the skip database manager
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

#ifndef _TKRZW_DBM_SKIP_IMPL_H
#define _TKRZW_DBM_SKIP_IMPL_H

#include <atomic>
#include <iostream>
#include <limits>
#include <mutex>
#include <set>
#include <string>
#include <string_view>
#include <tuple>
#include <utility>
#include <vector>

#include <cinttypes>
#include <cstdarg>

#include "tkrzw_dbm.h"
#include "tkrzw_file.h"
#include "tkrzw_file_util.h"
#include "tkrzw_lib_common.h"

namespace tkrzw {

class SkipRecordCache;

/**
 * Key and value record structure in the file skip database.
 */
class SkipRecord final {
 public:
  /**
   * Constructor.
   * @param file The pointer to the file object.
   * @param offset_width The width of the offset data.
   * @param step_unit The unit of stepping.
   * @param max_level The maximum level of the skip list.
   */
  SkipRecord(File* file, int32_t offset_width, int32_t step_unit, int32_t max_level);

  /**
   * Destructor.
   */
  ~SkipRecord();

  /**
   * Assigns the internal state from another moved record object.
   * @param rhs The other record object.
   */
  SkipRecord& operator =(SkipRecord&& rhs);

  /**
   * Reads the metadata and the key.
   * @param offset The offset of the record.
   * @param index The index of the record.
   * @return The result status.
   * @details If successful, the key data is always read.  However the value data is not always
   * read.  To read it, call ReadBody.
   */
  Status ReadMetadataKey(int64_t offset, int64_t index);

  /**
   * Reads the body data and fill all the properties.
   * @return The result status.
   */
  Status ReadBody();

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
   * Gets the offsets of the records to skip to.
   * @return The offsets of the records to skip to.
   */
  const std::vector<int64_t>& GetStepOffsets() const;

  /**
   * Gets the level of the record.
   * @return The level of the record.
   */
  int32_t GetLevel() const;

  /**
   * Gets the index of the record.
   * @return The index of the record.
   */
  int64_t GetIndex() const;

  /**
   * Gets the offset of the record.
   * @return The offset of the record.
   */
  int64_t GetOffset() const;

  /**
   * Gets the whole size of the record.
   * @return The whole size of the record.
   */
  int32_t GetWholeSize() const;

  /**
   * Sets the actual data of the record.
   * @param index The index of the record.
   * @param key_ptr The pointer to a key buffer.
   * @param key_size The size of the key buffer.
   * @param value_ptr The pointer to a value buffer.
   * @param value_size The size of the value buffer.
   */
  void SetData(int64_t index, const char* key_ptr, int32_t key_size,
               const char* value_ptr, int32_t value_size);

  /**
   * Writes the record in the file.
   * @return The result status.
   */
  Status Write();

  /**
   * Updates past records which refer to this record.
   * @param index The index of the record.
   * @param offset The offset of the record.
   * @param past_offsets The pointer to a vector of offsets of the past records, whose size must
   * be the same as the maximum level.
   * @return The result status.
   */
  Status UpdatePastRecords(
      int64_t index, int64_t offset, std::vector<int64_t>* past_offsets) const;

  /**
   * Searches records for the one with the same key.
   * @param record_base The record base offset.
   * @param cache The cache for skip records.
   * @param key The key to match with.
   * @param upper If true, the first upper record is retrieved if there's no record matching.
   * @return The result status.
   */
  Status Search(int64_t record_base, SkipRecordCache* cache, std::string_view key, bool upper);

  /**
   * Searches records for the one with the same index.
   * @param record_base The record base offset.
   * @param cache The cache for skip records.
   * @param index The index of the target record.
   * @return The result status.
   */
  Status SearchByIndex(int64_t record_base, SkipRecordCache* cache, int64_t index);

  /**
   * Gets the file object.
   * @return The pointer to the file object.
   */
  File* GetFile() const;

  /**
   * Searializes the internal data.
   * @return The result string.  The caller has the ownership.
   */
  char* Serialize() const;

  /**
   * Deserializes a string and prepare the internal data.
   * @param index The index of the serialized record.
   * @param serialized The serialized string.  Ownership is not taken.
   */
  void Deserialize(int64_t index, const char* serialized);

 private:
  /** The size of the stack buffer to read the record. */
  static constexpr int32_t READ_BUFFER_SIZE = 256;
  /** The size of the data to be read at first. */
  static constexpr int32_t READ_DATA_SIZE = 32;
  /** The size of the stack buffer to write the record. */
  static constexpr int32_t WRITE_BUFFER_SIZE = 4096;
  /** The magic number of the record. */
  static constexpr uint8_t RECORD_MAGIC = 0xFF;
  /** The file object, unowned. */
  File* file_;
  /** The width of the offset data. */
  int32_t offset_width_;
  /** The unit of stepping. */
  int32_t step_unit_;
  /** The maximum level of the skip list. */
  int32_t max_level_;
  /** The stack buffer with the consant size. */
  char buffer_[READ_BUFFER_SIZE];
  /** The level of the node. */
  int32_t level_;
  /** The offset of the record. */
  int64_t offset_;
  /** The index of the record. */
  int64_t index_;
  /** The whole size of the record. */
  int32_t whole_size_;
  /** The size of the key. */
  int32_t key_size_;
  /** The size of the value */
  int32_t value_size_;
  /** The offsets of records to skip to. */
  std::vector<int64_t> skip_offsets_;
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
 * Cache of skip records.
 */
class SkipRecordCache {
public:
  /**
   * Constructor.
   * @param step_unit The unit of stepping.
   * @param capacity The maximum number of records to contain.
   * @param num_records The number of records in the database.
   */
  SkipRecordCache(int32_t step_unit, int32_t capacity, int64_t num_records);

  /**
   * Destructor.
   */
  ~SkipRecordCache();

  /**
   * Prepare a record by reading the cache.
   * @param index The index of the record.
   * @param record The pointer to the record object.
   * @return True on success or false onfailure.
   */
  bool PrepareRecord(int64_t index, SkipRecord* record);

  /**
   * Add a record to the cache.
   * @param record The record to store.
   */
  void Add(const SkipRecord& record);

 private:
  /** The size of the cache. */
  int32_t size_;
  /** The unit number of the index. */
  int64_t cache_unit_;
  /** The array of atomic pointers, owned. */
  std::atomic<char*>* records_;
};

/**
 * Sorter for a large amound of records based on merge sort on files.
 */
class RecordSorter final {
 public:
  /**
   * Constructor.
   * @param base_path The base path of the temporary files.
   * @param max_mem_size The maximum memory size to use.
   * @param use_mmap If true, memory mapping files are used.
   */
  RecordSorter(const std::string& base_path, int64_t max_mem_size, bool use_mmap);

  /**
   * Destructor.
   */
  ~RecordSorter();

  /**
   * Adds a record.
   * @param key The key string.
   * @param value The key string.
   * @return The result status.
   */
  Status Add(std::string_view key, std::string_view value);

  /**
   * Adds a file of SkipRecord.
   * @param rec The pointer to a skip record, whose ownership is taken.
   * @param record_base The record base offset.
   */
  void AddSkipRecord(SkipRecord* rec, int64_t record_base);

  /**
   * Takes ownership of a file object.
   * @param file The unique pointer of the file object.
   */
  void TakeFileOwnership(std::unique_ptr<File>&& file);

  /**
   * Checks whether the sorter is updated.
   * @return True if the sorter is updated or false if not.
   */
  bool IsUpdated() const;

  /**
   * Finishes adding records and allows getting them.
   * @return The result status.
   */
  Status Finish();

  /**
   * Gets the minimum record.
   * @param key The pointer to a string object to contain the record key.
   * @param value The pointer to a string object to contain the record value.
   * @return The result status.
   */
  Status Get(std::string* key, std::string* value);

 private:
  /**
   * Structure of a temporary file of flat records.
   */
  struct TmpFileFlat final {
    /** The path of the file. */
    std::string path;
    /** File object, owned. */
    File* file;
    /** The record reader, owned. */
    FlatRecordReader* reader;
  };

  /**
   * Structure of a sorting slot.
   */
  struct SortSlot final {
    /** Id of the slot. */
    int32_t id;
    /** The last retrieved key. */
    std::string key;
    /** The last retrieved value. */
    std::string value;
    /** The file object, unowned. */
    File* file;
    /** The flat record reader, unowned. */
    FlatRecordReader* flat_reader;
    /** The skip record object, unowned. */
    SkipRecord* skip_record;
    /** The current offset. */
    int64_t offset;
    /** The end offset. */
    int64_t end_offset;
    /** Constructor. */
    SortSlot() : file(nullptr), offset(0), end_offset(0) {}
  };

  /**
   * Comparator for sorting slots.
   */
  struct SortSlotComparator final {
    /**
     * Compares two sorting slots.
     * @param lhs The first sorting slot.
     * @param rhs The second other sorting slot.
     * @param True if the first one is greater.
     */
    bool operator ()(const SortSlot* lhs, const SortSlot* rhs) const {
      return std::tie(lhs->key, lhs->id) > std::tie(rhs->key, rhs->id);
    }
  };

  /**
   * Flushes the current records into a file.
   * @return The result status.
   */
  Status Flush();

  /** Expected memory footprint for a record. */
  static constexpr int32_t REC_MEM_FOOT = 8;
  /** The maximum data size to use mmap files. */
  static constexpr int64_t MAX_DATA_SIZE_MMAP_USE = 4LL << 30;
  /** The base path of the temporary files. */
  std::string base_path_;
  /** The maximum memory size to use. */
  int64_t max_mem_size_;
  /** Whether to use memory mapping. */
  bool use_mmap_;
  /** The total size of data. */
  int64_t total_data_size_;
  /** True if adding is finished. */
  bool finished_;
  /** Current serialized records. */
  std::vector<std::string> current_records_;
  /** The current memory size. */
  int64_t current_mem_size_;
  /** The temporary files. */
  std::vector<TmpFileFlat> tmp_files_;
  /** The skip record files. */
  std::vector<std::pair<SkipRecord*, int64_t>> skip_records_;
  /** The heap to get the mimimum record. */
  std::vector<SortSlot*> heap_;
  /** Slots for merge sort. */
  std::vector<SortSlot> slots_;
  /** Owned file objects. */
  std::vector<std::shared_ptr<File>> owned_files_;
};

}  // namespace tkrzw

#endif  // _TKRZW_DBM_SKIP_IMPL_H

// END OF FILE
