/*************************************************************************************************
 * File database manager implementation based on hash table
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

#ifndef _TKRZW_DBM_HASH_H
#define _TKRZW_DBM_HASH_H

#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <cinttypes>

#include "tkrzw_dbm.h"
#include "tkrzw_file.h"
#include "tkrzw_lib_common.h"

namespace tkrzw {

class HashDBMImpl;
class HashDBMIteratorImpl;

/**
 * File database manager implementation based on hash table.
 * @details All operations are thread-safe; Multiple threads can access the same database
 * concurrently.  Every opened database must be closed explicitly to avoid data corruption.
 */
class HashDBM final : public DBM {
 public:
  /** The default value of the offset width. */
  static constexpr int32_t DEFAULT_OFFSET_WIDTH = 4;
  /** The default value of the alignment power. */
  static constexpr int32_t DEFAULT_ALIGN_POW = 3;
  /** The default value of the number of buckets. */
  static constexpr int64_t DEFAULT_NUM_BUCKETS = 1048583;
  /** The default value of the capacity of the free block pool. */
  static constexpr int32_t DEFAULT_FBP_CAPACITY = 2048;
  /** The size of the opaque metadata. */
  static constexpr int32_t OPAQUE_METADATA_SIZE = 64;

  /**
   * Iterator for each record.
   * @details When the database is updated, some iterators may or may not be invalided.
   * Operations with invalidated iterators fails gracefully with NOT_FOUND_ERROR.  One iterator
   * cannot be shared by multiple threads.
   */
  class Iterator final : public DBM::Iterator {
    friend class tkrzw::HashDBM;
   public:
    /**
     * Destructor.
     */
    virtual ~Iterator();

    /**
     * Copy and assignment are disabled.
     */
    explicit Iterator(const Iterator& rhs) = delete;
    Iterator& operator =(const Iterator& rhs) = delete;

    /**
     * Initializes the iterator to indicate the first record.
     * @return The result status.
     * @details Precondition: The database is opened.
     * @details Even if there's no record, the operation doesn't fail.
     */
    Status First() override;

    /**
     * Initializes the iterator to indicate the last record.
     * @return The result status.
     * @details This method is not supported.
     */
    Status Last() override {
      return Status(Status::NOT_IMPLEMENTED_ERROR);
    }

    /**
     * Initializes the iterator to indicate a specific record.
     * @param key The key of the record to look for.
     * @return The result status.
     * @details Precondition: The database is opened.
     * @details This database is unordered so it doesn't support "lower bound" jump.  If there's
     * no record matching to the given key, the operation fails.
     */
    Status Jump(std::string_view key) override;

    /**
     * Initializes the iterator to indicate the last record whose key is lower than a given key.
     * @param key The key to compare with.
     * @param inclusive If true, the considtion is inclusive: equal to or lower than the key.
     * @return The result status.
     * @details This method is not supported.
     */
    Status JumpLower(std::string_view key, bool inclusive = false) override {
      return Status(Status::NOT_IMPLEMENTED_ERROR);
    }

    /**
     * Initializes the iterator to indicate the first record whose key is upper than a given key.
     * @param key The key to compare with.
     * @param inclusive If true, the considtion is inclusive: equal to or upper than the key.
     * @return The result status.
     * @details This method is not supported.
     */
    Status JumpUpper(std::string_view key, bool inclusive = false) override {
      return Status(Status::NOT_IMPLEMENTED_ERROR);
    }

    /**
     * Moves the iterator to the next record.
     * @return The result status.
     * @details Precondition: The database is opened and the iterator is initialized.
     * @details If the current record is missing, the operation fails.  Even if there's no next
     * record, the operation doesn't fail.
     */
    Status Next() override;

    /**
     * Moves the iterator to the previous record.
     * @return The result status.
     * @details This method is not suppoerted.
     */
    Status Previous() override {
      return Status(Status::NOT_IMPLEMENTED_ERROR);
    }

    /**
     * Processes the current record with a processor.
     * @param proc The pointer to the processor object.
     * @param writable True if the processor can edit the record.
     * @return The result status.
     * @details Precondition: The database is opened and the iterator is initialized.
     * The writable parameter should be consistent to the open mode.
     * @details If the current record exists, the ProcessFull of the processor is called.
     * Otherwise, this method fails and no method of the processor is called.  If the current
     * record is removed, the iterator is moved to the next record.
     */
    Status Process(RecordProcessor* proc, bool writable) override;

   private:
    /**
     * Constructor.
     * @param dbm_impl The database implementation object.
     */
    explicit Iterator(HashDBMImpl* dbm_impl);

    /** Pointer to the actual implementation. */
    HashDBMIteratorImpl* impl_;
  };

  /**
   * Enumeration for update modes.
   */
   enum UpdateMode {
     /** The default behavior. */
     UPDATE_DEFAULT = 0,
     /** To do in-place writing. */
     UPDATE_IN_PLACE = 1,
     /** To do appending writing. */
     UPDATE_APPENDING = 2,
  };

  /**
   * Tuning parameters for the database.
   */
  struct TuningParameters {
    /**
     * How to update the database file.
     * @details In-place writing means that the data space of a existing records is modified when
     * it is overwritten or removed.  Appending writing means that modification is always done
     * by appending new data at the end of the file.  The default mode is in-place writing.
     */
    UpdateMode update_mode = UPDATE_DEFAULT;
    /**
     * The width to represent the offset of records.
     * @details This determines the maximum size of the database and the footprint.  -1 means
     * that the default value 4 is set.
     */
    int32_t offset_width = -1;
    /**
     * The power to align records.
     * @details This determines the maximum size of the datbase and the unit size allotted to
     * each record.  -1 means that The default value 3 is set.
     */
    int32_t align_pow = -1;
    /**
     * The number of buckets for hashing.
     * @details For good performance, the number of buckets should be larger than the number of
     * records.  -1 means that the default value 1048583 is set.
     */
    int64_t num_buckets = -1;
    /**
     * The capacity of the free block pool.
     * @details The free block pool is for reusing dead space of removed or moved records in
     * the in-place updating mode.  -1 means that the default value 2048 is set.  As this
     * parameter is not saved as a metadata of the database, it should be set each time when
     * opening the database.
     */
    int32_t fbp_capacity = -1;
    /**
     * Whether to lock the memory for the hash buckets.
     * @details If true and the underlying file class supports memory mapping, the memory region
     * of the hash buckets is locked in order not to be swapped out.  As this parameter is not
     * saved as a metadata of the database, it should be set each time when opening the database.
     */
    bool lock_mem_buckets = false;

    /**
     * Constructor
     */
    TuningParameters() {}
  };

  /**
   * Default constructor.
   * @details MemoryMapParallelFile is used to handle the data.
   */
  HashDBM();

  /**
   * Constructor with a file object.
   * @param file The file object to handle the data.  The ownership is taken.
   */
  HashDBM(std::unique_ptr<File> file);

  /**
   * Destructor.
   */
  virtual ~HashDBM();

  /**
   * Copy and assignment are disabled.
   */
  explicit HashDBM(const HashDBM& rhs) = delete;
  HashDBM& operator =(const HashDBM& rhs) = delete;

  /**
   * Opens a database file.
   * @param path A path of the file.
   * @param writable If true, the file is writable.  If false, it is read-only.
   * @param options Bit-sum options for opening the file.
   * @return The result status.
   * @details Precondition: The database is not opened.
   */
  Status Open(const std::string& path, bool writable,
              int32_t options = File::OPEN_DEFAULT) override {
    return OpenAdvanced(path, writable, options);
  }

  /**
   * Opens a database file, in an advanced way.
   * @param path A path of the file.
   * @param writable If true, the file is writable.  If false, it is read-only.
   * @param options Bit-sum options for opening the file.
   * @param tuning_params A structure for tuning parameters.
   * @return The result status.
   * @details Precondition: The database is not opened.
   */
  Status OpenAdvanced(const std::string& path, bool writable,
                      int32_t options = File::OPEN_DEFAULT,
                      const TuningParameters& tuning_params = TuningParameters());

  /**
   * Closes the database file.
   * @return The result status.
   * @details Precondition: The database is opened.
   */
  Status Close() override;

  /**
   * Processes a record with a processor.
   * @param key The key of the record.
   * @param proc The pointer to the processor object.
   * @param writable True if the processor can edit the record.
   * @return The result status.
   * @details Precondition: The database is opened.  The writable parameter should be
   * consistent to the open mode.
   * @details If the specified record exists, the ProcessFull of the processor is called.
   * Otherwise, the ProcessEmpty of the processor is called.
   */
  Status Process(std::string_view key, RecordProcessor* proc, bool writable) override;

  /**
   * Processes each and every record in the database with a processor.
   * @param proc The pointer to the processor object.
   * @param writable True if the processor can edit the record.
   * @return The result status.
   * @details Precondition: The database is opened.  The writable parameter should be
   * consistent to the open mode.
   * @details The ProcessFull of the processor is called repeatedly for each record.  The
   * ProcessEmpty of the processor is called once before the iteration and once after the
   * iteration.
   */
  Status ProcessEach(RecordProcessor* proc, bool writable) override;

  /**
   * Gets the number of records.
   * @param count The pointer to an integer object to contain the result count.
   * @return The result status.
   * @details Precondition: The database is opened.
   */
  Status Count(int64_t* count) override;

  /**
   * Gets the current file size of the database.
   * @param size The pointer to an integer object to contain the result size.
   * @return The result status.
   * @details Precondition: The database is opened.
   */
  Status GetFileSize(int64_t* size) override;

  /**
   * Gets the path of the database file.
   * @param path The pointer to a string object to contain the result path.
   * @return The result status.
   * @details Precondition: The database is opened.
   */
  Status GetFilePath(std::string* path) override;

  /**
   * Removes all records.
   * @return The result status.
   * @details Precondition: The database is opened as writable.
   */
  Status Clear() override;

  /**
   * Rebuilds the entire database.
   * @return The result status.
   * @details Precondition: The database is opened as writable.
   * @details Rebuilding a database is useful to reduce the size of the file by solving
   * fragmentation.  All tuning parameters are succeeded or calculated implicitly.
   */
  Status Rebuild() override {
    return RebuildAdvanced();
  }

  /**
   * Rebuilds the entire database, in an advanced way.
   * @param tuning_params A structure for tuning parameters.  The default value of each
   * parameter means that the current setting is succeeded or calculated implicitly.
   * @param skip_broken_records If true, the operation continues even if there are broken records
   * which can be skipped.
   * @return The result status.
   * @details Precondition: The database is opened as writable.
   * @details Rebuilding a database is useful to reduce the size of the file by solving
   * fragmentation.
   */
  Status RebuildAdvanced(
      const TuningParameters& tuning_params = TuningParameters(),
      bool skip_broken_records = false);

  /**
   * Checks whether the database should be rebuilt.
   * @param tobe The pointer to a boolean object to contain the result decision.
   * @return The result status.
   * @details Precondition: The database is opened.
   */
  Status ShouldBeRebuilt(bool* tobe) override;

  /**
   * Synchronizes the content of the database to the file system.
   * @param hard True to do physical synchronization with the hardware or false to do only
   * logical synchronization with the file system.
   * @param proc The pointer to the file processor object, whose Process method is called while
   * the content of the file is synchronized.  If it is nullptr, it is ignored.
   * @return The result status.
   * @details Precondition: The database is opened as writable.
   */
  Status Synchronize(bool hard, FileProcessor* proc = nullptr) override;

  /**
   * Inspects the database.
   * @return A vector of pairs of a property name and its value.
   */
  std::vector<std::pair<std::string, std::string>> Inspect() override;

  /**
   * Checks whether the database is open.
   * @return True if the database is open, or false if not.
   */
  bool IsOpen() const override;

  /**
   * Checks whether the database is writable.
   * @return True if the database is writable, or false if not.
   */
  bool IsWritable() const override;

  /**
   * Checks whether the database condition is healthy.
   * @return True if the database condition is healthy, or false if not.
   * @details Precondition: The database is opened.
   */
  bool IsHealthy() const override;

  /**
   * Checks whether ordered operations are supported.
   * @return Always false.  Ordered operations are not supported.
   */
  bool IsOrdered() const override {
    return false;
  }

  /**
   * Makes an iterator for each record.
   * @return The iterator for each record.
   * @details Precondition: The database is opened.
   */
  std::unique_ptr<DBM::Iterator> MakeIterator() override;

  /**
   * Makes a new DBM object of the same concrete class.
   * @return The new file object.
   */
  std::unique_ptr<DBM> MakeDBM() const override;

  /**
   * Gets the pointer to the internal file object.
   * @return The pointer to the internal file object.
   * @details Accessing the internal file viorates encapsulation policy.  This should be used
   * only for testing and debugging.
   */
  const File* GetInternalFile() const;

  /**
   * Gets the effective data size.
   * @return The effective data size, or -1 on failure.
   * @details Precondition: The database is opened.
   * @details The effective data size means the total size of the keys and the values.
   */
  int64_t GetEffectiveDataSize();

  /**
   * Gets the last modification time of the database.
   * @return The last modification time of the UNIX epoch, or -1 on failure.
   * @details Precondition: The database is opened.
   */
  double GetModificationTime();

  /**
   * Gets the database type
   * @return The database type, or -1 on failure.
   * @details Precondition: The database is opened.
   */
  int32_t GetDatabaseType();

  /**
   * Sets the database type.
   * @param db_type The database type.
   * @return The result status.
   * @details Precondition: The database is opened as writable.
   * @details This data is just for applications and not used by the database implementation.
   */
  Status SetDatabaseType(uint32_t db_type);

  /**
   * Gets the opaque metadata.
   * @return The opaque metadata, or an empty string on failure.
   * @details Precondition: The database is opened.
   */
  std::string GetOpaqueMetadata();

  /**
   * Sets the opaque metadata.
   * @param opaque The opaque metadata, of which leading 64 bytes are stored in the file.
   * @return The result status.
   * @details Precondition: The database is opened as writable.
   * @details This data is just for applications and not used by the database implementation.
   */
  Status SetOpaqueMetadata(const std::string& opaque);

  /**
   * Gets the number of buckets of the hash table.
   * @return The number of buckets of the hash table, or -1 on failure.
   * @details Precondition: The database is opened.
   */
  int64_t CountBuckets();

  /**
   * Gets the number of used buckets of the hash table.
   * @return The number of used buckets of the hash table, or -1 on failure.
   * @details Precondition: The database is opened.
   */
  int64_t CountUsedBuckets();

  /**
   * Gets the current update mode.
   * @return The current update mode or UPDATE_DEFAULT on failure.
   */
  UpdateMode GetUpdateMode();

  /**
   * Set the update mode appending.
   * @details Precondition: The database is opened as writable.
   * @details Whereas it is allowed to change a database in the in-place mode to the appending
   * mode, the opposite direction is not allowed.
   */
  Status SetUpdateModeAppending();

  /**
   * Imports records from another hash database file, in a forward manner.
   * @param path A path of the other hash database file.
   * @param skip_broken_records If true, the operation continues even if there are broken records
   * which can be skipped.
   * @param record_base The beginning offset of records to read.  Negative means the beginning
   * of the record section.
   * @param end_offset The exclusive end offset of records to read.  Negative means unlimited.
   * 0 means the size when the database is synched or closed properly.
   * @return The result status.
   * @details Precondition: The database is opened.
   */
  Status ImportFromFileForward(
      const std::string& path, bool skip_broken_records,
      int64_t record_base, int64_t end_offset);

  /**
   * Imports records from another hash database file, in a backward manner.
   * @param path A path of the other hash database file.
   * @param skip_broken_records If true, the operation continues even if there are broken records
   * which can be skipped.
   * @param record_base The beginning offset of records to read.  Negative means the beginning
   * of the record section.
   * @param end_offset The exclusive end offset of records to read.  Negative means unlimited.
   * 0 means the size when the database is synched or closed properly.
   * @return The result status.
   * @details Precondition: The database is opened.
   */
  Status ImportFromFileBackward(
      const std::string& path, bool skip_broken_records,
      int64_t record_base, int64_t end_offset);

  /**
   * Finds the record base of a hash database file.
   * @param file A file object having opened the database file.
   * @param record_base The pointer to an integer to store the record base offset.
   * @param offset_width The pointer to an integer to store the offset width.
   * @param align_pow The pointer to an integer to store the alignment power.
   * @param last_sync_size The pointer to an integer to store the file size when the database
   * was synched or closed properly at the last time.  This size is zero, if the metadata is
   * broken.
   * @return The result status.
   */
  static Status FindRecordBase(
      File* file, int64_t *record_base, int32_t* offset_width, int32_t* align_pow,
      int64_t* last_sync_size);

  /**
   * Restores a broken database as a new healthy database.
   * @param old_file_path The path of the broken database.
   * @param new_file_path The path of the new database to be created.
   * @param end_offset The exclusive end offset of records to read.  Negative means unlimited.
   * 0 means the size when the database is synched or closed properly.
   * @return The result status.
   */
  static Status RestoreDatabase(
      const std::string& old_file_path, const std::string& new_file_path, int64_t end_offset);

 private:
  /** Pointer to the actual implementation. */
  class HashDBMImpl* impl_;
};

}  // namespace tkrzw

#endif  // _TKRZW_DBM_HASH_H

// END OF FILE
