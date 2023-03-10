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

#include "tkrzw_compress.h"
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
  /** The default value of the minimum reading size to read a record. */
  static constexpr int32_t DEFAULT_MIN_READ_SIZE = 48;
  /** The size of the opaque metadata. */
  static constexpr int32_t OPAQUE_METADATA_SIZE = 64;

  /**
   * Iterator for each record.
   * @details When the database is updated, some iterators may or may not be invalided.
   * Operations with invalidated iterators fails gracefully with NOT_FOUND_ERROR.  One iterator
   * cannot be shared by multiple threads.
   */
  class Iterator final : public DBM::Iterator {
    friend class HashDBM;
   public:
    /**
     * Destructor.
     */
    ~Iterator();

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
  enum UpdateMode : int32_t {
    /** The default behavior: in-place or to succeed the current mode. */
    UPDATE_DEFAULT = 0,
    /** To do in-place writing. */
    UPDATE_IN_PLACE = 1,
    /** To do appending writing. */
    UPDATE_APPENDING = 2,
  };

  /**
   * Enumeration for record CRC modes.
   */
  enum RecordCRCMode : int32_t {
    /** The default behavior: no CRC or to succeed the current mode. */
    RECORD_CRC_DEFAULT = 0,
    /** To add no CRC to each record. */
    RECORD_CRC_NONE = 1,
    /** To add CRC-8 to each record. */
    RECORD_CRC_8 = 2,
    /** To add CRC-16 to each record. */
    RECORD_CRC_16 = 3,
    /** To add CRC-32 to each record. */
    RECORD_CRC_32 = 4,
  };

  /**
   * Enumeration for record compression modes.
   */
  enum RecordCompressionMode : int32_t {
    /** The default behavior: no compression or to succeed the current mode. */
    RECORD_COMP_DEFAULT = 0,
    /** To do no compression. */
    RECORD_COMP_NONE = 1,
    /** To compress with ZLib. */
    RECORD_COMP_ZLIB = 2,
    /** To compress with ZStd. */
    RECORD_COMP_ZSTD = 3,
    /** To compress with LZ4. */
    RECORD_COMP_LZ4 = 4,
    /** To compress with LZMA. */
    RECORD_COMP_LZMA = 5,
    /** To cipher with RC4. */
    RECORD_COMP_RC4 = 6,
    /** To cipher with AES. */
    RECORD_COMP_AES = 7,
  };

  /**
   * Enumeration for restore modes.
   */
  enum RestoreMode : int32_t {
    /** The default behavior: to restore as many records as possible. */
    RESTORE_DEFAULT = 0,
    /** To restore to the last synchronized state. */
    RESTORE_SYNC = 1,
    /** To make the database read-only. */
    RESTORE_READ_ONLY = 2,
    /** To do nothing. */
    RESTORE_NOOP = 3,
    /** Additional bit to not apply shortcuts. */
    RESTORE_NO_SHORTCUTS = 1 << 16,
    /** Additional bit to do physical synchronization. */
    RESTORE_WITH_HARDSYNC = 1 << 17,
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
     * How to add the CRC data to the record.
     * @details The cyclic redundancy check detects corruption of record data.  Although no CRC
     * is added, corruption is detected at a certain probability.  The more width the CRC has,
     * the more likely corruption is detected.
     */
    RecordCRCMode record_crc_mode = RECORD_CRC_DEFAULT;
    /**
     * How to compress the value data of the record.
     * @details Because compression is done for each record, it is effective only if the record
     * value is large.  Because compression is implemented with external libraries, available
     * algorithms are limited to the ones which were enabled when the database librarry was built.
     */
    RecordCompressionMode record_comp_mode = RECORD_COMP_DEFAULT;
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
     * How to restore the broken database file.
     * @details If the database file is not closed properly, when you open the file next time,
     * it is considered broken.  Then, restore operations are done implicitly.  By default,
     * the whole database is scanned to recover as many records as possible.  As this
     * parameter is not saved as a metadata of the database, it should be set each time when
     * opening the database.
     */
    int32_t restore_mode = RESTORE_DEFAULT;
    /**
     * The capacity of the free block pool.
     * @details The free block pool is for reusing dead space of removed or moved records in
     * the in-place updating mode.  -1 means that the default value 2048 is set.  As this
     * parameter is not saved as a metadata of the database, it should be set each time when
     * opening the database.
     */
    int32_t fbp_capacity = -1;
    /**
     * The minimum reading size to read a record.
     * @details When a record is read from the file, data of the specified size is read at once.
     * If the record size including the footprint is larger than the read data size, another
     * reading operation is done.  -1 means that the larger value of the default value 48 and
     * the alignemnt size is set.  As this parameter is not saved as a metadata of the database,
     * it should be set each time when opening the database.
     */
    int32_t min_read_size = -1;
    /**
     * Whether to cache the hash buckets on memory.
     * @details If positive and the underlying file class supports caching, the file region of
     * the hash buckets is cached in order to improve performance.  As this parameter is not
     * saved as a metadata of the database, it should be set each time when opening the database.
     */
    int32_t cache_buckets = -1;
    /**
     * The encryption key for cipher compressors.
     * @details This key is passed as-is as a binary data to the cipher compressor.  The database
     * doesn't check whether the key is valid or not.  Thus, a wrong key produces invalid record
     * values.  As this parameter is not saved as a metadata of the database, it should be set
     * each time when opening the database.
     */
    std::string cipher_key = "";

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
  explicit HashDBM(std::unique_ptr<File> file);

  /**
   * Destructor.
   */
  ~HashDBM();

  /**
   * Copy and assignment are disabled.
   */
  explicit HashDBM(const HashDBM& rhs) = delete;
  HashDBM& operator =(const HashDBM& rhs) = delete;

  /**
   * Opens a database file.
   * @param path A path of the file.
   * @param writable If true, the file is writable.  If false, it is read-only.
   * @param options Bit-sum options of File::OpenOption enums for opening the file.
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
   * Gets the value of a record of a key.
   * @param key The key of the record.
   * @param value The pointer to a string object to contain the result value.  If it is nullptr,
   * the value data is ignored.
   * @return The result status.  If there's no matching record, NOT_FOUND_ERROR is returned.
   * @details Precondition: The database is opened.
   */
  Status Get(std::string_view key, std::string* value = nullptr) override;

  /**
   * Sets a record of a key and a value.
   * @param key The key of the record.
   * @param value The value of the record.
   * @param overwrite Whether to overwrite the existing value if there's a record with the same
   * key.  If true, the existing value is overwritten by the new value.  If false, the operation
   * is given up and an error status is returned.
   * @param old_value The pointer to a string object to contain the old value.  Assignment is done
   * even on the duplication error.  If it is nullptr, it is ignored.
   * @return The result status.  If overwriting is abandoned, DUPLICATION_ERROR is returned.
   * @details Precondition: The database is opened as writable.
   */
  Status Set(std::string_view key, std::string_view value, bool overwrite = true,
             std::string* old_value = nullptr) override;

  /**
   * Removes a record of a key.
   * @param key The key of the record.
   * @param old_value The pointer to a string object to contain the old value.  If it is nullptr,
   * it is ignored.
   * @return The result status.  If there's no matching record, NOT_FOUND_ERROR is returned.
   * @details Precondition: The database is opened as writable.
   */
  Status Remove(std::string_view key, std::string* old_value = nullptr) override;

  /**
   * Processes multiple records with processors.
   * @param key_proc_pairs Pairs of the keys and their processor objects.
   * @param writable True if the processors can edit the records.
   * @return The result status.
   * @details Precondition: The database is opened.  The writable parameter should be
   * consistent to the open mode.
   * @details If the specified record exists, the ProcessFull of the processor is called.
   * Otherwise, the ProcessEmpty of the processor is called.
   */
  Status ProcessMulti(
      const std::vector<std::pair<std::string_view, DBM::RecordProcessor*>>& key_proc_pairs,
      bool writable) override;

  /**
   * Processes the first record with a processor.
   * @param proc The pointer to the processor object.
   * @param writable True if the processor can edit the record.
   * @return The result status.
   * @details Precondition: The database is opened.  The writable parameter should be
   * consistent to the open mode.
   * @details If the first record exists, the ProcessFull of the processor is called.
   * Otherwise, this method fails and no method of the processor is called.  This method can be
   * slow because it can scan all buckets.
   */
  Status ProcessFirst(RecordProcessor* proc, bool writable) override;

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
   * Gets the timestamp in seconds of the last modified time.
   * @param timestamp The pointer to a double object to contain the timestamp.
   * @return The result status.
   * @details Precondition: The database is opened.
   * @details The timestamp is updated when the database opened in the writable mode is closed
   * or synchronized, even if no updating opertion is done.
   */
  Status GetTimestamp(double* timestamp) override;

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
   * @param sync_hard True to do physical synchronization with the hardware before finishing the
   * rebuilt file.
   * @return The result status.
   * @details Precondition: The database is opened as writable.
   * @details Rebuilding a database is useful to reduce the size of the file by solving
   * fragmentation.
   */
  Status RebuildAdvanced(
      const TuningParameters& tuning_params = TuningParameters(),
      bool skip_broken_records = false, bool sync_hard = false);

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
   * Checks whether the database has been restored automatically.
   * @return True if the database condition has been restored by the last Open method.
   * @details Precondition: The database is opened.
   */
  bool IsAutoRestored() const;

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
   * @return The new DBM object.
   */
  std::unique_ptr<DBM> MakeDBM() const override;

  /**
   * Gets the logger to write all update operations.
   * @return The update logger if it has been set or nullptr if it hasn't.
   */
  UpdateLogger* GetUpdateLogger() const override;

  /**
   * Sets the logger to write all update operations.
   * @param update_logger The pointer to the update logger object.  Ownership is not taken.
   * If it is nullptr, no logger is used.
   */
  void SetUpdateLogger(UpdateLogger* update_logger) override;

  /**
   * Gets the pointer to the internal file object.
   * @return The pointer to the internal file object.
   * @details Accessing the internal file viorates encapsulation policy.  This should be used
   * only for testing and debugging.
   */
  File* GetInternalFile() const;

  /**
   * Gets the effective data size.
   * @return The effective data size, or -1 on failure.
   * @details Precondition: The database is opened.
   * @details The effective data size means the total size of the keys and the values.  This
   * figure might deviate if auto restore happens.
   */
  int64_t GetEffectiveDataSize();

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
   * @param file A file object of the other hash database file.
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
      File* file, bool skip_broken_records, int64_t record_base, int64_t end_offset);

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
   * @param file A file object of the other hash database file.
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
      File* file, bool skip_broken_records, int64_t record_base, int64_t end_offset);

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
   * Validate all buckets in the hash table.
   * @return The result status.
   */
  Status ValidateHashBuckets();

  /**
   * Validates records in a region.
   * @param record_base The beginning offset of records to check.  Negative means the beginning
   * of the record section.  0 means the size when the database is synched or closed properly.
   * @param end_offset The exclusive end offset of records to check.  Negative means unlimited.
   * 0 means the size when the database is synched or closed properly.
   * @return The result status.
   */
  Status ValidateRecords(int64_t record_base, int64_t end_offset);

  /**
   * Reads metadata from a database file.
   * @param file A file object having opened the database file.
   * @param cyclic_magic The pointer to a variable to store the cyclic magic data.
   * @param pkg_major_version The pointer to a variable to store the package major version.
   * @param pkg_minor_version The pointer to a variable to store the package minor version.
   * @param static_flags The pointer to a variable to store the static flags.
   * @param offset_width The pointer to a variable to store the offset width.
   * @param align_pow The pointer to a variable to store the alignment power.
   * @param closure_flags The pointer to a variable to store the closure flags.
   * @param num_buckets The pointer to a variable to store the number of buckets.
   * @param num_records The pointer to a variable to store the number of records.
   * @param eff_data_size The pointer to a variable to store the effective data size.
   * @param file_size The pointer to a variable to store the file size.
   * @param mod_time The pointer to a variable to store the last modified time.
   * @param db_type The pointer to a variable to store the database type.
   * @param opaque The pointer to a variable to store the opaque data.
   * @return The result status.
   * @details If the leading magic data is inconsistent, it returns failure.  If the cyclic magic
   * data is inconsistent, it returns success and -1 is assigned to cyclic_magic.
   */
  static Status ReadMetadata(
      File* file, int32_t* cyclic_magic, int32_t* pkg_major_version, int32_t* pkg_minor_version,
      int32_t* static_flags, int32_t* offset_width, int32_t* align_pow,
      int32_t* closure_flags, int64_t* num_buckets, int64_t* num_records,
      int64_t* eff_data_size, int64_t* file_size, int64_t* mod_time,
      int32_t* db_type, std::string* opaque);

  /**
   * Finds the record base of a hash database file.
   * @param file A file object having opened the database file.
   * @param record_base The pointer to an integer to store the record base offset.
   * @param static_flags The pointer to an integer to store the static flags.
   * @param offset_width The pointer to an integer to store the offset width.
   * @param align_pow The pointer to an integer to store the alignment power.
   * @param last_sync_size The pointer to an integer to store the file size when the database
   * was synched or closed properly at the last time.  This size is zero, if the metadata is
   * broken.
   * @return The result status.
   */
  static Status FindRecordBase(
      File* file, int64_t *record_base,
      int32_t* static_flags, int32_t* offset_width, int32_t* align_pow,
      int64_t* last_sync_size);

  /**
   * Gets the record CRC width from the static flags.
   * @param static_flags The static flags.
   * @return The record CRC width.
   */
  static int32_t GetCRCWidthFromStaticFlags(int32_t static_flags);

  /**
   * Makes the compressor from the static flags.
   * @param static_flags The static flags.
   * @param cipher_key The encryption key for cipher compressors.
   * @return The compressor object or nullptr for no compression.
   */
  static std::unique_ptr<Compressor> MakeCompressorFromStaticFlags(
      int32_t static_flags, std::string_view cipher_key = "");

  /**
   * Checks whether a record compression mode is supported on the platform.
   * @param mode The record compression mode.
   * @return True if the record compression mode is supported on the platform.
   */
  static bool CheckRecordCompressionModeIsSupported(RecordCompressionMode mode);

  /**
   * Restores a broken database as a new healthy database.
   * @param old_file_path The path of the broken database.
   * @param new_file_path The path of the new database to be created.
   * @param end_offset The exclusive end offset of records to read.  Negative means unlimited.
   * 0 means the size when the database is synched or closed properly.
   * @param cipher_key The encryption key for cipher compressors.
   * @return The result status.
   */
  static Status RestoreDatabase(
      const std::string& old_file_path, const std::string& new_file_path,
      int64_t end_offset, std::string_view cipher_key = "");

 private:
  /** Pointer to the actual implementation. */
  HashDBMImpl* impl_;
};

}  // namespace tkrzw

#endif  // _TKRZW_DBM_HASH_H

// END OF FILE
