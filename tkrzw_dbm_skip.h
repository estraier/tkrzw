/*************************************************************************************************
 * File database manager implementation based on skip list
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

#ifndef _TKRZW_DBM_SKIP_H
#define _TKRZW_DBM_SKIP_H

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

class SkipDBMImpl;
class SkipDBMIteratorImpl;

/**
 * File database manager implementation based on skip list.
 * @details All operations are thread-safe; Multiple threads can access the same database
 * concurrently.  Every opened database must be closed explicitly to avoid data corruption.
 */
class SkipDBM final : public DBM {
 public:
  /** The default value of the offset width. */
  static constexpr int32_t DEFAULT_OFFSET_WIDTH = 4;
  /** The default value of the step unit. */
  static constexpr int32_t DEFAULT_STEP_UNIT = 4;
  /** The default value of the maximum level. */
  static constexpr int32_t DEFAULT_MAX_LEVEL = 14;
  /** The default value of the memory size used for sorting. */
  static constexpr int64_t DEFAULT_SORT_MEM_SIZE = 256LL << 20;
  /** The default value of the maximum cached records. */
  static constexpr int32_t DEFAULT_MAX_CACHED_RECORDS = 65536;
  /** The size of the opaque metadata. */
  static constexpr int32_t OPAQUE_METADATA_SIZE = 64;
  /** The special removing value. */
  static const std::string REMOVING_VALUE;

  /**
   * Iterator for each record.
   * @details When the database is updated, some iterators may or may not be invalided.
   * Operations with invalidated iterators fails gracefully with NOT_FOUND_ERROR.  One iterator
   * cannot be shared by multiple threads.
   */
  class Iterator final : public DBM::Iterator {
    friend class tkrzw::SkipDBM;
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
     * @details Even if there's no record, the operation doesn't fail.
     */
    Status Last() override;

    /**
     * Initializes the iterator to indicate a specific record.
     * @param key The key of the record to look for.
     * @return The result status.
     * @details Precondition: The database is opened.
     * @details This database is ordered so it supports "lower bound" jump.  If there's no record
     * with the same key, the iterator refers to the first record whose key is greater than the
     * given key.
     */
    Status Jump(std::string_view key) override;

    /**
     * Initializes the iterator to indicate the last record whose key is lower than a given key.
     * @param key The key to compare with.
     * @param inclusive If true, the considtion is inclusive: equal to or lower than the key.
     * @return The result status.
     * @details Precondition: The database is opened.
     * @details Even if there's no matching record, the operation doesn't fail.
     */
    Status JumpLower(std::string_view key, bool inclusive = false);

    /**
     * Initializes the iterator to indicate the first record whose key is upper than a given key.
     * @param key The key to compare with.
     * @param inclusive If true, the considtion is inclusive: equal to or upper than the key.
     * @return The result status.
     * @details Precondition: The database is opened.
     * @details Even if there's no matching record, the operation doesn't fail.  If the inclusive
     * parameter is true, this method is the same as Jump(key).
     */
    Status JumpUpper(std::string_view key, bool inclusive = false);

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
     * @details If the current record is missing, the operation fails.  Even if there's no previous
     * previous record, the operation doesn't fail.
     */
    Status Previous() override;

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

    /**
     * Gets the key and the value of the current record of the iterator.
     * @param key The pointer to a string object to contain the record key.  If it is nullptr,
     * the key data is ignored.
     * @param value The pointer to a string object to contain the record value.  If it is nullptr,
     * the value data is ignored.
     * @return The result status.
     */
    Status Get(std::string* key = nullptr, std::string* value = nullptr) override;

   private:
    /**
     * Constructor.
     * @param dbm_impl The database implementation object.
     */
    explicit Iterator(SkipDBMImpl* dbm_impl);

    /** Pointer to the actual implementation. */
    SkipDBMIteratorImpl* impl_;
  };

  /**
   * Interface of a function to reduce records of the same key in the at-random build mode.
   * @details The first parameter is the key.  The second parameter is a vecgtor of record
   * values.  The return value is a vector of new values which are stored in the database.
   */
  typedef std::vector<std::string> (*ReducerType)(
      const std::string&, const std::vector<std::string>&);

  /**
   * Tuning parameters for the database.
   */
  struct TuningParameters {
    /**
     * The width to represent the offset of records.
     * @details This determines the maximum size of the database and the footprint.  -1 means
     * that the default value 4 is set.
     */
    int32_t offset_width = -1;
    /**
     * The step unit of the skip list.
     * @details This determines the search speed and the footprint of records.  -1 means that
     * the default value 4 is set.
     */
    int32_t step_unit = -1;
    /**
     * The maximum level of the skip list.
     * @details This determines the search speed and the footprint of records.  -1 means that
     * the default value 14 is set.
     */
    int32_t max_level = -1;
    /**
     * The memory size used for sorting to build the database in the at-random mode.
     * @details When total size of records exceeds this value, records are written in a temporary
     * file and they are merged later.  -1 means that the default value 256MB is set.  As this
     * parameter is not saved as a metadata of the database, it should be set each time when
     * opening the database.
     */
    int64_t sort_mem_size = -1;
    /**
     * If true, records are assumed to be inserted in ascending order of the key.
     * @details This assumption makes the insertions effective.  As this parameter is not saved
     * as a metadata of the database, it should be set each time when opening the database.
     */
    bool insert_in_order = false;
    /**
     * The maximum number of cached records.
     * @details The more this value is, the better performance is whereas memory usage is
     * increased.  -1 means that the default value 65536 is set.  As this parameter is not
     * saved as a metadata of the database, it should be set each time when opening the database.
     */
    int32_t max_cached_records = -1;

    /**
     * Constructor
     */
    TuningParameters() {}
  };

  /**
   * Default constructor.
   * @details MemoryMapParallelFile is used to handle the data.
   */
  SkipDBM();

  /**
   * Constructor with a file object.
   * @param file The file object to handle the data.  The ownership is taken.
   */
  SkipDBM(std::unique_ptr<File> file);

  /**
   * Destructor.
   */
  virtual ~SkipDBM();

  /**
   * Copy and assignment are disabled.
   */
  explicit SkipDBM(const SkipDBM& rhs) = delete;
  SkipDBM& operator =(const SkipDBM& rhs) = delete;

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
   * Otherwise, the ProcessEmpty of the processor is called.  Inserted records are not visible
   * until the database is synchronized.  The database allows duplication of keys.  Duplication
   * can be solved by the reducer applied when the database is synchronized.  Removing records is
   * implemented as inserting a special value REMOVING_VALUE, which is handled when the
   * database is synchronized.
   */
  Status Process(std::string_view key, RecordProcessor* proc, bool writable) override;

  /**
   * Sets a record of a key and a value.
   * @param key The key of the record.
   * @param value The value of the record.
   * @param overwrite Whether to overwrite the existing value if there's a record with the same
   * key.  If true, the existing value is ovewritten by the new value.  If false, the operation
   * is given up and an error status is returned.
   * @return The result status.
   * @details Precondition: The database is opened as writable.
   */
  Status Set(std::string_view key, std::string_view value, bool overwrite = true) override;

  /**
   * Removes a record of a key.
   * @param key The key of the record.
   * @return The result status.
   * @details Even if there's no matching record, this doesn't report failure.
   * @details Precondition: The database is opened as writable.
   */
  Status Remove(std::string_view key) override;

  /**
   * Gets the key and the value of the record of an index.
   * @param index The index of the record to retrieve.
   * @param key The pointer to a string object to contain the record key.  If it is nullptr,
   * the key data is ignored.
   * @param value The pointer to a string object to contain the record value.  If it is nullptr,
   * the value data is ignored.
   * @return The result status.
   */
  Status GetByIndex(int64_t index, std::string* key = nullptr, std::string* value = nullptr);

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
   * @details Rebuilding a database is useful to optimize the size of the file.
   * All tuning parameters are succeeded or calculated implicitly.
   */
  Status Rebuild() override {
    return RebuildAdvanced();
  }

  /**
   * Rebuilds the entire database, in an advanced way.
   * @param tuning_params A structure for tuning parameters.  The default value of each
   * parameter means that the current setting is succeeded or calculated implicitly.
   * @return The result status.
   * @details Precondition: The database is opened as writable.
   * @details Rebuilding a database is useful to optimize the size of the file.
   */
  Status RebuildAdvanced(
      const TuningParameters& tuning_params = TuningParameters());

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
   * @details All inserted records are written in the database file and they become visible.
   * This operation rebuilds the entire database by merging the existing records and inserted
   * records.  No reducer is applied to records so that all records with duplicated keys are
   * kept intact.
   */
  Status Synchronize(bool hard, FileProcessor* proc = nullptr) override {
    return SynchronizeAdvanced(hard, proc, nullptr);
  }

  /**
   * Synchronizes the content of the database to the file system.
   * @param hard True to do physical synchronization with the hardware or false to do only
   * logical synchronization with the file system.
   * @param proc The pointer to the file processor object, whose Process method is called while
   * the content of the file is synchronized.  If it is nullptr, it is ignored.
   * @return The result status.
   * @param reducer A reducer applied before stroing records.  If it is nullptr, no reducer is
   * applied and all records are kept intact.
   * @return The result status.
   * @details Precondition: The database is opened as writable.
   * @details All inserted records are written in the database file and they become visible.
   * This operation rebuilds the entire database by merging the existing records and inserted
   * records.  Records of the same key given to the reducer are ordered in a way that the old
   * value comes earlier than the new value.  Thus, if you want to keep the old value,
   * ReduceToFirst should be set.  If you want to replace the old value with the new value,
   * ReduceToLast should be set.
   */
  Status SynchronizeAdvanced(bool hard, FileProcessor* proc = nullptr,
                             ReducerType reducer = nullptr);

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
   * @return Always true.  Ordered operations are supported.
   */
  bool IsOrdered() const override {
    return true;
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
   * Reverts all updates.
   * @details Precondition: The database is opened as writable and not finished.
   * @details All inserted records are discarded and the contents are returned to the state when
   * the database was synchronized at the last time.
   */
  Status Revert();

  /**
   * Checks whether the database has been updated after being opened.
   * @return True if the database has been updated after being opened.
   * @details Precondition: The database is opened.
   */
  bool IsUpdated();

  /**
   * Merges the contents of another skip database file.
   * @param src_path A path to the source database file.
   * @return The result status.
   * @details Precondition: The database is opened as writable.
   * @details Merged records are shown after the database is synchronized.  Among records of the
   * same key, synchronized existing records come first, added records come second, and
   * unsynchronized existing records come last.
   */
  Status MergeSkipDatabase(const std::string& src_path);

  /**
   * Reduces the values of records of the same key by removing REMOVING_VALUE and past values.
   * @param key The common key of the records.
   * @param values The values of the records of the same key.
   * @return A vector containing surviving values.
   */
  static std::vector<std::string> ReduceRemove(
      const std::string& key, const std::vector<std::string>& values);

  /**
   * Reduces the values of records of the same key by keeping the first value only.
   * @param key The common key of the records.
   * @param values The values of the records of the same key.
   * @return A vector containing the first value.
   */
  static std::vector<std::string> ReduceToFirst(
      const std::string& key, const std::vector<std::string>& values);

  /**
   * Reduces the values of records of the same key by keeping the second or first value only.
   * @param key The common key of the records.
   * @param values The values of the records of the same key.
   * @return A vector containing the first value.
   */
  static std::vector<std::string> ReduceToSecond(
      const std::string& key, const std::vector<std::string>& values);

  /**
   * Reduces the values of records of the same key by keeping the last value only.
   * @param key The common key of the records.
   * @param values The values of the records of the same key.
   * @return A vector containing the last value.
   */
  static std::vector<std::string> ReduceToLast(
      const std::string& key, const std::vector<std::string>& values);

  /**
   * Reduces the values of records of the same key by concatenating all values.
   * @param key The common key of the records.
   * @param values The values of the records of the same key.
   * @return A vectro containing the concatenated values.
   */
  static std::vector<std::string> ReduceConcat(
      const std::string& key, const std::vector<std::string>& values);

  /**
   * Reduces the values of records of the same key by concatenating all values with null code.
   * @param key The common key of the records.
   * @param values The values of the records of the same key.
   * @return A vectro containing the concatenated values.
   */
  static std::vector<std::string> ReduceConcatWithNull(
      const std::string& key, const std::vector<std::string>& values);

  /**
   * Reduces the values of records of the same key by concatenating all values with tab.
   * @param key The common key of the records.
   * @param values The values of the records of the same key.
   * @return A vectro containing the concatenated values.
   */
  static std::vector<std::string> ReduceConcatWithTab(
      const std::string& key, const std::vector<std::string>& values);

  /**
   * Reduces the values of records of the same key by concatenating all values with linefeed.
   * @param key The common key of the records.
   * @param values The values of the records of the same key.
   * @return A vectro containing the concatenated values.
   */
  static std::vector<std::string> ReduceConcatWithLine(
      const std::string& key, const std::vector<std::string>& values);

  /**
   * Reduces the values of records of the same key by totaling numeric expressions.
   * @param key The common key of the records.
   * @param values The values of the records of the same key.
   * @return A vector containing the total numeric expression.
   */
  static std::vector<std::string> ReduceToTotal(
      const std::string& key, const std::vector<std::string>& values);

  /**
   * Restores a broken database as a new healthy database.
   * @param old_file_path The path of the broken database.
   * @param new_file_path The path of the new database to be created.
   * @return The result status.
   */
  static Status RestoreDatabase(
      const std::string& old_file_path, const std::string& new_file_path);

 private:
  /** Pointer to the actual implementation. */
  class SkipDBMImpl* impl_;
};

}  // namespace tkrzw

#endif  // _TKRZW_DBM_SKIP_H

// END OF FILE
