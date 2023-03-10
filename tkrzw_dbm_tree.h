/*************************************************************************************************
 * File database manager implementation based on B+ tree
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

#ifndef _TKRZW_DBM_TREE_H
#define _TKRZW_DBM_TREE_H

#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <cinttypes>

#include "tkrzw_dbm.h"
#include "tkrzw_dbm_hash.h"
#include "tkrzw_file.h"
#include "tkrzw_key_comparators.h"
#include "tkrzw_lib_common.h"

namespace tkrzw {

class TreeDBMImpl;
class TreeDBMIteratorImpl;

/**
 * File database manager implementation based on B+ tree.
 * @details All operations are thread-safe; Multiple threads can access the same database
 * concurrently.  Every opened database must be closed explicitly to avoid data corruption.
 */
class TreeDBM final : public DBM {
 public:
  /** The default value of the offset width. */
  static constexpr int32_t DEFAULT_OFFSET_WIDTH = 4;
  /** The default value of the alignment power. */
  static constexpr int32_t DEFAULT_ALIGN_POW = 10;
  /** The default value of the number of buckets. */
  static constexpr int64_t DEFAULT_NUM_BUCKETS = 131101;
  /** The default value of the capacity of the free block pool. */
  static constexpr int32_t DEFAULT_FBP_CAPACITY = 2048;
  /** The default value of the max page size. */
  static constexpr int32_t DEFAULT_MAX_PAGE_SIZE = 8130;
  /** The default value of the max branches. */
  static constexpr int32_t DEFAULT_MAX_BRANCHES = 256;
  /** The default value of the maximum number of cached pages. */
  static constexpr int32_t DEFAULT_MAX_CACHED_PAGES = 10000;
  /** The size of the opaque metadata. */
  static constexpr int32_t OPAQUE_METADATA_SIZE = 10;

  /**
   * Iterator for each record.
   * @details When the database is updated, some iterators may or may not be invalided.
   * Operations with invalidated iterators fails gracefully with NOT_FOUND_ERROR.  One iterator
   * cannot be shared by multiple threads.
   */
  class Iterator final : public DBM::Iterator {
    friend class TreeDBM;
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
     * @details Precondition: The database is opened.
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
    Status JumpLower(std::string_view key, bool inclusive = false) override;

    /**
     * Initializes the iterator to indicate the first record whose key is upper than a given key.
     * @param key The key to compare with.
     * @param inclusive If true, the considtion is inclusive: equal to or upper than the key.
     * @return The result status.
     * @details Precondition: The database is opened.
     * @details Even if there's no matching record, the operation doesn't fail.  If the inclusive
     * parameter is true, this method is the same as Jump(key).
     */
    Status JumpUpper(std::string_view key, bool inclusive = false) override;

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
     * @details Precondition: The database is opened.
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

   private:
    /**
     * Constructor.
     * @param dbm_impl The database implementation object.
     */
    explicit Iterator(TreeDBMImpl* dbm_impl);

    /** Pointer to the actual implementation. */
    TreeDBMIteratorImpl* impl_;
  };

  /**
   * Enumeration for page update modes.
   */
  enum PageUpdateMode : int32_t {
    /** The default behavior: no operation or to succeed the current mode. */
    PAGE_UPDATE_DEFAULT = 0,
    /** To do no operation. */
    PAGE_UPDATE_NONE = 1,
    /** To write immediately. */
    PAGE_UPDATE_WRITE = 2,
  };

  /**
   * Tuning parameters for the database.
   */
  struct TuningParameters : public HashDBM::TuningParameters {
    /**
     * The maximum size of a page.
     * @details If the size of a page exceeds this value, the page is divided into two.  -1 means
     * that the default value 4081 is set.  -1 means that the default value 8130 is set.
     */
    int32_t max_page_size = -1;
    /**
     * The maximum number of branches each inner node can have.
     * @details The more the number of branches is, the less number of inner nodes occur.  If the
     * number is too large, updating performance deteriorates.  -1 means that the default value
     * 256 is set.
     */
    int32_t max_branches = -1;
    /**
     * The maximum number of cached pages.
     * @details The more this value is, the better performance is whereas memory usage is
     * increased.  -1 means that the default value 10000 is set.  As this parameter is not
     * saved as a metadata of the database, it should be set each time when opening the database.
     */
    int32_t max_cached_pages = -1;
    /**
     * What to do when each page is updated.
     * @details By default, updated cached pages are just kept on memory and the updates of each
     * page are not written in the file until the page is cached out or the database is closed.
     * PAGE_UPDATE_WRITE improves durability by ensuring that each update is written immediately.
     */
    PageUpdateMode page_update_mode = PAGE_UPDATE_DEFAULT;
    /**
     * The comparator of record keys.
     * @details Records are put in the ascending order of this function.  If this is one of
     * built-in comparators, the type is stored in the database.  Otherwise, the custom comparator
     * must be specified every time when the database is opened.  nullptr means that the default
     * comparator LexicalKeyComparator is set.
     */
    KeyComparator key_comparator = nullptr;

    /**
     * Constructor
     */
    TuningParameters() {}
  };

  /**
   * Default constructor.
   * @details MemoryMapParallelFile is used to handle the data.
   */
  TreeDBM();

  /**
   * Constructor with a file object.
   * @param file The file object to handle the data.  The ownership is taken.
   */
  explicit TreeDBM(std::unique_ptr<File> file);

  /**
   * Destructor.
   */
  ~TreeDBM();

  /**
   * Copy and assignment are disabled.
   */
  explicit TreeDBM(const TreeDBM& rhs) = delete;
  TreeDBM& operator =(const TreeDBM& rhs) = delete;

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
   * Otherwise, this method fails and no method of the processor is called.  The first record
   * has the lowest key of all.
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
   * Gets the timestamp in seconds of the last modified time.
   * @param timestamp The pointer to a double object to contain the timestamp.
   * @return The result status.
   * @details Precondition: The database is opened.
   * @details The timestamp is updated when the database opened in the writable mode is closed
   * or synchronized, even if no updating opertion is done.
   */
  Status GetTimestamp(double* timestamp) override;

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
   * @param sync_hard True to do physical synchronization with the hardware before finishing the
   * rebuilt file.
   * @return The result status.
   * @details Precondition: The database is opened as writable.
   * @details Rebuilding a database is useful to reduce the size of the file by solving
   * fragmentation.  Tuning parameters for the underlying hash database are reflected on the
   * rebuilt file on the spot.  Tuning parameters for B+ tree are reflected gradually while
   * updating the database later.  The comparator of record keys cannot be changed.
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
   * @details Precondition: The database is not opened.
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
   * @param opaque The opaque metadata, of which leading 16 bytes are stored in the file.
   * @return The result status.
   * @details Precondition: The database is opened as writable.
   * @details This data is just for applications and not used by the database implementation.
   */
  Status SetOpaqueMetadata(const std::string& opaque);

  /**
   * Gets the comparator of record keys.
   * @return The key comparator function, or nullptr on failure.
   * @details Precondition: The database is opened.
   */
  KeyComparator GetKeyComparator() const;

  /**
   * Validate all buckets in the hash table.
   * @return The result status.
   */
  Status ValidateHashBuckets();

  /**
   * Validates records in a region.
   * @param record_base The beginning offset of records to check.  Negative means the beginning
   * of the record section.
   * @param end_offset The exclusive end offset of records to check.  Negative means unlimited.
   * 0 means the size when the database is synched or closed properly.
   * @return The result status.
   */
  Status ValidateRecords(int64_t record_base, int64_t end_offset);

  /**
   * Parses metadata on an opaque data sequence.
   * @param opaque The opaque data from the underlying database.
   * @param num_records The pointer to a variable to store the number of buckets.
   * @param eff_data_size The pointer to a variable to store the effective data size.
   * @param root_id The pointer to a variable to store the ID of the root node.
   * @param first_id The pointer to a variable to store the ID of the first node.
   * @param last_id The pointer to a variable to store the ID of the last node.
   * @param num_leaf_nodes The pointer to a variable to store the number of leaf nodes.
   * @param num_inner_nodes The pointer to a variable to store the number of inner nodes.
   * @param max_page_size The pointer to a variable to store the max page size.
   * @param max_branches The pointer to a variable to store the max branches.
   * @param tree_level The pointer to a variable to store the tree level.
   * @param key_comp_type The pointer to a variable to store the key comparator type.
   * @param mini_opaque The pointer to a variable to store the mini opaque data.
   * @return The result status.
   */
  static Status ParseMetadata(
      std::string_view opaque, int64_t* num_records, int64_t* eff_data_size,
      int64_t* root_id, int64_t* first_id, int64_t* last_id,
      int64_t* num_leaf_nodes, int64_t* num_inner_nodes,
      int32_t* max_page_size, int32_t* max_branches,
      int32_t* tree_level, int32_t* key_comp_type, std::string* mini_opaque);

  /**
   * Restores a broken database as a new healthy database.
   * @param old_file_path The path of the broken database.
   * @param new_file_path The path of the new database to be created.
   * @param end_offset The exclusive end offset of records to read.  Negative means unlimited.
   * 0 means the size when the database is synched or closed properly.  INT64MIN and INT64MAX
   * mean to omit restore of the underlying hash database.  Then, INT64MIN is unlimited and
   * INT64MAX means synched restoration.
   * @param cipher_key The encryption key for cipher compressors.
   * @return The result status.
   */
  static Status RestoreDatabase(
      const std::string& old_file_path, const std::string& new_file_path,
      int64_t end_offset, std::string_view cipher_key = "");

 private:
  /** Pointer to the actual implementation. */
  TreeDBMImpl* impl_;
};

}  // namespace tkrzw

#endif  // _TKRZW_DBM_TREE_H

// END OF FILE
