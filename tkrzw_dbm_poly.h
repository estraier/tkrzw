/*************************************************************************************************
 * Polymorphic database manager adapter
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

#ifndef _TKRZW_DBM_POLY_H
#define _TKRZW_DBM_POLY_H

#include <map>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <cinttypes>

#include "tkrzw_dbm.h"
#include "tkrzw_file.h"
#include "tkrzw_lib_common.h"
#include "tkrzw_message_queue.h"
#include "tkrzw_str_util.h"

namespace tkrzw {

/**
 * Parametric database manager interface.
 * @details This is commonly used by the PolyDBM and ShardDBM.
 */
class ParamDBM : public DBM {
 public:
  /**
   * Destructor.
   */
  virtual ~ParamDBM() = default;

  /**
   * Opens a database file, in an advanced way.
   * @param path A path of the file.
   * @param writable If true, the file is writable.  If false, it is read-only.
   * @param options Bit-sum options for opening the file.
   * @param params Optional parameters.
   * @return The result status.
   */
  virtual Status OpenAdvanced(const std::string& path, bool writable,
                              int32_t options = File::OPEN_DEFAULT,
                              const std::map<std::string, std::string>& params = {}) = 0;

  /**
   * Rebuilds the entire database, in an advanced way.
   * @param params Optional parameters.
   * @return The result status.
   */
  virtual Status RebuildAdvanced(const std::map<std::string, std::string>& params = {}) = 0;

  /**
   * Synchronizes the content of the database to the file system, in an advanced way.
   * @param hard True to do physical synchronization with the hardware or false to do only
   * logical synchronization with the file system.
   * @param proc The pointer to the file processor object, whose Process method is called while
   * the content of the file is synchronized.  If it is nullptr, it is ignored.
   * @param params Optional parameters.
   * @return The result status.
   */
  virtual Status SynchronizeAdvanced(bool hard, FileProcessor* proc = nullptr,
                                     const std::map<std::string, std::string>& params = {}) = 0;
};

/**
 * Polymorphic database manager adapter.
 * @details All operations except for Open and Close are thread-safe; Multiple threads can
 * access the same database concurrently.  Every opened database must be closed explicitly to
 * avoid data corruption.
 * @details This class is a wrapper of HashDBM, TreeDBM, SkipDBM, TinyDBM, BabyDBM, StdHashDBM,
 * and StdTreeDBM.  The open method specifies the actuall class used internally.
 */
class PolyDBM final : public ParamDBM {
 public:
  /**
   * Iterator for each record.
   * @details When the database is updated, some iterators may or may not be invalided.
   * Operations with invalidated iterators fails gracefully with NOT_FOUND_ERROR.  One iterator
   * cannot be shared by multiple threads.
   */
  class Iterator final : public DBM::Iterator {
    friend class PolyDBM;
   public:
    /**
     * Destructor.
     */
    virtual ~Iterator() = default;

    /**
     * Copy and assignment are disabled.
     */
    explicit Iterator(const Iterator& rhs) = delete;
    Iterator& operator =(const Iterator& rhs) = delete;

    /**
     * Initializes the iterator to indicate the first record.
     * @return The result status.
     * @details Even if there's no record, the operation doesn't fail.
     */
    Status First() override {
      return iter_->First();
    }

    /**
     * Initializes the iterator to indicate the last record.
     * @return The result status.
     * @details Even if there's no record, the operation doesn't fail.  This method is suppoerted
     * only by ordered databases.
     */
    Status Last() override {
      return iter_->Last();
    }

    /**
     * Initializes the iterator to indicate a specific record.
     * @param key The key of the record to look for.
     * @return The result status.
     * @details Ordered databases can support "lower bound" jump; If there's no record with the
     * same key, the iterator refers to the first record whose key is greater than the given key.
     * The operation fails with unordered databases if there's no record with the same key.
     */
    Status Jump(std::string_view key) override {
      return iter_->Jump(key);
    }

    /**
     * Initializes the iterator to indicate the last record whose key is lower than a given key.
     * @param key The key to compare with.
     * @param inclusive If true, the considtion is inclusive: equal to or lower than the key.
     * @return The result status.
     * @details Even if there's no matching record, the operation doesn't fail.  This method is
     * suppoerted only by ordered databases.
     */
    Status JumpLower(std::string_view key, bool inclusive = false) override {
      return iter_->JumpLower(key, inclusive);
    }

    /**
     * Initializes the iterator to indicate the first record whose key is upper than a given key.
     * @param key The key to compare with.
     * @param inclusive If true, the considtion is inclusive: equal to or upper than the key.
     * @return The result status.
     * @details Even if there's no matching record, the operation doesn't fail.  This method is
     * suppoerted only by ordered databases.
     */
    Status JumpUpper(std::string_view key, bool inclusive = false) override {
      return iter_->JumpUpper(key, inclusive);
    }

    /**
     * Moves the iterator to the next record.
     * @return The result status.
     * @details If the current record is missing, the operation fails.  Even if there's no next
     * record, the operation doesn't fail.
     */
    Status Next() override {
      return iter_->Next();
    }

    /**
     * Moves the iterator to the previous record.
     * @return The result status.
     * @details If the current record is missing, the operation fails.  Even if there's no previous
     * record, the operation doesn't fail.  This method is suppoerted only by ordered databases.
     */
    Status Previous() override {
      return iter_->Previous();
    }

    /**
     * Processes the current record with a processor.
     * @param proc The pointer to the processor object.
     * @param writable True if the processor can edit the record.
     * @return The result status.
     * @details If the current record exists, the ProcessFull of the processor is called.
     * Otherwise, this method fails and no method of the processor is called.  If the current
     * record is removed, the iterator is moved to the next record.
     */
    Status Process(RecordProcessor* proc, bool writable) override {
      return iter_->Process(proc, writable);
    }

    /**
     * Gets the key and the value of the current record of the iterator.
     * @param key The pointer to a string object to contain the record key.  If it is nullptr,
     * the key data is ignored.
     * @param value The pointer to a string object to contain the record value.  If it is nullptr,
     * the value data is ignored.
     * @return The result status.
     */
    Status Get(std::string* key = nullptr, std::string* value = nullptr) override {
      return iter_->Get(key, value);
    }

    /**
     * Sets the value of the current record.
     * @param value The value of the record.
     * @param old_key The pointer to a string object to contain the old key.  If it is
     * nullptr, it is ignored.
     * @param old_value The pointer to a string object to contain the old value.  If it is
     * nullptr, it is ignored.
     * @return The result status.
     */
    Status Set(std::string_view value, std::string* old_key = nullptr,
               std::string* old_value = nullptr) override {
      return iter_->Set(value, old_key, old_value);
    }

    /**
     * Removes the current record.
     * @param old_key The pointer to a string object to contain the old key.  If it is
     * nullptr, it is ignored.
     * @param old_value The pointer to a string object to contain the old value.  If it is
     * nullptr, it is ignored.
     * @return The result status.
     * @details If possible, the iterator moves to the next record.
     */
    Status Remove(std::string* old_key = nullptr, std::string* old_value = nullptr) override {
      return iter_->Remove(old_key, old_value);
    }

   private:
    explicit Iterator(std::unique_ptr<DBM::Iterator> iter) : iter_(std::move(iter)) {}

    /** The internal iterator object. */
    std::unique_ptr<DBM::Iterator> iter_;
  };

  /**
   * Default constructor.
   */
  PolyDBM();

  /**
   * Destructor.
   */
  virtual ~PolyDBM();

  /**
   * Copy and assignment are disabled.
   */
  explicit PolyDBM(const PolyDBM& rhs) = delete;
  PolyDBM& operator =(const PolyDBM& rhs) = delete;

  /**
   * Opens a database file.
   * @param path A path of the file.
   * @param writable If true, the file is writable.  If false, it is read-only.
   * @param options Bit-sum options of File::OpenOption enums for opening the file.
   * @return The result status.
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
   * @param params Optional parameters.
   * @return The result status.
   * @details The extension of the path indicates the type of the database.
   *   - .tkh : File hash database (HashDBM)
   *   - .tkt : File tree database (TreeDBM)
   *   - .tks : File skip database (SkipDBM)
   *   - .tkmt : On-memory hash database (TinyDBM)
   *   - .tkmb : On-memory tree database (BabyDBM)
   *   - .tkmc : On-memory LRU cache database (CacheDBM)
   *   - .tksh : On-memory STL hash database (StdHashDBM)
   *   - .tkst : On-memory STL tree database (StdTreeDBM)
   * @details The optional parameter "dbm" supercedes the decision of the database type by the
   * extension.  The value is the type name: "HashDBM", "TreeDBM", "SkipDBM", "TinyDBM",
   * "BabyDBM", "CacheDBM", "StdHashDBM", "StdTreeDBM".
   * @details The optional parameter "file" specifies the internal file implementation class.
   * The default file class is "MemoryMapAtomicFile".  The other supported classes are
   * "StdFile", "MemoryMapAtomicFile", "PositionalParallelFile", and "PositionalAtomicFile".
   * @details For HashDBM, these optional parameters are supported.
   *   - update_mode (string): How to update the database file: "UPDATE_IN_PLACE" for the
   *     in-palce or "UPDATE_APPENDING" for the appending mode.
   *   - record_crc_mode (string): How to add the CRC data to the record: "RECORD_CRC_NONE"
   *     to add no CRC to each record, "RECORD_CRC_8" to add CRC-8 to each record, "RECORD_CRC_16"
   *     to add CRC-16 to each record, or "RECORD_CRC_32" to add CRC-32 to each record.
   *   - record_comp_mode (string): How to compress the record data: "RECORD_COMP_NONE" to
   *     do no compression, "RECORD_COMP_ZLIB" to compress with ZLib, "RECORD_COMP_ZSTD" to
   *     compress with ZStd, "RECORD_COMP_LZ4" to compress with LZ4, "RECORD_COMP_LZMA" to
   *     compress with LZMA, "RECORD_COMP_RC4" to cipher with RC4, "RECORD_COMP_AES" to cipher
   *     with AES.
   *   - offset_width (int): The width to represent the offset of records.
   *   - align_pow (int): The power to align records.
   *   - num_buckets (int): The number of buckets for hashing.
   *   - restore_mode (string): How to restore the database file: "RESTORE_SYNC" to restore to
   *     the last synchronized state, "RESTORE_READ_ONLY" to make the database read-only, or
   *     "RESTORE_NOOP" to do nothing.  By default, as many records as possible are restored.
   *     Appending ":RESTORE_NO_SHORTCUTS" is to not apply shortcuts.  Appending
   *     ":RESTORE_WITH_HARDSYNC" is do physical synchronization.
   *   - fbp_capacity (int): The capacity of the free block pool.
   *   - min_read_size (int): The minimum reading size to read a record.
   *   - cache_buckets (bool): True to cache the hash buckets on memory.
   *   - cipher_key (string): The encryption key for cipher compressors.
   * @details For TreeDBM, all optional parameters for HashDBM are available.  In addition,
   * these optional parameters are supported.
   *   - max_page_size (int): The maximum size of a page.
   *   - max_branches (int): The maximum number of branches each inner node can have.
   *   - max_cached_pages (int): The maximum number of cached pages.
   *   - page_update_mode (string): What to do when each page is updated: "PAGE_UPDATE_NONE" is
   *     to do no operation or "PAGE_UPDATE_WRITE" is to write immediately.
   *   - key_comparator (string): The comparator of record keys: "LexicalKeyComparator" for the
   *     lexical order, "LexicalCaseKeyComparator" for the lexical order ignoring case,
   *     "DecimalKeyComparator" for the order of the decimal integer numeric expressions,
   *     "HexadecimalKeyComparator" for the order of the hexadecimal integer numeric expressions,
   *     "RealNumberKeyComparator" for the order of the decimal real number expressions.
   * @details For SkipDBM, these optional parameters are supported.
   *   - offset_width (int): The width to represent the offset of records.
   *   - step_unit (int): The step unit of the skip list.
   *   - max_level (int): The maximum level of the skip list.
   *   - restore_mode (string): How to restore the database file: "RESTORE_SYNC" to restore to
   *     the last synchronized state, "RESTORE_READ_ONLY" to make the database read-only, or
   *     "RESTORE_NOOP" to do nothing.  By default, as many records as possible are restored.
   *     Appending ":RESTORE_NO_SHORTCUTS" is to not apply shortcuts.  Appending
   *     ":RESTORE_WITH_HARDSYNC" is do physical synchronization.
   *   - sort_mem_size (int): The memory size used for sorting to build the database in the
   *     at-random mode.
   *   - insert_in_order (bool): If true, records are assumed to be inserted in ascending
   *     order of the key.
   *   - max_cached_records (int): The maximum number of cached records.
   * @details For TinyDBM, these optional parameters are supported.
   *   - num_buckets (int): The number of buckets for hashing.
   * @details For BabyDBM, these optional parameters are supported.
   *   - key_comparator (string): The comparator of record keys. The same ones as TreeDBM.
   * @details For CacheDBM, these optional parameters are supported.
   *   - cap_rec_num (int): The maximum number of records.
   *   - cap_mem_size (int): The total memory size to use.
   * @details All databases support taking update logs into files.  It is enabled by setting the
   * prefix of update log files.
   *   - ulog_prefix (str): The prefix of the update log files.
   *   - ulog_max_file_size (num): The maximum file size of each update log file.  By default,
   *     it is 1GiB.
   *   - ulog_server_id (num): The server ID attached to each log.  By default, it is 0.
   *   - ulog_dbm_index (num): The DBM index attached to each log.  By default, it is 0.
   * @details For the file "PositionalParallelFile" and "PositionalAtomicFile", these optional
   * parameters are supported.
   *   - block_size (int): The block size to which all blocks should be aligned.
   *   - access_options (str): Values separated by colon.  "direct" for direct I/O.  "sync" for
   *     synchrnizing I/O, "padding" for file size alignment by padding, "pagecache" for the mini
   *     page cache in the process.
   */
  Status OpenAdvanced(const std::string& path, bool writable,
                      int32_t options = File::OPEN_DEFAULT,
                      const std::map<std::string, std::string>& params = {}) override;

  /**
   * Closes the database file.
   * @return The result status.
   */
  Status Close() override;

  /**
   * Processes a record with a processor.
   * @param key The key of the record.
   * @param proc The pointer to the processor object.
   * @param writable True if the processor can edit the record.
   * @return The result status.
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
   */
  Status Set(std::string_view key, std::string_view value, bool overwrite = true,
             std::string* old_value = nullptr) override;

  /**
   * Removes a record of a key.
   * @param key The key of the record.
   * @param old_value The pointer to a string object to contain the old value.  If it is nullptr,
   * it is ignored.
   * @return The result status.  If there's no matching record, NOT_FOUND_ERROR is returned.
   */
  Status Remove(std::string_view key, std::string* old_value = nullptr) override;

  /**
   * Appends data at the end of a record of a key.
   * @param key The key of the record.
   * @param value The value to append.
   * @param delim The delimiter to put after the existing record.
   * @return The result status.
   * @details If there's no existing record, the value is set without the delimiter.
   */
  Status Append(
      std::string_view key, std::string_view value, std::string_view delim = "") override;

  /**
   * Processes the first record with a processor.
   * @param proc The pointer to the processor object.
   * @param writable True if the processor can edit the record.
   * @return The result status.
   * @details If the first record exists, the ProcessFull of the processor is called.
   * Otherwise, this method fails and no method of the processor is called.  Whereas ordered
   * databases have efficient implementations of this method, unordered databases have
   * inefficient implementations.
   */
  Status ProcessFirst(RecordProcessor* proc, bool writable) override;

  /**
   * Processes multiple records with processors.
   * @param key_proc_pairs Pairs of the keys and their processor objects.
   * @param writable True if the processors can edit the records.
   * @return The result status.
   * @details If the specified record exists, the ProcessFull of the processor is called.
   * Otherwise, the ProcessEmpty of the processor is called.
   */
  Status ProcessMulti(
      const std::vector<std::pair<std::string_view, DBM::RecordProcessor*>>& key_proc_pairs,
      bool writable) override;

  /**
   * Processes each and every record in the database with a processor.
   * @param proc The pointer to the processor object.
   * @param writable True if the processor can edit the record.
   * @return The result status.
   * @details The ProcessFull of the processor is called repeatedly for each record.  The
   * ProcessEmpty of the processor is called once before the iteration and once after the
   * iteration.
   */
  Status ProcessEach(RecordProcessor* proc, bool writable) override;

  /**
   * Gets the number of records.
   * @param count The pointer to an integer object to contain the result count.
   * @return The result status.
   */
  Status Count(int64_t* count) override;

  /**
   * Gets the current file size of the database.
   * @param size The pointer to an integer object to contain the result size.
   * @return The result status.
   */
  Status GetFileSize(int64_t* size) override;

  /**
   * Gets the path of the database file.
   * @param path The pointer to a string object to contain the result path.
   * @return The result status.
   */
  Status GetFilePath(std::string* path) override;

  /**
   * Gets the timestamp in seconds of the last modified time.
   * @param timestamp The pointer to a double object to contain the timestamp.
   * @return The result status.
   */
  Status GetTimestamp(double* timestamp) override;

  /**
   * Removes all records.
   * @return The result status.
   */
  Status Clear() override;

  /**
   * Rebuilds the entire database.
   * @return The result status.
   */
  Status Rebuild() override {
    return RebuildAdvanced();
  }

  /**
   * Rebuilds the entire database, in an advanced way.
   * @param params Optional parameters.
   * @return The result status.
   * @details Tuning options can be given by the optional parameters, as with the Open method.
   * A unset parameter means that the current setting is succeeded or calculated implicitly.
   * In addition, HashDBM, TreeDBM, and SkipDBM supports the following parameters.
   *   - skip_broken_records (bool): If true, the operation continues even if there are broken
   *     records which can be skipped.
   *   - sync_hard (bool): If true, physical synchronization with the hardware is done before
   *     finishing the rebuilt file.
   */
  Status RebuildAdvanced(const std::map<std::string, std::string>& params = {}) override;

  /**
   * Checks whether the database should be rebuilt.
   * @param tobe The pointer to a boolean object to contain the result decision.
   * @return The result status.
   */
  Status ShouldBeRebuilt(bool* tobe) override;

  /**
   * Synchronizes the content of the database to the file system.
   * @param hard True to do physical synchronization with the hardware or false to do only
   * logical synchronization with the file system.
   * @param proc The pointer to the file processor object, whose Process method is called while
   * the content of the file is synchronized.  If it is nullptr, it is ignored.
   * @return The result status.
   */
  Status Synchronize(bool hard, FileProcessor* proc = nullptr) override {
    return SynchronizeAdvanced(hard, proc);
  }

  /**
   * Synchronizes the content of the database to the file system, in an advanced way.
   * @param hard True to do physical synchronization with the hardware or false to do only
   * logical synchronization with the file system.
   * @param proc The pointer to the file processor object, whose Process method is called while
   * the content of the file is synchronized.  If it is nullptr, it is ignored.
   * @param params Optional parameters.
   * @return The result status.
   * @details Only SkipDBM uses the optional parameters.  The "merge" parameter specifies paths
   * of databases to merge, separated by colon.  The "reducer" parameter specifies the reducer
   * to apply to records of the same key.  "ReduceToFirst", "ReduceToSecond", "ReduceToLast",
   * etc are supported.
   */
  Status SynchronizeAdvanced(bool hard, FileProcessor* proc = nullptr,
                             const std::map<std::string, std::string>& params = {}) override;

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
   */
  bool IsHealthy() const override;

  /**
   * Checks whether ordered operations are supported.
   * @return True if ordered operations are supported, or false if not.
   */
  bool IsOrdered() const override;

  /**
   * Makes an iterator for each record.
   * @return The iterator for each record.
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
   * Gets the pointer to the internal database object.
   * @return The pointer to the internal database object, or nullptr on failure.
   */
  DBM* GetInternalDBM() const;

  /**
   * Restores a broken database as a new healthy database.
   * @param old_file_path The path of the broken database.
   * @param new_file_path The path of the new database to be created.
   * @param class_name The name of the database class.  If it is empty, the class is guessed from
   * the file extension.
   * @param end_offset The exclusive end offset of records to read.  Negative means unlimited.
   * 0 means the size when the database is synched or closed properly.
   * @param cipher_key The encryption key for cipher compressors.
   * @return The result status.
   */
  static Status RestoreDatabase(
    const std::string& old_file_path, const std::string& new_file_path,
    const std::string& class_name = "", int64_t end_offset = -1,
    std::string_view cipher_key = "");

 private:
  /** The internal database object. */
  std::unique_ptr<DBM> dbm_;
  /** The owned message queue for the update logger. */
  std::unique_ptr<MessageQueue> ulog_mq_;
  /** The owned update logger. */
  std::unique_ptr<DBM::UpdateLogger> ulog_;
  /** Whether the internal database is open. */
  bool open_;
};

}  // namespace tkrzw

#endif  // _TKRZW_DBM_POLY_H

// END OF FILE
