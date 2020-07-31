/*************************************************************************************************
 * Sharding datatabase manager adapter
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

#ifndef _TKRZW_DBM_SHARD_H
#define _TKRZW_DBM_SHARD_H

#include <map>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <cinttypes>

#include "tkrzw_dbm.h"
#include "tkrzw_file.h"
#include "tkrzw_key_comparators.h"
#include "tkrzw_lib_common.h"
#include "tkrzw_dbm_poly.h"
#include "tkrzw_str_util.h"

namespace tkrzw {

/**
 * Sharding database manager adapter.
 * @details All operations except for Open and Close are thread-safe; Multiple threads can
 * access the same database concurrently.  Every opened database must be closed explicitly to
 * avoid data corruption.
 * @details This class is a wrapper of PolyDBM for sharding the database into multiple instances.
 */
class ShardDBM final : public ParamDBM {
 public:
  /**
   * Iterator for each record.
   * @details When the database is updated, some iterators may or may not be invalided.
   * Operations with invalidated iterators fails gracefully with NOT_FOUND_ERROR.  One iterator
   * cannot be shared by multiple threads.
   */
  class Iterator final : public DBM::Iterator {
    friend class tkrzw::ShardDBM;
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
     * @details Even if there's no record, the operation doesn't fail.
     */
    Status First() override;

    /**
     * Initializes the iterator to indicate the last record.
     * @return The result status.
     * @details Even if there's no record, the operation doesn't fail.  This method is suppoerted
     * only by ordered databases.
     */
    Status Last() override;

    /**
     * Initializes the iterator to indicate a specific record.
     * @param key The key of the record to look for.
     * @return The result status.
     * @details Ordered databases can support "lower bound" jump; If there's no record with the
     * same key, the iterator refers to the first record whose key is greater than the given key.
     * The operation fails with unordered databases if there's no record with the same key.
     */
    Status Jump(std::string_view key) override;

    /**
     * Initializes the iterator to indicate the last record whose key is lower than a given key.
     * @param key The key to compare with.
     * @param inclusive If true, the considtion is inclusive: equal to or lower than the key.
     * @return The result status.
     * @details Even if there's no matching record, the operation doesn't fail.  This method is
     * suppoerted only by ordered databases.
     */
    Status JumpLower(std::string_view key, bool inclusive = false) override;

    /**
     * Initializes the iterator to indicate the first record whose key is upper than a given key.
     * @param key The key to compare with.
     * @param inclusive If true, the considtion is inclusive: equal to or upper than the key.
     * @return The result status.
     * @details Even if there's no matching record, the operation doesn't fail.  This method is
     * suppoerted only by ordered databases.
     */
    Status JumpUpper(std::string_view key, bool inclusive = false) override;

    /**
     * Moves the iterator to the next record.
     * @return The result status.
     * @details If the current record is missing, the operation fails.  Even if there's no next
     * record, the operation doesn't fail.
     */
    Status Next() override;

    /**
     * Moves the iterator to the previous record.
     * @return The result status.
     * @details If the current record is missing, the operation fails.  Even if there's no previous
     * record, the operation doesn't fail.  This method is suppoerted only by ordered databases.
     */
    Status Previous() override;

    /**
     * Processes the current record with a processor.
     * @param proc The pointer to the processor object.
     * @param writable True if the processor can edit the record.
     * @return The result status.
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

    /**
     * Sets the value of the current record.
     * @param value The value of the record.
     * @return The result status.
     */
    Status Set(std::string_view value) override;

    /**
     * Removes the current record.
     * @return The result status.
     * @details If possible, the iterator moves to the next record.
     */
    Status Remove() override;

   private:
    /**
     * Structure of a sharding slot.
     */
    struct ShardSlot final {
      /** The last retrieved key. */
      std::string key;
      /** The last retrieved value. */
      std::string value;
      /** The iterator object, owned */
      DBM::Iterator* iter;
      /** Constructor. */
      ShardSlot() : key(), value(), iter(nullptr) {}
    };

    /**
     * Ascending comparator for sharding slots.
     */
    struct ShardSlotComparator final {
      /** The key comparator. */
      KeyComparator comp;
      /** Whether the oder is ascending. */
      bool asc;

      /**
       * Constructor.
       * @param comp The key comparator.
       * @param asc True for ascending order and false for descending order.
       */
      ShardSlotComparator(KeyComparator comp, bool asc) : comp(comp), asc(asc) {}

      /**
       * Compares two sorting slots.
       * @param lhs The first sorting slot.
       * @param rhs The second other sorting slot.
       * @param True if the first one is greater.
       */
      bool operator ()(const ShardSlot* lhs, const ShardSlot* rhs) const {
        return asc ? comp(lhs->key, rhs->key) > 0 : comp(lhs->key, rhs->key) < 0;
      }
    };

    /**
     * Constructor.
     * @param dbms The internal database objects.
     */
    explicit Iterator(std::vector<std::shared_ptr<PolyDBM>>* dbms);

    /** The shard slots. */
    std::vector<ShardSlot> slots_;
    /** The heap to get the mimimum record. */
    std::vector<ShardSlot*> heap_;
    /** The key comparator. */
    KeyComparator comp_;
    /** Whether the oder is ascending. */
    bool asc_;
  };

  /**
   * Default constructor.
   */
  explicit ShardDBM();

  /**
   * Destructor.
   */
  virtual ~ShardDBM();

  /**
   * Opens a database file.
   * @param path A path of the file.
   * @param writable If true, the file is writable.  If false, it is read-only.
   * @param options Bit-sum options for opening the file.
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
   * @param params Optional parameters.  All parameters for PolyDBM::OpenAdvanced are supported.
   * Moreover, the parameter "num_shards" specifies the number of shards.  Each shard file has a
   * suffix like "-00003-of-00015".  If the number of shards is not specified and existing files
   * match the path, it is implicitly specified.  If there are no matching files, 1 is implicitly
   * set.
   * @return The result status.
   */
  Status OpenAdvanced(const std::string& path, bool writable,
                      int32_t options = File::OPEN_DEFAULT,
                      const std::map<std::string, std::string>& params = {});

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
   * @return The result status.
   */
  Status Get(std::string_view key, std::string* value = nullptr) override;

  /**
   * Sets a record of a key and a value.
   * @param key The key of the record.
   * @param value The value of the record.
   * @param overwrite Whether to overwrite the existing value if there's a record with the same
   * key.  If true, the existing value is overwritten by the new value.  If false, the operation
   * is given up and an error status is returned.
   * @return The result status.
   */
  Status Set(std::string_view key, std::string_view value, bool overwrite = true) override;

  /**
   * Removes a record of a key.
   * @param key The key of the record.
   * @return The result status.
   */
  Status Remove(std::string_view key) override;

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
   * @details The parameters work in the same way as with PolyDBM::RebuildAdvanced.
   */
  Status RebuildAdvanced(const std::map<std::string, std::string>& params = {});

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
   * @details The parameters work in the same way as with PolyDBM::OpenAdvanced.
   */
  Status SynchronizeAdvanced(bool hard, FileProcessor* proc = nullptr,
                             const std::map<std::string, std::string>& params = {});

  /**
   * Copies the content of the database files to other files.
   * @param dest_path A path prefix to the destination files.
   * @return The result status.
   * @details Copying is done while the content is synchronized and stable.  So, this method is
   * suitable for making a backup file while running a database service.  Each shard file is
   * copied and the destination file also has the same suffix.
   */
  Status CopyFile(const std::string& dest_path) override;

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
   * Make a new DBM object of the same concrete class.
   * @return The new file object.
   */
  std::unique_ptr<DBM> MakeDBM() const override;

  /**
   * Gets the pointer to the first internal database object.
   * @return The pointer to the first internal database object, or nullptr on failure.
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
   * @return The result status.
   */
  static Status RestoreDatabase(
    const std::string& old_file_path, const std::string& new_file_path,
    const std::string& class_name = "", int64_t end_offset = -1);

 private:
  /** The internal database objects. */
  std::vector<std::shared_ptr<PolyDBM>> dbms_;
  /** Whether the internal databases are open. */
  bool open_;
  /** The stem name of the file paths of the internal databases. */
  std::string path_;
};

}  // namespace tkrzw

#endif  // _TKRZW_DBM_SHARD_H

// END OF FILE
