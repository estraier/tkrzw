/*************************************************************************************************
 * Datatabase manager interface
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

#ifndef _TKRZW_DBM_H
#define _TKRZW_DBM_H

#include <initializer_list>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <cinttypes>

#include "tkrzw_file.h"
#include "tkrzw_lib_common.h"
#include "tkrzw_str_util.h"

namespace tkrzw {

/**
 * Interface of database manager.
 */
class DBM {
 public:
  /**
   * Interface of processor for a record.
   */
  class RecordProcessor {
   public:
    /** The special string indicating no operation. */
    static const std::string_view NOOP;
    /** The special string indicating removing operation. */
    static const std::string_view REMOVE;

    /**
     * Destructor.
     */
    virtual ~RecordProcessor() = default;

    /**
     * Processes an existing record.
     * @param key The key of the existing record.
     * @param value The value of the existing record.
     * @return A string reference to NOOP, REMOVE, or a strint of new value.
     * @details The memory referred to by the return value must be alive until the end of
     * the life-span of this object or until this function is called next time.
     */
    virtual std::string_view ProcessFull(std::string_view key, std::string_view value) {
      return NOOP;
    }

    /**
     * Processes an empty record space.
     * @param key The key of the existing record.
     * @return A string reference to NOOP, REMOVE, or a strint of new value.
     * @details The memory referred to by the return value must be alive until the end of
     * the life-span of this object or until this function is called next time.
     */
    virtual std::string_view ProcessEmpty(std::string_view key) {
      return NOOP;
    }
  };

  /**
   * Record processor to implement DBM::Get.
   */
  class RecordProcessorGet final : public RecordProcessor {
   public:
    /**
     * Constructor.
     * @param status The pointer to a status object to contain the result status.
     * @param value The pointer to a string object to contain the result value.
     */
    RecordProcessorGet(Status* status, std::string* value) : status_(status), value_(value) {}

    /**
     * Processes an existing record.
     */
    std::string_view ProcessFull(std::string_view key, std::string_view value) override {
      if (value_ != nullptr) {
        *value_ = value;
      }
      return NOOP;
    }

    /**
     * Processes an empty record space.
     */
    std::string_view ProcessEmpty(std::string_view key) override {
      status_->Set(Status::NOT_FOUND_ERROR);
      return NOOP;
    }

   private:
    /** Status to report. */
    Status* status_;
    /** Value to report. */
    std::string* value_;
  };

  /**
   * Record processor to implement DBM::Set.
   */
  class RecordProcessorSet final : public RecordProcessor {
   public:
    /**
     * Constructor.
     * @param status The pointer to a status object to contain the result status.
     * @param value A string of the value to set.
     * @param overwrite Whether to overwrite the existing value
     */
    RecordProcessorSet(Status* status, std::string_view value, bool overwrite)
        : status_(status), value_(value), overwrite_(overwrite) {}

    /**
     * Processes an existing record.
     */
    std::string_view ProcessFull(std::string_view key, std::string_view value) override {
      if (overwrite_) {
        return value_;
      }
      status_->Set(Status::DUPLICATION_ERROR);
      return NOOP;
    }

    /**
     * Processes an empty record space.
     */
    std::string_view ProcessEmpty(std::string_view key) override {
      return value_;
    }

   private:
    /** Status to report. */
    Status* status_;
    /** Value to store. */
    std::string_view value_;
    /** True to overwrite the existing value. */
    bool overwrite_;
  };

  /**
   * Record processor to implement DBM::Append.
   */
  class RecordProcessorAppend final : public RecordProcessor {
   public:
    /**
     * Constructor.
     * @param value A string of the value to set.
     * @param delim A string of the delimiter.
     */
    RecordProcessorAppend(std::string_view value, std::string_view delim)
        : value_(value), delim_(delim) {}

    /**
     * Processes an existing record.
     */
    std::string_view ProcessFull(std::string_view key, std::string_view value) override {
      if (delim_.empty()) {
        new_value_.reserve(value.size() + value_.size());
        new_value_.append(value);
        new_value_.append(value_);
      } else {
        new_value_.reserve(value.size() + delim_.size() + value_.size());
        new_value_.append(value);
        new_value_.append(delim_);
        new_value_.append(value_);
      }
      return new_value_;
    }

    /**
     * Processes an empty record space.
     */
    std::string_view ProcessEmpty(std::string_view key) override {
      return value_;
    }

   private:
    /** Value to store. */
    std::string_view value_;
    /** Delimiter after the existing value. */
    std::string_view delim_;
    /** The new value. */
    std::string new_value_;
  };

  /**
   * Record processor to implement DBM::CompareExchange.
   */
  class RecordProcessorCompareExchange final : public DBM::RecordProcessor {
   public:
    /**
     * Constructor.
     * @param status The pointer to a status object to contain the result status.
     * @param expected A string of the expected value.
     * @param desired A string of the expected value.
     * @param actual The pointer to a string object to contain the actual value.  If it is
     * nullptr, it is ignored.
     */
    RecordProcessorCompareExchange(Status* status, std::string_view expected,
                                   std::string_view desired, std::string* actual) :
        status_(status), expected_(expected), desired_(desired), actual_(actual) {}

    /**
     * Processes an existing record.
     */
    std::string_view ProcessFull(std::string_view key, std::string_view value) override {
      if (actual_ != nullptr) {
        *actual_ = value;
      }
      if (expected_.data() != nullptr && expected_ == value) {
        return desired_.data() == nullptr ? REMOVE : desired_;
      }
      status_->Set(Status::DUPLICATION_ERROR);
      return NOOP;
    }

    /**
     * Processes an empty record space.
     */
    std::string_view ProcessEmpty(std::string_view key) override {
      status_->Set(Status::NOT_FOUND_ERROR);
      return NOOP;
    }

   private:
    /** Status to report. */
    Status* status_;
    /** The expected value. */
    std::string_view expected_;
    /** The desired value. */
    std::string_view desired_;
    /** Actual value to report. */
    std::string* actual_;
  };

  /**
   * Record processor to implement DBM::Increment.
   */
  class RecordProcessorIncrement final : public DBM::RecordProcessor {
   public:
    /**
     * Constructor.
     * @param increment The incremental value.
     * @param current The pointer to a string object to contain the current value.
     * @param initial The initial value.
     */
    RecordProcessorIncrement(int64_t increment, int64_t* current, int64_t initial)
        : increment_(increment), current_(current), initial_(initial) {}

    /**
     * Processes an existing record.
     */
    std::string_view ProcessFull(std::string_view key, std::string_view value) override {
      if (increment_ == INT64MIN) {
        if (current_ != nullptr) {
          *current_ = StrToIntBigEndian(value);
        }
        return NOOP;
      }
      const int64_t num = StrToIntBigEndian(value) + increment_;
      if (current_ != nullptr) {
        *current_ = num;
      }
      value_ = IntToStrBigEndian(num);
      return value_;
    }

    /**
     * Processes an empty record space.
     */
    std::string_view ProcessEmpty(std::string_view key) override {
      if (increment_ == INT64MIN) {
        if (current_ != nullptr) {
          *current_ = initial_;
        }
        return NOOP;
      }
      const int64_t num = initial_ + increment_;
      if (current_ != nullptr) {
        *current_ = num;
      }
      value_ =  IntToStrBigEndian(num);
      return value_;
    }

   private:
    /** The incrementing value. */
    int64_t increment_;
    /** The current value to report. */
    int64_t* current_;
    /** The initial value. */
    int64_t initial_;
    /** The new string value. */
    std::string value_;
  };

  /**
   * Record processor to implement DBM::Remove.
   */
  class RecordProcessorRemove final : public RecordProcessor {
   public:
    /**
     * Constructor.
     * @param status The pointer to a status object to contain the result status.
     */
    explicit RecordProcessorRemove(Status* status) : status_(status) {}

    /**
     * Processes an existing record.
     */
    std::string_view ProcessFull(std::string_view key, std::string_view value) override {
      return REMOVE;
    }

    /**
     * Processes an empty record space.
     */
    std::string_view ProcessEmpty(std::string_view key) override {
      status_->Set(Status::NOT_FOUND_ERROR);
      return NOOP;
    }

   private:
    /** Status to report. */
    Status* status_;
  };

  /**
   * Record processor to implement DBM::Export.
   */
  class RecordProcessorExport final : public RecordProcessor {
   public:
    /**
     * Constructor.
     */
    RecordProcessorExport(Status* status, DBM* dbm) : status_(status), dbm_(dbm) {}

    /**
     * Processes an existing record.
     */
    std::string_view ProcessFull(std::string_view key, std::string_view value) override {
      *status_ |= dbm_->Set(key, value);
      return NOOP;
    }

   private:
    /** Status to report. */
    Status* status_;
    /** Destination database. */
    DBM* dbm_;
  };

  /**
   * Record processor to implement DBM::Iterator::Get.
   */
  class RecordProcessorIteratorGet final : public RecordProcessor {
   public:
    /**
     * Constructor.
     * @param key The pointer to a string object to contain the result key.
     * @param value The pointer to a string object to contain the result value.
     */
    RecordProcessorIteratorGet(std::string* key, std::string* value) : key_(key), value_(value) {}

    /**
     * Processes an existing record.
     */
    std::string_view ProcessFull(std::string_view key, std::string_view value) override {
      if (key_ != nullptr) {
        *key_ = key;
      }
      if (value_ != nullptr) {
        *value_ = value;
      }
      return NOOP;
    }

   private:
    /** Key to report. */
    std::string* key_;
    /** Value to report. */
    std::string* value_;
  };

  /**
   * Record processor to implement DBM::Iterator::Set.
   */
  class RecordProcessorIteratorSet final : public RecordProcessor {
   public:
    /**
     * Constructor.
     * @param value A string of the value to set.
     */
    explicit RecordProcessorIteratorSet(std::string_view value) : value_(value) {}

    /**
     * Processes an existing record.
     */
    std::string_view ProcessFull(std::string_view key, std::string_view value) override {
      return value_;
    }

   private:
    /** Value to store. */
    std::string_view value_;
  };

  /**
   * Record processor to implement DBM::Iterator::Remove.
   */
  class RecordProcessorIteratorRemove final : public RecordProcessor {
   public:
    /**
     * Processes an existing record.
     */
    std::string_view ProcessFull(std::string_view key, std::string_view value) override {
      return REMOVE;
    }
  };

  /**
   * Interface of iterator for each record.
   */
  class Iterator {
   public:
    /**
     * Destructor.
     */
    virtual ~Iterator() = default;

    /**
     * Initializes the iterator to indicate the first record.
     * @return The result status.
     * @details Even if there's no record, the operation doesn't fail.
     */
    virtual Status First() = 0;

    /**
     * Initializes the iterator to indicate the last record.
     * @return The result status.
     * @details Even if there's no record, the operation doesn't fail.  This method is suppoerted
     * only by ordered databases.
     */
    virtual Status Last() = 0;

    /**
     * Initializes the iterator to indicate a specific record.
     * @param key The key of the record to look for.
     * @return The result status.
     * @details Ordered databases can support "lower bound" jump; If there's no record with the
     * same key, the iterator refers to the first record whose key is greater than the given key.
     * The operation fails with unordered databases if there's no record with the same key.
     */
    virtual Status Jump(std::string_view key) = 0;

    /**
     * Initializes the iterator to indicate the last record whose key is lower than a given key.
     * @param key The key to compare with.
     * @param inclusive If true, the considtion is inclusive: equal to or lower than the key.
     * @return The result status.
     * @details Even if there's no matching record, the operation doesn't fail.  This method is
     * suppoerted only by ordered databases.
     */
    virtual Status JumpLower(std::string_view key, bool inclusive = false) = 0;

    /**
     * Initializes the iterator to indicate the first record whose key is upper than a given key.
     * @param key The key to compare with.
     * @param inclusive If true, the considtion is inclusive: equal to or upper than the key.
     * @return The result status.
     * @details Even if there's no matching record, the operation doesn't fail.  This method is
     * suppoerted only by ordered databases.
     */
    virtual Status JumpUpper(std::string_view key, bool inclusive = false) = 0;

    /**
     * Moves the iterator to the next record.
     * @return The result status.
     * @details If the current record is missing, the operation fails.  Even if there's no next
     * record, the operation doesn't fail.
     */
    virtual Status Next() = 0;

    /**
     * Moves the iterator to the previous record.
     * @return The result status.
     * @details If the current record is missing, the operation fails.  Even if there's no previous
     * record, the operation doesn't fail.  This method is suppoerted only by ordered databases.
     */
    virtual Status Previous() = 0;

    /**
     * Processes the current record with a processor.
     * @param proc The pointer to the processor object.
     * @param writable True if the processor can edit the record.
     * @return The result status.
     * @details If the current record exists, the ProcessFull of the processor is called.
     * Otherwise, this method fails and no method of the processor is called.  If the current
     * record is removed, the iterator is moved to the next record.
     */
    virtual Status Process(RecordProcessor* proc, bool writable) = 0;

    /**
     * Gets the key and the value of the current record of the iterator.
     * @param key The pointer to a string object to contain the record key.  If it is nullptr,
     * the key data is ignored.
     * @param value The pointer to a string object to contain the record value.  If it is nullptr,
     * the value data is ignored.
     * @return The result status.
     */
    virtual Status Get(std::string* key = nullptr, std::string* value = nullptr) {
      RecordProcessorIteratorGet proc(key, value);
      return Process(&proc, false);
    }

    /**
     * Gets the key of the current record, in a simple way.
     * @param default_value The value to be returned on failure.
     * @return The key of the current record on success, or the default value on failure.
     */
    virtual std::string GetKey(std::string_view default_value = "") {
      std::string key;
      return Get(&key, nullptr) == Status::SUCCESS ? key : std::string(default_value);
    }

    /**
     * Gets the value of the current record, in a simple way.
     * @param default_value The value to be returned on failure.
     * @return The value of the current record on success, or the default value on failure.
     */
    virtual std::string GetValue(std::string_view default_value = "") {
      std::string value;
      return Get(nullptr, &value) == Status::SUCCESS ? value : std::string(default_value);
    }

    /**
     * Sets the value of the current record.
     * @param value The value of the record.
     * @return The result status.
     */
    virtual Status Set(std::string_view value) {
      RecordProcessorIteratorSet proc(value);
      return Process(&proc, true);
    }

    /**
     * Removes the current record.
     * @return The result status.
     * @details If possible, the iterator moves to the next record.
     */
    virtual Status Remove() {
      RecordProcessorIteratorRemove proc;
      return Process(&proc, true);
    }
  };

  /**
   * Interface of processor for a record.
   */
  class FileProcessor {
   public:
    /**
     * Destructor.
     */
    virtual ~FileProcessor() = default;

    /**
     * Process a file.
     * @param path The path of the file.
     */
    virtual void Process(const std::string& path) = 0;
  };

  /**
   * File processor to implement DBM::CopyFile.
   */
  class FileProcessorCopyFile : public FileProcessor {
   public:
    /**
     * Constructor.
     * @param status The pointer to a status object to contain the result status.
     * @param dest_path The destination path for copying.
     */
    FileProcessorCopyFile(Status* status, const std::string dest_path);

    /**
     * Process a file.
     * @param path The path of the file.
     */
    void Process(const std::string& path) override;

   private:
    Status* status_;
    std::string dest_path_;
  };

  /**
   * Destructor.
   */
  virtual ~DBM() = default;

  /**
   * Opens a database file.
   * @param path A path of the file.
   * @param writable If true, the file is writable.  If false, it is read-only.
   * @param options Bit-sum options for opening the file.
   * @return The result status.
   */
  virtual Status Open(const std::string& path, bool writable,
                      int32_t options = File::OPEN_DEFAULT) = 0;

  /**
   * Closes the database file.
   * @return The result status.
   */
  virtual Status Close() = 0;

  /**
   * Processes a record with a processor.
   * @param key The key of the record.
   * @param proc The pointer to the processor object.
   * @param writable True if the processor can edit the record.
   * @return The result status.
   * @details If the specified record exists, the ProcessFull of the processor is called.
   * Otherwise, the ProcessEmpty of the processor is called.
   */
  virtual Status Process(std::string_view key, RecordProcessor* proc, bool writable) = 0;

  /**
   * Gets the value of a record of a key.
   * @param key The key of the record.
   * @param value The pointer to a string object to contain the result value.  If it is nullptr,
   * the value data is ignored.
   * @return The result status.
   */
  virtual Status Get(std::string_view key, std::string* value = nullptr) {
    Status impl_status(Status::SUCCESS);
    RecordProcessorGet proc(&impl_status, value);
    const Status status = Process(key, &proc, false);
    if (status != Status::SUCCESS) {
      return status;
    }
    return impl_status;
  }

  /**
   * Gets the value of a record of a key, in a simple way.
   * @param key The key of the record.
   * @param default_value The value to be returned on failure.
   * @return The value of the matching record on success, or the default value on failure.
   */
  virtual std::string GetSimple(std::string_view key, std::string_view default_value = "") {
    std::string value;
    return Get(key, &value) == Status::SUCCESS ? value : std::string(default_value);
  }

  /**
   * Gets the values of multiple records of keys.
   * @param keys The keys of records to retrieve.
   * @return A map of retrieved records.  Keys which don't match existing records are ignored.
   */
  virtual std::map<std::string, std::string> GetMulti(
      const std::initializer_list<std::string>& keys) {
    std::map<std::string, std::string> records;
    for (const auto& key : keys) {
      std::string value;
      if (Get(key, &value) == Status::SUCCESS) {
        records.emplace(key, value);
      }
    }
    return records;
  }

  /**
   * Gets the values of multiple records of keys, with a vector.
   * @param keys The keys of records to retrieve.
   * @return A map of retrieved records.  Keys which don't match existing records are ignored.
   */
  virtual std::map<std::string, std::string> GetMulti(const std::vector<std::string>& keys) {
    std::map<std::string, std::string> records;
    for (const auto& key : keys) {
      std::string value;
      if (Get(key, &value) == Status::SUCCESS) {
        records.emplace(key, value);
      }
    }
    return records;
  }

  /**
   * Sets a record of a key and a value.
   * @param key The key of the record.
   * @param value The value of the record.
   * @param overwrite Whether to overwrite the existing value if there's a record with the same
   * key.  If true, the existing value is overwritten by the new value.  If false, the operation
   * is given up and an error status is returned.
   * @return The result status.
   */
  virtual Status Set(std::string_view key, std::string_view value, bool overwrite = true) {
    Status impl_status(Status::SUCCESS);
    RecordProcessorSet proc(&impl_status, value, overwrite);
    const Status status = Process(key, &proc, true);
    if (status != Status::SUCCESS) {
      return status;
    }
    return impl_status;
  }

  /**
   * Sets multiple records.
   * @param records The records to store.
   * @param overwrite Whether to overwrite the existing value if there's a record with the same
   * key.  If true, the existing value is overwritten by the new value.  If false, the operation
   * is given up and an error status is returned.
   * @return The result status.
   */
  virtual Status SetMulti(
      const std::initializer_list<std::pair<std::string, std::string>>& records,
      bool overwrite = true) {
    for (const auto& record : records) {
      const Status status = Set(record.first, record.second, overwrite);
      if (status != Status::Status::SUCCESS) {
        return status;
      }
    }
    return Status(Status::SUCCESS);
  }

  /**
   * Sets multiple records, with a map of strings.
   * @param records The records to store.
   * @param overwrite Whether to overwrite the existing value if there's a record with the same
   * key.  If true, the existing value is overwritten by the new value.  If false, the operation
   * is given up and an error status is returned.
   * @return The result status.
   */
  virtual Status SetMulti(
      const std::map<std::string, std::string>& records, bool overwrite = true) {
    for (const auto& record : records) {
      const Status status = Set(record.first, record.second, overwrite);
      if (status != Status::Status::SUCCESS) {
        return status;
      }
    }
    return Status(Status::SUCCESS);
  }

  /**
   * Removes a record of a key.
   * @param key The key of the record.
   * @return The result status.
   */
  virtual Status Remove(std::string_view key) {
    Status impl_status(Status::SUCCESS);
    RecordProcessorRemove proc(&impl_status);
    const Status status = Process(key, &proc, true);
    if (status != Status::SUCCESS) {
      return status;
    }
    return impl_status;
  }

  /**
   * Appends data at the end of a record of a key.
   * @param key The key of the record.
   * @param value The value to append.
   * @param delim The delimiter to put after the existing record.
   * @return The result status.
   * @details If there's no existing record, the value is set without the delimiter.
   */
  virtual Status Append(
      std::string_view key, std::string_view value, std::string_view delim = "") {
    RecordProcessorAppend proc(value, delim);
    return Process(key, &proc, true);
  }

  /**
   * Compares the value of a record and exchanges if the condition meets.
   * @param key The key of the record.
   * @param expected The expected value.
   * @param desired The desired value.  If the data is nullptr, the record is to be removed.
   * @param actual The pointer to a string object to contain the result value.  If it is nullptr,
   * it is ignored.
   * @return The result status.
   * @details If the record doesn't exist, NOT_FOUND_ERROR is returned.  If the existing value is
   * different from the expected value, DUPLICATION_ERROR is returned.  Otherwise, the desired
   * value is set.
   */
  virtual Status CompareExchange(std::string_view key, std::string_view expected,
                                 std::string_view desired, std::string* actual = nullptr) {
    Status impl_status(Status::SUCCESS);
    RecordProcessorCompareExchange proc(&impl_status, expected, desired, actual);
    const Status status = Process(key, &proc, true);
    if (status != Status::SUCCESS) {
      return status;
    }
    return impl_status;
  }

  /**
   * Increments the numeric value of a record.
   * @param key The key of the record.
   * @param increment The incremental value.  If it is INT64MIN, the current value is not changed
   * and a new record is not created.
   * @param current The pointer to an integer to contain the current value.  If it is nullptr,
   * it is ignored.
   * @param initial The initial value.
   * @return The result status.
   * @details The record value is stored as an 8-byte big-endian integer.  Negative is also
   * supported.
   */
  virtual Status Increment(std::string_view key, int64_t increment = 1,
                           int64_t* current = nullptr, int64_t initial = 0) {
    RecordProcessorIncrement proc(increment, current, initial);
    return Process(key, &proc, increment != INT64MIN);
  }

  /**
   * Increments the numeric value of a record, in a simple way.
   * @param key The key of the record.
   * @param increment The incremental value.
   * @param initial The initial value.
   * @return The current value or INT64MIN on failure.
   * @details The record value is treated as a decimal integer.  Negative is also supported.
   */
  int64_t IncrementSimple(std::string_view key, int64_t increment = 1, int64_t initial = 0) {
    int64_t current = 0;
    return Increment(key, increment, &current, initial) == Status::SUCCESS ? current : INT64MIN;
  }

  /**
   * Processes each and every record in the database with a processor.
   * @param proc The pointer to the processor object.
   * @param writable True if the processor can edit the record.
   * @return The result status.
   * @details The ProcessFull of the processor is called repeatedly for each record.  The
   * ProcessEmpty of the processor is called once before the iteration and once after the
   * iteration.
   */
  virtual Status ProcessEach(RecordProcessor* proc, bool writable) = 0;

  /**
   * Gets the number of records.
   * @param count The pointer to an integer object to contain the result count.
   * @return The result status.
   */
  virtual Status Count(int64_t* count) = 0;

  /**
   * Gets the number of records, in a simple way.
   * @return The number of records on success, or -1 on failure.
   */
  virtual int64_t CountSimple() {
    int64_t count = 0;
    return Count(&count) == Status::SUCCESS ? count : -1;
  }

  /**
   * Gets the current file size of the database.
   * @param size The pointer to an integer object to contain the result size.
   * @return The result status.
   */
  virtual Status GetFileSize(int64_t* size) = 0;

  /**
   * Gets the current file size of the database, in a simple way.
   * @return The current file size of the database, or -1 on failure.
   */
  virtual int64_t GetFileSizeSimple() {
    int64_t size = 0;
    return GetFileSize(&size) == Status::SUCCESS ? size : -1;
  }

  /**
   * Gets the path of the database file.
   * @param path The pointer to a string object to contain the result path.
   * @return The result status.
   */
  virtual Status GetFilePath(std::string* path) = 0;

  /**
   * Gets the path of the database file, in a simple way.
   * @return The file path of the database, or an empty string on failure.
   */
  virtual std::string GetFilePathSimple() {
    std::string path;
    return GetFilePath(&path) == Status::SUCCESS ? path : std::string("");
  }

  /**
   * Removes all records.
   * @return The result status.
   */
  virtual Status Clear() = 0;

  /**
   * Rebuilds the entire database.
   * @return The result status.
   */
  virtual Status Rebuild() = 0;

  /**
   * Checks whether the database should be rebuilt.
   * @param tobe The pointer to a boolean object to contain the result decision.
   * @return The result status.
   */
  virtual Status ShouldBeRebuilt(bool* tobe) = 0;

  /**
   * Checks whether the database should be rebuilt, in a simple way.
   * @return True if the database should be rebuilt or false if not or on failure.
   */
  virtual bool ShouldBeRebuiltSimple() {
    bool tobe = false;
    return ShouldBeRebuilt(&tobe) == Status::SUCCESS ? tobe : false;
  }

  /**
   * Synchronizes the content of the database to the file system.
   * @param hard True to do physical synchronization with the hardware or false to do only
   * logical synchronization with the file system.
   * @param proc The pointer to the file processor object, whose Process method is called while
   * the content of the file is synchronized.  If it is nullptr, it is ignored.
   * @return The result status.
   */
  virtual Status Synchronize(bool hard, FileProcessor* proc = nullptr) = 0;

  /**
   * Copies the content of the database file to another file.
   * @param dest_path A path to the destination file.
   * @return The result status.
   * @details Copying is done while the content is synchronized and stable.  So, this method is
   * suitable for making a backup file while running a database service.
   */
  virtual Status CopyFile(const std::string& dest_path) {
    Status impl_status(Status::SUCCESS);
    FileProcessorCopyFile proc(&impl_status, dest_path);
    if (IsWritable()) {
      const Status status = Synchronize(false, &proc);
      if (status != Status::SUCCESS) {
        return status;
      }
    } else {
      std::string path;
      const Status status = GetFilePath(&path);
      if (status != Status::SUCCESS) {
        return status;
      }
      proc.Process(path);
    }
    return impl_status;
  }

  /**
   * Exports all records to another database.
   * @param dbm The pointer to the destination database.
   * @return The result status.
   */
  virtual Status Export(DBM* dbm) {
    Status impl_status(Status::SUCCESS);
    RecordProcessorExport proc(&impl_status, dbm);
    const Status status = ProcessEach(&proc, false);
    if (status != Status::SUCCESS) {
      return status;
    }
    return impl_status;
  }

  /**
   * Inspects the database.
   * @return A vector of pairs of a property name and its value.
   */
  virtual std::vector<std::pair<std::string, std::string>> Inspect() = 0;

  /**
   * Checks whether the database is open.
   * @return True if the database is open, or false if not.
   */
  virtual bool IsOpen() const = 0;

  /**
   * Checks whether the database is writable.
   * @return True if the database is writable, or false if not.
   */
  virtual bool IsWritable() const = 0;

  /**
   * Checks whether the database condition is healthy.
   * @return True if the database condition is healthy, or false if not.
   */
  virtual bool IsHealthy() const = 0;

  /**
   * Checks whether ordered operations are supported.
   * @return True if ordered operations are supported, or false if not.
   */
  virtual bool IsOrdered() const = 0;

  /**
   * Makes an iterator for each record.
   * @return The iterator for each record.
   */
  virtual std::unique_ptr<Iterator> MakeIterator() = 0;

  /**
   * Make a new DBM object of the same concrete class.
   * @return The new file object.
   */
  virtual std::unique_ptr<DBM> MakeDBM() const = 0;
};

}  // namespace tkrzw

#endif  // _TKRZW_DBM_H

// END OF FILE
