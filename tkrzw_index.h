/*************************************************************************************************
 * Secondary index implementations
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

#ifndef _TKRZW_INDEX_H
#define _TKRZW_INDEX_H

#include <list>
#include <shared_mutex>
#include <set>
#include <string>
#include <string_view>
#include <tuple>
#include <utility>
#include <vector>

#include <cinttypes>

#include "tkrzw_dbm.h"
#include "tkrzw_dbm_baby.h"
#include "tkrzw_dbm_tree.h"
#include "tkrzw_key_comparators.h"
#include "tkrzw_lib_common.h"
#include "tkrzw_str_util.h"
#include "tkrzw_thread_util.h"

namespace tkrzw {

/**
 * File secondary index implementation with TreeDBM.
 * @details All operations are thread-safe; Multiple threads can access the same database
 * concurrently.
 */
class FileIndex final {
 public:
  /**
   * Iterator for each record.
   */
  class Iterator {
    friend class FileIndex;
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
     */
    void First();

    /**
     * Initializes the iterator to indicate the first record.
     */
    void Last();

    /**
     * Initializes the iterator to indicate a specific range.
     * @param key The key of the lower bound.
     * @param value The value of the lower bound.
     */
    void Jump(std::string_view key, std::string_view value = "");

    /**
     * Gets the key and the value of the current record of the iterator.
     * @param key The pointer to a string object to contain the record key.  If it is nullptr,
     * the key data is ignored.
     * @param value The pointer to a string object to contain the record value.  If it is nullptr,
     * the value data is ignored.
     * @return True on success or false on failure.  If theres no record to fetch, false is
     * returned.
     */
    bool Get(std::string* key = nullptr, std::string* value = nullptr);

    /**
     * Moves the iterator to the next record.
     */
    void Next();

    /**
     * Moves the iterator to the previous record.
     */
    void Previous();

   private:
    /**
     * Constructor.
     * @param dbm_impl The database implementation object.
     */
    explicit Iterator(std::unique_ptr<DBM::Iterator> it);

    /** Unique pointer to a DBM iterator. */
    std::unique_ptr<DBM::Iterator> it_;
  };

  /**
   * Default constructor.
   * @details MemoryMapParallelFile is used to handle the data.
   */
  FileIndex() : iterators_(), dbm_() {}

  /**
   * Constructor with a file object.
   * @param file The file object to handle the data.  The ownership is taken.
   */
  explicit FileIndex(std::unique_ptr<File> file) : iterators_(), dbm_(std::move(file)) {}

  /**
   * Copy and assignment are disabled.
   */
  explicit FileIndex(const FileIndex& rhs) = delete;
  FileIndex& operator =(const FileIndex& rhs) = delete;

  /**
   * Opens a database file.
   * @param path A path of the file.
   * @param writable If true, the file is writable.  If false, it is read-only.
   * @param options Bit-sum options for opening the file.
   * @param tuning_params A structure for tuning parameters.
   * @return The result status.
   * @details If the key comparator of the tuning parameter is nullptr, PairLexicalKeyComparator
   * is set implicitly.  Other compatible key comparators are PairLexicalCaseKeyComparator,
   * PairDecimalKeyComparator, PairHexadecimalKeyComparator, and PairRealNumberKeyComparator.
   * The alignment power and the
   * maximum page size are also set implicitly to be suitable for random access.
   */
  Status Open(const std::string& path, bool writable,
              int32_t options = File::OPEN_DEFAULT,
              const TreeDBM::TuningParameters& tuning_params = TreeDBM::TuningParameters());

  /**
   * Closes the database file.
   * @return The result status.
   * @details Precondition: The database is opened.
   */
  Status Close();

  /**
   * Checks whether a record exists in the index.
   * @param key The key of the record.
   * @param value The value of the record.
   */
  bool Check(std::string_view key, std::string_view value);

  /**
   * Gets all values of records of a key.
   * @param key The key to look for.
   * @param max The maximum number of values to get.  0 means unlimited.
   * @return All values of the key.
   */
  std::vector<std::string> GetValues(std::string_view key, size_t max = 0);

  /**
   * Adds a record.
   * @param key The key of the record.  This can be an arbitrary expression to search the index.
   * @param value The value of the record.  This should be a primary value of another database.
   * @return The result status.
   */
  Status Add(std::string_view key, std::string_view value);

  /**
   * Removes a record.
   * @param key The key of the record.
   * @param value The value of the record.
   * @return The result status.
   */
  Status Remove(std::string_view key, std::string_view value);

  /**
   * Gets the number of records.
   * @return The number of records.
   */
  size_t Count();

  /**
   * Removes all records.
   * @return The result status.
   */
  Status Clear();

  /**
   * Rebuilds the entire database.
   * @return The result status.
   */
  Status Rebuild();

  /**
   * Synchronizes the content of the database to the file system.
   * @param hard True to do physical synchronization with the hardware or false to do only
   * logical synchronization with the file system.
   * @return The result status.
   */
  Status Synchronize(bool hard);

  /**
   * Checks whether the database is open.
   * @return True if the database is open, or false if not.
   */
  bool IsOpen() const;

  /**
   * Checks whether the database is writable.
   * @return True if the database is writable, or false if not.
   */
  bool IsWritable() const;

  /**
   * Gets the pointer to the internal database object.
   * @return The pointer to the internal database object, or nullptr on failure.
   */
  TreeDBM* GetInternalDBM() const;

  /**
   * Makes an iterator for each record.
   * @return The iterator for each record.
   */
  std::unique_ptr<Iterator> MakeIterator();

 private:
  /** The list of the current iterators. */
  std::list<Iterator*> iterators_;
  /** The database manager. */
  TreeDBM dbm_;
};

/**
 * On-memory secondary index implementation with BabyDBM.
 * @details All operations are thread-safe; Multiple threads can access the same database
 * concurrently.
 */
class MemIndex final {
 public:
  /**
   * Iterator for each record.
   */
  class Iterator {
    friend class MemIndex;
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
     */
    void First();

    /**
     * Initializes the iterator to indicate the first record.
     */
    void Last();

    /**
     * Initializes the iterator to indicate a specific range.
     * @param key The key of the lower bound.
     * @param value The value of the lower bound.
     */
    void Jump(std::string_view key, std::string_view value = "");

    /**
     * Gets the key and the value of the current record of the iterator.
     * @param key The pointer to a string object to contain the record key.  If it is nullptr,
     * the key data is ignored.
     * @param value The pointer to a string object to contain the record value.  If it is nullptr,
     * the value data is ignored.
     * @return True on success or false on failure.  If theres no record to fetch, false is
     * returned.
     */
    bool Get(std::string* key = nullptr, std::string* value = nullptr);

    /**
     * Moves the iterator to the next record.
     */
    void Next();

    /**
     * Moves the iterator to the previous record.
     */
    void Previous();

   private:
    /**
     * Constructor.
     * @param dbm_impl The database implementation object.
     */
    explicit Iterator(std::unique_ptr<DBM::Iterator> it);

    /** Unique pointer to a DBM iterator. */
    std::unique_ptr<DBM::Iterator> it_;
  };

  /**
   * Constructor.
   * @param key_comparator The comparator of record keys.
   * @details Compatible key comparators are PairLexicalKeyComparator,
   * PairLexicalCaseKeyComparator, PairDecimalKeyComparator, PairHexadecimalKeyComparator,
   * and PairRealNumberKeyComparator.
   */
  explicit MemIndex(KeyComparator key_comparator = PairLexicalKeyComparator);

  /**
   * Copy and assignment are disabled.
   */
  explicit MemIndex(const MemIndex& rhs) = delete;
  MemIndex& operator =(const MemIndex& rhs) = delete;

  /**
   * Checks whether a record exists in the index.
   * @param key The key of the record.
   * @param value The value of the record.
   */
  bool Check(std::string_view key, std::string_view value);

  /**
   * Gets all values of records of a key.
   * @param key The key to look for.
   * @param max The maximum number of values to get.  0 means unlimited.
   * @return All values of the key.
   */
  std::vector<std::string> GetValues(std::string_view key, size_t max = 0);

  /**
   * Adds a record.
   * @param key The key of the record.  This can be an arbitrary expression to search the index.
   * @param value The value of the record.  This should be a primary value of another database.
   */
  void Add(std::string_view key, std::string_view value);

  /**
   * Removes a record.
   * @param key The key of the record.
   * @param value The value of the record.
   */
  void Remove(std::string_view key, std::string_view value);

  /**
   * Gets the number of records.
   * @return The number of records.
   */
  size_t Count();

  /**
   * Removes all records.
   */
  void Clear();

  /**
   * Makes an iterator for each record.
   * @return The iterator for each record.
   */
  std::unique_ptr<Iterator> MakeIterator();

 private:
  /** The list of the current iterators. */
  std::list<Iterator*> iterators_;
  /** The database manager. */
  BabyDBM dbm_;
};

/**
 * On-memory secondary index implementation with std::map for generic types.
 * @param KEYTYPE the key type.
 * @param VALUETYPE the value type.
 * @param CMPTYPE the comparator type.
 * @details All operations are thread-safe; Multiple threads can access the same database
 * concurrently.
 */
template <typename KEYTYPE, typename VALUETYPE,
          typename CMPTYPE = std::less<std::pair<KEYTYPE, VALUETYPE>>>
class StdIndex final {
  typedef std::set<std::pair<KEYTYPE, VALUETYPE>, CMPTYPE> SETTYPE;
 public:
  /**
   * Iterator for each record.
   */
  class Iterator {
    friend class StdIndex<KEYTYPE, VALUETYPE, CMPTYPE>;
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
     */
    void First();

    /**
     * Initializes the iterator to indicate the last record.
     */
    void Last();

    /**
     * Initializes the iterator to indicate a specific range.
     * @param key The key of the lower bound.
     * @param value The value of the lower bound.
     * @details If you set std::greater<std::pair<KEYTYPE, VALUETYPE>> as the comparator,
     * values are also set in descending order.  Then, in order to visit all record of the
     * specified key, you should set the maximum value in the possible range.
     */
    void Jump(const KEYTYPE& key, const VALUETYPE& value = VALUETYPE());

    /**
     * Gets the key and the value of the current record of the iterator.
     * @param key The pointer to a string object to contain the record key.  If it is nullptr,
     * the key data is ignored.
     * @param value The pointer to a string object to contain the record value.  If it is nullptr,
     * the value data is ignored.
     * @return True on success or false on failure.  If theres no record to fetch, false is
     * returned.
     */
    bool Get(KEYTYPE* key = nullptr, VALUETYPE* value = nullptr);

    /**
     * Moves the iterator to the next record.
     */
    void Next();

    /**
     * Moves the iterator to the previous record.
     */
    void Previous();

   private:
    /**
     * Constructor.
     * @param dbm_impl The database implementation object.
     */
    explicit Iterator(StdIndex<KEYTYPE, VALUETYPE, CMPTYPE>* index);

    /** Pointer to the index. */
    StdIndex<KEYTYPE, VALUETYPE, CMPTYPE>* index_;
    /** Iterator for the current record. */
    //typename std::set<std::pair<KEYTYPE, VALUETYPE>>::const_iterator it_;
    typename SETTYPE::const_iterator it_;
  };

  /**
   * Default constructor.
   */
  StdIndex() {}

  /**
   * Destructor.
   */
  ~StdIndex();

  /**
   * Copy and assignment are disabled.
   */
  explicit StdIndex(const StdIndex& rhs) = delete;
  StdIndex& operator =(const StdIndex& rhs) = delete;

  /**
   * Checks whether a record exists in the index.
   * @param key The key of the record.
   * @param value The value of the record.
   */
  bool Check(const KEYTYPE& key, const VALUETYPE& value);

  /**
   * Gets all values of records of a key.
   * @param key The key to look for.
   * @param max The maximum number of values to get.  0 means unlimited.
   * @return All values of the key.
   */
  std::vector<VALUETYPE> GetValues(const KEYTYPE& key, size_t max = 0);

  /**
   * Adds a record.
   * @param key The key of the record.  This can be an arbitrary expression to search the index.
   * @param value The value of the record.  This should be a primary value of another database.
   */
  void Add(const KEYTYPE& key, const VALUETYPE& value);

  /**
   * Removes a record.
   * @param key The key of the record.
   * @param value The value of the record.
   */
  void Remove(const KEYTYPE& key, const VALUETYPE& value);

  /**
   * Gets the number of records.
   * @return The number of records.
   */
  size_t Count();

  /**
   * Removes all records.
   */
  void Clear();

  /**
   * Makes an iterator for each record.
   * @return The iterator for each record.
   */
  std::unique_ptr<Iterator> MakeIterator();

 private:
  /** The list of the current iterators. */
  std::list<Iterator*> iterators_;
  /** Serialized records. */
  SETTYPE records_;
  /** Mutex for the records. */
  std::shared_timed_mutex mutex_;
};

/**
 * On-memory secondary index implementation with std::map for strings.
 * @details All operations are thread-safe; Multiple threads can access the same database
 * concurrently.
 * @details This is slower than StdIndex<string, string> although space efficiency is better.
 */
class StdIndexStr final {
 public:
  /**
   * Iterator for each record.
   */
  class Iterator {
    friend class StdIndexStr;
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
     */
    void First();

    /**
     * Initializes the iterator to indicate the first record.
     */
    void Last();

    /**
     * Initializes the iterator to indicate a specific range.
     * @param key The key of the lower bound.
     * @param value The value of the lower bound.
     */
    void Jump(std::string_view key, std::string_view value = "");

    /**
     * Gets the key and the value of the current record of the iterator.
     * @param key The pointer to a string object to contain the record key.  If it is nullptr,
     * the key data is ignored.
     * @param value The pointer to a string object to contain the record value.  If it is nullptr,
     * the value data is ignored.
     * @return True on success or false on failure.  If theres no record to fetch, false is
     * returned.
     */
    bool Get(std::string* key = nullptr, std::string* value = nullptr);

    /**
     * Moves the iterator to the next record.
     */
    void Next();

    /**
     * Moves the iterator to the previous record.
     */
    void Previous();

   private:
    /**
     * Constructor.
     * @param dbm_impl The database implementation object.
     */
    explicit Iterator(StdIndexStr* index);

    /** Pointer to the index. */
    StdIndexStr* index_;
    /** Iterator for the current record. */
    std::set<std::string>::const_iterator it_;
  };

  /**
   * Comparator for sorting records.
   */
  struct RecordComparator final {
    /**
     * Compares two records.
     * @param lhs The first record.
     * @param rhs The second record.
     * @return True if the first one is prior.
     */
    bool operator ()(const std::string& lhs, const std::string& rhs) const {
      return PairLexicalKeyComparator(lhs, rhs) < 0;
    }
  };

  /**
   * Default constructor.
   */
  StdIndexStr() {}

  /**
   * Destructor.
   */
  ~StdIndexStr();

  /**
   * Copy and assignment are disabled.
   */
  explicit StdIndexStr(const StdIndexStr& rhs) = delete;
  StdIndexStr& operator =(const StdIndexStr& rhs) = delete;

  /**
   * Checks whether a record exists in the index.
   * @param key The key of the record.
   * @param value The value of the record.
   */
  bool Check(std::string_view key, std::string_view value);

  /**
   * Gets all values of records of a key.
   * @param key The key to look for.
   * @param max The maximum number of values to get.  0 means unlimited.
   * @return All values of the key.
   */
  std::vector<std::string> GetValues(std::string_view key, size_t max = 0);

  /**
   * Adds a record.
   * @param key The key of the record.  This can be an arbitrary expression to search the index.
   * @param value The value of the record.  This should be a primary value of another database.
   */
  void Add(std::string_view key, std::string_view value);

  /**
   * Removes a record.
   * @param key The key of the record.
   * @param value The value of the record.
   */
  void Remove(std::string_view key, std::string_view value);

  /**
   * Gets the number of records.
   * @return The number of records.
   */
  size_t Count();

  /**
   * Removes all records.
   */
  void Clear();

  /**
   * Makes an iterator for each record.
   * @return The iterator for each record.
   */
  std::unique_ptr<Iterator> MakeIterator();

 private:
  /** The list of the current iterators. */
  std::list<Iterator*> iterators_;
  /** Serialized records. */
  std::set<std::string, RecordComparator> records_;
  /** Mutex for the records. */
  std::shared_timed_mutex mutex_;
};

inline Status FileIndex::Open(
    const std::string& path, bool writable, int32_t options,
    const TreeDBM::TuningParameters& tuning_params) {
  TreeDBM::TuningParameters mod_tuning_params = tuning_params;
  if (mod_tuning_params.align_pow < 0) {
    mod_tuning_params.align_pow = 12;
  }
  if (mod_tuning_params.max_page_size < 0) {
    mod_tuning_params.max_page_size = 4080;
  }
  if (mod_tuning_params.key_comparator == nullptr) {
    mod_tuning_params.key_comparator = PairLexicalKeyComparator;
  }
  return dbm_.OpenAdvanced(path, writable, options, mod_tuning_params);
}

inline Status FileIndex::Close() {
  return dbm_.Close();
}

inline bool FileIndex::Check(std::string_view key, std::string_view value) {
  return dbm_.Get(SerializeStrPair(key, value)) == Status::SUCCESS;
}

inline std::vector<std::string> FileIndex::GetValues(std::string_view key, size_t max) {
  std::vector<std::string> values;
  auto iter = dbm_.MakeIterator();
  iter->Jump(SerializeStrPair(key, ""));
  std::string record;
  while (true) {
    if (max > 0 && values.size() >= max) {
      break;
    }
    if (iter->Get(&record) != Status::SUCCESS) {
      break;
    }
    std::string_view rec_key, rec_value;
    DeserializeStrPair(record, &rec_key, &rec_value);
    if (rec_key != key) {
      break;
    }
    values.emplace_back(rec_value);
    iter->Next();
  }
  return values;
}

inline Status FileIndex::Add(std::string_view key, std::string_view value) {
  return dbm_.Set(SerializeStrPair(key, value), "");
}

inline Status FileIndex::Remove(std::string_view key, std::string_view value) {
  return dbm_.Remove(SerializeStrPair(key, value));
}

inline size_t FileIndex::Count() {
  return dbm_.CountSimple();
}

inline Status FileIndex::Clear() {
  return dbm_.Clear();
}

inline Status FileIndex::Rebuild() {
  return dbm_.Rebuild();
}

inline Status FileIndex::Synchronize(bool hard) {
  return dbm_.Synchronize(hard);
}

inline bool FileIndex::IsOpen() const {
  return dbm_.IsOpen();
}

inline bool FileIndex::IsWritable() const {
  return dbm_.IsWritable();
}

inline TreeDBM* FileIndex::GetInternalDBM() const {
  return const_cast<TreeDBM*>(&dbm_);
}

inline std::unique_ptr<FileIndex::Iterator> FileIndex::MakeIterator() {
  std::unique_ptr<Iterator> iter(new Iterator(dbm_.MakeIterator()));
  return iter;
}

inline FileIndex::Iterator::Iterator(std::unique_ptr<DBM::Iterator> it) : it_(std::move(it)) {}

inline FileIndex::Iterator::~Iterator() {}

inline void FileIndex::Iterator::First() {
  it_->First();
}

inline void FileIndex::Iterator::Last() {
  it_->Last();
}

inline void FileIndex::Iterator::Jump(std::string_view key, std::string_view value) {
  it_->Jump(SerializeStrPair(key, value));
}

inline bool FileIndex::Iterator::Get(std::string* key, std::string* value) {
  std::string record;
  if (it_->Get(&record) != Status::SUCCESS) {
    return false;
  }
  std::string_view rec_key, rec_value;
  DeserializeStrPair(record, &rec_key, &rec_value);
  if (key != nullptr) {
    *key = rec_key;
  }
  if (value != nullptr) {
    *value = rec_value;
  }
  return true;
}

inline void FileIndex::Iterator::Next() {
  it_->Next();
}

inline void FileIndex::Iterator::Previous() {
  it_->Previous();
}

inline MemIndex::MemIndex(KeyComparator key_comparator) : dbm_(key_comparator) {}

inline bool MemIndex::Check(std::string_view key, std::string_view value) {
  return dbm_.Get(SerializeStrPair(key, value)) == Status::SUCCESS;
}

inline std::vector<std::string> MemIndex::GetValues(std::string_view key, size_t max) {
  std::vector<std::string> values;
  auto iter = dbm_.MakeIterator();
  iter->Jump(SerializeStrPair(key, ""));
  std::string record;
  while (true) {
    if (max > 0 && values.size() >= max) {
      break;
    }
    if (iter->Get(&record) != Status::SUCCESS) {
      break;
    }
    std::string_view rec_key, rec_value;
    DeserializeStrPair(record, &rec_key, &rec_value);
    if (rec_key != key) {
      break;
    }
    values.emplace_back(rec_value);
    iter->Next();
  }
  return values;
}

inline void MemIndex::Add(std::string_view key, std::string_view value) {
  dbm_.Set(SerializeStrPair(key, value), "");
}

inline void MemIndex::Remove(std::string_view key, std::string_view value) {
  dbm_.Remove(SerializeStrPair(key, value));
}

inline size_t MemIndex::Count() {
  return dbm_.CountSimple();
}

inline void MemIndex::Clear() {
  dbm_.Clear();
}

inline std::unique_ptr<MemIndex::Iterator> MemIndex::MakeIterator() {
  std::unique_ptr<Iterator> iter(new Iterator(dbm_.MakeIterator()));
  return iter;
}

inline MemIndex::Iterator::Iterator(std::unique_ptr<DBM::Iterator> it) : it_(std::move(it)) {}

inline MemIndex::Iterator::~Iterator() {}

inline void MemIndex::Iterator::First() {
  it_->First();
}

inline void MemIndex::Iterator::Last() {
  it_->Last();
}

inline void MemIndex::Iterator::Jump(std::string_view key, std::string_view value) {
  it_->Jump(SerializeStrPair(key, value));
}

inline bool MemIndex::Iterator::Get(std::string* key, std::string* value) {
  std::string record;
  if (it_->Get(&record) != Status::SUCCESS) {
    return false;
  }
  std::string_view rec_key, rec_value;
  DeserializeStrPair(record, &rec_key, &rec_value);
  if (key != nullptr) {
    *key = rec_key;
  }
  if (value != nullptr) {
    *value = rec_value;
  }
  return true;
}

inline void MemIndex::Iterator::Next() {
  it_->Next();
}

inline void MemIndex::Iterator::Previous() {
  it_->Previous();
}

template <typename KEYTYPE, typename VALUETYPE, typename CMPTYPE>
inline StdIndex<KEYTYPE, VALUETYPE, CMPTYPE>::~StdIndex() {
  for (auto* iterator : iterators_) {
    iterator->index_ = nullptr;
  }
}

template <typename KEYTYPE, typename VALUETYPE, typename CMPTYPE>
inline bool StdIndex<KEYTYPE, VALUETYPE, CMPTYPE>::Check(
    const KEYTYPE& key, const VALUETYPE& value) {
  std::shared_lock<std::shared_timed_mutex> lock(mutex_);
  const auto& const_records = records_;
  return const_records.find(std::make_pair(key, value)) != const_records.end();
}

template <typename KEYTYPE, typename VALUETYPE, typename CMPTYPE>
inline std::vector<VALUETYPE> StdIndex<KEYTYPE, VALUETYPE, CMPTYPE>::GetValues(
    const KEYTYPE& key, size_t max) {
  std::shared_lock<std::shared_timed_mutex> lock(mutex_);
  const auto& const_records = records_;
  std::vector<VALUETYPE> values;
  auto it = records_.lower_bound(std::make_pair(key, VALUETYPE()));
  while (it != const_records.end()) {
    if (max > 0 && values.size() >= max) {
      break;
    }
    if (it->first != key) {
      break;
    }
    values.emplace_back(it->second);
    ++it;
  }
  return values;
}

template <typename KEYTYPE, typename VALUETYPE, typename CMPTYPE>
inline void StdIndex<KEYTYPE, VALUETYPE, CMPTYPE>::Add(
    const KEYTYPE& key, const VALUETYPE& value) {
  std::lock_guard<std::shared_timed_mutex> lock(mutex_);
  records_.emplace(key, value);
}

template <typename KEYTYPE, typename VALUETYPE, typename CMPTYPE>
inline void StdIndex<KEYTYPE, VALUETYPE, CMPTYPE>::Remove(
    const KEYTYPE& key, const VALUETYPE& value) {
  std::lock_guard<std::shared_timed_mutex> lock(mutex_);
  auto it = records_.find(std::make_pair(key, value));
  if (it == records_.end()) {
    return;
  }
  for (auto* iterator : iterators_) {
    if (iterator->it_ == it) {
      ++iterator->it_;
    }
  }
  it = records_.erase(it);
}

template <typename KEYTYPE, typename VALUETYPE, typename CMPTYPE>
inline size_t StdIndex<KEYTYPE, VALUETYPE, CMPTYPE>::Count() {
  std::shared_lock<std::shared_timed_mutex> lock(mutex_);
  return records_.size();
}

template <typename KEYTYPE, typename VALUETYPE, typename CMPTYPE>
inline void StdIndex<KEYTYPE, VALUETYPE, CMPTYPE>::Clear() {
  std::lock_guard<std::shared_timed_mutex> lock(mutex_);
  records_.clear();
  for (auto* iterator : iterators_) {
    iterator->it_ = records_.end();
  }
}

template <typename KEYTYPE, typename VALUETYPE, typename CMPTYPE>
std::unique_ptr<typename StdIndex<KEYTYPE, VALUETYPE, CMPTYPE>::Iterator>
StdIndex<KEYTYPE, VALUETYPE, CMPTYPE>::MakeIterator() {
  std::unique_ptr<Iterator> iter(new Iterator(this));
  return iter;
}

template <typename KEYTYPE, typename VALUETYPE, typename CMPTYPE>
StdIndex<KEYTYPE, VALUETYPE, CMPTYPE>::Iterator::Iterator(
    StdIndex<KEYTYPE, VALUETYPE, CMPTYPE>* index) : index_(index) {
  std::lock_guard<std::shared_timed_mutex> lock(index_->mutex_);
  index_->iterators_.emplace_back(this);
  it_ = index_->records_.end();
}

template <typename KEYTYPE, typename VALUETYPE, typename CMPTYPE>
StdIndex<KEYTYPE, VALUETYPE, CMPTYPE>::Iterator::~Iterator() {
  if (index_ != nullptr) {
    std::lock_guard<std::shared_timed_mutex> lock(index_->mutex_);
    index_->iterators_.remove(this);
  }
}

template <typename KEYTYPE, typename VALUETYPE, typename CMPTYPE>
void StdIndex<KEYTYPE, VALUETYPE, CMPTYPE>::Iterator::First() {
  std::shared_lock<std::shared_timed_mutex> lock(index_->mutex_);
  const auto& const_records = index_->records_;
  it_ = const_records.begin();
}

template <typename KEYTYPE, typename VALUETYPE, typename CMPTYPE>
void StdIndex<KEYTYPE, VALUETYPE, CMPTYPE>::Iterator::Last() {
  std::shared_lock<std::shared_timed_mutex> lock(index_->mutex_);
  const auto& const_records = index_->records_;
  it_ = const_records.end();
  if (it_ != const_records.begin()) {
    it_--;
  }
}

template <typename KEYTYPE, typename VALUETYPE, typename CMPTYPE>
void StdIndex<KEYTYPE, VALUETYPE, CMPTYPE>::Iterator::Jump(
    const KEYTYPE& key, const VALUETYPE& value) {
  std::shared_lock<std::shared_timed_mutex> lock(index_->mutex_);
  const auto& const_records = index_->records_;
  it_ = const_records.lower_bound(std::make_pair(key, value));
}

template <typename KEYTYPE, typename VALUETYPE, typename CMPTYPE>
bool StdIndex<KEYTYPE, VALUETYPE, CMPTYPE>::Iterator::Get(KEYTYPE* key, VALUETYPE* value) {
  std::shared_lock<std::shared_timed_mutex> lock(index_->mutex_);
  if (it_ == index_->records_.end()) {
    return false;
  }
  if (key != nullptr) {
    *key = it_->first;
  }
  if (value != nullptr) {
    *value = it_->second;
  }
  return true;
}

template <typename KEYTYPE, typename VALUETYPE, typename CMPTYPE>
void StdIndex<KEYTYPE, VALUETYPE, CMPTYPE>::Iterator::Next() {
  std::shared_lock<std::shared_timed_mutex> lock(index_->mutex_);
  const auto& const_records = index_->records_;
  if (it_ != const_records.end()) {
    ++it_;
  }
}

template <typename KEYTYPE, typename VALUETYPE, typename CMPTYPE>
void StdIndex<KEYTYPE, VALUETYPE, CMPTYPE>::Iterator::Previous() {
  std::shared_lock<std::shared_timed_mutex> lock(index_->mutex_);
  const auto& const_records = index_->records_;
  if (it_ == const_records.begin()) {
    it_ = const_records.end();
  } else {
    --it_;
  }
}

inline StdIndexStr::~StdIndexStr() {
  for (auto* iterator : iterators_) {
    iterator->index_ = nullptr;
  }
}

inline bool StdIndexStr::Check(std::string_view key, std::string_view value) {
  std::shared_lock<std::shared_timed_mutex> lock(mutex_);
  const auto& const_records = records_;
  return const_records.find(SerializeStrPair(key, value)) != const_records.end();
}

inline std::vector<std::string> StdIndexStr::GetValues(
    std::string_view key, size_t max) {
  std::shared_lock<std::shared_timed_mutex> lock(mutex_);
  const auto& const_records = records_;
  std::vector<std::string> values;
  auto it = records_.lower_bound(SerializeStrPair(key, ""));
  while (it != const_records.end()) {
    if (max > 0 && values.size() >= max) {
      break;
    }
    std::string_view rec_key, rec_value;
    DeserializeStrPair(*it, &rec_key, &rec_value);
    if (rec_key != key) {
      break;
    }
    values.emplace_back(rec_value);
    ++it;
  }
  return values;
}

inline void StdIndexStr::Add(std::string_view key, std::string_view value) {
  std::lock_guard<std::shared_timed_mutex> lock(mutex_);
  records_.emplace(SerializeStrPair(key, value));
}

inline void StdIndexStr::Remove(std::string_view key, std::string_view value) {
  std::lock_guard<std::shared_timed_mutex> lock(mutex_);
  auto it = records_.find(SerializeStrPair(key, value));
  if (it == records_.end()) {
    return;
  }
  for (auto* iterator : iterators_) {
    if (iterator->it_ == it) {
      ++iterator->it_;
    }
  }
  it = records_.erase(it);
}

inline size_t StdIndexStr::Count() {
  std::shared_lock<std::shared_timed_mutex> lock(mutex_);
  return records_.size();
}

inline void StdIndexStr::Clear() {
  std::lock_guard<std::shared_timed_mutex> lock(mutex_);
  records_.clear();
  for (auto* iterator : iterators_) {
    iterator->it_ = records_.end();
  }
}

inline std::unique_ptr<StdIndexStr::Iterator> StdIndexStr::MakeIterator() {
  std::unique_ptr<Iterator> iter(new Iterator(this));
  return iter;
}

inline StdIndexStr::Iterator::Iterator(StdIndexStr* index) : index_(index) {
  std::lock_guard<std::shared_timed_mutex> lock(index_->mutex_);
  index_->iterators_.emplace_back(this);
  it_ = index_->records_.end();
}

inline StdIndexStr::Iterator::~Iterator() {
  if (index_ != nullptr) {
    std::lock_guard<std::shared_timed_mutex> lock(index_->mutex_);
    index_->iterators_.remove(this);
  }
}

inline void StdIndexStr::Iterator::First() {
  std::shared_lock<std::shared_timed_mutex> lock(index_->mutex_);
  const auto& const_records = index_->records_;
  it_ = const_records.begin();
}

inline void StdIndexStr::Iterator::Last() {
  std::shared_lock<std::shared_timed_mutex> lock(index_->mutex_);
  const auto& const_records = index_->records_;
  it_ = const_records.end();
  if (it_ != const_records.begin()) {
    it_--;
  }
}

inline void StdIndexStr::Iterator::Jump(std::string_view key, std::string_view value) {
  std::shared_lock<std::shared_timed_mutex> lock(index_->mutex_);
  const auto& const_records = index_->records_;
  it_ = const_records.lower_bound(SerializeStrPair(key, value));
}

inline bool StdIndexStr::Iterator::Get(std::string* key, std::string* value) {
  std::shared_lock<std::shared_timed_mutex> lock(index_->mutex_);
  if (it_ == index_->records_.end()) {
    return false;
  }
  std::string_view rec_key, rec_value;
  DeserializeStrPair(*it_, &rec_key, &rec_value);
  if (key != nullptr) {
    *key = rec_key;
  }
  if (value != nullptr) {
    *value = rec_value;
  }
  return true;
}

inline void StdIndexStr::Iterator::Next() {
  std::shared_lock<std::shared_timed_mutex> lock(index_->mutex_);
  if (it_ != index_->records_.end()) {
    ++it_;
  }
}

inline void StdIndexStr::Iterator::Previous() {
  std::shared_lock<std::shared_timed_mutex> lock(index_->mutex_);
  if (it_ == index_->records_.begin()) {
    it_ = index_->records_.end();
  } else {
    --it_;
  }
}

}  // namespace tkrzw

#endif  // _TKRZW_INDEX_H

// END OF FILE
