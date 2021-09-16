/*************************************************************************************************
 * Miscellaneous data containers
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

#ifndef _TKRZW_CONTAINERS_H
#define _TKRZW_CONTAINERS_H

#include <algorithm>
#include <atomic>
#include <memory>
#include <mutex>
#include <set>
#include <utility>
#include <vector>

#include <cinttypes>
#include <cstring>

#include "tkrzw_lib_common.h"
#include "tkrzw_thread_util.h"

namespace tkrzw {

/**
 * Doubly-linked hash map.
 * @param KEYTYPE the key type.
 * @param VALUETYPE the value type.
 * @param HASHTYPE the hash functor.
 * @param EQUALTOTYPE the equality checking functor.
 */
template <typename KEYTYPE, typename VALUETYPE,
          typename HASHTYPE = std::hash<KEYTYPE>, typename EQUALTOTYPE = std::equal_to<KEYTYPE>>
class LinkedHashMap final {
 public:
  /** The default value of the number of buckets. */
  static constexpr size_t DEFAULT_NUM_BUCKETS = 101;

  /**
   * Record data.
   */
  struct Record final {
    /** The key. */
    KEYTYPE key;
    /** The value. */
    VALUETYPE value;
    /** The child record. */
    Record* child;
    /** The previous record. */
    Record* prev;
    /** The next record. */
    Record* next;

    /**
     * Constructor.
     * @param key The key.
     * @param value The value.
     */
    Record(const KEYTYPE& key, const VALUETYPE& value) :
        key(key), value(value), child(nullptr), prev(nullptr), next(nullptr) {
    }
  };

  /**
   * Const iterator of records.
   * @details The iterator is invalidated when the current record is removed.
   */
  class ConstIterator final {
    friend class LinkedHashMap;
   public:
    /**
     * Copy constructor.
     * @param rhs The right-hand-side object.
     */
    ConstIterator(const ConstIterator& rhs) : map_(rhs.map_), rec_(rhs.rec_) {}

    /**
     * Gets the reference to the current record.
     * @return The reference to the current record.
     */
    const Record& operator *() const {
      return *rec_;
    }

    /**
     * Accesses a member of the current record.
     * @return The pointer to the current record.
     */
    const Record* operator->() const {
      return rec_;
    }

    /**
     * Assignment operator from the self type.
     * @param rhs The right-hand-side object.
     * @return The reference to itself.
     */
    ConstIterator& operator =(const ConstIterator& rhs) {
      if (&rhs == this) return *this;
      map_ = rhs.map_;
      rec_ = rhs.rec_;
      return *this;
    }

    /**
     * Equality operator with the self type.
     * @param rhs The right-hand-side object.
     * @return True if the both are equal, or false if not.
     */
    bool operator ==(const ConstIterator& rhs) const {
      return map_ == rhs.map_ && rec_ == rhs.rec_;
    }

    /**
     * Non-equality operator with the self type.
     * @param rhs The right-hand-side object.
     * @return False if the both are equal, or true if not.
     */
    bool operator !=(const ConstIterator& rhs) const {
      return map_ != rhs.map_ || rec_ != rhs.rec_;
    }

    /**
     * Preposting increment operator.
     * @return The iterator itself.
     */
    ConstIterator& operator ++() {
      rec_ = rec_->next;
      return *this;
    }

    /**
     * Postpositive increment operator.
     * @return An iterator of the old position.
     */
    ConstIterator operator ++(int) {
      ConstIterator old(*this);
      rec_ = rec_->next;
      return old;
    }

    /**
     * Preposting decrement operator.
     * @return The iterator itself.
     */
    ConstIterator& operator --() {
      if (rec_) {
        rec_ = rec_->prev;
      } else {
        rec_ = map_->last_;
      }
      return *this;
    }

    /**
     * Postpositive decrement operator.
     * @return An iterator of the old position.
     */
    ConstIterator operator --(int) {
      ConstIterator old(*this);
      if (rec_) {
        rec_ = rec_->prev;
      } else {
        rec_ = map_->last_;
      }
      return old;
    }

   private:
    /**
     * Constructor.
     * @param map The container.
     * @param rec The pointer to the current record.
     */
    ConstIterator(const LinkedHashMap* map, const Record* rec) : map_(map), rec_(rec) {}

    /** The container. */
    const LinkedHashMap* map_;
    /** The current record. */
    const Record* rec_;
  };

  /**
   * Iterator of records.
   * @details The iterator is invalidated when the current record is removed.
   */
  class Iterator final {
    friend class LinkedHashMap;
   public:
    /**
     * Copy constructor.
     * @param rhs The right-hand-side object.
     */
    Iterator(const Iterator& rhs) : map_(rhs.map_), rec_(rhs.rec_) {}

    /**
     * Cast operator to the const iterator.
     * @return The casted const iterator.
     */
    operator ConstIterator() const {
      return ConstIterator(map_, rec_);
    }

    /**
     * Gets the reference to the current record.
     * @return The reference to the current record.
     */
    Record& operator *() {
      return *rec_;
    }

    /**
     * Gets the reference to the current record.
     * @return The reference to the current record.
     */
    const Record& operator *() const {
      return *rec_;
    }

    /**
     * Accesses a member of the current record.
     * @return The pointer to the current record.
     */
    Record* operator->() {
      return rec_;
    }

    /**
     * Accesses a member of the current record.
     * @return The pointer to the current record.
     */
    Record* operator->() const {
      return rec_;
    }

    /**
     * Assignment operator from the self type.
     * @param rhs The right-hand-side object.
     * @return The reference to itself.
     */
    Iterator& operator =(const Iterator& rhs) {
      if (&rhs == this) return *this;
      map_ = rhs.map_;
      rec_ = rhs.rec_;
      return *this;
    }

    /**
     * Equality operator with the self type.
     * @param rhs The right-hand-side object.
     * @return True if the both are equal, or false if not.
     */
    bool operator ==(const Iterator& rhs) const {
      return map_ == rhs.map_ && rec_ == rhs.rec_;
    }

    /**
     * Equality operator with the const type.
     * @param rhs The right-hand-side object.
     * @return True if the both are equal, or false if not.
     */
    bool operator ==(const ConstIterator& rhs) const {
      return map_ == rhs.map_ && rec_ == rhs.rec_;
    }

    /**
     * Non-equality operator with the self type.
     * @param rhs The right-hand-side object.
     * @return False if the both are equal, or true if not.
     */
    bool operator !=(const Iterator& rhs) const {
      return map_ != rhs.map_ || rec_ != rhs.rec_;
    }

    /**
     * Non-equality operator with the const type.
     * @param rhs The right-hand-side object.
     * @return False if the both are equal, or true if not.
     */
    bool operator !=(const ConstIterator& rhs) const {
      return map_ != rhs.map_ || rec_ != rhs.rec_;
    }

    /**
     * Preposting increment operator.
     * @return The iterator itself.
     */
    Iterator& operator ++() {
      rec_ = rec_->next;
      return *this;
    }

    /**
     * Postpositive increment operator.
     * @return An iterator of the old position.
     */
    Iterator operator ++(int) {
      Iterator old(*this);
      rec_ = rec_->next;
      return old;
    }

    /**
     * Preposting decrement operator.
     * @return The iterator itself.
     */
    Iterator& operator --() {
      if (rec_) {
        rec_ = rec_->prev;
      } else {
        rec_ = map_->last_;
      }
      return *this;
    }

    /**
     * Postpositive decrement operator.
     * @return An iterator of the old position.
     */
    Iterator operator --(int) {
      Iterator old(*this);
      if (rec_) {
        rec_ = rec_->prev;
      } else {
        rec_ = map_->last_;
      }
      return old;
    }

   private:
    /**
     * Constructor.
     * @param map The container.
     * @param rec The pointer to the current record.
     */
    Iterator(LinkedHashMap* map, Record* rec) : map_(map), rec_(rec) {}

    /** The container. */
    LinkedHashMap* map_;
    /** The current record. */
    Record* rec_;
  };

  /**
   * Enumeration for move modes.
   */
  enum MoveMode : int32_t {
    /** To keep the current position. */
    MOVE_CURRENT = 0,
    /** To move to the first. */
    MOVE_FIRST = 1,
    /** To move to the last. */
    MOVE_LAST = 2,
  };

  /**
   * Default constructor.
   */
  LinkedHashMap();

  /**
   * Constructor.
   * @param num_buckets The number of buckets of the hash table.
   */
  explicit LinkedHashMap(size_t num_buckets);

  /**
   * Destructor.
   */
  ~LinkedHashMap();

  /**
   * Copy and assignment are disabled.
   */
  explicit LinkedHashMap(const LinkedHashMap& rhs) = delete;
  LinkedHashMap& operator =(const LinkedHashMap& rhs) = delete;

  /**
   * Retrieves a record.
   * @param key The key.
   * @param mode The moving mode.
   * @return The pointer to the corresponding record, or nullptr on failure.
   */
  Record* Get(const KEYTYPE& key, MoveMode mode = MOVE_CURRENT);

  /**
   * Retrieves a record.
   * @param key The key.
   * @param default_value The value to be returned on failure.
   * @param mode The moving mode.
   * @return The value of the record or the default value on failure.
   */
  VALUETYPE GetSimple(const KEYTYPE& key, VALUETYPE default_value = VALUETYPE(),
                      MoveMode mode = MOVE_CURRENT);

  /**
   * Stores a record.
   * @param key The key.
   * @param value The value.
   * @param mode The moving mode.
   * @param overwrite Whether to overwrite the existing value.
   * @return The pointer to the stored record or the existing record.
   */
  Record* Set(const KEYTYPE& key, const VALUETYPE& value,
              bool overwrite = true, MoveMode mode = MOVE_CURRENT);

  /**
   * Removes a record.
   * @param key The key.
   * @return True on success, or false on failure.
   */
  bool Remove(const KEYTYPE& key);

  /**
   * Migrates a record to another map.
   * @param key The key.
   * @param dest The destination map.
   * @param mode The moving mode.
   * @return The pointer to the migrated record, or nullptr on failure.
   */
  Record* Migrate(const KEYTYPE& key, LinkedHashMap* dest, MoveMode mode);

  /**
   * Removes all records.
   */
  void clear();

  /**
   * Gets the number of records.
   * @return The number of records.
   */
  size_t size() const;

  /**
   * Checks whether no records exist.
   * @return True if there's no record or false if there are one or more records.
   */
  bool empty() const;

  /**
   * Gets an iterator at the first record.
   * @return The iterator at the first record.
   */
  Iterator begin();

  /**
   * Gets a const iterator at the first record.
   * @return The const iterator at the first record.
   */
  ConstIterator begin() const;

  /**
   * Gets an iterator of the end sentry.
   * @return The iterator at the end sentry.
   */
  Iterator end();

  /**
   * Gets a const iterator of the end sentry.
   * @return The const iterator at the end sentry.
   */
  ConstIterator end() const;

  /**
   * Gets an iterator at a record.
   * @param key The key of the record to find.
   * @return The pointer to the value of the corresponding record, or nullptr on failure.
   */
  Iterator find(const KEYTYPE& key);

  /**
   * Gets an iterator at a record.
   * @param key The key of the record to find.
   * @return The pointer to the value of the corresponding record, or nullptr on failure.
   */
  ConstIterator find(const KEYTYPE& key) const;

  /**
   * Refers to a record.
   * @param key The key of the record to find.
   * @return The reference to the record.
   * @details If there's no matching record. a new record is created.
   */
  VALUETYPE& operator[](const KEYTYPE& key);

  /**
   * Insert a record without overwriting an existing record.
   * @param record The record to insert.
   * @return A pair.  The first element is an iterator to the inserted or existing element.
   * The second element is a boolean of whether the insertion is successful.
   */
  std::pair<Iterator, bool> insert(const Record& record);

  /**
   * Removes a record.
   * @param key The key.
   * @return 1 on success or 0 if there's no matching key.
   */
  size_t erase(const KEYTYPE& key);

  /**
   * Gets the reference of the first record.
   * @return The reference of the first record.
   */
  Record& front();

  /**
   * Gets the reference of the first record.
   * @return The reference of the first record.
   */
  const Record& front() const;

  /**
   * Gets the reference of the last record.
   * @return The reference of the last record.
   */
  Record& back();

  /**
   * Gets the reference of the last record.
   * @return The reference of the last record.
   */
  const Record& back() const;

  /**
   * Rehashes all records to a new number of records.
   * @param num_buckets The new number of buckets.
   */
  void rehash(size_t num_buckets);

  /**
   * Gets average number of elements per bucket.
   * @return The average number of elements per bucket.
   */
  float load_factor() const;

 private:
  /**
   * Initialize fields.
   */
  void Initialize();

  /**
   * Clean up fields.
   */
  void Destroy();

  /** The functor of the hash function. */
  HASHTYPE hash_;
  /** The functor of the equalto function. */
  EQUALTOTYPE equalto_;
  /** The bucket array. */
  Record** buckets_;
  /** The number of buckets. */
  size_t num_buckets_;
  /** The first record. */
  Record* first_;
  /** The last record. */
  Record* last_;
  /** The number of records. */
  size_t num_records_;
};

/**
 * LRU cache.
 * @param VALUETYPE the value type.
 */
template <class VALUETYPE>
class LRUCache final {
  typedef LinkedHashMap<int64_t, std::shared_ptr<VALUETYPE>> CacheMap;
 public:
  /**
   * Iterator to access each record.
   * @details The iterator is invalidated when the current record is removed.
   */
  class Iterator final {
    friend class LRUCache<VALUETYPE>;
   public:
    /**
     * Get the value of the current record.
     * @param id The pointer to store the ID of the record.  If it is nullptr, it is ignored.
     * @return A shared pointer to the value object.  It points to nullptr on failure.
     */
    std::shared_ptr<VALUETYPE> Get(int64_t* id);

    /**
     * Moves the iterator to the next record.
     */
    void Next();

   private:
    /**
     * Constructor.
     * @param map The pointer to the cache map.
     */
    Iterator(CacheMap* cache);

    CacheMap* cache_;
    typename CacheMap::Iterator it_;
  };

  /**
   * Constructor.
   * @param capacity The maximum number of records in the cache.
   */
  LRUCache(size_t capacity);

  /**
   * Adds a record.
   * @param id The ID of the record.
   * @param value The pointer to the value object.  Ownership is taken.
   * @return A shared pointer to the value object.
   * @details If there is an existing record of the same ID, the new value object is just deleted
   * and the return value refers to the existing record.
   */
  std::shared_ptr<VALUETYPE> Add(int64_t id, VALUETYPE* value);

  /**
   * Gives back a record which has been removed.
   * @param id The ID of the record.
   * @param value The moved pointer to the value object.  Ownership is taken.
   */
  void GiveBack(int64_t id, std::shared_ptr<VALUETYPE>&& value);

  /**
   * Gets the value of a record.
   * @param id The ID of the record.
   * @return A shared pointer to the value object.  It points to nullptr on failure.
   */
  std::shared_ptr<VALUETYPE> Get(int64_t id);

  /**
   * Removes a record.
   * @param id The ID of the record.
   */
  void Remove(int64_t id);

  /**
   * Removes the least recent used record.
   * @param id The pointer to store the ID of the record.  If it is nullptr, it is ignored.
   * @return A shared pointer to the value object.  It points to nullptr on failure.
   */
  std::shared_ptr<VALUETYPE> RemoveLRU(int64_t* id = nullptr);

  /**
   * Gets the number of records.
   * @return The number of records.
   */
  size_t Size() const;

  /**
   * Checks whether no records exist.
   * @return True if there's no record or false if there are one or more records.
   */
  bool IsEmpty() const;

  /**
   * Checks whether stored records exceeds the capacity.
   * @return True if stored records exceeds the capacity or false if not.
   */
  bool IsSaturated() const;

  /**
   * Removes all records.
   */
  void Clear();

  /**
   * Makes an iterator for each record.
   * @return The iterator for each record.
   */
  Iterator MakeIterator();

 private:
  size_t capacity_;
  CacheMap cache_;
};

/**
 * Double-layered LRU cache.
 * @param VALUETYPE the value type.
 */
template <class VALUETYPE>
class DoubleLRUCache final {
  typedef LinkedHashMap<int64_t, std::shared_ptr<VALUETYPE>> CacheMap;
 public:
  /**
   * Iterator to access each record.
   * @details The iterator is invalidated when the current record is removed.
   */
  class Iterator final {
    friend class DoubleLRUCache<VALUETYPE>;
   public:
    /**
     * Get the value of the current record.
     * @param id The pointer to store the ID of the record.  If it is nullptr, it is ignored.
     * @return A shared pointer to the value object.  It points to nullptr on failure.
     */
    std::shared_ptr<VALUETYPE> Get(int64_t* id);

    /**
     * Moves the iterator to the next record.
     */
    void Next();

   private:
    /**
     * Constructor.
     * @param hot The pointer to the hot cache.
     * @param warm The pointer to the warm cache.
     */
    Iterator(CacheMap* hot, CacheMap* warm);

    CacheMap* hot_;
    CacheMap* warm_;
    bool on_hot_;
    typename CacheMap::Iterator it_;
  };

  /**
   * Constructor.
   * @param hot_capacity The maximum number of records in the hot cache.
   * @param warm_capacity The maximum number of records in the worm cache.
   */
  DoubleLRUCache(size_t hot_capacity, size_t warm_capacity);

  /**
   * Adds a record.
   * @param id The ID of the record.
   * @param value The pointer to the value object.  Ownership is taken.
   * @return A shared pointer to the value object.
   * @details If there is an existing record of the same ID, the new value object is just deleted
   * and the return value refers to the existing record.
   */
  std::shared_ptr<VALUETYPE> Add(int64_t id, VALUETYPE* value);

  /**
   * Gives back a record which has been removed.
   * @param id The ID of the record.
   * @param value The moved pointer to the value object.  Ownership is taken.
   */
  void GiveBack(int64_t id, std::shared_ptr<VALUETYPE>&& value);

  /**
   * Gets the value of a record.
   * @param id The ID of the record.
   * @param promotion True if the record can be promoted to the hot list.
   * @return A shared pointer to the value object.  It points to nullptr on failure.
   */
  std::shared_ptr<VALUETYPE> Get(int64_t id, bool promotion);

  /**
   * Removes a record.
   * @param id The ID of the record.
   */
  void Remove(int64_t id);

  /**
   * Removes the least recent used record.
   * @param id The pointer to store the ID of the record.  If it is nullptr, it is ignored.
   * @return A shared pointer to the value object.  It points to nullptr on failure.
   */
  std::shared_ptr<VALUETYPE> RemoveLRU(int64_t* id = nullptr);

  /**
   * Gets the number of records.
   * @return The number of records.
   */
  size_t Size() const;

  /**
   * Checks whether no records exist.
   * @return True if there's no record or false if there are one or more records.
   */
  bool IsEmpty() const;

  /**
   * Checks whether stored records exceeds the capacity.
   * @return True if stored records exceeds the capacity or false if not.
   */
  bool IsSaturated() const;

  /**
   * Removes all records.
   */
  void Clear();

  /**
   * Makes an iterator for each record.
   * @return The iterator for each record.
   */
  Iterator MakeIterator();

 private:
  size_t hot_capacity_;
  size_t warm_capacity_;
  CacheMap hot_;
  CacheMap warm_;
};

/**
 * Thread-safe wrapper of std::set.
 */
template <class VALUETYPE>
class AtomicSet final {
 public:
  /**
   * Constructor.
   */
  AtomicSet();

  /**
   * Checks whether a record exists.
   * @param data The record data.
   * @return True if the record exists or false if not.
   */
  bool Check(const VALUETYPE& data);

  /**
   * Inserts a record.
   * @param data The record data.
   * @return True if the insertion is sucesss or false on failure.
   */
  bool Insert(const VALUETYPE& data);

  /**
   * Inserts a record in move semantics.
   * @param data The record data.
   * @return True if the insertion is sucesss or false on failure.
   */
  bool Insert(VALUETYPE&& data);

  /**
   * Removes a record.
   * @param data The record data.
   * @return True on success or false on failure.
   */
  bool Remove(const VALUETYPE& data);

  /**
   * Checks whether the set is empty.
   * @return True if the set is empty or false if not.
   */
  bool IsEmpty() const;

  /**
   * Removes the least ID from the set.
   * @return The least ID or the default value on failure.
   */
  VALUETYPE Pop();

  /**
   * Clears all IDs from the set.
   */
  void Clear();

 private:
  std::set<VALUETYPE> set_;
  std::atomic_bool empty_;
  SpinMutex mutex_;
};

/**
 * Adds a pair of a cont and a payload to a heap vector.
 * @param cost The cost.
 * @param payload The payload.
 * @param capacity The capacity of the heap vector.
 * @param heap The pointer to the heap vector.
 */
template<typename C, typename T>
void HeapByCostAdd(const C& cost, const T& payload, size_t capacity,
                   std::vector<std::pair<C, T>>* heap) {
  if (heap->size() >= capacity) {
    const auto& front = heap->front();
    if (cost > front.first || (cost == front.first && payload >= front.second)) {
      return;
    }
    std::pop_heap(heap->begin(), heap->end());
    heap->pop_back();
  }
  heap->emplace_back(std::make_pair(cost, payload));
  std::push_heap(heap->begin(), heap->end());
}

/**
 * Finishes a heap vector to be in sorted order.
 * @param heap The heap vector.
 */
template<typename C, typename T>
void HeapByCostFinish(std::vector<std::pair<C, T>>* heap) {
  auto it = heap->end();
  while (it > heap->begin()) {
    std::pop_heap(heap->begin(), it);
    it--;
  }
}

template <typename KEYTYPE, typename VALUETYPE, typename HASHTYPE, typename EQUALTOTYPE>
inline LinkedHashMap<KEYTYPE, VALUETYPE, HASHTYPE, EQUALTOTYPE>::LinkedHashMap() :
    buckets_(nullptr), num_buckets_(DEFAULT_NUM_BUCKETS),
    first_(nullptr), last_(nullptr), num_records_(0) {
  Initialize();
}

template <typename KEYTYPE, typename VALUETYPE, typename HASHTYPE, typename EQUALTOTYPE>
inline LinkedHashMap<KEYTYPE, VALUETYPE, HASHTYPE, EQUALTOTYPE>::LinkedHashMap(
    size_t num_buckets) :
    buckets_(nullptr), num_buckets_(num_buckets),
    first_(nullptr), last_(nullptr), num_records_(0) {
  if (num_buckets_ < 1) {
    num_buckets_ = DEFAULT_NUM_BUCKETS;
  }
  Initialize();
}

template <typename KEYTYPE, typename VALUETYPE, typename HASHTYPE, typename EQUALTOTYPE>
inline LinkedHashMap<KEYTYPE, VALUETYPE, HASHTYPE, EQUALTOTYPE>::~LinkedHashMap() {
  Destroy();
}

template <typename KEYTYPE, typename VALUETYPE, typename HASHTYPE, typename EQUALTOTYPE>
inline typename LinkedHashMap<KEYTYPE, VALUETYPE, HASHTYPE, EQUALTOTYPE>::Record*
LinkedHashMap<KEYTYPE, VALUETYPE, HASHTYPE, EQUALTOTYPE>::Get(
    const KEYTYPE& key, MoveMode mode) {
  const size_t bucket_index = hash_(key) % num_buckets_;
  Record* rec = buckets_[bucket_index];
  while (rec) {
    if (equalto_(rec->key, key)) {
      switch (mode) {
        default: {
          break;
        }
        case MOVE_FIRST: {
          if (first_ != rec) {
            if (last_ == rec) {
              last_ = rec->prev;
            }
            if (rec->prev != nullptr) {
              rec->prev->next = rec->next;
            }
            if (rec->next != nullptr) {
              rec->next->prev = rec->prev;
            }
            rec->prev = nullptr;
            rec->next = first_;
            first_->prev = rec;
            first_ = rec;
          }
          break;
        }
        case MOVE_LAST: {
          if (last_ != rec) {
            if (first_ == rec) {
              first_ = rec->next;
            }
            if (rec->prev != nullptr) {
              rec->prev->next = rec->next;
            }
            if (rec->next != nullptr) {
              rec->next->prev = rec->prev;
            }
            rec->prev = last_;
            rec->next = nullptr;
            last_->next = rec;
            last_ = rec;
          }
          break;
        }
      }
      return rec;
    } else {
      rec = rec->child;
    }
  }
  return nullptr;
}

template <typename KEYTYPE, typename VALUETYPE, typename HASHTYPE, typename EQUALTOTYPE>
VALUETYPE LinkedHashMap<KEYTYPE, VALUETYPE, HASHTYPE, EQUALTOTYPE>::GetSimple(
    const KEYTYPE& key, VALUETYPE default_value, MoveMode mode) {
  const Record* rec = Get(key, mode);
  return rec == nullptr ? default_value : rec->value;
}

template <typename KEYTYPE, typename VALUETYPE, typename HASHTYPE, typename EQUALTOTYPE>
typename LinkedHashMap<KEYTYPE, VALUETYPE, HASHTYPE, EQUALTOTYPE>::Record *
LinkedHashMap<KEYTYPE, VALUETYPE, HASHTYPE, EQUALTOTYPE>::Set(
    const KEYTYPE& key, const VALUETYPE& value, bool overwrite, MoveMode mode) {
  const size_t bucket_index = hash_(key) % num_buckets_;
  Record* rec = buckets_[bucket_index];
  while (rec) {
    if (equalto_(rec->key, key)) {
      if (!overwrite) {
        return rec;
      }
      rec->value = value;
      switch (mode) {
        default: {
          break;
        }
        case MOVE_FIRST: {
          if (first_ != rec) {
            if (last_ == rec) {
              last_ = rec->prev;
            }
            if (rec->prev != nullptr) {
              rec->prev->next = rec->next;
            }
            if (rec->next != nullptr) {
              rec->next->prev = rec->prev;
            }
            rec->prev = nullptr;
            rec->next = first_;
            first_->prev = rec;
            first_ = rec;
          }
          break;
        }
        case MOVE_LAST: {
          if (last_ != rec) {
            if (first_ == rec) {
              first_ = rec->next;
            }
            if (rec->prev != nullptr) {
              rec->prev->next = rec->next;
            }
            if (rec->next != nullptr) {
              rec->next->prev = rec->prev;
            }
            rec->prev = last_;
            rec->next = nullptr;
            last_->next = rec;
            last_ = rec;
          }
          break;
        }
      }
      return rec;
    } else {
      rec = rec->child;
    }
  }
  rec = new Record(key, value);
  switch (mode) {
    default: {
      rec->prev = last_;
      if (first_ == nullptr) {
        first_ = rec;
      }
      if (last_ != nullptr) {
        last_->next = rec;
      }
      last_ = rec;
      break;
    }
    case MOVE_FIRST: {
      rec->next = first_;
      if (last_ == nullptr) {
        last_ = rec;
      }
      if (first_ != nullptr) {
        first_->prev = rec;
      }
      first_ = rec;
      break;
    }
  }
  rec->child = buckets_[bucket_index];
  buckets_[bucket_index] = rec;
  num_records_++;
  return rec;
}

template <typename KEYTYPE, typename VALUETYPE, typename HASHTYPE, typename EQUALTOTYPE>
inline bool LinkedHashMap<KEYTYPE, VALUETYPE, HASHTYPE, EQUALTOTYPE>::Remove(
    const KEYTYPE& key) {
  const size_t bucket_index = hash_(key) % num_buckets_;
  Record* rec = buckets_[bucket_index];
  Record** entry_ptr = buckets_ + bucket_index;
  while (rec) {
    if (equalto_(rec->key, key)) {
      if (rec->prev != nullptr) {
        rec->prev->next = rec->next;
      }
      if (rec->next != nullptr) {
        rec->next->prev = rec->prev;
      }
      if (rec == first_) {
        first_ = rec->next;
      }
      if (rec == last_) {
        last_ = rec->prev;
      }
      *entry_ptr = rec->child;
      num_records_--;
      delete rec;
      return true;
    } else {
      entry_ptr = &rec->child;
      rec = rec->child;
    }
  }
  return false;
}

template <typename KEYTYPE, typename VALUETYPE, typename HASHTYPE, typename EQUALTOTYPE>
inline typename LinkedHashMap<KEYTYPE, VALUETYPE, HASHTYPE, EQUALTOTYPE>::Record*
LinkedHashMap<KEYTYPE, VALUETYPE, HASHTYPE, EQUALTOTYPE>::Migrate(
    const KEYTYPE& key, LinkedHashMap* dest, MoveMode mode) {
  size_t hash = hash_(key);
  size_t bucket_index = hash % num_buckets_;
  Record* rec = buckets_[bucket_index];
  Record** entry_ptr = buckets_ + bucket_index;
  while (rec) {
    if (equalto_(rec->key, key)) {
      if (rec->prev != nullptr) {
        rec->prev->next = rec->next;
      }
      if (rec->next != nullptr) {
        rec->next->prev = rec->prev;
      }
      if (rec == first_) {
        first_ = rec->next;
      }
      if (rec == last_) {
        last_ = rec->prev;
      }
      *entry_ptr = rec->child;
      num_records_--;
      rec->child = nullptr;
      rec->prev = nullptr;
      rec->next = nullptr;
      bucket_index = hash % dest->num_buckets_;
      Record* dest_rec = dest->buckets_[bucket_index];
      entry_ptr = dest->buckets_ + bucket_index;
      while (dest_rec) {
        if (dest->equalto_(dest_rec->key, key)) {
          if (dest_rec->child != nullptr) {
            rec->child = dest_rec->child;
          }
          if (dest_rec->prev != nullptr) {
            rec->prev = dest_rec->prev;
            rec->prev->next = rec;
          }
          if (dest_rec->next != nullptr) {
            rec->next = dest_rec->next;
            rec->next->prev = rec;
          }
          if (dest->first_ == dest_rec) {
            dest->first_ = rec;
          }
          if (dest->last_ == dest_rec) {
            dest->last_ = rec;
          }
          *entry_ptr = rec;
          delete dest_rec;
          switch (mode) {
            default: {
              break;
            }
            case MOVE_FIRST: {
              if (dest->first_ != rec) {
                if (dest->last_ == rec) {
                  dest->last_ = rec->prev;
                }
                if (rec->prev != nullptr) {
                  rec->prev->next = rec->next;
                }
                if (rec->next != nullptr) {
                  rec->next->prev = rec->prev;
                }
                rec->prev = nullptr;
                rec->next = dest->first_;
                dest->first_->prev = rec;
                dest->first_ = rec;
              }
              break;
            }
            case MOVE_LAST: {
              if (dest->last_ != rec) {
                if (dest->first_ == rec) {
                  dest->first_ = rec->next;
                }
                if (rec->prev != nullptr) {
                  rec->prev->next = rec->next;
                }
                if (rec->next != nullptr) {
                  rec->next->prev = rec->prev;
                }
                rec->prev = dest->last_;
                rec->next = nullptr;
                dest->last_->next = rec;
                dest->last_ = rec;
              }
              break;
            }
          }
          return rec;
        } else {
          entry_ptr = &dest_rec->child;
          dest_rec = dest_rec->child;
        }
      }
      switch (mode) {
        default: {
          rec->prev = dest->last_;
          if (dest->first_ == nullptr) {
            dest->first_ = rec;
          }
          if (dest->last_ != nullptr) {
            dest->last_->next = rec;
          }
          dest->last_ = rec;
          break;
        }
        case MOVE_FIRST: {
          rec->next = dest->first_;
          if (dest->last_ == nullptr) {
            dest->last_ = rec;
          }
          if (dest->first_ != nullptr) {
            dest->first_->prev = rec;
          }
          dest->first_ = rec;
          break;
        }
      }
      *entry_ptr = rec;
      dest->num_records_++;
      return rec;
    } else {
      entry_ptr = &rec->child;
      rec = rec->child;
    }
  }
  return nullptr;
}

template <typename KEYTYPE, typename VALUETYPE, typename HASHTYPE, typename EQUALTOTYPE>
inline void LinkedHashMap<KEYTYPE, VALUETYPE, HASHTYPE, EQUALTOTYPE>::clear() {
  if (num_records_ < 1) return;
  Record* rec = last_;
  while (rec) {
    Record* prev = rec->prev;
    delete rec;
    rec = prev;
  }
  for (size_t i = 0; i < num_buckets_; i++) {
    buckets_[i] = nullptr;
  }
  first_ = nullptr;
  last_ = nullptr;
  num_records_ = 0;
}

template <typename KEYTYPE, typename VALUETYPE, typename HASHTYPE, typename EQUALTOTYPE>
inline size_t LinkedHashMap<KEYTYPE, VALUETYPE, HASHTYPE, EQUALTOTYPE>::size() const {
  return num_records_;
}

template <typename KEYTYPE, typename VALUETYPE, typename HASHTYPE, typename EQUALTOTYPE>
inline bool LinkedHashMap<KEYTYPE, VALUETYPE, HASHTYPE, EQUALTOTYPE>::empty() const {
  return num_records_ == 0;
}

template <typename KEYTYPE, typename VALUETYPE, typename HASHTYPE, typename EQUALTOTYPE>
inline typename LinkedHashMap<KEYTYPE, VALUETYPE, HASHTYPE, EQUALTOTYPE>::Iterator
LinkedHashMap<KEYTYPE, VALUETYPE, HASHTYPE, EQUALTOTYPE>::begin() {
  return Iterator(this, first_);
}

template <typename KEYTYPE, typename VALUETYPE, typename HASHTYPE, typename EQUALTOTYPE>
inline typename LinkedHashMap<KEYTYPE, VALUETYPE, HASHTYPE, EQUALTOTYPE>::ConstIterator
LinkedHashMap<KEYTYPE, VALUETYPE, HASHTYPE, EQUALTOTYPE>::begin() const {
  return ConstIterator(this, first_);
}

template <typename KEYTYPE, typename VALUETYPE, typename HASHTYPE, typename EQUALTOTYPE>
inline typename LinkedHashMap<KEYTYPE, VALUETYPE, HASHTYPE, EQUALTOTYPE>::Iterator
LinkedHashMap<KEYTYPE, VALUETYPE, HASHTYPE, EQUALTOTYPE>::end() {
  return Iterator(this, nullptr);
}

template <typename KEYTYPE, typename VALUETYPE, typename HASHTYPE, typename EQUALTOTYPE>
inline typename LinkedHashMap<KEYTYPE, VALUETYPE, HASHTYPE, EQUALTOTYPE>::ConstIterator
LinkedHashMap<KEYTYPE, VALUETYPE, HASHTYPE, EQUALTOTYPE>::end() const {
  return ConstIterator(this, nullptr);
}

template <typename KEYTYPE, typename VALUETYPE, typename HASHTYPE, typename EQUALTOTYPE>
inline typename LinkedHashMap<KEYTYPE, VALUETYPE, HASHTYPE, EQUALTOTYPE>::Iterator
LinkedHashMap<KEYTYPE, VALUETYPE, HASHTYPE, EQUALTOTYPE>::find(
    const KEYTYPE& key) {
  const size_t bucket_index = hash_(key) % num_buckets_;
  Record* rec = buckets_[bucket_index];
  while (rec) {
    if (equalto_(rec->key, key)) {
      return Iterator(this, rec);
    } else {
      rec = rec->child;
    }
  }
  return Iterator(this, nullptr);
}

template <typename KEYTYPE, typename VALUETYPE, typename HASHTYPE, typename EQUALTOTYPE>
inline typename LinkedHashMap<KEYTYPE, VALUETYPE, HASHTYPE, EQUALTOTYPE>::ConstIterator
LinkedHashMap<KEYTYPE, VALUETYPE, HASHTYPE, EQUALTOTYPE>::find(
    const KEYTYPE& key) const {
  const size_t bucket_index = hash_(key) % num_buckets_;
  Record* rec = buckets_[bucket_index];
  while (rec) {
    if (equalto_(rec->key, key)) {
      return ConstIterator(this, rec);
    } else {
      rec = rec->child;
    }
  }
  return ConstIterator(this, nullptr);
}

template <typename KEYTYPE, typename VALUETYPE, typename HASHTYPE, typename EQUALTOTYPE>
inline VALUETYPE& LinkedHashMap<KEYTYPE, VALUETYPE, HASHTYPE, EQUALTOTYPE>::operator[](
    const KEYTYPE& key) {
  return Set(key, VALUETYPE())->value;
}

template <typename KEYTYPE, typename VALUETYPE, typename HASHTYPE, typename EQUALTOTYPE>
inline std::pair<typename LinkedHashMap<KEYTYPE, VALUETYPE, HASHTYPE, EQUALTOTYPE>::Iterator, bool>
LinkedHashMap<KEYTYPE, VALUETYPE, HASHTYPE, EQUALTOTYPE>::insert(const Record& record) {
  const size_t bucket_index = hash_(record.key) % num_buckets_;
  Record* rec = buckets_[bucket_index];
  while (rec) {
    if (equalto_(rec->key, record.key)) {
      return std::make_pair(Iterator(this, rec), false);
    } else {
      rec = rec->child;
    }
  }
  rec = new Record(record.key, record.value);
  rec->prev = last_;
  if (first_ == nullptr) {
    first_ = rec;
  }
  if (last_ != nullptr) {
    last_->next = rec;
  }
  last_ = rec;
  rec->child = buckets_[bucket_index];
  buckets_[bucket_index] = rec;
  num_records_++;
  return std::make_pair(Iterator(this, rec), true);
}

template <typename KEYTYPE, typename VALUETYPE, typename HASHTYPE, typename EQUALTOTYPE>
inline size_t LinkedHashMap<KEYTYPE, VALUETYPE, HASHTYPE, EQUALTOTYPE>::erase(
    const KEYTYPE& key) {
  return Remove(key) ? 1 : 0;
}

template <typename KEYTYPE, typename VALUETYPE, typename HASHTYPE, typename EQUALTOTYPE>
inline typename LinkedHashMap<KEYTYPE, VALUETYPE, HASHTYPE, EQUALTOTYPE>::Record&
LinkedHashMap<KEYTYPE, VALUETYPE, HASHTYPE, EQUALTOTYPE>::front() {
  return *first_;
}

template <typename KEYTYPE, typename VALUETYPE, typename HASHTYPE, typename EQUALTOTYPE>
inline const typename LinkedHashMap<KEYTYPE, VALUETYPE, HASHTYPE, EQUALTOTYPE>::Record&
LinkedHashMap<KEYTYPE, VALUETYPE, HASHTYPE, EQUALTOTYPE>::front() const {
  return *first_;
}

template <typename KEYTYPE, typename VALUETYPE, typename HASHTYPE, typename EQUALTOTYPE>
inline typename LinkedHashMap<KEYTYPE, VALUETYPE, HASHTYPE, EQUALTOTYPE>::Record&
LinkedHashMap<KEYTYPE, VALUETYPE, HASHTYPE, EQUALTOTYPE>::back() {
  return *last_;
}

template <typename KEYTYPE, typename VALUETYPE, typename HASHTYPE, typename EQUALTOTYPE>
inline const typename LinkedHashMap<KEYTYPE, VALUETYPE, HASHTYPE, EQUALTOTYPE>::Record&
LinkedHashMap<KEYTYPE, VALUETYPE, HASHTYPE, EQUALTOTYPE>::back() const {
  return *last_;
}

template <typename KEYTYPE, typename VALUETYPE, typename HASHTYPE, typename EQUALTOTYPE>
void LinkedHashMap<KEYTYPE, VALUETYPE, HASHTYPE, EQUALTOTYPE>::rehash(size_t num_buckets) {
  xfree(buckets_);
  num_buckets_ = num_buckets;
  Initialize();
  for (Record* rec = first_; rec != nullptr; rec = rec->next) {
    const size_t bucket_index = hash_(rec->key) % num_buckets_;
    rec->child = buckets_[bucket_index];
    buckets_[bucket_index] = rec;
  }
}

template <typename KEYTYPE, typename VALUETYPE, typename HASHTYPE, typename EQUALTOTYPE>
float LinkedHashMap<KEYTYPE, VALUETYPE, HASHTYPE, EQUALTOTYPE>::load_factor() const {
  return num_records_ / num_buckets_;
}

template <typename KEYTYPE, typename VALUETYPE, typename HASHTYPE, typename EQUALTOTYPE>
inline void LinkedHashMap<KEYTYPE, VALUETYPE, HASHTYPE, EQUALTOTYPE>::Initialize() {
  buckets_ = static_cast<Record**>(xcalloc(num_buckets_, sizeof(*buckets_)));
}

template <typename KEYTYPE, typename VALUETYPE, typename HASHTYPE, typename EQUALTOTYPE>
inline void LinkedHashMap<KEYTYPE, VALUETYPE, HASHTYPE, EQUALTOTYPE>::Destroy() {
  Record* rec = last_;
  while (rec) {
    Record* prev = rec->prev;
    delete rec;
    rec = prev;
  }
  xfree(buckets_);
}

template <typename VALUETYPE>
inline LRUCache<VALUETYPE>::LRUCache(size_t capacity) : capacity_(capacity) {}

template <typename VALUETYPE>
inline std::shared_ptr<VALUETYPE> LRUCache<VALUETYPE>::Add(int64_t id, VALUETYPE* value) {
  return cache_.Set(id, std::shared_ptr<VALUETYPE>(value), false, CacheMap::MOVE_LAST)->value;
}

template <typename VALUETYPE>
inline void LRUCache<VALUETYPE>::GiveBack(int64_t id, std::shared_ptr<VALUETYPE>&& value) {
  cache_.Set(id, std::move(value), false, CacheMap::MOVE_LAST);
}

template <typename VALUETYPE>
inline std::shared_ptr<VALUETYPE> LRUCache<VALUETYPE>::Get(int64_t id) {
  auto *rec = cache_.Get(id, CacheMap::MOVE_LAST);
  if (rec != nullptr) {
    return rec->value;
  }
  return std::shared_ptr<VALUETYPE>(nullptr);
}

template <typename VALUETYPE>
inline void LRUCache<VALUETYPE>::Remove(int64_t id) {
  cache_.Remove(id);
}

template <typename VALUETYPE>
inline std::shared_ptr<VALUETYPE> LRUCache<VALUETYPE>::RemoveLRU(int64_t* id) {
  if (!cache_.empty()) {
    auto& rec = cache_.front();
    if (id != nullptr) {
      *id = rec.key;
    }
    std::shared_ptr<VALUETYPE> value = rec.value;
    cache_.Remove(rec.key);
    return value;
  }
  return std::shared_ptr<VALUETYPE>(nullptr);
}

template <typename VALUETYPE>
inline size_t LRUCache<VALUETYPE>::Size() const {
  return cache_.size();
}

template <typename VALUETYPE>
inline bool LRUCache<VALUETYPE>::IsEmpty() const {
  return cache_.empty();
}

template <typename VALUETYPE>
inline bool LRUCache<VALUETYPE>::IsSaturated() const {
  return cache_.size() > capacity_;
}

template <typename VALUETYPE>
inline void LRUCache<VALUETYPE>::Clear() {
  cache_.clear();
}

template <typename VALUETYPE>
inline typename LRUCache<VALUETYPE>::Iterator LRUCache<VALUETYPE>::MakeIterator() {
  return Iterator(&cache_);
}

template <typename VALUETYPE>
inline LRUCache<VALUETYPE>::Iterator::Iterator(CacheMap* cache)
    : cache_(cache), it_(cache->begin()) {}

template <typename VALUETYPE>
inline std::shared_ptr<VALUETYPE> LRUCache<VALUETYPE>::Iterator::Get(int64_t* id) {
  if (it_ == cache_->end()) {
    return nullptr;
  }
  if (id != nullptr) {
    *id = it_->key;
  }
  return it_->value;
}

template <typename VALUETYPE>
inline void LRUCache<VALUETYPE>::Iterator::Next() {
  ++it_;
}

template <typename VALUETYPE>
inline DoubleLRUCache<VALUETYPE>::DoubleLRUCache(size_t hot_capacity, size_t warm_capacity)
    : hot_capacity_(hot_capacity), warm_capacity_(warm_capacity),
      hot_(hot_capacity * 2 + 1), warm_(warm_capacity * 2 + 1) {}

template <typename VALUETYPE>
inline std::shared_ptr<VALUETYPE> DoubleLRUCache<VALUETYPE>::Add(int64_t id, VALUETYPE* value) {
  auto* old_rec = hot_.Get(id);
  if (old_rec != nullptr) {
    delete value;
    return old_rec->value;
  }
  return warm_.Set(id, std::shared_ptr<VALUETYPE>(value), false, CacheMap::MOVE_LAST)->value;
}

template <typename VALUETYPE>
inline void DoubleLRUCache<VALUETYPE>::GiveBack(int64_t id, std::shared_ptr<VALUETYPE>&& value) {
  warm_.Set(id, std::move(value), false, CacheMap::MOVE_LAST);
}

template <typename VALUETYPE>
inline std::shared_ptr<VALUETYPE> DoubleLRUCache<VALUETYPE>::Get(int64_t id, bool promotion) {
  auto* rec = hot_.Get(id, CacheMap::MOVE_LAST);
  if (rec != nullptr) {
    return rec->value;
  }
  if (promotion) {
    if (hot_.size() >= hot_capacity_) {
      hot_.Migrate(hot_.front().key, &warm_, CacheMap::MOVE_LAST);
    }
    rec = warm_.Migrate(id, &hot_, CacheMap::MOVE_LAST);
    if (rec != nullptr) {
      return rec->value;
    }
  } else {
    rec = warm_.Get(id, CacheMap::MOVE_LAST);
    if (rec != nullptr) {
      return rec->value;
    }
  }
  return std::shared_ptr<VALUETYPE>(nullptr);
}

template <typename VALUETYPE>
inline void DoubleLRUCache<VALUETYPE>::Remove(int64_t id) {
  if (!hot_.Remove(id)) {
    warm_.Remove(id);
  }
}

template <typename VALUETYPE>
inline std::shared_ptr<VALUETYPE> DoubleLRUCache<VALUETYPE>::RemoveLRU(int64_t* id) {
  if (!warm_.empty()) {
    auto& rec = warm_.front();
    if (id != nullptr) {
      *id = rec.key;
    }
    std::shared_ptr<VALUETYPE> value = rec.value;
    warm_.Remove(rec.key);
    return value;
  }
  if (!hot_.empty()) {
    auto& rec = hot_.front();
    if (id != nullptr) {
      *id = rec.key;
    }
    std::shared_ptr<VALUETYPE> value = rec.value;
    hot_.Remove(rec.key);
    return value;
  }
  return std::shared_ptr<VALUETYPE>(nullptr);
}

template <typename VALUETYPE>
inline size_t DoubleLRUCache<VALUETYPE>::Size() const {
  return hot_.size() + warm_.size();
}

template <typename VALUETYPE>
inline bool DoubleLRUCache<VALUETYPE>::IsEmpty() const {
  return hot_.empty() && warm_.empty();
}

template <typename VALUETYPE>
inline bool DoubleLRUCache<VALUETYPE>::IsSaturated() const {
  return warm_.size() > warm_capacity_;
}

template <typename VALUETYPE>
inline void DoubleLRUCache<VALUETYPE>::Clear() {
  hot_.clear();
  warm_.clear();
}

template <typename VALUETYPE>
inline typename DoubleLRUCache<VALUETYPE>::Iterator DoubleLRUCache<VALUETYPE>::MakeIterator() {
  return Iterator(&hot_, &warm_);
}

template <typename VALUETYPE>
inline DoubleLRUCache<VALUETYPE>::Iterator::Iterator(CacheMap* hot, CacheMap* warm)
    : hot_(hot), warm_(warm), on_hot_(true), it_(hot->begin()) {
  if (it_ == hot_->end()) {
    on_hot_ = false;
    it_ = warm_->begin();
  }
}

template <typename VALUETYPE>
inline std::shared_ptr<VALUETYPE> DoubleLRUCache<VALUETYPE>::Iterator::Get(int64_t* id) {
  if (on_hot_) {
    if (it_ == hot_->end()) {
      return nullptr;
    }
    if (id != nullptr) {
      *id = it_->key;
    }
    return it_->value;
  }
  if (it_ == warm_->end()) {
    return nullptr;
  }
  if (id != nullptr) {
    *id = it_->key;
  }
  return it_->value;
}

template <typename VALUETYPE>
inline void DoubleLRUCache<VALUETYPE>::Iterator::Next() {
  if (on_hot_) {
    ++it_;
    if (it_ == hot_->end()) {
      on_hot_ = false;
      it_ = warm_->begin();
    }
  } else {
    ++it_;
  }
}

template <typename VALUETYPE>
inline AtomicSet<VALUETYPE>::AtomicSet() : set_(), empty_(true), mutex_() {}

template <typename VALUETYPE>
bool AtomicSet<VALUETYPE>::Check(const VALUETYPE& data) {
  std::lock_guard<SpinMutex> lock(mutex_);
  return set_.find(data) != set_.end();
}

template <typename VALUETYPE>
inline bool AtomicSet<VALUETYPE>::Insert(const VALUETYPE& data) {
  std::lock_guard<SpinMutex> lock(mutex_);
  empty_.store(false);
  return set_.emplace(data).second;
}

template <typename VALUETYPE>
inline bool AtomicSet<VALUETYPE>::Insert(VALUETYPE&& data) {
  std::lock_guard<SpinMutex> lock(mutex_);
  empty_.store(false);
  return set_.emplace(data).second;
}

template <typename VALUETYPE>
bool AtomicSet<VALUETYPE>::Remove(const VALUETYPE& data) {
  std::lock_guard<SpinMutex> lock(mutex_);
  if (set_.erase(data) == 0) {
    return false;
  }
  if (set_.empty()) {
    empty_.store(true);
  }
  return true;
}

template <typename VALUETYPE>
inline bool AtomicSet<VALUETYPE>::IsEmpty() const {
  return empty_.load();
}

template <typename VALUETYPE>
inline VALUETYPE AtomicSet<VALUETYPE>::Pop() {
  std::lock_guard<SpinMutex> lock(mutex_);
  if (set_.empty()) {
    return VALUETYPE();
  }
  auto it = set_.begin();
  const VALUETYPE data = *it;
  set_.erase(it);
  if (set_.empty()) {
    empty_.store(true);
  }
  return data;
}

template <typename VALUETYPE>
inline void AtomicSet<VALUETYPE>::Clear() {
  std::lock_guard<SpinMutex> lock(mutex_);
  set_.clear();
  empty_.store(true);
}

}  // namespace tkrzw

#endif  // _TKRZW_CONTAINER_H

// END OF FILE
