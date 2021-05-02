/*************************************************************************************************
 * On-memory database manager implementations with LRU deletion
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

#include "tkrzw_sys_config.h"

#include "tkrzw_dbm.h"
#include "tkrzw_dbm_common_impl.h"
#include "tkrzw_dbm_cache.h"
#include "tkrzw_file.h"
#include "tkrzw_file_mmap.h"
#include "tkrzw_file_pos.h"
#include "tkrzw_file_std.h"
#include "tkrzw_file_util.h"
#include "tkrzw_lib_common.h"
#include "tkrzw_str_util.h"
#include "tkrzw_thread_util.h"

namespace tkrzw {

constexpr int32_t NUM_CACHE_SLOTS = 32;
constexpr double MAX_LOAD_FACTOR = 1.2;

struct CacheRecord final {
  char* child;
  char* prev;
  char* next;
  int32_t key_size;
  const char* key_ptr;
  int32_t value_size;
  const char* value_ptr;
  char* Serialize() const;
  char* Reserialize(char* ptr, int32_t old_value_size) const;
  void Deserialize(const char* ptr);
  static char* GetChild(char* ptr);
  static void SetChild(char* ptr, const char* child);
  static char* GetPrev(char* ptr);
  static void SetPrev(char* ptr, const char* prev);
  static char* GetNext(char* ptr);
  static void SetNext(char* ptr, const char* next);
};

class CacheSlot final {
 public:
  CacheSlot();
  void Init(int64_t cap_rec_num, int64_t cap_mem_size);
  void CleanUp();
  void Process(std::string_view key, uint64_t hash, DBM::RecordProcessor* proc, bool writable);
  void ProcessEach(DBM::RecordProcessor* proc, bool writable);
  int64_t Count();
  int64_t GetEffectiveDataSize();
  int64_t GetMemoryUsage();
  int64_t GetMemoryUsageImpl();
  void Rebuild(int64_t cap_rec_num, int64_t cap_mem_size);
  Status ExportRecords(FlatRecord* flat_rec);
  void RemoveLRU();
  std::vector<std::string> GetKeys();

 private:
  char** buckets_;
  char* first_;
  char* last_;
  int64_t cap_rec_num_;
  int64_t cap_mem_size_;
  int64_t num_buckets_;
  int64_t num_records_;
  int64_t eff_data_size_;
  std::mutex mutex_;
};

class CacheDBMImpl final {
  friend class CacheDBMIteratorImpl;
  typedef std::list<CacheDBMIteratorImpl*> IteratorList;
 public:
  CacheDBMImpl(std::unique_ptr<File> file, int64_t cap_rec_num, int64_t cap_mem_size);
  ~CacheDBMImpl();
  Status Open(const std::string& path, bool writable, int32_t options);
  Status Close();
  Status Process(std::string_view key, DBM::RecordProcessor* proc, bool writable);
  Status ProcessEach(DBM::RecordProcessor* proc, bool writable);
  Status Count(int64_t* count);
  Status GetFileSize(int64_t* size);
  Status GetFilePath(std::string* path);
  Status Clear();
  Status Rebuild(int64_t cap_rec_num, int64_t cap_mem_size);
  Status Synchronize(bool hard, DBM::FileProcessor* proc);
  std::vector<std::pair<std::string, std::string>> Inspect();
  bool IsOpen();
  bool IsWritable();
  std::unique_ptr<DBM> MakeDBM();
  int64_t GetEffectiveDataSize();
  int64_t GetMemoryUsage();

 private:
  void CancelIterators();
  void InitAllSlots();
  void CleanUpAllSlots();
  Status ImportRecords();
  Status ExportRecords();
  Status ReadNextBucketRecords(CacheDBMIteratorImpl* iter);

  IteratorList iterators_;
  std::unique_ptr<File> file_;
  bool open_;
  bool writable_;
  std::string path_;
  int64_t cap_rec_num_;
  int64_t cap_mem_size_;
  CacheSlot slots_[NUM_CACHE_SLOTS];
  std::shared_timed_mutex mutex_;
};

class CacheDBMIteratorImpl final {
  friend class CacheDBMImpl;
 public:
  explicit CacheDBMIteratorImpl(CacheDBMImpl* dbm);
  ~CacheDBMIteratorImpl();
  Status First();
  Status Jump(std::string_view key);
  Status Next();
  Status Process(DBM::RecordProcessor* proc, bool writable);

 private:
  Status ReadKeys();

  CacheDBMImpl* dbm_;
  std::atomic_int64_t slot_index_;
  std::vector<std::string> keys_;
};

char* CacheRecord::Serialize() const {
  const int32_t size = sizeof(child) + sizeof(prev) + sizeof(next) +
      SizeVarNum(key_size) + key_size +
      SizeVarNum(value_size) + value_size;
  char* ptr = static_cast<char*>(xmalloc(size));
  char* wp = ptr;
  std::memcpy(wp, &child, sizeof(child));
  wp += sizeof(child);
  std::memcpy(wp, &prev, sizeof(prev));
  wp += sizeof(prev);
  std::memcpy(wp, &next, sizeof(next));
  wp += sizeof(next);
  wp += WriteVarNum(wp, key_size);
  std::memcpy(wp, key_ptr, key_size);
  wp += key_size;
  wp += WriteVarNum(wp, value_size);
  std::memcpy(wp, value_ptr, value_size);
  return ptr;
}

char* CacheRecord::Reserialize(char* ptr, int32_t old_value_size) const {
  const int32_t old_value_header_size = SizeVarNum(old_value_size);
  const int32_t new_value_header_size = SizeVarNum(value_size);
  if (new_value_header_size > old_value_header_size) {
    char* new_ptr = Serialize();
    xfree(ptr);
    return new_ptr;
  }
  if (value_size > old_value_size) {
    const int32_t size = sizeof(child) + sizeof(prev) + sizeof(next) +
        SizeVarNum(key_size) + key_size +
        SizeVarNum(value_size) + value_size;
    ptr = static_cast<char*>(xrealloc(ptr, size));
  }
  char* wp = ptr + sizeof(child) + sizeof(prev) + sizeof(next) + SizeVarNum(key_size) + key_size;
  wp += WriteVarNum(wp, value_size);
  std::memcpy(wp, value_ptr, value_size);
  return ptr;
}

void CacheRecord::Deserialize(const char* ptr) {
  const char* rp = ptr;
  std::memcpy(&child, rp, sizeof(child));
  rp += sizeof(child);
  std::memcpy(&prev, rp, sizeof(prev));
  rp += sizeof(prev);
  std::memcpy(&next, rp, sizeof(next));
  rp += sizeof(next);
  uint64_t num = 0;
  rp += ReadVarNum(rp, &num);
  key_size = num;
  key_ptr = rp;
  rp += key_size;
  rp += ReadVarNum(rp, &num);
  value_size = num;
  value_ptr = rp;
}

char* CacheRecord::GetChild(char* ptr) {
  const char* rp = ptr;
  char* child;
  std::memcpy(&child, rp, sizeof(child));
  return child;
}

void CacheRecord::SetChild(char* ptr, const char* child) {
  char* wp = ptr;
  std::memcpy(wp, &child, sizeof(child));
}

char* CacheRecord::GetPrev(char* ptr) {
  const char* rp = ptr + sizeof(char*);
  char* prev;
  std::memcpy(&prev, rp, sizeof(prev));
  return prev;
}

void CacheRecord::SetPrev(char* ptr, const char* prev) {
  char* wp = ptr + sizeof(char*);
  std::memcpy(wp, &prev, sizeof(prev));
}

char* CacheRecord::GetNext(char* ptr) {
  const char* rp = ptr + sizeof(char*) + sizeof(char*);
  char* next;
  std::memcpy(&next, rp, sizeof(next));
  return next;
}

void CacheRecord::SetNext(char* ptr, const char* next) {
  char* wp = ptr + sizeof(char*) + sizeof(char*);
  std::memcpy(wp, &next, sizeof(next));
}

CacheSlot::CacheSlot() :
    buckets_(nullptr), first_(nullptr), last_(nullptr),
    cap_rec_num_(0), cap_mem_size_(0), num_buckets_(0), num_records_(0),
    eff_data_size_(0), mutex_() {}

void CacheSlot::Init(int64_t cap_rec_num, int64_t cap_mem_size) {
  cap_rec_num_ = cap_rec_num;
  cap_mem_size_ = cap_mem_size;
  num_buckets_ = GetHashBucketSize(std::min<int64_t>(cap_rec_num_ * MAX_LOAD_FACTOR, INT32MAX));
  buckets_ = static_cast<char**>(xcalloc(num_buckets_, sizeof(*buckets_)));
  first_ = nullptr;
  last_ = nullptr;
  num_records_ = 0;
  eff_data_size_ = 0;
}

void CacheSlot::CleanUp() {
  if (buckets_ == nullptr) {
    return;
  }
  char* ptr = first_;
  while (ptr != nullptr) {
    char* next = CacheRecord::GetNext(ptr);
    xfree(ptr);
    ptr = next;
  }
  xfree(buckets_);
  buckets_ = nullptr;
  first_ = nullptr;
  last_ = nullptr;
  num_records_ = 0;
  eff_data_size_ = 0;
}

void CacheSlot::Process(
    std::string_view key, uint64_t hash, DBM::RecordProcessor* proc, bool writable) {
  std::lock_guard<std::mutex> lock(mutex_);
  const int32_t bucket_index = hash % num_buckets_;
  CacheRecord rec;
  char* top = buckets_[bucket_index];
  char* parent = nullptr;
  char* ptr = top;
  while (ptr != nullptr) {
    rec.Deserialize(ptr);
    const std::string_view rec_key(rec.key_ptr, rec.key_size);
    const std::string_view rec_value(rec.value_ptr, rec.value_size);
    if (key == rec_key) {
      if (last_ != ptr) {
        if (rec.prev != nullptr) {
          CacheRecord::SetNext(rec.prev, rec.next);
        }
        if (rec.next != nullptr) {
          CacheRecord::SetPrev(rec.next, rec.prev);
        }
        if (first_ == ptr) {
          first_ = rec.next;
        }
        CacheRecord::SetNext(last_, ptr);
        rec.prev = last_;
        CacheRecord::SetPrev(ptr, last_);
        rec.next = nullptr;
        CacheRecord::SetNext(ptr, nullptr);
        last_ = ptr;
      }
      std::string_view new_value = proc->ProcessFull(key, rec_value);
      if (new_value.data() != DBM::RecordProcessor::NOOP.data() && writable) {
        if (new_value.data() == DBM::RecordProcessor::REMOVE.data()) {
          xfree(ptr);
          if (parent == nullptr) {
            buckets_[bucket_index] = rec.child;
          } else {
            CacheRecord::SetChild(parent, rec.child);
          }
          if (rec.prev == nullptr) {
            first_ = nullptr;
            last_ = nullptr;
          } else {
            CacheRecord::SetNext(rec.prev, nullptr);
            last_ = rec.prev;
          }
          num_records_--;
          eff_data_size_ -= rec.key_size + rec.value_size;
        } else {
          const int32_t diff_size = new_value.size() - rec.value_size;
          rec.value_ptr = new_value.data();
          rec.value_size = new_value.size();
          char* new_ptr = rec.Reserialize(ptr, rec_value.size());
          if (new_ptr != ptr) {
            if (parent == nullptr) {
              buckets_[bucket_index] = new_ptr;
            } else {
              CacheRecord::SetChild(parent, new_ptr);
            }
            if (rec.prev != nullptr) {
              CacheRecord::SetNext(rec.prev, new_ptr);
            }
            if (first_ == ptr) {
              first_ = new_ptr;
            }
            last_ = new_ptr;
          }
          eff_data_size_ += diff_size;
        }
      }
      return;
    }
    parent = ptr;
    ptr = rec.child;
  }
  const std::string_view new_value = proc->ProcessEmpty(key);
  if (new_value.data() != DBM::RecordProcessor::NOOP.data() &&
      new_value.data() != DBM::RecordProcessor::REMOVE.data() && writable) {
    rec.child = top;
    rec.prev = last_;
    rec.next = nullptr;
    rec.key_ptr = key.data();
    rec.key_size = key.size();
    rec.value_ptr = new_value.data();
    rec.value_size = new_value.size();
    char* new_ptr = rec.Serialize();
    buckets_[bucket_index] = new_ptr;
    if (first_ == nullptr) {
      first_ = new_ptr;
    }
    if (last_ != nullptr) {
      CacheRecord::SetNext(last_, new_ptr);
    }
    last_ = new_ptr;
    num_records_++;
    eff_data_size_ += key.size() + new_value.size();
    if (num_records_ > cap_rec_num_ || GetMemoryUsageImpl() > cap_mem_size_) {
      RemoveLRU();
    }
  }
}

void CacheSlot::ProcessEach(DBM::RecordProcessor* proc, bool writable) {
  if (writable) {
    std::vector<std::string> keys;
    keys.reserve(num_records_);
    {
      std::lock_guard<std::mutex> lock(mutex_);
      char* ptr = first_;
      while (ptr != nullptr) {
        CacheRecord rec;
        rec.Deserialize(ptr);
        keys.emplace_back(std::string(rec.key_ptr, rec.key_size));
        ptr = rec.next;
      }
    }
    for (const auto& key : keys) {
      const uint64_t hash = PrimaryHash(key, UINT64MAX) >> 8;
      Process(key, hash, proc, true);
    }
  } else {
    std::lock_guard<std::mutex> lock(mutex_);
    char* ptr = first_;
    while (ptr != nullptr) {
      CacheRecord rec;
      rec.Deserialize(ptr);
      const std::string_view key(rec.key_ptr, rec.key_size);
      const std::string_view value(rec.value_ptr, rec.value_size);
      proc->ProcessFull(key, value);
      ptr = rec.next;
    }
  }
}

int64_t CacheSlot::Count() {
  std::lock_guard<std::mutex> lock(mutex_);
  return num_records_;
}

int64_t CacheSlot::GetEffectiveDataSize() {
  std::lock_guard<std::mutex> lock(mutex_);
  return eff_data_size_;
}

int64_t CacheSlot::GetMemoryUsage() {
  std::lock_guard<std::mutex> lock(mutex_);
  return GetMemoryUsageImpl();
}

int64_t CacheSlot::GetMemoryUsageImpl() {
  constexpr int32_t bucket_footprint = sizeof(char*);
  constexpr int32_t record_footprint = sizeof(char*) * 3 + sizeof(uint8_t) * 2;
  constexpr int32_t alloc_footprint = sizeof(void*);
  return num_buckets_ * bucket_footprint + num_records_ * (record_footprint + alloc_footprint) +
      eff_data_size_;
}

void CacheSlot::Rebuild(int64_t cap_rec_num, int64_t cap_mem_size) {
  std::lock_guard<std::mutex> lock(mutex_);
  cap_rec_num_ = cap_rec_num;
  cap_mem_size_ = cap_mem_size;
  num_buckets_ = GetHashBucketSize(std::min<int64_t>(cap_rec_num_ * MAX_LOAD_FACTOR, INT32MAX));
  xfree(buckets_);
  buckets_ = static_cast<char**>(xcalloc(num_buckets_, sizeof(*buckets_)));
  num_records_ = 0;
  eff_data_size_ = 0;
  char* ptr = first_;
  last_ = nullptr;
  while (ptr != nullptr) {
    char* next = CacheRecord::GetNext(ptr);
    if (num_records_ < cap_rec_num_ && GetMemoryUsageImpl() < cap_mem_size_) {
      CacheRecord rec;
      rec.Deserialize(ptr);
      const std::string_view key(rec.key_ptr, rec.key_size);
      const uint64_t hash = PrimaryHash(key, UINT64MAX) >> 8;
      const int32_t bucket_index = hash % num_buckets_;
      CacheRecord::SetChild(ptr, buckets_[bucket_index]);
      buckets_[bucket_index] = ptr;
      num_records_++;
      eff_data_size_ += rec.key_size + rec.value_size;
      last_ = ptr;
    } else {
      xfree(ptr);
    }
    ptr = next;
  }
  if (last_ == nullptr) {
    first_ = nullptr;
  } else {
    CacheRecord::SetNext(last_, nullptr);
  }
}

Status CacheSlot::ExportRecords(FlatRecord* flat_rec) {
  std::lock_guard<std::mutex> lock(mutex_);
  char* ptr = first_;
  while (ptr != nullptr) {
    CacheRecord rec;
    rec.Deserialize(ptr);
    Status status = flat_rec->Write(std::string_view(rec.key_ptr, rec.key_size));
    if (status != Status::SUCCESS) {
      return status;
    }
    status = flat_rec->Write(std::string_view(rec.value_ptr, rec.value_size));
    if (status != Status::SUCCESS) {
      return status;
    }
    ptr = CacheRecord::GetNext(ptr);
  }
  return Status(Status::SUCCESS);
}

void CacheSlot::RemoveLRU() {
  CacheRecord first_rec;
  first_rec.Deserialize(first_);
  const std::string_view key(first_rec.key_ptr, first_rec.key_size);
  const uint64_t hash = PrimaryHash(key, UINT64MAX) >> 8;
  const int32_t bucket_index = hash % num_buckets_;
  CacheRecord rec;
  char* top = buckets_[bucket_index];
  char* parent = nullptr;
  char* ptr = top;
  while (ptr != nullptr) {
    rec.Deserialize(ptr);
    if (ptr == first_) {
      xfree(ptr);
      if (parent == nullptr) {
        buckets_[bucket_index] = rec.child;
      } else {
        CacheRecord::SetChild(parent, rec.child);
      }
      if (rec.next != nullptr) {
        CacheRecord::SetPrev(rec.next, nullptr);
      }
      first_ = rec.next;
      if (last_ == ptr) {
        last_ = nullptr;
      }
      num_records_--;
      eff_data_size_ -= rec.key_size + rec.value_size;
      return;
    }
    parent = ptr;
    ptr = rec.child;
  }
}

std::vector<std::string> CacheSlot::GetKeys() {
  std::lock_guard<std::mutex> lock(mutex_);
  std::vector<std::string> keys;
  keys.reserve(num_records_);
  char* ptr = first_;
  while (ptr != nullptr) {
    CacheRecord rec;
    rec.Deserialize(ptr);
    keys.emplace_back(std::string(rec.key_ptr, rec.key_size));
    ptr = CacheRecord::GetNext(ptr);
  }
  return keys;
}

CacheDBMImpl::CacheDBMImpl(std::unique_ptr<File> file, int64_t cap_rec_num, int64_t cap_mem_size)
    : file_(std::move(file)), open_(false), writable_(false), path_(),
      cap_rec_num_(cap_rec_num > 0 ? cap_rec_num : CacheDBM::DEFAULT_CAP_REC_NUM),
      cap_mem_size_(cap_mem_size > 0 ? cap_mem_size : INT64MAX),
      slots_(), mutex_() {
  InitAllSlots();
}

CacheDBMImpl::~CacheDBMImpl() {
  if (open_) {
    Close();
  }
  for (auto* iterator : iterators_) {
    iterator->dbm_ = nullptr;
  }
  CleanUpAllSlots();
}

Status CacheDBMImpl::Open(const std::string& path, bool writable, int32_t options) {
  std::lock_guard<std::shared_timed_mutex> lock(mutex_);
  if (open_) {
    return Status(Status::PRECONDITION_ERROR, "opened database");
  }
  const std::string norm_path = NormalizePath(path);
  Status status = file_->Open(norm_path, writable, options);
  if (status != Status::SUCCESS) {
    return status;
  }
  status = ImportRecords();
  if (status != Status::SUCCESS) {
    file_->Close();
    return status;
  }
  open_ = true;
  writable_ = writable;
  path_ = norm_path;
  return Status(Status::SUCCESS);
}

Status CacheDBMImpl::Close() {
  std::lock_guard<std::shared_timed_mutex> lock(mutex_);
  if (!open_) {
    return Status(Status::PRECONDITION_ERROR, "not opened database");
  }
  Status status(Status::SUCCESS);
  if (writable_) {
    status |= ExportRecords();
  }
  status |= file_->Close();
  CleanUpAllSlots();
  CancelIterators();
  InitAllSlots();
  open_ = false;
  writable_ = false;
  path_.clear();
  return status;
}

Status CacheDBMImpl::Process(std::string_view key, DBM::RecordProcessor* proc, bool writable) {
  std::shared_lock<std::shared_timed_mutex> lock(mutex_);
  uint64_t hash = PrimaryHash(key, UINT64MAX);
  const int32_t slot_index = (hash & 0xff) % NUM_CACHE_SLOTS;
  hash >>= 8;
  slots_[slot_index].Process(key, hash, proc, writable);
  return Status(Status::SUCCESS);
}

Status CacheDBMImpl::ProcessEach(DBM::RecordProcessor* proc, bool writable) {
  std::shared_lock<std::shared_timed_mutex> lock(mutex_);
  proc->ProcessEmpty(DBM::RecordProcessor::NOOP);
  for (auto& slot : slots_) {
    slot.ProcessEach(proc, writable);
  }
  proc->ProcessEmpty(DBM::RecordProcessor::NOOP);
  return Status(Status::SUCCESS);
}

Status CacheDBMImpl::Count(int64_t* count) {
  std::shared_lock<std::shared_timed_mutex> lock(mutex_);
  *count = 0;
  for (auto& slot : slots_) {
    *count += slot.Count();
  }
  return Status(Status::SUCCESS);
}

Status CacheDBMImpl::GetFileSize(int64_t* size) {
  std::shared_lock<std::shared_timed_mutex> lock(mutex_);
  if (!open_) {
    return Status(Status::PRECONDITION_ERROR, "not opened database");
  }
  *size = file_->GetSizeSimple();
  return Status(Status::SUCCESS);
}

Status CacheDBMImpl::GetFilePath(std::string* path) {
  std::shared_lock<std::shared_timed_mutex> lock(mutex_);
  if (!open_) {
    return Status(Status::PRECONDITION_ERROR, "not opened database");
  }
  *path = path_;
  return Status(Status::SUCCESS);
}

Status CacheDBMImpl::Clear() {
  std::lock_guard<std::shared_timed_mutex> lock(mutex_);
  CleanUpAllSlots();
  CancelIterators();
  InitAllSlots();
  return Status(Status::SUCCESS);
}

Status CacheDBMImpl::Rebuild(int64_t cap_rec_num, int64_t cap_mem_size) {
  std::lock_guard<std::shared_timed_mutex> lock(mutex_);
  if (cap_rec_num > 0) {
    cap_rec_num_ = cap_rec_num;
  }
  if (cap_mem_size > 0) {
    cap_mem_size_ = cap_mem_size;
  }
  const int64_t slot_cap_rec_num = cap_rec_num_ / NUM_CACHE_SLOTS + 1;
  const int64_t slot_cap_mem_size = cap_mem_size_ / NUM_CACHE_SLOTS + 1;
  for (auto& slot : slots_) {
    slot.Rebuild(slot_cap_rec_num, slot_cap_mem_size);
  }
  CancelIterators();
  return Status(Status::SUCCESS);
}

Status CacheDBMImpl::Synchronize(bool hard, DBM::FileProcessor* proc) {
  std::lock_guard<std::shared_timed_mutex> lock(mutex_);
  Status status(Status::SUCCESS);
  if (open_ && writable_) {
    status |= ExportRecords();
    status |= file_->Synchronize(hard);
    if (proc != nullptr) {
      proc->Process(path_);
    }
  }
  return status;
}

std::vector<std::pair<std::string, std::string>> CacheDBMImpl::Inspect() {
  std::lock_guard<std::shared_timed_mutex> lock(mutex_);
  std::vector<std::pair<std::string, std::string>> meta;
  auto Add = [&](const std::string& name, const std::string& value) {
    meta.emplace_back(std::make_pair(name, value));
  };
  Add("class", "CacheDBM");
  if (open_) {
    Add("path", path_);
  }
  int64_t num_records = 0;
  int64_t eff_data_size = 0;
  int64_t mem_usage = 0;
  for (auto& slot : slots_) {
    num_records += slot.Count();
    eff_data_size += slot.GetEffectiveDataSize();
    mem_usage += slot.GetMemoryUsage();
  }
  Add("num_records", ToString(num_records));
  Add("eff_data_size", ToString(eff_data_size));
  Add("mem_usage", ToString(mem_usage));
  Add("cap_rec_num", ToString(cap_rec_num_));
  Add("cap_mem_size", ToString(cap_mem_size_));
  return meta;
}

bool CacheDBMImpl::IsOpen() {
  std::shared_lock<std::shared_timed_mutex> lock(mutex_);
  return open_;
}

bool CacheDBMImpl::IsWritable() {
  std::shared_lock<std::shared_timed_mutex> lock(mutex_);
  return open_ && writable_;
}

std::unique_ptr<DBM> CacheDBMImpl::MakeDBM() {
  std::shared_lock<std::shared_timed_mutex> lock(mutex_);
  return std::make_unique<CacheDBM>(file_->MakeFile(), cap_rec_num_, cap_mem_size_);
}

int64_t CacheDBMImpl::GetEffectiveDataSize() {
  std::shared_lock<std::shared_timed_mutex> lock(mutex_);
  int64_t eff_data_size = 0;
  for (auto& slot : slots_) {
    eff_data_size += slot.GetEffectiveDataSize();
  }
  return eff_data_size;
}

int64_t CacheDBMImpl::GetMemoryUsage() {
  std::shared_lock<std::shared_timed_mutex> lock(mutex_);
  int64_t mem_usage = 0;
  for (auto& slot : slots_) {
    mem_usage += slot.GetMemoryUsage();
  }
  return mem_usage;
}

void CacheDBMImpl::CancelIterators() {
  for (auto* iterator : iterators_) {
    iterator->slot_index_.store(-1);
  }
}

void CacheDBMImpl::InitAllSlots() {
  const int64_t slot_cap_rec_num = cap_rec_num_ / NUM_CACHE_SLOTS + 1;
  const int64_t slot_cap_mem_size = cap_mem_size_ / NUM_CACHE_SLOTS + 1;
  for (auto& slot : slots_) {
    slot.Init(slot_cap_rec_num, slot_cap_mem_size);
  }
}

void CacheDBMImpl::CleanUpAllSlots() {
  for (auto& slot : slots_) {
    slot.CleanUp();
  }
}

Status CacheDBMImpl::ImportRecords() {
  int64_t end_offset = 0;
  Status status = file_->GetSize(&end_offset);
  if (status != Status::SUCCESS) {
    return status;
  }
  FlatRecordReader reader(file_.get());
  std::string key_store;
  while (true) {
    std::string_view key;
    Status status = reader.Read(&key);
    if (status != Status::SUCCESS) {
      if (status != Status::NOT_FOUND_ERROR) {
        return status;
      }
      break;
    }
    key_store = key;
    std::string_view value;
    status = reader.Read(&value);
    if (status != Status::SUCCESS) {
      if (status != Status::NOT_FOUND_ERROR) {
        return status;
      }
      return Status(Status::BROKEN_DATA_ERROR, "odd number of records");
    }
    uint64_t hash = PrimaryHash(key_store, UINT64MAX);
    const int32_t slot_index = (hash & 0xff) % NUM_CACHE_SLOTS;
    hash >>= 8;
    DBM::RecordProcessorSet setter(&status, value, true, nullptr);
    slots_[slot_index].Process(key_store, hash, &setter, true);
  }
  return Status(Status::SUCCESS);
}

Status CacheDBMImpl::ExportRecords() {
  Status status = file_->Truncate(0);
  if (status != Status::SUCCESS) {
    return status;
  }
  FlatRecord flat_rec(file_.get());
  for (auto& slot : slots_) {
    status = slot.ExportRecords(&flat_rec);
    if (status != Status::SUCCESS) {
      return status;
    }
  }
  return Status(Status::SUCCESS);
}

Status CacheDBMImpl::ReadNextBucketRecords(CacheDBMIteratorImpl* iter) {
  while (true) {
    int64_t slot_index = iter->slot_index_.load();
    if (slot_index < 0 || slot_index >= NUM_CACHE_SLOTS) {
      break;
    }
    if (!iter->slot_index_.compare_exchange_strong(slot_index, slot_index + 1)) {
      break;
    }
    iter->keys_ = slots_[slot_index].GetKeys();
    if (!iter->keys_.empty()) {
      return Status(Status::SUCCESS);
    }
  }
  iter->slot_index_.store(-1);
  return Status(Status::NOT_FOUND_ERROR);
}

CacheDBMIteratorImpl::CacheDBMIteratorImpl(CacheDBMImpl* dbm)
    : dbm_(dbm), slot_index_(-1), keys_() {
  std::lock_guard<std::shared_timed_mutex> lock(dbm_->mutex_);
  dbm_->iterators_.emplace_back(this);
}

CacheDBMIteratorImpl::~CacheDBMIteratorImpl() {
  if (dbm_ != nullptr) {
    std::lock_guard<std::shared_timed_mutex> lock(dbm_->mutex_);
    dbm_->iterators_.remove(this);
  }
}

Status CacheDBMIteratorImpl::First() {
  std::shared_lock<std::shared_timed_mutex> lock(dbm_->mutex_);
  slot_index_.store(0);
  keys_.clear();
  return Status(Status::SUCCESS);
}

Status CacheDBMIteratorImpl::Jump(std::string_view key) {
  std::shared_lock<std::shared_timed_mutex> lock(dbm_->mutex_);
  slot_index_.store(-1);
  keys_.clear();
  const uint64_t hash = PrimaryHash(key, UINT64MAX);
  slot_index_.store((hash & 0xff) % NUM_CACHE_SLOTS);
  const Status status = dbm_->ReadNextBucketRecords(this);
  if (status != Status::SUCCESS) {
    return status;
  }
  auto it = std::find(keys_.begin(), keys_.end(), std::string(key));
  if (it == keys_.end()) {
    slot_index_.store(-1);
    keys_.clear();
    return Status(Status::NOT_FOUND_ERROR);
  }
  keys_.erase(keys_.begin(), it);
  return Status(Status::SUCCESS);
}

Status CacheDBMIteratorImpl::Next() {
  std::shared_lock<std::shared_timed_mutex> lock(dbm_->mutex_);
  const Status status = ReadKeys();
  if (status != Status::SUCCESS) {
    return status;
  }
  keys_.erase(keys_.begin());
  return Status(Status::SUCCESS);
}

Status CacheDBMIteratorImpl::Process(DBM::RecordProcessor* proc, bool writable) {
  std::shared_lock<std::shared_timed_mutex> lock(dbm_->mutex_);
  const Status status = ReadKeys();
  if (status != Status::SUCCESS) {
    return status;
  }
  auto it = keys_.begin();
  const std::string first_key = *it;
  class ProcWrapper final : public DBM::RecordProcessor {
   public:
    explicit ProcWrapper(DBM::RecordProcessor* proc) : proc_(proc) {}
    std::string_view ProcessFull(std::string_view key, std::string_view value) {
      value_ = proc_->ProcessFull(key, value);
      return value_;
    }
    std::string_view Value() {
      return value_;
    }
   private:
    DBM::RecordProcessor* proc_;
    std::string_view value_;
  } proc_wrapper(proc);
  uint64_t hash = PrimaryHash(first_key, UINT64MAX);
  const int32_t slot_index = (hash & 0xff) % NUM_CACHE_SLOTS;
  hash >>= 8;
  dbm_->slots_[slot_index].Process(first_key, hash, &proc_wrapper, writable);
  const std::string_view value = proc_wrapper.Value();
  if (value.data() == nullptr) {
    return Status(Status::NOT_FOUND_ERROR);
  }
  if (value.data() == DBM::RecordProcessor::REMOVE.data()) {
    keys_.erase(it);
  }
  return Status(Status::SUCCESS);
}

Status CacheDBMIteratorImpl::ReadKeys() {
  const int64_t slot_index = slot_index_.load();
  if (slot_index < 0) {
    return Status(Status::NOT_FOUND_ERROR);
  }
  if (keys_.empty()) {
    const Status status = dbm_->ReadNextBucketRecords(this);
    if (status != Status::SUCCESS) {
      return status;
    }
    if (keys_.empty()) {
      return Status(Status::NOT_FOUND_ERROR);
    }
  }
  return Status(Status::SUCCESS);
}

CacheDBM::CacheDBM(int64_t cap_rec_num, int64_t cap_mem_size) {
  impl_ = new CacheDBMImpl(std::make_unique<MemoryMapParallelFile>(), cap_rec_num, cap_mem_size);
}

CacheDBM::CacheDBM(std::unique_ptr<File> file, int64_t cap_rec_num, int64_t cap_mem_size) {
  impl_ = new CacheDBMImpl(std::move(file), cap_rec_num, cap_mem_size);
}

CacheDBM::~CacheDBM() {
  delete impl_;
}

Status CacheDBM::Open(const std::string& path, bool writable, int32_t options) {
  return impl_->Open(path, writable, options);
}

Status CacheDBM::Close() {
  return impl_->Close();
}

Status CacheDBM::Process(std::string_view key, RecordProcessor* proc, bool writable) {
  assert(proc != nullptr);
  return impl_->Process(key, proc, writable);
}

Status CacheDBM::ProcessEach(RecordProcessor* proc, bool writable) {
  assert(proc != nullptr);
  return impl_->ProcessEach(proc, writable);
}

Status CacheDBM::Count(int64_t* count) {
  assert(count != nullptr);
  return impl_->Count(count);
}

Status CacheDBM::GetFileSize(int64_t* size) {
  assert(size != nullptr);
  return impl_->GetFileSize(size);
}

Status CacheDBM::GetFilePath(std::string* path) {
  assert(path != nullptr);
  return impl_->GetFilePath(path);
}

Status CacheDBM::Clear() {
  return impl_->Clear();
}

Status CacheDBM::RebuildAdvanced(int64_t cap_rec_num, int64_t cap_mem_size) {
  return impl_->Rebuild(cap_rec_num, cap_mem_size);
}

Status CacheDBM::Synchronize(bool hard, FileProcessor* proc) {
  return impl_->Synchronize(hard, proc);
}

std::vector<std::pair<std::string, std::string>> CacheDBM::Inspect() {
  return impl_->Inspect();
}

bool CacheDBM::IsOpen() const {
  return impl_->IsOpen();
}

bool CacheDBM::IsWritable() const {
  return impl_->IsWritable();
}

std::unique_ptr<DBM::Iterator> CacheDBM::MakeIterator() {
  std::unique_ptr<CacheDBM::Iterator> iter(new CacheDBM::Iterator(impl_));
  return iter;
}

std::unique_ptr<DBM> CacheDBM::MakeDBM() const {
  return impl_->MakeDBM();
}

int64_t CacheDBM::GetEffectiveDataSize() {
  return impl_->GetEffectiveDataSize();
}

int64_t CacheDBM::GetMemoryUsage() {
  return impl_->GetMemoryUsage();
}

CacheDBM::Iterator::Iterator(CacheDBMImpl* dbm_impl) {
  impl_ = new CacheDBMIteratorImpl(dbm_impl);
}

CacheDBM::Iterator::~Iterator() {
  delete impl_;
}

Status CacheDBM::Iterator::First() {
  return impl_->First();
}

Status CacheDBM::Iterator::Jump(std::string_view key) {
  return impl_->Jump(key);
}

Status CacheDBM::Iterator::Next() {
  return impl_->Next();
}

Status CacheDBM::Iterator::Process(RecordProcessor* proc, bool writable) {
  assert(proc != nullptr);
  return impl_->Process(proc, writable);
}

}  // namespace tkrzw

// END OF FILE
