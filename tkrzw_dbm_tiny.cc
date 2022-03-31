/*************************************************************************************************
 * On-memory database manager implementations based on hash table
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
#include "tkrzw_dbm_tiny.h"
#include "tkrzw_file.h"
#include "tkrzw_file_mmap.h"
#include "tkrzw_file_pos.h"
#include "tkrzw_file_std.h"
#include "tkrzw_file_util.h"
#include "tkrzw_lib_common.h"
#include "tkrzw_str_util.h"
#include "tkrzw_thread_util.h"

namespace tkrzw {

static constexpr int32_t RECORD_MUTEX_NUM_SLOTS = 128;
static constexpr int64_t MAX_NUM_BUCKETS = 1099511627689LL;

struct TinyRecord final {
  char* child;
  int32_t key_size;
  int32_t value_size;
  const char* key_ptr;
  const char* value_ptr;
  char* Serialize() const;
  char* Reserialize(char* ptr, int32_t old_value_size) const;
  char* ReserializeAppend(
      char* ptr, const std::string_view cat_value, const std::string_view cat_delim) const;
  void Deserialize(const char* ptr);
};

class TinyDBMImpl final {
  friend class TinyDBMIteratorImpl;
  typedef std::list<TinyDBMIteratorImpl*> IteratorList;
 public:
  TinyDBMImpl(std::unique_ptr<File> file, int64_t num_buckets);
  ~TinyDBMImpl();
  Status Open(const std::string& path, bool writable, int32_t options);
  Status Close();
  Status Process(std::string_view key, DBM::RecordProcessor* proc, bool writable);
  Status Append(std::string_view key, std::string_view value, std::string_view delim);
  Status ProcessMulti(
      const std::vector<std::pair<std::string_view, DBM::RecordProcessor*>>& key_proc_pairs,
      bool writable);
  Status ProcessFirst(DBM::RecordProcessor* proc, bool writable);
  Status ProcessEach(DBM::RecordProcessor* proc, bool writable);
  Status Count(int64_t* count);
  Status GetFileSize(int64_t* size);
  Status GetFilePath(std::string* path);
  Status GetTimestamp(double* timestamp);
  Status Clear();
  Status Rebuild(int64_t num_buckets);
  Status ShouldBeRebuilt(bool* tobe);
  Status Synchronize(bool hard, DBM::FileProcessor* proc);
  std::vector<std::pair<std::string, std::string>> Inspect();
  bool IsOpen();
  bool IsWritable();
  std::unique_ptr<DBM> MakeDBM();
  DBM::UpdateLogger* GetUpdateLogger();
  void SetUpdateLogger(DBM::UpdateLogger* update_logger);
  File* GetInternalFile() const;

 private:
  void CancelIterators();
  void InitializeBuckets();
  void ReleaseAllRecords();
  Status ImportRecords();
  Status ExportRecords();
  void ProcessImpl(
      std::string_view key, int64_t bucket_index, DBM::RecordProcessor* proc, bool writable);
  void AppendImpl(
      std::string_view key, int64_t bucket_index, std::string_view value, std::string_view delim);
  Status ReadNextBucketRecords(TinyDBMIteratorImpl* iter);

  IteratorList iterators_;
  std::unique_ptr<File> file_;
  bool open_;
  bool writable_;
  int32_t open_options_;
  std::string path_;
  double timestamp_;
  std::atomic_int64_t num_records_;
  int64_t num_buckets_;
  char** buckets_;
  DBM::UpdateLogger* update_logger_;
  SpinSharedMutex mutex_;
  HashMutex<SpinSharedMutex> record_mutex_;
};

class TinyDBMIteratorImpl final {
  friend class TinyDBMImpl;
 public:
  explicit TinyDBMIteratorImpl(TinyDBMImpl* dbm);
  ~TinyDBMIteratorImpl();
  Status First();
  Status Jump(std::string_view key);
  Status Next();
  Status Process(DBM::RecordProcessor* proc, bool writable);

 private:
  Status ReadKeys();

  TinyDBMImpl* dbm_;
  std::atomic_int64_t bucket_index_;
  std::vector<std::string> keys_;
};

char* TinyRecord::Serialize() const {
  const int32_t size = sizeof(child) + SizeVarNum(key_size) + key_size +
      SizeVarNum(value_size) + value_size;
  char* ptr = static_cast<char*>(xmalloc(size));
  char* wp = ptr;
  std::memcpy(wp, &child, sizeof(child));
  wp += sizeof(child);
  wp += WriteVarNum(wp, key_size);
  std::memcpy(wp, key_ptr, key_size);
  wp += key_size;
  wp += WriteVarNum(wp, value_size);
  std::memcpy(wp, value_ptr, value_size);
  return ptr;
}

char* TinyRecord::Reserialize(char* ptr, int32_t old_value_size) const {
  const int32_t old_value_header_size = SizeVarNum(old_value_size);
  const int32_t new_value_header_size = SizeVarNum(value_size);
  if (new_value_header_size > old_value_header_size) {
    char* new_ptr = Serialize();
    xfree(ptr);
    return new_ptr;
  }
  if (value_size > old_value_size) {
    const int32_t size = sizeof(child) + SizeVarNum(key_size) + key_size +
        SizeVarNum(value_size) + value_size;
    ptr = static_cast<char*>(xrealloc(ptr, size));
  }
  char* wp = ptr + sizeof(child) + SizeVarNum(key_size) + key_size;
  wp += WriteVarNum(wp, value_size);
  std::memcpy(wp, value_ptr, value_size);
  return ptr;
}

char* TinyRecord::ReserializeAppend(
    char* ptr, const std::string_view cat_value, const std::string_view cat_delim) const {
  const int32_t new_value_size = value_size + cat_delim.size() + cat_value.size();
  const int32_t old_value_header_size = SizeVarNum(value_size);
  const int32_t new_value_header_size = SizeVarNum(new_value_size);
  if (new_value_header_size > old_value_header_size) {
    const int32_t size = sizeof(child) + SizeVarNum(key_size) + key_size +
        SizeVarNum(new_value_size) + new_value_size;
    char* new_ptr = static_cast<char*>(xreallocappend(nullptr, size));
    char* wp = new_ptr;
    std::memcpy(wp, &child, sizeof(child));
    wp += sizeof(child);
    wp += WriteVarNum(wp, key_size);
    std::memcpy(wp, key_ptr, key_size);
    wp += key_size;
    wp += WriteVarNum(wp, new_value_size);
    std::memcpy(wp, value_ptr, value_size);
    wp += value_size;
    std::memcpy(wp, cat_delim.data(), cat_delim.size());
    wp += cat_delim.size();
    std::memcpy(wp, cat_value.data(), cat_value.size());
    xfree(ptr);
    return new_ptr;
  }
  const int32_t size = sizeof(child) + SizeVarNum(key_size) + key_size +
      SizeVarNum(new_value_size) + new_value_size;
  ptr = static_cast<char*>(xreallocappend(ptr, size));
  char* wp = ptr + sizeof(child) + SizeVarNum(key_size) + key_size;
  wp += WriteVarNum(wp, new_value_size);
  wp += value_size;
  std::memcpy(wp, cat_delim.data(), cat_delim.size());
  wp += cat_delim.size();
  std::memcpy(wp, cat_value.data(), cat_value.size());
  return ptr;
}

void TinyRecord::Deserialize(const char* ptr) {
  const char* rp = ptr;
  std::memcpy(&child, rp, sizeof(child));
  rp += sizeof(child);
  uint64_t num = 0;
  rp += ReadVarNum(rp, &num);
  key_size = num;
  key_ptr = rp;
  rp += key_size;
  rp += ReadVarNum(rp, &num);
  value_size = num;
  value_ptr = rp;
}

TinyDBMImpl::TinyDBMImpl(std::unique_ptr<File> file, int64_t num_buckets)
    : iterators_(), file_(std::move(file)),
      open_(false), writable_(false), open_options_(0), path_(), timestamp_(0),
      num_records_(0), num_buckets_(0), buckets_(nullptr), update_logger_(nullptr),
      mutex_(),
      record_mutex_(RECORD_MUTEX_NUM_SLOTS, 1, PrimaryHash) {
  if (num_buckets > 0) {
    num_buckets_ = GetHashBucketSize(std::min(num_buckets, MAX_NUM_BUCKETS));
  } else {
    num_buckets_ = TinyDBM::DEFAULT_NUM_BUCKETS;
  }
  InitializeBuckets();
}

TinyDBMImpl::~TinyDBMImpl() {
  if (open_) {
    Close();
  }
  for (auto* iterator : iterators_) {
    iterator->dbm_ = nullptr;
  }
  ReleaseAllRecords();
  xfree(buckets_);
}

Status TinyDBMImpl::Open(const std::string& path, bool writable, int32_t options) {
  std::lock_guard<SpinSharedMutex> lock(mutex_);
  if (open_) {
    return Status(Status::PRECONDITION_ERROR, "opened database");
  }
  const std::string norm_path = NormalizePath(path);
  Status status = file_->Open(norm_path, writable, options);
  if (status != Status::SUCCESS) {
    return status;
  }
  if (file_->GetSizeSimple() < 1) {
    timestamp_ = GetWallTime();
  }
  status = ImportRecords();
  if (status != Status::SUCCESS) {
    file_->Close();
    return status;
  }
  open_ = true;
  writable_ = writable;
  open_options_ = options;
  path_ = norm_path;
  return Status(Status::SUCCESS);
}

Status TinyDBMImpl::Close() {
  std::lock_guard<SpinSharedMutex> lock(mutex_);
  if (!open_) {
    return Status(Status::PRECONDITION_ERROR, "not opened database");
  }
  Status status(Status::SUCCESS);
  if (writable_) {
    status |= ExportRecords();
  }
  status |= file_->Close();
  ReleaseAllRecords();
  CancelIterators();
  xfree(buckets_);
  InitializeBuckets();
  open_ = false;
  writable_ = false;
  open_options_ = 0;
  path_.clear();
  timestamp_ = 0;
  num_records_.store(0);
  return status;
}

Status TinyDBMImpl::Process(std::string_view key, DBM::RecordProcessor* proc, bool writable) {
  std::shared_lock<SpinSharedMutex> lock(mutex_);
  ScopedHashLock record_lock(record_mutex_, key, writable);
  const int64_t bucket_index = record_lock.GetBucketIndex();
  ProcessImpl(key, bucket_index, proc, writable);
  return Status(Status::SUCCESS);
}

Status TinyDBMImpl::Append(std::string_view key, std::string_view value, std::string_view delim) {
  std::shared_lock<SpinSharedMutex> lock(mutex_);
  ScopedHashLock record_lock(record_mutex_, key, true);
  const int64_t bucket_index = record_lock.GetBucketIndex();
  AppendImpl(key, bucket_index, value, delim);
  return Status(Status::SUCCESS);
}

Status TinyDBMImpl::ProcessMulti(
    const std::vector<std::pair<std::string_view, DBM::RecordProcessor*>>& key_proc_pairs,
    bool writable) {
  std::shared_lock<SpinSharedMutex> lock(mutex_);
  std::vector<std::string_view> keys;
  keys.reserve(key_proc_pairs.size());
  for (const auto& pair : key_proc_pairs) {
    keys.emplace_back(pair.first);
  }
  ScopedHashLockMulti record_lock(record_mutex_, keys, writable);
  const std::vector<int64_t>& bucket_indices = record_lock.GetBucketIndices();
  for (size_t i = 0; i < key_proc_pairs.size(); i++) {
    const auto& key_proc = key_proc_pairs[i];
    const int64_t bucket_index = bucket_indices[i];
    ProcessImpl(key_proc.first, bucket_index, key_proc.second, writable);
  }
  return Status(Status::SUCCESS);
}

Status TinyDBMImpl::ProcessFirst(DBM::RecordProcessor* proc, bool writable) {
  if (writable) {
    std::lock_guard<SpinSharedMutex> lock(mutex_);
    TinyRecord rec;
    for (int64_t bucket_index = 0; bucket_index < num_buckets_; bucket_index++) {
      char* ptr = buckets_[bucket_index];
      if (ptr != nullptr) {
        rec.Deserialize(ptr);
        const std::string key(rec.key_ptr, rec.key_size);
        ProcessImpl(key, bucket_index, proc, true);
        return Status(Status::SUCCESS);
      }
    }
  } else {
    std::shared_lock<SpinSharedMutex> lock(mutex_);
    TinyRecord rec;
    for (int64_t bucket_index = 0; bucket_index < num_buckets_; bucket_index++) {
      char* ptr = buckets_[bucket_index];
      if (ptr != nullptr) {
        rec.Deserialize(ptr);
        const std::string_view key(rec.key_ptr, rec.key_size);
        const std::string_view value (rec.value_ptr, rec.value_size);
        proc->ProcessFull(key, value);
        return Status(Status::SUCCESS);
      }
    }
  }
  return Status(Status::NOT_FOUND_ERROR);
}

Status TinyDBMImpl::ProcessEach(DBM::RecordProcessor* proc, bool writable) {
  if (writable) {
    std::lock_guard<SpinSharedMutex> lock(mutex_);
    proc->ProcessEmpty(DBM::RecordProcessor::NOOP);
    TinyRecord rec;
    for (int64_t bucket_index = 0; bucket_index < num_buckets_; bucket_index++) {
      char* ptr = buckets_[bucket_index];
      while (ptr != nullptr) {
        rec.Deserialize(ptr);
        const std::string key(rec.key_ptr, rec.key_size);
        ProcessImpl(key, bucket_index, proc, true);
        ptr = rec.child;
      }
    }
    proc->ProcessEmpty(DBM::RecordProcessor::NOOP);
  } else {
    std::shared_lock<SpinSharedMutex> lock(mutex_);
    proc->ProcessEmpty(DBM::RecordProcessor::NOOP);
    TinyRecord rec;
    for (int64_t bucket_index = 0; bucket_index < num_buckets_; bucket_index++) {
      char* ptr = buckets_[bucket_index];
      while (ptr != nullptr) {
        rec.Deserialize(ptr);
        const std::string_view key(rec.key_ptr, rec.key_size);
        const std::string_view value (rec.value_ptr, rec.value_size);
        proc->ProcessFull(key, value);
        ptr = rec.child;
      }
    }
    proc->ProcessEmpty(DBM::RecordProcessor::NOOP);
  }
  return Status(Status::SUCCESS);
}

Status TinyDBMImpl::Count(int64_t* count) {
  std::shared_lock<SpinSharedMutex> lock(mutex_);
  *count = num_records_.load();
  return Status(Status::SUCCESS);
}

Status TinyDBMImpl::GetFileSize(int64_t* size) {
  std::shared_lock<SpinSharedMutex> lock(mutex_);
  if (!open_) {
    return Status(Status::PRECONDITION_ERROR, "not opened database");
  }
  *size = file_->GetSizeSimple();
  return Status(Status::SUCCESS);
}

Status TinyDBMImpl::GetFilePath(std::string* path) {
  std::shared_lock<SpinSharedMutex> lock(mutex_);
  if (!open_) {
    return Status(Status::PRECONDITION_ERROR, "not opened database");
  }
  *path = path_;
  return Status(Status::SUCCESS);
}

Status TinyDBMImpl::GetTimestamp(double* timestamp) {
  std::shared_lock<SpinSharedMutex> lock(mutex_);
  if (!open_) {
    return Status(Status::PRECONDITION_ERROR, "not opened database");
  }
  *timestamp = timestamp_;
  return Status(Status::SUCCESS);
}

Status TinyDBMImpl::Clear() {
  std::lock_guard<SpinSharedMutex> lock(mutex_);
  if (update_logger_ != nullptr) {
    update_logger_->WriteClear();
  }
  ReleaseAllRecords();
  CancelIterators();
  xfree(buckets_);
  InitializeBuckets();
  num_records_.store(0);
  return Status(Status::SUCCESS);
}

Status TinyDBMImpl::Rebuild(int64_t num_buckets) {
  std::lock_guard<SpinSharedMutex> lock(mutex_);
  const int64_t old_num_buckets_ = num_buckets_;
  char** old_buckets = buckets_;
  num_buckets_ = num_buckets > 0 ? num_buckets : num_records_ * 2 + 1;
  num_buckets_ = GetHashBucketSize(std::min(num_buckets_, MAX_NUM_BUCKETS));
  InitializeBuckets();
  TinyRecord rec;
  for (int64_t old_bucket_index = 0; old_bucket_index < old_num_buckets_; old_bucket_index++) {
    char* ptr = old_buckets[old_bucket_index];
    while (ptr != nullptr) {
      rec.Deserialize(ptr);
      const std::string_view rec_key(rec.key_ptr, rec.key_size);
      const int64_t bucket_index = record_mutex_.GetBucketIndex(rec_key);
      const char* top = buckets_[bucket_index];
      memcpy(ptr, &top, sizeof(top));
      buckets_[bucket_index] = ptr;
      ptr = rec.child;
    }
  }
  xfree(old_buckets);
  CancelIterators();
  return Status(Status::SUCCESS);
}

Status TinyDBMImpl::ShouldBeRebuilt(bool* tobe) {
  std::shared_lock<SpinSharedMutex> lock(mutex_);
  *tobe = num_records_.load() > num_buckets_;
  return Status(Status::SUCCESS);
}

Status TinyDBMImpl::Synchronize(bool hard, DBM::FileProcessor* proc) {
  std::lock_guard<SpinSharedMutex> lock(mutex_);
  Status status(Status::SUCCESS);
  if (writable_ && update_logger_ != nullptr) {
    status |= update_logger_->Synchronize(hard);
  }
  if (open_ && writable_) {
    status |= ExportRecords();
    status |= file_->Synchronize(hard);
    if (proc != nullptr) {
      proc->Process(path_);
    }
  }
  return status;
}

std::vector<std::pair<std::string, std::string>> TinyDBMImpl::Inspect() {
  std::lock_guard<SpinSharedMutex> lock(mutex_);
  std::vector<std::pair<std::string, std::string>> meta;
  auto Add = [&](const std::string& name, const std::string& value) {
    meta.emplace_back(std::make_pair(name, value));
  };
  Add("class", "TinyDBM");
  if (open_) {
    Add("path", path_);
    Add("timestamp", SPrintF("%.6f", timestamp_));
  }
  Add("num_records", ToString(num_records_.load()));
  Add("num_buckets", ToString(num_buckets_));
  return meta;
}

bool TinyDBMImpl::IsOpen() {
  std::shared_lock<SpinSharedMutex> lock(mutex_);
  return open_;
}

bool TinyDBMImpl::IsWritable() {
  std::shared_lock<SpinSharedMutex> lock(mutex_);
  return open_ && writable_;
}

std::unique_ptr<DBM> TinyDBMImpl::MakeDBM() {
  std::shared_lock<SpinSharedMutex> lock(mutex_);
  return std::make_unique<TinyDBM>(file_->MakeFile(), num_buckets_);
}

DBM::UpdateLogger* TinyDBMImpl::GetUpdateLogger() {
  std::shared_lock<SpinSharedMutex> lock(mutex_);
  return update_logger_;
}

void TinyDBMImpl::SetUpdateLogger(DBM::UpdateLogger* update_logger) {
  std::lock_guard<SpinSharedMutex> lock(mutex_);
  update_logger_ = update_logger;
}

File* TinyDBMImpl::GetInternalFile() const {
  return file_.get();
}

void TinyDBMImpl::CancelIterators() {
  for (auto* iterator : iterators_) {
    iterator->bucket_index_.store(-1);
  }
}

void TinyDBMImpl::InitializeBuckets() {
  buckets_ = static_cast<char**>(xcalloc(num_buckets_, sizeof(*buckets_)));
  record_mutex_.Rehash(num_buckets_);
}

void TinyDBMImpl::ReleaseAllRecords() {
  for (int64_t bucket_index = 0; bucket_index < num_buckets_; bucket_index++) {
    char* ptr = buckets_[bucket_index];
    while (ptr != nullptr) {
      char* child;
      std::memcpy(&child, ptr, sizeof(child));
      xfree(ptr);
      ptr = child;
    }
  }
}

Status TinyDBMImpl::ImportRecords() {
  int64_t end_offset = 0;
  Status status = file_->GetSize(&end_offset);
  if (status != Status::SUCCESS) {
    return status;
  }
  FlatRecordReader reader(file_.get());
  std::string key_store;
  while (true) {
    std::string_view key;
    FlatRecord::RecordType rec_type;
    Status status = reader.Read(&key, &rec_type);
    if (status != Status::SUCCESS) {
      if (status != Status::NOT_FOUND_ERROR) {
        return status;
      }
      break;
    }
    if (rec_type != FlatRecord::RECORD_NORMAL) {
      if (rec_type == FlatRecord::RECORD_METADATA) {
        const auto& meta = DeserializeStrMap(key);
        if (StrContains(SearchMap(meta, "class", ""), "DBM")) {
          const auto& tsexpr = SearchMap(meta, "timestamp", "");
          if (!tsexpr.empty()) {
            timestamp_ = StrToDouble(tsexpr);
          }
        }
      }
      continue;
    }
    key_store = key;
    std::string_view value;
    status = reader.Read(&value, &rec_type);
    if (status != Status::SUCCESS) {
      if (status != Status::NOT_FOUND_ERROR) {
        return status;
      }
      return Status(Status::BROKEN_DATA_ERROR, "odd number of records");
    }
    if (rec_type != FlatRecord::RECORD_NORMAL) {
      return Status(Status::BROKEN_DATA_ERROR, "invalid metadata position");
    }
    DBM::RecordProcessorSet setter(&status, value, true, nullptr);
    ScopedHashLock record_lock(record_mutex_, key_store, true);
    const int64_t bucket_index = record_lock.GetBucketIndex();
    ProcessImpl(key_store, bucket_index, &setter, true);
  }
  return Status(Status::SUCCESS);
}

Status TinyDBMImpl::ExportRecords() {
  Status status = file_->Close();
  if (status != Status::SUCCESS) {
    return status;
  }
  const std::string export_path = path_ + ".tmp.export";
  const int32_t export_options = File::OPEN_TRUNCATE | (open_options_ & File::OPEN_SYNC_HARD);
  status = file_->Open(export_path, true, export_options);
  if (status != Status::SUCCESS) {
    file_->Open(path_, true, open_options_ & ~File::OPEN_TRUNCATE);
    return status;
  }
  FlatRecord flat_rec(file_.get());
  std::map<std::string, std::string> meta;
  meta["class"] = "TinyDBM";
  meta["timestamp"] = SPrintF("%.6f", GetWallTime());
  meta["num_records"] = ToString(num_records_.load());
  meta["num_buckets"] = ToString(num_buckets_);
  status |= flat_rec.Write(SerializeStrMap(meta), FlatRecord::RECORD_METADATA);
  TinyRecord rec;
  for (int64_t bucket_index = 0; bucket_index < num_buckets_; bucket_index++) {
    char* ptr = buckets_[bucket_index];
    while (ptr != nullptr) {
      rec.Deserialize(ptr);
      status |= flat_rec.Write(std::string_view(rec.key_ptr, rec.key_size));
      status |= flat_rec.Write(std::string_view(rec.value_ptr, rec.value_size));
      if (status != Status::SUCCESS) {
        break;
      }
      ptr = rec.child;
    }
  }
  status |= file_->Close();
  status |= RenameFile(export_path, path_);
  RemoveFile(export_path);
  status |= file_->Open(path_, true, open_options_ & ~File::OPEN_TRUNCATE);;
  return status;
}

void TinyDBMImpl::ProcessImpl(
    std::string_view key, int64_t bucket_index, DBM::RecordProcessor* proc, bool writable) {
  TinyRecord rec;
  char* top = buckets_[bucket_index];
  char* parent = nullptr;
  char* ptr = top;
  while (ptr != nullptr) {
    rec.Deserialize(ptr);
    const std::string_view rec_key(rec.key_ptr, rec.key_size);
    const std::string_view rec_value(rec.value_ptr, rec.value_size);
    if (key == rec_key) {
      std::string_view new_value = proc->ProcessFull(key, rec_value);
      if (new_value.data() != DBM::RecordProcessor::NOOP.data() && writable) {
        if (update_logger_ != nullptr) {
          if (new_value.data() == DBM::RecordProcessor::REMOVE.data()) {
            update_logger_->WriteRemove(key);
          } else {
            update_logger_->WriteSet(key, new_value);
          }
        }
        if (new_value.data() == DBM::RecordProcessor::REMOVE.data()) {
          xfree(ptr);
          if (parent == nullptr) {
            buckets_[bucket_index] = rec.child;
          } else {
            std::memcpy(parent, &rec.child, sizeof(rec.child));
          }
          num_records_.fetch_sub(1);
        } else {
          rec.value_ptr = new_value.data();
          rec.value_size = new_value.size();
          char* new_ptr = rec.Reserialize(ptr, rec_value.size());
          if (new_ptr != ptr) {
            if (parent == nullptr) {
              buckets_[bucket_index] = new_ptr;
            } else {
              std::memcpy(parent, &new_ptr, sizeof(new_ptr));
            }
          }
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
    if (update_logger_ != nullptr) {
      update_logger_->WriteSet(key, new_value);
    }
    rec.child = top;
    rec.key_ptr = key.data();
    rec.key_size = key.size();
    rec.value_ptr = new_value.data();
    rec.value_size = new_value.size();
    buckets_[bucket_index] = rec.Serialize();
    num_records_.fetch_add(1);
  }
}

void TinyDBMImpl::AppendImpl(
    std::string_view key, int64_t bucket_index, std::string_view value, std::string_view delim) {
  TinyRecord rec;
  char* top = buckets_[bucket_index];
  char* parent = nullptr;
  char* ptr = top;
  while (ptr != nullptr) {
    rec.Deserialize(ptr);
    const std::string_view rec_key(rec.key_ptr, rec.key_size);
    const std::string_view rec_value(rec.value_ptr, rec.value_size);
    if (key == rec_key) {
      char* new_ptr = rec.ReserializeAppend(ptr, value, delim);
      if (update_logger_ != nullptr) {
        rec.Deserialize(new_ptr);
        update_logger_->WriteSet(key, std::string_view(rec.value_ptr, rec.value_size));
      }
      if (new_ptr != ptr) {
        if (parent == nullptr) {
          buckets_[bucket_index] = new_ptr;
        } else {
          std::memcpy(parent, &new_ptr, sizeof(new_ptr));
        }
      }
      return;
    }
    parent = ptr;
    ptr = rec.child;
  }
  if (update_logger_ != nullptr) {
    update_logger_->WriteSet(key, value);
  }
  rec.child = top;
  rec.key_ptr = key.data();
  rec.key_size = key.size();
  rec.value_ptr = value.data();
  rec.value_size = value.size();
  buckets_[bucket_index] = rec.Serialize();
  num_records_.fetch_add(1);
}

Status TinyDBMImpl::ReadNextBucketRecords(TinyDBMIteratorImpl* iter) {
  while (true) {
    int64_t bucket_index = iter->bucket_index_.load();
    if (bucket_index < 0 || bucket_index >= num_buckets_)  {
      break;
    }
    if (!iter->bucket_index_.compare_exchange_strong(bucket_index, bucket_index + 1)) {
      break;
    }
    ScopedHashLock record_lock(record_mutex_, bucket_index, false);
    if (record_lock.GetBucketIndex() < 0) {
      break;
    }
    TinyRecord rec;
    char* ptr = buckets_[bucket_index];
    while (ptr != nullptr) {
      rec.Deserialize(ptr);
      iter->keys_.emplace_back(std::string(rec.key_ptr, rec.key_size));
      ptr = rec.child;
    }
    if (!iter->keys_.empty()) {
      return Status(Status::SUCCESS);
    }
  }
  iter->bucket_index_.store(-1);
  return Status(Status::NOT_FOUND_ERROR);
}

TinyDBMIteratorImpl::TinyDBMIteratorImpl(TinyDBMImpl* dbm)
    : dbm_(dbm), bucket_index_(-1), keys_() {
  std::lock_guard<SpinSharedMutex> lock(dbm_->mutex_);
  dbm_->iterators_.emplace_back(this);
}

TinyDBMIteratorImpl::~TinyDBMIteratorImpl() {
  if (dbm_ != nullptr) {
    std::lock_guard<SpinSharedMutex> lock(dbm_->mutex_);
    dbm_->iterators_.remove(this);
  }
}

Status TinyDBMIteratorImpl::First() {
  std::shared_lock<SpinSharedMutex> lock(dbm_->mutex_);
  bucket_index_.store(0);
  keys_.clear();
  return Status(Status::SUCCESS);
}

Status TinyDBMIteratorImpl::Jump(std::string_view key) {
  std::shared_lock<SpinSharedMutex> lock(dbm_->mutex_);
  bucket_index_.store(-1);
  keys_.clear();
  {
    ScopedHashLock record_lock(dbm_->record_mutex_, key, false);
    bucket_index_.store(record_lock.GetBucketIndex());
  }
  const Status status = dbm_->ReadNextBucketRecords(this);
  if (status != Status::SUCCESS) {
    return status;
  }
  auto it = std::find(keys_.begin(), keys_.end(), std::string(key));
  if (it == keys_.end()) {
    bucket_index_.store(-1);
    keys_.clear();
    return Status(Status::NOT_FOUND_ERROR);
  }
  keys_.erase(keys_.begin(), it);
  return Status(Status::SUCCESS);
}

Status TinyDBMIteratorImpl::Next() {
  std::shared_lock<SpinSharedMutex> lock(dbm_->mutex_);
  const Status status = ReadKeys();
  if (status != Status::SUCCESS) {
    return status;
  }
  keys_.erase(keys_.begin());
  return Status(Status::SUCCESS);
}

Status TinyDBMIteratorImpl::Process(DBM::RecordProcessor* proc, bool writable) {
  std::shared_lock<SpinSharedMutex> lock(dbm_->mutex_);
  const Status status = ReadKeys();
  if (status != Status::SUCCESS) {
    return status;
  }
  auto it = keys_.begin();
  const std::string first_key = *it;
  class ProcWrapper final : public DBM::RecordProcessor {
   public:
    explicit ProcWrapper(DBM::RecordProcessor* proc) : proc_(proc) {}
    std::string_view ProcessFull(std::string_view key, std::string_view value) override {
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
  {
    ScopedHashLock record_lock(dbm_->record_mutex_, first_key, writable);
    const int64_t bucket_index = record_lock.GetBucketIndex();
    dbm_->ProcessImpl(first_key, bucket_index, &proc_wrapper, writable);
  }
  const std::string_view value = proc_wrapper.Value();
  if (value.data() == nullptr) {
    return Status(Status::NOT_FOUND_ERROR);
  }
  if (value.data() == DBM::RecordProcessor::REMOVE.data()) {
    keys_.erase(it);
  }
  return Status(Status::SUCCESS);
}

Status TinyDBMIteratorImpl::ReadKeys() {
  const int64_t bucket_index = bucket_index_.load();
  if (bucket_index < 0) {
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

TinyDBM::TinyDBM(int64_t num_buckets) {
  impl_ = new TinyDBMImpl(std::make_unique<MemoryMapParallelFile>(), num_buckets);
}

TinyDBM::TinyDBM(std::unique_ptr<File> file, int64_t num_buckets) {
  impl_ = new TinyDBMImpl(std::move(file), num_buckets);
}

TinyDBM::~TinyDBM() {
  delete impl_;
}

Status TinyDBM::Open(const std::string& path, bool writable, int32_t options) {
  return impl_->Open(path, writable, options);
}

Status TinyDBM::Close() {
  return impl_->Close();
}

Status TinyDBM::Process(std::string_view key, RecordProcessor* proc, bool writable) {
  assert(proc != nullptr);
  return impl_->Process(key, proc, writable);
}

Status TinyDBM::Append(std::string_view key, std::string_view value, std::string_view delim) {
  return impl_->Append(key, value, delim);
}

Status TinyDBM::ProcessMulti(
    const std::vector<std::pair<std::string_view, RecordProcessor*>>& key_proc_pairs,
    bool writable) {
  return impl_->ProcessMulti(key_proc_pairs, writable);
}

Status TinyDBM::ProcessFirst(RecordProcessor* proc, bool writable) {
  assert(proc != nullptr);
  return impl_->ProcessFirst(proc, writable);
}

Status TinyDBM::ProcessEach(RecordProcessor* proc, bool writable) {
  assert(proc != nullptr);
  return impl_->ProcessEach(proc, writable);
}

Status TinyDBM::Count(int64_t* count) {
  assert(count != nullptr);
  return impl_->Count(count);
}

Status TinyDBM::GetFileSize(int64_t* size) {
  assert(size != nullptr);
  return impl_->GetFileSize(size);
}

Status TinyDBM::GetFilePath(std::string* path) {
  assert(path != nullptr);
  return impl_->GetFilePath(path);
}

Status TinyDBM::GetTimestamp(double* timestamp) {
  assert(timestamp != nullptr);
  return impl_->GetTimestamp(timestamp);
}

Status TinyDBM::Clear() {
  return impl_->Clear();
}

Status TinyDBM::RebuildAdvanced(int64_t num_buckets) {
  return impl_->Rebuild(num_buckets);
}

Status TinyDBM::ShouldBeRebuilt(bool* tobe) {
  assert(tobe != nullptr);
  return impl_->ShouldBeRebuilt(tobe);
}

Status TinyDBM::Synchronize(bool hard, FileProcessor* proc) {
  return impl_->Synchronize(hard, proc);
}

std::vector<std::pair<std::string, std::string>> TinyDBM::Inspect() {
  return impl_->Inspect();
}

bool TinyDBM::IsOpen() const {
  return impl_->IsOpen();
}

bool TinyDBM::IsWritable() const {
  return impl_->IsWritable();
}

std::unique_ptr<DBM::Iterator> TinyDBM::MakeIterator() {
  std::unique_ptr<TinyDBM::Iterator> iter(new TinyDBM::Iterator(impl_));
  return iter;
}

std::unique_ptr<DBM> TinyDBM::MakeDBM() const {
  return impl_->MakeDBM();
}

DBM::UpdateLogger* TinyDBM::GetUpdateLogger() const {
  return impl_->GetUpdateLogger();
}

void TinyDBM::SetUpdateLogger(UpdateLogger* update_logger) {
  impl_->SetUpdateLogger(update_logger);
}

File* TinyDBM::GetInternalFile() const {
  return impl_->GetInternalFile();
}

TinyDBM::Iterator::Iterator(TinyDBMImpl* dbm_impl) {
  impl_ = new TinyDBMIteratorImpl(dbm_impl);
}

TinyDBM::Iterator::~Iterator() {
  delete impl_;
}

Status TinyDBM::Iterator::First() {
  return impl_->First();
}

Status TinyDBM::Iterator::Jump(std::string_view key) {
  return impl_->Jump(key);
}

Status TinyDBM::Iterator::Next() {
  return impl_->Next();
}

Status TinyDBM::Iterator::Process(RecordProcessor* proc, bool writable) {
  assert(proc != nullptr);
  return impl_->Process(proc, writable);
}

}  // namespace tkrzw

// END OF FILE
