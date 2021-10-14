/*************************************************************************************************
 * On-memory database manager implementations with the C++ standard containers
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
#include "tkrzw_dbm_std.h"
#include "tkrzw_file.h"
#include "tkrzw_file_mmap.h"
#include "tkrzw_file_pos.h"
#include "tkrzw_file_std.h"
#include "tkrzw_file_util.h"
#include "tkrzw_lib_common.h"
#include "tkrzw_str_util.h"

namespace tkrzw {

typedef std::unordered_map<std::string, std::string> StringHashMap;
typedef std::map<std::string, std::string> StringTreeMap;
template <class STRMAP> class StdDBMIteratorImpl;

template <class STRMAP>
class StdDBMImpl {
  friend class StdDBMIteratorImpl<STRMAP>;
  typedef std::list<StdDBMIteratorImpl<STRMAP>*> IteratorList;
 public:
  StdDBMImpl(std::unique_ptr<File> file, int64_t num_buckets);
  ~StdDBMImpl();
  Status Open(const std::string& path, bool writable, int32_t options);
  Status Close();
  Status Process(std::string_view key, DBM::RecordProcessor* proc, bool writable);
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
  Status Rebuild();
  Status ShouldBeRebuilt(bool* tobe);
  Status Synchronize(bool hard, DBM::FileProcessor* proc);
  void Inspect(std::vector<std::pair<std::string, std::string>>* meta);
  bool IsOpen();
  bool IsWritable();
  std::unique_ptr<DBM> MakeDBM();
  DBM::UpdateLogger* GetUpdateLogger();
  void SetUpdateLogger(DBM::UpdateLogger* update_logger);
  File* GetInternalFile() const;

 private:
  void CancelIterators();
  Status ImportRecords();
  Status ExportRecords();

  STRMAP map_;
  IteratorList iterators_;
  std::unique_ptr<File> file_;
  bool open_;
  bool writable_;
  int32_t open_options_;
  std::string path_;
  double timestamp_;
  DBM::UpdateLogger* update_logger_;
  SpinSharedMutex mutex_;
};

template <class STRMAP>
class StdDBMIteratorImpl {
  friend class StdDBMImpl<STRMAP>;
 public:
  explicit StdDBMIteratorImpl(StdDBMImpl<STRMAP>* dbm);
  ~StdDBMIteratorImpl();
  Status First();
  Status Last();
  Status Jump(std::string_view key);
  Status JumpLower(std::string_view key, bool inclusive);
  Status JumpUpper(std::string_view key, bool inclusive);
  Status Next();
  Status Previous();
  Status Process(DBM::RecordProcessor* proc, bool writable);

 private:
  StdDBMImpl<STRMAP>* dbm_;
  typename STRMAP::const_iterator it_;
};

template <class STRMAP>
StdDBMImpl<STRMAP>::StdDBMImpl(std::unique_ptr<File> file, int64_t num_buckets)
    : file_(std::move(file)), open_(false), writable_(false), open_options_(0),
      path_(), timestamp_(0), update_logger_(nullptr), mutex_() {
}

template <>
StdDBMImpl<StringHashMap>::StdDBMImpl(std::unique_ptr<File> file, int64_t num_buckets)
    : file_(std::move(file)), open_(false), writable_(false), path_(),
      update_logger_(nullptr), mutex_() {
  map_.rehash(num_buckets);
  map_.max_load_factor(FLOATMAX);
}

template <class STRMAP>
StdDBMImpl<STRMAP>::~StdDBMImpl() {
  if (open_) {
    Close();
  }
  for (auto* iterator : iterators_) {
    iterator->dbm_ = nullptr;
  }
}

template <class STRMAP>
Status StdDBMImpl<STRMAP>::Open(const std::string& path, bool writable, int32_t options) {
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

template <class STRMAP>
Status StdDBMImpl<STRMAP>::Close() {
  std::lock_guard<SpinSharedMutex> lock(mutex_);
  if (!open_) {
    return Status(Status::PRECONDITION_ERROR, "not opened database");
  }
  Status status(Status::SUCCESS);
  if (writable_) {
    status |= ExportRecords();
  }
  status |= file_->Close();
  CancelIterators();
  map_.clear();
  open_ = false;
  writable_ = false;
  open_options_ = 0;
  path_.clear();
  timestamp_ = 0;
  return status;
}

template <class STRMAP>
Status StdDBMImpl<STRMAP>::Process(
    std::string_view key, DBM::RecordProcessor* proc, bool writable) {
  const std::string key_str(key);
  if (writable) {
    std::lock_guard<SpinSharedMutex> lock(mutex_);
    auto it = map_.find(key_str);
    if (it == map_.end()) {
      const std::string_view new_value = proc->ProcessEmpty(key);
      if (new_value.data() != DBM::RecordProcessor::NOOP.data() &&
          new_value.data() != DBM::RecordProcessor::REMOVE.data()) {
        if (update_logger_ != nullptr) {
          update_logger_->WriteSet(key, new_value);
        }
        map_.emplace(std::move(key_str), new_value);
      }
    } else {
      const std::string_view new_value = proc->ProcessFull(key, it->second);
      if (new_value.data() == DBM::RecordProcessor::NOOP.data()) {
      } else if (new_value.data() == DBM::RecordProcessor::REMOVE.data()) {
        if (update_logger_ != nullptr) {
          update_logger_->WriteRemove(key);
        }
        for (auto* iterator : iterators_) {
          if (iterator->it_ == it) {
            ++iterator->it_;
          }
        }
        map_.erase(it);
      } else {
        if (update_logger_ != nullptr) {
          update_logger_->WriteSet(key, new_value);
        }
        it->second = new_value;
      }
    }
  } else {
    std::shared_lock<SpinSharedMutex> lock(mutex_);
    const auto& const_map = map_;
    const auto it = const_map.find(key_str);
    if (it == const_map.end()) {
      proc->ProcessEmpty(key_str);
    } else {
      proc->ProcessFull(key_str, it->second);
    }
  }
  return Status(Status::SUCCESS);
}

template <class STRMAP>
Status StdDBMImpl<STRMAP>::ProcessMulti(
    const std::vector<std::pair<std::string_view, DBM::RecordProcessor*>>& key_proc_pairs,
    bool writable) {
  if (writable) {
    std::lock_guard<SpinSharedMutex> lock(mutex_);
    for (const auto& key_proc : key_proc_pairs) {
      const std::string_view key = key_proc.first;
      DBM::RecordProcessor* proc = key_proc.second;
      const std::string key_str(key);
      auto it = map_.find(key_str);
      if (it == map_.end()) {
        const std::string_view new_value = proc->ProcessEmpty(key);
        if (new_value.data() != DBM::RecordProcessor::NOOP.data() &&
            new_value.data() != DBM::RecordProcessor::REMOVE.data()) {
          if (update_logger_ != nullptr) {
            update_logger_->WriteSet(key, new_value);
          }
          map_.emplace(std::move(key_str), new_value);
        }
      } else {
        const std::string_view new_value = proc->ProcessFull(key, it->second);
        if (new_value.data() == DBM::RecordProcessor::NOOP.data()) {
        } else if (new_value.data() == DBM::RecordProcessor::REMOVE.data()) {
          if (update_logger_ != nullptr) {
            update_logger_->WriteRemove(key);
          }
          for (auto* iterator : iterators_) {
            if (iterator->it_ == it) {
              ++iterator->it_;
            }
          }
          map_.erase(it);
        } else {
          if (update_logger_ != nullptr) {
            update_logger_->WriteSet(key, new_value);
          }
          it->second = new_value;
        }
      }
    }
  } else {
    std::shared_lock<SpinSharedMutex> lock(mutex_);
    const auto& const_map = map_;
    for (const auto& key_proc : key_proc_pairs) {
      const std::string_view key = key_proc.first;
      DBM::RecordProcessor* proc = key_proc.second;
      const std::string key_str(key);
      const auto it = const_map.find(key_str);
      if (it == const_map.end()) {
        proc->ProcessEmpty(key_str);
      } else {
        proc->ProcessFull(key_str, it->second);
      }
    }
  }
  return Status(Status::SUCCESS);
}

template <class STRMAP>
Status StdDBMImpl<STRMAP>::ProcessFirst(DBM::RecordProcessor* proc, bool writable) {
  if (writable) {
    std::lock_guard<SpinSharedMutex> lock(mutex_);
    auto it = map_.begin();
    if (it != map_.end()) {
      const std::string_view new_value = proc->ProcessFull(it->first, it->second);
      if (new_value.data() == DBM::RecordProcessor::REMOVE.data()) {
        if (update_logger_ != nullptr) {
          update_logger_->WriteRemove(it->first);
        }
        for (auto* iterator : iterators_) {
          if (iterator->it_ == it) {
            ++iterator->it_;
          }
        }
        map_.erase(it);
      } else if (new_value.data() != DBM::RecordProcessor::NOOP.data()) {
        if (update_logger_ != nullptr) {
          update_logger_->WriteSet(it->first, new_value);
        }
        it->second = new_value;
      }
      return Status(Status::SUCCESS);
    }
  } else {
    std::shared_lock<SpinSharedMutex> lock(mutex_);
    const auto& const_map = map_;
    auto it = const_map.begin();
    if (it != const_map.end()) {
      proc->ProcessFull(it->first, it->second);
      return Status(Status::SUCCESS);
    }
  }
  return Status(Status::NOT_FOUND_ERROR);
}

template <class STRMAP>
Status StdDBMImpl<STRMAP>::ProcessEach(DBM::RecordProcessor* proc, bool writable) {
  if (writable) {
    std::lock_guard<SpinSharedMutex> lock(mutex_);
    proc->ProcessEmpty(DBM::RecordProcessor::NOOP);
    auto it = map_.begin();
    while (it != map_.end()) {
      const std::string_view new_value = proc->ProcessFull(it->first, it->second);
      if (new_value.data() == DBM::RecordProcessor::NOOP.data()) {
        ++it;
      } else if (new_value.data() == DBM::RecordProcessor::REMOVE.data()) {
        if (update_logger_ != nullptr) {
          update_logger_->WriteRemove(it->first);
        }
        for (auto* iterator : iterators_) {
          if (iterator->it_ == it) {
            ++iterator->it_;
          }
        }
        it = map_.erase(it);
      } else {
        if (update_logger_ != nullptr) {
          update_logger_->WriteSet(it->first, new_value);
        }
        it->second = new_value;
        ++it;
      }
    }
    proc->ProcessEmpty(DBM::RecordProcessor::NOOP);
  } else {
    std::shared_lock<SpinSharedMutex> lock(mutex_);
    proc->ProcessEmpty(DBM::RecordProcessor::NOOP);
    const auto& const_map = map_;
    auto it = const_map.begin();
    while (it != const_map.end()) {
      proc->ProcessFull(it->first, it->second);
      ++it;
    }
    proc->ProcessEmpty(DBM::RecordProcessor::NOOP);
  }
  return Status(Status::SUCCESS);
}

template <class STRMAP>
Status StdDBMImpl<STRMAP>::Count(int64_t* count) {
  std::shared_lock<SpinSharedMutex> lock(mutex_);
  *count = map_.size();
  return Status(Status::SUCCESS);
}

template <class STRMAP>
Status StdDBMImpl<STRMAP>::GetFileSize(int64_t* size) {
  std::shared_lock<SpinSharedMutex> lock(mutex_);
  if (!open_) {
    return Status(Status::PRECONDITION_ERROR, "not opened database");
  }
  *size = file_->GetSizeSimple();
  return Status(Status::SUCCESS);
}

template <class STRMAP>
Status StdDBMImpl<STRMAP>::GetFilePath(std::string* path) {
  std::shared_lock<SpinSharedMutex> lock(mutex_);
  if (!open_) {
    return Status(Status::PRECONDITION_ERROR, "not opened database");
  }
  *path = path_;
  return Status(Status::SUCCESS);
}

template <class STRMAP>
Status StdDBMImpl<STRMAP>::GetTimestamp(double* timestamp) {
  std::shared_lock<SpinSharedMutex> lock(mutex_);
  if (!open_) {
    return Status(Status::PRECONDITION_ERROR, "not opened database");
  }
  *timestamp = timestamp_;
  return Status(Status::SUCCESS);
}

template <class STRMAP>
Status StdDBMImpl<STRMAP>::Clear() {
  std::lock_guard<SpinSharedMutex> lock(mutex_);
  if (update_logger_ != nullptr) {
    update_logger_->WriteClear();
  }
  map_.clear();
  CancelIterators();
  return Status(Status::SUCCESS);
}

template <class STRMAP>
Status StdDBMImpl<STRMAP>::Rebuild() {
  std::lock_guard<SpinSharedMutex> lock(mutex_);
  CancelIterators();
  return Status(Status::SUCCESS);
}

template <>
Status StdDBMImpl<StringHashMap>::Rebuild() {
  std::lock_guard<SpinSharedMutex> lock(mutex_);
  map_.rehash(map_.size() * 2 + 1);
  CancelIterators();
  return Status(Status::SUCCESS);
}

template <class STRMAP>
Status StdDBMImpl<STRMAP>::ShouldBeRebuilt(bool* tobe) {
  std::shared_lock<SpinSharedMutex> lock(mutex_);
  *tobe = false;
  return Status(Status::SUCCESS);
}

template <>
Status StdDBMImpl<StringHashMap>::ShouldBeRebuilt(bool* tobe) {
  std::shared_lock<SpinSharedMutex> lock(mutex_);
  *tobe = map_.load_factor() > 1.0;
  return Status(Status::SUCCESS);
}

template <class STRMAP>
Status StdDBMImpl<STRMAP>::Synchronize(bool hard, DBM::FileProcessor* proc) {
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
  return Status(Status::SUCCESS);
}

template <class STRMAP>
void StdDBMImpl<STRMAP>::Inspect(std::vector<std::pair<std::string, std::string>>* meta) {
  std::shared_lock<SpinSharedMutex> lock(mutex_);
  auto Add = [&](const std::string& name, const std::string& value) {
    meta->emplace_back(std::make_pair(name, value));
  };
  if (open_) {
    Add("path", path_);
    Add("timestamp", SPrintF("%.6f", timestamp_));
  }
  Add("num_records", ToString(map_.size()));
}

template <>
void StdDBMImpl<StringHashMap>::Inspect(std::vector<std::pair<std::string, std::string>>* meta) {
  std::shared_lock<SpinSharedMutex> lock(mutex_);
 auto Add = [&](const std::string& name, const std::string& value) {
    meta->emplace_back(std::make_pair(name, value));
  };
  if (open_) {
    Add("path", path_);
  }
  Add("num_records", ToString(map_.size()));
  Add("num_buckets", ToString(map_.bucket_count()));
}

template <class STRMAP>
bool StdDBMImpl<STRMAP>::IsOpen() {
  std::shared_lock<SpinSharedMutex> lock(mutex_);
  return open_;
}

template <class STRMAP>
bool StdDBMImpl<STRMAP>::IsWritable() {
  std::shared_lock<SpinSharedMutex> lock(mutex_);
  return open_ && writable_;
}

template <class STRMAP>
std::unique_ptr<DBM> StdDBMImpl<STRMAP>::MakeDBM() {
  std::shared_lock<SpinSharedMutex> lock(mutex_);
  return std::make_unique<StdTreeDBM>();
}

template <>
std::unique_ptr<DBM> StdDBMImpl<StringHashMap>::MakeDBM() {
  std::shared_lock<SpinSharedMutex> lock(mutex_);
  return std::make_unique<StdHashDBM>(map_.bucket_count());
}

template <class STRMAP>
DBM::UpdateLogger* StdDBMImpl<STRMAP>::GetUpdateLogger() {
  return update_logger_;
}

template <class STRMAP>
void StdDBMImpl<STRMAP>::SetUpdateLogger(DBM::UpdateLogger* update_logger) {
  update_logger_ = update_logger;
}

template <class STRMAP>
File* StdDBMImpl<STRMAP>::GetInternalFile() const {
  return file_.get();
}

template <class STRMAP>
void StdDBMImpl<STRMAP>::CancelIterators() {
  for (auto* iterator : iterators_) {
    iterator->it_ = map_.end();
  }
}

template <class STRMAP>
Status StdDBMImpl<STRMAP>::ImportRecords() {
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
    map_.emplace(key_store, value);
  }
  return Status(Status::SUCCESS);
}

template <class STRMAP>
Status StdDBMImpl<STRMAP>::ExportRecords() {
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
  FlatRecord rec(file_.get());
  std::map<std::string, std::string> meta;
  if (typeid(map_) == typeid(StringHashMap)) {
    meta["class"] = "StdHashDBM";
  } else {
    meta["class"] = "StdTreeDBM";
  }
  meta["timestamp"] = SPrintF("%.6f", GetWallTime());
  meta["num_records"] = ToString(map_.size());
  status |= rec.Write(SerializeStrMap(meta), FlatRecord::RECORD_METADATA);
  auto it = map_.begin();
  while (it != map_.end()) {
    status |= rec.Write(it->first);
    status |= rec.Write(it->second);
    if (status != Status::SUCCESS) {
      break;
    }
    ++it;
  }
  status |= file_->Close();
  status |= RenameFile(export_path, path_);
  RemoveFile(export_path);
  status |= file_->Open(path_, true, open_options_ & ~File::OPEN_TRUNCATE);
  return status;
}

template <class STRMAP>
StdDBMIteratorImpl<STRMAP>::StdDBMIteratorImpl(StdDBMImpl<STRMAP>* dbm)
    : dbm_(dbm) {
  std::lock_guard<SpinSharedMutex> lock(dbm_->mutex_);
  dbm_->iterators_.emplace_back(this);
  it_ = dbm_->map_.end();
}

template <class STRMAP>
StdDBMIteratorImpl<STRMAP>::~StdDBMIteratorImpl() {
  if (dbm_ != nullptr) {
    std::lock_guard<SpinSharedMutex> lock(dbm_->mutex_);
    dbm_->iterators_.remove(this);
  }
}

template <class STRMAP>
Status StdDBMIteratorImpl<STRMAP>::First() {
  std::shared_lock<SpinSharedMutex> lock(dbm_->mutex_);
  const auto& const_map = dbm_->map_;
  it_ = const_map.begin();
  return Status(Status::SUCCESS);
}

template <class STRMAP>
Status StdDBMIteratorImpl<STRMAP>::Last() {
  return Status(Status::NOT_IMPLEMENTED_ERROR);
}

template <>
Status StdDBMIteratorImpl<StringTreeMap>::Last() {
  std::shared_lock<SpinSharedMutex> lock(dbm_->mutex_);
  const auto& const_map = dbm_->map_;
  it_ = const_map.end();
  if (it_ != const_map.begin()) {
    it_--;
  }
  return Status(Status::SUCCESS);
}

template <class STRMAP>
Status StdDBMIteratorImpl<STRMAP>::Jump(std::string_view key) {
  std::shared_lock<SpinSharedMutex> lock(dbm_->mutex_);
  const auto& const_map = dbm_->map_;
  it_ = const_map.find(std::string(key));
  if (it_ == dbm_->map_.end()) {
    return Status(Status::NOT_FOUND_ERROR);
  }
  return Status(Status::SUCCESS);
}

template <>
Status StdDBMIteratorImpl<StringTreeMap>::Jump(std::string_view key) {
  std::shared_lock<SpinSharedMutex> lock(dbm_->mutex_);
  const auto& const_map = dbm_->map_;
  it_ = const_map.lower_bound(std::string(key));
  return Status(Status::SUCCESS);
}

template <class STRMAP>
Status StdDBMIteratorImpl<STRMAP>::JumpLower(std::string_view key, bool inclusive) {
  return Status(Status::NOT_IMPLEMENTED_ERROR);
}

template <>
Status StdDBMIteratorImpl<StringTreeMap>::JumpLower(std::string_view key, bool inclusive) {
  std::shared_lock<SpinSharedMutex> lock(dbm_->mutex_);
  const auto& const_map = dbm_->map_;
  it_ = const_map.lower_bound(std::string(key));
  if (it_ == const_map.end()) {
    if (it_ == const_map.begin()) {
      return Status(Status::SUCCESS);
    }
    assert(!const_map.empty());
    it_--;
  }
  while (true) {
    const bool ok = inclusive ? it_->first <= key : it_->first < key;
    if (ok) {
      return Status(Status::SUCCESS);
    }
    if (it_ == const_map.begin()) {
      break;
    }
    it_--;
  }
  it_ = const_map.end();
  return Status(Status::SUCCESS);
}

template <class STRMAP>
Status StdDBMIteratorImpl<STRMAP>::JumpUpper(std::string_view key, bool inclusive) {
  std::shared_lock<SpinSharedMutex> lock(dbm_->mutex_);
  return Status(Status::NOT_IMPLEMENTED_ERROR);
}

template <>
Status StdDBMIteratorImpl<StringTreeMap>::JumpUpper(std::string_view key, bool inclusive) {
  std::shared_lock<SpinSharedMutex> lock(dbm_->mutex_);
  const auto& const_map = dbm_->map_;
  it_ = inclusive ? const_map.lower_bound(std::string(key)) :
      const_map.upper_bound(std::string(key));
  return Status(Status::SUCCESS);
}

template <class STRMAP>
Status StdDBMIteratorImpl<STRMAP>::Next() {
  std::shared_lock<SpinSharedMutex> lock(dbm_->mutex_);
  const auto& const_map = dbm_->map_;
  if (it_ == const_map.end()) {
    return Status(Status::NOT_FOUND_ERROR);
  }
  if (it_ != const_map.end()) {
    ++it_;
  }
  return Status(Status::SUCCESS);
}

template <class STRMAP>
Status StdDBMIteratorImpl<STRMAP>::Previous() {
  return Status(Status::NOT_IMPLEMENTED_ERROR);
}

template <>
Status StdDBMIteratorImpl<StringTreeMap>::Previous() {
  std::shared_lock<SpinSharedMutex> lock(dbm_->mutex_);
  const auto& const_map = dbm_->map_;
  if (it_ == const_map.end()) {
    return Status(Status::NOT_FOUND_ERROR);
  }
  if (it_ == const_map.begin()) {
    it_ = const_map.end();
  } else {
    --it_;
  }
  return Status(Status::SUCCESS);
}

template <class STRMAP>
Status StdDBMIteratorImpl<STRMAP>::Process(DBM::RecordProcessor* proc, bool writable) {
  if (writable) {
    std::lock_guard<SpinSharedMutex> lock(dbm_->mutex_);
    if (it_ == dbm_->map_.end()) {
      return Status(Status::NOT_FOUND_ERROR);
    }
    const std::string_view new_value = proc->ProcessFull(it_->first, it_->second);
    if (new_value.data() == DBM::RecordProcessor::NOOP.data()) {
    } else if (new_value.data() == DBM::RecordProcessor::REMOVE.data()) {
      if (dbm_->update_logger_ != nullptr) {
        dbm_->update_logger_->WriteRemove(it_->first);
      }
      for (auto* iterator : dbm_->iterators_) {
        if (iterator != this && iterator->it_ == it_) {
          ++iterator->it_;
        }
      }
      dbm_->map_.erase(it_++);
    } else {
      if (dbm_->update_logger_ != nullptr) {
        dbm_->update_logger_->WriteSet(it_->first, new_value);
      }
      dbm_->map_.find(it_->first)->second = new_value;
    }
  } else {
    std::shared_lock<SpinSharedMutex> lock(dbm_->mutex_);
    if (it_ == dbm_->map_.end()) {
      return Status(Status::NOT_FOUND_ERROR);
    }
    proc->ProcessFull(it_->first, it_->second);
  }
  return Status(Status::SUCCESS);
}

class StdHashDBMImpl final : public StdDBMImpl<StringHashMap> {
 public:
  explicit StdHashDBMImpl(std::unique_ptr<File> file, int64_t num_buckets)
      : StdDBMImpl<StringHashMap>(std::move(file), num_buckets) {}
};

class StdHashDBMIteratorImpl final : public StdDBMIteratorImpl<StringHashMap> {
 public:
  StdHashDBMIteratorImpl(StdHashDBMImpl* dbm) : StdDBMIteratorImpl<StringHashMap>(dbm) {}
};

StdHashDBM::StdHashDBM(int64_t num_buckets) {
  if (num_buckets < 1) {
    num_buckets = DEFAULT_NUM_BUCKETS;
  }
  impl_ = new StdHashDBMImpl(std::make_unique<MemoryMapParallelFile>(), num_buckets);
}

StdHashDBM::StdHashDBM(std::unique_ptr<File> file, int64_t num_buckets) {
  if (num_buckets < 1) {
    num_buckets = DEFAULT_NUM_BUCKETS;
  }
  impl_ = new StdHashDBMImpl(std::move(file), num_buckets);
}

StdHashDBM::~StdHashDBM() {
  delete impl_;
}

Status StdHashDBM::Open(const std::string& path, bool writable, int32_t options) {
  return impl_->Open(path, writable, options);
}

Status StdHashDBM::Close() {
  return impl_->Close();
}

Status StdHashDBM::Process(std::string_view key, RecordProcessor* proc, bool writable) {
  assert(proc != nullptr);
  return impl_->Process(key, proc, writable);
}

Status StdHashDBM::ProcessMulti(
    const std::vector<std::pair<std::string_view, RecordProcessor*>>& key_proc_pairs,
    bool writable) {
  return impl_->ProcessMulti(key_proc_pairs, writable);
}

Status StdHashDBM::ProcessFirst(RecordProcessor* proc, bool writable) {
  assert(proc != nullptr);
  return impl_->ProcessFirst(proc, writable);
}

Status StdHashDBM::ProcessEach(RecordProcessor* proc, bool writable) {
  assert(proc != nullptr);
  return impl_->ProcessEach(proc, writable);
}

Status StdHashDBM::Count(int64_t* count) {
  assert(count != nullptr);
  return impl_->Count(count);
}

Status StdHashDBM::GetFileSize(int64_t* size) {
  assert(size != nullptr);
  return impl_->GetFileSize(size);
}

Status StdHashDBM::GetFilePath(std::string* path) {
  assert(path != nullptr);
  return impl_->GetFilePath(path);
}

Status StdHashDBM::GetTimestamp(double* timestamp) {
  assert(timestamp != nullptr);
  return impl_->GetTimestamp(timestamp);
}

Status StdHashDBM::Clear() {
  return impl_->Clear();
}

Status StdHashDBM::Rebuild() {
  return impl_->Rebuild();
}

Status StdHashDBM::ShouldBeRebuilt(bool* tobe) {
  assert(tobe != nullptr);
  return impl_->ShouldBeRebuilt(tobe);
}

Status StdHashDBM::Synchronize(bool hard, FileProcessor* proc) {
  return impl_->Synchronize(hard, proc);
}

std::vector<std::pair<std::string, std::string>> StdHashDBM::Inspect() {
  std::vector<std::pair<std::string, std::string>> meta;
  meta.emplace_back(std::make_pair("class", "StdHashDBM"));
  impl_->Inspect(&meta);
  return meta;
}

bool StdHashDBM::IsOpen() const {
  return impl_->IsOpen();
}

bool StdHashDBM::IsWritable() const {
  return impl_->IsWritable();
}

std::unique_ptr<DBM::Iterator> StdHashDBM::MakeIterator() {
  std::unique_ptr<StdHashDBM::Iterator> iter(new StdHashDBM::Iterator(impl_));
  return iter;
}

std::unique_ptr<DBM> StdHashDBM::MakeDBM() const {
  return impl_->MakeDBM();
}

DBM::UpdateLogger* StdHashDBM::GetUpdateLogger() const {
  return impl_->GetUpdateLogger();
}

void StdHashDBM::SetUpdateLogger(UpdateLogger* update_logger) {
  impl_->SetUpdateLogger(update_logger);
}

File* StdHashDBM::GetInternalFile() const {
  return impl_->GetInternalFile();
}

StdHashDBM::Iterator::Iterator(StdHashDBMImpl* dbm_impl) {
  impl_ = new StdHashDBMIteratorImpl(dbm_impl);
}

StdHashDBM::Iterator::~Iterator() {
  delete impl_;
}

Status StdHashDBM::Iterator::First() {
  return impl_->First();
}

Status StdHashDBM::Iterator::Last() {
  return impl_->Last();
}

Status StdHashDBM::Iterator::Jump(std::string_view key) {
  return impl_->Jump(key);
}

Status StdHashDBM::Iterator::JumpLower(std::string_view key, bool inclusive) {
  return impl_->JumpLower(key, inclusive);
}

Status StdHashDBM::Iterator::JumpUpper(std::string_view key, bool inclusive) {
  return impl_->JumpUpper(key, inclusive);
}

Status StdHashDBM::Iterator::Next() {
  return impl_->Next();
}

Status StdHashDBM::Iterator::Previous() {
  return impl_->Previous();
}

Status StdHashDBM::Iterator::Process(RecordProcessor* proc, bool writable) {
  assert(proc != nullptr);
  return impl_->Process(proc, writable);
}

class StdTreeDBMImpl final : public StdDBMImpl<StringTreeMap> {
 public:
  explicit StdTreeDBMImpl(std::unique_ptr<File> file)
      : StdDBMImpl<StringTreeMap>(std::move(file), -1) {}
};

class StdTreeDBMIteratorImpl final : public StdDBMIteratorImpl<StringTreeMap> {
 public:
  StdTreeDBMIteratorImpl(StdTreeDBMImpl* dbm) : StdDBMIteratorImpl<StringTreeMap>(dbm) {}
};

StdTreeDBM::StdTreeDBM() {
  impl_ = new StdTreeDBMImpl(std::make_unique<MemoryMapParallelFile>());
}

StdTreeDBM::StdTreeDBM(std::unique_ptr<File> file) {
  impl_ = new StdTreeDBMImpl(std::move(file));
}

StdTreeDBM::~StdTreeDBM() {
  delete impl_;
}

Status StdTreeDBM::Open(const std::string& path, bool writable, int32_t options) {
  return impl_->Open(path, writable, options);
}

Status StdTreeDBM::Close() {
  return impl_->Close();
}

Status StdTreeDBM::Process(std::string_view key, RecordProcessor* proc, bool writable) {
  assert(proc != nullptr);
  return impl_->Process(key, proc, writable);
}

Status StdTreeDBM::ProcessMulti(
    const std::vector<std::pair<std::string_view, RecordProcessor*>>& key_proc_pairs,
    bool writable) {
  return impl_->ProcessMulti(key_proc_pairs, writable);
}

Status StdTreeDBM::ProcessFirst(RecordProcessor* proc, bool writable) {
  assert(proc != nullptr);
  return impl_->ProcessFirst(proc, writable);
}

Status StdTreeDBM::ProcessEach(RecordProcessor* proc, bool writable) {
  assert(proc != nullptr);
  return impl_->ProcessEach(proc, writable);
}

Status StdTreeDBM::Count(int64_t* count) {
  assert(count != nullptr);
  return impl_->Count(count);
}

Status StdTreeDBM::GetFileSize(int64_t* size) {
  assert(size != nullptr);
  return impl_->GetFileSize(size);
}

Status StdTreeDBM::GetFilePath(std::string* path) {
  assert(path != nullptr);
  return impl_->GetFilePath(path);
}

Status StdTreeDBM::GetTimestamp(double* timestamp) {
  assert(timestamp != nullptr);
  return impl_->GetTimestamp(timestamp);
}

Status StdTreeDBM::Clear() {
  return impl_->Clear();
}

Status StdTreeDBM::Rebuild() {
  return impl_->Rebuild();
}

Status StdTreeDBM::ShouldBeRebuilt(bool* tobe) {
  assert(tobe != nullptr);
  return impl_->ShouldBeRebuilt(tobe);
}

Status StdTreeDBM::Synchronize(bool hard, FileProcessor* proc) {
  return impl_->Synchronize(hard, proc);
}

std::vector<std::pair<std::string, std::string>> StdTreeDBM::Inspect() {
  std::vector<std::pair<std::string, std::string>> meta;
  meta.emplace_back(std::make_pair("class", "StdTreeDBM"));
  impl_->Inspect(&meta);
  return meta;
}

bool StdTreeDBM::IsOpen() const {
  return impl_->IsOpen();
}

bool StdTreeDBM::IsWritable() const {
  return impl_->IsWritable();
}

std::unique_ptr<DBM::Iterator> StdTreeDBM::MakeIterator() {
  std::unique_ptr<StdTreeDBM::Iterator> iter(new StdTreeDBM::Iterator(impl_));
  return iter;
}

std::unique_ptr<DBM> StdTreeDBM::MakeDBM() const {
  return impl_->MakeDBM();
}

DBM::UpdateLogger* StdTreeDBM::GetUpdateLogger() const {
  return impl_->GetUpdateLogger();
}

void StdTreeDBM::SetUpdateLogger(UpdateLogger* update_logger) {
  impl_->SetUpdateLogger(update_logger);
}

File* StdTreeDBM::GetInternalFile() const {
  return impl_->GetInternalFile();
}

StdTreeDBM::Iterator::Iterator(StdTreeDBMImpl* dbm_impl) {
  impl_ = new StdTreeDBMIteratorImpl(dbm_impl);
}

StdTreeDBM::Iterator::~Iterator() {
  delete impl_;
}

Status StdTreeDBM::Iterator::First() {
  return impl_->First();
}

Status StdTreeDBM::Iterator::Last() {
  return impl_->Last();
}

Status StdTreeDBM::Iterator::Jump(std::string_view key) {
  return impl_->Jump(key);
}

Status StdTreeDBM::Iterator::JumpLower(std::string_view key, bool inclusive) {
  return impl_->JumpLower(key, inclusive);
}

Status StdTreeDBM::Iterator::JumpUpper(std::string_view key, bool inclusive) {
  return impl_->JumpUpper(key, inclusive);
}

Status StdTreeDBM::Iterator::Next() {
  return impl_->Next();
}

Status StdTreeDBM::Iterator::Previous() {
  return impl_->Previous();
}

Status StdTreeDBM::Iterator::Process(RecordProcessor* proc, bool writable) {
  assert(proc != nullptr);
  return impl_->Process(proc, writable);
}

}  // namespace tkrzw

// END OF FILE
