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

#include "tkrzw_sys_config.h"

#include "tkrzw_dbm.h"
#include "tkrzw_dbm_common_impl.h"
#include "tkrzw_dbm_skip.h"
#include "tkrzw_dbm_skip_impl.h"
#include "tkrzw_file.h"
#include "tkrzw_file_mmap.h"
#include "tkrzw_file_pos.h"
#include "tkrzw_file_std.h"
#include "tkrzw_file_util.h"
#include "tkrzw_lib_common.h"
#include "tkrzw_str_util.h"
#include "tkrzw_thread_util.h"
#include "tkrzw_time_util.h"

namespace tkrzw {

static constexpr int32_t METADATA_SIZE = 128;
static const char META_MAGIC_DATA[] = "TkrzwSDB\n";
static constexpr int32_t META_OFFSET_CYCLIC_MAGIC_FRONT = 9;
static constexpr int32_t META_OFFSET_PKG_MAJOR_VERSION = 10;
static constexpr int32_t META_OFFSET_PKG_MINOR_VERSION = 11;
static constexpr int32_t META_OFFSET_OFFSET_WIDTH = 12;
static constexpr int32_t META_OFFSET_STEP_UNIT = 13;
static constexpr int32_t META_OFFSET_MAX_LEVEL = 14;
static constexpr int32_t META_OFFSET_CLOSURE_FLAGS = 15;
static constexpr int32_t META_OFFSET_NUM_RECORDS = 24;
static constexpr int32_t META_OFFSET_EFF_DATA_SIZE = 32;
static constexpr int32_t META_OFFSET_FILE_SIZE = 40;
static constexpr int32_t META_OFFSET_TIMESTAMP = 48;
static constexpr int32_t META_OFFSET_DB_TYPE = 56;
static constexpr int32_t META_OFFSET_OPAQUE = 62;
static constexpr int32_t META_OFFSET_CYCLIC_MAGIC_BACK = 127;
static constexpr int32_t MIN_OFFSET_WIDTH = 3;
static constexpr int32_t MAX_OFFSET_WIDTH = 6;
static constexpr int32_t MIN_STEP_UNIT = 2;
static constexpr int32_t MAX_STEP_UNIT = 64;
static constexpr int32_t MIN_MAX_LEVEL = 1;
static constexpr int32_t MAX_MAX_LEVEL = 32;
static constexpr int64_t MIN_SORT_MEM_SIZE = 1LL << 10;
static constexpr int64_t MAX_SORT_MEM_SIZE = 8LL << 30;
static constexpr int32_t MIN_MAX_CACHED_RECORDS = 1;
static constexpr int32_t MAX_MAX_CACHED_RECORDS = 1 << 24;
static constexpr int64_t MIN_DIO_BLOCK_SIZE = 512;
static const char* REBUILD_FILE_SUFFIX = ".tmp.rebuild";
static const char* SORTER_FILE_SUFFIX = ".tmp.sorter";
static const char* SORTED_FILE_SUFFIX = ".tmp.sorted";
static const char* MERGED_FILE_SUFFIX = ".tmp.merged";

enum ClosureFlag : uint8_t {
  CLOSURE_FLAG_NONE = 0,
  CLOSURE_FLAG_CLOSE = 1 << 0,
};

class SkipDBMImpl final {
  friend class SkipDBMIteratorImpl;
  typedef std::list<SkipDBMIteratorImpl*> IteratorList;
 public:
  SkipDBMImpl(std::unique_ptr<File> file);
  ~SkipDBMImpl();
  Status Open(const std::string& path, bool writable,
              int32_t options, const SkipDBM::TuningParameters& tuning_params);
  Status Close();
  Status Process(std::string_view key, DBM::RecordProcessor* proc, bool writable);
  Status Insert(std::string_view key, std::string_view value);
  Status GetByIndex(int64_t index, std::string* key, std::string* value);
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
  Status Rebuild(
      const SkipDBM::TuningParameters& tuning_params, bool skip_broken_records, bool sync_hard);
  Status ShouldBeRebuilt(bool* tobe);
  Status Synchronize(bool hard, DBM::FileProcessor* proc, SkipDBM::ReducerType reducer);
  std::vector<std::pair<std::string, std::string>> Inspect();
  bool IsOpen();
  bool IsWritable();
  bool IsHealthy();
  bool IsAutoRestored();
  std::unique_ptr<DBM> MakeDBM();
  DBM::UpdateLogger* GetUpdateLogger();
  void SetUpdateLogger(DBM::UpdateLogger* update_logger);
  File* GetInternalFile();
  int64_t GetEffectiveDataSize();
  int32_t GetDatabaseType();
  Status SetDatabaseTypeMetadata(uint32_t db_type);
  std::string GetOpaqueMetadata();
  Status SetOpaqueMetadata(const std::string& opaque);
  Status Revert();
  bool IsUpdated();
  Status MergeSkipDatabase(const std::string& src_path);
  Status ValidateRecords();

 private:
  void CancelIterators();
  Status SaveMetadata(File* file, bool finish);
  Status LoadMetadata();
  Status CheckFileBeforeOpen(File* file, const std::string& path, bool writable);
  Status CheckZeroRegion(int64_t offset, int64_t end_offset);
  Status PadFileForDirectIO();
  Status PrepareStorage();
  Status FinishStorage(SkipDBM::ReducerType reducer);
  Status DiscardStorage();
  Status UpdateRecord(std::string_view key, std::string_view new_value);
  Status WriteRecord(std::string_view key, std::string_view value, File* file);
  Status ProcessImpl(std::string_view key, DBM::RecordProcessor* proc, bool writable);

  bool open_;
  bool writable_;
  bool healthy_;
  bool auto_restored_;
  bool updated_;
  bool removed_;
  std::string path_;
  int32_t cyclic_magic_;
  int32_t pkg_major_version_;
  int32_t pkg_minor_version_;
  int32_t offset_width_;
  int32_t step_unit_;
  int32_t max_level_;
  int32_t closure_flags_;
  int64_t num_records_;
  int64_t eff_data_size_;
  int64_t file_size_;
  int64_t timestamp_;
  int32_t db_type_;
  std::string opaque_;
  IteratorList iterators_;
  std::unique_ptr<File> file_;
  std::unique_ptr<File> sorted_file_;
  int64_t record_index_;
  int64_t sort_mem_size_;
  bool insert_in_order_;
  int32_t max_cached_records_;
  std::unique_ptr<RecordSorter> record_sorter_;
  std::vector<int64_t> past_offsets_;
  std::unique_ptr<SkipRecordCache> cache_;
  DBM::UpdateLogger* update_logger_;
  int64_t old_num_records_;
  int64_t old_eff_data_size_;
  SpinSharedMutex mutex_;
};

class SkipDBMIteratorImpl final {
  friend class SkipDBMImpl;
 public:
  explicit SkipDBMIteratorImpl(SkipDBMImpl* dbm);
  ~SkipDBMIteratorImpl();
  Status First();
  Status Last();
  Status Jump(std::string_view key);
  Status JumpLower(std::string_view key, bool inclusive);
  Status JumpUpper(std::string_view key, bool inclusive);
  Status Next();
  Status Previous();
  Status Process(DBM::RecordProcessor* proc, bool writable);
  Status Get(std::string* key, std::string* value);

 private:
  void ClearPosition();

  SkipDBMImpl* dbm_;
  int64_t record_offset_;
  int64_t record_index_;
  int32_t record_size_;
};

SkipDBMImpl::SkipDBMImpl(std::unique_ptr<File> file)
    : open_(false),  writable_(false), healthy_(false), auto_restored_(false),
      updated_(false), removed_(false), path_(),
      cyclic_magic_(0), pkg_major_version_(0), pkg_minor_version_(0),
      offset_width_(SkipDBM::DEFAULT_OFFSET_WIDTH), step_unit_(SkipDBM::DEFAULT_STEP_UNIT),
      max_level_(SkipDBM::DEFAULT_MAX_LEVEL), closure_flags_(CLOSURE_FLAG_NONE),
      num_records_(0), eff_data_size_(0), file_size_(0), timestamp_(0),
      db_type_(0), opaque_(), iterators_(),
      file_(std::move(file)), sorted_file_(nullptr), record_index_(0),
      sort_mem_size_(SkipDBM::DEFAULT_SORT_MEM_SIZE), insert_in_order_(false),
      max_cached_records_(SkipDBM::DEFAULT_MAX_CACHED_RECORDS),
      record_sorter_(nullptr), past_offsets_(), cache_(nullptr), update_logger_(nullptr),
      old_num_records_(0), old_eff_data_size_(0),
      mutex_() {}

SkipDBMImpl::~SkipDBMImpl() {
  if (open_) {
    Close();
  }
  for (auto* iterator : iterators_) {
    iterator->dbm_ = nullptr;
  }
}

Status SkipDBMImpl::Open(const std::string& path, bool writable,
                         int32_t options, const SkipDBM::TuningParameters& tuning_params) {
  std::lock_guard<SpinSharedMutex> lock(mutex_);
  if (open_) {
    return Status(Status::PRECONDITION_ERROR, "opened database");
  }
  const std::string norm_path = NormalizePath(path);
  if (tuning_params.offset_width >= 0) {
    offset_width_ = std::min(std::max(static_cast<int32_t>(
        tuning_params.offset_width), MIN_OFFSET_WIDTH), MAX_OFFSET_WIDTH);
  }
  if (tuning_params.step_unit >= 0) {
    step_unit_ = std::min(std::max(static_cast<int32_t>(
        tuning_params.step_unit), MIN_STEP_UNIT), MAX_STEP_UNIT);
  }
  if (tuning_params.max_level >= 0) {
    max_level_ = std::min(std::max(static_cast<int32_t>(
        tuning_params.max_level), MIN_MAX_LEVEL), MAX_MAX_LEVEL);
  }
  if (tuning_params.sort_mem_size >= 0) {
    sort_mem_size_ = std::min(std::max(static_cast<int64_t>(
        tuning_params.sort_mem_size), MIN_SORT_MEM_SIZE), MAX_SORT_MEM_SIZE);
  }
  insert_in_order_ = tuning_params.insert_in_order;
  if (tuning_params.max_cached_records > 0) {
    max_cached_records_ = std::min(std::max(
        tuning_params.max_cached_records, MIN_MAX_CACHED_RECORDS), MAX_MAX_CACHED_RECORDS);
  }
  Status status = CheckFileBeforeOpen(file_.get(), path, writable);
  if (status != Status::SUCCESS) {
    return status;
  }
  status = file_->Open(norm_path, writable, options);
  if (status != Status::SUCCESS) {
    return status;
  }
  if (writable && file_->GetSizeSimple() < 1) {
    file_->Truncate(METADATA_SIZE);
    const auto version_nums = StrSplit(PACKAGE_VERSION, '.');
    pkg_major_version_ = version_nums.size() > 0 ? StrToInt(version_nums[0]) : 0;
    pkg_minor_version_ = version_nums.size() > 1 ? StrToInt(version_nums[1]) : 0;
    closure_flags_ |= CLOSURE_FLAG_CLOSE;
    file_size_ = file_->GetSizeSimple();
    timestamp_ = GetWallTime() * 1000000;
    const Status status = SaveMetadata(file_.get(), true);
    if (status != Status::SUCCESS) {
      return status;
    }
  }
  status = LoadMetadata();
  if (status != Status::SUCCESS) {
    file_->Close();
    return status;
  }
  if (cyclic_magic_ < 0) {
    file_size_ = INT64MAX;
  }
  bool healthy = closure_flags_ & CLOSURE_FLAG_CLOSE;
  bool invalid_file_size = false;
  const int64_t act_file_size = file_->GetSizeSimple();
  if (file_size_ != act_file_size) {
    if (file_size_ > act_file_size) {
      healthy = false;
      invalid_file_size = true;
    } else if (file_size_ > 0) {
      if (CheckZeroRegion(file_size_, act_file_size) == Status::SUCCESS) {
        status = writable ? file_->Truncate(file_size_) : file_->TruncateFakely(file_size_);
        if (status != Status::SUCCESS) {
          healthy = false;
        }
      } else {
        healthy = false;
        invalid_file_size = true;
      }
    }
  }
  const int32_t restore_mode = tuning_params.restore_mode & 0xffff;
  const int32_t restore_options = tuning_params.restore_mode & 0xffff0000;
  auto_restored_ = false;
  if (writable && !healthy && restore_mode != SkipDBM::RESTORE_READ_ONLY) {
    if (invalid_file_size && restore_mode != SkipDBM::RESTORE_NOOP) {
      file_->Close();
      const std::string tmp_path = norm_path + ".tmp.restore";
      RemoveFile(tmp_path);
      status = SkipDBM::RestoreDatabase(norm_path, tmp_path);
      if (status != Status::SUCCESS) {
        RemoveFile(tmp_path);
        return status;
      }
      if (restore_options & SkipDBM::RESTORE_WITH_HARDSYNC) {
        status = SynchronizeFile(tmp_path);
        if (status != Status::SUCCESS) {
          RemoveFile(tmp_path);
          return status;
        }
      }
      status = RenameFile(tmp_path, norm_path);
      if (status != Status::SUCCESS) {
        RemoveFile(tmp_path);
        return status;
      }
      if (restore_options & SkipDBM::RESTORE_WITH_HARDSYNC) {
        status = SynchronizeFile(norm_path);
        if (status != Status::SUCCESS) {
          return status;
        }
      }
      status = file_->Open(norm_path, writable, options);
      if (status != Status::SUCCESS) {
        return status;
      }
      status = LoadMetadata();
      if (status != Status::SUCCESS) {
        file_->Close();
        return status;
      }
    } else {
      closure_flags_ |= CLOSURE_FLAG_CLOSE;
    }
    healthy = true;
    auto_restored_ = true;
  }
  path_ = norm_path;
  if (writable && healthy) {
    status = SaveMetadata(file_.get(), false);
    if (status != Status::SUCCESS) {
      path_ .clear();
      return status;
    }
    status = PrepareStorage();
    if (status != Status::SUCCESS) {
      path_ .clear();
      return status;
    }
  }
  cache_ = std::make_unique<SkipRecordCache>(step_unit_, max_cached_records_, num_records_);
  open_ = true;
  writable_ = writable;
  healthy_ = healthy;
  updated_ = false;
  removed_ = false;
  return Status(Status::SUCCESS);
}

Status SkipDBMImpl::Close() {
  std::lock_guard<SpinSharedMutex> lock(mutex_);
  if (!open_) {
    return Status(Status::PRECONDITION_ERROR, "not opened database");
  }
  CancelIterators();
  Status status(Status::SUCCESS);
  if (writable_ && healthy_) {
    if (updated_) {
      status |= FinishStorage(nullptr);
    } else {
      status |= DiscardStorage();
      file_size_ = file_->GetSizeSimple();
      timestamp_ = GetWallTime() * 1000000;
      status |= SaveMetadata(file_.get(), true);
    }
  }
  status |= file_->Close();
  open_ = false;
  writable_ = false;
  healthy_ = false;
  auto_restored_ = false;
  updated_ = false;
  removed_ = false;
  path_.clear();
  pkg_major_version_ = 0;
  pkg_minor_version_ = 0;
  offset_width_ = SkipDBM::DEFAULT_OFFSET_WIDTH;
  step_unit_ = SkipDBM::DEFAULT_STEP_UNIT;
  max_level_ = SkipDBM::DEFAULT_MAX_LEVEL;
  closure_flags_ = CLOSURE_FLAG_NONE;
  num_records_ = 0;
  eff_data_size_ = 0;
  file_size_ = 0;
  timestamp_ = 0;
  db_type_ = 0;
  opaque_.clear();
  sort_mem_size_ = SkipDBM::DEFAULT_SORT_MEM_SIZE;
  insert_in_order_ = false;
  record_sorter_.reset(nullptr);
  past_offsets_.clear();
  cache_.reset(nullptr);
  old_num_records_ = 0;
  old_eff_data_size_ = 0;
  return status;
}

Status SkipDBMImpl::Process(std::string_view key, DBM::RecordProcessor* proc, bool writable) {
  if (writable) {
    std::lock_guard<SpinSharedMutex> lock(mutex_);
    if (!open_) {
      return Status(Status::PRECONDITION_ERROR, "not opened database");
    }
    if (!writable_) {
      return Status(Status::PRECONDITION_ERROR, "not writable database");
    }
    if (!healthy_) {
      return Status(Status::PRECONDITION_ERROR, "not healthy database");
    }
    Status status = ProcessImpl(key, proc, true);
    if (status != Status::SUCCESS) {
      return status;
    }
  } else {
    std::shared_lock<SpinSharedMutex> lock(mutex_);
    if (!open_) {
      return Status(Status::PRECONDITION_ERROR, "not opened database");
    }
    Status status = ProcessImpl(key, proc, false);
    if (status != Status::SUCCESS) {
      return status;
    }
  }
  return Status(Status::SUCCESS);
}

Status SkipDBMImpl::Insert(std::string_view key, std::string_view value) {
  std::lock_guard<SpinSharedMutex> lock(mutex_);
  if (!open_) {
    return Status(Status::PRECONDITION_ERROR, "not opened database");
  }
  if (!writable_) {
    return Status(Status::PRECONDITION_ERROR, "not writable database");
  }
  if (!healthy_) {
    return Status(Status::PRECONDITION_ERROR, "not healthy database");
  }
  return UpdateRecord(key, value);
}

Status SkipDBMImpl::GetByIndex(int64_t index, std::string* key, std::string* value) {
  std::lock_guard<SpinSharedMutex> lock(mutex_);
  if (!open_) {
    return Status(Status::PRECONDITION_ERROR, "not opened database");
  }
  SkipRecord rec(file_.get(), offset_width_, step_unit_, max_level_);
  Status status = rec.SearchByIndex(METADATA_SIZE, cache_.get(), index);
  if (status != Status::SUCCESS) {
    return status;
  }
  if (key != nullptr) {
    *key = rec.GetKey();
  }
  if (value != nullptr) {
    std::string_view rec_value = rec.GetValue();
    if (rec_value.data() == nullptr) {
      status = rec.ReadBody();
      if (status != Status::SUCCESS) {
        return status;
      }
      rec_value = rec.GetValue();
    }
    *value = rec_value;
  }
  return Status(Status::SUCCESS);
}

Status SkipDBMImpl::ProcessMulti(
    const std::vector<std::pair<std::string_view, DBM::RecordProcessor*>>& key_proc_pairs,
    bool writable) {
  if (writable) {
    std::lock_guard<SpinSharedMutex> lock(mutex_);
    if (!open_) {
      return Status(Status::PRECONDITION_ERROR, "not opened database");
    }
    if (!writable_) {
      return Status(Status::PRECONDITION_ERROR, "not writable database");
    }
    if (!healthy_) {
      return Status(Status::PRECONDITION_ERROR, "not healthy database");
    }
    for (const auto& key_proc : key_proc_pairs) {
      Status status = ProcessImpl(key_proc.first, key_proc.second, true);
      if (status != Status::SUCCESS) {
        return status;
      }
    }
  } else {
    std::shared_lock<SpinSharedMutex> lock(mutex_);
    if (!open_) {
      return Status(Status::PRECONDITION_ERROR, "not opened database");
    }
    for (const auto& key_proc : key_proc_pairs) {
      Status status = ProcessImpl(key_proc.first, key_proc.second, false);
      if (status != Status::SUCCESS) {
        return status;
      }
    }
  }
  return Status(Status::SUCCESS);
}

Status SkipDBMImpl::ProcessFirst(DBM::RecordProcessor* proc, bool writable) {
  if (writable) {
    std::lock_guard<SpinSharedMutex> lock(mutex_);
    if (!open_) {
      return Status(Status::PRECONDITION_ERROR, "not opened database");
    }
    if (!writable_) {
      return Status(Status::PRECONDITION_ERROR, "not writable database");
    }
    if (!healthy_) {
      return Status(Status::PRECONDITION_ERROR, "not healthy database");
    }
    if (file_->GetSizeSimple() <= METADATA_SIZE) {
      return Status(Status::NOT_FOUND_ERROR);
    }
    tkrzw::SkipRecord rec(file_.get(), offset_width_, step_unit_, max_level_);
    Status status = rec.ReadMetadataKey(METADATA_SIZE, 0);
    if (status != Status::SUCCESS) {
      return status;
    }
    const std::string_view key = rec.GetKey();
    std::string_view value = rec.GetValue();
    if (value.data() == nullptr) {
      status = rec.ReadBody();
      if (status != Status::SUCCESS) {
        return status;
      }
      value = rec.GetValue();
    }
    const std::string_view new_value = proc->ProcessFull(key, value);
    return UpdateRecord(key, new_value);
  }
  std::shared_lock<SpinSharedMutex> lock(mutex_);
  if (!open_) {
    return Status(Status::PRECONDITION_ERROR, "not opened database");
  }
  if (file_->GetSizeSimple() <= METADATA_SIZE) {
    return Status(Status::NOT_FOUND_ERROR);
  }
  tkrzw::SkipRecord rec(file_.get(), offset_width_, step_unit_, max_level_);
  Status status = rec.ReadMetadataKey(METADATA_SIZE, 0);
  if (status != Status::SUCCESS) {
    return status;
  }
  const std::string_view key = rec.GetKey();
  std::string_view value = rec.GetValue();
  if (value.data() == nullptr) {
    status = rec.ReadBody();
    if (status != Status::SUCCESS) {
      return status;
    }
    value = rec.GetValue();
  }
  proc->ProcessFull(key, value);
  return Status(Status::SUCCESS);
}

Status SkipDBMImpl::ProcessEach(DBM::RecordProcessor* proc, bool writable) {
  if (writable) {
    std::lock_guard<SpinSharedMutex> lock(mutex_);
    if (!open_) {
      return Status(Status::PRECONDITION_ERROR, "not opened database");
    }
    if (!writable_) {
      return Status(Status::PRECONDITION_ERROR, "not writable database");
    }
    if (!healthy_) {
      return Status(Status::PRECONDITION_ERROR, "not healthy database");
    }
    proc->ProcessEmpty(DBM::RecordProcessor::NOOP);
    const int64_t end_offset = file_->GetSizeSimple();
    int64_t offset = METADATA_SIZE;
    int64_t index = 0;
    tkrzw::SkipRecord rec(file_.get(), offset_width_, step_unit_, max_level_);
    while (offset < end_offset) {
      Status status = rec.ReadMetadataKey(offset, index);
      if (status != Status::SUCCESS) {
        return status;
      }
      const std::string_view key = rec.GetKey();
      std::string_view value = rec.GetValue();
      if (value.data() == nullptr) {
        status = rec.ReadBody();
        if (status != Status::SUCCESS) {
          return status;
        }
        value = rec.GetValue();
      }
      std::string_view new_value = proc->ProcessFull(key, value);
      status = UpdateRecord(key, new_value);
      if (status != Status::SUCCESS) {
        return status;
      }
      offset += rec.GetWholeSize();
      index++;
    }
    proc->ProcessEmpty(DBM::RecordProcessor::NOOP);
  } else {
    std::shared_lock<SpinSharedMutex> lock(mutex_);
    if (!open_) {
      return Status(Status::PRECONDITION_ERROR, "not opened database");
    }
    proc->ProcessEmpty(DBM::RecordProcessor::NOOP);
    SkipRecord rec(file_.get(), offset_width_, step_unit_, max_level_);
    const int64_t end_offset = file_->GetSizeSimple();
    int64_t offset = METADATA_SIZE;
    int64_t index = 0;
    while (offset < end_offset) {
      Status status = rec.ReadMetadataKey(offset, index);
      if (status != Status::SUCCESS) {
        return status;
      }
      const std::string_view key = rec.GetKey();
      std::string_view value = rec.GetValue();
      if (value.data() == nullptr) {
        status = rec.ReadBody();
        if (status != Status::SUCCESS) {
          return status;
        }
        value = rec.GetValue();
      }
      proc->ProcessFull(key, value);
      offset += rec.GetWholeSize();
      index++;
    }
    proc->ProcessEmpty(DBM::RecordProcessor::NOOP);
  }
  return Status(Status::SUCCESS);
}

Status SkipDBMImpl::Count(int64_t* count) {
  std::shared_lock<SpinSharedMutex> lock(mutex_);
  if (!open_) {
    return Status(Status::PRECONDITION_ERROR, "not opened database");
  }
  *count = num_records_;
  return Status(Status::SUCCESS);
}

Status SkipDBMImpl::GetFileSize(int64_t* size) {
  std::shared_lock<SpinSharedMutex> lock(mutex_);
  if (!open_) {
    return Status(Status::PRECONDITION_ERROR, "not opened database");
  }
  *size = file_->GetSizeSimple();
  return Status(Status::SUCCESS);
}

Status SkipDBMImpl::GetFilePath(std::string* path) {
  std::shared_lock<SpinSharedMutex> lock(mutex_);
  if (!open_) {
    return Status(Status::PRECONDITION_ERROR, "not opened database");
  }
  *path = path_;
  return Status(Status::SUCCESS);
}

Status SkipDBMImpl::GetTimestamp(double* timestamp) {
  std::shared_lock<SpinSharedMutex> lock(mutex_);
  if (!open_) {
    return Status(Status::PRECONDITION_ERROR, "not opened database");
  }
  *timestamp = timestamp_ / 1000000.0;
  return Status(Status::SUCCESS);
}

Status SkipDBMImpl::Clear() {
  std::lock_guard<SpinSharedMutex> lock(mutex_);
  if (!open_) {
    return Status(Status::PRECONDITION_ERROR, "not opened database");
  }
  if (!writable_) {
    return Status(Status::PRECONDITION_ERROR, "not writable database");
  }
  if (update_logger_ != nullptr) {
    const Status status = update_logger_->WriteClear();
    if (status != Status::SUCCESS) {
      return status;
    }
  }
  if (updated_) {
    const Status status = FinishStorage(nullptr);
    if (status != Status::SUCCESS) {
      return status;
    }
  }
  if (updated_) {
    const Status status = DiscardStorage();
    if (status != Status::SUCCESS) {
      return status;
    }
  }
  CancelIterators();
  Status status = file_->Truncate(METADATA_SIZE);
  num_records_ = 0;
  eff_data_size_ = 0;
  file_size_ = file_->GetSizeSimple();
  timestamp_ = GetWallTime() * 1000000;
  status |= SaveMetadata(file_.get(), false);
  status |= LoadMetadata();
  status |= PrepareStorage();
  cache_ = std::make_unique<SkipRecordCache>(step_unit_, max_cached_records_, num_records_);
  return status;
}

Status SkipDBMImpl::Rebuild(
    const SkipDBM::TuningParameters& tuning_params, bool skip_broken_records, bool sync_hard) {
  std::lock_guard<SpinSharedMutex> lock(mutex_);
  if (!open_) {
    return Status(Status::PRECONDITION_ERROR, "not opened database");
  }
  if (!writable_) {
    return Status(Status::PRECONDITION_ERROR, "not writable database");
  }
  if (updated_) {
    const Status status = FinishStorage(nullptr);
    if (status != Status::SUCCESS) {
      return status;
    }
  }
  int32_t step_unit = tuning_params.step_unit;
  if (step_unit < static_cast<int32_t>(MIN_STEP_UNIT) ||
      step_unit > static_cast<int32_t>(MAX_STEP_UNIT)) {
    step_unit = SkipDBM::DEFAULT_STEP_UNIT;
  }
  int32_t max_level = tuning_params.max_level;
  if (max_level < static_cast<int32_t>(MIN_MAX_LEVEL) ||
      max_level > static_cast<int32_t>(MAX_MAX_LEVEL)) {
    max_level = std::ceil(std::log(num_records_ + 1) / std::log(step_unit));
    max_level = std::min(std::max(max_level, static_cast<int32_t>(MIN_MAX_LEVEL)),
                         static_cast<int32_t>(MAX_MAX_LEVEL));
  }
  int64_t num_links = 0;
  for (int32_t level = 1; level <= max_level; ++level) {
    num_links += num_records_ / std::pow(step_unit, level) + 1;
  }
  int32_t offset_width = tuning_params.offset_width;
  if (offset_width < 1) {
    for (offset_width = MIN_OFFSET_WIDTH;
         offset_width < static_cast<int32_t>(MAX_OFFSET_WIDTH); offset_width++) {
      const int64_t max_file_size = (1LL << (8 * offset_width)) * 0.8;
      const int64_t expected_file_size = METADATA_SIZE + num_records_ * sizeof(uint8_t) * 6 +
          num_links * offset_width + eff_data_size_;
      if (expected_file_size < max_file_size) {
        break;
      }
    }
  }
  SkipDBM::TuningParameters tmp_tuning_params;
  tmp_tuning_params.offset_width = offset_width;
  tmp_tuning_params.step_unit = step_unit;
  tmp_tuning_params.max_level = max_level;
  tmp_tuning_params.insert_in_order = true;
  const std::string rebuild_path = path_ + REBUILD_FILE_SUFFIX;
  SkipDBM tmp_dbm(file_->MakeFile());
  auto CleanUp = [&]() {
    tmp_dbm.Close();
    RemoveFile(rebuild_path);
  };
  Status status =
      tmp_dbm.OpenAdvanced(rebuild_path, true, File::OPEN_TRUNCATE, tmp_tuning_params);
  if (status != Status::SUCCESS) {
    CleanUp();
    return status;
  }
  const int64_t end_offset = file_->GetSizeSimple();
  int64_t offset = METADATA_SIZE;
  int64_t index = 0;
  tkrzw::SkipRecord rec(file_.get(), offset_width_, step_unit_, max_level_);
  while (offset < end_offset) {
    status = rec.ReadMetadataKey(offset, index);
    if (status != Status::SUCCESS) {
      CleanUp();
      return status;
    }
    const std::string_view key = rec.GetKey();
    std::string_view value = rec.GetValue();
    if (value.data() == nullptr) {
      status = rec.ReadBody();
      if (status != Status::SUCCESS) {
        CleanUp();
        return status;
      }
      value = rec.GetValue();
    }
    status = tmp_dbm.Set(key, value);
    if (status != Status::SUCCESS) {
      CleanUp();
      return status;
    }
    offset += rec.GetWholeSize();
    index++;
  }
  status = tmp_dbm.Close();
  if (status != Status::SUCCESS) {
    CleanUp();
    return status;
  }
  CancelIterators();
  auto rebuild_file = file_->MakeFile();
  file_->CopyProperties(rebuild_file.get());
  status = rebuild_file->Open(rebuild_path, true);
  if (status != Status::SUCCESS) {
    CleanUp();
    return status;
  }
  if (sync_hard) {
    status |= rebuild_file->Synchronize(true);
  }
  status |= file_->DisablePathOperations();
  if (IS_POSIX) {
    status |= rebuild_file->Rename(path_);
    status |= file_->Close();
  } else {
    status |= file_->Close();
    status |= rebuild_file->Rename(path_);
  }
  if (sync_hard) {
    std::string real_path;
    status |= GetRealPath(path_, &real_path);
    if (status == Status::SUCCESS) {
      const std::string dir_path = PathToDirectoryName(real_path);
      if (!dir_path.empty()) {
        status |= SynchronizeFile(dir_path);
      }
    }
  }
  file_ = std::move(rebuild_file);
  const uint32_t db_type = db_type_;
  const std::string opaque = opaque_;
  LoadMetadata();
  db_type_ = db_type;
  opaque_ = opaque;
  file_size_ = file_->GetSizeSimple();
  status |= SaveMetadata(file_.get(), false);
  status |= PrepareStorage();
  cache_ = std::make_unique<SkipRecordCache>(step_unit_, max_cached_records_, num_records_);
  return status;
}

Status SkipDBMImpl::ShouldBeRebuilt(bool* tobe) {
  std::shared_lock<SpinSharedMutex> lock(mutex_);
  if (!open_) {
    return Status(Status::PRECONDITION_ERROR, "not opened database");
  }
  *tobe = false;
  if (num_records_ > std::pow(step_unit_, max_level_ + 1)) {
    *tobe = true;
  }
  if (offset_width_ > MIN_OFFSET_WIDTH &&
      file_->GetSizeSimple() * 2 < (1LL << (8 * (offset_width_ - 1)))) {
    *tobe = true;
  }
  return Status(Status::SUCCESS);
}

Status SkipDBMImpl::Synchronize(
    bool hard, DBM::FileProcessor* proc, SkipDBM::ReducerType reducer) {
  std::lock_guard<SpinSharedMutex> lock(mutex_);
  if (!open_) {
    return Status(Status::PRECONDITION_ERROR, "not opened database");
  }
  if (!writable_) {
    return Status(Status::PRECONDITION_ERROR, "not writable database");
  }
  if (!healthy_) {
    return Status(Status::PRECONDITION_ERROR, "not healthy database");
  }
  CancelIterators();
  Status status(Status::SUCCESS);
  if (update_logger_ != nullptr) {
    status |= update_logger_->Synchronize(hard);
  }
  status |= FinishStorage(reducer);
  status |= file_->Synchronize(hard);
  if (proc != nullptr) {
    proc->Process(path_);
  }
  status |= SaveMetadata(file_.get(), false);
  status |= PrepareStorage();
  cache_ = std::make_unique<SkipRecordCache>(step_unit_, max_cached_records_, num_records_);
  return status;
}

std::vector<std::pair<std::string, std::string>> SkipDBMImpl::Inspect() {
  std::shared_lock<SpinSharedMutex> lock(mutex_);
  std::vector<std::pair<std::string, std::string>> meta;
  auto Add = [&](const std::string& name, const std::string& value) {
    meta.emplace_back(std::make_pair(name, value));
  };
  Add("class", "SkipDBM");
  if (open_) {
    Add("healthy", ToString(healthy_));
    Add("auto_restored", ToString(auto_restored_));
    Add("updated", ToString(updated_));
    Add("removed", ToString(removed_));
    Add("path", path_);
    Add("cyclic_magic", ToString(static_cast<uint8_t>(cyclic_magic_)));
    Add("pkg_major_version", ToString(pkg_major_version_));
    Add("pkg_minor_version", ToString(pkg_minor_version_));
    Add("offset_width", ToString(offset_width_));
    Add("step_unit", ToString(step_unit_));
    Add("max_level", ToString(max_level_));
    Add("closure_flags", ToString(closure_flags_));
    Add("num_records", ToString(num_records_));
    Add("eff_data_size", ToString(eff_data_size_));
    Add("file_size", ToString(file_->GetSizeSimple()));
    Add("timestamp", SPrintF("%.6f", timestamp_ / 1000000.0));
    Add("db_type", ToString(db_type_));
    Add("sort_mem_size", ToString(sort_mem_size_));
    Add("max_file_size", ToString(1LL << (offset_width_ * 8)));
    Add("insert_in_order", ToString(insert_in_order_));
    Add("record_base", ToString(METADATA_SIZE));
  }
  return meta;
}

bool SkipDBMImpl::IsOpen() {
  std::shared_lock<SpinSharedMutex> lock(mutex_);
  return open_;
}

bool SkipDBMImpl::IsWritable() {
  std::shared_lock<SpinSharedMutex> lock(mutex_);
  return open_ && writable_;
}

bool SkipDBMImpl::IsHealthy() {
  std::shared_lock<SpinSharedMutex> lock(mutex_);
  return open_ && healthy_;
}

bool SkipDBMImpl::IsAutoRestored() {
  std::shared_lock<SpinSharedMutex> lock(mutex_);
  return open_ && auto_restored_;
}

std::unique_ptr<DBM> SkipDBMImpl::MakeDBM() {
  std::shared_lock<SpinSharedMutex> lock(mutex_);
  return std::make_unique<SkipDBM>(file_->MakeFile());
}

DBM::UpdateLogger* SkipDBMImpl::GetUpdateLogger() {
  std::shared_lock<SpinSharedMutex> lock(mutex_);
  return update_logger_;
}

void SkipDBMImpl::SetUpdateLogger(DBM::UpdateLogger* update_logger) {
  std::lock_guard<SpinSharedMutex> lock(mutex_);
  update_logger_ = update_logger;
}

File* SkipDBMImpl::GetInternalFile() {
  return file_.get();
}

int64_t SkipDBMImpl::GetEffectiveDataSize() {
  std::shared_lock<SpinSharedMutex> lock(mutex_);
  if (!open_) {
    return -1;
  }
  return eff_data_size_;
}

int32_t SkipDBMImpl::GetDatabaseType() {
  std::shared_lock<SpinSharedMutex> lock(mutex_);
  if (!open_) {
    return -1;
  }
  return db_type_;
}

Status SkipDBMImpl::SetDatabaseTypeMetadata(uint32_t db_type) {
  std::lock_guard<SpinSharedMutex> lock(mutex_);
  if (!open_) {
    return Status(Status::PRECONDITION_ERROR, "not opened database");
  }
  if (!writable_) {
    return Status(Status::PRECONDITION_ERROR, "not writable database");
  }
  db_type_ = db_type;
  return Status(Status::SUCCESS);
}

std::string SkipDBMImpl::GetOpaqueMetadata() {
  std::shared_lock<SpinSharedMutex> lock(mutex_);
  if (!open_) {
    return "";
  }
  return opaque_;
}

Status SkipDBMImpl::SetOpaqueMetadata(const std::string& opaque) {
  std::lock_guard<SpinSharedMutex> lock(mutex_);
  if (!open_) {
    return Status(Status::PRECONDITION_ERROR, "not opened database");
  }
  if (!writable_) {
    return Status(Status::PRECONDITION_ERROR, "not writable database");
  }
  opaque_ = opaque;
  return Status(Status::SUCCESS);
}

Status SkipDBMImpl::Revert() {
  std::lock_guard<SpinSharedMutex> lock(mutex_);
  if (!open_) {
    return Status(Status::PRECONDITION_ERROR, "not opened database");
  }
  if (!writable_) {
    return Status(Status::PRECONDITION_ERROR, "not writable database");
  }
  if (!healthy_) {
    return Status(Status::PRECONDITION_ERROR, "not healthy database");
  }
  Status status = DiscardStorage();
  status |= PrepareStorage();
  return status;
}

bool SkipDBMImpl::IsUpdated() {
  std::shared_lock<SpinSharedMutex> lock(mutex_);
  return open_ && updated_;
}

Status SkipDBMImpl::MergeSkipDatabase(const std::string& src_path) {
  std::shared_lock<SpinSharedMutex> lock(mutex_);
  if (!open_) {
    return Status(Status::PRECONDITION_ERROR, "not opened database");
  }
  if (!writable_) {
    return Status(Status::PRECONDITION_ERROR, "not writable database");
  }
  if (!healthy_) {
    return Status(Status::PRECONDITION_ERROR, "not healthy database");
  }
  uint32_t src_offset_width = 0;
  uint32_t src_step_unit = 0;
  uint32_t src_max_level = 0;
  {
    SkipDBMImpl src_impl(file_->MakeFile());
    Status status =
        src_impl.Open(src_path, false, File::OPEN_DEFAULT, SkipDBM::TuningParameters());
    if (status != Status::SUCCESS) {
      return status;
    }
    src_offset_width = src_impl.offset_width_;
    src_step_unit = src_impl.step_unit_;
    src_max_level = src_impl.max_level_;
    status = src_impl.Close();
    if (status != Status::SUCCESS) {
      return status;
    }
  }
  auto src_file = file_->MakeFile();
  Status status = src_file->Open(src_path, false, File::OPEN_NO_LOCK);
  if (status != Status::SUCCESS) {
    return status;
  }
  record_sorter_->AddSkipRecord(new SkipRecord(
      src_file.get(), src_offset_width, src_step_unit, src_max_level), METADATA_SIZE);
  record_sorter_->TakeFileOwnership(std::move(src_file));
  updated_ = true;
  return Status(Status::SUCCESS);
}

Status SkipDBMImpl::ValidateRecords() {
  std::lock_guard<SpinSharedMutex> lock(mutex_);
  if (!open_) {
    return Status(Status::PRECONDITION_ERROR, "not opened database");
  }
  const int64_t end_offset = file_->GetSizeSimple();
  int64_t offset = METADATA_SIZE;
  int64_t index = 0;
  tkrzw::SkipRecord rec(file_.get(), offset_width_, step_unit_, max_level_);
  std::string last_key;
  while (offset < end_offset) {
    Status status = rec.ReadMetadataKey(offset, index);
    if (status != Status::SUCCESS) {
      return status;
    }
    const std::string_view key = rec.GetKey();
    std::string_view value = rec.GetValue();
    if (value.data() == nullptr) {
      status = rec.ReadBody();
      if (status != Status::SUCCESS) {
        return status;
      }
      value = rec.GetValue();
    }
    offset += rec.GetWholeSize();
    index++;
    if (offset == end_offset || last_key != key) {
      std::string tmp_key(key);
      status = rec.Search(METADATA_SIZE, cache_.get(), tmp_key, false);
      if (status != Status::SUCCESS) {
        return status;
      }
    }
    last_key = key;
  }
  return Status(Status::SUCCESS);
}

void SkipDBMImpl::CancelIterators() {
  for (auto* iterator : iterators_) {
    iterator->ClearPosition();
  }
}

Status SkipDBMImpl::SaveMetadata(File* file, bool finish) {
  cyclic_magic_++;
  char meta[METADATA_SIZE];
  std::memset(meta, 0, METADATA_SIZE);
  std::memcpy(meta, META_MAGIC_DATA, sizeof(META_MAGIC_DATA) - 1);
  WriteFixNum(meta + META_OFFSET_CYCLIC_MAGIC_FRONT, cyclic_magic_, 1);
  WriteFixNum(meta + META_OFFSET_PKG_MAJOR_VERSION, pkg_major_version_, 1);
  WriteFixNum(meta + META_OFFSET_PKG_MINOR_VERSION, pkg_minor_version_, 1);
  WriteFixNum(meta + META_OFFSET_OFFSET_WIDTH, offset_width_, 1);
  WriteFixNum(meta + META_OFFSET_STEP_UNIT, step_unit_, 1);
  WriteFixNum(meta + META_OFFSET_MAX_LEVEL, max_level_, 1);
  uint8_t closure_flags = CLOSURE_FLAG_NONE;
  if ((closure_flags_ & CLOSURE_FLAG_CLOSE) && finish) {
    closure_flags |= CLOSURE_FLAG_CLOSE;
  }
  WriteFixNum(meta + META_OFFSET_CLOSURE_FLAGS, closure_flags, 1);
  WriteFixNum(meta + META_OFFSET_NUM_RECORDS, num_records_, 8);
  WriteFixNum(meta + META_OFFSET_EFF_DATA_SIZE, eff_data_size_, 8);
  WriteFixNum(meta + META_OFFSET_FILE_SIZE, file_size_, 8);
  WriteFixNum(meta + META_OFFSET_TIMESTAMP, timestamp_, 8);
  WriteFixNum(meta + META_OFFSET_DB_TYPE, db_type_, 4);
  const int32_t opaque_size = std::min<int32_t>(opaque_.size(), SkipDBM::OPAQUE_METADATA_SIZE);
  std::memcpy(meta + META_OFFSET_OPAQUE, opaque_.data(), opaque_size);
  WriteFixNum(meta + META_OFFSET_CYCLIC_MAGIC_BACK, cyclic_magic_, 1);
  return file->Write(0, meta, METADATA_SIZE);
}

Status SkipDBMImpl::LoadMetadata() {
  const Status status = SkipDBM::ReadMetadata(
      file_.get(), &cyclic_magic_, &pkg_major_version_, &pkg_minor_version_,
      &offset_width_, &step_unit_, &max_level_,
      &closure_flags_, &num_records_,
      &eff_data_size_, &file_size_, &timestamp_,
      &db_type_, &opaque_);
  if (status != Status::SUCCESS) {
    return status;
  }
  if (pkg_major_version_ < 1 && pkg_minor_version_ < 1) {
    return Status(Status::BROKEN_DATA_ERROR, "invalid package version");
  }
  if (offset_width_ < MIN_OFFSET_WIDTH || offset_width_ > MAX_OFFSET_WIDTH) {
    return Status(Status::BROKEN_DATA_ERROR, "the offset width is invalid");
  }
  if (step_unit_ < MIN_STEP_UNIT || step_unit_ > MAX_STEP_UNIT) {
    return Status(Status::BROKEN_DATA_ERROR, "the step unit is invalid");
  }
  if (max_level_ < MIN_MAX_LEVEL || max_level_ > MAX_MAX_LEVEL) {
    return Status(Status::BROKEN_DATA_ERROR, "the max level is invalid");
  }
  if (num_records_ < 0) {
    return Status(Status::BROKEN_DATA_ERROR, "invalid record count");
  }
  if (eff_data_size_ < 0) {
    return Status(Status::BROKEN_DATA_ERROR, "invalid effective data size");
  }
  if (file_size_ < static_cast<int64_t>(METADATA_SIZE)) {
    return Status(Status::BROKEN_DATA_ERROR, "invalid file size");
  }
  return Status(Status::SUCCESS);
}

Status SkipDBMImpl::CheckFileBeforeOpen(File* file, const std::string& path, bool writable) {
  auto* pos_file = dynamic_cast<PositionalFile*>(file_.get());
  if (pos_file != nullptr && pos_file->IsDirectIO()) {
    const int64_t file_size = tkrzw::GetFileSize(path);
    const int64_t block_size = pos_file->GetBlockSize();
    if (block_size % MIN_DIO_BLOCK_SIZE != 0) {
      return Status(Status::INFEASIBLE_ERROR, "Invalid block size for Direct I/O");
    }
    if (!writable && file_size > 0 && file_size % block_size != 0) {
      return Status(Status::INFEASIBLE_ERROR, "The file size not aligned to the block size");
    }
  }
  return Status(Status::SUCCESS);;
}

Status SkipDBMImpl::CheckZeroRegion(int64_t offset, int64_t end_offset) {
  const int64_t max_check_size = 128 * 1024;
  char buf[8192];
  end_offset = std::min<int64_t>(offset + max_check_size, end_offset);
  while (offset < end_offset) {
    const int32_t read_size = std::min<int32_t>(end_offset - offset, sizeof(buf));
    const Status status = file_->Read(offset, buf, read_size);
    if (status != Status::SUCCESS) {
      return status;
    }
    for (int32_t i = 0; i < read_size; i++) {
      if (buf[i] != 0) {
        return Status(Status::BROKEN_DATA_ERROR, "non-zero region");
      }
    }
    offset += read_size;
  }
  return Status(Status::SUCCESS);
}

Status SkipDBMImpl::PadFileForDirectIO() {
  auto* pos_file = dynamic_cast<PositionalFile*>(file_.get());
  if (pos_file == nullptr || !pos_file->IsDirectIO()) {
    return Status(Status::SUCCESS);
  }
  const int64_t file_size = file_->GetSizeSimple();
  const int64_t block_size = pos_file->GetBlockSize();
  const int64_t size_rem = file_size % block_size;
  if (size_rem == 0) {
    return Status(Status::SUCCESS);
  }
  return Status(Status::SUCCESS);
}

Status SkipDBMImpl::PrepareStorage() {
  const std::string sorter_path = path_ + SORTER_FILE_SUFFIX;
  const bool use_mmap = file_->IsMemoryMapping();
  record_sorter_ = std::make_unique<RecordSorter>(sorter_path, sort_mem_size_, use_mmap);
  if (insert_in_order_) {
    const std::string sorted_path = path_ + SORTED_FILE_SUFFIX;
    sorted_file_ = file_->MakeFile();
    Status status = sorted_file_->Open(sorted_path, true, File::OPEN_TRUNCATE);
    if (status != Status::SUCCESS) {
      return status;
    }
    status = sorted_file_->Truncate(METADATA_SIZE);
    if (status != Status::SUCCESS) {
      return status;
    }
    record_index_ = 0;
    past_offsets_.clear();
    past_offsets_.resize(max_level_, 0);
  }
  old_num_records_ = num_records_;
  old_eff_data_size_ = eff_data_size_;
  return Status(Status::SUCCESS);
}

Status SkipDBMImpl::FinishStorage(SkipDBM::ReducerType reducer) {
  const std::string sorted_path = path_ + SORTED_FILE_SUFFIX;
  const std::string merged_path = path_ + MERGED_FILE_SUFFIX;
  Status status(Status::SUCCESS);
  if (reducer == nullptr && sorted_file_ != nullptr &&
      file_->GetSizeSimple() == static_cast<int64_t>(METADATA_SIZE) &&
      !record_sorter_->IsUpdated()) {
    file_size_ = sorted_file_->GetSizeSimple();
    timestamp_ = GetWallTime() * 1000000;
    status |= SaveMetadata(sorted_file_.get(), true);
    file_->DisablePathOperations();
    if (IS_POSIX) {
      status |= sorted_file_->Rename(path_);
      status |= file_->Close();
    } else {
      status |= file_->Close();
      status |= sorted_file_->Rename(path_);
    }
    if (status != Status::SUCCESS) {
      return status;
    }
    file_ = std::move(sorted_file_);
  } else {
    if (file_->GetSizeSimple() > static_cast<int64_t>(METADATA_SIZE)) {
      record_sorter_->AddSkipRecord(new SkipRecord(
          file_.get(), offset_width_, step_unit_, max_level_), METADATA_SIZE);
    }
    if (sorted_file_ != nullptr &&
        sorted_file_->GetSizeSimple() > static_cast<int64_t>(METADATA_SIZE)) {
      record_sorter_->AddSkipRecord(new SkipRecord(
          sorted_file_.get(), offset_width_, step_unit_, max_level_), METADATA_SIZE);
    }
    status = record_sorter_->Finish();
    if (status != Status::SUCCESS) {
      return status;
    }
    auto merged_file = file_->MakeFile();
    merged_file->CopyProperties(file_.get());
    status = merged_file->Open(merged_path, true, File::OPEN_TRUNCATE);
    if (status != Status::SUCCESS) {
      return status;
    }
    merged_file->Truncate(METADATA_SIZE);
    num_records_ = 0;
    eff_data_size_ = 0;
    record_index_ = 0;
    past_offsets_.clear();
    past_offsets_.resize(max_level_, 0);
    std::string last_key;
    std::vector<std::string> last_values;
    while (true) {
      std::string key, value;
      tkrzw::Status status = record_sorter_->Get(&key, &value);
      if (status != tkrzw::Status::SUCCESS) {
        if (status != tkrzw::Status::NOT_FOUND_ERROR) {
          return status;
        }
        break;
      }
      if (reducer == nullptr && !removed_) {
        status = WriteRecord(key, value, merged_file.get());
        if (status != Status::SUCCESS) {
          return status;
        }
      } else {
        if (last_values.empty()) {
          last_key = std::move(key);
          last_values.emplace_back(std::move(value));
        } else if (key == last_key) {
          last_values.emplace_back(std::move(value));
        } else {
          const auto& live_values =
              removed_ ? SkipDBM::ReduceRemove(last_key, last_values) : last_values;
          if (!live_values.empty()) {
            const auto& new_values =
                reducer == nullptr ? live_values : reducer(last_key, live_values);
            for (const auto& new_value : new_values) {
              status = WriteRecord(last_key, new_value, merged_file.get());
              if (status != Status::SUCCESS) {
                return status;
              }
            }
          }
          last_key = std::move(key);
          last_values.clear();
          last_values.emplace_back(std::move(value));
        }
      }
    }
    if (!last_values.empty()) {
      const auto& live_values =
          removed_ ? SkipDBM::ReduceRemove(last_key, last_values) : last_values;
      if (!live_values.empty()) {
        const auto& new_values =
            reducer == nullptr ? live_values : reducer(last_key, live_values);
        for (const auto& new_value : new_values) {
          status = WriteRecord(last_key, new_value, merged_file.get());
          if (status != Status::SUCCESS) {
            return status;
          }
        }
      }
    }
    file_size_ = merged_file->GetSizeSimple();
    timestamp_ = GetWallTime() * 1000000;
    status |= SaveMetadata(merged_file.get(), true);
    file_->DisablePathOperations();
    if (IS_POSIX) {
      status |= merged_file->Rename(path_);
      status |= file_->Close();
    } else {
      status |= file_->Close();
      status |= merged_file->Rename(path_);
    }
    if (status != Status::SUCCESS) {
      return status;
    }
    file_ = std::move(merged_file);
    if (sorted_file_ != nullptr) {
      status |= sorted_file_->Close();
      status |= RemoveFile(sorted_path);
      sorted_file_.reset(nullptr);
    }
  }
  past_offsets_.clear();
  record_sorter_.reset(nullptr);
  updated_ = false;
  return status;
}

Status SkipDBMImpl::DiscardStorage() {
  Status status(Status::SUCCESS);
  if (sorted_file_ != nullptr) {
    const std::string sorted_path = path_ + SORTED_FILE_SUFFIX;
    status |= sorted_file_->Close();
    status |= RemoveFile(sorted_path);
    sorted_file_.reset(nullptr);
  }
  past_offsets_.clear();
  record_sorter_.reset(nullptr);
  updated_ = false;
  num_records_ = old_num_records_;
  eff_data_size_ = old_eff_data_size_;
  return status;
}

Status SkipDBMImpl::UpdateRecord(std::string_view key, std::string_view new_value) {
  if (new_value.data() != DBM::RecordProcessor::NOOP.data()) {
    if (update_logger_ != nullptr) {
      Status status(Status::SUCCESS);
      if (new_value.data() == DBM::RecordProcessor::REMOVE.data()) {
        status = update_logger_->WriteRemove(key);
      } else {
        status = update_logger_->WriteSet(key, new_value);
      }
      if (status != Status::SUCCESS) {
        return status;
      }
    }
    if (new_value.data() == DBM::RecordProcessor::REMOVE.data()) {
      new_value = SkipDBM::REMOVING_VALUE;
      removed_ = true;
    }
    if (sorted_file_ == nullptr) {
      const Status status = record_sorter_->Add(key, new_value);
      if (status != Status::SUCCESS) {
        return status;
      }
    } else {
      const Status status = WriteRecord(key, new_value, sorted_file_.get());
      if (status != Status::SUCCESS) {
        return status;
      }
    }
    updated_ = true;
  }
  return Status(Status::SUCCESS);
}

Status SkipDBMImpl::WriteRecord(std::string_view key, std::string_view value, File* file) {
  SkipRecord rec(file, offset_width_, step_unit_, max_level_);
  rec.SetData(record_index_, key.data(), key.size(), value.data(), value.size());
  Status status = rec.Write();
  if (status != Status::SUCCESS) {
    return status;
  }
  const int64_t offset = rec.GetOffset();
  status = rec.UpdatePastRecords(record_index_, offset, &past_offsets_);
  if (status != Status::SUCCESS) {
    return status;
  }
  num_records_++;
  eff_data_size_ += key.size() + value.size();
  record_index_++;
  return Status(Status::SUCCESS);
}

Status SkipDBMImpl::ProcessImpl(std::string_view key, DBM::RecordProcessor* proc, bool writable) {
  SkipRecord rec(file_.get(), offset_width_, step_unit_, max_level_);
  Status status = rec.Search(METADATA_SIZE, cache_.get(), key, false);
  std::string_view new_value;
  if (status == Status::SUCCESS) {
    std::string_view rec_value = rec.GetValue();
    if (rec_value.data() == nullptr) {
      status = rec.ReadBody();
      if (status != Status::SUCCESS) {
        return status;
      }
      rec_value = rec.GetValue();
    }
    new_value = proc->ProcessFull(key, rec_value);
  } else if (status == Status::NOT_FOUND_ERROR) {
    new_value = proc->ProcessEmpty(key);
  } else {
    return status;
  }
  if (writable) {
    status = UpdateRecord(key, new_value);
    if (status != Status::SUCCESS) {
      return status;
    }
  }
  return Status(Status::SUCCESS);
}

SkipDBMIteratorImpl::SkipDBMIteratorImpl(SkipDBMImpl* dbm)
    : dbm_(dbm), record_offset_(-1), record_index_(-1), record_size_(0) {
  std::lock_guard<SpinSharedMutex> lock(dbm_->mutex_);
  dbm_->iterators_.emplace_back(this);
}

SkipDBMIteratorImpl::~SkipDBMIteratorImpl() {
  if (dbm_ != nullptr) {
    std::lock_guard<SpinSharedMutex> lock(dbm_->mutex_);
    dbm_->iterators_.remove(this);
  }
}

Status SkipDBMIteratorImpl::First() {
  std::shared_lock<SpinSharedMutex> lock(dbm_->mutex_);
  if (!dbm_->open_) {
    return Status(Status::PRECONDITION_ERROR, "not opened database");
  }
  record_offset_ = METADATA_SIZE;
  record_index_ = 0;
  record_size_ = 0;
  return Status(Status::SUCCESS);
}

Status SkipDBMIteratorImpl::Last() {
  std::shared_lock<SpinSharedMutex> lock(dbm_->mutex_);
  if (!dbm_->open_) {
    return Status(Status::PRECONDITION_ERROR, "not opened database");
  }
  if (dbm_->num_records_ > 0) {
    SkipRecord rec(dbm_->file_.get(), dbm_->offset_width_, dbm_->step_unit_, dbm_->max_level_);
    Status status = rec.SearchByIndex(METADATA_SIZE, dbm_->cache_.get(), dbm_->num_records_ - 1);
    if (status != Status::SUCCESS) {
      return status;
    }
    record_offset_ = rec.GetOffset();
    record_index_ = rec.GetIndex();
    record_size_ = rec.GetWholeSize();
  } else {
    record_offset_ = METADATA_SIZE;
    record_index_ = 0;
    record_size_ = 0;
  }
  return Status(Status::SUCCESS);
}

Status SkipDBMIteratorImpl::Jump(std::string_view key) {
  std::shared_lock<SpinSharedMutex> lock(dbm_->mutex_);
  if (!dbm_->open_) {
    return Status(Status::PRECONDITION_ERROR, "not opened database");
  }
  SkipRecord rec(dbm_->file_.get(), dbm_->offset_width_, dbm_->step_unit_, dbm_->max_level_);
  const Status status = rec.Search(METADATA_SIZE, dbm_->cache_.get(), key, true);
  if (status != Status::SUCCESS) {
    ClearPosition();
    if (status == Status::NOT_FOUND_ERROR) {
      return Status(Status::SUCCESS);
    }
    return status;
  }
  record_offset_ = rec.GetOffset();
  record_index_ = rec.GetIndex();
  record_size_ = rec.GetWholeSize();
  return Status(Status::SUCCESS);
}

Status SkipDBMIteratorImpl::JumpLower(std::string_view key, bool inclusive) {
  std::shared_lock<SpinSharedMutex> lock(dbm_->mutex_);
  if (!dbm_->open_) {
    return Status(Status::PRECONDITION_ERROR, "not opened database");
  }
  SkipRecord rec(dbm_->file_.get(), dbm_->offset_width_, dbm_->step_unit_, dbm_->max_level_);
  Status status = rec.Search(METADATA_SIZE, dbm_->cache_.get(), key, true);
  if (status == Status::NOT_FOUND_ERROR) {
    if (dbm_->num_records_ < 1) {
      ClearPosition();
      return Status(Status::SUCCESS);
    }
    status = rec.SearchByIndex(METADATA_SIZE, dbm_->cache_.get(), dbm_->num_records_ - 1);
    if (status != Status::SUCCESS) {
      return status;
    }
  } else if (status != Status::SUCCESS) {
    ClearPosition();
    return status;
  }
  record_offset_ = rec.GetOffset();
  record_index_ = rec.GetIndex();
  record_size_ = rec.GetWholeSize();
  while (true) {
    const std::string_view rec_key = rec.GetKey();
    const bool ok = inclusive ? rec_key <= key : rec_key < key;
    if (ok) {
      return Status(Status::SUCCESS);
    }
    if (record_index_ == 0) {
      ClearPosition();
      return Status(Status::SUCCESS);
    }
    status = rec.SearchByIndex(METADATA_SIZE, dbm_->cache_.get(), record_index_ - 1);
    if (status != Status::SUCCESS) {
      ClearPosition();
      return status;
    }
    record_offset_ = rec.GetOffset();
    record_index_ -= 1;
    record_size_ = rec.GetWholeSize();
  }
  ClearPosition();
  return Status(Status::SUCCESS);
}

Status SkipDBMIteratorImpl::JumpUpper(std::string_view key, bool inclusive) {
  std::shared_lock<SpinSharedMutex> lock(dbm_->mutex_);
  if (!dbm_->open_) {
    return Status(Status::PRECONDITION_ERROR, "not opened database");
  }
  SkipRecord rec(dbm_->file_.get(), dbm_->offset_width_, dbm_->step_unit_, dbm_->max_level_);
  Status status = rec.Search(METADATA_SIZE, dbm_->cache_.get(), key, true);
  if (status != Status::SUCCESS) {
    ClearPosition();
    if (status == Status::NOT_FOUND_ERROR) {
      return Status(Status::SUCCESS);
    }
    return status;
  }
  record_offset_ = rec.GetOffset();
  record_index_ = rec.GetIndex();
  record_size_ = rec.GetWholeSize();
  while (true) {
    const std::string_view rec_key = rec.GetKey();
    const bool ok = inclusive ? rec_key >= key : rec_key > key;
    if (ok) {
      return Status(Status::SUCCESS);
    }
    if (record_index_ == dbm_->num_records_ - 1) {
      ClearPosition();
      return Status(Status::SUCCESS);
    }
    const Status status =
        rec.ReadMetadataKey(record_offset_ + rec.GetWholeSize(), record_index_ + 1);
    if (status != Status::SUCCESS) {
      ClearPosition();
      return status;
    }
    record_offset_ = rec.GetOffset();
    record_index_ = rec.GetIndex();
    record_size_ = rec.GetWholeSize();
  }
  ClearPosition();
  return Status(Status::SUCCESS);
}

Status SkipDBMIteratorImpl::Next() {
  std::shared_lock<SpinSharedMutex> lock(dbm_->mutex_);
  if (record_offset_ < 0 || record_offset_ >= dbm_->file_->GetSizeSimple()) {
    return Status(Status::NOT_FOUND_ERROR);
  }
  if (record_size_ < 1) {
    SkipRecord rec(dbm_->file_.get(), dbm_->offset_width_, dbm_->step_unit_, dbm_->max_level_);
    const Status status = rec.ReadMetadataKey(record_offset_, record_index_);
    if (status != Status::SUCCESS) {
      ClearPosition();
      return status;
    }
    record_size_ = rec.GetWholeSize();
  }
  record_offset_ += record_size_;
  record_index_++;
  record_size_ = 0;
  return Status(Status::SUCCESS);
}

Status SkipDBMIteratorImpl::Previous() {
  std::shared_lock<SpinSharedMutex> lock(dbm_->mutex_);
  if (record_offset_ < 0 || record_offset_ >= dbm_->file_->GetSizeSimple()) {
    return Status(Status::NOT_FOUND_ERROR);
  }
  if (record_index_ > 0) {
    SkipRecord rec(dbm_->file_.get(), dbm_->offset_width_, dbm_->step_unit_, dbm_->max_level_);
    const Status status = rec.SearchByIndex(METADATA_SIZE, dbm_->cache_.get(), record_index_ - 1);
    if (status != Status::SUCCESS) {
      ClearPosition();
      return status;
    }
    record_offset_ = rec.GetOffset();
    record_index_ -= 1;
    record_size_ = rec.GetWholeSize();
  } else {
    ClearPosition();
  }
  return Status(Status::SUCCESS);
}

Status SkipDBMIteratorImpl::Process(DBM::RecordProcessor* proc, bool writable) {
  if (writable) {
    std::lock_guard<SpinSharedMutex> lock(dbm_->mutex_);
    if (record_offset_ < 0 || record_offset_ >= dbm_->file_->GetSizeSimple()) {
      return Status(Status::NOT_FOUND_ERROR);
    }
    SkipRecord rec(dbm_->file_.get(), dbm_->offset_width_, dbm_->step_unit_, dbm_->max_level_);
    Status status = rec.ReadMetadataKey(record_offset_, record_index_);
    if (status != Status::SUCCESS) {
      return status;
    }
    const std::string_view key = rec.GetKey();
    std::string_view value = rec.GetValue();
    if (value.data() == nullptr) {
      status = rec.ReadBody();
      if (status != Status::SUCCESS) {
        return status;
      }
      value = rec.GetValue();
    }
    record_size_ = rec.GetWholeSize();
    std::string_view new_value = proc->ProcessFull(key, value);
    status = dbm_->UpdateRecord(key, new_value);
    if (status != Status::SUCCESS) {
      return status;
    }
    if (new_value.data() == DBM::RecordProcessor::REMOVE.data()) {
      record_offset_ += record_size_;
      record_index_++;
      record_size_ = 0;
    }
  } else {
    std::shared_lock<SpinSharedMutex> lock(dbm_->mutex_);
    if (record_offset_ < 0 || record_offset_ >= dbm_->file_->GetSizeSimple()) {
      return Status(Status::NOT_FOUND_ERROR);
    }
    SkipRecord rec(dbm_->file_.get(), dbm_->offset_width_, dbm_->step_unit_, dbm_->max_level_);
    Status status = rec.ReadMetadataKey(record_offset_, record_index_);
    if (status != Status::SUCCESS) {
      return status;
    }
    const std::string_view key = rec.GetKey();
    std::string_view value = rec.GetValue();
    if (value.data() == nullptr) {
      status = rec.ReadBody();
      if (status != Status::SUCCESS) {
        return status;
      }
      value = rec.GetValue();
    }
    record_size_ = rec.GetWholeSize();
    proc->ProcessFull(key, value);
  }
  return Status(Status::SUCCESS);
}

Status SkipDBMIteratorImpl::Get(std::string* key, std::string* value) {
  std::shared_lock<SpinSharedMutex> lock(dbm_->mutex_);
  if (record_offset_ < 0 || record_offset_ >= dbm_->file_->GetSizeSimple()) {
    return Status(Status::NOT_FOUND_ERROR);
  }
  SkipRecord rec(dbm_->file_.get(), dbm_->offset_width_, dbm_->step_unit_, dbm_->max_level_);
  Status status = rec.ReadMetadataKey(record_offset_, record_index_);
  if (status != Status::SUCCESS) {
    return status;
  }
  if (key != nullptr) {
    *key = rec.GetKey();
  }
  if (value != nullptr) {
    std::string_view rec_value = rec.GetValue();
    if (rec_value.data() == nullptr) {
      status = rec.ReadBody();
      if (status != Status::SUCCESS) {
        return status;
      }
      rec_value = rec.GetValue();
    }
    *value = rec_value;
  }
  record_size_ = rec.GetWholeSize();
  return Status(Status::SUCCESS);
}

void SkipDBMIteratorImpl::ClearPosition() {
  record_offset_ = -1;
  record_index_ = -1;
  record_size_ = 0;
}

const std::string SkipDBM::REMOVING_VALUE("\xDE\xAD\x00\x19\x78\x02\x11", 7);

SkipDBM::SkipDBM() {
  impl_ = new SkipDBMImpl(std::make_unique<MemoryMapParallelFile>());
}

SkipDBM::SkipDBM(std::unique_ptr<File> file) {
  impl_ = new SkipDBMImpl(std::move(file));
}

SkipDBM::~SkipDBM() {
  delete impl_;
}

Status SkipDBM::OpenAdvanced(const std::string& path, bool writable,
                             int32_t options, const TuningParameters& tuning_params) {
  return impl_->Open(path, writable, options, tuning_params);
}

Status SkipDBM::Close() {
  return impl_->Close();
}

Status SkipDBM::Process(std::string_view key, RecordProcessor* proc, bool writable) {
  assert(proc != nullptr);
  return impl_->Process(key, proc, writable);
}

Status SkipDBM::Set(std::string_view key, std::string_view value, bool overwrite,
                    std::string* old_value) {
  if (overwrite && old_value == nullptr) {
    return impl_->Insert(key, value);
  }
  return DBM::Set(key, value, false, old_value);
}

Status SkipDBM::Remove(std::string_view key, std::string* old_value) {
  if (old_value == nullptr) {
    return impl_->Insert(key, RecordProcessor::REMOVE);
  }
  return DBM::Remove(key, old_value);
}

Status SkipDBM::GetByIndex(int64_t index, std::string* key, std::string* value) {
  assert(index >= 0);
  return impl_->GetByIndex(index, key, value);
}

Status SkipDBM::ProcessMulti(
    const std::vector<std::pair<std::string_view, RecordProcessor*>>& key_proc_pairs,
    bool writable) {
  return impl_->ProcessMulti(key_proc_pairs, writable);
}

Status SkipDBM::ProcessFirst(RecordProcessor* proc, bool writable) {
  assert(proc != nullptr);
  return impl_->ProcessFirst(proc, writable);
}

Status SkipDBM::ProcessEach(RecordProcessor* proc, bool writable) {
  assert(proc != nullptr);
  return impl_->ProcessEach(proc, writable);
}

Status SkipDBM::Count(int64_t* count) {
  assert(count != nullptr);
  return impl_->Count(count);
}

Status SkipDBM::GetFileSize(int64_t* size) {
  assert(size != nullptr);
  return impl_->GetFileSize(size);
}

Status SkipDBM::GetFilePath(std::string* path) {
  assert(path != nullptr);
  return impl_->GetFilePath(path);
}

Status SkipDBM::GetTimestamp(double* timestamp) {
  assert(timestamp != nullptr);
  return impl_->GetTimestamp(timestamp);
}

Status SkipDBM::Clear() {
  return impl_->Clear();
}

Status SkipDBM::RebuildAdvanced(
    const TuningParameters& tuning_params, bool skip_broken_records, bool sync_hard) {
  return impl_->Rebuild(tuning_params, skip_broken_records, sync_hard);
}

Status SkipDBM::ShouldBeRebuilt(bool* tobe) {
  assert(tobe != nullptr);
  return impl_->ShouldBeRebuilt(tobe);
}

Status SkipDBM::SynchronizeAdvanced(
    bool hard, FileProcessor* proc, SkipDBM::ReducerType reducer) {
  return impl_->Synchronize(hard, proc, reducer);
}

std::vector<std::pair<std::string, std::string>> SkipDBM::Inspect() {
  return impl_->Inspect();
}

bool SkipDBM::IsOpen() const {
  return impl_->IsOpen();
}

bool SkipDBM::IsWritable() const {
  return impl_->IsWritable();
}

bool SkipDBM::IsHealthy() const {
  return impl_->IsHealthy();
}

bool SkipDBM::IsAutoRestored() const {
  return impl_->IsAutoRestored();
}

std::unique_ptr<DBM::Iterator> SkipDBM::MakeIterator() {
  std::unique_ptr<SkipDBM::Iterator> iter(new SkipDBM::Iterator(impl_));
  return iter;
}

std::unique_ptr<DBM> SkipDBM::MakeDBM() const {
  return impl_->MakeDBM();
}

DBM::UpdateLogger* SkipDBM::GetUpdateLogger() const {
  return impl_->GetUpdateLogger();
}

void SkipDBM::SetUpdateLogger(UpdateLogger* update_logger) {
  impl_->SetUpdateLogger(update_logger);
}

File* SkipDBM::GetInternalFile() const {
  return impl_->GetInternalFile();
}

int64_t SkipDBM::GetEffectiveDataSize() {
  return impl_->GetEffectiveDataSize();
}

int32_t SkipDBM::GetDatabaseType() {
  return impl_->GetDatabaseType();
}

Status SkipDBM::SetDatabaseType(uint32_t db_type) {
  return impl_->SetDatabaseTypeMetadata(db_type);
}

std::string SkipDBM::GetOpaqueMetadata() {
  return impl_->GetOpaqueMetadata();
}

Status SkipDBM::SetOpaqueMetadata(const std::string& opaque) {
  return impl_->SetOpaqueMetadata(opaque);
}

Status SkipDBM::Revert() {
  return impl_->Revert();
}

bool SkipDBM::IsUpdated() {
  return impl_->IsUpdated();
}

Status SkipDBM::MergeSkipDatabase(const std::string& src_path) {
  return impl_->MergeSkipDatabase(src_path);
}

Status SkipDBM::ValidateRecords() {
  return impl_->ValidateRecords();
}

SkipDBM::Iterator::Iterator(SkipDBMImpl* dbm_impl) {
  impl_ = new SkipDBMIteratorImpl(dbm_impl);
}

SkipDBM::Iterator::~Iterator() {
  delete impl_;
}

Status SkipDBM::Iterator::First() {
  return impl_->First();
}

Status SkipDBM::Iterator::Last() {
  return impl_->Last();
}

Status SkipDBM::Iterator::Jump(std::string_view key) {
  return impl_->Jump(key);
}

Status SkipDBM::Iterator::JumpLower(std::string_view key, bool inclusive) {
  return impl_->JumpLower(key, inclusive);
}

Status SkipDBM::Iterator::JumpUpper(std::string_view key, bool inclusive) {
  return impl_->JumpUpper(key, inclusive);
}

Status SkipDBM::Iterator::Next() {
  return impl_->Next();
}

Status SkipDBM::Iterator::Previous() {
  return impl_->Previous();
}

Status SkipDBM::Iterator::Process(RecordProcessor* proc, bool writable) {
  assert(proc != nullptr);
  return impl_->Process(proc, writable);
}

Status SkipDBM::Iterator::Get(std::string* key, std::string* value) {
  return impl_->Get(key, value);
}

std::vector<std::string> SkipDBM::ReduceRemove(
    const std::string& key, const std::vector<std::string>& values) {
  std::vector<std::string> result;
  result.reserve(values.size());
  for (const auto& value : values) {
    if (value == REMOVING_VALUE) {
      result.clear();
    } else {
      result.emplace_back(value);
    }
  }
  return result;
}

std::vector<std::string> SkipDBM::ReduceToFirst(
    const std::string& key, const std::vector<std::string>& values) {
  const std::vector<std::string> result = {values.front()};
  return result;
}

std::vector<std::string> SkipDBM::ReduceToSecond(
    const std::string& key, const std::vector<std::string>& values) {
  const std::vector<std::string> result = {values.size() > 1 ? values[1] : values[0]};
  return result;
}

std::vector<std::string> SkipDBM::ReduceToLast(
    const std::string& key, const std::vector<std::string>& values) {
  const std::vector<std::string> result = {values.back()};
  return result;
}

std::vector<std::string> SkipDBM::ReduceConcat(
    const std::string& key, const std::vector<std::string>& values) {
  const std::vector<std::string> result = {StrJoin(values, "")};
  return result;
}

std::vector<std::string> SkipDBM::ReduceConcatWithNull(
    const std::string& key, const std::vector<std::string>& values) {
  const std::vector<std::string> result = {StrJoin(values, std::string("\0", 1))};
  return result;
}

std::vector<std::string> SkipDBM::ReduceConcatWithTab(
    const std::string& key, const std::vector<std::string>& values) {
  const std::vector<std::string> result = {StrJoin(values, "\t")};
  return result;
}

std::vector<std::string> SkipDBM::ReduceConcatWithLine(
    const std::string& key, const std::vector<std::string>& values) {
  const std::vector<std::string> result = {StrJoin(values, "\n")};
  return result;
}

std::vector<std::string> SkipDBM::ReduceToTotal(
    const std::string& key, const std::vector<std::string>& values) {
  int64_t total = 0;
  for (const auto& value : values) {
    total += StrToInt(value);
  }
  const std::vector<std::string> result = {ToString(total)};
  return result;
}

std::vector<std::string> SkipDBM::ReduceToTotalBigEndian(
    const std::string& key, const std::vector<std::string>& values) {
  uint64_t total = 0;
  for (const auto& value : values) {
    total += StrToIntBigEndian(value);
  }
  const std::vector<std::string> result = {IntToStrBigEndian(total)};
  return result;
}

Status SkipDBM::ReadMetadata(
    File* file, int32_t* cyclic_magic, int32_t* pkg_major_version, int32_t* pkg_minor_version,
    int32_t* offset_width, int32_t* step_unit, int32_t *max_level,
    int32_t* closure_flags, int64_t* num_records,
    int64_t* eff_data_size, int64_t* file_size, int64_t* timestamp,
    int32_t* db_type, std::string* opaque) {
  int64_t act_file_size = 0;
  Status status = file->GetSize(&act_file_size);
  if (status != Status::SUCCESS) {
    return status;
  }
  if (act_file_size < METADATA_SIZE) {
    return Status(Status::BROKEN_DATA_ERROR, "too small metadata");
  }
  char meta[METADATA_SIZE];
  status = file->Read(0, meta, METADATA_SIZE);
  if (status != Status::SUCCESS) {
    return status;
  }
  if (std::memcmp(meta, META_MAGIC_DATA, sizeof(META_MAGIC_DATA) - 1) != 0) {
    return Status(Status::BROKEN_DATA_ERROR, "bad magic data");
  }
  *cyclic_magic = ReadFixNum(meta + META_OFFSET_CYCLIC_MAGIC_FRONT, 1);
  *pkg_major_version = ReadFixNum(meta + META_OFFSET_PKG_MAJOR_VERSION, 1);
  *pkg_minor_version = ReadFixNum(meta + META_OFFSET_PKG_MINOR_VERSION, 1);
  *offset_width = ReadFixNum(meta + META_OFFSET_OFFSET_WIDTH, 1);
  *step_unit = ReadFixNum(meta + META_OFFSET_STEP_UNIT, 1);
  *max_level = ReadFixNum(meta + META_OFFSET_MAX_LEVEL, 1);
  *closure_flags = ReadFixNum(meta + META_OFFSET_CLOSURE_FLAGS, 1);
  *num_records = ReadFixNum(meta + META_OFFSET_NUM_RECORDS, 8);
  *eff_data_size = ReadFixNum(meta + META_OFFSET_EFF_DATA_SIZE, 8);
  *file_size = ReadFixNum(meta + META_OFFSET_FILE_SIZE, 8);
  *timestamp = ReadFixNum(meta + META_OFFSET_TIMESTAMP, 8);
  *db_type = ReadFixNum(meta + META_OFFSET_DB_TYPE, 4);
  *opaque = std::string(meta + META_OFFSET_OPAQUE, SkipDBM::OPAQUE_METADATA_SIZE);
  const int32_t cyclic_magic_back = ReadFixNum(meta + META_OFFSET_CYCLIC_MAGIC_BACK, 1);
  if (*cyclic_magic != cyclic_magic_back) {
    *cyclic_magic = -1;
  }
  return Status(Status::SUCCESS);
}

Status SkipDBM::RestoreDatabase(
    const std::string& old_file_path, const std::string& new_file_path) {
  PositionalParallelFile old_file;
  Status status = old_file.Open(old_file_path, false);
  if (status != Status::SUCCESS) {
    return status;
  }
  int64_t act_old_file_size = 0;
  status = old_file.GetSize(&act_old_file_size);
  if (status != Status::SUCCESS) {
    return status;
  }
  if (act_old_file_size < METADATA_SIZE) {
    return Status(Status::BROKEN_DATA_ERROR, "too small file");
  }
  int32_t offset_width = -1;
  int32_t step_unit = -1;
  int32_t max_level = -1;
  int32_t old_cyclic_magic = 0;
  int32_t old_pkg_major_version = 0;
  int32_t old_pkg_minor_version = 0;
  int32_t old_offset_width = 0;
  int32_t old_step_unit = 0;
  int32_t old_max_level = 0;
  int32_t old_closure_flags = 0;
  int64_t old_num_records = 0;
  int64_t old_eff_data_size = 0;
  int64_t old_file_size = 0;
  int64_t old_timestamp = 0;
  int32_t old_db_type = 0;
  std::string old_opaque;
  if (ReadMetadata(
          &old_file, &old_cyclic_magic, &old_pkg_major_version, &old_pkg_minor_version,
          &old_offset_width, &old_step_unit, &old_max_level,
          &old_closure_flags, &old_num_records,
          &old_eff_data_size, &old_file_size, &old_timestamp,
          &old_db_type, &old_opaque) == Status::SUCCESS && old_cyclic_magic >= 0) {
    offset_width = old_offset_width;
    step_unit = old_step_unit;
    max_level = old_max_level;
  }
  std::unique_ptr<File> new_file;
  if (act_old_file_size <= UINT32MAX) {
    new_file = std::make_unique<MemoryMapParallelFile>();
  } else {
    new_file = std::make_unique<PositionalParallelFile>();
  }
  SkipDBM new_dbm(std::move(new_file));
  SkipDBM::TuningParameters tuning_params;
  tuning_params.insert_in_order = true;
  tuning_params.offset_width = offset_width;
  tuning_params.step_unit = step_unit;
  tuning_params.max_level = max_level;
  status = new_dbm.OpenAdvanced(new_file_path, true, File::OPEN_DEFAULT, tuning_params);
  if (status != Status::SUCCESS) {
    return status;
  }
  if (new_dbm.CountSimple() > 0) {
    return Status(Status::PRECONDITION_ERROR, "the new database is not empty");
  }
  new_dbm.SetDatabaseType(old_db_type);
  new_dbm.SetOpaqueMetadata(old_opaque);
  tkrzw::SkipRecord rec(&old_file, old_offset_width, old_step_unit, old_max_level);
  const int64_t end_offset = old_file.GetSizeSimple();
  int64_t offset = METADATA_SIZE;
  int64_t index = 0;
  while (offset < end_offset) {
    status = rec.ReadMetadataKey(offset, index);
    if (status != Status::SUCCESS) {
      break;
    }
    std::string_view value = rec.GetValue();
    if (value.data() == nullptr) {
      status = rec.ReadBody();
      if (status != Status::SUCCESS) {
        break;
      }
      value = rec.GetValue();
    }
    status = new_dbm.Set(rec.GetKey(), value);
    if (status != Status::SUCCESS) {
      break;
    }
    offset += rec.GetWholeSize();
    index++;
  }
  status = new_dbm.Close();
  status |= old_file.Close();
  return status;
}

}  // namespace tkrzw

// END OF FILE
