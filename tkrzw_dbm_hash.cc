/*************************************************************************************************
 * File database manager implementation based on hash table
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
#include "tkrzw_dbm_hash.h"
#include "tkrzw_dbm_hash_impl.h"
#include "tkrzw_file.h"
#include "tkrzw_file_mmap.h"
#include "tkrzw_file_pos.h"
#include "tkrzw_file_std.h"
#include "tkrzw_file_util.h"
#include "tkrzw_lib_common.h"
#include "tkrzw_str_util.h"
#include "tkrzw_thread_util.h"

namespace tkrzw {

constexpr int32_t METADATA_SIZE = 128;
const char META_MAGIC_DATA[] = "TkrzwHDB\n";
constexpr int32_t META_OFFSET_PKG_MAJOR_VERSION = 10;
constexpr int32_t META_OFFSET_PKG_MINOR_VERSION = 11;
constexpr int32_t META_OFFSET_STATIC_FLAGS = 12;
constexpr int32_t META_OFFSET_OFFSET_WIDTH = 13;
constexpr int32_t META_OFFSET_ALIGN_POW = 14;
constexpr int32_t META_OFFSET_CLOSURE_FLAGS = 15;
constexpr int32_t META_OFFSET_NUM_BUCKETS = 16;
constexpr int32_t META_OFFSET_NUM_RECORDS = 24;
constexpr int32_t META_OFFSET_EFF_DATA_SIZE = 32;
constexpr int32_t META_OFFSET_FILE_SIZE = 40;
constexpr int32_t META_OFFSET_MOD_TIME = 48;
constexpr int32_t META_OFFSET_DB_TYPE = 56;
constexpr int32_t META_OFFSET_OPAQUE = 64;
constexpr int32_t FBP_SECTION_SIZE = 1008;
constexpr int32_t RECORD_BASE_HEADER_SIZE = 16;
const char RECORD_BASE_MAGIC_DATA[] = "TkrzwREC\n";
constexpr int32_t RECHEAD_OFFSET_OFFSET_WIDTH = 10;
constexpr int32_t RECHEAD_OFFSET_ALIGN_POW = 11;
constexpr int32_t RECORD_MUTEX_NUM_SLOTS = 128;
constexpr int32_t RECORD_BASE_ALIGN = 4096;
constexpr int32_t MIN_OFFSET_WIDTH = 3;
constexpr int32_t MAX_OFFSET_WIDTH = 6;
constexpr int32_t MAX_ALIGN_POW = 16;
constexpr int64_t MAX_NUM_BUCKETS = 1099511627689LL;
constexpr int32_t REBUILD_NONBLOCKING_MAX_TRIES = 3;
constexpr int64_t REBUILD_BLOCKING_ALLOWANCE = 65536;

enum StaticFlag : uint8_t {
  STATIC_FLAG_NONE = 0,
  STATIC_FLAG_UPDATE_IN_PLACE = 1 << 0,
  STATIC_FLAG_UPDATE_APPENDING = 1 << 1,
};

enum ClosureFlag : uint8_t {
  CLOSURE_FLAG_NONE = 0,
  CLOSURE_FLAG_CLOSE = 1 << 0,
};

class HashDBMImpl final {
  friend class HashDBMIteratorImpl;
  typedef std::list<HashDBMIteratorImpl*> IteratorList;
 public:
  HashDBMImpl(std::unique_ptr<File> file);
  ~HashDBMImpl();
  Status Open(const std::string& path, bool writable,
              int32_t options, const HashDBM::TuningParameters& tuning_params);
  Status Close();
  Status Process(
      std::string_view key, DBM::RecordProcessor* proc, bool writable);
  Status ProcessEach(DBM::RecordProcessor* proc, bool writable);
  Status Count(int64_t* count);
  Status GetFileSize(int64_t* size);
  Status GetFilePath(std::string* path);
  Status Clear();
  Status Rebuild(const HashDBM::TuningParameters& tuning_params, bool skip_broken_records);
  Status ShouldBeRebuilt(bool* tobe);
  Status Synchronize(bool hard, DBM::FileProcessor* proc);
  std::vector<std::pair<std::string, std::string>> Inspect();
  bool IsOpen();
  bool IsWritable();
  bool IsHealthy();
  std::unique_ptr<DBM> MakeDBM();
  const File* GetInternalFile();
  int64_t GetEffectiveDataSize();
  double GetModificationTime();
  int32_t GetDatabaseType();
  Status SetDatabaseTypeMetadata(uint32_t db_type);
  std::string GetOpaqueMetadata();
  Status SetOpaqueMetadata(const std::string& opaque);
  int64_t CountBuckets();
  int64_t CountUsedBuckets();
  HashDBM::UpdateMode GetUpdateMode();
  Status SetUpdateModeAppending();
  Status ImportFromFileForward(
      const std::string& path, bool skip_broken_records,
      int64_t record_base, int64_t end_offset);
  Status ImportFromFileBackward(
      const std::string& path, bool skip_broken_records,
      int64_t record_base, int64_t end_offset);

 private:
  Status OpenImpl(bool writable);
  Status CloseImpl();
  void CancelIterators();
  Status SaveMetadata(bool finish);
  Status LoadMetadata();
  void SetRecordBase();
  Status InitializeBuckets();
  Status SaveFBP();
  Status LoadFBP();
  Status ProcessImpl(
      std::string_view key, int64_t bucket_index, DBM::RecordProcessor* proc, bool writable);
  Status GetBucketValue(int64_t bucket_index, int64_t* value);
  Status SetBucketValue(int64_t bucket_index, int64_t value);
  Status ReadNextBucketRecords(HashDBMIteratorImpl* iter);

  bool open_;
  bool writable_;
  bool healthy_;
  std::string path_;
  uint8_t pkg_major_version_;
  uint8_t pkg_minor_version_;
  uint8_t static_flags_;
  int32_t offset_width_;
  int32_t align_pow_;
  uint8_t closure_flags_;
  int64_t num_buckets_;
  std::atomic_int64_t num_records_;
  std::atomic_int64_t eff_data_size_;
  int64_t file_size_;
  int64_t mod_time_;
  uint32_t db_type_;
  std::string opaque_;
  int64_t record_base_;
  IteratorList iterators_;
  FreeBlockPool fbp_;
  bool lock_mem_buckets_;
  std::unique_ptr<File> file_;
  std::shared_timed_mutex mutex_;
  HashMutex record_mutex_;
  std::mutex file_mutex_;
};

class HashDBMIteratorImpl final {
  friend class HashDBMImpl;
 public:
  explicit HashDBMIteratorImpl(HashDBMImpl* dbm);
  ~HashDBMIteratorImpl();
  Status First();
  Status Jump(std::string_view key);
  Status Next();
  Status Process(DBM::RecordProcessor* proc, bool writable);

 private:
  Status ReadKeys();

  HashDBMImpl* dbm_;
  std::atomic_int64_t bucket_index_;
  std::set<std::string> keys_;
};

HashDBMImpl::HashDBMImpl(std::unique_ptr<File> file)
    : open_(false), writable_(false), healthy_(false), path_(),
      pkg_major_version_(0), pkg_minor_version_(0), static_flags_(STATIC_FLAG_NONE),
      offset_width_(HashDBM::DEFAULT_OFFSET_WIDTH), align_pow_(HashDBM::DEFAULT_ALIGN_POW),
      closure_flags_(CLOSURE_FLAG_NONE),
      num_buckets_(HashDBM::DEFAULT_NUM_BUCKETS),
      num_records_(0), eff_data_size_(0), file_size_(0), mod_time_(0),
      db_type_(0), opaque_(),
      record_base_(0), iterators_(),
      fbp_(HashDBM::DEFAULT_FBP_CAPACITY), lock_mem_buckets_(false),
      file_(std::move(file)),
      mutex_(), record_mutex_(RECORD_MUTEX_NUM_SLOTS, 1, PrimaryHash),
      file_mutex_() {}

HashDBMImpl::~HashDBMImpl() {
  if (open_) {
    Close();
  }
  for (auto* iterator : iterators_) {
    iterator->dbm_ = nullptr;
  }
}

Status HashDBMImpl::Open(const std::string& path, bool writable,
                         int32_t options, const HashDBM::TuningParameters& tuning_params) {
  std::lock_guard<std::shared_timed_mutex> lock(mutex_);
  if (open_) {
    return Status(Status::PRECONDITION_ERROR, "opened database");
  }
  const std::string norm_path = NormalizePath(path);
  if (tuning_params.update_mode == HashDBM::UPDATE_DEFAULT ||
      tuning_params.update_mode == HashDBM::UPDATE_IN_PLACE) {
    static_flags_ |= STATIC_FLAG_UPDATE_IN_PLACE;
  } else {
    static_flags_ |= STATIC_FLAG_UPDATE_APPENDING;
  }
  if (tuning_params.offset_width >= 0) {
    offset_width_ =
        std::min(std::max(tuning_params.offset_width, MIN_OFFSET_WIDTH), MAX_OFFSET_WIDTH);
  }
  if (tuning_params.align_pow >= 0) {
    align_pow_ = std::min(tuning_params.align_pow, MAX_ALIGN_POW);
  }
  if (tuning_params.num_buckets >= 0) {
    num_buckets_ = GetHashBucketSize(std::min(tuning_params.num_buckets, MAX_NUM_BUCKETS));
  }
  if (tuning_params.fbp_capacity >= 0) {
    fbp_.SetCapacity(std::max(1, tuning_params.fbp_capacity));
  }
  lock_mem_buckets_ = tuning_params.lock_mem_buckets;
  Status status = file_->Open(norm_path, writable, options);
  if (status != Status::SUCCESS) {
    return status;
  }
  path_ = norm_path;
  status = OpenImpl(writable);
  if (status != Status::SUCCESS) {
    file_->Close();
    return status;
  }
  return Status(Status::SUCCESS);
}

Status HashDBMImpl::Close() {
  std::lock_guard<std::shared_timed_mutex> lock(mutex_);
  if (!open_) {
    return Status(Status::PRECONDITION_ERROR, "not opened database");
  }
  CancelIterators();
  Status status = CloseImpl();
  status |= file_->Close();
  path_.clear();
  return status;
}

Status HashDBMImpl::Process(
    std::string_view key, DBM::RecordProcessor* proc, bool writable) {
  std::shared_lock<std::shared_timed_mutex> lock(mutex_);
  if (!open_) {
    return Status(Status::PRECONDITION_ERROR, "not opened database");
  }
  if (writable) {
    if (!writable_) {
      return Status(Status::PRECONDITION_ERROR, "not writable database");
    }
    if (!healthy_) {
      return Status(Status::PRECONDITION_ERROR, "not healthy database");
    }
  }
  ScopedHashLock record_lock(record_mutex_, key, writable);
  const int64_t bucket_index = record_lock.GetBucketIndex();
  return ProcessImpl(key, bucket_index, proc, writable);
}

Status HashDBMImpl::ProcessEach(DBM::RecordProcessor* proc, bool writable) {
  std::shared_lock<std::shared_timed_mutex> lock(mutex_);
  if (!open_) {
    return Status(Status::PRECONDITION_ERROR, "not opened database");
  }
  if (writable) {
    if (!writable_) {
      return Status(Status::PRECONDITION_ERROR, "not writable database");
    }
    if (!healthy_) {
      return Status(Status::PRECONDITION_ERROR, "not healthy database");
    }
  }
  if (!writable && (static_flags_ & STATIC_FLAG_UPDATE_IN_PLACE)) {
    ScopedHashLock record_lock(record_mutex_, writable);
    proc->ProcessEmpty(DBM::RecordProcessor::NOOP);
    const int64_t end_offset = file_->GetSizeSimple();
    int64_t offset = record_base_;
    HashRecord rec(file_.get(), offset_width_, align_pow_);
    while (offset < end_offset) {
      Status status = rec.ReadMetadataKey(offset);
      if (status != Status::SUCCESS) {
        return status;
      }
      int64_t rec_size = rec.GetWholeSize();
      if (rec_size == 0) {
        status = rec.ReadBody();
        if (status != Status::SUCCESS) {
          return status;
        }
        rec_size = rec.GetWholeSize();
      }
      const std::string_view key = rec.GetKey();
      if (rec.GetOperationType() == HashRecord::OP_SET) {
        std::string_view value = rec.GetValue();
        if (value.data() == nullptr) {
          status = rec.ReadBody();
          if (status != Status::SUCCESS) {
            return status;
          }
          value = rec.GetValue();
        }
        proc->ProcessFull(key, value);
      }
      offset += rec_size;
    }
    proc->ProcessEmpty(DBM::RecordProcessor::NOOP);
    return Status(Status::SUCCESS);
  }
  ScopedHashLock record_lock(record_mutex_, writable);
  proc->ProcessEmpty(DBM::RecordProcessor::NOOP);
  for (int64_t bucket_index = 0; bucket_index < num_buckets_; bucket_index++) {
    int64_t current_offset = 0;
    Status status = GetBucketValue(bucket_index, &current_offset);
    if (status != Status::SUCCESS) {
      return status;
    }
    if (current_offset == 0) {
      continue;
    }
    std::set<std::string> keys, dead_keys;
    while (current_offset > 0) {
      HashRecord rec(file_.get(), offset_width_, align_pow_);
      status = rec.ReadMetadataKey(current_offset);
      if (status != Status::SUCCESS) {
        return status;
      }
      current_offset = rec.GetChildOffset();
      const std::string key(rec.GetKey());
      if (CheckSet(keys, key) || CheckSet(dead_keys, key)) {
        continue;
      }
      switch (rec.GetOperationType()) {
        case HashRecord::OP_SET:
          if (!writable) {
            std::string_view value = rec.GetValue();
            if (value.data() == nullptr) {
              status = rec.ReadBody();
              if (status != Status::SUCCESS) {
                return status;
              }
              value = rec.GetValue();
            }
            proc->ProcessFull(key, value);
          }
          keys.emplace(std::move(key));
          break;
        case HashRecord::OP_REMOVE:
          dead_keys.emplace(std::move(key));
          break;
        default:
          break;
      }
    }
    if (writable) {
      for (const auto& key : keys) {
        status = ProcessImpl(key, bucket_index, proc, true);
        if (status != Status::SUCCESS) {
          return status;
        }
      }
    }
  }
  proc->ProcessEmpty(DBM::RecordProcessor::NOOP);
  return Status(Status::SUCCESS);
}

Status HashDBMImpl::Count(int64_t* count) {
  std::shared_lock<std::shared_timed_mutex> lock(mutex_);
  if (!open_) {
    return Status(Status::PRECONDITION_ERROR, "not opened database");
  }
  *count = num_records_.load();
  return Status(Status::SUCCESS);
}

Status HashDBMImpl::GetFileSize(int64_t* size) {
  std::shared_lock<std::shared_timed_mutex> lock(mutex_);
  if (!open_) {
    return Status(Status::PRECONDITION_ERROR, "not opened database");
  }
  *size = file_->GetSizeSimple();
  return Status(Status::SUCCESS);
}

Status HashDBMImpl::GetFilePath(std::string* path) {
  std::shared_lock<std::shared_timed_mutex> lock(mutex_);
  if (!open_) {
    return Status(Status::PRECONDITION_ERROR, "not opened database");
  }
  *path = path_;
  return Status(Status::SUCCESS);
}

Status HashDBMImpl::Clear() {
  std::lock_guard<std::shared_timed_mutex> lock(mutex_);
  if (!open_) {
    return Status(Status::PRECONDITION_ERROR, "not opened database");
  }
  if (!writable_) {
    return Status(Status::PRECONDITION_ERROR, "not writable database");
  }
  const uint32_t static_flags = static_flags_;
  const int32_t offset_width = offset_width_;
  const int32_t align_pow = align_pow_;
  const int64_t num_buckets = num_buckets_;
  const uint32_t db_type = db_type_;
  const std::string opaque = opaque_;
  CancelIterators();
  Status status = CloseImpl();
  status |= file_->Truncate(0);
  static_flags_ = static_flags;
  offset_width_ = offset_width;
  align_pow_ = align_pow;
  num_buckets_ = num_buckets;
  db_type_ = db_type;
  opaque_ = opaque;
  status |= OpenImpl(true);
  return status;
}

Status HashDBMImpl::Rebuild(
    const HashDBM::TuningParameters& tuning_params, bool skip_broken_records) {
  std::shared_lock<std::shared_timed_mutex> lock(mutex_);
  if (!open_) {
    return Status(Status::PRECONDITION_ERROR, "not opened database");
  }
  if (!writable_) {
    return Status(Status::PRECONDITION_ERROR, "not writable database");
  }
  std::lock_guard<std::mutex> file_lock(file_mutex_);
  const bool in_place = static_flags_ & STATIC_FLAG_UPDATE_IN_PLACE;
  const std::string tmp_path = path_ + ".tmp.rebuild";
  int64_t est_num_records = num_records_.load();
  if (est_num_records < HashDBM::DEFAULT_NUM_BUCKETS) {
    const int64_t record_section_size = file_->GetSizeSimple() - record_base_;
    est_num_records = std::max(est_num_records, record_section_size / PAGE_SIZE);
    est_num_records = std::min(est_num_records, HashDBM::DEFAULT_NUM_BUCKETS);
  }
  HashDBM::TuningParameters tmp_tuning_params;
  tmp_tuning_params.update_mode = HashDBM::UPDATE_IN_PLACE;
  tmp_tuning_params.offset_width = tuning_params.offset_width > 0 ?
      tuning_params.offset_width : offset_width_;
  tmp_tuning_params.align_pow = tuning_params.align_pow >= 0 ?
      tuning_params.align_pow : align_pow_;
  tmp_tuning_params.num_buckets = tuning_params.num_buckets >= 0 ?
      tuning_params.num_buckets : est_num_records * 2 + 1;
  HashDBM tmp_dbm(file_->MakeFile());
  auto CleanUp = [&]() {
    tmp_dbm.Close();
    RemoveFile(tmp_path);
  };
  Status status = tmp_dbm.OpenAdvanced(tmp_path, true, File::OPEN_TRUNCATE, tmp_tuning_params);
  if (status != Status::SUCCESS) {
    CleanUp();
    return status;
  }
  int64_t end_offset = INT64MAX;
  {
    ScopedHashLock record_lock(record_mutex_, true);
    static_flags_ &= ~STATIC_FLAG_UPDATE_IN_PLACE;
    static_flags_ |= STATIC_FLAG_UPDATE_APPENDING;
    end_offset = file_->GetSizeSimple();
  }
  if (in_place) {
    status = tmp_dbm.ImportFromFileForward(path_, skip_broken_records, -1, end_offset);
  } else {
    status = tmp_dbm.ImportFromFileBackward(path_, skip_broken_records, -1, end_offset);
  }
  if (status != Status::SUCCESS) {
    CleanUp();
    return status;
  }
  int64_t begin_offset = end_offset;
  for (int32_t num_tries = 0; num_tries < REBUILD_NONBLOCKING_MAX_TRIES; num_tries++) {
    {
      ScopedHashLock record_lock(record_mutex_, false);
      end_offset = file_->GetSizeSimple();
    }
    if (begin_offset >= end_offset - REBUILD_BLOCKING_ALLOWANCE) {
      break;
    }
    status = tmp_dbm.ImportFromFileForward(path_, false, begin_offset, end_offset);
    if (status != Status::SUCCESS) {
      CleanUp();
      return status;
    }
    begin_offset = end_offset;
  }
  {
    ScopedHashLock record_lock(record_mutex_, true);
    end_offset = file_->GetSizeSimple();
    if (begin_offset < end_offset) {
      status = tmp_dbm.ImportFromFileForward(path_, false, begin_offset, end_offset);
      if (status != Status::SUCCESS) {
        CleanUp();
        return status;
      }
    }
    if ((tuning_params.update_mode == HashDBM::UPDATE_DEFAULT && !in_place) ||
        tuning_params.update_mode == HashDBM::UPDATE_APPENDING) {
      status = tmp_dbm.SetUpdateModeAppending();
      if (status != Status::SUCCESS) {
        CleanUp();
        return status;
      }
    }
    status = tmp_dbm.Close();
    if (status != Status::SUCCESS) {
      CleanUp();
      return status;
    }
    const uint32_t db_type = db_type_;
    const std::string opaque = opaque_;
    CancelIterators();
    auto tmp_file = file_->MakeFile();
    status = tmp_file->Open(tmp_path, true);
    if (status != Status::SUCCESS) {
      CleanUp();
      return status;
    }
    fbp_.Clear();
    if (IS_POSIX) {
      status |= tmp_file->Rename(path_);
      status |= file_->Close();
    } else {
      status |= file_->Close();
      status |= tmp_file->Rename(path_);
    }
    file_ = std::move(tmp_file);
    if (tuning_params.fbp_capacity >= 0) {
      fbp_.SetCapacity(tuning_params.fbp_capacity);
    }
    lock_mem_buckets_ = tuning_params.lock_mem_buckets;
    status |= OpenImpl(true);
    db_type_ = db_type;
    opaque_ = opaque;
  }
  return status;
}

Status HashDBMImpl::ShouldBeRebuilt(bool* tobe) {
  std::shared_lock<std::shared_timed_mutex> lock(mutex_);
  if (!open_) {
    return Status(Status::PRECONDITION_ERROR, "not opened database");
  }
  if (writable_) {
    if (!file_mutex_.try_lock()) {
      *tobe = false;
      return Status(Status::SUCCESS);
    }
    file_mutex_.unlock();
  }
  const int64_t record_section_size = file_->GetSizeSimple() - record_base_;
  const int64_t min_record_size = sizeof(uint8_t) + offset_width_ + sizeof(uint8_t) * 3;
  const int64_t total_record_size = eff_data_size_.load() + min_record_size * num_records_.load();
  const int64_t aligned_min_size = (1 << align_pow_) * num_records_.load();
  const int64_t minimum_total_size = total_record_size + aligned_min_size;
  if (record_section_size > minimum_total_size * 2) {
    *tobe = true;
  }
  if (num_records_.load() > num_buckets_) {
    *tobe = true;
  }
  return Status(Status::SUCCESS);
}

Status HashDBMImpl::Synchronize(bool hard, DBM::FileProcessor* proc) {
  std::lock_guard<std::shared_timed_mutex> lock(mutex_);
  if (!open_) {
    return Status(Status::PRECONDITION_ERROR, "not opened database");
  }
  if (!writable_) {
    return Status(Status::PRECONDITION_ERROR, "not writable database");
  }
  if (!healthy_) {
    return Status(Status::PRECONDITION_ERROR, "not healthy database");
  }
  file_size_ = file_->GetSizeSimple();
  mod_time_ = GetWallTime() * 1000000;
  Status status = SaveMetadata(true);
  status |= file_->Synchronize(hard);
  if (proc != nullptr) {
    proc->Process(path_);
  }
  status |= SaveMetadata(false);
  return status;
}

std::vector<std::pair<std::string, std::string>> HashDBMImpl::Inspect() {
  std::shared_lock<std::shared_timed_mutex> lock(mutex_);
  std::vector<std::pair<std::string, std::string>> meta;
  auto Add = [&](const std::string& name, const std::string& value) {
    meta.emplace_back(std::make_pair(name, value));
  };
  Add("class", "HashDBM");
  if (open_) {
    Add("healthy", ToString(healthy_));
    Add("path", path_);
    Add("pkg_major_version", ToString(pkg_major_version_));
    Add("pkg_minor_version", ToString(pkg_minor_version_));
    Add("static_flags", ToString(static_flags_));
    Add("offset_width", ToString(offset_width_));
    Add("align_pow", ToString(align_pow_));
    Add("closure_flags", ToString(closure_flags_));
    Add("num_buckets", ToString(num_buckets_));
    Add("num_records", ToString(num_records_.load()));
    Add("eff_data_size", ToString(eff_data_size_.load()));
    Add("file_size", ToString(file_->GetSizeSimple()));
    Add("mod_time", ToString(mod_time_ / 1000000.0));
    Add("db_type", ToString(db_type_));
    Add("max_file_size", ToString(1LL << (offset_width_ * 8 + align_pow_)));
    Add("record_base", ToString(record_base_));
    if (static_flags_ & STATIC_FLAG_UPDATE_IN_PLACE) {
      Add("update_mode", "in-place");
    } else if (static_flags_ & STATIC_FLAG_UPDATE_APPENDING) {
      Add("update_mode", "appending");
    }
  }
  return meta;
}

bool HashDBMImpl::IsOpen() {
  std::shared_lock<std::shared_timed_mutex> lock(mutex_);
  return open_;
}

bool HashDBMImpl::IsWritable() {
  std::shared_lock<std::shared_timed_mutex> lock(mutex_);
  return open_ && writable_;
}

bool HashDBMImpl::IsHealthy() {
  std::shared_lock<std::shared_timed_mutex> lock(mutex_);
  return open_ && healthy_;
}

std::unique_ptr<DBM> HashDBMImpl::MakeDBM() {
  std::shared_lock<std::shared_timed_mutex> lock(mutex_);
  return std::make_unique<HashDBM>(file_->MakeFile());
}

const File* HashDBMImpl::GetInternalFile() {
  return file_.get();
}

int64_t HashDBMImpl::GetEffectiveDataSize() {
  std::lock_guard<std::shared_timed_mutex> lock(mutex_);
  if (!open_) {
    return -1;
  }
  return eff_data_size_.load();
}

double HashDBMImpl::GetModificationTime() {
  std::shared_lock<std::shared_timed_mutex> lock(mutex_);
  if (!open_) {
    return -1;
  }
  return mod_time_ / 1000000.0;
}

int32_t HashDBMImpl::GetDatabaseType() {
  std::shared_lock<std::shared_timed_mutex> lock(mutex_);
  if (!open_) {
    return -1;
  }
  return db_type_;
}

Status HashDBMImpl::SetDatabaseTypeMetadata(uint32_t db_type) {
  std::lock_guard<std::shared_timed_mutex> lock(mutex_);
  if (!open_) {
    return Status(Status::PRECONDITION_ERROR, "not opened database");
  }
  if (!writable_) {
    return Status(Status::PRECONDITION_ERROR, "not writable database");
  }
  db_type_ = db_type;
  return Status(Status::SUCCESS);
}

std::string HashDBMImpl::GetOpaqueMetadata() {
  std::shared_lock<std::shared_timed_mutex> lock(mutex_);
  if (!open_) {
    return "";
  }
  return opaque_;
}

Status HashDBMImpl::SetOpaqueMetadata(const std::string& opaque) {
  std::lock_guard<std::shared_timed_mutex> lock(mutex_);
  if (!open_) {
    return Status(Status::PRECONDITION_ERROR, "not opened database");
  }
  if (!writable_) {
    return Status(Status::PRECONDITION_ERROR, "not writable database");
  }
  opaque_ = opaque;
  return Status(Status::SUCCESS);
}

int64_t HashDBMImpl::CountBuckets() {
  std::shared_lock<std::shared_timed_mutex> lock(mutex_);
  if (!open_) {
    return -1;
  }
  return num_buckets_;
}

int64_t HashDBMImpl::CountUsedBuckets() {
  std::shared_lock<std::shared_timed_mutex> lock(mutex_);
  if (!open_) {
    return -1;
  }
  int64_t num_used = 0;
  int64_t bucket_index = 0;
  while (bucket_index < num_buckets_) {
    ScopedHashLock record_lock(record_mutex_, bucket_index, false);
    if (record_lock.GetBucketIndex() < 0) {
      num_used = 0;
      bucket_index = 0;
      continue;
    }
    int64_t offset = 0;
    const Status status = GetBucketValue(bucket_index, &offset);
    if (status != Status::SUCCESS) {
      return -1;
    }
    if (offset != 0) {
      num_used++;
    }
    bucket_index++;
  }
  return num_used;
}

HashDBM::UpdateMode HashDBMImpl::GetUpdateMode() {
  std::shared_lock<std::shared_timed_mutex> lock(mutex_);
  if (!open_) {
    return HashDBM::UPDATE_DEFAULT;
  }
  HashDBM::UpdateMode mode = HashDBM::UPDATE_DEFAULT;
  if (static_flags_ & STATIC_FLAG_UPDATE_IN_PLACE)  {
    mode = HashDBM::UPDATE_IN_PLACE;
  } else if (static_flags_ & STATIC_FLAG_UPDATE_APPENDING) {
    mode = HashDBM::UPDATE_APPENDING;
  }
  return mode;
}

Status HashDBMImpl::SetUpdateModeAppending() {
  std::lock_guard<std::shared_timed_mutex> lock(mutex_);
  if (!open_) {
    return Status(Status::PRECONDITION_ERROR, "not opened database");
  }
  if (!writable_) {
    return Status(Status::PRECONDITION_ERROR, "not writable database");
  }
  static_flags_ &= ~STATIC_FLAG_UPDATE_IN_PLACE;
  static_flags_ |= STATIC_FLAG_UPDATE_APPENDING;
  return Status(Status::SUCCESS);
}

Status HashDBMImpl::ImportFromFileForward(
    const std::string& path, bool skip_broken_records,
    int64_t record_base, int64_t end_offset) {
  {
    std::shared_lock<std::shared_timed_mutex> lock(mutex_);
    if (!open_) {
      return Status(Status::PRECONDITION_ERROR, "not opened database");
    }
    if (!writable_) {
      return Status(Status::PRECONDITION_ERROR, "not writable database");
    }
    if (!healthy_) {
      return Status(Status::PRECONDITION_ERROR, "not healthy database");
    }
  }
  auto file = file_->MakeFile();
  Status status = file->Open(path, false, File::OPEN_NO_LOCK);
  if (status != Status::SUCCESS) {
    return status;
  }
  int64_t tmp_record_base = 0;
  int32_t offset_width = 0;
  int32_t align_pow = 0;
  int64_t last_sync_size = 0;
  status = HashDBM::FindRecordBase(
      file.get(), &record_base, &offset_width, &align_pow, &last_sync_size);
  if (status != Status::SUCCESS) {
    file->Close();
    return status;
  }
  if (record_base < 0) {
    record_base = tmp_record_base;
  }
  if (end_offset == 0) {
    if (last_sync_size < record_base || last_sync_size % (1 << align_pow) != 0) {
      file->Close();
      return Status(Status::BROKEN_DATA_ERROR, "unavailable last sync size");
    }
    end_offset = last_sync_size;
  }
  Status import_status(Status::SUCCESS);
  class Importer final : public DBM::RecordProcessor {
   public:
    Importer(HashDBMImpl* impl, Status* status) : impl_(impl), status_(status) {}
    std::string_view ProcessFull(std::string_view key, std::string_view value) override {
      Status set_status(Status::SUCCESS);
      DBM::RecordProcessorSet setter(&set_status, value, true, nullptr);
      *status_ = impl_->Process(key, &setter, true);
      *status_ |= set_status;
      return NOOP;
    }
    std::string_view ProcessEmpty(std::string_view key) override {
      Status remove_status(Status::SUCCESS);
      DBM::RecordProcessorRemove remover(&remove_status, nullptr);
      *status_ = impl_->Process(key, &remover, true);
      *status_ |= remove_status;
      return NOOP;
    }
   private:
    HashDBMImpl* impl_;
    Status* status_;
  } importer(this, &import_status);
  status = HashRecord::ReplayOperations(
      file.get(), &importer, record_base, offset_width, align_pow,
      skip_broken_records, end_offset);
  if (status != Status::SUCCESS) {
    file->Close();
    return status;
  }
  import_status |= file->Close();
  return import_status;
}

Status HashDBMImpl::ImportFromFileBackward(
    const std::string& path, bool skip_broken_records,
    int64_t record_base, int64_t end_offset) {
  std::string offset_path, dead_path;
  {
    std::shared_lock<std::shared_timed_mutex> lock(mutex_);
    if (!open_) {
      return Status(Status::PRECONDITION_ERROR, "not opened database");
    }
    if (!writable_) {
      return Status(Status::PRECONDITION_ERROR, "not writable database");
    }
    if (!healthy_) {
      return Status(Status::PRECONDITION_ERROR, "not healthy database");
    }
    offset_path = path_ + ".tmp.offset";
    dead_path = path_ + ".tmp.dead";
  }
  auto file = file_->MakeFile();
  Status status = file->Open(path, false, File::OPEN_NO_LOCK);
  if (status != Status::SUCCESS) {
    return status;
  }
  int64_t tmp_record_base = 0;
  int32_t offset_width = 0;
  int32_t align_pow = 0;
  int64_t last_sync_size = 0;
  status = HashDBM::FindRecordBase(
      file.get(), &record_base, &offset_width, &align_pow, &last_sync_size);
  if (status != Status::SUCCESS) {
    file->Close();
    return status;
  }
  if (record_base < 0) {
    record_base = tmp_record_base;
  }
  if (end_offset == 0) {
    if (last_sync_size < record_base || last_sync_size % (1 << align_pow) != 0) {
      file->Close();
      return Status(Status::BROKEN_DATA_ERROR, "unavailable last sync size");
    }
    end_offset = last_sync_size;
  }
  auto offset_file = file_->MakeFile();
  HashDBM dead_dbm;
  auto CleanUp = [&]() {
    dead_dbm.Close();
    RemoveFile(dead_path);
    offset_file->Close();
    RemoveFile(offset_path);
  };
  status = offset_file->Open(offset_path, true, File::OPEN_TRUNCATE);
  if (status != Status::SUCCESS) {
    CleanUp();
    return status;
  }
  status = HashRecord::ExtractOffsets(
      file.get(), offset_file.get(), record_base, offset_width, align_pow,
      skip_broken_records, end_offset);
  if (status != Status::SUCCESS) {
    CleanUp();
    return status;
  }
  const int64_t num_offsets = offset_file->GetSizeSimple() / offset_width_;
  const int64_t dead_num_buckets = std::min(num_offsets, num_buckets_);
  HashDBM::TuningParameters dead_tuning_params;
  dead_tuning_params.offset_width = offset_width;
  dead_tuning_params.align_pow = 0;
  dead_tuning_params.num_buckets = dead_num_buckets;
  status = dead_dbm.OpenAdvanced(dead_path, true, File::OPEN_TRUNCATE, dead_tuning_params);
  OffsetReader reader(offset_file.get(), offset_width, align_pow, true);
  HashRecord rec(file.get(), offset_width, align_pow);
  while (true) {
    int64_t offset = 0;
    status = reader.ReadOffset(&offset);
    if (status != Status::SUCCESS) {
      if (status != Status::NOT_FOUND_ERROR) {
        CleanUp();
        return status;
      }
      break;
    }
    status = rec.ReadMetadataKey(offset);
    if (status != Status::SUCCESS) {
      CleanUp();
      return status;
    }
    std::string_view key = rec.GetKey();
    switch (rec.GetOperationType()) {
      case HashRecord::OP_SET: {
        std::string_view value = rec.GetValue();
        if (value.data() == nullptr) {
          status = rec.ReadBody();
          if (status != Status::SUCCESS) {
            CleanUp();
            return status;
          }
          value = rec.GetValue();
        }
        bool removed = false;
        status = dead_dbm.Get(key, nullptr);
        if (status == Status::SUCCESS) {
          removed = true;
        } else if (status != Status::NOT_FOUND_ERROR) {
          CleanUp();
          return status;
        }
        if (!removed) {
          Status set_status(Status::SUCCESS);
          DBM::RecordProcessorSet setter(&set_status, value, false, nullptr);
          status = Process(key, &setter, true);
          if (status != Status::SUCCESS) {
            CleanUp();
            return status;
          }
          if (set_status != Status::SUCCESS && set_status != Status::DUPLICATION_ERROR) {
            return set_status;
          }
        }
        break;
      }
      case HashRecord::OP_REMOVE: {
        bool setted = false;
        Status get_status(Status::SUCCESS);
        DBM::RecordProcessorGet getter(&get_status, nullptr);
        status = Process(key, &getter, true);
        if (status != Status::SUCCESS) {
          CleanUp();
          return status;
        }
        if (get_status == Status::SUCCESS) {
          setted = true;
        } else if (get_status != Status::NOT_FOUND_ERROR) {
          CleanUp();
          return get_status;
        }
        if (!setted) {
          status = dead_dbm.Set(key, "", false);
          if (status != Status::SUCCESS && status != Status::DUPLICATION_ERROR) {
            CleanUp();
            return status;
          }
        }
        break;
      }
      default: {
        return Status(Status::BROKEN_DATA_ERROR, "void data is read");
      }
    }
  }
  status = dead_dbm.Close();
  status |= RemoveFile(dead_path);
  status |= offset_file->Close();
  status |= RemoveFile(offset_path);
  return status;
}

Status HashDBMImpl::OpenImpl(bool writable) {
  if (writable && file_->GetSizeSimple() < 1) {
    SetRecordBase();
    Status status = InitializeBuckets();
    if (status != Status::SUCCESS) {
      return status;
    }
    const auto version_nums = StrSplit(PACKAGE_VERSION, '.');
    pkg_major_version_ = version_nums.size() > 0 ? StrToInt(version_nums[0]) : 0;
    pkg_minor_version_ = version_nums.size() > 1 ? StrToInt(version_nums[1]) : 0;
    closure_flags_ |= CLOSURE_FLAG_CLOSE;
    file_size_ = file_->GetSizeSimple();
    mod_time_ = GetWallTime() * 1000000;
    status = SaveMetadata(true);
    if (status != Status::SUCCESS) {
      return status;
    }
  }
  Status status = LoadMetadata();
  if (status != Status::SUCCESS) {
    return status;
  }
  SetRecordBase();
  bool healthy = closure_flags_ & CLOSURE_FLAG_CLOSE;
  if (file_size_ != file_->GetSizeSimple()) {
    healthy = false;
  }
  if (writable && healthy) {
    status = SaveMetadata(false);
    if (status != Status::SUCCESS) {
      return status;
    }
    if (static_flags_ & STATIC_FLAG_UPDATE_IN_PLACE) {
      status = LoadFBP();
      if (status != Status::SUCCESS) {
        return status;
      }
    }
  }
  record_mutex_.Rehash(num_buckets_);
  open_ = true;
  writable_ = writable;
  healthy_ = healthy;
  return Status(Status::SUCCESS);
}

Status HashDBMImpl::CloseImpl() {
  Status status(Status::SUCCESS);
  if (writable_ && healthy_) {
    file_size_ = file_->GetSizeSimple();
    mod_time_ = GetWallTime() * 1000000;
    status |= SaveMetadata(true);
    status |= SaveFBP();
  }
  open_ = false;
  writable_ = false;
  healthy_ = false;
  pkg_major_version_ = 0;
  pkg_minor_version_ = 0;
  static_flags_ = STATIC_FLAG_NONE;
  offset_width_ = HashDBM::DEFAULT_OFFSET_WIDTH;
  align_pow_ = HashDBM::DEFAULT_ALIGN_POW;
  closure_flags_ = CLOSURE_FLAG_NONE;
  num_buckets_ = HashDBM::DEFAULT_NUM_BUCKETS;
  num_records_.store(0);
  eff_data_size_.store(0);
  file_size_ = 0;
  mod_time_ = 0;
  db_type_ = 0;
  opaque_.clear();
  record_base_ = 0;
  fbp_.Clear();
  lock_mem_buckets_ = false;
  return status;
}

void HashDBMImpl::CancelIterators() {
  for (auto* iterator : iterators_) {
    iterator->bucket_index_.store(-1);
  }
}

Status HashDBMImpl::SaveMetadata(bool finish) {
  char meta[METADATA_SIZE];
  std::memset(meta, 0, METADATA_SIZE);
  std::memcpy(meta, META_MAGIC_DATA, sizeof(META_MAGIC_DATA));
  WriteFixNum(meta + META_OFFSET_PKG_MAJOR_VERSION, pkg_major_version_, 1);
  WriteFixNum(meta + META_OFFSET_PKG_MINOR_VERSION, pkg_minor_version_, 1);
  WriteFixNum(meta + META_OFFSET_STATIC_FLAGS, static_flags_, 1);
  WriteFixNum(meta + META_OFFSET_OFFSET_WIDTH, offset_width_, 1);
  WriteFixNum(meta + META_OFFSET_ALIGN_POW, align_pow_, 1);
  uint8_t closure_flags = CLOSURE_FLAG_NONE;
  if ((closure_flags_ & CLOSURE_FLAG_CLOSE) && finish) {
    closure_flags |= CLOSURE_FLAG_CLOSE;
  }
  WriteFixNum(meta + META_OFFSET_CLOSURE_FLAGS, closure_flags, 1);
  WriteFixNum(meta + META_OFFSET_NUM_BUCKETS, num_buckets_, 8);
  WriteFixNum(meta + META_OFFSET_NUM_RECORDS, num_records_.load(), 8);
  WriteFixNum(meta + META_OFFSET_EFF_DATA_SIZE, eff_data_size_.load(), 8);
  WriteFixNum(meta + META_OFFSET_FILE_SIZE, file_size_, 8);
  WriteFixNum(meta + META_OFFSET_MOD_TIME, mod_time_, 8);
  WriteFixNum(meta + META_OFFSET_DB_TYPE, db_type_, 4);
  const int32_t opaque_size =
      std::min<int32_t>(opaque_.size(), METADATA_SIZE - META_OFFSET_OPAQUE);
  std::memcpy(meta + META_OFFSET_OPAQUE, opaque_.data(), opaque_size);
  return file_->Write(0, meta, METADATA_SIZE);
}

Status HashDBMImpl::LoadMetadata() {
  char meta[METADATA_SIZE];
  const Status status = file_->Read(0, meta, METADATA_SIZE);
  if (status != Status::SUCCESS) {
    return status;
  }
  if (std::memcmp(meta, META_MAGIC_DATA, sizeof(META_MAGIC_DATA)) != 0) {
    return Status(Status::BROKEN_DATA_ERROR, "bad magic data");
  }
  pkg_major_version_ = ReadFixNum(meta + META_OFFSET_PKG_MAJOR_VERSION, 1);
  pkg_minor_version_ = ReadFixNum(meta + META_OFFSET_PKG_MINOR_VERSION, 1);
  static_flags_ = ReadFixNum(meta + META_OFFSET_STATIC_FLAGS, 1);
  offset_width_ = ReadFixNum(meta + META_OFFSET_OFFSET_WIDTH, 1);
  align_pow_ = ReadFixNum(meta + META_OFFSET_ALIGN_POW, 1);
  closure_flags_ = ReadFixNum(meta + META_OFFSET_CLOSURE_FLAGS, 1);
  num_buckets_ = ReadFixNum(meta + META_OFFSET_NUM_BUCKETS, 8);
  num_records_.store(ReadFixNum(meta + META_OFFSET_NUM_RECORDS, 8));
  eff_data_size_.store(ReadFixNum(meta + META_OFFSET_EFF_DATA_SIZE, 8));
  file_size_ = ReadFixNum(meta + META_OFFSET_FILE_SIZE, 8);
  mod_time_ = ReadFixNum(meta + META_OFFSET_MOD_TIME, 8);
  db_type_ = ReadFixNum(meta + META_OFFSET_DB_TYPE, 4);
  opaque_ = std::string(meta + META_OFFSET_OPAQUE, METADATA_SIZE - META_OFFSET_OPAQUE);
  if (pkg_major_version_ < 1 && pkg_minor_version_ < 1) {
    return Status(Status::BROKEN_DATA_ERROR, "invalid package version");
  }
  if (!(static_flags_ & STATIC_FLAG_UPDATE_IN_PLACE) &&
      !(static_flags_ & STATIC_FLAG_UPDATE_APPENDING)) {
    return Status(Status::BROKEN_DATA_ERROR, "invalid static flags");
  }
  if (offset_width_ < MIN_OFFSET_WIDTH || offset_width_ > MAX_OFFSET_WIDTH) {
    return Status(Status::BROKEN_DATA_ERROR, "the offset width is invalid");
  }
  if (align_pow_ > MAX_ALIGN_POW) {
    return Status(Status::BROKEN_DATA_ERROR, "the alignment power is invalid");
  }
  if (num_records_.load() < 0) {
    return Status(Status::BROKEN_DATA_ERROR, "invalid record count");
  }
  if (eff_data_size_.load() < 0) {
    return Status(Status::BROKEN_DATA_ERROR, "invalid effective data size");
  }
  if (file_size_ < METADATA_SIZE) {
    return Status(Status::BROKEN_DATA_ERROR, "invalid file size");
  }
  if (num_buckets_ < 1 || num_buckets_ > MAX_NUM_BUCKETS) {
    return Status(Status::BROKEN_DATA_ERROR, "the bucket size is invalid");
  }
  return Status(Status::SUCCESS);
}

void HashDBMImpl::SetRecordBase() {
  const int32_t align = std::max(RECORD_BASE_ALIGN, 1 << align_pow_);
  record_base_ = METADATA_SIZE + num_buckets_ * offset_width_ +
      FBP_SECTION_SIZE + RECORD_BASE_HEADER_SIZE;
  const int32_t diff = record_base_ % align;
  if (diff > 0) {
    record_base_ += align - diff;
  }
}

Status HashDBMImpl::InitializeBuckets() {
  int64_t offset = METADATA_SIZE;
  int64_t size = record_base_ - offset;
  Status status = file_->Truncate(record_base_);
  if (lock_mem_buckets_) {
    if (typeid(*file_) == typeid(MemoryMapParallelFile)) {
      auto* mem_file = dynamic_cast<MemoryMapParallelFile*>(file_.get());
      status |= mem_file->LockMemory(record_base_);
    } else if (typeid(*file_) == typeid(MemoryMapAtomicFile)) {
      auto* mem_file = dynamic_cast<MemoryMapAtomicFile*>(file_.get());
      status |= mem_file->LockMemory(record_base_);
    }
  }
  char* buf = new char[PAGE_SIZE];
  std::memset(buf, 0, PAGE_SIZE);
  while (size > 0) {
    const int64_t write_size = std::min<int64_t>(size, PAGE_SIZE);
    const Status write_status = file_->Write(offset, buf, write_size);
    if (write_status != Status::SUCCESS) {
      status |= write_status;
      break;
    }
    offset += write_size;
    size -= write_size;
  }
  delete[] buf;
  char head[RECORD_BASE_HEADER_SIZE];
  std::memset(head, 0, RECORD_BASE_HEADER_SIZE);
  std::memcpy(head, RECORD_BASE_MAGIC_DATA, sizeof(RECORD_BASE_MAGIC_DATA));
  WriteFixNum(head + RECHEAD_OFFSET_OFFSET_WIDTH, offset_width_, 1);
  WriteFixNum(head + RECHEAD_OFFSET_ALIGN_POW, align_pow_, 1);
  status |= file_->Write(record_base_ - RECORD_BASE_HEADER_SIZE, head, RECORD_BASE_HEADER_SIZE);
  return status;
}

Status HashDBMImpl::SaveFBP() {
  const std::string& serialized = fbp_.Serialize(offset_width_, align_pow_, FBP_SECTION_SIZE);
  return file_->Write(record_base_ - RECORD_BASE_HEADER_SIZE - FBP_SECTION_SIZE,
                      serialized.data(), serialized.size());
}

Status HashDBMImpl::LoadFBP() {
  char buf[FBP_SECTION_SIZE];
  const Status status = file_->Read(record_base_ - RECORD_BASE_HEADER_SIZE - FBP_SECTION_SIZE,
                                    buf, FBP_SECTION_SIZE);
  if (status != Status::SUCCESS) {
    return status;
  }
  fbp_.Deserialize(std::string_view(buf, FBP_SECTION_SIZE), offset_width_, align_pow_);
  return Status(Status::SUCCESS);
}

Status HashDBMImpl::ProcessImpl(
    std::string_view key, int64_t bucket_index, DBM::RecordProcessor* proc, bool writable) {
  const bool in_place = static_flags_ & STATIC_FLAG_UPDATE_IN_PLACE;
  int64_t top = 0;
  Status status = GetBucketValue(bucket_index, &top);
  if (status != Status::SUCCESS) {
    return status;
  }
  int64_t current_offset = top;
  int64_t parent_offset = 0;
  HashRecord rec(file_.get(), offset_width_, align_pow_);
  while (current_offset > 0) {
    status = rec.ReadMetadataKey(current_offset);
    if (status != Status::SUCCESS) {
      return status;
    }
    const auto rec_key = rec.GetKey();
    if (key == rec_key) {
      std::string_view new_value;
      const bool old_is_set = rec.GetOperationType() == HashRecord::OP_SET;
      std::string_view old_value = rec.GetValue();
      if (old_is_set) {
        if (old_value.data() == nullptr) {
          status = rec.ReadBody();
          if (status != Status::SUCCESS) {
            return status;
          }
          old_value = rec.GetValue();
        }
        new_value = proc->ProcessFull(key, old_value);
      } else {
        new_value = proc->ProcessEmpty(key);
      }
      if (new_value.data() != DBM::RecordProcessor::NOOP.data() && writable) {
        const int64_t child_offset = rec.GetChildOffset();
        int32_t old_rec_size = rec.GetWholeSize();
        if (old_rec_size == 0) {
          status = rec.ReadBody();
          if (status != Status::SUCCESS) {
            return status;
          }
          old_rec_size = rec.GetWholeSize();
        }
        int32_t new_rec_size = 0;
        if (in_place) {
          if (new_value.data() == DBM::RecordProcessor::REMOVE.data()) {
            rec.SetData(HashRecord::OP_VOID, old_rec_size, "", 0, "", 0, 0);
            new_rec_size = rec.GetWholeSize();
          } else {
            rec.SetData(HashRecord::OP_SET, old_rec_size, key.data(), key.size(),
                        new_value.data(), new_value.size(), child_offset);
            new_rec_size = rec.GetWholeSize();
          }
        }
        if (new_rec_size == old_rec_size) {
          if (new_value.data() == DBM::RecordProcessor::REMOVE.data()) {
            if (parent_offset > 0) {
              status = rec.WriteChildOffset(parent_offset, child_offset);
            } else {
              status = SetBucketValue(bucket_index, child_offset);
            }
            if (status != Status::SUCCESS) {
              return status;
            }
            status = rec.Write(current_offset, nullptr);
            if (status != Status::SUCCESS) {
              return status;
            }
            fbp_.InsertFreeBlock(current_offset, old_rec_size);
          } else {
            status = rec.Write(current_offset, nullptr);
            if (status != Status::SUCCESS) {
              return status;
            }
          }
        } else {
          const int64_t new_child_offset =
              (in_place || parent_offset == 0) ? child_offset : top;
          if (new_value.data() == DBM::RecordProcessor::REMOVE.data()) {
            rec.SetData(HashRecord::OP_REMOVE, 0, key.data(), key.size(), "", 0, new_child_offset);
            new_rec_size = rec.GetWholeSize();
          } else {
            rec.SetData(HashRecord::OP_SET, 0, key.data(), key.size(),
                        new_value.data(), new_value.size(), new_child_offset);
            new_rec_size = rec.GetWholeSize();
          }
          int64_t new_offset = 0;
          if (in_place) {
            FreeBlock fb;
            if (fbp_.FetchFreeBlock(new_rec_size, &fb)) {
              new_rec_size = fb.size;
              if (new_value.data() == DBM::RecordProcessor::REMOVE.data()) {
                rec.SetData(HashRecord::OP_REMOVE, new_rec_size, key.data(), key.size(),
                            "", 0, new_child_offset);
              } else {
                rec.SetData(HashRecord::OP_SET, new_rec_size, key.data(), key.size(),
                            new_value.data(), new_value.size(), new_child_offset);
              }
              new_offset = fb.offset;
            }
          }
          if (new_offset == 0) {
            status = rec.Write(-1, &new_offset);
          } else {
            status = rec.Write(new_offset, nullptr);
          }
          if (status != Status::SUCCESS) {
            return status;
          }
          if (in_place && parent_offset > 0) {
            status = rec.WriteChildOffset(parent_offset, new_offset);
          } else {
            status = SetBucketValue(bucket_index, new_offset);
          }
          if (status != Status::SUCCESS) {
            return status;
          }
          if (in_place) {
            rec.SetData(HashRecord::OP_VOID, old_rec_size, "", 0, "", 0, 0);
            status = rec.Write(current_offset, nullptr);
            if (status != Status::SUCCESS) {
              return status;
            }
            fbp_.InsertFreeBlock(current_offset, old_rec_size);
          }
        }
        if (new_value.data() == DBM::RecordProcessor::REMOVE.data()) {
          if (old_is_set) {
            num_records_.fetch_sub(1);
            eff_data_size_.fetch_sub(key.size() + old_value.size());
          }
        } else {
          if (old_is_set) {
            eff_data_size_.fetch_add(new_value.size() - old_value.size());
          } else {
            num_records_.fetch_add(1);
            eff_data_size_.fetch_add(key.size() + new_value.size());
          }
        }
      }
      return Status(Status::SUCCESS);
    } else {
      parent_offset = current_offset;
      current_offset = rec.GetChildOffset();
    }
  }
  const std::string_view new_value = proc->ProcessEmpty(key);
  if (new_value.data() != DBM::RecordProcessor::NOOP.data() &&
      new_value.data() != DBM::RecordProcessor::REMOVE.data() && writable) {
    rec.SetData(HashRecord::OP_SET, 0, key.data(), key.size(),
                new_value.data(), new_value.size(), top);
    int32_t new_rec_size = rec.GetWholeSize();
    int64_t new_offset = 0;
    if (in_place) {
      FreeBlock fb;
      if (fbp_.FetchFreeBlock(new_rec_size, &fb)) {
        new_rec_size = fb.size;
        rec.SetData(HashRecord::OP_SET, new_rec_size, key.data(), key.size(),
                    new_value.data(), new_value.size(), top);
        new_offset = fb.offset;
      }
    }
    if (new_offset == 0) {
      status = rec.Write(-1, &new_offset);
    } else {
      status = rec.Write(new_offset, nullptr);
    }
    if (status != Status::SUCCESS) {
      return status;
    }
    status = SetBucketValue(bucket_index, new_offset);
    if (status != Status::SUCCESS) {
      return status;
    }
    num_records_.fetch_add(1);
    eff_data_size_.fetch_add(key.size() + new_value.size());
  }
  return Status(Status::SUCCESS);
}

Status HashDBMImpl::GetBucketValue(int64_t bucket_index, int64_t* value) {
  char buf[sizeof(uint64_t)];
  const int64_t offset = METADATA_SIZE + bucket_index * offset_width_;
  Status status = file_->Read(offset, buf, offset_width_);
  if (status != Status::SUCCESS) {
    return status;
  }
  *value = ReadFixNum(buf, offset_width_) << align_pow_;
  return Status(Status::SUCCESS);
}

Status HashDBMImpl::SetBucketValue(int64_t bucket_index, int64_t value) {
  char buf[sizeof(uint64_t)];
  WriteFixNum(buf, value >> align_pow_, offset_width_);
  const int64_t offset = METADATA_SIZE + bucket_index * offset_width_;
  return file_->Write(offset, buf, offset_width_);
}

Status HashDBMImpl::ReadNextBucketRecords(HashDBMIteratorImpl* iter) {
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
    int64_t current_offset = 0;
    Status status = GetBucketValue(bucket_index, &current_offset);
    if (status != Status::SUCCESS) {
      return status;
    }
    if (current_offset != 0) {
      std::set<std::string> dead_keys;
      while (current_offset > 0) {
        HashRecord rec(file_.get(), offset_width_, align_pow_);
        status = rec.ReadMetadataKey(current_offset);
        if (status != Status::SUCCESS) {
          return status;
        }
        current_offset = rec.GetChildOffset();
        std::string key(rec.GetKey());
        if (CheckSet(dead_keys, key)) {
          continue;
        }
        switch (rec.GetOperationType()) {
          case HashRecord::OP_SET:
            iter->keys_.emplace(std::move(key));
            break;
          case HashRecord::OP_REMOVE:
            dead_keys.emplace(std::move(key));
            break;
          default:
            break;
        }
      }
      if (!iter->keys_.empty()) {
        return Status(Status::SUCCESS);
      }
    }
  }
  iter->bucket_index_.store(-1);
  return Status(Status::NOT_FOUND_ERROR);
}

HashDBMIteratorImpl::HashDBMIteratorImpl(HashDBMImpl* dbm)
    : dbm_(dbm), bucket_index_(-1), keys_() {
  std::lock_guard<std::shared_timed_mutex> lock(dbm_->mutex_);
  dbm_->iterators_.emplace_back(this);
}

HashDBMIteratorImpl::~HashDBMIteratorImpl() {
  if (dbm_ != nullptr) {
    std::lock_guard<std::shared_timed_mutex> lock(dbm_->mutex_);
    dbm_->iterators_.remove(this);
  }
}

Status HashDBMIteratorImpl::First() {
  std::shared_lock<std::shared_timed_mutex> lock(dbm_->mutex_);
  if (!dbm_->open_) {
    return Status(Status::PRECONDITION_ERROR, "not opened database");
  }
  bucket_index_.store(0);
  keys_.clear();
  return Status(Status::SUCCESS);
}

Status HashDBMIteratorImpl::Jump(std::string_view key) {
  std::shared_lock<std::shared_timed_mutex> lock(dbm_->mutex_);
  if (!dbm_->open_) {
    return Status(Status::PRECONDITION_ERROR, "not opened database");
  }
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
  auto it = keys_.find(std::string(key));
  if (it == keys_.end()) {
    bucket_index_.store(-1);
    keys_.clear();
    return Status(Status::NOT_FOUND_ERROR);
  }
  keys_.erase(keys_.begin(), it);
  return Status(Status::SUCCESS);
}

Status HashDBMIteratorImpl::Next() {
  std::shared_lock<std::shared_timed_mutex> lock(dbm_->mutex_);
  const Status status = ReadKeys();
  if (status != Status::SUCCESS) {
    return status;
  }
  keys_.erase(keys_.begin());
  return Status(Status::SUCCESS);
}

Status HashDBMIteratorImpl::Process(DBM::RecordProcessor* proc, bool writable) {
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
  {
    ScopedHashLock record_lock(dbm_->record_mutex_, first_key, writable);
    const int64_t bucket_index = record_lock.GetBucketIndex();
    const Status status = dbm_->ProcessImpl(first_key, bucket_index, &proc_wrapper, writable);
    if (status != Status::SUCCESS) {
      return status;
    }
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

Status HashDBMIteratorImpl::ReadKeys() {
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

HashDBM::HashDBM() {
  impl_ = new HashDBMImpl(std::make_unique<MemoryMapParallelFile>());
}

HashDBM::HashDBM(std::unique_ptr<File> file) {
  impl_ = new HashDBMImpl(std::move(file));
}

HashDBM::~HashDBM() {
  delete impl_;
}

Status HashDBM::OpenAdvanced(const std::string& path, bool writable,
                             int32_t options, const TuningParameters& tuning_params) {
  return impl_->Open(path, writable, options, tuning_params);
}

Status HashDBM::Close() {
  return impl_->Close();
}

Status HashDBM::Process(std::string_view key, RecordProcessor* proc, bool writable) {
  assert(proc != nullptr);
  return impl_->Process(key, proc, writable);
}

Status HashDBM::ProcessEach(RecordProcessor* proc, bool writable) {
  assert(proc != nullptr);
  return impl_->ProcessEach(proc, writable);
}

Status HashDBM::Count(int64_t* count) {
  assert(count != nullptr);
  return impl_->Count(count);
}

Status HashDBM::GetFileSize(int64_t* size) {
  assert(size != nullptr);
  return impl_->GetFileSize(size);
}

Status HashDBM::GetFilePath(std::string* path) {
  assert(path != nullptr);
  return impl_->GetFilePath(path);
}

Status HashDBM::Clear() {
  return impl_->Clear();
}

Status HashDBM::RebuildAdvanced(const TuningParameters& tuning_params, bool skip_broken_records) {
  return impl_->Rebuild(tuning_params, skip_broken_records);
}

Status HashDBM::ShouldBeRebuilt(bool* tobe) {
  assert(tobe != nullptr);
  return impl_->ShouldBeRebuilt(tobe);
}

Status HashDBM::Synchronize(bool hard, FileProcessor* proc) {
  return impl_->Synchronize(hard, proc);
}

std::vector<std::pair<std::string, std::string>> HashDBM::Inspect() {
  return impl_->Inspect();
}

bool HashDBM::IsOpen() const {
  return impl_->IsOpen();
}

bool HashDBM::IsWritable() const {
  return impl_->IsWritable();
}

bool HashDBM::IsHealthy() const {
  return impl_->IsHealthy();
}

std::unique_ptr<DBM::Iterator> HashDBM::MakeIterator() {
  std::unique_ptr<HashDBM::Iterator> iter(new HashDBM::Iterator(impl_));
  return iter;
}

std::unique_ptr<DBM> HashDBM::MakeDBM() const {
  return impl_->MakeDBM();
}

const File* HashDBM::GetInternalFile() const {
  return impl_->GetInternalFile();
}

int64_t HashDBM::GetEffectiveDataSize() {
  return impl_->GetEffectiveDataSize();
}

double HashDBM::GetModificationTime() {
  return impl_->GetModificationTime();
}

int32_t HashDBM::GetDatabaseType() {
  return impl_->GetDatabaseType();
}

Status HashDBM::SetDatabaseType(uint32_t db_type) {
  return impl_->SetDatabaseTypeMetadata(db_type);
}

std::string HashDBM::GetOpaqueMetadata() {
  return impl_->GetOpaqueMetadata();
}

Status HashDBM::SetOpaqueMetadata(const std::string& opaque) {
  return impl_->SetOpaqueMetadata(opaque);
}

int64_t HashDBM::CountBuckets() {
  return impl_->CountBuckets();
}

int64_t HashDBM::CountUsedBuckets() {
  return impl_->CountUsedBuckets();
}

HashDBM::UpdateMode HashDBM::GetUpdateMode() {
  return impl_->GetUpdateMode();
}

Status HashDBM::SetUpdateModeAppending() {
  return impl_->SetUpdateModeAppending();
}

Status HashDBM::ImportFromFileForward(
    const std::string& path, bool skip_broken_records, int64_t record_base, int64_t end_offset) {
  return impl_->ImportFromFileForward(path, skip_broken_records, record_base, end_offset);
}

Status HashDBM::ImportFromFileBackward(
    const std::string& path, bool skip_broken_records, int64_t record_base, int64_t end_offset) {
  return impl_->ImportFromFileBackward(path, skip_broken_records, record_base, end_offset);
}

HashDBM::Iterator::Iterator(HashDBMImpl* dbm_impl) {
  impl_ = new HashDBMIteratorImpl(dbm_impl);
}

HashDBM::Iterator::~Iterator() {
  delete impl_;
}

Status HashDBM::Iterator::First() {
  return impl_->First();
}

Status HashDBM::Iterator::Jump(std::string_view key) {
  return impl_->Jump(key);
}

Status HashDBM::Iterator::Next() {
  return impl_->Next();
}

Status HashDBM::Iterator::Process(RecordProcessor* proc, bool writable) {
  assert(proc != nullptr);
  return impl_->Process(proc, writable);
}

Status HashDBM::FindRecordBase(
    File* file, int64_t *record_base, int32_t* offset_width, int32_t* align_pow,
    int64_t* last_sync_size) {
  assert(file != nullptr &&
         record_base != nullptr && offset_width != nullptr && align_pow != nullptr &&
         last_sync_size != nullptr);
  *record_base = 0;
  char meta[METADATA_SIZE];
  Status status = file->Read(0, meta, METADATA_SIZE);
  if (status == Status::SUCCESS &&
      std::memcmp(meta, META_MAGIC_DATA, sizeof(META_MAGIC_DATA)) == 0) {
    *offset_width = ReadFixNum(meta + META_OFFSET_OFFSET_WIDTH, 1);
    *align_pow = ReadFixNum(meta + META_OFFSET_ALIGN_POW, 1);
    int64_t num_buckets = ReadFixNum(meta + META_OFFSET_NUM_BUCKETS, 8);
    if ((*offset_width >= MIN_OFFSET_WIDTH && *offset_width <= MAX_OFFSET_WIDTH) &&
        *align_pow <= MAX_ALIGN_POW && num_buckets <= MAX_NUM_BUCKETS) {
      const int32_t align = std::max(RECORD_BASE_ALIGN, 1 << *align_pow);
      *record_base = METADATA_SIZE + num_buckets * *offset_width +
          FBP_SECTION_SIZE + RECORD_BASE_HEADER_SIZE;
      const int32_t diff = *record_base % align;
      if (diff > 0) {
        *record_base += align - diff;
      }
    }
    *last_sync_size = ReadFixNum(meta + META_OFFSET_FILE_SIZE, 8);
  } else {
    *last_sync_size = 0;
  }
  if (*record_base == 0) {
    int64_t offset = RECORD_BASE_ALIGN;
    const int64_t file_size = file->GetSizeSimple();
    while (offset < file_size) {
      const int64_t magic_offset = offset - RECORD_BASE_HEADER_SIZE;
      char head[RECORD_BASE_HEADER_SIZE];
      status = file->Read(magic_offset, head, RECORD_BASE_HEADER_SIZE);
      if (status == Status::SUCCESS &&
          std::memcmp(head, RECORD_BASE_MAGIC_DATA, sizeof(RECORD_BASE_MAGIC_DATA)) == 0) {
        *record_base = offset;
        break;
      }
      offset += RECORD_BASE_ALIGN;
    }
  }
  if (*record_base < METADATA_SIZE + FBP_SECTION_SIZE + RECORD_BASE_HEADER_SIZE) {
    return Status(Status::BROKEN_DATA_ERROR, "missing record base");
  }
  const int64_t magic_offset = *record_base - RECORD_BASE_HEADER_SIZE;
  char head[RECORD_BASE_HEADER_SIZE];
  status = file->Read(magic_offset, head, RECORD_BASE_HEADER_SIZE);
  if (status != Status::SUCCESS) {
    return status;
  }
  if (std::memcmp(head, RECORD_BASE_MAGIC_DATA, sizeof(RECORD_BASE_MAGIC_DATA)) != 0) {
    return Status(Status::BROKEN_DATA_ERROR, "bad magic data");
  }
  *offset_width = ReadFixNum(head + RECHEAD_OFFSET_OFFSET_WIDTH, 1);
  *align_pow = ReadFixNum(head + RECHEAD_OFFSET_ALIGN_POW, 1);
  if (*offset_width < MIN_OFFSET_WIDTH || *offset_width > MAX_OFFSET_WIDTH) {
    return Status(Status::BROKEN_DATA_ERROR, "the offset width is invalid");
  }
  if (*align_pow > MAX_ALIGN_POW) {
    return Status(Status::BROKEN_DATA_ERROR, "the alignment power is invalid");
  }
  return Status(Status::SUCCESS);
}

Status HashDBM::RestoreDatabase(
    const std::string& old_file_path, const std::string& new_file_path, int64_t end_offset) {
  UpdateMode update_mode = UPDATE_DEFAULT;
  int64_t num_buckets = -1;
  int32_t db_type = 0;
  std::string opaque;
  {
    HashDBM old_dbm(std::make_unique<PositionalParallelFile>());
    if (old_dbm.Open(old_file_path, false) == Status::SUCCESS) {
      update_mode = old_dbm.GetUpdateMode();
      num_buckets = old_dbm.CountBuckets();
      db_type = old_dbm.GetDatabaseType();
      opaque = old_dbm.GetOpaqueMetadata();
    }
  }
  auto old_file = std::make_unique<PositionalParallelFile>();
  Status status = old_file->Open(old_file_path, false);
  if (status != Status::SUCCESS) {
    return status;
  }
  int64_t record_base = 0;
  int32_t offset_width = 0;
  int32_t align_pow = 0;
  int64_t last_sync_size = 0;
  status = FindRecordBase(
      old_file.get(), &record_base, &offset_width, &align_pow, &last_sync_size);
  if (status != Status::SUCCESS) {
    return status;
  }
  status = old_file->Close();
  if (status != Status::SUCCESS) {
    return status;
  }
  std::unique_ptr<File> new_file;
  if (last_sync_size <= UINT32MAX) {
    new_file = std::make_unique<MemoryMapParallelFile>();
  } else {
    new_file = std::make_unique<PositionalParallelFile>();
  }
  TuningParameters tuning_params;
  tuning_params.update_mode = update_mode;
  tuning_params.offset_width = offset_width;
  tuning_params.align_pow = align_pow;
  tuning_params.num_buckets = num_buckets;
  HashDBM new_dbm(std::move(new_file));
  status = new_dbm.OpenAdvanced(new_file_path, true, File::OPEN_DEFAULT, tuning_params);
  if (status != Status::SUCCESS) {
    return status;
  }
  if (new_dbm.CountSimple() > 0) {
    return Status(Status::PRECONDITION_ERROR, "the new database is not empty");
  }
  new_dbm.SetDatabaseType(db_type);
  new_dbm.SetOpaqueMetadata(opaque);
  if (update_mode == UPDATE_APPENDING) {
    status = new_dbm.ImportFromFileBackward(old_file_path, true, record_base, end_offset);
    if (status != Status::SUCCESS) {
      return status;
    }
  } else {
    status = new_dbm.ImportFromFileForward(old_file_path, true, record_base, end_offset);
    if (status != Status::SUCCESS) {
      return status;
    }
  }
  return new_dbm.Close();
}

}  // namespace tkrzw

// END OF FILE
