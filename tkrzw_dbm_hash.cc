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

#include "tkrzw_compress.h"
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
#include "tkrzw_time_util.h"

namespace tkrzw {

static constexpr int32_t METADATA_SIZE = 128;
static const char META_MAGIC_DATA[] = "TkrzwHDB\n";
static constexpr int32_t META_OFFSET_CYCLIC_MAGIC_FRONT = 9;
static constexpr int32_t META_OFFSET_PKG_MAJOR_VERSION = 10;
static constexpr int32_t META_OFFSET_PKG_MINOR_VERSION = 11;
static constexpr int32_t META_OFFSET_STATIC_FLAGS = 12;
static constexpr int32_t META_OFFSET_OFFSET_WIDTH = 13;
static constexpr int32_t META_OFFSET_ALIGN_POW = 14;
static constexpr int32_t META_OFFSET_CLOSURE_FLAGS = 15;
static constexpr int32_t META_OFFSET_NUM_BUCKETS = 16;
static constexpr int32_t META_OFFSET_NUM_RECORDS = 24;
static constexpr int32_t META_OFFSET_EFF_DATA_SIZE = 32;
static constexpr int32_t META_OFFSET_FILE_SIZE = 40;
static constexpr int32_t META_OFFSET_TIMESTAMP = 48;
static constexpr int32_t META_OFFSET_DB_TYPE = 56;
static constexpr int32_t META_OFFSET_OPAQUE = 62;
static constexpr int32_t META_OFFSET_CYCLIC_MAGIC_BACK = 127;
static constexpr int32_t FBP_SECTION_SIZE = 1008;
static constexpr int32_t RECORD_BASE_HEADER_SIZE = 16;
static const char RECORD_BASE_MAGIC_DATA[] = "TkrzwREC\n";
static constexpr int32_t RECHEAD_OFFSET_STATIC_FLAGS = 9;
static constexpr int32_t RECHEAD_OFFSET_OFFSET_WIDTH = 10;
static constexpr int32_t RECHEAD_OFFSET_ALIGN_POW = 11;
static constexpr int32_t RECORD_MUTEX_NUM_SLOTS = 256;
static constexpr int32_t RECORD_BASE_ALIGN = 4096;
static constexpr int32_t MIN_OFFSET_WIDTH = 3;
static constexpr int32_t MAX_OFFSET_WIDTH = 6;
static constexpr int32_t MAX_ALIGN_POW = 16;
static constexpr int64_t MAX_NUM_BUCKETS = 1099511627689LL;
static constexpr int32_t REBUILD_NONBLOCKING_MAX_TRIES = 3;
static constexpr int64_t REBUILD_BLOCKING_ALLOWANCE = 65536;
static constexpr int64_t MIN_DIO_BLOCK_SIZE = 512;

enum StaticFlag : uint8_t {
  STATIC_FLAG_NONE = 0,
  STATIC_FLAG_UPDATE_IN_PLACE = 1 << 0,
  STATIC_FLAG_UPDATE_APPENDING = 1 << 1,
  STATIC_FLAG_RECORD_CRC_8 = 1 << 2,
  STATIC_FLAG_RECORD_CRC_16 = 1 << 3,
  STATIC_FLAG_RECORD_CRC_32 = (1 << 2) | (1 << 3),
  STATIC_FLAG_RECORD_COMP_ZLIB = 1 << 4,
  STATIC_FLAG_RECORD_COMP_ZSTD = 1 << 5,
  STATIC_FLAG_RECORD_COMP_LZ4 = (1 << 4) | (1 << 5),
  STATIC_FLAG_RECORD_COMP_LZMA = 1 << 6,
  STATIC_FLAG_RECORD_COMP_RC4 = (1 << 6) | (1 << 4),
  STATIC_FLAG_RECORD_COMP_AES = (1 << 6) | (1 << 5),
  STATIC_FLAG_RECORD_COMP_EXTRA = (1 << 4) | (1 << 5) | (1 << 6),
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
      std::string_view key, DBM::RecordProcessor* proc, bool readable, bool writable);
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
      const HashDBM::TuningParameters& tuning_params, bool skip_broken_records, bool sync_hard);
  Status ShouldBeRebuilt(bool* tobe);
  Status Synchronize(bool hard, DBM::FileProcessor* proc);
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
  int64_t CountBuckets();
  int64_t CountUsedBuckets();
  HashDBM::UpdateMode GetUpdateMode();
  Status SetUpdateModeAppending();
  Status ImportFromFileForward(
      const std::string& path, bool skip_broken_records,
      int64_t record_base, int64_t end_offset);
  Status ImportFromFileForward(
      File* file, bool skip_broken_records,
      int64_t record_base, int64_t end_offset);
  Status ImportFromFileBackward(
      const std::string& path, bool skip_broken_records,
      int64_t record_base, int64_t end_offset);
  Status ImportFromFileBackward(
      File* file, bool skip_broken_records,
      int64_t record_base, int64_t end_offset);
  Status ValidateHashBuckets();
  Status ValidateRecords(int64_t record_base, int64_t end_offset);
  Status CheckZeroRegion(int64_t offset, int64_t end_offset);
  Status CheckHashChain(int64_t offset, const HashRecord& rec, int64_t bucket_index);

 private:
  void SetTuning(const HashDBM::TuningParameters& tuning_params);
  Status OpenImpl(bool writable);
  Status CloseImpl();
  void CancelIterators();
  Status SaveMetadata(bool finish);
  Status LoadMetadata();
  void SetRecordBase();
  Status CheckFileBeforeOpen(File* file, const std::string& path, bool writable);
  Status TuneFileAfterOpen();
  Status InitializeBuckets();
  Status SaveFBP();
  Status LoadFBP();
  Status ProcessImpl(
      std::string_view key, int64_t bucket_index, DBM::RecordProcessor* proc,
      bool readable, bool writable);
  Status GetBucketValue(int64_t bucket_index, int64_t* value);
  Status SetBucketValue(int64_t bucket_index, int64_t value);
  Status RebuildImpl(
      const HashDBM::TuningParameters& tuning_params, bool skip_broken_records, bool sync_hard);
  Status ReadNextBucketRecords(HashDBMIteratorImpl* iter);
  Status ImportFromFileForwardImpl(
      File* file, bool skip_broken_records,
      int64_t record_base, int64_t end_offset);
  Status ImportFromFileBackwardImpl(
      File* file, bool skip_broken_records,
      int64_t record_base, int64_t end_offset,
      const std::string& offset_path, const std::string& dead_path);
  Status ValidateHashBucketsImpl();
  Status ValidateRecordsImpl(int64_t record_base, int64_t end_offset,
                             int64_t* null_end_offset, int64_t* count, int64_t* eff_data_size);

  bool open_;
  bool writable_;
  bool healthy_;
  bool auto_restored_;
  std::string path_;
  int32_t cyclic_magic_;
  int32_t pkg_major_version_;
  int32_t pkg_minor_version_;
  int32_t static_flags_;
  int32_t crc_width_;
  std::unique_ptr<Compressor> compressor_;
  DBM::UpdateLogger* update_logger_;
  int32_t offset_width_;
  int32_t align_pow_;
  int32_t closure_flags_;
  int64_t num_buckets_;
  std::atomic_int64_t num_records_;
  std::atomic_int64_t eff_data_size_;
  int64_t file_size_;
  int64_t timestamp_;
  int32_t db_type_;
  std::string opaque_;
  int64_t record_base_;
  IteratorList iterators_;
  FreeBlockPool fbp_;
  int32_t min_read_size_;
  bool cache_buckets_;
  std::string cipher_key_;
  std::unique_ptr<File> file_;
  SpinSharedMutex mutex_;
  HashMutex<SpinSharedMutex> record_mutex_;
  SpinMutex rebuild_mutex_;
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
    : open_(false), writable_(false), healthy_(false), auto_restored_(false), path_(),
      cyclic_magic_(0), pkg_major_version_(0), pkg_minor_version_(0),
      static_flags_(STATIC_FLAG_NONE), crc_width_(0),
      compressor_(nullptr), update_logger_(nullptr),
      offset_width_(HashDBM::DEFAULT_OFFSET_WIDTH), align_pow_(HashDBM::DEFAULT_ALIGN_POW),
      closure_flags_(CLOSURE_FLAG_NONE),
      num_buckets_(HashDBM::DEFAULT_NUM_BUCKETS),
      num_records_(0), eff_data_size_(0), file_size_(0), timestamp_(0),
      db_type_(0), opaque_(),
      record_base_(0), iterators_(),
      fbp_(HashDBM::DEFAULT_FBP_CAPACITY), min_read_size_(0),
      cache_buckets_(false), cipher_key_(),
      file_(std::move(file)),
      mutex_(), record_mutex_(RECORD_MUTEX_NUM_SLOTS, 1, PrimaryHash),
      rebuild_mutex_() {}

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
  std::lock_guard<SpinSharedMutex> lock(mutex_);
  if (open_) {
    return Status(Status::PRECONDITION_ERROR, "opened database");
  }
  const std::string norm_path = NormalizePath(path);
  SetTuning(tuning_params);
  Status status = CheckFileBeforeOpen(file_.get(), path, writable);
  if (status != Status::SUCCESS) {
    return status;
  }
  status = file_->Open(norm_path, writable, options);
  if (status != Status::SUCCESS) {
    return status;
  }
  path_ = norm_path;
  status = OpenImpl(writable);
  if (status != Status::SUCCESS) {
    file_->Close();
    return status;
  }
  const int32_t restore_mode = tuning_params.restore_mode & 0xffff;
  const int32_t restore_options = tuning_params.restore_mode & 0xffff0000;
  const int64_t act_file_size = file_->GetSizeSimple();
  int64_t null_end_offset = 0;
  int64_t act_count = 0;
  int64_t act_eff_data_size = 0;
  auto_restored_ = false;
  if (writable && !healthy_ && restore_mode != HashDBM::RESTORE_READ_ONLY) {
    if (restore_mode == HashDBM::RESTORE_NOOP) {
      healthy_ = true;
      closure_flags_ |= CLOSURE_FLAG_CLOSE;
    } else if (!(restore_options & HashDBM::RESTORE_NO_SHORTCUTS) &&
               (static_flags_ & STATIC_FLAG_UPDATE_APPENDING) &&
               file_size_ == act_file_size &&
               ValidateHashBucketsImpl() == Status::SUCCESS) {
      healthy_ = true;
      closure_flags_ |= CLOSURE_FLAG_CLOSE;
    } else if (!(restore_options & HashDBM::RESTORE_NO_SHORTCUTS) &&
               restore_mode == HashDBM::RESTORE_DEFAULT &&
               (static_flags_ & STATIC_FLAG_UPDATE_IN_PLACE) &&
               file_size_ <= act_file_size &&
               ValidateHashBucketsImpl() == Status::SUCCESS &&
               ValidateRecordsImpl(record_base_, act_file_size,
                                   &null_end_offset, &act_count, &act_eff_data_size) ==
               Status::SUCCESS) {
      num_records_.store(act_count);
      eff_data_size_.store(act_eff_data_size);
      if (null_end_offset > 0) {
        status = file_->Truncate(null_end_offset);
        if (status != Status::SUCCESS) {
          file_->Close();
          return status;
        }
      }
      healthy_ = true;
      closure_flags_ |= CLOSURE_FLAG_CLOSE;
      auto_restored_ = true;
    } else if (!(restore_options & HashDBM::RESTORE_NO_SHORTCUTS) &&
               restore_mode == HashDBM::RESTORE_DEFAULT &&
               (static_flags_ & STATIC_FLAG_UPDATE_APPENDING) &&
               file_size_ <= act_file_size &&
               ValidateHashBucketsImpl() == Status::SUCCESS &&
               ValidateRecordsImpl(file_size_, act_file_size,
                                   &null_end_offset, &act_count, &act_eff_data_size) ==
               Status::SUCCESS) {
      num_records_.fetch_add(act_count);
      if (null_end_offset > 0) {
        status = file_->Truncate(null_end_offset);
        if (status != Status::SUCCESS) {
          file_->Close();
          return status;
        }
      }
      healthy_ = true;
      closure_flags_ |= CLOSURE_FLAG_CLOSE;
      auto_restored_ = true;
    } else {
      CloseImpl();
      file_->Close();
      const std::string tmp_path = path_ + ".tmp.restore";
      const int64_t end_offset = restore_mode == HashDBM::RESTORE_SYNC ? 0 : -1;
      RemoveFile(tmp_path);
      status = HashDBM::RestoreDatabase(path_, tmp_path, end_offset, cipher_key_);
      if (status != Status::SUCCESS) {
        RemoveFile(tmp_path);
        return status;
      }
      if (0 && restore_options & HashDBM::RESTORE_WITH_HARDSYNC) {
        status = SynchronizeFile(tmp_path);
        if (status != Status::SUCCESS) {
          RemoveFile(tmp_path);
          return status;
        }
      }
      status = RenameFile(tmp_path, path_);
      if (status != Status::SUCCESS) {
        RemoveFile(tmp_path);
        return status;
      }
      if (0 && restore_options & HashDBM::RESTORE_WITH_HARDSYNC) {
        status = SynchronizeFile(path_);
        if (status != Status::SUCCESS) {
          return status;
        }
      }
      SetTuning(tuning_params);
      status = CheckFileBeforeOpen(file_.get(), path_, writable);
      if (status != Status::SUCCESS) {
        return status;
      }
      status = file_->Open(path_, writable, options);
      if (status != Status::SUCCESS) {
        return status;
      }
      status = OpenImpl(writable);
      if (status != Status::SUCCESS) {
        file_->Close();
        return status;
      }
      auto_restored_ = true;
    }
  }
  return Status(Status::SUCCESS);
}

Status HashDBMImpl::Close() {
  std::lock_guard<SpinSharedMutex> lock(mutex_);
  if (!open_) {
    return Status(Status::PRECONDITION_ERROR, "not opened database");
  }
  CancelIterators();
  Status status(Status::SUCCESS);
  status |= CloseImpl();
  status |= file_->Close();
  path_.clear();
  return status;
}

Status HashDBMImpl::Process(
    std::string_view key, DBM::RecordProcessor* proc, bool readable, bool writable) {
  std::shared_lock<SpinSharedMutex> lock(mutex_);
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
  return ProcessImpl(key, bucket_index, proc, readable, writable);
}

Status HashDBMImpl::ProcessMulti(
    const std::vector<std::pair<std::string_view, DBM::RecordProcessor*>>& key_proc_pairs,
    bool writable) {
  std::shared_lock<SpinSharedMutex> lock(mutex_);
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
    const Status status =
        ProcessImpl(key_proc.first, bucket_index, key_proc.second, true, writable);
    if (status != Status::SUCCESS) {
      return status;
    }
  }
  return Status(Status::SUCCESS);
}

Status HashDBMImpl::ProcessFirst(DBM::RecordProcessor* proc, bool writable) {
  std::shared_lock<SpinSharedMutex> lock(mutex_);
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
  int64_t bucket_index = 0;
  while (bucket_index < num_buckets_) {
    ScopedHashLock record_lock(record_mutex_, bucket_index, writable);
    if (record_lock.GetBucketIndex() < 0) {
      bucket_index = 0;
      continue;
    }
    int64_t current_offset = 0;
    Status status = GetBucketValue(bucket_index, &current_offset);
    if (status != Status::SUCCESS) {
      return status;
    }
    if (current_offset != 0) {
      std::set<std::string> dead_keys;
      while (current_offset > 0) {
        HashRecord rec(file_.get(), crc_width_, offset_width_, align_pow_);
        status = rec.ReadMetadataKey(current_offset, min_read_size_);
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
          case HashRecord::OP_ADD:
            return ProcessImpl(key, bucket_index, proc, true, writable);
          case HashRecord::OP_REMOVE:
            dead_keys.emplace(std::move(key));
            break;
          default:
            break;
        }
      }
    }
    bucket_index++;
  }
  return Status(Status::NOT_FOUND_ERROR);
}

Status HashDBMImpl::ProcessEach(DBM::RecordProcessor* proc, bool writable) {
  std::shared_lock<SpinSharedMutex> lock(mutex_);
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
    HashRecord rec(file_.get(), crc_width_, offset_width_, align_pow_);
    ScopedStringView comp_data_placeholder;
    while (offset < end_offset) {
      Status status = rec.ReadMetadataKey(offset, min_read_size_);
      if (status != Status::SUCCESS) {
        return status;
      }
      const std::string_view key = rec.GetKey();
      if (rec.GetOperationType() == HashRecord::OP_SET ||
          rec.GetOperationType() == HashRecord::OP_ADD) {
        std::string_view value = rec.GetValue();
        if (value.data() == nullptr) {
          status = rec.ReadBody();
          if (status != Status::SUCCESS) {
            return status;
          }
          value = rec.GetValue();
        }
        std::string_view new_value_orig;
        const std::string_view new_value = CallRecordProcessFull(
            proc, key, value, &new_value_orig, compressor_.get(), &comp_data_placeholder);
        if (new_value.data() == nullptr) {
          return Status(Status::BROKEN_DATA_ERROR, "record processing failed");
        }
      }
      offset += rec.GetWholeSize();
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
      HashRecord rec(file_.get(), crc_width_, offset_width_, align_pow_);
      ScopedStringView comp_data_placeholder;
      status = rec.ReadMetadataKey(current_offset, min_read_size_);
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
        case HashRecord::OP_ADD:
          if (!writable) {
            std::string_view value = rec.GetValue();
            if (value.data() == nullptr) {
              status = rec.ReadBody();
              if (status != Status::SUCCESS) {
                return status;
              }
              value = rec.GetValue();
            }
            std::string_view new_value_orig;
            const std::string_view new_value = CallRecordProcessFull(
                proc, key, value, &new_value_orig, compressor_.get(), &comp_data_placeholder);
            if (new_value.data() == nullptr) {
              return Status(Status::BROKEN_DATA_ERROR, "record processing failed");
            }
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
        status = ProcessImpl(key, bucket_index, proc, true, true);
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
  std::shared_lock<SpinSharedMutex> lock(mutex_);
  if (!open_) {
    return Status(Status::PRECONDITION_ERROR, "not opened database");
  }
  *count = num_records_.load();
  return Status(Status::SUCCESS);
}

Status HashDBMImpl::GetFileSize(int64_t* size) {
  std::shared_lock<SpinSharedMutex> lock(mutex_);
  if (!open_) {
    return Status(Status::PRECONDITION_ERROR, "not opened database");
  }
  *size = file_->GetSizeSimple();
  return Status(Status::SUCCESS);
}

Status HashDBMImpl::GetFilePath(std::string* path) {
  std::shared_lock<SpinSharedMutex> lock(mutex_);
  if (!open_) {
    return Status(Status::PRECONDITION_ERROR, "not opened database");
  }
  *path = path_;
  return Status(Status::SUCCESS);
}

Status HashDBMImpl::GetTimestamp(double* timestamp) {
  std::shared_lock<SpinSharedMutex> lock(mutex_);
  if (!open_) {
    return Status(Status::PRECONDITION_ERROR, "not opened database");
  }
  *timestamp = timestamp_ / 1000000.0;
  return Status(Status::SUCCESS);
}

Status HashDBMImpl::Clear() {
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
  const uint32_t static_flags = static_flags_;
  const int32_t offset_width = offset_width_;
  const int32_t align_pow = align_pow_;
  const int64_t num_buckets = num_buckets_;
  const int32_t min_read_size = min_read_size_;
  const bool cache_buckets = cache_buckets_;
  const std::string cipher_key = cipher_key_;
  const uint32_t db_type = db_type_;
  const std::string opaque = opaque_;
  CancelIterators();
  Status status = CloseImpl();
  status |= file_->Truncate(0);
  static_flags_ = static_flags;
  offset_width_ = offset_width;
  align_pow_ = align_pow;
  num_buckets_ = num_buckets;
  min_read_size_ = min_read_size;
  cache_buckets_ = cache_buckets;
  cipher_key_ = cipher_key;
  db_type_ = db_type;
  opaque_ = opaque;
  status |= OpenImpl(true);
  return status;
}

Status HashDBMImpl::Rebuild(
    const HashDBM::TuningParameters& tuning_params, bool skip_broken_records, bool sync_hard) {
  std::shared_lock<SpinSharedMutex> lock(mutex_);
  if (!open_) {
    return Status(Status::PRECONDITION_ERROR, "not opened database");
  }
  if (!writable_) {
    return Status(Status::PRECONDITION_ERROR, "not writable database");
  }
  if (!rebuild_mutex_.try_lock()) {
    return Status(Status::SUCCESS);
  }
  const Status status = RebuildImpl(tuning_params, skip_broken_records, sync_hard);
  rebuild_mutex_.unlock();
  return status;
}

Status HashDBMImpl::ShouldBeRebuilt(bool* tobe) {
  std::shared_lock<SpinSharedMutex> lock(mutex_);
  if (!open_) {
    return Status(Status::PRECONDITION_ERROR, "not opened database");
  }
  if (writable_) {
    if (!rebuild_mutex_.try_lock()) {
      *tobe = false;
      return Status(Status::SUCCESS);
    }
    rebuild_mutex_.unlock();
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
  file_size_ = file_->GetSizeSimple();
  timestamp_ = GetWallTime() * 1000000;
  Status status(Status::SUCCESS);
  if (update_logger_ != nullptr) {
    status |= update_logger_->Synchronize(hard);
  }
  if (hard) {
    status |= file_->Synchronize(true);
    status |= SaveMetadata(true);
    status |= file_->Synchronize(true, 0, METADATA_SIZE);
  } else {
    status |= SaveMetadata(true);
    status |= file_->Synchronize(false);
  }
  if (proc != nullptr) {
    proc->Process(path_);
  }
  status |= SaveMetadata(false);
  return status;
}

std::vector<std::pair<std::string, std::string>> HashDBMImpl::Inspect() {
  std::shared_lock<SpinSharedMutex> lock(mutex_);
  std::vector<std::pair<std::string, std::string>> meta;
  auto Add = [&](const std::string& name, const std::string& value) {
    meta.emplace_back(std::make_pair(name, value));
  };
  Add("class", "HashDBM");
  if (open_) {
    Add("healthy", ToString(healthy_));
    Add("auto_restored", ToString(auto_restored_));
    Add("path", path_);
    Add("cyclic_magic", ToString(static_cast<uint8_t>(cyclic_magic_)));
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
    Add("timestamp", SPrintF("%.6f", timestamp_ / 1000000.0));
    Add("db_type", ToString(db_type_));
    Add("max_file_size", ToString(1LL << (offset_width_ * 8 + align_pow_)));
    Add("record_base", ToString(record_base_));
    const char* update_mode = "unknown";
    if (static_flags_ & STATIC_FLAG_UPDATE_IN_PLACE) {
      update_mode = "in-place";
    } else if (static_flags_ & STATIC_FLAG_UPDATE_APPENDING) {
      update_mode = "appending";
    }
    Add("update_mode", update_mode);
    const char* record_crc_mode = "none";
    if (crc_width_ == 1) {
      record_crc_mode = "crc-8";
    } else if (crc_width_ == 2) {
      record_crc_mode = "crc-16";
    } else if (crc_width_ == 4) {
      record_crc_mode = "crc-32";
    }
    Add("record_crc_mode", record_crc_mode);
    const char* record_comp_mode = "none";
    if (compressor_ != nullptr) {
      const auto& comp_type = compressor_->GetType();
      if (comp_type == typeid(ZLibCompressor)) {
        record_comp_mode = "zlib";
      } else if (comp_type == typeid(ZStdCompressor)) {
        record_comp_mode = "zstd";
      } else if (comp_type == typeid(LZ4Compressor)) {
        record_comp_mode = "lz4";
      } else if (comp_type == typeid(LZMACompressor)) {
        record_comp_mode = "lzma";
      } else if (comp_type == typeid(RC4Compressor)) {
        record_comp_mode = "rc4";
      } else if (comp_type == typeid(AESCompressor)) {
        record_comp_mode = "aes";
      } else {
        record_comp_mode = "unknown";
      }
    }
    Add("record_comp_mode", record_comp_mode);
  }
  return meta;
}

bool HashDBMImpl::IsOpen() {
  std::shared_lock<SpinSharedMutex> lock(mutex_);
  return open_;
}

bool HashDBMImpl::IsWritable() {
  std::shared_lock<SpinSharedMutex> lock(mutex_);
  return open_ && writable_;
}

bool HashDBMImpl::IsHealthy() {
  std::shared_lock<SpinSharedMutex> lock(mutex_);
  return open_ && healthy_;
}

bool HashDBMImpl::IsAutoRestored() {
  std::shared_lock<SpinSharedMutex> lock(mutex_);
  return open_ && auto_restored_;
}

std::unique_ptr<DBM> HashDBMImpl::MakeDBM() {
  std::shared_lock<SpinSharedMutex> lock(mutex_);
  return std::make_unique<HashDBM>(file_->MakeFile());
}

DBM::UpdateLogger* HashDBMImpl::GetUpdateLogger() {
  std::shared_lock<SpinSharedMutex> lock(mutex_);
  return update_logger_;
}

void HashDBMImpl::SetUpdateLogger(DBM::UpdateLogger* update_logger) {
  std::lock_guard<SpinSharedMutex> lock(mutex_);
  update_logger_ = update_logger;
}

File* HashDBMImpl::GetInternalFile() {
  return file_.get();
}

int64_t HashDBMImpl::GetEffectiveDataSize() {
  std::lock_guard<SpinSharedMutex> lock(mutex_);
  if (!open_) {
    return -1;
  }
  return eff_data_size_.load();
}

int32_t HashDBMImpl::GetDatabaseType() {
  std::shared_lock<SpinSharedMutex> lock(mutex_);
  if (!open_) {
    return -1;
  }
  return db_type_;
}

Status HashDBMImpl::SetDatabaseTypeMetadata(uint32_t db_type) {
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

std::string HashDBMImpl::GetOpaqueMetadata() {
  std::shared_lock<SpinSharedMutex> lock(mutex_);
  if (!open_) {
    return "";
  }
  return opaque_;
}

Status HashDBMImpl::SetOpaqueMetadata(const std::string& opaque) {
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

int64_t HashDBMImpl::CountBuckets() {
  std::shared_lock<SpinSharedMutex> lock(mutex_);
  if (!open_) {
    return -1;
  }
  return num_buckets_;
}

int64_t HashDBMImpl::CountUsedBuckets() {
  std::shared_lock<SpinSharedMutex> lock(mutex_);
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
  std::shared_lock<SpinSharedMutex> lock(mutex_);
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
  std::lock_guard<SpinSharedMutex> lock(mutex_);
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
  }
  auto file = file_->MakeFile();
  Status status = file->Open(path, false, File::OPEN_NO_LOCK);
  if (status != Status::SUCCESS) {
    return status;
  }
  status |= ImportFromFileForwardImpl(file.get(), skip_broken_records, record_base, end_offset);
  status |= file->Close();
  return status;
}

Status HashDBMImpl::ImportFromFileForward(
    File* file, bool skip_broken_records,
    int64_t record_base, int64_t end_offset) {
  {
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
  }
  return ImportFromFileForwardImpl(file, skip_broken_records, record_base, end_offset);
}

Status HashDBMImpl::ImportFromFileBackward(
    const std::string& path, bool skip_broken_records,
    int64_t record_base, int64_t end_offset) {
  std::string offset_path, dead_path;
  {
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
    offset_path = path_ + ".tmp.offset";
    dead_path = path_ + ".tmp.dead";
  }
  auto file = file_->MakeFile();
  Status status = file->Open(path, false, File::OPEN_NO_LOCK);
  if (status != Status::SUCCESS) {
    return status;
  }
  status = ImportFromFileBackwardImpl(
      file.get(), skip_broken_records, record_base, end_offset, offset_path, dead_path);
  status |= file->Close();
  return status;
}

Status HashDBMImpl::ImportFromFileBackward(
    File* file, bool skip_broken_records,
    int64_t record_base, int64_t end_offset) {
  std::string offset_path, dead_path;
  {
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
    offset_path = path_ + ".tmp.offset";
    dead_path = path_ + ".tmp.dead";
  }
  return ImportFromFileBackwardImpl(file, skip_broken_records, record_base, end_offset,
                                    offset_path, dead_path);
}

Status HashDBMImpl::ValidateHashBuckets() {
  std::lock_guard<SpinSharedMutex> lock(mutex_);
  if (!open_) {
    return Status(Status::PRECONDITION_ERROR, "not opened database");
  }
  return ValidateHashBucketsImpl();
}

Status HashDBMImpl::ValidateRecords(int64_t record_base, int64_t end_offset) {
  std::lock_guard<SpinSharedMutex> lock(mutex_);
  if (!open_) {
    return Status(Status::PRECONDITION_ERROR, "not opened database");
  }
  if (record_base < 0) {
    record_base = record_base_;
  } else if (record_base == 0) {
    record_base = file_size_;
  }
  if (end_offset < 0) {
    end_offset = INT64MAX;
  }  else if (end_offset == 0) {
    end_offset = file_size_;
  }
  const int64_t act_file_size = file_->GetSizeSimple();
  end_offset = std::min(end_offset, act_file_size);
  int64_t null_end_offset = 0;
  int64_t act_count = 0;
  int64_t act_eff_data_size = 0;
  const Status status = ValidateRecordsImpl(
      record_base, end_offset, &null_end_offset, &act_count, &act_eff_data_size);
  if (status != Status::SUCCESS) {
    return status;
  }
  if (record_base == record_base_ && end_offset == act_file_size) {
    if (act_count != num_records_.load()) {
      return Status(Status::BROKEN_DATA_ERROR,"inconsistent number of records");
    }
    if ((static_flags_ & STATIC_FLAG_UPDATE_IN_PLACE) &&
        act_eff_data_size != eff_data_size_.load()) {
      return Status(Status::BROKEN_DATA_ERROR,"inconsistent effective data size");
    }
  }
  return Status(Status::SUCCESS);
}

void HashDBMImpl::SetTuning(const HashDBM::TuningParameters& tuning_params) {
  if (tuning_params.update_mode == HashDBM::UPDATE_DEFAULT ||
      tuning_params.update_mode == HashDBM::UPDATE_IN_PLACE) {
    static_flags_ |= STATIC_FLAG_UPDATE_IN_PLACE;
  } else {
    static_flags_ |= STATIC_FLAG_UPDATE_APPENDING;
  }
  if (tuning_params.record_crc_mode == HashDBM::RECORD_CRC_8) {
    static_flags_ |= STATIC_FLAG_RECORD_CRC_8;
  } else if (tuning_params.record_crc_mode == HashDBM::RECORD_CRC_16) {
    static_flags_ |= STATIC_FLAG_RECORD_CRC_16;
  } else if (tuning_params.record_crc_mode == HashDBM::RECORD_CRC_32) {
    static_flags_ |= STATIC_FLAG_RECORD_CRC_32;
  }
  if (tuning_params.record_comp_mode == HashDBM::RECORD_COMP_ZLIB) {
    static_flags_ |= STATIC_FLAG_RECORD_COMP_ZLIB;
  } else if (tuning_params.record_comp_mode == HashDBM::RECORD_COMP_ZSTD) {
    static_flags_ |= STATIC_FLAG_RECORD_COMP_ZSTD;
  } else if (tuning_params.record_comp_mode == HashDBM::RECORD_COMP_LZ4) {
    static_flags_ |= STATIC_FLAG_RECORD_COMP_LZ4;
  } else if (tuning_params.record_comp_mode == HashDBM::RECORD_COMP_LZMA) {
    static_flags_ |= STATIC_FLAG_RECORD_COMP_LZMA;
  } else if (tuning_params.record_comp_mode == HashDBM::RECORD_COMP_RC4) {
    static_flags_ |= STATIC_FLAG_RECORD_COMP_RC4;
  } else if (tuning_params.record_comp_mode == HashDBM::RECORD_COMP_AES) {
    static_flags_ |= STATIC_FLAG_RECORD_COMP_AES;
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
  if (tuning_params.min_read_size > 0) {
    min_read_size_ = std::max(HashDBM::DEFAULT_MIN_READ_SIZE, tuning_params.min_read_size);
  } else {
    min_read_size_ = std::max(HashDBM::DEFAULT_MIN_READ_SIZE, 1 << align_pow_);
  }
  cache_buckets_ = tuning_params.cache_buckets > 0;
  cipher_key_ = tuning_params.cipher_key;
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
    timestamp_ = GetWallTime() * 1000000;
    status = SaveMetadata(true);
    if (status != Status::SUCCESS) {
      return status;
    }
  }
  Status status = LoadMetadata();
  if (status != Status::SUCCESS) {
    return status;
  }
  if (cyclic_magic_ < 0) {
    file_size_ = INT64MAX;
  }
  SetRecordBase();
  status = TuneFileAfterOpen();
  if (status != Status::SUCCESS) {
    return status;
  }
  bool healthy = closure_flags_ & CLOSURE_FLAG_CLOSE;
  const int64_t act_file_size = file_->GetSizeSimple();
  if (file_size_ != act_file_size) {
    if (file_size_ > act_file_size) {
      healthy = false;
    } else if (file_size_ > 0) {
      if (CheckZeroRegion(file_size_, act_file_size) == Status::SUCCESS) {
        status = writable ? file_->Truncate(file_size_) : file_->TruncateFakely(file_size_);
        if (status != Status::SUCCESS) {
          healthy = false;
        }
      } else {
        healthy = false;
      }
    }
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
    timestamp_ = GetWallTime() * 1000000;
    status |= SaveMetadata(true);
    status |= SaveFBP();
  }
  open_ = false;
  writable_ = false;
  healthy_ = false;
  cyclic_magic_ = 0;
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
  timestamp_ = 0;
  db_type_ = 0;
  opaque_.clear();
  record_base_ = 0;
  fbp_.Clear();
  min_read_size_ = 0;
  cache_buckets_ = false;
  cipher_key_.clear();
  return status;
}

void HashDBMImpl::CancelIterators() {
  for (auto* iterator : iterators_) {
    iterator->bucket_index_.store(-1);
  }
}

Status HashDBMImpl::SaveMetadata(bool finish) {
  cyclic_magic_ = cyclic_magic_ % 255 + 1;
  char meta[METADATA_SIZE];
  std::memset(meta, 0, METADATA_SIZE);
  std::memcpy(meta, META_MAGIC_DATA, sizeof(META_MAGIC_DATA) - 1);
  WriteFixNum(meta + META_OFFSET_CYCLIC_MAGIC_FRONT, cyclic_magic_, 1);
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
  WriteFixNum(meta + META_OFFSET_NUM_RECORDS, std::max<int64_t>(0, num_records_.load()), 8);
  WriteFixNum(meta + META_OFFSET_EFF_DATA_SIZE, std::max<int64_t>(0, eff_data_size_.load()), 8);
  WriteFixNum(meta + META_OFFSET_FILE_SIZE, file_size_, 8);
  WriteFixNum(meta + META_OFFSET_TIMESTAMP, timestamp_, 8);
  WriteFixNum(meta + META_OFFSET_DB_TYPE, db_type_, 4);
  const int32_t opaque_size = std::min<int32_t>(opaque_.size(), HashDBM::OPAQUE_METADATA_SIZE);
  std::memcpy(meta + META_OFFSET_OPAQUE, opaque_.data(), opaque_size);
  WriteFixNum(meta + META_OFFSET_CYCLIC_MAGIC_BACK, cyclic_magic_, 1);
  return file_->Write(0, meta, METADATA_SIZE);
}

Status HashDBMImpl::LoadMetadata() {
  int64_t num_records = 0;
  int64_t eff_data_size = 0;
  const Status status = HashDBM::ReadMetadata(
      file_.get(), &cyclic_magic_, &pkg_major_version_, &pkg_minor_version_,
      &static_flags_, &offset_width_, &align_pow_,
      &closure_flags_, &num_buckets_, &num_records,
      &eff_data_size, &file_size_, &timestamp_,
      &db_type_, &opaque_);
  if (status != Status::SUCCESS) {
    return status;
  }
  crc_width_ = HashDBM::GetCRCWidthFromStaticFlags(static_flags_);
  compressor_ = HashDBM::MakeCompressorFromStaticFlags(static_flags_, cipher_key_);
  num_records_.store(num_records);
  eff_data_size_.store(eff_data_size);
  if (pkg_major_version_ < 1 && pkg_minor_version_ < 1) {
    return Status(Status::BROKEN_DATA_ERROR, "invalid package version");
  }
  if (!(static_flags_ & STATIC_FLAG_UPDATE_IN_PLACE) &&
      !(static_flags_ & STATIC_FLAG_UPDATE_APPENDING)) {
    return Status(Status::BROKEN_DATA_ERROR, "invalid static flags");
  }
  if (compressor_ != nullptr && !compressor_->IsSupported()) {
    return Status(Status::NOT_IMPLEMENTED_ERROR, "unsupported compression");
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
  record_base_ = AlignNumber(record_base_, align);
}

Status HashDBMImpl::CheckFileBeforeOpen(File* file, const std::string& path, bool writable) {
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

Status HashDBMImpl::TuneFileAfterOpen() {
  Status status(Status::SUCCESS);
  if (cache_buckets_) {
    auto* pos_file = dynamic_cast<PositionalFile*>(file_.get());
    if (pos_file != nullptr) {
      status |= pos_file->SetHeadBuffer(record_base_);
    }
  }
  return status;
}

Status HashDBMImpl::InitializeBuckets() {
  int64_t offset = METADATA_SIZE;
  int64_t size = record_base_ - offset;
  Status status = file_->Truncate(record_base_);
  char buf[8192];
  std::memset(buf, 0, sizeof(buf));
  while (size > 0) {
    const int64_t write_size = std::min<int64_t>(size, sizeof(buf));
    const Status write_status = file_->Write(offset, buf, write_size);
    if (write_status != Status::SUCCESS) {
      status |= write_status;
      break;
    }
    offset += write_size;
    size -= write_size;
  }
  char head[RECORD_BASE_HEADER_SIZE];
  std::memset(head, 0, RECORD_BASE_HEADER_SIZE);
  std::memcpy(head, RECORD_BASE_MAGIC_DATA, sizeof(RECORD_BASE_MAGIC_DATA) - 1);
  WriteFixNum(head + RECHEAD_OFFSET_STATIC_FLAGS, static_flags_, 1);
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
    std::string_view key, int64_t bucket_index, DBM::RecordProcessor* proc,
    bool readable, bool writable) {
  const bool in_place = static_flags_ & STATIC_FLAG_UPDATE_IN_PLACE;
  int64_t top = 0;
  Status status = GetBucketValue(bucket_index, &top);
  if (status != Status::SUCCESS) {
    return status;
  }
  int64_t current_offset = top;
  int64_t parent_offset = 0;
  HashRecord rec(file_.get(), crc_width_, offset_width_, align_pow_);
  ScopedStringView comp_data_placeholder;
  while (current_offset > 0) {
    const int32_t min_read_size = readable ? HashRecord::META_MIN_READ_SIZE : min_read_size_;
    status = rec.ReadMetadataKey(current_offset, min_read_size);
    if (status != Status::SUCCESS) {
      return status;
    }
    const auto rec_key = rec.GetKey();
    if (key == rec_key) {
      std::string_view new_value, new_value_orig;
      const bool old_is_set = rec.GetOperationType() == HashRecord::OP_SET ||
          rec.GetOperationType() == HashRecord::OP_ADD;
      std::string_view old_value = rec.GetValue();
      if (old_is_set) {
        if (readable) {
          if (old_value.data() == nullptr) {
            status = rec.ReadBody();
            if (status != Status::SUCCESS) {
              return status;
            }
            old_value = rec.GetValue();
          }
        } else {
          old_value = std::string_view(nullptr, old_value.size());
        }
        new_value = CallRecordProcessFull(
            proc, key, old_value, &new_value_orig, compressor_.get(), &comp_data_placeholder);
      } else {
        new_value = CallRecordProcessEmpty(
            proc, key, &new_value_orig, compressor_.get(), &comp_data_placeholder);
      }
      if (new_value.data() == nullptr) {
        return Status(Status::BROKEN_DATA_ERROR, "record processing failed");
      }
      if (new_value.data() != DBM::RecordProcessor::NOOP.data() && writable) {
        if (update_logger_ != nullptr) {
          if (new_value.data() == DBM::RecordProcessor::REMOVE.data()) {
            status = update_logger_->WriteRemove(key);
          } else {
            status = update_logger_->WriteSet(key, new_value_orig);
          }
          if (status != Status::SUCCESS) {
            return status;
          }
        }
        const int64_t child_offset = rec.GetChildOffset();
        const int32_t old_rec_size = rec.GetWholeSize();
        int32_t new_rec_size = 0;
        if (in_place) {
          if (new_value.data() == DBM::RecordProcessor::REMOVE.data()) {
            rec.SetData(HashRecord::OP_VOID, old_rec_size, "", 0, "", 0, 0);
            new_rec_size = rec.GetWholeSize();
          } else {
            rec.SetData(old_is_set ? HashRecord::OP_SET : HashRecord::OP_ADD,
                        old_rec_size, key.data(), key.size(),
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
            rec.SetData(old_is_set ? HashRecord::OP_SET : HashRecord::OP_ADD,
                        0, key.data(), key.size(),
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
            eff_data_size_.fetch_add(
                static_cast<int32_t>(new_value.size()) - static_cast<int32_t>(old_value.size()));
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
  std::string_view new_value_orig;
  const std::string_view new_value = CallRecordProcessEmpty(
      proc, key, &new_value_orig, compressor_.get(), &comp_data_placeholder);
  if (new_value.data() == nullptr) {
    return Status(Status::BROKEN_DATA_ERROR, "record processing failed");
  }
  if (new_value.data() != DBM::RecordProcessor::NOOP.data() &&
      new_value.data() != DBM::RecordProcessor::REMOVE.data() && writable) {
    if (update_logger_ != nullptr) {
      status = update_logger_->WriteSet(key, new_value_orig);
      if (status != Status::SUCCESS) {
        return status;
      }
    }
    rec.SetData(HashRecord::OP_ADD, 0, key.data(), key.size(),
                new_value.data(), new_value.size(), top);
    int32_t new_rec_size = rec.GetWholeSize();
    int64_t new_offset = 0;
    if (in_place) {
      FreeBlock fb;
      if (fbp_.FetchFreeBlock(new_rec_size, &fb)) {
        new_rec_size = fb.size;
        rec.SetData(HashRecord::OP_ADD, new_rec_size, key.data(), key.size(),
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

Status HashDBMImpl::RebuildImpl(
    const HashDBM::TuningParameters& tuning_params, bool skip_broken_records, bool sync_hard) {
  const bool in_place = static_flags_ & STATIC_FLAG_UPDATE_IN_PLACE;
  const std::string tmp_path = path_ + ".tmp.rebuild";
  int64_t est_num_records = num_records_.load();
  if (est_num_records < HashDBM::DEFAULT_NUM_BUCKETS) {
    const int64_t record_section_size = file_->GetSizeSimple() - record_base_;
    const int64_t est_avg_record_size = 512;
    est_num_records = std::max(est_num_records, record_section_size / est_avg_record_size);
    est_num_records = std::min(est_num_records, HashDBM::DEFAULT_NUM_BUCKETS);
  }
  HashDBM::TuningParameters tmp_tuning_params;
  tmp_tuning_params.update_mode = HashDBM::UPDATE_IN_PLACE;
  if (tuning_params.record_crc_mode != HashDBM::RECORD_CRC_DEFAULT) {
    tmp_tuning_params.record_crc_mode = tuning_params.record_crc_mode;
  } else if (crc_width_ == 1) {
    tmp_tuning_params.record_crc_mode = HashDBM::RECORD_CRC_8;
  } else if (crc_width_ == 2) {
    tmp_tuning_params.record_crc_mode = HashDBM::RECORD_CRC_16;
  } else if (crc_width_ == 4) {
    tmp_tuning_params.record_crc_mode = HashDBM::RECORD_CRC_32;
  }
  if (tuning_params.record_comp_mode != HashDBM::RECORD_COMP_DEFAULT) {
    tmp_tuning_params.record_comp_mode = tuning_params.record_comp_mode;
  } else if (compressor_ != nullptr) {
    const auto& comp_type = compressor_->GetType();
    if (comp_type == typeid(ZLibCompressor)) {
      tmp_tuning_params.record_comp_mode = HashDBM::RECORD_COMP_ZLIB;
    } else if (comp_type == typeid(ZStdCompressor)) {
      tmp_tuning_params.record_comp_mode = HashDBM::RECORD_COMP_ZSTD;
    } else if (comp_type == typeid(LZ4Compressor)) {
      tmp_tuning_params.record_comp_mode = HashDBM::RECORD_COMP_LZ4;
    } else if (comp_type == typeid(LZMACompressor)) {
      tmp_tuning_params.record_comp_mode = HashDBM::RECORD_COMP_LZMA;
    } else if (comp_type == typeid(RC4Compressor)) {
      tmp_tuning_params.record_comp_mode = HashDBM::RECORD_COMP_RC4;
    } else if (comp_type == typeid(AESCompressor)) {
      tmp_tuning_params.record_comp_mode = HashDBM::RECORD_COMP_AES;
    }
  }
  tmp_tuning_params.offset_width = tuning_params.offset_width > 0 ?
      tuning_params.offset_width : offset_width_;
  tmp_tuning_params.align_pow = tuning_params.align_pow >= 0 ?
      tuning_params.align_pow : align_pow_;
  tmp_tuning_params.num_buckets = tuning_params.num_buckets >= 0 ?
      tuning_params.num_buckets : est_num_records * 2 + 1;
  tmp_tuning_params.fbp_capacity = HashDBM::DEFAULT_FBP_CAPACITY;
  tmp_tuning_params.min_read_size = tuning_params.min_read_size >= 0 ?
      tuning_params.min_read_size : min_read_size_;
  tmp_tuning_params.cache_buckets = tuning_params.cache_buckets >= 0 ?
      tuning_params.cache_buckets : cache_buckets_;
  tmp_tuning_params.cipher_key = tuning_params.cipher_key.empty() ?
      cipher_key_ : tuning_params.cipher_key;
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
    status = tmp_dbm.ImportFromFileForward(file_.get(), skip_broken_records, -1, end_offset);
  } else {
    status = tmp_dbm.ImportFromFileBackward(file_.get(), skip_broken_records, -1, end_offset);
  }
  if (sync_hard) {
    status |= tmp_dbm.Synchronize(true);
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
    status = tmp_dbm.ImportFromFileForward(file_.get(), false, begin_offset, end_offset);
    if (sync_hard) {
      status |= tmp_dbm.Synchronize(true);
    }
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
      status = tmp_dbm.ImportFromFileForward(file_.get(), false, begin_offset, end_offset);
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
    file_->CopyProperties(tmp_file.get());
    status = tmp_file->Open(tmp_path, true);
    if (status != Status::SUCCESS) {
      CleanUp();
      return status;
    }
    fbp_.Clear();
    if (sync_hard) {
      status |= tmp_file->Synchronize(true);
    }
    status |= file_->DisablePathOperations();
    if (IS_POSIX) {
      status |= tmp_file->Rename(path_);
      status |= file_->Close();
    } else {
      status |= file_->Close();
      status |= tmp_file->Rename(path_);
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
    file_ = std::move(tmp_file);
    if (tuning_params.fbp_capacity >= 0) {
      fbp_.SetCapacity(tuning_params.fbp_capacity);
    }
    if (tuning_params.min_read_size >= 0) {
      if (tuning_params.min_read_size > 0) {
        min_read_size_ = std::max(HashDBM::DEFAULT_MIN_READ_SIZE, tuning_params.min_read_size);
      } else {
        min_read_size_ = std::max(HashDBM::DEFAULT_MIN_READ_SIZE, 1 << align_pow_);
      }
    }
    if (tuning_params.cache_buckets >= 0) {
      cache_buckets_ = tuning_params.cache_buckets > 0;
    }
    if (!tuning_params.cipher_key.empty()) {
      cipher_key_ = tuning_params.cipher_key;
    }
    status |= OpenImpl(true);
    db_type_ = db_type;
    opaque_ = opaque;
  }
  return status;
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
        HashRecord rec(file_.get(), crc_width_, offset_width_, align_pow_);
        status = rec.ReadMetadataKey(current_offset, min_read_size_);
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
          case HashRecord::OP_ADD:
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

Status HashDBMImpl::ImportFromFileForwardImpl(
    File* file, bool skip_broken_records,
    int64_t record_base, int64_t end_offset) {
  int64_t tmp_record_base = 0;
  int32_t static_flags = 0;
  int32_t offset_width = 0;
  int32_t align_pow = 0;
  int64_t last_sync_size = 0;
  Status status = HashDBM::FindRecordBase(
      file, &tmp_record_base, &static_flags, &offset_width, &align_pow, &last_sync_size);
  if (status != Status::SUCCESS) {
    return status;
  }
  if (record_base < 0) {
    record_base = tmp_record_base;
  }
  const int32_t crc_width = HashDBM::GetCRCWidthFromStaticFlags(static_flags);
  auto compressor = HashDBM::MakeCompressorFromStaticFlags(static_flags, cipher_key_);
  if (end_offset == 0) {
    if (last_sync_size < record_base || last_sync_size % (1 << align_pow) != 0) {
      end_offset = -1;
    } else {
      end_offset = last_sync_size;
    }
  }
  Status import_status(Status::SUCCESS);
  class Importer final : public DBM::RecordProcessor {
   public:
    Importer(HashDBMImpl* impl, Status* status) : impl_(impl), status_(status) {}
    std::string_view ProcessFull(std::string_view key, std::string_view value) override {
      Status set_status(Status::SUCCESS);
      DBM::RecordProcessorSet setter(&set_status, value, true, nullptr);
      *status_ = impl_->Process(key, &setter, false, true);
      *status_ |= set_status;
      return NOOP;
    }
    std::string_view ProcessEmpty(std::string_view key) override {
      Status remove_status(Status::SUCCESS);
      DBM::RecordProcessorRemove remover(&remove_status, nullptr);
      *status_ = impl_->Process(key, &remover, false, true);
      *status_ |= remove_status;
      return NOOP;
    }
   private:
    HashDBMImpl* impl_;
    Status* status_;
  } importer(this, &import_status);
  return HashRecord::ReplayOperations(
      file, &importer, record_base, crc_width, compressor.get(),
      offset_width, align_pow, min_read_size_, skip_broken_records, end_offset);
}

Status HashDBMImpl::ImportFromFileBackwardImpl(
    File* file, bool skip_broken_records,
    int64_t record_base, int64_t end_offset,
    const std::string& offset_path, const std::string& dead_path) {
  int64_t tmp_record_base = 0;
  int32_t static_flags = 0;
  int32_t offset_width = 0;
  int32_t align_pow = 0;
  int64_t last_sync_size = 0;
  Status status = HashDBM::FindRecordBase(
      file, &tmp_record_base, &static_flags, &offset_width, &align_pow, &last_sync_size);
  if (status != Status::SUCCESS) {
    return status;
  }
  if (record_base < 0) {
    record_base = tmp_record_base;
  }
  const int32_t crc_width = HashDBM::GetCRCWidthFromStaticFlags(static_flags);
  auto compressor = HashDBM::MakeCompressorFromStaticFlags(static_flags, cipher_key_);
  if (end_offset == 0) {
    if (last_sync_size < record_base || last_sync_size % (1 << align_pow) != 0) {
      end_offset = -1;
    } else {
      end_offset = last_sync_size;
    }
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
      file, offset_file.get(), record_base, crc_width, offset_width, align_pow,
      skip_broken_records, end_offset);
  if (status != Status::SUCCESS) {
    CleanUp();
    return status;
  }
  if (end_offset < 0) {
    end_offset = INT64MAX;
  }
  end_offset = std::min(end_offset, file->GetSizeSimple());
  const int64_t num_offsets = offset_file->GetSizeSimple() / offset_width_;
  const int64_t dead_num_buckets = std::min(num_offsets, num_buckets_);
  HashDBM::TuningParameters dead_tuning_params;
  dead_tuning_params.offset_width = offset_width;
  dead_tuning_params.align_pow = 0;
  dead_tuning_params.num_buckets = dead_num_buckets;
  status = dead_dbm.OpenAdvanced(dead_path, true, File::OPEN_TRUNCATE, dead_tuning_params);
  OffsetReader reader(offset_file.get(), offset_width, align_pow, true);
  HashRecord rec(file, crc_width, offset_width, align_pow);
  ScopedStringView comp_data_placeholder;
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
    status = rec.ReadMetadataKey(offset, min_read_size_);
    if (status == Status::SUCCESS) {
      status = rec.CheckCRC();
    }
    if (status != Status::SUCCESS) {
      if (skip_broken_records) {
        continue;
      }
      CleanUp();
      return status;
    }
    std::string_view key = rec.GetKey();
    switch (rec.GetOperationType()) {
      case HashRecord::OP_SET:
      case HashRecord::OP_ADD: {
        std::string_view value = rec.GetValue();
        if (value.data() == nullptr) {
          status = rec.ReadBody();
          if (status != Status::SUCCESS) {
            if (skip_broken_records) {
              continue;
            }
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
          if (compressor != nullptr) {
            size_t decomp_size = 0;
            char* decomp_buf = compressor->Decompress(value.data(), value.size(), &decomp_size);
            if (decomp_buf == nullptr) {
              if (skip_broken_records) {
                continue;
              } else {
                return Status(Status::BROKEN_DATA_ERROR, "decompression failed");
              }
            }
            comp_data_placeholder.Set(decomp_buf, decomp_size);
            value = comp_data_placeholder.Get();
          }
          Status set_status(Status::SUCCESS);
          DBM::RecordProcessorSet setter(&set_status, value, false, nullptr);
          status = Process(key, &setter, false, true);
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
        status = Process(key, &getter, false, false);
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

Status HashDBMImpl::ValidateHashBucketsImpl() {
  Status status(Status::SUCCESS);
  const int64_t batch_capacity = std::min<int64_t>(num_buckets_, 32 * 1024 * 1024);
  struct Bucket {
    int64_t index;
    int64_t offset;
  };
  Bucket* batch = new Bucket[batch_capacity];
  int64_t batch_size = 0;
  auto ProcessBatch =
      [&]() {
        std::sort(batch, batch + batch_size,
                  [](const Bucket& a, const Bucket& b) {
                    return a.offset < b.offset;
                  });
        HashRecord rec(file_.get(), crc_width_, offset_width_, align_pow_);
        for (int64_t i = 0; i < batch_size; i++) {
          const auto& bucket = batch[i];
          status |= rec.ReadMetadataKey(bucket.offset, min_read_size_);
          if (status != Status::SUCCESS) {
            break;
          }
          status = rec.CheckCRC();
          if (status != Status::SUCCESS) {
            break;
          }
          if (PrimaryHash(rec.GetKey(), num_buckets_) != static_cast<uint64_t>(bucket.index)) {
            status |= Status(Status::BROKEN_DATA_ERROR, "inconsistent hash value");
            break;
          }
        }
        batch_size = 0;
      };
  int64_t bucket_index = 0;
  while (bucket_index < num_buckets_) {
    int64_t offset = 0;
    status |= GetBucketValue(bucket_index, &offset);
    if (status != Status::SUCCESS) {
      break;
    }
    if (offset != 0) {
      batch[batch_size] = {bucket_index, offset};
      batch_size++;
      if (batch_size >= batch_capacity) {
        ProcessBatch();
        if (status != Status::SUCCESS) {
          break;
        }
      }
    }
    bucket_index++;
  }
  if (batch_size > 0 && status == Status::SUCCESS) {
    ProcessBatch();
  }
  delete[] batch;
  return status;
}

Status HashDBMImpl::ValidateRecordsImpl(
    int64_t record_base, int64_t end_offset,
    int64_t* null_end_offset, int64_t* act_count, int64_t* act_eff_data_size) {
  const bool in_place = static_flags_ & STATIC_FLAG_UPDATE_IN_PLACE;
  *null_end_offset = -1;
  int64_t offset = record_base;
  HashRecord rec(file_.get(), crc_width_, offset_width_, align_pow_);
  HashRecord child_rec(file_.get(), crc_width_, offset_width_, align_pow_);
  while (offset < end_offset) {
    Status status = rec.ReadMetadataKey(offset, min_read_size_);
    if (status != Status::SUCCESS) {
      if (CheckZeroRegion(offset, end_offset) == Status::SUCCESS) {
        *null_end_offset = offset;
        return Status(Status::SUCCESS);
      }
      return status;
    }
    status = rec.CheckCRC();
    if (status != Status::SUCCESS) {
      if (!in_place &&
          CheckZeroRegion(offset + rec.GetWholeSize(), end_offset) == Status::SUCCESS) {
        *null_end_offset = offset;
        return Status(Status::SUCCESS);
      }
      return status;
    }
    const int64_t bucket_index = PrimaryHash(rec.GetKey(), num_buckets_);
    if (rec.GetOperationType() != HashRecord::OP_VOID) {
      status = CheckHashChain(offset, rec, bucket_index);
      if (status != Status::SUCCESS) {
        return status;
      }
      const int64_t child_offset = rec.GetChildOffset();
      if (child_offset > 0) {
        status = child_rec.ReadMetadataKey(child_offset, HashDBM::DEFAULT_MIN_READ_SIZE);
        if (status != Status::SUCCESS) {
          return status;
        }
        const int64_t child_bucket_index = PrimaryHash(child_rec.GetKey(), num_buckets_);
        if (child_bucket_index != bucket_index) {
          return Status(Status::BROKEN_DATA_ERROR, "inconsistent bucket index");
        }
      }
      switch (rec.GetOperationType()) {
        case HashRecord::OP_SET:
          if (in_place) {
            *(act_count) += 1;
            *(act_eff_data_size) += rec.GetKey().size() + rec.GetValue().size();
          }
          break;
        case HashRecord::OP_ADD:
          *(act_count) += 1;
          *(act_eff_data_size) += rec.GetKey().size() + rec.GetValue().size();
          break;
        case HashRecord::OP_REMOVE:
          *(act_count) -= 1;
          *(act_eff_data_size) -= rec.GetKey().size() + rec.GetValue().size();
          break;
        default:
          break;
      }
    }
    offset += rec.GetWholeSize();
  }
  if (offset != end_offset) {
    return Status(Status::BROKEN_DATA_ERROR, "inconsistent end offset");
  }
  return Status(Status::SUCCESS);
}

Status HashDBMImpl::CheckZeroRegion(int64_t offset, int64_t end_offset) {
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

Status HashDBMImpl::CheckHashChain(int64_t offset, const HashRecord& rec, int64_t bucket_index) {
  const auto key = rec.GetKey();
  ScopedHashLock record_lock(record_mutex_, key, false);
  const bool appending = static_flags_ & STATIC_FLAG_UPDATE_APPENDING;
  int64_t chain_offset = 0;
  Status status = GetBucketValue(bucket_index, &chain_offset);
  HashRecord chain_rec(file_.get(), crc_width_, offset_width_, align_pow_);
  while (chain_offset > 0) {
    if (chain_offset == offset) {
      return Status(Status::SUCCESS);
    }
    status = chain_rec.ReadMetadataKey(chain_offset, HashRecord::META_MIN_READ_SIZE);
    if (status != Status::SUCCESS) {
      return status;
    }
    const auto chain_key = chain_rec.GetKey();
    const int64_t chain_bucket_index = PrimaryHash(chain_key, num_buckets_);
    if (chain_bucket_index != bucket_index) {
      return Status(Status::BROKEN_DATA_ERROR, "inconsistent bucket index");
    }
    if (appending && chain_offset > offset && chain_key == key) {
      return Status(Status::SUCCESS);
    }
    chain_offset = chain_rec.GetChildOffset();
  }
  return Status(Status::NOT_FOUND_ERROR);
}

HashDBMIteratorImpl::HashDBMIteratorImpl(HashDBMImpl* dbm)
    : dbm_(dbm), bucket_index_(-1), keys_() {
  std::lock_guard<SpinSharedMutex> lock(dbm_->mutex_);
  dbm_->iterators_.emplace_back(this);
}

HashDBMIteratorImpl::~HashDBMIteratorImpl() {
  if (dbm_ != nullptr) {
    std::lock_guard<SpinSharedMutex> lock(dbm_->mutex_);
    dbm_->iterators_.remove(this);
  }
}

Status HashDBMIteratorImpl::First() {
  std::shared_lock<SpinSharedMutex> lock(dbm_->mutex_);
  if (!dbm_->open_) {
    return Status(Status::PRECONDITION_ERROR, "not opened database");
  }
  bucket_index_.store(0);
  keys_.clear();
  return Status(Status::SUCCESS);
}

Status HashDBMIteratorImpl::Jump(std::string_view key) {
  std::shared_lock<SpinSharedMutex> lock(dbm_->mutex_);
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
  std::shared_lock<SpinSharedMutex> lock(dbm_->mutex_);
  const Status status = ReadKeys();
  if (status != Status::SUCCESS) {
    return status;
  }
  keys_.erase(keys_.begin());
  return Status(Status::SUCCESS);
}

Status HashDBMIteratorImpl::Process(DBM::RecordProcessor* proc, bool writable) {
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
    const Status status =
        dbm_->ProcessImpl(first_key, bucket_index, &proc_wrapper, true, writable);
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
  return impl_->Process(key, proc, true, writable);
}

Status HashDBM::Get(std::string_view key, std::string* value) {
  Status impl_status(Status::SUCCESS);
  RecordProcessorGet proc(&impl_status, value);
  const Status status = impl_->Process(key, &proc, value != nullptr, false);
  if (status != Status::SUCCESS) {
    return status;
  }
  return impl_status;
}

Status HashDBM::Set(std::string_view key, std::string_view value, bool overwrite,
                    std::string* old_value) {
  Status impl_status(Status::SUCCESS);
  RecordProcessorSet proc(&impl_status, value, overwrite, old_value);
  const Status status = impl_->Process(key, &proc, old_value != nullptr, true);
  if (status != Status::SUCCESS) {
      return status;
  }
  return impl_status;
}

Status HashDBM::Remove(std::string_view key, std::string* old_value) {
  Status impl_status(Status::SUCCESS);
  RecordProcessorRemove proc(&impl_status, old_value);
  const Status status = impl_->Process(key, &proc, old_value != nullptr, true);
  if (status != Status::SUCCESS) {
    return status;
  }
  return impl_status;
}

Status HashDBM::ProcessMulti(
    const std::vector<std::pair<std::string_view, RecordProcessor*>>& key_proc_pairs,
    bool writable) {
  return impl_->ProcessMulti(key_proc_pairs, writable);
}

Status HashDBM::ProcessFirst(RecordProcessor* proc, bool writable) {
  assert(proc != nullptr);
  return impl_->ProcessFirst(proc, writable);
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

Status HashDBM::GetTimestamp(double* timestamp) {
  assert(timestamp != nullptr);
  return impl_->GetTimestamp(timestamp);
}

Status HashDBM::Clear() {
  return impl_->Clear();
}

Status HashDBM::RebuildAdvanced(
    const TuningParameters& tuning_params, bool skip_broken_records, bool sync_hard) {
  return impl_->Rebuild(tuning_params, skip_broken_records, sync_hard);
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

bool HashDBM::IsAutoRestored() const {
  return impl_->IsAutoRestored();
}

std::unique_ptr<DBM::Iterator> HashDBM::MakeIterator() {
  std::unique_ptr<HashDBM::Iterator> iter(new HashDBM::Iterator(impl_));
  return iter;
}

std::unique_ptr<DBM> HashDBM::MakeDBM() const {
  return impl_->MakeDBM();
}

DBM::UpdateLogger* HashDBM::GetUpdateLogger() const {
  return impl_->GetUpdateLogger();
}

void HashDBM::SetUpdateLogger(UpdateLogger* update_logger) {
  impl_->SetUpdateLogger(update_logger);
}

File* HashDBM::GetInternalFile() const {
  return impl_->GetInternalFile();
}

int64_t HashDBM::GetEffectiveDataSize() {
  return impl_->GetEffectiveDataSize();
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

Status HashDBM::ImportFromFileForward(
    File* file, bool skip_broken_records, int64_t record_base, int64_t end_offset) {
  assert(file != nullptr);
  return impl_->ImportFromFileForward(file, skip_broken_records, record_base, end_offset);
}

Status HashDBM::ImportFromFileBackward(
    const std::string& path, bool skip_broken_records, int64_t record_base, int64_t end_offset) {
  return impl_->ImportFromFileBackward(path, skip_broken_records, record_base, end_offset);
}

Status HashDBM::ImportFromFileBackward(
    File* file, bool skip_broken_records, int64_t record_base, int64_t end_offset) {
  assert(file != nullptr);
  return impl_->ImportFromFileBackward(file, skip_broken_records, record_base, end_offset);
}

Status HashDBM::ValidateHashBuckets() {
  return impl_->ValidateHashBuckets();
}

Status HashDBM::ValidateRecords(int64_t record_base, int64_t end_offset) {
  return impl_->ValidateRecords(record_base, end_offset);
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

Status HashDBM::ReadMetadata(
    File* file, int32_t* cyclic_magic,
    int32_t* pkg_major_version, int32_t* pkg_minor_version,
    int32_t* static_flags, int32_t* offset_width, int32_t* align_pow,
    int32_t* closure_flags, int64_t* num_buckets, int64_t* num_records,
    int64_t* eff_data_size, int64_t* file_size, int64_t* timestamp,
    int32_t* db_type, std::string* opaque) {
  assert(file != nullptr && pkg_major_version != nullptr && pkg_minor_version != nullptr &&
         static_flags != nullptr && offset_width != nullptr && align_pow != nullptr &&
         closure_flags != nullptr && num_buckets != nullptr && num_records != nullptr &&
         eff_data_size != nullptr && file_size != nullptr && timestamp != nullptr &&
         db_type != nullptr && opaque != nullptr);
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
  *static_flags = ReadFixNum(meta + META_OFFSET_STATIC_FLAGS, 1);
  *offset_width = ReadFixNum(meta + META_OFFSET_OFFSET_WIDTH, 1);
  *align_pow = ReadFixNum(meta + META_OFFSET_ALIGN_POW, 1);
  *closure_flags = ReadFixNum(meta + META_OFFSET_CLOSURE_FLAGS, 1);
  *num_buckets = ReadFixNum(meta + META_OFFSET_NUM_BUCKETS, 8);
  *num_records = ReadFixNum(meta + META_OFFSET_NUM_RECORDS, 8);
  *eff_data_size = ReadFixNum(meta + META_OFFSET_EFF_DATA_SIZE, 8);
  *file_size = ReadFixNum(meta + META_OFFSET_FILE_SIZE, 8);
  *timestamp = ReadFixNum(meta + META_OFFSET_TIMESTAMP, 8);
  *db_type = ReadFixNum(meta + META_OFFSET_DB_TYPE, 4);
  *opaque = std::string(meta + META_OFFSET_OPAQUE, HashDBM::OPAQUE_METADATA_SIZE);
  const int32_t cyclic_magic_back = ReadFixNum(meta + META_OFFSET_CYCLIC_MAGIC_BACK, 1);
  if (*cyclic_magic != cyclic_magic_back) {
    *cyclic_magic = -1;
  }
  return Status(Status::SUCCESS);
}

Status HashDBM::FindRecordBase(
    File* file, int64_t *record_base,
    int32_t* static_flags, int32_t* offset_width, int32_t* align_pow,
    int64_t* last_sync_size) {
  assert(file != nullptr && record_base != nullptr &&
         static_flags != nullptr && offset_width != nullptr && align_pow != nullptr &&
         last_sync_size != nullptr);
  *record_base = 0;
  *offset_width = 0;
  *align_pow = 0;
  *last_sync_size = 0;
  bool meta_ok = false;
  char meta[METADATA_SIZE];
  Status status = file->Read(0, meta, METADATA_SIZE);
  if (status == Status::SUCCESS &&
      std::memcmp(meta, META_MAGIC_DATA, sizeof(META_MAGIC_DATA) - 1) == 0) {
    *static_flags = ReadFixNum(meta + META_OFFSET_STATIC_FLAGS, 1);
    *offset_width = ReadFixNum(meta + META_OFFSET_OFFSET_WIDTH, 1);
    *align_pow = ReadFixNum(meta + META_OFFSET_ALIGN_POW, 1);
    const int64_t num_buckets = ReadFixNum(meta + META_OFFSET_NUM_BUCKETS, 8);
    if ((*offset_width >= MIN_OFFSET_WIDTH && *offset_width <= MAX_OFFSET_WIDTH) &&
        *align_pow <= MAX_ALIGN_POW && num_buckets <= MAX_NUM_BUCKETS) {
      const int32_t align = std::max(RECORD_BASE_ALIGN, 1 << *align_pow);
      *record_base = METADATA_SIZE + num_buckets * *offset_width +
          FBP_SECTION_SIZE + RECORD_BASE_HEADER_SIZE;
      *record_base = AlignNumber(*record_base, align);
      meta_ok = true;
    }
    if (ReadFixNum(meta + META_OFFSET_CYCLIC_MAGIC_FRONT, 1) ==
        ReadFixNum(meta + META_OFFSET_CYCLIC_MAGIC_BACK, 1)) {
      *last_sync_size = ReadFixNum(meta + META_OFFSET_FILE_SIZE, 8);
    }
  }
  if (*record_base == 0) {
    int64_t offset = RECORD_BASE_ALIGN;
    const int64_t file_size = file->GetSizeSimple();
    while (offset < file_size) {
      const int64_t magic_offset = offset - RECORD_BASE_HEADER_SIZE;
      char head[RECORD_BASE_HEADER_SIZE];
      status = file->Read(magic_offset, head, RECORD_BASE_HEADER_SIZE);
      if (status == Status::SUCCESS &&
          std::memcmp(head, RECORD_BASE_MAGIC_DATA, sizeof(RECORD_BASE_MAGIC_DATA) - 1) == 0) {
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
  if (!meta_ok) {
    if (std::memcmp(head, RECORD_BASE_MAGIC_DATA, sizeof(RECORD_BASE_MAGIC_DATA) - 1) != 0) {
      return Status(Status::BROKEN_DATA_ERROR, "bad magic data");
    }
    *static_flags = ReadFixNum(head + RECHEAD_OFFSET_STATIC_FLAGS, 1);
    *offset_width = ReadFixNum(head + RECHEAD_OFFSET_OFFSET_WIDTH, 1);
    *align_pow = ReadFixNum(head + RECHEAD_OFFSET_ALIGN_POW, 1);
  }
  if (*offset_width < MIN_OFFSET_WIDTH || *offset_width > MAX_OFFSET_WIDTH) {
    return Status(Status::BROKEN_DATA_ERROR, "the offset width is invalid");
  }
  if (*align_pow > MAX_ALIGN_POW) {
    return Status(Status::BROKEN_DATA_ERROR, "the alignment power is invalid");
  }
  return Status(Status::SUCCESS);
}

int32_t HashDBM::GetCRCWidthFromStaticFlags(int32_t static_flags) {
  switch (static_flags & STATIC_FLAG_RECORD_CRC_32) {
    case STATIC_FLAG_RECORD_CRC_8:
      return 1;
    case STATIC_FLAG_RECORD_CRC_16:
      return 2;
    case STATIC_FLAG_RECORD_CRC_32:
      return 4;
  }
  return 0;
}

std::unique_ptr<Compressor> HashDBM::MakeCompressorFromStaticFlags(
    int32_t static_flags, std::string_view cipher_key) {
  switch (static_flags & STATIC_FLAG_RECORD_COMP_EXTRA) {
    case STATIC_FLAG_RECORD_COMP_ZLIB:
      return std::make_unique<ZLibCompressor>();
    case STATIC_FLAG_RECORD_COMP_ZSTD:
      return std::make_unique<ZStdCompressor>();
    case STATIC_FLAG_RECORD_COMP_LZ4:
      return std::make_unique<LZ4Compressor>();
    case STATIC_FLAG_RECORD_COMP_LZMA:
      return std::make_unique<LZMACompressor>();
    case STATIC_FLAG_RECORD_COMP_RC4:
      return std::make_unique<RC4Compressor>(cipher_key, 0);
    case STATIC_FLAG_RECORD_COMP_AES:
      return std::make_unique<AESCompressor>(cipher_key, 0);
  }
  return nullptr;
}

bool HashDBM::CheckRecordCompressionModeIsSupported(RecordCompressionMode mode) {
  switch (mode) {
    case RECORD_COMP_ZLIB: {
      return ZLibCompressor().IsSupported();
    }
    case RECORD_COMP_ZSTD: {
      return ZStdCompressor().IsSupported();
    }
    case RECORD_COMP_LZ4: {
      return LZ4Compressor().IsSupported();
    }
    case RECORD_COMP_LZMA: {
      return LZMACompressor().IsSupported();
    }
    case RECORD_COMP_RC4: {
      return RC4Compressor("", 1).IsSupported();
    }
    case RECORD_COMP_AES: {
      return AESCompressor("", 1).IsSupported();
    }
    default:
      break;
  }
  return true;
}

Status HashDBM::RestoreDatabase(
    const std::string& old_file_path, const std::string& new_file_path,
    int64_t end_offset, std::string_view cipher_key) {
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
  if (act_old_file_size < RECORD_BASE_ALIGN) {
    return Status(Status::BROKEN_DATA_ERROR, "too small file");
  }
  UpdateMode update_mode = UPDATE_DEFAULT;
  RecordCRCMode record_crc_mode = RECORD_CRC_DEFAULT;
  RecordCompressionMode record_comp_mode = RECORD_COMP_DEFAULT;
  int32_t db_type = 0;
  std::string opaque;
  int64_t record_base = 0;
  int32_t static_flags = 0;
  int32_t offset_width = 0;
  int32_t align_pow = 0;
  int64_t last_sync_size = 0;
  status = FindRecordBase(
      &old_file, &record_base, &static_flags, &offset_width, &align_pow, &last_sync_size);
  if (status != Status::SUCCESS) {
    return status;
  }
  if (static_flags & STATIC_FLAG_UPDATE_IN_PLACE)  {
    update_mode = HashDBM::UPDATE_IN_PLACE;
  } else if (static_flags & STATIC_FLAG_UPDATE_APPENDING) {
    update_mode = HashDBM::UPDATE_APPENDING;
  }
  int64_t num_buckets = (record_base - METADATA_SIZE) / offset_width;
  switch (HashDBM::GetCRCWidthFromStaticFlags(static_flags)) {
    case 1:
      record_crc_mode = RECORD_CRC_8;
      break;
    case 2:
      record_crc_mode = RECORD_CRC_16;
      break;
    case 4:
      record_crc_mode = RECORD_CRC_32;
      break;
  }
  {
    auto tmp_compressor = HashDBM::MakeCompressorFromStaticFlags(static_flags, cipher_key);
    if (tmp_compressor != nullptr) {
      const auto& comp_type = tmp_compressor->GetType();
      if (comp_type == typeid(ZLibCompressor)) {
        record_comp_mode = HashDBM::RECORD_COMP_ZLIB;
      } else if (comp_type == typeid(ZStdCompressor)) {
        record_comp_mode = HashDBM::RECORD_COMP_ZSTD;
      } else if (comp_type == typeid(LZ4Compressor)) {
        record_comp_mode = HashDBM::RECORD_COMP_LZ4;
      } else if (comp_type == typeid(LZMACompressor)) {
        record_comp_mode = HashDBM::RECORD_COMP_LZMA;
      } else if (comp_type == typeid(RC4Compressor)) {
        record_comp_mode = HashDBM::RECORD_COMP_RC4;
      } else if (comp_type == typeid(AESCompressor)) {
        record_comp_mode = HashDBM::RECORD_COMP_AES;
      }
    }
  }
  int32_t old_cyclic_magic = 0;
  int32_t old_pkg_major_version = 0;
  int32_t old_pkg_minor_version = 0;
  int32_t old_static_flags = 0;
  int32_t old_offset_width = 0;
  int32_t old_align_pow = 0;
  int32_t old_closure_flags = 0;
  int64_t old_num_buckets = 0;
  int64_t old_num_records = 0;
  int64_t old_eff_data_size = 0;
  int64_t old_file_size = 0;
  int64_t old_timestamp = 0;
  int32_t old_db_type = 0;
  std::string old_opaque;
  if (ReadMetadata(
          &old_file, &old_cyclic_magic, &old_pkg_major_version, &old_pkg_minor_version,
          &old_static_flags, &old_offset_width, &old_align_pow,
          &old_closure_flags, &old_num_buckets, &old_num_records,
          &old_eff_data_size, &old_file_size, &old_timestamp,
          &old_db_type, &old_opaque) == Status::SUCCESS && old_cyclic_magic >= 0) {
    if (old_static_flags & STATIC_FLAG_UPDATE_IN_PLACE)  {
      update_mode = HashDBM::UPDATE_IN_PLACE;
    } else if (old_static_flags & STATIC_FLAG_UPDATE_APPENDING) {
      update_mode = HashDBM::UPDATE_APPENDING;
    }
    switch (HashDBM::GetCRCWidthFromStaticFlags(old_static_flags)) {
      case 1:
        record_crc_mode = RECORD_CRC_8;
        break;
      case 2:
        record_crc_mode = RECORD_CRC_16;
        break;
      case 4:
        record_crc_mode = RECORD_CRC_32;
        break;
    }
    auto old_compressor = HashDBM::MakeCompressorFromStaticFlags(old_static_flags, cipher_key);
    if (old_compressor != nullptr) {
      const auto& comp_type = old_compressor->GetType();
      if (comp_type == typeid(ZLibCompressor)) {
        record_comp_mode = HashDBM::RECORD_COMP_ZLIB;
      } else if (comp_type == typeid(ZStdCompressor)) {
        record_comp_mode = HashDBM::RECORD_COMP_ZSTD;
      } else if (comp_type == typeid(LZ4Compressor)) {
        record_comp_mode = HashDBM::RECORD_COMP_LZ4;
      } else if (comp_type == typeid(LZMACompressor)) {
        record_comp_mode = HashDBM::RECORD_COMP_LZMA;
      } else if (comp_type == typeid(RC4Compressor)) {
        record_comp_mode = HashDBM::RECORD_COMP_RC4;
      } else if (comp_type == typeid(AESCompressor)) {
        record_comp_mode = HashDBM::RECORD_COMP_AES;
      }
    }
    offset_width = old_offset_width;
    align_pow = old_align_pow;
    num_buckets = old_num_buckets;
    db_type = old_db_type;
    opaque = old_opaque;
  }
  status = old_file.Close();
  if (status != Status::SUCCESS) {
    return status;
  }
  std::unique_ptr<File> new_file;
  if (act_old_file_size <= UINT32MAX) {
    new_file = std::make_unique<MemoryMapParallelFile>();
  } else {
    new_file = std::make_unique<PositionalParallelFile>();
  }
  TuningParameters tuning_params;
  tuning_params.update_mode = update_mode;
  tuning_params.record_crc_mode = record_crc_mode;
  tuning_params.record_comp_mode = record_comp_mode;
  tuning_params.offset_width = offset_width;
  tuning_params.align_pow = align_pow;
  tuning_params.num_buckets = num_buckets;
  tuning_params.restore_mode = HashDBM::RESTORE_READ_ONLY;
  tuning_params.cipher_key = cipher_key;
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
