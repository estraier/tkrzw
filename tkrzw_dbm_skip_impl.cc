/*************************************************************************************************
 * Implementation components for the skip database manager
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
#include "tkrzw_dbm_skip_impl.h"
#include "tkrzw_file.h"
#include "tkrzw_file_mmap.h"
#include "tkrzw_file_pos.h"
#include "tkrzw_file_std.h"
#include "tkrzw_file_util.h"
#include "tkrzw_str_util.h"

namespace tkrzw {

SkipRecord::SkipRecord(File* file, int32_t offset_width, int32_t step_unit, int32_t max_level)
    : file_(file), offset_width_(offset_width), step_unit_(step_unit), max_level_(max_level),
      skip_offsets_(max_level, 0), body_buf_(nullptr) {}

SkipRecord::~SkipRecord() {
  delete[] body_buf_;
}

SkipRecord& SkipRecord::operator =(SkipRecord&& rhs) {
  delete[] body_buf_;
  file_ = rhs.file_;
  offset_width_ = rhs.offset_width_;
  step_unit_ = rhs.step_unit_;
  max_level_ = rhs.max_level_;
  level_ = rhs.level_;
  offset_ = rhs.offset_;
  index_ = rhs.index_;
  whole_size_ = rhs.whole_size_;
  key_size_ = rhs.key_size_;
  value_size_ = rhs.value_size_;
  skip_offsets_.swap(rhs.skip_offsets_);
  if (rhs.key_ptr_ != nullptr &&
      rhs.key_ptr_ >= rhs.buffer_ && rhs.key_ptr_ < rhs.buffer_ + READ_BUFFER_SIZE) {
    std::memcpy(buffer_, rhs.key_ptr_, rhs.key_size_);
    key_ptr_ = buffer_;
  } else {
    key_ptr_ = rhs.key_ptr_;
  }
  if (rhs.value_ptr_ != nullptr &&
      rhs.value_ptr_ >= rhs.buffer_ && rhs.value_ptr_ < rhs.buffer_ + READ_BUFFER_SIZE) {
    std::memcpy(buffer_ + key_size_, rhs.value_ptr_, rhs.value_size_);
    value_ptr_ = buffer_ + key_size_;
  } else {
    value_ptr_ = rhs.value_ptr_;
  }
  body_offset_ = rhs.body_offset_;
  body_buf_ = rhs.body_buf_;
  rhs.body_buf_ = nullptr;
  return *this;
}

Status SkipRecord::ReadMetadataKey(int64_t offset, int64_t index) {
  level_ = 0;
  index_ = index;
  while (level_ < max_level_ && index % step_unit_ == 0) {
    index /= step_unit_;
    level_++;
  }
  offset_ = offset;
  const int64_t min_record_size = sizeof(uint8_t) + offset_width_ * level_ + sizeof(uint8_t) * 2;
  const int64_t read_size = min_record_size + READ_DATA_SIZE;
  const int64_t max_read_size = std::min(file_->GetSizeSimple() - offset, MAX_MEMORY_SIZE);
  int64_t record_size = max_read_size;
  if (record_size > read_size) {
    record_size = read_size;
  } else {
    if (record_size < min_record_size) {
      return Status(Status::BROKEN_DATA_ERROR, "too short record data");
    }
  }
  Status status = file_->Read(offset, buffer_, record_size);
  if (status != Status::SUCCESS) {
    return status;
  }
  const char* rp = buffer_;
  if (*(uint8_t*)rp != RECORD_MAGIC) {
    return Status(Status::BROKEN_DATA_ERROR, "invalid record magic number");
  }
  rp++;
  record_size--;
  if (record_size < static_cast<int64_t>(offset_width_ * level_)) {
    return Status(Status::BROKEN_DATA_ERROR, "invalid child offset");
  }
  for (int32_t i = 0; i < level_; i++) {
    skip_offsets_[i] = ReadFixNum(rp, offset_width_);
    rp += offset_width_;
    record_size -= offset_width_;
  }
  for (int32_t i = level_; i < max_level_; i++) {
    skip_offsets_[i] = 0;
  }
  uint64_t num = 0;
  int32_t step = ReadVarNum(rp, record_size, &num);
  if (step < 1) {
    return Status(Status::BROKEN_DATA_ERROR, "invalid key size");
  }
  key_size_ = num;
  if (key_size_ > max_read_size) {
    return Status(Status::BROKEN_DATA_ERROR, "too large key size");
  }
  rp += step;
  record_size -= step;
  step = ReadVarNum(rp, record_size, &num);
  if (step < 1) {
    return Status(Status::BROKEN_DATA_ERROR, "invalid value size");
  }
  value_size_ = num;
  if (value_size_ > max_read_size) {
    return Status(Status::BROKEN_DATA_ERROR, "too large value size");
  }
  rp += step;
  record_size -= step;
  const int32_t header_size = sizeof(uint8_t) + offset_width_ * level_ +
      SizeVarNum(key_size_) + SizeVarNum(value_size_);
  whole_size_ = header_size + key_size_ + value_size_;
  key_ptr_ = nullptr;
  value_ptr_ = nullptr;
  body_offset_ = offset + header_size;
  if (record_size >= static_cast<int64_t>(key_size_)) {
    key_ptr_ = rp;
    rp += key_size_;
    record_size -= key_size_;
    if (record_size >= static_cast<int64_t>(value_size_)) {
      value_ptr_ = rp;
    }
    rp += value_size_;
    record_size -= value_size_;
  } else {
    if (offset + static_cast<int64_t>(whole_size_) > file_->GetSizeSimple()) {
      return Status(Status::BROKEN_DATA_ERROR, "invalid length of a record");
    }
    status = ReadBody();
    if (status != Status::SUCCESS) {
      return status;
    }
  }
  return Status(Status::SUCCESS);
}

Status SkipRecord::ReadBody() {
  const int32_t body_size = key_size_ + value_size_;
  delete[] body_buf_;
  body_buf_ = new char[body_size];
  const Status status = file_->Read(body_offset_, body_buf_, body_size);
  if (status != Status::SUCCESS) {
    return status;
  }
  key_ptr_ = body_buf_;
  value_ptr_ = body_buf_ + key_size_;
  return Status(Status::SUCCESS);
}

std::string_view SkipRecord::GetKey() const {
  return std::string_view(key_ptr_, key_size_);
}

std::string_view SkipRecord::GetValue() const {
  return std::string_view(value_ptr_, value_size_);
}

const std::vector<int64_t>& SkipRecord::GetStepOffsets() const {
  return skip_offsets_;
}

int32_t SkipRecord::GetLevel() const {
  return level_;
}

int64_t SkipRecord::GetIndex() const {
  return index_;
}

int64_t SkipRecord::GetOffset() const {
  return offset_;
}

int32_t SkipRecord::GetWholeSize() const {
  return whole_size_;
}

void SkipRecord::SetData(int64_t index, const char* key_ptr, int32_t key_size,
                         const char* value_ptr, int32_t value_size) {
  level_ = 0;
  index_ = index;
  while (level_ < max_level_ && index % step_unit_ == 0) {
    index /= step_unit_;
    level_++;
  }
  whole_size_ = sizeof(uint8_t) + offset_width_ * level_ +
      SizeVarNum(key_size) + SizeVarNum(value_size) + key_size + value_size;
  key_size_ = key_size;
  value_size_ = value_size;
  key_ptr_ = key_ptr;
  value_ptr_ = value_ptr;
}

Status SkipRecord::Write() {
  char stack[WRITE_BUFFER_SIZE];
  char* write_buf = whole_size_ > static_cast<int32_t>(sizeof(stack)) ?
      new char[whole_size_] : stack;
  char* wp = write_buf;
  *(wp++) = RECORD_MAGIC;
  std::memset(wp, 0, offset_width_ * level_);
  wp += offset_width_ * level_;
  wp += WriteVarNum(wp, key_size_);
  wp += WriteVarNum(wp, value_size_);
  std::memcpy(wp, key_ptr_, key_size_);
  wp += key_size_;
  std::memcpy(wp, value_ptr_, value_size_);
  wp += value_size_;
  Status status(Status::SUCCESS);
  status = file_->Append(write_buf, whole_size_, &offset_);
  if (write_buf != stack) {
    delete[] write_buf;
  }
  return status;
}

Status SkipRecord::UpdatePastRecords(
    int64_t index, int64_t offset, std::vector<int64_t>* past_offsets) const {
  int64_t past_index_diff = 1;
  for (int32_t i = 0; i < level_; i++) {
    past_index_diff *= step_unit_;
    int64_t past_offset = (*past_offsets)[i];
    if (past_offset <= 0) {
      continue;
    }
    int64_t past_index = index - past_index_diff;
    int32_t past_level = 0;
    while (past_level < max_level_ && past_index % step_unit_ == 0) {
      past_index /= step_unit_;
      past_level++;
    }
    if (i > past_level) {
      break;
    }
    char buf[sizeof(uint64_t)];
    WriteFixNum(buf, offset, offset_width_);
    past_offset += sizeof(uint8_t) + i * offset_width_;
    const Status status = file_->Write(past_offset, buf, offset_width_);
    if (status != Status::SUCCESS) {
      return status;
    }
  }
  for (int32_t i = 0; i < level_; i++) {
    (*past_offsets)[i] = offset;
  }
  return Status(Status::SUCCESS);
}

Status SkipRecord::Search(
    int64_t record_base, SkipRecordCache* cache, std::string_view key, bool upper) {
  int64_t offset = record_base;
  int64_t current_index = 0;
  int64_t end_offset = file_->GetSizeSimple();
  bool rec_ready = false;
  while (offset < end_offset) {
    if (!rec_ready) {
      if (!cache->PrepareRecord(current_index, this)) {
        const Status status = ReadMetadataKey(offset, current_index);
        if (status != Status::SUCCESS) {
          return status;
        }
        cache->Add(*this);
      }
    }
    const int32_t cmp = key.compare(std::string_view(key_ptr_, key_size_));
    if (cmp == 0) {
      return Status(Status::SUCCESS);
    }
    if (cmp < 0) {
      break;
    }
    rec_ready = false;
    bool skipped = false;
    for (int32_t level = level_; level > 0; level--) {
      const int64_t next_offset = skip_offsets_[level - 1];
      if (next_offset <= 0 || next_offset >= end_offset) {
        continue;
      }
      int64_t next_index_diff = 1;
      for (int32_t i = 0; i < level; i++) {
        next_index_diff *= step_unit_;
      }
      int64_t next_index = current_index + next_index_diff;
      SkipRecord next_rec(file_, offset_width_, step_unit_, max_level_);
      if (!cache->PrepareRecord(next_index, &next_rec)) {
        const Status status = next_rec.ReadMetadataKey(next_offset, next_index);
        if (status != Status::SUCCESS) {
          return status;
        }
        cache->Add(next_rec);
      }
      const std::string_view next_key = next_rec.GetKey();
      const int32_t cmp = key.compare(next_key);
      if (cmp > 0) {
        offset = next_offset;
        current_index = next_index;
        skipped = true;
        *this = std::move(next_rec);
        rec_ready = true;
        break;
      }
      if (cmp < 0) {
        end_offset = next_offset;
      }
    }
    if (!skipped) {
      offset += whole_size_;
      current_index++;
    }
  }
  if (upper && offset < file_->GetSizeSimple()) {
    if (!cache->PrepareRecord(current_index, this)) {
      const Status status = ReadMetadataKey(offset, current_index);
      if (status != Status::SUCCESS) {
        return status;
      }
      cache->Add(*this);
    }
    return Status(Status::SUCCESS);
  }
  return Status(Status::NOT_FOUND_ERROR);
}

Status SkipRecord::SearchByIndex(int64_t record_base, SkipRecordCache* cache, int64_t index) {
  int64_t offset = record_base;
  int64_t current_index = 0;
  int64_t end_offset = file_->GetSizeSimple();
  bool rec_ready = false;
  while (offset < end_offset) {
    if (!rec_ready) {
      if (!cache->PrepareRecord(current_index, this)) {
        const Status status = ReadMetadataKey(offset, current_index);
        if (status != Status::SUCCESS) {
          return status;
        }
        cache->Add(*this);
      }
    }
    if (current_index == index) {
      return Status(Status::SUCCESS);
    }
    rec_ready = false;
    bool skipped = false;
    for (int32_t level = level_; level > 0; level--) {
      const int64_t next_offset = skip_offsets_[level - 1];
      if (next_offset <= 0 || next_offset >= end_offset) {
        continue;
      }
      int64_t next_index_diff = 1;
      for (int32_t i = 0; i < level; i++) {
        next_index_diff *= step_unit_;
      }
      int64_t next_index = current_index + next_index_diff;
      SkipRecord next_rec(file_, offset_width_, step_unit_, max_level_);
      if (!cache->PrepareRecord(next_index, &next_rec)) {
        const Status status = next_rec.ReadMetadataKey(next_offset, next_index);
        if (status != Status::SUCCESS) {
          return status;
        }
        cache->Add(next_rec);
      }
      if (next_index <= index) {
        offset = next_offset;
        current_index = next_index;
        skipped = true;
        *this = std::move(next_rec);
        rec_ready = true;
        break;
      } else {
        end_offset = next_offset;
      }
    }
    if (!skipped) {
      offset += whole_size_;
      current_index++;
    }
  }
  return Status(Status::NOT_FOUND_ERROR);
}

File* SkipRecord::GetFile() const {
  return file_;
}


char* SkipRecord::Serialize() const {
  int32_t size = offset_width_ + SizeVarNum(level_) + sizeof(int64_t) * level_ +
      SizeVarNum(key_size_) + SizeVarNum(value_size_) + sizeof(uint8_t) + key_size_;
  if (value_ptr_ != nullptr) {
    size += value_size_;
  }
  char* serialized = new char[size];
  char* wp = serialized;
  WriteFixNum(wp, offset_, offset_width_);
  wp += offset_width_;
  wp += WriteVarNum(wp, level_);
  std::memcpy(wp, skip_offsets_.data(), sizeof(int64_t) * level_);
  wp += sizeof(int64_t) * level_;
  wp += WriteVarNum(wp, key_size_);
  wp += WriteVarNum(wp, value_size_);
  if (value_ptr_ == nullptr) {
    *(wp++) = 0;
    std::memcpy(wp, key_ptr_, key_size_);
    wp += key_size_;
  } else {
    *(wp++) = 1;
    std::memcpy(wp, key_ptr_, key_size_);
    wp += key_size_;
    std::memcpy(wp, value_ptr_, value_size_);
    wp += value_size_;
  }
  return serialized;
}

void SkipRecord::Deserialize(int64_t index, const char* serialized) {
  delete[] body_buf_;
  body_buf_ = nullptr;
  const char* rp = serialized;
  constexpr int32_t max_varnum_size = 6;
  offset_ = ReadFixNum(rp, offset_width_);
  rp += offset_width_;
  uint64_t num = 0;
  rp += ReadVarNum(rp, max_varnum_size, &num);
  level_ = num;
  std::memcpy(skip_offsets_.data(), rp, sizeof(int64_t) * level_);
  rp += sizeof(int64_t) * level_;
  rp += ReadVarNum(rp, max_varnum_size, &num);
  key_size_ = num;
  rp += ReadVarNum(rp, max_varnum_size, &num);
  value_size_ = num;
  const bool has_value = *(rp++);
  if (key_size_ <= READ_BUFFER_SIZE) {
    std::memcpy(buffer_, rp, key_size_);
    key_ptr_ = buffer_;
    rp += key_size_;
    if (has_value) {
      if (key_size_ + value_size_ <= READ_BUFFER_SIZE) {
        std::memcpy(buffer_ + key_size_, rp, value_size_);
        value_ptr_ = buffer_ + key_size_;
      } else {
        body_buf_ = new char[key_size_ + value_size_];
        std::memcpy(body_buf_, key_ptr_, key_size_);
        key_ptr_ = body_buf_;
        std::memcpy(body_buf_ + key_size_, rp, value_size_);
        value_ptr_ = body_buf_ + key_size_;
      }
    } else {
      value_ptr_ = nullptr;
    }
  } else {
    if (has_value) {
      body_buf_ = new char[key_size_ + value_size_];
      std::memcpy(body_buf_, rp, key_size_);
      rp += key_size_;
      key_ptr_ = body_buf_;
      std::memcpy(body_buf_ + key_size_, rp, value_size_);
      value_ptr_ = body_buf_ + key_size_;
    } else {
      body_buf_ = new char[key_size_];
      std::memcpy(body_buf_, rp, key_size_);
      key_ptr_ = body_buf_;
      value_ptr_ = nullptr;
    }
  }
  index_ = index;
  const int32_t header_size = sizeof(uint8_t) + offset_width_ * level_ +
      SizeVarNum(key_size_) + SizeVarNum(value_size_);
  whole_size_ = header_size + key_size_ + value_size_;
  body_offset_ = offset_ + header_size;
}

SkipRecordCache::SkipRecordCache(int32_t step_unit, int32_t capacity, int64_t num_records)
    : size_(0), cache_unit_(0) {
  cache_unit_ = 1;
  while (num_records / cache_unit_ > capacity) {
    cache_unit_ *= step_unit;
  }
  size_ = std::max(num_records / cache_unit_, static_cast<int64_t>(1));
  records_ = new std::atomic<char*>[size_];
  for (int32_t i = 0; i < size_; i++) {
    records_[i] = nullptr;
  }
}

SkipRecordCache::~SkipRecordCache() {
  for (int32_t i = 0; i < size_; i++) {
    delete[] records_[i];
  }
  delete[] records_;
}

bool SkipRecordCache::PrepareRecord(int64_t index, SkipRecord* record) {
  const auto index_div = std::lldiv(index, cache_unit_);
  if (index_div.rem != 0 || index_div.quot >= size_) {
    return false;
  }
  const char* cached = records_[index_div.quot].load();
  if (cached == nullptr) {
    return false;
  }
  record->Deserialize(index, cached);
  return true;
}

void SkipRecordCache::Add(const SkipRecord& record) {
  const auto index_div = std::lldiv(record.GetIndex(), cache_unit_);
  if (index_div.rem != 0 || index_div.quot >= size_) {
    return;
  }
  char* serialized = record.Serialize();
  char* expected = nullptr;
  if (!records_[index_div.quot].compare_exchange_strong(expected, serialized)) {
    delete[] serialized;
  }
}

RecordSorter::RecordSorter(const std::string& base_path, int64_t max_mem_size, bool use_mmap)
    : base_path_(base_path), max_mem_size_(max_mem_size), use_mmap_(use_mmap),
      total_data_size_(0), finished_(false), current_mem_size_(0) {}

RecordSorter::~RecordSorter() {
  for (const auto& tmp_file : tmp_files_) {
    delete tmp_file.reader;
    delete tmp_file.file;
    RemoveFile(tmp_file.path);
  }
  for (const auto& skip_record : skip_records_) {
    delete skip_record.first;
  }
}

Status RecordSorter::Add(std::string_view key, std::string_view value) {
  if (finished_) {
    return Status(Status::INFEASIBLE_ERROR, "already finished");
  }
  std::string record = SerializeStrPair(key, value);
  current_mem_size_ += record.size() + REC_MEM_FOOT;
  current_records_.emplace_back(std::move(record));
  Status status(Status::SUCCESS);
  if (!current_records_.empty() && current_mem_size_ >= max_mem_size_) {
    status |= Flush();
  }
  return status;
}

void RecordSorter::AddSkipRecord(SkipRecord* rec, int64_t record_base) {
  skip_records_.emplace_back(rec, record_base);
}

void RecordSorter::TakeFileOwnership(std::unique_ptr<File>&& file) {
  owned_files_.emplace_back(std::move(file));
}

bool RecordSorter::IsUpdated() const {
  return !current_records_.empty() || !tmp_files_.empty() || !skip_records_.empty();
}

Status RecordSorter::Finish() {
  if (finished_) {
    return Status(Status::INFEASIBLE_ERROR, "already finished");
  }
  if (!current_records_.empty()) {
    const Status status = Flush();
    if (status != Status::SUCCESS) {
      return status;
    }
  }
  slots_.reserve(skip_records_.size() + tmp_files_.size());
  for (const auto& skip_record : skip_records_) {
    SkipRecord* rec = skip_record.first;
    File* file = rec->GetFile();
    const int64_t offset = skip_record.second;
    const int64_t end_offset = file->GetSizeSimple();
    if (offset >= end_offset) {
      continue;
    }
    const Status status = rec->ReadMetadataKey(offset, 0);
    if (status != Status::SUCCESS) {
      return status;
    }
    const std::string_view key = rec->GetKey();
    std::string_view value = rec->GetValue();
    if (value.data() == nullptr) {
      const Status status = rec->ReadBody();
      if (status != Status::SUCCESS) {
        return status;
      }
      value = rec->GetValue();
    }
    SortSlot slot;
    slot.id = slots_.size();
    slot.key = key;
    slot.value = value;
    slot.file = nullptr;
    slot.flat_reader = nullptr;
    slot.skip_record = rec;
    slot.offset = offset + rec->GetWholeSize();
    slot.end_offset = end_offset;
    slots_.emplace_back(slot);
    heap_.emplace_back(&slots_.back());
    std::push_heap(heap_.begin(), heap_.end(), SortSlotComparator());
  }
  for (const auto& tmp_file : tmp_files_) {
    std::string_view rec;
    const Status status = tmp_file.reader->Read(&rec);
    if (status != Status::SUCCESS) {
      return status;
    }
    std::string_view key, value;
    DeserializeStrPair(rec, &key, &value);
    SortSlot slot;
    slot.id = slots_.size();
    slot.key = key;
    slot.value = value;
    slot.file = tmp_file.file;
    slot.flat_reader = tmp_file.reader;
    slot.skip_record = nullptr;
    slot.offset = 0;
    slot.end_offset = 0;
    slots_.emplace_back(slot);
    heap_.emplace_back(&slots_.back());
    std::push_heap(heap_.begin(), heap_.end(), SortSlotComparator());
  }
  finished_ = true;
  return Status(Status::SUCCESS);
}

Status RecordSorter::Get(std::string* key, std::string* value) {
  if (!finished_) {
    return Status(Status::INFEASIBLE_ERROR, "not finished yet");
  }
  if (heap_.empty()) {
    return Status(Status::NOT_FOUND_ERROR);
  }
  std::pop_heap(heap_.begin(), heap_.end(), SortSlotComparator());
  auto* slot = heap_.back();
  key->swap(slot->key);
  value->swap(slot->value);
  bool has_record = false;
  if (slot->skip_record == nullptr) {
    std::string_view rec;
    const Status status = slot->flat_reader->Read(&rec);
    if (status == Status::SUCCESS) {
      std::string_view rec_key, rec_value;
      DeserializeStrPair(rec, &rec_key, &rec_value);
      slot->key = rec_key;
      slot->value = rec_value;
      std::push_heap(heap_.begin(), heap_.end(), SortSlotComparator());
      has_record = true;
    } else if (status != Status::NOT_FOUND_ERROR) {
      return status;
    }
  } else {
    if (slot->offset < slot->end_offset) {
      SkipRecord* rec = slot->skip_record;
      const Status status = rec->ReadMetadataKey(slot->offset, rec->GetIndex() + 1);
      if (status != Status::SUCCESS) {
        return status;
      }
      const std::string_view rec_key = rec->GetKey();
      std::string_view rec_value = rec->GetValue();
      if (rec_value.data() == nullptr) {
        const Status status = rec->ReadBody();
        if (status != Status::SUCCESS) {
          return status;
        }
        rec_value = rec->GetValue();
      }
      slot->key = rec_key;
      slot->value = rec_value;
      slot->offset += rec->GetWholeSize();
      std::push_heap(heap_.begin(), heap_.end(), SortSlotComparator());
      has_record = true;
    }
  }
  if (!has_record) {
    heap_.pop_back();
  }
  return Status(Status::SUCCESS);
}

Status RecordSorter::Flush() {
  total_data_size_ += current_mem_size_;
  TmpFileFlat tmp_file;
  tmp_file.path = base_path_ + SPrintF(".%05d", tmp_files_.size());
  if (use_mmap_ && total_data_size_ <= MAX_DATA_SIZE_MMAP_USE) {
    tmp_file.file = new MemoryMapParallelFile();
  } else {
    tmp_file.file = new PositionalParallelFile();
  }
  tmp_file.reader = new FlatRecordReader(tmp_file.file);
  tmp_files_.emplace_back(tmp_file);
  Status status = tmp_file.file->Open(tmp_file.path, true, File::OPEN_TRUNCATE);
  if (status != Status::SUCCESS) {
    return status;
  }
  std::stable_sort(current_records_.begin(), current_records_.end(),
                   [](const std::string& a, const std::string& b) {
                     return GetFirstFromSerializedStrPair(a) < GetFirstFromSerializedStrPair(b);
                   });
  FlatRecord rec(tmp_file.file);
  for (const auto& record : current_records_) {
    status = rec.Write(record);
    if (status != Status::SUCCESS) {
      return status;
    }
  }
  current_records_.clear();
  current_mem_size_ = 0;
  return Status(Status::SUCCESS);
}

}  // namespace tkrzw

// END OF FILE
