/*************************************************************************************************
 * Implementation components for the hash database manager
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
#include "tkrzw_dbm_hash_impl.h"
#include "tkrzw_file.h"

namespace tkrzw {

HashRecord::HashRecord(File* file, int32_t offset_width, int32_t align_pow)
    : file_(file), offset_width_(offset_width), align_pow_(align_pow), body_buf_(nullptr) {}

HashRecord::~HashRecord() {
  delete[] body_buf_;
}

HashRecord::OperationType HashRecord::GetOperationType() const {
  return type_;
}

std::string_view HashRecord::GetKey() const {
  return std::string_view(key_ptr_, key_size_);
}

std::string_view HashRecord::GetValue() const {
  return std::string_view(value_ptr_, value_size_);
}

int64_t HashRecord::GetChildOffset() const {
  return child_offset_;
}

int32_t HashRecord::GetWholeSize() const {
  return whole_size_;
}

Status HashRecord::ReadMetadataKey(int64_t offset) {
  const int64_t min_record_size = sizeof(uint8_t) + offset_width_ + sizeof(uint8_t) * 3;
  int64_t record_size = file_->GetSizeSimple() - offset;
  if (record_size > READ_BUFFER_SIZE) {
    record_size = READ_BUFFER_SIZE;
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
  if (*(uint8_t*)rp == RECORD_MAGIC_VOID) {
    type_ = OP_VOID;
  } else if (*(uint8_t*)rp == RECORD_MAGIC_SET) {
    type_ = OP_SET;
  } else if (*(uint8_t*)rp == RECORD_MAGIC_REMOVE) {
    type_ = OP_REMOVE;
  } else {
    return Status(Status::BROKEN_DATA_ERROR, "invalid record magic number");
  }
  rp++;
  record_size--;
  if (record_size < offset_width_) {
    return Status(Status::BROKEN_DATA_ERROR, "invalid child offset");
  }
  child_offset_ = ReadFixNum(rp, offset_width_) << align_pow_;
  rp += offset_width_;
  record_size -= offset_width_;
  uint64_t num = 0;
  int32_t step = ReadVarNum(rp, record_size, &num);
  if (step < 1) {
    return Status(Status::BROKEN_DATA_ERROR, "invalid key size");
  }
  key_size_ = num;
  rp += step;
  record_size -= step;
  step = ReadVarNum(rp, record_size, &num);
  if (step < 1) {
    return Status(Status::BROKEN_DATA_ERROR, "invalid value size");
  }
  value_size_ = num;
  rp += step;
  record_size -= step;
  if (record_size < 1) {
    return Status(Status::BROKEN_DATA_ERROR, "invalid padding size");
  }
  padding_size_ = ReadFixNum(rp, 1);
  rp++;
  record_size--;
  header_size_ = rp - buffer_;
  whole_size_ = padding_size_ < PADDING_SIZE_MAGIC ?
                                header_size_ + key_size_ + value_size_ + padding_size_ : 0;
  key_ptr_ = nullptr;
  value_ptr_ = nullptr;
  body_offset_ = offset + header_size_;
  if (record_size >= key_size_) {
    key_ptr_ = rp;
    rp += key_size_;
    record_size -= key_size_;
    if (record_size >= value_size_) {
      value_ptr_ = rp;
    }
    rp += value_size_;
    record_size -= value_size_;
    if (padding_size_ >= PADDING_SIZE_MAGIC && record_size >= 4) {
      padding_size_ = ReadFixNum(rp, 4);
      if (padding_size_ < PADDING_SIZE_MAGIC) {
        return Status(Status::BROKEN_DATA_ERROR, "invalid padding size");
      }
      whole_size_ = header_size_ + key_size_ + value_size_ + padding_size_;
      rp += 4;
      record_size -= 4;
      if (record_size >= 1) {
        if (*(uint8_t*)rp != PADDING_TOP_MAGIC) {
          return Status(Status::BROKEN_DATA_ERROR, "invalid padding magic number");
        }
        rp += 1;
        record_size -= 1;
        if (record_size >= 1) {
          if (*(uint8_t*)rp != 0) {
            return Status(Status::BROKEN_DATA_ERROR, "invalid padding body data");
          }
        }
      }
    }
  } else {
    if (offset + whole_size_ > file_->GetSizeSimple()) {
      return Status(Status::BROKEN_DATA_ERROR, "invalid length of a record");
    }
    status = ReadBody();
    if (status != Status::SUCCESS) {
      return status;
    }
  }
  return Status(Status::SUCCESS);
}

Status HashRecord::ReadBody() {
  int32_t body_size = key_size_ + value_size_;
  if (whole_size_ == 0) {
    body_size += 6;
  }
  delete[] body_buf_;
  body_buf_ = new char[body_size];
  const Status status = file_->Read(body_offset_, body_buf_, body_size);
  if (status != Status::SUCCESS) {
    return status;
  }
  key_ptr_ = body_buf_;
  value_ptr_ = body_buf_ + key_size_;
  if (whole_size_ == 0) {
    const char* rp = body_buf_ + key_size_ + value_size_;
    padding_size_ = ReadFixNum(rp, 4);
    rp += 4;
    if (padding_size_ < PADDING_SIZE_MAGIC) {
      return Status(Status::BROKEN_DATA_ERROR, "invalid padding size");
    }
    if (*(uint8_t*)rp != PADDING_TOP_MAGIC) {
      return Status(Status::BROKEN_DATA_ERROR, "invalid padding magic data");
    }
    rp++;
    if (*(uint8_t*)rp != 0) {
      return Status(Status::BROKEN_DATA_ERROR, "invalid padding body data");
    }
    whole_size_ = header_size_ + key_size_ + value_size_ + padding_size_;
  }
  return Status(Status::SUCCESS);
}

void HashRecord::SetData(OperationType type, int32_t ideal_whole_size,
                         const char* key_ptr, int32_t key_size,
                         const char* value_ptr, int32_t value_size,
                         int64_t child_offset) {
  type_ = type;
  int32_t base_size = sizeof(uint8_t) + offset_width_ +
      SizeVarNum(key_size) + SizeVarNum(value_size) + sizeof(uint8_t) +
      key_size + value_size;
  whole_size_ = std::max(base_size, ideal_whole_size);
  const int32_t align = 1 << align_pow_;
  const int32_t diff = whole_size_ % align;
  if (diff > 0) {
    whole_size_ += align - diff;
  }
  key_size_ = key_size;
  value_size_ = value_size;
  padding_size_ = whole_size_ - base_size;
  child_offset_ = child_offset;
  key_ptr_ = key_ptr;
  value_ptr_ = value_ptr;
}

Status HashRecord::Write(int64_t offset, int64_t* new_offset) const {
  char stack[WRITE_BUFFER_SIZE];
  char* write_buf =
      whole_size_ > static_cast<int32_t>(sizeof(stack)) ? new char[whole_size_] : stack;
  char* wp = write_buf;
  switch (type_) {
    case OP_SET:
      *(wp++) = RECORD_MAGIC_SET;
      break;
    case OP_REMOVE:
      *(wp++) = RECORD_MAGIC_REMOVE;
      break;
    default:
      *(wp++) = RECORD_MAGIC_VOID;
      break;
  }
  WriteFixNum(wp, child_offset_ >> align_pow_, offset_width_);
  wp += offset_width_;
  wp += WriteVarNum(wp, key_size_);
  wp += WriteVarNum(wp, value_size_);
  if (padding_size_ >= PADDING_SIZE_MAGIC) {
    *(wp++) = PADDING_SIZE_MAGIC;
  } else {
    *(wp++) = padding_size_;
  }
  std::memcpy(wp, key_ptr_, key_size_);
  wp += key_size_;
  std::memcpy(wp, value_ptr_, value_size_);
  wp += value_size_;
  if (padding_size_ > 0) {
    std::memset(wp, 0, padding_size_);
    if (padding_size_ >= PADDING_SIZE_MAGIC) {
      WriteFixNum(wp, padding_size_, 4);
      wp += 4;
    }
    *wp = PADDING_TOP_MAGIC;
  }
  Status status(Status::SUCCESS);
  if (offset < 0) {
    status = file_->Append(write_buf, whole_size_, new_offset);
  } else {
    status = file_->Write(offset, write_buf, whole_size_);
  }
  if (write_buf != stack) {
    delete[] write_buf;
  }
  return status;
}

Status HashRecord::WriteChildOffset(int64_t offset, int64_t child_offset) {
  char buf[sizeof(uint64_t)];
  WriteFixNum(buf, child_offset >> align_pow_, offset_width_);
  offset += sizeof(uint8_t);
  return file_->Write(offset, buf, offset_width_);
}

Status HashRecord::FindNextOffset(int64_t offset, int64_t* next_offset) {
  const int64_t min_record_size = sizeof(uint8_t) + offset_width_ + sizeof(uint8_t) * 3;
  const int32_t align = 1 << align_pow_;
  offset += min_record_size;
  const int32_t diff = offset % align;
  if (diff > 0) {
    offset += align - diff;
  }
  int64_t file_size = file_->GetSizeSimple();
  HashRecord rec(file_, offset_width_, align_pow_);
  while (offset < file_size) {
    if (rec.ReadMetadataKey(offset) == Status::SUCCESS) {
      constexpr int32_t VALIDATION_COUNT = 3;
      constexpr int32_t MAX_REC_SIZE = 1 << 20;
      int32_t count = 0;
      while (offset < file_size && count < VALIDATION_COUNT) {
        if (rec.ReadMetadataKey(offset) != Status::SUCCESS) {
          break;
        }
        int32_t rec_size = rec.GetWholeSize();
        if (rec_size == 0) {
          if (rec.ReadBody() != Status::SUCCESS) {
            break;
          }
          rec_size = rec.GetWholeSize();
        }
        if (rec_size <= MAX_REC_SIZE && rec_size % align != 0) {
          break;
        }
        if (offset + rec_size == file_size) {
          count = VALIDATION_COUNT;
          break;
        }
        count++;
      }
      if (count >= VALIDATION_COUNT) {
        *next_offset = offset;
        return Status(Status::SUCCESS);
      }
    }
    offset += align;
  }
  return Status(Status::NOT_FOUND_ERROR);
}

Status HashRecord::ReplayOperations(
    File* file, DBM::RecordProcessor* proc,
    int64_t record_base, int32_t offset_width, int32_t align_pow,
    bool skip_broken_records, int64_t end_offset) {
  assert(file != nullptr && proc != nullptr && offset_width > 0);
  if (end_offset < 0) {
    end_offset = INT64MAX;
  }
  end_offset = std::min(end_offset, file->GetSizeSimple());
  int64_t offset = record_base;
  HashRecord rec(file, offset_width, align_pow);
  while (offset < end_offset) {
    Status status = rec.ReadMetadataKey(offset);
    if (status != Status::SUCCESS) {
      int64_t next_offset = 0;
      if (skip_broken_records) {
        if (rec.FindNextOffset(offset, &next_offset) == Status::SUCCESS) {
          offset = next_offset;
          continue;
        } else {
          break;
        }
      }
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
    std::string_view res;
    switch (rec.GetOperationType()) {
      case OP_SET: {
        std::string_view value = rec.GetValue();
        if (value.data() == nullptr) {
          status = rec.ReadBody();
          if (status != Status::SUCCESS) {
            return status;
          }
          value = rec.GetValue();
        }
        res = proc->ProcessFull(key, value);
        break;
      }
      case OP_REMOVE: {
        res = proc->ProcessEmpty(key);
        break;
      }
      default: {
        res = DBM::RecordProcessor::NOOP;
        break;
      }
    }
    if (res.data() != DBM::RecordProcessor::NOOP.data()) {
      return Status(Status::CANCELED_ERROR);
    }
    offset += rec_size;
  }
  return Status(Status::SUCCESS);
}

Status HashRecord::ExtractOffsets(
    File* in_file, File* out_file,
    int64_t record_base, int32_t offset_width, int32_t align_pow,
    bool skip_broken_records, int64_t end_offset) {
  assert(in_file != nullptr && out_file != nullptr && offset_width > 0);
  if (end_offset < 0) {
    end_offset = INT64MAX;
  }
  end_offset = std::min(end_offset, in_file->GetSizeSimple());
  int64_t offset = record_base;
  HashRecord rec(in_file, offset_width, align_pow);
  char buf[WRITE_BUFFER_SIZE];
  const char* ep = buf + WRITE_BUFFER_SIZE - offset_width;
  char* wp = buf;
  while (offset < end_offset) {
    Status status = rec.ReadMetadataKey(offset);
    if (status != Status::SUCCESS) {
      int64_t next_offset = 0;
      if (skip_broken_records) {
        if (rec.FindNextOffset(offset, &next_offset) == Status::SUCCESS) {
          offset = next_offset;
          continue;
        } else {
          break;
        }
      }
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
    if (rec.GetOperationType() != OP_VOID) {
      WriteFixNum(wp, offset >> align_pow, offset_width);
      wp += offset_width;
      if (wp > ep) {
        status = out_file->Append(buf, wp - buf);
        if (status != Status::SUCCESS) {
          return status;
        }
        wp = buf;
      }
    }
    offset += rec_size;
  }
  if (wp > buf) {
    const Status status = out_file->Append(buf, wp - buf);
    if (status != Status::SUCCESS) {
      return status;
    }
  }
  return Status(Status::SUCCESS);
}

OffsetReader::OffsetReader(File* file, int32_t offset_width, int32_t align_pow, bool reversal)
    : file_(file), offset_width_(offset_width), align_pow_(align_pow), reversal_(reversal),
      current_ptr_(nullptr), end_ptr_(nullptr), current_offset_(0) {
  if (reversal) {
    current_offset_ = file_->GetSizeSimple();
    current_offset_ -= current_offset_ % offset_width;
  }
}

Status OffsetReader::ReadOffset(int64_t* offset) {
  if (reversal_) {
    if (current_ptr_ == end_ptr_) {
      int32_t buffer_size = READ_BUFFER_SIZE;
      buffer_size -= buffer_size % offset_width_;
      buffer_size = std::min<int64_t>(current_offset_, buffer_size);
      if (buffer_size < offset_width_) {
        return Status(Status::NOT_FOUND_ERROR);
      }
      current_offset_ -= buffer_size;
      const Status status = file_->Read(current_offset_, buffer_, buffer_size);
      if (status != Status::SUCCESS) {
        return status;
      }
      current_ptr_ = buffer_;
      end_ptr_ = current_ptr_ + buffer_size;
    }
    end_ptr_ -= offset_width_;
    *offset = ReadFixNum(end_ptr_, offset_width_) << align_pow_;
  } else {
    if (current_ptr_ == end_ptr_) {
      const int64_t file_size = file_->GetSizeSimple();
      if (current_offset_ < file_size) {
        int32_t buffer_size = std::min<int64_t>(file_size - current_offset_, READ_BUFFER_SIZE);
        if (buffer_size < offset_width_) {
          return Status(Status::NOT_FOUND_ERROR);
        }
        buffer_size -= buffer_size % offset_width_;
        const Status status = file_->Read(current_offset_, buffer_, buffer_size);
        if (status != Status::SUCCESS) {
          return status;
        }
        current_ptr_ = buffer_;
        end_ptr_ = current_ptr_ + buffer_size;
        current_offset_ += buffer_size;
      } else {
        return Status(Status::NOT_FOUND_ERROR);
      }
    }
    *offset = ReadFixNum(current_ptr_, offset_width_) << align_pow_;
    current_ptr_ += offset_width_;
  }
  return Status(Status::SUCCESS);
}

FreeBlockPool::FreeBlockPool(int32_t capacity) : capacity_(capacity), data_(), mutex_() {}

void FreeBlockPool::SetCapacity(int32_t capacity) {
  std::lock_guard<std::mutex> lock(mutex_);
  capacity_ = capacity;
  while (static_cast<int32_t>(data_.size()) > capacity_) {
    data_.erase(data_.begin());
  }
}

void FreeBlockPool::Clear() {
  std::lock_guard<std::mutex> lock(mutex_);
  data_.clear();
}

void FreeBlockPool::InsertFreeBlock(int64_t offset, int32_t size) {
  std::lock_guard<std::mutex> lock(mutex_);
  if (static_cast<int32_t>(data_.size()) >= capacity_) {
    auto it = data_.begin();
    if (size <= it->size) return;
    data_.erase(it);
  }
  data_.emplace(FreeBlock(offset, size));
}

bool FreeBlockPool::FetchFreeBlock(int32_t min_size, FreeBlock* res) {
  std::lock_guard<std::mutex> lock(mutex_);
  const auto it = data_.lower_bound(FreeBlock(0, min_size));
  if (it == data_.end()) {
    return false;
  }
  *res = *it;
  data_.erase(it);
  return true;
}

int32_t FreeBlockPool::Size() {
  std::lock_guard<std::mutex> lock(mutex_);
  return data_.size();
}

std::string FreeBlockPool::Serialize(int32_t offset_width, int32_t align_pow, int32_t size) {
  std::lock_guard<std::mutex> lock(mutex_);
  const int32_t unit_size = offset_width + sizeof(uint32_t);
  const int32_t max_num = std::min<int32_t>(size / unit_size, data_.size());
  std::string str(size, 0);
  char* wp = const_cast<char*>(str.data());
  int32_t num = 0;
  for (auto it = data_.rbegin(); it != data_.rend() && num < max_num; ++it) {
    WriteFixNum(wp, it->offset >> align_pow, offset_width);
    wp += offset_width;
    WriteFixNum(wp, it->size, sizeof(uint32_t));
    wp += sizeof(uint32_t);
    num++;
  }
  std::memset(wp, 0, size - (wp - str.data()));
  return str;
}

void FreeBlockPool::Deserialize(std::string_view str, int32_t offset_width, int32_t align_pow) {
  std::lock_guard<std::mutex> lock(mutex_);
  data_.clear();
  const int32_t unit_size = offset_width + sizeof(uint32_t);
  const int32_t max_num = std::min<int32_t>(str.size() / unit_size, capacity_);
  const char* rp = str.data();
  for (int32_t num = 0; num < max_num; num++) {
    const int64_t offset = ReadFixNum(rp, offset_width) << align_pow;
    rp += offset_width;
    const int32_t size = ReadFixNum(rp, sizeof(uint32_t));
    rp += sizeof(uint32_t);
    if (size < 1) {
      break;
    }
    data_.emplace(FreeBlock(offset, size));
  }
}

}  // namespace tkrzw

// END OF FILE
