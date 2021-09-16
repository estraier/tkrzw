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
#include "tkrzw_dbm_common_impl.h"
#include "tkrzw_dbm_hash_impl.h"
#include "tkrzw_file.h"
#include "tkrzw_hash_util.h"

namespace tkrzw {

HashRecord::HashRecord(File* file, int32_t crc_width, int32_t offset_width, int32_t align_pow)
    : file_(file), crc_width_(crc_width), offset_width_(offset_width), align_pow_(align_pow),
      ext_meta_buf_(nullptr), body_buf_(nullptr) {}

HashRecord::~HashRecord() {
  xfree(body_buf_);
  xfree(ext_meta_buf_);
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

Status HashRecord::ReadMetadataKey(int64_t offset, int32_t min_read_size) {
  const int64_t min_record_size =
      sizeof(uint8_t) + offset_width_ + sizeof(uint8_t) * 3 + crc_width_;
  const int64_t max_read_size = file_->GetSizeSimple() - offset;
  int64_t record_size = max_read_size;
  if (record_size < min_record_size) {
    return Status(Status::BROKEN_DATA_ERROR, "too short record data");
  }
  if (record_size > min_read_size) {
    record_size = min_read_size;
  }
  char* read_buf = meta_buf_;
  if (record_size > META_BUFFER_SIZE) {
    ext_meta_buf_ = static_cast<char*>(xrealloc(ext_meta_buf_, record_size));
    read_buf = ext_meta_buf_;
  }
  Status status = file_->Read(offset, read_buf, record_size);
  if (status != Status::SUCCESS) {
    return status;
  }
  const char* rp = read_buf;
  uint32_t magic = *(uint8_t*)rp;
  magic_checksum_ = magic & ~RECORD_MAGIC_VOID;
  if (magic_checksum_ < 3) {
    return Status(Status::BROKEN_DATA_ERROR, "invalid magic checksum");
  }
  magic &= RECORD_MAGIC_VOID;
  if (magic == RECORD_MAGIC_VOID) {
    type_ = OP_VOID;
  } else if (magic == RECORD_MAGIC_SET) {
    type_ = OP_SET;
  } else if (magic == RECORD_MAGIC_REMOVE) {
    type_ = OP_REMOVE;
  } else {
    type_ = OP_ADD;
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
  if (num > MAX_KEY_SIZE || num > static_cast<uint64_t>(max_read_size)) {
    return Status(Status::BROKEN_DATA_ERROR, "too large key size");
  }
  key_size_ = num;
  rp += step;
  record_size -= step;
  step = ReadVarNum(rp, record_size, &num);
  if (step < 1) {
    return Status(Status::BROKEN_DATA_ERROR, "invalid value size");
  }
  if (num > MAX_VALUE_SIZE || num > static_cast<uint64_t>(max_read_size)) {
    return Status(Status::BROKEN_DATA_ERROR, "too large value size");
  }
  value_size_ = num;
  rp += step;
  record_size -= step;
  int32_t extra_padding_size = 0;
  step = ReadVarNum(rp, record_size, &num);
  if (step < 1) {
    return Status(Status::BROKEN_DATA_ERROR, "invalid padding size");
  }
  if (num > MAX_VALUE_SIZE || num > static_cast<uint64_t>(max_read_size)) {
    return Status(Status::BROKEN_DATA_ERROR, "too large padding size");
  }
  padding_size_ = num;
  rp += step;
  record_size -= step;
  extra_padding_size  = step - 1;
  if (record_size < crc_width_) {
    return Status(Status::BROKEN_DATA_ERROR, "invalid CRC value");
  }
  crc_value_ = ReadFixNum(rp, crc_width_);
  rp += crc_width_;
  record_size -= crc_width_;
  header_size_ = rp - read_buf;
  whole_size_ = header_size_ + key_size_ + value_size_ + padding_size_ - extra_padding_size;
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
    if (padding_size_ > 0 && record_size > 0) {
      if (*(uint8_t*)rp != PADDING_TOP_MAGIC) {
        return Status(Status::BROKEN_DATA_ERROR, "invalid padding magic number");
      }
      if (padding_size_ > 1 && record_size > 1) {
        if (*(uint8_t*)(rp + 1) != 0) {
          return Status(Status::BROKEN_DATA_ERROR, "invalid padding content");
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
  int64_t body_size = key_size_ + value_size_;
  body_buf_ = static_cast<char*>(xrealloc(body_buf_, body_size));
  const Status status = file_->Read(body_offset_, body_buf_, body_size);
  if (status != Status::SUCCESS) {
    return status;
  }
  key_ptr_ = body_buf_;
  value_ptr_ = body_buf_ + key_size_;
  return Status(Status::SUCCESS);
}

Status HashRecord::CheckCRC() {
  if (value_ptr_ == nullptr) {
    const Status status = ReadBody();
    if (status != Status::SUCCESS) {
      return status;
    }
  }
  const uint32_t act_magic_checksum =
      MagicChecksum(key_ptr_, key_size_, value_ptr_, value_size_);
  if (magic_checksum_ != act_magic_checksum) {
    return Status(Status::BROKEN_DATA_ERROR, "inconsistent magic checksum");
  }
  if (crc_width_ > 0) {
    uint32_t act_crc_value = 0;
    switch (crc_width_) {
      case 1:
        act_crc_value = HashCRC8Continuous(
            value_ptr_, value_size_, true, HashCRC8Continuous(key_ptr_, key_size_, false));
        break;
      case 2:
        act_crc_value = HashCRC16Continuous(
            value_ptr_, value_size_, true, HashCRC16Continuous(key_ptr_, key_size_, false));
        break;
      case 4:
        act_crc_value = HashCRC32Continuous(
            value_ptr_, value_size_, true, HashCRC32Continuous(key_ptr_, key_size_, false));
        break;
    }
    if (crc_value_ != act_crc_value) {
      return Status(Status::BROKEN_DATA_ERROR, "inconsistent extra CRC");
    }
  }
  return Status(Status::SUCCESS);
}

void HashRecord::SetData(OperationType type, int32_t ideal_whole_size,
                         const char* key_ptr, int32_t key_size,
                         const char* value_ptr, int32_t value_size,
                         int64_t child_offset) {
  type_ = type;
  int32_t base_size = sizeof(uint8_t) + offset_width_ +
      SizeVarNum(key_size) + SizeVarNum(value_size) + sizeof(uint8_t) + crc_width_ +
      key_size + value_size;
  whole_size_ = std::max(base_size, ideal_whole_size);
  whole_size_ = AlignNumber(whole_size_, 1 << align_pow_);
  key_size_ = key_size;
  value_size_ = value_size;
  padding_size_ = whole_size_ - base_size;
  child_offset_ = child_offset;
  magic_checksum_ = MagicChecksum(key_ptr, key_size, value_ptr, value_size);
  switch (crc_width_) {
    default:
      crc_value_ = 0;
      break;
    case 1: crc_value_ = HashCRC8Continuous(
        value_ptr, value_size, true, HashCRC8Continuous(key_ptr, key_size, false));
      break;
    case 2: crc_value_ = HashCRC16Continuous(
        value_ptr, value_size, true, HashCRC16Continuous(key_ptr, key_size, false));
      break;
    case 4: crc_value_ = HashCRC32Continuous(
        value_ptr, value_size, true, HashCRC32Continuous(key_ptr, key_size, false));
      break;
  }
  key_ptr_ = key_ptr;
  value_ptr_ = value_ptr;
}

Status HashRecord::Write(int64_t offset, int64_t* new_offset) const {
  char stack[WRITE_BUFFER_SIZE];
  char* write_buf = whole_size_ > static_cast<int32_t>(sizeof(stack)) ?
      static_cast<char*>(xmalloc(whole_size_)) : stack;
  char* wp = write_buf;
  uint32_t magic = magic_checksum_;
  switch (type_) {
    case OP_VOID:
      magic |= RECORD_MAGIC_VOID;
      break;
    case OP_SET:
      magic |= RECORD_MAGIC_SET;
      break;
    case OP_REMOVE:
      magic |= RECORD_MAGIC_REMOVE;
      break;
    default:
      break;
  }
  *(wp++) = magic;
  WriteFixNum(wp, child_offset_ >> align_pow_, offset_width_);
  wp += offset_width_;
  wp += WriteVarNum(wp, key_size_);
  wp += WriteVarNum(wp, value_size_);
  wp += WriteVarNum(wp, padding_size_);
  if (crc_width_ > 0) {
    WriteFixNum(wp, crc_value_, crc_width_);
    wp += crc_width_;
  }
  std::memcpy(wp, key_ptr_, key_size_);
  wp += key_size_;
  std::memcpy(wp, value_ptr_, value_size_);
  wp += value_size_;
  if (padding_size_ > 0) {
    std::memset(wp, 0, padding_size_ - SizeVarNum(padding_size_) + 1);
    *wp = PADDING_TOP_MAGIC;
  }
  Status status(Status::SUCCESS);
  if (offset < 0) {
    status = file_->Append(write_buf, whole_size_, new_offset);
  } else {
    status = file_->Write(offset, write_buf, whole_size_);
  }
  if (write_buf != stack) {
    xfree(write_buf);
  }
  return status;
}

Status HashRecord::WriteChildOffset(int64_t offset, int64_t child_offset) {
  char buf[sizeof(uint64_t)];
  WriteFixNum(buf, child_offset >> align_pow_, offset_width_);
  offset += sizeof(uint8_t);
  return file_->Write(offset, buf, offset_width_);
}

Status HashRecord::FindNextOffset(int64_t offset, int32_t min_read_size, int64_t* next_offset) {
  constexpr int32_t MAX_SHIFT_TRIES = 1000;
  constexpr int32_t VALIDATION_COUNT = 3;
  constexpr int32_t MAX_REC_SIZE = 1 << 20;
  const int64_t min_record_size =
      sizeof(uint8_t) + offset_width_ + sizeof(uint8_t) * 3 + crc_width_;
  const int32_t align = 1 << align_pow_;
  offset += min_record_size;
  offset = AlignNumber(offset, align);
  int64_t file_size = file_->GetSizeSimple();
  HashRecord rec(file_, crc_width_, offset_width_, align_pow_);
  int32_t num_shift_tries = MAX_SHIFT_TRIES;
  while (num_shift_tries-- > 0 && offset < file_size) {
    if (rec.ReadMetadataKey(offset, min_read_size) == Status::SUCCESS &&
        rec.CheckCRC() == Status::SUCCESS) {
      int32_t count = 0;
      int64_t forward_offset = offset + rec.GetWholeSize();
      while (forward_offset < file_size && count < VALIDATION_COUNT &&
             rec.ReadMetadataKey(forward_offset, min_read_size) == Status::SUCCESS &&
             rec.CheckCRC() == Status::SUCCESS) {
        const int32_t rec_size = rec.GetWholeSize();
        if (rec_size <= MAX_REC_SIZE && rec_size % align != 0) {
          break;
        }
        if (forward_offset + rec_size == file_size) {
          count = VALIDATION_COUNT;
          break;
        }
        count++;
        forward_offset += rec_size;
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
    int64_t record_base, int32_t crc_width, Compressor* compressor,
    int32_t offset_width, int32_t align_pow,
    int32_t min_read_size, bool skip_broken_records, int64_t end_offset) {
  assert(file != nullptr && proc != nullptr && offset_width > 0);
  if (end_offset < 0) {
    end_offset = INT64MAX;
  }
  end_offset = std::min(end_offset, file->GetSizeSimple());
  int64_t offset = record_base;
  HashRecord rec(file, crc_width, offset_width, align_pow);
  ScopedStringView comp_data_placeholder;
  while (offset < end_offset) {
    Status status = rec.ReadMetadataKey(offset, min_read_size);
    if (status == Status::SUCCESS) {
      status = rec.CheckCRC();
    }
    if (status != Status::SUCCESS) {
      int64_t next_offset = 0;
      if (skip_broken_records) {
        if (rec.FindNextOffset(offset, min_read_size, &next_offset) == Status::SUCCESS) {
          offset = next_offset;
          continue;
        } else {
          break;
        }
      }
      return status;
    }
    const int64_t rec_size = rec.GetWholeSize();
    const std::string_view key = rec.GetKey();
    std::string_view res;
    switch (rec.GetOperationType()) {
      case OP_SET:
      case OP_ADD: {
        std::string_view value = rec.GetValue();
        if (value.data() == nullptr) {
          status = rec.ReadBody();
          if (status != Status::SUCCESS) {
            if (skip_broken_records) {
              offset += rec_size;
              continue;
            }
            return status;
          }
          value = rec.GetValue();
        }
        if (compressor != nullptr) {
          size_t decomp_size = 0;
          char* decomp_buf = compressor->Decompress(value.data(), value.size(), &decomp_size);
          if (decomp_buf == nullptr) {
            if (skip_broken_records) {
              offset += rec_size;
              continue;
            } else {
              return Status(Status::BROKEN_DATA_ERROR, "decompression failed");
            }
          }
          comp_data_placeholder.Set(decomp_buf, decomp_size);
          value = comp_data_placeholder.Get();
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
    int64_t record_base, int32_t crc_width, int32_t offset_width, int32_t align_pow,
    bool skip_broken_records, int64_t end_offset) {
  assert(in_file != nullptr && out_file != nullptr && offset_width > 0);
  if (end_offset < 0) {
    end_offset = INT64MAX;
  }
  end_offset = std::min(end_offset, in_file->GetSizeSimple());
  int64_t offset = record_base;
  HashRecord rec(in_file, crc_width, offset_width, align_pow);
  char buf[WRITE_BUFFER_SIZE];
  const char* ep = buf + WRITE_BUFFER_SIZE - offset_width;
  char* wp = buf;
  while (offset < end_offset) {
    Status status = rec.ReadMetadataKey(offset, META_MIN_READ_SIZE);
    if (status != Status::SUCCESS) {
      int64_t next_offset = 0;
      if (skip_broken_records) {
        if (rec.FindNextOffset(offset, META_MIN_READ_SIZE, &next_offset) == Status::SUCCESS) {
          offset = next_offset;
          continue;
        } else {
          break;
        }
      }
      return status;
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
    offset += rec.GetWholeSize();
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
  std::lock_guard<SpinMutex> lock(mutex_);
  capacity_ = capacity;
  while (static_cast<int32_t>(data_.size()) > capacity_) {
    data_.erase(data_.begin());
  }
}

void FreeBlockPool::Clear() {
  std::lock_guard<SpinMutex> lock(mutex_);
  data_.clear();
}

void FreeBlockPool::InsertFreeBlock(int64_t offset, int32_t size) {
  std::lock_guard<SpinMutex> lock(mutex_);
  if (static_cast<int32_t>(data_.size()) >= capacity_) {
    auto it = data_.begin();
    if (size <= it->size) return;
    data_.erase(it);
  }
  data_.emplace(FreeBlock(offset, size));
}

bool FreeBlockPool::FetchFreeBlock(int32_t min_size, FreeBlock* res) {
  std::lock_guard<SpinMutex> lock(mutex_);
  const auto it = data_.lower_bound(FreeBlock(0, min_size));
  if (it == data_.end()) {
    return false;
  }
  *res = *it;
  data_.erase(it);
  return true;
}

int32_t FreeBlockPool::Size() {
  std::lock_guard<SpinMutex> lock(mutex_);
  return data_.size();
}

std::string FreeBlockPool::Serialize(int32_t offset_width, int32_t align_pow, int32_t size) {
  std::lock_guard<SpinMutex> lock(mutex_);
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
  std::lock_guard<SpinMutex> lock(mutex_);
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

std::string_view CallRecordProcessFull(
    DBM::RecordProcessor* proc, std::string_view key, std::string_view old_value,
    std::string_view* new_value_orig,
    Compressor* compressor, ScopedStringView* comp_data_placeholder) {
  if (old_value.data() == nullptr) {
    old_value = std::string_view("", 0);
  } else if (compressor != nullptr) {
    size_t decomp_size = 0;
    char* decomp_buf =
        compressor->Decompress(old_value.data(), old_value.size(), &decomp_size);
    if (decomp_buf == nullptr) {
      return std::string_view();
    }
    comp_data_placeholder->Set(decomp_buf, decomp_size);
    old_value = comp_data_placeholder->Get();
  }
  std::string_view new_value = proc->ProcessFull(key, old_value);
  *new_value_orig = new_value;
  if (compressor != nullptr && new_value.data() != DBM::RecordProcessor::NOOP.data() &&
      new_value.data() != DBM::RecordProcessor::REMOVE.data()) {
    size_t comp_size = 0;
    char* comp_buf =
        compressor->Compress(new_value.data(), new_value.size(), &comp_size);
    if (comp_buf == nullptr) {
      return std::string_view();
    }
    comp_data_placeholder->Set(comp_buf, comp_size);
    new_value = comp_data_placeholder->Get();
  }
  return new_value;
}

std::string_view CallRecordProcessEmpty(
    DBM::RecordProcessor* proc, std::string_view key,
    std::string_view* new_value_orig,
    Compressor* compressor, ScopedStringView* comp_data_placeholder) {
  std::string_view new_value = proc->ProcessEmpty(key);
  *new_value_orig = new_value;
  if (compressor != nullptr && new_value.data() != DBM::RecordProcessor::NOOP.data() &&
      new_value.data() != DBM::RecordProcessor::REMOVE.data()) {
    size_t comp_size = 0;
    char* comp_buf =
        compressor->Compress(new_value.data(), new_value.size(), &comp_size);
    if (comp_buf == nullptr) {
      return std::string_view();
    }
    comp_data_placeholder->Set(comp_buf, comp_size);
    new_value = comp_data_placeholder->Get();
  }
  return new_value;
}

}  // namespace tkrzw

// END OF FILE
