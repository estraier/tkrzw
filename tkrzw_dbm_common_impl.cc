/*************************************************************************************************
 * Common implementation components for database managers
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

#include "tkrzw_containers.h"
#include "tkrzw_dbm.h"
#include "tkrzw_dbm_common_impl.h"
#include "tkrzw_file.h"
#include "tkrzw_file_util.h"
#include "tkrzw_str_util.h"
#include "tkrzw_sys_config.h"

namespace tkrzw {

uint64_t PrimaryHash(std::string_view data, uint64_t num_buckets) {
  constexpr uint64_t seed = 19780211;
  uint64_t hash = HashMurmur(data, seed);
  if (num_buckets <= UINT32MAX) {
    hash = (((hash & 0xffff000000000000ULL) >> 48) | ((hash & 0x0000ffff00000000ULL) >> 16)) ^
        (((hash & 0x000000000000ffffULL) << 16) | ((hash & 0x00000000ffff0000ULL) >> 16));
  }
  return hash % num_buckets;
}

uint64_t SecondaryHash(std::string_view data, uint64_t num_shards) {
  uint64_t hash = HashFNV(data);
  if (num_shards <= UINT32MAX) {
    hash = (((hash & 0xffff000000000000ULL) >> 48) | ((hash & 0x0000ffff00000000ULL) >> 16)) ^
        (((hash & 0x000000000000ffffULL) << 16) | ((hash & 0x00000000ffff0000ULL) >> 16));
  }
  return hash % num_shards;
}

uint64_t IsPrimeNumber(uint64_t num) {
  if (num < 2) {
    return false;
  }
  if (num == 2) {
    return true;
  }
  if (num % 2 == 0) {
    return false;
  }
  const uint64_t sq = std::sqrt(num);
  for (uint64_t d = 3; d <= sq; d += 2) {
    if (num % d == 0) {
      return false;
    }
  }
  return true;
}

int64_t GetHashBucketSize(int64_t min_size) {
  if (min_size <= 100) {
    return std::max(min_size, static_cast<int64_t>(1));
  }
  int64_t num = min_size;
  while (num < INT64MAX) {
    if (IsPrimeNumber(num)) {
      return num;
    }
    num++;
  }
  return min_size;
}

Status SearchDBM(
    DBM* dbm, std::string_view pattern, std::vector<std::string>* matched, size_t capacity,
    bool (*matcher)(std::string_view, std::string_view)) {
  assert(dbm != nullptr && matched != nullptr && matcher != nullptr);
  if (capacity == 0) {
    capacity = SIZE_MAX;
  }
  matched->clear();
  Status impl_status(Status::SUCCESS);
  class Exporter final : public DBM::RecordProcessor {
   public:
    Exporter(Status* impl_status, std::string_view pattern,
             std::vector<std::string>* matched, size_t capacity,
             bool (*matcher)(std::string_view, std::string_view))
        : impl_status_(impl_status), pattern_(pattern), matched_(matched),
          capacity_(capacity), matcher_(matcher) {}
    std::string_view ProcessFull(std::string_view key, std::string_view value) override {
      if (matched_->size() < capacity_ && matcher_(key, pattern_)) {
        matched_->emplace_back(std::string(key));
      }
      return NOOP;
    }
   private:
    Status* impl_status_;
    std::string_view pattern_;
    std::vector<std::string>* matched_;
    size_t capacity_;
    bool (*matcher_)(std::string_view, std::string_view);
  } exporter(&impl_status, pattern, matched, capacity, matcher);
  const Status status = dbm->ProcessEach(&exporter, false);
  if (status != Status::SUCCESS) {
    return status;
  }
  return impl_status;
}

Status SearchDBMForwardMatch(
    DBM* dbm, std::string_view pattern, std::vector<std::string>* matched, size_t capacity) {
  assert(dbm != nullptr && matched != nullptr);
  if (!dbm->IsOrdered()) {
    return SearchDBM(dbm, pattern, matched, capacity, StrBeginsWith);
  }
  if (capacity == 0) {
    capacity = SIZE_MAX;
  }
  matched->clear();
  auto iter = dbm->MakeIterator();
  Status status = iter->Jump(pattern);
  if (status != Status::SUCCESS) {
    return status;
  }
  while (matched->size() < capacity) {
    std::string key;
    status = iter->Get(&key);
    if (status != Status::SUCCESS) {
      if (status != Status::NOT_FOUND_ERROR) {
        return status;
      }
      break;
    }
    if (!StrBeginsWith(key, pattern)) {
      break;
    }
    matched->emplace_back(std::move(key));
    iter->Next();
  }
  return Status(Status::SUCCESS);
}

Status SearchDBMRegex(
    DBM* dbm, std::string_view pattern, std::vector<std::string>* matched, size_t capacity,
    bool utf) {
  assert(dbm != nullptr && matched != nullptr);
  std::unique_ptr<std::regex> regex;
  std::unique_ptr<std::wregex> regex_wide;
  try {
    if (utf) {
      const std::wstring pattern_wide = ConvertUTF8ToWide(pattern);
      regex_wide = std::make_unique<std::wregex>(pattern_wide);
    } else {
      regex = std::make_unique<std::regex>(pattern.begin(), pattern.end());
    }
  } catch (const std::regex_error& err) {
    return Status(Status::INVALID_ARGUMENT_ERROR, StrCat("invalid regex: ", err.what()));
  }
  if (capacity == 0) {
    capacity = SIZE_MAX;
  }
  matched->clear();
  Status impl_status(Status::SUCCESS);
  class Exporter final : public DBM::RecordProcessor {
   public:
    Exporter(Status* impl_status, std::string_view pattern,
             std::vector<std::string>* matched, size_t capacity,
             std::regex* regex, std::wregex* regex_wide)
        : impl_status_(impl_status), pattern_(pattern), matched_(matched),
          capacity_(capacity), regex_(regex), regex_wide_(regex_wide) {}
    std::string_view ProcessFull(std::string_view key, std::string_view value) override {
      if (matched_->size() >= capacity_) {
        return NOOP;
      }
      if (regex_wide_ == nullptr) {
        if (std::regex_search(key.begin(), key.end(), *regex_)) {
          matched_->emplace_back(std::string(key));
        }
      } else {
        const std::wstring key_wide = ConvertUTF8ToWide(key);
        if (std::regex_search(key_wide, *regex_wide_)) {
          matched_->emplace_back(std::string(key));
        }
      }
      return NOOP;
    }
   private:
    Status* impl_status_;
    std::string_view pattern_;
    std::vector<std::string>* matched_;
    size_t capacity_;
    std::regex* regex_;
    std::wregex* regex_wide_;
  } exporter(&impl_status, pattern, matched, capacity, regex.get(), regex_wide.get());
  const Status status = dbm->ProcessEach(&exporter, false);
  if (status != Status::SUCCESS) {
    return status;
  }
  return impl_status;
}

Status SearchDBMEditDistance(
    DBM* dbm, std::string_view pattern, std::vector<std::string>* matched, size_t capacity,
    bool utf) {
  assert(dbm != nullptr && matched != nullptr);
  if (capacity == 0) {
    capacity = SIZE_MAX;
  }
  std::vector<uint32_t> pattern_ucs;
  if (utf) {
    pattern_ucs = ConvertUTF8ToUCS4(pattern);
  }
  Status impl_status(Status::SUCCESS);
  std::vector<std::pair<int32_t, std::string>> heap;
  class Exporter final : public DBM::RecordProcessor {
   public:
    Exporter(Status* impl_status, std::string_view pattern,
             std::vector<std::pair<int32_t, std::string>>* heap, size_t capacity,
             const std::vector<uint32_t>* pattern_ucs)
        : impl_status_(impl_status), pattern_(pattern), heap_(heap), capacity_(capacity),
          pattern_ucs_(pattern_ucs) {}
    std::string_view ProcessFull(std::string_view key, std::string_view value) override {
      int32_t dist = 0;
      if (pattern_ucs_ == nullptr) {
        dist = EditDistanceLev(pattern_, key);
      } else {
        const std::vector<uint32_t> key_ucs = ConvertUTF8ToUCS4(key);
        dist = EditDistanceLev(*pattern_ucs_, key_ucs);
      }
      HeapByCostAdd(dist, std::string(key), capacity_, heap_);
      return NOOP;
    }
   private:
    Status* impl_status_;
    std::string_view pattern_;
    std::vector<std::pair<int32_t, std::string>>* heap_;
    size_t capacity_;
    const std::vector<uint32_t>* pattern_ucs_;
  } exporter(&impl_status, pattern, &heap, capacity, utf ? &pattern_ucs : nullptr);
  const Status status = dbm->ProcessEach(&exporter, false);
  if (status != Status::SUCCESS) {
    return status;
  }
  HeapByCostFinish(&heap);
  matched->clear();
  for (const auto& rec : heap) {
    matched->emplace_back(rec.second);
  }
  return impl_status;
}

Status ExportDBMRecordsToFlatRecords(DBM* dbm, File* file) {
  assert(dbm != nullptr && file != nullptr);
  Status status = file->Truncate(0);
  if (status != Status::SUCCESS) {
    return status;
  }
  Status impl_status(Status::SUCCESS);
  class Exporter final : public DBM::RecordProcessor {
   public:
    Exporter(Status* impl_status, File* file) : impl_status_(impl_status), file_(file) {}
    std::string_view ProcessFull(std::string_view key, std::string_view value) override {
      FlatRecord rec(file_);
      *impl_status_ |= rec.Write(key);
      *impl_status_ |= rec.Write(value);
      return NOOP;
    }
   private:
    Status* impl_status_;
    File* file_;
  } exporter(&impl_status, file);
  status = dbm->ProcessEach(&exporter, false);
  if (status != Status::SUCCESS) {
    return status;
  }
  return impl_status;
}

Status ImportDBMRecordsFromFlatRecords(DBM* dbm, File* file) {
  assert(dbm != nullptr && file != nullptr);
  int64_t end_offset = 0;
  Status status = file->GetSize(&end_offset);
  if (status != Status::SUCCESS) {
    return status;
  }
  FlatRecordReader reader(file);
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
    status = dbm->Set(key_store, value);
    if (status != Status::SUCCESS) {
      return status;
    }
  }
  return Status(Status::SUCCESS);
}

Status ExportDBMKeysToFlatRecords(DBM* dbm, File* file) {
  assert(dbm != nullptr && file != nullptr);
  Status status = file->Truncate(0);
  if (status != Status::SUCCESS) {
    return status;
  }
  Status impl_status(Status::SUCCESS);
  class Exporter final : public DBM::RecordProcessor {
   public:
    Exporter(Status* impl_status, File* file) : impl_status_(impl_status), file_(file) {}
    std::string_view ProcessFull(std::string_view key, std::string_view value) override {
      FlatRecord rec(file_);
      *impl_status_ |= rec.Write(key);
      return NOOP;
    }
   private:
    Status* impl_status_;
    File* file_;
  } exporter(&impl_status, file);
  status = dbm->ProcessEach(&exporter, false);
  if (status != Status::SUCCESS) {
    return status;
  }
  return impl_status;
}

Status ExportDBMRecordsToTSV(DBM* dbm, File* file, bool escape) {
  assert(dbm != nullptr && file != nullptr);
  Status status = file->Truncate(0);
  if (status != Status::SUCCESS) {
    return status;
  }
  Status impl_status(Status::SUCCESS);
  class Exporter final : public DBM::RecordProcessor {
   public:
    Exporter(Status* impl_status, File* file, bool escape)
        : impl_status_(impl_status), file_(file), escape_(escape) {}
    std::string_view ProcessFull(std::string_view key, std::string_view value) override {
      const std::string& esc_key = escape_ ? StrEscapeC(key) : StrTrimForTSV(key);
      const std::string& esc_value = escape_ ? StrEscapeC(value) : StrTrimForTSV(value);
      const std::string& line = StrCat(esc_key, "\t", esc_value, "\n");
      *impl_status_ |= file_->Append(line.data(), line.size());
      return NOOP;
    }
   private:
    Status* impl_status_;
    File* file_;
    bool escape_;
  } exporter(&impl_status, file, escape);
  status = dbm->ProcessEach(&exporter, false);
  if (status != Status::SUCCESS) {
    return status;
  }
  return impl_status;
}

Status ImportDBMRecordsFromTSV(DBM* dbm, File* file, bool unescape) {
  assert(dbm != nullptr && file != nullptr);
  FileReader reader(file);
  while (true) {
    std::string line;
    Status status = reader.ReadLine(&line);
    if (status != Status::SUCCESS) {
      if (status != Status::NOT_FOUND_ERROR) {
        return status;
      }
      break;
    }
    const std::string_view content(
        line.data(), line.empty() || line.back() != '\n' ? line.size() : line.size() - 1);
    const size_t pos = content.find('\t');
    if (pos == std::string::npos) {
      continue;
    }
    if (unescape) {
      const std::string& key = StrUnescapeC(content.substr(0, pos));
      const std::string& value = StrUnescapeC(content.substr(pos + 1));
      status = dbm->Set(key, value);
    } else {
      const std::string_view key = content.substr(0, pos);
      const std::string_view value = content.substr(pos + 1);
      status = dbm->Set(key, value);
    }
    if (status != Status::SUCCESS) {
      return status;
    }
  }
  return Status(Status::SUCCESS);
}

Status ExportDBMKeysAsLines(DBM* dbm, File* file) {
  assert(dbm != nullptr && file != nullptr);
  Status status = file->Truncate(0);
  if (status != Status::SUCCESS) {
    return status;
  }
  Status impl_status(Status::SUCCESS);
  class Exporter final : public DBM::RecordProcessor {
   public:
    Exporter(Status* impl_status, File* file)
        : impl_status_(impl_status), file_(file) {}
    std::string_view ProcessFull(std::string_view key, std::string_view value) override {
      const std::string line = StrCat(key, "\n");
      *impl_status_ |= file_->Append(line.data(), line.size());
      return NOOP;
    }
   private:
    Status* impl_status_;
    File* file_;
  } exporter(&impl_status, file);
  status = dbm->ProcessEach(&exporter, false);
  if (status != Status::SUCCESS) {
    return status;
  }
  return impl_status;
}

Status SearchTextFile(
    File* file, std::string_view pattern, std::vector<std::string>* matched, size_t capacity,
    bool (*matcher)(std::string_view, std::string_view)) {
  assert(file != nullptr && matched != nullptr && matcher != nullptr);
  if (capacity == 0) {
    capacity = SIZE_MAX;
  }
  FileReader reader(file);
  matched->clear();
  while (matched->size() < capacity) {
    std::string line;
    Status status = reader.ReadLine(&line);
    if (status != Status::SUCCESS) {
      if (status != Status::NOT_FOUND_ERROR) {
        return status;
      }
      break;
    }
    const std::string_view content(
        line.data(), line.empty() || line.back() != '\n' ? line.size() : line.size() - 1);
    if (matcher(content, pattern)) {
      matched->emplace_back(std::string(content));
    }
  }
  return Status(Status::SUCCESS);
}


Status SearchTextFileRegex(
    File* file, std::string_view pattern, std::vector<std::string>* matched, size_t capacity,
    bool utf) {
  assert(file != nullptr && matched != nullptr);
  if (capacity == 0) {
    capacity = SIZE_MAX;
  }
  std::unique_ptr<std::regex> regex;
  std::unique_ptr<std::wregex> regex_wide;
  try {
    if (utf) {
      const std::wstring pattern_wide = ConvertUTF8ToWide(pattern);
      regex_wide = std::make_unique<std::wregex>(pattern_wide);
    } else {
      regex = std::make_unique<std::regex>(pattern.begin(), pattern.end());
    }
  } catch (const std::regex_error& err) {
    return Status(Status::INVALID_ARGUMENT_ERROR, StrCat("invalid regex: ", err.what()));
  }
  FileReader reader(file);
  matched->clear();
  while (matched->size() < capacity) {
    std::string line;
    Status status = reader.ReadLine(&line);
    if (status != Status::SUCCESS) {
      if (status != Status::NOT_FOUND_ERROR) {
        return status;
      }
      break;
    }
    const std::string_view content(
        line.data(), line.empty() || line.back() != '\n' ? line.size() : line.size() - 1);
    if (regex_wide == nullptr) {
      if (std::regex_search(content.begin(), content.end(), *regex)) {
        matched->emplace_back(std::string(content));
      }
    } else {
      const std::wstring content_wide = ConvertUTF8ToWide(content);
      if (std::regex_search(content_wide, *regex_wide)) {
        matched->emplace_back(std::string(content));
      }
    }
  }
  return Status(Status::SUCCESS);
}

Status SearchTextFileEditDistance(
    File* file, std::string_view pattern, std::vector<std::string>* matched, size_t capacity,
    bool utf) {
  if (capacity == 0) {
    capacity = SIZE_MAX;
  }
  std::vector<uint32_t> pattern_ucs;
  if (utf) {
    pattern_ucs = ConvertUTF8ToUCS4(pattern);
  }
  FileReader reader(file);
  std::vector<std::pair<int32_t, std::string>> heap;
  while (true) {
    std::string line;
    Status status = reader.ReadLine(&line);
    if (status != Status::SUCCESS) {
      if (status != Status::NOT_FOUND_ERROR) {
        return status;
      }
      break;
    }
    const std::string_view content(
        line.data(), line.empty() || line.back() != '\n' ? line.size() : line.size() - 1);
    int32_t dist = 0;
    if (utf) {
      const std::vector<uint32_t> content_ucs = ConvertUTF8ToUCS4(content);
      dist = EditDistanceLev(pattern_ucs, content_ucs);
    } else {
      dist = EditDistanceLev(pattern, content);
    }
    HeapByCostAdd(dist, std::string(content), capacity, &heap);
  }
  HeapByCostFinish(&heap);
  matched->clear();
  for (const auto& rec : heap) {
    matched->emplace_back(rec.second);
  }
  return Status(Status::SUCCESS);
}

}  // namespace tkrzw

// END OF FILE
