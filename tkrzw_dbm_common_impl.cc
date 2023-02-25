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

#include "tkrzw_sys_config.h"

#include "tkrzw_containers.h"
#include "tkrzw_dbm.h"
#include "tkrzw_dbm_common_impl.h"
#include "tkrzw_file.h"
#include "tkrzw_file_util.h"
#include "tkrzw_str_util.h"

namespace tkrzw {

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
  class Exporter final : public DBM::RecordProcessor {
   public:
    Exporter(std::string_view pattern, std::vector<std::string>* matched, size_t capacity,
             bool (*matcher)(std::string_view, std::string_view))
        : pattern_(pattern), matched_(matched), capacity_(capacity), matcher_(matcher) {}
    std::string_view ProcessFull(std::string_view key, std::string_view value) override {
      if (matched_->size() < capacity_ && matcher_(key, pattern_)) {
        matched_->emplace_back(std::string(key));
      }
      return NOOP;
    }
   private:
    std::string_view pattern_;
    std::vector<std::string>* matched_;
    size_t capacity_;
    bool (*matcher_)(std::string_view, std::string_view);
  } exporter(pattern, matched, capacity, matcher);
  return dbm->ProcessEach(&exporter, false);
}

Status SearchDBMLambda(
    DBM* dbm, std::function<bool(std::string_view)> matcher,
    std::vector<std::string>* matched, size_t capacity) {
  assert(dbm != nullptr && matched != nullptr);
  if (capacity == 0) {
    capacity = SIZE_MAX;
  }
  matched->clear();
  class Exporter final : public DBM::RecordProcessor {
   public:
    Exporter(std::vector<std::string>* matched, size_t capacity,
             std::function<bool(std::string_view)> matcher)
        : matched_(matched), capacity_(capacity), matcher_(matcher) {}
    std::string_view ProcessFull(std::string_view key, std::string_view value) override {
      if (matched_->size() < capacity_ && matcher_(key)) {
        matched_->emplace_back(std::string(key));
      }
      return NOOP;
    }
   private:
    std::vector<std::string>* matched_;
    size_t capacity_;
    std::function<bool(std::string_view)> matcher_;
  } exporter(matched, capacity, matcher);
  return dbm->ProcessEach(&exporter, false);
}

Status SearchDBMOrder(DBM* dbm, std::string_view pattern, bool upper, bool inclusive,
                      std::vector<std::string>* matched, size_t capacity) {
  assert(dbm != nullptr && matched != nullptr);
  auto iter = dbm->MakeIterator();
  Status status =
      upper ? iter->JumpUpper(pattern, inclusive) : iter->JumpLower(pattern, inclusive);
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
    matched->emplace_back(std::move(key));
    status = upper ? iter->Next() : iter->Previous();
    if (status != Status::SUCCESS) {
      return status;
    }
  }
  return Status(Status::SUCCESS);
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
    DBM* dbm, std::string_view pattern, std::vector<std::string>* matched, size_t capacity) {
  assert(dbm != nullptr && matched != nullptr);
  std::unique_ptr<std::regex> regex;
  try {
    std::regex_constants::syntax_option_type options = std::regex_constants::optimize;
    if (pattern.size() >= 2 && pattern[0] == '(' && pattern[1] == '?') {
      bool ended = false;
      size_t pos = 2;
      while (!ended && pos < pattern.size()) {
        switch (pattern[pos]) {
          case 'a':
            options |= std::regex_constants::awk;
            break;
          case 'b':
            options |= std::regex_constants::basic;
            break;
          case 'e':
            options |= std::regex_constants::extended;
            break;
          case 'i':
            options |= std::regex_constants::icase;
            break;
          case 'l':
            options |= std::regex_constants::egrep;
            break;
          case ')':
            ended = true;
            break;
          default:
            throw std::regex_error(std::regex_constants::error_complexity);
        }
        pos++;
      }
      if (!ended) {
        throw std::regex_error(std::regex_constants::error_complexity);
      }
      pattern = pattern.substr(pos);
    }
    regex = std::make_unique<std::regex>(pattern.begin(), pattern.end(), options);
  } catch (const std::regex_error& err) {
    return Status(Status::INVALID_ARGUMENT_ERROR, StrCat("invalid regex: ", err.what()));
  }
  if (capacity == 0) {
    capacity = SIZE_MAX;
  }
  matched->clear();
  class Exporter final : public DBM::RecordProcessor {
   public:
    Exporter(std::string_view pattern, std::vector<std::string>* matched, size_t capacity,
             std::regex* regex)
        : pattern_(pattern), matched_(matched), capacity_(capacity), regex_(regex) {}
    std::string_view ProcessFull(std::string_view key, std::string_view value) override {
      if (matched_->size() >= capacity_) {
        return NOOP;
      }
      if (std::regex_search(key.begin(), key.end(), *regex_)) {
        matched_->emplace_back(std::string(key));
      }
      return NOOP;
    }
   private:
    std::string_view pattern_;
    std::vector<std::string>* matched_;
    size_t capacity_;
    std::regex* regex_;
  } exporter(pattern, matched, capacity, regex.get());
  return dbm->ProcessEach(&exporter, false);
}

Status SearchDBMEditDistance(
    DBM* dbm, std::string_view pattern, std::vector<std::string>* matched, size_t capacity) {
  assert(dbm != nullptr && matched != nullptr);
  if (capacity == 0) {
    capacity = SIZE_MAX;
  }
  const std::vector<uint32_t> pattern_ucs = ConvertUTF8ToUCS4(pattern);
  std::vector<std::pair<int32_t, std::string>> heap;
  class Exporter final : public DBM::RecordProcessor {
   public:
    Exporter(std::string_view pattern, std::vector<std::pair<int32_t, std::string>>* heap,
             size_t capacity, const std::vector<uint32_t>* pattern_ucs)
        : pattern_(pattern), heap_(heap), capacity_(capacity),
          pattern_ucs_(pattern_ucs) {}
    std::string_view ProcessFull(std::string_view key, std::string_view value) override {
      const std::vector<uint32_t> key_ucs = ConvertUTF8ToUCS4(key);
      const int32_t dist = EditDistanceLev(*pattern_ucs_, key_ucs);
      HeapByCostAdd(dist, std::string(key), capacity_, heap_);
      return NOOP;
    }
   private:
    std::string_view pattern_;
    std::vector<std::pair<int32_t, std::string>>* heap_;
    size_t capacity_;
    const std::vector<uint32_t>* pattern_ucs_;
  } exporter(pattern, &heap, capacity, &pattern_ucs);
  const Status status = dbm->ProcessEach(&exporter, false);
  if (status != Status::SUCCESS) {
    return status;
  }
  HeapByCostFinish(&heap);
  matched->clear();
  for (const auto& rec : heap) {
    matched->emplace_back(rec.second);
  }
  return Status(Status::SUCCESS);
}

Status SearchDBMEditDistanceBinary(
    DBM* dbm, std::string_view pattern, std::vector<std::string>* matched, size_t capacity) {
  assert(dbm != nullptr && matched != nullptr);
  if (capacity == 0) {
    capacity = SIZE_MAX;
  }
  std::vector<std::pair<int32_t, std::string>> heap;
  class Exporter final : public DBM::RecordProcessor {
   public:
    Exporter(std::string_view pattern, std::vector<std::pair<int32_t, std::string>>* heap,
             size_t capacity)
        : pattern_(pattern), heap_(heap), capacity_(capacity) {}
    std::string_view ProcessFull(std::string_view key, std::string_view value) override {
      const int32_t dist = EditDistanceLev(pattern_, key);
      HeapByCostAdd(dist, std::string(key), capacity_, heap_);
      return NOOP;
    }
   private:
    std::string_view pattern_;
    std::vector<std::pair<int32_t, std::string>>* heap_;
    size_t capacity_;
  } exporter(pattern, &heap, capacity);
  const Status status = dbm->ProcessEach(&exporter, false);
  if (status != Status::SUCCESS) {
    return status;
  }
  HeapByCostFinish(&heap);
  matched->clear();
  for (const auto& rec : heap) {
    matched->emplace_back(rec.second);
  }
  return Status(Status::SUCCESS);
}

Status SearchDBMModal(
    DBM* dbm, std::string_view mode, std::string_view pattern,
    std::vector<std::string>* matched, size_t capacity) {
  Status status(tkrzw::Status::SUCCESS);
  if (mode == "contain") {
    status = SearchDBM(dbm, pattern, matched, capacity, StrContains);
  } else if (mode == "begin") {
    status = SearchDBMForwardMatch(dbm, pattern, matched, capacity);
  } else if (mode == "end") {
    status = SearchDBM(dbm, pattern, matched, capacity, StrEndsWith);
  } else if (mode == "regex") {
    status = SearchDBMRegex(dbm, pattern, matched, capacity);
  } else if (mode == "edit") {
    status = SearchDBMEditDistance(dbm, pattern, matched, capacity);
  } else if (mode == "editbin") {
    status = SearchDBMEditDistanceBinary(dbm, pattern, matched, capacity);
  } else if (mode == "containcase") {
    status = SearchDBM(dbm, pattern, matched, capacity, StrCaseContains);
  } else if (mode == "containword") {
    status = SearchDBM(dbm, pattern, matched, capacity, StrWordContains);
  } else if (mode == "containcaseword") {
    status = SearchDBM(dbm, pattern, matched, capacity, StrCaseWordContains);
  } else if (mode == "contain*") {
    const auto& patterns = StrSplit(pattern, '\n', true);
    auto matcher = [=](std::string_view text) -> bool {
      return StrContainsBatch(text, patterns);
    };
    status = SearchDBMLambda(dbm, matcher, matched, capacity);
  } else if (mode == "containcase*") {
    const auto& patterns = StrSplit(pattern, '\n', true);
    auto matcher = [=](std::string_view text) -> bool {
      return StrCaseContainsBatch(text, patterns);
    };
    status = SearchDBMLambda(dbm, matcher, matched, capacity);
  } else if (mode == "containword*") {
    const auto& patterns = StrSplit(pattern, '\n', true);
    auto matcher = [=](std::string_view text) -> bool {
      return StrWordContainsBatch(text, patterns);
    };
    status = SearchDBMLambda(dbm, matcher, matched, capacity);
  } else if (mode == "containcaseword*") {
    const auto& patterns = StrSplit(pattern, '\n', true);
    std::vector<std::string> lower_patterns;
    lower_patterns.reserve(patterns.size());
    for (const auto& pattern : patterns) {
      lower_patterns.emplace_back(StrLowerCase(pattern));
    }
    auto matcher = [=](std::string_view text) -> bool {
      return StrCaseWordContainsBatchLower(text, lower_patterns);
    };
    status = SearchDBMLambda(dbm, matcher, matched, capacity);
  } else if (mode == "upper") {
    status = SearchDBMOrder(dbm, pattern, true, false, matched, capacity);
  } else if (mode == "upperinc") {
    status = SearchDBMOrder(dbm, pattern, true, true, matched, capacity);
  } else if (mode == "lower") {
    status = SearchDBMOrder(dbm, pattern, false, false, matched, capacity);
  } else if (mode == "lowerinc") {
    status = SearchDBMOrder(dbm, pattern, false, true, matched, capacity);
  } else {
    status = Status(Status::INVALID_ARGUMENT_ERROR, "unknown mode");
  }
  return status;
}

Status ExportDBMToFlatRecords(DBM* dbm, File* dest_file) {
  assert(dbm != nullptr && dest_file != nullptr);
  Status status = dest_file->Truncate(0);
  if (status != Status::SUCCESS) {
    return status;
  }
  Status impl_status(Status::SUCCESS);
  class Exporter final : public DBM::RecordProcessor {
   public:
    Exporter(Status* impl_status, File* dest_file)
        : impl_status_(impl_status), dest_file_(dest_file) {}
    std::string_view ProcessFull(std::string_view key, std::string_view value) override {
      FlatRecord rec(dest_file_);
      *impl_status_ |= rec.Write(key);
      *impl_status_ |= rec.Write(value);
      return NOOP;
    }
   private:
    Status* impl_status_;
    File* dest_file_;
  } exporter(&impl_status, dest_file);
  status = dbm->ProcessEach(&exporter, false);
  if (status != Status::SUCCESS) {
    return status;
  }
  return impl_status;
}

Status ImportDBMFromFlatRecords(DBM* dbm, File* src_file) {
  assert(dbm != nullptr && src_file != nullptr);
  int64_t end_offset = 0;
  Status status = src_file->GetSize(&end_offset);
  if (status != Status::SUCCESS) {
    return status;
  }
  FlatRecordReader reader(src_file);
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
    status = dbm->Set(key_store, value);
    if (status != Status::SUCCESS) {
      return status;
    }
  }
  return Status(Status::SUCCESS);
}

Status ExportDBMKeysToFlatRecords(DBM* dbm, File* dest_file) {
  assert(dbm != nullptr && dest_file != nullptr);
  Status status = dest_file->Truncate(0);
  if (status != Status::SUCCESS) {
    return status;
  }
  Status impl_status(Status::SUCCESS);
  class Exporter final : public DBM::RecordProcessor {
   public:
    Exporter(Status* impl_status, File* dest_file)
        : impl_status_(impl_status), dest_file_(dest_file) {}
    std::string_view ProcessFull(std::string_view key, std::string_view value) override {
      FlatRecord rec(dest_file_);
      *impl_status_ |= rec.Write(key);
      return NOOP;
    }
   private:
    Status* impl_status_;
    File* dest_file_;
  } exporter(&impl_status, dest_file);
  status = dbm->ProcessEach(&exporter, false);
  if (status != Status::SUCCESS) {
    return status;
  }
  return impl_status;
}

Status ExportDBMToTSV(DBM* dbm, File* dest_file, bool escape) {
  assert(dbm != nullptr && dest_file != nullptr);
  Status status = dest_file->Truncate(0);
  if (status != Status::SUCCESS) {
    return status;
  }
  Status impl_status(Status::SUCCESS);
  class Exporter final : public DBM::RecordProcessor {
   public:
    Exporter(Status* impl_status, File* dest_file, bool escape)
        : impl_status_(impl_status), dest_file_(dest_file), escape_(escape) {}
    std::string_view ProcessFull(std::string_view key, std::string_view value) override {
      const std::string& esc_key = escape_ ? StrEscapeC(key) : StrTrimForTSV(key);
      const std::string& esc_value = escape_ ? StrEscapeC(value) : StrTrimForTSV(value, true);
      const std::string& line = StrCat(esc_key, "\t", esc_value, "\n");
      *impl_status_ |= dest_file_->Append(line.data(), line.size());
      return NOOP;
    }
   private:
    Status* impl_status_;
    File* dest_file_;
    bool escape_;
  } exporter(&impl_status, dest_file, escape);
  status = dbm->ProcessEach(&exporter, false);
  if (status != Status::SUCCESS) {
    return status;
  }
  return impl_status;
}

Status ImportDBMFromTSV(DBM* dbm, File* src_file, bool unescape) {
  assert(dbm != nullptr && src_file != nullptr);
  FileReader reader(src_file);
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

Status ExportDBMKeysAsLines(DBM* dbm, File* dest_file) {
  assert(dbm != nullptr && dest_file != nullptr);
  Status status = dest_file->Truncate(0);
  if (status != Status::SUCCESS) {
    return status;
  }
  Status impl_status(Status::SUCCESS);
  class Exporter final : public DBM::RecordProcessor {
   public:
    Exporter(Status* impl_status, File* dest_file)
        : impl_status_(impl_status), dest_file_(dest_file) {}
    std::string_view ProcessFull(std::string_view key, std::string_view value) override {
      const std::string line = StrCat(key, "\n");
      *impl_status_ |= dest_file_->Append(line.data(), line.size());
      return NOOP;
    }
   private:
    Status* impl_status_;
    File* dest_file_;
  } exporter(&impl_status, dest_file);
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

Status SearchTextFileLambda(
    File* file, std::function<bool(std::string_view)> matcher,
    std::vector<std::string>* matched, size_t capacity) {
  assert(file != nullptr && matched != nullptr);
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
    if (matcher(content)) {
      matched->emplace_back(std::string(content));
    }
  }
  return Status(Status::SUCCESS);
}

Status SearchTextFileRegex(
    File* file, std::string_view pattern, std::vector<std::string>* matched, size_t capacity) {
  assert(file != nullptr && matched != nullptr);
  if (capacity == 0) {
    capacity = SIZE_MAX;
  }
  std::unique_ptr<std::regex> regex;
  try {
    std::regex_constants::syntax_option_type options = std::regex_constants::optimize;
    if (pattern.size() >= 2 && pattern[0] == '(' && pattern[1] == '?') {
      bool ended = false;
      size_t pos = 2;
      while (!ended && pos < pattern.size()) {
        switch (pattern[pos]) {
          case 'a':
            options |= std::regex_constants::awk;
            break;
          case 'b':
            options |= std::regex_constants::basic;
            break;
          case 'e':
            options |= std::regex_constants::extended;
            break;
          case 'i':
            options |= std::regex_constants::icase;
            break;
          case 'l':
            options |= std::regex_constants::egrep;
            break;
          case ')':
            ended = true;
            break;
          default:
            throw std::regex_error(std::regex_constants::error_complexity);
        }
        pos++;
      }
      if (!ended) {
        throw std::regex_error(std::regex_constants::error_complexity);
      }
      pattern = pattern.substr(pos);
    }
    regex = std::make_unique<std::regex>(pattern.begin(), pattern.end(), options);
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
    if (std::regex_search(content.begin(), content.end(), *regex)) {
      matched->emplace_back(std::string(content));
    }
  }
  return Status(Status::SUCCESS);
}

Status SearchTextFileEditDistance(
    File* file, std::string_view pattern, std::vector<std::string>* matched, size_t capacity) {
  if (capacity == 0) {
    capacity = SIZE_MAX;
  }
  const std::vector<uint32_t> pattern_ucs = ConvertUTF8ToUCS4(pattern);
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
    const std::vector<uint32_t> content_ucs = ConvertUTF8ToUCS4(content);
    const int32_t dist = EditDistanceLev(pattern_ucs, content_ucs);
    HeapByCostAdd(dist, std::string(content), capacity, &heap);
  }
  HeapByCostFinish(&heap);
  matched->clear();
  for (const auto& rec : heap) {
    matched->emplace_back(rec.second);
  }
  return Status(Status::SUCCESS);
}

Status SearchTextFileEditDistanceBinary(
    File* file, std::string_view pattern, std::vector<std::string>* matched, size_t capacity) {
  if (capacity == 0) {
    capacity = SIZE_MAX;
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
    const int32_t dist = EditDistanceLev(pattern, content);
    HeapByCostAdd(dist, std::string(content), capacity, &heap);
  }
  HeapByCostFinish(&heap);
  matched->clear();
  for (const auto& rec : heap) {
    matched->emplace_back(rec.second);
  }
  return Status(Status::SUCCESS);
}

Status SearchTextFileModal(
    File* file, std::string_view mode, std::string_view pattern,
    std::vector<std::string>* matched, size_t capacity) {
  Status status(tkrzw::Status::SUCCESS);
  if (mode == "contain") {
    status = SearchTextFile(file, pattern, matched, capacity, StrContains);
  } else if (mode == "begin") {
    status = SearchTextFile(file, pattern, matched, capacity, StrBeginsWith);
  } else if (mode == "end") {
    status = SearchTextFile(file, pattern, matched, capacity, StrEndsWith);
  } else if (mode == "regex") {
    status = SearchTextFileRegex(file, pattern, matched, capacity);
  } else if (mode == "edit") {
    status = SearchTextFileEditDistance(file, pattern, matched, capacity);
  } else if (mode == "editbin") {
    status = SearchTextFileEditDistanceBinary(file, pattern, matched, capacity);
  } else if (mode == "containcase") {
    status = SearchTextFile(file, pattern, matched, capacity, StrCaseContains);
  } else if (mode == "containword") {
    status = SearchTextFile(file, pattern, matched, capacity, StrWordContains);
  } else if (mode == "containcaseword") {
    status = SearchTextFile(file, pattern, matched, capacity, StrCaseWordContains);
  } else if (mode == "contain*") {
    const auto& patterns = StrSplit(pattern, '\n', true);
    auto matcher = [=](std::string_view text) -> bool {
      return StrContainsBatch(text, patterns);
    };
    status = SearchTextFileLambda(file, matcher, matched, capacity);
  } else if (mode == "containcase*") {
    const auto& patterns = StrSplit(pattern, '\n', true);
    auto matcher = [=](std::string_view text) -> bool {
      return StrCaseContainsBatch(text, patterns);
    };
    status = SearchTextFileLambda(file, matcher, matched, capacity);
  } else if (mode == "containword*") {
    const auto& patterns = StrSplit(pattern, '\n', true);
    auto matcher = [=](std::string_view text) -> bool {
      return StrWordContainsBatch(text, patterns);
    };
    status = SearchTextFileLambda(file, matcher, matched, capacity);
  } else if (mode == "containcaseword*") {
    const auto& patterns = StrSplit(pattern, '\n', true);
    std::vector<std::string> lower_patterns;
    lower_patterns.reserve(patterns.size());
    for (const auto& pattern : patterns) {
      lower_patterns.emplace_back(StrLowerCase(pattern));
    }
    auto matcher = [=](std::string_view text) -> bool {
      return StrCaseWordContainsBatchLower(text, lower_patterns);
    };
    status = SearchTextFileLambda(file, matcher, matched, capacity);
  } else {
    status = Status(Status::INVALID_ARGUMENT_ERROR, "unknown mode");
  }
  return status;
}

}  // namespace tkrzw

// END OF FILE
