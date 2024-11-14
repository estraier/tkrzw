/*************************************************************************************************
 * Datatabase manager interface
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
#include "tkrzw_file_util.h"
#include "tkrzw_lib_common.h"
#include "tkrzw_str_util.h"

namespace tkrzw {

const std::string_view DBM::ANY_DATA("\x00\xBA\xBE\x02\x11", 5);

const std::string_view DBM::RecordProcessor::NOOP("\x00\xBE\xEF\x02\x11", 5);

const std::string_view DBM::RecordProcessor::REMOVE("\x00\xDE\xAD\x02\x11", 5);

std::string_view DBM::AnyData() {
  return ANY_DATA;
}

std::string_view DBM::RecordProcessor::NoOp() {
  return NOOP;
}

std::string_view DBM::RecordProcessor::Remove() {
  return REMOVE;
}

std::string_view DBM::RecordProcessor::ProcessFull(std::string_view key, std::string_view value) {
  return NOOP;
}

std::string_view DBM::RecordProcessor::ProcessEmpty(std::string_view key) {
  return NOOP;
}


DBM::RecordProcessorLambda::RecordProcessorLambda(RecordLambdaType proc_lambda)
    : proc_lambda_(proc_lambda) {}

std::string_view DBM::RecordProcessorLambda::ProcessFull(std::string_view key, std::string_view value) {
  return proc_lambda_(key, value);
}

std::string_view DBM::RecordProcessorLambda::ProcessEmpty(std::string_view key) {
  return proc_lambda_(key, NOOP);
}


DBM::RecordProcessorGet::RecordProcessorGet(Status* status, std::string* value)
    : status_(status), value_(value) {}

std::string_view DBM::RecordProcessorGet::ProcessFull(std::string_view key, std::string_view value) {
  if (value_ != nullptr) {
    *value_ = value;
  }
  return NOOP;
}


std::string_view DBM::RecordProcessorGet::ProcessEmpty(std::string_view key) {
  status_->Set(Status::NOT_FOUND_ERROR);
  return NOOP;
}

DBM::RecordProcessorSet::RecordProcessorSet(Status* status, std::string_view value,
    bool overwrite, std::string* old_value)
    : status_(status), value_(value), overwrite_(overwrite), old_value_(old_value) {}


std::string_view DBM::RecordProcessorSet::ProcessFull(std::string_view key, std::string_view value) {
  if (old_value_ != nullptr) {
    *old_value_ = value;
  }
  if (overwrite_) {
    return value_;
  }
  status_->Set(Status::DUPLICATION_ERROR);
  return NOOP;
}

std::string_view DBM::RecordProcessorSet::ProcessEmpty(std::string_view key) {
  return value_;
}


DBM::RecordProcessorRemove::RecordProcessorRemove(Status* status,
    std::string* old_value)
    : status_(status), old_value_(old_value) {}

std::string_view DBM::RecordProcessorRemove::ProcessFull(std::string_view key, std::string_view value) {
  if (old_value_ != nullptr) {
    *old_value_ = value;
  }
  return REMOVE;
}

std::string_view DBM::RecordProcessorRemove::ProcessEmpty(std::string_view key) {
  status_->Set(Status::NOT_FOUND_ERROR);
  return NOOP;
}


DBM::RecordProcessorAppend::RecordProcessorAppend(std::string_view value, std::string_view delim)
    : value_(value), delim_(delim) {}


std::string_view DBM::RecordProcessorAppend::ProcessFull(std::string_view key, std::string_view value) {
  if (delim_.empty()) {
    new_value_.reserve(value.size() + value_.size());
    new_value_.append(value);
    new_value_.append(value_);
  } else {
    new_value_.reserve(value.size() + delim_.size() + value_.size());
    new_value_.append(value);
    new_value_.append(delim_);
    new_value_.append(value_);
  }
  return new_value_;
}

std::string_view DBM::RecordProcessorAppend::ProcessEmpty(std::string_view key) {
  return value_;
}


DBM::RecordProcessorCompareExchange::RecordProcessorCompareExchange(
    Status* status, std::string_view expected, std::string_view desired, std::string* actual,
    bool* found)
    : status_(status), expected_(expected), desired_(desired), actual_(actual), found_(found) {}

std::string_view DBM::RecordProcessorCompareExchange::ProcessFull(
    std::string_view key, std::string_view value) {
  if (actual_ != nullptr) {
    *actual_ = value;
  }
  if (found_ != nullptr) {
    *found_ = true;
  }
  if (expected_.data() != nullptr && (expected_.data() == ANY_DATA.data() || expected_ == value)) {
    return desired_.data() == nullptr ? REMOVE :
      desired_.data() == ANY_DATA.data() ? NOOP : desired_;
  }
  status_->Set(Status::INFEASIBLE_ERROR);
  return NOOP;
}

std::string_view DBM::RecordProcessorCompareExchange::ProcessEmpty(std::string_view key) {
  if (actual_ != nullptr) {
    *actual_ = "";
  }
  if (found_ != nullptr) {
    *found_ = false;
  }
  if (expected_.data() == nullptr) {
    return desired_.data() == nullptr || desired_.data() == ANY_DATA.data() ? NOOP : desired_;
  }
  status_->Set(Status::INFEASIBLE_ERROR);
  return NOOP;
}


DBM::RecordProcessorIncrement::RecordProcessorIncrement(int64_t increment,
    int64_t* current, int64_t initial)
    : increment_(increment), current_(current), initial_(initial) {}

std::string_view DBM::RecordProcessorIncrement::ProcessFull(std::string_view key, std::string_view value) {
  if (increment_ == INT64MIN) {
    if (current_ != nullptr) {
      *current_ = StrToIntBigEndian(value);
    }
    return NOOP;
  }
  const int64_t num = StrToIntBigEndian(value) + increment_;
  if (current_ != nullptr) {
    *current_ = num;
  }
  value_ = IntToStrBigEndian(num);
  return value_;
}

std::string_view DBM::RecordProcessorIncrement::ProcessEmpty(std::string_view key) {
  if (increment_ == INT64MIN) {
    if (current_ != nullptr) {
      *current_ = initial_;
    }
    return NOOP;
  }
  const int64_t num = initial_ + increment_;
  if (current_ != nullptr) {
    *current_ = num;
  }
  value_ =  IntToStrBigEndian(num);
  return value_;
}


DBM::RecordCheckerCompareExchangeMulti::RecordCheckerCompareExchangeMulti(
    bool* noop, std::string_view expected)
    : noop_(noop), expected_(expected) {}

std::string_view DBM::RecordCheckerCompareExchangeMulti::ProcessFull(
    std::string_view key, std::string_view value) {
  if (expected_.data() == nullptr || (expected_.data() != ANY_DATA.data() && expected_ != value)) {
    *noop_ = true;
  }
  return NOOP;
}


std::string_view DBM::RecordCheckerCompareExchangeMulti::ProcessEmpty(std::string_view key) {
  if (expected_.data() != nullptr) {
    *noop_ = true;
  }
  return NOOP;
}


DBM::RecordSetterCompareExchangeMulti::RecordSetterCompareExchangeMulti(
    bool* noop, std::string_view desired)
    : noop_(noop), desired_(desired) {}

std::string_view DBM::RecordSetterCompareExchangeMulti::ProcessFull(std::string_view key, std::string_view value) {
  if (*noop_) {
    return NOOP;
  }
  return desired_.data() == nullptr ? REMOVE : desired_;
}

std::string_view DBM::RecordSetterCompareExchangeMulti::ProcessEmpty(std::string_view key) {
  if (*noop_) {
    return NOOP;
  }
  return desired_.data() == nullptr ? NOOP : desired_;
}


DBM::RecordCheckerRekey::RecordCheckerRekey(Status* status) : status_(status) {}

std::string_view DBM::RecordCheckerRekey::ProcessFull(std::string_view key, std::string_view value) {
  status_->Set(Status::DUPLICATION_ERROR);
  return NOOP;
}

std::string_view DBM::RecordCheckerRekey::ProcessEmpty(std::string_view key) {
  return NOOP;
}


DBM::RecordRemoverRekey::RecordRemoverRekey(Status* status, std::string* old_value, bool copying)
    : status_(status), old_value_(old_value), copying_(copying) {}

std::string_view DBM::RecordRemoverRekey::ProcessFull(std::string_view key, std::string_view value) {
  if (*status_ != Status::SUCCESS) {
    return NOOP;
  }
  *old_value_ = value;
  return copying_ ? NOOP : REMOVE;
}

std::string_view DBM::RecordRemoverRekey::ProcessEmpty(std::string_view key) {
  status_->Set(Status::NOT_FOUND_ERROR);
  return NOOP;
}


DBM::RecordSetterRekey::RecordSetterRekey(Status* status, const std::string* new_value)
    : status_(status), new_value_(new_value) {}

std::string_view DBM::RecordSetterRekey::ProcessFull(std::string_view key, std::string_view value) {
  if (*status_ != Status::SUCCESS) {
    return NOOP;
  }
  return *new_value_;
}

std::string_view DBM::RecordSetterRekey::ProcessEmpty(std::string_view key) {
  if (*status_ != Status::SUCCESS) {
    return NOOP;
  }
  return *new_value_;
}

DBM::RecordProcessorPopFirst::RecordProcessorPopFirst(std::string* key, std::string* value)
    : key_(key), value_(value) {}

std::string_view DBM::RecordProcessorPopFirst::ProcessFull(std::string_view key,
    std::string_view value) {
  if (key_ != nullptr) {
    *key_ = key;
  }
  if (value_ != nullptr) {
    *value_ = value;
  }
  return REMOVE;
}


DBM::RecordProcessorExport::RecordProcessorExport(Status* status, DBM* dbm)
    : status_(status), dbm_(dbm) {}

std::string_view DBM::RecordProcessorExport::ProcessFull(
    std::string_view key, std::string_view value) {
  *status_ |= dbm_->Set(key, value);
  return NOOP;
}


DBM::RecordProcessorIterator::RecordProcessorIterator(
    std::string_view new_value, std::string* cur_key, std::string* cur_value)
    : new_value_(new_value), cur_key_(cur_key), cur_value_(cur_value) {}

std::string_view DBM::RecordProcessorIterator::ProcessFull(
    std::string_view key, std::string_view value) {
  if (cur_key_ != nullptr) {
    *cur_key_ = key;
  }
  if (cur_value_ != nullptr) {
    *cur_value_ = value;
  }
  return new_value_;
}


Status DBM::Iterator::Process(RecordLambdaType rec_lambda, bool writable) {
  RecordProcessorLambda proc(rec_lambda);
  return Process(&proc, writable);
}

Status DBM::Iterator::Get(std::string* key, std::string* value) {
  RecordProcessorIterator proc(RecordProcessor::NOOP, key, value);
  return Process(&proc, false);
}

std::string DBM::Iterator::GetKey(std::string_view default_value) {
  std::string key;
  return Get(&key, nullptr) == Status::SUCCESS ? key : std::string(default_value);
}

std::string DBM::Iterator::GetValue(std::string_view default_value) {
  std::string value;
  return Get(nullptr, &value) == Status::SUCCESS ? value : std::string(default_value);
}

Status DBM::Iterator::Set(std::string_view value, std::string* old_key,
    std::string* old_value) {
  RecordProcessorIterator proc(value, old_key, old_value);
  return Process(&proc, true);
}

Status DBM::Iterator::Remove(std::string* old_key, std::string* old_value) {
  RecordProcessorIterator proc(RecordProcessor::REMOVE, old_key, old_value);
  return Process(&proc, true);
}

Status DBM::Iterator::Step(std::string* key, std::string* value) {
  Status status = Get(key, value);
  if (status != Status::SUCCESS) {
    return status;
  }
  status = Next();
  if (status == Status::NOT_FOUND_ERROR) {
    status.Set(Status::SUCCESS);
  }
  return status;
}


void DBM::FileProcessor::Process(const std::string& path) {}


DBM::FileProcessorCopyFileData::FileProcessorCopyFileData(
    Status* status, const std::string dest_path)
    : status_(status), dest_path_(dest_path) {}

void DBM::FileProcessorCopyFileData::Process(const std::string& path) {
  *status_ = tkrzw::CopyFileData(path, dest_path_);
}


Status DBM::UpdateLogger::Synchronize(bool hard) {
  return Status(Status::SUCCESS);
}


Status DBM::Process(std::string_view key, RecordLambdaType rec_lambda, bool writable) {
  RecordProcessorLambda proc(rec_lambda);
  return Process(key, &proc, writable);
}

Status DBM::Get(std::string_view key, std::string* value) {
  Status impl_status(Status::SUCCESS);
  RecordProcessorGet proc(&impl_status, value);
  const Status status = Process(key, &proc, false);
  if (status != Status::SUCCESS) {
    return status;
  }
  return impl_status;
}

std::string DBM::GetSimple(std::string_view key, std::string_view default_value) {
  std::string value;
  return Get(key, &value) == Status::SUCCESS ? value : std::string(default_value);
}

Status DBM::GetMulti(
    const std::vector<std::string_view>& keys, std::map<std::string, std::string>* records) {
  Status status(Status::SUCCESS);
  for (const auto& key : keys) {
    std::string value;
    const Status tmp_status = Get(key, &value);
    if (tmp_status == Status::SUCCESS) {
      records->emplace(key, std::move(value));
    } else {
      status |= tmp_status;
    }
  }
  return status;
}

Status DBM::GetMulti(const std::initializer_list<std::string_view>& keys,
    std::map<std::string, std::string>* records) {
  std::vector<std::string_view> vector_keys(keys.begin(), keys.end());
  return GetMulti(vector_keys, records);
}

Status DBM::GetMulti(
    const std::vector<std::string>& keys, std::map<std::string, std::string>* records) {
  return GetMulti(MakeStrViewVectorFromValues(keys), records);
}

Status DBM::Set(std::string_view key, std::string_view value, bool overwrite,
    std::string* old_value) {
  Status impl_status(Status::SUCCESS);
  RecordProcessorSet proc(&impl_status, value, overwrite, old_value);
  const Status status = Process(key, &proc, true);
  if (status != Status::SUCCESS) {
    return status;
  }
  return impl_status;
}

Status DBM::SetMulti(
    const std::map<std::string_view, std::string_view>& records, bool overwrite) {
  Status status(Status::SUCCESS);
  for (const auto& record : records) {
    status |= Set(record.first, record.second, overwrite);
    if (status != Status::SUCCESS && status != Status::DUPLICATION_ERROR) {
      break;
    }
  }
  return status;
}

Status DBM::SetMulti(
    const std::initializer_list<std::pair<std::string_view, std::string_view>>& records,
    bool overwrite) {
  std::map<std::string_view, std::string_view> map_records;
  for (const auto& record : records) {
    map_records.emplace(std::pair(
      std::string_view(record.first), std::string_view(record.second)));
  }
  return SetMulti(map_records, overwrite);
}

Status DBM::SetMulti(
    const std::map<std::string, std::string>& records, bool overwrite) {
  return SetMulti(MakeStrViewMapFromRecords(records), overwrite);
}

Status DBM::Remove(std::string_view key, std::string* old_value) {
  Status impl_status(Status::SUCCESS);
  RecordProcessorRemove proc(&impl_status, old_value);
  const Status status = Process(key, &proc, true);
  if (status != Status::SUCCESS) {
    return status;
  }
  return impl_status;
}

Status DBM::RemoveMulti(const std::vector<std::string_view>& keys) {
  Status status(Status::SUCCESS);
  for (const auto& key : keys) {
    status |= Remove(key);
    if (status != Status::Status::SUCCESS && status != Status::Status::NOT_FOUND_ERROR) {
      break;
    }
  }
  return status;
}

Status DBM::RemoveMulti(const std::initializer_list<std::string_view>& keys) {
  std::vector<std::string_view> vector_keys(keys.begin(), keys.end());
  return RemoveMulti(vector_keys);
}

Status DBM::RemoveMulti(const std::vector<std::string>& keys) {
  return RemoveMulti(MakeStrViewVectorFromValues(keys));
}

Status DBM::Append(
    std::string_view key, std::string_view value, std::string_view delim) {
  RecordProcessorAppend proc(value, delim);
  return Process(key, &proc, true);
}

Status DBM::AppendMulti(
    const std::map<std::string_view, std::string_view>& records, std::string_view delim) {
  Status status(Status::SUCCESS);
  for (const auto& record : records) {
    status |= Append(record.first, record.second, delim);
    if (status != Status::SUCCESS) {
      break;
    }
  }
  return status;
}

Status DBM::AppendMulti(
    const std::initializer_list<std::pair<std::string_view, std::string_view>>& records,
    std::string_view delim) {
  std::map<std::string_view, std::string_view> map_records;
  for (const auto& record : records) {
    map_records.emplace(std::pair(
      std::string_view(record.first), std::string_view(record.second)));
  }
  return AppendMulti(map_records, delim);
}

 Status DBM::AppendMulti(
    const std::map<std::string, std::string>& records, std::string_view delim) {
  return AppendMulti(MakeStrViewMapFromRecords(records), delim);
}

Status DBM::CompareExchange(std::string_view key, std::string_view expected,
    std::string_view desired, std::string* actual, bool* found) {
  Status impl_status(Status::SUCCESS);
  RecordProcessorCompareExchange proc(&impl_status, expected, desired, actual, found);
  const Status status = Process(key, &proc, desired.data() != ANY_DATA.data());
  if (status != Status::SUCCESS) {
    return status;
  }
  return impl_status;
}

Status DBM::Increment(std::string_view key, int64_t increment,
    int64_t* current, int64_t initial) {
  RecordProcessorIncrement proc(increment, current, initial);
  return Process(key, &proc, increment != INT64MIN);
}

int64_t DBM::IncrementSimple(std::string_view key, int64_t increment, int64_t initial) {
  int64_t current = 0;
  return Increment(key, increment, &current, initial) == Status::SUCCESS ? current : INT64MIN;
}

Status DBM::ProcessMulti(
    const std::vector<std::pair<std::string_view, RecordLambdaType>>& key_lambda_pairs,
    bool writable) {
  std::vector<std::pair<std::string_view, RecordProcessor*>> key_proc_pairs;
  key_proc_pairs.reserve(key_lambda_pairs.size());
  std::vector<RecordProcessorLambda> procs;
  procs.reserve(key_lambda_pairs.size());
  for (const auto& key_lambda : key_lambda_pairs) {
    procs.emplace_back(key_lambda.second);
    key_proc_pairs.emplace_back(std::make_pair(key_lambda.first, &procs.back()));
  }
  return ProcessMulti(key_proc_pairs, writable);
}

Status DBM::CompareExchangeMulti(
    const std::vector<std::pair<std::string_view, std::string_view>>& expected,
    const std::vector<std::pair<std::string_view, std::string_view>>& desired) {
  std::vector<std::pair<std::string_view, RecordProcessor*>> key_proc_pairs;
  key_proc_pairs.reserve(expected.size() + desired.size());
  bool noop = false;
  std::vector<RecordCheckerCompareExchangeMulti> checkers;
  checkers.reserve(expected.size());
  for (const auto& key_value : expected) {
    checkers.emplace_back(RecordCheckerCompareExchangeMulti(&noop, key_value.second));
    key_proc_pairs.emplace_back(std::pair(key_value.first, &checkers.back()));
  }
  std::vector<RecordSetterCompareExchangeMulti> setters;
  setters.reserve(desired.size());
  for (const auto& key_value : desired) {
    setters.emplace_back(RecordSetterCompareExchangeMulti(&noop, key_value.second));
    key_proc_pairs.emplace_back(std::pair(key_value.first, &setters.back()));
  }
  const Status status = ProcessMulti(key_proc_pairs, true);
  if (status != Status::SUCCESS) {
    return status;
  }
  return noop ? Status(Status::INFEASIBLE_ERROR) : Status(Status::SUCCESS);
}

Status DBM::Rekey(std::string_view old_key, std::string_view new_key,
    bool overwrite, bool copying, std::string* value) {
  std::vector<std::pair<std::string_view, RecordProcessor*>> key_proc_pairs;
  key_proc_pairs.reserve(3);
  Status proc_status(Status::SUCCESS);
  RecordCheckerRekey checker(&proc_status);
  if (!overwrite) {
    key_proc_pairs.emplace_back(std::pair(new_key, &checker));
  }
  std::string rec_value;
  RecordRemoverRekey remover(&proc_status, &rec_value, copying);
  key_proc_pairs.emplace_back(std::pair(old_key, &remover));
  RecordSetterRekey setter(&proc_status, &rec_value);
  key_proc_pairs.emplace_back(std::pair(new_key, &setter));
  const Status status = ProcessMulti(key_proc_pairs, true);
  if (status != Status::SUCCESS) {
    return status;
  }
  if (proc_status == Status::SUCCESS && value != nullptr) {
    *value = std::move(rec_value);
  }
  return proc_status;
}

Status DBM::ProcessFirst(RecordLambdaType rec_lambda, bool writable) {
  RecordProcessorLambda proc(rec_lambda);
  return ProcessFirst(&proc, writable);
}

Status DBM::PopFirst(std::string* key, std::string* value) {
  RecordProcessorPopFirst proc(key, value);
  return ProcessFirst(&proc, true);
}

Status DBM::PushLast(std::string_view value, double wtime, std::string* key) {
  for (uint64_t seq = 0; true; seq++) {
    const uint64_t timestamp =
        static_cast<int64_t>((wtime < 0 ? GetWallTime() : wtime) * 100000000 + seq);
    const std::string& time_key = IntToStrBigEndian(timestamp);
    const Status status = Set(time_key, value, false);
    if (status != Status::DUPLICATION_ERROR) {
      if (key != nullptr) {
        *key = time_key;
      }
      return status;
    }
  }
  return Status(Status::UNKNOWN_ERROR);
}

Status DBM::ProcessEach(RecordLambdaType rec_lambda, bool writable) {
  RecordProcessorLambda proc(rec_lambda);
  return ProcessEach(&proc, writable);
}

int64_t DBM::CountSimple() {
  int64_t count = 0;
  return Count(&count) == Status::SUCCESS ? count : -1;
}

int64_t DBM::GetFileSizeSimple() {
  int64_t size = 0;
  return GetFileSize(&size) == Status::SUCCESS ? size : -1;
}

std::string DBM::GetFilePathSimple() {
  std::string path;
  return GetFilePath(&path) == Status::SUCCESS ? path : std::string("");
}

double DBM::GetTimestampSimple() {
  double timestamp = 0;
  return GetTimestamp(&timestamp) == Status::SUCCESS ? timestamp : DOUBLENAN;
}

bool DBM::ShouldBeRebuiltSimple() {
  bool tobe = false;
  return ShouldBeRebuilt(&tobe) == Status::SUCCESS ? tobe : false;
}

Status DBM::CopyFileData(const std::string& dest_path, bool sync_hard) {
  Status impl_status(Status::SUCCESS);
  FileProcessorCopyFileData proc(&impl_status, dest_path);
  if (IsWritable()) {
    const Status status = Synchronize(sync_hard, &proc);
    if (status != Status::SUCCESS) {
      return status;
    }
  } else {
    std::string path;
    const Status status = GetFilePath(&path);
    if (status != Status::SUCCESS) {
      return status;
    }
    proc.Process(path);
  }
  return impl_status;
}

Status DBM::Export(DBM* dest_dbm) {
  Status impl_status(Status::SUCCESS);
  RecordProcessorExport proc(&impl_status, dest_dbm);
  const Status status = ProcessEach(&proc, false);
  if (status != Status::SUCCESS) {
    return status;
  }
  return impl_status;
}

const std::type_info& DBM::GetType() const {
  const auto& entity = *this;
  return typeid(entity);
}

}  // namespace tkrzw

// END OF FILE
