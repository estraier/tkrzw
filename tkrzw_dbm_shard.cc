/*************************************************************************************************
 * Polymorphic database manager adapter
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
#include "tkrzw_dbm_baby.h"
#include "tkrzw_dbm_cache.h"
#include "tkrzw_dbm_common_impl.h"
#include "tkrzw_dbm_hash.h"
#include "tkrzw_dbm_poly.h"
#include "tkrzw_dbm_shard.h"
#include "tkrzw_dbm_skip.h"
#include "tkrzw_dbm_std.h"
#include "tkrzw_dbm_tiny.h"
#include "tkrzw_dbm_tree.h"
#include "tkrzw_dbm_ulog.h"
#include "tkrzw_file.h"
#include "tkrzw_file_mmap.h"
#include "tkrzw_file_pos.h"
#include "tkrzw_file_std.h"
#include "tkrzw_file_util.h"
#include "tkrzw_lib_common.h"
#include "tkrzw_str_util.h"

namespace tkrzw {

static Status MakeOwnedUpdateLogger(
    std::map<std::string, std::string>* params,
    std::unique_ptr<MessageQueue>* ulog_mq, std::unique_ptr<DBM::UpdateLogger>* ulog) {
  Status status(Status::SUCCESS);
  const std::string prefix = SearchMap(*params, "ulog_prefix", "");
  const int64_t max_file_size = StrToIntMetric(SearchMap(*params, "ulog_max_file_size", "1Gi"));
  const int32_t server_id = StrToIntMetric(SearchMap(*params, "ulog_server_id", "0"));
  const int32_t dbm_index = StrToIntMetric(SearchMap(*params, "ulog_dbm_index", "0"));
  if (!prefix.empty() && max_file_size > 0) {
    *ulog_mq = std::make_unique<MessageQueue>();
    status |= ulog_mq->get()->Open(prefix, max_file_size);
    *ulog = std::make_unique<DBMUpdateLoggerMQ>(ulog_mq->get(), server_id, dbm_index);
  }
  params->erase("ulog_prefix");
  params->erase("ulog_max_file_size");
  params->erase("ulog_server_id");
  params->erase("ulog_dbm_index");
  return status;
}

ShardDBM::ShardDBM()
    : dbms_(), ulog_mq_(nullptr), ulog_(nullptr), open_(false), path_(), ulog_second_() {}

ShardDBM::~ShardDBM() {
  if (open_) {
    Close();
  }
}

Status ShardDBM::OpenAdvanced(
    const std::string& path, bool writable, int32_t options,
    const std::map<std::string, std::string>& params) {
  if (open_) {
    return Status(Status::PRECONDITION_ERROR, "opened database");
  }
  int32_t num_shards = StrToInt(SearchMap(params, "num_shards", "0"));
  if (num_shards < 1) {
    const Status status = GetNumberOfShards(path, &num_shards);
    if (status != Status::SUCCESS) {
      if (status == Status::NOT_FOUND_ERROR) {
        num_shards = 1;
      } else {
        return status;
      }
    }
  }
  dbms_.reserve(num_shards);
  for (int32_t i = 0; i < num_shards; i++) {
    dbms_.emplace_back(std::make_unique<PolyDBM>());
  }
  std::map<std::string, std::string> mod_params, ulog_params;
  for (const auto& param : params) {
    if (StrBeginsWith(param.first, "ulog_")) {
      ulog_params.emplace(param);
    } else {
      mod_params.emplace(param);
    }
  }
  mod_params.erase("num_shards");
  for (int32_t i = 0; i < static_cast<int32_t>(dbms_.size()); i++) {
    std::string shard_path;
    if (!path.empty()) {
      shard_path = StrCat(path, SPrintF("-%05d-of-%05d", i, static_cast<int32_t>(dbms_.size())));
    }
    const Status status = dbms_[i]->OpenAdvanced(shard_path, writable, options, mod_params);
    if (status != Status::SUCCESS) {
      for (int32_t j = i - 1; j >= 0; j--) {
        dbms_[j]->Close();
      }
      return status;
    }
  }
  Status status = MakeOwnedUpdateLogger(&ulog_params, &ulog_mq_, &ulog_);
  if (!ulog_params.empty()) {
    status |= Status(Status::INVALID_ARGUMENT_ERROR,
                     StrCat("unsupported parameter: ", ulog_params.begin()->first));
  }
  if (status != Status::SUCCESS) {
    ulog_.reset(nullptr);
    ulog_mq_.reset(nullptr);
    for (int32_t i = dbms_.size() - 1; i >= 0; i--) {
      dbms_[i]->Close();
    }
    return status;
  }
  open_ = true;
  path_ = path;
  if (ulog_ != nullptr) {
    SetUpdateLogger(ulog_.get());
  }
  return Status(Status::SUCCESS);
}

Status ShardDBM::Close() {
  if (!open_) {
    return Status(Status::PRECONDITION_ERROR, "not opened database");
  }
  Status status(Status::SUCCESS);
  if (ulog_ != nullptr) {
    ulog_.reset(nullptr);
  }
  if (ulog_mq_ != nullptr) {
    status |= ulog_mq_->Close();
    ulog_mq_.reset(nullptr);
  }
  for (int32_t i = dbms_.size() - 1; i >= 0; i--) {
    status |= dbms_[i]->Close();
  }
  open_ = false;
  path_.clear();
  dbms_.clear();
  return status;
}

Status ShardDBM::Process(std::string_view key, RecordProcessor* proc, bool writable) {
  if (!open_) {
    return Status(Status::PRECONDITION_ERROR, "not opened database");
  }
  const int32_t shard_index = SecondaryHash(key, dbms_.size());
  auto dbm = dbms_[shard_index];
  return dbm->Process(key, proc, writable);
}

Status ShardDBM::Get(std::string_view key, std::string* value) {
  if (!open_) {
    return Status(Status::PRECONDITION_ERROR, "not opened database");
  }
  const int32_t shard_index = SecondaryHash(key, dbms_.size());
  auto dbm = dbms_[shard_index];
  return dbm->Get(key, value);
}

Status ShardDBM::Set(std::string_view key, std::string_view value, bool overwrite,
                     std::string* old_value) {
  if (!open_) {
    return Status(Status::PRECONDITION_ERROR, "not opened database");
  }
  const int32_t shard_index = SecondaryHash(key, dbms_.size());
  auto dbm = dbms_[shard_index];
  return dbm->Set(key, value, overwrite, old_value);
}

Status ShardDBM::Remove(std::string_view key, std::string* old_value) {
  if (!open_) {
    return Status(Status::PRECONDITION_ERROR, "not opened database");
  }
  const int32_t shard_index = SecondaryHash(key, dbms_.size());
  auto dbm = dbms_[shard_index];
  return dbm->Remove(key, old_value);
}

Status ShardDBM::Append(std::string_view key, std::string_view value, std::string_view delim) {
  if (!open_) {
    return Status(Status::PRECONDITION_ERROR, "not opened database");
  }
  const int32_t shard_index = SecondaryHash(key, dbms_.size());
  auto dbm = dbms_[shard_index];
  return dbm->Append(key, value, delim);
}

Status ShardDBM::ProcessFirst(RecordProcessor* proc, bool writable) {
  assert(proc != nullptr);
  std::unique_ptr<ShardDBM::Iterator> iter(new Iterator(&dbms_));
  Status status = iter->First();
  if (status != Status::SUCCESS) {
    return status;
  }
  return iter->Process(proc, writable);
}

Status ShardDBM::ProcessMulti(
    const std::vector<std::pair<std::string_view, RecordProcessor*>>& key_proc_pairs,
    bool writable) {
  if (!open_) {
    return Status(Status::PRECONDITION_ERROR, "not opened database");
  }
  Status proc_status(Status::SUCCESS);
  std::vector<std::pair<std::string_view, DBM::RecordProcessor*>> key_wrap_pairs;
  key_wrap_pairs.reserve(key_proc_pairs.size());
  struct Delegator : public DBM::RecordProcessor {
    Status* status;
    DBM* dbm;
    DBM::RecordProcessor* proc;
    bool writable;
    std::string_view ProcessFull(std::string_view key, std::string_view value) override {
      *status |= dbm->Process(key, proc, writable);
      return NOOP;
    }
    std::string_view ProcessEmpty(std::string_view key) override {
      *status |= dbm->Process(key, proc, writable);
      return NOOP;
    }
  };
  std::vector<Delegator> delegators;
  delegators.reserve(key_proc_pairs.size());
  for (const auto& key_proc : key_proc_pairs) {
    const int32_t shard_index = SecondaryHash(key_proc.first, dbms_.size());
    if (shard_index == 0) {
      key_wrap_pairs.emplace_back(key_proc);
    } else {
      delegators.emplace_back(Delegator());
      auto& delegator = delegators.back();
      delegator.status = &proc_status;
      delegator.dbm = dbms_[shard_index].get();
      delegator.proc = key_proc.second;
      delegator.writable = writable;
      key_wrap_pairs.emplace_back(std::make_pair(key_proc.first, &delegator));
    }
  }
  Status status = dbms_[0]->ProcessMulti(key_wrap_pairs, writable);
  status |= proc_status;
  return status;
}

Status ShardDBM::CompareExchangeMulti(
    const std::vector<std::pair<std::string_view, std::string_view>>& expected,
    const std::vector<std::pair<std::string_view, std::string_view>>& desired) {
  typedef std::pair<std::string_view, std::string_view> Condition;
  typedef std::pair<std::vector<Condition>, std::vector<Condition>> ConditionListPair;
  std::map<int32_t, ConditionListPair> shard_conditions;
  for (auto& cond : expected) {
    const int32_t shard_index = SecondaryHash(cond.first, dbms_.size());
    shard_conditions[shard_index].first.emplace_back(cond);
  }
  for (auto& cond : desired) {
    const int32_t shard_index = SecondaryHash(cond.first, dbms_.size());
    shard_conditions[shard_index].second.emplace_back(cond);
  }
  if (shard_conditions.empty()) {
    return Status(Status::SUCCESS);
  }
  struct Command {
    DBM* dbm;
    std::vector<std::pair<std::string_view, RecordProcessor*>> params;
  };
  std::vector<Command> commands(shard_conditions.size());
  class Checker : public RecordProcessor {
   public:
    Checker(Status* status, std::string_view expected, Command* next_command)
        : status_(status), expected_(expected), next_command_(next_command) {}
    std::string_view ProcessFull(std::string_view key, std::string_view value) override {
      if (*status_ != Status::SUCCESS) {
        return NOOP;
      }
      if (expected_.data() == nullptr ||
          (expected_.data() != ANY_DATA.data() && expected_ != value)) {
        *status_ = Status(Status::INFEASIBLE_ERROR);
        return NOOP;
      }
      if (next_command_ != nullptr) {
        *status_ |= next_command_->dbm->ProcessMulti(next_command_->params, true);
      }
      return NOOP;
    }
    std::string_view ProcessEmpty(std::string_view key) override {
      if (*status_ != Status::SUCCESS) {
        return NOOP;
      }
      if (expected_.data() != nullptr) {
        *status_ = Status(Status::INFEASIBLE_ERROR);
        return NOOP;
      }
      if (next_command_ != nullptr) {
        *status_ |= next_command_->dbm->ProcessMulti(next_command_->params, true);
      }
      return NOOP;
    }
   private:
    Status* status_;
    std::string_view expected_;
    Command* next_command_;
  };
  std::vector<std::shared_ptr<Checker>> checkers;
  class Setter : public RecordProcessor {
   public:
   Setter(Status* status, std::string_view desired, Command* next_command)
       : status_(status), desired_(desired), next_command_(next_command) {}
    std::string_view ProcessFull(std::string_view key, std::string_view value) override {
      if (*status_ != Status::SUCCESS) {
        return NOOP;
      }
      if (next_command_ != nullptr) {
        *status_ |= next_command_->dbm->ProcessMulti(next_command_->params, true);
        if (*status_ != Status::SUCCESS) {
          return NOOP;
        }
      }
      return desired_.data() == nullptr ? REMOVE : desired_;
    }
    std::string_view ProcessEmpty(std::string_view key) override {
      if (*status_ != Status::SUCCESS) {
        return NOOP;
      }
      if (next_command_ != nullptr) {
        *status_ |= next_command_->dbm->ProcessMulti(next_command_->params, true);
        if (*status_ != Status::SUCCESS) {
          return NOOP;
        }
      }
      return desired_.data() == nullptr ? NOOP : desired_;
    }
   private:
    Status* status_;
    std::string_view desired_;
    Command* next_command_;
  };
  std::vector<std::shared_ptr<Setter>> setters;
  Status proc_status(Status::SUCCESS);
  size_t command_index = 0;
  for (const auto& shard_condition : shard_conditions) {
    auto& command = commands[command_index];
    const int32_t shard_index = shard_condition.first;
    const auto& shard_expected = shard_condition.second.first;
    const auto& shard_desired = shard_condition.second.second;
    command.dbm = dbms_[shard_index].get();
    for (size_t i = 0; i < shard_expected.size(); i++) {
      const auto& key_value = shard_expected[i];
      Command* next_command =
          command_index < commands.size() - 1 && i == shard_expected.size() - 1 ?
                          &commands[command_index + 1] : nullptr;
      checkers.emplace_back(std::make_unique<Checker>(
          &proc_status, key_value.second, next_command));
      command.params.emplace_back(std::make_pair(key_value.first, checkers.back().get()));
    }
    for (size_t i = 0; i < shard_desired.size(); i++) {
      const auto& key_value = shard_desired[i];
      Command* next_command =
          command_index < commands.size() - 1 && shard_expected.empty() && i == 0 ?
                          &commands[command_index + 1] : nullptr;
      setters.emplace_back(std::make_unique<Setter>(
          &proc_status, key_value.second, next_command));
      command.params.emplace_back(std::make_pair(key_value.first, setters.back().get()));
    }
    command_index++;
  }
  const Status status = commands[0].dbm->ProcessMulti(commands[0].params, true);
  if (status != Status::SUCCESS) {
    return status;
  }
  return proc_status;
}

Status ShardDBM::ProcessEach(RecordProcessor* proc, bool writable) {
  if (!open_) {
    return Status(Status::PRECONDITION_ERROR, "not opened database");
  }
  class ProxyProcessor final : public DBM::RecordProcessor {
   public:
    explicit ProxyProcessor(DBM::RecordProcessor* proc) : proc_(proc) {}
    std::string_view ProcessFull(std::string_view key, std::string_view value) override {
      return proc_->ProcessFull(key, value);
    }
   private:
    DBM::RecordProcessor* proc_;
  } proxy(proc);
  proc->ProcessEmpty(DBM::RecordProcessor::NOOP);
  for (auto& dbm : dbms_) {
    const Status status = dbm->ProcessEach(&proxy, writable);
    if (status != Status::SUCCESS) {
      return status;
    }
  }
  proc->ProcessEmpty(DBM::RecordProcessor::NOOP);
  return Status(Status::SUCCESS);
}

Status ShardDBM::Count(int64_t* count) {
  if (!open_) {
    return Status(Status::PRECONDITION_ERROR, "not opened database");
  }
  *count = 0;
  for (auto dbm : dbms_) {
    int64_t single_count = 0;
    const Status status = dbm->Count(&single_count);
    if (status != Status::SUCCESS) {
      return status;
    }
    *count += single_count;
  }
  return Status(Status::SUCCESS);
}

Status ShardDBM::GetFileSize(int64_t* size) {
  if (!open_) {
    return Status(Status::PRECONDITION_ERROR, "not opened database");
  }
  *size = 0;
  for (auto dbm : dbms_) {
    int64_t single_size = 0;
    const Status status = dbm->GetFileSize(&single_size);
    if (status != Status::SUCCESS) {
      return status;
    }
    *size += single_size;
  }
  return Status(Status::SUCCESS);
}

Status ShardDBM::GetFilePath(std::string* path) {
  if (!open_) {
    return Status(Status::PRECONDITION_ERROR, "not opened database");
  }
  *path = path_;
  return Status(Status::SUCCESS);
}

Status ShardDBM::GetTimestamp(double* timestamp) {
  if (!open_) {
    return Status(Status::PRECONDITION_ERROR, "not opened database");
  }
  Status status(Status::SUCCESS);
  double oldest = DOUBLEINF;
  for (auto& dbm : dbms_) {
    double timestamp = 0;
    status |= dbm->GetTimestamp(&timestamp);
    if (status == Status::SUCCESS) {
      oldest = std::min(oldest, timestamp);
    }
  }
  *timestamp = oldest;
  return status;
}

Status ShardDBM::Clear() {
  if (!open_) {
    return Status(Status::PRECONDITION_ERROR, "not opened database");
  }
  Status status(Status::SUCCESS);
  for (auto& dbm : dbms_) {
    status |= dbm->Clear();
  }
  return status;
}

Status ShardDBM::RebuildAdvanced(const std::map<std::string, std::string>& params) {
  if (!open_) {
    return Status(Status::PRECONDITION_ERROR, "not opened database");
  }
  Status status(Status::SUCCESS);
  for (auto& dbm : dbms_) {
    status |= dbm->RebuildAdvanced(params);
  }
  return status;
}

Status ShardDBM::ShouldBeRebuilt(bool* tobe) {
  if (!open_) {
    return Status(Status::PRECONDITION_ERROR, "not opened database");
  }
  *tobe = false;
  for (auto& dbm : dbms_) {
    bool single_tobe = false;
    const Status status = dbm->ShouldBeRebuilt(&single_tobe);
    if (status != Status::SUCCESS) {
      return status;
    }
    if (single_tobe) {
      *tobe = true;
    }
  }
  return Status(Status::SUCCESS);
}

Status ShardDBM::SynchronizeAdvanced(
    bool hard, FileProcessor* proc, const std::map<std::string, std::string>& params) {
  if (!open_) {
    return Status(Status::PRECONDITION_ERROR, "not opened database");
  }
  Status status(Status::SUCCESS);
  for (auto& dbm : dbms_) {
    status |= dbm->SynchronizeAdvanced(hard, proc, params);
  }
  return status;
}

Status ShardDBM::CopyFileData(const std::string& dest_path, bool sync_hard) {
  if (!open_) {
    return Status(Status::PRECONDITION_ERROR, "not opened database");
  }
  for (int32_t i = 0; i < static_cast<int32_t>(dbms_.size()); i++) {
    const std::string shard_path = StrCat(dest_path, SPrintF(
        "-%05d-of-%05d", i, static_cast<int32_t>(dbms_.size())));
    const Status status = dbms_[i]->CopyFileData(shard_path, sync_hard);
    if (status != Status::SUCCESS) {
      return status;
    }
  }
  return Status(Status::SUCCESS);
}

std::vector<std::pair<std::string, std::string>> ShardDBM::Inspect() {
  std::vector<std::pair<std::string, std::string>> merged;
  if (!open_) {
    return merged;
  }
  std::string class_name = "ShardDBM";
  for (const auto& rec : dbms_.front()->Inspect()) {
    if (rec.first == "class") {
      class_name = rec.second;
    }
  }
  merged.emplace_back(std::make_pair("class", class_name));
  bool healthy = true;
  int64_t num_records = 0;
  int64_t file_size = 0;
  for (auto dbm : dbms_) {
    if (!dbm->IsHealthy()) {
      healthy = false;
    }
    num_records += dbm->CountSimple();
    file_size += dbm->GetFileSizeSimple();
  }
  merged.emplace_back(std::make_pair("healthy", ToString(healthy)));
  merged.emplace_back(std::make_pair("num_records", ToString(num_records)));
  merged.emplace_back(std::make_pair("file_size", ToString(file_size)));
  merged.emplace_back(std::make_pair("path", path_));
  for (int32_t i = 0; i < static_cast<int32_t>(dbms_.size()); i++) {
    for (const auto& rec : dbms_[i]->Inspect()) {
      const std::string& name = SPrintF("%05d-%s", i, rec.first.c_str());
      merged.emplace_back(std::make_pair(name, rec.second));
    }
  }
  return merged;
}

bool ShardDBM::IsOpen() const {
  return open_;
}

bool ShardDBM::IsWritable() const {
  if (!open_) {
    return false;
  }
  return dbms_.front()->IsWritable();
}

bool ShardDBM::IsHealthy() const {
  if (!open_) {
    return false;
  }
  for (const auto& dbm : dbms_) {
    if (!dbm->IsHealthy()) {
      return false;
    }
  }
  return true;
}

bool ShardDBM::IsOrdered() const {
  if (!open_) {
    return false;
  }
  return dbms_.front()->IsOrdered();
}

std::unique_ptr<DBM::Iterator> ShardDBM::MakeIterator() {
  std::unique_ptr<ShardDBM::Iterator> iter(new Iterator(&dbms_));
  return iter;
}

std::unique_ptr<DBM> ShardDBM::MakeDBM() const {
  return std::make_unique<ShardDBM>();
}

DBM::UpdateLogger* ShardDBM::GetUpdateLogger() const {
  if (!open_) {
    return nullptr;
  }
  return dbms_.front()->GetUpdateLogger();
}

void ShardDBM::SetUpdateLogger(UpdateLogger* update_logger) {
  if (!open_) {
    return;
  }
  ulog_second_.SetUpdateLogger(update_logger);
  bool first = true;
  for (const auto& dbm : dbms_) {
    if (first) {
      dbm->SetUpdateLogger(update_logger);
      first = false;
    } else {
      dbm->SetUpdateLogger(&ulog_second_);
    }
  }
}

DBM* ShardDBM::GetInternalDBM() const {
  if (!open_) {
    return nullptr;
  }
  return dbms_.front()->GetInternalDBM();
}

ShardDBM::Iterator::Iterator(std::vector<std::shared_ptr<PolyDBM>>* dbms)
    : slots_(), heap_(), comp_(nullptr), asc_(false) {
  slots_.resize(dbms->size());
  for (int32_t i = 0; i < static_cast<int32_t>(dbms->size()); i++) {
    slots_[i].iter = (*dbms)[i]->MakeIterator().release();
  }
  const auto* dbm = dbms->front()->GetInternalDBM();
  const auto& dbm_type = dbm->GetType();
  if (dbm_type == typeid(TreeDBM)) {
    comp_ = dynamic_cast<const TreeDBM*>(dbm)->GetKeyComparator();
  }
  if (dbm_type == typeid(BabyDBM)) {
    comp_ = dynamic_cast<const BabyDBM*>(dbm)->GetKeyComparator();
  }
  if (comp_ == nullptr) {
    comp_ = LexicalKeyComparator;
  }
}

ShardDBM::Iterator::~Iterator() {
  for (auto& slot : slots_) {
    delete slot.iter;
  }
}

Status ShardDBM::Iterator::First() {
  heap_.clear();
  for (auto& slot : slots_) {
    const Status status = slot.iter->First();
    if (status != Status::SUCCESS) {
      return status;
    }
  }
  for (auto& slot : slots_) {
    const Status status = slot.iter->Get(&slot.key, &slot.value);
    if (status == Status::SUCCESS) {
      heap_.emplace_back(&slot);
      std::push_heap(heap_.begin(), heap_.end(), ShardSlotComparator(comp_, true));
    } else if (status != Status::NOT_FOUND_ERROR) {
      heap_.clear();
      return status;
    }
  }
  asc_ = true;
  return Status(Status::SUCCESS);
}

Status ShardDBM::Iterator::Last() {
  heap_.clear();
  for (auto& slot : slots_) {
    const Status status = slot.iter->Last();
    if (status != Status::SUCCESS) {
      return status;
    }
  }
  for (auto& slot : slots_) {
    const Status status = slot.iter->Get(&slot.key, &slot.value);
    if (status == Status::SUCCESS) {
      heap_.emplace_back(&slot);
      std::push_heap(heap_.begin(), heap_.end(), ShardSlotComparator(comp_, false));
    } else if (status != Status::NOT_FOUND_ERROR) {
      heap_.clear();
      return status;
    }
  }
  asc_ = false;
  return Status(Status::SUCCESS);
}

Status ShardDBM::Iterator::Jump(std::string_view key) {
  heap_.clear();
  for (auto& slot : slots_) {
    Status status = slot.iter->Jump(key);
    if (status != Status::SUCCESS) {
      if (status != Status::NOT_FOUND_ERROR) {
        return status;
      }
      continue;
    }
    status = slot.iter->Get(&slot.key, &slot.value);
    if (status == Status::SUCCESS) {
      heap_.emplace_back(&slot);
      std::push_heap(heap_.begin(), heap_.end(), ShardSlotComparator(comp_, true));
    } else if (status != Status::NOT_FOUND_ERROR) {
      heap_.clear();
      return status;
    }
  }
  asc_ = true;
  return Status(Status::SUCCESS);
}

Status ShardDBM::Iterator::JumpLower(std::string_view key, bool inclusive) {
  heap_.clear();
  for (auto& slot : slots_) {
    Status status = slot.iter->JumpLower(key, inclusive);
    if (status != Status::SUCCESS) {
      if (status != Status::NOT_FOUND_ERROR) {
        return status;
      }
      continue;
    }
    status = slot.iter->Get(&slot.key, &slot.value);
    if (status == Status::SUCCESS) {
      heap_.emplace_back(&slot);
      std::push_heap(heap_.begin(), heap_.end(), ShardSlotComparator(comp_, false));
    } else if (status != Status::NOT_FOUND_ERROR) {
      heap_.clear();
      return status;
    }
  }
  asc_ = false;
  return Status(Status::SUCCESS);
}

Status ShardDBM::Iterator::JumpUpper(std::string_view key, bool inclusive) {
  heap_.clear();
  for (auto& slot : slots_) {
    Status status = slot.iter->JumpUpper(key, inclusive);
    if (status != Status::SUCCESS) {
      if (status != Status::NOT_FOUND_ERROR) {
        return status;
      }
      continue;
    }
    status = slot.iter->Get(&slot.key, &slot.value);
    if (status == Status::SUCCESS) {
      heap_.emplace_back(&slot);
      std::push_heap(heap_.begin(), heap_.end(), ShardSlotComparator(comp_, true));
    } else if (status != Status::NOT_FOUND_ERROR) {
      heap_.clear();
      return status;
    }
  }
  asc_ = true;
  return Status(Status::SUCCESS);
}

Status ShardDBM::Iterator::Next() {
  if (heap_.empty()) {
    return Status(Status::NOT_FOUND_ERROR);
  }
  if (!asc_) {
    const Status status = Jump(heap_.front()->key);
    if (status != Status::SUCCESS) {
      return status;
    }
    if (heap_.empty()) {
      return Status(Status::NOT_FOUND_ERROR);
    }
  }
  std::pop_heap(heap_.begin(), heap_.end(), ShardSlotComparator(comp_, true));
  auto* slot = heap_.back();
  Status status = slot->iter->Next();
  if (status == Status::SUCCESS) {
    status = slot->iter->Get(&slot->key, &slot->value);
  }
  if (status == Status::SUCCESS) {
    std::push_heap(heap_.begin(), heap_.end(), ShardSlotComparator(comp_, true));
  } else {
    heap_.pop_back();
    if (status != Status::NOT_FOUND_ERROR) {
      return status;
    }
  }
  return Status(Status::SUCCESS);
}

Status ShardDBM::Iterator::Previous() {
  if (heap_.empty()) {
    return Status(Status::NOT_FOUND_ERROR);
  }
  if (asc_) {
    const Status status = JumpLower(heap_.front()->key, true);
    if (status != Status::SUCCESS) {
      return status;
    }
    if (heap_.empty()) {
      return Status(Status::NOT_FOUND_ERROR);
    }
  }
  std::pop_heap(heap_.begin(), heap_.end(), ShardSlotComparator(comp_, false));
  auto* slot = heap_.back();
  Status status = slot->iter->Previous();
  if (status == Status::SUCCESS) {
    status = slot->iter->Get(&slot->key, &slot->value);
  }
  if (status == Status::SUCCESS) {
    std::push_heap(heap_.begin(), heap_.end(), ShardSlotComparator(comp_, false));
  } else {
    heap_.pop_back();
    if (status != Status::NOT_FOUND_ERROR) {
      return status;
    }
  }
  return Status(Status::SUCCESS);
}

Status ShardDBM::Iterator::Process(RecordProcessor* proc, bool writable) {
  if (heap_.empty()) {
    return Status(Status::NOT_FOUND_ERROR);
  }
  auto* slot = heap_.front();
  class Checker : public DBM::RecordProcessor {
   public:
    explicit Checker(RecordProcessor* proc) : proc_(proc), removed_(false)  {}
    std::string_view ProcessFull(std::string_view key, std::string_view value) override {
      const std::string_view rv = proc_->ProcessFull(key, value);
      if (rv.data() == REMOVE.data()) {
        removed_ = true;
      }
      return rv;
    }
    std::string_view ProcessEmpty(std::string_view key) override {
      return proc_->ProcessEmpty(key);
    }
    bool HasRemoved() const {
      return removed_;
    }
   private:
    DBM::RecordProcessor* proc_;
    bool removed_;
  } checker(proc);
  const Status status = slot->iter->Process(&checker, writable);
  if (status == Status::SUCCESS && writable && checker.HasRemoved()) {
    std::pop_heap(heap_.begin(), heap_.end(), ShardSlotComparator(comp_, true));
    slot = heap_.back();
    const Status get_status = slot->iter->Get(&slot->key, &slot->value);
    if (get_status == Status::SUCCESS) {
      std::push_heap(heap_.begin(), heap_.end(), ShardSlotComparator(comp_, true));
    } else {
      heap_.pop_back();
    }
  }
  return status;
}

Status ShardDBM::Iterator::Get(std::string* key, std::string* value) {
  if (heap_.empty()) {
    return Status(Status::NOT_FOUND_ERROR);
  }
  auto* slot = heap_.front();
  if (key != nullptr) {
    *key = slot->key;
  }
  if (value != nullptr) {
    *value = slot->value;
  }
  return Status(Status::SUCCESS);
}

Status ShardDBM::Iterator::Set(
    std::string_view value, std::string* old_key, std::string* old_value) {
  if (heap_.empty()) {
    return Status(Status::NOT_FOUND_ERROR);
  }
  auto* slot = heap_.front();
  const Status status = slot->iter->Set(value, old_key, old_value);
  if (status == Status::SUCCESS) {
    slot->value = value;
  }
  return status;
}

Status ShardDBM::Iterator::Remove(std::string* old_key, std::string* old_value) {
  if (heap_.empty()) {
    return Status(Status::NOT_FOUND_ERROR);
  }
  auto* slot = heap_.front();
  const Status status = slot->iter->Remove(old_key, old_value);
  if (status == Status::SUCCESS) {
    std::pop_heap(heap_.begin(), heap_.end(), ShardSlotComparator(comp_, true));
    slot = heap_.back();
    const Status get_status = slot->iter->Get(&slot->key, &slot->value);
    if (get_status == Status::SUCCESS) {
      std::push_heap(heap_.begin(), heap_.end(), ShardSlotComparator(comp_, true));
    } else {
      heap_.pop_back();
    }
  }
  return status;
}

Status ShardDBM::GetNumberOfShards(const std::string& path, int32_t* num_shards) {
  assert(num_shards != nullptr);
  const std::string dir_path = tkrzw::PathToDirectoryName(path);
  const std::string base_name = tkrzw::PathToBaseName(path);
  const std::string zero_name = base_name + "-00000-of-";
  std::vector<std::string> child_names;
  Status status = ReadDirectory(dir_path, &child_names);
  if (status != Status::SUCCESS) {
    return status;
  }
  std::sort(child_names.begin(), child_names.end());
  int32_t cand_num_shards = 0;
  for (const auto& child_name : child_names) {
    if (tkrzw::StrBeginsWith(child_name, zero_name)) {
      if (cand_num_shards > 0) {
        return Status(Status::DUPLICATION_ERROR, "multiple candidates");
      }
      cand_num_shards = StrToInt(child_name.substr(zero_name.size()));
    }
  }
  if (cand_num_shards < 1) {
    return Status(Status::NOT_FOUND_ERROR, "no matching files");
  }
  *num_shards = cand_num_shards;
  return Status(Status::SUCCESS);
}

Status ShardDBM::RestoreDatabase(
    const std::string& old_file_path, const std::string& new_file_path,
    const std::string& class_name, int64_t end_offset, std::string_view cipher_key) {
  int32_t num_shards = 0;
  Status status = GetNumberOfShards(old_file_path, &num_shards);
  if (status != Status::SUCCESS) {
    return status;
  }
  for (int32_t i = 0; i < num_shards; i++) {
    const std::string old_join_path = old_file_path + SPrintF("-%05d-of-%05d", i, num_shards);
    const std::string new_join_path = new_file_path + SPrintF("-%05d-of-%05d", i, num_shards);
    status |= PolyDBM::RestoreDatabase(old_join_path, new_join_path, class_name,
                                       end_offset, cipher_key);
  }
  return status;
}

Status ShardDBM::RenameDatabase(
    const std::string& old_file_path, const std::string& new_file_path) {
  int32_t num_shards = 0;
  Status status = GetNumberOfShards(old_file_path, &num_shards);
  if (status != Status::SUCCESS) {
    return status;
  }
  for (int32_t i = 0; i < num_shards; i++) {
    const std::string old_join_path = old_file_path + SPrintF("-%05d-of-%05d", i, num_shards);
    const std::string new_join_path = new_file_path + SPrintF("-%05d-of-%05d", i, num_shards);
    status |= RenameFile(old_join_path, new_join_path);
  }
  return status;
}

}  // namespace tkrzw

// END OF FILE
