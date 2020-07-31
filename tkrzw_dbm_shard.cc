/*************************************************************************************************
 * Polymorphic datatabase manager adapter
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
#include "tkrzw_file.h"
#include "tkrzw_file_mmap.h"
#include "tkrzw_file_pos.h"
#include "tkrzw_file_util.h"
#include "tkrzw_lib_common.h"
#include "tkrzw_str_util.h"
#include "tkrzw_sys_config.h"

namespace tkrzw {

ShardDBM::ShardDBM() : dbms_(), open_(false), path_() {}

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
    const std::string dir_path = tkrzw::PathToDirectoryName(path);
    const std::string base_name = tkrzw::PathToBaseName(path);
    const std::string zero_name = base_name + "-00000-of-";
    std::vector<std::string> child_names;
    const Status status = tkrzw::ReadDirectory(dir_path, &child_names);
    if (status != Status::SUCCESS) {
      return status;
    }
    std::sort(child_names.begin(), child_names.end());
    for (const auto& child_name : child_names) {
      if (tkrzw::StrBeginsWith(child_name, zero_name)) {
        num_shards = StrToInt(child_name.substr(zero_name.size()));
        break;
      }
    }
    if (num_shards < 1) {
      num_shards = 1;
    }
  }
  dbms_.reserve(num_shards);
  for (int32_t i = 0; i < num_shards; i++) {
    dbms_.emplace_back(std::make_unique<PolyDBM>());
  }
  auto mod_params = params;
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
  open_ = true;
  path_ = path;
  return Status(Status::SUCCESS);
}

Status ShardDBM::Close() {
  if (!open_) {
    return Status(Status::PRECONDITION_ERROR, "not opened database");
  }
  Status status(Status::SUCCESS);
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

Status ShardDBM::Set(std::string_view key, std::string_view value, bool overwrite) {
  if (!open_) {
    return Status(Status::PRECONDITION_ERROR, "not opened database");
  }
  const int32_t shard_index = SecondaryHash(key, dbms_.size());
  auto dbm = dbms_[shard_index];
  return dbm->Set(key, value, overwrite);
}

Status ShardDBM::Remove(std::string_view key) {
  if (!open_) {
    return Status(Status::PRECONDITION_ERROR, "not opened database");
  }
  const int32_t shard_index = SecondaryHash(key, dbms_.size());
  auto dbm = dbms_[shard_index];
  return dbm->Remove(key);
}

Status ShardDBM::Append(std::string_view key, std::string_view value, std::string_view delim) {
  if (!open_) {
    return Status(Status::PRECONDITION_ERROR, "not opened database");
  }
  const int32_t shard_index = SecondaryHash(key, dbms_.size());
  auto dbm = dbms_[shard_index];
  return dbm->Append(key, value, delim);
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

Status ShardDBM::CopyFile(const std::string& dest_path) {
  if (!open_) {
    return Status(Status::PRECONDITION_ERROR, "not opened database");
  }
  for (int32_t i = 0; i < static_cast<int32_t>(dbms_.size()); i++) {
    const std::string shard_path = StrCat(dest_path, SPrintF(
        "-%05d-of-%05d", i, static_cast<int32_t>(dbms_.size())));
    const Status status = dbms_[i]->CopyFile(shard_path);
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
  if (typeid(*dbm) == typeid(TreeDBM)) {
    comp_ = dynamic_cast<const TreeDBM*>(dbm)->GetKeyComparator();
  }
  if (typeid(*dbm) == typeid(BabyDBM)) {
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
  return slot->iter->Process(proc, writable);
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

Status ShardDBM::Iterator::Set(std::string_view value) {
  if (heap_.empty()) {
    return Status(Status::NOT_FOUND_ERROR);
  }
  auto* slot = heap_.front();
  return slot->iter->Set(value);
}

Status ShardDBM::Iterator::Remove() {
  if (heap_.empty()) {
    return Status(Status::NOT_FOUND_ERROR);
  }
  auto* slot = heap_.front();
  return slot->iter->Remove();
}

Status ShardDBM::RestoreDatabase(
    const std::string& old_file_path, const std::string& new_file_path,
    const std::string& class_name, int64_t end_offset) {
  const std::string dir_path = tkrzw::PathToDirectoryName(old_file_path);
  const std::string base_name = tkrzw::PathToBaseName(old_file_path);
  const std::string zero_name = base_name + "-00000-of-";
  std::vector<std::string> child_names;
  Status status = tkrzw::ReadDirectory(dir_path, &child_names);
  if (status != Status::SUCCESS) {
    return status;
  }
  std::sort(child_names.begin(), child_names.end());
  int32_t num_shards = 0;
  for (const auto& child_name : child_names) {
    if (tkrzw::StrBeginsWith(child_name, zero_name)) {
      num_shards = StrToInt(child_name.substr(zero_name.size()));
    }
  }
  if (num_shards < 1) {
    return Status(Status::NOT_FOUND_ERROR);
  }
  for (int32_t i = 0; i < num_shards; i++) {
    const std::string old_join_path = old_file_path + SPrintF("-%05d-of-%05d", i, num_shards);
    const std::string new_join_path = new_file_path + SPrintF("-%05d-of-%05d", i, num_shards);
    status |= PolyDBM::RestoreDatabase(old_join_path, new_join_path, class_name, end_offset);
  }
  return status;
}

}  // namespace tkrzw

// END OF FILE
