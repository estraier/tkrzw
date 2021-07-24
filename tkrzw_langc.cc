/*************************************************************************************************
 * C language binding of Tkrzw
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

#include <string>
#include <string_view>
#include <map>
#include <memory>
#include <vector>

#include <cstddef>
#include <cstdint>

#include "tkrzw_dbm.h"
#include "tkrzw_dbm_common_impl.h"
#include "tkrzw_dbm_poly.h"
#include "tkrzw_dbm_shard.h"
#include "tkrzw_file.h"
#include "tkrzw_file_poly.h"
#include "tkrzw_langc.h"
#include "tkrzw_lib_common.h"
#include "tkrzw_str_util.h"
#include "tkrzw_thread_util.h"

using namespace tkrzw;

extern "C" {

const char* const TKRZW_PACKAGE_VERSION = PACKAGE_VERSION;

const char* const TKRZW_LIBRARY_VERSION = LIBRARY_VERSION;

const char* const TKRZW_OS_NAME = OS_NAME;

const int64_t TKRZW_INT64MIN = INT64MIN;

const int64_t TKRZW_INT64MAX = INT64MAX;

thread_local Status last_status(Status::SUCCESS);
thread_local std::string last_message;

const char* const TKRZW_REC_PROC_NOOP = (char*)-1;

const char* const TKRZW_REC_PROC_REMOVE = (char*)-2;

struct RecordProcessorWrapper : public DBM::RecordProcessor {
 public:
  tkrzw_record_processor proc = nullptr;
  void* arg = nullptr;
  std::string_view ProcessFull(std::string_view key, std::string_view value) override {
    int32_t new_value_size = 0;
    const char* new_value_ptr =
        proc(arg, key.data(), key.size(), value.data(), value.size(), &new_value_size);
    if (new_value_ptr == TKRZW_REC_PROC_NOOP) {
      return NOOP;
    }
    if (new_value_ptr == TKRZW_REC_PROC_REMOVE) {
      return REMOVE;
    }
    return std::string_view(new_value_ptr, new_value_size);
  }
  std::string_view ProcessEmpty(std::string_view key) override {
    int32_t new_value_size = 0;
    const char* new_value_ptr = proc(arg, key.data(), key.size(), nullptr, -1, &new_value_size);
    if (new_value_ptr == TKRZW_REC_PROC_NOOP) {
      return NOOP;
    }
    if (new_value_ptr == TKRZW_REC_PROC_REMOVE) {
      return REMOVE;
    }
    return std::string_view(new_value_ptr, new_value_size);
  }
};

void tkrzw_set_last_status(int32_t code, const char* message) {
  if (message == nullptr) {
    last_status.Set(Status::Code(code));
    last_message.clear();
  } else {
    last_status.Set(Status::Code(code), message);
    last_message = message;
  }
}

TkrzwStatus tkrzw_get_last_status() {
  TkrzwStatus status;
  status.code = last_status.GetCode();
  if (last_status.HasMessage()) {
    last_message = last_status.GetMessage();
    status.message = last_message.c_str();
  } else {
    status.message = "";
  }
  return status;
}

int32_t tkrzw_get_last_status_code() {
  return last_status.GetCode();
}

const char* tkrzw_get_last_status_message() {
  if (last_status.HasMessage()) {
    last_message = last_status.GetMessage();
    return last_message.c_str();
  }
  return "";
}

const char* tkrzw_status_code_name(int32_t code) {
  return Status::CodeName(static_cast<Status::Code>(code));
}

double tkrzw_get_wall_time() {
  return GetWallTime();
}

int64_t tkrzw_get_memory_capacity() {
  return GetMemoryCapacity();
}

int64_t tkrzw_get_memory_usage() {
  return GetMemoryUsage();
}

uint64_t tkrzw_primary_hash(const char* data_ptr, int32_t data_size, uint64_t num_buckets) {
  assert(data_ptr != nullptr);
  if (data_size < 0) {
    data_size = std::strlen(data_ptr);
  }
  return PrimaryHash(std::string_view(data_ptr, data_size), num_buckets);
}

uint64_t tkrzw_secondary_hash(const char* data_ptr, int32_t data_size, uint64_t num_shards) {
  assert(data_ptr != nullptr);
  if (data_size < 0) {
    data_size = std::strlen(data_ptr);
  }
  return SecondaryHash(std::string_view(data_ptr, data_size), num_shards);
}

void tkrzw_free_str_array(TkrzwStr* array, int32_t size) {
  assert(array != nullptr);
  for (int32_t i = 0; i < size; i++) {
    xfree(const_cast<char*>(array[i].ptr));
  }
  xfree(array);
}

void tkrzw_free_str_map(TkrzwKeyValuePair* array, int32_t size) {
  assert(array != nullptr);
  for (int32_t i = 0; i < size; i++) {
    xfree(const_cast<char*>(array[i].key_ptr));
  }
  xfree(array);
}

TkrzwKeyValuePair* tkrzw_search_str_map(TkrzwKeyValuePair* array, int32_t size,
                                        const char* key_ptr, int32_t key_size) {
  assert(array != nullptr);
  if (key_size < 0) {
    key_size = std::strlen(key_ptr);
  }
  for (int32_t i = 0; i < size; i++) {
    TkrzwKeyValuePair* elem = array + i;
    if (elem->key_size == key_size && memcmp(elem->key_ptr, key_ptr, key_size) == 0) {
      return elem;
    }
  }
  return nullptr;
}

int32_t tkrzw_str_search_regex(const char* text, const char* pattern) {
  assert(text != nullptr && pattern != nullptr);
  return StrSearchRegex(text, pattern);
}

char* tkrzw_str_replace_regex(const char* text, const char* pattern, const char* replace) {
  assert(text != nullptr && pattern != nullptr && replace != nullptr);
  const std::string& processed = StrReplaceRegex(text, pattern, replace);
  char* result = static_cast<char*>(xmalloc(processed.size() + 1));
  std::memcpy(result, processed.c_str(), processed.size() + 1);
  return result;
}

int32_t tkrzw_str_edit_distance_lev(const char* a, const char* b, bool utf) {
  assert(a != nullptr && b != nullptr);
  if (utf) {
    const std::vector<uint32_t> a_ucs = ConvertUTF8ToUCS4(a);
    const std::vector<uint32_t> b_ucs = ConvertUTF8ToUCS4(b);
    return EditDistanceLev(a_ucs, b_ucs);
  }
  return EditDistanceLev(std::string_view(a), std::string_view(b));
}

char* tkrzw_str_escape_c(const char* ptr, int32_t size, bool esc_nonasc, int32_t* res_size) {
  assert(ptr != nullptr);
  if (size < 0) {
    size = strlen(ptr);
  }
  const std::string& result = StrEscapeC(std::string_view(ptr, size), esc_nonasc);
  char* res_ptr = static_cast<char*>(xmalloc(result.size() + 1));
  std::memcpy(res_ptr, result.c_str(), result.size() + 1);
  if (res_size != nullptr) {
    *res_size = result.size();
  }
  return res_ptr;
}

char* tkrzw_str_unescape_c(const char* ptr, int32_t size, int32_t* res_size) {
  assert(ptr != nullptr);
  if (size < 0) {
    size = strlen(ptr);
  }
  const std::string& result = StrUnescapeC(std::string_view(ptr, size));
  char* res_ptr = static_cast<char*>(xmalloc(result.size() + 1));
  std::memcpy(res_ptr, result.c_str(), result.size() + 1);
  if (res_size != nullptr) {
    *res_size = result.size();
  }
  return res_ptr;
}

char* tkrzw_str_append(char* modified, const char* appended) {
  assert(appended != nullptr);
  const size_t append_size = strlen(appended);
  if (modified == nullptr) {
    char* modified = static_cast<char*>(xreallocappend(nullptr, append_size + 1));
    std::memcpy(modified, appended, append_size + 1);
    return modified;
  }
  const size_t orig_size = strlen(modified);
  modified = static_cast<char*>(xreallocappend(modified, orig_size + append_size + 1));
  std::memcpy(modified + orig_size, appended, append_size + 1);
  return modified;
}

TkrzwDBM* tkrzw_dbm_open(const char* path, bool writable, const char* params) {
  assert(path != nullptr && params != nullptr);
  std::map<std::string, std::string> xparams = StrSplitIntoMap(params, ",", "=");
  const int32_t num_shards = tkrzw::StrToInt(tkrzw::SearchMap(xparams, "num_shards", "-1"));
  int32_t open_options = 0;
  if (tkrzw::StrToBool(tkrzw::SearchMap(xparams, "truncate", "false"))) {
    open_options |= tkrzw::File::OPEN_TRUNCATE;
  }
  if (tkrzw::StrToBool(tkrzw::SearchMap(xparams, "no_create", "false"))) {
    open_options |= tkrzw::File::OPEN_NO_CREATE;
  }
  if (tkrzw::StrToBool(tkrzw::SearchMap(xparams, "no_wait", "false"))) {
    open_options |= tkrzw::File::OPEN_NO_WAIT;
  }
  if (tkrzw::StrToBool(tkrzw::SearchMap(xparams, "no_lock", "false"))) {
    open_options |= tkrzw::File::OPEN_NO_LOCK;
  }
  xparams.erase("truncate");
  xparams.erase("no_create");
  xparams.erase("no_wait");
  xparams.erase("no_lock");
  ParamDBM* dbm = nullptr;
  if (num_shards >= 0) {
    dbm = new tkrzw::ShardDBM();
  } else {
    dbm = new tkrzw::PolyDBM();
  }
  last_status = dbm->OpenAdvanced(path, writable, open_options, xparams);
  if (last_status != Status::SUCCESS) {
    delete dbm;
    return nullptr;
  }
  return reinterpret_cast<TkrzwDBM*>(dbm);
}

bool tkrzw_dbm_close(TkrzwDBM* dbm) {
  assert(dbm != nullptr);
  ParamDBM* xdbm = reinterpret_cast<ParamDBM*>(dbm);
  last_status = xdbm->Close();
  bool rv = last_status == Status::SUCCESS;
  delete xdbm;
  return rv;
}

bool tkrzw_dbm_process(
    TkrzwDBM* dbm, const char* key_ptr, int32_t key_size, tkrzw_record_processor proc,
    void* proc_arg, bool writable) {
  assert(dbm != nullptr && key_ptr != nullptr && proc != nullptr);
  if (key_size < 0) {
    key_size = std::strlen(key_ptr);
  }
  ParamDBM* xdbm = reinterpret_cast<ParamDBM*>(dbm);
  RecordProcessorWrapper xproc;
  xproc.proc = proc;
  xproc.arg = proc_arg;
  last_status = xdbm->Process(std::string_view(key_ptr, key_size), &xproc, writable);
  return last_status == Status::SUCCESS;
}

char* tkrzw_dbm_get(TkrzwDBM* dbm, const char* key_ptr, int32_t key_size, int32_t* value_size) {
  assert(dbm != nullptr && key_ptr != nullptr);
  if (key_size < 0) {
    key_size = std::strlen(key_ptr);
  }
  ParamDBM* xdbm = reinterpret_cast<ParamDBM*>(dbm);
  std::string value;
  last_status = xdbm->Get(std::string_view(key_ptr, key_size), &value);
  if (last_status != Status::SUCCESS) {
    return nullptr;
  }
  char* value_ptr = reinterpret_cast<char*>(xmalloc(value.size() + 1));
  std::memcpy(value_ptr, value.c_str(), value.size() + 1);
  if (value_size != nullptr) {
    *value_size = value.size();
  }
  return value_ptr;
}

TkrzwKeyValuePair* tkrzw_dbm_get_multi(
    TkrzwDBM* dbm, const TkrzwStr* keys, int32_t num_keys, int32_t* num_matched) {
  assert(dbm != nullptr && keys != nullptr && num_matched != nullptr);
  ParamDBM* xdbm = reinterpret_cast<ParamDBM*>(dbm);
  std::vector<std::string> xkeys;
  xkeys.reserve(num_keys);
  for (int32_t i = 0; i < num_keys; i++) {
    const auto& key = keys[i];
    const int32_t key_size = key.size < 0 ? strlen(key.ptr) : key.size;
    xkeys.emplace_back(std::string(key.ptr, key_size));
  }
  std::map<std::string, std::string> records;
  last_status = xdbm->GetMulti(xkeys, &records);
  TkrzwKeyValuePair* array = static_cast<TkrzwKeyValuePair*>(xmalloc(
      sizeof(TkrzwKeyValuePair) * records.size() + 1));
  int32_t num_recs = 0;
  for (const auto& record : records) {
    auto& elem = array[num_recs];
    char* key_ptr = static_cast<char*>(xmalloc(record.first.size() + record.second.size() + 2));
    std::memcpy(key_ptr, record.first.c_str(), record.first.size() + 1);
    char* value_ptr = key_ptr + record.first.size() + 1;
    std::memcpy(value_ptr, record.second.c_str(), record.second.size() + 1);
    elem.key_ptr = key_ptr;
    elem.key_size = record.first.size();
    elem.value_ptr = value_ptr;
    elem.value_size = record.second.size();
    num_recs++;
  }
  *num_matched = records.size();
  return array;
}

bool tkrzw_dbm_set(
    TkrzwDBM* dbm, const char* key_ptr, int32_t key_size,
    const char* value_ptr, int32_t value_size, bool overwrite) {
  assert(dbm != nullptr && key_ptr != nullptr && value_ptr != nullptr);
  if (key_size < 0) {
    key_size = std::strlen(key_ptr);
  }
  if (value_size < 0) {
    value_size = std::strlen(value_ptr);
  }
  ParamDBM* xdbm = reinterpret_cast<ParamDBM*>(dbm);
  last_status = xdbm->Set(
      std::string_view(key_ptr, key_size), std::string_view(value_ptr, value_size), overwrite);
  return last_status == Status::SUCCESS;
}

bool tkrzw_dbm_set_multi(
    TkrzwDBM* dbm, const TkrzwKeyValuePair* records, int32_t num_records, bool overwrite) {
  assert(dbm != nullptr && records != nullptr);
  ParamDBM* xdbm = reinterpret_cast<ParamDBM*>(dbm);
  std::map<std::string, std::string> xrecords;
  for (int32_t i = 0; i < num_records; i++) {
    const auto& record = records[i];
    const int32_t key_size =
        record.key_size < 0 ? strlen(record.key_ptr) : record.key_size;
    const int32_t value_size =
        record.value_size < 0 ? strlen(record.value_ptr) : record.value_size;
    xrecords.emplace(std::string(record.key_ptr, key_size),
                     std::string(record.value_ptr, value_size));
  }
  last_status = xdbm->SetMulti(xrecords, overwrite);
  return last_status == Status::SUCCESS;
}

bool tkrzw_dbm_remove(TkrzwDBM* dbm, const char* key_ptr, int32_t key_size) {
  assert(dbm != nullptr && key_ptr != nullptr);
  if (key_size < 0) {
    key_size = std::strlen(key_ptr);
  }
  ParamDBM* xdbm = reinterpret_cast<ParamDBM*>(dbm);
  last_status = xdbm->Remove(std::string_view(key_ptr, key_size));
  return last_status == Status::SUCCESS;
}

bool tkrzw_dbm_remove_multi(TkrzwDBM* dbm, const TkrzwStr* keys, int32_t num_keys) {
  assert(dbm != nullptr && keys != nullptr);
  ParamDBM* xdbm = reinterpret_cast<ParamDBM*>(dbm);
  std::vector<std::string> xkeys;
  xkeys.reserve(num_keys);
  for (int32_t i = 0; i < num_keys; i++) {
    const auto& key = keys[i];
    const int32_t key_size = key.size < 0 ? strlen(key.ptr) : key.size;
    xkeys.emplace_back(std::string(key.ptr, key_size));
  }
  last_status = xdbm->RemoveMulti(xkeys);
  return last_status == Status::SUCCESS;
}

bool tkrzw_dbm_append(
    TkrzwDBM* dbm, const char* key_ptr, int32_t key_size,
    const char* value_ptr, int32_t value_size,
    const char* delim_ptr, int32_t delim_size) {
  assert(dbm != nullptr && key_ptr != nullptr && value_ptr != nullptr && delim_ptr != nullptr);
  if (key_size < 0) {
    key_size = std::strlen(key_ptr);
  }
  if (value_size < 0) {
    value_size = std::strlen(value_ptr);
  }
  if (delim_size < 0) {
    delim_size = std::strlen(delim_ptr);
  }
  ParamDBM* xdbm = reinterpret_cast<ParamDBM*>(dbm);
  last_status = xdbm->Append(
      std::string_view(key_ptr, key_size), std::string_view(value_ptr, value_size),
      std::string_view(delim_ptr, delim_size));
  return last_status == Status::SUCCESS;
}

bool tkrzw_dbm_append_multi(
    TkrzwDBM* dbm, const TkrzwKeyValuePair* records, int32_t num_records,
    const char* delim_ptr, int32_t delim_size) {
  assert(dbm != nullptr && records != nullptr && delim_ptr != nullptr);
  if (delim_size < 0) {
    delim_size = std::strlen(delim_ptr);
  }
  ParamDBM* xdbm = reinterpret_cast<ParamDBM*>(dbm);
  std::map<std::string, std::string> xrecords;
  for (int32_t i = 0; i < num_records; i++) {
    const auto& record = records[i];
    const int32_t key_size =
        record.key_size < 0 ? strlen(record.key_ptr) : record.key_size;
    const int32_t value_size =
        record.value_size < 0 ? strlen(record.value_ptr) : record.value_size;
    xrecords.emplace(std::string(record.key_ptr, key_size),
                     std::string(record.value_ptr, value_size));
  }
  last_status = xdbm->AppendMulti(xrecords, std::string_view(delim_ptr, delim_size));
  return last_status == Status::SUCCESS;
}

bool tkrzw_dbm_compare_exchange(
    TkrzwDBM* dbm, const char* key_ptr, int32_t key_size,
    const char* expected_ptr, int32_t expected_size,
    const char* desired_ptr, int32_t desired_size) {
  assert(dbm != nullptr && key_ptr != nullptr);
  if (key_size < 0) {
    key_size = std::strlen(key_ptr);
  }
  if (expected_ptr != nullptr && expected_size < 0) {
    expected_size = std::strlen(expected_ptr);
  }
  if (desired_ptr != nullptr && desired_size < 0) {
    desired_size = std::strlen(desired_ptr);
  }
  ParamDBM* xdbm = reinterpret_cast<ParamDBM*>(dbm);
  last_status = xdbm->CompareExchange(
      std::string_view(key_ptr, key_size), std::string_view(expected_ptr, expected_size),
      std::string_view(desired_ptr, desired_size));
  return last_status == Status::SUCCESS;
}

int64_t tkrzw_dbm_increment(
    TkrzwDBM* dbm, const char* key_ptr, int32_t key_size,
    int64_t increment, int64_t initial) {
  assert(dbm != nullptr && key_ptr != nullptr);
  if (key_size < 0) {
    key_size = std::strlen(key_ptr);
  }
  ParamDBM* xdbm = reinterpret_cast<ParamDBM*>(dbm);
  int64_t current = 0;
  last_status =
      xdbm->Increment(std::string_view(key_ptr, key_size), increment, &current, initial);
  if (last_status != Status::SUCCESS) {
    return INT64MIN;
  }
  return current;
}

bool tkrzw_dbm_process_multi(
    TkrzwDBM* dbm, TkrzwKeyProcPair* key_proc_pairs, int32_t num_pairs, bool writable) {
  assert(dbm != nullptr && key_proc_pairs != nullptr);
  std::vector<RecordProcessorWrapper> xprocs(num_pairs);
  std::vector<std::pair<std::string_view, DBM::RecordProcessor*>> xkey_proc_pairs(num_pairs);
  for (int32_t i = 0; i < num_pairs; i++) {
    auto& key_proc_pair = key_proc_pairs[i];
    auto& xproc = xprocs[i];
    xproc.proc = key_proc_pair.proc;
    xproc.arg = key_proc_pair.proc_arg;
    auto& xpair = xkey_proc_pairs[i];
    const int32_t key_size =
        key_proc_pair.key_size < 0 ? strlen(key_proc_pair.key_ptr) : key_proc_pair.key_size;
    xpair.first = std::string_view(key_proc_pair.key_ptr, key_size);
    xpair.second = &xproc;
  }
  ParamDBM* xdbm = reinterpret_cast<ParamDBM*>(dbm);
  last_status = xdbm->ProcessMulti(xkey_proc_pairs, writable);
  return last_status == Status::SUCCESS;
}

bool tkrzw_dbm_compare_exchange_multi(
    TkrzwDBM* dbm, TkrzwKeyValuePair* expected, int32_t num_expected,
    TkrzwKeyValuePair* desired, int32_t num_desired) {
  assert(dbm != nullptr && expected != nullptr && desired != nullptr);
  std::vector<std::pair<std::string_view, std::string_view>> expected_vec(num_expected);
  for (int32_t i = 0; i < num_expected; i++) {
    auto& key_value_pair = expected[i];
    auto& xpair = expected_vec[i];
    const int32_t key_size =
        key_value_pair.key_size < 0 ? strlen(key_value_pair.key_ptr) : key_value_pair.key_size;
    xpair.first = std::string_view(key_value_pair.key_ptr, key_size);
    const int32_t value_size =
        key_value_pair.value_ptr != nullptr && key_value_pair.value_size < 0 ?
        strlen(key_value_pair.value_ptr) : key_value_pair.value_size;
    xpair.second = std::string_view(key_value_pair.value_ptr, value_size);
  }
  std::vector<std::pair<std::string_view, std::string_view>> desired_vec(num_desired);
  for (int32_t i = 0; i < num_desired; i++) {
    auto& key_value_pair = desired[i];
    auto& xpair = desired_vec[i];
    const int32_t key_size =
        key_value_pair.key_size < 0 ? strlen(key_value_pair.key_ptr) : key_value_pair.key_size;
    xpair.first = std::string_view(key_value_pair.key_ptr, key_size);
    const int32_t value_size =
        key_value_pair.value_ptr != nullptr && key_value_pair.value_size < 0 ?
        strlen(key_value_pair.value_ptr) : key_value_pair.value_size;
    xpair.second = std::string_view(key_value_pair.value_ptr, value_size);
  }
  ParamDBM* xdbm = reinterpret_cast<ParamDBM*>(dbm);
  last_status = xdbm->CompareExchangeMulti(expected_vec, desired_vec);
  return last_status == Status::SUCCESS;
}

bool tkrzw_dbm_process_each(
    TkrzwDBM* dbm, tkrzw_record_processor proc, void* proc_arg, bool writable) {
  assert(dbm != nullptr && proc != nullptr);
  ParamDBM* xdbm = reinterpret_cast<ParamDBM*>(dbm);
  RecordProcessorWrapper xproc;
  xproc.proc = proc;
  xproc.arg = proc_arg;
  last_status = xdbm->ProcessEach(&xproc, writable);
  return last_status == Status::SUCCESS;
}

int64_t tkrzw_dbm_count(TkrzwDBM* dbm) {
  assert(dbm != nullptr);
  ParamDBM* xdbm = reinterpret_cast<ParamDBM*>(dbm);
  int64_t count = 0;
  last_status = xdbm->Count(&count);
  if (last_status != Status::SUCCESS) {
    return -1;
  }
  return count;
}

int64_t tkrzw_dbm_get_file_size(TkrzwDBM* dbm) {
  assert(dbm != nullptr);
  ParamDBM* xdbm = reinterpret_cast<ParamDBM*>(dbm);
  int64_t file_size = 0;
  last_status = xdbm->GetFileSize(&file_size);
  if (last_status != Status::SUCCESS) {
    return -1;
  }
  return file_size;
}

char* tkrzw_dbm_get_file_path(TkrzwDBM* dbm) {
  assert(dbm != nullptr);
  ParamDBM* xdbm = reinterpret_cast<ParamDBM*>(dbm);
  std::string path;
  last_status = xdbm->GetFilePath(&path);
  if (last_status != Status::SUCCESS) {
    return nullptr;
  }
  char* path_ptr = reinterpret_cast<char*>(xmalloc(path.size() + 1));
  std::memcpy(path_ptr, path.c_str(), path.size() + 1);
  return path_ptr;
}

bool tkrzw_dbm_clear(TkrzwDBM* dbm) {
  assert(dbm != nullptr);
  ParamDBM* xdbm = reinterpret_cast<ParamDBM*>(dbm);
  last_status = xdbm->Clear();
  return last_status == Status::SUCCESS;
}

bool tkrzw_dbm_rebuild(TkrzwDBM* dbm, const char* params) {
  assert(dbm != nullptr && params != nullptr);
  std::map<std::string, std::string> xparams = StrSplitIntoMap(params, ",", "=");
  ParamDBM* xdbm = reinterpret_cast<ParamDBM*>(dbm);
  last_status = xdbm->RebuildAdvanced(xparams);
  return last_status == Status::SUCCESS;
}

bool tkrzw_dbm_should_be_rebuilt(TkrzwDBM* dbm) {
  assert(dbm != nullptr);
  ParamDBM* xdbm = reinterpret_cast<ParamDBM*>(dbm);
  bool tobe = false;
  last_status = xdbm->ShouldBeRebuilt(&tobe);
  return last_status == Status::SUCCESS && tobe;
}

bool tkrzw_dbm_synchronize(
    TkrzwDBM* dbm, bool hard, tkrzw_file_processor proc, void* proc_arg, const char* params) {
  assert(dbm != nullptr && params != nullptr);
  std::map<std::string, std::string> xparams = StrSplitIntoMap(params, ",", "=");
  ParamDBM* xdbm = reinterpret_cast<ParamDBM*>(dbm);
  class Proc : public DBM::FileProcessor {
   public:
    Proc(tkrzw_file_processor proc, void* arg) : proc_(proc), arg_(arg) {}
    void Process(const std::string& path) override {
      if (proc_ != nullptr) {
        proc_(arg_, path.c_str());
      }
    }
   private:
    tkrzw_file_processor proc_;
    void* arg_;
  };
  Proc xproc(proc, proc_arg);
  last_status = xdbm->SynchronizeAdvanced(hard, &xproc, xparams);
  return last_status == Status::SUCCESS;
}

bool tkrzw_dbm_copy_file_data(TkrzwDBM* dbm, const char* dest_path) {
  assert(dbm != nullptr && dest_path != nullptr);
  ParamDBM* xdbm = reinterpret_cast<ParamDBM*>(dbm);
  last_status = xdbm->CopyFileData(dest_path);
  return last_status == Status::SUCCESS;
}

bool tkrzw_dbm_export(TkrzwDBM* dbm, TkrzwDBM* dest_dbm) {
  assert(dbm != nullptr && dest_dbm != nullptr);
  ParamDBM* xdbm = reinterpret_cast<ParamDBM*>(dbm);
  ParamDBM* dest_xdbm = reinterpret_cast<ParamDBM*>(dest_dbm);
  last_status = xdbm->Export(dest_xdbm);
  return last_status == Status::SUCCESS;
}


bool tkrzw_dbm_export_to_flat_records(TkrzwDBM* dbm, TkrzwFile* file) {
  assert(dbm != nullptr && file != nullptr);
  ParamDBM* xdbm = reinterpret_cast<ParamDBM*>(dbm);
  PolyFile* xfile = reinterpret_cast<PolyFile*>(file);
  last_status = ExportDBMRecordsToFlatRecords(xdbm, xfile);
  return last_status == Status::SUCCESS;
}

bool tkrzw_dbm_import_from_flat_records(TkrzwDBM* dbm, TkrzwFile* file) {
  assert(dbm != nullptr && file != nullptr);
  ParamDBM* xdbm = reinterpret_cast<ParamDBM*>(dbm);
  PolyFile* xfile = reinterpret_cast<PolyFile*>(file);
  last_status = ImportDBMRecordsFromFlatRecords(xdbm, xfile);
  return last_status == Status::SUCCESS;
}

bool tkrzw_dbm_export_keys_as_lines(TkrzwDBM* dbm, TkrzwFile* file) {
  assert(dbm != nullptr && file != nullptr);
  ParamDBM* xdbm = reinterpret_cast<ParamDBM*>(dbm);
  PolyFile* xfile = reinterpret_cast<PolyFile*>(file);
  last_status = ExportDBMKeysAsLines(xdbm, xfile);
  return last_status == Status::SUCCESS;
}

TkrzwKeyValuePair* tkrzw_dbm_inspect(TkrzwDBM* dbm, int32_t* num_records) {
  assert(dbm != nullptr && num_records != nullptr);
  ParamDBM* xdbm = reinterpret_cast<ParamDBM*>(dbm);
  const auto& records = xdbm->Inspect();
  TkrzwKeyValuePair* array = static_cast<TkrzwKeyValuePair*>(xmalloc(
      sizeof(TkrzwKeyValuePair) * records.size() + 1));
  int32_t num_recs = 0;
  for (const auto& record : records) {
    auto& elem = array[num_recs];
    char* key_ptr = static_cast<char*>(xmalloc(record.first.size() + record.second.size() + 2));
    std::memcpy(key_ptr, record.first.c_str(), record.first.size() + 1);
    char* value_ptr = key_ptr + record.first.size() + 1;
    std::memcpy(value_ptr, record.second.c_str(), record.second.size() + 1);
    elem.key_ptr = key_ptr;
    elem.key_size = record.first.size();
    elem.value_ptr = value_ptr;
    elem.value_size = record.second.size();
    num_recs++;
  }
  *num_records = records.size();
  return array;
}

bool tkrzw_dbm_is_writable(TkrzwDBM* dbm) {
  assert(dbm != nullptr);
  ParamDBM* xdbm = reinterpret_cast<ParamDBM*>(dbm);
  return xdbm->IsWritable();
}

bool tkrzw_dbm_is_healthy(TkrzwDBM* dbm) {
  assert(dbm != nullptr);
  ParamDBM* xdbm = reinterpret_cast<ParamDBM*>(dbm);
  return xdbm->IsHealthy();
}

bool tkrzw_dbm_is_ordered(TkrzwDBM* dbm) {
  assert(dbm != nullptr);
  ParamDBM* xdbm = reinterpret_cast<ParamDBM*>(dbm);
  return xdbm->IsOrdered();
}

TkrzwStr* tkrzw_dbm_search(
    TkrzwDBM* dbm, const char* mode, const char* pattern_ptr, int32_t pattern_size,
    int32_t capacity, int32_t* num_matched) {
  assert(dbm != nullptr && mode != nullptr && pattern_ptr != nullptr && num_matched != nullptr);
  if (pattern_size < 0) {
    pattern_size = std::strlen(pattern_ptr);
  }
  capacity = std::max(0, capacity);
  ParamDBM* xdbm = reinterpret_cast<ParamDBM*>(dbm);
  std::vector<std::string> keys;
  last_status = SearchDBMModal(xdbm, mode, std::string_view(pattern_ptr, pattern_size),
                               &keys, capacity);
  if (last_status != Status::SUCCESS) {
    return nullptr;
  }
  TkrzwStr* array = static_cast<TkrzwStr*>(xmalloc(sizeof(TkrzwStr) * keys.size() + 1));
  for (size_t i = 0; i < keys.size(); i++) {
    const auto& key = keys[i];
    auto& elem = array[i];
    char*ptr = static_cast<char*>(xmalloc(key.size() + 1));
    std::memcpy(ptr, key.c_str(), key.size() + 1);
    elem.ptr = ptr;
    elem.size = key.size();
  }
  *num_matched = keys.size();
  return array;
}

TkrzwDBMIter* tkrzw_dbm_make_iterator(TkrzwDBM* dbm) {
  assert(dbm != nullptr);
  ParamDBM* xdbm = reinterpret_cast<ParamDBM*>(dbm);
  return reinterpret_cast<TkrzwDBMIter*>(xdbm->MakeIterator().release());
}

void tkrzw_dbm_iter_free(TkrzwDBMIter* iter) {
  assert(iter != nullptr);
  DBM::Iterator* xiter = reinterpret_cast<DBM::Iterator*>(iter);
  delete xiter;
}

bool tkrzw_dbm_iter_first(TkrzwDBMIter* iter) {
  assert(iter != nullptr);
  DBM::Iterator* xiter = reinterpret_cast<DBM::Iterator*>(iter);
  last_status = xiter->First();
  return last_status == Status::SUCCESS;
}

bool tkrzw_dbm_iter_last(TkrzwDBMIter* iter) {
  assert(iter != nullptr);
  DBM::Iterator* xiter = reinterpret_cast<DBM::Iterator*>(iter);
  last_status = xiter->Last();
  return last_status == Status::SUCCESS;
}

bool tkrzw_dbm_iter_jump(TkrzwDBMIter* iter, const char* key_ptr, int32_t key_size) {
  assert(iter != nullptr && key_ptr != nullptr);
  DBM::Iterator* xiter = reinterpret_cast<DBM::Iterator*>(iter);
  if (key_size < 0) {
    key_size = std::strlen(key_ptr);
  }
  last_status = xiter->Jump(std::string_view(key_ptr, key_size));
  return last_status == Status::SUCCESS;
}

bool tkrzw_dbm_iter_jump_lower(TkrzwDBMIter* iter, const char* key_ptr, int32_t key_size,
                               bool inclusive) {
  assert(iter != nullptr && key_ptr != nullptr);
  DBM::Iterator* xiter = reinterpret_cast<DBM::Iterator*>(iter);
  if (key_size < 0) {
    key_size = std::strlen(key_ptr);
  }
  last_status = xiter->JumpLower(std::string_view(key_ptr, key_size), inclusive);
  return last_status == Status::SUCCESS;
}

bool tkrzw_dbm_iter_jump_upper(TkrzwDBMIter* iter, const char* key_ptr, int32_t key_size,
                               bool inclusive) {
  assert(iter != nullptr && key_ptr != nullptr);
  DBM::Iterator* xiter = reinterpret_cast<DBM::Iterator*>(iter);
  if (key_size < 0) {
    key_size = std::strlen(key_ptr);
  }
  last_status = xiter->JumpUpper(std::string_view(key_ptr, key_size), inclusive);
  return last_status == Status::SUCCESS;
}

bool tkrzw_dbm_iter_next(TkrzwDBMIter* iter) {
  assert(iter != nullptr);
  DBM::Iterator* xiter = reinterpret_cast<DBM::Iterator*>(iter);
  last_status = xiter->Next();
  return last_status == Status::SUCCESS;
}

bool tkrzw_dbm_iter_previous(TkrzwDBMIter* iter) {
  assert(iter != nullptr);
  DBM::Iterator* xiter = reinterpret_cast<DBM::Iterator*>(iter);
  last_status = xiter->Previous();
  return last_status == Status::SUCCESS;
}

bool tkrzw_dbm_iter_process(
    TkrzwDBMIter* iter, tkrzw_record_processor proc, void* proc_arg, bool writable) {
  assert(iter != nullptr && proc != nullptr);
  DBM::Iterator* xiter = reinterpret_cast<DBM::Iterator*>(iter);
  RecordProcessorWrapper xproc;
  xproc.proc = proc;
  xproc.arg = proc_arg;
  last_status = xiter->Process(&xproc, writable);
  return last_status == Status::SUCCESS;
}

bool tkrzw_dbm_iter_get(
    TkrzwDBMIter* iter, char** key_ptr, int32_t* key_size,
    char** value_ptr, int32_t* value_size) {
  assert(iter != nullptr);
  DBM::Iterator* xiter = reinterpret_cast<DBM::Iterator*>(iter);
  bool rv = false;
  if (key_ptr == nullptr && value_ptr == nullptr) {
    last_status = xiter->Get();
    rv = last_status == Status::SUCCESS;
  } else if (value_ptr == nullptr) {
    std::string key;
    last_status = xiter->Get(&key);
    if (last_status == Status::SUCCESS) {
      *key_ptr = static_cast<char*>(xmalloc(key.size() + 1));
      std::memcpy(*key_ptr, key.c_str(), key.size() + 1);
      if (key_size != nullptr) {
        *key_size = key.size();
      }
      rv = true;
    }
  } else if (key_ptr == nullptr) {
    std::string value;
    last_status = xiter->Get(nullptr, &value);
    if (last_status == Status::SUCCESS) {
      *value_ptr = static_cast<char*>(xmalloc(value.size() + 1));
      std::memcpy(*value_ptr, value.c_str(), value.size() + 1);
      if (value_size != nullptr) {
        *value_size = value.size();
      }
      rv = true;
    }
  } else {
    std::string key, value;
    last_status = xiter->Get(&key, &value);
    if (last_status == Status::SUCCESS) {
      *key_ptr = static_cast<char*>(xmalloc(key.size() + 1));
      std::memcpy(*key_ptr, key.c_str(), key.size() + 1);
      if (key_size != nullptr) {
        *key_size = key.size();
      }
      *value_ptr = static_cast<char*>(xmalloc(value.size() + 1));
      std::memcpy(*value_ptr, value.c_str(), value.size() + 1);
      if (value_size != nullptr) {
        *value_size = value.size();
      }
      rv = true;
    }
  }
  return rv;
}

char* tkrzw_dbm_iter_get_key(TkrzwDBMIter* iter, int32_t* key_size) {
  assert(iter != nullptr);
  DBM::Iterator* xiter = reinterpret_cast<DBM::Iterator*>(iter);
  std::string key;
  last_status = xiter->Get(&key);
  if (last_status != Status::SUCCESS) {
    return nullptr;
  }
  char* key_ptr = static_cast<char*>(xmalloc(key.size() + 1));
  std::memcpy(key_ptr, key.c_str(), key.size() + 1);
  if (key_size != nullptr) {
    *key_size = key.size();
  }
  return key_ptr;
}

char* tkrzw_dbm_iter_get_value(TkrzwDBMIter* iter, int32_t* value_size) {
  assert(iter != nullptr);
  DBM::Iterator* xiter = reinterpret_cast<DBM::Iterator*>(iter);
  std::string value;
  last_status = xiter->Get(nullptr, &value);
  if (last_status != Status::SUCCESS) {
    return nullptr;
  }
  char* value_ptr = static_cast<char*>(xmalloc(value.size() + 1));
  std::memcpy(value_ptr, value.c_str(), value.size() + 1);
  if (value_size != nullptr) {
    *value_size = value.size();
  }
  return value_ptr;
}

bool tkrzw_dbm_iter_set(TkrzwDBMIter* iter, const char* value_ptr, int32_t value_size) {
  assert(iter != nullptr && value_ptr != nullptr);
  if (value_size < 0) {
    value_size = std::strlen(value_ptr);
  }
  DBM::Iterator* xiter = reinterpret_cast<DBM::Iterator*>(iter);
  last_status = xiter->Set(std::string_view(value_ptr, value_size));
  return last_status == Status::SUCCESS;
}

bool tkrzw_dbm_iter_remove(TkrzwDBMIter* iter) {
  assert(iter != nullptr);
  DBM::Iterator* xiter = reinterpret_cast<DBM::Iterator*>(iter);
  last_status = xiter->Remove();
  return last_status == Status::SUCCESS;
}

bool tkrzw_dbm_restore_database(
    const char* old_file_path, const char* new_file_path,
    const char* class_name, int64_t end_offset) {
  assert(old_file_path != nullptr && new_file_path != nullptr);
  if (class_name == nullptr) {
    class_name = "";
  }
  int32_t num_shards = 0;
  if (ShardDBM::GetNumberOfShards(old_file_path, &num_shards) == Status::SUCCESS) {
    last_status = ShardDBM::RestoreDatabase(
        old_file_path, new_file_path, class_name, end_offset);
  } else {
    last_status = PolyDBM::RestoreDatabase(
        old_file_path, new_file_path, class_name, end_offset);
  }
  return last_status == Status::SUCCESS;
}

TkrzwFile* tkrzw_file_open(const char* path, bool writable, const char* params) {
  assert(path != nullptr && params != nullptr);
  std::map<std::string, std::string> xparams = StrSplitIntoMap(params, ",", "=");
  int32_t open_options = 0;
  if (tkrzw::StrToBool(tkrzw::SearchMap(xparams, "truncate", "false"))) {
    open_options |= tkrzw::File::OPEN_TRUNCATE;
  }
  if (tkrzw::StrToBool(tkrzw::SearchMap(xparams, "no_create", "false"))) {
    open_options |= tkrzw::File::OPEN_NO_CREATE;
  }
  if (tkrzw::StrToBool(tkrzw::SearchMap(xparams, "no_wait", "false"))) {
    open_options |= tkrzw::File::OPEN_NO_WAIT;
  }
  if (tkrzw::StrToBool(tkrzw::SearchMap(xparams, "no_lock", "false"))) {
    open_options |= tkrzw::File::OPEN_NO_LOCK;
  }
  PolyFile* file = new tkrzw::PolyFile();
  last_status = file->OpenAdvanced(path, writable, open_options, xparams);
  if (last_status != Status::SUCCESS) {
    delete file;
    return nullptr;
  }
  return reinterpret_cast<TkrzwFile*>(file);
}

bool tkrzw_file_close(TkrzwFile* file) {
  assert(file != nullptr);
  PolyFile* xfile = reinterpret_cast<PolyFile*>(file);
  last_status = xfile->Close();
  bool rv = last_status == Status::SUCCESS;
  delete xfile;
  return rv;
}

bool tkrzw_file_read(TkrzwFile* file, int64_t off, void* buf, size_t size) {
  assert(file != nullptr && off >= 0 && buf != nullptr);
  PolyFile* xfile = reinterpret_cast<PolyFile*>(file);
  last_status = xfile->Read(off, buf, size);
  return last_status == Status::SUCCESS;
}

bool tkrzw_file_write(TkrzwFile* file, int64_t off, const void* buf, size_t size) {
  assert(file != nullptr && off >= 0 && buf != nullptr);
  PolyFile* xfile = reinterpret_cast<PolyFile*>(file);
  last_status = xfile->Write(off, buf, size);
  return last_status == Status::SUCCESS;
}

bool tkrzw_file_append(TkrzwFile* file, const void* buf, size_t size, int64_t* off) {
  assert(file != nullptr && buf != nullptr);
  PolyFile* xfile = reinterpret_cast<PolyFile*>(file);
  last_status = xfile->Append(buf, size, off);
  return last_status == Status::SUCCESS;
}

bool tkrzw_file_truncate(TkrzwFile* file, int64_t size) {
  assert(file != nullptr);
  PolyFile* xfile = reinterpret_cast<PolyFile*>(file);
  last_status = xfile->Truncate(size);
  return last_status == Status::SUCCESS;
}

bool tkrzw_file_synchronize(TkrzwFile* file, bool hard, int64_t off, int64_t size) {
  assert(file != nullptr);
  PolyFile* xfile = reinterpret_cast<PolyFile*>(file);
  last_status = xfile->Synchronize(hard, off, size);
  return last_status == Status::SUCCESS;
}

int64_t tkrzw_file_get_size(TkrzwFile* file) {
  assert(file != nullptr);
  PolyFile* xfile = reinterpret_cast<PolyFile*>(file);
  int64_t size = 0;
  last_status = xfile->GetSize(&size);
  return last_status == Status::SUCCESS ? size : -1;
}

}  // extern "C"

// END OF FILE
