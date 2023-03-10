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
#include "tkrzw_dbm_hash.h"
#include "tkrzw_dbm_poly.h"
#include "tkrzw_dbm_skip.h"
#include "tkrzw_dbm_std.h"
#include "tkrzw_dbm_tiny.h"
#include "tkrzw_dbm_tree.h"
#include "tkrzw_dbm_ulog.h"
#include "tkrzw_file.h"
#include "tkrzw_file_mmap.h"
#include "tkrzw_file_poly.h"
#include "tkrzw_file_pos.h"
#include "tkrzw_file_std.h"
#include "tkrzw_file_util.h"
#include "tkrzw_lib_common.h"
#include "tkrzw_str_util.h"

namespace tkrzw {

class NoopIterator final : public DBM::Iterator {
 public:
  virtual ~NoopIterator() = default;
  Status First() override {
    return Status(Status::PRECONDITION_ERROR);
  }
  Status Last() override {
    return Status(Status::PRECONDITION_ERROR);
  }
  Status Jump(std::string_view key) override {
    return Status(Status::PRECONDITION_ERROR);
  }
  Status JumpLower(std::string_view key, bool inclusive = false) override {
    return Status(Status::PRECONDITION_ERROR);
  }
  Status JumpUpper(std::string_view key, bool inclusive = false) override {
    return Status(Status::PRECONDITION_ERROR);
  }
  Status Next() override {
    return Status(Status::PRECONDITION_ERROR);
  }
  Status Previous() override {
    return Status(Status::PRECONDITION_ERROR);
  }
  Status Process(DBM::RecordProcessor* proc, bool writable) override {
    return Status(Status::PRECONDITION_ERROR);
  }
};

static std::string GuessClassNameFromPath(const std::string& path) {
  const std::string base = StrLowerCase(PathToBaseName(path));
  std::string ext = PathToExtension(base);
  const size_t sym_pos = ext.find("-");
  if (sym_pos != std::string::npos) {
    ext = ext.substr(0, sym_pos);
  }
  if (ext == "tkh" || ext == "hash") {
    return "hash";
  } else if (ext == "tkt" || ext == "tree") {
    return "tree";
  } else if (ext == "tks" || ext == "skip") {
    return "skip";
  } else if (ext == "tkmt" || ext == "tiny" || ext == "flat") {
    return "tiny";
  } else if (ext == "tkmb" || ext == "baby") {
    return "baby";
  } else if (ext == "tkmc" || ext == "cache") {
    return "cache";
  } else if (ext == "tksh" || ext == "stdhash") {
    return "stdhash";
  } else if (ext == "tkst" || ext == "stdtree") {
    return "stdtree";
  }
  return "";
}

static KeyComparator GetKeyComparatorByName(const std::string& comp_name) {
  const std::string lower_name = StrLowerCase(comp_name);
  if (lower_name == "lexicalkeycomparator" || lower_name == "lexical") {
    return LexicalKeyComparator;
  }
  if (lower_name == "lexicalCasekeycomparator" || lower_name == "lexicalcase") {
    return LexicalCaseKeyComparator;
  }
  if (lower_name == "decimalkeycomparator" || lower_name == "decimal") {
    return DecimalKeyComparator;
  }
  if (lower_name == "hexadecimalkeycomparator" || lower_name == "hexadecimal") {
    return HexadecimalKeyComparator;
  }
  if (lower_name == "realnumberkeycomparator" || lower_name == "realnumber") {
    return RealNumberKeyComparator;
  }
  if (lower_name == "pairlexicalkeycomparator" || lower_name == "pairlexical") {
    return PairLexicalKeyComparator;
  }
  if (lower_name == "pairlexicalCasekeycomparator" || lower_name == "pairlexicalcase") {
    return PairLexicalCaseKeyComparator;
  }
  if (lower_name == "pairdecimalkeycomparator" || lower_name == "pairdecimal") {
    return PairDecimalKeyComparator;
  }
  if (lower_name == "pairhexadecimalkeycomparator" || lower_name == "pairhexadecimal") {
    return PairHexadecimalKeyComparator;
  }
  if (lower_name == "pairrealnumberkeycomparator" || lower_name == "pairrealnumber") {
    return PairRealNumberKeyComparator;
  }
  return nullptr;
}

static SkipDBM::ReducerType GetReducerByName(const std::string& func_name) {
  const std::string lower_name = StrLowerCase(func_name);
  if (lower_name == "reduceremove" || func_name == "remove") {
    return SkipDBM::ReduceRemove;
  } else if (lower_name == "reducetofirst" || lower_name == "first") {
    return SkipDBM::ReduceToFirst;
  } else if (lower_name == "reducetosecond" || lower_name == "second") {
    return SkipDBM::ReduceToSecond;
  } else if (lower_name == "reducetolast" || lower_name == "last") {
    return SkipDBM::ReduceToLast;
  } else if (lower_name == "reduceconcat" || lower_name == "concat") {
    return SkipDBM::ReduceConcat;
  } else if (lower_name == "reduceconcatwithnull" || lower_name == "concatnull") {
    return SkipDBM::ReduceConcatWithNull;
  } else if (lower_name == "reduceconcatwithtab" || lower_name == "concattab") {
    return SkipDBM::ReduceConcatWithTab;
  } else if (lower_name == "reduceconcatwithline" || lower_name == "concatline") {
    return SkipDBM::ReduceConcatWithLine;
  } else if (lower_name == "reducetototal" || lower_name == "total") {
    return SkipDBM::ReduceToTotal;
  } else if (lower_name == "reducetototalbigendian" || lower_name == "totalbigendian" ||
             lower_name == "totalbe") {
    return SkipDBM::ReduceToTotalBigEndian;
  }
  return nullptr;
}

static void SetHashTuningParams(
    std::map<std::string, std::string>* params, HashDBM::TuningParameters* tuning_params) {
  const std::string update_mode = StrLowerCase(SearchMap(*params, "update_mode", ""));
  if (update_mode == "update_in_place" || update_mode == "in_place") {
    tuning_params->update_mode = HashDBM::UPDATE_IN_PLACE;
  }
  if (update_mode == "update_appending" || update_mode == "appending") {
    tuning_params->update_mode = HashDBM::UPDATE_APPENDING;
  }
  const std::string record_crc_mode = StrLowerCase(SearchMap(*params, "record_crc_mode", ""));
  if (record_crc_mode == "record_crc_none" || record_crc_mode == "crc_none" ||
      record_crc_mode == "none") {
    tuning_params->record_crc_mode = HashDBM::RECORD_CRC_NONE;
  }
  if (record_crc_mode == "record_crc_8" || record_crc_mode == "crc_8" ||
      record_crc_mode == "crc-8" || record_crc_mode == "8") {
    tuning_params->record_crc_mode = HashDBM::RECORD_CRC_8;
  }
  if (record_crc_mode == "record_crc_16" || record_crc_mode == "crc_16" ||
      record_crc_mode == "crc-16" || record_crc_mode == "16") {
    tuning_params->record_crc_mode = HashDBM::RECORD_CRC_16;
  }
  if (record_crc_mode == "record_crc_32" || record_crc_mode == "crc_32" ||
      record_crc_mode == "crc-32" || record_crc_mode == "32") {
    tuning_params->record_crc_mode = HashDBM::RECORD_CRC_32;
  }
  const std::string record_comp_mode = StrLowerCase(SearchMap(*params, "record_comp_mode", ""));
  if (record_comp_mode == "record_comp_none" || record_comp_mode == "comp_none" ||
      record_comp_mode == "none") {
    tuning_params->record_comp_mode = HashDBM::RECORD_COMP_NONE;
  }
  if (record_comp_mode == "record_comp_zlib" || record_comp_mode == "comp_zlib" ||
      record_comp_mode == "zlib" || record_comp_mode == "deflate") {
    tuning_params->record_comp_mode = HashDBM::RECORD_COMP_ZLIB;
  }
  if (record_comp_mode == "record_comp_zstd" || record_comp_mode == "comp_zstd" ||
      record_comp_mode == "zstd") {
    tuning_params->record_comp_mode = HashDBM::RECORD_COMP_ZSTD;
  }
  if (record_comp_mode == "record_comp_lz4" || record_comp_mode == "comp_lz4" ||
      record_comp_mode == "lz4") {
    tuning_params->record_comp_mode = HashDBM::RECORD_COMP_LZ4;
  }
  if (record_comp_mode == "record_comp_lzma" || record_comp_mode == "comp_lzma" ||
      record_comp_mode == "lzma" || record_comp_mode == "xz") {
    tuning_params->record_comp_mode = HashDBM::RECORD_COMP_LZMA;
  }
  if (record_comp_mode == "record_comp_rc4" || record_comp_mode == "comp_rc4" ||
      record_comp_mode == "rc4" || record_comp_mode == "arcfour") {
    tuning_params->record_comp_mode = HashDBM::RECORD_COMP_RC4;
  }
  if (record_comp_mode == "record_comp_aes" || record_comp_mode == "comp_aes" ||
      record_comp_mode == "aes" || record_comp_mode == "rijndael") {
    tuning_params->record_comp_mode = HashDBM::RECORD_COMP_AES;
  }
  tuning_params->offset_width = StrToIntMetric(SearchMap(*params, "offset_width", "-1"));
  tuning_params->align_pow = StrToIntMetric(SearchMap(*params, "align_pow", "-1"));
  tuning_params->num_buckets = StrToIntMetric(SearchMap(*params, "num_buckets", "-1"));
  bool restore_no_shortcut = false;
  bool restore_with_hardsync = false;
  for (const auto& expr : StrSplit(SearchMap(*params, "restore_mode", ""), ':')) {
    const std::string norm_expr = StrLowerCase(StrStripSpace(expr));
    if (norm_expr == "restore_sync" || norm_expr == "sync") {
      tuning_params->restore_mode = HashDBM::RESTORE_SYNC;
    }
    if (norm_expr == "restore_read_only" || norm_expr == "read_only") {
      tuning_params->restore_mode = HashDBM::RESTORE_READ_ONLY;
    }
    if (norm_expr == "restore_noop" || norm_expr == "noop") {
      tuning_params->restore_mode = HashDBM::RESTORE_NOOP;
    }
    if (norm_expr == "restore_no_shortcuts" || norm_expr == "no_shortcuts") {
      restore_no_shortcut = true;
    }
    if (norm_expr == "restore_with_hardsync" || norm_expr == "with_hardsync") {
      restore_with_hardsync = true;
    }
  }
  if (restore_no_shortcut) {
    tuning_params->restore_mode |= HashDBM::RESTORE_NO_SHORTCUTS;
  }
  if (restore_with_hardsync) {
    tuning_params->restore_mode |= HashDBM::RESTORE_WITH_HARDSYNC;
  }
  tuning_params->fbp_capacity = StrToIntMetric(SearchMap(*params, "fbp_capacity", "-1"));
  tuning_params->min_read_size = StrToIntMetric(SearchMap(*params, "min_read_size", "-1"));
  tuning_params->cache_buckets = StrToBool(SearchMap(*params, "cache_buckets", "false"));
  tuning_params->cipher_key = SearchMap(*params, "cipher_key", "");
  params->erase("update_mode");
  params->erase("record_crc_mode");
  params->erase("record_comp_mode");
  params->erase("offset_width");
  params->erase("align_pow");
  params->erase("num_buckets");
  params->erase("restore_mode");
  params->erase("fbp_capacity");
  params->erase("min_read_size");
  params->erase("cache_buckets");
  params->erase("cipher_key");
}

static void SetTreeTuningParams(
    std::map<std::string, std::string>* params, TreeDBM::TuningParameters* tuning_params) {
  SetHashTuningParams(params, tuning_params);
  tuning_params->max_page_size = StrToIntMetric(SearchMap(*params, "max_page_size", "-1"));
  tuning_params->max_branches = StrToIntMetric(SearchMap(*params, "max_branches", "-1"));
  tuning_params->max_cached_pages = StrToIntMetric(SearchMap(*params, "max_cached_pages", "-1"));
  const std::string page_update_mode = StrLowerCase(SearchMap(*params, "page_update_mode", ""));
  if (page_update_mode == "page_update_none" || page_update_mode == "none") {
    tuning_params->page_update_mode = TreeDBM::PAGE_UPDATE_NONE;
  }
  if (page_update_mode == "page_update_write" || page_update_mode == "write") {
    tuning_params->page_update_mode = TreeDBM::PAGE_UPDATE_WRITE;
  }
  tuning_params->key_comparator =
      GetKeyComparatorByName(SearchMap(*params, "key_comparator", ""));
  params->erase("max_page_size");
  params->erase("max_branches");
  params->erase("max_cached_pages");
  params->erase("page_update_mode");
  params->erase("key_comparator");
}

static void SetSkipTuningParams(
    std::map<std::string, std::string>* params, SkipDBM::TuningParameters* tuning_params) {
  tuning_params->offset_width = StrToIntMetric(SearchMap(*params, "offset_width", "-1"));
  tuning_params->step_unit = StrToIntMetric(SearchMap(*params, "step_unit", "-1"));
  tuning_params->max_level = StrToIntMetric(SearchMap(*params, "max_level", "-1"));
  bool restore_no_shortcut = false;
  bool restore_with_hardsync = false;
  for (const auto& expr : StrSplit(SearchMap(*params, "restore_mode", ""), ':')) {
    const std::string norm_expr = StrLowerCase(StrStripSpace(expr));
    if (norm_expr == "restore_sync" || norm_expr == "sync") {
      tuning_params->restore_mode = SkipDBM::RESTORE_SYNC;
    }
    if (norm_expr == "restore_read_only" || norm_expr == "read_only") {
      tuning_params->restore_mode = SkipDBM::RESTORE_READ_ONLY;
    }
    if (norm_expr == "restore_noop" || norm_expr == "noop") {
      tuning_params->restore_mode = SkipDBM::RESTORE_NOOP;
    }
    if (norm_expr == "restore_no_shortcuts" || norm_expr == "no_shortcuts") {
      restore_no_shortcut = true;
    }
    if (norm_expr == "restore_with_hardsync" || norm_expr == "with_hardsync") {
      restore_with_hardsync = true;
    }
  }
  if (restore_no_shortcut) {
    tuning_params->restore_mode |= SkipDBM::RESTORE_NO_SHORTCUTS;
  }
  if (restore_with_hardsync) {
    tuning_params->restore_mode |= SkipDBM::RESTORE_WITH_HARDSYNC;
  }
  tuning_params->sort_mem_size = StrToIntMetric(SearchMap(*params, "sort_mem_size", "-1"));
  tuning_params->insert_in_order = StrToBool(SearchMap(*params, "insert_in_order", "false"));
  tuning_params->max_cached_records =
      StrToIntMetric(SearchMap(*params, "max_cached_records", "-1"));
  params->erase("offset_width");
  params->erase("step_unit");
  params->erase("max_level");
  params->erase("restore_mode");
  params->erase("sort_mem_size");
  params->erase("insert_in_order");
  params->erase("max_cached_records");
}

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

PolyDBM::PolyDBM() : dbm_(nullptr), ulog_mq_(nullptr), ulog_(nullptr), open_(false) {}

PolyDBM::~PolyDBM() {
  if (open_) {
    Close();
  }
}

Status PolyDBM::OpenAdvanced(
    const std::string& path, bool writable, int32_t options,
    const std::map<std::string, std::string>& params) {
  if (dbm_ != nullptr) {
    return Status(Status::PRECONDITION_ERROR, "opened database");
  }
  std::map<std::string, std::string> mod_params, ulog_params;
  for (const auto& param : params) {
    if (StrBeginsWith(param.first, "ulog_")) {
      ulog_params.emplace(param);
    } else {
      mod_params.emplace(param);
    }
  }
  std::string class_name = StrLowerCase(SearchMap(mod_params, "dbm", ""));
  if (class_name.empty()) {
    class_name = GuessClassNameFromPath(path);
  }
  if (class_name.empty() && path.empty()) {
    class_name = "tiny";
  }
  mod_params.erase("dbm");
  if (class_name == "hash" || class_name == "hashdbm") {
    auto file = PolyFile::MakeFileInstance(&mod_params);
    if (file == nullptr) {
      return Status(Status::INVALID_ARGUMENT_ERROR, "unknown File class");
    }
    HashDBM::TuningParameters tuning_params;
    SetHashTuningParams(&mod_params, &tuning_params);
    if (!mod_params.empty()) {
      return Status(Status::INVALID_ARGUMENT_ERROR,
                    StrCat("unsupported parameter: ", mod_params.begin()->first));
    }
    auto hash_dbm = std::make_unique<HashDBM>(std::move(file));
    const Status status = hash_dbm->OpenAdvanced(path, writable, options, tuning_params);
    if (status != Status::SUCCESS) {
      return status;
    }
    open_ = true;
    dbm_ = std::move(hash_dbm);
  } else if (class_name == "tree" || class_name == "treedbm") {
    auto file = PolyFile::MakeFileInstance(&mod_params);
    if (file == nullptr) {
      return Status(Status::INVALID_ARGUMENT_ERROR, "unknown File class");
    }
    TreeDBM::TuningParameters tuning_params;
    SetTreeTuningParams(&mod_params, &tuning_params);
    if (!SearchMap(mod_params, "key_comparator", "").empty() &&
        tuning_params.key_comparator == nullptr) {
      return Status(Status::INVALID_ARGUMENT_ERROR, "unsupported key comparator");
    }
    if (!mod_params.empty()) {
      return Status(Status::INVALID_ARGUMENT_ERROR,
                    StrCat("unsupported parameter: ", mod_params.begin()->first));
    }
    auto tree_dbm = std::make_unique<TreeDBM>(std::move(file));
    const Status status = tree_dbm->OpenAdvanced(path, writable, options, tuning_params);
    if (status != Status::SUCCESS) {
      return status;
    }
    open_ = true;
    dbm_ = std::move(tree_dbm);
  } else if (class_name == "skip" || class_name == "skipdbm") {
    auto file = PolyFile::MakeFileInstance(&mod_params);
    if (file == nullptr) {
      return Status(Status::INVALID_ARGUMENT_ERROR, "unknown File class");
    }
    SkipDBM::TuningParameters tuning_params;
    SetSkipTuningParams(&mod_params, &tuning_params);
    if (!mod_params.empty()) {
      return Status(Status::INVALID_ARGUMENT_ERROR,
                    StrCat("unsupported parameter: ", mod_params.begin()->first));
    }
    auto skip_dbm = std::make_unique<SkipDBM>(std::move(file));
    const Status status = skip_dbm->OpenAdvanced(path, writable, options, tuning_params);
    if (status != Status::SUCCESS) {
      return status;
    }
    open_ = true;
    dbm_ = std::move(skip_dbm);
  } else if (class_name == "tiny" || class_name == "tinydbm") {
    auto file = PolyFile::MakeFileInstance(&mod_params);
    if (file == nullptr) {
      return Status(Status::INVALID_ARGUMENT_ERROR, "unknown File class");
    }
    const int64_t num_buckets = StrToIntMetric(SearchMap(mod_params, "num_buckets", "-1"));
    mod_params.erase("num_buckets");
    if (!mod_params.empty()) {
      return Status(Status::INVALID_ARGUMENT_ERROR,
                    StrCat("unsupported parameter: ", mod_params.begin()->first));
    }
    auto tiny_dbm = std::make_unique<TinyDBM>(std::move(file), num_buckets);
    if (!path.empty()) {
      const Status status = tiny_dbm->Open(path, writable, options);
      if (status != Status::SUCCESS) {
        return status;
      }
      open_ = true;
    }
    dbm_ = std::move(tiny_dbm);
  } else if (class_name == "baby" || class_name == "babydbm") {
    auto file = PolyFile::MakeFileInstance(&mod_params);
    if (file == nullptr) {
      return Status(Status::INVALID_ARGUMENT_ERROR, "unknown File class");
    }
    KeyComparator key_comparator = LexicalKeyComparator;
    const std::string comp_name = SearchMap(mod_params, "key_comparator", "");
    if (!comp_name.empty()) {
      key_comparator = GetKeyComparatorByName(comp_name);
      if (key_comparator == nullptr) {
        return Status(Status::INVALID_ARGUMENT_ERROR,
                      StrCat("unsupported key comparator: ", comp_name));
      }
    }
    mod_params.erase("key_comparator");
    if (!mod_params.empty()) {
      return Status(Status::INVALID_ARGUMENT_ERROR,
                    StrCat("unsupported parameter: ", mod_params.begin()->first));
    }
    auto baby_dbm = std::make_unique<BabyDBM>(std::move(file), key_comparator);
    if (!path.empty()) {
      const Status status = baby_dbm->Open(path, writable, options);
      if (status != Status::SUCCESS) {
        return status;
      }
      open_ = true;
    }
    dbm_ = std::move(baby_dbm);
  } else if (class_name == "cache" || class_name == "cachedbm") {
    auto file = PolyFile::MakeFileInstance(&mod_params);
    if (file == nullptr) {
      return Status(Status::INVALID_ARGUMENT_ERROR, "unknown File class");
    }
    const int64_t cap_rec_num = StrToIntMetric(SearchMap(mod_params, "cap_rec_num", "-1"));
    const int64_t cap_mem_size = StrToIntMetric(SearchMap(mod_params, "cap_mem_size", "-1"));
    mod_params.erase("cap_rec_num");
    mod_params.erase("cap_mem_size");
    if (!mod_params.empty()) {
      return Status(Status::INVALID_ARGUMENT_ERROR,
                    StrCat("unsupported parameter: ", mod_params.begin()->first));
    }
    auto cache_dbm = std::make_unique<CacheDBM>(std::move(file), cap_rec_num, cap_mem_size);
    if (!path.empty()) {
      const Status status = cache_dbm->Open(path, writable, options);
      if (status != Status::SUCCESS) {
        return status;
      }
      open_ = true;
    }
    dbm_ = std::move(cache_dbm);
  } else if (class_name == "stdhash" || class_name == "stdhashdbm") {
    auto file = PolyFile::MakeFileInstance(&mod_params);
    if (file == nullptr) {
      return Status(Status::INVALID_ARGUMENT_ERROR, "unknown File class");
    }
    const int64_t num_buckets = StrToIntMetric(SearchMap(mod_params, "num_buckets", "-1"));
    mod_params.erase("num_buckets");
    if (!mod_params.empty()) {
      return Status(Status::INVALID_ARGUMENT_ERROR,
                    StrCat("unsupported parameter: ", mod_params.begin()->first));
    }
    auto stdhash_dbm = std::make_unique<StdHashDBM>(std::move(file), num_buckets);
    if (!path.empty()) {
      const Status status = stdhash_dbm->Open(path, writable, options);
      if (status != Status::SUCCESS) {
        return status;
      }
      open_ = true;
    }
    dbm_ = std::move(stdhash_dbm);
  } else if (class_name == "stdtree" || class_name == "stdtreedbm") {
    auto file = PolyFile::MakeFileInstance(&mod_params);
    if (file == nullptr) {
      return Status(Status::INVALID_ARGUMENT_ERROR, "unknown File class");
    }
    if (!mod_params.empty()) {
      return Status(Status::INVALID_ARGUMENT_ERROR,
                    StrCat("unsupported parameter: ", mod_params.begin()->first));
    }
    auto stdtree_dbm = std::make_unique<StdTreeDBM>(std::move(file));
    if (!path.empty()) {
      const Status status = stdtree_dbm->Open(path, writable, options);
      if (status != Status::SUCCESS) {
        return status;
      }
      open_ = true;
    }
    dbm_ = std::move(stdtree_dbm);
  } else {
    return Status(Status::INVALID_ARGUMENT_ERROR, "unknown DBM class");
  }
  Status status = MakeOwnedUpdateLogger(&ulog_params, &ulog_mq_, &ulog_);
  if (!ulog_params.empty()) {
    status |= Status(Status::INVALID_ARGUMENT_ERROR,
                     StrCat("unsupported parameter: ", ulog_params.begin()->first));
  }
  if (status != Status::SUCCESS) {
    ulog_.reset(nullptr);
    ulog_mq_.reset(nullptr);
    dbm_.reset(nullptr);
    return status;
  }
  if (ulog_ != nullptr) {
    dbm_->SetUpdateLogger(ulog_.get());
  }
  return Status(Status::SUCCESS);
}

Status PolyDBM::Close() {
  if (dbm_ == nullptr) {
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
  if (open_) {
    status |= dbm_->Close();
  }
  open_ = false;
  dbm_.reset(nullptr);
  return status;
}

Status PolyDBM::Process(std::string_view key, RecordProcessor* proc, bool writable) {
  if (dbm_ == nullptr) {
    return Status(Status::PRECONDITION_ERROR, "not opened database");
  }
  return dbm_->Process(key, proc, writable);
}

Status PolyDBM::Get(std::string_view key, std::string* value) {
  if (dbm_ == nullptr) {
    return Status(Status::PRECONDITION_ERROR, "not opened database");
  }
  return dbm_->Get(key, value);
}

Status PolyDBM::Set(std::string_view key, std::string_view value, bool overwrite,
                    std::string* old_value) {
  if (dbm_ == nullptr) {
    return Status(Status::PRECONDITION_ERROR, "not opened database");
  }
  return dbm_->Set(key, value, overwrite, old_value);
}

Status PolyDBM::Remove(std::string_view key, std::string* old_value) {
  if (dbm_ == nullptr) {
    return Status(Status::PRECONDITION_ERROR, "not opened database");
  }
  return dbm_->Remove(key, old_value);
}

Status PolyDBM::Append(std::string_view key, std::string_view value, std::string_view delim) {
  if (dbm_ == nullptr) {
    return Status(Status::PRECONDITION_ERROR, "not opened database");
  }
  return dbm_->Append(key, value, delim);
}

Status PolyDBM::ProcessFirst(RecordProcessor* proc, bool writable) {
  if (dbm_ == nullptr) {
    return Status(Status::PRECONDITION_ERROR, "not opened database");
  }
  return dbm_->ProcessFirst(proc, writable);
}

Status PolyDBM::ProcessMulti(
    const std::vector<std::pair<std::string_view, RecordProcessor*>>& key_proc_pairs,
    bool writable) {
  return dbm_->ProcessMulti(key_proc_pairs, writable);
}

Status PolyDBM::ProcessEach(RecordProcessor* proc, bool writable) {
  if (dbm_ == nullptr) {
    return Status(Status::PRECONDITION_ERROR, "not opened database");
  }
  return dbm_->ProcessEach(proc, writable);
}

Status PolyDBM::Count(int64_t* count) {
  if (dbm_ == nullptr) {
    return Status(Status::PRECONDITION_ERROR, "not opened database");
  }
  return dbm_->Count(count);
}

Status PolyDBM::GetFileSize(int64_t* size) {
  if (dbm_ == nullptr) {
    return Status(Status::PRECONDITION_ERROR, "not opened database");
  }
  return dbm_->GetFileSize(size);
}

Status PolyDBM::GetFilePath(std::string* path) {
  if (dbm_ == nullptr) {
    return Status(Status::PRECONDITION_ERROR, "not opened database");
  }
  return dbm_->GetFilePath(path);
}

Status PolyDBM::GetTimestamp(double* timestamp) {
  if (dbm_ == nullptr) {
    return Status(Status::PRECONDITION_ERROR, "not opened database");
  }
  return dbm_->GetTimestamp(timestamp);
}

Status PolyDBM::Clear() {
  if (dbm_ == nullptr) {
    return Status(Status::PRECONDITION_ERROR, "not opened database");
  }
  return dbm_->Clear();
}

Status PolyDBM::RebuildAdvanced(const std::map<std::string, std::string>& params) {
  if (dbm_ == nullptr) {
    return Status(Status::PRECONDITION_ERROR, "not opened database");
  }
  std::map<std::string, std::string> mod_params = params;
  const auto& dbm_type = dbm_->GetType();
  if (dbm_type == typeid(HashDBM)) {
    HashDBM* hash_dbm = dynamic_cast<HashDBM*>(dbm_.get());
    HashDBM::TuningParameters tuning_params;
    SetHashTuningParams(&mod_params, &tuning_params);
    const bool skip_broken_records = StrToBool(SearchMap(params, "skip_broken_records", "false"));
    const bool sync_hard = StrToBool(SearchMap(params, "sync_hard", "false"));
    mod_params.erase("skip_broken_records");
    mod_params.erase("sync_hard");
    if (!mod_params.empty()) {
      return Status(Status::INVALID_ARGUMENT_ERROR,
                    StrCat("unsupported parameter: ", mod_params.begin()->first));
    }
    return hash_dbm->RebuildAdvanced(tuning_params, skip_broken_records, sync_hard);
  }
  if (dbm_type == typeid(TreeDBM)) {
    TreeDBM* tree_dbm = dynamic_cast<TreeDBM*>(dbm_.get());
    TreeDBM::TuningParameters tuning_params;
    SetTreeTuningParams(&mod_params, &tuning_params);
    const bool skip_broken_records = StrToBool(SearchMap(params, "skip_broken_records", "false"));
    const bool sync_hard = StrToBool(SearchMap(params, "sync_hard", "false"));
    mod_params.erase("skip_broken_records");
    mod_params.erase("sync_hard");
    if (!mod_params.empty()) {
      return Status(Status::INVALID_ARGUMENT_ERROR,
                    StrCat("unsupported parameter: ", mod_params.begin()->first));
    }
    return tree_dbm->RebuildAdvanced(tuning_params, skip_broken_records, sync_hard);
  }
  if (dbm_type == typeid(SkipDBM)) {
    SkipDBM* skip_dbm = dynamic_cast<SkipDBM*>(dbm_.get());
    SkipDBM::TuningParameters tuning_params;
    SetSkipTuningParams(&mod_params, &tuning_params);
    const bool skip_broken_records = StrToBool(SearchMap(params, "skip_broken_records", "false"));
    const bool sync_hard = StrToBool(SearchMap(params, "sync_hard", "false"));
    mod_params.erase("skip_broken_records");
    mod_params.erase("sync_hard");
    if (!mod_params.empty()) {
      return Status(Status::INVALID_ARGUMENT_ERROR,
                    StrCat("unsupported parameter: ", mod_params.begin()->first));
    }
    return skip_dbm->RebuildAdvanced(tuning_params, skip_broken_records, sync_hard);
  }
  if (dbm_type == typeid(TinyDBM)) {
    TinyDBM* tiny_dbm = dynamic_cast<TinyDBM*>(dbm_.get());
    const int64_t num_buckets = StrToIntMetric(SearchMap(mod_params, "num_buckets", "-1"));
    mod_params.erase("num_buckets");
    if (!mod_params.empty()) {
      return Status(Status::INVALID_ARGUMENT_ERROR,
                    StrCat("unsupported parameter: ", mod_params.begin()->first));
    }
    return tiny_dbm->RebuildAdvanced(num_buckets);
  }
  if (dbm_type == typeid(CacheDBM)) {
    CacheDBM* cache_dbm = dynamic_cast<CacheDBM*>(dbm_.get());
    const int64_t cap_rec_num = StrToIntMetric(SearchMap(mod_params, "cap_rec_num", "-1"));
    const int64_t cap_mem_size = StrToIntMetric(SearchMap(mod_params, "cap_mem_size", "-1"));
    mod_params.erase("cap_rec_num");
    mod_params.erase("cap_mem_size");
    if (!mod_params.empty()) {
      return Status(Status::INVALID_ARGUMENT_ERROR,
                    StrCat("unsupported parameter: ", mod_params.begin()->first));
    }
    return cache_dbm->RebuildAdvanced(cap_rec_num, cap_mem_size);
  }
  if (!mod_params.empty()) {
    return Status(Status::INVALID_ARGUMENT_ERROR,
                  StrCat("unsupported parameter: ", mod_params.begin()->first));
  }
  return dbm_->Rebuild();
}

Status PolyDBM::ShouldBeRebuilt(bool* tobe) {
  if (dbm_ == nullptr) {
    return Status(Status::PRECONDITION_ERROR, "not opened database");
  }
  return dbm_->ShouldBeRebuilt(tobe);
}

Status PolyDBM::SynchronizeAdvanced(
    bool hard, FileProcessor* proc, const std::map<std::string, std::string>& params) {
  if (dbm_ == nullptr) {
    return Status(Status::PRECONDITION_ERROR, "not opened database");
  }
  std::map<std::string, std::string> mod_params = params;
  const auto& dbm_type = dbm_->GetType();
  if (dbm_type == typeid(SkipDBM)) {
    SkipDBM* skip_dbm = dynamic_cast<SkipDBM*>(dbm_.get());
    const auto& merge_paths = StrSplit(SearchMap(mod_params, "merge", ""), ':', true);
    for (const auto& merge_path : merge_paths) {
      const Status status = skip_dbm->MergeSkipDatabase(merge_path);
      if (status != Status::SUCCESS) {
        return status;
      }
    }
    const std::string reducer_name = SearchMap(mod_params, "reducer", "");
    SkipDBM::ReducerType reducer = nullptr;
    if (!reducer_name.empty()) {
      reducer = GetReducerByName(reducer_name);
      if (reducer == nullptr) {
        return Status(Status::INVALID_ARGUMENT_ERROR,
                      StrCat("unsupported reducer: ", reducer_name));
      }
    }
    mod_params.erase("merge");
    mod_params.erase("reducer");
    if (!mod_params.empty()) {
      return Status(Status::INVALID_ARGUMENT_ERROR,
                    StrCat("unsupported parameter: ", mod_params.begin()->first));
    }
    return skip_dbm->SynchronizeAdvanced(hard, proc, reducer);
  } else {
    if (!mod_params.empty()) {
      return Status(Status::INVALID_ARGUMENT_ERROR,
                    StrCat("unsupported parameter: ", mod_params.begin()->first));
    }
  }
  return dbm_->Synchronize(hard, proc);
}

std::vector<std::pair<std::string, std::string>> PolyDBM::Inspect() {
  if (dbm_ == nullptr) {
    return std::vector<std::pair<std::string, std::string>>();
  }
  return dbm_->Inspect();
}

bool PolyDBM::IsOpen() const {
  if (dbm_ == nullptr) {
    return false;
  }
  return dbm_->IsOpen();
}

bool PolyDBM::IsWritable() const {
  if (dbm_ == nullptr) {
    return false;
  }
  return dbm_->IsWritable();
}

bool PolyDBM::IsHealthy() const {
  if (dbm_ == nullptr) {
    return false;
  }
  return dbm_->IsHealthy();
}

bool PolyDBM::IsOrdered() const {
  if (dbm_ == nullptr) {
    return false;
  }
  return dbm_->IsOrdered();
}

std::unique_ptr<DBM::Iterator> PolyDBM::MakeIterator() {
  if (dbm_ == nullptr) {
    return std::make_unique<NoopIterator>();
  }
  std::unique_ptr<PolyDBM::Iterator> iter(new PolyDBM::Iterator(dbm_->MakeIterator()));
  return iter;
}

std::unique_ptr<DBM> PolyDBM::MakeDBM() const {
  return std::make_unique<PolyDBM>();
}

DBM::UpdateLogger* PolyDBM::GetUpdateLogger() const {
  if (dbm_ == nullptr) {
    return nullptr;
  }
  return dbm_->GetUpdateLogger();
}

void PolyDBM::SetUpdateLogger(UpdateLogger* update_logger) {
  if (dbm_ == nullptr) {
    return;
  }
  dbm_->SetUpdateLogger(update_logger);
}

DBM* PolyDBM::GetInternalDBM() const {
  return dbm_.get();
}

Status PolyDBM::RestoreDatabase(
    const std::string& old_file_path, const std::string& new_file_path,
    const std::string& class_name, int64_t end_offset, std::string_view cipher_key) {
  std::string mod_class_name = StrLowerCase(class_name);
  if (mod_class_name.empty()) {
    mod_class_name = GuessClassNameFromPath(old_file_path);
  }
  if (mod_class_name == "hash" || mod_class_name == "hashdbm") {
    return HashDBM::RestoreDatabase(old_file_path, new_file_path, end_offset, cipher_key);
  } else if (mod_class_name == "tree" || mod_class_name == "treedbm") {
    return TreeDBM::RestoreDatabase(old_file_path, new_file_path, end_offset, cipher_key);
  } else if (mod_class_name == "skip" || mod_class_name == "skipdbm") {
    return SkipDBM::RestoreDatabase(old_file_path, new_file_path);
  }
  return Status(Status::INFEASIBLE_ERROR, "unknown database class");
}

}  // namespace tkrzw

// END OF FILE
