/*************************************************************************************************
 * Performance checker of file implementations
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

#include "tkrzw_cmd_util.h"

namespace tkrzw {

// Prints the usage to the standard error and dies.
static void PrintUsageAndDie() {
  auto P = EPrintF;
  const char* progname = "tkrzw_dbm_perf";
  P("%s: Performance checker of DBM implementations of Tkrzw\n", progname);
  P("\n");
  P("Usage:\n");
  P("  %s sequence [options]\n", progname);
  P("    : Checks setting/getting/removing performance in sequence.\n");
  P("  %s parallel [options]\n", progname);
  P("    : Checks setting/getting/removing performance in parallel.\n");
  P("  %s wicked [options]\n", progname);
  P("    : Checks consistency with various operations.\n");
  P("  %s index [options]\n", progname);
  P("    : Checks performance of on-memory indexing.\n");
  P("\n");
  P("\n");
  P("Common options:\n");
  P("  --dbm impl : The name of a DBM implementation:"
    " auto, hash, tree, skip, tiny, baby, cache, stdhash, stdtree, poly, shard."
    " (default: auto)\n");
  P("  --iter num : The number of iterations. (default: 10000)\n");
  P("  --size num : The size of each record value. (default: 8)\n");
  P("  --threads num : The number of threads. (default: 1)\n");
  P("  --verbose : Prints verbose reports.\n");
  P("  --path path : The path of the file to write or read.\n");
  P("  --file impl : The name of a file implementation:"
    " std, mmap-para, mmap-atom, pos-para, pos-atom. (default: mmap-para)\n");
  P("  --no_wait : Fails if the file is locked by another process.\n");
  P("  --no_lock : Omits file locking.\n");
  P("  --alloc_init num : The initial allocation size. (default: %lld)\n",
    File::DEFAULT_ALLOC_INIT_SIZE);
  P("  --alloc_inc num : The allocation increment factor. (default: %.1f)\n",
    File::DEFAULT_ALLOC_INC_FACTOR);
  P("\n");
  P("Options for the sequence subcommand:\n");
  P("  --random_key : Uses random keys rather than sequential ones.\n");
  P("  --random_value : Uses random length values rather than fixed ones.\n");
  P("  --set_only : Does only setting.\n");
  P("  --get_only : Does only getting.\n");
  P("  --remove_only : Does only removing.\n");
  P("\n");
  P("Options for the parallel subcommand:\n");
  P("  --random_key : Uses random keys rather than sequential ones.\n");
  P("  --random_value : Uses random length values rather than fixed ones.\n");
  P("  --keys : The number of unique keys.\n");
  P("  --rebuild : Rebuilds the database occasionally.\n");
  P("  --sleep num : The duration to sleep between iterations. (default: 0)\n");
  P("\n");
  P("Options for the wicked subcommand:\n");
  P("  --iterator : Uses iterators occasionally.\n");
  P("  --clear : Clears the database occasionally.\n");
  P("  --rebuild : Rebuilds the database occasionally.\n");
  P("\n");
  P("Options for the index subcommand:\n");
  P("  --type expr : The types of the key and value of the index:"
    " file, mem, n2n, n2s, s2n, s2s, str, file. (default: file)\n");
  P("  --random_key : Uses random keys rather than sequential ones.\n");
  P("  --random_value : Uses random length values rather than fixed ones.\n");
  P("\n");
  P("Options for HashDBM:\n");
  P("  --append : Uses the appending mode rather than the in-place mode.\n");
  P("  --offset_width num : The width to represent the offset of records. (default: %d)\n",
    HashDBM::DEFAULT_OFFSET_WIDTH);
  P("  --align_pow num : Sets the power to align records. (default: %d)\n",
    HashDBM::DEFAULT_ALIGN_POW);
  P("  --buckets num : Sets the number of buckets for hashing. (default: %lld)\n",
    HashDBM::DEFAULT_NUM_BUCKETS);
  P("  --fbp_cap num : Sets the capacity of the free block pool. (default: %d)\n",
    HashDBM::DEFAULT_FBP_CAPACITY);
  P("  --lock_mem_buckets : Locks the memory for the hash buckets.\n");
  P("\n");
  P("Options for TreeDBM and FileIndex:\n");
  P("  --append : Uses the appending mode rather than the in-place mode.\n");
  P("  --offset_width num : The width to represent the offset of records. (default: %d)\n",
    TreeDBM::DEFAULT_OFFSET_WIDTH);
  P("  --align_pow num : Sets the power to align records. (default: %d)\n",
    TreeDBM::DEFAULT_ALIGN_POW);
  P("  --buckets num : Sets the number of buckets for hashing. (default: %lld)\n",
    TreeDBM::DEFAULT_NUM_BUCKETS);
  P("  --fbp_cap num : Sets the capacity of the free block pool. (default: %d)\n",
    TreeDBM::DEFAULT_FBP_CAPACITY);
  P("  --lock_mem_buckets : Locks the memory for the hash buckets.\n");
  P("  --max_page_size num : Sets the maximum size of a page. (default: %d)\n",
    TreeDBM::DEFAULT_MAX_PAGE_SIZE);
  P("  --max_branches num : Sets the maximum number of branches of inner nodes. (default: %d)\n",
    TreeDBM::DEFAULT_MAX_BRANCHES);
  P("  --max_chached_pages num : Sets the maximum number of cached pages. (default: %d)\n",
    TreeDBM::DEFAULT_MAX_CACHED_PAGES);
  P("\n");
  P("Options for SkipDBM:\n");
  P("  --offset_width num : The width to represent the offset of records. (default: %d)\n",
    SkipDBM::DEFAULT_OFFSET_WIDTH);
  P("  --step_unit num : Sets the step unit of the skip list. (default: %d)\n",
    SkipDBM::DEFAULT_STEP_UNIT);
  P("  --max_level num : Sets the maximum level of the skip list. (default: %d)\n",
    SkipDBM::DEFAULT_MAX_LEVEL);
  P("  --sort_mem_size num : Sets the memory size used for sorting. (default: %lld)\n",
    SkipDBM::DEFAULT_SORT_MEM_SIZE);
  P("  --insert_in_order : Inserts records in ascending order order of the key.\n");
  P("  --max_cached_records num : Sets the number of cached records (default: %d)\n",
    SkipDBM::DEFAULT_MAX_CACHED_RECORDS);
  P("  --reducer func : Sets the reducer: none, first, second, last, concat, total."
    " (default: none)\n");
  P("\n");
  P("Options for TinyDBM and StdHashDBM:\n");
  P("  --buckets num : Sets the number of buckets for hashing. (default: %lld)\n",
    TinyDBM::DEFAULT_NUM_BUCKETS);
  P("\n");
  P("Options for CacheDBM:\n");
  P("  --cap_rec_num num : Sets the maximum number of records. (default: %lld)\n",
    CacheDBM::DEFAULT_CAP_REC_NUM);
  P("  --cap_memsize num : Sets the total memory size to use.\n");
  P("\n");
  P("Options for PolyDBM and ShardDBM:\n");
  P("  --params str : Sets the parameters in \"key=value,key=value\" format.\n");
  P("\n");
  std::exit(1);
}

// Makes a DBM object or die.
std::unique_ptr<DBM> MakeDBMOrDie(
    const std::string& dbm_impl, const std::string& file_impl,
    const std::string& file_path, int32_t alloc_init_size, double alloc_increment,
    int64_t num_buckets, int64_t cap_rec_num, int64_t cap_mem_size) {
  std::string dbm_impl_mod = dbm_impl;
  if (dbm_impl == "auto") {
    const std::string ext = StrLowerCase(PathToExtension(file_path));
    if (ext == "tkh") {
      dbm_impl_mod = "hash";
    } else if (ext == "tkt") {
      dbm_impl_mod = "tree";
    } else if (ext == "tks") {
      dbm_impl_mod = "skip";
    } else if (ext == "tkmt" || ext == "flat") {
      dbm_impl_mod = "tiny";
    } else if (ext == "tkmb") {
      dbm_impl_mod = "baby";
    } else if (ext == "tkmc") {
      dbm_impl_mod = "cache";
    } else if (ext == "tksh") {
      dbm_impl_mod = "stdhash";
    } else if (ext == "tkst") {
      dbm_impl_mod = "stdtree";
    } else if (!ext.empty()) {
      dbm_impl_mod = ext;
    }
  }
  std::unique_ptr<DBM> dbm;
  if (dbm_impl_mod == "hash") {
    if (file_path.empty()) {
      Die("The file path must be specified");
    }
    dbm = std::make_unique<HashDBM>(MakeFileOrDie(file_impl, alloc_init_size, alloc_increment));
  } else if (dbm_impl_mod == "tree") {
    if (file_path.empty()) {
      Die("The file path must be specified");
    }
    dbm = std::make_unique<TreeDBM>(MakeFileOrDie(file_impl, alloc_init_size, alloc_increment));
  } else if (dbm_impl_mod == "skip") {
    if (file_path.empty()) {
      Die("The file path must be specified");
    }
    dbm = std::make_unique<SkipDBM>(MakeFileOrDie(file_impl, alloc_init_size, alloc_increment));
  } else if (dbm_impl_mod == "tiny") {
    dbm = std::make_unique<TinyDBM>(
        MakeFileOrDie(file_impl, alloc_init_size, alloc_increment), num_buckets);
  } else if (dbm_impl_mod == "baby") {
    dbm = std::make_unique<BabyDBM>(
        MakeFileOrDie(file_impl, alloc_init_size, alloc_increment));
  } else if (dbm_impl_mod == "cache") {
    dbm = std::make_unique<CacheDBM>(
        MakeFileOrDie(file_impl, alloc_init_size, alloc_increment), cap_rec_num, cap_mem_size);
  } else if (dbm_impl_mod == "stdhash") {
    dbm = std::make_unique<StdHashDBM>(
        MakeFileOrDie(file_impl, alloc_init_size, alloc_increment), num_buckets);
  } else if (dbm_impl_mod == "stdtree") {
    dbm = std::make_unique<StdTreeDBM>(
        MakeFileOrDie(file_impl, alloc_init_size, alloc_increment));
  } else if (dbm_impl_mod == "poly") {
    dbm = std::make_unique<PolyDBM>();
  } else if (dbm_impl_mod == "shard") {
    dbm = std::make_unique<ShardDBM>();
  } else {
    Die("Unknown DBM implementation: ", dbm_impl);
  }
  return dbm;
}

// Sets up a DBM object.
bool SetUpDBM(DBM* dbm, bool writable, bool initialize, const std::string& file_path,
              bool with_no_wait, bool with_no_lock,
              bool is_append, int32_t offset_width, int32_t align_pow, int64_t num_buckets,
              int32_t fbp_cap, bool lock_mem_buckets,
              int32_t max_page_size, int32_t max_branches, int32_t max_cached_pages,
              int32_t step_unit, int32_t max_level, int64_t sort_mem_size,
              bool insert_in_order, int32_t max_cached_records,
              const std::string& poly_params) {
  bool has_error = false;
  int32_t open_options = File::OPEN_DEFAULT;
  if (initialize) {
    open_options |= File::OPEN_TRUNCATE;
  }
  if (with_no_wait) {
    open_options |= File::OPEN_NO_WAIT;
  }
  if (with_no_lock) {
    open_options |= File::OPEN_NO_LOCK;
  }
  if (typeid(*dbm) == typeid(HashDBM)) {
    HashDBM* hash_dbm = dynamic_cast<HashDBM*>(dbm);
    tkrzw::HashDBM::TuningParameters tuning_params;
    tuning_params.update_mode =
        is_append ? tkrzw::HashDBM::UPDATE_APPENDING : tkrzw::HashDBM::UPDATE_IN_PLACE;
    tuning_params.offset_width = offset_width;
    tuning_params.align_pow = align_pow;
    tuning_params.num_buckets = num_buckets;
    tuning_params.fbp_capacity = fbp_cap;
    tuning_params.lock_mem_buckets = lock_mem_buckets;
    const Status status =
        hash_dbm->OpenAdvanced(file_path, writable, open_options, tuning_params);
    if (status != Status::SUCCESS) {
      EPrintL("OpenAdvanced failed: ", status);
      has_error = true;
    }
  }
  if (typeid(*dbm) == typeid(TreeDBM)) {
    TreeDBM* tree_dbm = dynamic_cast<TreeDBM*>(dbm);
    tkrzw::TreeDBM::TuningParameters tuning_params;
    tuning_params.update_mode =
        is_append ? tkrzw::HashDBM::UPDATE_APPENDING : tkrzw::HashDBM::UPDATE_IN_PLACE;
    tuning_params.offset_width = offset_width;
    tuning_params.align_pow = align_pow;
    tuning_params.num_buckets = num_buckets;
    tuning_params.fbp_capacity = fbp_cap;
    tuning_params.lock_mem_buckets = lock_mem_buckets;
    tuning_params.max_page_size = max_page_size;
    tuning_params.max_branches = max_branches;
    tuning_params.max_cached_pages = max_cached_pages;
    const Status status =
        tree_dbm->OpenAdvanced(file_path, writable, open_options, tuning_params);
    if (status != Status::SUCCESS) {
      EPrintL("OpenAdvanced failed: ", status);
      has_error = true;
    }
  }
  if (typeid(*dbm) == typeid(SkipDBM)) {
    SkipDBM* skip_dbm = dynamic_cast<SkipDBM*>(dbm);
    tkrzw::SkipDBM::TuningParameters tuning_params;
    tuning_params.offset_width = offset_width;
    tuning_params.step_unit = step_unit;
    tuning_params.max_level = max_level;
    tuning_params.sort_mem_size = sort_mem_size;
    tuning_params.insert_in_order = insert_in_order;
    tuning_params.max_cached_records = max_cached_records;
    const Status status =
        skip_dbm->OpenAdvanced(file_path, writable, open_options, tuning_params);
    if (status != Status::SUCCESS) {
      EPrintL("OpenAdvanced failed: ", status);
      has_error = true;
    }
  }
  if (typeid(*dbm) == typeid(TinyDBM) || typeid(*dbm) == typeid(BabyDBM) ||
      typeid(*dbm) == typeid(CacheDBM) ||
      typeid(*dbm) == typeid(StdHashDBM) || typeid(*dbm) == typeid(StdTreeDBM)) {
    if (!file_path.empty()) {
      const Status status = dbm->Open(file_path, writable, open_options);
      if (status != Status::SUCCESS) {
        EPrintL("Open failed: ", status);
        has_error = true;
      }
    }
  }
  if (typeid(*dbm) == typeid(PolyDBM) || typeid(*dbm) == typeid(ShardDBM)) {
    ParamDBM* param_dbm = dynamic_cast<ParamDBM*>(dbm);
    const std::map<std::string, std::string> tuning_params =
        tkrzw::StrSplitIntoMap(poly_params, ",", "=");
    const Status status =
        param_dbm->OpenAdvanced(file_path, writable, open_options, tuning_params);
    if (status != Status::SUCCESS) {
      EPrintL("OpenAdvanced failed: ", status);
      has_error = true;
    }
  }
  return !has_error;
}

// Gets a reducer or die.
SkipDBM::ReducerType GetReducerOrDier(const std::string& reducer_name) {
  SkipDBM::ReducerType reducer = nullptr;
  if (reducer_name == "none") {
    reducer = nullptr;
  } else if (reducer_name == "first") {
    reducer = SkipDBM::ReduceToFirst;
  } else if (reducer_name == "second") {
    reducer = SkipDBM::ReduceToSecond;
  } else if (reducer_name == "last") {
    reducer = SkipDBM::ReduceToLast;
  } else if (reducer_name == "concat") {
    reducer = SkipDBM::ReduceConcat;
  } else if (reducer_name == "total") {
    reducer = SkipDBM::ReduceToTotal;
  } else {
    Die("Unknown ReducerType implementation: ", reducer_name);
  }
  return reducer;
}

// Synchronizes a DBM object.
bool SynchronizeDBM(DBM* dbm, const std::string& reducer_name) {
  bool has_error = false;
  if (typeid(*dbm) == typeid(SkipDBM)) {
    SkipDBM* skip_dbm = dynamic_cast<SkipDBM*>(dbm);
    const Status status = skip_dbm->SynchronizeAdvanced(
        false, nullptr, GetReducerOrDier(reducer_name));
    if (status != Status::SUCCESS) {
      EPrintL("SynchronizeAdvanced failed: ", status);
      has_error = true;
    }
  } else if (typeid(*dbm) == typeid(PolyDBM) || typeid(*dbm) == typeid(ShardDBM)) {
    ParamDBM* param_dbm = dynamic_cast<ParamDBM*>(dbm);
    std::map<std::string, std::string> params;
    if (!reducer_name.empty() && reducer_name != "none") {
      params.emplace("reducer", reducer_name);
    }
    const Status status = param_dbm->SynchronizeAdvanced(
        false, nullptr, params);
    if (status != Status::SUCCESS) {
      EPrintL("SynchronizeAdvanced failed: ", status);
      has_error = true;
    }
  } else {
    const Status status = dbm->Synchronize(false);
    if (status != Status::SUCCESS) {
      EPrintL("Synchronize failed: ", status);
      has_error = true;
    }
  }
  return !has_error;
}

// Tears down a DBM object.
bool TearDownDBM(DBM* dbm, const std::string& file_path, bool is_verbose) {
  bool has_error = false;
  if (typeid(*dbm) == typeid(HashDBM)) {
    HashDBM* hash_dbm = dynamic_cast<HashDBM*>(dbm);
    const int64_t file_size = hash_dbm->GetFileSizeSimple();
    const int64_t eff_data_size = hash_dbm->GetEffectiveDataSize();
    PrintF("  file_size=%lld eff_data_size=%lld efficiency=%.2f%%\n",
           file_size, eff_data_size, eff_data_size * 100.0 / file_size);
    const int64_t num_buckets = hash_dbm->CountBuckets();
    const double load_factor = hash_dbm->CountSimple() * 1.0 / num_buckets;
    PrintF("  num_buckets=%lld load_factor=%.2f\n", num_buckets, load_factor);
    if (is_verbose) {
      const int64_t num_used_buckets = hash_dbm->CountUsedBuckets();
      const double used_bucket_ratio = num_used_buckets * 1.0 / num_buckets;
      PrintF("  num_used_buckets=%lld used_bucket_ratio=%.2f%%\n",
             num_used_buckets, used_bucket_ratio * 100);
    }
    const Status status = hash_dbm->Close();
    if (status != Status::SUCCESS) {
      EPrintL("Close failed: ", status);
      has_error = true;
    }
  }
  if (typeid(*dbm) == typeid(TreeDBM)) {
    TreeDBM* tree_dbm = dynamic_cast<TreeDBM*>(dbm);
    const int64_t file_size = tree_dbm->GetFileSizeSimple();
    const int64_t eff_data_size = tree_dbm->GetEffectiveDataSize();
    PrintF("  file_size=%lld eff_data_size=%lld efficiency=%.2f%%\n",
           file_size, eff_data_size, eff_data_size * 100.0 / file_size);
    if (is_verbose) {
      const auto& meta = dbm->Inspect();
      const std::map<std::string, std::string> meta_map(meta.begin(), meta.end());
      const int32_t level = StrToInt(SearchMap(meta_map, "tree_level", "0"));
      const int64_t num_pages = StrToInt(SearchMap(meta_map, "hash_num_records", "0"));
      const double load_factor =
          num_pages * 1.0 / StrToInt(SearchMap(meta_map, "hash_num_buckets", "0"));
      PrintF("  level=%d num_pages=%lld load_factor=%.2f\n", level, num_pages, load_factor);
    }
    const Status status = tree_dbm->Close();
    if (status != Status::SUCCESS) {
      EPrintL("Close failed: ", status);
      has_error = true;
    }
  }
  if (typeid(*dbm) == typeid(SkipDBM)) {
    SkipDBM* skip_dbm = dynamic_cast<SkipDBM*>(dbm);
    const int64_t file_size = skip_dbm->GetFileSizeSimple();
    const int64_t eff_data_size = skip_dbm->GetEffectiveDataSize();
    PrintF("  file_size=%lld eff_data_size=%lld efficiency=%.2f%%\n",
           file_size, eff_data_size, eff_data_size * 100.0 / file_size);
    const Status status = skip_dbm->Close();
    if (status != Status::SUCCESS) {
      EPrintL("Close failed: ", status);
      has_error = true;
    }
  }
  if (typeid(*dbm) == typeid(TinyDBM) || typeid(*dbm) == typeid(StdHashDBM)) {
    const auto& meta = dbm->Inspect();
    const std::map<std::string, std::string> meta_map(meta.begin(), meta.end());
    const int64_t num_buckets = StrToInt(SearchMap(meta_map, "num_buckets", "0"));
    const double load_factor = dbm->CountSimple() * 1.0 / num_buckets;
    PrintF("  num_buckets=%lld load_factor=%.2f\n", num_buckets, load_factor);
    if (!file_path.empty()) {
      const Status status = dbm->Close();
      if (status != Status::SUCCESS) {
        EPrintL("Close failed: ", status);
        has_error = true;
      }
    }
  }
  if (typeid(*dbm) == typeid(CacheDBM)) {
    CacheDBM* cache_dbm = dynamic_cast<CacheDBM*>(dbm);
    const int64_t eff_data_size = cache_dbm->GetEffectiveDataSize();
    const int64_t mem_usage = cache_dbm->GetMemoryUsage();
    PrintF("  eff_data_size=%lld mem_usage=%lld\n", eff_data_size, mem_usage);
    if (!file_path.empty()) {
      const Status status = dbm->Close();
      if (status != Status::SUCCESS) {
        EPrintL("Close failed: ", status);
        has_error = true;
      }
    }
  }
  if (typeid(*dbm) == typeid(BabyDBM) || typeid(*dbm) == typeid(StdTreeDBM)) {
    if (!file_path.empty()) {
      const Status status = dbm->Close();
      if (status != Status::SUCCESS) {
        EPrintL("Close failed: ", status);
        has_error = true;
      }
    }
  }
  if (typeid(*dbm) == typeid(PolyDBM) || typeid(*dbm) == typeid(ShardDBM)) {
    PrintF("  file_size=%lld\n", dbm->GetFileSizeSimple());
    const Status status = dbm->Close();
    if (status != Status::SUCCESS) {
      EPrintL("Close failed: ", status);
      has_error = true;
    }
  }
  return !has_error;
}

// Processes the sequence subcommand.
static int32_t ProcessSequence(int32_t argc, const char** args) {
  const std::map<std::string, int32_t>& cmd_configs = {
    {"", 0}, {"--dbm", 1}, {"--iter", 1}, {"--size", 1}, {"--threads", 1}, {"--verbose", 0},
    {"--random_key", 0}, {"--random_value", 0},
    {"--set_only", 0}, {"--get_only", 0}, {"--remove_only", 0},
    {"--path", 1}, {"--file", 1}, {"--no_wait", 0}, {"--no_lock", 0},
    {"--alloc_init", 1}, {"--alloc_inc", 1},
    {"--append", 0}, {"--offset_width", 1}, {"--align_pow", 1}, {"--buckets", 1},
    {"--fbp_cap", 1}, {"--lock_mem_buckets", 0},
    {"--max_page_size", 1}, {"--max_branches", 1}, {"--max_cached_pages", 1},
    {"--step_unit", 1}, {"--max_level", 1}, {"--sort_mem_size", 1}, {"--insert_in_order", 0},
    {"--max_cached_records", 1}, {"--reducer", 1},
    {"--cap_rec_num", 1}, {"--cap_mem_size", 1}, {"--params", 1},
  };
  std::map<std::string, std::vector<std::string>> cmd_args;
  std::string cmd_error;
  if (!ParseCommandArguments(argc, args, cmd_configs, &cmd_args, &cmd_error)) {
    EPrint("Invalid command: ", cmd_error, "\n\n");
    PrintUsageAndDie();
  }
  const std::string dbm_impl = GetStringArgument(cmd_args, "--dbm", 0, "auto");
  const int32_t num_iterations = GetIntegerArgument(cmd_args, "--iter", 0, 10000);
  const int32_t value_size = GetIntegerArgument(cmd_args, "--size", 0, 8);
  const int32_t num_threads = GetIntegerArgument(cmd_args, "--threads", 0, 1);
  const bool is_verbose = CheckMap(cmd_args, "--verbose");
  const bool is_random_key = CheckMap(cmd_args, "--random_key");
  const bool is_random_value = CheckMap(cmd_args, "--random_value");
  const bool is_get_only = CheckMap(cmd_args, "--get_only");
  const bool is_set_only = CheckMap(cmd_args, "--set_only");
  const bool is_remove_only = CheckMap(cmd_args, "--remove_only");
  const std::string file_path = GetStringArgument(cmd_args, "--path", 0, "");
  const std::string file_impl = GetStringArgument(cmd_args, "--file", 0, "mmap-para");
  const bool with_no_wait = CheckMap(cmd_args, "--no_wait");
  const bool with_no_lock = CheckMap(cmd_args, "--no_lock");
  const int32_t alloc_init_size = GetIntegerArgument(cmd_args, "--alloc_init", 0, -1);
  const double alloc_increment = GetDoubleArgument(cmd_args, "--alloc_inc", 0, 0);
  const bool is_append = CheckMap(cmd_args, "--append");
  const int32_t offset_width = GetIntegerArgument(cmd_args, "--offset_width", 0, -1);
  const int32_t align_pow = GetIntegerArgument( cmd_args, "--align_pow", 0, -1);
  const int64_t num_buckets = GetIntegerArgument(cmd_args, "--buckets", 0, -1);
  const int32_t fbp_cap = GetIntegerArgument(cmd_args, "--fbp_cap", 0, -1);
  const bool lock_mem_buckets = CheckMap(cmd_args, "--lock_mem_buckets");
  const int32_t max_page_size = GetIntegerArgument(cmd_args, "--max_page_size", 0, -1);
  const int32_t max_branches = GetIntegerArgument(cmd_args, "--max_branches", 0, -1);
  const int32_t max_cached_pages = GetIntegerArgument(cmd_args, "--max_cached_pages", 0, -1);
  const int32_t step_unit = GetIntegerArgument(cmd_args, "--step_unit", 0, -1);
  const int32_t max_level = GetIntegerArgument(cmd_args, "--max_level", 0, -1);
  const int64_t sort_mem_size = GetIntegerArgument(cmd_args, "--sort_mem_size", 0, -1);
  const bool insert_in_order = CheckMap(cmd_args, "--insert_in_order");
  const std::string reducer_name = GetStringArgument(cmd_args, "--reducer", 0, "none");
  const int32_t max_cached_records = GetIntegerArgument(cmd_args, "--max_cached_records", 0, -1);
  const int64_t cap_rec_num = GetIntegerArgument(cmd_args, "--cap_rec_num", 0, -1);
  const int64_t cap_mem_size = GetIntegerArgument(cmd_args, "--cap_mem_size", 0, -1);
  const std::string poly_params = GetStringArgument(cmd_args, "--params", 0, "");
  if (num_iterations < 1) {
    Die("Invalid number of iterations");
  }
  if (value_size < 1) {
    Die("Invalid size of a record");
  }
  if (num_threads < 1) {
    Die("Invalid number of threads");
  }
  const int64_t start_mem_rss = StrToInt(GetSystemInfo()["mem_rss"]);
  std::unique_ptr<DBM> dbm =
      MakeDBMOrDie(dbm_impl, file_impl, file_path, alloc_init_size, alloc_increment,
                   num_buckets, cap_rec_num, cap_mem_size);
  std::atomic_bool has_error(false);
  const int32_t dot_mod = std::max(num_iterations / 1000, 1);
  const int32_t fold_mod = std::max(num_iterations / 20, 1);
  auto setting_task = [&](int32_t id) {
    std::mt19937 key_mt(id);
    std::mt19937 misc_mt(id * 2 + 1);
    std::uniform_int_distribution<int32_t> key_num_dist(0, num_iterations * num_threads - 1);
    std::uniform_int_distribution<int32_t> value_size_dist(0, value_size);
    char* value_buf = new char[value_size];
    std::memset(value_buf, '0' + id % 10, value_size);
    bool midline = false;
    for (int32_t i = 0; i < num_iterations; i++) {
      const int32_t key_num = is_random_key ? key_num_dist(key_mt) : i * num_threads + id;
      const std::string& key = SPrintF("%08d", key_num);
      std::string_view value(value_buf, is_random_value ? value_size_dist(misc_mt) : value_size);
      const Status status = dbm->Set(key, value);
      if (status != Status::SUCCESS) {
        EPrintL("Set failed: ", status);
        has_error = true;
        break;
      }
      if (id == 0 && (i + 1) % dot_mod == 0) {
        PutChar('.');
        midline = true;
        if ((i + 1) % fold_mod == 0) {
          PrintF(" (%08d)\n", i + 1);
          midline = false;
        }
      }
    }
    if (midline) {
      PrintF(" (%08d)\n", num_iterations);
    }
    delete[] value_buf;
  };
  if (!is_get_only && !is_remove_only) {
    if (!SetUpDBM(dbm.get(), true, true, file_path, with_no_wait, with_no_lock,
                  is_append, offset_width, align_pow, num_buckets, fbp_cap, lock_mem_buckets,
                  max_page_size, max_branches, max_cached_pages,
                  step_unit, max_level, sort_mem_size, insert_in_order, max_cached_records,
                  poly_params)) {
      has_error = true;
    }
    PrintF("Setting: impl=%s num_iterations=%d value_size=%d num_threads=%d\n",
           dbm_impl.c_str(), num_iterations, value_size, num_threads);
    const double start_time = GetWallTime();
    std::vector<std::thread> threads;
    for (int32_t i = 0; i < num_threads; i++) {
      threads.emplace_back(std::thread(setting_task, i));
    }
    for (auto& thread : threads) {
      thread.join();
    }
    Print("Synchronizing: ... ");
    const double sync_start_time = GetWallTime();
    if (!SynchronizeDBM(dbm.get(), reducer_name)) {
      has_error = true;
    }
    const double sync_end_time = GetWallTime();
    PrintF("done (elapsed=%.6f)\n", sync_end_time - sync_start_time);
    const double end_time = GetWallTime();
    const double elapsed_time = end_time - start_time;
    const int64_t num_records = dbm->CountSimple();
    const int64_t mem_usage = StrToInt(GetSystemInfo()["mem_rss"]) - start_mem_rss;
    PrintF("Setting done: elapsed_time=%.6f num_records=%lld qps=%.0f mem=%lld\n",
           elapsed_time, num_records, num_iterations * num_threads / elapsed_time,
           mem_usage);
    if (!TearDownDBM(dbm.get(), file_path, is_verbose)) {
      has_error = true;
    }
    PrintL();
  }
  auto getting_task = [&](int32_t id) {
    std::mt19937 key_mt(id);
    std::uniform_int_distribution<int32_t> key_num_dist(0, num_iterations * num_threads - 1);
    std::uniform_int_distribution<int32_t> value_size_dist(0, value_size);
    bool midline = false;
    for (int32_t i = 0; i < num_iterations; i++) {
      const int32_t key_num = is_random_key ? key_num_dist(key_mt) : i * num_threads + id;
      const std::string& key = SPrintF("%08d", key_num);
      const Status status = dbm->Get(key);
      if (status != Status::SUCCESS) {
        EPrintL("Get failed: ", status);
        has_error = true;
        break;
      }
      if (id == 0 && (i + 1) % dot_mod == 0) {
        PutChar('.');
        midline = true;
        if ((i + 1) % fold_mod == 0) {
          PrintF(" (%08d)\n", i + 1);
          midline = false;
        }
      }
    }
    if (midline) {
      PrintF(" (%08d)\n", num_iterations);
    }
  };
  if (!is_set_only && !is_remove_only) {
    if (!SetUpDBM(dbm.get(), false, false, file_path, with_no_wait, with_no_lock,
                  is_append, offset_width, align_pow, num_buckets, fbp_cap, lock_mem_buckets,
                  max_page_size, max_branches, max_cached_pages,
                  step_unit, max_level, sort_mem_size, insert_in_order, max_cached_records,
                  poly_params)) {
      has_error = true;
    }
    PrintF("Getting: impl=%s num_iterations=%d value_size=%d num_threads=%d\n",
           dbm_impl.c_str(), num_iterations, value_size, num_threads);
    const double start_time = GetWallTime();
    std::vector<std::thread> threads;
    for (int32_t i = 0; i < num_threads; i++) {
      threads.emplace_back(std::thread(getting_task, i));
    }
    for (auto& thread : threads) {
      thread.join();
    }
    const double end_time = GetWallTime();
    const double elapsed_time = end_time - start_time;
    const int64_t num_records = dbm->CountSimple();
    const int64_t mem_usage = StrToInt(GetSystemInfo()["mem_rss"]) - start_mem_rss;
    PrintF("Getting done: elapsed_time=%.6f num_records=%lld qps=%.0f mem=%lld\n",
           elapsed_time, num_records, num_iterations * num_threads / elapsed_time,
           mem_usage);
    if (!TearDownDBM(dbm.get(), file_path, is_verbose)) {
      has_error = true;
    }
    PrintL();
  }
  auto removing_task = [&](int32_t id) {
    std::mt19937 key_mt(id);
    std::uniform_int_distribution<int32_t> key_num_dist(0, num_iterations * num_threads - 1);
    std::uniform_int_distribution<int32_t> value_size_dist(0, value_size);
    bool midline = false;
    for (int32_t i = 0; i < num_iterations; i++) {
      const int32_t key_num = is_random_key ? key_num_dist(key_mt) : i * num_threads + id;
      const std::string& key = SPrintF("%08d", key_num);
      const Status status = dbm->Remove(key);
      if (status != Status::SUCCESS && status != Status::NOT_FOUND_ERROR) {
        EPrintL("Remove failed: ", status);
        has_error = true;
        break;
      }
      if (id == 0 && (i + 1) % dot_mod == 0) {
        PutChar('.');
        midline = true;
        if ((i + 1) % fold_mod == 0) {
          PrintF(" (%08d)\n", i + 1);
          midline = false;
        }
      }
    }
    if (midline) {
      PrintF(" (%08d)\n", num_iterations);
    }
  };
  if (!is_set_only && !is_get_only) {
    if (!SetUpDBM(dbm.get(), true, false, file_path, with_no_wait, with_no_lock,
                  is_append, offset_width, align_pow, num_buckets, fbp_cap, lock_mem_buckets,
                  max_page_size, max_branches, max_cached_pages,
                  step_unit, max_level, sort_mem_size, insert_in_order, max_cached_records,
                  poly_params)) {
      has_error = true;
    }
    PrintF("Removing: impl=%s num_iterations=%d value_size=%d num_threads=%d\n",
           dbm_impl.c_str(), num_iterations, value_size, num_threads);
    const double start_time = GetWallTime();
    std::vector<std::thread> threads;
    for (int32_t i = 0; i < num_threads; i++) {
      threads.emplace_back(std::thread(removing_task, i));
    }
    for (auto& thread : threads) {
      thread.join();
    }
    Print("Synchronizing: ... ");
    const double sync_start_time = GetWallTime();
    if (!SynchronizeDBM(dbm.get(), reducer_name)) {
      has_error = true;
    }
    const double sync_end_time = GetWallTime();
    PrintF("done (elapsed=%.6f)\n", sync_end_time - sync_start_time);
    const double end_time = GetWallTime();
    const double elapsed_time = end_time - start_time;
    const int64_t num_records = dbm->CountSimple();
    const int64_t mem_usage = StrToInt(GetSystemInfo()["mem_rss"]) - start_mem_rss;
    PrintF("Removing done: elapsed_time=%.6f num_records=%lld qps=%.0f mem=%lld\n",
           elapsed_time, num_records, num_iterations * num_threads / elapsed_time,
           mem_usage);
    if (!TearDownDBM(dbm.get(), file_path, is_verbose)) {
      has_error = true;
    }
    PrintL();
  }
  return has_error ? 1 : 0;
}

// Processes the parallel subcommand.
static int32_t ProcessParallel(int32_t argc, const char** args) {
  const std::map<std::string, int32_t>& cmd_configs = {
    {"", 0}, {"--dbm", 1}, {"--iter", 1}, {"--size", 1}, {"--threads", 1}, {"--verbose", 0},
    {"--random_key", 0}, {"--random_value", 0}, {"--keys", 1}, {"--rebuild", 0}, {"--sleep", 1},
    {"--path", 1}, {"--file", 1}, {"--no_wait", 0}, {"--no_lock", 0},
    {"--alloc_init", 1}, {"--alloc_inc", 1},
    {"--append", 0}, {"--offset_width", 1}, {"--align_pow", 1}, {"--buckets", 1},
    {"--fbp_cap", 1}, {"--lock_mem_buckets", 0},
    {"--max_page_size", 1}, {"--max_branches", 1}, {"--max_cached_pages", 1},
    {"--step_unit", 1}, {"--max_level", 1}, {"--sort_mem_size", 1}, {"--insert_in_order", 0},
    {"--max_cached_records", 1}, {"--reducer", 1},
    {"--cap_rec_num", 1}, {"--cap_mem_size", 1}, {"--params", 1},
  };
  std::map<std::string, std::vector<std::string>> cmd_args;
  std::string cmd_error;
  if (!ParseCommandArguments(argc, args, cmd_configs, &cmd_args, &cmd_error)) {
    EPrint("Invalid command: ", cmd_error, "\n\n");
    PrintUsageAndDie();
  }
  const std::string dbm_impl = GetStringArgument(cmd_args, "--dbm", 0, "auto");
  const int32_t num_iterations = GetIntegerArgument(cmd_args, "--iter", 0, 10000);
  const int32_t value_size = GetIntegerArgument(cmd_args, "--size", 0, 8);
  const int32_t num_threads = GetIntegerArgument(cmd_args, "--threads", 0, 1);
  const bool is_verbose = CheckMap(cmd_args, "--verbose");
  const bool is_random_key = CheckMap(cmd_args, "--random_key");
  const bool is_random_value = CheckMap(cmd_args, "--random_value");
  const int32_t num_keys = GetIntegerArgument(cmd_args, "--keys", 0, 0);
  const bool with_rebuild = CheckMap(cmd_args, "--rebuild");
  const double time_sleep = GetDoubleArgument(cmd_args, "--sleep", 0, 0);
  const std::string file_path = GetStringArgument(cmd_args, "--path", 0, "");
  const std::string file_impl = GetStringArgument(cmd_args, "--file", 0, "mmap-para");
  const bool with_no_wait = CheckMap(cmd_args, "--no_wait");
  const bool with_no_lock = CheckMap(cmd_args, "--no_lock");
  const int32_t alloc_init_size = GetIntegerArgument(cmd_args, "--alloc_init", 0, -1);
  const double alloc_increment = GetDoubleArgument(cmd_args, "--alloc_inc", 0, 0);
  const bool is_append = CheckMap(cmd_args, "--append");
  const int32_t offset_width = GetIntegerArgument(cmd_args, "--offset_width", 0, -1);
  const int32_t align_pow = GetIntegerArgument(cmd_args, "--align_pow", 0, -1);
  const int64_t num_buckets = GetIntegerArgument(cmd_args, "--buckets", 0, -1);
  const int32_t fbp_cap = GetIntegerArgument(cmd_args, "--fbp_cap", 0, -1);
  const bool lock_mem_buckets = CheckMap(cmd_args, "--lock_mem_buckets");
  const int32_t max_page_size = GetIntegerArgument(cmd_args, "--max_page_size", 0, -1);
  const int32_t max_branches = GetIntegerArgument(cmd_args, "--max_branches", 0, -1);
  const int32_t max_cached_pages = GetIntegerArgument(cmd_args, "--max_cached_pages", 0, -1);
  const int32_t step_unit = GetIntegerArgument(cmd_args, "--step_unit", 0, -1);
  const int32_t max_level = GetIntegerArgument(cmd_args, "--max_level", 0, -1);
  const int64_t sort_mem_size = GetIntegerArgument(cmd_args, "--sort_mem_size", 0, -1);
  const bool insert_in_order = CheckMap(cmd_args, "--insert_in_order");
  const std::string reducer_name = GetStringArgument(cmd_args, "--reducer", 0, "none");
  const int32_t max_cached_records = GetIntegerArgument(cmd_args, "--max_cached_records", 0, -1);
  const int64_t cap_rec_num = GetIntegerArgument(cmd_args, "--cap_rec_num", 0, -1);
  const int64_t cap_mem_size = GetIntegerArgument(cmd_args, "--cap_mem_size", 0, -1);
  const std::string poly_params = GetStringArgument(cmd_args, "--params", 0, "");
  if (num_iterations < 1) {
    Die("Invalid number of iterations");
  }
  if (value_size < 1) {
    Die("Invalid size of a record");
  }
  if (num_threads < 1) {
    Die("Invalid number of threads");
  }
  const int64_t start_mem_rss = StrToInt(GetSystemInfo()["mem_rss"]);
  std::unique_ptr<DBM> dbm =
      MakeDBMOrDie(dbm_impl, file_impl, file_path, alloc_init_size, alloc_increment,
                   num_buckets, cap_rec_num, cap_mem_size);
  std::atomic_bool has_error(false);
  const int32_t dot_mod = std::max(num_iterations / 1000, 1);
  const int32_t fold_mod = std::max(num_iterations / 20, 1);
  std::atomic_int32_t master_id(0);
  auto task = [&](int32_t id) {
    std::mt19937 mt(id);
    std::uniform_int_distribution<int32_t> key_num_dist(0, num_iterations * num_threads - 1);
    std::uniform_int_distribution<int32_t> value_size_dist(0, value_size);
    std::uniform_int_distribution<int32_t> op_dist(0, INT32MAX);
    char* value_buf = new char[value_size];
    std::memset(value_buf, '0' + id % 10, value_size);
    bool midline = false;
    for (int32_t i = 0; i < num_iterations; i++) {
      int32_t key_num = is_random_key ? key_num_dist(mt) : i * num_threads + id;
      if (num_keys > 0) {
        key_num %= num_keys;
      }
      const std::string& key = SPrintF("%08d", key_num);
      std::string_view value(value_buf, is_random_value ? value_size_dist(mt) : value_size);
      if (with_rebuild && i % 100 == 0 && id == master_id.load()) {
        bool tobe = false;
        Status status = dbm->ShouldBeRebuilt(&tobe);
        if (status == Status::SUCCESS) {
          if (tobe) {
            status = dbm->Rebuild();
            if (status != Status::SUCCESS) {
              EPrintL("Rebuild failed: ", status);
              has_error = true;
            }
            master_id.store((master_id.load() + 1) % num_threads);
          }
        } else {
          EPrintL("ShouldBeRebuilt failed: ", status);
          has_error = true;
          break;
        }
      }
      if (op_dist(mt) % 5 == 0) {
        const Status status = dbm->Remove(key);
        if (status != Status::SUCCESS && status != Status::NOT_FOUND_ERROR) {
          EPrintL("Set failed: ", status);
          has_error = true;
          break;
        }
      } else if (op_dist(mt) % 2 == 0) {
        const Status status = dbm->Set(key, value);
        if (status != Status::SUCCESS) {
          EPrintL("Set failed: ", status);
          has_error = true;
          break;
        }
      } else {
        const Status status = dbm->Get(key);
        if (status != Status::SUCCESS && status != Status::NOT_FOUND_ERROR) {
          EPrintL("Set failed: ", status);
          has_error = true;
          break;
        }
      }
      if (time_sleep > 0) {
        tkrzw::Sleep(time_sleep);
      }
      if (id == 0 && (i + 1) % dot_mod == 0) {
        PutChar('.');
        midline = true;
        if ((i + 1) % fold_mod == 0) {
          PrintF(" (%08d)\n", i + 1);
          midline = false;
        }
      }
    }
    if (midline) {
      PrintF(" (%08d)\n", num_iterations);
    }
    delete[] value_buf;
  };
  if (!SetUpDBM(dbm.get(), true, true, file_path, with_no_wait, with_no_lock,
                is_append, offset_width, align_pow, num_buckets, fbp_cap, lock_mem_buckets,
                max_page_size, max_branches, max_cached_pages,
                step_unit, max_level, sort_mem_size, insert_in_order, max_cached_records,
                poly_params)) {
    has_error = true;
  }
  PrintF("Doing: impl=%s num_iterations=%d value_size=%d num_threads=%d\n",
         dbm_impl.c_str(), num_iterations, value_size, num_threads);
  const double start_time = GetWallTime();
  std::vector<std::thread> threads;
  for (int32_t i = 0; i < num_threads; i++) {
    threads.emplace_back(std::thread(task, i));
  }
  for (auto& thread : threads) {
    thread.join();
  }
  Print("Synchronizing: ... ");
  const double sync_start_time = GetWallTime();
  if (!SynchronizeDBM(dbm.get(), reducer_name)) {
    has_error = true;
  }
  const double sync_end_time = GetWallTime();
  PrintF("done (elapsed=%.6f)\n", sync_end_time - sync_start_time);
  const double end_time = GetWallTime();
  const double elapsed_time = end_time - start_time;
  const int64_t num_records = dbm->CountSimple();
  const int64_t mem_usage = StrToInt(GetSystemInfo()["mem_rss"]) - start_mem_rss;
  PrintF("Done: elapsed_time=%.6f num_records=%lld qps=%.0f mem=%lld\n",
         elapsed_time, num_records, num_iterations * num_threads / elapsed_time,
         mem_usage);
  if (!TearDownDBM(dbm.get(), file_path, is_verbose)) {
    has_error = true;
  }
  PrintL();
  return has_error ? 1 : 0;
}

// Processes the wicked subcommand.
static int32_t ProcessWicked(int32_t argc, const char** args) {
  const std::map<std::string, int32_t>& cmd_configs = {
    {"", 0}, {"--dbm", 1}, {"--iter", 1}, {"--size", 1}, {"--threads", 1}, {"--verbose", 0},
    {"--iterator", 0}, {"--sync", 0}, {"--clear", 0}, {"--rebuild", 0},
    {"--path", 1}, {"--file", 1}, {"--no_wait", 0}, {"--no_lock", 0},
    {"--alloc_init", 1}, {"--alloc_inc", 1},
    {"--append", 0}, {"--offset_width", 1}, {"--align_pow", 1}, {"--buckets", 1},
    {"--fbp_cap", 1}, {"--lock_mem_buckets", 0},
    {"--max_page_size", 1}, {"--max_branches", 1}, {"--max_cached_pages", 1},
    {"--step_unit", 1}, {"--max_level", 1}, {"--sort_mem_size", 1}, {"--insert_in_order", 0},
    {"--max_cached_records", 1}, {"--reducer", 1},
    {"--cap_rec_num", 1}, {"--cap_mem_size", 1}, {"--params", 1},
  };
  std::map<std::string, std::vector<std::string>> cmd_args;
  std::string cmd_error;
  if (!ParseCommandArguments(argc, args, cmd_configs, &cmd_args, &cmd_error)) {
    EPrint("Invalid command: ", cmd_error, "\n\n");
    PrintUsageAndDie();
  }
  const std::string dbm_impl = GetStringArgument(cmd_args, "--dbm", 0, "auto");
  const int32_t num_iterations = GetIntegerArgument(cmd_args, "--iter", 0, 10000);
  const int32_t value_size = GetIntegerArgument(cmd_args, "--size", 0, 8);
  const int32_t num_threads = GetIntegerArgument(cmd_args, "--threads", 0, 1);
  const bool is_verbose = CheckMap(cmd_args, "--verbose");
  const bool with_iterator = CheckMap(cmd_args, "--iterator");
  const bool with_sync = CheckMap(cmd_args, "--sync");
  const bool with_clear = CheckMap(cmd_args, "--clear");
  const bool with_rebuild = CheckMap(cmd_args, "--rebuild");
  const std::string file_path = GetStringArgument(cmd_args, "--path", 0, "");
  const std::string file_impl = GetStringArgument(cmd_args, "--file", 0, "mmap-para");
  const bool with_no_wait = CheckMap(cmd_args, "--no_wait");
  const bool with_no_lock = CheckMap(cmd_args, "--no_lock");
  const int32_t alloc_init_size = GetIntegerArgument(cmd_args, "--alloc_init", 0, -1);
  const double alloc_increment = GetDoubleArgument(cmd_args, "--alloc_inc", 0, 0);
  const bool is_append = CheckMap(cmd_args, "--append");
  const int32_t offset_width = GetIntegerArgument(cmd_args, "--offset_width", 0, -1);
  const int32_t align_pow = GetIntegerArgument(cmd_args, "--align_pow", 0, -1);
  const int64_t num_buckets = GetIntegerArgument(cmd_args, "--buckets", 0, -1);
  const int32_t fbp_cap = GetIntegerArgument(cmd_args, "--fbp_cap", 0, -1);
  const bool lock_mem_buckets = CheckMap(cmd_args, "--lock_mem_buckets");
  const int32_t max_page_size = GetIntegerArgument(cmd_args, "--max_page_size", 0, -1);
  const int32_t max_branches = GetIntegerArgument(cmd_args, "--max_branches", 0, -1);
  const int32_t max_cached_pages = GetIntegerArgument(cmd_args, "--max_cached_pages", 0, -1);
  const int32_t step_unit = GetIntegerArgument(cmd_args, "--step_unit", 0, -1);
  const int32_t max_level = GetIntegerArgument(cmd_args, "--max_level", 0, -1);
  const int64_t sort_mem_size = GetIntegerArgument(cmd_args, "--sort_mem_size", 0, -1);
  const bool insert_in_order = CheckMap(cmd_args, "--insert_in_order");
  const std::string reducer_name = GetStringArgument(cmd_args, "--reducer", 0, "none");
  const int32_t max_cached_records = GetIntegerArgument(cmd_args, "--max_cached_records", 0, -1);
  const int64_t cap_rec_num = GetIntegerArgument(cmd_args, "--cap_rec_num", 0, -1);
  const int64_t cap_mem_size = GetIntegerArgument(cmd_args, "--cap_mem_size", 0, -1);
  const std::string poly_params = GetStringArgument(cmd_args, "--params", 0, "");
  if (num_iterations < 1) {
    Die("Invalid number of iterations");
  }
  if (value_size < 1) {
    Die("Invalid size of a record");
  }
  if (num_threads < 1) {
    Die("Invalid number of threads");
  }
  const int64_t start_mem_rss = StrToInt(GetSystemInfo()["mem_rss"]);
  std::unique_ptr<DBM> dbm =
      MakeDBMOrDie(dbm_impl, file_impl, file_path, alloc_init_size, alloc_increment,
                   num_buckets, cap_rec_num, cap_mem_size);
  std::atomic_bool has_error(false);
  const int32_t dot_mod = std::max(num_iterations / 1000, 1);
  const int32_t fold_mod = std::max(num_iterations / 20, 1);
  auto task = [&](int32_t id) {
    std::mt19937 key_mt(id);
    std::mt19937 misc_mt(id * 2 + 1);
    std::uniform_int_distribution<int32_t> key_num_dist(0, num_iterations * num_threads - 1);
    std::uniform_int_distribution<int32_t> value_size_dist(0, value_size);
    std::uniform_int_distribution<int32_t> op_dist(0, INT32MAX);
    char* value_buf = new char[value_size];
    std::memset(value_buf, '0' + id % 10, value_size);
    bool midline = false;
    for (int32_t i = 0; i < num_iterations; i++) {
      const int32_t key_num = key_num_dist(key_mt);
      const std::string& key = SPrintF("%08d", key_num);
      std::string_view value(value_buf, value_size_dist(misc_mt));
      if (with_rebuild && op_dist(misc_mt) % (num_iterations / 2) == 0) {
        const Status status = dbm->Rebuild();
        if (status != Status::SUCCESS) {
          EPrintL("Rebuild failed: ", status);
          has_error = true;
          break;
        }
      } else if (with_clear && op_dist(misc_mt) % (num_iterations / 2) == 0) {
        const Status status = dbm->Clear();
        if (status != Status::SUCCESS) {
          EPrintL("Clear failed: ", status);
          has_error = true;
          break;
        }
      } else if (with_sync && op_dist(misc_mt) % (num_iterations / 2) == 0) {
        const Status status = dbm->Synchronize(false);
        if (status != Status::SUCCESS) {
          EPrintL("Synchronize failed: ", status);
          has_error = true;
          break;
        }
      } else if (with_iterator && op_dist(misc_mt) % 100 == 0) {
        auto iter = dbm->MakeIterator();
        if (dbm->IsOrdered() && op_dist(misc_mt) % 4 == 0) {
          const Status status = iter->Last();
          if (status != Status::SUCCESS) {
            EPrintL("Iterator::Last failed: ", status);
            has_error = true;
            break;
          }
        } else if (dbm->IsOrdered() && op_dist(misc_mt) % 4 == 0) {
          const bool inclusive = op_dist(misc_mt) % 2 == 0;
          const Status status = op_dist(misc_mt) % 2 == 0 ?
              iter->JumpLower(key, inclusive) : iter->JumpUpper(key, inclusive);
          if (status != Status::SUCCESS) {
            EPrintL("Iterator::Jump with bound failed: ", status);
            has_error = true;
            break;
          }
        } else if (op_dist(misc_mt) % 2 == 0) {
          const Status status = iter->First();
          if (status != Status::SUCCESS && status != Status::NOT_FOUND_ERROR) {
            EPrintL("Iterator::First failed: ", status);
            has_error = true;
            break;
          }
        } else {
          const Status status = iter->Jump(key);
          if (status != Status::SUCCESS && status != Status::NOT_FOUND_ERROR) {
            EPrintL("Iterator::Jump failed: ", status);
            has_error = true;
            break;
          }
        }
        for (int32_t j = 0; j < 3; ++j) {
          if (op_dist(misc_mt) % 5 == 0) {
            const Status status = iter->Remove();
            if (status != Status::SUCCESS) {
              if (status != Status::NOT_FOUND_ERROR) {
                EPrintL("Iterator::Remove failed: ", status);
                has_error = true;
              }
              break;
            }
          } else if (op_dist(misc_mt) % 3 == 0) {
            const Status status = iter->Set(value);
            if (status != Status::SUCCESS) {
              if (status != Status::NOT_FOUND_ERROR) {
                EPrintL("Iterator::Set failed: ", status);
                has_error = true;
              }
              break;
            }
          } else {
            std::string tmp_key, tmp_value;
            const Status status = iter->Get(&tmp_key, &tmp_value);
            if (status != Status::SUCCESS) {
              if (status != Status::NOT_FOUND_ERROR) {
                EPrintL("Iterator::Get failed: ", status);
                has_error = true;
              }
              break;
            }
          }
          if (dbm->IsOrdered() && op_dist(misc_mt) % 2 == 0) {
            const Status status = iter->Previous();
            if (status != Status::SUCCESS) {
              if (status != Status::NOT_FOUND_ERROR) {
                EPrintL("Iterator::Previous failed: ", status);
                has_error = true;
              }
              break;
            }
          } else {
            const Status status = iter->Next();
            if (status != Status::SUCCESS) {
              if (status != Status::NOT_FOUND_ERROR) {
                EPrintL("Iterator::Next failed: ", status);
                has_error = true;
              }
              break;
            }
          }
        }
      } else if (op_dist(misc_mt) % 8 == 0) {
        const Status status = dbm->Append(key, value, ",");
        if (status != Status::SUCCESS) {
          EPrintL("Append failed: ", status);
          has_error = true;
          break;
        }
      } else if (op_dist(misc_mt) % 5 == 0) {
        const Status status = dbm->Remove(key);
        if (status != Status::SUCCESS && status != Status::NOT_FOUND_ERROR) {
          EPrintL("Remove failed: ", status);
          has_error = true;
          break;
        }
      } else if (op_dist(misc_mt) % 3 == 0) {
        const bool overwrite = op_dist(misc_mt) % 3 != 0;
        std::string old_value;
        const Status status = dbm->Set(key, value, overwrite, &old_value);
        if (status != Status::SUCCESS && status != Status::DUPLICATION_ERROR) {
          EPrintL("Set failed: ", status);
          has_error = true;
          break;
        }
      } else {
        std::string rec_value;
        const Status status = dbm->Get(key, &rec_value);
        if (status != Status::SUCCESS && status != Status::NOT_FOUND_ERROR) {
          EPrintL("Get failed: ", status);
          has_error = true;
          break;
        }
      }
      if (id == 0 && i == num_iterations / 2) {
        const Status status = dbm->Synchronize(false);
        if (status != Status::SUCCESS) {
          EPrintL("Synchronize failed: ", status);
          has_error = true;
          break;
        }
      }
      if (id == 0 && (i + 1) % dot_mod == 0) {
        PutChar('.');
        midline = true;
        if ((i + 1) % fold_mod == 0) {
          PrintF(" (%08d)\n", i + 1);
          midline = false;
        }
      }
    }
    if (midline) {
      PrintF(" (%08d)\n", num_iterations);
    }
    delete[] value_buf;
  };
  if (!SetUpDBM(dbm.get(), true, true, file_path, with_no_wait, with_no_lock,
                is_append, offset_width, align_pow, num_buckets, fbp_cap, lock_mem_buckets,
                max_page_size, max_branches, max_cached_pages,
                step_unit, max_level, sort_mem_size, insert_in_order, max_cached_records,
                poly_params)) {
    has_error = true;
  }
  PrintF("Doing: impl=%s num_iterations=%d value_size=%d num_threads=%d\n",
         dbm_impl.c_str(), num_iterations, value_size, num_threads);
  const double start_time = GetWallTime();
  std::vector<std::thread> threads;
  for (int32_t i = 0; i < num_threads; i++) {
    threads.emplace_back(std::thread(task, i));
  }
  for (auto& thread : threads) {
    thread.join();
  }
  Print("Synchronizing: ... ");
  const double sync_start_time = GetWallTime();
  if (!SynchronizeDBM(dbm.get(), reducer_name)) {
    has_error = true;
  }
  const double sync_end_time = GetWallTime();
  PrintF("done (elapsed=%.6f)\n", sync_end_time - sync_start_time);
  const double end_time = GetWallTime();
  const double elapsed_time = end_time - start_time;
  const int64_t num_records = dbm->CountSimple();
  const int64_t mem_usage = StrToInt(GetSystemInfo()["mem_rss"]) - start_mem_rss;
  PrintF("Done: elapsed_time=%.6f num_records=%lld qps=%.0f mem=%lld\n",
         elapsed_time, num_records, num_iterations * num_threads / elapsed_time,
         mem_usage);
  if (!TearDownDBM(dbm.get(), file_path, is_verbose)) {
    has_error = true;
  }
  PrintL();
  return has_error ? 1 : 0;
}

// Processes the index subcommand.
static int32_t ProcessIndex(int32_t argc, const char** args) {
  const std::map<std::string, int32_t>& cmd_configs = {
    {"", 0}, {"--type", 1}, {"--iter", 1}, {"--threads", 1},
    {"--random_key", 0}, {"--random_value", 0},
    {"--path", 1},
    {"--append", 0}, {"--offset_width", 1}, {"--align_pow", 1}, {"--buckets", 1},
    {"--fbp_cap", 1}, {"--lock_mem_buckets", 0},
    {"--max_page_size", 1}, {"--max_branches", 1}, {"--max_cached_pages", 1},
  };
  std::map<std::string, std::vector<std::string>> cmd_args;
  std::string cmd_error;
  if (!ParseCommandArguments(argc, args, cmd_configs, &cmd_args, &cmd_error)) {
    EPrint("Invalid command: ", cmd_error, "\n\n");
    PrintUsageAndDie();
  }
  const std::string type = GetStringArgument(cmd_args, "--type", 0, "file");
  const int32_t num_iterations = GetIntegerArgument(cmd_args, "--iter", 0, 10000);
  const int32_t num_threads = GetIntegerArgument(cmd_args, "--threads", 0, 1);
  const bool is_random_key = CheckMap(cmd_args, "--random_key");
  const bool is_random_value = CheckMap(cmd_args, "--random_value");
  const std::string file_path = GetStringArgument(cmd_args, "--path", 0, "");
  const bool is_append = CheckMap(cmd_args, "--append");
  const int32_t offset_width = GetIntegerArgument(cmd_args, "--offset_width", 0, -1);
  const int32_t align_pow = GetIntegerArgument( cmd_args, "--align_pow", 0, -1);
  const int64_t num_buckets = GetIntegerArgument(cmd_args, "--buckets", 0, -1);
  const int32_t fbp_cap = GetIntegerArgument(cmd_args, "--fbp_cap", 0, -1);
  const bool lock_mem_buckets = CheckMap(cmd_args, "--lock_mem_buckets");
  const int32_t max_page_size = GetIntegerArgument(cmd_args, "--max_page_size", 0, -1);
  const int32_t max_branches = GetIntegerArgument(cmd_args, "--max_branches", 0, -1);
  const int32_t max_cached_pages = GetIntegerArgument(cmd_args, "--max_cached_pages", 0, -1);
  if (num_iterations < 1) {
    Die("Invalid number of iterations");
  }
  if (num_threads < 1) {
    Die("Invalid number of threads");
  }
  tkrzw::FileIndex index_file;
  tkrzw::MemIndex index_mem;
  tkrzw::StdIndex<int64_t, int64_t> index_n2n;
  tkrzw::StdIndex<int64_t, std::string> index_n2s;
  tkrzw::StdIndex<std::string, int64_t> index_s2n;
  tkrzw::StdIndex<std::string, std::string> index_s2s;
  tkrzw::StdIndexStr index_str;
  std::function<void(int64_t, int64_t)> adder, checker, remover;
  std::function<int64_t()> counter;
  if (type == "file") {
    if (file_path.empty()) {
      Die("The file path must be specified");
    }
    adder = [&](int64_t key, int64_t value) {
      index_file.Add(tkrzw::ToString(key), tkrzw::ToString(value));
    };
    checker = [&](int64_t key, int64_t value) {
      index_file.Check(tkrzw::ToString(key), tkrzw::ToString(value));
    };
    remover = [&](int64_t key, int64_t value) {
      index_file.Remove(tkrzw::ToString(key), tkrzw::ToString(value));
    };
    counter = [&]() {
      return index_file.Count();
    };
    tkrzw::TreeDBM::TuningParameters tuning_params;
    tuning_params.update_mode =
        is_append ? tkrzw::HashDBM::UPDATE_APPENDING : tkrzw::HashDBM::UPDATE_IN_PLACE;
    tuning_params.offset_width = offset_width;
    tuning_params.align_pow = align_pow;
    tuning_params.num_buckets = num_buckets;
    tuning_params.fbp_capacity = fbp_cap;
    tuning_params.lock_mem_buckets = lock_mem_buckets;
    tuning_params.max_page_size = max_page_size;
    tuning_params.max_branches = max_branches;
    tuning_params.max_cached_pages = max_cached_pages;
    const Status status = index_file.Open(file_path, true, File::OPEN_TRUNCATE, tuning_params);
    if (status != Status::SUCCESS) {
      Die("Open failed: ", status);
    }
  } else if (type == "mem") {
    adder = [&](int64_t key, int64_t value) {
      index_mem.Add(tkrzw::ToString(key), tkrzw::ToString(value));
    };
    checker = [&](int64_t key, int64_t value) {
      index_mem.Check(tkrzw::ToString(key), tkrzw::ToString(value));
    };
    remover = [&](int64_t key, int64_t value) {
      index_mem.Remove(tkrzw::ToString(key), tkrzw::ToString(value));
    };
    counter = [&]() {
      return index_mem.Count();
    };
  } else if (type == "n2n") {
    adder = [&](int64_t key, int64_t value) {
      index_n2n.Add(key, value);
    };
    checker = [&](int64_t key, int64_t value) {
      index_n2n.Check(key, value);
    };
    remover = [&](int64_t key, int64_t value) {
      index_n2n.Remove(key, value);
    };
    counter = [&]() {
      return index_n2n.Count();
    };
  } else if (type == "n2s") {
    adder = [&](int64_t key, int64_t value) {
      index_n2s.Add(key, tkrzw::ToString(value));
    };
    checker = [&](int64_t key, int64_t value) {
      index_n2s.Check(key, tkrzw::ToString(value));
    };
    remover = [&](int64_t key, int64_t value) {
      index_n2s.Remove(key, tkrzw::ToString(value));
    };
    counter = [&]() {
      return index_n2s.Count();
    };
  } else if (type == "s2n") {
    adder = [&](int64_t key, int64_t value) {
      index_s2n.Add(tkrzw::ToString(key), value);
    };
    checker = [&](int64_t key, int64_t value) {
      index_s2n.Check(tkrzw::ToString(key), value);
    };
    remover = [&](int64_t key, int64_t value) {
      index_s2n.Remove(tkrzw::ToString(key), value);
    };
    counter = [&]() {
      return index_s2n.Count();
    };
  } else if (type == "s2s") {
    adder = [&](int64_t key, int64_t value) {
      index_s2s.Add(tkrzw::ToString(key), tkrzw::ToString(value));
    };
    checker = [&](int64_t key, int64_t value) {
      index_s2s.Check(tkrzw::ToString(key), tkrzw::ToString(value));
    };
    remover = [&](int64_t key, int64_t value) {
      index_s2s.Remove(tkrzw::ToString(key), tkrzw::ToString(value));
    };
    counter = [&]() {
      return index_s2s.Count();
    };
  } else if (type == "str") {
    adder = [&](int64_t key, int64_t value) {
      index_str.Add(tkrzw::ToString(key), tkrzw::ToString(value));
    };
    checker = [&](int64_t key, int64_t value) {
      index_str.Check(tkrzw::ToString(key), tkrzw::ToString(value));
    };
    remover = [&](int64_t key, int64_t value) {
      index_str.Remove(tkrzw::ToString(key), tkrzw::ToString(value));
    };
    counter = [&]() {
      return index_str.Count();
    };
  } else {
    Die("Unknown index type");
  }
  const int64_t start_mem_rss = StrToInt(GetSystemInfo()["mem_rss"]);
  const int32_t dot_mod = std::max(num_iterations / 1000, 1);
  const int32_t fold_mod = std::max(num_iterations / 20, 1);
  auto task = [&](std::function<void(int64_t, int64_t)> op, int32_t id) {
    std::mt19937 key_mt(id);
    std::mt19937 value_mt(id * 2 + 1);
    std::uniform_int_distribution<int32_t> num_dist(0, num_iterations * num_threads - 1);
    bool midline = false;
    for (int32_t i = 0; i < num_iterations; i++) {
      const int32_t key_num = is_random_key ? num_dist(key_mt) : i * num_threads + id;
      const int32_t value_num = is_random_value ? num_dist(value_mt) : i * num_threads + id;
      op(key_num, value_num);
      if (id == 0 && (i + 1) % dot_mod == 0) {
        PutChar('.');
        midline = true;
        if ((i + 1) % fold_mod == 0) {
          PrintF(" (%08d)\n", i + 1);
          midline = false;
        }
      }
    }
    if (midline) {
      PrintF(" (%08d)\n", num_iterations);
    }
  };
  struct Menu final {
    std::function<void(int64_t, int64_t)> op;
    std::string label;
  };
  const std::vector<Menu> menus = {
    {adder, "Adding"},
    {checker, "Checking"},
    {remover, "Removing"},
  };
  for (const auto& menu : menus) {
    PrintF("%s: type=%s num_iterations=%d num_threads=%d\n",
           menu.label.c_str(), type.c_str(), num_iterations, num_threads);
    const double start_time = GetWallTime();
    std::vector<std::thread> threads;
    for (int32_t i = 0; i < num_threads; i++) {
      threads.emplace_back(std::thread(task, menu.op, i));
    }
    for (auto& thread : threads) {
      thread.join();
    }
    const double end_time = GetWallTime();
    const double elapsed_time = end_time - start_time;
    const int64_t num_records = counter();
    const int64_t mem_usage = StrToInt(GetSystemInfo()["mem_rss"]) - start_mem_rss;
    PrintF("%s done: elapsed_time=%.6f num_records=%lld qps=%.0f mem=%lld\n",
           menu.label.c_str(), elapsed_time, num_records,
           num_iterations * num_threads / elapsed_time, mem_usage);
    PrintL();
  }
  if (type == "file") {
    const Status status = index_file.Close();
    if (status != Status::SUCCESS) {
      Die("Close failed: ", status);
    }
  }
  return 0;
}

}  // namespace tkrzw

// Main routine
int main(int argc, char** argv) {
  const char** args = const_cast<const char**>(argv);
  if (argc < 2) {
    tkrzw::PrintUsageAndDie();
  }
  int32_t rv = 0;
  try {
    if (std::strcmp(args[1], "sequence") == 0) {
      rv = tkrzw::ProcessSequence(argc - 1, args + 1);
    } else if (std::strcmp(args[1], "parallel") == 0) {
      rv = tkrzw::ProcessParallel(argc - 1, args + 1);
    } else if (std::strcmp(args[1], "wicked") == 0) {
      rv = tkrzw::ProcessWicked(argc - 1, args + 1);
    } else if (std::strcmp(args[1], "index") == 0) {
      rv = tkrzw::ProcessIndex(argc - 1, args + 1);
    } else {
      tkrzw::PrintUsageAndDie();
    }
  } catch (const std::runtime_error& e) {
    std::cerr << e.what() << std::endl;
    rv = 1;
  }
  return rv;
}

// END OF FILE
