/************************************************************************************************
 * Command line interface of DBM utilities
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

// Prints the usage to the standard error and die.
static void PrintUsageAndDie() {
  auto P = EPrintF;
  const char* progname = "tkrzw_dbm_util";
  P("%s: DBM utilities of Tkrzw\n", progname);
  P("\n");
  P("Usage:\n");
  P("  %s create [common_options] [tuning_options] [options] file\n", progname);
  P("    : Creates a database file.\n");
  P("  %s inspect [common_options] file\n", progname);
  P("    : Prints inspection of a database file.\n");
  P("  %s get [common_options] file key\n", progname);
  P("    : Gets a record and prints it.\n");
  P("  %s set [common_options] [options] file key\n", progname);
  P("    : Sets a record.\n");
  P("  %s remove [common_options] file key\n", progname);
  P("    : Removes a record.\n");
  P("  %s list [common_options] file\n", progname);
  P("    : Lists up records and prints them.\n");
  P("  %s rebuild [common_options] [tuning_options] file\n", progname);
  P("    : Rebuilds a database file for optimization.\n");
  P("  %s restore [common_options] old_file new_file\n", progname);
  P("    : Restores a broken database file.\n");
  P("  %s merge [common_options] dest_file src_files...\n", progname);
  P("    : Merges database files.\n");
  P("  %s export [common_options] [options] dbm_file rec_file\n", progname);
  P("    : Exports records to a flat record file.\n");
  P("  %s import [common_options] [options] dbm_file rec_file\n", progname);
  P("    : Imports records from a flat record file.\n");
  P("\n");
  P("Common options:\n");
  P("  --dbm impl : The name of a DBM implementation:"
    " auto, hash, tree, skip, tiny, baby, cache, stdhash, stdtree, poly, shard."
    " (default: auto)\n");
  P("  --file impl : The name of a file implementation:"
    " mmap-para, mmap-atom, pos-para, pos-atom. (default: mmap-para)\n");
  P("  --no_wait : Fails if the file is locked by another process.\n");
  P("  --no_lock : Omits file locking.\n");
  P("\n");
  P("Options for the create subcommand:\n");
  P("  --truncate : Truncates an existing database file.\n");
  P("\n");
  P("Options for the set subcommand:\n");
  P("  --no_overwrite : Fails if there's an existing record wit the same key.\n");
  P("  --reducer func : Sets the reducer for the skip database:"
    " none, first, second, last, concat, concatnull, concattab, concatline, total."
    " (default: none)\n");
  P("\n");
  P("Options for the list subcommand:\n");
  P("  --jump pattern : Jumps to the position of the pattern.\n");
  P("  --items num : The number of items to print.\n");
  P("  --escape : C-style escape is applied to the TSV data.\n");
  P("\n");
  P("Options for the rebuild subcommand:\n");
  P("  --restore : Skips broken records to restore a broken database.\n");
  P("\n");
  P("Options for the restore subcommand:\n");
  P("  --end_offset : The exclusive end offset of records to read. (default: -1)\n");
  P("\n");
  P("Options for the merge subcommand:\n");
  P("  --reducer func : Sets the reducer for the skip database:"
    " none, first, second, last, concat, concatnull, concattab, concatline, total."
    " (default: none)\n");
  P("\n");
  P("Options for the export and import subcommands:\n");
  P("  --tsv : The record file is in TSV format instead of flat record.\n");
  P("  --escape : C-style escape/unescape is applied to the TSV data.\n");
  P("  --keys : Exports keys only.\n");
  P("\n");
  P("Tuning options for HashDBM:\n");
  P("  --in_place : Uses in-place rather than pre-defined ones.\n");
  P("  --append : Uses appending rather than pre-defined ones.\n");
  P("  --offset_width num : The width to represent the offset of records. (default: %d or -1)\n",
    HashDBM::DEFAULT_OFFSET_WIDTH);
  P("  --align_pow num : Sets the power to align records. (default: %d or -1)\n",
    HashDBM::DEFAULT_ALIGN_POW);
  P("  --buckets num : Sets the number of buckets for hashing. (default: %lld or -1)\n",
    HashDBM::DEFAULT_NUM_BUCKETS);
  P("\n");
  P("Tuning options for TreeDBM:\n");
  P("  --in_place : Uses in-place rather than pre-defined ones.\n");
  P("  --append : Uses appending rather than pre-defined ones.\n");
  P("  --offset_width num : The width to represent the offset of records. (default: %d or -1)\n",
    TreeDBM::DEFAULT_OFFSET_WIDTH);
  P("  --align_pow num : Sets the power to align records. (default: %d or -1)\n",
    TreeDBM::DEFAULT_ALIGN_POW);
  P("  --buckets num : Sets the number of buckets for hashing. (default: %lld or -1)\n",
    TreeDBM::DEFAULT_NUM_BUCKETS);
  P("  --max_page_size num : Sets the maximum size of a page. (default: %d or -1)\n",
    TreeDBM::DEFAULT_MAX_PAGE_SIZE);
  P("  --max_branches num : Sets the maximum number of branches of inner nodes."
    " (default: %d or -1)\n", TreeDBM::DEFAULT_MAX_BRANCHES);
  P("  --comparator func : Sets the key comparator:"
    " lex, lexcase, dec, hex. (default: lex)\n");
  P("\n");
  P("Tuning options for SkipDBM:\n");
  P("  --offset_width num : The width to represent the offset of records. (default: %d)\n",
    SkipDBM::DEFAULT_OFFSET_WIDTH);
  P("  --step_unit num : Sets the step unit of the skip list. (default: %d)\n",
    SkipDBM::DEFAULT_STEP_UNIT);
  P("  --max_level num : Sets the maximum level of the skip list. (default: %d)\n",
    SkipDBM::DEFAULT_MAX_LEVEL);
  P("  --sort_mem_size num : Sets the memory size used for sorting. (default: %lld)\n",
    SkipDBM::DEFAULT_SORT_MEM_SIZE);
  P("  --insert_in_order : Inserts records in ascending order order of the key.\n");
  P("\n");
  P("Options for PolyDBM and ShardDBM:\n");
  P("  --params str : Sets the parameters in \"key=value,key=value\" format.\n");
  P("\n");
  std::exit(1);
}

// Gets a DBM implemenation name.
std::string GetDBMImplName(const std::string& dbm_impl, const std::string& file_path) {
  if (dbm_impl == "auto") {
    const std::string ext = StrLowerCase(PathToExtension(file_path));
    if (ext == "tkh") {
      return "hash";
    } else if (ext == "tkt") {
      return "tree";
    } else if (ext == "tks") {
      return "skip";
    } else if (ext == "tkmt" || ext == "flat") {
      return "tiny";
    } else if (ext == "tkmb") {
      return "baby";
    } else if (ext == "tkmc") {
      return "cache";
    } else if (ext == "tksh") {
      return "stdhash";
    } else if (ext == "tkst") {
      return "stdtree";
    } else if (!ext.empty()) {
      return ext;
    }
  }
  return dbm_impl;
}

// Makes a DBM object or die.
std::unique_ptr<DBM> MakeDBMOrDie(
    const std::string& dbm_impl, const std::string& file_impl,
    const std::string& file_path) {
  if (file_path.empty()) {
    Die("The file path must be specified");
  }
  const std::string dbm_impl_mod = GetDBMImplName(dbm_impl, file_path);
  std::unique_ptr<DBM> dbm;
  if (dbm_impl_mod == "hash") {
    dbm = std::make_unique<HashDBM>(MakeFileOrDie(file_impl, 0, 0));
  } else if (dbm_impl_mod == "tree") {
    dbm = std::make_unique<TreeDBM>(MakeFileOrDie(file_impl, 0, 0));
  } else if (dbm_impl_mod == "skip") {
    dbm = std::make_unique<SkipDBM>(MakeFileOrDie(file_impl, 0, 0));
  } else if (dbm_impl_mod == "tiny") {
    dbm = std::make_unique<TinyDBM>(MakeFileOrDie(file_impl, 0, 0));
  } else if (dbm_impl_mod == "baby") {
    dbm = std::make_unique<BabyDBM>(MakeFileOrDie(file_impl, 0, 0));
  } else if (dbm_impl_mod == "cache") {
    dbm = std::make_unique<CacheDBM>(MakeFileOrDie(file_impl, 0, 0));
  } else if (dbm_impl_mod == "stdhash") {
    dbm = std::make_unique<StdHashDBM>(MakeFileOrDie(file_impl, 0, 0));
  } else if (dbm_impl_mod == "stdtree") {
    dbm = std::make_unique<StdTreeDBM>(MakeFileOrDie(file_impl, 0, 0));
  } else if (dbm_impl_mod == "poly") {
    dbm = std::make_unique<PolyDBM>();
  } else if (dbm_impl_mod == "shard") {
    dbm = std::make_unique<ShardDBM>();
  } else {
    Die("Unknown DBM implementation: ", dbm_impl_mod);
  }
  return dbm;
}

// Gets a key comparator or die.
KeyComparator GetKeyComparatorOrDie(const std::string& cmp_name) {
  KeyComparator comp = nullptr;
  if (cmp_name == "lex") {
    comp = LexicalKeyComparator;
  } else if (cmp_name == "lexcase") {
    comp = LexicalCaseKeyComparator;
  } else if (cmp_name == "dec") {
    comp = DecimalKeyComparator;
  } else if (cmp_name == "hex") {
    comp = HexadecimalKeyComparator;
  } else if (cmp_name == "real") {
    comp = RealNumberKeyComparator;
  } else {
    Die("Unknown KeyComparator implementation: ", cmp_name);
  }
  return comp;
}

// Gets a reducer or die.
SkipDBM::ReducerType GetReducerOrDie(const std::string& reducer_name) {
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
  } else if (reducer_name == "concatnull") {
    reducer = SkipDBM::ReduceConcatWithNull;
  } else if (reducer_name == "concattab") {
    reducer = SkipDBM::ReduceConcatWithTab;
  } else if (reducer_name == "concatline") {
    reducer = SkipDBM::ReduceConcatWithLine;
  } else if (reducer_name == "total") {
    reducer = SkipDBM::ReduceToTotal;
  } else {
    Die("Unknown ReducerType implementation: ", reducer_name);
  }
  return reducer;
}

// Opens a database file.
bool OpenDBM(DBM* dbm, const std::string& path, bool writable, bool create, bool truncate,
             bool with_no_wait, bool with_no_lock,
             bool is_in_place, bool is_append,
             int32_t offset_width, int32_t align_pow, int64_t num_buckets,
             int32_t max_page_size, int32_t max_branches, const std::string& cmp_name,
             int32_t step_unit, int32_t max_level, int64_t sort_mem_size, bool insert_in_order,
             const std::string& poly_params) {
  bool has_error= false;
  int32_t open_options = File::OPEN_DEFAULT;
  if (!create) {
    open_options |= File::OPEN_NO_CREATE;
  }
  if (truncate) {
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
    if (is_in_place) {
      tuning_params.update_mode = tkrzw::HashDBM::UPDATE_IN_PLACE;
    } else if (is_append) {
      tuning_params.update_mode = tkrzw::HashDBM::UPDATE_APPENDING;
    }
    tuning_params.offset_width = offset_width;
    tuning_params.align_pow = align_pow;
    tuning_params.num_buckets = num_buckets;
    tuning_params.lock_mem_buckets = false;
    const Status status = hash_dbm->OpenAdvanced(path, writable, open_options, tuning_params);
    if (status != Status::SUCCESS) {
      EPrintL("OpenAdvanced failed: ", status);
      has_error = true;
    }
  }
  if (typeid(*dbm) == typeid(TreeDBM)) {
    TreeDBM* tree_dbm = dynamic_cast<TreeDBM*>(dbm);
    tkrzw::TreeDBM::TuningParameters tuning_params;
    if (is_in_place) {
      tuning_params.update_mode = tkrzw::HashDBM::UPDATE_IN_PLACE;
    } else if (is_append) {
      tuning_params.update_mode = tkrzw::HashDBM::UPDATE_APPENDING;
    }
    tuning_params.offset_width = offset_width;
    tuning_params.align_pow = align_pow;
    tuning_params.num_buckets = num_buckets;
    tuning_params.lock_mem_buckets = false;
    tuning_params.max_page_size = max_page_size;
    tuning_params.max_branches = max_branches;
    if (!cmp_name.empty()) {
      tuning_params.key_comparator = GetKeyComparatorOrDie(cmp_name);
    }
    const Status status = tree_dbm->OpenAdvanced(path, writable, open_options, tuning_params);
    if (status != Status::SUCCESS) {
      EPrintL("OpenAdvanced failed: ", status);
      has_error = true;
    }
  }
  if (typeid(*dbm) == typeid(SkipDBM)) {
    SkipDBM* skip_dbm = dynamic_cast<SkipDBM*>(dbm);
    int32_t open_options = File::OPEN_DEFAULT;
    tkrzw::SkipDBM::TuningParameters tuning_params;
    tuning_params.offset_width = offset_width;
    tuning_params.step_unit = step_unit;
    tuning_params.max_level = max_level;
    tuning_params.sort_mem_size = sort_mem_size;
    tuning_params.insert_in_order = insert_in_order;
    const Status status = skip_dbm->OpenAdvanced(path, writable, open_options, tuning_params);
    if (status != Status::SUCCESS) {
      EPrintL("OpenAdvanced failed: ", status);
      has_error = true;
    }
  }
  if (typeid(*dbm) == typeid(TinyDBM) || typeid(*dbm) == typeid(BabyDBM) ||
      typeid(*dbm) == typeid(CacheDBM) ||
      typeid(*dbm) == typeid(StdHashDBM) || typeid(*dbm) == typeid(StdTreeDBM)) {
    const Status status = dbm->Open(path, writable, open_options);
    if (status != Status::SUCCESS) {
      EPrintL("Open failed: ", status);
      has_error = true;
    }
  }
  if (typeid(*dbm) == typeid(PolyDBM) || typeid(*dbm) == typeid(ShardDBM)) {
    ParamDBM* param_dbm = dynamic_cast<ParamDBM*>(dbm);
    const std::map<std::string, std::string> tuning_params =
        tkrzw::StrSplitIntoMap(poly_params, ",", "=");
    const Status status =
        param_dbm->OpenAdvanced(path, writable, open_options, tuning_params);
    if (status != Status::SUCCESS) {
      EPrintL("OpenAdvanced failed: ", status);
      has_error = true;
    }
  }
  return !has_error;
}

// Closes a database file
bool CloseDBM(DBM* dbm) {
  bool has_error = false;
  const Status status = dbm->Close();
  if (status != Status::SUCCESS) {
    EPrintL("Close failed: ", status);
    has_error = true;
  }
  return !has_error;
}

// Rebuilds a database file.
bool RebuildDBM(DBM* dbm, bool is_in_place, bool is_append,
                int32_t offset_width, int32_t align_pow, int64_t num_buckets,
                int32_t max_page_size, int32_t max_branches,
                int32_t step_unit, int32_t max_level,
                const std::string& poly_params, bool restore) {
  bool has_error= false;
  if (typeid(*dbm) == typeid(HashDBM)) {
    HashDBM* hash_dbm = dynamic_cast<HashDBM*>(dbm);
    tkrzw::HashDBM::TuningParameters tuning_params;
    if (is_in_place) {
      tuning_params.update_mode = tkrzw::HashDBM::UPDATE_IN_PLACE;
    } else if (is_append) {
      tuning_params.update_mode = tkrzw::HashDBM::UPDATE_APPENDING;
    }
    tuning_params.offset_width = offset_width;
    tuning_params.align_pow = align_pow;
    tuning_params.num_buckets = num_buckets;
    tuning_params.lock_mem_buckets = false;
    const Status status = hash_dbm->RebuildAdvanced(tuning_params, restore);
    if (status != Status::SUCCESS) {
      EPrintL("RebuildAdvanced failed: ", status);
      has_error = true;
    }
  }
  if (typeid(*dbm) == typeid(TreeDBM)) {
    TreeDBM* tree_dbm = dynamic_cast<TreeDBM*>(dbm);
    tkrzw::TreeDBM::TuningParameters tuning_params;
    if (is_in_place) {
      tuning_params.update_mode = tkrzw::HashDBM::UPDATE_IN_PLACE;
    } else if (is_append) {
      tuning_params.update_mode = tkrzw::HashDBM::UPDATE_APPENDING;
    }
    tuning_params.offset_width = offset_width;
    tuning_params.align_pow = align_pow;
    tuning_params.num_buckets = num_buckets;
    tuning_params.lock_mem_buckets = false;
    tuning_params.max_page_size = max_page_size;
    tuning_params.max_branches = max_branches;
    const Status status = tree_dbm->RebuildAdvanced(tuning_params);
    if (status != Status::SUCCESS) {
      EPrintL("RebuildAdvanced failed: ", status);
      has_error = true;
    }
  }
  if (typeid(*dbm) == typeid(SkipDBM)) {
    SkipDBM* skip_dbm = dynamic_cast<SkipDBM*>(dbm);
    tkrzw::SkipDBM::TuningParameters tuning_params;
    tuning_params.offset_width = offset_width;
    tuning_params.step_unit = step_unit;
    tuning_params.max_level = max_level;
    const Status status = skip_dbm->RebuildAdvanced(tuning_params);
    if (status != Status::SUCCESS) {
      EPrintL("RebuildAdvanced failed: ", status);
      has_error = true;
    }
  }
  if (typeid(*dbm) == typeid(PolyDBM) || typeid(*dbm) == typeid(ShardDBM)) {
    ParamDBM* param_dbm = dynamic_cast<ParamDBM*>(dbm);
    const std::map<std::string, std::string> tuning_params =
        tkrzw::StrSplitIntoMap(poly_params, ",", "=");
    const Status status = param_dbm->RebuildAdvanced(tuning_params);
    if (status != Status::SUCCESS) {
      EPrintL("RebuildAdvanced failed: ", status);
      has_error = true;
    }
  }
  return !has_error;
}

// Processes the create subcommand.
static int32_t ProcessCreate(int32_t argc, const char** args) {
  const std::map<std::string, int32_t>& cmd_configs = {
    {"", 1}, {"--dbm", 1}, {"--file", 1}, {"--no_wait", 0}, {"--no_lock", 0},
    {"--in_place", 0}, {"--append", 0},
    {"--offset_width", 1}, {"--align_pow", 1}, {"--buckets", 1},
    {"--max_page_size", 1}, {"--max_branches", 1}, {"--comparator", 1},
    {"--step_unit", 1}, {"--max_level", 1},
    {"--params", 1}, {"--truncate", 0},
  };
  std::map<std::string, std::vector<std::string>> cmd_args;
  std::string cmd_error;
  if (!ParseCommandArguments(argc, args, cmd_configs, &cmd_args, &cmd_error)) {
    EPrint("Invalid command: ", cmd_error, "\n\n");
    PrintUsageAndDie();
  }
  const std::string file_path = GetStringArgument(cmd_args, "", 0, "");
  const std::string dbm_impl = GetStringArgument(cmd_args, "--dbm", 0, "auto");
  const std::string file_impl = GetStringArgument(cmd_args, "--file", 0, "mmap-para");
  const bool with_no_wait = CheckMap(cmd_args, "--no_wait");
  const bool with_no_lock = CheckMap(cmd_args, "--no_lock");
  const bool is_in_place = CheckMap(cmd_args, "--in_place");
  const bool is_append = CheckMap(cmd_args, "--append");
  const int32_t offset_width = GetIntegerArgument(cmd_args, "--offset_width", 0, -1);
  const int32_t align_pow = GetIntegerArgument(cmd_args, "--align_pow", 0, -1);
  const int64_t num_buckets = GetIntegerArgument(cmd_args, "--buckets", 0, -1);
  const int32_t max_page_size = GetIntegerArgument(cmd_args, "--max_page_size", 0, -1);
  const int32_t max_branches = GetIntegerArgument(cmd_args, "--max_branches", 0, -1);
  const std::string cmp_name = GetStringArgument(cmd_args, "--comparator", 0, "lex");
  const int32_t step_unit = GetIntegerArgument(cmd_args, "--step_unit", 0, -1);
  const int32_t max_level = GetIntegerArgument(cmd_args, "--max_level", 0, -1);
  const std::string poly_params = GetStringArgument(cmd_args, "--params", 0, "");
  const bool with_truncate = CheckMap(cmd_args, "--truncate");
  if (file_path.empty()) {
    Die("The file path must be specified");
  }
  std::unique_ptr<DBM> dbm = MakeDBMOrDie(dbm_impl, file_impl, file_path);
  if (!OpenDBM(dbm.get(), file_path, true, true, with_truncate, with_no_wait, with_no_lock,
               is_in_place, is_append, offset_width, align_pow, num_buckets,
               max_page_size, max_branches, cmp_name,
               step_unit, max_level, -1, false,
               poly_params)) {
    return 1;
  }
  if (!CloseDBM(dbm.get())) {
    return 1;
  }
  return 0;
}

// Processes the inspect subcommand.
static int32_t ProcessInspect(int32_t argc, const char** args) {
  const std::map<std::string, int32_t>& cmd_configs = {
    {"", 1}, {"--dbm", 1}, {"--file", 1}, {"--no_wait", 0}, {"--no_lock", 0},
  };
  std::map<std::string, std::vector<std::string>> cmd_args;
  std::string cmd_error;
  if (!ParseCommandArguments(argc, args, cmd_configs, &cmd_args, &cmd_error)) {
    EPrint("Invalid command: ", cmd_error, "\n\n");
    PrintUsageAndDie();
  }
  const std::string file_path = GetStringArgument(cmd_args, "", 0, "");
  const std::string dbm_impl = GetStringArgument(cmd_args, "--dbm", 0, "auto");
  const std::string file_impl = GetStringArgument(cmd_args, "--file", 0, "mmap-para");
  const bool with_no_wait = CheckMap(cmd_args, "--no_wait");
  const bool with_no_lock = CheckMap(cmd_args, "--no_lock");
  if (file_path.empty()) {
    Die("The file path must be specified");
  }
  std::unique_ptr<DBM> dbm = MakeDBMOrDie(dbm_impl, file_impl, file_path);
  if (!OpenDBM(dbm.get(), file_path, false, false, false, with_no_wait, with_no_lock,
               false, false, -1, -1, -1,
               -1, -1, "",
               -1, -1, -1, false,
               "")) {
    return 1;
  }
  PrintF("Inspection:\n");
  for (const auto& meta : dbm->Inspect()) {
    PrintL(StrCat("  ", meta.first, "=", meta.second));
  }
  PrintF("Actual File Size: %lld\n", dbm->GetFileSizeSimple());
  PrintF("Number of Records: %lld\n", dbm->CountSimple());
  PrintF("Should be Rebuilt: %s\n", dbm->ShouldBeRebuiltSimple() ? "true" : "false");
  if (!CloseDBM(dbm.get())) {
    return 1;
  }
  return 0;
}

// Processes the get subcommand.
static int32_t ProcessGet(int32_t argc, const char** args) {
  const std::map<std::string, int32_t>& cmd_configs = {
    {"", 2}, {"--dbm", 1}, {"--file", 1}, {"--no_wait", 0}, {"--no_lock", 0},
  };
  std::map<std::string, std::vector<std::string>> cmd_args;
  std::string cmd_error;
  if (!ParseCommandArguments(argc, args, cmd_configs, &cmd_args, &cmd_error)) {
    EPrint("Invalid command: ", cmd_error, "\n\n");
    PrintUsageAndDie();
  }
  const std::string file_path = GetStringArgument(cmd_args, "", 0, "");
  const std::string key = GetStringArgument(cmd_args, "", 1, "");
  const std::string dbm_impl = GetStringArgument(cmd_args, "--dbm", 0, "auto");
  const std::string file_impl = GetStringArgument(cmd_args, "--file", 0, "mmap-para");
  const bool with_no_wait = CheckMap(cmd_args, "--no_wait");
  const bool with_no_lock = CheckMap(cmd_args, "--no_lock");
  if (file_path.empty()) {
    Die("The file path must be specified");
  }
  std::unique_ptr<DBM> dbm = MakeDBMOrDie(dbm_impl, file_impl, file_path);
  if (!OpenDBM(dbm.get(), file_path, false, false, false, with_no_wait, with_no_lock,
               false, false, -1, -1, -1,
               -1, -1, "",
               -1, -1, -1, false,
               "")) {
    return 1;
  }
  bool ok = false;
  std::string value;
  const Status status = dbm->Get(key, &value);
  if (status == Status::SUCCESS) {
    PrintL(value);
    ok = true;
  } else {
    EPrintL("Get failed: ", status);
  }
  if (!CloseDBM(dbm.get())) {
    return 1;
  }
  return ok ? 0 : 1;
}

// Processes the set subcommand.
static int32_t ProcessSet(int32_t argc, const char** args) {
  const std::map<std::string, int32_t>& cmd_configs = {
    {"", 3}, {"--dbm", 1}, {"--file", 1}, {"--no_wait", 0}, {"--no_lock", 0},
    {"--no_overwrite", 0}, {"--reducer", 1},
  };
  std::map<std::string, std::vector<std::string>> cmd_args;
  std::string cmd_error;
  if (!ParseCommandArguments(argc, args, cmd_configs, &cmd_args, &cmd_error)) {
    EPrint("Invalid command: ", cmd_error, "\n\n");
    PrintUsageAndDie();
  }
  const std::string file_path = GetStringArgument(cmd_args, "", 0, "");
  const std::string key = GetStringArgument(cmd_args, "", 1, "");
  const std::string value = GetStringArgument(cmd_args, "", 2, "");
  const std::string dbm_impl = GetStringArgument(cmd_args, "--dbm", 0, "auto");
  const std::string file_impl = GetStringArgument(cmd_args, "--file", 0, "mmap-para");
  const bool with_no_wait = CheckMap(cmd_args, "--no_wait");
  const bool with_no_lock = CheckMap(cmd_args, "--no_lock");
  const bool with_no_overwrite = CheckMap(cmd_args, "--no_overwrite");
  const std::string reducer_name = GetStringArgument(cmd_args, "--reducer", 0, "none");
  if (file_path.empty()) {
    Die("The file path must be specified");
  }
  std::unique_ptr<DBM> dbm = MakeDBMOrDie(dbm_impl, file_impl, file_path);
  if (!OpenDBM(dbm.get(), file_path, true, false, false, with_no_wait, with_no_lock,
               false, false, -1, -1, -1,
               -1, -1, "",
               -1, -1, -1, false,
               "")) {
    return 1;
  }
  bool ok = false;
  const Status status = dbm->Set(key, value, !with_no_overwrite);
  if (status == Status::SUCCESS) {
    ok = true;
  } else {
    EPrintL("Set failed: ", status);
  }
  if (typeid(*dbm) == typeid(SkipDBM)) {
    SkipDBM* skip_dbm = dynamic_cast<SkipDBM*>(dbm.get());
    const Status status = skip_dbm->SynchronizeAdvanced(
        false, nullptr, GetReducerOrDie(reducer_name));
    if (status != Status::SUCCESS) {
      EPrintL("SynchronizeAdvanced failed: ", status);
      ok = false;
    }
  }
  if (!CloseDBM(dbm.get())) {
    return 1;
  }
  return ok ? 0 : 1;
}

// Processes the remove subcommand.
static int32_t ProcessRemove(int32_t argc, const char** args) {
  const std::map<std::string, int32_t>& cmd_configs = {
    {"", 2}, {"--dbm", 1}, {"--file", 1}, {"--no_wait", 0}, {"--no_lock", 0},
  };
  std::map<std::string, std::vector<std::string>> cmd_args;
  std::string cmd_error;
  if (!ParseCommandArguments(argc, args, cmd_configs, &cmd_args, &cmd_error)) {
    EPrint("Invalid command: ", cmd_error, "\n\n");
    PrintUsageAndDie();
  }
  const std::string file_path = GetStringArgument(cmd_args, "", 0, "");
  const std::string key = GetStringArgument(cmd_args, "", 1, "");
  const std::string dbm_impl = GetStringArgument(cmd_args, "--dbm", 0, "auto");
  const std::string file_impl = GetStringArgument(cmd_args, "--file", 0, "mmap-para");
  const bool with_no_wait = CheckMap(cmd_args, "--no_wait");
  const bool with_no_lock = CheckMap(cmd_args, "--no_lock");
  if (file_path.empty()) {
    Die("The file path must be specified");
  }
  std::unique_ptr<DBM> dbm = MakeDBMOrDie(dbm_impl, file_impl, file_path);
  if (!OpenDBM(dbm.get(), file_path, true, false, false, with_no_wait, with_no_lock,
               false, false, -1, -1, -1,
               -1, -1, "",
               -1, -1, -1, false,
               "")) {
    return 1;
  }
  bool ok = false;
  const Status status = dbm->Remove(key);
  if (status == Status::SUCCESS) {
    ok = true;
  } else {
    EPrintL("Remove failed: ", status);
  }
  if (!CloseDBM(dbm.get())) {
    return 1;
  }
  return ok ? 0 : 1;
}

// Processes the list subcommand.
static int32_t ProcessList(int32_t argc, const char** args) {
  const std::map<std::string, int32_t>& cmd_configs = {
    {"", 1}, {"--dbm", 1}, {"--file", 1}, {"--no_wait", 0}, {"--no_lock", 0},
    {"--jump", 1}, {"--items", 1}, {"--escape", 0},
  };
  std::map<std::string, std::vector<std::string>> cmd_args;
  std::string cmd_error;
  if (!ParseCommandArguments(argc, args, cmd_configs, &cmd_args, &cmd_error)) {
    EPrint("Invalid command: ", cmd_error, "\n\n");
    PrintUsageAndDie();
  }
  const std::string file_path = GetStringArgument(cmd_args, "", 0, "");
  const std::string key = GetStringArgument(cmd_args, "", 1, "");
  const std::string dbm_impl = GetStringArgument(cmd_args, "--dbm", 0, "auto");
  const std::string file_impl = GetStringArgument(cmd_args, "--file", 0, "mmap-para");
  const bool with_no_wait = CheckMap(cmd_args, "--no_wait");
  const bool with_no_lock = CheckMap(cmd_args, "--no_lock");
  const std::string jump_pattern = GetStringArgument(cmd_args, "--jump", 0, "");
  const int64_t num_items = GetIntegerArgument(cmd_args, "--items", 0, INT64MAX);
  const bool with_escape = CheckMap(cmd_args, "--escape");
  if (file_path.empty()) {
    Die("The file path must be specified");
  }
  std::unique_ptr<DBM> dbm = MakeDBMOrDie(dbm_impl, file_impl, file_path);
  if (!OpenDBM(dbm.get(), file_path, false, false, false, with_no_wait, with_no_lock,
               false, false, -1, -1, -1,
               -1, -1, "",
               -1, -1, -1, false,
               "")) {
    return 1;
  }
  bool ok = true;
  if (jump_pattern.empty() && num_items == INT64MAX) {
    class Printer final : public DBM::RecordProcessor {
     public:
      explicit Printer(bool escape) : escape_(escape) {}
      std::string_view ProcessFull(std::string_view key, std::string_view value) override {
        const std::string& esc_key = escape_ ? StrEscapeC(key) : StrTrimForTSV(key);
        const std::string& esc_value = escape_ ? StrEscapeC(value) : StrTrimForTSV(value, true);
        PrintL(esc_key, "\t", esc_value);
        return NOOP;
      }
     private:
      bool escape_;
    } printer(with_escape);
    const Status status = dbm->ProcessEach(&printer, false);
    if (status != Status::SUCCESS) {
      EPrintL("ProcessEach failed: ", status);
      ok = false;
    }
  } else {
    auto iter = dbm->MakeIterator();
    if (jump_pattern.empty()) {
      const Status status = iter->First();
      if (status != Status::SUCCESS) {
        EPrintL("First failed: ", status);
        ok = false;
      }
    } else {
      const Status status = iter->Jump(jump_pattern);
      if (status != Status::SUCCESS) {
        EPrintL("Jump failed: ", status);
        ok = false;
      }
    }
    for (int64_t count = 0; count < num_items; count++) {
      std::string key, value;
      Status status = iter->Get(&key, &value);
      if (status != Status::SUCCESS) {
        if (status != Status::NOT_FOUND_ERROR) {
          EPrintL("Get failed: ", status);
          ok = false;
        }
        break;
      }
      const std::string& esc_key = with_escape ? StrEscapeC(key) : StrTrimForTSV(key);
      const std::string& esc_value = with_escape ? StrEscapeC(value) : StrTrimForTSV(value, true);
      PrintL(esc_key, "\t", esc_value);
      status = iter->Next();
      if (status != Status::SUCCESS) {
        EPrintL("Next failed: ", status);
        ok = false;
        break;
      }
    }
  }
  if (!CloseDBM(dbm.get())) {
    return 1;
  }
  return ok ? 0 : 1;
}

// Processes the rebuild subcommand.
static int32_t ProcessRebuild(int32_t argc, const char** args) {
  const std::map<std::string, int32_t>& cmd_configs = {
    {"", 1}, {"--dbm", 1}, {"--file", 1}, {"--no_wait", 0}, {"--no_lock", 0},
    {"--in_place", 0}, {"--append", 0},
    {"--offset_width", 1}, {"--align_pow", 1}, {"--buckets", 1},
    {"--max_page_size", 1}, {"--max_branches", 1},
    {"--step_unit", 1}, {"--max_level", 1},
    {"--params", 1}, {"--restore", 0},
  };
  std::map<std::string, std::vector<std::string>> cmd_args;
  std::string cmd_error;
  if (!ParseCommandArguments(argc, args, cmd_configs, &cmd_args, &cmd_error)) {
    EPrint("Invalid command: ", cmd_error, "\n\n");
    PrintUsageAndDie();
  }
  const std::string file_path = GetStringArgument(cmd_args, "", 0, "");
  const std::string dbm_impl = GetStringArgument(cmd_args, "--dbm", 0, "auto");
  const std::string file_impl = GetStringArgument(cmd_args, "--file", 0, "mmap-para");
  const bool with_no_wait = CheckMap(cmd_args, "--no_wait");
  const bool with_no_lock = CheckMap(cmd_args, "--no_lock");
  const bool is_in_place = CheckMap(cmd_args, "--in_place");
  const bool is_append = CheckMap(cmd_args, "--append");
  const int32_t offset_width = GetIntegerArgument(cmd_args, "--offset_width", 0, -1);
  const int32_t align_pow = GetIntegerArgument(cmd_args, "--align_pow", 0, -1);
  const int64_t num_buckets = GetIntegerArgument(cmd_args, "--buckets", 0, -1);
  const int32_t max_page_size = GetIntegerArgument(cmd_args, "--max_page_size", 0, -1);
  const int32_t max_branches = GetIntegerArgument(cmd_args, "--max_branches", 0, -1);
  const int32_t step_unit = GetIntegerArgument(cmd_args, "--step_unit", 0, -1);
  const int32_t max_level = GetIntegerArgument(cmd_args, "--max_level", 0, -1);
  const std::string poly_params = GetStringArgument(cmd_args, "--params", 0, "");
  const bool with_restore = CheckMap(cmd_args, "--restore");
  if (file_path.empty()) {
    Die("The file path must be specified");
  }
  std::unique_ptr<DBM> dbm = MakeDBMOrDie(dbm_impl, file_impl, file_path);
  if (!OpenDBM(dbm.get(), file_path, true, false, false, with_no_wait, with_no_lock,
               false, false, -1, -1, -1,
               max_page_size, max_branches, "",
               -1, -1, -1, false,
               poly_params)) {
    return 1;
  }
  bool ok = RebuildDBM(dbm.get(), is_in_place, is_append,
                       offset_width, align_pow, num_buckets,
                       max_page_size, max_branches,
                       step_unit, max_level,
                       poly_params,
                       with_restore);
  if (!CloseDBM(dbm.get())) {
    return 1;
  }
  return ok ? 0 : 1;
}

// Processes the restore subcommand.
static int32_t ProcessRestore(int32_t argc, const char** args) {
  const std::map<std::string, int32_t>& cmd_configs = {
    {"", 2}, {"--dbm", 1}, {"--end_offset", 1}, {"--class", 1},
  };
  std::map<std::string, std::vector<std::string>> cmd_args;
  std::string cmd_error;
  if (!ParseCommandArguments(argc, args, cmd_configs, &cmd_args, &cmd_error)) {
    EPrint("Invalid command: ", cmd_error, "\n\n");
    PrintUsageAndDie();
  }
  const std::string old_file_path = GetStringArgument(cmd_args, "", 0, "");
  const std::string new_file_path = GetStringArgument(cmd_args, "", 1, "");
  const std::string dbm_impl = GetStringArgument(cmd_args, "--dbm", 0, "auto");
  const int64_t end_offset = GetIntegerArgument(cmd_args, "--end_offset", 0, -1);
  const std::string class_name = GetStringArgument(cmd_args, "--class", 0, "");
  if (old_file_path.empty()) {
    Die("The old file path must be specified");
  }
  if (new_file_path.empty()) {
    Die("The new file path must be specified");
  }
  if (old_file_path == new_file_path) {
    Die("The old file and the new file must be different");
  }
  bool has_error = false;
  const std::string dbm_impl_mod = GetDBMImplName(dbm_impl, old_file_path);
  if (dbm_impl_mod == "hash") {
    const Status status = HashDBM::RestoreDatabase(old_file_path, new_file_path, end_offset);
    if (status != Status::SUCCESS) {
      EPrintL("RestoreDabase failed: ", status);
      has_error = true;
    }
  } else if (dbm_impl_mod == "tree") {
    const Status status = TreeDBM::RestoreDatabase(old_file_path, new_file_path, end_offset);
    if (status != Status::SUCCESS) {
      EPrintL("RestoreDabase failed: ", status);
      has_error = true;
    }
  } else if (dbm_impl_mod == "skip") {
    const Status status = SkipDBM::RestoreDatabase(old_file_path, new_file_path);
    if (status != Status::SUCCESS) {
      EPrintL("RestoreDabase failed: ", status);
      has_error = true;
    }
  } else if (dbm_impl_mod == "poly") {
    const Status status = PolyDBM::RestoreDatabase(
        old_file_path, new_file_path, class_name, end_offset);
    if (status != Status::SUCCESS) {
      EPrintL("RestoreDabase failed: ", status);
      has_error = true;
    }
  } else if (dbm_impl_mod == "shard") {
    const Status status = ShardDBM::RestoreDatabase(
        old_file_path, new_file_path, class_name, end_offset);
    if (status != Status::SUCCESS) {
      EPrintL("RestoreDabase failed: ", status);
      has_error = true;
    }
  } else {
    Die("Unknown DBM implementation: ", dbm_impl);
  }
  return has_error ? 1 : 0;
}

// Processes the merge subcommand.
static int32_t ProcessMerge(int32_t argc, const char** args) {
  const std::map<std::string, int32_t>& cmd_configs = {
    {"--dbm", 1}, {"--file", 1}, {"--no_wait", 0}, {"--no_lock", 0},
    {"--reducer", 1},
    {"--params", 1},
  };
  std::map<std::string, std::vector<std::string>> cmd_args;
  std::string cmd_error;
  if (!ParseCommandArguments(argc, args, cmd_configs, &cmd_args, &cmd_error)) {
    EPrint("Invalid command: ", cmd_error, "\n\n");
    PrintUsageAndDie();
  }
  const std::string dest_path = GetStringArgument(cmd_args, "", 0, "");
  const std::string dbm_impl = GetStringArgument(cmd_args, "--dbm", 0, "auto");
  const std::string file_impl = GetStringArgument(cmd_args, "--file", 0, "mmap-para");
  const bool with_no_wait = CheckMap(cmd_args, "--no_wait");
  const bool with_no_lock = CheckMap(cmd_args, "--no_lock");
  const std::string reducer_name = GetStringArgument(cmd_args, "--reducer", 0, "none");
  const std::string poly_params = GetStringArgument(cmd_args, "--params", 0, "");
  if (dest_path.empty()) {
    Die("The destination DBM path must be specified");
  }
  std::vector<std::string> src_paths;
  for (int32_t i = 1; true; i++) {
    const std::string src_path = GetStringArgument(cmd_args, "", i, "");
    if (src_path.empty()) {
      break;
    }
    src_paths.emplace_back(src_path);
  }
  std::unique_ptr<DBM> dbm = MakeDBMOrDie(dbm_impl, file_impl, dest_path);
  if (!OpenDBM(dbm.get(), dest_path, true, true, false, with_no_wait, with_no_lock,
               false, false, -1, -1, -1,
               -1, -1, "",
               -1, -1, -1, false,
               poly_params)) {
    return 1;
  }
  bool has_error = false;
  if (typeid(*dbm) == typeid(SkipDBM)) {
    SkipDBM* skip_dbm = dynamic_cast<SkipDBM*>(dbm.get());
    Status status(Status::SUCCESS);
    for (const auto& src_path : src_paths) {
      status |= skip_dbm->MergeSkipDatabase(src_path);
    }
    if (status != Status::SUCCESS) {
      EPrintL("MergeSkipDatabase failed: ", status);
      has_error = true;
    }
    status = skip_dbm->SynchronizeAdvanced(
        false, nullptr, GetReducerOrDie(reducer_name));
    if (status != Status::SUCCESS) {
      EPrintL("SynchronizeAdvanced failed: ", status);
      has_error = true;
    }
  } else {
    Status status(Status::SUCCESS);
    for (const auto& src_path : src_paths) {
      auto src_dbm = dbm->MakeDBM();
      status = src_dbm->Open(src_path, false);
      if (status != Status::SUCCESS) {
        EPrintL("Open failed: ", status);
        has_error = true;
        break;
      }
      status = src_dbm->Export(dbm.get());
      if (status != Status::SUCCESS) {
        EPrintL("Export failed: ", status);
        has_error = true;
        break;
      }
      status = src_dbm->Close();
      if (status != Status::SUCCESS) {
        EPrintL("Close failed: ", status);
        has_error = true;
        break;
      }
    }
  }
  if (!CloseDBM(dbm.get())) {
    return 1;
  }
  return has_error ? 1 : 0;
}

// Processes the export subcommand.
static int32_t ProcessExport(int32_t argc, const char** args) {
  const std::map<std::string, int32_t>& cmd_configs = {
    {"", 2}, {"--dbm", 1}, {"--file", 1}, {"--no_wait", 0}, {"--no_lock", 0},
    {"--tsv", 0}, {"--escape", 0}, {"--keys", 0},
  };
  std::map<std::string, std::vector<std::string>> cmd_args;
  std::string cmd_error;
  if (!ParseCommandArguments(argc, args, cmd_configs, &cmd_args, &cmd_error)) {
    EPrint("Invalid command: ", cmd_error, "\n\n");
    PrintUsageAndDie();
  }
  const std::string file_path = GetStringArgument(cmd_args, "", 0, "");
  const std::string rec_file_path = GetStringArgument(cmd_args, "", 1, "");
  const std::string dbm_impl = GetStringArgument(cmd_args, "--dbm", 0, "auto");
  const std::string file_impl = GetStringArgument(cmd_args, "--file", 0, "mmap-para");
  const bool with_no_wait = CheckMap(cmd_args, "--no_wait");
  const bool with_no_lock = CheckMap(cmd_args, "--no_lock");
  const bool is_tsv = CheckMap(cmd_args, "--tsv");
  const bool with_escape = CheckMap(cmd_args, "--escape");
  const bool keys_only = CheckMap(cmd_args, "--keys");
  if (file_path.empty()) {
    Die("The DBM file path must be specified");
  }
  if (rec_file_path.empty()) {
    Die("The flat file path must be specified");
  }
  if (file_path == rec_file_path) {
    Die("The DBM file and the record file must be different");
  }
  std::unique_ptr<File> rec_file = MakeFileOrDie(file_impl, 0, 0);
  Status status = rec_file->Open(rec_file_path, true);
  if (status != Status::SUCCESS) {
    EPrintL("Open failed: ", status);
    return 1;
  }
  if (rec_file->GetSizeSimple() > 0) {
    EPrintL("The record file is not empty");
    return 1;
  }
  std::unique_ptr<DBM> dbm = MakeDBMOrDie(dbm_impl, file_impl, file_path);
  if (!OpenDBM(dbm.get(), file_path, false, false, false, with_no_wait, with_no_lock,
               false, false, -1, -1, -1,
               -1, -1, "",
               -1, -1, -1, false,
               "")) {
    return 1;
  }
  bool ok = true;
  if (is_tsv) {
    if (keys_only) {
      status = tkrzw::ExportDBMKeysAsLines(dbm.get(), rec_file.get());
      if (status != Status::SUCCESS) {
        EPrintL("ExportDBMKeysAsLines failed: ", status);
        ok = false;
      }
    } else {
      status = tkrzw::ExportDBMRecordsToTSV(dbm.get(), rec_file.get(), with_escape);
      if (status != Status::SUCCESS) {
        EPrintL("ExportDBMRecordsToTSV failed: ", status);
        ok = false;
      }
    }
  } else {
    if (keys_only) {
      status = tkrzw::ExportDBMKeysToFlatRecords(dbm.get(), rec_file.get());
      if (status != Status::SUCCESS) {
        EPrintL("ExportDBMKeysToFlatRecords failed: ", status);
        ok = false;
      }
    } else {
      status = tkrzw::ExportDBMRecordsToFlatRecords(dbm.get(), rec_file.get());
      if (status != Status::SUCCESS) {
        EPrintL("ExportDBMRecordsToFlatRecords failed: ", status);
        ok = false;
      }
    }
  }
  if (!CloseDBM(dbm.get())) {
    return 1;
  }
  status = rec_file->Close();
  if (status != Status::SUCCESS) {
    EPrintL("Close failed: ", status);
    return 1;
  }
  return ok ? 0 : 1;
}

// Processes the import subcommand.
static int32_t ProcessImport(int32_t argc, const char** args) {
  const std::map<std::string, int32_t>& cmd_configs = {
    {"", 2}, {"--dbm", 1}, {"--file", 1}, {"--no_wait", 0}, {"--no_lock", 0},
    {"--sort_mem_size", 1}, {"--insert_in_order", 0},
    {"--params", 1},
    {"--tsv", 0}, {"--escape", 0},
  };
  std::map<std::string, std::vector<std::string>> cmd_args;
  std::string cmd_error;
  if (!ParseCommandArguments(argc, args, cmd_configs, &cmd_args, &cmd_error)) {
    EPrint("Invalid command: ", cmd_error, "\n\n");
    PrintUsageAndDie();
  }
  const std::string file_path = GetStringArgument(cmd_args, "", 0, "");
  const std::string rec_file_path = GetStringArgument(cmd_args, "", 1, "");
  const std::string dbm_impl = GetStringArgument(cmd_args, "--dbm", 0, "auto");
  const std::string file_impl = GetStringArgument(cmd_args, "--file", 0, "mmap-para");
  const bool with_no_wait = CheckMap(cmd_args, "--no_wait");
  const bool with_no_lock = CheckMap(cmd_args, "--no_lock");
  const int64_t sort_mem_size = GetIntegerArgument(cmd_args, "--sort_mem_size", 0, -1);
  const bool insert_in_order = CheckMap(cmd_args, "--insert_in_order");
  const std::string poly_params = GetStringArgument(cmd_args, "--params", 0, "");
  const bool is_tsv = CheckMap(cmd_args, "--tsv");
  const bool with_escape = CheckMap(cmd_args, "--escape");
  if (file_path.empty()) {
    Die("The DBM file path must be specified");
  }
  if (rec_file_path.empty()) {
    Die("The record file path must be specified");
  }
  if (file_path == rec_file_path) {
    Die("The DBM file and the record file must be different");
  }
  std::unique_ptr<File> rec_file = MakeFileOrDie(file_impl, 0, 0);
  Status status = rec_file->Open(rec_file_path, false);
  if (status != Status::SUCCESS) {
    EPrintL("Open failed: ", status);
    return 1;
  }
  std::unique_ptr<DBM> dbm = MakeDBMOrDie(dbm_impl, file_impl, file_path);
  if (!OpenDBM(dbm.get(), file_path, true, true, false, with_no_wait, with_no_lock,
               false, false, -1, -1, -1,
               -1, -1, "",
               -1, -1, sort_mem_size, insert_in_order,
               poly_params)) {
    return 1;
  }
  bool ok = true;
  if (is_tsv) {
    status = tkrzw::ImportDBMRecordsFromTSV(dbm.get(), rec_file.get(), with_escape);
    if (status != Status::SUCCESS) {
      EPrintL("ImportDBMRecordsFromTSV failed: ", status);
      ok = false;
    }
  } else {
    status = tkrzw::ImportDBMRecordsFromFlatRecords(dbm.get(), rec_file.get());
    if (status != Status::SUCCESS) {
      EPrintL("ExportDBMRecordsToFlatRecords failed: ", status);
      ok = false;
    }
  }
  if (!CloseDBM(dbm.get())) {
    return 1;
  }
  status = rec_file->Close();
  if (status != Status::SUCCESS) {
    EPrintL("Close failed: ", status);
    return 1;
  }
  return ok ? 0 : 1;
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
    if (std::strcmp(args[1], "create") == 0) {
      rv = tkrzw::ProcessCreate(argc - 1, args + 1);
    } else if (std::strcmp(args[1], "inspect") == 0) {
      rv = tkrzw::ProcessInspect(argc - 1, args + 1);
    } else if (std::strcmp(args[1], "get") == 0) {
      rv = tkrzw::ProcessGet(argc - 1, args + 1);
    } else if (std::strcmp(args[1], "set") == 0) {
      rv = tkrzw::ProcessSet(argc - 1, args + 1);
    } else if (std::strcmp(args[1], "remove") == 0) {
      rv = tkrzw::ProcessRemove(argc - 1, args + 1);
    } else if (std::strcmp(args[1], "list") == 0) {
      rv = tkrzw::ProcessList(argc - 1, args + 1);
    } else if (std::strcmp(args[1], "rebuild") == 0) {
      rv = tkrzw::ProcessRebuild(argc - 1, args + 1);
    } else if (std::strcmp(args[1], "restore") == 0) {
      rv = tkrzw::ProcessRestore(argc - 1, args + 1);
    } else if (std::strcmp(args[1], "merge") == 0) {
      rv = tkrzw::ProcessMerge(argc - 1, args + 1);
    } else if (std::strcmp(args[1], "export") == 0) {
      rv = tkrzw::ProcessExport(argc - 1, args + 1);
    } else if (std::strcmp(args[1], "import") == 0) {
      rv = tkrzw::ProcessImport(argc - 1, args + 1);
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
