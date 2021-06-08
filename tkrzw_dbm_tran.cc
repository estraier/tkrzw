/*************************************************************************************************
 * Consistency checker of DBM transaction
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
  P("  %s build [options] path\n", progname);
  P("    : Builds the database.\n");
  P("  %s check [options]\n", progname);
  P("    : Checks consistency of the database.\n");
  P("\n");
  P("Common options:\n");
  P("  --params str : Sets the parameters in \"key=value,key=value\" format.\n");
  P("  --incr num : The number of increments. (default: 3)\n");
  P("\n");
  P("Options for the build subcommand:\n");
  P("  --iter num : The number of iterations. (default: 10000)\n");
  P("  --threads num : The number of threads. (default: 1)\n");
  P("  --sync_freq num : Frequency of synchronization (default: 100).\n");
  P("  --sync_hard  : Synchronizes physically.\n");
  P("  --remove : Removes some records.\n");
  P("  --rebuild : Rebuilds the database occasionally.\n");
  P("  --abort : Aborts the process at the end.\n");
  P("\n");
  std::exit(1);
}

// Processes the build subcommand.
static int32_t ProcessBuild(int32_t argc, const char** args) {
  const std::map<std::string, int32_t>& cmd_configs = {
    {"", 1}, {"--params", 1}, {"--iter", 1}, {"--threads", 1}, {"--incr", 1},
    {"--sync_freq", 1}, {"--sync_hard", 0},
    {"--remove", 0}, {"--rebuild", 0}, {"--abort", 0},
  };
  std::map<std::string, std::vector<std::string>> cmd_args;
  std::string cmd_error;
  if (!ParseCommandArguments(argc, args, cmd_configs, &cmd_args, &cmd_error)) {
    EPrint("Invalid command: ", cmd_error, "\n\n");
    PrintUsageAndDie();
  }
  const std::string path = GetStringArgument(cmd_args, "", 0, "");
  const std::string poly_params = GetStringArgument(cmd_args, "--params", 0, "");
  const int32_t num_iterations = GetIntegerArgument(cmd_args, "--iter", 0, 10000);
  const int32_t num_threads = GetIntegerArgument(cmd_args, "--threads", 0, 1);
  const int32_t num_increments = GetIntegerArgument(cmd_args, "--incr", 0, 3);
  const int32_t sync_freq = GetIntegerArgument(cmd_args, "--sync_freq", 0, 100);
  const bool sync_hard = CheckMap(cmd_args, "--sync_hard");
  const bool with_rebuild = CheckMap(cmd_args, "--rebuild");
  const bool with_remove = CheckMap(cmd_args, "--remove");
  const bool with_abort = CheckMap(cmd_args, "--abort");
  if (num_iterations < 1) {
    Die("Invalid number of iterations");
  }
  if (num_threads < 1) {
    Die("Invalid number of threads");
  }
  if (num_increments < 1) {
    Die("Invalid number of increments");
  }
  const int64_t start_mem_rss = StrToInt(GetSystemInfo()["mem_rss"]);
  std::atomic_bool has_error(false);
  tkrzw::PolyDBM dbm;
  Status status = dbm.Open(path, true, File::OPEN_TRUNCATE);
  if (status != Status::SUCCESS) {
    PrintL("Open failed: ", status);
    has_error = true;
  }
  std::map<std::string, std::string> sync_params;
  if (dbm.GetInternalDBM()->GetType() == typeid(tkrzw::SkipDBM)) {
    sync_params["reducer"] = "totalbe";
  }
  const int32_t dot_mod = std::max(num_iterations / 1000, 1);
  const int32_t fold_mod = std::max(num_iterations / 20, 1);
  std::atomic_int32_t master_id(0);
  auto task = [&](int32_t id) {
    const uint32_t mt_seed = std::random_device()();
    std::mt19937 mt(mt_seed);
    std::uniform_int_distribution<int32_t> key_num_dist(0, num_iterations * num_threads - 1);
    std::uniform_int_distribution<int32_t> op_dist(0, INT32MAX);
    bool midline = false;
    for (int32_t i = 0; !has_error && i < num_iterations; i++) {
      const int32_t key_num = key_num_dist(mt);
      const std::string& key = SPrintF("%08d", key_num);
      if (with_remove && op_dist(mt) % 5 == 0) {
        const Status status = dbm.Remove(key);
        if (status != Status::SUCCESS && status != Status::NOT_FOUND_ERROR) {
          EPrintL("Remove failed: ", status);
          has_error = true;
        }
      } else {
        tkrzw::DBM::RecordProcessorIncrement proc(1, nullptr, 0);
        std::vector<std::pair<std::string_view, tkrzw::DBM::RecordProcessor*>> key_proc_pairs;
        for (int32_t j = 0; j < num_increments; j++) {
          key_proc_pairs.emplace_back(std::make_pair(key, &proc));
        }
        const Status status = dbm.ProcessMulti(key_proc_pairs, true);
        if (status != Status::SUCCESS) {
          EPrintL("ProcessMulti failed: ", status);
          has_error = true;
        }
      }
      if (sync_freq > 0 && i % sync_freq == 0) {
        const Status status = dbm.SynchronizeAdvanced(sync_hard, nullptr, sync_params);
        if (status != Status::SUCCESS) {
          EPrintL("Synchronize failed: ", status);
          has_error = true;
        }
      }
      if (with_rebuild && op_dist(mt) % (num_iterations / num_threads + 1) == 0 &&
          id == master_id.load()) {
        const Status status = dbm.Rebuild();
        if (status != Status::SUCCESS) {
          EPrintL("Rebuild failed: ", status);
          has_error = true;
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
    if (with_abort) {
      EPrintF("[ABORT:%d]\n", id);
      abort();
    }
  };
  PrintF("Building: num_iterations=%d num_threads=%d\n", num_iterations, num_threads);
  const double start_time = GetWallTime();
  std::vector<std::thread> threads;
  for (int32_t i = 0; i < num_threads; i++) {
    threads.emplace_back(std::thread(task, i));
  }
  for (auto& thread : threads) {
    thread.join();
  }
  const double end_time = GetWallTime();
  const double elapsed_time = end_time - start_time;
  const int64_t num_records = dbm.CountSimple();
  const int64_t mem_usage = StrToInt(GetSystemInfo()["mem_rss"]) - start_mem_rss;
  PrintF("Building done: elapsed_time=%.6f num_records=%lld qps=%.0f mem=%lld\n",
         elapsed_time, num_records, num_iterations * num_threads / elapsed_time,
         mem_usage);
  status = dbm.Close();
  if (status != tkrzw::Status::SUCCESS) {
    EPrintL("Close failed: ", status);
    has_error = true;
  }
  PrintL();
  return has_error ? 1 : 0;
}

// Processes the check subcommand.
static int32_t ProcessCheck(int32_t argc, const char** args) {
  const std::map<std::string, int32_t>& cmd_configs = {
    {"", 1}, {"--params", 1}, {"--incr", 1}
  };
  std::map<std::string, std::vector<std::string>> cmd_args;
  std::string cmd_error;
  if (!ParseCommandArguments(argc, args, cmd_configs, &cmd_args, &cmd_error)) {
    EPrint("Invalid command: ", cmd_error, "\n\n");
    PrintUsageAndDie();
  }
  const std::string path = GetStringArgument(cmd_args, "", 0, "");
  const std::string poly_params = GetStringArgument(cmd_args, "--params", 0, "");
  const int32_t num_increments = GetIntegerArgument(cmd_args, "--incr", 0, 3);
  bool has_error = false;
  tkrzw::PolyDBM dbm;
  Status status = dbm.Open(path, true, File::OPEN_DEFAULT);
  if (status != Status::SUCCESS) {
    PrintL("Open failed: ", status);
    has_error = true;
  }
  PrintL("Healthy: ", dbm.IsHealthy());
  bool restored = false;
  auto* hash_dbm = dynamic_cast<tkrzw::HashDBM*>(dbm.GetInternalDBM());
  if (hash_dbm != nullptr) {
    restored = hash_dbm->IsAutoRestored();
  }
  auto* tree_dbm = dynamic_cast<tkrzw::TreeDBM*>(dbm.GetInternalDBM());
  if (tree_dbm != nullptr) {
    restored = tree_dbm->IsAutoRestored();
  }
  auto* skip_dbm = dynamic_cast<tkrzw::SkipDBM*>(dbm.GetInternalDBM());
  if (skip_dbm != nullptr) {
    restored = skip_dbm->IsAutoRestored();
  }
  PrintL("Restored: ", restored);
  int64_t num_records = dbm.CountSimple();
  PrintL("Records: ", num_records);
  const int64_t log_freq = std::max<int64_t>(num_records / 25, 10);
  int64_t count = 0;
  auto iter = dbm.MakeIterator();
  status = iter->First();
  if (status != Status::SUCCESS) {
    EPrintL("Iterator::First failed: ", status);
    has_error = true;
  }
  while (!has_error) {
    std::string key, value;
    status = iter->Get(&key, &value);
    if (status != Status::SUCCESS) {
      if (status != Status::NOT_FOUND_ERROR) {
        EPrintL("Iterator::Next failed: ", status);
        has_error = true;
      }
      break;
    }
    count++;
    if (count % log_freq == 0) {
      PrintF("Checking %lld\n", count);
    }
    if (value.size() == sizeof(uint64_t)) {
      const uint64_t num_value = tkrzw::StrToIntBigEndian(value);
      if (num_value % static_cast<uint64_t>(num_increments != 0)) {
        EPrintF("Inconsistent value: %s: %llu", key.c_str(), num_value);
        has_error = true;
      }
    } else {
      EPrintF("Invalid value size: %s", key.c_str());
      has_error = true;
    }
    status = iter->Next();
    if (status != Status::SUCCESS) {
      EPrintL("Iterator::Next failed: ", status);
      has_error = true;
    }
  }
  status = dbm.Close();
  if (status != tkrzw::Status::SUCCESS) {
    EPrintL("Close failed: ", status);
    has_error = true;
  }
  if (!has_error) {
    PrintL("All OK");
  }
  PrintL();
  return has_error ? 1 : 0;
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
    if (std::strcmp(args[1], "build") == 0) {
      rv = tkrzw::ProcessBuild(argc - 1, args + 1);
    } else if (std::strcmp(args[1], "check") == 0) {
      rv = tkrzw::ProcessCheck(argc - 1, args + 1);
    } else {
      tkrzw::PrintUsageAndDie();
    }
  } catch (const std::runtime_error& e) {
    tkrzw::EPrintL(e.what());
    rv = 1;
  }
  return rv;
}

// END OF FILE
