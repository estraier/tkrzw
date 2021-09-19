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
  P("  %s async [options]\n", progname);
  P("    : Checks asynchronous operations of the database.\n");
  P("\n");
  P("\n");
  P("Common options:\n");
  P("  --params str : Sets the parameters in \"key=value,key=value\" format.\n");
  P("\n");
  P("Options for the build subcommand:\n");
  P("  --incr num : The number of increments. (default: 3)\n");
  P("  --iter num : The number of iterations. (default: 10000)\n");
  P("  --threads num : The number of threads. (default: 1)\n");
  P("  --sync_freq num : Frequency of synchronization (default: 100).\n");
  P("  --sync_hard  : Synchronizes physically.\n");
  P("  --remove : Removes some records.\n");
  P("  --rebuild : Rebuilds the database occasionally.\n");
  P("  --async num : Uses the asynchronous API and sets the number of worker threads."
    " (default: 0)\n");
  P("  --abort : Aborts the process at the end.\n");
  P("\n");
  P("Options for the build subcommand:\n");
  P("  --incr num : The number of increments. (default: 3)\n");
  P("  --restore : Restores the database and validate it.\n");
  P("\n");
  P("Options for the async subcommand:\n");
  P("  --iter num : The number of iterations. (default: 10000)\n");
  P("  --threads num : The number of threads. (default: 1)\n");
  P("  --rebuild : Rebuilds the database occasionally.\n");
  P("  --async num : Uses the asynchronous API and sets the number of worker threads."
    " (default: 0)\n");
  P("  --wait_freq num : Frequency of waiting (default: 0).\n");
  P("  --random_key : Uses random keys rather than sequential ones.\n");
  P("  --set_only : Does only setting.\n");
  P("  --get_only : Does only getting.\n");
  P("  --remove_only : Does only removing.\n");
  P("\n");
  std::exit(1);
}

// Wrapper of RecordProcessorIncrement as RecordProcessor.
class Incrementor : public tkrzw::AsyncDBM::RecordProcessor {
 public:
  Incrementor(int64_t increment, int64_t* current, int64_t initial)
      : proc_(increment, current, initial) {}
  std::string_view ProcessFull(std::string_view key, std::string_view value) override {
    return proc_.ProcessFull(key, value);
  }
  std::string_view ProcessEmpty(std::string_view key) override {
    return proc_.ProcessEmpty(key);
  }
 private:
  tkrzw::DBM::RecordProcessorIncrement proc_;
};

// Processes the build subcommand.
static int32_t ProcessBuild(int32_t argc, const char** args) {
  const std::map<std::string, int32_t>& cmd_configs = {
    {"", 1}, {"--params", 1}, {"--iter", 1}, {"--threads", 1}, {"--incr", 1},
    {"--sync_freq", 1}, {"--sync_hard", 0},
    {"--remove", 0}, {"--rebuild", 0},
    {"--async", 1}, {"--abort", 0},
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
  const bool with_remove = CheckMap(cmd_args, "--remove");
  const bool with_rebuild = CheckMap(cmd_args, "--rebuild");
  const int32_t num_async_threads = GetIntegerArgument(cmd_args, "--async", 0, 0);
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
  const int64_t start_mem_rss = GetMemoryUsage();
  std::atomic_bool has_error(false);
  tkrzw::PolyDBM dbm;
  const std::map<std::string, std::string> tuning_params =
      tkrzw::StrSplitIntoMap(poly_params, ",", "=");
  Status status = dbm.OpenAdvanced(path, true, File::OPEN_TRUNCATE, tuning_params);
  if (status != Status::SUCCESS) {
    PrintL("Open failed: ", status);
    has_error = true;
  }
  std::unique_ptr<tkrzw::AsyncDBM> async(nullptr);
  if (num_async_threads > 0) {
    async = std::make_unique<tkrzw::AsyncDBM>(&dbm, num_async_threads);
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
    std::mt19937 mt(mt_seed + id);
    std::uniform_int_distribution<int32_t> key_num_dist(0, num_iterations * num_threads - 1);
    std::uniform_int_distribution<int32_t> op_dist(0, INT32MAX);
    bool midline = false;
    for (int32_t i = 0; !has_error && i < num_iterations; i++) {
      const int32_t key_num = key_num_dist(mt);
      const std::string& key = SPrintF("%08d", key_num);
      if (with_remove && op_dist(mt) % 5 == 0) {
        if (async == nullptr) {
          const Status status = dbm.Remove(key);
          if (status != Status::SUCCESS && status != Status::NOT_FOUND_ERROR) {
            EPrintL("Remove failed: ", status);
            has_error = true;
          }
        } else {
          const Status status = async->Remove(key).get();
          if (status != Status::SUCCESS && status != Status::NOT_FOUND_ERROR) {
            EPrintL("Remove failed: ", status);
            has_error = true;
          }
        }
      } else {
        if (async == nullptr) {
          Incrementor proc(1, nullptr, 0);
          std::vector<std::pair<std::string_view, tkrzw::DBM::RecordProcessor*>> key_proc_pairs;
          for (int32_t j = 0; j < num_increments; j++) {
            key_proc_pairs.emplace_back(std::make_pair(std::string_view(key), &proc));
          }
          const Status status = dbm.ProcessMulti(key_proc_pairs, true);
          if (status != Status::SUCCESS) {
            EPrintL("ProcessMulti failed: ", status);
            has_error = true;
          }
        } else {
          std::vector<std::pair<std::string_view,
                                std::shared_ptr<tkrzw::AsyncDBM::RecordProcessor>>>
              key_proc_pairs;
          for (int32_t j = 0; j < num_increments; j++) {
            key_proc_pairs.emplace_back(std::make_pair(
                std::string_view(key),
                std::make_unique<Incrementor>(1, nullptr, 0)));
          }
          const Status status = async->ProcessMulti(key_proc_pairs, true).get().first;
          if (status != Status::SUCCESS) {
            EPrintL("ProcessMulti failed: ", status);
            has_error = true;
          }
        }
      }
      if (sync_freq > 0 && i % sync_freq == 0) {
        if (async == nullptr) {
          const Status status = dbm.SynchronizeAdvanced(sync_hard, nullptr, sync_params);
          if (status != Status::SUCCESS) {
            EPrintL("Synchronize failed: ", status);
            has_error = true;
          }
        } else {
          const Status status = async->Synchronize(sync_hard).get();
          if (status != Status::SUCCESS) {
            EPrintL("Synchronize failed: ", status);
            has_error = true;
          }
        }
      }
      if (with_rebuild && op_dist(mt) % (num_iterations / 4 + 1) == 0 &&
          id == master_id.load()) {
        if (async == nullptr) {
          const Status status = dbm.Rebuild();
          if (status != Status::SUCCESS) {
            EPrintL("Rebuild failed: ", status);
            has_error = true;
          }
        } else {
          const Status status = async->Rebuild().get();
          if (status != Status::SUCCESS) {
            EPrintL("Rebuild failed: ", status);
            has_error = true;
          }
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
  async.reset(nullptr);
  status = dbm.SynchronizeAdvanced(sync_hard, nullptr, sync_params);
  if (status != Status::SUCCESS) {
    EPrintL("Synchronize failed: ", status);
    has_error = true;
  }
  const double end_time = GetWallTime();
  const double elapsed_time = end_time - start_time;
  const int64_t num_records = dbm.CountSimple();
  const int64_t mem_usage = GetMemoryUsage() - start_mem_rss;
  PrintF("Building done: elapsed_time=%.6f num_records=%lld qps=%.0f mem=%lld\n",
         elapsed_time, num_records, num_iterations * num_threads / elapsed_time,
         mem_usage);
  int64_t act_count = 0;
  auto iter = dbm.MakeIterator();
  if (iter->First() != Status::SUCCESS) {
    EPrintL("First failed: ", status);
    has_error = true;
  }
  while (true) {
    status = iter->Get();
    if (status != Status::SUCCESS) {
      if (status != Status::NOT_FOUND_ERROR) {
        EPrintL("Get failed: ", status);
        has_error = true;
      }
      break;
    }
    act_count++;
    if (iter->Next() != Status::SUCCESS) {
      EPrintL("Next failed: ", status);
      has_error = true;
    }
  }
  if (num_records != act_count) {
    PrintL("Inconsistent count: meta=", num_records, " vs actual=", act_count);
    has_error = true;
  }
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
    {"", 1}, {"--params", 1}, {"--incr", 1}, {"--restore", 0},
  };
  std::map<std::string, std::vector<std::string>> cmd_args;
  std::string cmd_error;
  if (!ParseCommandArguments(argc, args, cmd_configs, &cmd_args, &cmd_error)) {
    EPrint("Invalid command: ", cmd_error, "\n\n");
    PrintUsageAndDie();
  }
  std::string path = GetStringArgument(cmd_args, "", 0, "");
  const std::string poly_params = GetStringArgument(cmd_args, "--params", 0, "");
  const int32_t num_increments = GetIntegerArgument(cmd_args, "--incr", 0, 3);
  const bool with_restore = CheckMap(cmd_args, "--restore");
  bool has_error = false;
  std::string restored_path;
  if (with_restore) {
    const std::string base = PathToBaseName(path);
    const std::string ext = PathToExtension(path);
    restored_path = base + ".tmp.restore";
    if (!ext.empty()) {
      restored_path += "." + ext;
    }
    tkrzw::RemoveFile(restored_path);
    static tkrzw::Status status = tkrzw::PolyDBM::RestoreDatabase(path, restored_path);
    if (status != Status::SUCCESS) {
      PrintL("RestoreDatabase failed: ", status);
      has_error = true;
    }
  }
  tkrzw::PolyDBM dbm, restored_dbm;
  const std::map<std::string, std::string> tuning_params =
      tkrzw::StrSplitIntoMap(poly_params, ",", "=");
  Status status = dbm.OpenAdvanced(path, true, File::OPEN_DEFAULT, tuning_params);
  if (status != Status::SUCCESS) {
    PrintL("Open failed: ", status);
    has_error = true;
  }
  if (!restored_path.empty()) {
    status = restored_dbm.OpenAdvanced(restored_path, false, File::OPEN_DEFAULT, tuning_params);
    if (status != Status::SUCCESS) {
      PrintL("Open failed: ", status);
      has_error = true;
    }
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
  const int64_t num_records = dbm.CountSimple();
  PrintL("Records: ", num_records);
  if (restored_dbm.IsOpen()) {
    const int64_t restored_num_records = restored_dbm.CountSimple();
    if (restored_num_records != num_records) {
      PrintL("Inconsistent count: original=", num_records, " vs restored=", restored_num_records);
      has_error = true;
    }
  }
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
    if (restored_dbm.IsOpen()) {
      std::string restored_value;
      status = restored_dbm.Get(key, &restored_value);
      if (status != Status::SUCCESS) {
        EPrintL("Get failed: ", status, " key=", key);
        has_error = true;
      }
      if (restored_value != value) {
        EPrintL("Inconsistent value: key=", key);
        has_error = true;
      }
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
  if (restored_dbm.IsOpen()) {
    status = restored_dbm.Close();
    if (status != tkrzw::Status::SUCCESS) {
      EPrintL("Close failed: ", status);
      has_error = true;
    }
  }
  status = dbm.Close();
  if (status != tkrzw::Status::SUCCESS) {
    EPrintL("Close failed: ", status);
    has_error = true;
  }
  if (!restored_path.empty()) {
    status = tkrzw::RemoveFile(restored_path);
    if (status != tkrzw::Status::SUCCESS) {
      EPrintL("RemoveFile failed: ", status);
      has_error = true;
    }
  }
  if (!has_error) {
    PrintL("All OK");
  }
  PrintL();
  return has_error ? 1 : 0;
}

// Processes the async subcommand.
static int32_t ProcessAsync(int32_t argc, const char** args) {
  const std::map<std::string, int32_t>& cmd_configs = {
    {"", 1}, {"--params", 1}, {"--iter", 1}, {"--threads", 1},
    {"--rebuild", 0}, {"--async", 1}, {"--wait_freq", 1},
    {"--random_key", 0}, {"--set_only", 0}, {"--get_only", 0}, {"--remove_only", 0},
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
  const bool with_rebuild = CheckMap(cmd_args, "--rebuild");
  const int32_t num_async_threads = GetIntegerArgument(cmd_args, "--async", 0, 0);
  const int32_t wait_freq = GetIntegerArgument(cmd_args, "--wait_freq", 0, 0);
  const bool is_random_key = CheckMap(cmd_args, "--random_key");
  const bool is_get_only = CheckMap(cmd_args, "--get_only");
  const bool is_set_only = CheckMap(cmd_args, "--set_only");
  const bool is_remove_only = CheckMap(cmd_args, "--remove_only");
  if (num_iterations < 1) {
    Die("Invalid number of iterations");
  }
  if (num_threads < 1) {
    Die("Invalid number of threads");
  }
  const int64_t start_mem_rss = GetMemoryUsage();
  std::atomic_bool has_error(false);
  tkrzw::PolyDBM dbm;
  const std::map<std::string, std::string> tuning_params =
      tkrzw::StrSplitIntoMap(poly_params, ",", "=");
  const int32_t open_options =
      !is_get_only && !is_remove_only ? File::OPEN_TRUNCATE : File::OPEN_DEFAULT;
  Status status = dbm.OpenAdvanced(path, true, open_options, tuning_params);
  if (status != Status::SUCCESS) {
    PrintL("Open failed: ", status);
    has_error = true;
  }
  const int32_t dot_mod = std::max(num_iterations / 1000, 1);
  const int32_t fold_mod = std::max(num_iterations / 20, 1);
  auto setting_task = [&](int32_t id, tkrzw::AsyncDBM* async) {
    std::mt19937 key_mt(id);
    std::uniform_int_distribution<int32_t> key_num_dist(0, num_iterations * num_threads - 1);
    bool midline = false;
    std::vector<std::future<Status>> futures;
    if (wait_freq > 0) {
      futures.reserve(wait_freq);
    }
    const int32_t rebuild_pos = id == 0 && with_rebuild ? num_iterations / 4 : -1;
    std::future<Status> rebuild_future;
    for (int32_t i = 0; !has_error && i < num_iterations; i++) {
      const int32_t key_num = is_random_key ? key_num_dist(key_mt) : i * num_threads + id;
      const std::string& key = SPrintF("%08d", key_num);
      const std::string& value = SPrintF(
          "%08llX", static_cast<unsigned long long>(key_num) *  key_num);
      if (async == nullptr) {
        const Status status = dbm.Set(key, value);
        if (status != Status::SUCCESS) {
          EPrintL("Set failed: ", status);
          has_error = true;
          break;
        }
      } else {
        futures.emplace_back(async->Set(key, value));
        if (wait_freq > 0 && static_cast<int32_t>(futures.size()) >= wait_freq) {
          for (auto& future : futures) {
            const Status status = future.get();
            if (status != Status::SUCCESS) {
              EPrintL("Set failed: ", status);
              has_error = true;
              break;
            }
          }
          futures.clear();
          futures.reserve(wait_freq);
        }
      }
      if (i == rebuild_pos) {
        if (async == nullptr) {
          const Status status = dbm.Rebuild();
          if (status != Status::SUCCESS) {
            EPrintL("Rebuild failed: ", status);
            has_error = true;
            break;
          }
        } else{
          rebuild_future = async->Rebuild();
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
    if (rebuild_future.valid()) {
      const Status status = rebuild_future.get();
      if (status != Status::SUCCESS) {
        EPrintL("Rebuild failed: ", status);
        has_error = true;
      }
    }
  };
  if (!is_get_only && !is_remove_only) {
    PrintF("Setting: num_iterations=%d num_threads=%d\n", num_iterations, num_threads);
    const double start_time = GetWallTime();
    std::unique_ptr<tkrzw::AsyncDBM> async(nullptr);
    if (num_async_threads > 0) {
      async = std::make_unique<tkrzw::AsyncDBM>(&dbm, num_async_threads);
    }
    std::vector<std::thread> threads;
    for (int32_t i = 0; i < num_threads; i++) {
      threads.emplace_back(std::thread(setting_task, i, async.get()));
    }
    for (auto& thread : threads) {
      thread.join();
    }
    if (async != nullptr) {
      PrintF("Destroying AsyncDBM (tasks=%d): ... ", async->GetTaskQueue()->GetSize());
      const double async_del_start_time = GetWallTime();
      async.reset(nullptr);
      const double async_del_end_time = GetWallTime();
      PrintF("done (elapsed=%.6f)\n", async_del_end_time - async_del_start_time);
    }
    Print("Synchronizing: ... ");
    const double sync_start_time = GetWallTime();
    status = dbm.Synchronize(false);
    if (status != Status::SUCCESS) {
      EPrintL("Synchhronize failed: ", status);
      has_error = true;
    }
    const double sync_end_time = GetWallTime();
    PrintF("done (elapsed=%.6f)\n", sync_end_time - sync_start_time);
    const double end_time = GetWallTime();
    const double elapsed_time = end_time - start_time;
    const int64_t num_records = dbm.CountSimple();
    const int64_t mem_usage = GetMemoryUsage() - start_mem_rss;
    PrintF("Setting done: elapsed_time=%.6f num_records=%lld qps=%.0f mem=%lld\n",
           elapsed_time, num_records, num_iterations * num_threads / elapsed_time,
           mem_usage);
    PrintL();
  }
  auto getting_task = [&](int32_t id, tkrzw::AsyncDBM* async) {
    std::mt19937 key_mt(id);
    std::uniform_int_distribution<int32_t> key_num_dist(0, num_iterations * num_threads - 1);
    bool midline = false;
    std::vector<std::future<std::pair<Status, std::string>>> futures;
    if (wait_freq > 0) {
      futures.reserve(wait_freq);
    }
    const int32_t rebuild_pos = id == 0 && with_rebuild ? num_iterations / 4 : -1;
    std::future<Status> rebuild_future;
    for (int32_t i = 0; !has_error && i < num_iterations; i++) {
      const int32_t key_num = is_random_key ? key_num_dist(key_mt) : i * num_threads + id;
      const std::string& key = SPrintF("%08d", key_num);
      if (async == nullptr) {
        std::string value;
        const Status status = dbm.Get(key, &value);
        if (status != Status::SUCCESS && status != Status::NOT_FOUND_ERROR) {
          EPrintL("Get failed: ", status);
          has_error = true;
          break;
        }
      } else {
        futures.emplace_back(async->Get(key));
        if (wait_freq > 0 && static_cast<int32_t>(futures.size()) >= wait_freq) {
          for (auto& future : futures) {
            const Status status = future.get().first;
            if (status != Status::SUCCESS && status != Status::NOT_FOUND_ERROR) {
              EPrintL("Get failed: ", status);
              has_error = true;
              break;
            }
          }
          futures.clear();
          futures.reserve(wait_freq);
        }
      }
      if (i == rebuild_pos) {
        if (async == nullptr) {
          const Status status = dbm.Rebuild();
          if (status != Status::SUCCESS) {
            EPrintL("Rebuild failed: ", status);
            has_error = true;
            break;
          }
        } else{
          rebuild_future = async->Rebuild();
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
    if (rebuild_future.valid()) {
      const Status status = rebuild_future.get();
      if (status != Status::SUCCESS) {
        EPrintL("Rebuild failed: ", status);
        has_error = true;
      }
    }
  };
  if (!is_set_only && !is_remove_only) {
    PrintF("Getting: num_iterations=%d num_threads=%d\n", num_iterations, num_threads);
    const double start_time = GetWallTime();
    std::unique_ptr<tkrzw::AsyncDBM> async(nullptr);
    if (num_async_threads > 0) {
      async = std::make_unique<tkrzw::AsyncDBM>(&dbm, num_async_threads);
    }
    std::vector<std::thread> threads;
    for (int32_t i = 0; i < num_threads; i++) {
      threads.emplace_back(std::thread(getting_task, i, async.get()));
    }
    for (auto& thread : threads) {
      thread.join();
    }
    if (async != nullptr) {
      PrintF("Destroying AsyncDBM (tasks=%d): ... ", async->GetTaskQueue()->GetSize());
      const double async_del_start_time = GetWallTime();
      async.reset(nullptr);
      const double async_del_end_time = GetWallTime();
      PrintF("done (elapsed=%.6f)\n", async_del_end_time - async_del_start_time);
    }
    const double end_time = GetWallTime();
    const double elapsed_time = end_time - start_time;
    const int64_t num_records = dbm.CountSimple();
    const int64_t mem_usage = GetMemoryUsage() - start_mem_rss;
    PrintF("Getting done: elapsed_time=%.6f num_records=%lld qps=%.0f mem=%lld\n",
           elapsed_time, num_records, num_iterations * num_threads / elapsed_time,
           mem_usage);
    PrintL();
  }
  auto removing_task = [&](int32_t id, tkrzw::AsyncDBM* async) {
    std::mt19937 key_mt(id);
    std::uniform_int_distribution<int32_t> key_num_dist(0, num_iterations * num_threads - 1);
    bool midline = false;
    std::vector<std::future<Status>> futures;
    if (wait_freq > 0) {
      futures.reserve(wait_freq);
    }
    const int32_t rebuild_pos = id == 0 && with_rebuild ? num_iterations / 4 : -1;
    std::future<Status> rebuild_future;
    for (int32_t i = 0; !has_error && i < num_iterations; i++) {
      const int32_t key_num = is_random_key ? key_num_dist(key_mt) : i * num_threads + id;
      const std::string& key = SPrintF("%08d", key_num);
      if (async == nullptr) {
        const Status status = dbm.Remove(key);
        if (status != Status::SUCCESS && status != Status::NOT_FOUND_ERROR) {
          EPrintL("Remove failed: ", status);
          has_error = true;
          break;
        }
      } else {
        futures.emplace_back(async->Remove(key));
        if (wait_freq > 0 && static_cast<int32_t>(futures.size()) >= wait_freq) {
          for (auto& future : futures) {
            const Status status = future.get();
            if (status != Status::SUCCESS && status != Status::NOT_FOUND_ERROR) {
              EPrintL("Remove failed: ", status);
              has_error = true;
              break;
            }
          }
          futures.clear();
          futures.reserve(wait_freq);
        }
      }
      if (i == rebuild_pos) {
        if (async == nullptr) {
          const Status status = dbm.Rebuild();
          if (status != Status::SUCCESS) {
            EPrintL("Rebuild failed: ", status);
            has_error = true;
            break;
          }
        } else{
          rebuild_future = async->Rebuild();
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
    if (rebuild_future.valid()) {
      const Status status = rebuild_future.get();
      if (status != Status::SUCCESS) {
        EPrintL("Rebuild failed: ", status);
        has_error = true;
      }
    }
  };
  if (!is_set_only && !is_get_only) {
    PrintF("Removing: num_iterations=%d num_threads=%d\n", num_iterations, num_threads);
    const double start_time = GetWallTime();
    std::unique_ptr<tkrzw::AsyncDBM> async(nullptr);
    if (num_async_threads > 0) {
      async = std::make_unique<tkrzw::AsyncDBM>(&dbm, num_async_threads);
    }
    std::vector<std::thread> threads;
    for (int32_t i = 0; i < num_threads; i++) {
      threads.emplace_back(std::thread(removing_task, i, async.get()));
    }
    for (auto& thread : threads) {
      thread.join();
    }
    if (async != nullptr) {
      PrintF("Destroying AsyncDBM (tasks=%d): ... ", async->GetTaskQueue()->GetSize());
      const double async_del_start_time = GetWallTime();
      async.reset(nullptr);
      const double async_del_end_time = GetWallTime();
      PrintF("done (elapsed=%.6f)\n", async_del_end_time - async_del_start_time);
    }
    Print("Synchronizing: ... ");
    const double sync_start_time = GetWallTime();
    status = dbm.Synchronize(false);
    if (status != Status::SUCCESS) {
      EPrintL("Synchhronize failed: ", status);
      has_error = true;
    }
    const double sync_end_time = GetWallTime();
    PrintF("done (elapsed=%.6f)\n", sync_end_time - sync_start_time);
    const double end_time = GetWallTime();
    const double elapsed_time = end_time - start_time;
    const int64_t num_records = dbm.CountSimple();
    const int64_t mem_usage = GetMemoryUsage() - start_mem_rss;
    PrintF("Removing done: elapsed_time=%.6f num_records=%lld qps=%.0f mem=%lld\n",
           elapsed_time, num_records, num_iterations * num_threads / elapsed_time,
           mem_usage);
    PrintL();
  }
  status = dbm.Close();
  if (status != tkrzw::Status::SUCCESS) {
    EPrintL("Close failed: ", status);
    has_error = true;
  }
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
    } else if (std::strcmp(args[1], "async") == 0) {
      rv = tkrzw::ProcessAsync(argc - 1, args + 1);
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
