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
  const char* progname = "tkrzw_file_perf";
  P("%s: Performance checker of file implementations of Tkrzw\n", progname);
  P("\n");
  P("Usage:\n");
  P("  %s sequence [options] path\n", progname);
  P("    : Checks writing/reading performance in sequence.\n");
  P("  %s wicked [options] path\n", progname);
  P("    : Checks consistency with various operations.\n");
  P("\n");
  P("Common options:\n");
  P("  --file impl : The name of a file implementation:"
    " std, mmap-para, mmap-atom, pos-para, pos-atom. (default: mmap-para)\n");
  P("  --iter num : The number of iterations. (default: 10000)\n");
  P("  --size num : The size of each record. (default: 100)\n");
  P("  --threads num : The number of threads. (default: 1)\n");
  P("  --random_seed num : The random seed or nagative for real RNG. (default: 0)\n");
  P("  --alloc_init num : The initial allocation size. (default: %lld)\n",
    File::DEFAULT_ALLOC_INIT_SIZE);
  P("  --alloc_inc num : The allocation increment factor. (default: %.1f)\n",
    File::DEFAULT_ALLOC_INC_FACTOR);
  P("  --block_size num : The block size of the positional access file. (default: 1)\n");
  P("  --head_buffer num : The head buffer size of the positional access file. (default: 0)\n");
  P("  --direct_io : Enables the direct I/O option of the positional access file.\n");
  P("  --sync_io : Enables the synchronous I/O option of the positional access file.\n");
  P("  --padding : Enables padding at the end of the file.\n");
  P("  --pagecache : Enables the mini page cache in the process.\n");
  P("\n");
  P("Options for the sequence subcommand:\n");
  P("  --random : Uses random offsets rather than pre-defined ones.\n");
  P("  --append : Uses appending offsets rather than pre-defined ones.\n");
  P("  --write_only : Does only writing.\n");
  P("  --read_only : Does only reading.\n");
  P("\n");
  std::exit(1);
}

// Processes the sequence subcommand.
static int32_t ProcessSequence(int32_t argc, const char** args) {
  const std::map<std::string, int32_t>& cmd_configs = {
    {"", 1}, {"--file", 1}, {"--iter", 1}, {"--size", 1}, {"--threads", 1}, {"--random_seed", 1},
    {"--alloc_init", 1}, {"--alloc_inc", 1},
    {"--block_size", 1}, {"--head_buffer", 1},
    {"--direct_io", 0}, {"--sync_io", 0}, {"--padding", 0}, {"--pagecache", 0},
    {"--random", 0}, {"--append", 0}, {"--write_only", 0}, {"--read_only", 0},
  };
  std::map<std::string, std::vector<std::string>> cmd_args;
  std::string cmd_error;
  if (!ParseCommandArguments(argc, args, cmd_configs, &cmd_args, &cmd_error)) {
    EPrint("Invalid command: ", cmd_error, "\n\n");
    PrintUsageAndDie();
  }
  const std::string path = GetStringArgument(cmd_args, "", 0, "");
  const std::string file_impl = GetStringArgument(cmd_args, "--file", 0, "mmap-para");
  const int32_t num_iterations = GetIntegerArgument(cmd_args, "--iter", 0, 10000);
  const int32_t record_size = GetIntegerArgument(cmd_args, "--size", 0, 100);
  const int32_t num_threads = GetIntegerArgument(cmd_args, "--threads", 0, 1);
  const int32_t random_seed = GetIntegerArgument(cmd_args, "--random_seed", 0, 0);
  const int32_t alloc_init_size = GetIntegerArgument(
      cmd_args, "--alloc_init", 0, File::DEFAULT_ALLOC_INIT_SIZE);
  const double alloc_increment = GetDoubleArgument(
      cmd_args, "--alloc_inc", 0, File::DEFAULT_ALLOC_INC_FACTOR);
  const int64_t block_size = GetIntegerArgument(cmd_args, "--block_size", 0, 1);
  const int64_t head_buffer_size = GetIntegerArgument(cmd_args, "--head_buffer", 0, 0);
  const bool is_direct_io = CheckMap(cmd_args, "--direct_io");
  const bool is_sync_io = CheckMap(cmd_args, "--sync_io");
  const bool is_padding = CheckMap(cmd_args, "--padding");
  const bool is_pagecache = CheckMap(cmd_args, "--pagecache");
  const bool is_random = CheckMap(cmd_args, "--random");
  const bool is_append = CheckMap(cmd_args, "--append");
  const bool is_write_only = CheckMap(cmd_args, "--write_only");
  const bool is_read_only = CheckMap(cmd_args, "--read_only");
  auto file = MakeFileOrDie(file_impl, alloc_init_size, alloc_increment);
  SetAccessStrategyOrDie(
      file.get(), block_size, is_direct_io, is_sync_io, is_padding, is_pagecache);
  if (path.empty()) {
    Die("The file path must be specified");
  }
  if (num_iterations < 1) {
    Die("Invalid number of iterations");
  }
  if (record_size < 1) {
    Die("Invalid size of a record");
  }
  if (num_threads < 1) {
    Die("Invalid number of threads");
  }
  std::atomic_bool has_error(false);
  const int64_t est_file_size = num_iterations * num_threads * record_size;
  const int32_t dot_mod = std::max(num_iterations / 1000, 1);
  const int32_t fold_mod = std::max(num_iterations / 20, 1);
  auto writing_task = [&](int32_t id) {
    const uint32_t mt_seed = random_seed >= 0 ? random_seed : std::random_device()();
    std::mt19937 mt(mt_seed + id);
    std::uniform_int_distribution<int64_t> off_dist(0, est_file_size - record_size);
    char* write_buf = new char[record_size];
    std::memset(write_buf, '0' + id, record_size);
    bool midline = false;
    for (int32_t i = 0; !has_error && i < num_iterations; i++) {
      if (is_append) {
        const Status status = file->Append(write_buf, record_size);
        if (status != Status::SUCCESS) {
          PrintL("Append failed: ", status);
          has_error = true;
          break;
        }
      } else {
        int64_t off = 0;
        if (is_random) {
          off = off_dist(mt);
        } else {
          off = (static_cast<int64_t>(i) * num_threads + id) * record_size;
        }
        const Status status = file->Write(off, write_buf, record_size);
        if (status != Status::SUCCESS) {
          PrintL("Write failed: ", status);
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
    delete[] write_buf;
  };
  if (!is_read_only) {
    Status status = file->Open(path, true, File::OPEN_TRUNCATE);
    if (status != Status::SUCCESS) {
      PrintL("Open failed: ", status);
      has_error = true;
    }
    if (head_buffer_size > 0) {
      SetHeadBufferOfFileOrDie(file.get(), head_buffer_size);
    }
    PrintF("Writing: path=%s num_iteratins=%d record_size=%d num_threads=%d\n",
           path.c_str(), num_iterations, record_size, num_threads);
    const double start_time = GetWallTime();
    std::vector<std::thread> threads;
    for (int32_t i = 0; i < num_threads; i++) {
      threads.emplace_back(std::thread(writing_task, i));
    }
    for (auto& thread : threads) {
      thread.join();
    }
    const double end_time = GetWallTime();
    const double elapsed_time = end_time - start_time;
    int64_t file_size = file->GetSizeSimple();
    PrintF("Writing done: elapsed_time=%.6f file_size=%lld qps=%.0f\n",
           elapsed_time, file_size, num_iterations * num_threads / elapsed_time);
    status = file->Close();
    if (status != Status::SUCCESS) {
      PrintL("Close failed: ", status);
      has_error = true;
    }
    PrintL();
  }
  auto reading_task = [&](int32_t id) {
    const uint32_t mt_seed = random_seed >= 0 ? random_seed : std::random_device()();
    std::mt19937 mt(mt_seed + id);
    std::uniform_int_distribution<int64_t> off_dist(0, est_file_size - record_size);
    char* read_buf = new char[record_size];
    bool midline = false;
    for (int32_t i = 0; !has_error && i < num_iterations; i++) {
      int64_t off = 0;
      if (is_random) {
        off = off_dist(mt);
      } else {
        off = (static_cast<int64_t>(i) * num_threads + id) * record_size;
      }
      const Status status = file->Read(off, read_buf, record_size);
      if (status != Status::SUCCESS && !(is_random && status == Status::INFEASIBLE_ERROR)) {
        PrintL("Read failed: ", status);
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
    delete[] read_buf;
  };
  if (!is_write_only) {
    Status status = file->Open(path, false);
    if (status != Status::SUCCESS) {
      PrintL("Open failed: ", status);
      has_error = true;
    }
    if (head_buffer_size > 0) {
      SetHeadBufferOfFileOrDie(file.get(), head_buffer_size);
    }
    PrintF("Reading: path=%s num_iteratins=%d record_size=%d num_threads=%d\n",
           path.c_str(), num_iterations, record_size, num_threads);
    const double start_time = GetWallTime();
    std::vector<std::thread> threads;
    for (int32_t i = 0; i < num_threads; i++) {
      threads.emplace_back(std::thread(reading_task, i));
    }
    for (auto& thread : threads) {
      thread.join();
    }
    const double end_time = GetWallTime();
    const double elapsed_time = end_time - start_time;
    size_t file_size = file->GetSizeSimple();
    PrintF("Reading done: elapsed_time=%.6f file_size=%lld qps=%.0f\n",
           elapsed_time, file_size, num_iterations * num_threads / elapsed_time);
    status = file->Close();
    if (status != Status::SUCCESS) {
      PrintL("Close failed: ", status);
      has_error = true;
    }
    PrintL();
  }
  return has_error ? 1 : 0;
}

// Processes the wicked subcommand.
static int32_t ProcessWicked(int32_t argc, const char** args) {
  const std::map<std::string, int32_t>& cmd_configs = {
    {"", 1}, {"--file", 1}, {"--iter", 1}, {"--size", 1}, {"--threads", 1}, {"--random_seed", 1},
    {"--alloc_init", 1}, {"--alloc_inc", 1},
    {"--block_size", 1}, {"--head_buffer", 1},
    {"--direct_io", 0}, {"--sync_io", 0}, {"--padding", 0}, {"--pagecache", 0},
  };
  std::map<std::string, std::vector<std::string>> cmd_args;
  std::string cmd_error;
  if (!ParseCommandArguments(argc, args, cmd_configs, &cmd_args, &cmd_error)) {
    EPrint("Invalid command: ", cmd_error, "\n\n");
    PrintUsageAndDie();
  }
  const std::string path = GetStringArgument(cmd_args, "", 0, "");
  const std::string file_impl = GetStringArgument(cmd_args, "--file", 0, "mmap-para");
  const int32_t num_iterations = GetIntegerArgument(cmd_args, "--iter", 0, 10000);
  const int32_t record_size = GetIntegerArgument(cmd_args, "--size", 0, 100);
  const int32_t num_threads = GetIntegerArgument(cmd_args, "--threads", 0, 1);
  const int32_t random_seed = GetIntegerArgument(cmd_args, "--random_seed", 0, 0);
  const int32_t alloc_init_size = GetIntegerArgument(
      cmd_args, "--alloc_init", 0, File::DEFAULT_ALLOC_INIT_SIZE);
  const double alloc_increment = GetDoubleArgument(
      cmd_args, "--alloc_inc", 0, File::DEFAULT_ALLOC_INC_FACTOR);
  const int64_t block_size = GetIntegerArgument(cmd_args, "--block_size", 0, 1);
  const int64_t head_buffer_size = GetIntegerArgument(cmd_args, "--head_buffer", 0, 0);
  const bool is_direct_io = CheckMap(cmd_args, "--direct_io");
  const bool is_sync_io = CheckMap(cmd_args, "--sync_io");
  const bool is_padding = CheckMap(cmd_args, "--padding");
  const bool is_pagecache = CheckMap(cmd_args, "--pagecache");
  auto file = MakeFileOrDie(file_impl, alloc_init_size, alloc_increment);
  SetAccessStrategyOrDie(
      file.get(), block_size, is_direct_io, is_sync_io, is_padding, is_pagecache);
  if (path.empty()) {
    Die("The file path must be specified");
  }
  if (num_iterations < 1) {
    Die("Invalid number of iterations");
  }
  if (record_size < 1) {
    Die("Invalid size of a record");
  }
  if (num_threads < 1) {
    Die("Invalid number of threads");
  }
  std::atomic_bool has_error(false);
  const int64_t est_file_size = num_iterations * record_size;
  const int32_t dot_mod = std::max(num_iterations / 1000, 1);
  const int32_t fold_mod = std::max(num_iterations / 20, 1);
  std::mutex file_mutex;
  auto task = [&](int32_t id) {
    const uint32_t mt_seed = random_seed >= 0 ? random_seed : std::random_device()();
    std::mt19937 mt(mt_seed + id);
    std::uniform_int_distribution<int64_t> off_dist(0, est_file_size - record_size);
    std::uniform_int_distribution<int32_t> op_dist(0, INT32MAX);
    char* write_buf = new char[record_size];
    std::memset(write_buf, '0' + id, record_size);
    char* read_buf = new char[record_size];
    std::memset(read_buf, 'x', record_size);
    bool midline = false;
    for (int32_t i = 0; !has_error && i < num_iterations; i++) {
      const int64_t off = off_dist(mt);
      if (id == 0 && file->IsAtomic() && op_dist(mt) % 1000 == 0) {
        const Status status = file->Truncate(off_dist(mt));
        if (status != Status::SUCCESS) {
          PrintL("Truncate failed: ", status);
          has_error = true;
          break;
        }
      } else if (op_dist(mt) % 20 == 0) {
        const Status status = file->Expand(record_size);
        if (status != Status::SUCCESS) {
          PrintL("Expand failed: ", status);
          has_error = true;
          break;
        }
      } else if (op_dist(mt) % 10 == 0) {
        const Status status = file->Append(write_buf, record_size);
        if (status != Status::SUCCESS) {
          PrintL("Append failed: ", status);
          has_error = true;
          break;
        }
      } else if (op_dist(mt) % 3 == 0) {
        const Status status = file->Write(off, write_buf, record_size);
        if (status != Status::SUCCESS) {
          PrintL("Write failed: ", status);
          has_error = true;
          break;
        }
      } else {
        const Status status = file->Read(off, read_buf, record_size);
        if (status != Status::SUCCESS && status != Status::INFEASIBLE_ERROR) {
          PrintL("Read failed: ", status);
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
    delete[] read_buf;
    delete[] write_buf;
  };
  Status status = file->Open(path, true, File::OPEN_TRUNCATE);
  if (status != Status::SUCCESS) {
    PrintL("Open failed: ", status);
    has_error = true;
  }
  if (head_buffer_size > 0) {
    SetHeadBufferOfFileOrDie(file.get(), head_buffer_size);
  }
  PrintF("Doing: path=%s num_iteratins=%d record_size=%d num_threads=%d\n",
         path.c_str(), num_iterations, record_size, num_threads);
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
  int64_t file_size = file->GetSizeSimple();
  PrintF("Done: elapsed_time=%.6f file_size=%lld qps=%.0f\n",
         elapsed_time, file_size, num_iterations * num_threads / elapsed_time);
  status = file->Close();
  if (status != Status::SUCCESS) {
    PrintL("Close failed: ", status);
    has_error = true;
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
    if (std::strcmp(args[1], "sequence") == 0) {
      rv = tkrzw::ProcessSequence(argc - 1, args + 1);
    } else if (std::strcmp(args[1], "wicked") == 0) {
      rv = tkrzw::ProcessWicked(argc - 1, args + 1);
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
