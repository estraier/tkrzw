/*************************************************************************************************
 * Command-line utilities
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

#include <cassert>
#include <cstdarg>
#include <cstdint>

#include <iostream>
#include <string>
#include <string_view>
#include <vector>

#include "tkrzw_cmd_util.h"

namespace tkrzw {

void PrintF(const char* format, ...) {
  assert(format != nullptr);
  std::string msg;
  va_list ap;
  va_start(ap, format);
  tkrzw::VSPrintF(&msg, format, ap);
  va_end(ap);
  std::cout << msg;
  std::cout.flush();
}

void PutChar(char c) {
  std::cout << c;
  std::cout.flush();
}

void EPrintF(const char* format, ...) {
  assert(format != nullptr);
  std::string msg;
  va_list ap;
  va_start(ap, format);
  tkrzw::VSPrintF(&msg, format, ap);
  va_end(ap);
  std::cerr << msg;
  std::cerr.flush();
}

void EPutChar(char c) {
  std::cerr << c;
  std::cerr.flush();
}

bool ParseCommandArguments(
    int32_t argc, const char** argv,
    const std::map<std::string, int32_t>& configs,
    std::map<std::string, std::vector<std::string>>* result,
    std::string* error_message) {
  assert(result != nullptr && error_message != nullptr);
  result->clear();
  error_message->clear();
  bool no_option = false;
  int32_t i = 1;
  while (i < argc) {
    const char* const arg = argv[i++];
    if (std::strcmp(arg, "--") == 0) {
      no_option = true;
    } else if (!no_option && arg[0] == '-') {
      const auto it = configs.find(arg);
      if (it == configs.end()) {
        *error_message = StrCat("invalid option: ", arg);
        return false;
      }
      const auto res_it = result->find(arg);
      if (res_it != result->end()) {
        *error_message = StrCat("duplicated option: ", arg);
        return false;
      }
      if (i + it->second > argc) {
        *error_message = StrCat("fewer arguments: ", arg);
        return false;
      }
      auto& opt_args = (*result)[arg];
      const int32_t end = i + it->second;
      while (i < end) {
        opt_args.emplace_back(argv[i++]);
      }
    } else {
      (*result)[""].emplace_back(arg);
    }
  }
  const int32_t arg_size = (*result)[""].size();
  const auto it = configs.find("");
  if (it != configs.end()) {
    if (arg_size < it->second) {
      *error_message = StrCat("too few arguments");
      return false;
    }
    if (arg_size > it->second) {
      *error_message = StrCat("too many arguments");
      return false;
    }
  }
  return true;
}

std::string GetStringArgument(
    const std::map<std::string, std::vector<std::string>>& args,
    const std::string& name, int32_t index, const std::string& default_value) {
  const auto& it = args.find(name);
  return (it != args.end() && index < static_cast<int32_t>(it->second.size())) ?
      it->second[index] : default_value;
}

int64_t GetIntegerArgument(
    const std::map<std::string, std::vector<std::string>>& args,
    const std::string& name, int32_t index, int64_t default_value) {
  const auto& it = args.find(name);
  return (it != args.end() && index < static_cast<int32_t>(it->second.size())) ?
      StrToIntMetric(it->second[index]) : default_value;
}

double GetDoubleArgument(
    const std::map<std::string, std::vector<std::string>>& args,
    const std::string& name, int32_t index, double default_value) {
  const auto& it = args.find(name);
  return (it != args.end() && index < static_cast<int32_t>(it->second.size())) ?
      StrToDouble(it->second[index]) : default_value;
}

std::unique_ptr<File> MakeFileOrDie(
    const std::string& impl_name, int64_t alloc_init_size, double alloc_inc_factor) {
  std::unique_ptr<File> file;
  if (impl_name == "std") {
    file = std::make_unique<StdFile>();
  } else if (impl_name == "mmap-para") {
    file = std::make_unique<MemoryMapParallelFile>();
  } else if (impl_name == "mmap-atom") {
    file = std::make_unique<MemoryMapAtomicFile>();
  } else if (impl_name == "pos-para") {
    file = std::make_unique<PositionalParallelFile>();
  } else if (impl_name == "pos-atom") {
    file = std::make_unique<PositionalAtomicFile>();
  } else {
    Die("Unknown File implementation: ", impl_name);
  }
  if (alloc_init_size > 0 && alloc_inc_factor > 0) {
    file->SetAllocationStrategy(alloc_init_size, alloc_inc_factor);
  }
  return file;
}

void SetAccessStrategyOrDie(File* file, int64_t block_size,
                            bool is_direct_io, bool is_sync_io, bool is_padding,
                            bool is_pagecache) {
  auto* pos_file = dynamic_cast<PositionalFile*>(file);;
  if (pos_file != nullptr) {
    int32_t options = PositionalFile::ACCESS_DEFAULT;
    if (is_direct_io) {
      options |= PositionalFile::ACCESS_DIRECT;
    }
    if (is_sync_io) {
      options |= PositionalFile::ACCESS_SYNC;
    }
    if (is_padding) {
      options |= PositionalFile::ACCESS_PADDING;
    }
    if (is_pagecache) {
      options |= PositionalFile::ACCESS_PAGECACHE;
    }
    pos_file->SetAccessStrategy(block_size, options).OrDie();
  }
}

void SetHeadBufferOfFileOrDie(File* file, int64_t size) {
  auto* pos_file = dynamic_cast<PositionalFile*>(file);;
  if (pos_file != nullptr) {
    pos_file->SetHeadBuffer(size).OrDie();
  }
}

void PrintDBMRecordsInTSV(DBM* dbm) {
  class Printer final : public DBM::RecordProcessor {
   public:
    std::string_view ProcessFull(std::string_view key, std::string_view value) override {
      PrintL(StrTrimForTSV(key), "\t", StrTrimForTSV(value));
      return NOOP;
    }
  } printer;
  dbm->ProcessEach(&printer, false);
}

std::string MakeCyclishText(size_t size, int32_t seed) {
  std::mt19937 mt(seed);
  std::uniform_int_distribution<int32_t> space_dist(0, 7);
  std::string text;
  text.reserve(size);
  for (size_t i = 0; i < size; i++) {
    if (!text.empty() && text.back() != ' ' && i < size - 1 && space_dist(mt) == 0) {
      text.append(1, ' ');
    } else {
      const int32_t mod = 26 - (i % 2);
      text.append(1, 'a' + (seed++) % mod);
    }
  }
  return text;
}

std::string MakeNaturalishText(size_t size, int32_t seed) {
  const char all_chars[] = "abcdefghijklmnopqrstuvwzyz";
  const char freq_chars[] = "aeiou";
  std::mt19937 mt(seed);
  std::uniform_int_distribution<int32_t> space_dist(0, 7);
  std::uniform_int_distribution<int32_t> mode_dist(0, 2);
  std::normal_distribution<double> norm_dist(std::size(all_chars) / 2.0, sizeof(all_chars) / 4.0);
  std::uniform_int_distribution<int32_t> all_dist(0, sizeof(all_chars) - 2);
  std::uniform_int_distribution<int32_t> freq_dist(0, sizeof(freq_chars) - 2);
  std::string text;
  text.reserve(size);
  for (size_t i = 0; i < size; i++) {
    if (!text.empty() && text.back() != ' ' && i < size - 1 && space_dist(mt) == 0) {
      text.append(1, ' ');
    } else {
      int32_t c = -1;
      switch (mode_dist(mt)) {
        default:
          while (c < 0 || c >= static_cast<int32_t>(sizeof(all_chars) - 2)) {
            c = norm_dist(mt);
          }
          text.append(1, all_chars[c]);
          break;
        case 1:
          text.append(1, freq_chars[freq_dist(mt)]);
          break;
        case 2:
          c = text.empty() || text.back() == ' ' ? all_chars[all_dist(mt)] : text.back();
          text.append(1, c);
          break;
      }
    }
  }
  return text;
}

}  // namespace tkrzw

// END OF FILE
