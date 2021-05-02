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


std::map<std::string, std::string> GetSystemInfo() {
  std::map<std::string, std::string> info;
#if defined(_SYS_LINUX_)
  struct ::rusage rbuf;
  std::memset(&rbuf, 0, sizeof(rbuf));
  if (::getrusage(RUSAGE_SELF, &rbuf) == 0) {
    info["ru_utime"] = SPrintF("%0.6f",
                               rbuf.ru_utime.tv_sec + rbuf.ru_utime.tv_usec / 1000000.0);
    info["ru_stime"] = SPrintF("%0.6f",
                               rbuf.ru_stime.tv_sec + rbuf.ru_stime.tv_usec / 1000000.0);
    if (rbuf.ru_maxrss > 0) {
      int64_t size = rbuf.ru_maxrss * 1024LL;
      info["mem_peak"] = SPrintF("%lld", (long long)size);
      info["mem_size"] = SPrintF("%lld", (long long)size);
      info["mem_rss"] = SPrintF("%lld", (long long)size);
    }
  }
  std::ifstream ifs;
  ifs.open("/proc/self/status", std::ios_base::in | std::ios_base::binary);
  if (ifs) {
    std::string line;
    while (getline(ifs, line)) {
      size_t index = line.find(':');
      if (index != std::string::npos) {
        const std::string& name = line.substr(0, index);
        index++;
        while (index < line.size() && line[index] >= '\0' && line[index] <= ' ') {
          index++;
        }
        const std::string& value = line.substr(index);
        if (name == "VmPeak") {
          int64_t size = StrToIntMetric(value.c_str());
          if (size > 0) info["mem_peak"] = SPrintF("%lld", (long long)size);
        } else if (name == "VmSize") {
          int64_t size = StrToIntMetric(value.c_str());
          if (size > 0) info["mem_size"] = SPrintF("%lld", (long long)size);
        } else if (name == "VmRSS") {
          int64_t size = StrToIntMetric(value.c_str());
          if (size > 0) info["mem_rss"] = SPrintF("%lld", (long long)size);
        }
      }
    }
    ifs.close();
  }
  ifs.open("/proc/meminfo", std::ios_base::in | std::ios_base::binary);
  if (ifs) {
    std::string line;
    while (getline(ifs, line)) {
      size_t index = line.find(':');
      if (index != std::string::npos) {
        const std::string& name = line.substr(0, index);
        index++;
        while (index < line.size() && line[index] >= '\0' && line[index] <= ' ') {
          index++;
        }
        const std::string& value = line.substr(index);
        if (name == "MemTotal") {
          int64_t size = StrToIntMetric(value.c_str());
          if (size > 0) info["mem_total"] = SPrintF("%lld", (long long)size);
        } else if (name == "MemFree") {
          int64_t size = StrToIntMetric(value.c_str());
          if (size > 0) info["mem_free"] = SPrintF("%lld", (long long)size);
        } else if (name == "Cached") {
          int64_t size = StrToIntMetric(value.c_str());
          if (size > 0) info["mem_cached"] = SPrintF("%lld", (long long)size);
        }
      }
    }
    ifs.close();
  }
#elif defined(_SYS_MACOSX_)
  struct ::rusage rbuf;
  std::memset(&rbuf, 0, sizeof(rbuf));
  if (::getrusage(RUSAGE_SELF, &rbuf) == 0) {
    info["ru_utime"] = SPrintF("%0.6f",
                               rbuf.ru_utime.tv_sec + rbuf.ru_utime.tv_usec / 1000000.0);
    info["ru_stime"] = SPrintF("%0.6f",
                               rbuf.ru_stime.tv_sec + rbuf.ru_stime.tv_usec / 1000000.0);
    if (rbuf.ru_maxrss > 0) {
      int64_t size = rbuf.ru_maxrss;
      info["mem_peak"] = SPrintF("%lld", (long long)size);
      info["mem_size"] = SPrintF("%lld", (long long)size);
      info["mem_rss"] = SPrintF("%lld", (long long)size);
    }
  }
#elif defined(_SYS_FREEBSD_) || defined(_SYS_SUNOS_)
  struct ::rusage rbuf;
  std::memset(&rbuf, 0, sizeof(rbuf));
  if (::getrusage(RUSAGE_SELF, &rbuf) == 0) {
    info["ru_utime"] = SPrintF("%0.6f",
                               rbuf.ru_utime.tv_sec + rbuf.ru_utime.tv_usec / 1000000.0);
    info["ru_stime"] = SPrintF("%0.6f",
                               rbuf.ru_stime.tv_sec + rbuf.ru_stime.tv_usec / 1000000.0);
    if (rbuf.ru_maxrss > 0) {
      int64_t size = rbuf.ru_maxrss * 1024LL;
      info["mem_peak"] = SPrintF("%lld", (long long)size);
      info["mem_size"] = SPrintF("%lld", (long long)size);
      info["mem_rss"] = SPrintF("%lld", (long long)size);
    }
  }
#endif
  return info;
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

void LockMemoryOfFileOrDie(File* file, size_t size) {
  if (typeid(*file) == typeid(MemoryMapParallelFile)) {
    auto* mem_file = dynamic_cast<MemoryMapParallelFile*>(file);
    mem_file->LockMemory(size).OrDie();
  } else if (typeid(*file) == typeid(MemoryMapAtomicFile)) {
    auto* mem_file = dynamic_cast<MemoryMapAtomicFile*>(file);
    mem_file->LockMemory(size).OrDie();
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

}  // namespace tkrzw

// END OF FILE
