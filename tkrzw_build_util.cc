/*************************************************************************************************
 * Command line interface of miscellaneous utilities
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

#include "tkrzw_cmd_util.h"

namespace tkrzw {

// Prints the usage to the standard error and die.
static void PrintUsageAndDie() {
  auto P = EPrintF;
  const char* progname = "tkrzw_build_util";
  P("%s: Build utilities of Tkrzw\n", progname);
  P("\n");
  P("Usage:\n");
  P("  %s config [options]\n", progname);
  P("    : Prints configurations.\n");
  P("  %s version\n", progname);
  P("    : Prints the version information.\n");
  P("\n");
  P("Options of the config subcommand:\n");
  P("  -v : Prints the version number.\n");
  P("  -i : Prints C++ preprocessor options for build.\n");
  P("  -l : Prints linker options for build.\n");
  P("  -p : Prints the prefix for installation.\n");
  P("\n");
  std::exit(1);
}

// Processes the config subcommand.
static int32_t ProcessConfig(int32_t argc, const char** args) {
  const std::map<std::string, int32_t>& cmd_configs = {
    {"-v", 0}, {"-i", 0}, {"-l", 0}, {"-p", 0}, {"", 0},
  };
  std::map<std::string, std::vector<std::string>> cmd_args;
  std::string cmd_error;
  if (!ParseCommandArguments(argc, args, cmd_configs, &cmd_args, &cmd_error)) {
    EPrint("Invalid command: ", cmd_error, "\n\n");
    PrintUsageAndDie();
  }
  if (CheckMap(cmd_args, "-v")) {
    PrintF("%s\n", PACKAGE_VERSION);
  } else if (CheckMap(cmd_args, "-i")) {
    PrintF("%s\n", _TKRZW_APPINC);
  } else if (CheckMap(cmd_args, "-l")) {
    PrintF("%s\n", _TKRZW_APPLIBS);
  } else if (CheckMap(cmd_args, "-p")) {
    PrintF("%s\n", _TKRZW_BINDIR);
  } else {
    PrintF("PACKAGE_VERSION: %s\n", PACKAGE_VERSION);
    PrintF("LIBRARY_VERSION: %s\n", LIBRARY_VERSION);
    PrintF("OS_NAME: %s\n", OS_NAME);
    PrintF("IS_BIG_ENDIAN: %d\n", IS_BIG_ENDIAN);
    PrintF("PAGE_SIZE: %d\n", PAGE_SIZE);
    PrintF("TYPES: void*=%d short=%d int=%d long=%d long_long=%d size_t=%d"
           " float=%d double=%d long_double=%d\n",
           (int)sizeof(void*), (int)sizeof(short), (int)sizeof(int), (int)sizeof(long),
           (int)sizeof(long long), (int)sizeof(size_t),
           (int)sizeof(float), (int)sizeof(double), (int)sizeof(long double));
    std::vector<std::string> compressors;
    if (LZ4Compressor().IsSupported()) {
      compressors.emplace_back("lz4");
    }
    if (ZStdCompressor().IsSupported()) {
      compressors.emplace_back("zstd");
    }
    if (ZLibCompressor().IsSupported()) {
      compressors.emplace_back("zlib");
    }
    if (LZMACompressor().IsSupported()) {
      compressors.emplace_back("lzma");
    }
    if (!compressors.empty()) {
      PrintF("COMPRESSORS: %s\n", StrJoin(compressors, ", ").c_str());
    }
    std::map<std::string, std::string> info = GetSystemInfo();
    if (!info["proc_id"].empty()) {
      PrintF("PROCESS_ID: %s\n", info["proc_id"].c_str());
    }
    if (!info["mem_total"].empty()) {
      PrintF("MEMORY: total=%s free=%s cached=%s rss=%s\n",
             info["mem_total"].c_str(), info["mem_free"].c_str(),
             info["mem_cached"].c_str(), info["mem_rss"].c_str());
    }
    if (*_TKRZW_PREFIX != '\0') {
      PrintF("prefix: %s\n", _TKRZW_PREFIX);
    }
    if (*_TKRZW_INCLUDEDIR != '\0') {
      PrintF("includedir: %s\n", _TKRZW_INCLUDEDIR);
    }
    if (*_TKRZW_LIBDIR != '\0') {
      PrintF("libdir: %s\n", _TKRZW_LIBDIR);
    }
    if (*_TKRZW_BINDIR != '\0') {
      PrintF("bindir: %s\n", _TKRZW_BINDIR);
    }
    if (*_TKRZW_BINDIR != '\0') {
      PrintF("libexecdir: %s\n", _TKRZW_LIBEXECDIR);
    }
    if (*_TKRZW_APPINC) {
      PrintF("appinc: %s\n", _TKRZW_APPINC);
    }
    if (*_TKRZW_APPLIBS) {
      PrintF("applibs: %s\n", _TKRZW_APPLIBS);
    }
  }
  return 0;
}

// Prints the version information.
void PrintVersion() {
  PrintF("Tkrzw %s (library %s) on %s (%s) (%s endian)\n",
         PACKAGE_VERSION, LIBRARY_VERSION, OS_NAME,
         IS_POSIX ? "POSIX" : "non-POSIX",
         IS_BIG_ENDIAN ? "big" : "little");
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
    if (std::strcmp(args[1], "config") == 0) {
      rv = tkrzw::ProcessConfig(argc - 1, args + 1);
    } else if (std::strcmp(args[1], "version") == 0 || std::strcmp(args[1], "--version") == 0) {
      tkrzw::PrintVersion();
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
