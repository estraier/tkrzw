/*************************************************************************************************
 * Performance checker of string utilities
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
  const char* progname = "tkrzw_str_perf";
  P("%s: Performance checker of string utilities of Tkrzw\n", progname);
  P("\n");
  P("Usage:\n");
  P("  %s search [-i num] [-t num] [-p num] [-b]\n", progname);
  P("    : Checks search performance.\n");
  P("\n");
  P("Options of the search subcommand:\n");
  P("  --iter num : The number of iterations. (default: 10000)\n");
  P("  --threads num : The size of each text to search. (default: 10000)\n");
  P("  --patterns num : The size of each pattern to search for. (default: 5)\n");
  P("  --chars num : The number of character variations in the text and the pattern."
    " (default: 26)\n");
  P("  --whole num : The maximum number of results to get. 0 means the first only."
    " (default: 0)\n");
  P("  --batch num : The number of patterns in a batch. 0 menas no batching. (default: 0)\n");
  P("\n");
  std::exit(1);
}

// Processes the search subcommand.
static int32_t ProcessSearch(int32_t argc, const char** args) {
  const std::map<std::string, int32_t>& cmd_configs = {
    {"--iter", 1}, {"--threads", 1}, {"--pattern", 1}, {"--chars", 1},
    {"--whole", 1}, {"--batch", 1},
  };
  std::map<std::string, std::vector<std::string>> cmd_args;
  std::string cmd_error;
  if (!ParseCommandArguments(argc, args, cmd_configs, &cmd_args, &cmd_error)) {
    EPrint("Invalid command: ", cmd_error, "\n\n");
    PrintUsageAndDie();
  }
  const int32_t num_iterations = GetIntegerArgument(cmd_args, "--iter", 0, 10000);
  const int32_t text_size = GetIntegerArgument(cmd_args, "--threads", 0, 10000);
  const int32_t pattern_size = GetIntegerArgument(cmd_args, "--patterns", 0, 5);
  const int32_t num_chars = GetIntegerArgument(cmd_args, "--chars", 0, 26);
  const int32_t whole_size = GetIntegerArgument(cmd_args, "--whole", 0, 0);
  const int32_t batch_size = GetIntegerArgument(cmd_args, "--batch", 0, 0);
  if (num_iterations < 1) {
    Die("Invalid number of iterations");
  }
  if (text_size < 1) {
    Die("Invalid text size");
  }
  if (pattern_size < 1) {
    Die("Invalid pattern size");
  }
  if (num_chars < 1 || num_chars > 255) {
    Die("Invalid pattern size");
  }
  if (whole_size < 0) {
    Die("Invalid whole result size");
  }
  if (batch_size < 0) {
    Die("Invalid batch size");
  }
  int8_t first_char = 'a';
  if (num_chars > 95) {
    first_char = 1;
  } else if (num_chars > 26) {
    first_char = ' ';
  }
  const int32_t last_char = first_char + num_chars - 1;
  std::vector<std::string> texts;
  for (int32_t i = 0; i < 1009; ++i) {
    texts.emplace_back(MakeRandomCharacterText(text_size, first_char, last_char));
  }
  std::vector<std::string> patterns;
  for (int32_t i = 0; i < 1013; ++i) {
    patterns.emplace_back(MakeRandomCharacterText(pattern_size, first_char, last_char));
  }
  std::string sample_pattern;
  if (first_char >= ' ') {
    sample_pattern = patterns.front();
  } else {
    sample_pattern = "(unprintable)";
  }
  PrintL("Search: iterations=", num_iterations, " text_size=", text_size,
         " pattern_size=", pattern_size, " num_chars=", num_chars,
         " sample_pattern=", sample_pattern);
  if (batch_size > 0) {
    const std::vector<std::pair<
      std::vector<std::vector<int32_t>> (*)(
          std::string_view, const std::vector<std::string>&, size_t), const char*>> test_sets = {
      {tkrzw::StrSearchBatch, "Standard"},
      {tkrzw::StrSearchBatchKMP, "KMP"},
      {tkrzw::StrSearchBatchBM, "BM"},
      {tkrzw::StrSearchBatchRK, "RK"},
    };
    for (const auto& test_set : test_sets) {
      const auto start_time = tkrzw::GetWallTime();
      int32_t hit_count = 0;
      for (int32_t i = 0; i < num_iterations; i++) {
        const std::string& text = texts[i % texts.size()];
        std::vector<std::string> batch_patterns;
        for (int32_t j = 0; j < batch_size; j++) {
          batch_patterns.emplace_back(patterns[(i + j) % patterns.size()]);
        }
        const auto result = test_set.first(text, batch_patterns, whole_size);
        for (const auto& pat_result : result) {
          if (!pat_result.empty()) {
            hit_count++;
          }
        }
      }
      const auto end_time = tkrzw::GetWallTime();
      const auto elapsed_time = end_time - start_time;
      PrintL("func=", test_set.second, " time=", elapsed_time, " hit_count=", hit_count);
    }
  } else if (whole_size > 0) {
    const std::vector<std::pair<
      std::vector<int32_t> (*)(
          std::string_view, std::string_view, size_t), const char*>> test_sets = {
      {tkrzw::StrSearchWhole, "Standard"},
      {tkrzw::StrSearchWholeKMP, "KMP"},
      {tkrzw::StrSearchWholeBM, "BM"},
      {tkrzw::StrSearchWholeRK, "RK"},
    };
    for (const auto& test_set : test_sets) {
      const auto start_time = tkrzw::GetWallTime();
      int32_t hit_count = 0;
      for (int32_t i = 0; i < num_iterations; i++) {
        const std::string& text = texts[i % texts.size()];
        const std::string& pattern = patterns[i % patterns.size()];
        if (!test_set.first(text, pattern, whole_size).empty()) {
          hit_count++;
        }
      }
      const auto end_time = tkrzw::GetWallTime();
      const auto elapsed_time = end_time - start_time;
      PrintL("func=", test_set.second, " time=", elapsed_time, " hit_count=", hit_count);
    }
  } else {
    const std::vector<std::pair<
      int32_t (*)(std::string_view, std::string_view), const char*>> test_sets = {
      {tkrzw::StrSearch, "Standard"},
      {tkrzw::StrSearchDoubleLoop, "Naive"},
      {tkrzw::StrSearchMemchr, "Memchr"},
      {tkrzw::StrSearchMemmem, "Memmem"},
      {tkrzw::StrSearchKMP, "KMP" },
      {tkrzw::StrSearchBM, "BM"},
      {tkrzw::StrSearchRK, "RK"},
      {tkrzw::StrSearchZ, "Z"},
    };
    for (const auto& test_set : test_sets) {
      const auto start_time = tkrzw::GetWallTime();
      int32_t hit_count = 0;
      for (int32_t i = 0; i < num_iterations; i++) {
        const std::string& text = texts[i % texts.size()];
        const std::string& pattern = patterns[i % patterns.size()];
        if (test_set.first(text, pattern) >= 0) {
          hit_count++;
        }
      }
      const auto end_time = tkrzw::GetWallTime();
      const auto elapsed_time = end_time - start_time;
      PrintL("func=", test_set.second, " time=", elapsed_time, " hit_count=", hit_count);
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
  if (std::strcmp(args[1], "search") == 0) {
    rv = tkrzw::ProcessSearch(argc - 1, args + 1);
  } else {
    tkrzw::PrintUsageAndDie();
  }
  return rv;
}

// END OF FILE
