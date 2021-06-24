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
  P("  %s search [options]\n", progname);
  P("    : Checks search performance.\n");
  P("  %s hash [options]\n", progname);
  P("    : Checks hashing performance.\n");
  P("  %s compress [options]\n", progname);
  P("    : Checks compression performance.\n");
  P("\n");
  P("Options of the search subcommand:\n");
  P("  --iter num : The number of iterations. (default: 10000)\n");
  P("  --text num : The size of each text to search. (default: 10000)\n");
  P("  --pattern num : The size of each pattern to search for. (default: 5)\n");
  P("  --chars num : The number of character variations in the text and the pattern."
    " (default: 26)\n");
  P("  --whole num : The maximum number of results to get. 0 means the first only."
    " (default: 0)\n");
  P("  --batch num : The number of patterns in a batch. 0 menas no batching. (default: 0)\n");
  P("\n");
  P("Options of the hash subcommand:\n");
  P("  --iter num : The number of iterations. (default: 10000)\n");
  P("  --text num : The size of each text to search. (default: 10000)\n");
  P("\n");
  P("Options of the compress subcommand:\n");
  P("  --iter num : The number of iterations. (default: 10000)\n");
  P("  --text num : The size of each text to search. (default: 10000)\n");
  P("  --error_type str : The type of errors to detect: zero, random, alpha.\n");
  P("  --error_size num : The size of each error section. (default: 1)\n");
  P("\n");
  std::exit(1);
}

// Make a text of random characters.
std::string MakeRandomCharacterText(int32_t length, uint8_t first_char, uint8_t last_char) {
  static std::mt19937 mt(19780211);
  std::uniform_int_distribution<int32_t> dist(0, INT32MAX);
  std::string text;
  const int32_t range = last_char - first_char + 1;
  text.resize(length);
  for (int32_t i = 0; i < length; i++) {
    text[i] = dist(mt) % range + first_char;
  }
  return text;
}

// Processes the search subcommand.
static int32_t ProcessSearch(int32_t argc, const char** args) {
  const std::map<std::string, int32_t>& cmd_configs = {
    {"--iter", 1}, {"--text", 1}, {"--pattern", 1}, {"--chars", 1},
    {"--whole", 1}, {"--batch", 1},
  };
  std::map<std::string, std::vector<std::string>> cmd_args;
  std::string cmd_error;
  if (!ParseCommandArguments(argc, args, cmd_configs, &cmd_args, &cmd_error)) {
    EPrint("Invalid command: ", cmd_error, "\n\n");
    PrintUsageAndDie();
  }
  const int32_t num_iterations = GetIntegerArgument(cmd_args, "--iter", 0, 10000);
  const int32_t text_size = GetIntegerArgument(cmd_args, "--text", 0, 10000);
  const int32_t pattern_size = GetIntegerArgument(cmd_args, "--pattern", 0, 5);
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

// Processes the hash subcommand.
static int32_t ProcessHash(int32_t argc, const char** args) {
  const std::map<std::string, int32_t>& cmd_configs = {
    {"--iter", 1}, {"--text", 1}, {"--error_type", 1}, {"--error_size", 1},
  };
  std::map<std::string, std::vector<std::string>> cmd_args;
  std::string cmd_error;
  if (!ParseCommandArguments(argc, args, cmd_configs, &cmd_args, &cmd_error)) {
    EPrint("Invalid command: ", cmd_error, "\n\n");
    PrintUsageAndDie();
  }
  const int32_t num_iterations = GetIntegerArgument(cmd_args, "--iter", 0, 10000);
  const int32_t text_size = GetIntegerArgument(cmd_args, "--text", 0, 10000);
  const std::string error_type = GetStringArgument(cmd_args, "--error_type", 0, "");
  const int32_t error_size = GetIntegerArgument(cmd_args, "--error_size", 0, 1);
  if (num_iterations < 1) {
    Die("Invalid number of iterations");
  }
  if (text_size < 1) {
    Die("Invalid text size");
  }
  std::string text;
  text.reserve(text_size);
  for (int32_t i = 0; i < text_size; i++) {
    text.append(1, i % 256);
  }
  PrintL("Hash: iterations=", num_iterations, " text_size=", text_size);
  auto murmur = [](std::string_view sv) -> uint32_t {
                  return tkrzw::HashMurmur(sv, 0);
                };
  auto fnv = [](std::string_view sv) -> uint32_t {
                  return tkrzw::HashFNV(sv);
                };
  auto checksum6 = [](std::string_view sv) -> uint32_t {
                  return tkrzw::HashChecksum6(sv);
                };
  auto checksum8 = [](std::string_view sv) -> uint32_t {
                  return tkrzw::HashChecksum8(sv);
                };
  auto adler6 = [](std::string_view sv) -> uint32_t {
                  return tkrzw::HashAdler6(sv);
                };
  auto adler8 = [](std::string_view sv) -> uint32_t {
                  return tkrzw::HashAdler8(sv);
                };
  auto adler16 = [](std::string_view sv) -> uint32_t {
                  return tkrzw::HashAdler16(sv);
                };
  auto adler32 = [](std::string_view sv) -> uint32_t {
                  return tkrzw::HashAdler32(sv);
                };
  auto crc4 = [](std::string_view sv) -> uint32_t {
                  return tkrzw::HashCRC4(sv);
                };
  auto crc8 = [](std::string_view sv) -> uint32_t {
                  return tkrzw::HashCRC8(sv);
                };
  auto crc16 = [](std::string_view sv) -> uint32_t {
                  return tkrzw::HashCRC16(sv);
                };
  auto crc32 = [](std::string_view sv) -> uint32_t {
                  return tkrzw::HashCRC32(sv);
                };
  auto checksum6_crc8 = [](std::string_view sv) -> uint32_t {
                  return (tkrzw::HashChecksum6(sv) << 16) + tkrzw::HashCRC8(sv);
                };
  auto checksum6_crc16 = [](std::string_view sv) -> uint32_t {
                  return (tkrzw::HashChecksum6(sv) << 16) + tkrzw::HashCRC16(sv);
                };
  auto checksum8_crc8 = [](std::string_view sv) -> uint32_t {
                  return (tkrzw::HashChecksum8(sv) << 16) + tkrzw::HashCRC8(sv);
                };
  auto checksum8_crc16 = [](std::string_view sv) -> uint32_t {
                  return (tkrzw::HashChecksum8(sv) << 16) + tkrzw::HashCRC16(sv);
                };
  auto adler6_crc8 = [](std::string_view sv) -> uint32_t {
                  return (tkrzw::HashAdler6(sv) << 16) + tkrzw::HashCRC8(sv);
                };
  auto adler6_crc16 = [](std::string_view sv) -> uint32_t {
                  return (tkrzw::HashAdler6(sv) << 16) + tkrzw::HashCRC16(sv);
                };
  auto adler8_crc8 = [](std::string_view sv) -> uint32_t {
                  return (tkrzw::HashAdler8(sv) << 16) + tkrzw::HashCRC8(sv);
                };
  auto adler8_crc16 = [](std::string_view sv) -> uint32_t {
                  return (tkrzw::HashAdler8(sv) << 16) + tkrzw::HashCRC16(sv);
                };
  auto crc4_crc8 = [](std::string_view sv) -> uint32_t {
                  return (tkrzw::HashCRC4(sv) << 16) + tkrzw::HashCRC8(sv);
                };
  auto crc4_crc16 = [](std::string_view sv) -> uint32_t {
                  return (tkrzw::HashCRC4(sv) << 16) + tkrzw::HashCRC16(sv);
                };
  auto crc8_crc16 = [](std::string_view sv) -> uint32_t {
                  return (tkrzw::HashCRC8(sv) << 16) + tkrzw::HashCRC16(sv);
                };
  if (error_type.empty()) {
    const std::vector<std::pair<uint32_t(*)(std::string_view), const char*>> test_sets = {
      {murmur, "Murmur"},
      {fnv, "FNV"},
      {checksum6, "Checksum-6"},
      {checksum8, "Checksum-8"},
      {adler6, "Adler-6"},
      {adler8, "Adler-8"},
      {adler16, "Adler-16"},
      {adler32, "Adler-32"},
      {crc4, "CRC-4"},
      {crc8, "CRC-8"},
      {crc16, "CRC-16"},
      {crc32, "CRC-32"},
    };
    for (const auto& test_set : test_sets) {
      const uint32_t expected_value = test_set.first(text);
      const auto start_time = tkrzw::GetWallTime();
      for (int32_t i = 0; i < num_iterations; i++) {
        if (test_set.first(text) != expected_value) {
          Die("Inconsistent result");
        }
      }
      const auto end_time = tkrzw::GetWallTime();
      const auto elapsed_time = end_time - start_time;
      PrintL("func=", test_set.second, " time=", elapsed_time, " value=", expected_value);
    }
  } else {
    const std::vector<std::pair<uint32_t(*)(std::string_view), const char*>> test_sets = {
      {checksum6, "Checksum-6"},
      {checksum8, "Checksum-8"},
      {adler6, "Adler-6"},
      {adler8, "Adler-8"},
      {adler16, "Adler-16"},
      {adler32, "Adler-32"},
      {crc4, "CRC-4"},
      {crc8, "CRC-8"},
      {crc16, "CRC-16"},
      {crc32, "CRC-32"},
      {checksum6_crc8, "Checksum-6+CRC-8"},
      {checksum6_crc16, "Checksum-6+CRC-16"},
      {checksum8_crc8, "Checksum-8+CRC-8"},
      {checksum8_crc16, "Checksum-8+CRC-16"},
      {adler6_crc8, "Adler-6+CRC-8"},
      {adler6_crc16, "Adler-6+CRC-16"},
      {adler8_crc8, "Adler-8+CRC-8"},
      {adler8_crc16, "Adler-8+CRC-16"},
      {crc4_crc8, "CRC-4+CRC-8"},
      {crc4_crc16, "CRC-4+CRC-16"},
      {crc8_crc16, "CRC-8+CRC-16"},
    };
    bool random = false;
    bool alpha = false;
    if (error_type == "random") {
      random = true;
    } else if (error_type == "alpha") {
      alpha = true;
    } else if (error_type != "zero") {
      Die("Unknown error type");
    }
    for (const auto& test_set : test_sets) {
      std::vector<std::string> texts;
      for (int32_t i = 0; i < 1000; i++) {
        texts.emplace_back(MakeRandomCharacterText(text_size, 0, 255));
      }
      std::mt19937 mt(1);
      std::uniform_int_distribution<int32_t> off_dist(0, text_size);
      std::uniform_int_distribution<int32_t> char_dist(0, 255);
      int32_t detected = 0;
      for (int32_t i = 0; i < num_iterations; i++) {
        const std::string& text = texts[i % texts.size()];
        std::string error_text = text;
        const int32_t off = off_dist(mt);
        const int32_t end = off + error_size;
        if (end > static_cast<int32_t>(error_text.size())) {
          error_text.resize(end);
        }
        for (int32_t j = off; j < end; j++) {
          uint32_t c = 0;
          if (alpha) {
            c = 'a' + char_dist(mt) % 25;
          } else if (random) {
            c = char_dist(mt);
          }
          if (static_cast<uint8_t>(error_text[j]) == c) {
            c = (c + 1) % 256;
          }
          error_text[j] = c;
        }
        if (text == error_text) {
          EPrintL("wrong text: off=", off, " end=" , end);
        }
        if (test_set.first(text) != test_set.first(error_text)) {
          detected++;
        }
      }
      const double precision = detected * 1.0 / num_iterations;
      PrintL("func=", test_set.second, " precision= ", precision);
    }
  }
  return 0;
}

// Processes the compress subcommand.
static int32_t ProcessCompress(int32_t argc, const char** args) {
  const std::map<std::string, int32_t>& cmd_configs = {
    {"--iter", 1}, {"--text", 1}, {"--pattern", 1},
  };
  std::map<std::string, std::vector<std::string>> cmd_args;
  std::string cmd_error;
  if (!ParseCommandArguments(argc, args, cmd_configs, &cmd_args, &cmd_error)) {
    EPrint("Invalid command: ", cmd_error, "\n\n");
    PrintUsageAndDie();
  }
  const int32_t num_iterations = GetIntegerArgument(cmd_args, "--iter", 0, 10000);
  const int32_t text_size = GetIntegerArgument(cmd_args, "--text", 0, 10000);
  const std::string pattern = GetStringArgument(cmd_args, "--pattern", 0, "natural");
  if (num_iterations < 1) {
    Die("Invalid number of iterations");
  }
  if (text_size < 1) {
    Die("Invalid text size");
  }
  const int32_t num_texts = 1000;
  std::vector<std::string> texts;
  texts.reserve(num_texts);
  std::string (*text_gen)(size_t size, int32_t seed) = nullptr;
  if (pattern == "natural") {
    text_gen = tkrzw::MakeNaturalishText;
  } else if (pattern == "cycle") {
    text_gen = tkrzw::MakeCyclishText;
  } else {
    Die("Unknown text pattern");
  }
  for (int32_t i = 0; i < num_texts; i++) {
    texts.emplace_back(text_gen(text_size, i));
  }
  PrintL("Compress: iterations=", num_iterations, " text_size=", text_size);
  std::vector<std::pair<Compressor*, const char*>> test_sets;
  tkrzw::DummyCompressor dummy_default;
  if (dummy_default.IsSupported()) {
    test_sets.emplace_back(std::make_pair(&dummy_default, "dummy-default"));
  }
  tkrzw::DummyCompressor dummy_checksum(true);
  if (dummy_checksum.IsSupported()) {
    test_sets.emplace_back(std::make_pair(&dummy_checksum, "dummy-checksum"));
  }
  tkrzw::LZ4Compressor lz4_fast(5);
  if (lz4_fast.IsSupported()) {
    test_sets.emplace_back(std::make_pair(&lz4_fast, "LZ4-fast"));
  }
  tkrzw::LZ4Compressor lz4_default;
  if (lz4_default.IsSupported()) {
    test_sets.emplace_back(std::make_pair(&lz4_default, "LZ4-default"));
  }
  tkrzw::ZStdCompressor zstd_fast(-1);
  if (zstd_fast.IsSupported()) {
    test_sets.emplace_back(std::make_pair(&zstd_fast, "zstd-fast"));
  }
  tkrzw::ZStdCompressor zstd_default;
  if (zstd_default.IsSupported()) {
    test_sets.emplace_back(std::make_pair(&zstd_default, "zstd-default"));
  }
  tkrzw::ZStdCompressor zstd_slow(10);
  if (zstd_slow.IsSupported()) {
    test_sets.emplace_back(std::make_pair(&zstd_slow, "zstd-slow"));
  }
  tkrzw::ZLibCompressor zlib_fast(1);
  if (zlib_fast.IsSupported()) {
    test_sets.emplace_back(std::make_pair(&zlib_fast, "zlib-fast"));
  }
  tkrzw::ZLibCompressor zlib_default;
  if (zlib_default.IsSupported()) {
    test_sets.emplace_back(std::make_pair(&zlib_default, "zlib-default"));
  }
  tkrzw::ZLibCompressor zlib_slow(9);
  if (zlib_slow.IsSupported()) {
    test_sets.emplace_back(std::make_pair(&zlib_slow, "zlib-slow"));
  }
  tkrzw::LZMACompressor lzma_fast(1);
  if (lzma_fast.IsSupported()) {
    test_sets.emplace_back(std::make_pair(&lzma_fast, "lzma-fast"));
  }
  tkrzw::LZMACompressor lzma_default;
  if (lzma_default.IsSupported()) {
    test_sets.emplace_back(std::make_pair(&lzma_default, "lzma-default"));
  }
  tkrzw::LZMACompressor lzma_slow(9);
  if (lzma_slow.IsSupported()) {
    test_sets.emplace_back(std::make_pair(&lzma_slow, "lzma-slow"));
  }
  for (const auto& test_set : test_sets) {
    tkrzw::Compressor* compressor = test_set.first;
    std::vector<std::string_view> comp_values;
    comp_values.reserve(num_texts);
    int64_t output_size = 0;
    const auto comp_start_time = tkrzw::GetWallTime();
    for (int32_t i = 0; i < num_iterations; i++) {
      const std::string& text = texts[i % texts.size()];
      size_t comp_size = 0;
      char* comp_data = compressor->Compress(text.data(), text.size(), &comp_size);
      output_size += comp_size;
      if (static_cast<int32_t>(comp_values.size()) < text_size) {
        comp_values.emplace_back(std::string_view(comp_data, comp_size));
      } else {
        xfree(comp_data);
      }
    }
    const auto comp_end_time = tkrzw::GetWallTime();
    const auto comp_elapsed_time = comp_end_time - comp_start_time;
    const auto decomp_start_time = tkrzw::GetWallTime();
    for (int32_t i = 0; i < num_iterations; i++) {
      auto& comp_value = comp_values[i % comp_values.size()];
      size_t decomp_size = 0;
      char* decomp_data =
          compressor->Decompress(comp_value.data(), comp_value.size(), &decomp_size);
      xfree(decomp_data);
    }
    const auto decomp_end_time = tkrzw::GetWallTime();
    const auto decomp_elapsed_time = decomp_end_time - decomp_start_time;
    for (auto& comp_value : comp_values) {
      xfree(const_cast<char*>(comp_value.data()));
    }
    const double ratio = output_size * 1.0 / (text_size * num_iterations);
    PrintL("config=", test_set.second, " comp_time=", comp_elapsed_time,
           " decomp_time=", decomp_elapsed_time, " ratio=", ratio);
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
    if (std::strcmp(args[1], "search") == 0) {
      rv = tkrzw::ProcessSearch(argc - 1, args + 1);
    } else if (std::strcmp(args[1], "hash") == 0) {
      rv = tkrzw::ProcessHash(argc - 1, args + 1);
    } else if (std::strcmp(args[1], "compress") == 0) {
      rv = tkrzw::ProcessCompress(argc - 1, args + 1);
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
