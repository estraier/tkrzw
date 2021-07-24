/*************************************************************************************************
 * Example for building an inverted index with the skip database
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
#include "tkrzw_dbm_baby.h"
#include "tkrzw_dbm_skip.h"
#include "tkrzw_str_util.h"

using namespace tkrzw;
void DumpBuffer(BabyDBM* buffer, int32_t file_id);

// Main routine.
int main(int argc, char** argv) {
  const std::vector<std::string> documents = {
    "this is a pen", "this boy loves a girl", "the girl has the pen", "tokyo go",
    "she loves tokyo", "tokyo is a big city", "boy meets girl", "girl meets boy"};
  constexpr int32_t BUFFER_CAPACITY = 10;

  // Register all documents into separate index files.
  BabyDBM buffer;
  int32_t num_files = 0;
  int32_t num_words_in_buffer = 0;
  int64_t doc_id = 0;
  for (const auto& doc : documents) {
    const std::string value = IntToStrBigEndian(doc_id);
    const std::vector<std::string> words = StrSplit(doc, " ");
    for (const std::string& word : words) {
      buffer.Append(word, value);
      num_words_in_buffer++;
    }
    if (num_words_in_buffer > BUFFER_CAPACITY) {
      DumpBuffer(&buffer, num_files);
      buffer.Clear();
      num_files++;
      num_words_in_buffer = 0;
    }
    doc_id++;
  }
  if (num_words_in_buffer > 0) {
    DumpBuffer(&buffer, num_files);
    buffer.Clear();
    num_files++;
  }

  // Merge separate index files into one.
  SkipDBM merged_dbm;
  merged_dbm.Open("index-merged.tks", true, File::OPEN_TRUNCATE).OrDie();
  for (int file_id = 0; file_id < num_files; file_id++) {
    const std::string file_name = SPrintF("index-%05d.tks", file_id);
    merged_dbm.MergeSkipDatabase(file_name).OrDie();
  }
  merged_dbm.SynchronizeAdvanced(false, nullptr, SkipDBM::ReduceConcat).OrDie();
  merged_dbm.Close().OrDie();

  // Search the merged index file.
  merged_dbm.Open("index-merged.tks", false).OrDie();
  const std::vector<std::string> queries = {"pen", "boy", "girl", "tokyo"};
  for (const auto& query : queries) {
    std::cout << query << ":";
    std::string value;
    if (merged_dbm.Get(query, &value).IsOK()) {
      size_t pos = 0;
      while (pos < value.size()) {
        doc_id = StrToIntBigEndian(std::string_view(value.data() + pos, sizeof(int64_t)));
        std::cout << " " << doc_id;
        pos += sizeof(int64_t);
      }
    }
    std::cout << std::endl;
  }
  merged_dbm.Close().OrDie();
}

// Dump the posting lists of a buffer into a database file.
void DumpBuffer(BabyDBM* buffer, int32_t file_id) {
  const std::string file_name = SPrintF("index-%05d.tks", file_id);
  SkipDBM::TuningParameters tuning_params;
  tuning_params.insert_in_order = true;
  SkipDBM dbm;
  dbm.OpenAdvanced(file_name, true, File::OPEN_TRUNCATE, tuning_params).OrDie();
  auto iter = buffer->MakeIterator();
  iter->First();
  std::string key, value;
  while (iter->Get(&key, &value).IsOK()) {
    dbm.Set(key, value).OrDie();
    iter->Next();
  }
  dbm.Close().OrDie();
}

// END OF FILE
