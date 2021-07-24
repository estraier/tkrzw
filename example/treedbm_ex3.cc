/*************************************************************************************************
 * Example for advanced operations of the tree database
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
#include "tkrzw_dbm_tree.h"
#include "tkrzw_file_pos.h"
#include "tkrzw_file_util.h"
#include "tkrzw_str_util.h"

// Main routine.
int main(int argc, char** argv) {
  // All symbols of Tkrzw are under the namespace "tkrzw".
  using namespace tkrzw;

  // Create a sample input file.
  const std::string tsv_data =
      "apple\tA red fluit.\n"
      "Apple\tA computer company.\n"
      "applet\tA small application.\n"
      "banana\tA yellow fluit.\n";
  WriteFile("casket.tsv", tsv_data);

  // Creates the database manager.
  TreeDBM dbm;

  // Opens a new database with tuning for a small database.
  TreeDBM::TuningParameters tuning_params;
  tuning_params.align_pow = 6;
  tuning_params.num_buckets = 10000;
  tuning_params.max_page_size = 4000;
  dbm.OpenAdvanced("casket.tkt", true, File::OPEN_TRUNCATE, tuning_params);

  // Opens the input file.
  PositionalParallelFile file;
  file.Open("casket.tsv", false);

  // Record processor to concat values with linefeed.
  class ConcatProcessor : public DBM::RecordProcessor {
   public:
    explicit ConcatProcessor(const std::string& value) : value_(value) {}
    std::string_view ProcessFull(std::string_view key, std::string_view value) override {
      value_ = StrCat(value, "\n", value_);
      return value_;
    }
    std::string_view ProcessEmpty(std::string_view key) override {
      return value_;
    }
   private:
    std::string value_;
  };

  // Read each line of the input file.
  FileReader reader(&file);
  std::string line;
  while (reader.ReadLine(&line) == Status::SUCCESS) {
    line = StrStripLine(line);
    const std::vector<std::string> columns = StrSplit(line, "\t");
    if (columns.size() < 2) continue;
    const std::string key = StrLowerCase(columns[0]);
    ConcatProcessor proc(line);
    dbm.Process(key, &proc, true);
  }

  // Find records by forward matching with "app".
  std::unique_ptr<DBM::Iterator> iter = dbm.MakeIterator();
  iter->Jump("app");
  std::string key, value;
  while (iter->Get(&key, &value) == Status::SUCCESS) {
    if (!StrBeginsWith(key, "app")) break;
    std::cout << "---- " << key << " ----" << std::endl;
    std::cout << value << std::endl;
    iter->Next();
  }

  // Close the input file
  file.Close();

  // Closes the database.
  dbm.Close();

  return 0;
}

// END OF FILE
