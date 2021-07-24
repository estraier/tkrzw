/*************************************************************************************************
 * Example for advanced operations of the hash database
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
#include "tkrzw_dbm_hash.h"
#include "tkrzw_str_util.h"

// Main routine.
int main(int argc, char** argv) {
  // All symbols of Tkrzw are under the namespace "tkrzw".
  using namespace tkrzw;

  // Creates the database manager.
  HashDBM dbm;

  // Opens a new database,
  dbm.Open("casket.tkh", true, File::OPEN_TRUNCATE);

  // Record processor to count events.
  class Counter : public DBM::RecordProcessor {
   public:
    // Update an existing record.
    virtual std::string_view ProcessFull(std::string_view key, std::string_view value) {
      new_value_ = ToString(StrToInt(value) + 1);
      return new_value_;
    }
    // Register a new record.
    virtual std::string_view ProcessEmpty(std::string_view key) {
      return "1";
    }
   private:
    std::string new_value_;
  };

  // Procedure to count up an event.
  // DBM::IncrementSimple does the same job.
  const auto CountUp = [&](std::string_view name) {
    Counter counter;
    dbm.Process(name, &counter, true);
  };

  // Counts up events.
  CountUp("foo");
  CountUp("foo");
  CountUp("bar");

  // Reports counts.
  std::cout << dbm.GetSimple("foo", "0") << std::endl;
  std::cout << dbm.GetSimple("bar", "0") << std::endl;
  std::cout << dbm.GetSimple("baz", "0") << std::endl;

  // Closes the database.
  dbm.Close();

  return 0;
}

// END OF FILE
