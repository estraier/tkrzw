/*************************************************************************************************
 * Example for basic usage of the sharding database
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

#include "tkrzw_dbm_shard.h"
#include "tkrzw_str_util.h"

// Main routine.
int main(int argc, char** argv) {
  // All symbols of Tkrzw are under the namespace "tkrzw".
  using namespace tkrzw;

  // Creates the database manager.
  ShardDBM dbm;

  // Opens a new database.
  const std::map<std::string, std::string> open_params = {
    {"num_shards", "10"},
    {"dbm", "TreeDBM"}, {"key_comparator", "DecimalKeyComparator"},
  };
  dbm.OpenAdvanced("casket", true, File::OPEN_TRUNCATE, open_params).OrDie();
  
  // Stores records.
  for (int32_t i = 1; i <= 100; i++) {
    const std::string key = ToString(i);
    const std::string value = ToString(i * i);
    dbm.Set(key, value).OrDie();
  }

  // Retrieve records whose keys are 50 or more.
  auto iter = dbm.MakeIterator();
  iter->Jump("50");
  std::string key, value;
  while (iter->Get(&key, &value).IsOK()) {
    std::cout << key << ":" << value << std::endl;
    iter->Next();
  }
  
  // Closes the database.
  dbm.Close().OrDie();

  return 0;
}

// END OF FILE
