/*************************************************************************************************
 * Example for typical usage of the polymorphic secondary index
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

#include <map>
#include <string>
#include <vector>
#include "tkrzw_index.h"

// All symbols of Tkrzw are under the namespace "tkrzw".
using namespace tkrzw;

// Main routine.
int main(int argc, char** argv) {
  // Opens the database.
  PolyIndex index;
  const std::map<std::string, std::string> open_params = {{"num_buckets", "100"}};
  index.Open("casket.tkt", true, File::OPEN_TRUNCATE, open_params).OrDie();

  // Adds records to the index.
  // The key is a division name and the value is person name.
  index.Add("general", "anne").OrDie();
  index.Add("general", "matthew").OrDie();
  index.Add("general", "marilla").OrDie();
  index.Add("sales", "gilbert").OrDie();

  // Anne moves to the sales division.
  index.Remove("general", "anne").OrDie();
  index.Add("sales", "anne").OrDie();

  // Prints all members for each division.
  const std::vector<std::string> divisions = {"general", "sales"};
  for (const auto& division : divisions) {
    std::cout << division << std::endl;
    const std::vector<std::string>& members = index.GetValues(division);
    for (const auto& member : members) {
      std::cout << " -- " + member << std::endl;
    }
  }

  // Prints every record by iterator.
  std::unique_ptr<PolyIndex::Iterator> iter = index.MakeIterator();
  iter->First();
  std::string key, value;
  while (iter->Get(&key, &value)) {
    std::cout << key << ": " << value << std::endl;
    iter->Next();
  }

  // Closes the index
  index.Close().OrDie();

  return 0;
}

// END OF FILE
