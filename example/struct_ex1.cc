/*************************************************************************************************
 * Example to store structured data
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
#include <vector>
#include "tkrzw_dbm_hash.h"
#include "tkrzw_str_util.h"

// Main routine.
int main(int argc, char** argv) {
  // All symbols of Tkrzw are under the namespace "tkrzw".
  using namespace tkrzw;

  // Creates the database manager.
  HashDBM dbm;

  // Opens a new database.
  dbm.Open("casket.tkh", true, File::OPEN_TRUNCATE);

  // Makes a map representing structured data.
  std::map<std::string, std::string> record;
  record["name"] = "John Doe";
  record["id"] = SerializeBasicValue<int32_t>(98765);
  record["rating"] = SerializeBasicValue<double>(0.8);
  std::vector<std::string> medals = {"gold", "silver", "bronze"};
  record["medals"] = SerializeStrVector(medals);
  std::vector<int32_t> rivals = {123456, 654321};
  record["rivals"] = SerializeBasicVector(rivals);
  std::vector<double> scores = {48.9, 52.5, 60.3};
  record["scores"] = SerializeBasicVector(scores);

  // Stores the serialized string.
  dbm.Set("john", SerializeStrMap(record));

  // Retrieves the serialized string and restore it.
  std::string serialized;
  std::map<std::string, std::string> restored;
  if (dbm.Get("john", &serialized).IsOK()) {
    restored = DeserializeStrMap(serialized);
  }

  // Shows the restored content.
  std::cout << "name: " << restored["name"] << std::endl;
  std::cout << "id: " <<
      DeserializeBasicValue<int32_t>(restored["id"]) << std::endl;
  std::cout << "rating: " <<
      DeserializeBasicValue<double>(restored["rating"]) << std::endl;
  for (const auto& value : DeserializeStrVector(restored["medals"])) {
    std::cout << "medals: " << value << std::endl;
  }
  for (const auto& value :
           DeserializeBasicVector<int32_t>(restored["rivals"])) {
    std::cout << "rivals: " << value << std::endl;
  }
  for (const auto& value :
           DeserializeBasicVector<double>(restored["scores"])) {
    std::cout << "scores: " << value << std::endl;
  }
  
  // Closes the database.
  dbm.Close();

  return 0;
}

// END OF FILE
