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
  dbm.Open("casket.tkh", true);

  // Makes a map representing structured data.
  std::map<std::string, std::string> record;
  record["name"] = "John Doe";
  record["salary"] = tkrzw::SerializeBasicValue<uint32_t>(98765);
  record["rating"] = tkrzw::SerializeBasicValue<double>(0.8);
  std::vector<std::string> medals = {"gold", "silver", "bronze"};
  record["medals"] = tkrzw::StrJoin(medals, std::string_view("\0", 1));
  std::vector<int32_t> accounts = {123456, 654321};
  record["lengths"] = tkrzw::SerializeBasicVector(accounts);
  std::vector<double> temps = {-15.2, 0.32, 102.4};
  record["temps"] = tkrzw::SerializeBasicVector(temps);

  // Stores the serialized string.
  dbm.Set("john", SerializeStrMap(record));

  // Retrieves the serialized string and restore it.
  std::string serialized;
  std::map<std::string, std::string> restored;
  if (dbm.Get("john", &serialized).IsOK()) {
    restored = DeserializeStrMap(serialized);
  }

  // Shows the restored content.
  std::cout << restored["name"] << std::endl;
  std::cout << tkrzw::DeserializeBasicValue<uint32_t>(restored["salary"]) << std::endl;
  std::cout << tkrzw::DeserializeBasicValue<double>(restored["rating"]) << std::endl;
  for (const auto& value : tkrzw::StrSplit(restored["medals"], std::string_view("\0", 1))) {
    std::cout << value << std::endl;
  }
  for (const auto& value : tkrzw::DeserializeBasicVector<int32_t>(restored["accounts"])) {
    std::cout << value << std::endl;
  }
  for (const auto& value : tkrzw::DeserializeBasicVector<double>(restored["temps"])) {
    std::cout << value << std::endl;
  }
  
  // Closes the database.
  dbm.Close();

  return 0;
}

// END OF FILE
