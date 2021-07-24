/*************************************************************************************************
 * Example for basic usage of the polymorphic database
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

#include "tkrzw_dbm_poly.h"

// Main routine.
int main(int argc, char** argv) {
  // All symbols of Tkrzw are under the namespace "tkrzw".
  using namespace tkrzw;

  // Creates the database manager.
  PolyDBM dbm;

  // Opens a new database.
  const std::map<std::string, std::string> open_params = {
    {"update_mode", "UPDATE_APPENDING"},
    {"max_page_size", "1000"}, {"max_branches", "128"},
    {"key_comparator", "DecimalKeyComparator"},
  };
  dbm.OpenAdvanced("casket.tkt", true, File::OPEN_TRUNCATE, open_params).OrDie();
  
  // Stores records.
  dbm.Set("1", "one").OrDie();
  dbm.Set("2", "two").OrDie();
  dbm.Set("3", "three").OrDie();

  // Rebuild the database.
  const std::map<std::string, std::string> rebuild_params = {
    {"update_mode", "UPDATE_IN_PLACE"},
    {"max_page_size", "4080"}, {"max_branches", "256"},
  };
  dbm.RebuildAdvanced(rebuild_params).OrDie();

  // Retrieves records.
  std::cout << dbm.GetSimple("1", "*") << std::endl;
  std::cout << dbm.GetSimple("2", "*") << std::endl;
  std::cout << dbm.GetSimple("3", "*") << std::endl;
  std::cout << dbm.GetSimple("4", "*") << std::endl;
  
  // Closes the database.
  dbm.Close().OrDie();

  return 0;
}

// END OF FILE
