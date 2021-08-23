/*************************************************************************************************
 * Example for basic usage of the asynchronous database
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

#include "tkrzw_dbm_async.h"
#include "tkrzw_dbm_poly.h"

// Main routine.
int main(int argc, char** argv) {
  // All symbols of Tkrzw are under the namespace "tkrzw".
  using namespace tkrzw;

  // Opens a new database.
  PolyDBM dbm;
  dbm.OpenAdvanced("casket.tkt", true, File::OPEN_TRUNCATE).OrDie();
  
  {
    // Makes an asynchronouns adapter with 10 worker threads.
    // We limit its scope to destroy it bofore the database instance.
    AsyncDBM async(&dbm, 10);

    // Sets records without checking the results of operations.
    async.Set("one", "hop");
    async.Set("two", "step");
    async.Set("three", "jump");

    // Retrieves a record value and evaluates the result.
    // The record may or may not be found due to scheduling.
    auto get_result = async.Get("three").get();
    std::cout << get_result.first << std::endl;
    if (get_result.first == Status::SUCCESS) {
      std::cout << get_result.second << std::endl;
    }

    // Prints progression while rebuilding the database.
    auto rebuild_future = async.Rebuild();
    do {
      std::cout << "Rebuilding the database" << std::endl;
    } while (rebuild_future.wait_for(
        std::chrono::seconds(1)) != std::future_status::ready);
    std::cout << rebuild_future.get() << std::endl;

    // Scans all records.
    std::string last_key = "";
    while (true) {
      // Retrieve 100 keys which are upper than the last key.
      auto [search_status, keys] =
          async.SearchModal("upper", last_key, 100).get();
      if (!search_status.IsOK() || keys.empty()) {
        break;
      }
      last_key = keys.back();
      // Retrieves and prints the values of the keys.
      auto [get_status, records] = async.GetMulti(keys).get();
      if (get_status.IsOK())  {
        for (const auto& record : records) {
          std::cout << record.first << ":" << record.second << std::endl;
        }
      }
    }
  }
  
  // Closes the database.
  dbm.Close().OrDie();

  return 0;
}

// END OF FILE
