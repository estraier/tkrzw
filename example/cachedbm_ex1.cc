/*************************************************************************************************
 * Example for basic usage of the on-memory cache database
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

#include "tkrzw_dbm_cache.h"

// Main routine.
int main(int argc, char** argv) {
  // All symbols of Tkrzw are under the namespace "tkrzw".
  using namespace tkrzw;

  // Creates the database manager.
  // No need to call the Open and Close methods.
  // The capacity is set with the constructor.
  constexpr int64_t cap_num_rec = 1000;
  constexpr int64_t cap_mem_size = cap_num_rec * 100;
  CacheDBM dbm(cap_num_rec, cap_mem_size);

  // Stores records.
  for (int32_t i = 0; i < 2000; i++) {
    dbm.Set(ToString(i), ToString(i));
  }

  // Check the number of records.
  std::cout << "count: " << dbm.CountSimple() << std::endl;

  // Recent records should be alive.
  for (int32_t i = 1990; i < 2000; i++) {
    std::cout << dbm.GetSimple(ToString(i), "*") << std::endl;
  }

  return 0;
}

// END OF FILE
