/*************************************************************************************************
 * Example for typical usage of the on-memory secondary index
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
#include "tkrzw_index.h"
#include "tkrzw_str_util.h"

// All symbols of Tkrzw are under the namespace "tkrzw".
using namespace tkrzw;

// Main routine.
int main(int argc, char** argv) {
  // Prepare an index for divisions and their members.
  MemIndex index;

  // Adds records.
  for (int32_t i = 0; i < 100; i++) {
    // The key don't have to be unique.
    const std::string key = ToString(i / 10);
    // The value must be unique because it is derived from the primary key of the main DB.
    const std::string value = ToString(i);
    // Adds a pair of key-value.
    index.Add(key, value);
  }

  // Makes an iterator.
  auto it = index.MakeIterator();
  // Jumps to the first record.
  it->First();
  // Shows all records.
  std::string key, value;
  while (it->Get(&key, &value)) {
    std::cout << key << ":" << value << std::endl;
    it->Next();
  }

  return 0;
}

// END OF FILE
