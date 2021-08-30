/*************************************************************************************************
 * Example for serious use cases of the on-memory tree database
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
#include "tkrzw_dbm_baby.h"

// Main routine.
int main(int argc, char** argv) {
  // All symbols of Tkrzw are under the namespace "tkrzw".
  using namespace tkrzw;

  // Creates the database manager.
  // The key comparator can be set by the constructor.
  BabyDBM dbm(DecimalKeyComparator);

  // Opens a database by associating it with a file.
  // The result is returned as a Status object.
  Status status = dbm.Open("casket.flat", true, File::OPEN_TRUNCATE);
  if (status != Status::SUCCESS) {
    // Failure of the Open operation is critical so we stop.
    Die("Open failed: ", status);
  }
  
  // Stores records.
  // On-memory databases don't cause errors except for logical ones:
  // NOT_FOUND_ERROR and DUPLICATION_ERROR.
  dbm.Set("3", "hop");
  dbm.Set("20", "step");
  dbm.Set("100", "jump");

  // Closes the database.
  status = dbm.Close();
  if (status != Status::SUCCESS) {
    // The Close operation shouldn't fail.  So we stop if it happens.
    Die("Close failed: ", status);
  }

  // Opens the existing database as a reader mode.
  status = dbm.Open("casket.flat", false);
  if (status != Status::SUCCESS) {
    // Failure of the Open operation is critical so we stop.
    Die("Open failed: ", status);
  }

  // Iterate each record in reverse order.
  auto iter = dbm.MakeIterator();
  iter->Last();
  std::string key, value;
  while (iter->Get(&key, &value) == Status::SUCCESS) {
    std::cout << key << ":" << value << std::endl;
    iter->Previous();
  }

  // Closes the database.
  // In the reader mode, the file is not updated and no error occurs.
  dbm.Close();

  return 0;
}

// END OF FILE
