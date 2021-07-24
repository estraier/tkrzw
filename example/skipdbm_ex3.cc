/*************************************************************************************************
 * Example for advanced operations of the skip database
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
#include "tkrzw_dbm_skip.h"
#include "tkrzw_str_util.h"

// Main routine.
int main(int argc, char** argv) {
  // All symbols of Tkrzw are under the namespace "tkrzw".
  using namespace tkrzw;

  // Creates the database manager.
  SkipDBM dbm;

  // Opens a new database,
  Status status = dbm.Open("casket.tks", true, File::OPEN_TRUNCATE);

  // Adds records.
  // Duplicated keys are allowed.
  dbm.Set("Japan", "Tokyo");
  dbm.Set("Japan", "Osaka");
  dbm.Set("France", "Paris");
  dbm.Set("China", "Beijing");

  // Synchronizes the database.
  // This makes new records visible.
  dbm.Synchronize(false);

  // Prints all records.
  PrintL("-- Original Records --");
  PrintDBMRecordsInTSV(&dbm);

  // Adds more records.
  dbm.Set("Japan", "Nagoya");
  dbm.Set("China", "Shanghai");

  // Removes a record.
  dbm.Remove("France");

  // Synchronizes the database.
  // This makes the updates visible.
  dbm.Synchronize(false);

  // Prints all records.
  PrintL("-- Records After Updates --");
  PrintDBMRecordsInTSV(&dbm);

  // Synchronizes the database with a reducer to deduplicate records.
  dbm.SynchronizeAdvanced(false, nullptr, SkipDBM::ReduceToFirst);
  
  // Prints all records.
  PrintL("-- Records After Deduplication --");
  PrintDBMRecordsInTSV(&dbm);

  // Closes the database.
  status = dbm.Close();

  return 0;
}

// END OF FILE
