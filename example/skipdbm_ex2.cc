/*************************************************************************************************
 * Example for serious use cases of the skip database
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

// Main routine.
int main(int argc, char** argv) {
  // All symbols of Tkrzw are under the namespace "tkrzw".
  using namespace tkrzw;

  // Creates the database manager.
  SkipDBM dbm;

  // Tuning parameters.
  // Using a 4-byte integer for addressing.
  // Setting 1 in 3 records to have skip links.
  // Setting 8 as the maximum level of the skip list.
  // Assuming the input records are in ascending order.
  SkipDBM::TuningParameters tuning_params;
  tuning_params.offset_width = 4;
  tuning_params.step_unit = 3;
  tuning_params.max_level = 8;
  tuning_params.insert_in_order = true;
  
  // Opens a new database, OPEN_TRUNCATE means to clear the file content.
  // The result is returned as a Status object.
  Status status = dbm.OpenAdvanced(
      "casket.tks", true, File::OPEN_TRUNCATE, tuning_params);
  if (status != Status::SUCCESS) {
    // Failure of the Open operation is critical so we stop.
    Die("Open failed: ", status);
  }
  
  // Prepares records in ascending order of the key.
  // The in-order mode requires sorted records.
  std::map<std::string, std::string> input_records;
  input_records["foo"] = "hop";
  input_records["bar"] = "step";
  input_records["baz"] = "jump";

  // Stores records.
  for (const auto& input_record : input_records) {
    status = dbm.Set(input_record.first, input_record.second);
    if (status != Status::SUCCESS) {
      // The Set operation shouldn't fail.  So we stop if it happens.
      Die("Set failed: ", status);
    }
  }

  // Closes the database.
  status = dbm.Close();
  if (status != Status::SUCCESS) {
    // The Close operation shouldn't fail.  So we stop if it happens.
    Die("Close failed: ", status);
  }

  // Opens the existing database as a reader mode.
  status = dbm.Open("casket.tks", false);
  if (status != Status::SUCCESS) {
    // Failure of the Open operation is critical so we stop.
    Die("Open failed: ", status);
  }

  // Retrieves records.
  // If there was no record, NOT_FOUND_ERROR would be returned.
  std::string value;
  status = dbm.Get("foo", &value);
  if (status == Status::SUCCESS) {
    std::cout << value << std::endl;
  } else {
    std::cerr << "missing: " << status << std::endl;
  }

  // Traverses records with forward matching with "ba"
  std::unique_ptr<DBM::Iterator> iter = dbm.MakeIterator();
  status = iter->Jump("ba");
  if (status == Status::NOT_FOUND_ERROR) {
    // This could happen if there are no records equal to or greater than it.
    std::cerr << "missing: " << status << std::endl;
  } else if (status != Status::SUCCESS) {
    // Other errors are critical so we stop.
    Die("Jump failed: ", status);
  }
  while (true) {
    // Retrieves the current record data.
    std::string iter_key, iter_value;
    status = iter->Get(&iter_key, &iter_value);
    if (status == Status::SUCCESS) {
      if (!StrBeginsWith(iter_key, "ba")) break;
      std::cout << iter_key << ":" << iter_value << std::endl;
    } else {
      // This happens at the end of iteration.
      if (status != Status::NOT_FOUND_ERROR) {
        // Error types other than NOT_FOUND_ERROR are critical.
        Die("Iterator::Get failed: ", status);
      }
      break;
    }
    // Moves the iterator to the next record.
    status = iter->Next();
    if (status != Status::SUCCESS) {
      // This could happen if another thread cleared the database.
      if (status != Status::NOT_FOUND_ERROR) {
        // Error types other than NOT_FOUND_ERROR are critical.
        Die("Iterator::Get failed: ", status);
      }
      std::cerr << "missing: " << status << std::endl;
      break;
    }
  }

  // Closes the database.
  // Even if you forgot to close it, the destructor would close it.
  // However, checking the status is a good manner.
  status = dbm.Close();
  if (status != Status::SUCCESS) {
    // The Close operation shouldn't fail.  So we stop if it happens.
    Die("Close failed: ", status);
  }

  return 0;
}

// END OF FILE
