/*************************************************************************************************
 * Example for serious use cases of the hash database
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

// Main routine.
int main(int argc, char** argv) {
  // All symbols of Tkrzw are under the namespace "tkrzw".
  using namespace tkrzw;

  // Creates the database manager.
  HashDBM dbm;

  // Tuning parameters.
  // The update mode is in-place writing.
  // Using a 4-byte integer for addressing.
  // Using 3-bit = 8 byte unit alignment.
  // Using 7 buckets for the hash table.
  HashDBM::TuningParameters tuning_params;
  tuning_params.update_mode = HashDBM::UPDATE_IN_PLACE;
  tuning_params.offset_width = 4;
  tuning_params.align_pow = 3;
  tuning_params.num_buckets = 7;
  
  // Opens a new database, OPEN_TRUNCATE means to clear the file content.
  // The result is returned as a Status object.
  Status status = dbm.OpenAdvanced(
      "casket.tkh", true, File::OPEN_TRUNCATE, tuning_params);
  if (status != Status::SUCCESS) {
    // Failure of the Open operation is critical so we stop.
    Die("Open failed: ", status);
  }
  
  // Stores records.
  // Bit-or assignment to the status updates the status if the original
  // state is SUCCESS and the new state is an error.
  status |= dbm.Set("foo", "hop");
  status |= dbm.Set("bar", "step");
  status |= dbm.Set("baz", "jump");
  if (status != Status::SUCCESS) {
    // The Set operation shouldn't fail.  So we stop if it happens.
    Die("Set failed: ", status);
  }

  // Closes the database.
  status = dbm.Close();
  if (status != Status::SUCCESS) {
    // The Close operation shouldn't fail.  So we stop if it happens.
    Die("Close failed: ", status);
  }

  // Opens the existing database as a reader mode.
  status = dbm.Open("casket.tkh", false);
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

  // Traverses records.
  std::unique_ptr<DBM::Iterator> iter = dbm.MakeIterator();
  if (iter->First() != Status::SUCCESS) {
    // Failure of the First operation is critical so we stop.
    Die("First failed: ", status);
  }
  while (true) {
    // Retrieves the current record data.
    std::string iter_key, iter_value;
    status = iter->Get(&iter_key, &iter_value);
    if (status == Status::SUCCESS) {
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
      // This could happen if another thread removed the current record.
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
