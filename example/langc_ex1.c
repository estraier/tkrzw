/*************************************************************************************************
 * Example for the C language interface
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

#include <stdio.h>
#include "tkrzw_langc.h"

// Main routine.
int main(int argc, char** argv) {
  // Opens the database file.
  TkrzwDBM* dbm = tkrzw_dbm_open(
      "casket.tkh", true, "truncate=true,num_buckets=100");
  
  // Stores records.
  tkrzw_dbm_set(dbm, "foo", -1, "hop", -1, true);
  tkrzw_dbm_set(dbm, "bar", -1, "step", -1, true);
  tkrzw_dbm_set(dbm, "baz", -1, "jump", -1, true);

  // Retrieves a record.
  char* value_ptr = tkrzw_dbm_get(dbm, "foo", -1, NULL);
  if (value_ptr) {
    puts(value_ptr);
    free(value_ptr);
  }

  // Traverses records.
  TkrzwDBMIter* iter = tkrzw_dbm_make_iterator(dbm);
  tkrzw_dbm_iter_first(iter);
  while (true) {
    char* key_ptr = NULL;
    if (!tkrzw_dbm_iter_get(iter, &key_ptr, NULL, &value_ptr, NULL)) {
      break;
    }
    printf("%s:%s\n", key_ptr, value_ptr);
    free(key_ptr);
    free(value_ptr);
    tkrzw_dbm_iter_next(iter);
  }
  tkrzw_dbm_iter_free(iter);
  
  // Closes the database file.
  tkrzw_dbm_close(dbm);

  return 0;
}

// END OF FILE
