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
#include <stdlib.h>
#include "tkrzw_langc.h"

// Main routine.
int main(int argc, char** argv) {
  // Opens the index file.
  TkrzwIndex* index = tkrzw_index_open("casket.tkt", true, "truncate=true,num_buckets=100");

  // Adds records to the index.
  // The key is a division name and the value is person name.
  tkrzw_index_add(index, "general", -1, "anne", -1);
  tkrzw_index_add(index, "general", -1, "matthew", -1);
  tkrzw_index_add(index, "general", -1, "marilla", -1);
  tkrzw_index_add(index, "sales", -1, "gilvert", -1);

  // Anne moves to the sales division.
  tkrzw_index_remove(index, "general", -1, "anne", -1);
  tkrzw_index_add(index, "sales", -1, "anne", -1);

  // Prints all members for each division.
  const char* divisions[] = {"general", "sales"};
  for (int i = 0; i < sizeof(divisions) / sizeof(*divisions); i++) {
    const char* division = divisions[i];
    printf("%s\n", division);
    int32_t num_members = 0;
    TkrzwStr* members = tkrzw_index_get_values(index, division, -1, 0, &num_members);
    for (int j = 0; j < num_members; j++) {
      printf(" -- %s\n", members[j].ptr);
    }
    tkrzw_free_str_array(members, num_members);
  }
  
  // Prints every record by iterator.
  TkrzwIndexIter* iter = tkrzw_index_make_iterator(index);
  tkrzw_index_iter_first(iter);
  char *key, *value;
  while (tkrzw_index_iter_get(iter, &key, NULL, &value, NULL)) {
    printf("%s: %s\n", key, value);
    free(value);
    free(key);
    tkrzw_index_iter_next(iter);
  }
  tkrzw_index_iter_free(iter);
  
  // Closes the index.
  tkrzw_index_close(index);

  return 0;
}

// END OF FILE
