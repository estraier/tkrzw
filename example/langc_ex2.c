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
  TkrzwDBM* dbm = tkrzw_dbm_open("casket.tkt", true, "truncate=true");

  // Makes an asynchronouns adapter with 10 worker threads.
  TkrzwAsyncDBM* async = tkrzw_async_dbm_new(dbm, 10);
  
  // Begins an operation to set a records asynchronously.
  TkrzwFuture* set_future = tkrzw_async_dbm_set(async, "one", -1, "hop", -1, true);

  // Gets the result of the setting operation.
  // The status is set to the thread-local storage.
  tkrzw_future_get(set_future);

  // Checks the status of the last operation.
  const int32_t status_code = tkrzw_get_last_status_code();
  if (status_code == TKRZW_STATUS_SUCCESS) {
    printf("OK\n");
  } else {
    printf("Error: %s: %s\n",
           tkrzw_status_code_name(status_code), tkrzw_get_last_status_message());
  }

  // Releases the future object.
  tkrzw_future_free(set_future);

  // Sets records without checking the results of operations.
  tkrzw_future_free(tkrzw_async_dbm_set(async, "two", -1, "step", -1, true));
  tkrzw_future_free(tkrzw_async_dbm_set(async, "three", -1, "jump", -1, true));

  // Begins an operation to get a records value asynchronously.
  TkrzwFuture* get_future = tkrzw_async_dbm_get(async, "one", -1);

  // Gets the result of the getting operation.
  // The status is set to the thread-local storage.
  char* value_ptr = tkrzw_future_get_str(get_future, NULL);
  if (tkrzw_get_last_status_code() == TKRZW_STATUS_SUCCESS) {
    printf("%s\n", value_ptr);
  }
  free(value_ptr);
  tkrzw_future_free(get_future);

  // Releases the asynchronous adapter.
  tkrzw_async_dbm_free(async);
  
  // Closes the database.
  tkrzw_dbm_close(dbm);

  return 0;
}

// END OF FILE
