/*************************************************************************************************
 * Simplest example code of an Tkrzw application
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

#include "tkrzw_dbm_hash.h"

int main(int argc, char** argv) {
  tkrzw::HashDBM dbm;
  dbm.Open("casket.tkh", true).OrDie();
  dbm.Set("hello", "world").OrDie();
  std::cout << dbm.GetSimple("hello") << std::endl;
  dbm.Close().OrDie();
  return 0;
}

// END OF FILE
