/*************************************************************************************************
 * Time utilities
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

#include "tkrzw_sys_config.h"

#include "tkrzw_lib_common.h"
#include "tkrzw_time_util.h"

namespace tkrzw {

double GetWallTime() {
  const auto epoch = std::chrono::time_point<std::chrono::system_clock>();
  const auto current = std::chrono::system_clock::now();
  const auto elapsed = std::chrono::duration_cast<std::chrono::microseconds>(current - epoch);
  return elapsed.count() / 1000000.0;
}

}  // namespace tkrzw

// END OF FILE
