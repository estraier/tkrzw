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

#ifndef _TKRZW_TIME_UTIL_H
#define _TKRZW_TIME_UTIL_H

#include <string>
#include <string_view>

#include <cinttypes>

#include "tkrzw_lib_common.h"

namespace tkrzw {

/**
 * Gets the number of seconds since the UNIX epoch.
 * @return The number of seconds since the UNIX epoch with microsecond precision.
 */
double GetWallTime();

}  // namespace tkrzw

#endif  // _TKRZW_TIME_UTIL_H

// END OF FILE
