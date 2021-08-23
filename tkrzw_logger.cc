/*************************************************************************************************
 * Logger interface and implementations
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
#include "tkrzw_logger.h"
#include "tkrzw_str_util.h"
#include "tkrzw_thread_util.h"
#include "tkrzw_time_util.h"

namespace tkrzw {

/*
void Logger::LogF(Level level, const char* format, ...) {
  assert(format != nullptr);
  std::string msg;
  va_list ap;
  va_start(ap, format);
  tkrzw::VSPrintF(&msg, format, ap);
  va_end(ap);
  std::cout << msg;
  std::cout.flush();
}
*/

}  // namespace tkrzw

// END OF FILE
