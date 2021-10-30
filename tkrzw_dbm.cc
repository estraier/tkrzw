/*************************************************************************************************
 * Datatabase manager interface
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

#include "tkrzw_dbm.h"
#include "tkrzw_file_util.h"
#include "tkrzw_lib_common.h"
#include "tkrzw_str_util.h"

namespace tkrzw {

const std::string_view DBM::ANY_DATA("\x00\xBA\xBE\x02\x11", 5);

const std::string_view DBM::RecordProcessor::NOOP("\x00\xBE\xEF\x02\x11", 5);

const std::string_view DBM::RecordProcessor::REMOVE("\x00\xDE\xAD\x02\x11", 5);

DBM::FileProcessorCopyFileData::FileProcessorCopyFileData(
    Status* status, const std::string dest_path)
    : status_(status), dest_path_(dest_path) {}

void DBM::FileProcessorCopyFileData::Process(const std::string& path) {
  *status_ = tkrzw::CopyFileData(path, dest_path_);
}

}  // namespace tkrzw

// END OF FILE
