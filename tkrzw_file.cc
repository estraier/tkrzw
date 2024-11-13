/*************************************************************************************************
 * File system utilities
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

#include "tkrzw_file.h"

namespace tkrzw {

std::string File::ReadSimple(int64_t off, size_t size) {
  std::string data(size, 0);
  if (Read(off, const_cast<char*>(data.data()), size) != Status::SUCCESS) {
    data.clear();
  }
  return data;
}

bool File::WriteSimple(int64_t off, std::string_view data) {
  return Write(off, data.data(), data.size()) == Status::SUCCESS;
}

int64_t File::AppendSimple(const std::string& data) {
  int64_t off = 0;
  return Append(data.data(), data.size(), &off) == Status::SUCCESS ? off : -1;
}

int64_t File::ExpandSimple(size_t inc_size) {
  int64_t old_size = 0;
  return Expand(inc_size, &old_size) == Status::SUCCESS ? old_size : -1;
}

int64_t File::GetSizeSimple() {
  int64_t size = 0;
  return GetSize(&size) == Status::SUCCESS ? size : -1;
}

std::string File::GetPathSimple() {
  std::string path;
  return GetPath(&path) == Status::SUCCESS ? path : "";
}

const std::type_info& File::GetType() const {
  const auto& entity = *this;
  return typeid(entity);
}

}  // namespace tkrzw

// END OF FILE