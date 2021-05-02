/*************************************************************************************************
 * Implementation components for the tree database manager
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
#include "tkrzw_dbm_tree_impl.h"
#include "tkrzw_file.h"

namespace tkrzw {

std::string_view TreeRecord::GetKey() const {
  const char* rp = reinterpret_cast<const char*>(this) + sizeof(*this);
  return std::string_view(rp, key_size);
}

std::string_view TreeRecord::GetValue() const {
  const char* rp = reinterpret_cast<const char*>(this) + sizeof(*this) + key_size;
  return std::string_view(rp, value_size);
}

int32_t TreeRecord::GetSerializedSize() const {
  return SizeVarNum(key_size) + key_size + SizeVarNum(value_size) + value_size;
}

TreeRecord* CreateTreeRecord(std::string_view key, std::string_view value) {
  TreeRecord* rec =
      static_cast<TreeRecord*>(xmalloc(sizeof(TreeRecord) + key.size() + value.size()));
  rec->key_size = key.size();
  rec->value_size = value.size();
  char* wp = reinterpret_cast<char*>(rec) + sizeof(*rec);
  std::memcpy(wp, key.data(), key.size());
  std::memcpy(wp + key.size(), value.data(), value.size());
  return rec;
}

TreeRecord* ModifyTreeRecord(TreeRecord* record, std::string_view new_value) {
  if (static_cast<int32_t>(new_value.size()) > record->value_size) {
    record = static_cast<TreeRecord*>(xrealloc(
        record, sizeof(TreeRecord) + record->key_size + new_value.size()));
  }
  record->value_size = new_value.size();
  char* wp = reinterpret_cast<char*>(record) + sizeof(*record) + record->key_size;
  std::memcpy(wp, new_value.data(), new_value.size());
  return record;
}

void FreeTreeRecord(TreeRecord* record) {
  xfree(record);
}

void FreeTreeRecords(std::vector<TreeRecord*>* records) {
  for (auto* rec : *records) {
    xfree(rec);
  }
}

TreeRecordOnStack::TreeRecordOnStack(std::string_view key) {
  const int32_t size = sizeof(TreeRecord) + key.size();
  buffer = size <= STACK_BUFFER_SIZE ? stack : new char[size];
  record = reinterpret_cast<TreeRecord*>(buffer);
  record->key_size = key.size();
  char* wp = reinterpret_cast<char*>(record) + sizeof(*record);
  std::memcpy(wp, key.data(), key.size());
}

TreeRecordOnStack::~TreeRecordOnStack() {
  if (buffer != stack) {
    delete[] buffer;
  }
}

std::string_view TreeLink::GetKey() const {
  const char* rp = reinterpret_cast<const char*>(this) + sizeof(*this);
  return std::string_view(rp, key_size);
}

int32_t TreeLink::GetSerializedSize(int32_t page_id_width) const {
  return SizeVarNum(key_size) + key_size + page_id_width;
}

TreeLink* CreateTreeLink(std::string_view key, int64_t child) {
  TreeLink* link = static_cast<TreeLink*>(xmalloc(sizeof(TreeLink) + key.size()));
  link->key_size = key.size();
  link->child = child;
  char* wp = reinterpret_cast<char*>(link) + sizeof(*link);
  std::memcpy(wp, key.data(), key.size());
  return link;
}

void FreeTreeLink(TreeLink* link) {
  xfree(link);
}

void FreeTreeLinks(std::vector<TreeLink*>* links) {
  for (auto* link : *links) {
    xfree(link);
  }
}

TreeLinkOnStack::TreeLinkOnStack(std::string_view key) {
  const int32_t size = sizeof(TreeLink) + key.size();
  buffer = size <= STACK_BUFFER_SIZE ? stack : new char[size];
  link = reinterpret_cast<TreeLink*>(buffer);
  link->key_size = key.size();
  char* wp = reinterpret_cast<char*>(link) + sizeof(*link);
  std::memcpy(wp, key.data(), key.size());
}

TreeLinkOnStack::~TreeLinkOnStack() {
  if (buffer != stack) {
    delete[] buffer;
  }
}

}  // namespace tkrzw

// END OF FILE
