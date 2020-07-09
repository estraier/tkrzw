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

#ifndef _TKRZW_DBM_TREE_IMPL_H
#define _TKRZW_DBM_TREE_IMPL_H

#include <iostream>
#include <limits>
#include <set>
#include <string>
#include <string_view>
#include <vector>

#include <cinttypes>
#include <cstdarg>

#include "tkrzw_containers.h"
#include "tkrzw_dbm.h"
#include "tkrzw_file.h"
#include "tkrzw_key_comparators.h"
#include "tkrzw_lib_common.h"

namespace tkrzw {

/**
 * Key and value record structure in the file tree database.
 */
struct TreeRecord final {
  /** The size of the key. */
  int32_t key_size;
  /** The size of the value. */
  int32_t value_size;

  /**
   * Gets the key data.
   * @return The key data.
   */
  std::string_view GetKey() const;

  /**
   * Gets the value data.
   * @return The value data.
   */
  std::string_view GetValue() const;

  /**
   * Gets the serialized size.
   * @return The serialized size.
   */
  int32_t GetSerializedSize() const;
};

/**
 * Creates a tree record.
 * @param key The key data.
 * @param value The value data.
 * @return A new record object.
 */
TreeRecord* CreateTreeRecord(std::string_view key, std::string_view value);

/**
 * Modifies the value of a tree record.
 * @param record The record to modify.
 * @param new_value The new value data.
 * @return A modified record object, which might be reallocated.
 */
TreeRecord* ModifyTreeRecord(TreeRecord* record, std::string_view new_value);

/**
 * Frees the region of a tree record.
 * @param record The record to free.
 */
void FreeTreeRecord(TreeRecord* record);

/**
 * Frees the regions of tree records.
 * @param records A vector of the records to free.
 */
void FreeTreeRecords(std::vector<TreeRecord*>* records);

/**
 * Holder of TreeRecord on stack for search.
 */
struct TreeRecordOnStack final {
  /** The size of the stack buffer. */
  static constexpr int32_t STACK_BUFFER_SIZE = 256;
  /** The record object. */
  TreeRecord* record;
  /** The stack buffer. */
  char stack[STACK_BUFFER_SIZE];
  /** The actual buffer. */
  char* buffer;

  /**
   * Constructor.
   */
  explicit TreeRecordOnStack(std::string_view key);

  /**
   * Destructor.
   */
  ~TreeRecordOnStack();
};

/**
 * Comparator for TreeRecord objects.
 */
struct TreeRecordComparator final {
  /** The key comparator. */
  KeyComparator comp;

  /**
   * Constructor.
   */
  explicit TreeRecordComparator(KeyComparator comp) : comp(comp) {}

  /**
   * Comparing operator.
   */
  bool operator ()(const TreeRecord* const& a, const TreeRecord* const& b) const {
    return comp(a->GetKey(), b->GetKey()) < 0;
  }
};

/**
 * Link to a child node.
 */
struct TreeLink final {
  /** The size of the key. */
  int32_t key_size;
  /** The page ID of the child node. */
  int64_t child;

  /**
   * Gets the key data.
   * @return The key data.
   */
  std::string_view GetKey() const;

  /**
   * Gets the serialized size.
   * @param page_id_width The width of the page ID.
   * @return The serialized size.
   */
  int32_t GetSerializedSize(int32_t page_id_width) const;
};

/**
 * Creates a tree link.
 * @param key The key data.
 * @param child The page ID of the child node.
 * @return A new link object.
 */
TreeLink* CreateTreeLink(std::string_view key, int64_t child);

/**
 * Frees the region of a tree link.
 * @param link The link to free.
 */
void FreeTreeLink(TreeLink* link);

/**
 * Frees the regions of tree links.
 * @param links A vector of the links to free.
 */
void FreeTreeLinks(std::vector<TreeLink*>* links);

/**
 * Holder of TreeLink on stack for search.
 */
struct TreeLinkOnStack final {
  /** The size of the stack buffer. */
  static constexpr int32_t STACK_BUFFER_SIZE = 256;
  /** The link object. */
  TreeLink* link;
  /** The stack buffer. */
  char stack[STACK_BUFFER_SIZE];
  /** The actual buffer. */
  char* buffer;

  /**
   * Constructor.
   */
  explicit TreeLinkOnStack(std::string_view key);

  /**
   * Destructor.
   */
  ~TreeLinkOnStack();
};

/**
 * Comparator for TreeLink objects.
 */
struct TreeLinkComparator final {
  /** The key comparator. */
  KeyComparator comp;

  /**
   * Constructor.
   */
  explicit TreeLinkComparator(KeyComparator comp) : comp(comp) {}

  /**
   * Comparing operator.
   */
  bool operator ()(const TreeLink* const& a, const TreeLink* const& b) const {
    return comp(a->GetKey(), b->GetKey()) < 0;
  }
};

}  // namespace tkrzw

#endif  // _TKRZW_DBM_TREE_IMPL_H

// END OF FILE
