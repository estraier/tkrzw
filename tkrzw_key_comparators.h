/*************************************************************************************************
 * Built-in comparators for record keys
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

#ifndef _TKRZW_COMPARATORS_H
#define _TKRZW_COMPARATORS_H

#include <atomic>
#include <memory>
#include <mutex>
#include <set>

#include <cinttypes>
#include <cstring>

#include "tkrzw_lib_common.h"
#include "tkrzw_str_util.h"

namespace tkrzw {

/**
 * Interfrace of comparator of record keys.
 * @details The fucntion returns -1 if the first parameter is less, 1 if the first parameter is
 * greater, and 0 if both are equivalent.
 */
typedef int32_t (*KeyComparator)(std::string_view, std::string_view);

/**
 * Key comparator in the lexical order.
 * @param a One key.
 * @param b The other key.
 * @return -1 if "a" is less, 1 if "a" is greater, and 0 if both are equivalent.
 */
inline int32_t LexicalKeyComparator(std::string_view a, std::string_view b) {
  return a.compare(b);
}

/**
 * Key comparator in the lexical order ignoring case.
 * @param a One key.
 * @param b The other key.
 * @return -1 if "a" is less, 1 if "a" is greater, and 0 if both are equivalent.
 */
inline int32_t LexicalCaseKeyComparator(std::string_view a, std::string_view b) {
  return StrCaseCompare(a, b);
}

/**
 * Key comparator in the order of the decimal integer numeric expressions.
 * @param a One key.
 * @param b The other key.
 * @return -1 if "a" is less, 1 if "a" is greater, and 0 if both are equivalent.
 */
inline int32_t DecimalKeyComparator(std::string_view a, std::string_view b) {
  const int64_t a_num = StrToInt(a);
  const int64_t b_num = StrToInt(b);
  return a_num < b_num ? -1 : (a_num > b_num ? 1 : 0);
}

/**
 * Key comparator in the order of the hexadecimal integer numeric expressions.
 * @param a One key.
 * @param b The other key.
 * @return -1 if "a" is less, 1 if "a" is greater, and 0 if both are equivalent.
 */
inline int32_t HexadecimalKeyComparator(std::string_view a, std::string_view b) {
  const int64_t a_num = StrToIntHex(a);
  const int64_t b_num = StrToIntHex(b);
  return a_num < b_num ? -1 : (a_num > b_num ? 1 : 0);
}

/**
 * Key comparator in the order of the decimal real number expressions.
 * @param a One key.
 * @param b The other key.
 * @return -1 if "a" is less, 1 if "a" is greater, and 0 if both are equivalent.
 */
inline int32_t RealNumberKeyComparator(std::string_view a, std::string_view b) {
  const double a_num = StrToDouble(a);
  const double b_num = StrToDouble(b);
  return a_num < b_num ? -1 : (a_num > b_num ? 1 : 0);
}

/**
 * Key comparator for serialized pair strings in the lexical order.
 * @param a One key.
 * @param b The other key.
 * @return -1 if "a" is less, 1 if "a" is greater, and 0 if both are equivalent.
 */
inline int32_t PairLexicalKeyComparator(std::string_view a, std::string_view b) {
  std::string_view a_key, a_value, b_key, b_value;
  DeserializeStrPair(a, &a_key, &a_value);
  DeserializeStrPair(b, &b_key, &b_value);
  const int32_t key_cmp = LexicalKeyComparator(a_key, b_key);
  if (key_cmp != 0) {
    return key_cmp;
  }
  return LexicalKeyComparator(a_value, b_value);
}

/**
 * Key comparator for serialized pair strings in the lexical order ignoring case.
 * @param a One key.
 * @param b The other key.
 * @return -1 if "a" is less, 1 if "a" is greater, and 0 if both are equivalent.
 */
inline int32_t PairLexicalCaseKeyComparator(std::string_view a, std::string_view b) {
  std::string_view a_key, a_value, b_key, b_value;
  DeserializeStrPair(a, &a_key, &a_value);
  DeserializeStrPair(b, &b_key, &b_value);
  const int32_t key_cmp = LexicalCaseKeyComparator(a_key, b_key);
  if (key_cmp != 0) {
    return key_cmp;
  }
  return LexicalKeyComparator(a_value, b_value);
}

/**
 * Key comparator for serialized pair strings in the decimal integer order.
 * @param a One key.
 * @param b The other key.
 * @return -1 if "a" is less, 1 if "a" is greater, and 0 if both are equivalent.
 */
inline int32_t PairDecimalKeyComparator(std::string_view a, std::string_view b) {
  std::string_view a_key, a_value, b_key, b_value;
  DeserializeStrPair(a, &a_key, &a_value);
  DeserializeStrPair(b, &b_key, &b_value);
  const int32_t key_cmp = DecimalKeyComparator(a_key, b_key);
  if (key_cmp != 0) {
    return key_cmp;
  }
  return LexicalKeyComparator(a_value, b_value);
}

/**
 * Key comparator for serialized pair strings in the hexadecimal integer order.
 * @param a One key.
 * @param b The other key.
 * @return -1 if "a" is less, 1 if "a" is greater, and 0 if both are equivalent.
 */
inline int32_t PairHexadecimalKeyComparator(std::string_view a, std::string_view b) {
  std::string_view a_key, a_value, b_key, b_value;
  DeserializeStrPair(a, &a_key, &a_value);
  DeserializeStrPair(b, &b_key, &b_value);
  const int32_t key_cmp = HexadecimalKeyComparator(a_key, b_key);
  if (key_cmp != 0) {
    return key_cmp;
  }
  return LexicalKeyComparator(a_value, b_value);
}

/**
 * Key comparator for serialized pair strings in the decimal real number order.
 * @param a One key.
 * @param b The other key.
 * @return -1 if "a" is less, 1 if "a" is greater, and 0 if both are equivalent.
 */
inline int32_t PairRealNumberKeyComparator(std::string_view a, std::string_view b) {
  std::string_view a_key, a_value, b_key, b_value;
  DeserializeStrPair(a, &a_key, &a_value);
  DeserializeStrPair(b, &b_key, &b_value);
  const int32_t key_cmp = RealNumberKeyComparator(a_key, b_key);
  if (key_cmp != 0) {
    return key_cmp;
  }
  return LexicalKeyComparator(a_value, b_value);
}

}  // namespace tkrzw

#endif  // _TKRZW_COMPARATORS_H

// END OF FILE
