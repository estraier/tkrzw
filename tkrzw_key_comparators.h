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
int32_t LexicalKeyComparator(std::string_view a, std::string_view b);

/**
 * Key comparator in the lexical order ignoring case.
 * @param a One key.
 * @param b The other key.
 * @return -1 if "a" is less, 1 if "a" is greater, and 0 if both are equivalent.
 */
int32_t LexicalCaseKeyComparator(std::string_view a, std::string_view b);

/**
 * Key comparator in the order of decimal integer numeric expressions.
 * @param a One key.
 * @param b The other key.
 * @return -1 if "a" is less, 1 if "a" is greater, and 0 if both are equivalent.
 */
int32_t DecimalKeyComparator(std::string_view a, std::string_view b);

/**
 * Key comparator in the order of hexadecimal integer numeric expressions.
 * @param a One key.
 * @param b The other key.
 * @return -1 if "a" is less, 1 if "a" is greater, and 0 if both are equivalent.
 */
int32_t HexadecimalKeyComparator(std::string_view a, std::string_view b);

/**
 * Key comparator in the order of decimal real number expressions.
 * @param a One key.
 * @param b The other key.
 * @return -1 if "a" is less, 1 if "a" is greater, and 0 if both are equivalent.
 */
int32_t RealNumberKeyComparator(std::string_view a, std::string_view b);

/**
 * Key comparator in the order of big-endian binaries of signed integer numbers.
 * @param a One key.
 * @param b The other key.
 * @return -1 if "a" is less, 1 if "a" is greater, and 0 if both are equivalent.
 */
int32_t SignedBigEndianKeyComparator(std::string_view a, std::string_view b);

/**
 * Key comparator in the order of big-endian binaries of floating-point numbers.
 * @param a One key.
 * @param b The other key.
 * @return -1 if "a" is less, 1 if "a" is greater, and 0 if both are equivalent.
 */
int32_t FloatBigEndianKeyComparator(std::string_view a, std::string_view b);

/**
 * Key comparator for serialized pair strings in the lexical order.
 * @param a One key.
 * @param b The other key.
 * @return -1 if "a" is less, 1 if "a" is greater, and 0 if both are equivalent.
 */
int32_t PairLexicalKeyComparator(std::string_view a, std::string_view b);

/**
 * Key comparator for serialized pair strings in the lexical order ignoring case.
 * @param a One key.
 * @param b The other key.
 * @return -1 if "a" is less, 1 if "a" is greater, and 0 if both are equivalent.
 */
int32_t PairLexicalCaseKeyComparator(std::string_view a, std::string_view b);

/**
 * Key comparator for serialized pair strings in the decimal integer order.
 * @param a One key.
 * @param b The other key.
 * @return -1 if "a" is less, 1 if "a" is greater, and 0 if both are equivalent.
 */
int32_t PairDecimalKeyComparator(std::string_view a, std::string_view b);

/**
 * Key comparator for serialized pair strings in the hexadecimal integer order.
 * @param a One key.
 * @param b The other key.
 * @return -1 if "a" is less, 1 if "a" is greater, and 0 if both are equivalent.
 */
int32_t PairHexadecimalKeyComparator(std::string_view a, std::string_view b);

/**
 * Key comparator for serialized pair strings in the decimal real number order.
 * @param a One key.
 * @param b The other key.
 * @return -1 if "a" is less, 1 if "a" is greater, and 0 if both are equivalent.
 */
int32_t PairRealNumberKeyComparator(std::string_view a, std::string_view b);

/**
 * Key comparator for serialized pair strings in the big-endian binary signed integer order.
 * @param a One key.
 * @param b The other key.
 * @return -1 if "a" is less, 1 if "a" is greater, and 0 if both are equivalent.
 */
int32_t PairSignedBigEndianKeyComparator(std::string_view a, std::string_view b);

/**
 * Key comparator for serialized pair strings in the big-endian binary floating-point number order.
 * @param a One key.
 * @param b The other key.
 * @return -1 if "a" is less, 1 if "a" is greater, and 0 if both are equivalent.
 */
int32_t PairFloatBigEndianKeyComparator(std::string_view a, std::string_view b);

}  // namespace tkrzw

#endif  // _TKRZW_COMPARATORS_H

// END OF FILE
