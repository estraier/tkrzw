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

#include "tkrzw_key_comparators.h"

namespace tkrzw {

int32_t LexicalKeyComparator(std::string_view a, std::string_view b) {
  return a.compare(b);
}

int32_t LexicalCaseKeyComparator(std::string_view a, std::string_view b) {
  return StrCaseCompare(a, b);
}

int32_t DecimalKeyComparator(std::string_view a, std::string_view b) {
  const int64_t a_num = StrToInt(a);
  const int64_t b_num = StrToInt(b);
  return a_num < b_num ? -1 : (a_num > b_num ? 1 : 0);
}

int32_t HexadecimalKeyComparator(std::string_view a, std::string_view b) {
  const int64_t a_num = StrToIntHex(a);
  const int64_t b_num = StrToIntHex(b);
  return a_num < b_num ? -1 : (a_num > b_num ? 1 : 0);
}

int32_t RealNumberKeyComparator(std::string_view a, std::string_view b) {
  const double a_num = StrToDouble(a);
  const double b_num = StrToDouble(b);
  return a_num < b_num ? -1 : (a_num > b_num ? 1 : 0);
}

int32_t SignedBigEndianKeyComparator(std::string_view a, std::string_view b) {
  auto cast = [](std::string_view str) -> int64_t {
    uint64_t num = StrToIntBigEndian(str);
    switch(str.size()) {
      case sizeof(int8_t):
        num = static_cast<int8_t>(num);
        break;
      case sizeof(int16_t):
        num = static_cast<int16_t>(num);
        break;
      case sizeof(int32_t):
        num = static_cast<int32_t>(num);
        break;
    }
    return static_cast<int64_t>(num);
  };
  const int64_t a_num = cast(a);
  const int64_t b_num = cast(b);
  return a_num < b_num ? -1 : (a_num > b_num ? 1 : 0);
}

int32_t FloatBigEndianKeyComparator(std::string_view a, std::string_view b) {
  const long double a_num = StrToFloatBigEndian(a);
  const long double b_num = StrToFloatBigEndian(b);
  if (std::isnan(a_num)) return std::isnan(b_num) ? 0 : -1;
  if (std::isnan(b_num)) return 1;
  return a_num < b_num ? -1 : (a_num > b_num ? 1 : 0);
}

int32_t PairLexicalKeyComparator(std::string_view a, std::string_view b) {
  std::string_view a_key, a_value, b_key, b_value;
  DeserializeStrPair(a, &a_key, &a_value);
  DeserializeStrPair(b, &b_key, &b_value);
  const int32_t key_cmp = LexicalKeyComparator(a_key, b_key);
  if (key_cmp != 0) {
    return key_cmp;
  }
  return LexicalKeyComparator(a_value, b_value);
}

int32_t PairLexicalCaseKeyComparator(std::string_view a, std::string_view b) {
  std::string_view a_key, a_value, b_key, b_value;
  DeserializeStrPair(a, &a_key, &a_value);
  DeserializeStrPair(b, &b_key, &b_value);
  const int32_t key_cmp = LexicalCaseKeyComparator(a_key, b_key);
  if (key_cmp != 0) {
    return key_cmp;
  }
  return LexicalKeyComparator(a_value, b_value);
}

int32_t PairDecimalKeyComparator(std::string_view a, std::string_view b) {
  std::string_view a_key, a_value, b_key, b_value;
  DeserializeStrPair(a, &a_key, &a_value);
  DeserializeStrPair(b, &b_key, &b_value);
  const int32_t key_cmp = DecimalKeyComparator(a_key, b_key);
  if (key_cmp != 0) {
    return key_cmp;
  }
  return LexicalKeyComparator(a_value, b_value);
}

int32_t PairHexadecimalKeyComparator(std::string_view a, std::string_view b) {
  std::string_view a_key, a_value, b_key, b_value;
  DeserializeStrPair(a, &a_key, &a_value);
  DeserializeStrPair(b, &b_key, &b_value);
  const int32_t key_cmp = HexadecimalKeyComparator(a_key, b_key);
  if (key_cmp != 0) {
    return key_cmp;
  }
  return LexicalKeyComparator(a_value, b_value);
}

int32_t PairRealNumberKeyComparator(std::string_view a, std::string_view b) {
  std::string_view a_key, a_value, b_key, b_value;
  DeserializeStrPair(a, &a_key, &a_value);
  DeserializeStrPair(b, &b_key, &b_value);
  const int32_t key_cmp = RealNumberKeyComparator(a_key, b_key);
  if (key_cmp != 0) {
    return key_cmp;
  }
  return LexicalKeyComparator(a_value, b_value);
}

int32_t PairSignedBigEndianKeyComparator(std::string_view a, std::string_view b) {
  std::string_view a_key, a_value, b_key, b_value;
  DeserializeStrPair(a, &a_key, &a_value);
  DeserializeStrPair(b, &b_key, &b_value);
  const int32_t key_cmp = SignedBigEndianKeyComparator(a_key, b_key);
  if (key_cmp != 0) {
    return key_cmp;
  }
  return LexicalKeyComparator(a_value, b_value);
}

int32_t PairFloatBigEndianKeyComparator(std::string_view a, std::string_view b) {
  std::string_view a_key, a_value, b_key, b_value;
  DeserializeStrPair(a, &a_key, &a_value);
  DeserializeStrPair(b, &b_key, &b_value);
  const int32_t key_cmp = FloatBigEndianKeyComparator(a_key, b_key);
  if (key_cmp != 0) {
    return key_cmp;
  }
  return LexicalKeyComparator(a_value, b_value);
}

}  // namespace tkrzw

// END OF FILE