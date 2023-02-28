/*************************************************************************************************
 * String utilities
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
#include "tkrzw_str_util.h"

namespace tkrzw {

#if defined(_SYS_POSIX_) && !defined(_TKRZW_STDONLY)

inline void* tkrzw_memmem(const void* haystack, size_t haystacklen,
                          const void* needle, size_t needlelen) {
  return memmem(haystack, haystacklen, needle, needlelen);
}

#else

inline void* tkrzw_memmem(const void* haystack, size_t haystacklen,
                          const void* needle, size_t needlelen) {
  if (needlelen > haystacklen) {
    return nullptr;
  }
  const char* haystack_pivot = static_cast<const char*>(haystack);
  const char* haystack_end = haystack_pivot + haystacklen - needlelen + 1;
  const char* needle_end = static_cast<const char*>(needle) + needlelen;
  while (haystack_pivot < haystack_end) {
    const char* haystack_cursor = haystack_pivot;
    const char* needle_cursor = static_cast<const char*>(needle);
    bool match = true;
    while (needle_cursor < needle_end) {
      if (*(haystack_cursor++) != *(needle_cursor++)) {
        match = false;
        break;
      }
    }
    if (match) {
      return static_cast<void*>(const_cast<char*>(haystack_pivot));
    }
    haystack_pivot++;
  }
  return nullptr;
}

#endif

inline bool tkrzw_isalnum(char c) {
  return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z');
}

int64_t StrToInt(std::string_view str, int64_t defval) {
  const unsigned char* rp = reinterpret_cast<const unsigned char*>(str.data());
  const unsigned char* ep = rp + str.size();
  while (rp < ep && *rp <= ' ') {
    rp++;
  }
  int32_t sign = 1;
  if (rp < ep) {
    if (*rp == '-') {
      rp++;
      sign = -1;
    } else if (*rp == '+') {
      rp++;
    }
  }
  while (rp < ep && *rp <= ' ') {
    rp++;
  }
  bool has_number = false;
  int64_t num = 0;
  while (rp < ep) {
    if (*rp < '0' || *rp > '9') break;
    has_number = true;
    num = num * 10 + *rp - '0';
    rp++;
  }
  if (!has_number) {
    return defval;
  }
  return num * sign;
}

int64_t StrToIntMetric(std::string_view str, int64_t defval) {
  const unsigned char* rp = reinterpret_cast<const unsigned char*>(str.data());
  const unsigned char* ep = rp + str.size();
  while (rp < ep && *rp <= ' ') {
    rp++;
  }
  int32_t sign = 1;
  if (rp < ep) {
    if (*rp == '-') {
      rp++;
      sign = -1;
    } else if (*rp == '+') {
      rp++;
    }
  }
  while (rp < ep && *rp <= ' ') {
    rp++;
  }
  bool has_number = false;
  long double num = 0;
  while (rp < ep) {
    if (*rp < '0' || *rp > '9') break;
    has_number = true;
    num = num * 10 + *rp - '0';
    rp++;
  }
  if (rp < ep && *rp == '.') {
    rp++;
    long double base = 10;
    while (rp < ep) {
      if (*rp < '0' || *rp > '9') break;
      has_number = true;
      num += (*rp - '0') / base;
      rp++;
      base *= 10;
    }
  }
  if (!has_number) {
    return defval;
  }
  num *= sign;
  while (rp < ep && *rp <= ' ') {
    rp++;
  }
  if (rp < ep) {
    if (*rp == 'k' || *rp == 'K') {
      rp++;
      if (rp < ep && (*rp == 'i' || *rp == 'I')) {
        num *= 1LL << 10;
      } else {
        num *= 1LL * 1000LL;
      }
    } else if (*rp == 'm' || *rp == 'M') {
      rp++;
      if (rp < ep && (*rp == 'i' || *rp == 'I')) {
        num *= 1LL << 20;
      } else {
        num *= 1LL * 1000000LL;
      }
    } else if (*rp == 'g' || *rp == 'G') {
      rp++;
      if (rp < ep && (*rp == 'i' || *rp == 'I')) {
        num *= 1LL << 30;
      } else {
        num *= 1LL * 1000000000LL;
      }
    } else if (*rp == 't' || *rp == 'T') {
      rp++;
      if (rp < ep && (*rp == 'i' || *rp == 'I')) {
        num *= 1LL << 40;
      } else {
        num *= 1LL * 1000000000000LL;
      }
    } else if (*rp == 'p' || *rp == 'P') {
      rp++;
      if (rp < ep && (*rp == 'i' || *rp == 'I')) {
        num *= 1LL << 50;
      } else {
        num *= 1LL * 1000000000000000LL;
      }
    } else if (*rp == 'e' || *rp == 'E') {
      rp++;
      if (rp < ep && (*rp == 'i' || *rp == 'I')) {
        num *= 1LL << 60;
      } else {
        num *= 1LL * 1000000000000000000LL;
      }
    }
  }
  if (num > INT64MAX) return INT64MAX;
  if (num < INT64MIN) return INT64MIN;
  return static_cast<int64_t>(num);
}

uint64_t StrToIntOct(std::string_view str, uint64_t defval) {
  const unsigned char* rp = reinterpret_cast<const unsigned char*>(str.data());
  const unsigned char* ep = rp + str.size();
  while (rp < ep && *rp <= ' ') {
    rp++;
  }
  bool has_number = false;
  uint64_t num = 0;
  while (rp < ep) {
    if (*rp >= '0' && *rp <= '7') {
      num = num * 8 + *rp - '0';
    } else {
      break;
    }
    has_number = true;
    rp++;
  }
  if (!has_number) {
    return defval;
  }
  return num;
}

uint64_t StrToIntHex(std::string_view str, uint64_t defval) {
  const unsigned char* rp = reinterpret_cast<const unsigned char*>(str.data());
  const unsigned char* ep = rp + str.size();
  while (rp < ep && *rp <= ' ') {
    rp++;
  }
  if (rp < ep - 1 && rp[0] == '0' && (rp[1] == 'x' || rp[1] == 'X')) {
    rp += 2;
  }
  bool has_number = false;
  uint64_t num = 0;
  while (rp < ep) {
    if (*rp >= '0' && *rp <= '9') {
      num = num * 0x10 + *rp - '0';
    } else if (*rp >= 'a' && *rp <= 'f') {
      num = num * 0x10 + *rp - 'a' + 10;
    } else if (*rp >= 'A' && *rp <= 'F') {
      num = num * 0x10 + *rp - 'A' + 10;
    } else {
      break;
    }
    has_number = true;
    rp++;
  }
  if (!has_number) {
    return defval;
  }
  return num;
}

uint64_t StrToIntBigEndian(std::string_view str) {
  if (str.empty()) {
    return 0;
  }
  const size_t size = std::min(str.size(), sizeof(uint64_t));
  return ReadFixNum(str.data(), size);
}

double StrToDouble(std::string_view str, double defval) {
  const unsigned char* rp = reinterpret_cast<const unsigned char*>(str.data());
  const unsigned char* ep = rp + str.size();
  while (rp < ep && *rp <= ' ') {
    rp++;
  }
  int32_t sign = 1;
  if (rp < ep) {
    if (*rp == '-') {
      rp++;
      sign = -1;
    } else if (*rp == '+') {
      rp++;
    }
  }
  if (rp < ep - 2 &&
      (rp[0] == 'i' || rp[0] == 'I') && (rp[1] == 'n' || rp[1] == 'N') &&
      (rp[2] == 'f' || rp[2] == 'F')) {
    return HUGE_VAL * sign;
  }
  if (rp < ep - 2 &&
      (rp[0] == 'n' || rp[0] == 'N') && (rp[1] == 'a' || rp[1] == 'A') &&
      (rp[2] == 'n' || rp[2] == 'N')) {
    return DOUBLENAN;
  }
  while (rp < ep && *rp <= ' ') {
    rp++;
  }
  bool has_number = false;
  long double num = 0;
  int32_t col = 0;
  while (rp < ep) {
    if (*rp < '0' || *rp > '9') break;
    has_number = true;
    num = num * 10 + *rp - '0';
    rp++;
    if (num > 0) col++;
  }
  if (rp < ep && *rp == '.') {
    rp++;
    long double fract = 0.0;
    long double base = 10;
    while (col < 16 && rp < ep) {
      if (*rp < '0' || *rp > '9') break;
      has_number = true;
      fract += (*rp - '0') / base;
      rp++;
      col++;
      base *= 10;
    }
    num += fract;
  }
  if (!has_number) {
    return defval;
  }
  if (rp < ep && (*rp == 'e' || *rp == 'E')) {
    rp++;
    const std::string_view pow_expr(reinterpret_cast<const char*>(rp), ep - rp);
    num *= std::pow((long double)10, (long double)StrToInt(pow_expr));
  }
  return num * sign;
}

void VSPrintF(std::string* dest, const char* format, va_list ap) {
  assert(dest != nullptr && format != nullptr);
  while (*format != '\0') {
    if (*format == '%') {
      char cbuf[NUM_BUFFER_SIZE];
      cbuf[0] = '%';
      size_t cbsiz = 1;
      int32_t lnum = 0;
      format++;
      while (std::strchr("0123456789 .+-hlLz", *format) && *format != '\0' &&
             cbsiz < NUM_BUFFER_SIZE - 1) {
        if (*format == 'l' || *format == 'L') lnum++;
        cbuf[cbsiz++] = *(format++);
      }
      cbuf[cbsiz++] = *format;
      cbuf[cbsiz] = '\0';
      switch (*format) {
        case 's': {
          const char* tmp = va_arg(ap, const char*);
          if (tmp) {
            if (cbsiz == 2) {
              dest->append(tmp);
            } else {
              char tbuf[NUM_BUFFER_SIZE * 2];
              size_t tsiz = std::snprintf(tbuf, sizeof(tbuf), cbuf, tmp);
              dest->append(tbuf, tsiz);
            }
          } else {
            dest->append("(null)");
          }
          break;
        }
        case 'd': {
          char tbuf[NUM_BUFFER_SIZE];
          size_t tsiz;
          if (lnum >= 2) {
            tsiz = std::sprintf(tbuf, cbuf, va_arg(ap, long long));
          } else if (lnum >= 1) {
            tsiz = std::sprintf(tbuf, cbuf, va_arg(ap, long));
          } else {
            tsiz = std::sprintf(tbuf, cbuf, va_arg(ap, int));
          }
          dest->append(tbuf, tsiz);
          break;
        }
        case 'o': case 'u': case 'x': case 'X': case 'c': {
          char tbuf[NUM_BUFFER_SIZE];
          size_t tsiz;
          if (lnum >= 2) {
            tsiz = std::sprintf(tbuf, cbuf, va_arg(ap, unsigned long long));
          } else if (lnum >= 1) {
            tsiz = std::sprintf(tbuf, cbuf, va_arg(ap, unsigned long));
          } else {
            tsiz = std::sprintf(tbuf, cbuf, va_arg(ap, unsigned int));
          }
          dest->append(tbuf, tsiz);
          break;
        }
        case 'e': case 'E': case 'f': case 'g': case 'G': {
          char tbuf[NUM_BUFFER_SIZE * 2];
          size_t tsiz;
          if (lnum >= 1) {
            tsiz = std::snprintf(tbuf, sizeof(tbuf), cbuf, va_arg(ap, long double));
          } else {
            tsiz = std::snprintf(tbuf, sizeof(tbuf), cbuf, va_arg(ap, double));
          }
          if (tsiz > sizeof(tbuf)) {
            tbuf[sizeof(tbuf)-1] = '*';
            tsiz = sizeof(tbuf);
          }
          dest->append(tbuf, tsiz);
          break;
        }
        case 'p': {
          char tbuf[NUM_BUFFER_SIZE];
          size_t tsiz = std::sprintf(tbuf, "%p", va_arg(ap, void*));
          dest->append(tbuf, tsiz);
          break;
        }
        case '%': {
          dest->push_back('%');
          break;
        }
      }
    } else {
      dest->append(format, 1);
    }
    format++;
  }
}

std::string ToString(double data) {
  char buf[NUM_BUFFER_SIZE];
  int32_t size = std::sprintf(buf, "%.6f", data);
  while (size > 0 && buf[size - 1] == '0') {
    buf[size--] = '\0';
  }
  if (size > 0 && buf[size - 1] == '.') {
    buf[size--] = '\0';
  }
  return std::string(buf, size);
}

bool StrToBool(std::string_view str, bool defval) {
  const std::string& lower = StrLowerCase(StrStripSpace(str));
  if (lower == "true" || lower == "t" || lower == "yes" || lower == "y" || lower == "1") {
    return true;
  }
  if (lower == "false" || lower == "f" || lower == "no" || lower == "n" || lower == "0") {
    return false;
  }
  return defval;
}

int64_t StrToIntOrBool(std::string_view str, int64_t defval) {
  const std::string& lower = StrLowerCase(StrStripSpace(str));
  if (lower == "true" || lower == "t" || lower == "yes" || lower == "y" || lower == "1") {
    return 1;
  }
  if (lower == "false" || lower == "f" || lower == "no" || lower == "n" || lower == "0") {
    return 0;
  }
  return StrToInt(str, defval);
}

void SPrintF(std::string* dest, const char* format, ...) {
  assert(dest != nullptr && format != nullptr);
  va_list ap;
  va_start(ap, format);
  VSPrintF(dest, format, ap);
  va_end(ap);
}

std::string SPrintF(const char* format, ...) {
  assert(format != nullptr);
  std::string str;
  va_list ap;
  va_start(ap, format);
  VSPrintF(&str, format, ap);
  va_end(ap);
  return str;
}

std::string IntToStrBigEndian(uint64_t data, size_t size) {
  if (size < 1) {
    return "";
  }
  size = std::min(size, sizeof(data));
  std::string str(size, 0);
  WriteFixNum(const_cast<char*>(str.data()), data, size);
  return str;
}

std::vector<std::string> StrSplit(std::string_view str, char delim, bool skip_empty) {
  std::vector<std::string> segments;
  segments.reserve(2);
  size_t i = 0;
  while (i < str.size()) {
    const size_t pos = str.find(delim, i);
    if (pos == std::string::npos) {
      break;
    }
    if (!skip_empty || i < pos) {
      segments.emplace_back(str.substr(i, pos - i));
    }
    i = pos + 1;
  }
  if (!skip_empty || i < str.size()) {
    segments.emplace_back(str.substr(i));
  }
  return segments;
}

std::vector<std::string> StrSplit(std::string_view str, std::string_view delim,
                                  bool skip_empty) {
  std::vector<std::string> segments;
  if (delim.empty()) {
    segments.reserve(str.size());
    for (const char c : str) {
      segments.emplace_back(std::string(1, c));
    }
  } else {
    segments.reserve(2);
    size_t i = 0;
    while (i < str.size()) {
      const size_t pos = str.find(delim, i);
      if (pos == std::string::npos) {
        break;
      }
      if (!skip_empty || i < pos) {
        segments.emplace_back(str.substr(i, pos - i));
      }
      i = pos + delim.size();
    }
    if (!skip_empty || i < str.size()) {
      segments.emplace_back(str.substr(i));
    }
  }
  return segments;
}


std::vector<std::string> StrSplitAny(std::string_view str, std::string_view delims,
                                     bool skip_empty) {
  std::vector<std::string> segments;
  segments.reserve(2);
  size_t i = 0;
  while (i < str.size()) {
    const size_t pos = str.find_first_of(delims, i);
    if (pos == std::string::npos) {
      break;
    }
    if (!skip_empty || i < pos) {
      segments.emplace_back(str.substr(i, pos - i));
    }
    i = pos + 1;
  }
  if (!skip_empty || i < str.size()) {
    segments.emplace_back(str.substr(i));
  }
  return segments;
}

std::map<std::string, std::string> StrSplitIntoMap(
    std::string_view str, std::string_view delim_records, std::string_view delim_kv) {
  std::map<std::string, std::string> map;
  for (const auto& record : StrSplit(str, delim_records, true)) {
    const size_t pos = record.find(delim_kv);
    if (pos != std::string::npos) {
      map.emplace(record.substr(0, pos), record.substr(pos + delim_kv.size()));
    }
  }
  return map;
}

std::string StrUpperCase(std::string_view str) {
  std::string converted;
  converted.reserve(str.size());
  for (int32_t c : str) {
    if (c >= 'a' && c <= 'z') {
      c -= 'a' - 'A';
    }
    converted.push_back(c);
  }
  return converted;
}

void StrUpperCase(std::string* str) {
  assert(str != nullptr);
  for (auto it = str->begin(); it != str->end(); ++it) {
    int32_t c = *it;
    if (c >= 'a' && c <= 'z') {
      *it = c - ('a' - 'A');
    }
  }
}

std::string StrLowerCase(std::string_view str) {
  std::string converted;
  converted.reserve(str.size());
  for (int32_t c : str) {
    if (c <= 'Z' && c >= 'A') {
      c += 'a' - 'A';
    }
    converted.push_back(c);
  }
  return converted;
}

void StrLowerCase(std::string* str) {
  assert(str != nullptr);
  for (auto it = str->begin(); it != str->end(); ++it) {
    int32_t c = *it;
    if (c <= 'Z' && c >= 'A') {
      *it = c + ('a' - 'A');
    }
  }
}

std::string StrReplace(std::string_view str, std::string_view before, std::string_view after) {
  if (before.size() > str.size() || before.empty()) {
    return std::string(str);
  }
  std::string result;
  result.reserve(str.size());
  const size_t end = str.size() - before.size() + 1;
  size_t i = 0;
  while (i < end) {
    bool match = true;
    for (size_t j = 0; j < before.size(); j++) {
      if (str[i + j] != before[j]) {
        match = false;
        break;
      }
    }
    if (match) {
      result.append(after);
      i += before.size();
    } else {
      result.append(1, str[i]);
      i++;
    }
  }
  if (i < str.size()) {
    result.append(str.substr(i));
  }
  return result;
}

void StrReplaceCharacters(std::string* str, std::string_view before, std::string_view after) {
  size_t out_size = 0;
  for (size_t in_size = 0; in_size < str->size(); in_size++) {
    const char c = (*str)[in_size];
    size_t match_index = std::string::npos;
    for (size_t i = 0; i < before.size(); i++) {
      if (c == before[i]) {
        match_index = i;
        break;
      }
    }
    if (match_index == std::string::npos) {
      (*str)[out_size++] = c;
    } else if (match_index < after.size()) {
      (*str)[out_size++] = after[match_index];
    }
  }
  str->resize(out_size);
}

bool StrContains(std::string_view text, std::string_view pattern) {
  return tkrzw_memmem(text.data(), text.size(), pattern.data(), pattern.size()) != nullptr;
}

bool StrCaseContains(std::string_view text, std::string_view pattern) {
  return StrCaseSearch(text, pattern) >= 0;
}

bool StrWordContains(std::string_view text, std::string_view pattern) {
  return StrWordSearch(text, pattern) >= 0;
}

bool StrCaseWordContains(std::string_view text, std::string_view pattern) {
  return StrCaseWordSearch(text, pattern) >= 0;
}

bool StrContainsBatch(std::string_view text, const std::vector<std::string>& patterns) {
  for (const auto& pattern : patterns) {
    if (tkrzw_memmem(text.data(), text.size(), pattern.data(), pattern.size()) != nullptr) {
      return true;
    }
  }
  return false;
}

bool StrCaseContainsBatch(std::string_view text, const std::vector<std::string>& patterns) {
  for (const auto& pattern : patterns) {
    if (StrCaseSearch(text, pattern) >= 0) {
      return true;
    }
  }
  return false;
}

bool StrWordContainsBatch(std::string_view text, const std::vector<std::string>& patterns) {
  for (const auto& pattern : patterns) {
    if (StrWordSearch(text, pattern) >= 0) {
      return true;
    }
  }
  return false;
}

bool StrCaseWordContainsBatch(std::string_view text, const std::vector<std::string>& patterns) {
  for (const auto& pattern : patterns) {
    if (StrCaseWordSearch(text, pattern) >= 0) {
      return true;
    }
  }
  return false;
}

bool StrCaseWordContainsBatchLower(
    std::string_view text, const std::vector<std::string>& patterns) {
  constexpr size_t STACK_BUFFER_SIZE = 512;
  char stack_buffer[STACK_BUFFER_SIZE];
  char* buffer = stack_buffer;
  if (text.size() > STACK_BUFFER_SIZE) {
    buffer = static_cast<char*>(xmalloc(text.size()));
  }
  char* wp = buffer;
  const char* rp = text.data();
  const char* ep = rp + text.size();
  while (rp < ep) {
    const char c = *(rp++);
    if (c <= 'Z' && c >= 'A') {
      *(wp++) = c + 'a' - 'A';
    } else {
      *(wp++) = c;
    }
  }
  for (const auto& pattern : patterns) {
    if (pattern.size() == 0) {
      if (buffer != stack_buffer) {
        xfree(buffer);
      }
      return true;
    }
    rp = buffer;
    size_t size = text.size();
    while (size > 0) {
      const char* pv =
          (const char*)tkrzw_memmem((void*)rp, size, pattern.data(), pattern.size());
      if (!pv) break;
      if (pv != buffer && tkrzw_isalnum(*(pv - 1)) && tkrzw_isalnum(*pv)) {
        rp = pv + 1;
        size = text.size() - (rp - buffer);
        continue;
      }
      if (pv + pattern.size() != rp + size &&
          tkrzw_isalnum(*(pv + pattern.size())) && tkrzw_isalnum(*(pv + pattern.size() - 1))) {
        rp = pv + 1;
        size = text.size() - (rp - buffer);
        continue;
      }
      if (buffer != stack_buffer) {
        xfree(buffer);
      }
      return true;
    }
  }
  if (buffer != stack_buffer) {
    xfree(buffer);
  }
  return false;
}

bool StrBeginsWith(std::string_view text, std::string_view pattern) {
  if (pattern.size() > text.size()) {
    return false;
  }
  return std::memcmp(text.data(), pattern.data(), pattern.size()) == 0;
}

bool StrEndsWith(std::string_view text, std::string_view pattern) {
  if (pattern.size() > text.size()) {
    return false;
  }
  return std::memcmp(text.data() + text.size() - pattern.size(),
                     pattern.data(), pattern.size()) == 0;
}

int32_t StrCaseCompare(std::string_view a, std::string_view b) {
  const int32_t length = std::min(a.size(), b.size());
  for (int32_t i = 0; i < length; i++) {
    int32_t ac = static_cast<unsigned char>(a[i]);
    if (ac <= 'Z' && ac >= 'A') {
      ac += 'a' - 'A';
    }
    int32_t bc = static_cast<unsigned char>(b[i]);
    if (bc <= 'Z' && bc >= 'A') {
      bc += 'a' - 'A';
    }
    if (ac != bc) {
      return ac < bc ? -1 : 1;
    }
  }
  if (a.size() < b.size()) {
    return -1;
  }
  if (a.size() > b.size()) {
    return 1;
  }
  return 0;
}

int32_t StrSearch(std::string_view text, std::string_view pattern) {
  return text.find(pattern);
}

int32_t StrSearchDoubleLoop(std::string_view text, std::string_view pattern) {
  if (pattern.size() > text.size()) {
    return -1;
  }
  const size_t text_end = text.size() - pattern.size() + 1;
  for (size_t text_index = 0; text_index < text_end; text_index++) {
    size_t pattern_index = 0;
    while (pattern_index < pattern.size() &&
           text[text_index + pattern_index] == pattern[pattern_index]) {
      pattern_index++;
    }
    if (pattern_index == pattern.size()) {
      return text_index;
    }
  }
  return -1;
}

int32_t StrSearchMemchr(std::string_view text, std::string_view pattern) {
  if (pattern.empty()) {
    return 0;
  }
  const int32_t first_pattern_char = pattern.front();
  const char* pattern_data = pattern.data() + 1;
  const int32_t pattern_size = pattern.size() - 1;
  const char* current_pointer = text.data();
  int32_t size = static_cast<int32_t>(text.size()) - static_cast<int32_t>(pattern.size()) + 1;
  while (size > 0) {
    const char* match_pointer =
        static_cast<const char*>(std::memchr(current_pointer, first_pattern_char, size));
    if (match_pointer == nullptr) {
      break;
    }
    if (memcmp(match_pointer + 1, pattern_data, pattern_size) == 0) {
      return match_pointer - text.data();
    }
    match_pointer++;
    size -= match_pointer - current_pointer;
    current_pointer = match_pointer;
  }
  return -1;
}

int32_t StrSearchMemmem(std::string_view text, std::string_view pattern) {
  const void* result =
      tkrzw_memmem(text.data(), text.size(), pattern.data(), pattern.size());
  if (result == nullptr) {
    return -1;
  }
  return static_cast<const char*>(result) - text.data();
}

int32_t StrSearchKMP(std::string_view text, std::string_view pattern) {
  if (pattern.empty()) {
    return 0;
  }
  std::vector<int32_t> table(pattern.size() + 1);
  table[0] = -1;
  size_t pattern_index = 0;
  int32_t offset = -1;
  while (pattern_index < pattern.size()) {
    while (offset >= 0 && pattern[pattern_index] != pattern[offset]) {
      offset = table[offset];
    }
    pattern_index++;
    offset++;
    table[pattern_index] = offset;
  }
  size_t text_index = 0;
  offset = 0;
  while (text_index < text.size() &&
         offset < static_cast<int32_t>(pattern.size())) {
    while (offset >= 0 && text[text_index] != pattern[offset]) {
      offset = table[offset];
    }
    text_index++;
    offset++;
  }
  if (offset == static_cast<int32_t>(pattern.size())) {
    return text_index - pattern.size();
  }
  return -1;
}

int32_t StrSearchBM(std::string_view text, std::string_view pattern) {
  if (pattern.empty()) {
    return 0;
  }
  int32_t table[UINT8MAX];
  for (int32_t table_index = 0; table_index < UINT8MAX; table_index++) {
    table[table_index] = pattern.size();
  }
  int32_t shift = pattern.size();
  int32_t pattern_index = 0;
  while (shift > 0) {
    const uint8_t table_index = pattern[pattern_index++];
    table[table_index] = --shift;
  }
  int32_t pattern_end = pattern.size() - 1;
  int32_t begin_index = 0;
  int32_t end_index = text.size() - pattern_end;
  while (begin_index < end_index) {
    int32_t pattern_index = pattern_end;
    while (text[begin_index + pattern_index] == pattern[pattern_index]) {
      if (pattern_index == 0) {
        return begin_index;
      }
      pattern_index--;
    }
    const uint8_t table_index = text[begin_index + pattern_index];
    const int32_t step = table[table_index] - pattern_end + pattern_index;
    begin_index += step > 0 ? step : 2;
  }
  return -1;
}

int32_t StrSearchRK(std::string_view text, std::string_view pattern) {
  if (pattern.empty()) {
    return 0;
  }
  const unsigned char* text_p = reinterpret_cast<const unsigned char*>(text.data());
  const unsigned char* pattern_p = reinterpret_cast<const unsigned char*>(pattern.data());
  constexpr int32_t base = 239;
  constexpr int32_t modulo = 1798201;
  int32_t power = 1;
  for (size_t i = 0; i < pattern.size(); i++) {
    power = (power * base) % modulo;
  }
  int32_t pattern_hash = 0;
  for (size_t i = 0; i < pattern.size(); i++) {
    pattern_hash = pattern_hash * base + pattern_p[i];
    pattern_hash %= modulo;
  }
  int32_t text_hash = 0;
  for (size_t i = 0; i < text.size(); i++) {
    text_hash = text_hash * base + text_p[i];
    text_hash %= modulo;
    if (i >= pattern.size()) {
      text_hash -= power * text_p[i - pattern.size()] % modulo;
      if (text_hash < 0) {
        text_hash += modulo;
      }
    }
    if (pattern_hash == text_hash && i >= pattern.size() - 1) {
      const size_t offset = i - (pattern.size() - 1);
      if (std::memcmp(text_p + offset, pattern_p, pattern.size()) == 0) {
        return offset;
      }
    }
  }
  return -1;
}

int32_t StrSearchZ(std::string_view text, std::string_view pattern) {
  std::string concat;
  concat.reserve(pattern.size() + 1 + text.size());
  concat.append(pattern);
  concat.push_back('\0');
  concat.append(text);
  std::vector<size_t> z(concat.size(), 0);
  size_t left = 0;
  size_t right = 0;
  for (size_t i = 1; i < concat.size(); i++) {
    if (i > right) {
      left = i;
      right = i;
      while (right < concat.size() && concat[right - left] == concat[right]) {
        right++;
      }
      z[i] = right - left;
      right--;
    } else {
      size_t k = i - left;
      if (z[k] < right - i + 1) {
        z[i] = z[k];
      } else {
        left = i;
        while (right < concat.size() && concat[right - left] == concat[right]) {
          right++;
        }
        z[i] = right - left;
        right--;
      }
    }
  }
  for (size_t i = pattern.size() + 1; i < concat.size(); i++) {
    if (z[i] == pattern.size()) {
      return i - 1 - pattern.size();
    }
  }
  return -1;
}

std::vector<int32_t> StrSearchWhole(
    std::string_view text, std::string_view pattern, size_t max_results) {
  std::vector<int32_t> result;
  if (pattern.empty()) {
    const size_t num_results = max_results > 0 ? std::min(max_results, text.size()) : text.size();
    result.reserve(num_results);
    for (size_t i = 0; i < num_results; ++i) {
      result.emplace_back(i);
    }
    return result;
  }
  size_t pos = 0;
  while (pos != text.size()) {
    pos = text.find(pattern, pos);
    if (pos == std::string::npos) {
      break;
    }
    result.emplace_back(pos);
    if (max_results > 0 && result.size() >= max_results) {
      return result;
    }
    pos++;
  }
  return result;
}

std::vector<int32_t> StrSearchWholeKMP(
    std::string_view text, std::string_view pattern, size_t max_results) {
  std::vector<int32_t> result;
  if (pattern.empty()) {
    const size_t num_results = max_results > 0 ? std::min(max_results, text.size()) : text.size();
    result.reserve(num_results);
    for (size_t i = 0; i < num_results; ++i) {
      result.emplace_back(i);
    }
    return result;
  }
  std::vector<int32_t> table(pattern.size() + 1);
  table[0] = -1;
  size_t pattern_index = 0;
  int32_t offset = -1;
  while (pattern_index < pattern.size()) {
    while (offset >= 0 && pattern[pattern_index] != pattern[offset]) {
      offset = table[offset];
    }
    pattern_index++;
    offset++;
    table[pattern_index] = offset;
  }
  size_t text_index = 0;
  while (text_index < text.size()) {
    offset = 0;
    while (text_index < text.size() &&
           offset < static_cast<int32_t>(pattern.size())) {
      while (offset >= 0 && text[text_index] != pattern[offset]) {
        offset = table[offset];
      }
      text_index++;
      offset++;
    }
    if (offset == static_cast<int32_t>(pattern.size())) {
      const size_t offset = text_index - pattern.size();
      result.emplace_back(offset);
      if (max_results > 0 && result.size() >= max_results) {
        return result;
      }
      text_index = offset + 1;
    }
  }
  return result;
}

std::vector<int32_t> StrSearchWholeBM(
    std::string_view text, std::string_view pattern, size_t max_results) {
  std::vector<int32_t> result;
  if (pattern.empty()) {
    const size_t num_results = max_results > 0 ? std::min(max_results, text.size()) : text.size();
    result.reserve(num_results);
    for (size_t i = 0; i < num_results; ++i) {
      result.emplace_back(i);
    }
    return result;
  }
  int32_t table[UINT8MAX];
  for (int32_t table_index = 0; table_index < UINT8MAX; table_index++) {
    table[table_index] = pattern.size();
  }
  int32_t shift = pattern.size();
  int32_t pattern_index = 0;
  while (shift > 0) {
    const uint8_t table_index = pattern[pattern_index++];
    table[table_index] = --shift;
  }
  int32_t pattern_end = pattern.size() - 1;
  int32_t begin_index = 0;
  int32_t end_index = text.size() - pattern_end;
  while (begin_index < end_index) {
    int32_t pattern_index = pattern_end;
    bool hit = false;
    while (text[begin_index + pattern_index] == pattern[pattern_index]) {
      if (pattern_index == 0) {
        result.emplace_back(begin_index);
        if (max_results > 0 && result.size() >= max_results) {
          return result;
        }
        hit = true;
        break;
      }
      pattern_index--;
    }
    const uint8_t table_index = text[begin_index + pattern_index];
    const int32_t step = table[table_index] - pattern_end + pattern_index;
    begin_index += step > 0 ? step : (hit ? 1 : 2);
  }
  return result;
}

std::vector<int32_t> StrSearchWholeRK(
    std::string_view text, std::string_view pattern, size_t max_results) {
  std::vector<int32_t> result;
  if (pattern.empty()) {
    const size_t num_results = max_results > 0 ? std::min(max_results, text.size()) : text.size();
    result.reserve(num_results);
    for (size_t i = 0; i < num_results; ++i) {
      result.emplace_back(i);
    }
    return result;
  }
  const unsigned char* text_p = reinterpret_cast<const unsigned char*>(text.data());
  const unsigned char* pattern_p = reinterpret_cast<const unsigned char*>(pattern.data());
  constexpr int32_t base = 239;
  constexpr int32_t modulo = 1798201;
  int32_t power = 1;
  for (size_t i = 0; i < pattern.size(); i++) {
    power = (power * base) % modulo;
  }
  int32_t pattern_hash = 0;
  for (size_t i = 0; i < pattern.size(); i++) {
    pattern_hash = (pattern_hash * base + pattern_p[i]) % modulo;
  }
  int32_t text_hash = 0;
  for (size_t i = 0; i < text.size(); i++) {
    text_hash = (text_hash * base + text_p[i]) % modulo;
    if (i >= pattern.size()) {
      text_hash -= power * text_p[i - pattern.size()] % modulo;
      if (text_hash < 0) {
        text_hash += modulo;
      }
    }
    if (pattern_hash == text_hash && i >= pattern.size() - 1) {
      const size_t offset = i - (pattern.size() - 1);
      if (std::memcmp(text_p + offset, pattern_p, pattern.size()) == 0) {
        result.emplace_back(offset);
        if (max_results > 0 && result.size() >= max_results) {
          return result;
        }
      }
    }
  }
  return result;
}

std::vector<std::vector<int32_t>> StrSearchBatch(
    std::string_view text, const std::vector<std::string>& patterns, size_t max_results) {
  std::vector<std::vector<int32_t>> result;
  result.resize(patterns.size());
  for (size_t i = 0; i < patterns.size(); i++) {
    result[i] = StrSearchWhole(text, patterns[i], max_results);
  }
  return result;
}

std::vector<std::vector<int32_t>> StrSearchBatchKMP(
    std::string_view text, const std::vector<std::string>& patterns, size_t max_results) {
  std::vector<std::vector<int32_t>> result;
  result.resize(patterns.size());
  for (size_t i = 0; i < patterns.size(); i++) {
    result[i] = StrSearchWholeKMP(text, patterns[i], max_results);
  }
  return result;
}

std::vector<std::vector<int32_t>> StrSearchBatchBM(
    std::string_view text, const std::vector<std::string>& patterns, size_t max_results) {
  std::vector<std::vector<int32_t>> result;
  result.resize(patterns.size());
  for (size_t i = 0; i < patterns.size(); i++) {
    result[i] = StrSearchWholeBM(text, patterns[i], max_results);
  }
  return result;
}

std::vector<std::vector<int32_t>> StrSearchBatchRK(
    std::string_view text, const std::vector<std::string>& patterns, size_t max_results) {
  std::vector<std::vector<int32_t>> result(patterns.size());
  const unsigned char* text_p = reinterpret_cast<const unsigned char*>(text.data());
  std::map<size_t, std::vector<size_t>> batch;
  for (size_t i = 0; i < patterns.size(); i++) {
    batch[patterns[i].size()].emplace_back(i);
  }
  for (const auto& batch_item : batch) {
    const size_t pattern_size = batch_item.first;
    const auto& pattern_indices = batch_item.second;
    if (pattern_size < 1) {
      for (const auto& pattern_index : pattern_indices) {
        for (size_t i = 0; i < text.size(); i++) {
          result[pattern_index].emplace_back(i);
        }
      }
      continue;
    }
    constexpr int32_t base = 239;
    constexpr int32_t modulo = 1798201;
    int32_t power = 1;
    for (size_t i = 0; i < pattern_size; i++) {
      power = (power * base) % modulo;
    }
    std::vector<std::pair<int32_t, size_t>> pattern_hashes;
    for (const auto& pattern_index : pattern_indices) {
      const auto& pattern = patterns[pattern_index];
      const unsigned char* pattern_p = reinterpret_cast<const unsigned char*>(pattern.data());
      int32_t pattern_hash = 0;
      for (size_t i = 0; i < pattern_size; i++) {
        const int32_t c = i < pattern.size() ? pattern_p[i] : 0;
        pattern_hash = (pattern_hash * base + c) % modulo;
      }
      pattern_hashes.emplace_back(std::make_pair(pattern_hash, pattern_index));
    }
    int32_t text_hash = 0;
    for (size_t i = 0; i < text.size(); i++) {
      text_hash = (text_hash * base + text_p[i]) % modulo;
      if (i >= pattern_size) {
        text_hash -= power * text_p[i - pattern_size] % modulo;
        if (text_hash < 0) {
          text_hash += modulo;
        }
      }
      for (const auto& pattern_hash : pattern_hashes) {
        const auto& pattern = patterns[pattern_hash.second];
        if (pattern_hash.first == text_hash) {
          if (i >= pattern.size() - 1) {
            const size_t offset = i - (pattern.size() - 1);
            if (std::memcmp(text_p + offset, pattern.data(), pattern.size()) == 0) {
              if (max_results > 0) {
                auto& pat_result = result[pattern_hash.second];
                if (pat_result.size() < max_results) {
                  pat_result.emplace_back(offset);
                }
              } else {
                result[pattern_hash.second].emplace_back(offset);
              }
            }
          }
        }
      }
    }
  }
  return result;
}

int32_t StrCaseSearch(std::string_view text, std::string_view pattern) {
  if (pattern.size() > text.size()) {
    return -1;
  }
  if (pattern.empty()) {
    return 0;
  }
  constexpr size_t STACK_BUFFER_SIZE = 512;
  char stack_buffer[STACK_BUFFER_SIZE];
  char* buffer = stack_buffer;
  if (pattern.size() > STACK_BUFFER_SIZE) {
    buffer = static_cast<char*>(xmalloc(pattern.size()));
  }
  for (size_t pi = 0; pi < pattern.size(); pi++) {
    char pc = pattern[pi];
    if (pc <= 'Z' && pc >= 'A') {
      pc += 'a' - 'A';
    }
    buffer[pi] = pc;
  }
  int32_t rv = -1;
  size_t end = text.size() - pattern.size() + 1;
  for (size_t ti = 0; ti < end; ti++) {
    size_t pi = 0;
    for (; pi < pattern.size(); pi++) {
      char tc = text[ti + pi];
      if (tc <= 'Z' && tc >= 'A') {
        tc += 'a' - 'A';
      }
      if (tc != buffer[pi]) {
        break;
      }
    }
    if (pi == pattern.size()) {
      rv = ti;
      break;
    }
  }
  if (buffer != stack_buffer) {
    xfree(buffer);
  }
  return rv;
}

int32_t StrWordSearch(std::string_view text, std::string_view pattern) {
  if (pattern.size() > text.size()) {
    return -1;
  }
  if (pattern.empty()) {
    return 0;
  }
  const char* rp = text.data();
  size_t size = text.size();
  while (size > 0) {
    const char* pv =
        (const char*)tkrzw_memmem((void*)rp, size, pattern.data(), pattern.size());
    if (!pv) break;
    if (pv != text.data() && tkrzw_isalnum(*(pv - 1)) && tkrzw_isalnum(*pv)) {
      rp = pv + 1;
      size = text.size() - (rp - text.data());
      continue;
    }
    if (pv + pattern.size() != rp + size &&
        tkrzw_isalnum(*(pv + pattern.size())) && tkrzw_isalnum(*(pv + pattern.size() - 1))) {
      rp = pv + 1;
      size = text.size() - (rp - text.data());
      continue;
    }

    return pv - text.data();
  }
  return -1;
}

int32_t StrCaseWordSearch(std::string_view text, std::string_view pattern) {
  if (pattern.size() > text.size()) {
    return -1;
  }
  if (pattern.empty()) {
    return 0;
  }
  constexpr size_t STACK_BUFFER_SIZE = 512;
  char stack_buffer[STACK_BUFFER_SIZE];
  char* buffer = stack_buffer;
  if (pattern.size() > STACK_BUFFER_SIZE) {
    buffer = static_cast<char*>(xmalloc(pattern.size()));
  }
  for (size_t pi = 0; pi < pattern.size(); pi++) {
    char pc = pattern[pi];
    if (pc <= 'Z' && pc >= 'A') {
      pc += 'a' - 'A';
    }
    buffer[pi] = pc;
  }
  int32_t rv = -1;
  size_t end = text.size() - pattern.size() + 1;
  for (size_t ti = 0; ti < end; ti++) {
    size_t pi = 0;
    for (; pi < pattern.size(); pi++) {
      char tc = text[ti + pi];
      if (tc <= 'Z' && tc >= 'A') {
        tc += 'a' - 'A';
      }
      if (tc != buffer[pi]) {
        break;
      }
    }
    if (pi == pattern.size()) {
      if (ti != 0 && tkrzw_isalnum(text[ti - 1]) && tkrzw_isalnum(text[ti])) {
        continue;
      }
      if (ti != end - 1 && tkrzw_isalnum(text[ti + pattern.size()]) &&
          tkrzw_isalnum(text[ti + pattern.size() - 1])) {
        continue;
      }
      rv = ti;
      break;
    }
  }
  if (buffer != stack_buffer) {
    xfree(buffer);
  }
  return rv;
}

std::string StrStripSpace(std::string_view str) {
  std::string converted;
  converted.reserve(str.size());
  int32_t content_size = 0;
  for (size_t i = 0; i < str.size(); i++) {
    const int32_t c = static_cast<unsigned char>(str[i]);
    if (c <= ' ') {
      if (converted.empty()) {
        continue;
      }
      converted.push_back(c);
    } else {
      converted.push_back(c);
      content_size = converted.size();
    }
  }
  converted.resize(content_size);
  return converted;
}

std::string StrStripLine(std::string_view str) {
  size_t size = str.size();
  while (size > 0) {
    const int32_t c = str[size - 1];
    if (c != '\r' && c != '\n') {
      break;
    }
    size--;
  }
  return std::string(str.data(), size);
}

void StrStripLine(std::string* str) {
  assert(str != nullptr);
  while (!str->empty()) {
    const int32_t c = str->back();
    if (c != '\r' && c != '\n') {
      break;
    }
    str->resize(str->size() - 1);
  }
}

std::string StrSqueezeAndStripSpace(std::string_view str) {
  std::string converted;
  converted.reserve(str.size());
  int32_t content_size = 0;
  bool last_is_space = true;
  for (size_t i = 0; i < str.size(); i++) {
    const int32_t c = static_cast<unsigned char>(str[i]);
    if (c <= ' ') {
      if (last_is_space) {
        continue;
      }
      converted.push_back(c);
      last_is_space = true;
    } else {
      converted.push_back(c);
      last_is_space = false;
      content_size = converted.size();
    }
  }
  converted.resize(content_size);
  return converted;
}

std::string StrTrimForTSV(std::string_view str, bool keep_tab) {
  std::string converted;
  converted.reserve(str.size());
  for (size_t i = 0; i < str.size(); i++) {
    int32_t c = static_cast<unsigned char>(str[i]);
    if (c <= ' ' || c == 0x7f) {
      if (c != '\t' || !keep_tab) {
        c = ' ';
      }
    }
    converted.push_back(c);
  }
  return converted;
}

std::string StrEscapeC(std::string_view str, bool esc_nonasc) {
  std::string converted;
  converted.reserve(str.size() * 2);
  const uint8_t* rp = reinterpret_cast<const uint8_t*>(str.data());
  const uint8_t* ep = rp + str.size();
  while (rp < ep) {
    const int32_t c = *rp;
    switch (c) {
      case '\0': {
        if (rp == ep - 1) {
          converted.append("\\0");
        } else {
          const int32_t n = *(rp + 1);
          if (n >= '0' && n <= '7') {
            converted.append("\\x00");
          } else {
            converted.append("\\0");
          }
        }
        break;
      }
      case '\a': converted.append("\\a"); break;
      case '\b': converted.append("\\b"); break;
      case '\t': converted.append("\\t"); break;
      case '\n': converted.append("\\n"); break;
      case '\v': converted.append("\\v"); break;
      case '\f': converted.append("\\f"); break;
      case '\r': converted.append("\\r"); break;
      case '\\': converted.append("\\\\"); break;
      default: {
        if ((c >= 0 && c < ' ') || c == 0x7F) {
          converted.append(SPrintF("\\x%02x", c));
        } else if (esc_nonasc && (c >= 0x80)) {
          converted.append(SPrintF("\\x%02x", c));
        } else {
          converted.push_back(c);
        }
        break;
      }
    }
    rp++;
  }
  return converted;
}

std::string StrUnescapeC(std::string_view str) {
  std::string converted;
  converted.reserve(str.size());
  for (size_t i = 0; i < str.size();) {
    int32_t c = str[i];
    if (c == '\\') {
      i++;
      if (i >= str.size()) {
        break;
      }
      c = str[i];
      switch (c) {
        case 'a': i++; converted.push_back('\a'); break;
        case 'b': i++; converted.push_back('\b'); break;
        case 't': i++; converted.push_back('\t'); break;
        case 'n': i++; converted.push_back('\n'); break;
        case 'v': i++; converted.push_back('\v'); break;
        case 'f': i++; converted.push_back('\f'); break;
        case 'r': i++; converted.push_back('\r'); break;
        case 'x': case 'X': {
          i++;
          int32_t num = 0;
          const size_t end = std::min(i + 2, str.size());
          while (i < end) {
            c = str[i];
            if (c >= '0' && c <= '9') {
              num = num * 16 + c - '0';
            } else if (c >= 'A' && c <= 'F') {
              num = num * 16 + c - 'A' + 10;
            } else if (c >= 'a' && c <= 'f') {
              num = num * 16 + c - 'a' + 10;
            } else {
              break;
            }
            i++;
          }
          converted.push_back(num);
          break;
        }
        default:
          if (c >= '0' && c <= '8') {
            i++;
            int32_t num = c - '0';
            const size_t end = std::min(i + 2, str.size());
            while (i < end) {
              c = str[i];
              if (c >= '0' && c <= '7') {
                num = num * 8 + c - '0';
              } else {
                break;
              }
              i++;
            }
            converted.push_back(num);
          } else {
            converted.push_back(c);
            i++;
          }
          break;
      }
    } else {
      converted.push_back(c);
      i++;
    }
  }
  return converted;
}

std::string StrEncodeBase64(std::string_view str) {
  const char* const table = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
  const unsigned char* rp = reinterpret_cast<const unsigned char*>(str.data());
  std::string encoded;
  encoded.reserve(str.size() * 4 / 3 + 4);
  for (size_t i = 0; i < str.size(); i += 3) {
    switch (str.size() - i) {
      case 1: {
        encoded.push_back(table[rp[0] >> 2]);
        encoded.push_back(table[(rp[0] & 3) << 4]);
        encoded.push_back('=');
        encoded.push_back('=');
        break;
      }
      case 2: {
        encoded.push_back(table[rp[0] >> 2]);
        encoded.push_back(table[((rp[0] & 3) << 4) + (rp[1] >> 4)]);
        encoded.push_back(table[(rp[1] & 0xf) << 2]);
        encoded.push_back('=');
        break;
      }
      default: {
        encoded.push_back(table[rp[0] >> 2]);
        encoded.push_back(table[((rp[0] & 3) << 4) + (rp[1] >> 4)]);
        encoded.push_back(table[((rp[1] & 0xf) << 2) + (rp[2] >> 6)]);
        encoded.push_back(table[rp[2] & 0x3f]);
        break;
      }
    }
    rp += 3;
  }
  return encoded;
}

std::string StrDecodeBase64(std::string_view str) {
  size_t index = 0;
  size_t num_equals = 0;
  std::string decoded;
  decoded.reserve(str.size());
  while (index < str.size() && num_equals == 0) {
    size_t bits = 0;
    size_t step = 0;
    while (index < str.size() && step < 4) {
      if (str[index] >= 'A' && str[index] <= 'Z') {
        bits = (bits << 6) | (str[index] - 'A');
        step++;
      } else if (str[index] >= 'a' && str[index] <= 'z') {
        bits = (bits << 6) | (str[index] - 'a' + 26);
        step++;
      } else if (str[index] >= '0' && str[index] <= '9') {
        bits = (bits << 6) | (str[index] - '0' + 52);
        step++;
      } else if (str[index] == '+') {
        bits = (bits << 6) | 62;
        step++;
      } else if (str[index] == '/') {
        bits = (bits << 6) | 63;
        step++;
      } else if (str[index] == '=') {
        bits <<= 6;
        step++;
        num_equals++;
      }
      index++;
    }
    if (step == 0 && index >= str.size()) continue;
    switch (num_equals) {
      case 0: {
        decoded.push_back((bits >> 16) & 0xff);
        decoded.push_back((bits >> 8) & 0xff);
        decoded.push_back(bits & 0xff);;
        break;
      }
      case 1: {
        decoded.push_back((bits >> 16) & 0xff);
        decoded.push_back((bits >> 8) & 0xff);
        break;
      }
      case 2: {
        decoded.push_back((bits >> 16) & 0xff);
        break;
      }
    }
  }
  return decoded;
}

std::string StrEncodeURL(std::string_view str) {
  const unsigned char* rp = reinterpret_cast<const unsigned char*>(str.data());
  std::string encoded;
  encoded.reserve(str.size() * 2);
  for (const unsigned char* ep = rp + str.size(); rp < ep; rp++) {
    int32_t c = *rp;
    if ((c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') ||
        (c >= '0' && c <= '9') || (c != '\0' && std::strchr("_-.~", c))) {
      encoded.push_back(c);
    } else {
      encoded.push_back('%');
      int32_t num = c >> 4;
      if (num < 10) {
        encoded.push_back('0' + num);
      } else {
        encoded.push_back('a' + num - 10);
      }
      num = c & 0x0f;
      if (num < 10) {
        encoded.push_back('0' + num);
      } else {
        encoded.push_back('a' + num - 10);
      }
    }
  }
  return encoded;
}

std::string StrDecodeURL(std::string_view str) {
  std::string decoded;
  decoded.reserve(str.size());
  const char *rp = str.data();
  const char* ep = rp + str.size();
  while (rp < ep) {
    int32_t c = *rp;
    if (c == '%') {
      int32_t num = 0;
      if (++rp >= ep) break;
      c = *rp;
      if (c >= '0' && c <= '9') {
        num = c - '0';
      } else if (c >= 'a' && c <= 'f') {
        num = c - 'a' + 10;
      } else if (c >= 'A' && c <= 'F') {
        num = c - 'A' + 10;
      }
      if (++rp >= ep) break;
      c = *rp;
      if (c >= '0' && c <= '9') {
        num = num * 0x10 + c - '0';
      } else if (c >= 'a' && c <= 'f') {
        num = num * 0x10 + c - 'a' + 10;
      } else if (c >= 'A' && c <= 'F') {
        num = num * 0x10 + c - 'A' + 10;
      }
      decoded.push_back(num);
      rp++;
    } else if (c == '+') {
      decoded.push_back(' ');
      rp++;
    } else if (c <= ' ' || c == 0x7f) {
      rp++;
    } else {
      decoded.push_back(c);
      rp++;
    }
  }
  return decoded;
}

int32_t StrSearchRegex(std::string_view text, std::string_view pattern) {
  int32_t pos = -1;
  try {
    std::regex_constants::syntax_option_type options
        = static_cast<std::regex_constants::syntax_option_type>(0);
    if (pattern.size() >= 2 && pattern[0] == '(' && pattern[1] == '?') {
      bool ended = false;
      size_t pos = 2;
      while (!ended && pos < pattern.size()) {
        switch (pattern[pos]) {
          case 'a':
            options |= std::regex_constants::awk;
            break;
          case 'b':
            options |= std::regex_constants::basic;
            break;
          case 'e':
            options |= std::regex_constants::extended;
            break;
          case 'i':
            options |= std::regex_constants::icase;
            break;
          case 'l':
            options |= std::regex_constants::egrep;
            break;
          case ')':
            ended = true;
            break;
          default:
            throw std::regex_error(std::regex_constants::error_complexity);
        }
        pos++;
      }
      if (!ended) {
        throw std::regex_error(std::regex_constants::error_complexity);
      }
      pattern = pattern.substr(pos);
    }
    std::regex regex(pattern.begin(), pattern.end(), options);
    std::match_results<std::string_view::const_iterator> matched;
    if (std::regex_search(text.begin(), text.end(), matched, regex)) {
      return matched.position(0);
    }
  } catch (const std::regex_error&) {
    return -2;
  }
  return pos;
}

std::string StrReplaceRegex(std::string_view text, std::string_view pattern,
                            std::string_view replace) {
  std::string result;
  try {
    std::regex_constants::syntax_option_type options
        = static_cast<std::regex_constants::syntax_option_type>(0);
    if (pattern.size() >= 2 && pattern[0] == '(' && pattern[1] == '?') {
      bool ended = false;
      size_t pos = 2;
      while (!ended && pos < pattern.size()) {
        switch (pattern[pos]) {
          case 'a':
            options |= std::regex_constants::awk;
            break;
          case 'b':
            options |= std::regex_constants::basic;
            break;
          case 'e':
            options |= std::regex_constants::extended;
            break;
          case 'i':
            options |= std::regex_constants::icase;
            break;
          case 'l':
            options |= std::regex_constants::egrep;
            break;
          case ')':
            ended = true;
            break;
          default:
            throw std::regex_error(std::regex_constants::error_complexity);
        }
        pos++;
      }
      if (!ended) {
        throw std::regex_error(std::regex_constants::error_complexity);
      }
      pattern = pattern.substr(pos);
    }
    std::regex regex(pattern.begin(), pattern.end(), options);
    std::regex_replace(std::back_inserter(result), text.begin(), text.end(), regex,
                       std::string(replace));
  } catch (const std::regex_error&) {
    result = "";
  }
  return result;
}

std::vector<uint32_t> ConvertUTF8ToUCS4(std::string_view utf) {
  std::vector<uint32_t> ucs;
  ucs.reserve(utf.size());
  const size_t size = utf.size();
  size_t index = 0;
  while (index < size) {
    uint32_t c = (unsigned char)utf[index];
    if (c < 0x80) {
      ucs.emplace_back(c);
    } else if (c < 0xe0) {
      if (c >= 0xc0 && index + 1 < size) {
        c = ((c & 0x1f) << 6) | (utf[index+1] & 0x3f);
        if (c >= 0x80) ucs.emplace_back(c);
        index++;
      }
    } else if (c < 0xf0) {
      if (index + 2 < size) {
        c = ((c & 0x0f) << 12) | ((utf[index+1] & 0x3f) << 6) | (utf[index+2] & 0x3f);
        if (c >= 0x800) ucs.emplace_back(c);
        index += 2;
      }
    } else if (c < 0xf8) {
      if (index + 3 < size) {
        c = ((c & 0x07) << 18) | ((utf[index+1] & 0x3f) << 12) | ((utf[index+2] & 0x3f) << 6) |
            (utf[index+3] & 0x3f);
        if (c >= 0x10000) ucs.emplace_back(c);
        index += 3;
      }
    } else if (c < 0xfc) {
      if (index + 4 < size) {
        c = ((c & 0x03) << 24) | ((utf[index+1] & 0x3f) << 18) | ((utf[index+2] & 0x3f) << 12) |
            ((utf[index+3] & 0x3f) << 6) | (utf[index+4] & 0x3f);
        if (c >= 0x200000) ucs.emplace_back(c);
        index += 4;
      }
    } else if (c < 0xfe) {
      if (index + 5 < size) {
        c = ((c & 0x01) << 30) | ((utf[index+1] & 0x3f) << 24) | ((utf[index+2] & 0x3f) << 18) |
            ((utf[index+3] & 0x3f) << 12) | ((utf[index+4] & 0x3f) << 6) | (utf[index+5] & 0x3f);
        if (c >= 0x4000000) ucs.emplace_back(c);
        index += 5;
      }
    }
    index++;
  }
  return ucs;
}

std::string ConvertUCS4ToUTF8(const std::vector<uint32_t>& ucs) {
  std::string utf;
  utf.reserve(ucs.size() * 3);
  std::vector<uint32_t>::const_iterator it = ucs.begin();
  std::vector<uint32_t>::const_iterator itend = ucs.end();
  while (it != itend) {
    const uint32_t c = *it;
    if (c < 0x80) {
      utf.push_back(c);
    } else if (c < 0x800) {
      utf.push_back(0xc0 | (c >> 6));
      utf.push_back(0x80 | (c & 0x3f));
    } else if (c < 0x10000) {
      utf.push_back(0xe0 | (c >> 12));
      utf.push_back(0x80 | ((c & 0xfff) >> 6));
      utf.push_back(0x80 | (c & 0x3f));
    } else if (c < 0x200000) {
      utf.push_back(0xf0 | (c >> 18));
      utf.push_back(0x80 | ((c & 0x3ffff) >> 12));
      utf.push_back(0x80 | ((c & 0xfff) >> 6));
      utf.push_back(0x80 | (c & 0x3f));
    } else if (c < 0x4000000) {
      utf.push_back(0xf8 | (c >> 24));
      utf.push_back(0x80 | ((c & 0xffffff) >> 18));
      utf.push_back(0x80 | ((c & 0x3ffff) >> 12));
      utf.push_back(0x80 | ((c & 0xfff) >> 6));
      utf.push_back(0x80 | (c & 0x3f));
    } else if (c < 0x80000000) {
      utf.push_back(0xfc | (c >> 30));
      utf.push_back(0x80 | ((c & 0x3fffffff) >> 24));
      utf.push_back(0x80 | ((c & 0xffffff) >> 18));
      utf.push_back(0x80 | ((c & 0x3ffff) >> 12));
      utf.push_back(0x80 | ((c & 0xfff) >> 6));
      utf.push_back(0x80 | (c & 0x3f));
    }
    ++it;
  }
  return utf;
}

std::wstring ConvertUTF8ToWide(std::string_view utf) {
  std::wstring wstr;
  wstr.reserve(utf.size());
  const size_t size = utf.size();
  size_t index = 0;
  while (index < size) {
    uint32_t c = (unsigned char)utf[index];
    if (c < 0x80) {
      wstr.push_back(c);
    } else if (c < 0xe0) {
      if (c >= 0xc0 && index + 1 < size) {
        c = ((c & 0x1f) << 6) | (utf[index+1] & 0x3f);
        if (c >= 0x80) wstr.push_back(c);
        index++;
      }
    } else if (c < 0xf0) {
      if (index + 2 < size) {
        c = ((c & 0x0f) << 12) | ((utf[index+1] & 0x3f) << 6) | (utf[index+2] & 0x3f);
        if (c >= 0x800) wstr.push_back(c);
        index += 2;
      }
    } else if (c < 0xf8) {
      if (index + 3 < size) {
        c = ((c & 0x07) << 18) | ((utf[index+1] & 0x3f) << 12) | ((utf[index+2] & 0x3f) << 6) |
            (utf[index+3] & 0x3f);
        if (c >= 0x10000) wstr.push_back(c);
        index += 3;
      }
    } else if (c < 0xfc) {
      if (index + 4 < size) {
        c = ((c & 0x03) << 24) | ((utf[index+1] & 0x3f) << 18) | ((utf[index+2] & 0x3f) << 12) |
            ((utf[index+3] & 0x3f) << 6) | (utf[index+4] & 0x3f);
        if (c >= 0x200000) wstr.push_back(c);
        index += 4;
      }
    } else if (c < 0xfe) {
      if (index + 5 < size) {
        c = ((c & 0x01) << 30) | ((utf[index+1] & 0x3f) << 24) | ((utf[index+2] & 0x3f) << 18) |
            ((utf[index+3] & 0x3f) << 12) | ((utf[index+4] & 0x3f) << 6) | (utf[index+5] & 0x3f);
        if (c >= 0x4000000) wstr.push_back(c);
        index += 5;
      }
    }
    index++;
  }
  return wstr;
}

std::string ConvertWideToUTF8(const std::wstring& wstr) {
  std::string utf;
  utf.reserve(wstr.size() * 3);
  std::wstring::const_iterator it = wstr.begin();
  std::wstring::const_iterator itend = wstr.end();
  while (it != itend) {
    const uint32_t c = *it;
    if (c < 0x80) {
      utf.push_back(c);
    } else if (c < 0x800) {
      utf.push_back(0xc0 | (c >> 6));
      utf.push_back(0x80 | (c & 0x3f));
    } else if (c < 0x10000) {
      utf.push_back(0xe0 | (c >> 12));
      utf.push_back(0x80 | ((c & 0xfff) >> 6));
      utf.push_back(0x80 | (c & 0x3f));
    } else if (c < 0x200000) {
      utf.push_back(0xf0 | (c >> 18));
      utf.push_back(0x80 | ((c & 0x3ffff) >> 12));
      utf.push_back(0x80 | ((c & 0xfff) >> 6));
      utf.push_back(0x80 | (c & 0x3f));
    } else if (c < 0x4000000) {
      utf.push_back(0xf8 | (c >> 24));
      utf.push_back(0x80 | ((c & 0xffffff) >> 18));
      utf.push_back(0x80 | ((c & 0x3ffff) >> 12));
      utf.push_back(0x80 | ((c & 0xfff) >> 6));
      utf.push_back(0x80 | (c & 0x3f));
    } else if (c < 0x80000000) {
      utf.push_back(0xfc | (c >> 30));
      utf.push_back(0x80 | ((c & 0x3fffffff) >> 24));
      utf.push_back(0x80 | ((c & 0xffffff) >> 18));
      utf.push_back(0x80 | ((c & 0x3ffff) >> 12));
      utf.push_back(0x80 | ((c & 0xfff) >> 6));
      utf.push_back(0x80 | (c & 0x3f));
    }
    ++it;
  }
  return utf;
}

std::string SerializeStrPair(std::string_view first, std::string_view second) {
  const size_t size = SizeVarNum(first.size()) + first.size() +
      SizeVarNum(second.size()) + second.size();
  std::string serialized(size, 0);
  char* wp = const_cast<char*>(serialized.data());
  wp += WriteVarNum(wp, first.size());
  std::memcpy(wp, first.data(), first.size());
  wp += first.size();
  wp += WriteVarNum(wp, second.size());
  std::memcpy(wp, second.data(), second.size());
  return serialized;
}

void DeserializeStrPair(
    std::string_view serialized, std::string_view* first, std::string_view* second) {
  assert(first != nullptr && second != nullptr);
  const char* rp = serialized.data();
  size_t size = serialized.size();
  uint64_t first_size = 0;
  size_t step = ReadVarNum(rp, size, &first_size);
  rp += step;
  size -= step;
  *first = std::string_view(rp, first_size);
  rp += first_size;
  size -= first_size;
  uint64_t second_size = 0;
  step = ReadVarNum(rp, size, &second_size);
  rp += step;
  *second = std::string_view(rp, second_size);
}

std::string_view GetFirstFromSerializedStrPair(std::string_view serialized) {
  uint64_t first_size = 0;
  size_t step = ReadVarNum(serialized.data(), serialized.size(), &first_size);
  return std::string_view(serialized.data() + step, first_size);
}

std::string SerializeStrVector(const std::vector<std::string>& values) {
  size_t size = 0;
  for (const auto& value : values) {
    size += SizeVarNum(value.size()) + value.size();
  }
  std::string serialized(size, 0);
  char* wp = const_cast<char*>(serialized.data());
  for (const auto& value : values) {
    wp += WriteVarNum(wp, value.size());
    std::memcpy(wp, value.data(), value.size());
    wp += value.size();
  }
  return serialized;
}

std::vector<std::string> DeserializeStrVector(std::string_view serialized) {
  std::vector<std::string> values;
  const char* rp = serialized.data();
  int32_t size = serialized.size();
  while (size > 0) {
    uint64_t value_size = 0;
    const size_t step = ReadVarNum(rp, size, &value_size);
    rp += step;
    size -= step;
    if (size < static_cast<int64_t>(value_size)) {
      break;
    }
    values.emplace_back(std::string(rp, value_size));
    rp += value_size;
    size -= value_size;
  }
  return values;
}

std::vector<std::string> MakeStrVectorFromViews(const std::vector<std::string_view>& views) {
  std::vector<std::string> values;
  values.reserve(views.size());
  for (const auto& view : views) {
    values.emplace_back(view);
  }
  return values;
}

std::vector<std::string_view> MakeStrViewVectorFromValues(
    const std::vector<std::string>& values) {
  std::vector<std::string_view> views;
  views.reserve(values.size());
  for (const auto& value : values) {
    views.emplace_back(std::string_view(value));
  }
  return views;
}

std::string SerializeStrMap(const std::map<std::string, std::string>& records) {
  size_t size = 0;
  for (const auto& record : records) {
    size += SizeVarNum(record.first.size()) + record.first.size();
    size += SizeVarNum(record.second.size()) + record.second.size();
  }
  std::string serialized(size, 0);
  char* wp = const_cast<char*>(serialized.data());
  for (const auto& record : records) {
    wp += WriteVarNum(wp, record.first.size());
    std::memcpy(wp, record.first.data(), record.first.size());
    wp += record.first.size();
    wp += WriteVarNum(wp, record.second.size());
    std::memcpy(wp, record.second.data(), record.second.size());
    wp += record.second.size();
  }
  return serialized;
}

std::map<std::string, std::string> DeserializeStrMap(std::string_view serialized) {
  std::map<std::string, std::string> records;
  const char* rp = serialized.data();
  int32_t size = serialized.size();
  while (size > 0) {
    uint64_t key_size = 0;
    size_t step = ReadVarNum(rp, size, &key_size);
    rp += step;
    size -= step;
    if (size < static_cast<int64_t>(key_size)) {
      break;
    }
    const char* key_ptr = rp;
    rp += key_size;
    size -= key_size;
    uint64_t value_size = 0;
    step = ReadVarNum(rp, size, &value_size);
    rp += step;
    size -= step;
    if (size < static_cast<int64_t>(value_size)) {
      break;
    }
    const char* value_ptr = rp;
    rp += value_size;
    size -= value_size;
    records.emplace(std::string(key_ptr, key_size), std::string(value_ptr, value_size));
  }
  return records;
}

std::map<std::string, std::string> MakeStrMapFromViews(
    const std::map<std::string_view, std::string_view>& views) {
  std::map<std::string, std::string> records;
  for (const auto& view : views) {
    records.emplace(std::make_pair(view.first, view.second));
  }
  return records;
}

std::map<std::string_view, std::string_view> MakeStrViewMapFromRecords(
    const std::map<std::string, std::string>& records) {
  std::map<std::string_view, std::string_view> views;
  for (const auto& record : records) {
    views.emplace(std::make_pair(
        std::string_view(record.first), std::string_view(record.second)));
  }
  return views;
}

}  // namespace tkrzw

// END OF FILE
