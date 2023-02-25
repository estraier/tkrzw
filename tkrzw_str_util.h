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

#ifndef _TKRZW_STR_UTIL_H
#define _TKRZW_STR_UTIL_H

#include <map>
#include <string>
#include <vector>

#include <cinttypes>

#include "tkrzw_lib_common.h"

namespace tkrzw {

/**
 * Converts a decimal string to an integer.
 * @param str The decimal string.
 * @param defval The default value to be returned on failure.
 * @return The converted integer.
 */
int64_t StrToInt(std::string_view str, int64_t defval = 0);

/**
 * Converts a decimal string with a metric prefix to an integer.
 * @param str The decimal string, which can be trailed by a binary metric prefix.  "K", "M", "G",
 * "T", "P", and "E" are supported.  They are case-insensitive.
 * @param defval The default value to be returned on failure.
 * @return The converted integer.  If the integer overflows the domain, INT64MAX or INT64MIN is
 * returned according to the sign.
 */
int64_t StrToIntMetric(std::string_view str, int64_t defval = 0);

/**
 * Converts a octal string to an integer.
 * @param str The octal string.
 * @param defval The default value to be returned on failure.
 * @return The converted integer.
 */
uint64_t StrToIntOct(std::string_view str, uint64_t defval = 0);

/**
 * Converts a hexadecimal string to an integer.
 * @param str The hexadecimal string.
 * @param defval The default value to be returned on failure.
 * @return The converted integer.
 */
uint64_t StrToIntHex(std::string_view str, uint64_t defval = 0);

/**
 * Converts a big-endian binary string to an integer.
 * @param str The big endian binary string.
 * @return The converted integer.
 */
uint64_t StrToIntBigEndian(std::string_view str);

/**
 * Converts a decimal string to a real number.
 * @param str The decimal string.
 * @param defval The default value to be returned on failure.
 * @return The converted real number.
 */
double StrToDouble(std::string_view str, double defval = 0.0);

/**
 * Converts a boolean string to a boolean value.
 * @param str The decimal string.
 * @param defval The default value to be returned on failure.
 * @return The converted boolean value.
 */
bool StrToBool(std::string_view str, bool defval = false);

/**
 * Converts a decimal or boolean string to an integer or boolean value.
 * @param str The decimal string.
 * @param defval The default value to be returned on failure.
 * @return The converted integer.
 */
int64_t StrToIntOrBool(std::string_view str, int64_t defval = 0);

/**
 * Appends a formatted string at the end of a string.
 * @param dest The destination string.
 * @param format The printf-like format string.  The conversion character `%' can be used with
 * such flag characters as `s', `d', `o', `u', `x', `X', `c', `e', `E', `f', `g', `G', and `%'.
 * @param ap Arguments used according to the format string.
 */
void VSPrintF(std::string* dest, const char* format, va_list ap);

/**
 * Appends a formatted string at the end of a string.
 * @param dest The destination string.
 * @param format The printf-like format string.  The conversion character `%' can be used with
 * such flag characters as `s', `d', `o', `u', `x', `X', `c', `e', `E', `f', `g', `G', and `%'.
 * @param ... Arguments used according to the format string.
 */
void SPrintF(std::string* dest, const char* format, ...);

/**
 * Generates a formatted string.
 * @param format The printf-like format string.  The conversion character `%' can be used with
 * such flag characters as `s', `d', `o', `u', `x', `X', `c', `e', `E', `f', `g', `G', and `%'.
 * @param ... Arguments used according to the format string.
 * @return The result string.
 */
std::string SPrintF(const char* format, ...);

/**
 * Converts an integer to a decimal string.
 * @param data The integer to convert.
 * @return The converted string.
 */
template <typename T>
inline std::string ToString(T data) {
  return std::to_string(data);
}

/**
 * Converts a real number to a decimal string.
 * @param data The real number to convert.
 * @return The converted string.
 */
std::string ToString(double data);

/**
 * Converts a real number to a decimal string.
 * @param data The real number to convert.
 * @return The converted string.
 */
inline std::string ToString(float data) {
  return ToString(static_cast<double>(data));
}

/**
 * Converts a real number to a decimal string.
 * @param data The real number to convert.
 * @return The converted string.
 */
inline std::string ToString(long double data) {
  return ToString(static_cast<double>(data));
}

/**
 * Converts a boolean value to a decimal string.
 * @param data The integer to convert.
 * @return The converted string.
 */
inline std::string ToString(bool data) {
  return std::string(data ? "true" : "false");
}

/**
 * Converts a character into a string.
 * @param data The character.
 * @return The converted string.
 */
inline std::string ToString(char data) {
  return std::string(1, data);
}

/**
 * Converts a C-style string into a string.
 * @param data The C-style string to convert.
 * @return The converted string.
 */
inline std::string ToString(const char* data) {
  return data;
}

/**
 * Converts a string view into a string.
 * @param data The string view.
 * @return The converted string.
 */
inline std::string ToString(std::string_view data) {
  return std::string(data);
}

/**
 * Copies a string.
 * @param data The string.
 * @return The copied string.
 */
inline std::string ToString(const std::string& data) {
  return data;
}

/**
 * Converts an integer into a big-endian binary string.
 * @param data The integer to convert.
 * @param size The size of the converted string.
 * @return The converted string.
 */
std::string IntToStrBigEndian(uint64_t data, size_t size = sizeof(uint64_t));

/**
 * Converts each record of a container into strings and join them.
 * @param elems An iterable container.
 * @param delim A string to delimit elements.
 * @return The joined string.
 */
template <typename T>
std::string StrJoin(const T& elems, const std::string_view& delim) {
  std::string str;
  for (const auto& elem : elems) {
    if (!str.empty()) {
      str += delim;
    }
    str += ToString(elem);
  }
  return str;
}

/**
 * Returns an empty string.
 * @return The empty string.
 */
inline std::string StrCat() {
  return "";
}

/**
 * Concatenates data of arbitrary parameters into a string.
 * @param first The first parameter.
 * @param rest The rest parameters.
 * @return The concatenated string.
 */
template <typename FIRST, typename... REST>
std::string StrCat(const FIRST& first, const REST&... rest) {
  return ToString(first) + StrCat(rest...);
}

/**
 * Splits a string with a delimiter character.
 * @param str The string.
 * @param delim The delimiter character.
 * @param skip_empty If true, fields with empty values are skipped.
 * @return A vector object into which the result segments are pushed.
 */
std::vector<std::string> StrSplit(std::string_view str, char delim,
                                  bool skip_empty = false);

/**
 * Splits a string with a delimiter string.
 * @param str The string.
 * @param delim The delimiter string.  If it is empty, each character is separated.
 * @param skip_empty If true, fields with empty values are skipped.
 * @return A vector object into which the result segments are pushed.
 */
std::vector<std::string> StrSplit(std::string_view str, std::string_view delim,
                                  bool skip_empty = false);

/**
 * Splits a string with delimiter characters.
 * @param str The string to split.
 * @param delims A string containing a set of the delimiters.
 * @param skip_empty If true, fields with empty values are skipped.
 * @return A vector object into which the result segments are pushed.
 */
std::vector<std::string> StrSplitAny(std::string_view str, std::string_view delims,
                                     bool skip_empty = false);

/**
 * Splits a string into a key-value map.
 * @param str The string to split.
 * @param delim_records The delimiter string between records.
 * @param delim_kv The delimiter string between the key and the value.
 * @return A map object containing key-value pairs.
 */
std::map<std::string, std::string> StrSplitIntoMap(
    std::string_view str, std::string_view delim_records, std::string_view delim_kv);

/**
 * Converts letters of a string into upper case.
 * @param str The string to convert.
 * @return The converted string.
 */
std::string StrUpperCase(std::string_view str);

/**
 * Converts letters of a string into upper case in-place.
 * @param str The pointer to the string to modify.
 */
void StrUpperCase(std::string* str);

/**
 * Converts letters of a string into lower case.
 * @param str The string to convert.
 * @return The converted string.
 */
std::string StrLowerCase(std::string_view str);

/**
 * Converts letters of a string into lower case in-place.
 * @param str The pointer to the string to modify.
 */
void StrLowerCase(std::string* str);

/**
 * Converts a string by replacing substrings to diffent substrings.
 * @param str The string to convert.
 * @param before The substring before replacement.
 * @param after The substring after replacement.
 * @return The converted string.
 */
std::string StrReplace(std::string_view str, std::string_view before, std::string_view after);

/**
 * Modifies a string by replacing characters to diffent characters in-place.
 * @param str The pointer to the string to modify.
 * @param before The characters to remove.
 * @param after The characters to insert.  The character at the same index as the index of the
 * before character is used.  If there's no counterpart, no character is inserted.
 */
void StrReplaceCharacters(std::string* str, std::string_view before, std::string_view after);

/**
 * Checks whether a text contains a pattern.
 * @param text The text to search.
 * @param pattern The pattern to search for.
 * @return True if the text contains the pattern.
 */
bool StrContains(std::string_view text, std::string_view pattern);

/**
 * Checks whether a text contains a pattern in a case-insensitive manner.
 * @param text The text to search.
 * @param pattern The pattern to search for.
 * @return True if the text contains the pattern.
 */
bool StrCaseContains(std::string_view text, std::string_view pattern);

/**
 * Checks whether a text contains a word surrounded by non-alphanumeric word boundaries.
 * @param text The text to search.
 * @param pattern The pattern to search for.
 * @return True if the text contains the pattern.
 */
bool StrWordContains(std::string_view text, std::string_view pattern);

/**
 * Checks whether a text contains a word in a case-sensitive manner.
 * @param text The text to search.
 * @param pattern The pattern to search for.
 * @return True if the text contains the pattern.
 */
bool StrCaseWordContains(std::string_view text, std::string_view pattern);

/**
 * Checks a text contains at least one of patterns.
 * @param text The text to search.
 * @param patterns The patterns to search for.
 * @return True if the text contains at least one of patterns.
 */
bool StrContainsBatch(std::string_view text, const std::vector<std::string>& patterns);

/**
 * Checks a text contains at least one of patterns in a case-sensitive manner.
 * @param text The text to search.
 * @param patterns The patterns to search for.
 * @return True if the text contains at least one of patterns.
 */
bool StrCaseContainsBatch(std::string_view text, const std::vector<std::string>& patterns);

/**
 * Checks a text contains at least one of words surrounded by non-alphanumeric word boundaries.
 * @param text The text to search.
 * @param patterns The patterns to search for.
 * @return True if the text contains at least one of patterns.
 */
bool StrWordContainsBatch(std::string_view text, const std::vector<std::string>& patterns);

/**
 * Checks a text contains at least one of words in a case-sensitive manner.
 * @param text The text to search.
 * @param patterns The patterns to search for.
 * @return True if the text contains at least one of patterns.
 */
bool StrCaseWordContainsBatch(std::string_view text, const std::vector<std::string>& patterns);

/**
 * Checks a text contains at least one of lowercase words in a case-sensitive manner.
 * @param text The text to search.
 * @param patterns The patterns to search for.  Each must be lowercased.
 * @return True if the text contains at least one of patterns.
 */
bool StrCaseWordContainsBatchLower(
    std::string_view text, const std::vector<std::string>& patterns);

/**
 * Checks whether a text begins with a pattern.
 * @param text The text to search.
 * @param pattern The pattern to search for.
 * @return True if the text begins with the pattern.
 */
bool StrBeginsWith(std::string_view text, std::string_view pattern);

/**
 * Checks whether a text ends with a pattern.
 * @param text The text to search.
 * @param pattern The pattern to search for.
 * @return True if the text ends with the pattern.
 */
bool StrEndsWith(std::string_view text, std::string_view pattern);

/**
 * Compares two strings ignoring case.
 * @param a A string.
 * @param b The other string.
 * @return Negative if the former is less, 0 if both are equivalent, positive if latter is less.
 */
int32_t StrCaseCompare(std::string_view a, std::string_view b);

/**
 * Searches a text for a pattern, with string::find.
 * @param text The text to search.
 * @param pattern The pattern to search for.
 * @return The index of the first matched position, or -1 if there's no matches.
 */
int32_t StrSearch(std::string_view text, std::string_view pattern);

/**
 * Searches a text for a pattern, by naive double loop.
 * @param text The text to search.
 * @param pattern The pattern to search for.
 * @return The index of the first matched position, or -1 if there's no matches.
 */
int32_t StrSearchDoubleLoop(std::string_view text, std::string_view pattern);

/**
 * Searches a text for a pattern, with memchr.
 * @param text The text to search.
 * @param pattern The pattern to search for.
 * @return The index of the first matched position, or -1 if there's no matches.
 */
int32_t StrSearchMemchr(std::string_view text, std::string_view pattern);

/**
 * Searches a text for a pattern, with memmem.
 * @param text The text to search.
 * @param pattern The pattern to search for.
 * @return The index of the first matched position, or -1 if there's no matches.
 */
int32_t StrSearchMemmem(std::string_view text, std::string_view pattern);

/**
 * Searches a text for a pattern, by Knuth-Morris-Pratt algorithm.
 * @param text The text to search.
 * @param pattern The pattern to search for.
 * @return The index of the first matched position, or -1 if there's no matches.
 */
int32_t StrSearchKMP(std::string_view text, std::string_view pattern);

/**
 * Searches a text for a pattern, by Boyer-Moore algorithm.
 * @param text The text to search.
 * @param pattern The pattern to search for.
 * @return The index of the first matched position, or -1 if there's no matches.
 */
int32_t StrSearchBM(std::string_view text, std::string_view pattern);

/**
 * Searches a text for a pattern, by Rabin-Karp algorithm.
 * @param text The text to search.
 * @param pattern The pattern to search for.
 * @return The index of the first matched position, or -1 if there's no matches.
 */
int32_t StrSearchRK(std::string_view text, std::string_view pattern);

/**
 * Searches a text for a pattern, by Z algorithm.'
 * @param text The text to search.
 * @param pattern The pattern to search for.
 * @return The index of the first matched position, or -1 if there's no matches.
 */
int32_t StrSearchZ(std::string_view text, std::string_view pattern);

/**
 * Searches a text for a pattern and get indices of all occurrences, with string::find.
 * @param text The text to search.
 * @param pattern The pattern to search for.
 * @param max_results The maximum number of results to store.  0 means unlimited.
 * @return A vector of indeces of all ocurrences of the pattern.
 */
std::vector<int32_t> StrSearchWhole(
    std::string_view text, std::string_view pattern, size_t max_results = 0);

/**
 * Searches a text for a pattern and get indices of all occurrences, by KMP algorithm.
 * @param text The text to search.
 * @param pattern The pattern to search for.
 * @param max_results The maximum number of results to store.  0 means unlimited.
 * @return A vector of indeces of all ocurrences of the pattern.
 */
std::vector<int32_t> StrSearchWholeKMP(
    std::string_view text, std::string_view pattern, size_t max_results = 0);

/**
 * Searches a text for a pattern and get indices of all occurrences, by BM algorithm.
 * @param text The text to search.
 * @param pattern The pattern to search for.
 * @param max_results The maximum number of results to store.  0 means unlimited.
 * @return A vector of indeces of all ocurrences of the pattern.
 */
std::vector<int32_t> StrSearchWholeBM(
    std::string_view text, std::string_view pattern, size_t max_results = 0);

/**
 * Searches a text for a pattern and get indices of all occurrences, by RK algorithm.
 * @param text The text to search.
 * @param pattern The pattern to search for.
 * @param max_results The maximum number of results to store.  0 means unlimited.
 * @return A vector of indeces of all ocurrences of the pattern.
 */
std::vector<int32_t> StrSearchWholeRK(
    std::string_view text, std::string_view pattern, size_t max_results = 0);

/**
 * Searches a text for patterns and get indices of all occurrences, by string::find.
 * @param text The text to search.
 * @param patterns The patterns to search for.
 * @param max_results The maximum number of results to store for each pattern.  0 means unlimited.
 * @return A vector of the result for each pattern.  Each element is a vector of indices of all
 * occurrences of the pattern.
 */
std::vector<std::vector<int32_t>> StrSearchBatch(
    std::string_view text, const std::vector<std::string>& patterns, size_t max_results = 0);

/**
 * Searches a text for patterns and get indices of all occurrences, by KMP algorithm.
 * @param text The text to search.
 * @param patterns The patterns to search for.
 * @param max_results The maximum number of results to store for each pattern.  0 means unlimited.
 * @return A vector of the result for each pattern.  Each element is a vector of indices of all
 * occurrences of the pattern.
 */
std::vector<std::vector<int32_t>> StrSearchBatchKMP(
    std::string_view text, const std::vector<std::string>& patterns, size_t max_results = 0);

/**
 * Searches a text for patterns and get indices of all occurrences, by BM algorithm.
 * @param text The text to search.
 * @param patterns The patterns to search for.
 * @param max_results The maximum number of results to store for each pattern.  0 means unlimited.
 * @return A vector of the result for each pattern.  Each element is a vector of indices of all
 * occurrences of the pattern.
 */
std::vector<std::vector<int32_t>> StrSearchBatchBM(
    std::string_view text, const std::vector<std::string>& patterns, size_t max_results = 0);

/**
 * Searches a text for patterns and get indices of all occurrences, by RK algorithm.
 * @param text The text to search.
 * @param patterns The patterns to search for.
 * @param max_results The maximum number of results to store for each pattern.  0 means unlimited.
 * @return A vector of the result for each pattern.  Each element is a vector of indices of all
 * occurrences of the pattern.
 */
std::vector<std::vector<int32_t>> StrSearchBatchRK(
    std::string_view text, const std::vector<std::string>& patterns, size_t max_results = 0);

/**
 * Searches a text for a pattern in a case-insensitive manner.
 * @param text The text to search.
 * @param pattern The pattern to search for.
 * @return The index of the first matched position, or -1 if there's no matches.
 */
int32_t StrCaseSearch(std::string_view text, std::string_view pattern);

/**
 * Searches a text for a word surrounded by non-alphanumeric word boundaries.
 * @param text The text to search.
 * @param pattern The pattern to search for.
 * @return The index of the first matched position, or -1 if there's no matches.
 */
int32_t StrWordSearch(std::string_view text, std::string_view pattern);

/**
 * Searches a text for a word surrounded in a case-insensitive manner.
 * @param text The text to search.
 * @param pattern The pattern to search for.
 * @return The index of the first matched position, or -1 if there's no matches.
 */
int32_t StrCaseWordSearch(std::string_view text, std::string_view pattern);

/**
 * Removes space characters at the head or the tail of a string.
 * @param str The string to convert.
 * @return The converted string.
 */
std::string StrStripSpace(std::string_view str);

/**
 * Removes linefeed characters from the end of a string.
 * @param str The string to convert.
 * @return The converted string.
 */
std::string StrStripLine(std::string_view str);

/**
 * Remove linefeed characters from the end of a string in-place.
 * @param str The pointer to the string to modify.
 */
void StrStripLine(std::string* str);

/**
 * Squeezes space characters in a string and removes spaces at both ends.
 * @param str The string to convert.
 * @return The converted string.
 */
std::string StrSqueezeAndStripSpace(std::string_view str);

/**
 * Trims a string for TSV by normalizing space and control characters.
 * @param str The string to convert.
 * @param keep_tab If true, tab is kept and not escaped.
 * @return The converted string.
 */
std::string StrTrimForTSV(std::string_view str, bool keep_tab = false);

/**
 * Escapes C-style meta characters in a string.
 * @param str The string to convert.
 * @param esc_nonasc If true, non-ASCII characters are excaped.
 * @return The converted string.
 */
std::string StrEscapeC(std::string_view str, bool esc_nonasc = false);

/**
 * Unescapes C-style escape sequences in a string.
 * @param str The string to convert.
 * @return The converted string.
 */
std::string StrUnescapeC(std::string_view str);

/**
 * Encodes a string into a Base64 string.
 * @param str The string to encode.
 * @return The encoded string.
 */
std::string StrEncodeBase64(std::string_view str);

/**
 * Decodes a Base64 string into a string.
 * @param str The Base64 string to decode.
 * @return The decoded string.
 */
std::string StrDecodeBase64(std::string_view str);

/**
 * Encodes a string into a URL part string.
 * @param str The string to encode.
 * @return The encoded string.
 */
std::string StrEncodeURL(std::string_view str);

/**
 * Decodes a URL part string into a string.
 * @param str The URL part string to decode.
 * @return The decoded string.
 */
std::string StrDecodeURL(std::string_view str);

/**
 * Searches a string for a pattern matching a regular expression.
 * @param text The text to search.
 * @param pattern The regular expression pattern to search for.  Leading "(?i)" makes the pattern
 * case-insensitive.  Other options "a" (AWK regex), "b" (basic POSIX regex), "e" (extended POSIX
 * regex), and "l" (egrep) are available in addition to "i".  The default regex format is
 * ECMAScript.
 * @return The position of the first matching pattern.  If there's no matching pattern. -1 is
 * returned.  If the regular expression is invalid, -2 is returned.
 */
int32_t StrSearchRegex(std::string_view text, std::string_view pattern);

/**
 * Replaces substrings matching a pattern of regular expression.
 * @param text The text to process.
 * @param pattern The regular expression pattern to search for.  Leading "(?i)" makes the pattern
 * case-insensitive.  Other options "a" (AWK regex), "b" (basic POSIX regex), "e" (extended POSIX
 * regex), and "l" (egrep) are available in addition to "i".  The default regex format is
 * ECMAScript.
 * @param replace The replacing expression. "$&" means the entire matched pattern.  "$1", "$2",
 * and etc represent n-th bracketed patterns.
 * @return The result string.  If the regular expression is invalid, an empty string is returned.
 */
std::string StrReplaceRegex(std::string_view text, std::string_view pattern,
                            std::string_view replace);

/**
 * Converts a UTF-8 string into a UCS-4 vector.
 * @param utf The UTF-8 string.
 * @return The UCS-4 vector.
 */
std::vector<uint32_t> ConvertUTF8ToUCS4(std::string_view utf);

/**
 * Converts a UCS-4 vector into a UTF-8 string.
 * @param ucs The UCS-4 vector.
 * @return The UTF-8 string.
 */
std::string ConvertUCS4ToUTF8(const std::vector<uint32_t>& ucs);

/**
 * Converts a UTF-8 string into a wide string.
 * @param utf The UTF-8 string.
 * @return The wide string.
 */
std::wstring ConvertUTF8ToWide(std::string_view utf);

/**
 * Converts a wide string into a UTF-8 string.
 * @param wstr The wide string.
 * @return The UTF-8 string.
 */
std::string ConvertWideToUTF8(const std::wstring& wstr);

/**
 * Gets the Levenshtein edit distance of two sequences.
 * @param a A sequence.
 * @param b The other sequence.
 * @return The Levenshtein edit distance of the two sequences.
 */
template<typename T = std::string_view>
static int32_t EditDistanceLev(const T& a, const T&b) {
  const int32_t a_size = std::end(a) - std::begin(a);
  const int32_t b_size = std::end(b) - std::begin(b);
  const int32_t column_size = b_size + 1;
  const int32_t table_size = (a_size + 1) * column_size;
  constexpr int32_t stack_size = 2048;
  int32_t table_stack[stack_size];
  int32_t* table = table_size > stack_size ? new int32_t[table_size] : table_stack;
  table[0] = 0;
  for (int32_t i = 1; i <= a_size; i++) {
    table[i * column_size] = i;
  }
  for (int32_t i = 1; i <= b_size; i++) {
    table[i] = i;
  }
  for (int32_t i = 1; i <= a_size; i++) {
    for (int32_t j = 1; j <= b_size; j++) {
      const int32_t ac = table[(i - 1) * column_size + j] + 1;
      const int32_t bc = table[i * column_size + j - 1] + 1;
      const int32_t cc = table[(i - 1) * column_size + j - 1] + (a[i - 1] != b[j - 1]);
      table[i * column_size + j] = std::min(std::min(ac, bc), cc);
    }
  }
  const int32_t dist = table[a_size * column_size + b_size];
  if (table != table_stack) {
    delete[] table;
  }
  return dist;
}

/**
 * Serializes a pair of strings into a string.
 * @param first The first string.
 * @param second The second string.
 * @return The serialized string.
 * @details The size of the first value and the size of the second value come in the byte delta
 * encoding.  The first data and the second data follow them.
 */
std::string SerializeStrPair(std::string_view first, std::string_view second);

/**
 * Deserializes a serialized string into a pair of strings.
 * @param serialized The serialized string.
 * @param first The pointer to a string view object to refer to the first string.
 * @param second The pointer to a string view object to refer to the second string.
 */
void DeserializeStrPair(
    std::string_view serialized, std::string_view* first, std::string_view* second);

/**
 * Get the first part from a serialized string pair.
 * @param serialized The serialized string.
 * @return The first part string.
 */
std::string_view GetFirstFromSerializedStrPair(std::string_view serialized);

/**
 * Serializes a vector of strings into a string.
 * @param values The string vector.
 * @return The serialized string.
 * @details The size of a value comes in the byte delta encoding and the data follows.  The pairs
 * for all values come consequitively.
 */
std::string SerializeStrVector(const std::vector<std::string>& values);

/**
 * Deserializes a serialized string into a string vector.
 * @param serialized The serialized string.
 * @return The result string vector.
 */
std::vector<std::string> DeserializeStrVector(std::string_view serialized);

/**
 * Makes a string vector from a string view vector.
 * @param views The string view vector.
 * @return The result string vector.
 */
std::vector<std::string> MakeStrVectorFromViews(const std::vector<std::string_view>& views);

/**
 * Makes a string view vector from a string vector.
 * @param values The string vector.
 * @return The result string view vector.  Elements refer to the elements of the input vector.
 */
std::vector<std::string_view> MakeStrViewVectorFromValues(
    const std::vector<std::string>& values);

/**
 * Serializes a map of strings into a string.
 * @param records The string map.
 * @return The serialized string.
 * @details The size of a kay comes in the byte delta encoding and the data follows.  The size of
 * its value comes in the byte delta encoding and the data follows.  The tuples for all records
 * come consequitively.
 */
std::string SerializeStrMap(const std::map<std::string, std::string>& records);

/**
 * Deserializes a serialized string into a string map.
 * @param serialized The serialized string.
 * @return The result string map.
 */
std::map<std::string, std::string> DeserializeStrMap(std::string_view serialized);

/**
 * Makes a string map from a string view map.
 * @param views The string view map.
 * @return The result string map.
 */
std::map<std::string, std::string> MakeStrMapFromViews(
    const std::map<std::string_view, std::string_view>& views);

/**
 * Makes a string view map from a string map.
 * @param records The string map.
 * @return The result string view map.  Elements refer to the elements of the input map.
 */
std::map<std::string_view, std::string_view> MakeStrViewMapFromRecords(
    const std::map<std::string, std::string>& records);

/**
 * Wrapper of string_view of allocated memory.
 */
class ScopedStringView {
 public:
  /**
   * Constructor.
   */
  ScopedStringView() : buf_(nullptr), size_(0) {}

  /**
   * Constructor.
   * @param buf The pointer to a region allocated by xmalloc.  The ownership of is taken.
   * @param size The size of the region.
   */
  ScopedStringView(void* buf, size_t size) : buf_(buf), size_(size) {}

  /**
   * Constructor.
   * @param str A string view object whose data is allocated by xmalloc.  The ownership of the
   * data is taken.
   */
  explicit ScopedStringView(std::string_view str)
      : buf_(const_cast<char*>(str.data())), size_(str.size()) {}

  /**
   * Destructor.
   */
  ~ScopedStringView() {
    xfree(buf_);
  }

  /**
   * Gets the string view object
   */
  std::string_view Get() const {
    return std::string_view(static_cast<char*>(buf_), size_);
  }

  /**
   * Sets the data.
   * @details The region of the old data is released.
   */
  void Set(void* buf, size_t size) {
    xfree(buf_);
    buf_ = buf;
    size_ = size;
  }

  /**
   * Sets the data by a string view.
   * @details The region of the old data is released.
   */
  void Set(std::string_view str) {
    xfree(buf_);
    buf_ = const_cast<char*>(str.data());
    size_ = str.size();
  }

 private:
  /** The allocated buffer. */
  void* buf_;
  /** The buffer size. */
  size_t size_;
};

}  // namespace tkrzw

#endif  // _TKRZW_STR_UTIL_H

// END OF FILE
