/*************************************************************************************************
 * Common implementation components for database managers
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

#ifndef _TKRZW_DBM_COMMON_IMPL_H
#define _TKRZW_DBM_COMMON_IMPL_H

#include <string>
#include <string_view>

#include <cinttypes>
#include <cstdarg>

#include "tkrzw_dbm.h"
#include "tkrzw_hash_util.h"
#include "tkrzw_file.h"
#include "tkrzw_lib_common.h"

namespace tkrzw {

/**
 * Primary hash function for the hash database.
 * @param data The data to calculate the hash value for.
 * @param num_buckets The number of buckets of the hash table.
 * @return The hash value.
 */
inline uint64_t PrimaryHash(std::string_view data, uint64_t num_buckets) {
  constexpr uint64_t seed = 19780211;
  uint64_t hash = HashMurmur(data, seed);
  if (num_buckets <= UINT32MAX) {
    hash = (((hash & 0xffff000000000000ULL) >> 48) | ((hash & 0x0000ffff00000000ULL) >> 16)) ^
        (((hash & 0x000000000000ffffULL) << 16) | ((hash & 0x00000000ffff0000ULL) >> 16));
  }
  return hash % num_buckets;
}

/**
 * Secondary hash function for sharding.
 * @param data The data to calculate the hash value for.
 * @param num_shards The number of shards.
 * @return The hash value.
 */
inline uint64_t SecondaryHash(std::string_view data, uint64_t num_shards) {
  uint64_t hash = HashFNV(data);
  if (num_shards <= UINT32MAX) {
    hash = (((hash & 0xffff000000000000ULL) >> 48) | ((hash & 0x0000ffff00000000ULL) >> 16)) ^
        (((hash & 0x000000000000ffffULL) << 16) | ((hash & 0x00000000ffff0000ULL) >> 16));
  }
  return hash % num_shards;
}

/**
 * Returns true if an integer is a prime number.
 * @param num The integer.
 * @return True if the integer is a prime number.
 */
uint64_t IsPrimeNumber(uint64_t num);

/**
 * Gets a proper bucket size for hashing.
 * @param min_size The minimum size.
 * @return The calculated bucket size.
 */
int64_t GetHashBucketSize(int64_t min_size);

/**
 * Searches a database and get keys which match a pattern.
 * @param dbm The DBM object of the database.
 * @param pattern The pattern for matching.
 * @param matched A vector to contain the result.
 * @param capacity The maximum records to obtain.  0 means unlimited.
 * @param matcher A matching function which takes the pattern and a candidate.
 * @return The result status.
 * @details This scans the whole database so it can take long time.
 */
Status SearchDBM(
    DBM* dbm, std::string_view pattern, std::vector<std::string>* matched, size_t capacity = 0,
    bool (*matcher)(std::string_view, std::string_view) = StrContains);

/**
 * Searches a database and get keys which match a lambda function.
 * @param dbm The DBM object of the database.
 * @param matcher A matching function which takes a candidate.
 * @param matched A vector to contain the result.
 * @param capacity The maximum records to obtain.  0 means unlimited.
 * @return The result status.
 * @details This scans the whole database so it can take long time.
 */
Status SearchDBMLambda(
    DBM* dbm, std::function<bool(std::string_view)> matcher,
    std::vector<std::string>* matched, size_t capacity = 0);

/**
 * Searches an ordered database and get keys which match a boundary condition.
 * @param dbm The DBM object of the database.
 * @param pattern The boundary pattern of the origin.
 * @param upper If true, keys whose positions are upper than the boundary pattern are picked up.
 * If false, keys whose positions are lower than the boundary pattern are picked up.
 * @param inclusive If true, keys whose position are the same as the boundary pattern are included.
 * @param matched A vector to contain the result.
 * @param capacity The maximum records to obtain.  0 means unlimited.
 * @return The result status.
 * @details Even if there's no matching record, the operation doesn't fail.  This method is
 * suppoerted only for ordered databases.
 */
Status SearchDBMOrder(DBM* dbm, std::string_view pattern, bool upper, bool inclusive,
                      std::vector<std::string>* matched, size_t capacity = 0);

/**
 * Searches a database and get keys which begin with a pattern.
 * @param dbm The DBM object of the database.
 * @param pattern The pattern for forward matching.
 * @param matched A vector to contain the result.
 * @param capacity The maximum records to obtain.  0 means unlimited.
 * @return The result status.
 * @details If the database is ordered, an efficient way is used.  However, if the key comparator
 * is not LexicalKeyComparator, all matching keys are not extracted.  If the database is unordered,
 * this scans the whole database so it can take long time.
 */
Status SearchDBMForwardMatch(
    DBM* dbm, std::string_view pattern, std::vector<std::string>* matched, size_t capacity = 0);

/**
 * Searches a database and get keys which match a regular expression.
 * @param dbm The DBM object of the database.
 * @param pattern The regular expression pattern to search for.  Leading "(?i)" makes the pattern
 * case-insensitive.  Other options "a" (AWK regex), "b" (basic POSIX regex), "e" (extended POSIX
 * regex), and "l" (egrep) are available in addition to "i".  The default regex format is
 * ECMAScript.
 * @param matched A vector to contain the result.
 * @param capacity The maximum records to obtain.  0 means unlimited.
 * @return The result status.
 * @details This scans the whole database so it can take long time.
 */
Status SearchDBMRegex(
    DBM* dbm, std::string_view pattern, std::vector<std::string>* matched, size_t capacity = 0);

/**
 * Searches a database and get keys whose edit distance with a UTF-8 pattern is the least.
 * @param dbm The DBM object of the database.
 * @param pattern The pattern for matching.
 * @param matched A vector to contain the result.
 * @param capacity The maximum records to obtain.  0 means unlimited.
 * @details This scans the whole database so it can take long time.
 */
Status SearchDBMEditDistance(
    DBM* dbm, std::string_view pattern, std::vector<std::string>* matched, size_t capacity = 0);

/**
 * Searches a database and get keys whose edit distance with a binary pattern is the least.
 * @param dbm The DBM object of the database.
 * @param pattern The pattern for matching.
 * @param matched A vector to contain the result.
 * @param capacity The maximum records to obtain.  0 means unlimited.
 * @details This scans the whole database so it can take long time.
 */
Status SearchDBMEditDistanceBinary(
    DBM* dbm, std::string_view pattern, std::vector<std::string>* matched, size_t capacity = 0);

/**
 * Searches a database and get keys which match a pattern, according to a mode expression.
 * @param dbm The DBM object of the database.
 * @param mode The search mode.  "contain" extracts keys containing the pattern.  "begin"
 * extracts keys beginning with the pattern.  "end" extracts keys ending with the pattern.
 * "regex" extracts keys partially matches the pattern of a regular expression.  "edit"
 * extracts keys whose edit distance to the UTF-8 pattern is the least.  "editbin" extracts
 * keys whose edit distance to the binary pattern is the least.  "containcase", "containword",
 * and "containcaseword" extract keys considering case and word boundary.  "contain*",
 * "containcase*", "containword*", and "containcaseword*" take a null-code-separatable pattern and
 * do batch operations for each element.  Ordered databases support "upper" and "lower" which
 * extract keys whose positions are upper/lower than the pattern.  "upperinc" and "lowerinc" are
 * their inclusive versions.
 * @param pattern The pattern for matching.
 * @param matched A vector to contain the result.
 * @param capacity The maximum records to obtain.  0 means unlimited.
 * @return The result status.
 */
Status SearchDBMModal(
    DBM* dbm, std::string_view mode, std::string_view pattern,
    std::vector<std::string>* matched, size_t capacity = 0);

/**
 * Exports all records of a database to a flat record file.
 * @param dbm The DBM object of the database.
 * @param dest_file The file object to write records in.
 * @return The result status.
 * @details A flat record file contains a sequence of binary records without any high level
 * structure so it is useful as a intermediate file for data migration.
 */
Status ExportDBMToFlatRecords(DBM* dbm, File* dest_file);

/**
 * Imports records to a database from a flat record file.
 * @param dbm The DBM object of the database.
 * @param src_file The file object to read records from.
 * @return The result status.
 */
Status ImportDBMFromFlatRecords(DBM* dbm, File* src_file);

/**
 * Exports the keys of all records of a database to a flat record file.
 * @param dbm The DBM object of the database.
 * @param dest_file The file object to write keys in.
 * @return The result status.
 */
Status ExportDBMKeysToFlatRecords(DBM* dbm, File* dest_file);

/**
 * Exports all records of a database to a TSV file.
 * @param dbm The DBM object of the database.
 * @param dest_file The file object to write records in.
 * @param escape If true, C-style escaping is applied to the output.
 * @return The result status.
 */
Status ExportDBMToTSV(DBM* dbm, File* dest_file, bool escape = false);

/**
 * Imports records to a database from a TSV file.
 * @param dbm The DBM object of the database.
 * @param dest_file The file object to read records from.
 * @param unescape If true, C-style unescaping is applied to the input.
 * @return The result status.
 */
Status ImportDBMFromTSV(DBM* dbm, File* dest_file, bool unescape = false);

/**
 * Exports the keys of all records of a database as lines to a text file.
 * @param dbm The DBM object of the database.
 * @param dest_file The file object to write keys in.
 * @return The result status.
 */
Status ExportDBMKeysAsLines(DBM* dbm, File* dest_file);

/**
 * Searches a text file and get lines which match a pattern.
 * @param file The file to search.
 * @param pattern The pattern for matching.
 * @param matched A vector to contain the result.
 * @param capacity The maximum records to obtain.  0 means unlimited.
 * @param matcher A matching function which takes the pattern and a candidate.
 * @return The result status.
 */
Status SearchTextFile(
    File* file, std::string_view pattern, std::vector<std::string>* matched, size_t capacity = 0,
    bool (*matcher)(std::string_view, std::string_view) = StrContains);

/**
 * Searches a text file and get lines which match a lambda function.
 * @param file The file to search.
 * @param matcher A matching function which takes a candidate.
 * @param matched A vector to contain the result.
 * @param capacity The maximum records to obtain.  0 means unlimited.
 * @return The result status.
 */
Status SearchTextFileLambda(
    File* file, std::function<bool(std::string_view)> matcher,
    std::vector<std::string>* matched, size_t capacity = 0);

/**
 * Searches a text file and get lines which match a regular expression.
 * @param file The file to search.
 * @param pattern The regular expression pattern to search for.  Leading "(?i)" makes the pattern
 * case-insensitive.  Other options "a" (AWK regex), "b" (basic POSIX regex), "e" (extended POSIX
 * regex), and "l" (egrep) are available in addition to "i".  The default regex format is
 * ECMAScript.
 * @param matched A vector to contain the result.
 * @param capacity The maximum records to obtain.  0 means unlimited.
 * @return The result status.
 */
Status SearchTextFileRegex(
    File* file, std::string_view pattern, std::vector<std::string>* matched, size_t capacity = 0);

/**
 * Searches a text file and get lines whose edit distance with a UTF-8 pattern is the least.
 * @param file The file to search.
 * @param pattern The pattern for matching.
 * @param matched A vector to contain the result.
 * @param capacity The maximum records to obtain.  0 means unlimited.
 * @return The result status.
 */
Status SearchTextFileEditDistance(
    File* file, std::string_view pattern, std::vector<std::string>* matched, size_t capacity = 0);

/**
 * Searches a text file and get lines whose edit distance with a binary pattern is the least.
 * @param file The file to search.
 * @param pattern The pattern for matching.
 * @param matched A vector to contain the result.
 * @param capacity The maximum records to obtain.  0 means unlimited.
 * @return The result status.
 */
Status SearchTextFileEditDistanceBinary(
    File* file, std::string_view pattern, std::vector<std::string>* matched, size_t capacity = 0);

/**
 * Searches a text file and get lines which match a pattern, according to a mode expression.
 * @param file The file to search.
 * @param mode The search mode.  "contain" extracts keys containing the pattern.  "begin"
 * extracts keys beginning with the pattern.  "end" extracts keys ending with the pattern.
 * "regex" extracts keys partially matches the pattern of a regular expression.  "edit"
 * extracts keys whose edit distance to the UTF-8 pattern is the least.  "editbin" extracts
 * keys whose edit distance to the binary pattern is the least.  "containcase", "containword",
 * and "containcaseword" extract keys considering case and word boundary.  "contain*",
 * "containcase*", "containword*", and "containcaseword*" take a line-separatable pattern and
 * do batch operations for each elements.
 * @param pattern The pattern for matching.
 * @param matched A vector to contain the result.
 * @param capacity The maximum records to obtain.  0 means unlimited.
 * @return The result status.
 */
Status SearchTextFileModal(
    File* file, std::string_view mode, std::string_view pattern,
    std::vector<std::string>* matched, size_t capacity = 0);

}  // namespace tkrzw

#endif  // _TKRZW_DBM_COMMON_IMPL_H

// END OF FILE
