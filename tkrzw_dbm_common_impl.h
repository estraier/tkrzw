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
#include "tkrzw_file.h"
#include "tkrzw_lib_common.h"

namespace tkrzw {

/**
 * Primary hash function for the hash database.
 * @param data The data to calculate the hash value for.
 * @param num_buckets The number of buckets of the hash table.
 * @return The hash value.
 */
uint64_t PrimaryHash(std::string_view data, uint64_t num_buckets);

/**
 * Secondary hash function for sharding.
 * @param data The data to calculate the hash value for.
 * @param num_shards The number of shards.
 * @return The hash value.
 */
uint64_t SecondaryHash(std::string_view data, uint64_t num_shards);

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
 * @param pattern The regular expression pattern for partial matching.
 * @param matched A vector to contain the result.
 * @param capacity The maximum records to obtain.  0 means unlimited.
 * @param utf If true, text is treated as UTF-8 and matching is done with UCS sequences.
 * @return The result status.
 * @details This scans the whole database so it can take long time.
 */
Status SearchDBMRegex(
    DBM* dbm, std::string_view pattern, std::vector<std::string>* matched, size_t capacity = 0,
    bool utf = false);

/**
 * Searches a database and get keys whose edit distance with a pattern is the least.
 * @param dbm The DBM object of the database.
 * @param pattern The pattern for matching.
 * @param matched A vector to contain the result.
 * @param capacity The maximum records to obtain.  0 means unlimited.
 * @param utf If true, text is treated as UTF-8 and the distance is calculated between UCS
 * sequences.
 * @return The result status.
 * @details This scans the whole database so it can take long time.
 */
Status SearchDBMEditDistance(
    DBM* dbm, std::string_view pattern, std::vector<std::string>* matched, size_t capacity = 0,
    bool utf = false);

/**
 * Exports all records of a database to a flat record file.
 * @param dbm The DBM object of the database.
 * @param file The file object to write records in.
 * @return The result status.
 */
Status ExportDBMRecordsToFlatRecords(DBM* dbm, File* file);

/**
 * Imports records to a database from a flat record file.
 * @param dbm The DBM object of the database.
 * @param file The file object to read records from.
 * @return The result status.
 */
Status ImportDBMRecordsFromFlatRecords(DBM* dbm, File* file);

/**
 * Exports the keys of all records of a database to a flat record file.
 * @param dbm The DBM object of the database.
 * @param file The file object to write keys in.
 * @return The result status.
 */
Status ExportDBMKeysToFlatRecords(DBM* dbm, File* file);

/**
 * Exports all records of a database to a TSV file.
 * @param dbm The DBM object of the database.
 * @param file The file object to write records in.
 * @param escape If true, C-style escaping is applied to the output.
 * @return The result status.
 */
Status ExportDBMRecordsToTSV(DBM* dbm, File* file, bool escape = false);

/**
 * Imports records to a database from a TSV file.
 * @param dbm The DBM object of the database.
 * @param file The file object to read records from.
 * @param unescape If true, C-style unescaping is applied to the input.
 * @return The result status.
 */
Status ImportDBMRecordsFromTSV(DBM* dbm, File* file, bool unescape = false);

/**
 * Exports the keys of all records of a database as lines to a text file.
 * @param dbm The DBM object of the database.
 * @param file The file object to write keys in.
 * @return The result status.
 */
Status ExportDBMKeysAsLines(DBM* dbm, File* file);

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
 * Searches a text file and get lines which match a regular expression.
 * @param file The file to search.
 * @param pattern The regular expression pattern for partial matching.
 * @param matched A vector to contain the result.
 * @param capacity The maximum records to obtain.  0 means unlimited.
 * @param utf If true, text is decoded as UTF-8 and edit distance is calculated by the unicode
 * characters.
 * @return The result status.
 */
Status SearchTextFileRegex(
    File* file, std::string_view pattern, std::vector<std::string>* matched, size_t capacity = 0,
    bool utf = false);

/**
 * Searches a text file and get lines whose edit distance with a pattern is the least.
 * @param file The file to search.
 * @param pattern The pattern for matching.
 * @param matched A vector to contain the result.
 * @param capacity The maximum records to obtain.  0 means unlimited.
 * @param utf If true, text is decoded as UTF-8 and edit distance is calculated by the unicode
 * characters.
 * @return The result status.
 */
Status SearchTextFileEditDistance(
    File* file, std::string_view pattern, std::vector<std::string>* matched, size_t capacity = 0,
    bool utf = false);

}  // namespace tkrzw

#endif  // _TKRZW_DBM_COMMON_IMPL_H

// END OF FILE
