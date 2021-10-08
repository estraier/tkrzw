/*************************************************************************************************
 * Command-line utilities
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

#ifndef _TKRZW_CMD_UTIL_H
#define _TKRZW_CMD_UTIL_H

#include <functional>
#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <random>
#include <set>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include <cinttypes>
#include <cstdarg>

#include "tkrzw_containers.h"
#include "tkrzw_compress.h"
#include "tkrzw_dbm.h"
#include "tkrzw_dbm_async.h"
#include "tkrzw_dbm_baby.h"
#include "tkrzw_dbm_cache.h"
#include "tkrzw_dbm_common_impl.h"
#include "tkrzw_dbm_hash.h"
#include "tkrzw_dbm_hash_impl.h"
#include "tkrzw_dbm_poly.h"
#include "tkrzw_dbm_shard.h"
#include "tkrzw_dbm_skip.h"
#include "tkrzw_dbm_skip_impl.h"
#include "tkrzw_dbm_std.h"
#include "tkrzw_dbm_tiny.h"
#include "tkrzw_dbm_tree.h"
#include "tkrzw_dbm_tree_impl.h"
#include "tkrzw_dbm_ulog.h"
#include "tkrzw_file.h"
#include "tkrzw_file_mmap.h"
#include "tkrzw_file_pos.h"
#include "tkrzw_file_std.h"
#include "tkrzw_file_util.h"
#include "tkrzw_hash_util.h"
#include "tkrzw_index.h"
#include "tkrzw_key_comparators.h"
#include "tkrzw_lib_common.h"
#include "tkrzw_logger.h"
#include "tkrzw_message_queue.h"
#include "tkrzw_str_util.h"
#include "tkrzw_thread_util.h"
#include "tkrzw_time_util.h"

namespace tkrzw {

/**
 * Prints an empty string to the stdout and flush the buffer.
 */
inline void Print() {
  std::cout.flush();
}

/**
 * Prints strings to the stdout and flush the buffer.
 * @param first The first string.
 * @param rest The rest strings.
 */
template <typename FIRST, typename... REST>
inline void Print(const FIRST& first, const REST&... rest) {
  std::cout << ToString(first);
  Print(rest...);
}

/**
 * Prints an empty string and a line feed to the stdout and flush the buffer.
 */
inline void PrintL() {
  std::cout << std::endl;
  std::cout.flush();
}

/**
 * Prints strings and a line feed to the stdout and flush the buffer.
 * @param first The first string.
 * @param rest The rest strings.
 */
template <typename FIRST, typename... REST>
inline void PrintL(const FIRST& first, const REST&... rest) {
  std::cout << ToString(first);
  PrintL(rest...);
}

/**
 * Prints a formatted string to the stdout and flush the buffer.
 * @param format The format string.
 * @param ... The other arguments.
 */
void PrintF(const char* format, ...);

/**
 * Prints a character to the stdout and flush the buffer.
 * @param c The character to print.
 */
void PutChar(char c);

/**
 * Prints an empty string to the stderr and flush the buffer.
 */
inline void EPrint() {
  std::cerr.flush();
}

/**
 * Prints strings to the stderr and flush the buffer.
 * @param first The first string.
 * @param rest The rest strings.
 */
template <typename FIRST, typename... REST>
inline void EPrint(const FIRST& first, const REST&... rest) {
  std::cerr << ToString(first);
  EPrint(rest...);
}

/**
 * Prints an empty string and a line feed to the stderr and flush the buffer.
 */
inline void EPrintL() {
  std::cerr << std::endl;
  std::cerr.flush();
}

/**
 * Prints strings and a line feed to the stderr and flush the buffer.
 * @param first The first string.
 * @param rest The rest strings.
 */
template <typename FIRST, typename... REST>
inline void EPrintL(const FIRST& first, const REST&... rest) {
  std::cerr << ToString(first);
  EPrintL(rest...);
}

/**
 * Prints a formatted string to the stderr and flush the buffer
 * @param format The format string.
 * @param ... The other arguments.
 */
void EPrintF(const char* format, ...);

/**
 * Prints a character to the stderr and flush the buffer.
 * @param c The character to print.
 */
void EPutChar(char c);

/**
 * Parses command line arguments.
 * @param argc The number of input arguments.
 * @param argv The input arguments.
 * @param configs A map of option names and numbers of the required arguments.  If an empty
 * string represents positional arguments.
 * @param result The pointer to a map object to contain option names and their arguments.
 * @param error_message The pointer to a string object to contain the error message.
 * @return True on success or false on failure.
 */
bool ParseCommandArguments(
    int32_t argc, const char** argv,
    const std::map<std::string, int32_t>& configs,
    std::map<std::string, std::vector<std::string>>* result,
    std::string* error_message);

/**
 * Gets a string argument of parsed command arguments.
 * @param args The parsed command arguments.
 * @param name The name of the argument.
 * @param index The index of the value.
 * @param default_value The value to be returned on failure.
 * @return The value of the matching argument on success, or the default value on failure.
 */
std::string GetStringArgument(
    const std::map<std::string, std::vector<std::string>>& args,
    const std::string& name, int32_t index, const std::string& default_value);

/**
 * Gets an integer argument of parsed command arguments.
 * @param args The parsed command arguments.
 * @param name The name of the argument.
 * @param index The index of the value.
 * @param default_value The value to be returned on failure.
 * @return The value of the matching argument on success, or the default value on failure.
 */
int64_t GetIntegerArgument(
    const std::map<std::string, std::vector<std::string>>& args,
    const std::string& name, int32_t index, int64_t default_value);

/**
 * Gets a real number argument of parsed command arguments.
 * @param args The parsed command arguments.
 * @param name The name of the argument.
 * @param index The index of the value.
 * @param default_value The value to be returned on failure.
 * @return The value of the matching argument on success, or the default value on failure.
 */
double GetDoubleArgument(
    const std::map<std::string, std::vector<std::string>>& args,
    const std::string& name, int32_t index, double default_value);

/**
 * Throws an exception of StatusException to terminates the process with a message.
 * @param message The message to print.
 */
inline void Die(const std::string& message) {
  throw StatusException(Status(Status::APPLICATION_ERROR, message));
}

/**
 * Throws an exception of StatusException to terminates the process with a message.
 * @param first The first parameter.
 * @param rest The rest parameters.
 */
template <typename FIRST, typename... REST>
inline void Die(const FIRST& first, const REST&... rest) {
  Die(StrCat(first, rest...));
}

/**
 * Makes a file object or die.
 * @param impl_name The name of a File implementation: "mmap-para" for MemoryMapParallelFile,
 * "mmap-atom" for MemoryMapAtomicFile, "pos-para" for PositionalParallelFile. "pos-atom" fo
 * PositionalAtomicFile,
 * @param alloc_init_size An initial size of allocation.
 * @param alloc_inc_factor A factor to increase the size of allocation.
 * @return The created object.
 */
std::unique_ptr<File> MakeFileOrDie(
    const std::string& impl_name, int64_t alloc_init_size, double alloc_inc_factor);

/**
 * Sets access strategy of the positional access file.
 * @param file The file object.
 * @param block_size The block size to which all records should be aligned.
 * @param is_direct_io If true, the direct I/O access option is set.
 * @param is_sync_io If true, the synchronous I/O access option is set.
 * @param is_padding If true, the padding access option is set.
 * @param is_pagecache If true, the mini page cache option is set.
 */
void SetAccessStrategyOrDie(File* file, int64_t block_size,
                            bool is_direct_io, bool is_sync_io, bool is_padding,
                            bool is_pagecache);

/**
 * Sets the head buffer of the positional access file.
 * @param file The file object.
 * @param size The size of the head buffer.
 */
void SetHeadBufferOfFileOrDie(File* file, int64_t size);

/**
 * Prints all records of a DBM in TSV format.
 * @param dbm The DBM object.
 */
void PrintDBMRecordsInTSV(DBM* dbm);

/**
 * Makes a text whose characters appear in a cyclic pattern.
 * @param size The size of the output text.
 * @param seed The random seed.
 * @return The result text.
 */
std::string MakeCyclishText(size_t size, int32_t seed);

/**
 * Makes a text whose character distribution is sililar to natural Englsh.
 * @param size The size of the output text.
 * @param seed The random seed.
 * @return The result text.
 */
std::string MakeNaturalishText(size_t size, int32_t seed);

}  // namespace tkrzw

#endif  // _TKRZW_CMD_UTIL_H

// END OF FILE
