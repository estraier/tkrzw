/*************************************************************************************************
 * C language binding of Tkrzw
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

#ifndef _TKRZW_LANGC_H
#define _TKRZW_LANGC_H

#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>

#if defined(__cplusplus)
extern "C" {
#endif

/** The string expression of the package version. */
extern const char* const TKRZW_PACKAGE_VERSION;

/** The string expression of the library version. */
extern const char* const TKRZW_LIBRARY_VERSION;

/** The recognized OS name. */
extern const char* const TKRZW_OS_NAME;

/** The size of a memory page on the OS. */
extern const int32_t TKRZW_PAGE_SIZE;

/** The minimum value of int64_t. */
extern const int64_t TKRZW_INT64MIN;

/** The maximum value of int64_t. */
extern const int64_t TKRZW_INT64MAX;

/** Enumeration for status codes. */
enum {
  /** Success. */
  TKRZW_STATUS_SUCCESS = 0,
  /** Generic error whose cause is unknown. */
  TKRZW_STATUS_UNKNOWN_ERROR = 1,
  /** Generic error from underlying systems. */
  TKRZW_STATUS_SYSTEM_ERROR = 2,
  /** Error that the feature is not implemented. */
  TKRZW_STATUS_NOT_IMPLEMENTED_ERROR = 3,
  /** Error that a precondition is not met. */
  TKRZW_STATUS_PRECONDITION_ERROR = 4,
  /** Error that a given argument is invalid. */
  TKRZW_STATUS_INVALID_ARGUMENT_ERROR = 5,
  /** Error that the operation is canceled. */
  TKRZW_STATUS_CANCELED_ERROR = 6,
  /** Error that a specific resource is not found. */
  TKRZW_STATUS_NOT_FOUND_ERROR = 7,
  /** Error that the operation is not permitted. */
  TKRZW_STATUS_PERMISSION_ERROR = 8,
  /** Error that the operation is infeasible. */
  TKRZW_STATUS_INFEASIBLE_ERROR = 9,
  /** Error that a specific resource is duplicated. */
  TKRZW_STATUS_DUPLICATION_ERROR = 10,
  /** Error that internal data are broken. */
  TKRZW_STATUS_BROKEN_DATA_ERROR = 11,
  /** Error caused by networking failure. */
  TKRZW_STATUS_NETWORK_ERROR = 12,
  /** Generic error caused by the application logic. */
  TKRZW_STATUS_APPLICATION_ERROR = 13,
};

/**
 * Pair of a status code and a message.
 */
typedef struct {
  /** The status code. */
  int32_t code;
  /** The message string. */
  const char* message;
} TkrzwStatus;

/**
 * Future interface, just for type check.
 */
typedef struct {
  /** A dummy member which is never used. */
  void* _dummy_;
} TkrzwFuture;

/**
 * DBM interface, just for type check.
 */
typedef struct {
  /** A dummy member which is never used. */
  void* _dummy_;
} TkrzwDBM;

/**
 * Iterator interface, just for type check.
 */
typedef struct {
  /** A dummy member which is never used. */
  void* _dummy_;
} TkrzwDBMIter;

/**
 * Asynchronous DBM interface, just for type check.
 */
typedef struct {
  /** A dummy member which is never used. */
  void* _dummy_;
} TkrzwAsyncDBM;

/**
 * File interface, just for type check.
 */
typedef struct {
  /** A dummy member which is never used. */
  void* _dummy_;
} TkrzwFile;

/** The special string_view value to represent any data. */
extern const char* const TKRZW_ANY_DATA;

/**
 * Type of the record processor function.
 * @details The first parameter is an opaque argument set by the caller.  The second parameter is
 * the key pointer.  The third parameter is the key size.  The fourth parameter is the value
 * pointer or NULL if there's no existing record.  The fifth parameter is the value size or -1 if
 * there's no existing record.  The sixth parameter is the pointer where the size of the region
 * of the return value is to be stored.
 */
typedef const char* (*tkrzw_record_processor)(
    void*, const char*, int32_t, const char*, int32_t, int32_t*);

/** The special string indicating no operation. */
extern const char* const TKRZW_REC_PROC_NOOP;

/** The special string indicating removing operation. */
extern const char* const TKRZW_REC_PROC_REMOVE;

/**
 * String pointer and its size.
 */
typedef struct {
  /** The pointer to the region. */
  const char* ptr;
  /** The size of the region. */
  int32_t size;
} TkrzwStr;

/**
 * Pair of a key and its value.
 */
typedef struct {
  /** The key pointer. */
  const char* key_ptr;
  /** The key size. */
  int32_t key_size;
  /** The value pointer. */
  const char* value_ptr;
  /** The value size. */
  int32_t value_size;
} TkrzwKeyValuePair;

/**
 * Pair of a key and its processor.
 */
typedef struct {
  /** The key pointer. */
  const char* key_ptr;
  /** The key size. */
  int32_t key_size;
  /** The function pointer to process the key. */
  tkrzw_record_processor proc;
  /** An arbitrary data which is given to the callback function. */
  void* proc_arg;
} TkrzwKeyProcPair;

/**
 * Type of the file processor function.
 * @details The first parameter is an opaque argument set by the caller.  The second parameter is
 * the path of file.
 */
typedef void (*tkrzw_file_processor)(void* arg, const char*);

/**
 * Sets the status code and the message as if it is from the last system operation.
 * @param code The status code.
 * @param message The status message.  If it is NULL, no message is set.
 */
void tkrzw_set_last_status(int32_t code, const char* message);

/**
 * Gets the status code and the message of the last system operation.
 * @return The status code and the message of the last system operation.
 * @details The region of the message string is available until the this function or
 * tkrzw_get_last_status_message function is called next time.
 */
TkrzwStatus tkrzw_get_last_status();

/**
 * Gets the status code of the last system operation.
 * @return The status code of the last system operation.
 */
int32_t tkrzw_get_last_status_code();

/**
 * Gets the status message of the last system operation.
 * @return The status message of the last system operation.
 * @details The region of the message string is available until the this function or
 * tkrzw_get_last_status function is called next time.
 */
const char* tkrzw_get_last_status_message();

/**
 * Gets the string name of a status code.
 * @param code The status code.
 * @return The name of the status code.
 */
const char* tkrzw_status_code_name(int32_t code);

/**
 * Gets the number of seconds since the UNIX epoch.
 * @return The number of seconds since the UNIX epoch with microsecond precision.
 */
double tkrzw_get_wall_time();

/**
 * Gets the memory capacity of the platform.
 * @return The memory capacity of the platform in bytes, or -1 on failure.
 */
int64_t tkrzw_get_memory_capacity();

/**
 * Gets the current memory usage of the process.
 * @return The current memory usage of the process in bytes, or -1 on failure.
 */
int64_t tkrzw_get_memory_usage();

/**
 * Primary hash function for the hash database.
 * @param data_ptr The pointer to the data to calculate the hash value for.
 * @param data_size The size of the data.  If it is negative, strlen(data_ptr) is used.
 * @param num_buckets The number of buckets of the hash table.
 * @return The hash value.
 */
uint64_t tkrzw_primary_hash(const char* data_ptr, int32_t data_size, uint64_t num_buckets);

/**
 * Secondary hash function for sharding.
 * @param data_ptr The pointer to the data to calculate the hash value for.
 * @param data_size The size of the data.  If it is negative, strlen(data_ptr) is used.
 * @param num_shards The number of shards.
 * @return The hash value.
 */
uint64_t tkrzw_secondary_hash(const char* data_ptr, int32_t data_size, uint64_t num_shards);

/**
 * Releases an allocated array and its elements of allocated strings.
 * @param array The pointer to the array to release.
 * @param size The number of the elements of the array.
 */
void tkrzw_free_str_array(TkrzwStr* array, int32_t size);

/**
 * Releases an allocated array and its elements of allocated key-value pairs.
 * @param array The pointer to the array to release.
 * @param size The number of the elements of the array.
 */
void tkrzw_free_str_map(TkrzwKeyValuePair* array, int32_t size);

/**
 * Searches an array of key-value pairs for a record with the given key.
 * @param array The pointer to the array to search.
 * @param size The number of the elements of the array.
 * @param key_ptr The key pointer to search for.
 * @param key_size The key size.  If it is negative, strlen(key_ptr) is used.
 * @return The pointer to the matched record or NULL on failure.
 */
TkrzwKeyValuePair* tkrzw_search_str_map(TkrzwKeyValuePair* array, int32_t size,
                                        const char* key_ptr, int32_t key_size);

/**
 * Searches a string for a pattern matching a regular expression.
 * @param text The text to search.
 * @param pattern The regular expression pattern to search for.
 * @return The position of the first matching pattern.  If there's no matching pattern. -1 is
 * returned.  If the regular expression is invalid, -2 is returned.
 */
int32_t tkrzw_str_search_regex(const char* text, const char* pattern);

/**
 * Replaces substrings matching a pattern of regular expression.
 * @param text The text to process.
 * @param pattern The regular expression pattern to search for.
 * @param replace The replacing expression. "$&" means the entire matched pattern.  "$1", "$2",
 * and etc represent n-th bracketed patterns.
 * @return The pointer to the result string, which should be released by the free function.
 */
char* tkrzw_str_replace_regex(const char* text, const char* pattern, const char* replace);

/**
 * Gets the Levenshtein edit distance of two strings.
 * @param a A string.
 * @param b The other string.
 * @param utf If true, text is treated as UTF-8.  If false, it is treated as raw bytes.
 * @return The Levenshtein edit distance of the two strings.
 */
int32_t tkrzw_str_edit_distance_lev(const char* a, const char* b, bool utf);

/**
 * Escapes C-style meta characters in a string.
 * @param ptr The pointer to the string to convert.
 * @param size The size of the string to convert.  If it is negative, strlen(ptr) is used.
 * @param esc_nonasc If true, non-ASCII characters are excaped.
 * @param res_size The pointer to the variable to store the result string size.  If it is NULL,
 * it is not used.
 * @return The result string, which should be released by the free function.
 */
char* tkrzw_str_escape_c(const char* ptr, int32_t size, bool esc_nonasc, int32_t* res_size);

/**
 * Unescapes C-style escape sequences in a string.
 * @param ptr The pointer to the string to convert.
 * @param size The size of the string to convert.  If it is negative, strlen(ptr) is used.
 * @param res_size The pointer to the variable to store the result string size.  If it is NULL,
 * it is not used.
 * @return The result string, which should be released by the free function.
 */
char* tkrzw_str_unescape_c(const char* ptr, int32_t size, int32_t* res_size);

/**
 * Appends a string at the end of another allocated string.
 * @param modified The string to be modified.  It must be allocated by malloc.  The ownership is
 * taken.  If it is NULL, a new string is allocated.
 * @param appended The string to be appended at the end.
 * @return The result string, which should be released by the free function.
 */
char* tkrzw_str_append(char* modified, const char* appended);

/**
 * Releases the future object.
 * @param future The future object.
 */
void tkrzw_future_free(TkrzwFuture* future);

/**
 * Waits for the operation of the future object to be done.
 * @param future The future object.
 * @param timeout The waiting time in seconds.  If it is negative, no timeout is set.
 * @return True if the operation has done.  False if timeout occurs.
 */
bool tkrzw_future_wait(TkrzwFuture* future, double timeout);

/**
 * Gets the status of the operation of the future object.
 * @param future the future object.
 * @details The status is set as the last system operation status so the data can be accessed by
 * the tkrzw_get_last_status and so on.  This can be called with the future which is associated
 * to the single status without any extra data.  This can be called only once for each future.
 * object.
 */
void tkrzw_future_get(TkrzwFuture* future);

/**
 * Gets the status and the extra string data of the operation of the future object.
 * @param future the future object.
 * @param size The pointer to the variable to store the extra string size.  If it is NULL, it is
 * not used.
 * @return The pointer to the extra string data, which should be released by the free function.
 * An empty string is returned on failure.  The string data is trailed by a null code so that the
 * region can be treated as a C-style string.
 * @details The status is set as the last system operation status so the data can be accessed by
 * the tkrzw_get_last_status and so on.  This can be called with the future which is associated
 * to the status with an extra string data.  This can be called only once for each future.
 * object.
 */
char* tkrzw_future_get_str(TkrzwFuture* future, int32_t* size);

/**
 * Gets the status and the extra string pair of the operation of the future object.
 * @param future the future object.
 * @return The pointer to the extra string pair, which should be released by the free function.
 * An empty string pair is returned on failure.
 * @details The status is set as the last system operation status so the data can be accessed by
 * the tkrzw_get_last_status and so on.  This can be called with the future which is associated
 * to the status with an extra string pair.  This can be called only once for each future.
 * object.
 */
TkrzwKeyValuePair* tkrzw_future_get_str_pair(TkrzwFuture* future);

/**
 * Gets the status and the extra string array of the operation of the future object.
 * @param future the future object.
 * @param num_elems The pointer to the variable to store the number of elements of the extra
 * string array.
 * @return The pointer to an array of the extra string array, which should be released by the
 * tkrzw_free_str_array function.  An empty array is returned on failure.
 * @details The status is set as the last system operation status so the data can be accessed by
 * the tkrzw_get_last_status and so on.  This can be called with the future which is associated
 * to the status with an extra string array.  This can be called only once for each future.
 * object.
 */
TkrzwStr* tkrzw_future_get_str_array(TkrzwFuture* future, int32_t* num_elems);

/**
 * Gets the status and the extra string map of the operation of the future object.
 * @param future the future object.
 * @param num_elems The pointer to the variable to store the number of elements of the extra
 * string map.
 * @return The pointer to an array of the extra string map, which should be released by the
 * tkrzw_free_str_map function.  An empty array is returned on failure.
 * @details The status is set as the last system operation status so the data can be accessed by
 * the tkrzw_get_last_status and so on.  This can be called with the future which is associated
 * to the status with an extra string map.  This can be called only once for each future.
 * object.
 */
TkrzwKeyValuePair* tkrzw_future_get_str_map(TkrzwFuture* future, int32_t* num_elems);

/**
 * Gets the status and the extra integer data of the operation of the future object.
 * @param future the future object.
 * @return The extra integer data.
 * @details The status is set as the last system operation status so the data can be accessed by
 * the tkrzw_get_last_status and so on.  This can be called with the future which is associated
 * to the status with an extra integer data.  This can be called only once for each future.
 * object.
 */
int64_t tkrzw_future_get_int(TkrzwFuture* future);

/**
 * Opens a database file and makes a database object.
 * @param path A path of the file.
 * @param writable If true, the file is writable.  If false, it is read-only.
 * @param params Optional parameters in \"key=value,key=value\" format.  The options for the file
 * opening operation are set by "truncate", "no_create", "no_wait", "no_lock", and "sync_hard".
 * The option for the number of shards is set by "num_shards".  Other options are the same as
 * PolyDBM::OpenAdvanced.
 * @return The new database object, which should be released by the tkrzw_dbm_close function.
 * NULL is returned on failure.
 */
TkrzwDBM* tkrzw_dbm_open(const char* path, bool writable, const char* params);

/**
 * Closes the database file and releases the database object.
 * @param dbm The database object.
 * @return True on success or false on failure.
 */
bool tkrzw_dbm_close(TkrzwDBM* dbm);

/**
 * Processes a record with callback functions.
 * @param dbm The database object.
 * @param key_ptr The key pointer.
 * @param key_size The key size.  If it is negative, strlen(key_ptr) is used.
 * @param proc The callback function to process the record.
 * @param proc_arg An arbitrary data which is given to the callback function.
 * @param writable True if the processor can edit the record.
 * @return True on success or false on failure.
 * @details If the specified record exists, the value is given to the callback function.  If it
 * doesn't exist, NULL is given instead.  The callback function returns TKRZW_REC_PROC_NOOP to
 * keep the current value, TKRZW_REC_PROC_REMOVE to remove the record, or a string pointer to a
 * new value to set.  The ownership of the return value is not taken.
 */
bool tkrzw_dbm_process(
    TkrzwDBM* dbm, const char* key_ptr, int32_t key_size, tkrzw_record_processor proc,
    void* proc_arg, bool writable);

/**
 * Checks if a record exists or not.
 * @param dbm The database object.
 * @param key_ptr The key pointer.
 * @param key_size The key size.  If it is negative, strlen(key_ptr) is used.
 * @return True if the record exists, or false if not.
 */
bool tkrzw_dbm_check(TkrzwDBM* dbm, const char* key_ptr, int32_t key_size);

/**
 * Gets the value of a record of a key.
 * @param dbm The database object.
 * @param key_ptr The key pointer.
 * @param key_size The key size.  If it is negative, strlen(key_ptr) is used.
 * @param value_size The pointer to the variable to store the value size.  If it is NULL, it is
 * not used.
 * @return The pointer to the value data, which should be released by the free function.  NULL
 * is returned on failure.  The value data is trailed by a null code so that the region can be
 * treated as a C-style string.
 * @details If there's no matching record, NOT_FOUND_ERROR status code is set.
 */
char* tkrzw_dbm_get(TkrzwDBM* dbm, const char* key_ptr, int32_t key_size, int32_t* value_size);

/**
 * Gets the values of multiple records of keys.
 * @param dbm The database object.
 * @param keys An array of the keys of records to retrieve.
 * @param num_keys The number of elements of the key array.
 * @param num_matched The pointer to the variable to store the number of the elements of the
 * return value.
 * @return The pointer to an array of matched key-value pairs.  This function returns an empty
 * array on failure.  If all records of the given keys are found, the status is set SUCCESS.
 * If one or more records are missing, NOT_FOUND_ERROR is set.  The array and its elements are
 * allocated dynamically so they should be released by the tkrzw_free_str_map function.
 */
TkrzwKeyValuePair* tkrzw_dbm_get_multi(
    TkrzwDBM* dbm, const TkrzwStr* keys, int32_t num_keys, int32_t* num_matched);

/**
 * Sets a record of a key and a value.
 * @param dbm The database object.
 * @param key_ptr The key pointer.
 * @param key_size The key size.  If it is negative, strlen(key_ptr) is used.
 * @param value_ptr The value pointer.
 * @param value_size The value size.  If it is negative, strlen(value_ptr) is used.
 * @param overwrite Whether to overwrite the existing value if there's a record with the same
 * key.  If true, the existing value is overwritten by the new value.  If false, the operation
 * is given up and an error status is set.
 * @return True on success or false on failure.
 * @details If overwriting is abandoned, DUPLICATION_ERROR status code is set.
 */
bool tkrzw_dbm_set(
    TkrzwDBM* dbm, const char* key_ptr, int32_t key_size,
    const char* value_ptr, int32_t value_size, bool overwrite);

/**
 * Sets a record and get the old value.
 * @param dbm The database object.
 * @param key_ptr The key pointer.
 * @param key_size The key size.  If it is negative, strlen(key_ptr) is used.
 * @param value_ptr The new value pointer.
 * @param value_size The new value size.  If it is negative, strlen(value_ptr) is used.
 * @param overwrite Whether to overwrite the existing value if there's a record with the same
 * key.  If true, the existing value is overwritten by the new value.  If false, the operation
 * is given up and an error status is set.
 * @param old_value_size The pointer to the variable to store the value size.  If it is NULL, it
 * is not used.
 * @return The pointer to the old value data, which should be released by the free function.  NULL
 * is returned if there's no existing record.  The value data is trailed by a null code so that
 * the region can be treated as a C-style string.
 * @details If overwriting is abandoned, DUPLICATION_ERROR status code is set.
 */
char* tkrzw_dbm_set_and_get(
    TkrzwDBM* dbm, const char* key_ptr, int32_t key_size,
    const char* value_ptr, int32_t value_size, bool overwrite, int32_t* old_value_size);

/**
 * Sets multiple records.
 * @param dbm The database object.
 * @param records An array of the key-value pairs of records to store.
 * @param num_records The number of elements of the record array.
 * @param overwrite Whether to overwrite the existing value if there's a record with the same
 * key.  If true, the existing value is overwritten by the new value.  If false, the operation
 * is given up and an error status is set.
 * @return True on success or false on failure.  If there are records avoiding overwriting, false
 * is returned and DUPLICATION_ERROR status code is set.
 */
bool tkrzw_dbm_set_multi(
    TkrzwDBM* dbm, const TkrzwKeyValuePair* records, int32_t num_records, bool overwrite);

/**
 * Removes a record of a key.
 * @param dbm The database object.
 * @param key_ptr The key pointer.
 * @param key_size The key size.  If it is negative, strlen(key_ptr) is used.
 * @return True on success or false on failure.
 * @details If there's no matching record, NOT_FOUND_ERROR status code is set.
 */
bool tkrzw_dbm_remove(TkrzwDBM* dbm, const char* key_ptr, int32_t key_size);

/**
 * Removes a record and get the value.
 * @param dbm The database object.
 * @param key_ptr The key pointer.
 * @param key_size The key size.  If it is negative, strlen(key_ptr) is used.
 * @param value_size The pointer to the variable to store the value size.  If it is NULL, it is
 * not used.
 * @return The pointer to the value data, which should be released by the free function.  NULL
 * is returned if there's no existing record.  The value data is trailed by a null code so that
 * the region can be treated as a C-style string.
 * @details If there's no matching record, NOT_FOUND_ERROR status code is set.
 */
char* tkrzw_dbm_remove_and_get(TkrzwDBM* dbm, const char* key_ptr, int32_t key_size,
                               int32_t* value_size);

/**
 * Removes records of keys.
 * @param dbm The database object.
 * @param keys An array of the keys of records to retrieve.
 * @param num_keys The number of elements of the key array.
 * @return True on success or false on failure.  If there are missing records, false is returned
 * and NOT_FOUND_ERROR status code is set.
 */
bool tkrzw_dbm_remove_multi(TkrzwDBM* dbm, const TkrzwStr* keys, int32_t num_keys);

/**
 * Appends data at the end of a record of a key.
 * @param dbm The database object.
 * @param key_ptr The key pointer.
 * @param key_size The key size.  If it is negative, strlen(key_ptr) is used.
 * @param value_ptr The value pointer.
 * @param value_size The value size.  If it is negative, strlen(value_ptr) is used.
 * @param delim_ptr The delimiter pointer.
 * @param delim_size The delimiter size.  If it is negative, strlen(delim_ptr) is used.
 * @return True on success or false on failure.
 * @details If there's no existing record, the value is set without the delimiter.
 */
bool tkrzw_dbm_append(
    TkrzwDBM* dbm, const char* key_ptr, int32_t key_size,
    const char* value_ptr, int32_t value_size,
    const char* delim_ptr, int32_t delim_size);

/**
 * Appends data to multiple records.
 * @param dbm The database object.
 * @param records An array of the key-value pairs of records to append.
 * @param num_records The number of elements of the record array.
 * @param delim_ptr The delimiter pointer.
 * @param delim_size The delimiter size.  If it is negative, strlen(delim_ptr) is used.
 * @return True on success or false on failure.
 * @details If there's no existing record, the value is set without the delimiter.
 */
bool tkrzw_dbm_append_multi(
    TkrzwDBM* dbm, const TkrzwKeyValuePair* records, int32_t num_records,
    const char* delim_ptr, int32_t delim_size);

/**
 * Compares the value of a record and exchanges if the condition meets.
 * @param dbm The database object.
 * @param key_ptr The key pointer.
 * @param key_size The key size.  If it is negative, strlen(key_ptr) is used.
 * @param expected_ptr The expected value pointer.  If it is NULL, no existing record is
 * expected.  If it is TKRZW_ANY_DATA, an existing record with any value is expacted.
 * @param expected_size The expected value size.  If it is negative, strlen(expected_ptr) is used.
 * @param desired_ptr The desired value pointer.  If it is NULL, the record is to be removed.
 * expected.  If it is TKRZW_ANY_DATA, no update is done.
 * @param desired_size The desired value size.  If it is negative, strlen(desired_ptr) is used.
 * @return True on success or false on failure.
 * @details If the condition doesn't meet, INFEASIBLE_ERROR status code is set.
 */
bool tkrzw_dbm_compare_exchange(
    TkrzwDBM* dbm, const char* key_ptr, int32_t key_size,
    const char* expected_ptr, int32_t expected_size,
    const char* desired_ptr, int32_t desired_size);

/**
 * Does compare-and-exchange and/or gets the old value of the record.
 * @param dbm The database object.
 * @param key_ptr The key pointer.
 * @param key_size The key size.  If it is negative, strlen(key_ptr) is used.
 * @param expected_ptr The expected value pointer.  If it is NULL, no existing record is
 * expected.  If it is TKRZW_ANY_DATA, an existing record with any value is expacted.
 * @param expected_size The expected value size.  If it is negative, strlen(expected_ptr) is used.
 * @param desired_ptr The desired value pointer.  If it is NULL, the record is to be removed.
 * expected.  If it is TKRZW_ANY_DATA, no update is done.
 * @param desired_size The desired value size.  If it is negative, strlen(desired_ptr) is used.
 * @param actual_size The pointer to the variable to store the value size.  If it is NULL, it
 * is not used.
 * @return The pointer to the old value data, which should be released by the free function.  NULL
 * is returned if there's no existing record.  The value data is trailed by a null code so that
 * the region can be treated as a C-style string.
 * @details If the condition doesn't meet, INFEASIBLE_ERROR status code is set.
 */
char* tkrzw_dbm_compare_exchange_and_get(
    TkrzwDBM* dbm, const char* key_ptr, int32_t key_size,
    const char* expected_ptr, int32_t expected_size,
    const char* desired_ptr, int32_t desired_size, int32_t* actual_size);

/**
 * Increments the numeric value of a record.
 * @param dbm The database object.
 * @param key_ptr The key pointer.
 * @param key_size The key size.  If it is negative, strlen(key_ptr) is used.
 * @param increment The incremental value.  If it is TKRZW_INT64MIN, the current value is not
 * changed and a new record is not created.
 * @param initial The initial value.
 * @return The current value or TKRZW_INT64MIN on failure.
 * @details The record value is stored as an 8-byte big-endian integer.  Negative is also
 * supported.
 */
int64_t tkrzw_dbm_increment(
    TkrzwDBM* dbm, const char* key_ptr, int32_t key_size,
    int64_t increment, int64_t initial);

/**
 * Processes multiple records with processors.
 * @param dbm The database object.
 * @param key_proc_pairs An array of pairs of the keys and their processor functions.
 * @param num_pairs The number of the array.
 * @param writable True if the processors can edit the records.
 * @return True on success or false on failure.
 * @details If the specified record exists, the value is given to the callback function.  If it
 * doesn't exist, NULL is given instead.  The callback function returns TKRZW_REC_PROC_NOOP
 * to keep the current value, TKRZW_REC_PROC_REMOVE to remove the record, or a string pointer to a
 * new value to set.  The ownership of the return value is not taken.
 */
bool tkrzw_dbm_process_multi(
    TkrzwDBM* dbm, TkrzwKeyProcPair* key_proc_pairs, int32_t num_pairs, bool writable);

/**
 * Compares the values of records and exchanges if the condition meets.
 * @param dbm The database object.
 * @param expected An array of the record keys and their expected values.  If the value is NULL,
 * no existing record is expected.  If the value is TKRZW_ANY_DATA, an existing record with any
 * value is expacted.
 * @param num_expected The number of the expected array.
 * @param desired An array of the record keys and their desired values.  If the value is NULL,
 * the record is to be removed.
 * @param num_desired The number of the desired array.
 * @return True on success or false on failure.
 * @details If the condition doesn't meet, INFEASIBLE_ERROR is returned.
 */
bool tkrzw_dbm_compare_exchange_multi(
    TkrzwDBM* dbm, const TkrzwKeyValuePair* expected, int32_t num_expected,
    const TkrzwKeyValuePair* desired, int32_t num_desired);

/**
 * Changes the key of a record.
 * @param dbm The database object.
 * @param old_key_ptr The old key pointer.
 * @param old_key_size The old key size.  If it is negative, strlen(old_key_ptr) is used.
 * @param new_key_ptr The new key pointer.
 * @param new_key_size The new key size.  If it is negative, strlen(new_key_ptr) is used.
 * @param overwrite Whether to overwrite the existing record of the new key.
 * @param copying Whether to retain the record of the old key.
 * @return True on success or false on failure.
 * @details If there's no matching record to the old key, NOT_FOUND_ERROR is set.  If the
 * overwrite flag is false and there is an existing record of the new key, DUPLICATION ERROR is
 * set.  This method is done atomically by ProcessMulti.  The other threads observe that the
 * record has either the old key or the new key.  No intermediate states are observed.
 */
bool tkrzw_dbm_rekey(
    TkrzwDBM* dbm, const char* old_key_ptr, int32_t old_key_size,
    const char* new_key_ptr, int32_t new_key_size, bool overwrite, bool copying);

/**
 * Processes the first record with a processor.
 * @param dbm The database object.
 * @param proc The callback function to process the record.
 * @param proc_arg An arbitrary data which is given to the callback function.
 * @param writable True if the processor can edit the record.
 * @details If the first record exists, the callback function is called.  Otherwise, this
 * method fails and the callback is not called.  If the callback function returns
 * TKRZW_REC_PROC_NOOP, TKRZW_REC_PROC_REMOVE, or a string pointer to a new value, whose
 * ownership is not taken.
 */
bool tkrzw_dbm_process_first(
    TkrzwDBM* dbm, tkrzw_record_processor proc, void* proc_arg, bool writable);

/**
 * Gets the first record and removes it.
 * @param dbm The database object.
 * @param key_ptr The pointer to a variable which points to the region containing the record key.
 * If this function returns true, the region should be released by the free function.  If it is
 * NULL, it is not used.
 * @param key_size The pointer to a variable which stores the size of the region containing the
 * record key.  If it is NULL, it is not used.
 * @param value_ptr The pointer to a variable which points to the region containing the record
 * value.  If this function returns true, the region should be released by the free function.
 * If it is NULL, it is not used.
 * @param value_size The pointer to a variable which stores the size of the region containing
 * the record value.  If it is NULL, it is not used.
 * @return True on success or false on failure.
 */
bool tkrzw_dbm_pop_first(TkrzwDBM* dbm, char** key_ptr, int32_t* key_size,
                         char** value_ptr, int32_t* value_size);

/**
 * Adds a record with a key of the current timestamp.
 * @param dbm The database object.
 * @param value_ptr The value pointer.
 * @param value_size The value size.  If it is negative, strlen(value_ptr) is used.
 * @param wtime The current wall time used to generate the key.  If it is negative, the system
 * clock is used.
 * @return True on success or false on failure.
 * @details The key is generated as an 8-bite big-endian binary string of the timestamp.  If
 * there is an existing record matching the generated key, the key is regenerated and the
 * attempt is repeated until it succeeds.
 */
bool tkrzw_dbm_push_last(TkrzwDBM* dbm, const char* value_ptr, int32_t value_size, double wtime);

/**
 * Processes each and every record in the database with a processor.
 * @param dbm The database object.
 * @param proc The callback function to process the record.
 * @param proc_arg An arbitrary data which is given to the callback function.
 * @param writable True if the processor can edit the record.
 * @details If the specified record exists, the value is given to the callback function.  If it
 * doesn't exist, NULL is given instead.  The callback function returns TKRZW_REC_PROC_NOOP to
 * keep the current value, TKRZW_REC_PROC_REMOVE to remove the record, or a string pointer to a
 * new value to set.  The ownership of the return value is not taken.  It is also called once
 * before the iteration and once after the iteration with both the key and the value being NULL.
 */
bool tkrzw_dbm_process_each(
    TkrzwDBM* dbm, tkrzw_record_processor proc, void* proc_arg, bool writable);

/**
 * Gets the number of records.
 * @param dbm The database object.
 * @return The number of records or -1 on failure.
 */
int64_t tkrzw_dbm_count(TkrzwDBM* dbm);

/**
 * Gets the current file size of the database.
 * @param dbm The database object.
 * @return The current file size of the database or -1 on failure.
 */
int64_t tkrzw_dbm_get_file_size(TkrzwDBM* dbm);

/**
 * Gets the path of the database file.
 * @param dbm The database object.
 * @return The pointer to the path data, which should be released by the free function.  NULL
 * is returned on failure.
 */
char* tkrzw_dbm_get_file_path(TkrzwDBM* dbm);

/**
 * Gets the timestamp in seconds of the last modified time.
 * @param dbm The database object.
 * @return The timestamp of the last modified time, or NaN on failure.
 */
double tkrzw_dbm_get_timestamp(TkrzwDBM* dbm);

/**
 * Removes all records.
 * @param dbm The database object.
 * @return True on success or false on failure.
 */
bool tkrzw_dbm_clear(TkrzwDBM* dbm);

/**
 * Rebuilds the entire database.
 * @param dbm The database object.
 * @param params Optional parameters in \"key=value,key=value\" format.  The parameters work in
 * the same way as with PolyDBM::RebuildAdvanced.
 * @return True on success or false on failure.
 */
bool tkrzw_dbm_rebuild(TkrzwDBM* dbm, const char* params);

/**
 * Checks whether the database should be rebuilt.
 * @return True to be rebuilt, or false to not be rebuilt.
 */
bool tkrzw_dbm_should_be_rebuilt(TkrzwDBM* dbm);

/**
 * Synchronizes the content of the database to the file system.
 * @param dbm The database object.
 * @param hard True to do physical synchronization with the hardware or false to do only
 * logical synchronization with the file system.
 * @param proc The callback function to process the file, which is called while the content of
 * the file is synchronized.  If it is NULL, it is ignored.
 * @param proc_arg An arbitrary data which is given to the callback function.
 * @param params Optional parameters in \"key=value,key=value\" format.  The parameters work in
 * the same way as with PolyDBM::OpenAdvanced.
 * @return True on success or false on failure.
 */
bool tkrzw_dbm_synchronize(
    TkrzwDBM* dbm, bool hard, tkrzw_file_processor proc, void* proc_arg, const char* params);

/**
 * Copies the content of the database files to other files.
 * @param dbm The database object.
 * @param dest_path The path prefix to the destination files.
 * @param sync_hard True to do physical synchronization with the hardware.
 * @return True on success or false on failure.
 * @details Copying is done while the content is synchronized and stable.  So, this method is
 * suitable for making a backup file while running a database service.
 */
bool tkrzw_dbm_copy_file_data(TkrzwDBM* dbm, const char* dest_path, bool sync_hard);

/**
 * Exports all records to another database.
 * @param dbm The database object.
 * @param dest_dbm The destination database object.
 * @return True on success or false on failure.
 */
bool tkrzw_dbm_export(TkrzwDBM* dbm, TkrzwDBM* dest_dbm);

/**
 * Exports all records of a database to a flat record file.
 * @param dbm The database object.
 * @param dest_file The file object to write records in.
 * @return True on success or false on failure.
 */
bool tkrzw_dbm_export_to_flat_records(TkrzwDBM* dbm, TkrzwFile* dest_file);

/**
 * Imports records to a database from a flat record file.
 * @param dbm The database object.
 * @param src_file The file object to read records from.
 * @return True on success or false on failure.
 */
bool tkrzw_dbm_import_from_flat_records(TkrzwDBM* dbm, TkrzwFile* src_file);

/**
 * Exports the keys of all records of a database as lines to a text file.
 * @param dbm The database object of the database.
 * @param dest_file The file object to write keys in.
 * @return True on success or false on failure.
 */
bool tkrzw_dbm_export_keys_as_lines(TkrzwDBM* dbm, TkrzwFile* dest_file);

/**
 * Inspects the database.
 * @param dbm The database object.
 * @param num_records The pointer to the variable to store the number of the elements of the
 * return value.
 * @return The pointer to an array of property key-value pairs.  This function returns an empty
 * array on failure.  The array and its elements are allocated dynamically so they should be
 * released by the tkrzw_free_str_map function.
 */
TkrzwKeyValuePair* tkrzw_dbm_inspect(TkrzwDBM* dbm, int32_t* num_records);

/**
 * Checks whether the database is writable.
 * @param dbm The database object.
 * @return True if the database is writable, or false if not.
 */
bool tkrzw_dbm_is_writable(TkrzwDBM* dbm);

/**
 * Checks whether the database condition is healthy.
 * @param dbm The database object.
 * @return True if the database condition is healthy, or false if not.
 */
bool tkrzw_dbm_is_healthy(TkrzwDBM* dbm);

/**
 * Checks whether ordered operations are supported.
 * @param dbm The database object.
 * @return True if ordered operations are supported, or false if not.
 */
bool tkrzw_dbm_is_ordered(TkrzwDBM* dbm);

/**
 * Searches a database and get keys which match a pattern, according to a mode expression.
 * @param dbm The database object.
 * @param mode The search mode.  "contain" extracts keys containing the pattern.  "begin"
 * extracts keys beginning with the pattern.  "end" extracts keys ending with the pattern.
 * "regex" extracts keys partially matches the pattern of a regular expression.  "edit"
 * extracts keys whose edit distance to the pattern is the least.  "editbin" extracts keys whose
 * edit distance to the binary pattern is the least.  "containcase", "containword", and
 * "containcaseword" extract keys considering case and word boundary.  "contain*", "containcase*",
 * "containword*", and "containcaseword*" take a null-code-separatable pattern and do batch
 * operations for each element.  Ordered databases support "upper" and "lower" which extract keys
 * whose positions are equal to or upper/lower than the pattern.  "upperex" and "lowerex" are
 * their exclusive versions.
 * @param pattern_ptr The pattern pointer.
 * @param pattern_size The pattern size.  If it is negative, strlen(pattern_ptr) is used.
 * @param capacity The maximum records to obtain.  0 means unlimited.
 * @param num_matched The pointer to the variable to store the number of the element of the
 * return value.
 * @return The pointer to an array of matched keys or NULL on failure.  If not NULL, the array
 * and its elements are allocated dynamically so they should be released by the
 * tkrzw_free_str_array function.
 */
TkrzwStr* tkrzw_dbm_search(
    TkrzwDBM* dbm, const char* mode, const char* pattern_ptr, int32_t pattern_size,
    int32_t capacity, int32_t* num_matched);

/**
 * Makes an iterator for each record.
 * @param dbm The database object.
 * @return The new iterator object, which should be released by the tkrzw_dbm_iter_free function.
 */
TkrzwDBMIter* tkrzw_dbm_make_iterator(TkrzwDBM* dbm);

/**
 * Releases the iterator object.
 * @param iter The iterator object.
 */
void tkrzw_dbm_iter_free(TkrzwDBMIter* iter);

/**
 * Initializes the iterator to indicate the first record.
 * @param iter The iterator object.
 * @return True on success or false on failure.
 * @details Even if there's no record, the operation doesn't fail.
 */
bool tkrzw_dbm_iter_first(TkrzwDBMIter* iter);

/**
 * Initializes the iterator to indicate the last record.
 * @param iter The iterator object.
 * @return True on success or false on failure.
 * @details Even if there's no record, the operation doesn't fail.  This method is suppoerted
 * only by ordered databases.
 */
bool tkrzw_dbm_iter_last(TkrzwDBMIter* iter);

/**
 * Initializes the iterator to indicate a specific record.
 * @param iter The iterator object.
 * @param key_ptr The key pointer.
 * @param key_size The key size.  If it is negative, strlen(key_ptr) is used.
 * @return True on success or false on failure.
 * @details Ordered databases can support "lower bound" jump; If there's no record with the
 * same key, the iterator refers to the first record whose key is greater than the given key.
 * The operation fails with unordered databases if there's no record with the same key.
 */
bool tkrzw_dbm_iter_jump(TkrzwDBMIter* iter, const char* key_ptr, int32_t key_size);

/**
 * Initializes the iterator to indicate the last record whose key is lower than a given key.
 * @param iter The iterator object.
 * @param key_ptr The key pointer.
 * @param key_size The key size.  If it is negative, strlen(key_ptr) is used.
 * @param inclusive If true, the condition is inclusive: equal to or lower than the key.
 * @return True on success or false on failure.
 * @details Even if there's no matching record, the operation doesn't fail.  This method is
 * suppoerted only by ordered databases.
 */
bool tkrzw_dbm_iter_jump_lower(TkrzwDBMIter* iter, const char* key_ptr, int32_t key_size,
                               bool inclusive);

/**
 * Initializes the iterator to indicate the first record whose key is upper than a given key.
 * @param iter The iterator object.
 * @param key_ptr The key pointer.
 * @param key_size The key size.  If it is negative, strlen(key_ptr) is used.
 * @param inclusive If true, the condition is inclusive: equal to or upper than the key.
 * @return True on success or false on failure.
 * @details Even if there's no matching record, the operation doesn't fail.  This method is
 * suppoerted only by ordered databases.
 */
bool tkrzw_dbm_iter_jump_upper(TkrzwDBMIter* iter, const char* key_ptr, int32_t key_size,
                               bool inclusive);

/**
 * Moves the iterator to the next record.
 * @param iter The iterator object.
 * @return True on success or false on failure.
 * @details If the current record is missing, the operation fails.  Even if there's no next
 * record, the operation doesn't fail.
 */
bool tkrzw_dbm_iter_next(TkrzwDBMIter* iter);

/**
 * Moves the iterator to the previous record.
 * @return True on success or false on failure.
 * @details If the current record is missing, the operation fails.  Even if there's no previous
 * record, the operation doesn't fail.  This method is suppoerted only by ordered databases.
 */
bool tkrzw_dbm_iter_previous(TkrzwDBMIter* iter);

/**
 * Processes the current record with a processor.
 * @param iter The iterator object.
 * @param proc The callback function to process the record.
 * @param proc_arg An arbitrary data which is given to the callback function.
 * @param writable True if the processor can edit the record.
 * @return True on success or false on failure.
 * @details If the current record exists, the callback function is called.  Otherwise, this
 * method fails and the callback is not called.  The callback function returns TKRZW_REC_PROC_NOOP
 * to keep the current value, TKRZW_REC_PROC_REMOVE to remove the record, or a string pointer to a
 * new value to set.  The ownership of the return value is not taken.  If the current record is
 * removed, the iterator is moved to the next record.
 */
bool tkrzw_dbm_iter_process(
    TkrzwDBMIter* iter, tkrzw_record_processor proc, void* proc_arg, bool writable);

/**
 * Gets the key and the value of the current record of the iterator.
 * @param iter The iterator object.
 * @param key_ptr The pointer to a variable which points to the region containing the record key.
 * If this function returns true, the region should be released by the free function.  If it is
 * NULL, it is not used.
 * @param key_size The pointer to a variable which stores the size of the region containing the
 * record key.  If it is NULL, it is not used.
 * @param value_ptr The pointer to a variable which points to the region containing the record
 * value.  If this function returns true, the region should be released by the free function.
 * If it is NULL, it is not used.
 * @param value_size The pointer to a variable which stores the size of the region containing
 * the record value.  If it is NULL, it is not used.
 */
bool tkrzw_dbm_iter_get(
    TkrzwDBMIter* iter, char** key_ptr, int32_t* key_size,
    char** value_ptr, int32_t* value_size);

/**
 * Gets the key of the current record, in a simple way.
 * @param iter The iterator object.
 * @param key_size The pointer to the variable to store the key size.  If it is NULL, it is
 * not used.
 * @return The pointer to the key data, which should be released by the free function.  NULL
 * is returned on failure.
 */
char* tkrzw_dbm_iter_get_key(TkrzwDBMIter* iter, int32_t* key_size);

/**
 * Gets the key of the current record, in a simple way.
 * @param iter The iterator object.
 * @param value_size The pointer to the variable to store the value size.  If it is NULL, it is
 * not used.
 * @return The pointer to the key data, which should be released by the free function.  NULL
 * is returned on failure.
 */
char* tkrzw_dbm_iter_get_value(TkrzwDBMIter* iter, int32_t* value_size);

/**
 * Sets the value of the current record.
 * @param iter The iterator object.
 * @param value_ptr The value pointer.
 * @param value_size The value size.  If it is negative, strlen(value_ptr) is used.
 * @return True on success or false on failure.
 */
bool tkrzw_dbm_iter_set(TkrzwDBMIter* iter, const char* value_ptr, int32_t value_size);

/**
 * Removes the current record.
 * @return True on success or false on failure.
 * @details If possible, the iterator moves to the next record.
 */
bool tkrzw_dbm_iter_remove(TkrzwDBMIter* iter);

/**
 * Gets the current record and moves the iterator to the next record.
 * @param iter The iterator object.
 * @param key_ptr The pointer to a variable which points to the region containing the record key.
 * If this function returns true, the region should be released by the free function.  If it is
 * NULL, it is not used.
 * @param key_size The pointer to a variable which stores the size of the region containing the
 * record key.  If it is NULL, it is not used.
 * @param value_ptr The pointer to a variable which points to the region containing the record
 * value.  If this function returns true, the region should be released by the free function.
 * If it is NULL, it is not used.
 * @param value_size The pointer to a variable which stores the size of the region containing
 * the record value.  If it is NULL, it is not used.
 */
bool tkrzw_dbm_iter_step(
    TkrzwDBMIter* iter, char** key_ptr, int32_t* key_size,
    char** value_ptr, int32_t* value_size);

/**
 * Restores a broken database as a new healthy database.
 * @param old_file_path The path of the broken database.
 * @param new_file_path The path of the new database to be created.
 * @param class_name The name of the database class.  If it is NULL or empty, the class is
 * guessed from the file extension.
 * @param end_offset The exclusive end offset of records to read.  Negative means unlimited.
 * 0 means the size when the database is synched or closed properly.  Using a positive value
 * is not meaningful if the number of shards is more than one.
 * @param cipher_key The encryption key for cipher compressors.  If it is NULL, an empty key is
 * used.
 * @return True on success or false on failure.
 */
bool tkrzw_dbm_restore_database(
    const char* old_file_path, const char* new_file_path,
    const char* class_name, int64_t end_offset, const char* cipher_key);

/**
 * Creates an asynchronous database adapter.
 * @param dbm A database object which has been opened.  The ownership is not taken.
 * @param num_worker_threads The number of threads in the internal thread pool.
 * @return The new database object, which should be released by the tkrzw_async_dbm_destruct
 * function.
 * @details This class is a wrapper of DBM for asynchronous operations.  A task queue with a
 * thread pool is used inside.  Every methods except for the constructor and the destructor are
 * run by a thread in the thread pool and the result is set in the future oject of the return
 * value.  While the caller should release the returned future object, it can omit evaluation of
 * the result status if it is not interesting.  The destructor of this asynchronous database
 * manager waits for all tasks to be done.  Therefore, the destructor should be called before the
 * database is closed.  Asynchronous functions which take pointers to C-strings copies the regions
 * of them so the caller doesn't have to expand the lifetime of them.
 */
TkrzwAsyncDBM* tkrzw_async_dbm_new(TkrzwDBM* dbm, int32_t num_worker_threads);

/**
 * Releases the asynchronous database adapter.
 * @param async the asynchronous database adapter.
 */
void tkrzw_async_dbm_free(TkrzwAsyncDBM* async);

/**
 * Gets the value of a record of a key asynchronously.
 * @param async the asynchronous database adapter.
 * @param key_ptr The key pointer.
 * @param key_size The key size.  If it is negative, strlen(key_ptr) is used.
 * @return The future object to monitor the result.  The future object should be released by the
 * tkrzw_future_free function.  The result should be gotten by the tkrzw_future_get_str function.
 * @details If there's no matching record, NOT_FOUND_ERROR status code is set.
 */
TkrzwFuture* tkrzw_async_dbm_get(TkrzwAsyncDBM* async, const char* key_ptr, int32_t key_size);

/**
 * Gets the values of multiple records of keys asynchronously.
 * @param async the asynchronous database adapter.
 * @param keys An array of the keys of records to retrieve.
 * @param num_keys The number of elements of the key array.
 * @return The future object to monitor the result.  The future object should be released by the
 * tkrzw_future_free function.  The result should be gotten by the tkrzw_future_get_str_map
 * function.
 */
TkrzwFuture* tkrzw_async_dbm_get_multi(
    TkrzwAsyncDBM* async, const TkrzwStr* keys, int32_t num_keys);

/**
 * Sets a record of a key and a value asynchronously.
 * @param async the asynchronous database adapter.
 * @param key_ptr The key pointer.
 * @param key_size The key size.  If it is negative, strlen(key_ptr) is used.
 * @param value_ptr The value pointer.
 * @param value_size The value size.  If it is negative, strlen(value_ptr) is used.
 * @param overwrite Whether to overwrite the existing value if there's a record with the same
 * key.  If true, the existing value is overwritten by the new value.  If false, the operation
 * is given up and an error status is set.
 * @return The future object to monitor the result.  The future object should be released by the
 * tkrzw_future_free function.  The result should be gotten by the tkrzw_future_get function.
 * @details If overwriting is abandoned, DUPLICATION_ERROR status code is set.
 */
TkrzwFuture* tkrzw_async_dbm_set(
    TkrzwAsyncDBM* async, const char* key_ptr, int32_t key_size,
    const char* value_ptr, int32_t value_size, bool overwrite);

/**
 * Sets multiple records asynchronously.
 * @param async the asynchronous database adapter.
 * @param records An array of the key-value pairs of records to store.
 * @param num_records The number of elements of the record array.
 * @param overwrite Whether to overwrite the existing value if there's a record with the same
 * key.  If true, the existing value is overwritten by the new value.  If false, the operation
 * is given up and an error status is set.
 * @return The future object to monitor the result.  The future object should be released by the
 * tkrzw_future_free function.  The result should be gotten by the tkrzw_future_get function.
 * @details If overwriting is abandoned, DUPLICATION_ERROR status code is set.
 */
TkrzwFuture* tkrzw_async_dbm_set_multi(
    TkrzwAsyncDBM* async, const TkrzwKeyValuePair* records, int32_t num_records,
    bool overwrite);

/**
 * Removes a record of a key asynchronously.
 * @param async the asynchronous database adapter.
 * @param key_ptr The key pointer.
 * @param key_size The key size.  If it is negative, strlen(key_ptr) is used.
 * @return The future object to monitor the result.  The future object should be released by the
 * tkrzw_future_free function.  The result should be gotten by the tkrzw_future_get function.
 * @details If there's no matching record, NOT_FOUND_ERROR status code is set.
 */
TkrzwFuture* tkrzw_async_dbm_remove(TkrzwAsyncDBM* async, const char* key_ptr, int32_t key_size);

/**
 * Removes records of keys asynchronously.
 * @param async the asynchronous database adapter.
 * @param keys An array of the keys of records to retrieve.
 * @param num_keys The number of elements of the key array.
 * @return The future object to monitor the result.  The future object should be released by the
 * tkrzw_future_free function.  The result should be gotten by the tkrzw_future_get function.
 * @details If there are missing records, NOT_FOUND_ERROR status code is set.
 */
TkrzwFuture* tkrzw_async_dbm_remove_multi(
    TkrzwAsyncDBM* async, const TkrzwStr* keys, int32_t num_keys);

/**
 * Appends data at the end of a record of a key asynchronously.
 * @param async the asynchronous database adapter.
 * @param key_ptr The key pointer.
 * @param key_size The key size.  If it is negative, strlen(key_ptr) is used.
 * @param value_ptr The value pointer.
 * @param value_size The value size.  If it is negative, strlen(value_ptr) is used.
 * @param delim_ptr The delimiter pointer.
 * @param delim_size The delimiter size.  If it is negative, strlen(delim_ptr) is used.
 * @return The future object to monitor the result.  The future object should be released by the
 * tkrzw_future_free function.  The result should be gotten by the tkrzw_future_get function.
 * @details If there's no existing record, the value is set without the delimiter.
 */
TkrzwFuture* tkrzw_async_dbm_append(
    TkrzwAsyncDBM* async, const char* key_ptr, int32_t key_size,
    const char* value_ptr, int32_t value_size,
    const char* delim_ptr, int32_t delim_size);

/**
 * Appends data to multiple records asynchronously.
 * @param async the asynchronous database adapter.
 * @param records An array of the key-value pairs of records to append.
 * @param num_records The number of elements of the record array.
 * @param delim_ptr The delimiter pointer.
 * @param delim_size The delimiter size.  If it is negative, strlen(delim_ptr) is used.
 * @return The future object to monitor the result.  The future object should be released by the
 * tkrzw_future_free function.  The result should be gotten by the tkrzw_future_get function.
 * @details If there's no existing record, the value is set without the delimiter.
 */
TkrzwFuture* tkrzw_async_dbm_append_multi(
    TkrzwAsyncDBM* async, const TkrzwKeyValuePair* records, int32_t num_records,
    const char* delim_ptr, int32_t delim_size);

/**
 * Compares the value of a record and exchanges if the condition meets asynchronously.
 * @param async the asynchronous database adapter.
 * @param key_ptr The key pointer.
 * @param key_size The key size.  If it is negative, strlen(key_ptr) is used.
 * @param expected_ptr The expected value pointer.  If it is NULL, no existing record is
 * expected.  If it is TKRZW_ANY_DATA, an existing record with any value is expacted.
 * @param expected_size The expected value size.  If it is negative, strlen(expected_ptr) is used.
 * @param desired_ptr The desired value pointer.  If it is NULL, the record is to be removed.
 * expected.  If it is TKRZW_ANY_DATA, no update is done.
 * @param desired_size The desired value size.  If it is negative, strlen(desired_ptr) is used.
 * @return The future object to monitor the result.  The future object should be released by the
 * tkrzw_future_free function.  The result should be gotten by the tkrzw_future_get function.
 * @details If the condition doesn't meet, INFEASIBLE_ERROR status code is set.
 */
TkrzwFuture* tkrzw_async_dbm_compare_exchange(
    TkrzwAsyncDBM* async, const char* key_ptr, int32_t key_size,
    const char* expected_ptr, int32_t expected_size,
    const char* desired_ptr, int32_t desired_size);

/**
 * Increments the numeric value of a record asynchronously.
 * @param async the asynchronous database adapter.
 * @param key_ptr The key pointer.
 * @param key_size The key size.  If it is negative, strlen(key_ptr) is used.
 * @param increment The incremental value.  If it is TKRZW_INT64MIN, the current value is not
 * changed and a new record is not created.
 * @param initial The initial value.
 * @return The future object to monitor the result.  The future object should be released by the
 * tkrzw_future_free function.  The result should be gotten by the tkrzw_future_get_int function.
 * @details The record value is stored as an 8-byte big-endian integer.  Negative is also
 * supported.
 */
TkrzwFuture* tkrzw_async_dbm_increment(
    TkrzwAsyncDBM* async, const char* key_ptr, int32_t key_size,
    int64_t increment, int64_t initial);

/**
 * Compares the values of records and exchanges if the condition meets asynchronously.
 * @param async the asynchronous database adapter.
 * @param expected An array of the record keys and their expected values.  If the value is NULL,
 * no existing record is expected.  If the value is TKRZW_ANY_DATA, an existing record with any
 * value is expacted.
 * @param num_expected The number of the expected array.
 * @param desired An array of the record keys and their desired values.  If the value is NULL,
 * the record is to be removed.
 * @param num_desired The number of the desired array.
 * @return The future object to monitor the result.  The future object should be released by the
 * tkrzw_future_free function.  The result should be gotten by the tkrzw_future_get function.
 * @details If the condition doesn't meet, INFEASIBLE_ERROR is returned.
 */
TkrzwFuture* tkrzw_async_dbm_compare_exchange_multi(
    TkrzwAsyncDBM* async, const TkrzwKeyValuePair* expected, int32_t num_expected,
    const TkrzwKeyValuePair* desired, int32_t num_desired);

/**
 * Changes the key of a record asynchronously.
 * @param async the asynchronous database adapter.
 * @param old_key_ptr The old key pointer.
 * @param old_key_size The old key size.  If it is negative, strlen(old_key_ptr) is used.
 * @param new_key_ptr The new key pointer.
 * @param new_key_size The new key size.  If it is negative, strlen(new_key_ptr) is used.
 * @param overwrite Whether to overwrite the existing record of the new key.
 * @param copying Whether to retain the record of the old key.
 * @return The future object to monitor the result.  The future object should be released by the
 * tkrzw_future_free function.  The result should be gotten by the tkrzw_future_get function.
 * @details If there's no matching record to the old key, NOT_FOUND_ERROR is set.  If the
 * overwrite flag is false and there is an existing record of the new key, DUPLICATION ERROR is
 * set.  This method is done atomically by ProcessMulti.  The other threads observe that the
 * record has either the old key or the new key.  No intermediate states are observed.
 */
TkrzwFuture* tkrzw_async_dbm_rekey(
    TkrzwAsyncDBM* async, const char* old_key_ptr, int32_t old_key_size,
    const char* new_key_ptr, int32_t new_key_size, bool overwrite, bool copying);

/**
 * Gets the first record and removes it asynchronously.
 * @param async the asynchronous database adapter.
 * @return The future object to monitor the result.  The future object should be released by the
 * tkrzw_future_free function.  The result should be gotten by the tkrzw_future_get_str_pair
 * function.
 */
TkrzwFuture* tkrzw_async_dbm_pop_first(TkrzwAsyncDBM* async);

/**
 * Adds a record with a key of the current timestamp.
 * @param async the asynchronous database adapter.
 * @param value_ptr The value pointer.
 * @param value_size The value size.  If it is negative, strlen(value_ptr) is used.
 * @param wtime The current wall time used to generate the key.  If it is negative, the system
 * clock is used.
 * @return The future object to monitor the result.  The future object should be released by the
 * tkrzw_future_free function.  The result should be gotten by the tkrzw_future_get function.
 * @details The key is generated as an 8-bite big-endian binary string of the timestamp.  If
 * there is an existing record matching the generated key, the key is regenerated and the
 * attempt is repeated until it succeeds.
 */
TkrzwFuture* tkrzw_async_dbm_push_last(
    TkrzwAsyncDBM* async, const char* value_ptr, int32_t value_size, double wtime);

/**
 * Removes all records asynchronously.
 * @param async the asynchronous database adapter.
 * @return The future object to monitor the result.  The future object should be released by the
 * tkrzw_future_free function.  The result should be gotten by the tkrzw_future_get function.
 */
TkrzwFuture* tkrzw_async_dbm_clear(TkrzwAsyncDBM* async);

/**
 * Rebuilds the entire database asynchronously.
 * @param async the asynchronous database adapter.
 * @param params Optional parameters in \"key=value,key=value\" format.  The parameters work in
 * the same way as with PolyDBM::RebuildAdvanced.
 * @return The future object to monitor the result.  The future object should be released by the
 * tkrzw_future_free function.  The result should be gotten by the tkrzw_future_get function.
 */
TkrzwFuture* tkrzw_async_dbm_rebuild(TkrzwAsyncDBM* async, const char* params);

/**
 * Synchronizes the content of the database to the file system asynchronously.
 * @param async the asynchronous database adapter.
 * @param hard True to do physical synchronization with the hardware or false to do only
 * logical synchronization with the file system.
 * @param params Optional parameters in \"key=value,key=value\" format.  The parameters work in
 * the same way as with PolyDBM::OpenAdvanced.
 * @return The future object to monitor the result.  The future object should be released by the
 * tkrzw_future_free function.  The result should be gotten by the tkrzw_future_get function.
 */
TkrzwFuture* tkrzw_async_dbm_synchronize(
    TkrzwAsyncDBM* async, bool hard, const char* params);

/**
 * Copies the content of the database files to other files.
 * @param async the asynchronous database adapter.
 * @param dest_path The path prefix to the destination files.
 * @param sync_hard True to do physical synchronization with the hardware.
 * @return The future object to monitor the result.  The future object should be released by the
 * tkrzw_future_free function.  The result should be gotten by the tkrzw_future_get function.
 * @details Copying is done while the content is synchronized and stable.  So, this method is
 * suitable for making a backup file while running a database service.
 */
TkrzwFuture* tkrzw_async_dbm_copy_file_data(
    TkrzwAsyncDBM* async, const char* dest_path, bool sync_hard);

/**
 * Exports all records to another database.
 * @param async the asynchronous database adapter.
 * @param dest_dbm The destination database object.  The lefetime of the database object
 * must last until the task finishes.
 * @return The future object to monitor the result.  The future object should be released by the
 * tkrzw_future_free function.  The result should be gotten by the tkrzw_future_get function.
 */
TkrzwFuture* tkrzw_async_dbm_export(TkrzwAsyncDBM* async, TkrzwDBM* dest_dbm);

/**
 * Exports all records of a database to a flat record file.
 * @param async the asynchronous database adapter.
 * @param dest_file The file object to write records in.  The lefetime of the file object
 * must last until the task finishes.
 * @return The future object to monitor the result.  The future object should be released by the
 * tkrzw_future_free function.  The result should be gotten by the tkrzw_future_get function.
 */
TkrzwFuture* tkrzw_async_dbm_export_to_flat_records(
    TkrzwAsyncDBM* async, TkrzwFile* dest_file);

/**
 * Imports records to a database from a flat record file.
 * @param async the asynchronous database adapter.
 * @param src_file The file object to read records from.  The lefetime of the file object
 * must last until the task finishes.
 * @return The future object to monitor the result.  The future object should be released by the
 * tkrzw_future_free function.  The result should be gotten by the tkrzw_future_get function.
 */
TkrzwFuture* tkrzw_async_dbm_import_from_flat_records(
    TkrzwAsyncDBM* async, TkrzwFile* src_file);

/**
 * Searches a database and get keys asynchronously.
 * @param async the asynchronous database adapter.
 * @param mode The search mode.  "contain" extracts keys containing the pattern.  "begin"
 * extracts keys beginning with the pattern.  "end" extracts keys ending with the pattern.
 * "regex" extracts keys partially matches the pattern of a regular expression.  "edit"
 * extracts keys whose edit distance to the pattern is the least.  Ordered databases support
 * "upper" and "lower" which extract keys whose positions are equal to or upper/lower than the
 * pattern.  "upperex" and "lowerex" are their exclusive versions.
 * @param pattern_ptr The pattern pointer.
 * @param pattern_size The pattern size.  If it is negative, strlen(pattern_ptr) is used.
 * @param capacity The maximum records to obtain.  0 means unlimited.
 * @return The future object to monitor the result.  The future object should be released by the
 * tkrzw_future_free function.  The result should be gotten by the tkrzw_future_get_str_array
 * function.
 */
TkrzwFuture* tkrzw_async_dbm_search(
    TkrzwAsyncDBM* async, const char* mode, const char* pattern_ptr, int32_t pattern_size,
    int32_t capacity);

/**
 * Opens a file.
 * @param path A path of the file.
 * @param writable If true, the file is writable.  If false, it is read-only.
 * @param params Optional parameters in \"key=value,key=value\" format.
 * @return The new file object, which should be released by the tkrzw_dbm_close function.
 * NULL is returned on failure.
 * @details The optional parameters can include options for the file opening operation.
 *   - truncate (bool): True to truncate the file.
 *   - no_create (bool): True to omit file creation.
 *   - no_wait (bool): True to fail if the file is locked by another process.
 *   - no_lock (bool): True to omit file locking.
 *   - sync_hard (bool): True to do physical synchronization when closing.
 * @details The optional parameter "file" specifies the internal file implementation class.
 * The default file class is "MemoryMapAtomicFile".  The other supported classes are
 * "StdFile", "MemoryMapAtomicFile", "PositionalParallelFile", and "PositionalAtomicFile".
 * @details For the file "PositionalParallelFile" and "PositionalAtomicFile", these optional
 * parameters are supported.
 *   - block_size (int): The block size to which all blocks should be aligned.
 *   - access_options (str): Values separated by colon.  "direct" for direct I/O.  "sync" for
 *     synchrnizing I/O, "padding" for file size alignment by padding, "pagecache" for the mini
 *     page cache in the process.
 */
TkrzwFile* tkrzw_file_open(const char* path, bool writable, const char* params);

/**
 * Closes the file.
 * @param file The file object.
 * @return True on success or false on failure.
 */
bool tkrzw_file_close(TkrzwFile* file);

/**
 * Reads data.
 * @param file The file object.
 * @param off The offset of a source region.
 * @param buf The pointer to the destination buffer.
 * @param size The size of the data to be read.
 * @return True on success or false on failure.
 */
bool tkrzw_file_read(TkrzwFile* file, int64_t off, void* buf, size_t size);

/**
 * Writes data.
 * @param file The file object.
 * @param off The offset of the destination region.
 * @param buf The pointer to the source buffer.
 * @param size The size of the data to be written.
 * @return True on success or false on failure.
 */
bool tkrzw_file_write(TkrzwFile* file, int64_t off, const void* buf, size_t size);

/**
 * Appends data at the end of the file.
 * @param file The file object.
 * @param buf The pointer to the source buffer.
 * @param size The size of the data to be written.
 * @param off The pointer to an integer object to contain the offset at which the data has been
 * put.  If it is nullptr, it is ignored.
 * @return True on success or false on failure.
 */
bool tkrzw_file_append(TkrzwFile* file, const void* buf, size_t size, int64_t* off);

/**
 * Truncates the file.
 * @param file The file object.
 * @param size The new size of the file.
 * @return True on success or false on failure.
 * @details If the file is shrunk, data after the new file end is discarded.  If the file is
 * expanded, null codes are filled after the old file end.
 */
bool tkrzw_file_truncate(TkrzwFile* file, int64_t size);

/**
 * Synchronizes the content of the file to the file system.
 * @param file The file object.
 * @param hard True to do physical synchronization with the hardware or false to do only
 * logical synchronization with the file system.
 * @param off The offset of the region to be synchronized.
 * @param size The size of the region to be synchronized.  If it is zero, the length to the
 * end of file is specified.
 * @return True on success or false on failure.
 * @details The pysical file size can be larger than the logical size in order to improve
 * performance by reducing frequency of allocation.  Thus, you should call this function before
 * accessing the file with external tools.
 */
bool tkrzw_file_synchronize(TkrzwFile* file, bool hard, int64_t off, int64_t size);

/**
 * Gets the size of the file.
 * @param file The file object.
 * @return The size of the on success, or -1 on failure.
 */
int64_t tkrzw_file_get_size(TkrzwFile* file);

/**
 * Gets the path of the file.
 * @param file The file object.
 * @return The pointer to the path data, which should be released by the free function.  NULL
 * is returned on failure.
 */
char* tkrzw_file_get_path(TkrzwFile* file);

/**
 * Searches the file and get lines which match a pattern, according to a mode expression.
 * @param file The file object.
 * @param mode The search mode.  "contain" extracts keys containing the pattern.  "begin"
 * extracts keys beginning with the pattern.  "end" extracts keys ending with the pattern.
 * "regex" extracts keys partially matches the pattern of a regular expression.  "edit"
 * extracts keys whose edit distance to the UTF-8 pattern is the least.  "editbin" extracts
 * keys whose edit distance to the binary pattern is the least.
 * @param pattern_ptr The key pointer.
 * @param pattern_size The key size.  If it is negative, strlen(pattern_ptr) is used.
 * @param capacity The maximum records to obtain.  0 means unlimited.
 * @param num_matched The pointer to the variable to store the number of the element of the
 * return value.
 * @return The pointer to an array of matched keys or NULL on failure.  If not NULL, the array
 * and its elements are allocated dynamically so they should be released by the
 * tkrzw_free_str_array function.
 */
TkrzwStr* tkrzw_file_search(
    TkrzwFile* file, const char* mode, const char* pattern_ptr, int32_t pattern_size,
    int32_t capacity, int32_t* num_matched);

#if defined(__cplusplus)
}
#endif

#endif  // _TKRZW_LANGC_H

/* END OF FILE */
