/*************************************************************************************************
 * Time utilities
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

#ifndef _TKRZW_TIME_UTIL_H
#define _TKRZW_TIME_UTIL_H

#include <string>
#include <string_view>

#include <cinttypes>
#include <cmath>
#include <ctime>

#include "tkrzw_lib_common.h"

namespace tkrzw {

/**
 * Gets the number of seconds since the UNIX epoch.
 * @return The number of seconds since the UNIX epoch with microsecond precision.
 */
double GetWallTime();

/**
 * Gets the local calendar of a time.
 * @param wtime the time since the UNIX epoch.
 * @param cal the pointer to a calendar object to store the result.
 */
void GetLocalCalendar(int64_t wtime, struct std::tm* cal);

/**
 * Gets the universal calendar of a time.
 * @param wtime the time since the UNIX epoch.
 * @param cal the pointer to a calendar object to store the result.
 */
void GetUniversalCalendar(int64_t wtime, struct std::tm* cal);

/**
 * Makes the UNIX time from a universal calendar.
 * @param cal the time struct of the universal calendar.
 * @return The UNIX time of the universal calendar
 */
int64_t MakeUniversalTime(struct std::tm& cal);

/**
 * Gets the time difference of the local time zone.
 * @param use_cache If true, the result of the first call is cached and reused for later calls.
 * @return The time difference of the local time zone in seconds, which is positive in the east
 * of the prime meridian and negative in the west.
 */
int32_t GetLocalTimeDifference(bool use_cache = false);

/**
 * Gets the day of week of a date.
 * @param year the year of the date.
 * @param mon the month of the date.
 * @param day the day of the date.
 * @return The day of week of the date.  0 means Sunday and 6 means Saturday.
 */
int32_t GetDayOfWeek(int32_t year, int32_t mon, int32_t day);

/**
 * Formats a date as a simple string in "YYYY/MM/DD hh:mm:ss" format.
 * @param result the pointer to the region into which the result string is written.  The size of
 * the buffer should be equal to or more than 48 bytes.
 * @param wtime the time since the UNIX epoch.  If it is INT64MIN, the current time is specified.
 * @param td the time difference of the timze zone.  If it is INT32MIN, the local time zone is
 * specified.
 * @return The size of the result string excluding the sentinel null code.
 */
size_t FormatDateSimple(char* result, int64_t wtime = INT64MIN, int32_t td = INT32MIN);

/**
 * Formats a date as a simple string in "YYYY/MM/DD hh:mm:ss" format.
 * @param result the pointer to the region into which the result string is written.  The size of
 * the buffer should be equal to or more than 48 bytes.
 * @param wtime the time since the UNIX epoch.  If it is negative, the current time is specified.
 * @param td the time difference of the timze zone.  If it is INT32MIN, the local time zone is
 * specified.
 * @param frac_cols The number of columns for the fraction part.
 * @return The size of the result string excluding the sentinel null code.
 */
size_t FormatDateSimpleWithFrac(char* result, double wtime = -1, int32_t td = INT32MIN,
                                int32_t frac_cols = 6);

/**
 * Formats a date as a string in W3CDTF.
 * @param result the pointer to the region into which the result string is written.  The size of
 * the buffer should be equal to or more than 48 bytes.
 * @param wtime the time since the UNIX epoch.  If it is INT64MIN, the current time is specified.
 * @param td the time difference of the timze zone.  If it is INT32MIN, the local time zone is
 * specified.
 * @return The size of the result string excluding the sentinel null code.
 */
size_t FormatDateW3CDTF(char* result, int64_t wtime = INT64MIN, int32_t td = INT32MIN);

/**
 * Formats a date as a string in W3CDTF.
 * @param result the pointer to the region into which the result string is written.  The size of
 * the buffer should be equal to or more than 48 bytes.
 * @param wtime the time since the UNIX epoch.  If it is negative, the current time is specified.
 * @param td the time difference of the timze zone.  If it is INT32MIN, the local time zone is
 * specified.
 * @param frac_cols The number of columns for the fraction part.
 * @return The size of the result string excluding the sentinel null code.
 */
size_t FormatDateW3CDTFWithFrac(char* result, double wtime = -1, int32_t td = INT32MIN,
                                int32_t frac_cols = 6);

/**
 * Formats a date as a string in RFC 1123 format.
 * @param result the pointer to the region into which the result string is written.  The size of
 * the buffer should be equal to or more than 48 bytes.
 * @param wtime the time since the UNIX epoch.  If it is INT64MIN, the current time is specified.
 * @param td the time difference of the timze zone.  If it is INT32MIN, the local time zone is
 * specified.
 * @return The size of the result string excluding the sentinel null code.
 */
size_t FormatDateRFC1123(char* result, int64_t wtime = INT64MIN, int32_t td = INT32MIN);

/**
 * Parses a date string to get the time value since the UNIX epoch.
 * @param str the date string in decimal, hexadecimal, W3CDTF, or RFC 822 (1123).  Decimal can
 * be trailed by "s" for in seconds, "m" for in minutes, "h" for in hours, and "d" for in days.
 * @return The time value of the date or NaN if the format is invalid.
 */
double ParseDateStr(std::string_view str);

/**
 * Parses a date string in "YYYYMMDD" to get the time value since the UNIX epoch.
 * @param str the date string in "YYYYMMDD" or "YYMMDDhhmmss".  As characters except for numbers
 * are ignored, "YYYY-MM-DD" etc are also supported.
 * @param td the time difference of the timze zone.  If it is INT32MIN, the local time zone is
 * specified.
 * @return The time value of the date or NaN if the format is invalid.
 */
double ParseDateStrYYYYMMDD(std::string_view str, int32_t td = INT32MIN);

/**
 * Makes a human-readable relative time expression of a time difference.
 * @param diff The time difference in seconds.
 * @return The result relative time expression, like "1.5 days" and "2.8 hours".
 */
std::string MakeRelativeTimeExpr(double diff);

}  // namespace tkrzw

#endif  // _TKRZW_TIME_UTIL_H

// END OF FILE
