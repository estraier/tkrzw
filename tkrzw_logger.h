/*************************************************************************************************
 * Logger interface and implementations
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

#ifndef _TKRZW_LOGGER_H
#define _TKRZW_LOGGER_H

#include <iostream>
#include <mutex>
#include <string>
#include <string_view>

#include <cinttypes>
#include <cmath>
#include <ctime>
#include <cstdarg>

#include "tkrzw_lib_common.h"
#include "tkrzw_str_util.h"
#include "tkrzw_thread_util.h"

namespace tkrzw {

/**
 * Interface for logging operations.
 */
class Logger {
 public:
  /**
   * Enumeration of log levels.
   */
  enum Level : int32_t {
    /** No log is recorded. */
    LEVEL_NONE = 0,
    /** For data only for debugging. */
    LEVEL_DEBUG = 1,
    /** For data informative for normal operations. */
    LEVEL_INFO = 2,
    /** For issues which potentially cause application oddities. */
    LEVEL_WARN = 3,
    /** For errors which should be fixed. */
    LEVEL_ERROR = 4,
    /** For critical errors which immediately stop the service. */
    LEVEL_FATAL = 5,
  };

  /**
   * Constructor.
   * @param min_level The minimum log level to be stored.
   */
  explicit Logger(Level min_level = LEVEL_INFO)
      : min_level_(min_level == LEVEL_NONE ? static_cast<Level>(INT32MAX) : min_level) {}

  /**
   * Destructor.
   */
  virtual ~Logger() = default;

  /**
   * Sets the minimum log level.
   * @param min_level The minimum log level to be stored.
   */
  virtual void SetMinLevel(Level min_level) {
    min_level_ = min_level == LEVEL_NONE ? static_cast<Level>(INT32MAX) : min_level;
  }

  /**
   * Checks whether a log level is worth storing.
   * @return True if the log level is worth storing.
   */
  virtual bool CheckLevel(Level level) const {
    return level >= min_level_;
  }

  /**
   * Logs a message.
   * @param level The log level.
   * @param message The message to write.
   */
  virtual void Log(Level level, std::string_view message) = 0;

  /**
   * Logs a formatted message.
   * @param level The log level.
   * @param format The format string.
   * @param ... The other arguments.
   */
  virtual void LogF(Level level, const char* format, ...) {
    if (level < min_level_) {
      return;
    }
    std::string msg;
    va_list ap;
    va_start(ap, format);
    VSPrintF(&msg, format, ap);
    va_end(ap);
    Log(level, msg);
  }

  /**
   * Logs a message made of substrings.
   * @param level The log level.
   * @param first The first substring.
   * @param rest The rest substrings.
   */
  template <typename FIRST, typename... REST>
  void LogCat(Level level, const FIRST& first, const REST&... rest) {
    if (level < min_level_) {
      return;
    }
    Log(level, StrCat(first, rest...));
  }

  /**
   * Parses a string to get an enum of log levels.
   * @param str The string to parse
   * @return The result enum of log levels.
   */
  static Level ParseLevelStr(std::string_view str) {
    if (str.size() > 6 && StrCaseCompare(str.substr(0, 6), "level_") == 0) {
      str = str.substr(6);
    }
    if (StrCaseCompare(str, "debug") == 0) {
      return LEVEL_DEBUG;
    }
    if (StrCaseCompare(str, "info") == 0) {
      return LEVEL_INFO;
    }
    if (StrCaseCompare(str, "warn") == 0) {
      return LEVEL_WARN;
    }
    if (StrCaseCompare(str, "error") == 0) {
      return LEVEL_ERROR;
    }
    if (StrCaseCompare(str, "fatal") == 0) {
      return LEVEL_FATAL;
    }
    return LEVEL_NONE;
  }

 protected:
  /** The minimum log level to be stored. */
  Level min_level_;
};

/**
 * Base implementation for logging operations.
 */
class BaseLogger : public Logger {
 public:
  /**
   * Enumeration of date formats.
   */
  enum DateFormat : int32_t {
    /** No date data. */
    DATE_NONE = 0,
    /** Simple format. */
    DATE_SIMPLE = 1,
    /** Simple format in microseconds. */
    DATE_SIMPLE_MICRO = 2,
    /** W3CDTF format. */
    DATE_W3CDTF = 3,
    /** Simple format in microseconds. */
    DATE_W3CDTF_MICRO = 4,
    /** RFC1123 format. */
    DATE_RFC1123 = 5,
    /** Decimal number of the UNIX epoch. */
    DATE_EPOCH = 6,
    /** Decimal number of the UNIX epoch in microseconds. */
    DATE_EPOCH_MICRO = 7,
  };

  /**
   * Constructor.
   * @param min_level The minimum log level to be stored.
   * @param separator The separator string between fields.
   * @param date_format The date format.
   * @param date_td the time difference of the timze zone.  If it is INT32MIN, the local time
   * zone is specified.
   */
  explicit BaseLogger(Level min_level = LEVEL_INFO, const char* separator = " ",
                      DateFormat date_format = DATE_SIMPLE, int32_t date_td = INT32MIN)
      : Logger(min_level), separator_(separator),
        date_format_(date_format), date_td_(date_td) {}

  /**
   * Destructor.
   */
  virtual ~BaseLogger() = default;

  /**
   * Sets the separator string between fields.
   * @param separator The separator string between fields.
   */
  virtual void SetSeparator(const char* separator) {
    separator_ = separator;
  }

  /**
   * Sets the data format of each log.
   * @param date_format The date format.
   * @param date_td the time difference of the timze zone.  If it is INT32MIN, the local time
   * zone is specified.
   */
  virtual void SetDateFormat(DateFormat date_format, int32_t date_td = INT32MIN) {
    date_format_ = date_format;
    date_td_ = date_td;
  }

  /**
   * Writes a log into the media.
   * @param raw_data Formatted log data.
   */
  virtual void WriteRaw(std::string_view raw_data) = 0;

  /**
   * Formats properties of a log into a string and write it.
   * @param level The log level.
   * @param message The message to write.
   */
  virtual void WriteProperties(Level level, std::string_view message) {
    char stack_buf[1024];
    char* wp = stack_buf;
    switch (date_format_) {
      case DATE_SIMPLE:
        wp += FormatDateSimple(wp, INT64MIN, date_td_);
        break;
      case DATE_SIMPLE_MICRO:
        wp += FormatDateSimpleWithFrac(wp, -1, date_td_, 6);
        break;
      case DATE_W3CDTF:
        wp += FormatDateW3CDTF(wp, INT64MIN, date_td_);
        break;
      case DATE_W3CDTF_MICRO:
        wp += FormatDateW3CDTFWithFrac(wp, -1, date_td_, 6);
        break;
      case DATE_RFC1123:
        wp += FormatDateRFC1123(wp, INT64MIN, date_td_);
        break;
      case DATE_EPOCH:
        wp += std::sprintf(wp, "%.0f", GetWallTime());
        break;
      case DATE_EPOCH_MICRO:
        wp += std::sprintf(wp, "%.6f", GetWallTime());
        break;
      default:
        break;
    }
    if (wp > stack_buf) {
      std::memcpy(wp, separator_.data(), separator_.size());
      wp += separator_.size();
    }
    switch (level) {
      case LEVEL_DEBUG: wp += std::sprintf(wp, "[DEBUG]"); break;
      case LEVEL_INFO: wp += std::sprintf(wp, "[INFO]"); break;
      case LEVEL_WARN: wp += std::sprintf(wp, "[WARN]"); break;
      case LEVEL_ERROR: wp += std::sprintf(wp, "[ERROR]"); break;
      case LEVEL_FATAL: wp += std::sprintf(wp, "[FATAL]"); break;
      default: break;
    }
    std::memcpy(wp, separator_.data(), separator_.size());
    wp += separator_.size();
    const size_t header_size = wp - stack_buf;
    if (header_size + message.size() < sizeof(stack_buf) - 1) {
      std::memcpy(wp, message.data(), message.size());
      WriteRaw(std::string_view(stack_buf, header_size + message.size()));
    } else {
      char* heap_buf = new char[header_size + message.size()];
      std::memcpy(heap_buf, stack_buf, header_size);
      std::memcpy(heap_buf + header_size, message.data(), message.size());
      WriteRaw(std::string_view(heap_buf, header_size + message.size()));
      delete[] heap_buf;
    }
  }

  /**
   * Logs a message.
   * @param level The log level.
   * @param message The message to write.
   */
  void Log(Level level, std::string_view message) override {
    if (level < min_level_) {
      return;
    }
    WriteProperties(level, message);
  }

  /**
   * Parses a string to get an enum of date formats.
   * @param str The string to parse
   * @return The result enum of date formats.
   */
  static DateFormat ParseDateFormatStr(std::string_view str) {
    if (str.size() > 5 && StrCaseCompare(str.substr(0, 5), "date_") == 0) {
      str = str.substr(5);
    }
    std::string norm_str = StrLowerCase(str);
    StrReplaceCharacters(&norm_str, "-_", "");
    if (norm_str == "simple") {
      return DATE_SIMPLE;
    }
    if (norm_str == "simplemicro") {
      return DATE_SIMPLE_MICRO;
    }
    if (norm_str == "w3cdtf") {
      return DATE_W3CDTF;
    }
    if (norm_str == "w3cdtfmicro") {
      return DATE_W3CDTF_MICRO;
    }
    if (norm_str == "rfc1123") {
      return DATE_RFC1123;
    }
    if (norm_str == "epoch") {
      return DATE_EPOCH;
    }
    if (norm_str == "epochmicro") {
      return DATE_EPOCH_MICRO;
    }
    return DATE_NONE;
  }

 protected:
  /** The separator between fields. */
  std::string_view separator_;
  /** The date format. */
  DateFormat date_format_;
  /** The date time difference. */
  int32_t date_td_;
};

/**
 * Stream implementation for logging operations.
 */
class StreamLogger : public BaseLogger {
 public:
  /**
   * Constructor.
   * @param stream The pointer to the output stream.  The ownership is not taken.  If it is
   * nullptr, logging is not done.
   * @param min_level The minimum log level to be stored.
   * @param separator The separator string between fields.
   * @param date_format The date format.
   * @param date_td the time difference of the timze zone.  If it is INT32MIN, the local time
   * zone is specified.
   */
  explicit StreamLogger(std::ostream* stream = nullptr,
                        Level min_level = LEVEL_INFO, const char* separator = " ",
                        DateFormat date_format = DATE_SIMPLE, int32_t date_td = INT32MIN)
      : BaseLogger(min_level, separator, date_format, date_td),
        stream_(stream), mutex_() {}

  /**
   * Destructor.
   */
  virtual ~StreamLogger() = default;

  /**
   * Sets the stream object.
   * @param stream The pointer to the output stream.  The ownership is not taken.  If it is
   * nullptr, logging is not done.
   */
  virtual void SetStream(std::ostream* stream) {
    std::lock_guard lock(mutex_);
    stream_ = stream;
  }

  /**
   * Writes a log into the media.
   * @param raw_data Formatted log data.
   */
  void WriteRaw(std::string_view raw_data) override {
    std::lock_guard lock(mutex_);
    if (stream_ == nullptr) {
      return;
    }
    *stream_ << raw_data << std::endl;
  }

 protected:
  /** The output stream. */
  std::ostream* stream_;
  /** The mutex for the stream. */
  SpinMutex mutex_;
};

}  // namespace tkrzw

#endif  // _TKRZW_LOGGER_H

// END OF FILE
