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
   * Destructor.
   */
  virtual ~Logger() = default;

  /**
   * Enumeration of log levels.
   */
  enum Level : int32_t {
    /** For data only for debugging. */
    DEBUG = 0,
    /** For data informative for normal operations. */
    INFO = 1,
    /** For issues which potentially cause application oddities. */
    WARN = 2,
    /** For errors which should be fixed. */
    ERROR = 3,
    /** For critical errors which immediately stop the service. */
    FATAL = 4,
  };

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
  virtual void LogF(Level level, const char* format, ...) = 0;
};

/**
 * Base implementation for logging operations.
 */
class BaseLogger : public Logger {
 public:
  /**
   * Constructor.
   * @param min_level The minimum log level to be stored.
   */
  explicit BaseLogger(Level min_level = INFO) : min_level_(min_level), separator_(" ") {}

  /**
   * Destructor.
   */
  virtual ~BaseLogger() = default;

  /**
   * Sets the minimum log level.
   * @param min_level The minimum log level to be stored.
   */
  virtual void SetMinLevel(Level min_level) {
    min_level_ = min_level;
  }

  /**
   * Sets the separator string between fields.
   * @param separator The separator string between fields.
   */
  virtual void SetSeparator(const char* separator) {
    separator_ = separator;
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
    wp += FormatDateW3CDTFWithFrac(wp, -1, INT32MIN, 6);
    std::memcpy(wp, separator_.data(), separator_.size());
    wp += separator_.size();
    switch (level) {
      case DEBUG: wp += std::sprintf(wp, "[DEBUG]"); break;
      case INFO: wp += std::sprintf(wp, "[INFO]"); break;
      case WARN: wp += std::sprintf(wp, "[WARN]"); break;
      case ERROR: wp += std::sprintf(wp, "[ERROR]"); break;
      case FATAL: wp += std::sprintf(wp, "[FATAL]"); break;
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
  virtual void Log(Level level, std::string_view message) {
    if (level < min_level_) {
      return;
    }
    WriteProperties(level, message);
  }

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
    tkrzw::VSPrintF(&msg, format, ap);
    va_end(ap);
    WriteProperties(level, msg);
  }

 protected:
  /** The minimum log level to be stored. */
  Level min_level_;
  /** The separator between fields. */
  std::string_view separator_;
};

/**
 * Stream implementation for logging operations.
 */
class StreamLogger : public BaseLogger {
 public:
  /**
   * Constructor.
   * @param stream The pointer to the output stream.  The ownership is not taken.
   */
  StreamLogger(std::ostream* stream) : stream_(stream), mutex_() {}

  /**
   * Destructor.
   */
  virtual ~StreamLogger() = default;

  /**
   * Writes a log into the media.
   * @param raw_data Formatted log data.
   */
  virtual void WriteRaw(std::string_view raw_data) {
    std::lock_guard lock(mutex_);
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
