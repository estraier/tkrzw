/*************************************************************************************************
 * Common library features
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

#ifndef _TKRZW_LIB_COMMON_H
#define _TKRZW_LIB_COMMON_H

#include <limits>
#include <iostream>
#include <map>
#include <memory>
#include <set>
#include <stdexcept>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

#include <cinttypes>
#include <cstdlib>
#include <cstring>

namespace tkrzw {

/** Disable macros to avoid possible name confliction. */
#undef INT8MIN
#undef INT8MAX
#undef UINT8MAX
#undef INT16MIN
#undef INT16MAX
#undef UINT16MAX
#undef INT32MIN
#undef INT32MAX
#undef UINT32MAX
#undef INT64MIN
#undef INT64MAX
#undef UINT64MAX
#undef SIZEMAX
#undef FLOATMIN
#undef FLOATMAX
#undef DOUBLEMIN
#undef DOUBLEMAX
#undef DOUBLENAN
#undef DOUBLEINF
#undef NUM_BUFFER_SIZE
#undef MAX_MEMORY_SIZE
#undef PAGE_SIZE
#undef PACKAGE_VERSION
#undef LIBRARY_VERSION
#undef OS_NAME
#undef IS_POSIX
#undef IS_BIG_ENDIAN

/** The minimum value of int8_t. */
constexpr int8_t INT8MIN = std::numeric_limits<int8_t>::min();

/** The maximum value of int8_t. */
constexpr int8_t INT8MAX = std::numeric_limits<int8_t>::max();

/** The maximum value of uint8_t. */
constexpr uint8_t UINT8MAX = std::numeric_limits<uint8_t>::max();

/** The minimum value of int16_t. */
constexpr int16_t INT16MIN = std::numeric_limits<int16_t>::min();

/** The maximum value of int16_t. */
constexpr int16_t INT16MAX = std::numeric_limits<int16_t>::max();

/** The maximum value of uint16_t. */
constexpr uint16_t UINT16MAX = std::numeric_limits<uint16_t>::max();

/** The minimum value of int32_t. */
constexpr int32_t INT32MIN = std::numeric_limits<int32_t>::min();

/** The maximum value of int32_t. */
constexpr int32_t INT32MAX = std::numeric_limits<int32_t>::max();

/** The maximum value of uint32_t. */
constexpr uint32_t UINT32MAX = std::numeric_limits<uint32_t>::max();

/** The minimum value of int64_t. */
constexpr int64_t INT64MIN = std::numeric_limits<int64_t>::min();

/** The maximum value of int64_t. */
constexpr int64_t INT64MAX = std::numeric_limits<int64_t>::max();

/** The maximum value of uint64_t. */
constexpr uint64_t UINT64MAX = std::numeric_limits<uint64_t>::max();

/** The maximum value of size_t. */
constexpr size_t SIZEMAX = std::numeric_limits<size_t>::max();

/** The minimum value of float. */
constexpr float FLOATMIN = std::numeric_limits<float>::min();

/** The maximum value of float. */
constexpr float FLOATMAX = std::numeric_limits<float>::max();

/** The minimum value of double. */
constexpr double DOUBLEMIN = std::numeric_limits<double>::min();

/** The maximum value of double. */
constexpr double DOUBLEMAX = std::numeric_limits<double>::max();

/** The quiet Not-a-Number value of double. */
constexpr double DOUBLENAN = std::numeric_limits<double>::quiet_NaN();

/** The positive infinity value of double. */
constexpr double DOUBLEINF = std::numeric_limits<double>::infinity();;

/** The buffer size for a numeric string expression. */
constexpr int32_t NUM_BUFFER_SIZE = 32;

/** The maximum memory size. */
constexpr int64_t MAX_MEMORY_SIZE = (1LL <<  40);

/** The size of a memory page on the OS. */
extern const int32_t PAGE_SIZE;

/** The string expression of the package version. */
extern const char* const PACKAGE_VERSION;

/** The string expression of the library version. */
extern const char* const LIBRARY_VERSION;

/** The recognized OS name. */
extern const char* const OS_NAME;

/** True if the OS is conforming to POSIX. */
extern const bool IS_POSIX;

/** True if the byte order is big endian. */
extern const bool IS_BIG_ENDIAN;

/**
 * Allocates a region on memory.
 * @param size The size of the region.
 * @return The pointer to the allocated region.
 */
inline void* xmalloc(size_t size) {
  void* ptr = std::malloc(size);
  if (ptr == nullptr) {
    throw std::bad_alloc();
  }
  return ptr;
}

/**
 * Allocates a nullified region on memory.
 * @param nmemb The number of elements.
 * @param size The size of each element.
 * @return The pointer to the allocated region.
 */
inline void* xcalloc(size_t nmemb, size_t size) {
  void* ptr = std::calloc(nmemb, size);
  if (ptr == nullptr) {
    throw std::bad_alloc();
  }
  return ptr;
}

/**
 * Re-allocates a region on memory.
 * @param ptr The pointer to the region.
 * @param size The size of the region.
 * @return The pointer to the re-allocated region.
 */
inline void* xrealloc(void* ptr, size_t size) {
  ptr = std::realloc(ptr, size);
  if (ptr == nullptr) {
    throw std::bad_alloc();
  }
  return ptr;
}

/**
 * Re-allocates a region on memory for appending operations.
 * @param ptr The pointer to the region.
 * @param size The size of the region.
 * @return The pointer to the re-allocated region.
 */
inline void* xreallocappend(void* ptr, size_t size) {
  size_t aligned_size = 8;
  while (aligned_size < size) {
    aligned_size += aligned_size >> 1;
  }
  return xrealloc(ptr, aligned_size);
}

/**
 * Frees a region on memory.
 * @param ptr The pointer to the region.
 */
inline void xfree(void* ptr) {
  std::free(ptr);
}

/**
 * Allocates an aligned region on memory.
 * @param alignment The alignment of the address.  It must be a power of two and more than
 * sizeof(void*).
 * @param size The size of the region.  It is ceiled implicitly to a multiple of the alignment.
 * @return The pointer to the allocated region.
 */
void* xmallocaligned(size_t alignment, size_t size);

/**
 * Frees an aligned region on memory.
 * @param ptr The pointer to the region.
 */
void xfreealigned(void* ptr);

/**
 * Checks whether a set has an element.
 * @param set The set to search.
 * @param elem The element to search for.
 * @return True if the set has the element.
 */
template <typename SET>
inline bool CheckSet(SET set, const typename SET::key_type& elem) {
  return set.find(elem) != set.end();
}

/**
 * Checks whether a map has a key.
 * @param map The map to search.
 * @param key The key to search for.
 * @return True if the map has the key.
 */
template <typename MAP>
inline bool CheckMap(MAP map, const typename MAP::key_type& key) {
  return map.find(key) != map.end();
}

/**
 * Searches a map and get the value of a record.
 * @param map The map to search.
 * @param key The key to search for.
 * @param default_value The value to be returned on failure.
 * @return The value of the matching record on success, or the default value on failure.
 */
template <typename MAP>
inline typename MAP::value_type::second_type SearchMap(
    MAP map, const typename MAP::key_type& key,
    const typename MAP::value_type::second_type& default_value) {
  const auto& it = map.find(key);
  return it == map.end() ? default_value : it->second;
}

/**
 * Gets the current processs ID.
 * @return The current processs ID.
 */
int64_t GetProcessID();

/**
 * Gets system information of the environment.
 * @return A map of labels and their values.
 */
std::map<std::string, std::string> GetSystemInfo();

/**
 * Gets the memory capacity of the platform.
 * @return The memory capacity of the platform in bytes, or -1 on failure.
 */
int64_t GetMemoryCapacity();

/**
 * Gets the current memory usage of the process.
 * @return The current memory usage of the process in bytes, or -1 on failure.
 */
int64_t GetMemoryUsage();

/**
 * Status of operations.
 */
class Status final {
 public:
  /**
   * Enumeration of status codes.
   */
  enum Code : int32_t {
    /** Success. */
    SUCCESS = 0,
    /** Generic error whose cause is unknown. */
    UNKNOWN_ERROR = 1,
    /** Generic error from underlying systems. */
    SYSTEM_ERROR = 2,
    /** Error that the feature is not implemented. */
    NOT_IMPLEMENTED_ERROR = 3,
    /** Error that a precondition is not met. */
    PRECONDITION_ERROR = 4,
    /** Error that a given argument is invalid. */
    INVALID_ARGUMENT_ERROR = 5,
    /** Error that the operation is canceled. */
    CANCELED_ERROR = 6,
    /** Error that a specific resource is not found. */
    NOT_FOUND_ERROR = 7,
    /** Error that the operation is not permitted. */
    PERMISSION_ERROR = 8,
    /** Error that the operation is infeasible. */
    INFEASIBLE_ERROR = 9,
    /** Error that a specific resource is duplicated. */
    DUPLICATION_ERROR = 10,
    /** Error that internal data are broken. */
    BROKEN_DATA_ERROR = 11,
    /** Error caused by networking failure. */
    NETWORK_ERROR = 12,
    /** Generic error caused by the application logic. */
    APPLICATION_ERROR = 13,
  };

  /**
   * Default constructor representing the success code.
   */
  Status() : code_(Code::SUCCESS), message_(nullptr) {}

  /**
   * Constructor representing a specific status.
   * @param code The status code.
   */
  explicit Status(Code code) : code_(code), message_(nullptr) {}

  /**
   * Constructor representing a specific status with a message.
   * @param code The status code.
   * @param message An arbitrary status message.
   */
  Status(Code code, std::string_view message) : code_(code), message_(nullptr) {
    message_ = static_cast<char*>(xmalloc(message.size() + 1));
    std::memcpy(message_, message.data(), message.size());
    message_[message.size()] = '\0';
  }

  /**
   * Copy constructor.
   * @param rhs The right-hand-side object.
   */
  Status(const Status& rhs) : code_(rhs.code_), message_(nullptr) {
    if (rhs.message_ != nullptr) {
      const size_t message_size = std::strlen(rhs.message_);
      message_ = static_cast<char*>(xrealloc(message_, message_size + 1));
      std::memcpy(message_, rhs.message_, message_size);
      message_[message_size] = '\0';
    }
  }

  /**
   * Move constructor.
   * @param rhs The right-hand-side object.
   */
  Status(Status&& rhs) : code_(rhs.code_), message_(rhs.message_) {
    rhs.message_ = nullptr;
  }

  /**
   * Destructor.
   */
  ~Status() {
    xfree(message_);
  }

  /**
   * Assigns the internal state from another status object.
   * @param rhs The status object.
   */
  Status& operator =(const Status& rhs) {
    if (this != &rhs) {
      code_ = rhs.code_;
      if (rhs.message_ == nullptr) {
        xfree(message_);
        message_ = nullptr;
      } else {
        const size_t message_size = std::strlen(rhs.message_);
        message_ = static_cast<char*>(xrealloc(message_, message_size + 1));
        std::memcpy(message_, rhs.message_, message_size);
        message_[message_size] = '\0';
      }
    }
    return *this;
  }

  /**
   * Assigns the internal state from another moved status object.
   * @param rhs The status object.
   */
  Status& operator =(Status&& rhs) {
    if (this != &rhs) {
      code_ = rhs.code_;
      xfree(message_);
      message_ = rhs.message_;
      rhs.message_ = nullptr;
    }
    return *this;
  }

  /**
   * Assigns the internal state from another status object only if the current state is success.
   * @param rhs The status object.
   */
  Status& operator |=(const Status& rhs) {
    if (this != &rhs && code_ == SUCCESS && rhs.code_ != SUCCESS) {
      code_ = rhs.code_;
      if (rhs.message_ == nullptr) {
        xfree(message_);
        message_ = nullptr;
      } else {
        const size_t message_size = std::strlen(rhs.message_);
        message_ = static_cast<char*>(xrealloc(message_, message_size + 1));
        std::memcpy(message_, rhs.message_, message_size);
        message_[message_size] = '\0';
      }
    }
    return *this;
  }

  /**
   * Assigns the internal state from another status object only if the current state is success.
   * @param rhs The status object.
   */
  Status& operator |=(Status&& rhs) {
    if (this != &rhs && code_ == SUCCESS && rhs.code_ != SUCCESS) {
      code_ = rhs.code_;
      xfree(message_);
      message_ = rhs.message_;
      rhs.message_ = nullptr;
    }
    return *this;
  }

  /**
   * Gets the status code.
   * @return The status code.
   */
  Code GetCode() const {
    return code_;
  }

  /**
   * Gets the status message.
   * @return The status message.
   */
  std::string GetMessage() const {
    return message_ == nullptr ? "" : message_;
  }

  /**
   * Checks whether the status has a non-empty message.
   * @return True if the status has a non-empty message.
   */
  bool HasMessage() const {
    return message_ != nullptr && *message_ != '\0';
  }

  /**
   * Makes a C string of the message.
   * @return The C message string, which should be released by the free function.
   */
  char* MakeMessageC() const {
    if (message_ == nullptr) {
      char* str = static_cast<char*>(xmalloc(1));
      *str = '\0';
      return str;
    }
    const size_t size = std::strlen(message_);
    char* str = static_cast<char*>(xmalloc(size + 1));
    std::memcpy(str, message_, size + 1);
    return str;
  }

  /**
   * Sets the code and an empty message.
   * @param code The status code.
   */
  void Set(Code code) {
    code_ = code;
    xfree(message_);
    message_ = nullptr;
  }

  /**
   * Sets the code and the message.
   * @param code The status code.
   * @param message An arbitrary status message.
   */
  void Set(Code code, std::string_view message) {
    code_ = code;
    message_ = static_cast<char*>(xrealloc(message_, message.size() + 1));
    std::memcpy(message_, message.data(), message.size());
    message_[message.size()] = '\0';
  }

  /**
   * Checks whether the internal status code is equal to a given status.
   * @param rhs The status to compare.
   * @return True if the internal status code is equal to the given status.
   */
  bool operator ==(const Status& rhs) const {
    return code_ == rhs.code_;
  }

  /**
   * Checks whether the internal status code is not equal to a given status.
   * @param rhs The status to compare.
   * @return True if the internal status code is not equal to the given status.
   */
  bool operator !=(const Status& rhs) const {
    return code_ != rhs.code_;
  }

  /**
   * Checks whether the internal status code is equal to a given code.
   * @param code The code to compare.
   * @return True if the internal status code is equal to the given code.
   */
  bool operator ==(const Code& code) const {
    return code_ == code;
  }

  /**
   * Checks whether the internal status code is not equal to a given code.
   * @param code The code to compare.
   * @return True if the internal status code is not equal to the given code.
   */
  bool operator !=(const Code& code) const {
    return code_ != code;
  }

  /**
   * Compares this object with another status object.
   * @param rhs The status to compare.
   * @return True if this object is considered less than the given object.
   */
  bool operator <(const Status& rhs) const {
    if (code_ != rhs.code_) {
      return code_ < rhs.code_;
    }
    return std::strcmp(message_ != nullptr ? message_ : "",
                       rhs.message_ != nullptr ? rhs.message_ : "");
  }

  /**
   * Gets a string expression of the status.
   * @return The string expression
   */
  operator std::string() const {
    std::string expr(CodeName(code_));
    if (message_ != nullptr) {
      expr += ": ";
      expr += message_;
    }
    return expr;
  }

  /**
   * Returns true if the status is success.
   * @return True if the status is success, or false on failure.
   */
  bool IsOK() const {
    return code_ == SUCCESS;
  }

  /**
   * Throws an exception if the status is not success.
   * @return The reference to this object.
   */
  const Status& OrDie() const;

  /**
   * Gets the string name of a status code.
   * @param code The status code.
   * @return The name of the status code.
   */
  static const char* CodeName(Code code) {
    switch (code) {
      case SUCCESS: return "SUCCESS";
      case UNKNOWN_ERROR: return "UNKNOWN_ERROR";
      case SYSTEM_ERROR: return "SYSTEM_ERROR";
      case NOT_IMPLEMENTED_ERROR: return "NOT_IMPLEMENTED_ERROR";
      case PRECONDITION_ERROR: return "PRECONDITION_ERROR";
      case INVALID_ARGUMENT_ERROR: return "INVALID_ARGUMENT_ERROR";
      case CANCELED_ERROR : return "CANCELED_ERROR";
      case NOT_FOUND_ERROR: return "NOT_FOUND_ERROR";
      case PERMISSION_ERROR: return "PERMISSION_ERROR";
      case INFEASIBLE_ERROR: return "INFEASIBLE_ERROR";
      case DUPLICATION_ERROR: return "DUPLICATION_ERROR";
      case BROKEN_DATA_ERROR: return "BROKEN_DATA_ERROR";
      case NETWORK_ERROR: return "NETWORK_ERROR";
      case APPLICATION_ERROR: return "APPLICATION_ERROR";
    }
    return "unnamed error";
  }

 private:
  /** Status code. */
  Code code_;
  /** Message string. */
  char* message_;
};

/**
 * Checks whether a status code is equal to another status object.
 * @param lhs The status code to compare.
 * @param rhs The status object to compare.
 * @return True if The status code is equal to the status object.
 */
inline bool operator ==(const Status::Code& lhs, const Status& rhs) {
  return lhs == rhs.GetCode();
}

/**
 * Checks whether a status code is not equal to another status object.
 * @param lhs The status code to compare.
 * @param rhs The status object to compare.
 * @return True if The status code is equal to the status object.
 */
inline bool operator !=(const Status::Code& lhs, const Status& rhs) {
  return lhs != rhs.GetCode();
}

/**
 * Converts a status into a string.
 * @param status The status object.
 * @return The converted string.
 */
inline std::string ToString(const Status& status) {
  return std::string(status);
}

/**
 * Outputs a status string into an output stream.
 * @param os The output stream.
 * @param status The status.
 * @return The output stream.
 */
inline std::ostream& operator<<(std::ostream& os, const Status& status) {
  return os << std::string(status);
}

/**
 * Exception to convey the status of operations.
 */
class StatusException final : public std::runtime_error {
 public:
  /**
   * Constructor.
   * @param status The status to convey.
   */
  explicit StatusException(const Status& status)
      : std::runtime_error(ToString(status)), status_(status) {}

  /**
   * Gets the status object.
   * @return The status object.
   */
  Status GetStatus() const {
    return status_;
  }

  /**
   * Gets a string expression of the status.
   * @return The string expression
   */
  operator std::string() const {
    return std::string(status_);
  }

 private:
  /** The status object. */
  Status status_;
};

/**
 * Gets a status according to a system error number of a system call.
 * @param call_name The name of the system call.
 * @param sys_err_num The value of "errno".
 * @return The status object.
 */
Status GetErrnoStatus(const char* call_name, int32_t sys_err_num);

}  // namespace tkrzw

#endif  // _TKRZW_LIB_COMMON_H

// END OF FILE
