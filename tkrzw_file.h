/*************************************************************************************************
 * File interface
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

#ifndef _TKRZW_FILE_H
#define _TKRZW_FILE_H

#include <memory>
#include <string>
#include <typeinfo>

#include <cinttypes>

#include "tkrzw_lib_common.h"

namespace tkrzw {

/**
 * Interface of file operations.
 */
class File {
 public:
  /**
   * Destructor.
   */
  virtual ~File() = default;

  /** The default value of the initial allocation size. */
  static constexpr int64_t DEFAULT_ALLOC_INIT_SIZE = 1LL << 20;
  /** The default value of the allocation increment factor. */
  static constexpr double DEFAULT_ALLOC_INC_FACTOR = 2.0;

  /**
   * Enumeration of options for Open.
   */
  enum OpenOption : int32_t {
    /** The default behavior. */
    OPEN_DEFAULT = 0,
    /** To truncate the file. */
    OPEN_TRUNCATE = 1 << 0,
    /** To omit file creation. */
    OPEN_NO_CREATE = 1 << 1,
    /** To fail if the file is locked by another process. */
    OPEN_NO_WAIT = 1 << 2,
    /** To omit file locking. */
    OPEN_NO_LOCK = 1 << 3,
    /** To do physical synchronization when closing. */
    OPEN_SYNC_HARD = 1 << 4,
  };

  /**
   * Opens a file.
   * @param path A path of the file.
   * @param writable If true, the file is writable.  If false, it is read-only.
   * @param options Bit-sum options of File::OpenOption enums.
   * @return The result status.
   * @details By default, exclusive locking against other processes is done for a writer and
   * shared locking against other processes is done for a reader.
   */
  virtual Status Open(const std::string& path, bool writable, int32_t options = OPEN_DEFAULT) = 0;

  /**
   * Closes the file.
   * @return The result status.
   */
  virtual Status Close() = 0;

  /**
   * Reads data.
   * @param off The offset of a source region.
   * @param buf The pointer to the destination buffer.
   * @param size The size of the data to be read.
   * @return The result status.
   */
  virtual Status Read(int64_t off, void* buf, size_t size) = 0;

  /**
   * Reads data, in a simple way.
   * @param off The offset of a source region.
   * @param size The size of the data to be read.
   * @return A string of the read data.  It is empty on failure.
   */
  virtual std::string ReadSimple(int64_t off, size_t size) {
    std::string data(size, 0);
    if (Read(off, const_cast<char*>(data.data()), size) != Status::SUCCESS) {
      data.clear();
    }
    return data;
  }

  /**
   * Writes data.
   * @param off The offset of the destination region.
   * @param buf The pointer to the source buffer.
   * @param size The size of the data to be written.
   * @return The result status.
   */
  virtual Status Write(int64_t off, const void* buf, size_t size) = 0;

  /**
   * Writes data, in a simple way.
   * @param off The offset of the destination region.
   * @param data The data to be written.
   * @return True on success or false on failure.
   */
  virtual bool WriteSimple(int64_t off, std::string_view data) {
    return Write(off, data.data(), data.size()) == Status::SUCCESS;
  }

  /**
   * Appends data at the end of the file.
   * @param buf The pointer to the source buffer.
   * @param size The size of the data to be written.
   * @param off The pointer to an integer object to contain the offset at which the data has been
   * put.  If it is nullptr, it is ignored.
   * @return The result status.
   */
  virtual Status Append(const void* buf, size_t size, int64_t* off = nullptr) = 0;

  /**
   * Appends data at the end of the file, in a simple way.
   * @param data The data to be written.
   * @return The offset at which the data has been put, or -1 on failure.
   */
  virtual int64_t AppendSimple(const std::string& data) {
    int64_t off = 0;
    return Append(data.data(), data.size(), &off) == Status::SUCCESS ? off : -1;
  }

  /**
   * Expands the file size without writing data.
   * @param inc_size The size to increment the file size by.
   * @param old_size The pointer to an integer object to contain the old size of the file.
   * If it is nullptr, it is ignored.
   * @return The result status.
   */
  virtual Status Expand(size_t inc_size, int64_t* old_size = nullptr) = 0;

  /**
   * Expands the file size without writing data, in a simple way.
   * @param inc_size The size to increment the file size by.
   * @return The old size of the file, or -1 on failure.
   */
  virtual int64_t ExpandSimple(size_t inc_size) {
    int64_t old_size = 0;
    return Expand(inc_size, &old_size) == Status::SUCCESS ? old_size : -1;
  }

  /**
   * Truncates the file.
   * @param size The new size of the file.
   * @return The result status.
   * @details If the file is shrunk, data after the new file end is discarded.  If the file is
   * expanded, null codes are filled after the old file end.
   */
  virtual Status Truncate(int64_t size) = 0;

  /**
   * Truncate the file fakely.
   * @param size The new size of the file.
   * @return The result status.
   * @details This doesn't modify the actual file but modifies the internal length parameter,
   * which affects behavior of Close, Synchronize, Append, Expand, and GetSize.  If the
   * specified size is more than the actual file size, the operation fails.
   size,
   */
  virtual Status TruncateFakely(int64_t size) = 0;

  /**
   * Synchronizes the content of the file to the file system.
   * @param hard True to do physical synchronization with the hardware or false to do only
   * logical synchronization with the file system.
   * @param off The offset of the region to be synchronized.
   * @param size The size of the region to be synchronized.  If it is zero, the length to the
   * end of file is specified.
   * @return The result status.
   * @details The pysical file size can be larger than the logical size in order to improve
   * performance by reducing frequency of allocation.  Thus, you should call this function before
   * accessing the file with external tools.
   */
  virtual Status Synchronize(bool hard, int64_t off = 0, int64_t size = 0) = 0;

  /**
   * Gets the size of the file.
   * @param size The pointer to an integer object to contain the result size.
   * @return The result status.
   */
  virtual Status GetSize(int64_t* size) = 0;

  /**
   * Gets the size of the file, in a simple way.
   * @return The size of the on success, or -1 on failure.
   */
  virtual int64_t GetSizeSimple() {
    int64_t size = 0;
    return GetSize(&size) == Status::SUCCESS ? size : -1;
  }

  /**
   * Sets allocation strategy.
   * @param init_size An initial size of allocation.
   * @param inc_factor A factor to increase the size of allocation.
   * @return The result status.
   * @details By default, the initial size is 1MB and the increasing factor is 2.  This method
   * must be called before the file is opened.
   */
  virtual Status SetAllocationStrategy(int64_t init_size, double inc_factor) = 0;

  /**
   * Copies internal properties to another file object.
   * @param file The other file object.
   * @return The result status.
   */
  virtual Status CopyProperties(File* file) = 0;

  /**
   * Gets the path of the file.
   * @param path The pointer to a string object to store the path.
   * @return The result status.
   */
  virtual Status GetPath(std::string* path) = 0;

  /**
   * Gets the path of the file, in a simple way.
   * @return The path of the file on success, or an empty string on failure.
   */
  virtual std::string GetPathSimple() {
    std::string path;
    return GetPath(&path) == Status::SUCCESS ? path : "";
  }

  /**
   * Renames the file.
   * @param new_path A new path of the file.
   * @return The result status.
   */
  virtual Status Rename(const std::string& new_path) = 0;

  /**
   * Disables operations related to the path.
   * @return The result status.
   * @details This should be called if the file is overwritten by external operations.
   */
  virtual Status DisablePathOperations() = 0;

  /**
   * Checks whether the file is open.
   * @return True if the file is open, or false if not.
   */
  virtual bool IsOpen() const = 0;

  /**
   * Checks whether operations are done by memory mapping.
   * @return True if operations are done by memory mapping, or false if not.
   */
  virtual bool IsMemoryMapping() const = 0;

  /**
   * Checks whether updating operations are atomic and thread-safe.
   * @return True if every updating operation is atomic and thread-safe, or false any of them
   * are not.
   */
  virtual bool IsAtomic() const = 0;

  /**
   * Makes a new file object of the same concrete class.
   * @return The new file object.
   */
  virtual std::unique_ptr<File> MakeFile() const = 0;

  /**
   * Gets the type information of the actual class.
   * @return The type information of the actual class.
   */
  const std::type_info& GetType() const {
    const auto& entity = *this;
    return typeid(entity);
  }
};

}  // namespace tkrzw

#endif  // _TKRZW_FILE_H

// END OF FILE
