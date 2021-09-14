/*************************************************************************************************
 * File implementations by the C++ standard file stream
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

#ifndef _TKRZW_FILE_STD_H
#define _TKRZW_FILE_STD_H

#include <fstream>
#include <memory>
#include <string>
#include <vector>

#include <cinttypes>

#include "tkrzw_file.h"
#include "tkrzw_lib_common.h"

namespace tkrzw {

class StdFileImpl;

/**
 * File implementation with the std::fstream.
 * @details All operations are thread-safe; Multiple threads can access the same file
 * concurrently.
 */
class StdFile final : public File {
 public:
  /**
   * Default constructor
   */
  StdFile();

  /**
   * Destructor.
   */
  ~StdFile();

  /**
   * Copy and assignment are disabled.
   */
  explicit StdFile(const StdFile& rhs) = delete;
  StdFile& operator =(const StdFile& rhs) = delete;

  /**
   * Opens a file.
   * @param path A path of the file.
   * @param writable If true, the file is writable.  If false, it is read-only.
   * @param options Bit-sum options of File::OpenOption enums.  OPEN_NO_WAIT are OPEN_NO_LOCK
   * are ignored.
   * @return The result status.
   */
  Status Open(const std::string& path, bool writable, int32_t options = OPEN_DEFAULT) override;

  /**
   * Closes the file.
   * @return The result status.
   */
  Status Close() override;

  /**
   * Reads data.
   * @param off The offset of a source region.
   * @param buf The pointer to the destination buffer.
   * @param size The size of the data to be read.
   * @return The result status.
   */
  Status Read(int64_t off, void* buf, size_t size) override;

  /**
   * Writes data.
   * @param off The offset of the destination region.
   * @param buf The pointer to the source buffer.
   * @param size The size of the data to be written.
   * @return The result status.
   */
  Status Write(int64_t off, const void* buf, size_t size) override;

  /**
   * Appends data at the end of the file.
   * @param buf The pointer to the source buffer.
   * @param size The size of the data to be written.
   * @param off The pointer to an integer object to contain the offset at which the data has been
   * put.  If it is nullptr, it is ignored.
   * @return The result status.
   */
  Status Append(const void* buf, size_t size, int64_t* off = nullptr) override;

  /**
   * Expands the file size without writing data.
   * @param inc_size The size to increment the file size by.
   * @param old_size The pointer to an integer object to contain the old size of the file.
   * If it is nullptr, it is ignored.
   * @return The result status.
   */
  Status Expand(size_t inc_size, int64_t* old_size = nullptr) override;

  /**
   * Truncates the file.
   * @param size The new size of the file.
   * @return The result status.
   */
  Status Truncate(int64_t size) override;

  /**
   * Truncate the file fakely.
   * @param size The new size of the file.
   * @return The result status.
   * @details This doesn't modify the actual file but modifies the internal length parameter,
   * which affects behavior of Close, Synchronize, Append, Expand, and GetSize.  If the
   * specified size is more than the actual file size, the operation fails.
   */
  Status TruncateFakely(int64_t size) override;

  /**
   * Synchronizes the content of the file to the file system.
   * @param hard True to do physical synchronization with the hardware or false to do only
   * logical synchronization with the file system.
   * @param off The offset of the region to be synchronized.
   * @param size The size of the region to be synchronized.  If it is zero, the length to the
   * end of file is specified.
   * @return The result status.
   * @details This is a dummy implementation and has no effect.
   */
  Status Synchronize(bool hard, int64_t off = 0, int64_t size = 0) override;

  /**
   * Gets the size of the file.
   * @param size The pointer to an integer object to contain the result size.
   * @return The result status.
   */
  Status GetSize(int64_t* size) override;

  /**
   * Sets allocation strategy.
   * @param init_size An initial size of allocation.
   * @param inc_factor A factor to increase the size of allocation.
   * @return The result status.
   * @details This is a dummy implementation and has no effect.
   */
  Status SetAllocationStrategy(int64_t init_size, double inc_factor) override;

  /**
   * Copies internal properties to another file object.
   * @param file The other file object.
   * @return The result status.
   */
  Status CopyProperties(File* file) override;

  /**
   * Gets the path of the file.
   * @param path The pointer to a string object to store the path.
   * @return The result status.
   */
  Status GetPath(std::string* path) override;

  /**
   * Renames the file.
   * @param new_path A new path of the file.
   * @return The result status.
   */
  Status Rename(const std::string& new_path) override;

  /**
   * Disables operations related to the path.
   * @return The result status.
   * @details This should be called if the file is overwritten by external operations.
   */
  Status DisablePathOperations() override;

  /**
   * Lock this object.
   * @return The file size for a open file or -1 for an unopen file.
   * @details Unlock must be done by the caller.
   */
  int64_t Lock();

  /**
   * Unlock this object.
   * @return The file size for a open file or -1 for an unopen file.
   * @details This is allowed only by the lock holder.
   */
  int64_t Unlock();

  /**
   * Reads data in the critical section by the lock.
   * @param off The offset of a source region.
   * @param buf The pointer to the destination buffer.
   * @param size The size of the data to be read.
   * @return The result status.
   * @details This is allowed only by the lock holder.
   */
  Status ReadInCriticalSection(int64_t off, void* buf, size_t size);

  /**
   * Writes data in the critical section by the lock.
   * @param off The offset of the destination region.
   * @param buf The pointer to the source buffer.
   * @param size The size of the data to be written.
   * @return The result status.
   * @details This is allowed only by the lock holder.
   */
  Status WriteInCriticalSection(int64_t off, const void* buf, size_t size);

  /**
   * Checks whether the file is open.
   * @return True if the file is open, or false if not.
   */
  bool IsOpen() const override;

  /**
   * Checks whether operations are done by memory mapping.
   * @return Always false.  This is slow, but the file size can exceed the virtual memory.
   */
  bool IsMemoryMapping() const override {
    return false;
  }

  /**
   * Checks whether updating operations are atomic and thread-safe.
   * @return Always true.  Atomicity is assured.  All operations are thread-safe.
   */
  bool IsAtomic() const override {
    return true;
  }

  /**
   * Makes a new file object of the same concrete class.
   * @return The new file object.
   */
  std::unique_ptr<File> MakeFile() const override {
    return std::make_unique<StdFile>();
  }

 private:
  /** Pointer to the actual implementation. */
  StdFileImpl* impl_;
};

}  // namespace tkrzw

#endif  // _TKRZW_FILE_STD_H

// END OF FILE
