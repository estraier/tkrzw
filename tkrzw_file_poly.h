/*************************************************************************************************
 * Polymorphic file adapter
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

#ifndef _TKRZW_FILE_POLY_H
#define _TKRZW_FILE_POLY_H

#include <map>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <cinttypes>

#include "tkrzw_file.h"
#include "tkrzw_lib_common.h"
#include "tkrzw_str_util.h"

namespace tkrzw {

/**
 * Polymorphic file adapter.
 * @details All operations except for Open and Close are thread-safe; Multiple threads can
 * access the same file concurrently.  Every opened file must be closed explicitly to avoid
 * data corruption.
 * @details This class is a wrapper of StdFile, MemoryMapParallelFile, MemoryMapAtomicFile, and
 * PositionalParallelFile,.  The open method specifies the actuall class used internally.
 */
class PolyFile final : public File {
 public:
  /**
   * Default constructor.
   */
  PolyFile();

  /**
   * Destructor.
   */
  virtual ~PolyFile() = default;

  /**
   * Opens a file.
   * @param path A path of the file.
   * @param writable If true, the file is writable.  If false, it is read-only.
   * @param options Bit-sum options of File::OpenOption enums.
   * @return The result status.
   * @details By default, exclusive locking against other processes is done for a writer and
   * shared locking against other processes is done for a reader.
   */
  Status Open(const std::string& path, bool writable, int32_t options = OPEN_DEFAULT) override {
    return OpenAdvanced(path, writable, options);
  }

  /**
   * Opens a file, in an advanced way.
   * @param path A path of the file.
   * @param writable If true, the file is writable.  If false, it is read-only.
   * @param options Bit-sum options of File::OpenOption enums.
   * @param params Optional parameters.
   * @return The result status.
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
  Status OpenAdvanced(const std::string& path, bool writable, int32_t options = OPEN_DEFAULT,
                      const std::map<std::string, std::string>& params = {});

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
   * which affects behavior of Close, Synchronize, Append, Expand, and GetSize.
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
   * @details The pysical file size can be larger than the logical size in order to improve
   * performance by reducing frequency of allocation.  Thus, you should call this function before
   * accessing the file with external tools.
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
   * @details By default, the initial size is 1MB and the increasing factor is 2.  This method
   * must be called before the file is opened.
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
   * Checks whether the file is open.
   * @return True if the file is open, or false if not.
   */
  bool IsOpen() const override;

  /**
   * Checks whether operations are done by memory mapping.
   * @return Always false.  This is slow, but the file size can exceed the virtual memory.
   */
  bool IsMemoryMapping() const override;

  /**
   * Checks whether updating operations are atomic and thread-safe.
   * @return Always false.  Atomicity is not assured.  Some operations are not thread-safe.
   */
  bool IsAtomic() const override;

  /**
   * Makes a new file object of the same concrete class.
   * @return The new file object.
   */
  std::unique_ptr<File> MakeFile() const override;

  /**
   * Gets the pointer to the internal file object.
   * @return The pointer to the internal file object, or nullptr on failure.
   */
  File* GetInternalFile() const;

  /**
   * Make a File instance according to the given parameters.
   * @param params The parameters to make the instance.  "file", "block_size", and
   * "access_options" are used and removed from the map.
   * @return The new instance.
   */
  static std::unique_ptr<File> MakeFileInstance(std::map<std::string, std::string>* params);

 private:
  /** The internal file object. */
  std::unique_ptr<File> file_;
};

}  // namespace tkrzw

#endif  // _TKRZW_DBM_POLY_H

// END OF FILE
