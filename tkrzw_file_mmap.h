/*************************************************************************************************
 * File implementations by memory mapping
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

#ifndef _TKRZW_FILE_MMAP_H
#define _TKRZW_FILE_MMAP_H

#include <memory>
#include <string>

#include <cinttypes>

#include "tkrzw_file.h"
#include "tkrzw_lib_common.h"

namespace tkrzw {

/**
 * Interface for memory mapping file implementations.
 */
class MemoryMapFile : public File {
 public:
  /**
   * Destructor.
   */
  virtual ~MemoryMapFile() = default;
};

class MemoryMapParallelFileImpl;

/**
 * File implementation by memory mapping and locking for parallel operations.
 * @details Reading and writing operations are thread-safe; Multiple threads can access the same
 * file concurrently.  Other operations including Open, Close, Truncate, and Synchronize are not
 * thread-safe.  Moreover, locking doesn't assure atomicity of reading and writing operations.
 */
class MemoryMapParallelFile final : public MemoryMapFile {
 public:
  /**
   * Structure to make a shared section where a region can be accessed.
   * @details The zone object contains a mutex which is locked by the constructor and unlocked by
   * the destructor.  The user can access the region freely while the zone object is alive.
   */
  class Zone {
    friend class MemoryMapParallelFile;
   public:
    /**
     * Destuctor.
     */
    ~Zone();

    /**
     * Copy and assignment are disabled.
     */
    explicit Zone(const Zone& rhs) = delete;
    Zone& operator =(const Zone& rhs) = delete;

    /**
     * Gets the offset of the region to access.
     * @return The offset of the region to access.
     */
    int64_t Offset() const;

    /**
     * Gets the pointer to the region to access.
     * @return The pointer to the region to access.
     */
    char* Pointer() const;

    /**
     * Gets the size of the region to access.
     * @return The size of the region to access.
     */
    size_t Size() const;

   private:
    /**
     * Constructor.
     * @param file The file implementation object.
     * @param writable Whether the zone is writable.
     * @param off The offset of a region.
     * @param size The size of the data to be read.
     */
    explicit Zone(
        MemoryMapParallelFileImpl* file, bool writable,
        int64_t off, size_t size, Status* status);

    /** The file object. */
    MemoryMapParallelFileImpl* file_;
    /** The offset of the region. */
    int64_t off_;
    /** The size of the region. */
    size_t size_;
  };

  /**
   * Default constructor
   */
  MemoryMapParallelFile();

  /**
   * Destructor.
   */
  ~MemoryMapParallelFile();

  /**
   * Copy and assignment are disabled.
   */
  explicit MemoryMapParallelFile(const MemoryMapParallelFile& rhs) = delete;
  MemoryMapParallelFile& operator =(const MemoryMapParallelFile& rhs) = delete;

  /**
   * Opens a file.
   * @param path A path of the file.
   * @param writable If true, the file is writable.  If false, it is read-only.
   * @param options Bit-sum options of File::OpenOption enums.
   * @return The result status.
   * @details By default, exclusive locking against other processes is done for a writer and
   * shared locking against other processes is done for a reader.
   */
  Status Open(const std::string& path, bool writable, int32_t options = OPEN_DEFAULT) override;

  /**
   * Closes the file.
   * @return The result status.
   */
  Status Close() override;

  /**
   * Makes an accessible zone.
   * @param writable If true, the region is for reading and writing.  If false, the region is
   * only for reading.
   * @param off The offset of the region to access.
   * @param size The size of the region to access.
   * @param zone A unique pointer to own the writable zone.
   * @return The result status.
   * @details If the writable flag is true and the region is out of the current file size, the file
   * is expanded.  If the writable flag is true and the offset is negative, the offset is set at
   * the end of the file.  If the writable flag is false and the region is out of the current file
   * size, the size of the region is fitted to the file size or the operation fails.
   */
  Status MakeZone(bool writable, int64_t off, size_t size, std::unique_ptr<Zone>* zone);

  /**
   * Reads data.
   * @param off The offset of a source region.
   * @param buf The pointer to the destination buffer.
   * @param size The size of the data to be read.
   * @return The result status.
   */
  Status Read(int64_t off, void* buf, size_t size) override;

  /**
   * Reads data, in a simple way.
   * @param off The offset of a source region.
   * @param size The size of the data to be read.
   * @return A string of the read data.  It is empty on failure.
   */
  std::string ReadSimple(int64_t off, size_t size) override;

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
   * @details By default, the initial size is 1MB and the increasing factor is 2.  Note that
   * a memory map cannot be empty and its size must be aligned to the page size.  This adjustment
   * is done implicitly.  This method must be called before the file is opened.
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
   * @return Always true.  This is fast, but the file size cannot exceed the virtual memory.
   */
  bool IsMemoryMapping() const override {
    return true;
  }

  /**
   * Checks whether updating operations are atomic and thread-safe.
   * @return Always false.  Atomicity is not assured.  Some operations are not thread-safe.
   * are not.
   */
  bool IsAtomic() const override {
    return false;
  }

  /**
   * Makes a new file object of the same concrete class.
   * @return The new file object.
   */
  std::unique_ptr<File> MakeFile() const override {
    return std::make_unique<MemoryMapParallelFile>();
  }

 private:
  /** Pointer to the actual implementation. */
  MemoryMapParallelFileImpl* impl_;
};

class MemoryMapAtomicFileImpl;

/**
 * File implementation by memory mapping and locking for atomic operations.
 * @details All operations are thread-safe; Multiple threads can access the same file concurrently.
 * Also, locking assures that every operation is observed in an atomic manner.
 */
class MemoryMapAtomicFile final : public MemoryMapFile {
 public:
  /**
   * Structure to make a critical section where a region can be accessed.
   * @details The zone object contains a mutex which is locked by the constructor and unlocked by
   * the destructor.  The user can access the region freely while the zone object is alive.
   */
  class Zone {
    friend class MemoryMapAtomicFile;
   public:
    /**
     * Destuctor.
     */
    ~Zone();

    /**
     * Copy and assignment are disabled.
     */
    explicit Zone(const Zone& rhs) = delete;
    Zone& operator =(const Zone& rhs) = delete;

    /**
     * Gets the offset of the region to access.
     * @return The offset of the region to access.
     */
    int64_t Offset() const;

    /**
     * Gets the pointer to the region to access.
     * @return The pointer to the region to access.
     */
    char* Pointer() const;

    /**
     * Gets the size of the region to access.
     * @return The size of the region to access.
     */
    size_t Size() const;

   private:
    /**
     * Constructor.
     * @param file The file implementation object.
     * @param writable Whether the zone is writable.
     * @param off The offset of a region.
     * @param size The size of the data to be read.
     */
    explicit Zone(
        MemoryMapAtomicFileImpl* file, bool writable,
        int64_t off, size_t size, Status* status);

    /** The file object. */
    MemoryMapAtomicFileImpl* file_;
    /** The offset of the region. */
    int64_t off_;
    /** The size of the region. */
    size_t size_;
    /** Whether the zone is writable. */
    bool writable_;
  };

  /**
   * Default constructor
   */
  MemoryMapAtomicFile();

  /**
   * Destructor.
   */
  ~MemoryMapAtomicFile();

  /**
   * Copy and assignment are disabled.
   */
  explicit MemoryMapAtomicFile(const MemoryMapAtomicFile& rhs) = delete;
  MemoryMapAtomicFile& operator =(const MemoryMapAtomicFile& rhs) = delete;

  /**
   * Opens a file.
   * @param path A path of the file.
   * @param writable If true, the file is writable.  If false, it is read-only.
   * @param options Bit-sum options of File::OpenOption enums.
   * @return The result status.
   * @details By default, exclusive locking against other processes is done for a writer and
   * shared locking against other processes is done for a reader.
   */
  Status Open(const std::string& path, bool writable, int32_t options = OPEN_DEFAULT) override;

  /**
   * Closes the file.
   * @return The result status.
   */
  Status Close() override;

  /**
   * Makes an accessible zone.
   * @param writable If true, the region is for reading and writing.  If false, the region is
   * only for reading.
   * @param off The offset of the region to access.
   * @param size The size of the region to access.
   * @param zone A unique pointer to own the writable zone.
   * @return The result status.
   * @details If the writable flag is true and the region is out of the current file size, the file
   * is expanded.  If the writable flag is true and the offset is negative, the offset is set at
   * the end of the file.  If the writable flag is false and the region is out of the current file
   * size, the size of the region is fitted to the file size or the operation fails.
   */
  Status MakeZone(bool writable, int64_t off, size_t size, std::unique_ptr<Zone>* zone);

  /**
   * Reads data.
   * @param off The offset of a source region.
   * @param buf The pointer to the destination buffer.
   * @param size The size of the data to be read.
   * @return The result status.
   */
  Status Read(int64_t off, void* buf, size_t size) override;

  /**
   * Reads data, in a simple way.
   * @param off The offset of a source region.
   * @param size The size of the data to be read.
   * @return A string of the read data.  It is empty on failure.
   */
  std::string ReadSimple(int64_t off, size_t size) override;

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
   * @details By default, the initial size is 1MB and the increasing factor is 2.  Note that
   * a memory map cannot be empty and its size must be aligned to the page size.  This adjustment
   * is done implicitly.  This method must be called before the file is opened.
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
   * @return Always true.  This is fast, but the file size cannot exceed the virtual memory.
   */
  bool IsMemoryMapping() const override {
    return true;
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
    return std::make_unique<MemoryMapAtomicFile>();
  }

 private:
  /** Pointer to the actual implementation. */
  MemoryMapAtomicFileImpl* impl_;
};

}  // namespace tkrzw

#endif  // _TKRZW_FILE_MMAP_H

// END OF FILE
