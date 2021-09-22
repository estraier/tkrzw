/*************************************************************************************************
 * File system utilities
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

#ifndef _TKRZW_FILE_UTIL_H
#define _TKRZW_FILE_UTIL_H

#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include <cinttypes>

#include "tkrzw_containers.h"
#include "tkrzw_file.h"
#include "tkrzw_lib_common.h"
#include "tkrzw_thread_util.h"

namespace tkrzw {

/** Directory separator character. */
extern const char DIR_SEP_CHR;

/** Directory separator string. */
extern const char* const DIR_SEP_STR;

/** Extension separator character. */
extern const char EXT_SEP_CHR;

/** Extension separator string. */
extern const char* const EXT_SEP_STR;

/** Current directory name. */
extern const char* const CURRENT_DIR_NAME;

/** Parent directory name. */
extern const char* const PARENT_DIR_NAME;

/**
 * Makes a unique name for a temporary file.
 * @return The unique name.
 */
std::string MakeTemporaryName();

/**
 * Joins a base path and a child name.
 * @param base_path The base path.
 * @param child_name The child name.
 * @return The joined path.
 */
std::string JoinPath(const std::string& base_path, const std::string& child_name);

/**
 * Normalizes a file path.
 * @param path The path to normalize.
 * @return The normalized path.
 */
std::string NormalizePath(const std::string& path);

/**
 * Gets the base name part of a path.
 * @param path The path.
 * @return The base name part
 */
std::string PathToBaseName(const std::string& path);

/**
 * Gets the directory name part of a path.
 * @param path The path.
 * @return The directory name part.
 */
std::string PathToDirectoryName(const std::string& path);

/**
 * Gets the extention part of a path.
 * @param path The path.
 * @return The extension part or an empty string on failure.
 */
std::string PathToExtension(const std::string& path);

/**
 * Gets the normalized and canonical form of a path.
 * @param path The path to check.
 * @param real_path The pointer to a string object to store the content.
 * @return The result status.
 */
Status GetRealPath(const std::string& path, std::string* real_path);

/**
 * Status information of a file.
 */
struct FileStatus final {
  /** Whether it is a regular file. */
  bool is_file = false;
  /** Whether it is a directory. */
  bool is_directory = false;
  /** File size in bytes. */
  int64_t file_size = 0;
  /** Modified time duration from the UNIX epoch. */
  int64_t modified_time = 0;
};

/**
 * Reads status information of a file.
 * @param path The path to the file.
 * @param fstats The pointer to a file status object.
 * @return The result status.
 */
Status ReadFileStatus(const std::string& path, FileStatus* fstats);

/**
 * Checks if a path indicates a regular file.
 * @param path The path to check.
 * @return True if the path indicates a regular file.
 */
bool PathIsFile(const std::string& path);

/**
 * Gets the size of a file.
 * @param path The path to the file.
 * @return The file size on success or -1 on failure.
 */
int64_t GetFileSize(const std::string& path);

/**
 * Checks if a path indicates a directory.
 * @param path The path to check.
 * @return True if the path indicates a directory.
 */
bool PathIsDirectory(const std::string& path);

/**
 * Gets the path to a directory for temporary files.
 * @return The path of the directory for temporary files.
 */
std::string GetPathToTemporaryDirectory();

/**
 * Writes a file with a content.
 * @param path The path of the file to write.
 * @param content The content.
 * @return The result status.
 */
Status WriteFile(const std::string& path, std::string_view content);

/**
 * Writes a file with a content, in an atomic manner by file renaming.
 * @param path The path of the file to write.
 * @param content The content.
 * @param tmp_path The path of the temporary file which is renamed to the above path.  If it is
 * empty, a string made of the original path and the extension ".tmp" is used.
 * @return The result status.
 */
Status WriteFileAtomic(const std::string& path, std::string_view content,
                       const std::string& tmp_path = "");

/**
 * Reads the content from a file.
 * @param path The path of the file to make.
 * @param content The pointer to a string object to contain the content.
 * @param max_size The maximum size in bytes to read.
 * @return The result status.
 */
Status ReadFile(const std::string& path, std::string* content, int64_t max_size = INT32MAX);

/**
 * Reads the content from a file, in a simple way.
 * @param path The path of the file to make.
 * @param default_value The value to be returned on failure.
 * @param max_size The maximum size in bytes to read.
 * @return The content of the file on success, or the default value on failure.
 */
std::string ReadFileSimple(const std::string& path, std::string_view default_value = "",
                           int64_t max_size = INT32MAX);

/**
 * Truncates a file.
 * @param path The path of the file to truncate.
 * @param size The new size of the file.
 * @return The result status.
 */
Status TruncateFile(const std::string& path, int64_t size);

/**
 * Removes a file.
 * @param path The path of the file.
 * @return The result status.
 */
Status RemoveFile(const std::string& path);

/**
 * Renames a file.
 * @param src_path The source path of the file.
 * @param dest_path The destination path of the file.
 * @return The result status.
 * @details If there is a file at the destination path, the file is overwritten.  This function
 * can rename directories too.
 */
Status RenameFile(const std::string& src_path, const std::string& dest_path);

/**
 * Copies the data of a file.
 * @param src_path The source path of the file.
 * @param dest_path The destination path of the file.
 * @return The result status.
 * @details Copying is done in the fastest way available on the platform.
 */
Status CopyFileData(const std::string& src_path, const std::string& dest_path);

/**
 * Reads a directory.
 * @param path The path of the directory.
 * @param children A vector object to contain the names of all children.
 * @return The result status.
 */
Status ReadDirectory(const std::string& path, std::vector<std::string>* children);

/**
 * Makes a directory.
 * @param path The path of the directory.
 * @param recursive If true, parent directories are made recursively.
 * @return The result status.
 */
Status MakeDirectory(const std::string& path, bool recursive = false);

/**
 * Removes a directory.
 * @param path The path of the directory.
 * @param recursive If true, contents of children are removed recursively.
 * @return The result status.
 */
Status RemoveDirectory(const std::string& path, bool recursive = false);

/**
 * Synchronizes a file or a directory.
 * @param path The path of the file or the directory.
 * @return The result status.
 */
Status SynchronizeFile(const std::string& path);

/**
 * Temporary directory whose life duration is bound with the object.
 */
class TemporaryDirectory {
 public:
  /**
   * Constructor.
   * @param cleanup If true, the temporary directory is removed when the object dies.
   * @param prefix A prefix given to the directory.  If empty, it is not used.
   * @param base_dir A base directory to contain the directory.  If empty, the standard temporary
   * directory of the sytem is used.
   */
  TemporaryDirectory(
      bool cleanup = true, const std::string& prefix = "", const std::string& base_dir = "");

  /**
   * Destructor.
   */
  ~TemporaryDirectory();

  /**
   * Checks whether the creation of the directory is successful.
   * @return The result status.
   */
  Status CreationStatus() const;

  /**
   * Gets the path of the directory.
   * @return The path of the directory.
   */
  std::string Path() const;

  /**
   * Makes a unique path in the temporary directory.
   */
  std::string MakeUniquePath(
      const std::string& prefix = "", const std::string& suffix = "") const;

  /**
   * Removes all contents of the temporary directory.
   * @return The result status.
   */
  Status CleanUp() const;

 private:
  // True to clean up the directory.
  bool cleanup_;
  // The path of the temporary directory.
  std::string tmp_dir_path_;
  /** The creation status. */
  Status creation_status_;
};

/**
 * Page cache for buffering I/O operations.
 */
class PageCache final {
 public:
  /** The number of slots for cuncurrency. */
  static constexpr int32_t NUM_SLOTS = 16;

  /**
   * Type of callback function to read a clean buffer from the file.
   */
  typedef std::function<Status(int64_t off, void* buf, size_t size)> ReadType;

  /**
   * Type of callback function to write a dirty buffer to the file.
   */
  typedef std::function<Status(int64_t off, const void* buf, size_t size)> WriteType;

  /**
   * Constructor.
   * @param page_size The page size of the I/O operation.
   * @param capacity The capacity of the cache by the number of pages.
   * @param read_func The callback function to read a clean buffer from the file.
   * @param write_func The callback function to write a dirty buffer to the file.
   */
  PageCache(int64_t page_size, int64_t capacity, ReadType read_func, WriteType write_func);

  /**
   * Destructor.
   */
  ~PageCache();

  /**
   * Reads data.
   * @param off The offset of a source region.
   * @param buf The pointer to the destination buffer.
   * @param size The size of the data to be read.
   * @return The result status.
   */
  Status Read(int64_t off, void* buf, size_t size);

  /**
   * Writes data.
   * @param off The offset of the destination region.
   * @param buf The pointer to the source buffer.
   * @param size The size of the data to be written.
   * @return The result status.
   */
  Status Write(int64_t off, const void* buf, size_t size);

  /**
   * Flushes all dirty buffers to the file.
   * @param off The offset of the region to be synchronized.
   * @param size The size of the region to be synchronized.  If it is zero, the length to the
   * end of file is specified.
   * @return The result status.
   */
  Status Flush(int64_t off = 0, int64_t size = 0);

  /**
   * Clear all data.
   * @details Dirty buffers are not written back.
   */
  void Clear();

  /**
   * Gets the region size used for reading.
   * @return The region size used for reading.
   */
  int64_t GetRegionSize();

  /**
   * Sets the region size used for reading.
   * @param size The region size used for reading.
   */
  void SetRegionSize(int64_t size);

 private:
  /**
   * Request for a page.
   */
  struct PageRequest {
    /** The offset of the page. */
    int64_t offset;
    /** The prefix size. */
    int64_t prefix_size;
    /** The whole size of the page. */
    int64_t data_size;
    /** The data size to be copied. */
    int64_t copy_size;
  };

  /**
   * Page buffer.
   */
  struct Page {
    /** Data buffer aligned to the page size. */
    char* buf;
    /** The size of data. */
    int64_t size;
    /** Whether the buffer should be written back. */
    bool dirty;
  };

  /**
   * Slot for concurrency.
   */
  struct Slot {
    /** Offsets and pages in the slot. */
    LinkedHashMap<int64_t, Page>* pages;
    /** Mutex for consistency. */
    std::mutex mutex;
  };

  /** Make page requests for the region. */
  PageRequest* MakePageRequest(
      int64_t off, int64_t size, PageRequest* buf, size_t* num_requests);
  /** Get the slot index of the offset. */
  int32_t GetSlotIndex(int64_t off);
  /** Finds the page for the given offset. */
  Page* FindPage(Slot* slot, int64_t off);
  /** Locks all slot of the requests and fill their indices. */
  int32_t LockSlots(const PageRequest* requests, size_t num_requests, int32_t* slot_indices);
  /** Unlocks all slots. */
  void UnlockSlots(int32_t* slot_indices, int32_t num_slots);
  /** Prepareas the page for the given offset. */
  Status PreparePage(Slot* slot, int64_t off, int64_t size, bool do_load,
                     const char* batch_ptr, Page** page);
  /** Reduces pages by discarding excessive ones. */
  Status ReduceCache(Slot* slot);
  /** Writes a page and its neighbors. */
  Status WritePages(Slot* slot, int64_t off, Page* page);
  /** The size of the page request buffer. */
  static constexpr int32_t PAGE_REQUEST_BUFFER_SIZE = 3;
  /** The page size for I/O operations. */
  int64_t page_size_;
  /** The reading callback. */
  ReadType read_func_;
  /** The writing callback. */
  WriteType write_func_;
  /** The maxmum number of pages in each slot. */
  int64_t slot_capacity_;
  /** The slots for cuncurrency. */
  Slot slots_[NUM_SLOTS];
  /** The region size. */
  std::atomic_int64_t region_size_;
};

/**
 * File reader.
 */
class FileReader {
 public:
  /**
   * Constructor.
   * @param file A file object to read.  Ownership is not taken.
   */
  explicit FileReader(File* file);

  /**
   * Reads a line.
   * @param str The pointer to a string object which stores the result.
   * @param max_size The maximum size of data to read.  0 means unlimited.
   * @return The result status.  NOT_FOUND_ERROR is returned at the end of file.
   */
  Status ReadLine(std::string* str, size_t max_size = 0);

 private:
  /** The size of the input buffer. */
  static constexpr size_t BUFFER_SIZE = 8192;
  /** The input buffer. */
  char buffer_[BUFFER_SIZE];
  /** The file object, unowned. */
  File* file_;
  /** The offset in the file. */
  int64_t offset_;
  /** The size of data in the buffer. */
  size_t data_size_;
  /** The index in the buffer. */
  size_t index_;
};

/**
 * Flat record structure in the file.
 */
class FlatRecord final {
 public:
  /**
   * Enumeration of record types.
   */
  enum RecordType : int32_t {
    /** For normal records. */
    RECORD_NORMAL = 0,
    /** For metadata records. */
    RECORD_METADATA = 1,
  };

  /**
   * Constructor.
   * @param file The pointer to the file object.  The ownership is not taken.
   */
  explicit FlatRecord(File* file);

  /**
   * Destructor.
   */
  ~FlatRecord();

  /**
   * Reads the next data.
   * @param offset The offset of the record.
   * @return The result status.
   */
  Status Read(int64_t offset);

  /**
   * Gets the data.
   * @return The data.
   */
  std::string_view GetData() const;

  /**
   * Gets the record type.
   * @return The record type.
   */
  RecordType GetRecordType() const;

  /**
   * Gets the offset of the record.
   * @return The offset of the record.
   */
  size_t GetOffset() const;

  /**
   * Gets the whole size of the record.
   * @return The whole size of the record.
   */
  size_t GetWholeSize() const;

  /**
   * Writes the record in the file.
   * @param data The record data.
   * @param rec_type The record type.
   * @return The result status.
   */
  Status Write(std::string_view data, RecordType rec_type = RECORD_NORMAL);

 private:
  friend class FlatRecordReader;
  /** The magic number of the normal record. */
  static constexpr uint8_t RECORD_MAGIC_NORMAL = 0xFF;
  /** The magic number of the metadata. */
  static constexpr uint8_t RECORD_MAGIC_METADATA = 0xFE;
  /** The size of the stack buffer to read the record. */
  static constexpr size_t READ_BUFFER_SIZE = 48;
  /** The size of the stack buffer to write the record. */
  static constexpr size_t WRITE_BUFFER_SIZE = 4096;
  /** The file object, unowned. */
  File* file_;
  /** The stack buffer with the consant size. */
  char buffer_[READ_BUFFER_SIZE];
  /** The offset of the record. */
  int64_t offset_;
  /** The whole_size of the record. */
  size_t whole_size_;
  /** The pointer to the data. */
  const char* data_ptr_;
  /** The size of the data. */
  size_t data_size_;
  /** The buffer for the body data. */
  char* body_buf_;
  /** The record type. */
  RecordType rec_type_;
};

/**
 * Reader of flat records.
 */
class FlatRecordReader {
 public:
  /** The initial size of the input buffer. */
  static constexpr size_t DEFAULT_BUFFER_SIZE = 32768;

  /**
   * Constructor.
   * @param file A file object to read.  Ownership is not taken.
   * @param buffer_size The initial size of the buffer.  0 means the default buffer size 32768
   * is set.
   */
  FlatRecordReader(File* file, size_t buffer_size = 0);

  /**
   * Destructor.
   */
  ~FlatRecordReader();

  /**
   * Reads a record.
   * @param str The pointer to a string_view object which stores the result.  The region is
   * available until this method is called again or this object is deleted.
   * @param rec_type The pointer to a variable into which the record type is assigned.  If it
   * is nullptr, it is ignored.
   * @return The result status.  NOT_FOUND_ERROR is returned at the end of file.
   */
  Status Read(std::string_view* str, FlatRecord::RecordType* rec_type = nullptr);

 private:
  /** The file object, unowned. */
  File* file_;
  /** The offset in the file. */
  int64_t offset_;
  /** The input buffer. */
  char* buffer_;
  /** The size of the input buffer. */
  size_t buffer_size_;
  /** The size of data in the buffer. */
  size_t data_size_;
  /** The index in the buffer. */
  size_t index_;
};

}  // namespace tkrzw

#endif  // _TKRZW_FILE_UTIL_H

// END OF FILE
