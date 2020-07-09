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

#include "tkrzw_file.h"
#include "tkrzw_lib_common.h"

namespace tkrzw {

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
 * Write a file with a content.
 * @param path The path of the file to write.
 * @param content The content.
 * @return The result status.
 */
Status WriteFile(const std::string& path, std::string_view content);

/**
 * Read the content from a file.
 * @param path The path of the file to make.
 * @param content The pointer to a string object to contain the content.
 * @return The result status.
 */
Status ReadFile(const std::string& path, std::string* content);

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
 */
Status RenameFile(const std::string& src_path, const std::string& dest_path);

/**
 * Copies a file.
 * @param src_path The source path of the file.
 * @param dest_path The destination path of the file.
 * @return The result status.
 */
Status CopyFile(const std::string& src_path, const std::string& dest_path);

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
  /** The magic number of the record. */
  static constexpr uint8_t RECORD_MAGIC = 0xFF;

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
  std::string_view GetData();

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
   * @return The result status.
   */
  Status Write(std::string_view data);

 private:
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
   * @return The result status.  NOT_FOUND_ERROR is returned at the end of file.
   */
  Status Read(std::string_view* str);

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
