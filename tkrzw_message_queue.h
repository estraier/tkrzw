/*************************************************************************************************
 * Message queue on the file stream
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

#ifndef _TKRZW_MESSAGE_QUEUE_H
#define _TKRZW_MESSAGE_QUEUE_H

#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <cinttypes>

#include "tkrzw_lib_common.h"

namespace tkrzw {

class MessageQueueImpl;
class MessageQueueReaderImpl;

/**
 * Message queue on the file stream.
 */
class MessageQueue final {
 public:
  /**
   * Messsage reader.
   */
  class Reader final {
    friend class MessageQueue;
   public:
    /**
     * Destructor.
     */
    ~Reader();

    /**
     * Reads a message from the queue.
     * @param timeout The timeout to wait for in seconds.
     * @param timestamp The pointer to a variable to store the timestamp of the message.
     * @param message The pointer to a string object to store the msssage data.
     * @return The result status.
     */
    Status Read(double timeout, double* timestamp, std::string* message);

   private:
    /**
     * Constructor.
     * @param queue_impl The queue implementation object.
     * @param min_timestamp The minimum timestamp of messages to read.
     */
    Reader(MessageQueueImpl* queue_impl, double min_timestamp);

    /** Pointer to the actual implementation. */
    MessageQueueReaderImpl* impl_;
  };

  /**
   * Default constructor.
   */
  MessageQueue();

  /**
   * Destructor.
   */
  ~MessageQueue();

  /**
   * Opens the queue files to store the messages.
   * @param prefix The prefix for the file names.  The actual name of each file has the suffix
   * of the current local date in ".YYYYMMDDhhmmss" format, like ".20210211183005".
   * @param max_file_size The maximum size of each file.  When the actual file exceeds the limit,
   * a new file is created and new messages are written into it.
   * @param timestamp The timestamp of the new file if it is created.
   * @param sync_hard True to do physical synchronization with the hardware for each output.
   * @return The result status.
   */
  Status Open(const std::string& prefix, int64_t max_file_size,
              double timestamp, bool sync_hard = false);

  /**
   * Closes the queue files.
   * @return The result status.
   */
  Status Close();

  /**
   * Writes a message.
   * @param timestamp The timestamp of the massage.
   * @param message The message data.
   * @return The result status.
   */
  Status Write(double timestamp, std::string_view message);

  /**
   * Makes a message reader.
   * @param min_timestamp The minimum timestamp of messages to read.
   * @return The message reader.
   */
  std::unique_ptr<Reader> MakeReader(double min_timestamp);

  /**
   * Finds files matching the given prefix and the date format suffix.
   * @param prefix The prefix for the file names.  The actual name of each file has the suffix
   * of the current local date in ".YYYYMMDDhhmmss" format, like ".20210211183005".  A unique
   * suffix can be appended too, like ".20210211183005-00001".
   * @param paths The pointer to a vector object to store the result paths.
   * @return The result status.  Even if there's no matching file, SUCCESS is returned.
   * @details The matched paths are sorted in ascending order of the file names.
   */
  static Status FindFiles(const std::string& prefix, std::vector<std::string>* paths);

 private:
  /** Pointer to the actual implementation. */
  MessageQueueImpl* impl_;
};

}  // namespace tkrzw

#endif  // _TKRZW_MESSAGE_QUEUE_H

// END OF FILE
