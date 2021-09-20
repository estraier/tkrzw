/*************************************************************************************************
 * Example of restoration with update logs
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

#include "tkrzw_dbm_tree.h"
#include "tkrzw_dbm_ulog.h"
#include "tkrzw_file_util.h"
#include "tkrzw_message_queue.h"

using namespace tkrzw;

int main(int argc, char** argv) {
  // Makes the initial content and the first full backup.
  {
    TreeDBM dbm;
    dbm.Open("casket.tkt", true, File::OPEN_TRUNCATE).OrDie();
    dbm.Set("one", "hop");
    dbm.CopyFileData("casket-backup.tkt").OrDie();
    dbm.Close().OrDie();
  }

  // Updates the database with the update logging enabled.
  {
    MessageQueue mq;
    mq.Open("casket-ulog", 512 << 20, MessageQueue::OPEN_TRUNCATE).OrDie();
    DBMUpdateLoggerMQ ulog(&mq);
    TreeDBM dbm;
    dbm.SetUpdateLogger(&ulog);
    dbm.Open("casket.tkt", true).OrDie();
    dbm.Set("two", "step");
    dbm.Close().OrDie();
    mq.Close().OrDie();
  }

  // The database is lost somehow.
  RemoveFile("casket.tkt").OrDie();

  // Applies the update log to the backup file.
  {
    CopyFileData("casket-backup.tkt", "casket-restored.tkt").OrDie();
    TreeDBM dbm;
    dbm.Open("casket-restored.tkt", true).OrDie();
    DBMUpdateLoggerMQ::ApplyUpdateLogFromFiles(&dbm, "casket-ulog");
    dbm.Close().OrDie();
  }

  // Checks the recovered content.
  {
    TreeDBM dbm;
    dbm.Open("casket-restored.tkt", false).OrDie();
    auto iter = dbm.MakeIterator();
    iter->First();
    std::string key, value;
    while (iter->Get(&key, &value).IsOK()) {
      std::cout << key << ":" << value << std::endl;
      iter->Next();
    }
    dbm.Close().OrDie();
  }

  return 0;
}

// END OF FILE
