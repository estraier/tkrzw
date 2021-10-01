/************************************************************************************************
 * Command line interface of update logging utilities
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

#include "tkrzw_cmd_util.h"

namespace tkrzw {

// Prints the usage to the standard error and die.
static void PrintUsageAndDie() {
  auto P = EPrintF;
  const char* progname = "tkrzw_ulog_util";
  P("%s: Update logging utilities of Tkrzw\n", progname);
  P("\n");
  P("Usage:\n");
  P("  %s writeset [options] prefix key value\n", progname);
  P("    : Writes a SET operation.\n");
  P("  %s writeremove [options] prefix key\n", progname);
  P("    : Writes a REMOVE operation.\n");
  P("  %s writeclear [options] prefix\n", progname);
  P("    : Writes a CLEAR operation.\n");
  P("  %s read [options] prefix\n", progname);
  P("    : Reads each log.\n");
  P("  %s listfiles [options] prefix\n", progname);
  P("    : Lists log files.\n");
  P("  %s removeoldfiles [options] prefix\n", progname);
  P("    : Removes old log files.\n");
  P("\n");
  P("Options for the write subcommands:\n");
  P("  --timestamp num : The timestamp to record. (default: current time)\n");
  P("  --max_file_size num : The maximum file size. (default: 1Gi)\n");
  P("  --server_id num : The server ID to record. (default: 0)\n");
  P("  --dbm_index num : The DBM index to record. (default: 0)\n");
  P("\n");
  P("Options for the read subcommand:\n");
  P("  --timestamp num : The timestamp to start at (default: 0)\n");
  P("  --server_id num : The server ID to focus on. (default: all)\n");
  P("  --dbm_index num : The DBM index to focus on. (default: all)\n");
  P("  --items num : The number of items to print. (default: 10)\n");
  P("  --escape : C-style escape is applied to the TSV data.\n");
  P("\n");
  P("Options for the listfiles subcommand:\n");
  P("  --timestamp num : The timestamp to start at (default: 0)\n");
  P("\n");
  P("Options for the removeoldfiles subcommand:\n");
  P("  --timestamp num : The timestamp of threshold. (default: -7D = 7 days ago)\n");
  P("  --exclude_latest : Avoid removing the latest file.\n");
  P("\n");
  std::exit(1);
}

// Processes the writeset subcommand.
static int32_t ProcessWriteSet(int32_t argc, const char** args) {
  const std::map<std::string, int32_t>& cmd_configs = {
    {"", 3}, {"--timestamp", 1}, {"--max_file_size", 1},
    {"--server_id", 1}, {"--dbm_index", 1},
  };
  std::map<std::string, std::vector<std::string>> cmd_args;
  std::string cmd_error;
  if (!ParseCommandArguments(argc, args, cmd_configs, &cmd_args, &cmd_error)) {
    EPrint("Invalid command: ", cmd_error, "\n\n");
    PrintUsageAndDie();
  }
  const std::string prefix = GetStringArgument(cmd_args, "", 0, "");
  const std::string key = GetStringArgument(cmd_args, "", 1, "");
  const std::string value = GetStringArgument(cmd_args, "", 2, "");
  const int64_t timestamp = MessageQueue::ParseTimestamp(
      GetStringArgument(cmd_args, "--timestamp", 0, "+0"), GetWallTime() * 1000);
  const int64_t max_file_size = GetIntegerArgument(cmd_args, "--max_file_size", 0, 1LL << 30);
  const int32_t server_id = GetIntegerArgument(cmd_args, "--server_id", 0, 0);
  const int32_t dbm_index = GetIntegerArgument(cmd_args, "--dbm_index", 0, 0);
  if (prefix.empty()) {
    Die("The prefix must be specified");
  }
  MessageQueue mq;
  Status status = mq.Open(prefix, max_file_size);
  if (status != Status::SUCCESS) {
    EPrintL("Open failed: ", status);
    return 1;
  }
  bool has_error = false;
  DBMUpdateLoggerMQ ulog(&mq, server_id, dbm_index, timestamp);
  status = ulog.WriteSet(key, value);
  if (status != Status::SUCCESS) {
    EPrintL("WriteSet failed: ", status);
    has_error = true;
  }
  status = mq.Close();
  if (status != Status::SUCCESS) {
    EPrintL("Close failed: ", status);
    has_error = true;
  }
  return has_error ? 1 : 0;
}

// Processes the writeremove subcommand.
static int32_t ProcessWriteRemove(int32_t argc, const char** args) {
  const std::map<std::string, int32_t>& cmd_configs = {
    {"", 2}, {"--timestamp", 1}, {"--max_file_size", 1},
    {"--server_id", 1}, {"--dbm_index", 1},
  };
  std::map<std::string, std::vector<std::string>> cmd_args;
  std::string cmd_error;
  if (!ParseCommandArguments(argc, args, cmd_configs, &cmd_args, &cmd_error)) {
    EPrint("Invalid command: ", cmd_error, "\n\n");
    PrintUsageAndDie();
  }
  const std::string prefix = GetStringArgument(cmd_args, "", 0, "");
  const std::string key = GetStringArgument(cmd_args, "", 1, "");
  const int64_t timestamp = MessageQueue::ParseTimestamp(
      GetStringArgument(cmd_args, "--timestamp", 0, "+0"), GetWallTime() * 1000);
  const int64_t max_file_size = GetIntegerArgument(cmd_args, "--max_file_size", 0, 1LL << 30);
  const int32_t server_id = GetIntegerArgument(cmd_args, "--server_id", 0, 0);
  const int32_t dbm_index = GetIntegerArgument(cmd_args, "--dbm_index", 0, 0);
  if (prefix.empty()) {
    Die("The prefix must be specified");
  }
  MessageQueue mq;
  Status status = mq.Open(prefix, max_file_size);
  if (status != Status::SUCCESS) {
    EPrintL("Open failed: ", status);
    return 1;
  }
  bool has_error = false;
  DBMUpdateLoggerMQ ulog(&mq, server_id, dbm_index, timestamp);
  status = ulog.WriteRemove(key);
  if (status != Status::SUCCESS) {
    EPrintL("WriteRemove failed: ", status);
    has_error = true;
  }
  status = mq.Close();
  if (status != Status::SUCCESS) {
    EPrintL("Close failed: ", status);
    has_error = true;
  }
  return has_error ? 1 : 0;
}

// Processes the writeclear subcommand.
static int32_t ProcessWriteClear(int32_t argc, const char** args) {
  const std::map<std::string, int32_t>& cmd_configs = {
    {"", 1}, {"--timestamp", 1}, {"--max_file_size", 1},
    {"--server_id", 1}, {"--dbm_index", 1},
  };
  std::map<std::string, std::vector<std::string>> cmd_args;
  std::string cmd_error;
  if (!ParseCommandArguments(argc, args, cmd_configs, &cmd_args, &cmd_error)) {
    EPrint("Invalid command: ", cmd_error, "\n\n");
    PrintUsageAndDie();
  }
  const std::string prefix = GetStringArgument(cmd_args, "", 0, "");
  const int64_t timestamp = MessageQueue::ParseTimestamp(
      GetStringArgument(cmd_args, "--timestamp", 0, "+0"), GetWallTime() * 1000);
  const int64_t max_file_size = GetIntegerArgument(cmd_args, "--max_file_size", 0, 1LL << 30);
  const int32_t server_id = GetIntegerArgument(cmd_args, "--server_id", 0, 0);
  const int32_t dbm_index = GetIntegerArgument(cmd_args, "--dbm_index", 0, 0);
  if (prefix.empty()) {
    Die("The prefix must be specified");
  }
  MessageQueue mq;
  Status status = mq.Open(prefix, max_file_size);
  if (status != Status::SUCCESS) {
    EPrintL("Open failed: ", status);
    return 1;
  }
  bool has_error = false;
  DBMUpdateLoggerMQ ulog(&mq, server_id, dbm_index, timestamp);
  status = ulog.WriteClear();
  if (status != Status::SUCCESS) {
    EPrintL("WriteClear failed: ", status);
    has_error = true;
  }
  status = mq.Close();
  if (status != Status::SUCCESS) {
    EPrintL("Close failed: ", status);
    has_error = true;
  }
  return has_error ? 1 : 0;
}

// Processes the read subcommand.
static int32_t ProcessRead(int32_t argc, const char** args) {
  const std::map<std::string, int32_t>& cmd_configs = {
    {"", 1}, {"--timestamp", 1}, {"--server_id", 1}, {"--dbm_index", 1},
    {"--items", 1}, {"--escape", 0},
  };
  std::map<std::string, std::vector<std::string>> cmd_args;
  std::string cmd_error;
  if (!ParseCommandArguments(argc, args, cmd_configs, &cmd_args, &cmd_error)) {
    EPrint("Invalid command: ", cmd_error, "\n\n");
    PrintUsageAndDie();
  }
  const std::string prefix = GetStringArgument(cmd_args, "", 0, "");
  const int64_t min_timestamp = MessageQueue::ParseTimestamp(
      GetStringArgument(cmd_args, "--timestamp", 0, "0"), GetWallTime() * 1000);
  const int32_t server_id = GetIntegerArgument(cmd_args, "--server_id", 0, -1);
  const int32_t dbm_index = GetIntegerArgument(cmd_args, "--dbm_index", 0, -1);
  const int64_t num_items = GetIntegerArgument(cmd_args, "--items", 0, 10);
  const bool with_escape = CheckMap(cmd_args, "--escape");
  if (prefix.empty()) {
    Die("The prefix must be specified");
  }
  MessageQueue mq;
  Status status = mq.Open(prefix, 0, MessageQueue::OPEN_READ_ONLY);
  if (status != Status::SUCCESS) {
    EPrintL("Open failed: ", status);
    return 1;
  }
  bool has_error = false;
  auto reader = mq.MakeReader(min_timestamp);
  int64_t timestamp = 0;
  std::string message;
  int64_t count = 0;
  while (count < num_items) {
    status = reader->Read(&timestamp, &message);
    if (status != Status::SUCCESS) {
      if (status != Status::NOT_FOUND_ERROR) {
        EPrintL("Read failed: ", status);
        has_error = true;
      }
      break;
    }
    DBMUpdateLoggerMQ::UpdateLog op;
    status = DBMUpdateLoggerMQ::ParseUpdateLog(message, &op);
    if (status != Status::SUCCESS) {
      EPrintL("ParseUpdateLog failed: ", status);
      break;
    }
    if (server_id >= 0 && op.server_id != server_id) {
      continue;
    }
    if (dbm_index >= 0 && op.dbm_index != dbm_index) {
      continue;
    }
    const std::string& esc_key = with_escape ? StrEscapeC(op.key) : StrTrimForTSV(op.key);
    const std::string& esc_value =
        with_escape ? StrEscapeC(op.value) : StrTrimForTSV(op.value, true);
    switch (op.op_type) {
      case DBMUpdateLoggerMQ::OP_SET:
        PrintL(timestamp, "\t", op.server_id, "\t", op.dbm_index,
               "\tSET\t", esc_key, "\t", esc_value);
        break;
      case DBMUpdateLoggerMQ::OP_REMOVE:
        PrintL(timestamp, "\t", op.server_id, "\t", op.dbm_index, "\tREMOVE\t", esc_key);
        break;
      case DBMUpdateLoggerMQ::OP_CLEAR:
        PrintL(timestamp, "\t", op.server_id, "\t", op.dbm_index, "\tCLEAR");
        break;
      default:
        break;
    }
    count++;
  }
  status = mq.Close();
  if (status != Status::SUCCESS) {
    EPrintL("Close failed: ", status);
    has_error = true;
  }
  return has_error ? 1 : 0;
}

// Processes the listfiles subcommand.
static int32_t ProcessListFiles(int32_t argc, const char** args) {
  const std::map<std::string, int32_t>& cmd_configs = {
    {"", 1}, {"--timestamp", 1},
  };
  std::map<std::string, std::vector<std::string>> cmd_args;
  std::string cmd_error;
  if (!ParseCommandArguments(argc, args, cmd_configs, &cmd_args, &cmd_error)) {
    EPrint("Invalid command: ", cmd_error, "\n\n");
    PrintUsageAndDie();
  }
  const std::string prefix = GetStringArgument(cmd_args, "", 0, "");
  const int64_t min_timestamp = MessageQueue::ParseTimestamp(
      GetStringArgument(cmd_args, "--timestamp", 0, "0"), GetWallTime() * 1000);
  if (prefix.empty()) {
    Die("The prefix must be specified");
  }
  std::vector<std::string> paths;
  Status status = MessageQueue::FindFiles(prefix, &paths);
  if (status != Status::SUCCESS) {
    EPrintL("FindFiles failed: ", status);
    return 1;
  }
  bool has_error = false;
  const double current_ts = GetWallTime();
  for (const auto& path : paths) {
    int64_t file_id = 0;
    int64_t timestamp = 0;
    int64_t file_size = 0;
    status = MessageQueue::ReadFileMetadata(path, &file_id, &timestamp, &file_size);
    if (status != Status::SUCCESS) {
      EPrintL("ReadFileMetadata failed: ", status);
      has_error = true;
    }
    if (timestamp < min_timestamp) {
      continue;
    }
    const double ts_sec = timestamp / 1000;
    const std::string ts_expr = MakeRelativeTimeExpr(current_ts - ts_sec);
    PrintL(path, "\t", file_id, "\t", timestamp, "\t", ts_expr, "\t", file_size);
  }
  return has_error ? 1 : 0;
}

// Processes the removeoldfiles subcommand.
static int32_t ProcessRemoveOldFiles(int32_t argc, const char** args) {
  const std::map<std::string, int32_t>& cmd_configs = {
    {"", 1}, {"--timestamp", 1}, {"--exclude_latest", 0},
  };
  std::map<std::string, std::vector<std::string>> cmd_args;
  std::string cmd_error;
  if (!ParseCommandArguments(argc, args, cmd_configs, &cmd_args, &cmd_error)) {
    EPrint("Invalid command: ", cmd_error, "\n\n");
    PrintUsageAndDie();
  }
  const std::string prefix = GetStringArgument(cmd_args, "", 0, "");
  const int64_t min_timestamp = MessageQueue::ParseTimestamp(
      GetStringArgument(cmd_args, "--timestamp", 0, "-7D"), GetWallTime() * 1000);
  const bool exclude_latest = CheckMap(cmd_args, "--exclude_latest");
  if (prefix.empty()) {
    Die("The prefix must be specified");
  }
  const Status status = MessageQueue::RemoveOldFiles(prefix, min_timestamp, exclude_latest);
  if (status != Status::SUCCESS) {
    EPrintL("RemoveOldFiles failed: ", status);
    return 1;
  }
  return 0;
}

}  // namespace tkrzw

// Main routine
int main(int argc, char** argv) {
  const char** args = const_cast<const char**>(argv);
  if (argc < 2) {
    tkrzw::PrintUsageAndDie();
  }
  int32_t rv = 0;
  try {
    if (std::strcmp(args[1], "writeset") == 0) {
      rv = tkrzw::ProcessWriteSet(argc - 1, args + 1);
    } else if (std::strcmp(args[1], "writeremove") == 0) {
      rv = tkrzw::ProcessWriteRemove(argc - 1, args + 1);
    } else if (std::strcmp(args[1], "writeclear") == 0) {
      rv = tkrzw::ProcessWriteClear(argc - 1, args + 1);
    } else if (std::strcmp(args[1], "read") == 0) {
      rv = tkrzw::ProcessRead(argc - 1, args + 1);
    } else if (std::strcmp(args[1], "listfiles") == 0) {
      rv = tkrzw::ProcessListFiles(argc - 1, args + 1);
    } else if (std::strcmp(args[1], "removeoldfiles") == 0) {
      rv = tkrzw::ProcessRemoveOldFiles(argc - 1, args + 1);
    } else {
      tkrzw::PrintUsageAndDie();
    }
  } catch (const std::runtime_error& e) {
    tkrzw::EPrintL(e.what());
    rv = 1;
  }
  return rv;
}

// END OF FILE
