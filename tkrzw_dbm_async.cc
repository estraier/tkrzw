/*************************************************************************************************
 * Asynchronous database manager adapter
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

#include "tkrzw_sys_config.h"

#include "tkrzw_dbm.h"
#include "tkrzw_dbm_async.h"
#include "tkrzw_str_util.h"
#include "tkrzw_thread_util.h"

namespace tkrzw {

AsyncDBM::AsyncDBM(DBM* dbm, int32_t num_worker_threads) : dbm_(dbm), queue_() {
  assert(dbm != nullptr && num_worker_threads > 0);
  queue_.Start(num_worker_threads);
}

AsyncDBM::~AsyncDBM() {
  queue_.Stop(INT32MAX);
}

std::future<std::pair<Status, std::string>> AsyncDBM::Get(std::string_view key) {
  struct GetTask : public TaskQueue::Task {
    DBM* dbm;
    std::string key;
    std::promise<std::pair<Status, std::string>> promise;
    void Do() override {
      std::string value;
      Status status = dbm->Get(key, &value);
      promise.set_value(std::make_pair(std::move(status), std::move(value)));
    }
  };
  auto task = std::make_unique<GetTask>();
  task->dbm = dbm_;
  task->key = key;
  auto future = task->promise.get_future();
  queue_.Add(std::move(task));
  return future;
}

std::future<std::map<std::string, std::string>> AsyncDBM::GetMulti(
    const std::vector<std::string_view>& keys) {
  struct GetMultiTask : public TaskQueue::Task {
    DBM* dbm;
    std::vector<std::string> keys;
    std::vector<std::string_view> key_views;
    std::promise<std::map<std::string, std::string>> promise;
    void Do() override {
      auto records = dbm->GetMulti(key_views);
      promise.set_value(std::move(records));
    }
  };
  auto task = std::make_unique<GetMultiTask>();
  task->dbm = dbm_;
  task->keys.reserve(keys.size());
  task->key_views.reserve(keys.size());
  for (const auto& key : keys) {
    task->keys.emplace_back(key);
    task->key_views.emplace_back(task->keys.back());
  }
  auto future = task->promise.get_future();
  queue_.Add(std::move(task));
  return future;
}

std::future<Status> AsyncDBM::Set(std::string_view key, std::string_view value, bool overwrite) {
  struct SetTask : public TaskQueue::Task {
    DBM* dbm;
    std::string key;
    std::string value;
    bool overwrite;
    std::promise<Status> promise;
    void Do() override {
      Status status = dbm->Set(key, value, overwrite);
      promise.set_value(std::move(status));
    }
  };
  auto task = std::make_unique<SetTask>();
  task->dbm = dbm_;
  task->key = key;
  task->value = value;
  task->overwrite = overwrite;
  auto future = task->promise.get_future();
  queue_.Add(std::move(task));
  return future;
}

std::future<Status> AsyncDBM::SetMulti(
    const std::map<std::string_view, std::string_view>& records, bool overwrite) {
  struct SetMultiTask : public TaskQueue::Task {
    DBM* dbm;
    std::map<std::string, std::string> records;
    std::map<std::string_view, std::string_view> record_views;
    bool overwrite;
    std::promise<Status> promise;
    void Do() override {
      Status status = dbm->SetMulti(record_views, overwrite);
      promise.set_value(std::move(status));
    }
  };
  auto task = std::make_unique<SetMultiTask>();
  task->dbm = dbm_;
  for (const auto& record : records) {
    task->records.emplace(std::make_pair(record.first, record.second));
  }
  for (const auto& record : task->records) {
    task->record_views.emplace(std::make_pair(
        std::string_view(record.first), std::string_view(record.second)));
  }
  task->overwrite = overwrite;
  auto future = task->promise.get_future();
  queue_.Add(std::move(task));
  return future;
}

std::future<Status> AsyncDBM::Append(
    std::string_view key, std::string_view value, std::string_view delim) {
  struct AppendTask : public TaskQueue::Task {
    DBM* dbm;
    std::string key;
    std::string value;
    std::string delim;
    std::promise<Status> promise;
    void Do() override {
      Status status = dbm->Append(key, value, delim);
      promise.set_value(std::move(status));
    }
  };
  auto task = std::make_unique<AppendTask>();
  task->dbm = dbm_;
  task->key = key;
  task->value = value;
  task->delim = delim;
  auto future = task->promise.get_future();
  queue_.Add(std::move(task));
  return future;
}

std::future<Status> AsyncDBM::Remove(std::string_view key) {
  struct RemoveTask : public TaskQueue::Task {
    DBM* dbm;
    std::string key;
    std::promise<Status> promise;
    void Do() override {
      Status status = dbm->Remove(key);
      promise.set_value(std::move(status));
    }
  };
  auto task = std::make_unique<RemoveTask>();
  task->dbm = dbm_;
  task->key = key;
  auto future = task->promise.get_future();
  queue_.Add(std::move(task));
  return future;
}

std::future<Status> AsyncDBM::RemoveMulti(const std::vector<std::string_view>& keys) {
  struct RemoveMultiTask : public TaskQueue::Task {
    DBM* dbm;
    std::vector<std::string> keys;
    std::vector<std::string_view> key_views;
    std::promise<Status> promise;
    void Do() override {
      Status status = dbm->RemoveMulti(key_views);
      promise.set_value(std::move(status));
    }
  };
  auto task = std::make_unique<RemoveMultiTask>();
  task->dbm = dbm_;
  task->keys.reserve(keys.size());
  task->key_views.reserve(keys.size());
  for (const auto& key : keys) {
    task->keys.emplace_back(key);
    task->key_views.emplace_back(task->keys.back());
  }
  auto future = task->promise.get_future();
  queue_.Add(std::move(task));
  return future;
}

std::future<Status> AsyncDBM::AppendMulti(
    const std::map<std::string_view, std::string_view>& records, std::string_view delim) {
  struct AppendMultiTask : public TaskQueue::Task {
    DBM* dbm;
    std::map<std::string, std::string> records;
    std::map<std::string_view, std::string_view> record_views;
    std::string delim;
    std::promise<Status> promise;
    void Do() override {
      Status status = dbm->AppendMulti(record_views, delim);
      promise.set_value(std::move(status));
    }
  };
  auto task = std::make_unique<AppendMultiTask>();
  task->dbm = dbm_;
  for (const auto& record : records) {
    task->records.emplace(std::make_pair(record.first, record.second));
  }
  for (const auto& record : task->records) {
    task->record_views.emplace(std::make_pair(
        std::string_view(record.first), std::string_view(record.second)));
  }
  task->delim = delim;
  auto future = task->promise.get_future();
  queue_.Add(std::move(task));
  return future;
}

std::future<Status> AsyncDBM::CompareExchange(std::string_view key, std::string_view expected,
                                              std::string_view desired) {
  struct CompareExchangeTask : public TaskQueue::Task {
    DBM* dbm;
    std::string key;
    std::string expected;
    std::string_view expected_view;
    std::string desired;
    std::string_view desired_view;
    std::promise<Status> promise;
    void Do() override {
      Status status = dbm->CompareExchange(key, expected_view, desired_view);
      promise.set_value(std::move(status));
    }
  };
  auto task = std::make_unique<CompareExchangeTask>();
  task->dbm = dbm_;
  task->key = key;
  if (expected.data() != nullptr) {
    task->expected = expected;
    task->expected_view = task->expected;
  }
  if (desired.data() != nullptr) {
    task->desired = desired;
    task->desired_view = task->desired;
  }
  auto future = task->promise.get_future();
  queue_.Add(std::move(task));
  return future;
}

std::future<Status> AsyncDBM::CompareExchangeMulti(
    const std::vector<std::pair<std::string_view, std::string_view>>& expected,
    const std::vector<std::pair<std::string_view, std::string_view>>& desired) {
  struct CompareExchangeMultiTask : public TaskQueue::Task {
    DBM* dbm;
    std::vector<std::string> placeholders;
    std::vector<std::pair<std::string_view, std::string_view>> expected;
    std::vector<std::pair<std::string_view, std::string_view>> desired;
    std::promise<Status> promise;
    void Do() override {
      Status status = dbm->CompareExchangeMulti(expected, desired);
      promise.set_value(std::move(status));
    }
  };
  auto task = std::make_unique<CompareExchangeMultiTask>();
  task->dbm = dbm_;
  task->placeholders.reserve(expected.size() * 2 + desired.size() * 2);
  for (const auto& record : expected) {
    task->placeholders.emplace_back(record.first);
    const std::string_view key = task->placeholders.back();
    if (record.second.data() == nullptr) {
      task->expected.emplace_back(std::make_pair(key, std::string_view()));
    } else {
      task->placeholders.emplace_back(record.second);
      task->expected.emplace_back(std::make_pair(
          key, std::string_view(task->placeholders.back())));
    }
  }
  for (const auto& record : desired) {
    task->placeholders.emplace_back(record.first);
    const std::string_view key = task->placeholders.back();
    if (record.second.data() == nullptr) {
      task->desired.emplace_back(std::make_pair(key, std::string_view()));
    } else {
      task->placeholders.emplace_back(record.second);
      task->desired.emplace_back(std::make_pair(
          key, std::string_view(task->placeholders.back())));
    }
  }
  auto future = task->promise.get_future();
  queue_.Add(std::move(task));
  return future;
}

std::future<std::pair<Status, int64_t>> AsyncDBM::Increment(
    std::string_view key, int64_t increment, int64_t initial) {
  struct IncrementTask : public TaskQueue::Task {
    DBM* dbm;
    std::string key;
    int64_t increment;
    int64_t initial;
    std::promise<std::pair<Status, int64_t>> promise;
    void Do() override {
      int64_t current = 0;
      Status status = dbm->Increment(key, increment, &current, initial);
      promise.set_value(std::make_pair(std::move(status), current));
    }
  };
  auto task = std::make_unique<IncrementTask>();
  task->dbm = dbm_;
  task->key = key;
  task->increment = increment;
  task->initial = initial;
  auto future = task->promise.get_future();
  queue_.Add(std::move(task));
  return future;
}

std::future<Status> AsyncDBM::Clear() {
  struct ClearTask : public TaskQueue::Task {
    DBM* dbm;
    std::promise<Status> promise;
    void Do() override {
      Status status = dbm->Clear();
      promise.set_value(std::move(status));
    }
  };
  auto task = std::make_unique<ClearTask>();
  task->dbm = dbm_;
  auto future = task->promise.get_future();
  queue_.Add(std::move(task));
  return future;
}

std::future<Status> AsyncDBM::Rebuild() {
  struct RebuildTask : public TaskQueue::Task {
    DBM* dbm;
    std::promise<Status> promise;
    void Do() override {
      Status status = dbm->Rebuild();
      promise.set_value(std::move(status));
    }
  };
  auto task = std::make_unique<RebuildTask>();
  task->dbm = dbm_;
  auto future = task->promise.get_future();
  queue_.Add(std::move(task));
  return future;
}

std::future<Status> AsyncDBM::Synchronize(bool hard) {
  struct SynchronizeTask : public TaskQueue::Task {
    DBM* dbm;
    bool hard;
    std::promise<Status> promise;
    void Do() override {
      Status status = dbm->Synchronize(hard);
      promise.set_value(std::move(status));
    }
  };
  auto task = std::make_unique<SynchronizeTask>();
  task->dbm = dbm_;
  task->hard = hard;
  auto future = task->promise.get_future();
  queue_.Add(std::move(task));
  return future;
}




}  // namespace tkrzw

// END OF FILE
