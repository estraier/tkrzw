/*************************************************************************************************
 * On-memory database manager implementations based on B+ tree
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

#include "tkrzw_containers.h"
#include "tkrzw_dbm.h"
#include "tkrzw_dbm_baby.h"
#include "tkrzw_dbm_common_impl.h"
#include "tkrzw_file.h"
#include "tkrzw_file_mmap.h"
#include "tkrzw_file_pos.h"
#include "tkrzw_file_std.h"
#include "tkrzw_file_util.h"
#include "tkrzw_lib_common.h"
#include "tkrzw_key_comparators.h"
#include "tkrzw_str_util.h"
#include "tkrzw_thread_util.h"

namespace tkrzw {

static constexpr int32_t MAX_LEAF_NODE_RECORDS = 256;
static constexpr int32_t MAX_INNER_NODE_BRANCHES = 256;
static constexpr int32_t TREE_LEVEL_MAX = 20;
static constexpr int32_t ITER_BUFFER_SIZE = 128;

struct BabyRecord final {
  int32_t key_size;
  int32_t value_size;
  std::string_view GetKey() const;
  std::string_view GetValue() const;
};

BabyRecord* CreateBabyRecord(std::string_view key, std::string_view value);
BabyRecord* ModifyBabyRecord(BabyRecord* record, std::string_view new_value);
BabyRecord* AppendBabyRecord(
    BabyRecord* record, std::string_view cat_value, std::string_view cat_delim);
void FreeBabyRecord(BabyRecord* record);
void FreeBabyRecords(std::vector<BabyRecord*>* records);

struct BabyRecordOnStack final {
  static constexpr int32_t STACK_BUFFER_SIZE = 256;
  BabyRecord* record;
  char stack[STACK_BUFFER_SIZE];
  char* buffer;
  explicit BabyRecordOnStack(std::string_view key);
  ~BabyRecordOnStack();
};

struct BabyRecordComparator final {
  KeyComparator comp;
  explicit BabyRecordComparator(KeyComparator comp) : comp(comp) {}
  bool operator ()(const BabyRecord* const& a, const BabyRecord* const& b) const {
    return comp(a->GetKey(), b->GetKey()) < 0;
  }
};

struct BabyLink final {
  int32_t key_size;
  std::string_view GetKey() const;
  void* GetChild() const;
  void SetChild(void*);
};

BabyLink* CreateBabyLink(std::string_view key, const void* child);
void FreeBabyLink(BabyLink* link);
void FreeBabyLinks(std::vector<BabyLink*>* links);

struct BabyLinkOnStack final {
  static constexpr int32_t STACK_BUFFER_SIZE = 256;
  BabyLink* link;
  char stack[STACK_BUFFER_SIZE];
  char* buffer;
  explicit BabyLinkOnStack(std::string_view key);
  ~BabyLinkOnStack();
};

struct BabyLinkComparator final {
  KeyComparator comp;
  explicit BabyLinkComparator(KeyComparator comp) : comp(comp) {}
  bool operator ()(const BabyLink* const& a, const BabyLink* const& b) const {
    return comp(a->GetKey(), b->GetKey()) < 0;
  }
};

struct BabyLeafNode final {
  BabyLeafNode* prev;
  BabyLeafNode* next;
  std::vector<BabyRecord*> records;
  SpinSharedMutex mutex;
  BabyLeafNode(BabyLeafNode* prev, BabyLeafNode* next)
      : prev(prev), next(next), records(), mutex() {}
  ~BabyLeafNode() {
    FreeBabyRecords(&records);
  }
};

struct BabyInnerNode final {
  void* heir;
  std::vector<BabyLink*> links;
  BabyInnerNode(void* heir) : heir(heir), links() {}
  ~BabyInnerNode() {
    FreeBabyLinks(&links);
  }
};

class BabyDBMImpl final {
  friend class BabyDBMIteratorImpl;
  typedef std::list<BabyDBMIteratorImpl*> IteratorList;
 public:
  BabyDBMImpl(std::unique_ptr<File> file, KeyComparator key_comparator);
  ~BabyDBMImpl();
  Status Open(const std::string& path, bool writable, int32_t options);
  Status Close();
  Status Process(std::string_view key, DBM::RecordProcessor* proc, bool writable);
  Status Append(std::string_view key, std::string_view value, std::string_view delim);
  Status ProcessMulti(
      const std::vector<std::pair<std::string_view, DBM::RecordProcessor*>>& key_proc_pairs,
      bool writable);
  Status ProcessFirst(DBM::RecordProcessor* proc, bool writable);
  Status ProcessEach(DBM::RecordProcessor* proc, bool writable);
  Status Count(int64_t* count);
  Status GetFileSize(int64_t* size);
  Status GetFilePath(std::string* path);
  Status GetTimestamp(double* timestamp);
  Status Clear();
  Status Rebuild(int64_t num_buckets);
  Status ShouldBeRebuilt(bool* tobe);
  Status Synchronize(bool hard, DBM::FileProcessor* proc);
  std::vector<std::pair<std::string, std::string>> Inspect();
  bool IsOpen();
  bool IsWritable();
  std::unique_ptr<DBM> MakeDBM();
  DBM::UpdateLogger* GetUpdateLogger();
  void SetUpdateLogger(DBM::UpdateLogger* update_logger);
  File* GetInternalFile() const;
  KeyComparator GetKeyComparator();

 private:
  void InitializeNodes();
  void FreeNodes();
  BabyLeafNode* SearchTree(std::string_view key);
  void TraceTree(std::string_view key, BabyInnerNode** hist, int32_t* hist_size);
  void ReorganizeTree();
  bool CheckLeafNodeToDivide(BabyLeafNode* node);
  bool CheckLeafNodeToMerge(BabyLeafNode* node);
  void DivideNodes(BabyLeafNode* leaf_node, const std::string& node_key);
  void MergeNodes(BabyLeafNode* leaf_node, const std::string& node_key);
  void AddLinkToInnerNode(BabyInnerNode* node, void* child, std::string_view key);
  void JoinPrevLinkInInnerNode(BabyInnerNode* node, void* child);
  void JoinNextLinkInInnerNode(BabyInnerNode* node, void* child, void* next);
  Status ImportRecords();
  Status ExportRecords();
  void ProcessImpl(
      BabyLeafNode* node, std::string_view key, DBM::RecordProcessor* proc, bool writable);
  void AppendImpl(
      BabyLeafNode* node, std::string_view key, std::string_view value, std::string_view delim);

  IteratorList iterators_;
  std::unique_ptr<File> file_;
  bool open_;
  bool writable_;
  int32_t open_options_;
  std::string path_;
  double timestamp_;
  KeyComparator key_comparator_;
  BabyRecordComparator record_comp_;
  BabyLinkComparator link_comp_;
  std::atomic_int64_t num_records_;
  int32_t tree_level_;
  void* root_node_;
  BabyLeafNode* first_node_;
  BabyLeafNode* last_node_;
  AtomicSet<std::pair<BabyLeafNode*, std::string>> reorg_nodes_;
  DBM::UpdateLogger* update_logger_;
  SpinSharedMutex mutex_;
};

class BabyDBMIteratorImpl final {
  friend class BabyDBMImpl;
 public:
  explicit BabyDBMIteratorImpl(BabyDBMImpl* dbm);
  ~BabyDBMIteratorImpl();
  Status First();
  Status Last();
  Status Jump(std::string_view key);
  Status JumpLower(std::string_view key, bool inclusive);
  Status JumpUpper(std::string_view key, bool inclusive);
  Status Next();
  Status Previous();
  Status Process(DBM::RecordProcessor* proc, bool writable);

 private:
  void ClearPosition();
  bool SetPositionFirst(BabyLeafNode* leaf_node);
  bool SetPositionLast(BabyLeafNode* leaf_node);
  void SetPositionWithKey(BabyLeafNode* leaf_node, std::string_view key);
  Status NextImpl(std::string_view key);
  Status PreviousImpl(std::string_view key);
  Status SyncPosition(std::string_view key);
  Status ProcessImpl(std::string_view key, DBM::RecordProcessor* proc, bool writable);

  BabyDBMImpl* dbm_;
  char stack_[ITER_BUFFER_SIZE];
  char* key_ptr_;
  int32_t key_size_;
  BabyLeafNode* leaf_node_;
};

std::string_view BabyRecord::GetKey() const {
  const char* rp = reinterpret_cast<const char*>(this) + sizeof(*this);
  return std::string_view(rp, key_size);
}

std::string_view BabyRecord::GetValue() const {
  const char* rp = reinterpret_cast<const char*>(this) + sizeof(*this) + key_size;
  return std::string_view(rp, value_size);
}

BabyRecord* CreateBabyRecord(std::string_view key, std::string_view value) {
  BabyRecord* rec =
      static_cast<BabyRecord*>(xmalloc(sizeof(BabyRecord) + key.size() + value.size()));
  rec->key_size = key.size();
  rec->value_size = value.size();
  char* wp = reinterpret_cast<char*>(rec) + sizeof(*rec);
  std::memcpy(wp, key.data(), key.size());
  std::memcpy(wp + key.size(), value.data(), value.size());
  return rec;
}

BabyRecord* ModifyBabyRecord(BabyRecord* record, std::string_view new_value) {
  if (static_cast<int32_t>(new_value.size()) > record->value_size) {
    record = static_cast<BabyRecord*>(xrealloc(
        record, sizeof(BabyRecord) + record->key_size + new_value.size()));
  }
  char* wp = reinterpret_cast<char*>(record) + sizeof(*record) + record->key_size;
  std::memcpy(wp, new_value.data(), new_value.size());
  record->value_size = new_value.size();
  return record;
}

BabyRecord* AppendBabyRecord(
    BabyRecord* record, std::string_view cat_value, std::string_view cat_delim) {
  int32_t new_value_size = record->value_size + cat_delim.size() + cat_value.size();
  record = static_cast<BabyRecord*>(xreallocappend(
      record, sizeof(BabyRecord) + record->key_size + new_value_size));
  char* wp = reinterpret_cast<char*>(record) + sizeof(*record) +
      record->key_size + record->value_size;
  std::memcpy(wp, cat_delim.data(), cat_delim.size());
  wp += cat_delim.size();
  std::memcpy(wp, cat_value.data(), cat_value.size());
  record->value_size = new_value_size;
  return record;
}

void FreeBabyRecord(BabyRecord* record) {
  xfree(record);
}

void FreeBabyRecords(std::vector<BabyRecord*>* records) {
  for (auto* rec : *records) {
    xfree(rec);
  }
}

BabyRecordOnStack::BabyRecordOnStack(std::string_view key) {
  const int32_t size = sizeof(BabyRecord) + key.size();
  buffer = size <= STACK_BUFFER_SIZE ? stack : new char[size];
  record = reinterpret_cast<BabyRecord*>(buffer);
  record->key_size = key.size();
  char* wp = reinterpret_cast<char*>(record) + sizeof(*record);
  std::memcpy(wp, key.data(), key.size());
}

BabyRecordOnStack::~BabyRecordOnStack() {
  if (buffer != stack) {
    delete[] buffer;
  }
}

std::string_view BabyLink::GetKey() const {
  const char* rp = reinterpret_cast<const char*>(this) + sizeof(*this);
  return std::string_view(rp, key_size);
}

void* BabyLink::GetChild() const {
  const char* rp = reinterpret_cast<const char*>(this) + sizeof(*this);
  rp += key_size;
  void* child;
  std::memcpy(&child, rp, sizeof(child));
  return child;
}

void BabyLink::SetChild(void* child) {
  char* wp = reinterpret_cast<char*>(this) + sizeof(*this);
  wp += key_size;
  std::memcpy(wp, &child, sizeof(child));
}

BabyLink* CreateBabyLink(std::string_view key, const void* child) {
  BabyLink* link = static_cast<BabyLink*>(xmalloc(sizeof(BabyLink) + key.size() + sizeof(child)));
  link->key_size = key.size();
  char* wp = reinterpret_cast<char*>(link) + sizeof(*link);
  std::memcpy(wp, key.data(), key.size());
  wp += key.size();
  std::memcpy(wp, &child, sizeof(child));
  return link;
}

void FreeBabyLink(BabyLink* link) {
  xfree(link);
}

void FreeBabyLinks(std::vector<BabyLink*>* links) {
  for (auto* link : *links) {
    xfree(link);
  }
}

BabyLinkOnStack::BabyLinkOnStack(std::string_view key) {
  const int32_t size = sizeof(BabyLink) + key.size();
  buffer = size <= STACK_BUFFER_SIZE ? stack : new char[size];
  link = reinterpret_cast<BabyLink*>(buffer);
  link->key_size = key.size();
  char* wp = reinterpret_cast<char*>(link) + sizeof(*link);
  std::memcpy(wp, key.data(), key.size());
}

BabyLinkOnStack::~BabyLinkOnStack() {
  if (buffer != stack) {
    delete[] buffer;
  }
}

BabyDBMImpl::BabyDBMImpl(std::unique_ptr<File> file, KeyComparator key_comparator)
    : iterators_(), file_(std::move(file)),
      open_(false), writable_(false), open_options_(0), path_(), timestamp_(0),
      key_comparator_(key_comparator),
      record_comp_(BabyRecordComparator(key_comparator)),
      link_comp_(BabyLinkComparator(key_comparator)),
      num_records_(0), tree_level_(0),
      root_node_(nullptr), first_node_(nullptr), last_node_(nullptr),
      reorg_nodes_(), update_logger_(nullptr),
      mutex_() {
  InitializeNodes();
  num_records_.store(0);
}

BabyDBMImpl::~BabyDBMImpl() {
  if (open_) {
    Close();
  }
  for (auto* iterator : iterators_) {
    iterator->dbm_ = nullptr;
  }
  FreeNodes();
}

Status BabyDBMImpl::Open(const std::string& path, bool writable, int32_t options) {
  std::lock_guard<SpinSharedMutex> lock(mutex_);
  if (open_) {
    return Status(Status::PRECONDITION_ERROR, "opened database");
  }
  const std::string norm_path = NormalizePath(path);
  Status status = file_->Open(norm_path, writable, options);
  if (status != Status::SUCCESS) {
    return status;
  }
  if (file_->GetSizeSimple() < 1) {
    timestamp_ = GetWallTime();
  }
  status = ImportRecords();
  if (status != Status::SUCCESS) {
    file_->Close();
    return status;
  }
  open_ = true;
  writable_ = writable;
  open_options_ = options;
  path_ = norm_path;
  return Status(Status::SUCCESS);
}

Status BabyDBMImpl::Close() {
  std::lock_guard<SpinSharedMutex> lock(mutex_);
  if (!open_) {
    return Status(Status::PRECONDITION_ERROR, "not opened database");
  }
  for (auto* iterator : iterators_) {
    iterator->ClearPosition();
  }
  Status status(Status::SUCCESS);
  if (writable_) {
    status |= ExportRecords();
  }
  status |= file_->Close();
  FreeNodes();
  InitializeNodes();
  num_records_.store(0);
  open_ = false;
  writable_ = false;
  open_options_ = 0;
  path_.clear();
  timestamp_ = 0;
  return status;
}

Status BabyDBMImpl::Process(std::string_view key, DBM::RecordProcessor* proc, bool writable) {
  if (writable && !reorg_nodes_.IsEmpty()) {
    std::lock_guard<SpinSharedMutex> lock(mutex_);
    ReorganizeTree();
  }
  std::shared_lock<SpinSharedMutex> lock(mutex_);
  BabyLeafNode* leaf_node = SearchTree(key);
  if (writable) {
    std::lock_guard<SpinSharedMutex> page_lock(leaf_node->mutex);
    ProcessImpl(leaf_node, key, proc, true);
  } else {
    std::shared_lock<SpinSharedMutex> page_lock(leaf_node->mutex);
    ProcessImpl(leaf_node, key, proc, false);
  }
  return Status(Status::SUCCESS);
}

Status BabyDBMImpl::Append(std::string_view key, std::string_view value, std::string_view delim) {
  if (!reorg_nodes_.IsEmpty()) {
    std::lock_guard<SpinSharedMutex> lock(mutex_);
    ReorganizeTree();
  }
  std::shared_lock<SpinSharedMutex> lock(mutex_);
  BabyLeafNode* leaf_node = SearchTree(key);
  std::lock_guard<SpinSharedMutex> page_lock(leaf_node->mutex);
  AppendImpl(leaf_node, key, value, delim);
  return Status(Status::SUCCESS);
}

Status BabyDBMImpl::ProcessMulti(
    const std::vector<std::pair<std::string_view, DBM::RecordProcessor*>>& key_proc_pairs,
    bool writable) {
  if (!reorg_nodes_.IsEmpty()) {
    std::lock_guard<SpinSharedMutex> lock(mutex_);
    ReorganizeTree();
  }
  std::shared_lock<SpinSharedMutex> lock(mutex_);
  std::vector<BabyLeafNode*> leaf_nodes;
  std::set<BabyLeafNode*> uniq_leaf_nodes;
  for (const auto& key_proc : key_proc_pairs) {
    BabyLeafNode* leaf_node = SearchTree(key_proc.first);
    leaf_nodes.emplace_back(leaf_node);
    uniq_leaf_nodes.emplace(leaf_node);
  }
  for (const auto& leaf_node : uniq_leaf_nodes) {
    if (writable) {
      leaf_node->mutex.lock();
    } else {
      leaf_node->mutex.lock_shared();
    }
  }
  for (size_t i = 0; i < key_proc_pairs.size(); i++) {
    auto& key_proc = key_proc_pairs[i];
    auto& leaf_node = leaf_nodes[i];
    ProcessImpl(leaf_node, key_proc.first, key_proc.second, writable);
  }
  for (auto leaf_node = uniq_leaf_nodes.rbegin();
       leaf_node != uniq_leaf_nodes.rend(); leaf_node++) {
    if (writable) {
      (*leaf_node)->mutex.unlock();
    } else {
      (*leaf_node)->mutex.unlock_shared();
    }
  }
  return Status(Status::SUCCESS);
}

Status BabyDBMImpl::ProcessFirst(DBM::RecordProcessor* proc, bool writable) {
  std::shared_lock<SpinSharedMutex> lock(mutex_);
  BabyLeafNode* leaf_node = first_node_;
  while (leaf_node != nullptr) {
    if (writable) {
      std::lock_guard<SpinSharedMutex> page_lock(leaf_node->mutex);
      if (!leaf_node->records.empty()) {
        const std::string key(leaf_node->records.front()->GetKey());
        ProcessImpl(leaf_node, key, proc, true);
        return Status(Status::SUCCESS);
      }
    } else {
      std::shared_lock<SpinSharedMutex> page_lock(leaf_node->mutex);
      if (!leaf_node->records.empty()) {
        const std::string key(leaf_node->records.front()->GetKey());
        ProcessImpl(leaf_node, key, proc, true);
        return Status(Status::SUCCESS);
      }
    }
    leaf_node = leaf_node->next;
  }
  return Status(Status::NOT_FOUND_ERROR);
}

Status BabyDBMImpl::ProcessEach(DBM::RecordProcessor* proc, bool writable) {
  std::shared_lock<SpinSharedMutex> lock(mutex_);
  proc->ProcessEmpty(DBM::RecordProcessor::NOOP);
  BabyLeafNode* leaf_node = first_node_;
  while (leaf_node != nullptr) {
    if (writable) {
      std::lock_guard<SpinSharedMutex> page_lock(leaf_node->mutex);
      std::vector<std::string> keys;
      keys.reserve(leaf_node->records.size());
      for (const auto* rec : leaf_node->records) {
        keys.emplace_back(std::string(rec->GetKey()));
      }
      for (const auto& key : keys) {
        ProcessImpl(leaf_node, key, proc, true);
      }
    } else {
      std::shared_lock<SpinSharedMutex> page_lock(leaf_node->mutex);
      for (const auto* rec : leaf_node->records) {
        proc->ProcessFull(rec->GetKey(), rec->GetValue());
      }
    }
    leaf_node = leaf_node->next;
  }
  proc->ProcessEmpty(DBM::RecordProcessor::NOOP);
  return Status(Status::SUCCESS);
}

Status BabyDBMImpl::Count(int64_t* count) {
  std::shared_lock<SpinSharedMutex> lock(mutex_);
  *count = num_records_.load();
  return Status(Status::SUCCESS);
}

Status BabyDBMImpl::GetFileSize(int64_t* size) {
  std::shared_lock<SpinSharedMutex> lock(mutex_);
  if (!open_) {
    return Status(Status::PRECONDITION_ERROR, "not opened database");
  }
  *size = file_->GetSizeSimple();
  return Status(Status::SUCCESS);
}

Status BabyDBMImpl::GetFilePath(std::string* path) {
  std::shared_lock<SpinSharedMutex> lock(mutex_);
  if (!open_) {
    return Status(Status::PRECONDITION_ERROR, "not opened database");
  }
  *path = path_;
  return Status(Status::SUCCESS);
}

Status BabyDBMImpl::GetTimestamp(double* timestamp) {
  std::shared_lock<SpinSharedMutex> lock(mutex_);
  if (!open_) {
    return Status(Status::PRECONDITION_ERROR, "not opened database");
  }
  *timestamp = timestamp_;
  return Status(Status::SUCCESS);
}

Status BabyDBMImpl::Clear() {
  std::lock_guard<SpinSharedMutex> lock(mutex_);
  if (update_logger_ != nullptr) {
    update_logger_->WriteClear();
  }
  for (auto* iterator : iterators_) {
    iterator->ClearPosition();
  }
  FreeNodes();
  InitializeNodes();
  num_records_.store(0);
  return Status(Status::SUCCESS);
}

Status BabyDBMImpl::Synchronize(bool hard, DBM::FileProcessor* proc) {
  std::lock_guard<SpinSharedMutex> lock(mutex_);
  Status status(Status::SUCCESS);
  if (writable_ && update_logger_ != nullptr) {
    status |= update_logger_->Synchronize(hard);
  }
  if (open_ && writable_) {
    status |= ExportRecords();
    status |= file_->Synchronize(hard);
    if (proc != nullptr) {
      proc->Process(path_);
    }
  }
  return status;
}

std::vector<std::pair<std::string, std::string>> BabyDBMImpl::Inspect() {
  std::lock_guard<SpinSharedMutex> lock(mutex_);
  std::vector<std::pair<std::string, std::string>> meta;
  auto Add = [&](const std::string& name, const std::string& value) {
    meta.emplace_back(std::make_pair(name, value));
  };
  Add("class", "BabyDBM");
  if (open_) {
    Add("path", path_);
    Add("timestamp", SPrintF("%.6f", timestamp_));
  }
  Add("num_records", ToString(num_records_.load()));
  Add("tree_level", ToString(tree_level_));
  return meta;
}

bool BabyDBMImpl::IsOpen() {
  std::shared_lock<SpinSharedMutex> lock(mutex_);
  return open_;
}

bool BabyDBMImpl::IsWritable() {
  std::shared_lock<SpinSharedMutex> lock(mutex_);
  return open_ && writable_;
}

std::unique_ptr<DBM> BabyDBMImpl::MakeDBM() {
  std::shared_lock<SpinSharedMutex> lock(mutex_);
  return std::make_unique<BabyDBM>(file_->MakeFile());
}

DBM::UpdateLogger* BabyDBMImpl::GetUpdateLogger() {
  std::shared_lock<SpinSharedMutex> lock(mutex_);
  return update_logger_;
}

void BabyDBMImpl::SetUpdateLogger(DBM::UpdateLogger* update_logger) {
  std::lock_guard<SpinSharedMutex> lock(mutex_);
  update_logger_ = update_logger;
}

File* BabyDBMImpl::GetInternalFile() const {
  return file_.get();
}

KeyComparator BabyDBMImpl::GetKeyComparator() {
  return key_comparator_;
}

void BabyDBMImpl::InitializeNodes() {
  root_node_ = new BabyLeafNode(nullptr, nullptr);
  first_node_ = reinterpret_cast<BabyLeafNode*>(root_node_);
  last_node_ = reinterpret_cast<BabyLeafNode*>(root_node_);
  tree_level_ = 1;
}

void BabyDBMImpl::FreeNodes() {
  std::vector<std::pair<int32_t, void*>> stack;
  stack.emplace_back(std::make_pair(1, root_node_));
  while (!stack.empty()) {
    auto pair = stack.back();
    stack.pop_back();
    if (pair.first == tree_level_) {
      BabyLeafNode* leaf_node = reinterpret_cast<BabyLeafNode*>(pair.second);
      delete leaf_node;
    } else {
      BabyInnerNode* inner_node = reinterpret_cast<BabyInnerNode*>(pair.second);
      stack.emplace_back(std::make_pair(pair.first + 1, inner_node->heir));
      for (const auto* link : inner_node->links) {
        stack.emplace_back(std::make_pair(pair.first + 1, link->GetChild()));
      }
      delete inner_node;
    }
  }
  tree_level_ = 0;
  reorg_nodes_.Clear();
}

BabyLeafNode* BabyDBMImpl::SearchTree(std::string_view key) {
  void* node = root_node_;
  int32_t level = 1;
  BabyLinkOnStack search_stack(key);
  const BabyLink* search_link = search_stack.link;
  while (level < tree_level_) {
    BabyInnerNode* inner_node = reinterpret_cast<BabyInnerNode*>(node);
    const auto& links = inner_node->links;
    auto it = std::upper_bound(links.begin(), links.end(), search_link, link_comp_);
    if (it == links.begin()) {
      node = inner_node->heir;
    } else {
      --it;
      node = (*it)->GetChild();
    }
    level++;
  }
  return reinterpret_cast<BabyLeafNode*>(node);
}

void BabyDBMImpl::TraceTree(std::string_view key, BabyInnerNode** hist, int32_t* hist_size) {
  void* node = root_node_;
  int32_t level = 1;
  BabyLinkOnStack search_stack(key);
  const BabyLink* search_link = search_stack.link;
  *hist_size = 0;
  while (level < tree_level_) {
    BabyInnerNode* inner_node = reinterpret_cast<BabyInnerNode*>(node);
    hist[(*hist_size)++] = inner_node;
    const auto& links = inner_node->links;
    auto it = std::upper_bound(links.begin(), links.end(), search_link, link_comp_);
    if (it == links.begin()) {
      node = inner_node->heir;
    } else {
      --it;
      node = (*it)->GetChild();
    }
    level++;
  }
}

void BabyDBMImpl::ReorganizeTree() {
  std::set<BabyLeafNode*> done_nodes;
  while (!reorg_nodes_.IsEmpty()) {
    const auto& node_key = reorg_nodes_.Pop();
    if (!done_nodes.emplace(node_key.first).second) {
      continue;
    }
    if (CheckLeafNodeToDivide(node_key.first)) {
      DivideNodes(node_key.first, node_key.second);
    } else if (CheckLeafNodeToMerge(node_key.first)) {
      MergeNodes(node_key.first, node_key.second);
    }
  }
}

bool BabyDBMImpl::CheckLeafNodeToDivide(BabyLeafNode* node) {
  return node->records.size() > MAX_LEAF_NODE_RECORDS;
}

bool BabyDBMImpl::CheckLeafNodeToMerge(BabyLeafNode* node) {
  return node->records.size() < MAX_LEAF_NODE_RECORDS / 2 && node != root_node_;
}

void BabyDBMImpl::DivideNodes(BabyLeafNode* leaf_node, const std::string& node_key) {
  BabyInnerNode* hist[TREE_LEVEL_MAX];
  int32_t hist_size = 0;
  TraceTree(node_key, hist, &hist_size);
  BabyLeafNode* new_leaf_node = new BabyLeafNode(leaf_node, leaf_node->next);
  if (new_leaf_node->next != nullptr) {
    new_leaf_node->next->prev = new_leaf_node;
  }
  leaf_node->next = new_leaf_node;
  auto& records = leaf_node->records;
  auto mid = records.begin() + records.size() / 2;
  auto it = mid;
  auto& new_records = new_leaf_node->records;
  new_records.reserve(records.end() - it);
  while (it != records.end()) {
    BabyRecord* rec = *it;
    new_records.emplace_back(rec);
    ++it;
  }
  if (last_node_ == leaf_node) {
    last_node_ = new_leaf_node;
  }
  for (auto* iterator : iterators_) {
    if (iterator->leaf_node_ == leaf_node) {
      BabyRecordOnStack search_stack(std::string_view(iterator->key_ptr_, iterator->key_size_));
      if (!record_comp_(search_stack.record, *mid)) {
        iterator->leaf_node_ = new_leaf_node;
      }
    }
  }
  records.erase(mid, records.end());
  records.shrink_to_fit();
  void* heir = leaf_node;
  void* child = new_leaf_node;
  std::string new_node_key(new_leaf_node->records.front()->GetKey());
  while (true) {
    if (hist_size < 1) {
      BabyInnerNode* inner_node = new BabyInnerNode(heir);
      AddLinkToInnerNode(inner_node, child, new_node_key);
      root_node_ = inner_node;
      tree_level_++;
      break;
    }
    BabyInnerNode* inner_node = hist[--hist_size];
    AddLinkToInnerNode(inner_node, child, new_node_key);
    auto& links = inner_node->links;
    if (static_cast<int32_t>(links.size()) <= MAX_INNER_NODE_BRANCHES) {
      break;
    }
    auto mid = links.begin() + links.size() / 2;
    BabyLink* link = *mid;
    BabyInnerNode* new_inner_node = new BabyInnerNode(link->GetChild());
    new_node_key = std::string(link->GetKey());
    auto link_it = mid + 1;
    new_inner_node->links.reserve(links.end() - link_it);
    while (link_it != links.end()) {
      link = *link_it;
      AddLinkToInnerNode(new_inner_node, link->GetChild(), link->GetKey());
      ++link_it;
    }
    int32_t num = new_inner_node->links.size();
    for (int32_t i = 0; i <= num; i++) {
      FreeBabyLink(links.back());
      links.pop_back();
    }
    links.shrink_to_fit();
    heir = inner_node;
    child = new_inner_node;
  }
}

void BabyDBMImpl::MergeNodes(BabyLeafNode* leaf_node, const std::string& node_key) {
  BabyInnerNode* hist[TREE_LEVEL_MAX];
  int32_t hist_size = 0;
  TraceTree(node_key, hist, &hist_size);
  BabyInnerNode* parent_node = hist[hist_size - 1];
  const auto& links = parent_node->links;
  BabyLeafNode* prev_leaf_node = nullptr;
  BabyLeafNode* next_leaf_node = nullptr;
  if (parent_node->heir == leaf_node) {
    if (!links.empty()) {
      next_leaf_node = reinterpret_cast<BabyLeafNode*>(links.front()->GetChild());
    }
  } else {
    for (int32_t link_index = 0; link_index < static_cast<int32_t>(links.size()); link_index++) {
      if (links[link_index]->GetChild() == leaf_node) {
        prev_leaf_node = reinterpret_cast<BabyLeafNode*>(
            link_index == 0 ? parent_node->heir : links[link_index - 1]->GetChild());
        if (link_index < static_cast<int32_t>(links.size()) - 1) {
          next_leaf_node = reinterpret_cast<BabyLeafNode*>(links[link_index + 1]->GetChild());
        }
        break;
      }
    }
  }
  if (prev_leaf_node != nullptr &&
      (next_leaf_node == nullptr ||
       prev_leaf_node->records.size() <= next_leaf_node->records.size())) {
    prev_leaf_node->records.reserve(prev_leaf_node->records.size() + leaf_node->records.size());
    prev_leaf_node->records.insert(
        prev_leaf_node->records.end(), leaf_node->records.begin(), leaf_node->records.end());
    leaf_node->records.clear();
    prev_leaf_node->next = leaf_node->next;
    if (leaf_node->next != nullptr) {
      if (next_leaf_node == nullptr) {
        next_leaf_node = leaf_node->next;
      }
      next_leaf_node->prev = prev_leaf_node;
    }
    JoinPrevLinkInInnerNode(parent_node, leaf_node);
    if (last_node_ == leaf_node) {
      last_node_ = prev_leaf_node;
    }
    for (auto* iterator : iterators_) {
      if (iterator->leaf_node_ == leaf_node) {
        iterator->leaf_node_ = prev_leaf_node;
      }
    }
    delete leaf_node;
  } else if (next_leaf_node != nullptr) {
    next_leaf_node->records.swap(leaf_node->records);
    next_leaf_node->records.reserve(next_leaf_node->records.size() + leaf_node->records.size());
    next_leaf_node->records.insert(
        next_leaf_node->records.end(), leaf_node->records.begin(), leaf_node->records.end());
    leaf_node->records.clear();
    next_leaf_node->prev = leaf_node->prev;
    if (leaf_node->prev != nullptr) {
      if (prev_leaf_node == nullptr) {
        prev_leaf_node = leaf_node->prev;
      }
      prev_leaf_node->next = next_leaf_node;
    }
    JoinNextLinkInInnerNode(parent_node, leaf_node, next_leaf_node);
    if (first_node_ == leaf_node) {
      first_node_ = next_leaf_node;
    }
    for (auto* iterator : iterators_) {
      if (iterator->leaf_node_ == leaf_node) {
        iterator->leaf_node_ = next_leaf_node;
      }
    }
    delete leaf_node;
  }
  BabyInnerNode* inner_node = parent_node;
  while (static_cast<int32_t>(inner_node->links.size()) < MAX_INNER_NODE_BRANCHES / 2) {
    hist_size--;
    if (hist_size == 0) {
      if (inner_node->links.empty()) {
        root_node_ = inner_node->heir;
        tree_level_--;
        delete inner_node;
      }
      break;
    }
    BabyInnerNode* parent_node = hist[hist_size - 1];
    const auto& links = parent_node->links;
    BabyInnerNode* prev_inner_node = nullptr;
    BabyInnerNode* next_inner_node = nullptr;
    std::string_view inner_key, next_key;
    if (parent_node->heir == inner_node) {
      if (!links.empty()) {
        next_inner_node = reinterpret_cast<BabyInnerNode*>(links.front()->GetChild());
        next_key = links.front()->GetKey();
      }
    } else {
      for (int32_t link_index = 0;
           link_index < static_cast<int32_t>(links.size()); link_index++) {
        if (links[link_index]->GetChild() == inner_node) {
          prev_inner_node = reinterpret_cast<BabyInnerNode*>(
              link_index == 0 ? parent_node->heir : links[link_index - 1]->GetChild());
          inner_key = links[link_index]->GetKey();
          if (link_index < static_cast<int32_t>(links.size()) - 1) {
            next_inner_node = reinterpret_cast<BabyInnerNode*>(links[link_index + 1]->GetChild());
            next_key = links[link_index + 1]->GetKey();
          }
          break;
        }
      }
    }
    if (prev_inner_node != nullptr &&
        (next_inner_node == nullptr ||
         prev_inner_node->links.size() <= next_inner_node->links.size())) {
      prev_inner_node->links.reserve(prev_inner_node->links.size() + 1 + inner_node->links.size());
      if (inner_node->heir != nullptr) {
        prev_inner_node->links.emplace_back(CreateBabyLink(inner_key, inner_node->heir));
      }
      prev_inner_node->links.insert(
          prev_inner_node->links.end(), inner_node->links.begin(), inner_node->links.end());
      inner_node->links.clear();
      JoinPrevLinkInInnerNode(parent_node, inner_node);
      delete inner_node;
    } else if (next_inner_node != nullptr) {
      inner_node->links.reserve(inner_node->links.size() + 1 + next_inner_node->links.size());
      if (next_inner_node->heir != nullptr) {
        inner_node->links.emplace_back(CreateBabyLink(next_key, next_inner_node->heir));
      }
      inner_node->links.insert(
          inner_node->links.end(), next_inner_node->links.begin(), next_inner_node->links.end());
      next_inner_node->links.clear();
      JoinPrevLinkInInnerNode(parent_node, next_inner_node);
      delete next_inner_node;
    }
    inner_node = parent_node;
  }
}

void BabyDBMImpl::AddLinkToInnerNode(BabyInnerNode* node, void* child, std::string_view key) {
  BabyLink* link = CreateBabyLink(key, child);
  auto& links = node->links;
  auto it = std::upper_bound(links.begin(), links.end(), link, link_comp_);
  links.insert(it, link);
}

void BabyDBMImpl::JoinPrevLinkInInnerNode(BabyInnerNode* node, void* child) {
  auto& links = node->links;
  for (auto it = links.begin(); it != links.end(); ++it) {
    if ((*it)->GetChild() == child) {
      FreeBabyLink(*it);
      links.erase(it);
      break;
    }
  }
}

void BabyDBMImpl::JoinNextLinkInInnerNode(BabyInnerNode* node, void* child, void* next) {
  auto& links = node->links;
  if (node->heir == child) {
    node->heir = next;
    FreeBabyLink(links.front());
    links.erase(links.begin());
  } else {
    for (auto it = links.begin(); it != links.end(); ++it) {
      if ((*it)->GetChild() == child) {
        (*it)->SetChild(next);
        ++it;
        FreeBabyLink(*it);
        links.erase(it);
        break;
      }
    }
  }
}

Status BabyDBMImpl::ImportRecords() {
  int64_t end_offset = 0;
  Status status = file_->GetSize(&end_offset);
  if (status != Status::SUCCESS) {
    return status;
  }
  FlatRecordReader reader(file_.get());
  std::string key_store;
  while (true) {
    std::string_view key;
    FlatRecord::RecordType rec_type;
    Status status = reader.Read(&key, &rec_type);
    if (status != Status::SUCCESS) {
      if (status != Status::NOT_FOUND_ERROR) {
        return status;
      }
      break;
    }
    if (rec_type != FlatRecord::RECORD_NORMAL) {
      if (rec_type == FlatRecord::RECORD_METADATA) {
        const auto& meta = DeserializeStrMap(key);
        if (StrContains(SearchMap(meta, "class", ""), "DBM")) {
          const auto& tsexpr = SearchMap(meta, "timestamp", "");
          if (!tsexpr.empty()) {
            timestamp_ = StrToDouble(tsexpr);
          }
        }
      }
      continue;
    }
    key_store = key;
    std::string_view value;
    status = reader.Read(&value, &rec_type);
    if (status != Status::SUCCESS) {
      if (status != Status::NOT_FOUND_ERROR) {
        return status;
      }
      return Status(Status::BROKEN_DATA_ERROR, "odd number of records");
    }
    if (rec_type != FlatRecord::RECORD_NORMAL) {
      return Status(Status::BROKEN_DATA_ERROR, "invalid metadata position");
    }
    DBM::RecordProcessorSet setter(&status, value, true, nullptr);
    BabyLeafNode* leaf_node = SearchTree(key_store);
    std::lock_guard<SpinSharedMutex> page_lock(leaf_node->mutex);
    ProcessImpl(leaf_node, key_store, &setter, true);
  }
  return Status(Status::SUCCESS);
}

Status BabyDBMImpl::ExportRecords() {
  Status status = file_->Close();
  if (status != Status::SUCCESS) {
    return status;
  }
  const std::string export_path = path_ + ".tmp.export";
  const int32_t export_options = File::OPEN_TRUNCATE | (open_options_ & File::OPEN_SYNC_HARD);
  status = file_->Open(export_path, true, export_options);
  if (status != Status::SUCCESS) {
    file_->Open(path_, true, open_options_ & ~File::OPEN_TRUNCATE);
    return status;
  }
  FlatRecord flat_rec(file_.get());
  std::map<std::string, std::string> meta;
  meta["class"] = "BabyDBM";
  meta["timestamp"] = SPrintF("%.6f", GetWallTime());
  meta["num_records"] = ToString(num_records_.load());
  meta["tree_level"] = ToString(tree_level_);
  status |= flat_rec.Write(SerializeStrMap(meta), FlatRecord::RECORD_METADATA);
  BabyLeafNode* leaf_node = first_node_;
  while (leaf_node != nullptr) {
    std::shared_lock<SpinSharedMutex> page_lock(leaf_node->mutex);
    for (const auto* rec : leaf_node->records) {
      status |= flat_rec.Write(rec->GetKey());
      status |= flat_rec.Write(rec->GetValue());
      if (status != Status::SUCCESS) {
        break;
      }
    }
    leaf_node = leaf_node->next;
  }
  status |= file_->Close();
  status |= RenameFile(export_path, path_);
  RemoveFile(export_path);
  status |= file_->Open(path_, true, open_options_ & ~File::OPEN_TRUNCATE);
  return status;
}

void BabyDBMImpl::ProcessImpl(
    BabyLeafNode* node, std::string_view key, DBM::RecordProcessor* proc, bool writable) {
  BabyRecordOnStack search_stack(key);
  const BabyRecord* search_rec = search_stack.record;
  auto& records = node->records;
  auto it = std::lower_bound(records.begin(), records.end(), search_rec, record_comp_);
  if (it != records.end() && !record_comp_(search_rec, *it)) {
    BabyRecord* rec = *it;
    const std::string_view new_value = proc->ProcessFull(rec->GetKey(), rec->GetValue());
    if (new_value.data() != DBM::RecordProcessor::NOOP.data() && writable) {
      if (update_logger_ != nullptr) {
        if (new_value.data() == DBM::RecordProcessor::REMOVE.data()) {
          update_logger_->WriteRemove(key);
        } else {
          update_logger_->WriteSet(key, new_value);
        }
      }
      if (new_value.data() == DBM::RecordProcessor::REMOVE.data()) {
        node->records.erase(it);
        if (CheckLeafNodeToMerge(node)) {
          reorg_nodes_.Insert(std::make_pair(node, std::string(records.front()->GetKey())));
        }
        FreeBabyRecord(rec);
        num_records_.fetch_sub(1);
      } else {
        *it = ModifyBabyRecord(rec, new_value);
      }
    }
  } else {
    const std::string_view new_value = proc->ProcessEmpty(key);
    if (new_value.data() != DBM::RecordProcessor::NOOP.data() &&
        new_value.data() != DBM::RecordProcessor::REMOVE.data() &&
        writable) {
      if (update_logger_ != nullptr) {
        update_logger_->WriteSet(key, new_value);
      }
      BabyRecord* new_rec = CreateBabyRecord(key, new_value);
      node->records.insert(it, new_rec);
      num_records_.fetch_add(1);
      if (CheckLeafNodeToDivide(node)) {
        reorg_nodes_.Insert(std::make_pair(node, std::string(records.front()->GetKey())));
      }
    }
  }
}

void BabyDBMImpl::AppendImpl(
    BabyLeafNode* node, std::string_view key, std::string_view value, std::string_view delim) {
  BabyRecordOnStack search_stack(key);
  const BabyRecord* search_rec = search_stack.record;
  auto& records = node->records;
  auto it = std::lower_bound(records.begin(), records.end(), search_rec, record_comp_);
  if (it != records.end() && !record_comp_(search_rec, *it)) {
    BabyRecord* rec = *it;
    *it = AppendBabyRecord(rec, value, delim);
    if (update_logger_ != nullptr) {
      update_logger_->WriteSet(key, (*it)->GetValue());
    }
  } else {
    if (update_logger_ != nullptr) {
      update_logger_->WriteSet(key, value);
    }
    BabyRecord* new_rec = CreateBabyRecord(key, value);
    node->records.insert(it, new_rec);
    num_records_.fetch_add(1);
    if (CheckLeafNodeToDivide(node)) {
      reorg_nodes_.Insert(std::make_pair(node, std::string(records.front()->GetKey())));
    }
  }
}

BabyDBMIteratorImpl::BabyDBMIteratorImpl(BabyDBMImpl* dbm)
    : dbm_(dbm), key_ptr_(nullptr), key_size_(0), leaf_node_(nullptr) {
  std::lock_guard<SpinSharedMutex> lock(dbm_->mutex_);
  dbm_->iterators_.emplace_back(this);
}

BabyDBMIteratorImpl::~BabyDBMIteratorImpl() {
  if (dbm_ != nullptr) {
    std::lock_guard<SpinSharedMutex> lock(dbm_->mutex_);
    dbm_->iterators_.remove(this);
  }
  ClearPosition();
}

Status BabyDBMIteratorImpl::First() {
  std::shared_lock<SpinSharedMutex> lock(dbm_->mutex_);
  ClearPosition();
  SetPositionFirst(dbm_->first_node_);
  return Status(Status::SUCCESS);
}

Status BabyDBMIteratorImpl::Last() {
  std::shared_lock<SpinSharedMutex> lock(dbm_->mutex_);
  ClearPosition();
  SetPositionLast(dbm_->last_node_);
  return Status(Status::SUCCESS);
}

Status BabyDBMIteratorImpl::Jump(std::string_view key) {
  std::shared_lock<SpinSharedMutex> lock(dbm_->mutex_);
  ClearPosition();
  SetPositionWithKey(nullptr, key);
  return Status(Status::SUCCESS);
}

Status BabyDBMIteratorImpl::JumpLower(std::string_view key, bool inclusive) {
  std::shared_lock<SpinSharedMutex> lock(dbm_->mutex_);
  ClearPosition();
  SetPositionWithKey(nullptr, key);
  Status status = SyncPosition(key);
  if (status == Status::NOT_FOUND_ERROR) {
    if (!SetPositionLast(dbm_->last_node_)) {
      return Status(Status::SUCCESS);
    }
  } else if (status != Status::SUCCESS) {
    return status;
  }
  while (key_ptr_ != nullptr) {
    const std::string_view rec_key(key_ptr_, key_size_);
    const int32_t cmp = dbm_->key_comparator_(rec_key, key);
    if (cmp < 0 || (inclusive && cmp == 0)) {
      return Status(Status::SUCCESS);
    }
    status = PreviousImpl(key);
    if (status != Status::SUCCESS) {
      if (status == Status::NOT_FOUND_ERROR) {
        return Status(Status::SUCCESS);
      }
      return status;
    }
  }
  ClearPosition();
  return Status(Status::SUCCESS);
}

Status BabyDBMIteratorImpl::JumpUpper(std::string_view key, bool inclusive) {
  std::shared_lock<SpinSharedMutex> lock(dbm_->mutex_);
  ClearPosition();
  SetPositionWithKey(nullptr, key);
  Status status = SyncPosition(key);
  if (status != Status::SUCCESS) {
    if (status == Status::NOT_FOUND_ERROR) {
      return Status(Status::SUCCESS);
    }
    return status;
  }
  while (key_ptr_ != nullptr) {
    const std::string_view rec_key(key_ptr_, key_size_);
    const int32_t cmp = dbm_->key_comparator_(rec_key, key);
    if (cmp > 0 || (inclusive && cmp == 0)) {
      return Status(Status::SUCCESS);
    }
    status = NextImpl(key);
    if (status != Status::SUCCESS) {
      if (status == Status::NOT_FOUND_ERROR) {
        return Status(Status::SUCCESS);
      }
      return status;
    }
  }
  ClearPosition();
  return Status(Status::SUCCESS);
}

Status BabyDBMIteratorImpl::Next() {
  std::shared_lock<SpinSharedMutex> lock(dbm_->mutex_);
  if (key_ptr_ == nullptr) {
    return Status(Status::NOT_FOUND_ERROR);
  }
  const std::string_view key(key_ptr_, key_size_);
  return NextImpl(key);
}

Status BabyDBMIteratorImpl::Previous() {
  std::shared_lock<SpinSharedMutex> lock(dbm_->mutex_);
  if (key_ptr_ == nullptr) {
    return Status(Status::NOT_FOUND_ERROR);
  }
  const std::string_view key(key_ptr_, key_size_);
  return PreviousImpl(key);
}

Status BabyDBMIteratorImpl::Process(DBM::RecordProcessor* proc, bool writable) {
  if (writable && !dbm_->reorg_nodes_.IsEmpty()) {
    std::lock_guard<SpinSharedMutex> lock(dbm_->mutex_);
    dbm_->ReorganizeTree();
  }
  std::shared_lock<SpinSharedMutex> lock(dbm_->mutex_);
  if (key_ptr_ == nullptr) {
    return Status(Status::NOT_FOUND_ERROR);
  }
  const std::string_view key(key_ptr_, key_size_);
  return ProcessImpl(key, proc, writable);
}

void BabyDBMIteratorImpl::ClearPosition() {
  if (key_ptr_ != stack_) {
    delete[] key_ptr_;
  }
  key_ptr_ = nullptr;
  key_size_ = 0;
  leaf_node_ = nullptr;
}

bool BabyDBMIteratorImpl::SetPositionFirst(BabyLeafNode* leaf_node) {
  while (leaf_node != nullptr) {
    std::shared_lock<SpinSharedMutex> page_lock(leaf_node->mutex);
    auto& records = leaf_node->records;
    if (!records.empty()) {
      SetPositionWithKey(leaf_node, records.front()->GetKey());
      return true;
    } else {
      leaf_node = leaf_node->next;
    }
  }
  return false;
}

bool BabyDBMIteratorImpl::SetPositionLast(BabyLeafNode* leaf_node) {
  while (leaf_node != nullptr) {
    std::shared_lock<SpinSharedMutex> page_lock(leaf_node->mutex);
    auto& records = leaf_node->records;
    if (!records.empty()) {
      SetPositionWithKey(leaf_node, records.back()->GetKey());
      return true;
    } else {
      leaf_node = leaf_node->prev;
    }
  }
  return false;
}

void BabyDBMIteratorImpl::SetPositionWithKey(BabyLeafNode* leaf_node, std::string_view key) {
  key_ptr_ = key.size() > sizeof(stack_) ? new char[key.size()] : stack_;
  std::memcpy(key_ptr_, key.data(), key.size());
  key_size_ = key.size();
  leaf_node_ = leaf_node;
}

Status BabyDBMIteratorImpl::NextImpl(std::string_view key) {
  if (leaf_node_ == nullptr) {
    leaf_node_ = dbm_->SearchTree(key);
  }
  bool hit = false;
  BabyRecordOnStack search_stack(key);
  const BabyRecord* search_rec = search_stack.record;
  {
    std::shared_lock<SpinSharedMutex> lock(leaf_node_->mutex);
    auto& records = leaf_node_->records;
    auto it = std::upper_bound(records.begin(), records.end(), search_rec, dbm_->record_comp_);
    if (it != records.end()) {
      SetPositionWithKey(leaf_node_, (*it)->GetKey());
      hit = true;
    }
  }
  if (!hit) {
    if (!SetPositionFirst(leaf_node_->next)) {
      ClearPosition();
    }
  }
  return Status(Status::SUCCESS);
}

Status BabyDBMIteratorImpl::PreviousImpl(std::string_view key) {
  if (leaf_node_ == nullptr) {
    leaf_node_ = dbm_->SearchTree(key);
  }
  bool hit = false;
  BabyRecordOnStack search_stack(key);
  const BabyRecord* search_rec = search_stack.record;
  {
    std::shared_lock<SpinSharedMutex> lock(leaf_node_->mutex);
    auto& records = leaf_node_->records;
    auto it = std::lower_bound(records.begin(), records.end(), search_rec, dbm_->record_comp_);
    if (it != records.end()) {
      if (it != records.begin()) {
        --it;
        SetPositionWithKey(leaf_node_, (*it)->GetKey());
        hit = true;
      }
    }
  }
  if (!hit) {
    if (!SetPositionLast(leaf_node_->prev)) {
      ClearPosition();
    }
  }
  return Status(Status::SUCCESS);
}

Status BabyDBMIteratorImpl::SyncPosition(std::string_view key) {
  if (leaf_node_ == nullptr) {
    leaf_node_ = dbm_->SearchTree(key);
  }
  BabyRecordOnStack search_stack(key);
  const BabyRecord* search_rec = search_stack.record;
  bool hit = false;
  std::shared_lock<SpinSharedMutex> lock(leaf_node_->mutex);
  auto& records = leaf_node_->records;
  auto it = std::lower_bound(records.begin(), records.end(), search_rec, dbm_->record_comp_);
  if (it != records.end()) {
    BabyRecord* rec = *it;
    SetPositionWithKey(leaf_node_, rec->GetKey());
    hit = true;
  }
  if (!hit) {
    leaf_node_ = leaf_node_->next;
    while (leaf_node_ != nullptr) {
      std::shared_lock<SpinSharedMutex> lock(leaf_node_->mutex);
      auto& records = leaf_node_->records;
      if (!records.empty()) {
        BabyRecord* rec = records.front();
        SetPositionWithKey(leaf_node_, rec->GetKey());
        hit = true;
        break;
      } else {
        leaf_node_ = leaf_node_->next;
      }
    }
  }
  if (!hit) {
    ClearPosition();
    return Status(Status::NOT_FOUND_ERROR);
  }
  return Status(Status::SUCCESS);
}

Status BabyDBMIteratorImpl::ProcessImpl(
    std::string_view key, DBM::RecordProcessor* proc, bool writable) {
  if (leaf_node_ == nullptr) {
    leaf_node_ = dbm_->SearchTree(key);
  }
  BabyRecordOnStack search_stack(key);
  const BabyRecord* search_rec = search_stack.record;
  bool hit = false;
  if (writable) {
    std::lock_guard<SpinSharedMutex> lock(leaf_node_->mutex);
    auto& records = leaf_node_->records;
    auto it = std::lower_bound(records.begin(), records.end(), search_rec, dbm_->record_comp_);
    if (it != records.end()) {
      BabyRecord* rec = *it;
      SetPositionWithKey(leaf_node_, rec->GetKey());
      dbm_->ProcessImpl(leaf_node_, std::string_view(key_ptr_, key_size_), proc, true);
      hit = true;
    }
  } else {
    std::shared_lock<SpinSharedMutex> lock(leaf_node_->mutex);
    auto& records = leaf_node_->records;
    auto it = std::lower_bound(records.begin(), records.end(), search_rec, dbm_->record_comp_);
    if (it != records.end()) {
      BabyRecord* rec = *it;
      SetPositionWithKey(leaf_node_, rec->GetKey());
      proc->ProcessFull(rec->GetKey(), rec->GetValue());
      hit = true;
    }
  }
  if (!hit) {
    leaf_node_ = leaf_node_->next;
    while (leaf_node_ != nullptr) {
      if (writable) {
        std::lock_guard<SpinSharedMutex> lock(leaf_node_->mutex);
        auto& records = leaf_node_->records;
        if (!records.empty()) {
          BabyRecord* rec = records.front();
          SetPositionWithKey(leaf_node_, rec->GetKey());
          dbm_->ProcessImpl(leaf_node_, std::string_view(key_ptr_, key_size_), proc, true);
          hit = true;
          break;
        } else {
          leaf_node_ = leaf_node_->next;
        }
      } else {
        std::shared_lock<SpinSharedMutex> lock(leaf_node_->mutex);
        auto& records = leaf_node_->records;
        if (!records.empty()) {
          BabyRecord* rec = records.front();
          SetPositionWithKey(leaf_node_, rec->GetKey());
          proc->ProcessFull(rec->GetKey(), rec->GetValue());
          hit = true;
          break;
        } else {
          leaf_node_ = leaf_node_->next;
        }
      }
    }
  }
  if (!hit) {
    ClearPosition();
    return Status(Status::NOT_FOUND_ERROR);
  }
  return Status(Status::SUCCESS);
}

BabyDBM::BabyDBM(KeyComparator key_comparator) {
  assert(key_comparator != nullptr);
  impl_ = new BabyDBMImpl(std::make_unique<MemoryMapParallelFile>(), key_comparator);
}

BabyDBM::BabyDBM(std::unique_ptr<File> file, KeyComparator key_comparator) {
  assert(key_comparator != nullptr);
  impl_ = new BabyDBMImpl(std::move(file), key_comparator);
}

BabyDBM::~BabyDBM() {
  delete impl_;
}

Status BabyDBM::Open(const std::string& path, bool writable, int32_t options) {
  return impl_->Open(path, writable, options);
}

Status BabyDBM::Close() {
  return impl_->Close();
}

Status BabyDBM::Process(std::string_view key, RecordProcessor* proc, bool writable) {
  assert(proc != nullptr);
  return impl_->Process(key, proc, writable);
}

Status BabyDBM::Append(std::string_view key, std::string_view value, std::string_view delim) {
  return impl_->Append(key, value, delim);
}

Status BabyDBM::ProcessMulti(
    const std::vector<std::pair<std::string_view, RecordProcessor*>>& key_proc_pairs,
    bool writable) {
  return impl_->ProcessMulti(key_proc_pairs, writable);
}

Status BabyDBM::ProcessFirst(RecordProcessor* proc, bool writable) {
  assert(proc != nullptr);
  return impl_->ProcessFirst(proc, writable);
}

Status BabyDBM::ProcessEach(RecordProcessor* proc, bool writable) {
  assert(proc != nullptr);
  return impl_->ProcessEach(proc, writable);
}

Status BabyDBM::Count(int64_t* count) {
  assert(count != nullptr);
  return impl_->Count(count);
}

Status BabyDBM::GetFileSize(int64_t* size) {
  assert(size != nullptr);
  return impl_->GetFileSize(size);
}

Status BabyDBM::GetFilePath(std::string* path) {
  assert(path != nullptr);
  return impl_->GetFilePath(path);
}

Status BabyDBM::GetTimestamp(double* timestamp) {
  assert(timestamp != nullptr);
  return impl_->GetTimestamp(timestamp);
}

Status BabyDBM::Clear() {
  return impl_->Clear();
}

Status BabyDBM::Synchronize(bool hard, FileProcessor* proc) {
  return impl_->Synchronize(hard, proc);
}

std::vector<std::pair<std::string, std::string>> BabyDBM::Inspect() {
  return impl_->Inspect();
}

bool BabyDBM::IsOpen() const {
  return impl_->IsOpen();
}

bool BabyDBM::IsWritable() const {
  return impl_->IsWritable();
}

std::unique_ptr<DBM::Iterator> BabyDBM::MakeIterator() {
  std::unique_ptr<BabyDBM::Iterator> iter(new BabyDBM::Iterator(impl_));
  return iter;
}

std::unique_ptr<DBM> BabyDBM::MakeDBM() const {
  return impl_->MakeDBM();
}

DBM::UpdateLogger* BabyDBM::GetUpdateLogger() const {
  return impl_->GetUpdateLogger();
}

void BabyDBM::SetUpdateLogger(UpdateLogger* update_logger) {
  impl_->SetUpdateLogger(update_logger);
}

File* BabyDBM::GetInternalFile() const {
  return impl_->GetInternalFile();
}

KeyComparator BabyDBM::GetKeyComparator() const {
  return impl_->GetKeyComparator();
}

BabyDBM::Iterator::Iterator(BabyDBMImpl* dbm_impl) {
  impl_ = new BabyDBMIteratorImpl(dbm_impl);
}

BabyDBM::Iterator::~Iterator() {
  delete impl_;
}

Status BabyDBM::Iterator::First() {
  return impl_->First();
}

Status BabyDBM::Iterator::Last() {
  return impl_->Last();
}

Status BabyDBM::Iterator::Jump(std::string_view key) {
  return impl_->Jump(key);
}

Status BabyDBM::Iterator::JumpLower(std::string_view key, bool inclusive) {
  return impl_->JumpLower(key, inclusive);
}

Status BabyDBM::Iterator::JumpUpper(std::string_view key, bool inclusive) {
  return impl_->JumpUpper(key, inclusive);
}

Status BabyDBM::Iterator::Next() {
  return impl_->Next();
}

Status BabyDBM::Iterator::Previous() {
  return impl_->Previous();
}

Status BabyDBM::Iterator::Process(RecordProcessor* proc, bool writable) {
  assert(proc != nullptr);
  return impl_->Process(proc, writable);
}

}  // namespace tkrzw

// END OF FILE
