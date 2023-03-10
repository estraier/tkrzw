/*************************************************************************************************
 * File database manager implementation based on B+ tree
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
#include "tkrzw_dbm_common_impl.h"
#include "tkrzw_dbm_hash.h"
#include "tkrzw_dbm_tree.h"
#include "tkrzw_dbm_tree_impl.h"
#include "tkrzw_file.h"
#include "tkrzw_file_mmap.h"
#include "tkrzw_file_pos.h"
#include "tkrzw_file_std.h"
#include "tkrzw_file_util.h"
#include "tkrzw_lib_common.h"
#include "tkrzw_str_util.h"
#include "tkrzw_thread_util.h"

namespace tkrzw {

static constexpr char META_MAGIC_DATA[] = "TDB";
static constexpr int32_t META_OFFSET_NUM_RECORDS = 4;
static constexpr int32_t META_OFFSET_EFF_DATA_SIZE = 10;
static constexpr int32_t META_OFFSET_ROOT_ID = 16;
static constexpr int32_t META_OFFSET_FIRST_ID = 22;
static constexpr int32_t META_OFFSET_LAST_ID = 28;
static constexpr int32_t META_OFFSET_NUM_LEAF_NODES = 34;
static constexpr int32_t META_OFFSET_NUM_INNER_NODES = 40;
static constexpr int32_t META_OFFSET_MAX_PAGE_SIZE = 46;
static constexpr int32_t META_OFFSET_MAX_BRANCHES = 49;
static constexpr int32_t META_OFFSET_TREE_LEVEL = 52;
static constexpr int32_t META_OFFSET_KEY_COMPARATOR = 53;
static constexpr int32_t META_OFFSET_OPAQUE = 54;
static constexpr int32_t NUM_PAGE_SLOTS = 64;
static constexpr double INNER_PAGE_CACHE_RATIO = 0.25;
static constexpr double HOT_CACHE_RATIO = 0.35;
static constexpr int32_t PAGE_ID_WIDTH = 6;
static constexpr int32_t ADJUST_CACHES_INV_FREQ = 4;
static constexpr int64_t LEAF_NODE_ID_BASE = 1LL;
static constexpr int64_t INNER_NODE_ID_BASE = (1LL << (8 * PAGE_ID_WIDTH - 2)) * 3;
static constexpr int32_t WRITE_BUFFER_SIZE = 16384;
static constexpr int32_t TREE_LEVEL_MAX = 32;
static constexpr int32_t ITER_BUFFER_SIZE = 128;

class TreeDBMImpl;

struct TreeLeafNode final {
  TreeDBMImpl* impl;
  int64_t id;
  int64_t prev_id;
  int64_t next_id;
  std::vector<TreeRecord*> records;
  int32_t page_size;
  bool dirty;
  bool on_disk;
  SpinSharedMutex mutex;
  TreeLeafNode(TreeDBMImpl* impl, int64_t prev_id, int64_t next_id);
  TreeLeafNode(TreeDBMImpl* impl, int64_t id, int64_t prev_id, int64_t next_id,
               std::vector<TreeRecord*>&& records, int32_t page_size);
  ~TreeLeafNode();
  std::shared_ptr<TreeLeafNode> AddToCache();
};

struct TreeInnerNode final {
  TreeDBMImpl* impl;
  int64_t id;
  int64_t heir_id;
  std::vector<TreeLink*> links;
  bool dirty;
  bool on_disk;
  TreeInnerNode(TreeDBMImpl* impl, int64_t heir_id);
  TreeInnerNode(TreeDBMImpl* impl, int64_t id, int64_t heir_id, std::vector<TreeLink*>&& links);
  ~TreeInnerNode();
  std::shared_ptr<TreeInnerNode> AddToCache();
};

typedef DoubleLRUCache<TreeLeafNode> LeafCache;
typedef LRUCache<TreeInnerNode> InnerCache;

struct LeafSlot final {
  std::unique_ptr<LeafCache> cache;
  SpinMutex mutex;
};

struct InnerSlot final {
  std::unique_ptr<InnerCache> cache;
  SpinMutex mutex;
};

class TreeDBMImpl final {
  friend struct TreeLeafNode;
  friend struct TreeInnerNode;
  friend class TreeDBMIteratorImpl;
  typedef std::list<TreeDBMIteratorImpl*> IteratorList;
 public:
  TreeDBMImpl(std::unique_ptr<File> file);
  ~TreeDBMImpl();
  Status Open(const std::string& path, bool writable,
              int32_t options, const TreeDBM::TuningParameters& tuning_params);
  Status Close();
  Status Process(
      std::string_view key, DBM::RecordProcessor* proc, bool writable);
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
  Status Rebuild(
      const TreeDBM::TuningParameters& tuning_params, bool skip_broken_records, bool sync_hard);
  Status ShouldBeRebuilt(bool* tobe);
  Status Synchronize(bool hard, DBM::FileProcessor* proc);
  std::vector<std::pair<std::string, std::string>> Inspect();
  bool IsOpen();
  bool IsWritable();
  bool IsHealthy();
  bool IsAutoRestored();
  std::unique_ptr<DBM> MakeDBM();
  DBM::UpdateLogger* GetUpdateLogger();
  void SetUpdateLogger(DBM::UpdateLogger* update_logger);
  File* GetInternalFile();
  int64_t GetEffectiveDataSize();
  int32_t GetDatabaseType();
  Status SetDatabaseType(uint32_t db_type);
  std::string GetOpaqueMetadata();
  Status SetOpaqueMetadata(const std::string& opaque);
  KeyComparator GetKeyComparator();
  Status ValidateHashBuckets();
  Status ValidateRecords(int64_t record_base, int64_t end_offset);

 private:
  Status SaveMetadata();
  Status LoadMetadata();
  void InitializePageCache();
  Status LoadLeafNode(int64_t id, bool promotion, std::shared_ptr<TreeLeafNode>* node);
  Status SaveLeafNode(TreeLeafNode* node);
  Status SaveLeafNodeImpl(TreeLeafNode* node);
  Status RemoveLeafNode(TreeLeafNode* node);
  Status FlushLeafCacheAll(bool empty);
  Status FlushLeafCacheOne(bool empty, int32_t slot_index);
  void DiscardLeafCache();
  Status LoadInnerNode(int64_t id, bool promotion, std::shared_ptr<TreeInnerNode>* node);
  Status SaveInnerNode(TreeInnerNode* node);
  Status RemoveInnerNode(TreeInnerNode* node);
  Status FlushInnerCacheAll(bool empty);
  Status FlushInnerCacheOne(bool empty, int32_t slot_index);
  void DiscardInnerCache();
  Status SearchTree(std::string_view key, std::shared_ptr<TreeLeafNode>* leaf_node);
  Status TraceTree(std::string_view key, int64_t leaf_node_id, int64_t* hist, int32_t* hist_size);
  Status ReorganizeTree();
  bool CheckLeafNodeToDivide(TreeLeafNode* node);
  bool CheckLeafNodeToMerge(TreeLeafNode* node);
  Status DivideNodes(TreeLeafNode* leaf_node, const std::string& node_key);
  Status MergeNodes(TreeLeafNode* leaf_node, const std::string& node_key);
  void AddLinkToInnerNode(TreeInnerNode* node, int64_t child_id, std::string_view key);
  void JoinPrevLinkInInnerNode(TreeInnerNode* node, int64_t child_id);
  void JoinNextLinkInInnerNode(TreeInnerNode* node, int64_t child_id, int64_t next_id);
  void ProcessImpl(
      TreeLeafNode* node, std::string_view key, DBM::RecordProcessor* proc, bool writable);
  Status AdjustCaches();

  bool open_;
  bool writable_;
  bool healthy_;
  bool auto_restored_;
  std::string path_;
  std::atomic_int64_t num_records_;
  std::atomic_int64_t eff_data_size_;
  int64_t root_id_;
  int64_t first_id_;
  int64_t last_id_;
  int64_t num_leaf_nodes_;
  int64_t num_inner_nodes_;
  int32_t tree_level_;
  int32_t max_page_size_;
  int32_t max_branches_;
  int32_t max_cached_pages_;
  TreeDBM::PageUpdateMode page_update_mode_;
  LeafSlot leaf_slots_[NUM_PAGE_SLOTS];
  InnerSlot inner_slots_[NUM_PAGE_SLOTS];
  KeyComparator key_comparator_;
  TreeRecordComparator record_comp_;
  TreeLinkComparator link_comp_;
  DBM::UpdateLogger* update_logger_;
  std::string mini_opaque_;
  AtomicSet<std::pair<int64_t, std::string>> reorg_ids_;
  IteratorList iterators_;
  std::unique_ptr<HashDBM> hash_dbm_;
  std::atomic_uint32_t proc_clock_;
  SpinSharedMutex mutex_;
  SpinMutex rebuild_mutex_;
};

class TreeDBMIteratorImpl final {
  friend class TreeDBMImpl;
 public:
  explicit TreeDBMIteratorImpl(TreeDBMImpl* dbm);
  ~TreeDBMIteratorImpl();
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
  Status SetPositionFirst(int64_t leaf_id);
  Status SetPositionLast(int64_t leaf_id);
  void SetPositionWithKey(int64_t leaf_id, std::string_view key);
  Status NextImpl(std::string_view key);
  Status PreviousImpl(std::string_view key);
  Status SyncPosition(std::string_view key);
  Status ProcessImpl(std::string_view key, DBM::RecordProcessor* proc, bool writable);

  TreeDBMImpl* dbm_;
  char stack_[ITER_BUFFER_SIZE];
  char* key_ptr_;
  int32_t key_size_;
  int64_t leaf_id_;
};

TreeLeafNode::TreeLeafNode(TreeDBMImpl* impl, int64_t prev_id, int64_t next_id)
    : impl(impl), id(0), prev_id(prev_id), next_id(next_id), records(),
      page_size(PAGE_ID_WIDTH * 2), dirty(true), on_disk(false), mutex() {
  id = impl->num_leaf_nodes_++ + LEAF_NODE_ID_BASE;
}

TreeLeafNode::TreeLeafNode(TreeDBMImpl* impl, int64_t id, int64_t prev_id, int64_t next_id,
                           std::vector<TreeRecord*>&& records, int32_t page_size)
    : impl(impl), id(id), prev_id(prev_id), next_id(next_id),
      records(records), page_size(page_size), dirty(false), on_disk(true), mutex() {
}

TreeLeafNode::~TreeLeafNode() {
  impl->SaveLeafNode(this);
  FreeTreeRecords(&records);
}

std::shared_ptr<TreeLeafNode> TreeLeafNode::AddToCache() {
  int32_t slot_index = id % NUM_PAGE_SLOTS;
  LeafSlot* slot = impl->leaf_slots_ + slot_index;
  std::lock_guard<SpinMutex> lock(slot->mutex);
  return slot->cache->Add(id, this);
}

TreeInnerNode::TreeInnerNode(TreeDBMImpl* impl, int64_t heir_id)
    : impl(impl), id(0), heir_id(heir_id), links(), dirty(true), on_disk(false) {
  id = impl->num_inner_nodes_++ + INNER_NODE_ID_BASE;
}

TreeInnerNode::TreeInnerNode(TreeDBMImpl* impl, int64_t id, int64_t heir_id,
                             std::vector<TreeLink*>&& links)
    : impl(impl), id(id), heir_id(heir_id), links(links), dirty(false), on_disk(true) {
}

TreeInnerNode::~TreeInnerNode() {
  impl->SaveInnerNode(this);
  FreeTreeLinks(&links);
}

std::shared_ptr<TreeInnerNode> TreeInnerNode::AddToCache() {
  int32_t slot_index = id % NUM_PAGE_SLOTS;
  InnerSlot* slot = impl->inner_slots_ + slot_index;
  std::lock_guard<SpinMutex> lock(slot->mutex);
  return slot->cache->Add(id, this);
}

Status DeserializeLeafNode(
    std::string_view serialized,
    int64_t* prev_id, int64_t* next_id, std::vector<TreeRecord*>* records) {
  const char* rp = serialized.data();
  int32_t record_size = serialized.size();
  if (record_size < static_cast<int32_t>(PAGE_ID_WIDTH * 2)) {
    return Status(Status::BROKEN_DATA_ERROR, "invalid leaf metadata");
  }
  *prev_id = ReadFixNum(rp, PAGE_ID_WIDTH);
  rp += PAGE_ID_WIDTH;
  record_size -= PAGE_ID_WIDTH;
  *next_id = ReadFixNum(rp, PAGE_ID_WIDTH);
  rp += PAGE_ID_WIDTH;
  record_size -= PAGE_ID_WIDTH;
  while (record_size > 0) {
    uint64_t key_size = 0;
    int32_t step = ReadVarNum(rp, record_size, &key_size);
    if (step < 1) {
      return Status(Status::BROKEN_DATA_ERROR, "invalid record key size");
    }
    rp += step;
    record_size -= step;
    if (record_size < static_cast<int32_t>(key_size)) {
      return Status(Status::BROKEN_DATA_ERROR, "too short record key");
    }
    const std::string_view rec_key(rp, key_size);
    rp += key_size;
    record_size -= key_size;
    uint64_t value_size = 0;
    step = ReadVarNum(rp, record_size, &value_size);
    if (step < 1) {
      return Status(Status::BROKEN_DATA_ERROR, "invalid record value size");
    }
    rp += step;
    record_size -= step;
    if (record_size < static_cast<int32_t>(value_size)) {
      return Status(Status::BROKEN_DATA_ERROR, "too short record value");
    }
    const std::string_view rec_value(rp, value_size);
    rp += value_size;
    record_size -= value_size;
    records->emplace_back(CreateTreeRecord(rec_key, rec_value));
  }
  return Status(Status::SUCCESS);
}

Status DeserializeInnerNode(
    std::string_view serialized, int64_t* heir_id, std::vector<TreeLink*>* links) {
  const char* rp = serialized.data();
  int32_t record_size = serialized.size();
  if (record_size < static_cast<int32_t>(PAGE_ID_WIDTH)) {
    return Status(Status::BROKEN_DATA_ERROR, "invalid inner metadata");
  }
  *heir_id = ReadFixNum(rp, PAGE_ID_WIDTH);
  rp += PAGE_ID_WIDTH;
  record_size -= PAGE_ID_WIDTH;
  while (record_size > 0) {
    uint64_t key_size = 0;
    int32_t step = ReadVarNum(rp, record_size, &key_size);
    if (step < 1) {
      return Status(Status::BROKEN_DATA_ERROR, "invalid link key size");
    }
    rp += step;
    record_size -= step;
    if (record_size < static_cast<int32_t>(key_size)) {
      return Status(Status::BROKEN_DATA_ERROR, "too short link key");
    }
    const std::string_view link_key(rp, key_size);
    rp += key_size;
    record_size -= key_size;
    if (record_size < static_cast<int32_t>(PAGE_ID_WIDTH)) {
      return Status(Status::BROKEN_DATA_ERROR, "too short link child ID");
    }
    const int64_t child_id = ReadFixNum(rp, PAGE_ID_WIDTH);
    rp += PAGE_ID_WIDTH;
    record_size -= PAGE_ID_WIDTH;
    links->emplace_back(CreateTreeLink(link_key, child_id));
  }
  return Status(Status::SUCCESS);
}

TreeDBMImpl::TreeDBMImpl(std::unique_ptr<File> file)
    : open_(false), writable_(false), healthy_(false), auto_restored_(false), path_(),
      num_records_(0), eff_data_size_(0),
      root_id_(0), first_id_(0), last_id_(0),
      num_leaf_nodes_(0), num_inner_nodes_(0), tree_level_(0),
      max_page_size_(TreeDBM::DEFAULT_MAX_PAGE_SIZE),
      max_branches_(TreeDBM::DEFAULT_MAX_BRANCHES),
      max_cached_pages_(TreeDBM::DEFAULT_MAX_CACHED_PAGES),
      page_update_mode_(TreeDBM::PAGE_UPDATE_DEFAULT),
      key_comparator_(nullptr), record_comp_(nullptr), link_comp_(nullptr),
      update_logger_(nullptr), mini_opaque_(), reorg_ids_(),
      hash_dbm_(new HashDBM(std::move(file))), proc_clock_(0),
      mutex_(), rebuild_mutex_() {}

TreeDBMImpl::~TreeDBMImpl() {
  if (open_) {
    Close();
  }
  for (auto* iterator : iterators_) {
    iterator->dbm_ = nullptr;
  }
}

Status TreeDBMImpl::Open(const std::string& path, bool writable,
                         int32_t options, const TreeDBM::TuningParameters& tuning_params) {
  std::lock_guard<SpinSharedMutex> lock(mutex_);
  if (open_) {
    return Status(Status::PRECONDITION_ERROR, "opened database");
  }
  const std::string norm_path = NormalizePath(path);
  HashDBM::TuningParameters hash_params = tuning_params;
  hash_params.offset_width = tuning_params.offset_width >= 0 ?
      tuning_params.offset_width : TreeDBM::DEFAULT_OFFSET_WIDTH;
  hash_params.align_pow = tuning_params.align_pow >= 0 ?
      tuning_params.align_pow : TreeDBM::DEFAULT_ALIGN_POW;
  hash_params.num_buckets = tuning_params.num_buckets >= 0 ?
      tuning_params.num_buckets : TreeDBM::DEFAULT_NUM_BUCKETS;
  hash_params.fbp_capacity = tuning_params.fbp_capacity >= 0 ?
      tuning_params.fbp_capacity : TreeDBM::DEFAULT_FBP_CAPACITY;
  hash_params.min_read_size = tuning_params.min_read_size;
  if (hash_params.min_read_size < 1) {
    hash_params.min_read_size = 1;
    auto* pos_file = dynamic_cast<const PositionalFile*>(hash_dbm_->GetInternalFile());
    if (pos_file != nullptr && pos_file->IsDirectIO()) {
      hash_params.min_read_size = AlignNumber(max_page_size_, 1 << hash_params.align_pow);
    }
  }
  if (tuning_params.max_page_size > 0) {
    max_page_size_ = tuning_params.max_page_size;
  }
  if (tuning_params.max_branches > 1) {
    max_branches_ = tuning_params.max_branches;
  }
  if (tuning_params.max_cached_pages > 0) {
    max_cached_pages_ = tuning_params.max_cached_pages;
  }
  if (tuning_params.page_update_mode == TreeDBM::PAGE_UPDATE_DEFAULT) {
    page_update_mode_ = TreeDBM::PAGE_UPDATE_NONE;
  } else {
    page_update_mode_ = tuning_params.page_update_mode;
  }
  if (tuning_params.key_comparator != nullptr) {
    key_comparator_ = tuning_params.key_comparator;
  }
  Status status = hash_dbm_->OpenAdvanced(norm_path, writable, options, hash_params);
  if (status != Status::SUCCESS) {
    return status;
  }
  auto_restored_ = false;
  if (writable && hash_dbm_->IsAutoRestored() &&
      tuning_params.restore_mode != HashDBM::RESTORE_READ_ONLY &&
      tuning_params.restore_mode != HashDBM::RESTORE_NOOP) {
    hash_dbm_->Close();
    const std::string tmp_path = norm_path + ".tmp.restore";
    const int64_t end_offset =
        tuning_params.restore_mode == HashDBM::RESTORE_SYNC ? INT64MAX : INT64MIN;
    RemoveFile(tmp_path);
    status = TreeDBM::RestoreDatabase(norm_path, tmp_path, end_offset, tuning_params.cipher_key);
    if (status != Status::SUCCESS) {
      RemoveFile(tmp_path);
      return status;
    }
    status = RenameFile(tmp_path, norm_path);
    if (status != Status::SUCCESS) {
      RemoveFile(tmp_path);
      return status;
    }
    status = hash_dbm_->OpenAdvanced(norm_path, true, options, hash_params);
    if (status != Status::SUCCESS) {
      return status;
    }
    auto_restored_ = true;
  }
  if (writable && hash_dbm_->CountSimple() == 0 &&
      hash_dbm_->GetOpaqueMetadata() == std::string(HashDBM::OPAQUE_METADATA_SIZE, 0)) {
    num_records_.store(0);
    eff_data_size_.store(0);
    root_id_ = 0;
    first_id_ = 0;
    last_id_ = 0;
    num_leaf_nodes_ = 0;
    num_inner_nodes_ = 0;
    InitializePageCache();
    auto leaf_node = (new TreeLeafNode(this, 0, 0))->AddToCache();
    root_id_ = leaf_node->id;
    first_id_ = leaf_node->id;
    last_id_ = leaf_node->id;
    tree_level_ = 1;
    status = FlushLeafCacheAll(false);
    if (status != Status::SUCCESS) {
      hash_dbm_->Close();
      return status;
    }
    status = SaveMetadata();
    if (status != Status::SUCCESS) {
      hash_dbm_->Close();
      return status;
    }
    status = hash_dbm_->Synchronize(false);
    if (status != Status::SUCCESS) {
      hash_dbm_->Close();
      return status;
    }
    status = LoadMetadata();
    if (status != Status::SUCCESS) {
      hash_dbm_->Close();
      return status;
    }
  } else {
    InitializePageCache();
    status = LoadMetadata();
    if (status != Status::SUCCESS) {
      hash_dbm_->Close();
      return status;
    }
  }
  open_ = true;
  writable_ = writable;
  healthy_ = hash_dbm_->IsHealthy();
  path_ = norm_path;
  return Status(Status::SUCCESS);
}

Status TreeDBMImpl::Close() {
  std::lock_guard<SpinSharedMutex> lock(mutex_);
  if (!open_) {
    return Status(Status::PRECONDITION_ERROR, "not opened database");
  }
  for (auto* iterator : iterators_) {
    iterator->ClearPosition();
  }
  Status status(Status::SUCCESS);
  if (!reorg_ids_.IsEmpty()) {
    status |= ReorganizeTree();
  }
  status |= FlushLeafCacheAll(true);
  status |= FlushInnerCacheAll(true);
  if (writable_) {
    status |= SaveMetadata();
  }
  status |= hash_dbm_->Close();
  open_ = false;
  writable_ = false;
  healthy_ = false;
  auto_restored_ = false;
  path_.clear();
  num_records_.store(0);
  eff_data_size_.store(0);
  root_id_ = 0;
  first_id_ = 0;
  last_id_ = 0;
  num_leaf_nodes_ = 0;
  num_inner_nodes_ = 0;
  tree_level_ = 0;
  max_page_size_ = TreeDBM::DEFAULT_MAX_PAGE_SIZE;
  max_branches_ = TreeDBM::DEFAULT_MAX_BRANCHES;
  max_cached_pages_ = TreeDBM::DEFAULT_MAX_CACHED_PAGES;
  record_comp_ = TreeRecordComparator(LexicalKeyComparator);
  link_comp_ = TreeLinkComparator(LexicalKeyComparator);
  mini_opaque_.clear();
  reorg_ids_.Clear();
  proc_clock_.store(0);
  return status;
}

Status TreeDBMImpl::Process(
    std::string_view key, DBM::RecordProcessor* proc, bool writable) {
  if (writable && !reorg_ids_.IsEmpty()) {
    std::lock_guard<SpinSharedMutex> lock(mutex_);
    const Status status = ReorganizeTree();
    if (status != Status::SUCCESS) {
      return status;
    }
  }
  std::shared_lock<SpinSharedMutex> lock(mutex_);
  if (!open_) {
    return Status(Status::PRECONDITION_ERROR, "not opened database");
  }
  if (writable) {
    if (!writable_) {
      return Status(Status::PRECONDITION_ERROR, "not writable database");
    }
    if (!healthy_) {
      return Status(Status::PRECONDITION_ERROR, "not healthy database");
    }
  }
  std::shared_ptr<TreeLeafNode> leaf_node;
  Status status = SearchTree(key, &leaf_node);
  if (status != Status::SUCCESS) {
    return status;
  }
  if (writable) {
    std::lock_guard<SpinSharedMutex> leaf_lock(leaf_node->mutex);
    ProcessImpl(leaf_node.get(), key, proc, true);
    if (leaf_node->dirty && page_update_mode_ == TreeDBM::PAGE_UPDATE_WRITE) {
      status = SaveLeafNodeImpl(leaf_node.get());
      if (status != Status::SUCCESS) {
        return status;
      }
    }
  } else {
    std::shared_lock<SpinSharedMutex> leaf_lock(leaf_node->mutex);
    ProcessImpl(leaf_node.get(), key, proc, false);
  }
  return AdjustCaches();
}

Status TreeDBMImpl::ProcessMulti(
      const std::vector<std::pair<std::string_view, DBM::RecordProcessor*>>& key_proc_pairs,
      bool writable) {
  if (writable && !reorg_ids_.IsEmpty()) {
    std::lock_guard<SpinSharedMutex> lock(mutex_);
    const Status status = ReorganizeTree();
    if (status != Status::SUCCESS) {
      return status;
    }
  }
  std::shared_lock<SpinSharedMutex> lock(mutex_);
  if (!open_) {
    return Status(Status::PRECONDITION_ERROR, "not opened database");
  }
  if (writable) {
    if (!writable_) {
      return Status(Status::PRECONDITION_ERROR, "not writable database");
    }
    if (!healthy_) {
      return Status(Status::PRECONDITION_ERROR, "not healthy database");
    }
  }
  std::vector<std::shared_ptr<TreeLeafNode>> leaf_nodes;
  std::map<int64_t, std::shared_ptr<TreeLeafNode>> id_leaf_nodes;
  for (const auto& key_proc : key_proc_pairs) {
    std::shared_ptr<TreeLeafNode> leaf_node;
    const Status status = SearchTree(key_proc.first, &leaf_node);
    if (status != Status::SUCCESS) {
      return status;
    }
    leaf_nodes.emplace_back(leaf_node);
    id_leaf_nodes.emplace(std::make_pair(leaf_node->id, leaf_node));
  }
  for (const auto& id_leaf_node : id_leaf_nodes) {
    if (writable) {
      id_leaf_node.second->mutex.lock();
    } else {
      id_leaf_node.second->mutex.lock_shared();
    }
  }
  for (size_t i = 0; i < key_proc_pairs.size(); i++) {
    auto& key_proc = key_proc_pairs[i];
    auto& leaf_node = leaf_nodes[i];
    ProcessImpl(leaf_node.get(), key_proc.first, key_proc.second, writable);
    if (leaf_node->dirty && page_update_mode_ == TreeDBM::PAGE_UPDATE_WRITE) {
      const Status status = SaveLeafNodeImpl(leaf_node.get());
      if (status != Status::SUCCESS) {
        return status;
      }
    }
  }
  for (auto id_leaf_node = id_leaf_nodes.rbegin();
       id_leaf_node != id_leaf_nodes.rend(); id_leaf_node++) {
    if (writable) {
      id_leaf_node->second->mutex.unlock();
    } else {
      id_leaf_node->second->mutex.unlock_shared();
    }
  }
  return AdjustCaches();
}

Status TreeDBMImpl::ProcessFirst(DBM::RecordProcessor* proc, bool writable) {
  if (writable && !reorg_ids_.IsEmpty()) {
    std::lock_guard<SpinSharedMutex> lock(mutex_);
    const Status status = ReorganizeTree();
    if (status != Status::SUCCESS) {
      return status;
    }
  }
  std::shared_lock<SpinSharedMutex> lock(mutex_);
  if (!open_) {
    return Status(Status::PRECONDITION_ERROR, "not opened database");
  }
  if (writable) {
    if (!writable_) {
      return Status(Status::PRECONDITION_ERROR, "not writable database");
    }
    if (!healthy_) {
      return Status(Status::PRECONDITION_ERROR, "not healthy database");
    }
  }
  int64_t leaf_id = first_id_;
  while (leaf_id > 0) {
    std::shared_ptr<TreeLeafNode> node;
    Status status = LoadLeafNode(leaf_id, false, &node);
    if (status != Status::SUCCESS) {
      return status;
    }
    if (writable) {
      std::lock_guard<SpinSharedMutex> leaf_lock(node->mutex);
      if (!node->records.empty()) {
        const std::string key(node->records.front()->GetKey());
        ProcessImpl(node.get(), key, proc, true);
        if (node->dirty && page_update_mode_ == TreeDBM::PAGE_UPDATE_WRITE) {
          status = SaveLeafNodeImpl(node.get());
          if (status != Status::SUCCESS) {
            return status;
          }
        }
        return Status(Status::SUCCESS);
      }
    } else {
      std::shared_lock<SpinSharedMutex> leaf_lock(node->mutex);
      if (!node->records.empty()) {
        const std::string key(node->records.front()->GetKey());
        ProcessImpl(node.get(), key, proc, false);
        return Status(Status::SUCCESS);
      }
    }
    leaf_id = node->next_id;
    status = AdjustCaches();
    if (status != Status::SUCCESS) {
      return status;
    }
  }
  return Status(Status::NOT_FOUND_ERROR);
}

Status TreeDBMImpl::ProcessEach(DBM::RecordProcessor* proc, bool writable) {
  if (writable && !reorg_ids_.IsEmpty()) {
    std::lock_guard<SpinSharedMutex> lock(mutex_);
    const Status status = ReorganizeTree();
    if (status != Status::SUCCESS) {
      return status;
    }
  }
  std::shared_lock<SpinSharedMutex> lock(mutex_);
  if (!open_) {
    return Status(Status::PRECONDITION_ERROR, "not opened database");
  }
  if (writable) {
    if (!writable_) {
      return Status(Status::PRECONDITION_ERROR, "not writable database");
    }
    if (!healthy_) {
      return Status(Status::PRECONDITION_ERROR, "not healthy database");
    }
  }
  proc->ProcessEmpty(DBM::RecordProcessor::NOOP);
  int64_t leaf_id = first_id_;
  while (leaf_id > 0) {
    std::shared_ptr<TreeLeafNode> node;
    Status status = LoadLeafNode(leaf_id, false, &node);
    if (status != Status::SUCCESS) {
      return status;
    }
    if (writable) {
      std::lock_guard<SpinSharedMutex> leaf_lock(node->mutex);
      std::vector<std::string> keys;
      keys.reserve(node->records.size());
      for (const auto* rec : node->records) {
        keys.emplace_back(std::string(rec->GetKey()));
      }
      for (const auto& key : keys) {
        ProcessImpl(node.get(), key, proc, true);
      }
      if (node->dirty && page_update_mode_ == TreeDBM::PAGE_UPDATE_WRITE) {
        status = SaveLeafNodeImpl(node.get());
        if (status != Status::SUCCESS) {
          return status;
        }
      }
    } else {
      std::shared_lock<SpinSharedMutex> leaf_lock(node->mutex);
      for (const auto* rec : node->records) {
        proc->ProcessFull(rec->GetKey(), rec->GetValue());
      }
    }
    leaf_id = node->next_id;
    status = AdjustCaches();
    if (status != Status::SUCCESS) {
      return status;
    }
  }
  proc->ProcessEmpty(DBM::RecordProcessor::NOOP);
  return Status(Status::SUCCESS);
}

Status TreeDBMImpl::Count(int64_t* count) {
  std::shared_lock<SpinSharedMutex> lock(mutex_);
  if (!open_) {
    return Status(Status::PRECONDITION_ERROR, "not opened database");
  }
  *count = num_records_.load();
  return Status(Status::SUCCESS);
}

Status TreeDBMImpl::GetFileSize(int64_t* size) {
  std::shared_lock<SpinSharedMutex> lock(mutex_);
  if (!open_) {
    return Status(Status::PRECONDITION_ERROR, "not opened database");
  }
  return hash_dbm_->GetFileSize(size);
}

Status TreeDBMImpl::GetFilePath(std::string* path) {
  std::shared_lock<SpinSharedMutex> lock(mutex_);
  if (!open_) {
    return Status(Status::PRECONDITION_ERROR, "not opened database");
  }
  *path = path_;
  return Status(Status::SUCCESS);
}

Status TreeDBMImpl::GetTimestamp(double* timestamp) {
  std::shared_lock<SpinSharedMutex> lock(mutex_);
  if (!open_) {
    return Status(Status::PRECONDITION_ERROR, "not opened database");
  }
  return hash_dbm_->GetTimestamp(timestamp);
}

Status TreeDBMImpl::Clear() {
  std::lock_guard<SpinSharedMutex> lock(mutex_);
  if (!open_) {
    return Status(Status::PRECONDITION_ERROR, "not opened database");
  }
  if (!writable_) {
    return Status(Status::PRECONDITION_ERROR, "not writable database");
  }
  if (update_logger_ != nullptr) {
    const Status status = update_logger_->WriteClear();
    if (status != Status::SUCCESS) {
      return status;
    }
  }
  for (auto* iterator : iterators_) {
    iterator->ClearPosition();
  }
  reorg_ids_.Clear();
  DiscardLeafCache();
  DiscardInnerCache();
  Status status = hash_dbm_->Clear();
  num_records_.store(0);
  eff_data_size_.store(0);
  root_id_ = 0;
  first_id_ = 0;
  last_id_ = 0;
  num_leaf_nodes_ = 0;
  num_inner_nodes_ = 0;
  InitializePageCache();
  auto leaf_node = (new TreeLeafNode(this, 0, 0))->AddToCache();
  root_id_ = leaf_node->id;
  first_id_ = leaf_node->id;
  last_id_ = leaf_node->id;
  tree_level_ = 1;
  status |= SaveMetadata();
  return status;
}

Status TreeDBMImpl::Rebuild(
    const TreeDBM::TuningParameters& tuning_params, bool skip_broken_records, bool sync_hard) {
  if (!reorg_ids_.IsEmpty()) {
    std::lock_guard<SpinSharedMutex> lock(mutex_);
    const Status status = ReorganizeTree();
    if (status != Status::SUCCESS) {
      return status;
    }
  }
  for (int32_t slot_index = NUM_PAGE_SLOTS - 1; slot_index >= 0; slot_index--) {
    std::lock_guard<SpinSharedMutex> lock(mutex_);
    if (!open_) {
      return Status(Status::PRECONDITION_ERROR, "not opened database");
    }
    if (!writable_) {
      return Status(Status::PRECONDITION_ERROR, "not writable database");
    }
    if (!healthy_) {
      return Status(Status::PRECONDITION_ERROR, "not healthy database");
    }
    const Status status = FlushLeafCacheOne(false, slot_index);
    if (status != Status::SUCCESS) {
      return status;
    }
  }
  for (int32_t slot_index = NUM_PAGE_SLOTS - 1; slot_index >= 0; slot_index--) {
    std::lock_guard<SpinSharedMutex> lock(mutex_);
    const Status status = FlushInnerCacheOne(false, slot_index);
    if (status != Status::SUCCESS) {
      return status;
    }
  }
  Status status(Status::SUCCESS);
  {
    std::lock_guard<SpinSharedMutex> lock(mutex_);
    if (rebuild_mutex_.try_lock()) {
      rebuild_mutex_.unlock();
    } else {
      return Status(Status::SUCCESS);
    }
    status |= FlushLeafCacheAll(false);
    status |= FlushInnerCacheAll(false);
    status |= SaveMetadata();
  }
  if (status != Status::SUCCESS) {
    return status;
  }
  std::lock_guard<SpinMutex> rebulid_lock(rebuild_mutex_);
  const HashDBM::TuningParameters hash_params = tuning_params;
  if (tuning_params.max_page_size > 0) {
    max_page_size_ = tuning_params.max_page_size;
  }
  if (tuning_params.max_branches > 1) {
    max_branches_ = tuning_params.max_branches;
  }
  if (tuning_params.max_cached_pages > 0) {
    max_cached_pages_ = tuning_params.max_cached_pages;
  }
  if (tuning_params.page_update_mode != TreeDBM::PAGE_UPDATE_DEFAULT) {
    page_update_mode_ = tuning_params.page_update_mode;
  }
  status = hash_dbm_->RebuildAdvanced(hash_params, skip_broken_records, sync_hard);
  return status;
}

Status TreeDBMImpl::ShouldBeRebuilt(bool* tobe) {
  std::shared_lock<SpinSharedMutex> lock(mutex_);
  if (!open_) {
    return Status(Status::PRECONDITION_ERROR, "not opened database");
  }
  return hash_dbm_->ShouldBeRebuilt(tobe);
}

Status TreeDBMImpl::Synchronize(bool hard, DBM::FileProcessor* proc) {
  for (int32_t slot_index = NUM_PAGE_SLOTS - 1; slot_index >= 0; slot_index--) {
    std::lock_guard<SpinSharedMutex> lock(mutex_);
    if (!open_) {
      return Status(Status::PRECONDITION_ERROR, "not opened database");
    }
    if (!writable_) {
      return Status(Status::PRECONDITION_ERROR, "not writable database");
    }
    if (!healthy_) {
      return Status(Status::PRECONDITION_ERROR, "not healthy database");
    }
    const Status status = FlushLeafCacheOne(false, slot_index);
    if (status != Status::SUCCESS) {
      return status;
    }
  }
  for (int32_t slot_index = NUM_PAGE_SLOTS - 1; slot_index >= 0; slot_index--) {
    std::lock_guard<SpinSharedMutex> lock(mutex_);
    const Status status = FlushInnerCacheOne(false, slot_index);
    if (status != Status::SUCCESS) {
      return status;
    }
  }
  std::lock_guard<SpinSharedMutex> lock(mutex_);
  Status status(Status::SUCCESS);
  if (update_logger_ != nullptr) {
    status |= update_logger_->Synchronize(hard);
  }
  if (!reorg_ids_.IsEmpty()) {
    status |= ReorganizeTree();
  }
  status |= FlushLeafCacheAll(false);
  status |= FlushInnerCacheAll(false);
  status |= SaveMetadata();
  status |= hash_dbm_->Synchronize(hard, proc);
  return status;
}

std::vector<std::pair<std::string, std::string>> TreeDBMImpl::Inspect() {
  std::shared_lock<SpinSharedMutex> lock(mutex_);
  std::vector<std::pair<std::string, std::string>> meta;
  auto Add = [&](const std::string& name, const std::string& value) {
    meta.emplace_back(std::make_pair(name, value));
  };
  Add("class", "TreeDBM");
  if (open_) {
    Add("healthy", ToString(healthy_));
    Add("auto_restored", ToString(auto_restored_));
    Add("path", path_);
    Add("num_records", ToString(num_records_.load()));
    Add("eff_data_size", ToString(eff_data_size_.load()));
    Add("root_id", ToString(root_id_));
    Add("first_id", ToString(first_id_));
    Add("last_id", ToString(last_id_));
    Add("num_leaf_nodes", ToString(num_leaf_nodes_));
    Add("num_inner_nodes", ToString(num_inner_nodes_));
    Add("tree_level", ToString(tree_level_));
    Add("max_page_size", ToString(max_page_size_));
    Add("max_branches", ToString(max_branches_));
    Add("max_cached_pages", ToString(max_cached_pages_));
    std::string comp_name;
    if (key_comparator_ == LexicalKeyComparator) {
      comp_name = "LexicalKeyComparator";
    } else if (key_comparator_ == LexicalCaseKeyComparator) {
      comp_name = "LexicalCaseKeyComparator";
    } else if (key_comparator_ == DecimalKeyComparator) {
      comp_name = "DecimalKeyComparator";
    } else if (key_comparator_ == HexadecimalKeyComparator) {
      comp_name = "HexadecimalKeyComparator";
    } else if (key_comparator_ == RealNumberKeyComparator) {
      comp_name = "RealNumberKeyComparator";
    } else if (key_comparator_ == PairLexicalKeyComparator) {
      comp_name = "PairLexicalKeyComparator";
    } else if (key_comparator_ == PairLexicalCaseKeyComparator) {
      comp_name = "PairLexicalCaseKeyComparator";
    } else if (key_comparator_ == PairDecimalKeyComparator) {
      comp_name = "PairDecimalKeyComparator";
    } else if (key_comparator_ == PairHexadecimalKeyComparator) {
      comp_name = "PairHexadecimalKeyComparator";
    } else if (key_comparator_ == PairRealNumberKeyComparator) {
      comp_name = "PairRealNumberKeyComparator";
    } else {
      comp_name = "custom";
    }
    Add("key_comparator", ToString(comp_name));
    const std::map<std::string, std::string> name_map = {
      {"class", ""}, {"path", ""},
      {"num_buckets", "hash_num_buckets"},
      {"num_records", "hash_num_records"},
      {"eff_data_size", "hash_eff_data_size"},
    };
    for (const auto& rec : hash_dbm_->Inspect()) {
      std::string name = rec.first;
      const auto it = name_map.find(name);
      if (it != name_map.end()) {
        name = it->second;
      }
      if (!name.empty()) {
        Add(name, rec.second);
      }
    }
  }
  return meta;
}

bool TreeDBMImpl::IsOpen() {
  std::shared_lock<SpinSharedMutex> lock(mutex_);
  return open_;
}

bool TreeDBMImpl::IsWritable() {
  std::shared_lock<SpinSharedMutex> lock(mutex_);
  return open_ && writable_;
}

bool TreeDBMImpl::IsHealthy() {
  std::shared_lock<SpinSharedMutex> lock(mutex_);
  return open_ && healthy_;
}

bool TreeDBMImpl::IsAutoRestored() {
  std::shared_lock<SpinSharedMutex> lock(mutex_);
  return open_ && auto_restored_;
}

std::unique_ptr<DBM> TreeDBMImpl::MakeDBM() {
  std::shared_lock<SpinSharedMutex> lock(mutex_);
  return std::make_unique<TreeDBM>(hash_dbm_->GetInternalFile()->MakeFile());
}

DBM::UpdateLogger* TreeDBMImpl::GetUpdateLogger() {
  std::shared_lock<SpinSharedMutex> lock(mutex_);
  return update_logger_;
}

void TreeDBMImpl::SetUpdateLogger(DBM::UpdateLogger* update_logger) {
  std::lock_guard<SpinSharedMutex> lock(mutex_);
  update_logger_ = update_logger;
}

File* TreeDBMImpl::GetInternalFile() {
  return hash_dbm_->GetInternalFile();
}

int64_t TreeDBMImpl::GetEffectiveDataSize() {
  std::shared_lock<SpinSharedMutex> lock(mutex_);
  if (!open_) {
    return -1;
  }
  return eff_data_size_.load();
}

int32_t TreeDBMImpl::GetDatabaseType() {
  std::shared_lock<SpinSharedMutex> lock(mutex_);
  if (!open_) {
    return -1;
  }
  return hash_dbm_->GetDatabaseType();
}

Status TreeDBMImpl::SetDatabaseType(uint32_t db_type) {
  std::lock_guard<SpinSharedMutex> lock(mutex_);
  if (!open_) {
    return Status(Status::PRECONDITION_ERROR, "not opened database");
  }
  if (!writable_) {
    return Status(Status::PRECONDITION_ERROR, "not writable database");
  }
  return hash_dbm_->SetDatabaseType(db_type);
}

std::string TreeDBMImpl::GetOpaqueMetadata() {
  std::shared_lock<SpinSharedMutex> lock(mutex_);
  if (!open_) {
    return "";
  }
  return mini_opaque_;
}

Status TreeDBMImpl::SetOpaqueMetadata(const std::string& opaque) {
  std::lock_guard<SpinSharedMutex> lock(mutex_);
  if (!open_) {
    return Status(Status::PRECONDITION_ERROR, "not opened database");
  }
  if (!writable_) {
    return Status(Status::PRECONDITION_ERROR, "not writable database");
  }
  mini_opaque_ = opaque;
  return Status(Status::SUCCESS);
}

KeyComparator TreeDBMImpl::GetKeyComparator() {
  std::shared_lock<SpinSharedMutex> lock(mutex_);
  if (!open_) {
    return nullptr;
  }
  return key_comparator_;
}

Status TreeDBMImpl::ValidateHashBuckets() {
  std::lock_guard<SpinSharedMutex> lock(mutex_);
  if (!open_) {
    return Status(Status::PRECONDITION_ERROR, "not opened database");
  }
  return hash_dbm_->ValidateHashBuckets();
}

Status TreeDBMImpl::ValidateRecords(int64_t record_base, int64_t end_offset) {
  std::lock_guard<SpinSharedMutex> lock(mutex_);
  if (!open_) {
    return Status(Status::PRECONDITION_ERROR, "not opened database");
  }
  return hash_dbm_->ValidateRecords(record_base, end_offset);
}

Status TreeDBMImpl::SaveMetadata() {
  std::string opaque(HashDBM::OPAQUE_METADATA_SIZE, 0);
  char* wp = const_cast<char*>(opaque.data());
  std::memcpy(wp, META_MAGIC_DATA, sizeof(META_MAGIC_DATA));
  WriteFixNum(wp + META_OFFSET_NUM_RECORDS, num_records_.load(), 6);
  WriteFixNum(wp + META_OFFSET_EFF_DATA_SIZE, eff_data_size_.load(), 6);
  WriteFixNum(wp + META_OFFSET_ROOT_ID, root_id_, 6);
  WriteFixNum(wp + META_OFFSET_FIRST_ID, first_id_, 6);
  WriteFixNum(wp + META_OFFSET_LAST_ID, last_id_, 6);
  WriteFixNum(wp + META_OFFSET_NUM_LEAF_NODES, num_leaf_nodes_, 6);
  WriteFixNum(wp + META_OFFSET_NUM_INNER_NODES, num_inner_nodes_, 6);
  WriteFixNum(wp + META_OFFSET_MAX_PAGE_SIZE, max_page_size_, 3);
  WriteFixNum(wp + META_OFFSET_MAX_BRANCHES, max_branches_, 3);
  WriteFixNum(wp + META_OFFSET_TREE_LEVEL, tree_level_, 1);
  uint32_t key_comp_type = 0;
  if (key_comparator_ == nullptr || key_comparator_ == LexicalKeyComparator) {
    key_comp_type = 1;
  } else if (key_comparator_ == LexicalCaseKeyComparator) {
    key_comp_type = 2;
  } else if (key_comparator_ == DecimalKeyComparator) {
    key_comp_type = 3;
  } else if (key_comparator_ == HexadecimalKeyComparator) {
    key_comp_type = 4;
  } else if (key_comparator_ == RealNumberKeyComparator) {
    key_comp_type = 5;
  } else if (key_comparator_ == PairLexicalKeyComparator) {
    key_comp_type = 101;
  } else if (key_comparator_ == PairLexicalCaseKeyComparator) {
    key_comp_type = 102;
  } else if (key_comparator_ == PairDecimalKeyComparator) {
    key_comp_type = 103;
  } else if (key_comparator_ == PairHexadecimalKeyComparator) {
    key_comp_type = 104;
  } else if (key_comparator_ == PairRealNumberKeyComparator) {
    key_comp_type = 105;
  } else {
    key_comp_type = 255;
  }
  WriteFixNum(wp + META_OFFSET_KEY_COMPARATOR, key_comp_type, 1);
  const int32_t opaque_size =
      std::min<int32_t>(mini_opaque_.size(), HashDBM::OPAQUE_METADATA_SIZE - META_OFFSET_OPAQUE);
  std::memcpy(wp + META_OFFSET_OPAQUE, mini_opaque_.data(), opaque_size);
  return hash_dbm_->SetOpaqueMetadata(opaque);
}

Status TreeDBMImpl::LoadMetadata() {
  const std::string opaque = hash_dbm_->GetOpaqueMetadata();
  int64_t num_records = 0;
  int64_t eff_data_size = 0;
  int32_t key_comp_type = 0;
  const Status status = TreeDBM::ParseMetadata(
      opaque, &num_records, &eff_data_size,
      &root_id_, &first_id_, &last_id_,
      &num_leaf_nodes_, &num_inner_nodes_,
      &max_page_size_, &max_branches_,
      &tree_level_, &key_comp_type, &mini_opaque_);
  if (status != Status::SUCCESS) {
    return status;
  }
  num_records_.store(num_records);
  eff_data_size_.store(eff_data_size);
  if (num_records_.load() < 0) {
    return Status(Status::BROKEN_DATA_ERROR, "invalid record count");
  }
  if (eff_data_size_.load() < 0) {
    return Status(Status::BROKEN_DATA_ERROR, "invalid effective data size");
  }
  if (root_id_ < 0) {
    return Status(Status::BROKEN_DATA_ERROR, "invalid root node ID");
  }
  if (first_id_ < 0) {
    return Status(Status::BROKEN_DATA_ERROR, "invalid first node ID");
  }
  if (last_id_ < 0) {
    return Status(Status::BROKEN_DATA_ERROR, "invalid last node ID");
  }
  if (num_leaf_nodes_ < 1) {
    return Status(Status::BROKEN_DATA_ERROR, "invalid number of leaf nodes");
  }
  if (num_inner_nodes_ < 0) {
    return Status(Status::BROKEN_DATA_ERROR, "invalid number of inner nodes");
  }
  if (max_page_size_ < 1) {
    return Status(Status::BROKEN_DATA_ERROR, "invalid maximum page size");
  }
  if (max_branches_ < 2) {
    return Status(Status::BROKEN_DATA_ERROR, "invalid maximum branches");
  }
  if (tree_level_ < 1) {
    return Status(Status::BROKEN_DATA_ERROR, "invalid tree level");
  }
  switch (key_comp_type) {
    case 1: key_comparator_ = LexicalKeyComparator; break;
    case 2: key_comparator_ = LexicalCaseKeyComparator; break;
    case 3: key_comparator_ = DecimalKeyComparator; break;
    case 4: key_comparator_ = HexadecimalKeyComparator; break;
    case 5: key_comparator_ = RealNumberKeyComparator; break;
    case 101: key_comparator_ = PairLexicalKeyComparator; break;
    case 102: key_comparator_ = PairLexicalCaseKeyComparator; break;
    case 103: key_comparator_ = PairDecimalKeyComparator; break;
    case 104: key_comparator_ = PairHexadecimalKeyComparator; break;
    case 105: key_comparator_ = PairRealNumberKeyComparator; break;
    case 255:
      if (key_comparator_ == nullptr) {
        return Status(Status::BROKEN_DATA_ERROR, "invalid_key_comparator");
      }
      break;
    default:
      return Status(Status::BROKEN_DATA_ERROR, "invalid_key_comparator");
  }
  record_comp_ = TreeRecordComparator(key_comparator_);
  link_comp_ = TreeLinkComparator(key_comparator_);
  return Status(Status::SUCCESS);
}

void TreeDBMImpl::InitializePageCache() {
  const int32_t num_leaf_pages = max_cached_pages_ * (1 - INNER_PAGE_CACHE_RATIO);
  for (int32_t i = 0; i < NUM_PAGE_SLOTS; i++) {
    const int32_t num_pages = num_leaf_pages / NUM_PAGE_SLOTS;
    const int32_t hot_capacity = std::max(num_pages * HOT_CACHE_RATIO, 1.0);
    const int32_t warm_capacity = std::max(num_pages * (1 - HOT_CACHE_RATIO), 1.0);
    leaf_slots_[i].cache = std::make_unique<LeafCache>(hot_capacity, warm_capacity);
  }
  const int32_t num_inner_pages = max_cached_pages_ * INNER_PAGE_CACHE_RATIO;
  for (int32_t i = 0; i < NUM_PAGE_SLOTS; i++) {
    const int32_t capacity = std::max(num_inner_pages / NUM_PAGE_SLOTS, 1);
    inner_slots_[i].cache = std::make_unique<InnerCache>(capacity);
  }
}

Status TreeDBMImpl::LoadLeafNode(
    int64_t id, bool promotion, std::shared_ptr<TreeLeafNode>* node) {
  int32_t slot_index = id % NUM_PAGE_SLOTS;
  LeafSlot* slot = leaf_slots_ + slot_index;
  {
    std::lock_guard<SpinMutex> lock(slot->mutex);
    *node = slot->cache->Get(id, promotion);
    if (*node != nullptr) {
      return Status(Status::SUCCESS);
    }
  }
  char node_key_buf[PAGE_ID_WIDTH];
  WriteFixNum(node_key_buf, id, PAGE_ID_WIDTH);
  const std::string_view node_key(node_key_buf, sizeof(node_key_buf));
  Status load_status(Status::SUCCESS);
  int64_t prev_id = 0;
  int64_t next_id = 0;
  std::vector<TreeRecord*> records;
  int32_t page_size = 0;
  class Loader final : public DBM::RecordProcessor {
   public:
    Loader(Status* status, int64_t* prev_id, int64_t* next_id,
           std::vector<TreeRecord*>* records, int32_t* page_size)
        : status_(status), prev_id_(prev_id), next_id_(next_id),
          records_(records), page_size_(page_size) {}
    std::string_view ProcessFull(std::string_view key, std::string_view value) override {
      *status_ = DeserializeLeafNode(value, prev_id_, next_id_, records_);
      *page_size_ = value.size();
      return NOOP;
    }
    std::string_view ProcessEmpty(std::string_view key) override {
      status_->Set(Status::BROKEN_DATA_ERROR, "invalid leaf node ID");
      return NOOP;
    }
   private:
    Status* status_;
    int64_t* prev_id_;
    int64_t* next_id_;
    std::vector<TreeRecord*>* records_;
    int32_t* page_size_;
  } loader(&load_status, &prev_id, &next_id, &records, &page_size);
  const Status status = hash_dbm_->Process(node_key, &loader, false);
  if (status != Status::SUCCESS) {
    return status;
  }
  if (load_status != Status::SUCCESS) {
    FreeTreeRecords(&records);
    return load_status;
  }
  *node = (new TreeLeafNode(this, id, prev_id, next_id,
                            std::move(records), page_size))->AddToCache();
  return Status(Status::SUCCESS);
}

Status TreeDBMImpl::SaveLeafNode(TreeLeafNode* node) {
  std::lock_guard<SpinSharedMutex> lock(node->mutex);
  if (!node->dirty) {
    return Status(Status::SUCCESS);
  }
  return SaveLeafNodeImpl(node);
}

Status TreeDBMImpl::SaveLeafNodeImpl(TreeLeafNode* node) {
  char stack[WRITE_BUFFER_SIZE];
  char* write_buf = node->page_size > WRITE_BUFFER_SIZE ? new char[node->page_size] : stack;
  char* wp = write_buf;
  WriteFixNum(wp, node->prev_id, PAGE_ID_WIDTH);
  wp += PAGE_ID_WIDTH;
  WriteFixNum(wp, node->next_id, PAGE_ID_WIDTH);
  wp += PAGE_ID_WIDTH;
  for (const auto* rec : node->records) {
    const std::string_view key = rec->GetKey();
    wp += WriteVarNum(wp, key.size());
    std::memcpy(wp, key.data(), key.size());
    wp += key.size();
    const std::string_view value = rec->GetValue();
    wp += WriteVarNum(wp, value.size());
    std::memcpy(wp, value.data(), value.size());
    wp += value.size();
  }
  char node_key_buf[PAGE_ID_WIDTH];
  WriteFixNum(node_key_buf, node->id, PAGE_ID_WIDTH);
  const std::string_view node_key(node_key_buf, sizeof(node_key_buf));
  const Status status = hash_dbm_->Set(node_key, std::string_view(write_buf, node->page_size));
  if (write_buf != stack) {
    delete[] write_buf;
  }
  node->dirty = false;
  node->on_disk = true;
  return status;
}

Status TreeDBMImpl::RemoveLeafNode(TreeLeafNode* node) {
  Status status(Status::SUCCESS);
  if (node->on_disk) {
    char node_key_buf[PAGE_ID_WIDTH];
    WriteFixNum(node_key_buf, node->id, PAGE_ID_WIDTH);
    const std::string_view node_key(node_key_buf, sizeof(node_key_buf));
    status |= hash_dbm_->Remove(node_key);
  }
  int32_t slot_index = node->id % NUM_PAGE_SLOTS;
  LeafSlot* slot = leaf_slots_ + slot_index;
  node->dirty = false;
  slot->cache->Remove(node->id);
  return status;
}

Status TreeDBMImpl::FlushLeafCacheAll(bool empty) {
  Status status(Status::SUCCESS);
  for (int32_t slot_index = NUM_PAGE_SLOTS - 1; slot_index >= 0; slot_index--) {
    status |= FlushLeafCacheOne(empty, slot_index);
  }
  return status;
}

Status TreeDBMImpl::FlushLeafCacheOne(bool empty, int32_t slot_index) {
  Status status(Status::SUCCESS);
  LeafSlot* slot = leaf_slots_ + slot_index;
  std::lock_guard<SpinMutex> lock(slot->mutex);
  std::vector<std::shared_ptr<TreeLeafNode>> deferred;
  std::shared_ptr<TreeLeafNode> node;
  while ((node = slot->cache->RemoveLRU()) != nullptr) {
    if (node.use_count() > 1) {
      if (empty) {
        status |= SaveLeafNode(node.get());
        status |= Status(Status::UNKNOWN_ERROR, "unexpected reference");
      } else {
        deferred.emplace_back(node);
      }
    } else {
      status |= SaveLeafNode(node.get());
    }
  }
  while (!deferred.empty()) {
    auto node = deferred.back();
    deferred.pop_back();
    std::this_thread::yield();
    status |= SaveLeafNode(node.get());
    if (node.use_count() > 1) {
      slot->cache->GiveBack(node->id, std::move(node));
    }
  }
  return status;
}

void TreeDBMImpl::DiscardLeafCache() {
  for (int32_t slot_index = NUM_PAGE_SLOTS - 1; slot_index >= 0; slot_index--) {
    LeafSlot* slot = leaf_slots_ + slot_index;
    std::shared_ptr<TreeLeafNode> node;
    while ((node = slot->cache->RemoveLRU()) != nullptr) {
      node->dirty = false;
    }
  }
}

Status TreeDBMImpl::LoadInnerNode(
    int64_t id, bool promotion, std::shared_ptr<TreeInnerNode>* node) {
  int32_t slot_index = id % NUM_PAGE_SLOTS;
  InnerSlot* slot = inner_slots_ + slot_index;
  {
    std::lock_guard<SpinMutex> lock(slot->mutex);
    *node = slot->cache->Get(id);
    if (*node != nullptr) {
      return Status(Status::SUCCESS);
    }
  }
  char node_key_buf[PAGE_ID_WIDTH];
  WriteFixNum(node_key_buf, id, PAGE_ID_WIDTH);
  const std::string_view node_key(node_key_buf, sizeof(node_key_buf));
  Status load_status(Status::SUCCESS);
  int64_t heir_id = 0;
  std::vector<TreeLink*> links;
  class Loader final : public DBM::RecordProcessor {
   public:
    Loader(Status* status, int64_t* heir_id, std::vector<TreeLink*>* links)
        : status_(status), heir_id_(heir_id), links_(links) {}
    std::string_view ProcessFull(std::string_view key, std::string_view value) override {
      *status_ = DeserializeInnerNode(value, heir_id_, links_);
      return NOOP;
    }
    std::string_view ProcessEmpty(std::string_view key) override {
      status_->Set(Status::BROKEN_DATA_ERROR, "invalid inner node ID");
      return NOOP;
    }
   private:
    Status* status_;
    int64_t* heir_id_;
    std::vector<TreeLink*>* links_;
  } loader(&load_status, &heir_id, &links);
  const Status status = hash_dbm_->Process(node_key, &loader, false);
  if (status != Status::SUCCESS) {
    return status;
  }
  if (load_status != Status::SUCCESS) {
    FreeTreeLinks(&links);
    return load_status;
  }
  *node = (new TreeInnerNode(this, id, heir_id, std::move(links)))->AddToCache();
  return Status(Status::SUCCESS);
}

Status TreeDBMImpl::SaveInnerNode(TreeInnerNode* node) {
  if (!node->dirty) {
    return Status(Status::SUCCESS);
  }
  char stack[WRITE_BUFFER_SIZE];
  int32_t page_size = PAGE_ID_WIDTH;
  for (const auto* link : node->links) {
    page_size += link->GetSerializedSize(PAGE_ID_WIDTH);
  }
  char* write_buf = page_size > WRITE_BUFFER_SIZE ? new char[page_size] : stack;
  char* wp = write_buf;
  WriteFixNum(wp, node->heir_id, PAGE_ID_WIDTH);
  wp += PAGE_ID_WIDTH;
  for (const auto* link : node->links) {
    const std::string_view key = link->GetKey();
    wp += WriteVarNum(wp, key.size());
    std::memcpy(wp, key.data(), key.size());
    wp += key.size();
    WriteFixNum(wp, link->child, PAGE_ID_WIDTH);
    wp += PAGE_ID_WIDTH;
  }
  char node_key_buf[PAGE_ID_WIDTH];
  WriteFixNum(node_key_buf, node->id, PAGE_ID_WIDTH);
  const std::string_view node_key(node_key_buf, sizeof(node_key_buf));
  const Status status = hash_dbm_->Set(node_key, std::string_view(write_buf, page_size));
  if (write_buf != stack) {
    delete[] write_buf;
  }
  node->dirty = false;
  node->on_disk = true;
  return status;
}

Status TreeDBMImpl::RemoveInnerNode(TreeInnerNode* node) {
  Status status(Status::SUCCESS);
  if (node->on_disk) {
    char node_key_buf[PAGE_ID_WIDTH];
    WriteFixNum(node_key_buf, node->id, PAGE_ID_WIDTH);
    const std::string_view node_key(node_key_buf, sizeof(node_key_buf));
    status |= hash_dbm_->Remove(node_key);
  }
  int32_t slot_index = node->id % NUM_PAGE_SLOTS;
  InnerSlot* slot = inner_slots_ + slot_index;
  node->dirty = false;
  slot->cache->Remove(node->id);
  return status;
}

Status TreeDBMImpl::FlushInnerCacheAll(bool empty) {
  Status status(Status::SUCCESS);
  for (int32_t slot_index = NUM_PAGE_SLOTS - 1; slot_index >= 0; slot_index--) {
    status |= FlushInnerCacheOne(empty, slot_index);
  }
  return status;
}

Status TreeDBMImpl::FlushInnerCacheOne(bool empty, int32_t slot_index) {
  Status status(Status::SUCCESS);
  InnerSlot* slot = inner_slots_ + slot_index;
  std::lock_guard<SpinMutex> lock(slot->mutex);
  std::vector<std::shared_ptr<TreeInnerNode>> deferred;
  std::shared_ptr<TreeInnerNode> node;
  while ((node = slot->cache->RemoveLRU()) != nullptr) {
    if (node.use_count() > 1) {
      if (empty) {
        status |= SaveInnerNode(node.get());
        status |= Status(Status::UNKNOWN_ERROR, "unexpected reference");
      } else {
        deferred.emplace_back(node);
      }
    } else {
      status |= SaveInnerNode(node.get());
    }
  }
  while (!deferred.empty()) {
    auto node = deferred.back();
    deferred.pop_back();
    std::this_thread::yield();
    status |= SaveInnerNode(node.get());
    if (node.use_count() > 1) {
      slot->cache->GiveBack(node->id, std::move(node));
    }
  }
  return status;
}

void TreeDBMImpl::DiscardInnerCache() {
  for (int32_t slot_index = NUM_PAGE_SLOTS - 1; slot_index >= 0; slot_index--) {
    InnerSlot* slot = inner_slots_ + slot_index;
    std::shared_ptr<TreeInnerNode> node;
    while ((node = slot->cache->RemoveLRU()) != nullptr) {
      node->dirty = false;
    }
  }
}

Status TreeDBMImpl::SearchTree(std::string_view key, std::shared_ptr<TreeLeafNode>* leaf_node) {
  int64_t id = root_id_;
  TreeLinkOnStack search_stack(key);
  const TreeLink* search_link = search_stack.link;
  while (id >= INNER_NODE_ID_BASE) {
    std::shared_ptr<TreeInnerNode> inner_node;
    Status status = LoadInnerNode(id, true, &inner_node);
    if (status != Status::SUCCESS) {
      return status;
    }
    const auto& links = inner_node->links;
    auto it = std::upper_bound(links.begin(), links.end(), search_link, link_comp_);
    if (it == links.begin()) {
      id = inner_node->heir_id;
    } else {
      --it;
      id = (*it)->child;
    }
  }
  return LoadLeafNode(id, true, leaf_node);
}

Status TreeDBMImpl::TraceTree(
    std::string_view key, int64_t leaf_node_id, int64_t* hist, int32_t* hist_size) {
  int64_t id = root_id_;
  TreeLinkOnStack search_stack(key);
  const TreeLink* search_link = search_stack.link;
  *hist_size = 0;
  while (id >= INNER_NODE_ID_BASE) {
    std::shared_ptr<TreeInnerNode> inner_node;
    Status status = LoadInnerNode(id, true, &inner_node);
    if (status != Status::SUCCESS) {
      return status;
    }
    hist[(*hist_size)++] = id;
    const auto& links = inner_node->links;
    auto it = std::upper_bound(links.begin(), links.end(), search_link, link_comp_);
    if (it == links.begin()) {
      id = inner_node->heir_id;
    } else {
      --it;
      id = (*it)->child;
    }
  }
  if (id != leaf_node_id) {
    return Status(Status::BROKEN_DATA_ERROR, "inconsistent path to a leaf node");
  }
  return Status(Status::SUCCESS);
}

Status TreeDBMImpl::ReorganizeTree() {
  std::set<int64_t> done_ids;
  while (!reorg_ids_.IsEmpty()) {
    const auto& id_key = reorg_ids_.Pop();
    if (!done_ids.emplace(id_key.first).second) {
      continue;
    }
    std::shared_ptr<TreeLeafNode> leaf_node;
    Status status = LoadLeafNode(id_key.first, false, &leaf_node);
    if (status != Status::SUCCESS) {
      return status;
    }
    if (CheckLeafNodeToDivide(leaf_node.get())) {
      status = DivideNodes(leaf_node.get(), id_key.second);
      if (status != Status::SUCCESS) {
        return status;
      }
    } else if (CheckLeafNodeToMerge(leaf_node.get())) {
      status = MergeNodes(leaf_node.get(), id_key.second);
      if (status != Status::SUCCESS) {
        return status;
      }
    }
  }
  return Status(Status::SUCCESS);
}

bool TreeDBMImpl::CheckLeafNodeToDivide(TreeLeafNode* node) {
  return static_cast<int32_t>(node->page_size) > max_page_size_ && node->records.size() > 1;
}

bool TreeDBMImpl::CheckLeafNodeToMerge(TreeLeafNode* node) {
  return static_cast<int32_t>(node->page_size) < max_page_size_ / 2 && node->id != root_id_;
}

Status TreeDBMImpl::DivideNodes(TreeLeafNode* leaf_node, const std::string& node_key) {
  int64_t hist[TREE_LEVEL_MAX];
  int32_t hist_size = 0;
  Status status = TraceTree(node_key, leaf_node->id, hist, &hist_size);
  if (status != Status::SUCCESS) {
    return status;
  }
  auto new_leaf_node = (new TreeLeafNode(this, leaf_node->id, leaf_node->next_id))->AddToCache();
  if (new_leaf_node->next_id > 0) {
    std::shared_ptr<TreeLeafNode> next_leaf_node;
    status = LoadLeafNode(new_leaf_node->next_id, false, &next_leaf_node);
    if (status != Status::SUCCESS) {
      return status;
    }
    next_leaf_node->prev_id = new_leaf_node->id;
    next_leaf_node->dirty = true;
  }
  leaf_node->next_id = new_leaf_node->id;
  leaf_node->dirty = true;
  auto& records = leaf_node->records;
  auto mid = records.begin() + records.size() / 2;
  auto it = mid;
  auto& new_records = new_leaf_node->records;
  new_records.reserve(records.end() - it);
  while (it != records.end()) {
    TreeRecord* rec = *it;
    new_records.emplace_back(rec);
    const int32_t data_size = rec->GetSerializedSize();
    leaf_node->page_size -= data_size;
    new_leaf_node->page_size += data_size;
    ++it;
  }
  if (last_id_ == leaf_node->id) {
    last_id_ = new_leaf_node->id;
  }
  for (auto* iterator : iterators_) {
    if (iterator->leaf_id_ == leaf_node->id) {
      TreeRecordOnStack search_stack(std::string_view(iterator->key_ptr_, iterator->key_size_));
      if (!record_comp_(search_stack.record, *mid)) {
        iterator->leaf_id_ = new_leaf_node->id;
      }
    }
  }
  records.erase(mid, records.end());
  records.shrink_to_fit();
  int64_t heir_id = leaf_node->id;
  int64_t child_id = new_leaf_node->id;
  std::string new_node_key(new_leaf_node->records.front()->GetKey());
  while (true) {
    if (hist_size < 1) {
      auto inner_node = (new TreeInnerNode(this, heir_id))->AddToCache();
      AddLinkToInnerNode(inner_node.get(), child_id, new_node_key);
      root_id_ = inner_node->id;
      tree_level_++;
      break;
    }
    const int64_t parent_id = hist[--hist_size];
    std::shared_ptr<TreeInnerNode> inner_node;
    status = LoadInnerNode(parent_id, false, &inner_node);
    if (status != Status::SUCCESS) {
      return status;
    }
    AddLinkToInnerNode(inner_node.get(), child_id, new_node_key);
    auto& links = inner_node->links;
    if (static_cast<int32_t>(links.size()) <= max_branches_) {
      break;
    }
    auto mid = links.begin() + links.size() / 2;
    TreeLink* link = *mid;
    auto new_inner_node = (new TreeInnerNode(this, link->child))->AddToCache();
    inner_node->dirty = true;
    new_node_key = std::string(link->GetKey());
    auto link_it = mid + 1;
    new_inner_node->links.reserve(links.end() - link_it);
    while (link_it != links.end()) {
      link = *link_it;
      AddLinkToInnerNode(new_inner_node.get(), link->child, link->GetKey());
      ++link_it;
    }
    int32_t num = new_inner_node->links.size();
    for (int32_t i = 0; i <= num; i++) {
      FreeTreeLink(links.back());
      links.pop_back();
    }
    links.shrink_to_fit();
    heir_id = inner_node->id;
    child_id = new_inner_node->id;
  }
  if (page_update_mode_ == TreeDBM::PAGE_UPDATE_WRITE) {
    status = SaveLeafNodeImpl(leaf_node);
    status |= SaveLeafNodeImpl(new_leaf_node.get());
    if (status != Status::SUCCESS) {
      return status;
    }
  }
  return Status(Status::SUCCESS);
}

Status TreeDBMImpl::MergeNodes(TreeLeafNode* leaf_node, const std::string& node_key) {
  int64_t hist[TREE_LEVEL_MAX];
  int32_t hist_size = 0;
  Status status = TraceTree(node_key, leaf_node->id, hist, &hist_size);
  if (status != Status::SUCCESS) {
    return status;
  }
  std::shared_ptr<TreeInnerNode> parent_node;
  const int64_t parent_id = hist[hist_size - 1];
  status = LoadInnerNode(parent_id, false, &parent_node);
  if (status != Status::SUCCESS) {
    return status;
  }
  const auto& links = parent_node->links;
  int64_t prev_id = 0;
  int64_t next_id = 0;
  if (parent_node->heir_id == leaf_node->id) {
    if (!links.empty()) {
      next_id = links.front()->child;
    }
  } else {
    for (int32_t link_index = 0; link_index < static_cast<int32_t>(links.size()); link_index++) {
      if (links[link_index]->child == leaf_node->id) {
        prev_id = link_index == 0 ? parent_node->heir_id : links[link_index - 1]->child;
        if (link_index < static_cast<int32_t>(links.size()) - 1) {
          next_id = links[link_index + 1]->child;
        }
        break;
      }
    }
  }
  std::shared_ptr<TreeLeafNode> prev_leaf_node;
  if (prev_id > 0) {
    status = LoadLeafNode(prev_id, false, &prev_leaf_node);
    if (status != Status::SUCCESS) {
      return status;
    }
  }
  std::shared_ptr<TreeLeafNode> next_leaf_node;
  if (next_id > 0) {
    status = LoadLeafNode(next_id, false, &next_leaf_node);
    if (status != Status::SUCCESS) {
      return status;
    }
  }
  if (prev_leaf_node != nullptr &&
      (next_leaf_node == nullptr || prev_leaf_node->page_size <= next_leaf_node->page_size)) {
    prev_leaf_node->records.reserve(prev_leaf_node->records.size() + leaf_node->records.size());
    prev_leaf_node->records.insert(
        prev_leaf_node->records.end(), leaf_node->records.begin(), leaf_node->records.end());
    leaf_node->records.clear();
    prev_leaf_node->page_size += leaf_node->page_size - PAGE_ID_WIDTH * 2;
    prev_leaf_node->next_id = leaf_node->next_id;
    prev_leaf_node->dirty = true;
    if (leaf_node->next_id > 0) {
      if (next_leaf_node == nullptr) {
        status = LoadLeafNode(leaf_node->next_id, false, &next_leaf_node);
        if (status != Status::SUCCESS) {
          return status;
        }
      }
      next_leaf_node->prev_id = prev_leaf_node->id;
      next_leaf_node->dirty = true;
    }
    JoinPrevLinkInInnerNode(parent_node.get(), leaf_node->id);
    if (last_id_ == leaf_node->id) {
      last_id_ = prev_leaf_node->id;
    }
    for (auto* iterator : iterators_) {
      if (iterator->leaf_id_ == leaf_node->id) {
        iterator->leaf_id_ = prev_leaf_node->id;
      }
    }
    RemoveLeafNode(leaf_node);
    if (page_update_mode_ == TreeDBM::PAGE_UPDATE_WRITE) {
      status = SaveLeafNodeImpl(prev_leaf_node.get());
      if (status != Status::SUCCESS) {
        return status;
      }
    }
  } else if (next_leaf_node != nullptr) {
    next_leaf_node->records.swap(leaf_node->records);
    next_leaf_node->records.reserve(next_leaf_node->records.size() + leaf_node->records.size());
    next_leaf_node->records.insert(
        next_leaf_node->records.end(), leaf_node->records.begin(), leaf_node->records.end());
    leaf_node->records.clear();
    next_leaf_node->page_size += leaf_node->page_size - PAGE_ID_WIDTH * 2;
    next_leaf_node->prev_id = leaf_node->prev_id;
    next_leaf_node->dirty = true;
    if (leaf_node->prev_id > 0) {
      if (prev_leaf_node == nullptr) {
        status = LoadLeafNode(leaf_node->prev_id, false, &prev_leaf_node);
        if (status != Status::SUCCESS) {
          return status;
        }
      }
      prev_leaf_node->next_id = next_leaf_node->id;
      prev_leaf_node->dirty = true;
    }
    JoinNextLinkInInnerNode(parent_node.get(), leaf_node->id, next_leaf_node->id);
    if (first_id_ == leaf_node->id) {
      first_id_ = next_leaf_node->id;
    }
    for (auto* iterator : iterators_) {
      if (iterator->leaf_id_ == leaf_node->id) {
        iterator->leaf_id_ = next_leaf_node->id;
      }
    }
    RemoveLeafNode(leaf_node);
    if (page_update_mode_ == TreeDBM::PAGE_UPDATE_WRITE) {
      status = SaveLeafNodeImpl(next_leaf_node.get());
      if (status != Status::SUCCESS) {
        return status;
      }
    }
  }
  std::shared_ptr<TreeInnerNode> inner_node = std::move(parent_node);
  while (static_cast<int32_t>(inner_node->links.size()) < max_branches_ / 2) {
    hist_size--;
    if (hist_size == 0) {
      if (inner_node->links.empty()) {
        root_id_ = inner_node->heir_id;
        tree_level_--;
        RemoveInnerNode(inner_node.get());
      }
      break;
    }
    const int64_t parent_id = hist[hist_size - 1];
    status = LoadInnerNode(parent_id, false, &parent_node);
    if (status != Status::SUCCESS) {
      return status;
    }
    const auto& links = parent_node->links;
    int64_t prev_id = 0;
    int64_t next_id = 0;
    std::string_view inner_key, next_key;
    if (parent_node->heir_id == inner_node->id) {
      if (!links.empty()) {
        next_id = links.front()->child;
        next_key = links.front()->GetKey();
      }
    } else {
      for (int32_t link_index = 0;
           link_index < static_cast<int32_t>(links.size()); link_index++) {
        if (links[link_index]->child == inner_node->id) {
          prev_id = link_index == 0 ? parent_node->heir_id : links[link_index - 1]->child;
          inner_key = links[link_index]->GetKey();
          if (link_index < static_cast<int32_t>(links.size()) - 1) {
            next_id = links[link_index + 1]->child;
            next_key = links[link_index + 1]->GetKey();
          }
          break;
        }
      }
    }
    std::shared_ptr<TreeInnerNode> prev_inner_node;
    if (prev_id > 0) {
      status = LoadInnerNode(prev_id, false, &prev_inner_node);
      if (status != Status::SUCCESS) {
        return status;
      }
    }
    std::shared_ptr<TreeInnerNode> next_inner_node;
    if (next_id > 0) {
      status = LoadInnerNode(next_id, false, &next_inner_node);
      if (status != Status::SUCCESS) {
        return status;
      }
    }
    if (prev_inner_node != nullptr &&
        (next_inner_node == nullptr ||
         prev_inner_node->links.size() <= next_inner_node->links.size())) {
      prev_inner_node->links.reserve(prev_inner_node->links.size() + 1 + inner_node->links.size());
      if (inner_node->heir_id > 0) {
        prev_inner_node->links.emplace_back(CreateTreeLink(inner_key, inner_node->heir_id));
      }
      prev_inner_node->links.insert(
          prev_inner_node->links.end(), inner_node->links.begin(), inner_node->links.end());
      inner_node->links.clear();
      prev_inner_node->dirty = true;
      JoinPrevLinkInInnerNode(parent_node.get(), inner_node->id);
      RemoveInnerNode(inner_node.get());
    } else if (next_inner_node != nullptr) {
      inner_node->links.reserve(inner_node->links.size() + 1 + next_inner_node->links.size());
      if (next_inner_node->heir_id > 0) {
        inner_node->links.emplace_back(CreateTreeLink(next_key, next_inner_node->heir_id));
      }
      inner_node->links.insert(
          inner_node->links.end(), next_inner_node->links.begin(), next_inner_node->links.end());
      next_inner_node->links.clear();
      inner_node->dirty = true;
      JoinPrevLinkInInnerNode(parent_node.get(), next_inner_node->id);
      RemoveInnerNode(next_inner_node.get());
    }
    inner_node = std::move(parent_node);
  }
  return Status(Status::SUCCESS);
}

void TreeDBMImpl::AddLinkToInnerNode(
    TreeInnerNode* node, int64_t child_id, std::string_view key) {
  TreeLink* link = CreateTreeLink(key, child_id);
  auto& links = node->links;
  auto it = std::upper_bound(links.begin(), links.end(), link, link_comp_);
  links.insert(it, link);
  node->dirty = true;
}

void TreeDBMImpl::JoinPrevLinkInInnerNode(TreeInnerNode* node, int64_t child_id) {
  auto& links = node->links;
  for (auto it = links.begin(); it != links.end(); ++it) {
    if ((*it)->child == child_id) {
      FreeTreeLink(*it);
      links.erase(it);
      break;
    }
  }
  node->dirty = true;
}

void TreeDBMImpl::JoinNextLinkInInnerNode(
    TreeInnerNode* node, int64_t child_id, int64_t next_id) {
  auto& links = node->links;
  if (node->heir_id == child_id) {
    node->heir_id = next_id;
    FreeTreeLink(links.front());
    links.erase(links.begin());
  } else {
    for (auto it = links.begin(); it != links.end(); ++it) {
      if ((*it)->child == child_id) {
        (*it)->child = next_id;
        ++it;
        FreeTreeLink(*it);
        links.erase(it);
        break;
      }
    }
  }
  node->dirty = true;
}

void TreeDBMImpl::ProcessImpl(
    TreeLeafNode* node, std::string_view key, DBM::RecordProcessor* proc, bool writable) {
  TreeRecordOnStack search_stack(key);
  const TreeRecord* search_rec = search_stack.record;
  auto& records = node->records;
  auto it = std::lower_bound(records.begin(), records.end(), search_rec, record_comp_);
  if (it != records.end() && !record_comp_(search_rec, *it)) {
    TreeRecord* rec = *it;
    const std::string_view new_value = proc->ProcessFull(rec->GetKey(), rec->GetValue());
    if (new_value.data() != DBM::RecordProcessor::NOOP.data() && writable) {
      if (update_logger_ != nullptr) {
        if (new_value.data() == DBM::RecordProcessor::REMOVE.data()) {
          update_logger_->WriteRemove(key);
        } else {
          update_logger_->WriteSet(key, new_value);
        }
      }
      const int32_t old_rec_size = rec->GetSerializedSize();
      const int32_t old_key_size = rec->key_size;
      const int32_t old_value_size = rec->value_size;
      if (new_value.data() == DBM::RecordProcessor::REMOVE.data()) {
        node->records.erase(it);
        node->page_size -= old_rec_size;
        node->dirty = true;
        if (CheckLeafNodeToMerge(node)) {
          reorg_ids_.Insert(std::make_pair(node->id, std::string(records.front()->GetKey())));
        }
        FreeTreeRecord(rec);
        num_records_.fetch_sub(1);
        eff_data_size_.fetch_sub(old_key_size + old_value_size);
      } else {
        TreeRecord* new_rec = ModifyTreeRecord(rec, new_value);
        const int32_t new_rec_size = new_rec->GetSerializedSize();
        *it = new_rec;
        node->page_size +=
            static_cast<int32_t>(new_rec_size) - static_cast<int32_t>(old_rec_size);
        node->dirty = true;
        eff_data_size_.fetch_add(
            static_cast<int32_t>(new_value.size()) - static_cast<int32_t>(old_value_size));
        if (static_cast<int32_t>(new_value.size()) > old_value_size) {
          if (CheckLeafNodeToDivide(node)) {
            reorg_ids_.Insert(std::make_pair(node->id, std::string(records.front()->GetKey())));
          }
        } else if (static_cast<int32_t>(new_value.size()) < old_value_size) {
          if (CheckLeafNodeToMerge(node)) {
            reorg_ids_.Insert(std::make_pair(node->id, std::string(records.front()->GetKey())));
          }
        }
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
      TreeRecord* new_rec = CreateTreeRecord(key, new_value);
      node->records.insert(it, new_rec);
      node->page_size += new_rec->GetSerializedSize();
      node->dirty = true;
      num_records_.fetch_add(1);
      eff_data_size_.fetch_add(key.size() + new_value.size());
      if (CheckLeafNodeToDivide(node)) {
        reorg_ids_.Insert(std::make_pair(node->id, std::string(records.front()->GetKey())));
      }
    }
  }
}

Status TreeDBMImpl::AdjustCaches() {
  const auto clock_div = std::lldiv(proc_clock_.fetch_add(1), ADJUST_CACHES_INV_FREQ);
  if (clock_div.rem != 0) {
    return Status(Status::SUCCESS);
  }
  const int32_t slot_index = clock_div.quot % NUM_PAGE_SLOTS;
  {
    auto* slot = leaf_slots_ + slot_index;
    std::lock_guard<SpinMutex> lock(slot->mutex);
    std::vector<std::shared_ptr<TreeLeafNode>> deferred;
    while (slot->cache->IsSaturated()) {
      auto node = slot->cache->RemoveLRU();
      if (node.use_count() > 1) {
        deferred.emplace_back(node);
      } else {
        const Status status = SaveLeafNode(node.get());
        if (status != Status::SUCCESS) {
          return status;
        }
      }
    }
    while (!deferred.empty()) {
      auto node = deferred.back();
      deferred.pop_back();
      slot->cache->GiveBack(node->id, std::move(node));
    }
  }
  {
    auto* slot = inner_slots_ + slot_index;
    std::lock_guard<SpinMutex> lock(slot->mutex);
    std::vector<std::shared_ptr<TreeInnerNode>> deferred;
    while (slot->cache->IsSaturated()) {
      auto node = slot->cache->RemoveLRU();
      if (node.use_count() > 1) {
        deferred.emplace_back(node);
      } else {
        const Status status = SaveInnerNode(node.get());
        if (status != Status::SUCCESS) {
          return status;
        }
      }
    }
    while (!deferred.empty()) {
      auto node = deferred.back();
      deferred.pop_back();
      slot->cache->GiveBack(node->id, std::move(node));
    }
  }
  return Status(Status::SUCCESS);
}

TreeDBMIteratorImpl::TreeDBMIteratorImpl(TreeDBMImpl* dbm)
    : dbm_(dbm), key_ptr_(nullptr), key_size_(0), leaf_id_(0) {
  std::lock_guard<SpinSharedMutex> lock(dbm_->mutex_);
  dbm_->iterators_.emplace_back(this);
}

TreeDBMIteratorImpl::~TreeDBMIteratorImpl() {
  if (dbm_ != nullptr) {
    std::lock_guard<SpinSharedMutex> lock(dbm_->mutex_);
    dbm_->iterators_.remove(this);
  }
  ClearPosition();
}

Status TreeDBMIteratorImpl::First() {
  std::shared_lock<SpinSharedMutex> lock(dbm_->mutex_);
  if (!dbm_->open_) {
    return Status(Status::PRECONDITION_ERROR, "not opened database");
  }
  ClearPosition();
  const Status status = SetPositionFirst(dbm_->first_id_);
  if (status == Status::NOT_FOUND_ERROR) {
    return Status(Status::SUCCESS);
  }
  return status;
}

Status TreeDBMIteratorImpl::Last() {
  std::shared_lock<SpinSharedMutex> lock(dbm_->mutex_);
  if (!dbm_->open_) {
    return Status(Status::PRECONDITION_ERROR, "not opened database");
  }
  ClearPosition();
  const Status status = SetPositionLast(dbm_->last_id_);
  if (status == Status::NOT_FOUND_ERROR) {
    return Status(Status::SUCCESS);
  }
  return status;
}

Status TreeDBMIteratorImpl::Jump(std::string_view key) {
  std::shared_lock<SpinSharedMutex> lock(dbm_->mutex_);
  if (!dbm_->open_) {
    return Status(Status::PRECONDITION_ERROR, "not opened database");
  }
  ClearPosition();
  SetPositionWithKey(0, key);
  return Status(Status::SUCCESS);
}

Status TreeDBMIteratorImpl::JumpLower(std::string_view key, bool inclusive) {
  std::shared_lock<SpinSharedMutex> lock(dbm_->mutex_);
  if (!dbm_->open_) {
    return Status(Status::PRECONDITION_ERROR, "not opened database");
  }
  ClearPosition();
  SetPositionWithKey(0, key);
  Status status = SyncPosition(key);
  if (status == Status::NOT_FOUND_ERROR) {
    status = SetPositionLast(dbm_->last_id_);
    if (status != Status::SUCCESS) {
      if (status == Status::NOT_FOUND_ERROR) {
        return Status(Status::SUCCESS);
      }
      return status;
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

Status TreeDBMIteratorImpl::JumpUpper(std::string_view key, bool inclusive) {
  std::shared_lock<SpinSharedMutex> lock(dbm_->mutex_);
  if (!dbm_->open_) {
    return Status(Status::PRECONDITION_ERROR, "not opened database");
  }
  ClearPosition();
  SetPositionWithKey(0, key);
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

Status TreeDBMIteratorImpl::Next() {
  std::shared_lock<SpinSharedMutex> lock(dbm_->mutex_);
  if (key_ptr_ == nullptr) {
    return Status(Status::NOT_FOUND_ERROR);
  }
  const std::string_view key(key_ptr_, key_size_);
  return NextImpl(key);
}

Status TreeDBMIteratorImpl::Previous() {
  std::shared_lock<SpinSharedMutex> lock(dbm_->mutex_);
  if (key_ptr_ == nullptr) {
    return Status(Status::NOT_FOUND_ERROR);
  }
  const std::string_view key(key_ptr_, key_size_);
  return PreviousImpl(key);
}

Status TreeDBMIteratorImpl::Process(DBM::RecordProcessor* proc, bool writable) {
  if (writable && !dbm_->reorg_ids_.IsEmpty()) {
    std::lock_guard<SpinSharedMutex> lock(dbm_->mutex_);
    const Status status = dbm_->ReorganizeTree();
    if (status != Status::SUCCESS) {
      return status;
    }
  }
  std::shared_lock<SpinSharedMutex> lock(dbm_->mutex_);
  if (key_ptr_ == nullptr) {
    return Status(Status::NOT_FOUND_ERROR);
  }
  const std::string_view key(key_ptr_, key_size_);
  return ProcessImpl(key, proc, writable);
}

void TreeDBMIteratorImpl::ClearPosition() {
  if (key_ptr_ != stack_) {
    delete[] key_ptr_;
  }
  key_ptr_ = nullptr;
  key_size_ = 0;
  leaf_id_ = 0;
}

Status TreeDBMIteratorImpl::SetPositionFirst(int64_t leaf_id) {
  while (leaf_id > 0) {
    std::shared_ptr<TreeLeafNode> node;
    const Status status = dbm_->LoadLeafNode(leaf_id, false, &node);
    if (status != Status::SUCCESS) {
      return status;
    }
    std::shared_lock<SpinSharedMutex> lock(node->mutex);
    auto& records = node->records;
    if (!records.empty()) {
      SetPositionWithKey(leaf_id, records.front()->GetKey());
      return Status(Status::SUCCESS);
    } else {
      leaf_id = node->next_id;
    }
  }
  return Status(Status::NOT_FOUND_ERROR);
}

Status TreeDBMIteratorImpl::SetPositionLast(int64_t leaf_id) {
  while (leaf_id > 0) {
    std::shared_ptr<TreeLeafNode> node;
    const Status status = dbm_->LoadLeafNode(leaf_id, false, &node);
    if (status != Status::SUCCESS) {
      return status;
    }
    std::shared_lock<SpinSharedMutex> lock(node->mutex);
    auto& records = node->records;
    if (!records.empty()) {
      SetPositionWithKey(leaf_id, records.back()->GetKey());
      return Status(Status::SUCCESS);
    } else {
      leaf_id = node->prev_id;
    }
  }
  return Status(Status::NOT_FOUND_ERROR);
}

void TreeDBMIteratorImpl::SetPositionWithKey(int64_t leaf_id, std::string_view key) {
  key_ptr_ = key.size() > sizeof(stack_) ? new char[key.size()] : stack_;
  std::memcpy(key_ptr_, key.data(), key.size());
  key_size_ = key.size();
  leaf_id_ = leaf_id;
}

Status TreeDBMIteratorImpl::NextImpl(std::string_view key) {
  std::shared_ptr<TreeLeafNode> node;
  if (leaf_id_ > 0) {
    const Status status = dbm_->LoadLeafNode(leaf_id_, false, &node);
    if (status != Status::SUCCESS) {
      return status;
    }
  } else {
    const Status status = dbm_->SearchTree(key, &node);
    if (status != Status::SUCCESS) {
      ClearPosition();
      if (status == Status::NOT_FOUND_ERROR) {
        return Status(Status::SUCCESS);
      }
      return status;
    }
  }
  bool hit = false;
  TreeRecordOnStack search_stack(key);
  const TreeRecord* search_rec = search_stack.record;
  {
    std::shared_lock<SpinSharedMutex> lock(node->mutex);
    auto& records = node->records;
    auto it = std::upper_bound(records.begin(), records.end(), search_rec, dbm_->record_comp_);
    if (it != records.end()) {
      SetPositionWithKey(leaf_id_, (*it)->GetKey());
      hit = true;
    }
  }
  Status status(Status::SUCCESS);
  if (!hit) {
    status = SetPositionFirst(node->next_id);
    if (status != Status::SUCCESS) {
      ClearPosition();
      if (status == Status::NOT_FOUND_ERROR) {
        status.Set(Status::SUCCESS);
      }
    }
  }
  status |= dbm_->AdjustCaches();
  return status;
}

Status TreeDBMIteratorImpl::PreviousImpl(std::string_view key) {
  std::shared_ptr<TreeLeafNode> node;
  if (leaf_id_ > 0) {
    const Status status = dbm_->LoadLeafNode(leaf_id_, false, &node);
    if (status != Status::SUCCESS) {
      return status;
    }
  } else {
    const Status status = dbm_->SearchTree(key, &node);
    if (status != Status::SUCCESS) {
      ClearPosition();
      if (status == Status::NOT_FOUND_ERROR) {
        return Status(Status::SUCCESS);
      }
      return status;
    }
  }
  bool hit = false;
  TreeRecordOnStack search_stack(key);
  const TreeRecord* search_rec = search_stack.record;
  {
    std::shared_lock<SpinSharedMutex> lock(node->mutex);
    auto& records = node->records;
    auto it = std::lower_bound(records.begin(), records.end(), search_rec, dbm_->record_comp_);
    if (it != records.end()) {
      if (it != records.begin()) {
        --it;
        SetPositionWithKey(leaf_id_, (*it)->GetKey());
        hit = true;
      }
    }
  }
  Status status(Status::SUCCESS);
  if (!hit) {
    status = SetPositionLast(node->prev_id);
    if (status != Status::SUCCESS) {
      ClearPosition();
      if (status == Status::NOT_FOUND_ERROR) {
        status.Set(Status::SUCCESS);
      }
    }
  }
  status |= dbm_->AdjustCaches();
  return status;
}

Status TreeDBMIteratorImpl::SyncPosition(std::string_view key) {
  std::shared_ptr<TreeLeafNode> node;
  Status status(Status::SUCCESS);
  if (leaf_id_ > 0) {
    status = dbm_->LoadLeafNode(leaf_id_, false, &node);
  } else {
    status = dbm_->SearchTree(key, &node);
  }
  if (status != Status::SUCCESS) {
    ClearPosition();
    if (status == Status::NOT_FOUND_ERROR) {
      return Status(Status::SUCCESS);
    }
    return status;
  }
  TreeRecordOnStack search_stack(key);
  const TreeRecord* search_rec = search_stack.record;
  bool hit = false;
  std::shared_lock<SpinSharedMutex> lock(node->mutex);
  auto& records = node->records;
  auto it = std::lower_bound(records.begin(), records.end(), search_rec, dbm_->record_comp_);
  if (it != records.end()) {
    TreeRecord* rec = *it;
    SetPositionWithKey(node->id, rec->GetKey());
    hit = true;
  }
  if (!hit) {
    int64_t leaf_id = node->next_id;
    while (leaf_id > 0) {
      const Status status = dbm_->LoadLeafNode(leaf_id, false, &node);
      if (status != Status::SUCCESS) {
        return status;
      }
      std::shared_lock<SpinSharedMutex> lock(node->mutex);
      auto& records = node->records;
      if (!records.empty()) {
        TreeRecord* rec = records.front();
        SetPositionWithKey(node->id, rec->GetKey());
        hit = true;
        break;
      } else {
        leaf_id = node->next_id;
      }
    }
  }
  if (!hit) {
    ClearPosition();
    return Status(Status::NOT_FOUND_ERROR);
  }
  return Status(Status::SUCCESS);
}

Status TreeDBMIteratorImpl::ProcessImpl(
    std::string_view key, DBM::RecordProcessor* proc, bool writable) {
  std::shared_ptr<TreeLeafNode> node;
  Status status(Status::SUCCESS);
  if (leaf_id_ > 0) {
    status = dbm_->LoadLeafNode(leaf_id_, false, &node);
  } else {
    status = dbm_->SearchTree(key, &node);
  }
  if (status != Status::SUCCESS) {
    ClearPosition();
    return status;
  }
  TreeRecordOnStack search_stack(key);
  const TreeRecord* search_rec = search_stack.record;
  bool hit = false;
  if (writable) {
    std::lock_guard<SpinSharedMutex> lock(node->mutex);
    auto& records = node->records;
    auto it = std::lower_bound(records.begin(), records.end(), search_rec, dbm_->record_comp_);
    if (it != records.end()) {
      TreeRecord* rec = *it;
      SetPositionWithKey(node->id, rec->GetKey());
      dbm_->ProcessImpl(node.get(), std::string_view(key_ptr_, key_size_), proc, true);
      hit = true;
    }
  } else {
    std::shared_lock<SpinSharedMutex> lock(node->mutex);
    auto& records = node->records;
    auto it = std::lower_bound(records.begin(), records.end(), search_rec, dbm_->record_comp_);
    if (it != records.end()) {
      TreeRecord* rec = *it;
      SetPositionWithKey(node->id, rec->GetKey());
      proc->ProcessFull(rec->GetKey(), rec->GetValue());
      hit = true;
    }
  }
  if (!hit) {
    int64_t leaf_id = node->next_id;
    while (leaf_id > 0) {
      const Status status = dbm_->LoadLeafNode(leaf_id, false, &node);
      if (status != Status::SUCCESS) {
        return status;
      }
      if (writable) {
        std::lock_guard<SpinSharedMutex> lock(node->mutex);
        auto& records = node->records;
        if (!records.empty()) {
          TreeRecord* rec = records.front();
          SetPositionWithKey(node->id, rec->GetKey());
          dbm_->ProcessImpl(node.get(), std::string_view(key_ptr_, key_size_), proc, true);
          hit = true;
          break;
        } else {
          leaf_id = node->next_id;
        }
      } else {
        std::shared_lock<SpinSharedMutex> lock(node->mutex);
        auto& records = node->records;
        if (!records.empty()) {
          TreeRecord* rec = records.front();
          SetPositionWithKey(node->id, rec->GetKey());
          proc->ProcessFull(rec->GetKey(), rec->GetValue());
          hit = true;
          break;
        } else {
          leaf_id = node->next_id;
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

TreeDBM::TreeDBM() {
  impl_ = new TreeDBMImpl(std::make_unique<MemoryMapParallelFile>());
}

TreeDBM::TreeDBM(std::unique_ptr<File> file) {
  impl_ = new TreeDBMImpl(std::move(file));
}

TreeDBM::~TreeDBM() {
  delete impl_;
}

Status TreeDBM::OpenAdvanced(const std::string& path, bool writable,
                             int32_t options, const TuningParameters& tuning_params) {
  return impl_->Open(path, writable, options, tuning_params);
}

Status TreeDBM::Close() {
  return impl_->Close();
}

Status TreeDBM::Process(std::string_view key, RecordProcessor* proc, bool writable) {
  assert(proc != nullptr);
  return impl_->Process(key, proc, writable);
}

Status TreeDBM::ProcessMulti(
    const std::vector<std::pair<std::string_view, RecordProcessor*>>& key_proc_pairs,
    bool writable) {
  return impl_->ProcessMulti(key_proc_pairs, writable);
}

Status TreeDBM::ProcessFirst(RecordProcessor* proc, bool writable) {
  assert(proc != nullptr);
  return impl_->ProcessFirst(proc, writable);
}

Status TreeDBM::ProcessEach(RecordProcessor* proc, bool writable) {
  assert(proc != nullptr);
  return impl_->ProcessEach(proc, writable);
}

Status TreeDBM::Count(int64_t* count) {
  assert(count != nullptr);
  return impl_->Count(count);
}

Status TreeDBM::GetFileSize(int64_t* size) {
  assert(size != nullptr);
  return impl_->GetFileSize(size);
}

Status TreeDBM::GetFilePath(std::string* path) {
  assert(path != nullptr);
  return impl_->GetFilePath(path);
}

Status TreeDBM::GetTimestamp(double* timestamp) {
  assert(timestamp != nullptr);
  return impl_->GetTimestamp(timestamp);
}

Status TreeDBM::Clear() {
  return impl_->Clear();
}

Status TreeDBM::RebuildAdvanced(
    const TuningParameters& tuning_params, bool skip_broken_records, bool sync_hard) {
  return impl_->Rebuild(tuning_params, skip_broken_records, sync_hard);
}

Status TreeDBM::ShouldBeRebuilt(bool* tobe) {
  assert(tobe != nullptr);
  return impl_->ShouldBeRebuilt(tobe);
}

Status TreeDBM::Synchronize(bool hard, FileProcessor* proc) {
  return impl_->Synchronize(hard, proc);
}

std::vector<std::pair<std::string, std::string>> TreeDBM::Inspect() {
  return impl_->Inspect();
}

bool TreeDBM::IsOpen() const {
  return impl_->IsOpen();
}

bool TreeDBM::IsWritable() const {
  return impl_->IsWritable();
}

bool TreeDBM::IsHealthy() const {
  return impl_->IsHealthy();
}

bool TreeDBM::IsAutoRestored() const {
  return impl_->IsAutoRestored();
}

std::unique_ptr<DBM::Iterator> TreeDBM::MakeIterator() {
  std::unique_ptr<TreeDBM::Iterator> iter(new TreeDBM::Iterator(impl_));
  return iter;
}

std::unique_ptr<DBM> TreeDBM::MakeDBM() const {
  return impl_->MakeDBM();
}

DBM::UpdateLogger* TreeDBM::GetUpdateLogger() const {
  return impl_->GetUpdateLogger();
}

void TreeDBM::SetUpdateLogger(UpdateLogger* update_logger) {
  impl_->SetUpdateLogger(update_logger);
}

File* TreeDBM::GetInternalFile() const {
  return impl_->GetInternalFile();
}

int64_t TreeDBM::GetEffectiveDataSize() {
  return impl_->GetEffectiveDataSize();
}

int32_t TreeDBM::GetDatabaseType() {
  return impl_->GetDatabaseType();
}

Status TreeDBM::SetDatabaseType(uint32_t db_type) {
  return impl_->SetDatabaseType(db_type);
}

std::string TreeDBM::GetOpaqueMetadata() {
  return impl_->GetOpaqueMetadata();
}

Status TreeDBM::SetOpaqueMetadata(const std::string& opaque) {
  return impl_->SetOpaqueMetadata(opaque);
}

KeyComparator TreeDBM::GetKeyComparator() const {
  return impl_->GetKeyComparator();
}

Status TreeDBM::ValidateHashBuckets() {
  return impl_->ValidateHashBuckets();
}

Status TreeDBM::ValidateRecords(int64_t record_base, int64_t end_offset) {
  return impl_->ValidateRecords(record_base, end_offset);
}

TreeDBM::Iterator::Iterator(TreeDBMImpl* dbm_impl) {
  impl_ = new TreeDBMIteratorImpl(dbm_impl);
}

TreeDBM::Iterator::~Iterator() {
  delete impl_;
}

Status TreeDBM::Iterator::First() {
  return impl_->First();
}

Status TreeDBM::Iterator::Last() {
  return impl_->Last();
}

Status TreeDBM::Iterator::Jump(std::string_view key) {
  return impl_->Jump(key);
}

Status TreeDBM::Iterator::JumpLower(std::string_view key, bool inclusive) {
  return impl_->JumpLower(key, inclusive);
}

Status TreeDBM::Iterator::JumpUpper(std::string_view key, bool inclusive) {
  return impl_->JumpUpper(key, inclusive);
}

Status TreeDBM::Iterator::Next() {
  return impl_->Next();
}

Status TreeDBM::Iterator::Previous() {
  return impl_->Previous();
}

Status TreeDBM::Iterator::Process(RecordProcessor* proc, bool writable) {
  assert(proc != nullptr);
  return impl_->Process(proc, writable);
}

Status TreeDBM::ParseMetadata(
      std::string_view opaque, int64_t* num_records, int64_t* eff_data_size,
      int64_t* root_id, int64_t* first_id, int64_t* last_id,
      int64_t* num_leaf_nodes, int64_t* num_inner_nodes,
      int32_t* max_page_size, int32_t* max_branches,
      int32_t* tree_level, int32_t* key_comp_type, std::string* mini_opaque) {
  assert(num_records != nullptr && eff_data_size != nullptr &&
         root_id != nullptr && first_id != nullptr && last_id != nullptr &&
         num_leaf_nodes != nullptr && num_inner_nodes != nullptr &&
         max_page_size != nullptr && max_branches != nullptr &&
         tree_level != nullptr && key_comp_type != nullptr && mini_opaque != nullptr);
  if (opaque.size() != HashDBM::OPAQUE_METADATA_SIZE) {
    return Status(Status::BROKEN_DATA_ERROR, "missing metadata");
  }
  const char* rp = opaque.data();
  if (std::memcmp(rp, META_MAGIC_DATA, sizeof(META_MAGIC_DATA)) != 0) {
    return Status(Status::BROKEN_DATA_ERROR, "bad magic data");
  }
  *num_records = ReadFixNum(rp + META_OFFSET_NUM_RECORDS, 6);
  *eff_data_size = ReadFixNum(rp + META_OFFSET_EFF_DATA_SIZE, 6);
  *root_id = ReadFixNum(rp + META_OFFSET_ROOT_ID, 6);
  *first_id = ReadFixNum(rp + META_OFFSET_FIRST_ID, 6);
  *last_id = ReadFixNum(rp + META_OFFSET_FIRST_ID, 6);
  *num_leaf_nodes = ReadFixNum(rp + META_OFFSET_NUM_LEAF_NODES, 6);
  *num_inner_nodes = ReadFixNum(rp + META_OFFSET_NUM_INNER_NODES, 6);
  *max_page_size = ReadFixNum(rp + META_OFFSET_MAX_PAGE_SIZE, 3);
  *max_branches = ReadFixNum(rp + META_OFFSET_MAX_BRANCHES, 3);
  *tree_level = ReadFixNum(rp + META_OFFSET_TREE_LEVEL, 1);
  *key_comp_type = ReadFixNum(rp + META_OFFSET_KEY_COMPARATOR, 1);
  *mini_opaque =
      std::string(rp + META_OFFSET_OPAQUE, HashDBM::OPAQUE_METADATA_SIZE - META_OFFSET_OPAQUE);
  return Status(Status::SUCCESS);
}

Status TreeDBM::RestoreDatabase(
    const std::string& old_file_path, const std::string& new_file_path,
    int64_t end_offset, std::string_view cipher_key) {
  std::string tmp_file_path = old_file_path;
  if (end_offset == INT64MIN || end_offset == INT64MAX) {
    end_offset = end_offset == INT64MIN ? -1 : 0;
  } else {
    tmp_file_path = new_file_path + ".tmp.restore";
    const Status status =
        HashDBM::RestoreDatabase(old_file_path, tmp_file_path, end_offset, cipher_key);
    if (status != Status::SUCCESS) {
      RemoveFile(tmp_file_path);
      return status;
    }
  }
  HashDBM::TuningParameters hash_params;
  hash_params.cipher_key = cipher_key;
  HashDBM tmp_dbm;
  Status status = tmp_dbm.OpenAdvanced(tmp_file_path, false, File::OPEN_DEFAULT, hash_params);
  if (status != Status::SUCCESS) {
    RemoveFile(tmp_file_path);
    return status;
  }
  const auto& meta = tmp_dbm.Inspect();
  const std::map<std::string, std::string> meta_map(meta.begin(), meta.end());
  const std::string& record_crc_expr = SearchMap(meta_map, "record_crc_mode", "");
  HashDBM::RecordCRCMode record_crc_mode = HashDBM::RECORD_CRC_NONE;
  if (record_crc_expr == "crc-8") {
    record_crc_mode = HashDBM::RECORD_CRC_8;
  } else if (record_crc_expr == "crc-16") {
    record_crc_mode = HashDBM::RECORD_CRC_16;
  } else if (record_crc_expr == "crc-32") {
    record_crc_mode = HashDBM::RECORD_CRC_32;
  }
  const std::string& record_comp_expr = SearchMap(meta_map, "record_comp_mode", "");
  HashDBM::RecordCompressionMode record_comp_mode = HashDBM::RECORD_COMP_NONE;
  if (record_comp_expr == "zlib") {
    record_comp_mode = HashDBM::RECORD_COMP_ZLIB;
  } else if (record_comp_expr == "zstd") {
    record_comp_mode = HashDBM::RECORD_COMP_ZSTD;
  } else if (record_comp_expr == "lz4") {
    record_comp_mode = HashDBM::RECORD_COMP_LZ4;
  } else if (record_comp_expr == "lzma") {
    record_comp_mode = HashDBM::RECORD_COMP_LZMA;
  } else if (record_comp_expr == "rc4") {
    record_comp_mode = HashDBM::RECORD_COMP_RC4;
  } else if (record_comp_expr == "aes") {
    record_comp_mode = HashDBM::RECORD_COMP_AES;
  }
  int32_t offset_width = StrToInt(SearchMap(meta_map, "offset_width", "-1"));
  int32_t align_pow = StrToInt(SearchMap(meta_map, "align_pow", "-1"));
  int64_t num_buckets = StrToInt(SearchMap(meta_map, "hash_num_buckets", "-1"));
  int64_t num_records = 0;
  int64_t eff_data_size = 0;
  int64_t root_id = 0;
  int64_t first_id = 0;
  int64_t last_id = 0;
  int64_t num_leaf_nodes = 0;
  int64_t num_inner_nodes = 0;
  int32_t max_page_size = 0;
  int32_t max_branches = 0;
  int32_t tree_level = 0;
  int32_t key_comp_type = 0;
  std::string mini_opaque;
  const std::string opaque = tmp_dbm.GetOpaqueMetadata();
  if (ParseMetadata(
          opaque, &num_records, &eff_data_size,
          &root_id, &first_id, &last_id,
          &num_leaf_nodes, &num_inner_nodes,
          &max_page_size, &max_branches,
          &tree_level, &key_comp_type, &mini_opaque) != Status::SUCCESS) {
    offset_width = TreeDBM::DEFAULT_OFFSET_WIDTH;
    align_pow = TreeDBM::DEFAULT_ALIGN_POW;
    num_buckets = std::max(tmp_dbm.CountSimple() * 2, DEFAULT_NUM_BUCKETS);
    max_page_size = -1;
    max_branches = -1;
  }
  TuningParameters params;
  params.update_mode = tmp_dbm.GetUpdateMode();
  params.record_crc_mode = record_crc_mode;
  params.record_comp_mode = record_comp_mode;
  params.offset_width = offset_width;
  params.align_pow = align_pow;
  params.num_buckets = num_buckets;
  params.cipher_key = cipher_key;
  params.max_page_size = max_page_size;
  params.max_branches = max_branches;
  TreeDBM new_dbm;
  status = new_dbm.OpenAdvanced(new_file_path, true, File::OPEN_TRUNCATE, params);
  if (status != Status::SUCCESS) {
    tmp_dbm.Close();
    RemoveFile(tmp_file_path);
    RemoveFile(new_file_path);
    return status;
  }
  new_dbm.SetDatabaseType(tmp_dbm.GetDatabaseType());
  new_dbm.SetOpaqueMetadata(mini_opaque);
  class Loader final : public RecordProcessor {
   public:
    explicit Loader(TreeDBM* dbm) : dbm_(dbm) {}
    std::string_view ProcessFull(std::string_view key, std::string_view value) override {
      if (key.size() != PAGE_ID_WIDTH) {
        return NOOP;
      }
      const int64_t id = StrToIntBigEndian(key);
      if (id < LEAF_NODE_ID_BASE || id >= INNER_NODE_ID_BASE) {
        return NOOP;
      }
      int64_t prev_id = 0;
      int64_t next_id = 0;
      std::vector<TreeRecord*> records;
      if (DeserializeLeafNode(value, &prev_id, &next_id, &records) == Status::SUCCESS) {
        for (const auto* rec : records) {
          dbm_->Set(rec->GetKey(), rec->GetValue(), false);
        }
      }
      FreeTreeRecords(&records);
      return NOOP;
    }
   private:
    TreeDBM* dbm_;
  } loader(&new_dbm);
  status = tmp_dbm.ProcessEach(&loader, false);
  status |= new_dbm.Close();
  tmp_dbm.Close();
  if (tmp_file_path != old_file_path) {
    RemoveFile(tmp_file_path);
  }
  return status;
}

}  // namespace tkrzw

// END OF FILE
