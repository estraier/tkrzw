/*************************************************************************************************
 * Tests for tkrzw_dbm_tree.h
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

#include "gtest/gtest.h"
#include "gmock/gmock.h"

#include "tkrzw_dbm.h"
#include "tkrzw_dbm_tree.h"
#include "tkrzw_dbm_tree_impl.h"
#include "tkrzw_dbm_test_common.h"
#include "tkrzw_file.h"
#include "tkrzw_file_mmap.h"
#include "tkrzw_file_pos.h"
#include "tkrzw_file_util.h"
#include "tkrzw_lib_common.h"
#include "tkrzw_str_util.h"
#include "tkrzw_sys_config.h"

using namespace testing;

// Main routine
int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

class TreeDBMTest : public CommonDBMTest {
 protected:
  void TreeDBMEmptyDatabaseTest(tkrzw::TreeDBM* dbm);
  void TreeDBMFileTest(tkrzw::TreeDBM* dbm);
  void TreeDBMLargeRecordTest(tkrzw::TreeDBM* dbm);
  void TreeDBMBasicTest(tkrzw::TreeDBM* dbm);
  void TreeDBMAppendTest(tkrzw::TreeDBM* dbm);
  void TreeDBMProcessTest(tkrzw::TreeDBM* dbm);
  void TreeDBMProcessEachTest(tkrzw::TreeDBM* dbm);
  void TreeDBMRandomTestOne(tkrzw::TreeDBM* dbm);
  void TreeDBMRandomTestThread(tkrzw::TreeDBM* dbm);
  void TreeDBMRecordMigrationTest(tkrzw::TreeDBM* dbm, tkrzw::File* file);
  void TreeDBMBackIteratorTest(tkrzw::TreeDBM* dbm);
  void TreeDBMIteratorBoundTest(tkrzw::TreeDBM* dbm);
  void TreeDBMReorganizeTestOne(tkrzw::TreeDBM* dbm);
  void TreeDBMReorganizeTestAll(tkrzw::TreeDBM* dbm);
  void TreeDBMIteratorTest(tkrzw::TreeDBM* dbm);
  void TreeDBMKeyComparatorTest(tkrzw::TreeDBM* dbm);
  void TreeDBMRebuildStaticTestOne(
      tkrzw::TreeDBM* dbm, const tkrzw::TreeDBM::TuningParameters& tuning_params);
  void TreeDBMRebuildStaticTestAll(tkrzw::TreeDBM* dbm);
  void TreeDBMRebuildRandomTest(tkrzw::TreeDBM* dbm);
  void TreeDBMRestoreTest(tkrzw::TreeDBM* dbm);
};

void TreeDBMTest::TreeDBMEmptyDatabaseTest(tkrzw::TreeDBM* dbm) {
  tkrzw::TemporaryDirectory tmp_dir(true, "tkrzw-");
  const std::string file_path = tmp_dir.MakeUniquePath();
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Open(file_path, true));
  EXPECT_TRUE(dbm->IsHealthy());
  EXPECT_GT(dbm->GetFileSizeSimple(), 0);
  EXPECT_GT(dbm->GetModificationTime(), 0);
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->SetDatabaseType(123));
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->SetOpaqueMetadata("0123456789"));
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Close());
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Open(file_path, false));
  EXPECT_EQ(123, dbm->GetDatabaseType());
  EXPECT_TRUE(dbm->IsHealthy());
  const int64_t file_size = dbm->GetFileSizeSimple();
  EXPECT_GT(file_size, 0);
  EXPECT_GT(dbm->GetModificationTime(), 0);
  EXPECT_EQ(123, dbm->GetDatabaseType());
  EXPECT_EQ("0123456789", dbm->GetOpaqueMetadata().substr(0, 10));
  const auto& meta = dbm->Inspect();
  const std::map<std::string, std::string> meta_map(meta.begin(), meta.end());
  EXPECT_TRUE(tkrzw::CheckMap(meta_map, "pkg_major_version"));
  EXPECT_TRUE(tkrzw::CheckMap(meta_map, "root_id"));
  EXPECT_TRUE(tkrzw::CheckMap(meta_map, "tree_level"));
  EXPECT_EQ("true", tkrzw::SearchMap(meta_map, "healthy", ""));
  EXPECT_TRUE(dbm->IsOrdered());
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Close());
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Open(file_path, true));
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Clear());
  EXPECT_EQ(0, dbm->CountSimple());
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Synchronize(true));
  EXPECT_EQ(file_size, dbm->GetFileSizeSimple());
  EXPECT_EQ(123, dbm->GetDatabaseType());
  EXPECT_EQ("0123456789", dbm->GetOpaqueMetadata().substr(0, 10));
  EXPECT_EQ(tkrzw::LexicalKeyComparator, dbm->GetKeyComparator());
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Synchronize(false));
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Close());
}

void TreeDBMTest::TreeDBMFileTest(tkrzw::TreeDBM* dbm) {
  tkrzw::TemporaryDirectory tmp_dir(true, "tkrzw-");
  const std::string file_path = tmp_dir.MakeUniquePath();
  FileTest(dbm, file_path);
}

void TreeDBMTest::TreeDBMLargeRecordTest(tkrzw::TreeDBM* dbm) {
  tkrzw::TemporaryDirectory tmp_dir(true, "tkrzw-");
  const std::string file_path = tmp_dir.MakeUniquePath();
  const std::vector<tkrzw::HashDBM::UpdateMode> update_modes =
      {tkrzw::HashDBM::UPDATE_IN_PLACE, tkrzw::HashDBM::UPDATE_APPENDING};
  const std::vector<int32_t> max_page_size = {200};
  const std::vector<int32_t> max_branches = {5};
  const std::vector<int32_t> max_cached_pages = {64};
  for (const auto& update_mode : update_modes) {
    for (const auto& max_page_size : max_page_size) {
      for (const auto& max_branches : max_branches) {
        for (const auto& max_cached_pages : max_cached_pages) {
          tkrzw::TreeDBM::TuningParameters tuning_params;
          tuning_params.update_mode = update_mode;
          tuning_params.max_page_size = max_page_size;
          tuning_params.max_branches = max_branches;
          tuning_params.max_cached_pages = max_cached_pages;
          EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->OpenAdvanced(
              file_path, true, tkrzw::File::OPEN_TRUNCATE, tuning_params));
          LargeRecordTest(dbm);
          EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Close());
        }
      }
    }
  }
}

void TreeDBMTest::TreeDBMBasicTest(tkrzw::TreeDBM* dbm) {
  tkrzw::TemporaryDirectory tmp_dir(true, "tkrzw-");
  const std::string file_path = tmp_dir.MakeUniquePath();
  const std::vector<tkrzw::HashDBM::UpdateMode> update_modes =
      {tkrzw::HashDBM::UPDATE_IN_PLACE, tkrzw::HashDBM::UPDATE_APPENDING};
  const std::vector<int32_t> max_page_size = {100, 200, 400};
  const std::vector<int32_t> max_branches = {2, 5, 16};
  const std::vector<int32_t> max_cached_pages = {1, 64, 256};
  for (const auto& update_mode : update_modes) {
    for (const auto& max_page_size : max_page_size) {
      for (const auto& max_branches : max_branches) {
        for (const auto& max_cached_pages : max_cached_pages) {
          tkrzw::TreeDBM::TuningParameters tuning_params;
          tuning_params.update_mode = update_mode;
          tuning_params.max_page_size = max_page_size;
          tuning_params.max_branches = max_branches;
          tuning_params.max_cached_pages = max_cached_pages;
          EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->OpenAdvanced(
              file_path, true, tkrzw::File::OPEN_TRUNCATE, tuning_params));
          BasicTest(dbm);
          EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Close());
        }
      }
    }
  }
}

void TreeDBMTest::TreeDBMAppendTest(tkrzw::TreeDBM* dbm) {
  tkrzw::TemporaryDirectory tmp_dir(true, "tkrzw-");
  const std::string file_path = tmp_dir.MakeUniquePath();
  const std::vector<tkrzw::HashDBM::UpdateMode> update_modes =
      {tkrzw::HashDBM::UPDATE_IN_PLACE, tkrzw::HashDBM::UPDATE_APPENDING};
  const std::vector<int32_t> max_page_size = {200};
  const std::vector<int32_t> max_branches = {16};
  const std::vector<int32_t> max_cached_pages = {64};
  for (const auto& update_mode : update_modes) {
    for (const auto& max_page_size : max_page_size) {
      for (const auto& max_branches : max_branches) {
        for (const auto& max_cached_pages : max_cached_pages) {
          tkrzw::TreeDBM::TuningParameters tuning_params;
          tuning_params.update_mode = update_mode;
          tuning_params.max_page_size = max_page_size;
          tuning_params.max_branches = max_branches;
          tuning_params.max_cached_pages = max_cached_pages;
          EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->OpenAdvanced(
              file_path, true, tkrzw::File::OPEN_TRUNCATE, tuning_params));
          AppendTest(dbm);
          EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Close());
        }
      }
    }
  }
}

void TreeDBMTest::TreeDBMProcessTest(tkrzw::TreeDBM* dbm) {
  tkrzw::TemporaryDirectory tmp_dir(true, "tkrzw-");
  const std::string file_path = tmp_dir.MakeUniquePath();
  const std::vector<tkrzw::HashDBM::UpdateMode> update_modes =
      {tkrzw::HashDBM::UPDATE_IN_PLACE, tkrzw::HashDBM::UPDATE_APPENDING};
  const std::vector<int32_t> max_page_size = {100, 200, 400};
  const std::vector<int32_t> max_branches = {2, 5, 16};
  const std::vector<int32_t> max_cached_pages = {1, 64, 256};
  for (const auto& update_mode : update_modes) {
    for (const auto& max_page_size : max_page_size) {
      for (const auto& max_branches : max_branches) {
        for (const auto& max_cached_pages : max_cached_pages) {
          tkrzw::TreeDBM::TuningParameters tuning_params;
          tuning_params.update_mode = update_mode;
          tuning_params.max_page_size = max_page_size;
          tuning_params.max_branches = max_branches;
          tuning_params.max_cached_pages = max_cached_pages;
          EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->OpenAdvanced(
              file_path, true, tkrzw::File::OPEN_TRUNCATE, tuning_params));
          ProcessTest(dbm);
          EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Close());
        }
      }
    }
  }
}

void TreeDBMTest::TreeDBMProcessEachTest(tkrzw::TreeDBM* dbm) {
  tkrzw::TemporaryDirectory tmp_dir(true, "tkrzw-");
  const std::string file_path = tmp_dir.MakeUniquePath();
  const std::vector<tkrzw::HashDBM::UpdateMode> update_modes =
      {tkrzw::HashDBM::UPDATE_IN_PLACE, tkrzw::HashDBM::UPDATE_APPENDING};
  const std::vector<int32_t> max_page_size = {100, 200};
  const std::vector<int32_t> max_branches = {2, 16};
  const std::vector<int32_t> max_cached_pages = {64, 256};
  for (const auto& update_mode : update_modes) {
    for (const auto& max_page_size : max_page_size) {
      for (const auto& max_branches : max_branches) {
        for (const auto& max_cached_pages : max_cached_pages) {
          tkrzw::TreeDBM::TuningParameters tuning_params;
          tuning_params.update_mode = update_mode;
          tuning_params.max_page_size = max_page_size;
          tuning_params.max_branches = max_branches;
          tuning_params.max_cached_pages = max_cached_pages;
          EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->OpenAdvanced(
              file_path, true, tkrzw::File::OPEN_TRUNCATE, tuning_params));
          ProcessEachTest(dbm);
          EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Close());
        }
      }
    }
  }
}

void TreeDBMTest::TreeDBMRandomTestOne(tkrzw::TreeDBM* dbm) {
  tkrzw::TemporaryDirectory tmp_dir(true, "tkrzw-");
  const std::string file_path = tmp_dir.MakeUniquePath();
  const std::vector<tkrzw::HashDBM::UpdateMode> update_modes =
      {tkrzw::HashDBM::UPDATE_IN_PLACE};
  const std::vector<int32_t> max_page_size = {100, 200};
  const std::vector<int32_t> max_branches = {4, 16};
  const std::vector<int32_t> max_cached_pages = {64, 256};
  for (const auto& update_mode : update_modes) {
    for (const auto& max_page_size : max_page_size) {
      for (const auto& max_branches : max_branches) {
        for (const auto& max_cached_pages : max_cached_pages) {
          tkrzw::TreeDBM::TuningParameters tuning_params;
          tuning_params.update_mode = update_mode;
          tuning_params.max_page_size = max_page_size;
          tuning_params.max_branches = max_branches;
          tuning_params.max_cached_pages = max_cached_pages;
          EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->OpenAdvanced(
              file_path, true, tkrzw::File::OPEN_TRUNCATE, tuning_params));
          RandomTest(dbm, 1);
          EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Close());
        }
      }
    }
  }
}

void TreeDBMTest::TreeDBMRandomTestThread(tkrzw::TreeDBM* dbm) {
  tkrzw::TemporaryDirectory tmp_dir(true, "tkrzw-");
  const std::string file_path = tmp_dir.MakeUniquePath();
  const std::vector<tkrzw::HashDBM::UpdateMode> update_modes =
      {tkrzw::HashDBM::UPDATE_IN_PLACE};
  const std::vector<int32_t> max_page_size = {200, 300};
  const std::vector<int32_t> max_branches = {4, 16};
  const std::vector<int32_t> max_cached_pages = {256};
  for (const auto& update_mode : update_modes) {
    for (const auto& max_page_size : max_page_size) {
      for (const auto& max_branches : max_branches) {
        for (const auto& max_cached_pages : max_cached_pages) {
          tkrzw::TreeDBM::TuningParameters tuning_params;
          tuning_params.update_mode = update_mode;
          tuning_params.max_page_size = max_page_size;
          tuning_params.max_branches = max_branches;
          tuning_params.max_cached_pages = max_cached_pages;
          EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->OpenAdvanced(
              file_path, true, tkrzw::File::OPEN_TRUNCATE, tuning_params));
          RandomTestThread(dbm);
          EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Close());
        }
      }
    }
  }
}

void TreeDBMTest::TreeDBMRecordMigrationTest(tkrzw::TreeDBM* dbm, tkrzw::File* file) {
  tkrzw::TemporaryDirectory tmp_dir(true, "tkrzw-");
  const std::string file_path = tmp_dir.MakeUniquePath();
  const std::string flat_file_path = tmp_dir.MakeUniquePath();
  const std::vector<tkrzw::HashDBM::UpdateMode> update_modes =
      {tkrzw::HashDBM::UPDATE_IN_PLACE, tkrzw::HashDBM::UPDATE_APPENDING};
  for (const auto& update_mode : update_modes) {
    tkrzw::TreeDBM::TuningParameters tuning_params;
    tuning_params.update_mode = update_mode;
    tuning_params.num_buckets = 1000;
    EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->OpenAdvanced(
        file_path, true, tkrzw::File::OPEN_TRUNCATE, tuning_params));
    EXPECT_EQ(tkrzw::Status::SUCCESS, file->Open(flat_file_path, true));
    RecordMigrationTest(dbm, file);
    EXPECT_EQ(tkrzw::Status::SUCCESS, file->Close());
    EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Close());
  }
}

void TreeDBMTest::TreeDBMBackIteratorTest(tkrzw::TreeDBM* dbm) {
  tkrzw::TemporaryDirectory tmp_dir(true, "tkrzw-");
  const std::string file_path = tmp_dir.MakeUniquePath();
  tkrzw::TreeDBM::TuningParameters tuning_params;
  tuning_params.max_page_size = 100;
  tuning_params.max_branches = 2;
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->OpenAdvanced(
      file_path, true, tkrzw::File::OPEN_TRUNCATE, tuning_params));
  BackIteratorTest(dbm);
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Close());
}

void TreeDBMTest::TreeDBMIteratorBoundTest(tkrzw::TreeDBM* dbm) {
  tkrzw::TemporaryDirectory tmp_dir(true, "tkrzw-");
  const std::string file_path = tmp_dir.MakeUniquePath();
  tkrzw::TreeDBM::TuningParameters tuning_params;
  tuning_params.max_page_size = 1;
  tuning_params.max_branches = 2;
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->OpenAdvanced(
      file_path, true, tkrzw::File::OPEN_TRUNCATE, tuning_params));
  IteratorBoundTest(dbm);
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Close());
}

void TreeDBMTest::TreeDBMReorganizeTestOne(tkrzw::TreeDBM* dbm) {
  constexpr int32_t num_iterations = 100;
  constexpr int32_t num_operations = 100;
  for (int32_t i = 0; i < num_iterations; i++) {
    for (int32_t op = 1; op < num_operations; op++) {
      const int32_t uniq_num = (i * num_operations + op);
      const std::string key = tkrzw::ToString(uniq_num * uniq_num);
      const std::string value = tkrzw::ToString(uniq_num);
      EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Set(key, value));
    }
    for (int32_t op = 1; op < num_operations; op++) {
      const int32_t uniq_num = (i * num_operations + op);
      const std::string key = tkrzw::ToString(uniq_num * uniq_num);
      std::string value;
      EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Get(key, &value));
      EXPECT_EQ(tkrzw::ToString(uniq_num), value);
    }
    for (int32_t op = 1; op < num_operations; op++) {
      const int32_t uniq_num = (i * num_operations + op);
      const std::string key = tkrzw::ToString(uniq_num * uniq_num);
      std::string value;
      EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Get(key, &value));
      EXPECT_EQ(tkrzw::ToString(uniq_num), value);
      if (i % 2 == 0) {
        EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Remove(key));
        EXPECT_EQ(tkrzw::Status::NOT_FOUND_ERROR, dbm->Get(key));
      }
    }
  }
  for (int32_t i = 1; i < num_iterations; i += 2) {
    for (int32_t op = 1; op < num_operations; op++) {
      const int32_t uniq_num = (i * num_operations + op);
      const std::string key = tkrzw::ToString(uniq_num * uniq_num);
      std::string value;
      EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Get(key, &value));
      EXPECT_EQ(tkrzw::ToString(uniq_num), value);
      EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Remove(key));
      EXPECT_EQ(tkrzw::Status::NOT_FOUND_ERROR, dbm->Get(key));
    }
  }
  EXPECT_EQ(0, dbm->CountSimple());
  EXPECT_EQ(0, dbm->GetEffectiveDataSize());
}

void TreeDBMTest::TreeDBMReorganizeTestAll(tkrzw::TreeDBM* dbm) {
  tkrzw::TemporaryDirectory tmp_dir(true, "tkrzw-");
  const std::string file_path = tmp_dir.MakeUniquePath();
  const std::vector<tkrzw::HashDBM::UpdateMode> update_modes =
      {tkrzw::HashDBM::UPDATE_IN_PLACE};
  const std::vector<int32_t> max_page_size = {100};
  const std::vector<int32_t> max_branches = {2, 16};
  const std::vector<int32_t> max_cached_pages = {64};
  for (const auto& update_mode : update_modes) {
    for (const auto& max_page_size : max_page_size) {
      for (const auto& max_branches : max_branches) {
        for (const auto& max_cached_pages : max_cached_pages) {
          tkrzw::TreeDBM::TuningParameters tuning_params;
          tuning_params.update_mode = update_mode;
          tuning_params.max_page_size = max_page_size;
          tuning_params.max_branches = max_branches;
          tuning_params.max_cached_pages = max_cached_pages;
          EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->OpenAdvanced(
              file_path, true, tkrzw::File::OPEN_TRUNCATE, tuning_params));
          TreeDBMReorganizeTestOne(dbm);
          EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Close());
        }
      }
    }
  }
}

void TreeDBMTest::TreeDBMIteratorTest(tkrzw::TreeDBM* dbm) {
  tkrzw::TemporaryDirectory tmp_dir(true, "tkrzw-");
  const std::string file_path = tmp_dir.MakeUniquePath();
  tkrzw::TreeDBM::TuningParameters tuning_params;
  tuning_params.max_page_size = 100;
  tuning_params.max_branches = 2;
  tuning_params.max_cached_pages = 1;
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->OpenAdvanced(
      file_path, true, tkrzw::File::OPEN_TRUNCATE, tuning_params));
  std::vector<std::unique_ptr<tkrzw::DBM::Iterator>> iters;
  for (int32_t i = 1; i <= 100; i++) {
    const std::string key = tkrzw::SPrintF("%08d", i);
    const std::string value = tkrzw::ToString(i);
    EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Set(key, value));
    auto iter = dbm->MakeIterator();
    EXPECT_EQ(tkrzw::Status::SUCCESS, iter->Jump(key));
    std::string iter_key, iter_value;
    EXPECT_EQ(tkrzw::Status::SUCCESS, iter->Get(&iter_key, &iter_value));
    EXPECT_EQ(key, iter_key);
    EXPECT_EQ(value, iter_value);
    iters.emplace_back(std::move(iter));
  }
  EXPECT_EQ(100, dbm->CountSimple());
  auto iter = dbm->MakeIterator();
  EXPECT_EQ(tkrzw::Status::SUCCESS, iter->First());
  int32_t count = 0;
  while (true) {
    std::string iter_key, iter_value;
    const tkrzw::Status status = iter->Get(&iter_key, &iter_value);
    if (status != tkrzw::Status::SUCCESS) {
      EXPECT_EQ(tkrzw::Status::NOT_FOUND_ERROR, status);
      break;
    }
    EXPECT_EQ(count + 1, tkrzw::StrToInt(iter_key));
    EXPECT_EQ(count + 1, tkrzw::StrToInt(iter_value));
    count++;
    EXPECT_EQ(tkrzw::Status::SUCCESS, iter->Next());
  }
  EXPECT_EQ(tkrzw::Status::SUCCESS, iter->First());
  EXPECT_EQ("00000001", iter->GetKey());
  EXPECT_EQ("1", iter->GetValue());
  for (int32_t i = 1; i <= 33; i++) {
    const std::string key = tkrzw::SPrintF("%08d", i);
    EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Remove(key));
    std::string iter_key, iter_value;
    EXPECT_EQ(tkrzw::Status::SUCCESS, iter->Get(&iter_key, &iter_value));
    EXPECT_EQ(i + 1, tkrzw::StrToInt(iter_key));
    EXPECT_EQ(i + 1, tkrzw::StrToInt(iter_value));
  }
  EXPECT_EQ(67, dbm->CountSimple());
  for (int32_t i = 99; i >= 66; i--) {
    const std::string key = tkrzw::SPrintF("%08d", i);
    EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Remove(key));
  }
  EXPECT_EQ(33, dbm->CountSimple());
  EXPECT_EQ("00000034", iter->GetKey());
  EXPECT_EQ("34", iter->GetValue());
  for (size_t i = 0; i < iters.size(); i++) {
    std::string iter_key, iter_value;
    EXPECT_EQ(tkrzw::Status::SUCCESS, iters[i]->Get(&iter_key, &iter_value));
    if (i <= 33) {
      EXPECT_EQ(34, tkrzw::StrToInt(iter_key));
    } else if (i >= 65) {
      EXPECT_EQ(100, tkrzw::StrToInt(iter_key));
    } else {
      EXPECT_EQ(i + 1, tkrzw::StrToInt(iter_key));
    }
  }
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Clear());
  EXPECT_EQ(0, dbm->CountSimple());
  EXPECT_EQ("*", iter->GetKey("*"));
  EXPECT_EQ("*", iter->GetValue("*"));
  iters.clear();
  for (int32_t i = 0; i <= 100; i++) {
    const std::string key = tkrzw::ToString(i * i);
    const std::string value = tkrzw::ToString(i);
    EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Set(key, value));
    auto iter = dbm->MakeIterator();
    EXPECT_EQ(tkrzw::Status::SUCCESS, iter->Jump(key));
    std::string iter_key, iter_value;
    EXPECT_EQ(tkrzw::Status::SUCCESS, iter->Get(&iter_key, &iter_value));
    EXPECT_EQ(key, iter_key);
    EXPECT_EQ(value, iter_value);
    iters.emplace_back(std::move(iter));
  }
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Set("z", "zzz"));
  for (int32_t i = 0; i <= 50; i++) {
    const std::string key = tkrzw::ToString(i * i);
    EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Remove(key));
  }
  EXPECT_EQ(51, dbm->CountSimple());
  for (size_t i = 0; i < iters.size(); i++) {
    std::string iter_key, iter_value;
    EXPECT_EQ(tkrzw::Status::SUCCESS, iters[i]->Get(&iter_key, &iter_value));
    if (i > 50) {
      EXPECT_EQ(i * i, tkrzw::StrToInt(iter_key));
      EXPECT_EQ(i, tkrzw::StrToInt(iter_value));
    }
  }
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Close());
}

void TreeDBMTest::TreeDBMKeyComparatorTest(tkrzw::TreeDBM* dbm) {
  tkrzw::TemporaryDirectory tmp_dir(true, "tkrzw-");
  const std::string file_path = tmp_dir.MakeUniquePath();
  tkrzw::TreeDBM::TuningParameters tuning_params;
  tuning_params.key_comparator = tkrzw::LexicalCaseKeyComparator;
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->OpenAdvanced(
      file_path, true, tkrzw::File::OPEN_TRUNCATE, tuning_params));
  EXPECT_TRUE(dbm->IsOpen());
  EXPECT_TRUE(dbm->IsWritable());
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Set("a", "first"));
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Set("A", "first2"));
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Set("B", "second"));
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Set("b", "second2"));
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Set("c", "third"));
  EXPECT_EQ(3, dbm->CountSimple());
  EXPECT_EQ("first2", dbm->GetSimple("a"));
  EXPECT_EQ("second2", dbm->GetSimple("B"));
  EXPECT_EQ("third", dbm->GetSimple("C"));
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Close());
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Open(file_path, false));
  EXPECT_TRUE(dbm->IsOpen());
  EXPECT_FALSE(dbm->IsWritable());
  EXPECT_EQ(tkrzw::LexicalCaseKeyComparator, dbm->GetKeyComparator());
  EXPECT_EQ(3, dbm->CountSimple());
  EXPECT_EQ("first2", dbm->GetSimple("a"));
  EXPECT_EQ("second2", dbm->GetSimple("B"));
  EXPECT_EQ("third", dbm->GetSimple("C"));
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Close());
  tuning_params.key_comparator = tkrzw::DecimalKeyComparator;
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->OpenAdvanced(
      file_path, true, tkrzw::File::OPEN_TRUNCATE, tuning_params));
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Set("1", "first"));
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Set("001", "first2"));
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Set("002", "second"));
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Set("2", "second2"));
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Set("  3  ", "third"));
  EXPECT_EQ(3, dbm->CountSimple());
  EXPECT_EQ("first2", dbm->GetSimple("1"));
  EXPECT_EQ("second2", dbm->GetSimple("0002"));
  EXPECT_EQ("third", dbm->GetSimple("3"));
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Close());
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Open(file_path, false));
  EXPECT_EQ(tkrzw::DecimalKeyComparator, dbm->GetKeyComparator());
  EXPECT_EQ(3, dbm->CountSimple());
  EXPECT_EQ("first2", dbm->GetSimple("1"));
  EXPECT_EQ("second2", dbm->GetSimple("0002"));
  EXPECT_EQ("third", dbm->GetSimple("3"));
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Close());
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Open(file_path, true));
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Rebuild());
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Set("3\n", "third2"));
  EXPECT_EQ("third2", dbm->GetSimple("3"));
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Clear());
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Set("9", "nine"));
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Set("\t9", "nine2"));
  EXPECT_EQ("nine2", dbm->GetSimple("9"));
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Close());
}

void TreeDBMTest::TreeDBMRebuildStaticTestOne(
    tkrzw::TreeDBM* dbm, const tkrzw::TreeDBM::TuningParameters& tuning_params) {
  tkrzw::TemporaryDirectory tmp_dir(true, "tkrzw-");
  const std::string file_path = tmp_dir.MakeUniquePath();
  constexpr int32_t num_records = 1000;
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->OpenAdvanced(
      file_path, true, tkrzw::File::OPEN_TRUNCATE, tuning_params));
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->SetDatabaseType(123));
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->SetOpaqueMetadata("0123456789"));
  for (int32_t i = 0; i < num_records; i++) {
    const std::string& key = tkrzw::SPrintF("%d", i * i);
    const std::string& value = tkrzw::SPrintF("%08d", i);
    EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Set(key, value, false));
  }
  for (int32_t i = 0; i < num_records; i += 2) {
    const std::string& key = tkrzw::SPrintF("%d", i * i);
    EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Remove(key));
  }
  for (int32_t i = 0; i < num_records; i += 4) {
    const std::string& key = tkrzw::SPrintF("%d", i * i);
    const std::string& value = tkrzw::SPrintF("%08d", i);
    EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Set(key, value));
  }
  for (int32_t i = 0; i < num_records; i += 3) {
    const std::string& key = tkrzw::SPrintF("%d", i * i);
    const std::string& value = tkrzw::SPrintF("%016d", i);
    EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Set(key, value, true));
  }
  for (int32_t i = 0; i < num_records; i += 5) {
    const std::string& key = tkrzw::SPrintF("%d", i * i);
    const tkrzw::Status status =  dbm->Remove(key);
    EXPECT_TRUE(status == tkrzw::Status::SUCCESS || status == tkrzw::Status::NOT_FOUND_ERROR);
  }
  for (int32_t i = 0; i < num_records; i += 7) {
    const std::string& key = tkrzw::SPrintF("%d", i * i);
    const std::string& value = tkrzw::SPrintF("%024d", i);
    EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Set(key, value, true));
  }
  for (int32_t i = 0; i < num_records; i += 9) {
    const std::string& key = tkrzw::SPrintF("%d", i * i);
    const std::string& value = tkrzw::SPrintF("%08d", i);
    EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Set(key, value, true));
  }
  for (int32_t i = 0; i < num_records; i += 11) {
    const std::string& key = tkrzw::SPrintF("%d", i * i);
    const std::string& value = tkrzw::SPrintF("%016d", i);
    EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Set(key, value, true));
  }
  std::map<std::string, std::string> first_data;
  auto iter = dbm->MakeIterator();
  EXPECT_EQ(tkrzw::Status::SUCCESS, iter->First());
  while (true) {
    std::string key, value;
    const tkrzw::Status status = iter->Get(&key, &value);
    if (status != tkrzw::Status::SUCCESS) {
      EXPECT_EQ(tkrzw::Status::NOT_FOUND_ERROR, status);
      break;
    }
    EXPECT_TRUE(first_data.emplace(key, value).second);
    EXPECT_EQ(tkrzw::Status::SUCCESS, iter->Next());
  }
  EXPECT_EQ(first_data.size(), dbm->CountSimple());
  const int64_t first_eff_data_size = dbm->GetEffectiveDataSize();
  EXPECT_EQ(tkrzw::Status::SUCCESS, iter->First());
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Rebuild());
  EXPECT_EQ(tkrzw::Status::SUCCESS, iter->Get());
  for (const auto& record : first_data) {
    EXPECT_EQ(record.second, dbm->GetSimple(record.first));
  }
  EXPECT_EQ(first_data.size(), dbm->CountSimple());
  EXPECT_EQ(first_eff_data_size, dbm->GetEffectiveDataSize());
  EXPECT_EQ(123, dbm->GetDatabaseType());
  EXPECT_EQ("0123456789", dbm->GetOpaqueMetadata().substr(0, 10));
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Close());
}

void TreeDBMTest::TreeDBMRebuildStaticTestAll(tkrzw::TreeDBM* dbm) {
  const std::vector<tkrzw::HashDBM::UpdateMode> update_modes =
      {tkrzw::HashDBM::UPDATE_IN_PLACE, tkrzw::HashDBM::UPDATE_APPENDING};
  const std::vector<int32_t> max_page_size = {100, 200,};
  const std::vector<int32_t> max_branches = {4, 16};
  const std::vector<int32_t> max_cached_pages = {64, 256};
  for (const auto& update_mode : update_modes) {
    for (const auto& max_page_size : max_page_size) {
      for (const auto& max_branches : max_branches) {
        for (const auto& max_cached_pages : max_cached_pages) {
          tkrzw::TreeDBM::TuningParameters tuning_params;
          tuning_params.update_mode = update_mode;
          tuning_params.max_page_size = max_page_size;
          tuning_params.max_branches = max_branches;
          tuning_params.max_cached_pages = max_cached_pages;
          TreeDBMRebuildStaticTestOne(dbm, tuning_params);
        }
      }
    }
  }
}

void TreeDBMTest::TreeDBMRebuildRandomTest(tkrzw::TreeDBM* dbm) {
  tkrzw::TemporaryDirectory tmp_dir(true, "tkrzw-");
  const std::string file_path = tmp_dir.MakeUniquePath();
  const std::vector<tkrzw::HashDBM::UpdateMode> update_modes =
      {tkrzw::HashDBM::UPDATE_IN_PLACE};
  const std::vector<int32_t> max_page_size = {200};
  const std::vector<int32_t> max_branches = {16};
  const std::vector<int32_t> max_cached_pages = {256};
  for (const auto& update_mode : update_modes) {
    for (const auto& max_page_size : max_page_size) {
      for (const auto& max_branches : max_branches) {
        for (const auto& max_cached_pages : max_cached_pages) {
          tkrzw::TreeDBM::TuningParameters tuning_params;
          tuning_params.update_mode = update_mode;
          tuning_params.max_page_size = max_page_size;
          tuning_params.max_branches = max_branches;
          tuning_params.max_cached_pages = max_cached_pages;
          EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->OpenAdvanced(
              file_path, true, tkrzw::File::OPEN_TRUNCATE, tuning_params));
          RebuildRandomTest(dbm);
          EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Close());
        }
      }
    }
  }
}

void TreeDBMTest::TreeDBMRestoreTest(tkrzw::TreeDBM* dbm) {
  tkrzw::TemporaryDirectory tmp_dir(true, "tkrzw-");
  const std::string old_file_path = tmp_dir.MakeUniquePath();
  const std::string new_file_path = tmp_dir.MakeUniquePath();
  const std::vector<tkrzw::HashDBM::UpdateMode> update_modes =
      {tkrzw::HashDBM::UPDATE_IN_PLACE, tkrzw::HashDBM::UPDATE_APPENDING};
  constexpr int32_t num_records = 100;
  for (const auto& update_mode : update_modes) {
    tkrzw::RemoveFile(new_file_path);
    tkrzw::TreeDBM::TuningParameters tuning_params;
    tuning_params.update_mode = update_mode;
    tuning_params.offset_width = 3;
    tuning_params.align_pow = 1;
    tuning_params.num_buckets = 1234;
    tuning_params.max_page_size = 100;
    tuning_params.max_branches = 3;
    EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->OpenAdvanced(
        old_file_path, true, tkrzw::File::OPEN_TRUNCATE, tuning_params));
    EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->SetDatabaseType(123));
    EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->SetOpaqueMetadata("0123456789"));
    for (int32_t i = 0; i < num_records; i++) {
      const std::string key = tkrzw::ToString(i * i);
      const std::string value = tkrzw::ToString(i);
      EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Set(key, value));
    }
    EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Synchronize(false));
    EXPECT_EQ(tkrzw::Status::SUCCESS, tkrzw::TreeDBM::RestoreDatabase(
        old_file_path, new_file_path, 0));
    EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Close());
    tkrzw::TreeDBM new_dbm;
    EXPECT_EQ(tkrzw::Status::SUCCESS, new_dbm.Open(new_file_path, false));
    EXPECT_TRUE(new_dbm.IsHealthy());
    EXPECT_EQ(123, new_dbm.GetDatabaseType());
    EXPECT_EQ("0123456789", new_dbm.GetOpaqueMetadata().substr(0, 10));
    EXPECT_EQ(num_records, new_dbm.CountSimple());
    for (int32_t i = 0; i < 100; i++) {
      const std::string key = tkrzw::ToString(i * i);
      const std::string value = tkrzw::ToString(i);
      EXPECT_EQ(value, new_dbm.GetSimple(key));
    }
    EXPECT_EQ(tkrzw::Status::SUCCESS, new_dbm.Close());
  }
}

TEST_F(TreeDBMTest, EmptyDatabase) {
  tkrzw::TreeDBM dbm(std::make_unique<tkrzw::MemoryMapParallelFile>());
  TreeDBMEmptyDatabaseTest(&dbm);
}

TEST_F(TreeDBMTest, File) {
  tkrzw::TreeDBM dbm(std::make_unique<tkrzw::MemoryMapParallelFile>());
  TreeDBMFileTest(&dbm);
}

TEST_F(TreeDBMTest, LargeRecord) {
  tkrzw::TreeDBM dbm(std::make_unique<tkrzw::MemoryMapParallelFile>());
  TreeDBMLargeRecordTest(&dbm);
}

TEST_F(TreeDBMTest, Basic) {
  tkrzw::TreeDBM dbm(std::make_unique<tkrzw::MemoryMapParallelFile>());
  TreeDBMBasicTest(&dbm);
}

TEST_F(TreeDBMTest, Append) {
  tkrzw::TreeDBM dbm(std::make_unique<tkrzw::MemoryMapParallelFile>());
  TreeDBMAppendTest(&dbm);
}

TEST_F(TreeDBMTest, Process) {
  tkrzw::TreeDBM dbm(std::make_unique<tkrzw::MemoryMapParallelFile>());
  TreeDBMProcessTest(&dbm);
}

TEST_F(TreeDBMTest, ProcessEach) {
  tkrzw::TreeDBM dbm(std::make_unique<tkrzw::MemoryMapParallelFile>());
  TreeDBMProcessEachTest(&dbm);
}

TEST_F(TreeDBMTest, Random) {
  tkrzw::TreeDBM dbm(std::make_unique<tkrzw::MemoryMapParallelFile>());
  TreeDBMRandomTestOne(&dbm);
}

TEST_F(TreeDBMTest, RandomThread) {
  tkrzw::TreeDBM dbm(std::make_unique<tkrzw::MemoryMapParallelFile>());
  TreeDBMRandomTestThread(&dbm);
}

TEST_F(TreeDBMTest, RecordMigration) {
  tkrzw::TreeDBM dbm(std::make_unique<tkrzw::MemoryMapParallelFile>());
  tkrzw::MemoryMapParallelFile file;
  TreeDBMRecordMigrationTest(&dbm, &file);
}

TEST_F(TreeDBMTest, BackIterator) {
  tkrzw::TreeDBM dbm(std::make_unique<tkrzw::MemoryMapParallelFile>());
  TreeDBMBackIteratorTest(&dbm);
}

TEST_F(TreeDBMTest, IteratorBound) {
  tkrzw::TreeDBM dbm(std::make_unique<tkrzw::MemoryMapParallelFile>());
  TreeDBMIteratorBoundTest(&dbm);
}

TEST_F(TreeDBMTest, Reorganize) {
  tkrzw::TreeDBM dbm(std::make_unique<tkrzw::MemoryMapParallelFile>());
  TreeDBMReorganizeTestAll(&dbm);
}

TEST_F(TreeDBMTest, Iterator) {
  tkrzw::TreeDBM dbm(std::make_unique<tkrzw::MemoryMapParallelFile>());
  TreeDBMIteratorTest(&dbm);
}

TEST_F(TreeDBMTest, KeyComparator) {
  tkrzw::TreeDBM dbm(std::make_unique<tkrzw::MemoryMapParallelFile>());
  TreeDBMKeyComparatorTest(&dbm);
}

TEST_F(TreeDBMTest, RebuildStatic) {
  tkrzw::TreeDBM dbm(std::make_unique<tkrzw::MemoryMapParallelFile>());
  TreeDBMRebuildStaticTestAll(&dbm);
}

TEST_F(TreeDBMTest, RebuildRandom) {
  tkrzw::TreeDBM dbm(std::make_unique<tkrzw::MemoryMapParallelFile>());
  TreeDBMRebuildRandomTest(&dbm);
}

TEST_F(TreeDBMTest, Restore) {
  tkrzw::TreeDBM dbm(std::make_unique<tkrzw::MemoryMapParallelFile>());
  TreeDBMRestoreTest(&dbm);
}

// END OF FILE
