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

#include "tkrzw_sys_config.h"

#include "gtest/gtest.h"
#include "gmock/gmock.h"

#include "tkrzw_dbm.h"
#include "tkrzw_dbm_tree.h"
#include "tkrzw_dbm_tree_impl.h"
#include "tkrzw_dbm_test_common.h"
#include "tkrzw_dbm_ulog.h"
#include "tkrzw_file.h"
#include "tkrzw_file_mmap.h"
#include "tkrzw_file_pos.h"
#include "tkrzw_file_std.h"
#include "tkrzw_file_util.h"
#include "tkrzw_lib_common.h"
#include "tkrzw_str_util.h"

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
  void TreeDBMProcessMultiTest(tkrzw::TreeDBM* dbm);
  void TreeDBMProcessEachTest(tkrzw::TreeDBM* dbm);
  void TreeDBMRandomTestOne(tkrzw::TreeDBM* dbm);
  void TreeDBMRandomTestThread(tkrzw::TreeDBM* dbm);
  void TreeDBMRecordMigrationTest(tkrzw::TreeDBM* dbm, tkrzw::File* file);
  void TreeDBMBackIteratorTest(tkrzw::TreeDBM* dbm);
  void TreeDBMIteratorBoundTest(tkrzw::TreeDBM* dbm);
  void TreeDBMQueueTest(tkrzw::TreeDBM* dbm);
  void TreeDBMReorganizeTestOne(tkrzw::TreeDBM* dbm);
  void TreeDBMReorganizeTestAll(tkrzw::TreeDBM* dbm);
  void TreeDBMIteratorTest(tkrzw::TreeDBM* dbm);
  void TreeDBMKeyComparatorTest(tkrzw::TreeDBM* dbm);
  void TreeDBMVariousSizeTest(tkrzw::TreeDBM* dbm);
  void TreeDBMRebuildStaticTestOne(
      tkrzw::TreeDBM* dbm, const tkrzw::TreeDBM::TuningParameters& tuning_params);
  void TreeDBMRebuildStaticTestAll(tkrzw::TreeDBM* dbm);
  void TreeDBMRebuildRandomTest(tkrzw::TreeDBM* dbm);
  void TreeDBMRestoreTest(tkrzw::TreeDBM* dbm);
  void TreeDBMAutoRestoreTest(tkrzw::TreeDBM* dbm);
  void TreeDBMCorruptionTest(tkrzw::TreeDBM* dbm);
  void TreeDBMPageUpdateTest(tkrzw::TreeDBM* dbm);
  void TreeDBMDirectIOTest(tkrzw::TreeDBM* dbm);
  void TreeDBMUpdateLoggerTest(tkrzw::TreeDBM* dbm);
};

void TreeDBMTest::TreeDBMEmptyDatabaseTest(tkrzw::TreeDBM* dbm) {
  tkrzw::TemporaryDirectory tmp_dir(true, "tkrzw-");
  const std::string file_path = tmp_dir.MakeUniquePath();
  EXPECT_EQ(typeid(tkrzw::TreeDBM), static_cast<tkrzw::DBM*>(dbm)->GetType());
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Open(file_path, true));
  EXPECT_TRUE(dbm->IsHealthy());
  EXPECT_FALSE(dbm->IsAutoRestored());
  EXPECT_GT(dbm->GetFileSizeSimple(), 0);
  EXPECT_GT(dbm->GetTimestampSimple(), 0);
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->SetDatabaseType(123));
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->SetOpaqueMetadata("0123456789"));
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Close());
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Open(file_path, false));
  EXPECT_EQ(123, dbm->GetDatabaseType());
  EXPECT_TRUE(dbm->IsHealthy());
  EXPECT_FALSE(dbm->IsAutoRestored());
  const int64_t file_size = dbm->GetFileSizeSimple();
  EXPECT_GT(file_size, 0);
  EXPECT_GT(dbm->GetTimestampSimple(), 0);
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
  const std::vector<tkrzw::HashDBM::RecordCompressionMode> record_comp_modes =
      {tkrzw::HashDBM::RECORD_COMP_NONE, tkrzw::HashDBM::RECORD_COMP_ZSTD};
  const std::vector<int32_t> max_page_size = {200};
  const std::vector<int32_t> max_branches = {5};
  const std::vector<int32_t> max_cached_pages = {64};
  for (const auto& update_mode : update_modes) {
    for (const auto& record_comp_mode : record_comp_modes) {
      if (!tkrzw::HashDBM::CheckRecordCompressionModeIsSupported(record_comp_mode)) {
        continue;
      }
      for (const auto& max_page_size : max_page_size) {
        for (const auto& max_branches : max_branches) {
          for (const auto& max_cached_pages : max_cached_pages) {
            tkrzw::TreeDBM::TuningParameters tuning_params;
            tuning_params.update_mode = update_mode;
            tuning_params.record_crc_mode = tkrzw::HashDBM::RECORD_CRC_8;
            tuning_params.record_comp_mode = record_comp_mode;
            tuning_params.restore_mode = tkrzw::HashDBM::RESTORE_READ_ONLY;
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
}

void TreeDBMTest::TreeDBMBasicTest(tkrzw::TreeDBM* dbm) {
  tkrzw::TemporaryDirectory tmp_dir(true, "tkrzw-");
  const std::string file_path = tmp_dir.MakeUniquePath();
  const std::vector<tkrzw::HashDBM::UpdateMode> update_modes =
      {tkrzw::HashDBM::UPDATE_IN_PLACE, tkrzw::HashDBM::UPDATE_APPENDING};
  const std::vector<tkrzw::HashDBM::RecordCompressionMode> record_comp_modes =
      {tkrzw::HashDBM::RECORD_COMP_NONE, tkrzw::HashDBM::RECORD_COMP_ZSTD};
  const std::vector<int32_t> max_page_size = {100, 200, 400};
  const std::vector<int32_t> max_branches = {2, 16};
  const std::vector<int32_t> max_cached_pages = {1, 64, 256};
  const std::vector<tkrzw::TreeDBM::PageUpdateMode> page_update_modes =
      {tkrzw::TreeDBM::PAGE_UPDATE_NONE, tkrzw::TreeDBM::PAGE_UPDATE_WRITE};
  for (const auto& update_mode : update_modes) {
    for (const auto& record_comp_mode : record_comp_modes) {
      if (!tkrzw::HashDBM::CheckRecordCompressionModeIsSupported(record_comp_mode)) {
        continue;
      }
      for (const auto& max_page_size : max_page_size) {
        for (const auto& max_branches : max_branches) {
          for (const auto& max_cached_pages : max_cached_pages) {
            for (const auto& page_update_mode : page_update_modes) {
              tkrzw::TreeDBM::TuningParameters tuning_params;
              tuning_params.update_mode = update_mode;
              tuning_params.record_crc_mode = tkrzw::HashDBM::RECORD_CRC_8;
              tuning_params.record_comp_mode = record_comp_mode;
              tuning_params.restore_mode = tkrzw::HashDBM::RESTORE_READ_ONLY;
              tuning_params.max_page_size = max_page_size;
              tuning_params.max_branches = max_branches;
              tuning_params.max_cached_pages = max_cached_pages;
              tuning_params.page_update_mode = page_update_mode;
              EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->OpenAdvanced(
                  file_path, true, tkrzw::File::OPEN_TRUNCATE, tuning_params));
              BasicTest(dbm);
              EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Close());
            }
          }
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
  const std::vector<tkrzw::HashDBM::RecordCompressionMode> record_comp_modes =
      {tkrzw::HashDBM::RECORD_COMP_NONE, tkrzw::HashDBM::RECORD_COMP_ZSTD};
  const std::vector<int32_t> max_page_size = {200};
  const std::vector<int32_t> max_branches = {16};
  const std::vector<int32_t> max_cached_pages = {64};
  for (const auto& update_mode : update_modes) {
    for (const auto& record_comp_mode : record_comp_modes) {
      if (!tkrzw::HashDBM::CheckRecordCompressionModeIsSupported(record_comp_mode)) {
        continue;
      }
      for (const auto& max_page_size : max_page_size) {
        for (const auto& max_branches : max_branches) {
          for (const auto& max_cached_pages : max_cached_pages) {
            tkrzw::TreeDBM::TuningParameters tuning_params;
            tuning_params.update_mode = update_mode;
            tuning_params.record_crc_mode = tkrzw::HashDBM::RECORD_CRC_8;
            tuning_params.record_comp_mode = record_comp_mode;
            tuning_params.restore_mode = tkrzw::HashDBM::RESTORE_READ_ONLY;
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
}

void TreeDBMTest::TreeDBMProcessTest(tkrzw::TreeDBM* dbm) {
  tkrzw::TemporaryDirectory tmp_dir(true, "tkrzw-");
  const std::string file_path = tmp_dir.MakeUniquePath();
  const std::vector<tkrzw::HashDBM::UpdateMode> update_modes =
      {tkrzw::HashDBM::UPDATE_IN_PLACE, tkrzw::HashDBM::UPDATE_APPENDING};
  const std::vector<tkrzw::HashDBM::RecordCompressionMode> record_comp_modes =
      {tkrzw::HashDBM::RECORD_COMP_NONE, tkrzw::HashDBM::RECORD_COMP_ZSTD};
  const std::vector<int32_t> max_page_size = {100, 200, 400};
  const std::vector<int32_t> max_branches = {2, 5, 16};
  const std::vector<int32_t> max_cached_pages = {1, 64, 256};
  for (const auto& update_mode : update_modes) {
    for (const auto& record_comp_mode : record_comp_modes) {
      if (!tkrzw::HashDBM::CheckRecordCompressionModeIsSupported(record_comp_mode)) {
        continue;
      }
      for (const auto& max_page_size : max_page_size) {
        for (const auto& max_branches : max_branches) {
          for (const auto& max_cached_pages : max_cached_pages) {
            tkrzw::TreeDBM::TuningParameters tuning_params;
            tuning_params.update_mode = update_mode;
            tuning_params.record_crc_mode = tkrzw::HashDBM::RECORD_CRC_8;
            tuning_params.record_comp_mode = record_comp_mode;
            tuning_params.restore_mode = tkrzw::HashDBM::RESTORE_READ_ONLY;
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
}

void TreeDBMTest::TreeDBMProcessMultiTest(tkrzw::TreeDBM* dbm) {
  tkrzw::TemporaryDirectory tmp_dir(true, "tkrzw-");
  const std::string file_path = tmp_dir.MakeUniquePath();
  const std::vector<tkrzw::HashDBM::UpdateMode> update_modes =
      {tkrzw::HashDBM::UPDATE_IN_PLACE, tkrzw::HashDBM::UPDATE_APPENDING};
  const std::vector<tkrzw::HashDBM::RecordCompressionMode> record_comp_modes =
      {tkrzw::HashDBM::RECORD_COMP_NONE, tkrzw::HashDBM::RECORD_COMP_ZSTD};
  const std::vector<int32_t> max_page_size = {100};
  const std::vector<int32_t> max_branches = {5};
  const std::vector<int32_t> max_cached_pages = {64};
  for (const auto& update_mode : update_modes) {
    for (const auto& record_comp_mode : record_comp_modes) {
      if (!tkrzw::HashDBM::CheckRecordCompressionModeIsSupported(record_comp_mode)) {
        continue;
      }
      for (const auto& max_page_size : max_page_size) {
        for (const auto& max_branches : max_branches) {
          for (const auto& max_cached_pages : max_cached_pages) {
            tkrzw::TreeDBM::TuningParameters tuning_params;
            tuning_params.update_mode = update_mode;
            tuning_params.record_crc_mode = tkrzw::HashDBM::RECORD_CRC_8;
            tuning_params.record_comp_mode = record_comp_mode;
            tuning_params.restore_mode = tkrzw::HashDBM::RESTORE_READ_ONLY;
            tuning_params.max_page_size = max_page_size;
            tuning_params.max_branches = max_branches;
            tuning_params.max_cached_pages = max_cached_pages;
            EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->OpenAdvanced(
                file_path, true, tkrzw::File::OPEN_TRUNCATE, tuning_params));
            ProcessMultiTest(dbm);
            EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Close());
          }
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
  const std::vector<tkrzw::HashDBM::RecordCompressionMode> record_comp_modes =
      {tkrzw::HashDBM::RECORD_COMP_NONE, tkrzw::HashDBM::RECORD_COMP_ZSTD};
  const std::vector<int32_t> max_page_size = {100, 200};
  const std::vector<int32_t> max_branches = {2, 16};
  const std::vector<int32_t> max_cached_pages = {64, 256};
  const std::vector<tkrzw::TreeDBM::PageUpdateMode> page_update_modes =
      {tkrzw::TreeDBM::PAGE_UPDATE_NONE, tkrzw::TreeDBM::PAGE_UPDATE_WRITE};
  for (const auto& update_mode : update_modes) {
    for (const auto& record_comp_mode : record_comp_modes) {
      if (!tkrzw::HashDBM::CheckRecordCompressionModeIsSupported(record_comp_mode)) {
        continue;
      }
      for (const auto& max_page_size : max_page_size) {
        for (const auto& max_branches : max_branches) {
          for (const auto& max_cached_pages : max_cached_pages) {
            for (const auto& page_update_mode : page_update_modes) {
              tkrzw::TreeDBM::TuningParameters tuning_params;
              tuning_params.update_mode = update_mode;
              tuning_params.record_crc_mode = tkrzw::HashDBM::RECORD_CRC_8;
              tuning_params.record_comp_mode = record_comp_mode;
              tuning_params.restore_mode = tkrzw::HashDBM::RESTORE_READ_ONLY;
              tuning_params.max_page_size = max_page_size;
              tuning_params.max_branches = max_branches;
              tuning_params.max_cached_pages = max_cached_pages;
              tuning_params.page_update_mode = page_update_mode;
              EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->OpenAdvanced(
                  file_path, true, tkrzw::File::OPEN_TRUNCATE, tuning_params));
              ProcessEachTest(dbm);
              EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Close());
            }
          }
        }
      }
    }
  }
}

void TreeDBMTest::TreeDBMRandomTestOne(tkrzw::TreeDBM* dbm) {
  tkrzw::TemporaryDirectory tmp_dir(true, "tkrzw-");
  const std::string file_path = tmp_dir.MakeUniquePath();
  const std::vector<tkrzw::HashDBM::UpdateMode> update_modes =
      {tkrzw::HashDBM::UPDATE_IN_PLACE, tkrzw::HashDBM::UPDATE_APPENDING};
  const std::vector<int32_t> max_page_size = {100, 200};
  const std::vector<int32_t> max_branches = {4, 16};
  const std::vector<int32_t> max_cached_pages = {64, 256};
  for (const auto& update_mode : update_modes) {
    for (const auto& max_page_size : max_page_size) {
      for (const auto& max_branches : max_branches) {
        for (const auto& max_cached_pages : max_cached_pages) {
          tkrzw::TreeDBM::TuningParameters tuning_params;
          tuning_params.update_mode = update_mode;
          tuning_params.record_crc_mode = tkrzw::HashDBM::RECORD_CRC_8;
          tuning_params.restore_mode = tkrzw::HashDBM::RESTORE_READ_ONLY;
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
          tuning_params.record_crc_mode = tkrzw::HashDBM::RECORD_CRC_8;
          tuning_params.restore_mode = tkrzw::HashDBM::RESTORE_READ_ONLY;
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
    tuning_params.record_crc_mode = tkrzw::HashDBM::RECORD_CRC_8;
    tuning_params.num_buckets = 1000;
    tuning_params.restore_mode = tkrzw::HashDBM::RESTORE_READ_ONLY;
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
  tuning_params.restore_mode = tkrzw::HashDBM::RESTORE_READ_ONLY;
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
  tuning_params.restore_mode = tkrzw::HashDBM::RESTORE_READ_ONLY;
  tuning_params.max_page_size = 1;
  tuning_params.max_branches = 2;
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->OpenAdvanced(
      file_path, true, tkrzw::File::OPEN_TRUNCATE, tuning_params));
  IteratorBoundTest(dbm);
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Close());
}

void TreeDBMTest::TreeDBMQueueTest(tkrzw::TreeDBM* dbm) {
  tkrzw::TemporaryDirectory tmp_dir(true, "tkrzw-");
  const std::string file_path = tmp_dir.MakeUniquePath();
  tkrzw::TreeDBM::TuningParameters tuning_params;
  tuning_params.restore_mode = tkrzw::HashDBM::RESTORE_READ_ONLY;
  tuning_params.max_page_size = 64;
  tuning_params.max_branches = 4;
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->OpenAdvanced(
      file_path, true, tkrzw::File::OPEN_TRUNCATE, tuning_params));
  QueueTest(dbm);
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
          tuning_params.record_crc_mode = tkrzw::HashDBM::RECORD_CRC_8;
          tuning_params.restore_mode = tkrzw::HashDBM::RESTORE_READ_ONLY;
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
  tuning_params.restore_mode = tkrzw::HashDBM::RESTORE_READ_ONLY;
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
  tuning_params.restore_mode = tkrzw::HashDBM::RESTORE_READ_ONLY;
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

void TreeDBMTest::TreeDBMVariousSizeTest(tkrzw::TreeDBM* dbm) {
  tkrzw::TemporaryDirectory tmp_dir(true, "tkrzw-");
  const std::string file_path = tmp_dir.MakeUniquePath();
  constexpr int32_t num_iterations = 300;
  constexpr int32_t max_value_size = 0x20000;
  std::string value_buf;
  value_buf.reserve(max_value_size);
  while (value_buf.size() < max_value_size) {
    value_buf.append(1, 'a' + value_buf.size() % 26);
    value_buf.append(1, 'a' + value_buf.size() % 25);
  }
  const int32_t value_sizes[] = {
    0, 1, 0x7D, 0x7E, 0x7F, 0x80, 0xEC, 0xED, 0xEE, 0xEF, 0xFD, 0xFE, 0xFF, 0x100,
    0x3FFE, 0x3FFF, 0x4000};
  const std::vector<tkrzw::HashDBM::UpdateMode> update_modes =
      {tkrzw::HashDBM::UPDATE_IN_PLACE, tkrzw::HashDBM::UPDATE_APPENDING};
  const std::vector<int32_t> align_pows = {0, 4, 10};
  const std::vector<tkrzw::HashDBM::RecordCRCMode> record_crc_modes =
      {tkrzw::HashDBM::RECORD_CRC_NONE, tkrzw::HashDBM::RECORD_CRC_32};
  const std::vector<tkrzw::HashDBM::RecordCompressionMode> record_comp_modes =
      {tkrzw::HashDBM::RECORD_COMP_NONE, tkrzw::HashDBM::RECORD_COMP_ZSTD};
  for (const auto& update_mode : update_modes) {
    for (const auto& align_pow : align_pows) {
      for (const auto& record_crc_mode : record_crc_modes) {
        for (const auto& record_comp_mode : record_comp_modes) {
          if (!tkrzw::HashDBM::CheckRecordCompressionModeIsSupported(record_comp_mode)) {
            continue;
          }
          tkrzw::TreeDBM::TuningParameters tuning_params;
          tuning_params.update_mode = update_mode;
          tuning_params.record_crc_mode = record_crc_mode;
          tuning_params.record_comp_mode = record_comp_mode;
          tuning_params.align_pow = align_pow;
          tuning_params.num_buckets = 100;
          tuning_params.max_page_size = 65536;
          tuning_params.max_cached_pages = 64;
          EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->OpenAdvanced(
              file_path, true, tkrzw::File::OPEN_TRUNCATE, tuning_params));
          for (int32_t value_size : value_sizes) {
            const std::string key = tkrzw::ToString(value_size);
            const std::string_view value(value_buf.data(), value_size);
            EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Set(key, value));
            std::string rec_value;
            EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Get(key, &rec_value));
            EXPECT_EQ(value, rec_value);
            EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Set(key, ""));
            EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Get(key, &rec_value));
            EXPECT_EQ("", rec_value);
          }
          EXPECT_EQ(std::size(value_sizes), dbm->CountSimple());
          auto iter = dbm->MakeIterator();
          EXPECT_EQ(tkrzw::Status::SUCCESS, iter->First());
          int32_t count = 0;
          while (true) {
            std::string key, value;
            const tkrzw::Status status = iter->Get(&key, &value);
            if (status != tkrzw::Status::SUCCESS) {
              EXPECT_EQ(tkrzw::Status::NOT_FOUND_ERROR, status);
              break;
            }
            EXPECT_EQ("", value);
            EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Get(key, &value));
            EXPECT_EQ("", value);
            count++;
            EXPECT_EQ(tkrzw::Status::SUCCESS, iter->Next());
          }
          EXPECT_EQ(std::size(value_sizes), count);
          for (int32_t value_size : value_sizes) {
            const std::string key = tkrzw::ToString(value_size);
            const std::string_view value(value_buf.data(), value_size);
            EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Set(key, value));
            std::string rec_value;
            EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Get(key, &rec_value));
            EXPECT_EQ(value, rec_value);
            const std::string_view value_wide(value_buf.data(), value_size * 2);
            EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Set(key, value_wide));
            EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Get(key, &rec_value));
            EXPECT_EQ(value_wide, rec_value);
          }
          EXPECT_EQ(std::size(value_sizes), dbm->CountSimple());
          EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Close());
          EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->OpenAdvanced(
              file_path, true, tkrzw::File::OPEN_TRUNCATE, tuning_params));
          int64_t value_size = 0;
          for (int32_t i = 1; i <= num_iterations; i++) {
            const std::string key = tkrzw::ToString(i * i * i);
            const std::string_view value(value_buf.data(), value_size);
            EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Set(key, value));
            value_size = std::ceil((value_size + 1) * 1.2);
            if (static_cast<size_t>(value_size) > value_buf.size()) {
              value_size = 0;
            }
          }
          value_size = 0;
          for (int32_t i = 1; i <= num_iterations; i++) {
            const std::string key = tkrzw::ToString(i * i * i);
            const std::string_view value(value_buf.data(), value_size);
            EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Set(key, value));
            value_size = std::ceil((value_size + 1) * 1.4);
            if (static_cast<size_t>(value_size) > value_buf.size()) {
              value_size = 0;
            }
          }
          EXPECT_EQ(num_iterations, dbm->CountSimple());
          value_size = 0;
          for (int32_t i = 1; i <= num_iterations; i++) {
            const std::string key = tkrzw::ToString(i * i * i);
            const std::string_view value(value_buf.data(), value_size);
            std::string rec_value;
            EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Get(key, &rec_value));
            EXPECT_EQ(value, rec_value);
            if (i % 2 == 0) {
              rec_value.clear();
              EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Remove(key, &rec_value));
              EXPECT_EQ(value, rec_value);
            }
            value_size = std::ceil((value_size + 1) * 1.4);
            if (static_cast<size_t>(value_size) > value_buf.size()) {
              value_size = 0;
            }
          }
          EXPECT_EQ(num_iterations / 2, dbm->CountSimple());
          iter = dbm->MakeIterator();
          EXPECT_EQ(tkrzw::Status::SUCCESS, iter->First());
          count = 0;
          while (true) {
            std::string key, value;
            const tkrzw::Status status = iter->Get(&key, &value);
            if (status != tkrzw::Status::SUCCESS) {
              EXPECT_EQ(tkrzw::Status::NOT_FOUND_ERROR, status);
              break;
            }
            std::string rec_value;
            EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Get(key, &rec_value));
            EXPECT_EQ(value, rec_value);
            count++;
            EXPECT_EQ(tkrzw::Status::SUCCESS, iter->Next());
          }
          EXPECT_EQ(num_iterations / 2, count);
          EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Close());
        }
      }
    }
  }
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
          tuning_params.record_crc_mode = tkrzw::HashDBM::RECORD_CRC_8;
          tuning_params.restore_mode = tkrzw::HashDBM::RESTORE_READ_ONLY;
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
          tuning_params.record_crc_mode = tkrzw::HashDBM::RECORD_CRC_8;
          tuning_params.restore_mode = tkrzw::HashDBM::RESTORE_READ_ONLY;
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
    tuning_params.record_crc_mode = tkrzw::HashDBM::RECORD_CRC_8;
    tuning_params.offset_width = 3;
    tuning_params.align_pow = 1;
    tuning_params.num_buckets = 1234;
    tuning_params.restore_mode = tkrzw::HashDBM::RESTORE_READ_ONLY;
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
    EXPECT_FALSE(new_dbm.IsAutoRestored());
    EXPECT_EQ(123, new_dbm.GetDatabaseType());
    EXPECT_EQ("0123456789", new_dbm.GetOpaqueMetadata().substr(0, 10));
    EXPECT_EQ(num_records, new_dbm.CountSimple());
    EXPECT_EQ(tkrzw::Status::SUCCESS, new_dbm.ValidateHashBuckets());
    EXPECT_EQ(tkrzw::Status::SUCCESS, new_dbm.ValidateRecords(-1, -1));
    for (int32_t i = 0; i < 100; i++) {
      const std::string key = tkrzw::ToString(i * i);
      const std::string value = tkrzw::ToString(i);
      EXPECT_EQ(value, new_dbm.GetSimple(key));
    }
    EXPECT_EQ(tkrzw::Status::SUCCESS, new_dbm.Close());
  }
  const std::string first_file_path = tmp_dir.MakeUniquePath();
  const std::string second_file_path = tmp_dir.MakeUniquePath();
  constexpr int32_t num_iterations = 10000;
  for (const auto& update_mode : update_modes) {
    tkrzw::TreeDBM::TuningParameters tuning_params;
    tuning_params.update_mode = update_mode;
    tuning_params.record_crc_mode = tkrzw::HashDBM::RECORD_CRC_8;
    tuning_params.offset_width = 3;
    tuning_params.num_buckets = 1234;
    tuning_params.restore_mode = tkrzw::HashDBM::RESTORE_READ_ONLY;
    tuning_params.max_page_size = 200;
    tuning_params.max_branches = 5;
    tkrzw::TreeDBM second_dbm;
    EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->OpenAdvanced(
        first_file_path, true, tkrzw::File::OPEN_TRUNCATE, tuning_params));
    std::mt19937 mt(1);
    std::uniform_int_distribution<int32_t> num_dist(1, num_iterations);
    std::uniform_int_distribution<int32_t> op_dist(0, tkrzw::INT32MAX);
    for (int32_t i = 0; i < num_iterations; i++) {
      const int32_t num_key = num_dist(mt) / 2;
      const std::string& key = tkrzw::ToString(num_key);
      const int32_t num_value = num_dist(mt);
      const std::string& value = tkrzw::ToString(num_value * num_value);
      if (op_dist(mt) % 5 == 0) {
        const tkrzw::Status status = dbm->Remove(key);
        EXPECT_TRUE(status == tkrzw::Status::SUCCESS ||
                    status == tkrzw::Status::NOT_FOUND_ERROR);
      } else if (op_dist(mt) % 3 == 0) {
        const tkrzw::Status status = dbm->Append(key, value, ":");
        EXPECT_TRUE(status == tkrzw::Status::SUCCESS);
      } else {
        EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Set(key, value));
      }
    }
    EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Close());
    EXPECT_EQ(tkrzw::Status::SUCCESS,
              tkrzw::HashDBM::RestoreDatabase(first_file_path, second_file_path, -1));
    EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Open(first_file_path, false));
    EXPECT_EQ(tkrzw::Status::SUCCESS, second_dbm.Open(second_file_path, false));
    EXPECT_TRUE(second_dbm.IsHealthy());
    EXPECT_EQ(dbm->CountSimple(), second_dbm.CountSimple());
    EXPECT_EQ(tkrzw::Status::SUCCESS, second_dbm.ValidateHashBuckets());
    EXPECT_EQ(tkrzw::Status::SUCCESS, second_dbm.ValidateRecords(-1, -1));
    int64_t count = 0;
    auto iter = second_dbm.MakeIterator();
    EXPECT_EQ(tkrzw::Status::SUCCESS, iter->First());
    while (true) {
      std::string key, value;
      tkrzw::Status status = iter->Get(&key, &value);
      if (status != tkrzw::Status::SUCCESS) {
        EXPECT_EQ(tkrzw::Status::NOT_FOUND_ERROR, status);
        break;
      }
      count++;
      EXPECT_EQ(dbm->GetSimple(key), value);
      EXPECT_EQ(tkrzw::Status::SUCCESS, iter->Next());
    }
    EXPECT_EQ(dbm->CountSimple(), count);
    EXPECT_EQ(tkrzw::Status::SUCCESS, second_dbm.Close());
    EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Close());
    EXPECT_EQ(tkrzw::Status::SUCCESS, tkrzw::RemoveFile(second_file_path));
  }
}

void TreeDBMTest::TreeDBMAutoRestoreTest(tkrzw::TreeDBM* dbm) {
  tkrzw::TemporaryDirectory tmp_dir(true, "tkrzw-");
  const std::string file_path = tmp_dir.MakeUniquePath();
  const std::vector<tkrzw::HashDBM::UpdateMode> update_modes =
      {tkrzw::HashDBM::UPDATE_IN_PLACE, tkrzw::HashDBM::UPDATE_APPENDING};
  const std::vector<tkrzw::HashDBM::RecordCompressionMode> record_comp_modes =
      {tkrzw::HashDBM::RECORD_COMP_NONE, tkrzw::HashDBM::RECORD_COMP_ZSTD};
  for (const auto& update_mode : update_modes) {
    for (const auto& record_comp_mode : record_comp_modes) {
      if (!tkrzw::HashDBM::CheckRecordCompressionModeIsSupported(record_comp_mode)) {
        continue;
      }
      tkrzw::TreeDBM::TuningParameters tuning_params;
      tuning_params.update_mode = update_mode;
      tuning_params.record_crc_mode = tkrzw::HashDBM::RECORD_CRC_8;
      tuning_params.record_comp_mode = record_comp_mode;
      tuning_params.offset_width = 4;
      tuning_params.align_pow = 0;
      tuning_params.num_buckets = 3;
      tuning_params.restore_mode = tkrzw::HashDBM::RESTORE_READ_ONLY;
      tuning_params.cache_buckets = 1;
      EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->OpenAdvanced(
          file_path, true, tkrzw::File::OPEN_TRUNCATE, tuning_params));
      EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Set("one", "first"));
      EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Set("two", "second"));
      tkrzw::File* file = dbm->GetInternalFile();
      EXPECT_EQ(tkrzw::Status::SUCCESS, file->Close());
      EXPECT_EQ(tkrzw::Status::PRECONDITION_ERROR, dbm->Close());
      EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->OpenAdvanced(
          file_path, true, tkrzw::File::OPEN_DEFAULT, tuning_params));
      EXPECT_FALSE(dbm->IsHealthy());
      EXPECT_FALSE(dbm->IsAutoRestored());
      EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->ValidateRecords(-1, -1));
      std::string value;
      EXPECT_EQ(tkrzw::Status::NOT_FOUND_ERROR, dbm->Get("one", &value));
      EXPECT_EQ(tkrzw::Status::NOT_FOUND_ERROR, dbm->Get("two", &value));
      EXPECT_EQ(0, dbm->CountSimple());
      EXPECT_EQ(tkrzw::Status::PRECONDITION_ERROR, dbm->Set("three", "third"));
      EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Close());
      tuning_params.restore_mode = tkrzw::HashDBM::RESTORE_DEFAULT;
      EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->OpenAdvanced(
          file_path, true, tkrzw::File::OPEN_DEFAULT, tuning_params));
      EXPECT_TRUE(dbm->IsHealthy());
      if (update_mode == tkrzw::HashDBM::UPDATE_APPENDING) {
        EXPECT_FALSE(dbm->IsAutoRestored());
      } else {
        EXPECT_TRUE(dbm->IsAutoRestored());
      }
      EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->ValidateRecords(-1, -1));
      EXPECT_EQ(tkrzw::Status::NOT_FOUND_ERROR, dbm->Get("one", &value));
      EXPECT_EQ(tkrzw::Status::NOT_FOUND_ERROR, dbm->Get("two", &value));
      EXPECT_EQ(0, dbm->CountSimple());
      EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Close());
      EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->OpenAdvanced(
          file_path, true, tkrzw::File::OPEN_TRUNCATE, tuning_params));
      EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Set("one", "first"));
      EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Synchronize(false));
      EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Set("two", "second"));
      file = dbm->GetInternalFile();
      EXPECT_EQ(tkrzw::Status::SUCCESS, file->Close());
      EXPECT_EQ(tkrzw::Status::PRECONDITION_ERROR, dbm->Close());
      tuning_params.restore_mode = tkrzw::HashDBM::RESTORE_SYNC;
      EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->OpenAdvanced(
          file_path, true, tkrzw::File::OPEN_DEFAULT, tuning_params));
      EXPECT_TRUE(dbm->IsHealthy());
      if (update_mode == tkrzw::HashDBM::UPDATE_APPENDING) {
        EXPECT_FALSE(dbm->IsAutoRestored());
      } else {
        EXPECT_TRUE(dbm->IsAutoRestored());
      }
      EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->ValidateRecords(-1, -1));
      EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Get("one", &value));
      EXPECT_EQ("first", value);
      EXPECT_EQ(tkrzw::Status::NOT_FOUND_ERROR, dbm->Get("two", &value));
      EXPECT_EQ(1, dbm->CountSimple());
      EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Set("three", "third"));
      EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Get("three", &value));
      EXPECT_EQ("third", value);
      EXPECT_EQ(2, dbm->CountSimple());
      EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Close());
      auto tmp_file = std::make_unique<tkrzw::MemoryMapParallelFile>();
      EXPECT_EQ(tkrzw::Status::SUCCESS, tmp_file->Open(file_path, true));
      EXPECT_EQ(tkrzw::Status::SUCCESS, tmp_file->Write(9, "\xFF", 1));
      EXPECT_EQ(tkrzw::Status::SUCCESS, tmp_file->Close());
      EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->OpenAdvanced(
          file_path, true, tkrzw::File::OPEN_DEFAULT, tuning_params));
      EXPECT_TRUE(dbm->IsHealthy());
      EXPECT_TRUE(dbm->IsAutoRestored());
      EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->ValidateRecords(-1, -1));
      EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Get("one", &value));
      EXPECT_EQ("first", value);
      EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Get("three", &value));
      EXPECT_EQ("third", value);
      EXPECT_EQ(2, dbm->CountSimple());
      EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Close());
    }
  }
}

void TreeDBMTest::TreeDBMCorruptionTest(tkrzw::TreeDBM* dbm) {
  tkrzw::TemporaryDirectory tmp_dir(true, "tkrzw-");
  const std::string corrupt_file_path = tmp_dir.MakeUniquePath();
  const std::string restored_file_path = tmp_dir.MakeUniquePath();
  const std::vector<tkrzw::HashDBM::UpdateMode> update_modes =
      {tkrzw::HashDBM::UPDATE_IN_PLACE, tkrzw::HashDBM::UPDATE_APPENDING};
  const std::vector<tkrzw::HashDBM::RecordCRCMode> record_crc_modes =
      {tkrzw::HashDBM::RECORD_CRC_NONE, tkrzw::HashDBM::RECORD_CRC_8,
       tkrzw::HashDBM::RECORD_CRC_16, tkrzw::HashDBM::RECORD_CRC_32};
  const std::vector<tkrzw::HashDBM::RecordCompressionMode> record_comp_modes =
      {tkrzw::HashDBM::RECORD_COMP_NONE, tkrzw::HashDBM::RECORD_COMP_ZLIB,
       tkrzw::HashDBM::RECORD_COMP_ZSTD, tkrzw::HashDBM::RECORD_COMP_LZ4,
       tkrzw::HashDBM::RECORD_COMP_LZMA};
  for (const auto& update_mode : update_modes) {
    for (const auto& record_crc_mode : record_crc_modes) {
      for (const auto& record_comp_mode : record_comp_modes) {
        if (!tkrzw::HashDBM::CheckRecordCompressionModeIsSupported(record_comp_mode)) {
          continue;
        }
        tkrzw::TreeDBM::TuningParameters tuning_params;
        tuning_params.update_mode = update_mode;
        tuning_params.record_crc_mode = record_crc_mode;
        tuning_params.record_comp_mode = record_comp_mode;
        tuning_params.offset_width = 4;
        tuning_params.align_pow = 0;
        tuning_params.num_buckets = 10;
        tuning_params.restore_mode = tkrzw::HashDBM::RESTORE_READ_ONLY;
        std::string value;
        for (int32_t i = 0; value.size() < 256; i++) {
          value.append(1, 'a' + i % 26);
          value.append(1, 'A' + i % 25);
        }
        std::string garbage(3, '\0');
        EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->OpenAdvanced(
            corrupt_file_path, true, tkrzw::File::OPEN_TRUNCATE, tuning_params));
        EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Set("abc", value));
        EXPECT_EQ(value, dbm->GetSimple("abc"));
        EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Close());
        auto file = dbm->GetInternalFile()->MakeFile();
        EXPECT_EQ(tkrzw::Status::SUCCESS, file->Open(corrupt_file_path, true));
        EXPECT_EQ(tkrzw::Status::SUCCESS,
                  file->Write(file->GetSizeSimple() - 100, garbage.data(), garbage.size()));
        EXPECT_EQ(tkrzw::Status::SUCCESS, file->Close());
        EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Open(corrupt_file_path,true));
        EXPECT_TRUE(dbm->IsHealthy());
        EXPECT_FALSE(dbm->IsAutoRestored());
        EXPECT_EQ(1, dbm->CountSimple());
        std::string rec_value;
        if (record_comp_mode == tkrzw::HashDBM::RECORD_COMP_NONE) {
          EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Get("abc", &rec_value));
        } else {
          const tkrzw::Status status = dbm->Get("abc", &rec_value);
          EXPECT_TRUE(status == tkrzw::Status::SUCCESS ||
                      status == tkrzw::Status::BROKEN_DATA_ERROR);
        }
        EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Close());
        EXPECT_EQ(tkrzw::Status::SUCCESS,
                  tkrzw::TreeDBM::RestoreDatabase(corrupt_file_path, restored_file_path, -1));
        EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Open(restored_file_path, true));
        EXPECT_TRUE(dbm->IsHealthy());
        EXPECT_FALSE(dbm->IsAutoRestored());
        const auto& meta = dbm->Inspect();
        const std::map<std::string, std::string> meta_map(meta.begin(), meta.end());
        const std::string& update_name = tkrzw::SearchMap(meta_map, "update_mode", "");
        const std::string& crc_name = tkrzw::SearchMap(meta_map, "record_crc_mode", "");
        const std::string& comp_name = tkrzw::SearchMap(meta_map, "record_comp_mode", "");
        const char* expected_update_name = "";
        switch (update_mode) {
          case tkrzw::HashDBM::UPDATE_IN_PLACE:
            expected_update_name = "in-place";
            break;
          case tkrzw::HashDBM::UPDATE_APPENDING:
            expected_update_name = "appending";
            break;
          default:
            break;
        }
        EXPECT_EQ(expected_update_name, update_name);
        const char* expected_crc_name = "";
        switch (record_crc_mode) {
          case tkrzw::HashDBM::RECORD_CRC_NONE:
            expected_crc_name = "none";
            break;
          case tkrzw::HashDBM::RECORD_CRC_8:
            expected_crc_name = "crc-8";
            break;
          case tkrzw::HashDBM::RECORD_CRC_16:
            expected_crc_name = "crc-16";
            break;
          case tkrzw::HashDBM::RECORD_CRC_32:
            expected_crc_name = "crc-32";
            break;
          default:
            break;
        }
        EXPECT_EQ(expected_crc_name, crc_name);
        const char* expected_comp_name = "";
        switch (record_comp_mode) {
          case tkrzw::HashDBM::RECORD_COMP_NONE:
            expected_comp_name = "none";
            break;
          case tkrzw::HashDBM::RECORD_COMP_ZLIB:
            expected_comp_name = "zlib";
            break;
          case tkrzw::HashDBM::RECORD_COMP_ZSTD:
            expected_comp_name = "zstd";
            break;
          case tkrzw::HashDBM::RECORD_COMP_LZ4:
            expected_comp_name = "lz4";
            break;
          case tkrzw::HashDBM::RECORD_COMP_LZMA:
            expected_comp_name = "lzma";
            break;
          default:
            break;
        }
        EXPECT_EQ(expected_comp_name, comp_name);
        EXPECT_EQ(0, dbm->CountSimple());
        EXPECT_EQ(tkrzw::Status::NOT_FOUND_ERROR, dbm->Get("abc"));
        EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Set("abc", "12345"));
        EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Set("xyz", "67890"));
        EXPECT_EQ(2, dbm->CountSimple());
        EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Rebuild());
        const auto& rebuilt_meta = dbm->Inspect();
        const std::map<std::string, std::string> rebuilt_meta_map(
            rebuilt_meta.begin(), rebuilt_meta.end());
        const std::string& rebuilt_update_name =
            tkrzw::SearchMap(rebuilt_meta_map, "update_mode", "");
        EXPECT_EQ(expected_update_name, rebuilt_update_name);
        const std::string& rebuilt_crc_name =
            tkrzw::SearchMap(rebuilt_meta_map, "record_crc_mode", "");
        EXPECT_EQ(expected_crc_name, rebuilt_crc_name);
        EXPECT_EQ("12345", dbm->GetSimple("abc"));
        EXPECT_EQ("67890", dbm->GetSimple("xyz"));
        EXPECT_EQ(2, dbm->CountSimple());
        EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Close());
        EXPECT_EQ(tkrzw::Status::SUCCESS, tkrzw::RemoveFile(restored_file_path));
      }
    }
  }
}

void TreeDBMTest::TreeDBMPageUpdateTest(tkrzw::TreeDBM* dbm) {
  tkrzw::TemporaryDirectory tmp_dir(true, "tkrzw-");
  const std::string orig_file_path = tmp_dir.MakeUniquePath();
  const std::string copy_file_path = tmp_dir.MakeUniquePath();
  constexpr int32_t max_records = 512;
  tkrzw::TreeDBM::TuningParameters tuning_params;
  tuning_params.num_buckets = 1000;
  tuning_params.max_page_size = 30;
  tuning_params.max_branches = 2;
  tuning_params.max_cached_pages = 1;
  tuning_params.page_update_mode = tkrzw::TreeDBM::PAGE_UPDATE_WRITE;
  for (int32_t num_records = 1; num_records <= max_records; num_records *= 2) {
    EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->OpenAdvanced(
        orig_file_path, true, tkrzw::File::OPEN_TRUNCATE, tuning_params));
    for (int32_t i = 1; i <= num_records; i++) {
      std::string expr = tkrzw::ToString(i * i);
      EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Set(expr, expr));
    }
    EXPECT_EQ(num_records, dbm->CountSimple());
    tkrzw::RemoveFile(copy_file_path);
    EXPECT_EQ(tkrzw::Status::SUCCESS, tkrzw::CopyFileData(orig_file_path, copy_file_path));
    auto copy_dbm = dbm->MakeDBM();
    EXPECT_EQ(tkrzw::Status::SUCCESS, copy_dbm->Open(copy_file_path, true));
    EXPECT_EQ(num_records, copy_dbm->CountSimple());
    for (int32_t i = 1; i <= num_records; i++) {
      std::string key = tkrzw::ToString(i * i);
      std::string value;
      EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Get(key, &value));
      EXPECT_EQ(key, value);
    }
    EXPECT_EQ(tkrzw::Status::SUCCESS, copy_dbm->Close());
    for (int32_t i = 1; i <= num_records; i++) {
      std::string expr = tkrzw::ToString(i * i);
      EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Remove(expr));
    }
    EXPECT_EQ(0, dbm->CountSimple());
    tkrzw::RemoveFile(copy_file_path);
    EXPECT_EQ(tkrzw::Status::SUCCESS, tkrzw::CopyFileData(orig_file_path, copy_file_path));
    EXPECT_EQ(tkrzw::Status::SUCCESS, copy_dbm->Open(copy_file_path, true));
    EXPECT_EQ(0, copy_dbm->CountSimple());
    EXPECT_EQ(tkrzw::Status::SUCCESS, copy_dbm->Close());
    EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Close());
  }
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->OpenAdvanced(
      orig_file_path, true, tkrzw::File::OPEN_TRUNCATE, tuning_params));
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Set("one", "first"));
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Set("two", "second"));
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Set("three", "third"));
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->RebuildAdvanced(tuning_params));
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Synchronize(false));
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Append("two", "2", ":"));
  EXPECT_EQ("first", dbm->GetSimple("one"));
  EXPECT_EQ("second:2", dbm->GetSimple("two"));
  EXPECT_EQ("third", dbm->GetSimple("three"));
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Close());
}

void TreeDBMTest::TreeDBMDirectIOTest(tkrzw::TreeDBM* dbm) {
  tkrzw::TemporaryDirectory tmp_dir(true, "tkrzw-");
  const std::string file_path = tmp_dir.MakeUniquePath();
  tkrzw::TreeDBM::TuningParameters tuning_params;
  tuning_params.update_mode = tkrzw::HashDBM::UPDATE_IN_PLACE;
  tuning_params.record_crc_mode = tkrzw::HashDBM::RECORD_CRC_8;
  tuning_params.offset_width = 4;
  tuning_params.align_pow = 0;
  tuning_params.num_buckets = 3;
  tuning_params.restore_mode = tkrzw::HashDBM::RESTORE_READ_ONLY;
  tuning_params.cache_buckets = true;
  tuning_params.max_page_size = 1;
  tuning_params.max_branches = 2;
  tuning_params.max_cached_pages = 1;
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->OpenAdvanced(
      file_path, true, tkrzw::File::OPEN_TRUNCATE, tuning_params));
  for (int32_t i = 1; i <= 10; i++) {
    const std::string& expr = tkrzw::ToString(i * i);
    EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Set(expr, expr, false));
  }
  for (int32_t i = 1; i <= 10; i++) {
    const std::string& key = tkrzw::ToString(i * i);
    std::string value;
    EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Get(key, &value));
    EXPECT_EQ(key, value);
  }
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Close());
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->OpenAdvanced(
      file_path, false, tkrzw::File::OPEN_DEFAULT, tuning_params));
  for (int32_t i = 1; i <= 10; i++) {
    const std::string& key = tkrzw::ToString(i * i);
    std::string value;
    EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Get(key, &value));
    EXPECT_EQ(key, value);
  }
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Close());
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->OpenAdvanced(
      file_path, true, tkrzw::File::OPEN_DEFAULT, tuning_params));
  for (int32_t i = 11; i <= 20; i++) {
    const std::string& expr = tkrzw::ToString(i * i);
    EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Set(expr, expr, false));
  }
  for (int32_t i = 1; i <= 20; i++) {
    const std::string& key = tkrzw::ToString(i * i);
    const std::string& value = tkrzw::ToString(i % 2 == 0 ? i : i * i * i);
    EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Set(key, value, true));
  }
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Synchronize(false));
  for (int32_t i = 1; i <= 20; i++) {
    const std::string& key = tkrzw::ToString(i * i);
    std::string value;
    EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Get(key, &value));
    EXPECT_EQ(tkrzw::ToString(i % 2 == 0 ? i : i * i * i), value);
  }
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Close());
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->OpenAdvanced(
      file_path, true, tkrzw::File::OPEN_DEFAULT, tuning_params));
  for (int32_t i = 1; i <= 20; i++) {
    const std::string& key = tkrzw::ToString(i * i);
    const std::string& value = tkrzw::ToString(i % 2 == 0 ? i : i * i * i);
    EXPECT_EQ(value, dbm->GetSimple(key, ""));
  }
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Close());
  tuning_params.offset_width = 4;
  tuning_params.align_pow = 0;
  tuning_params.num_buckets = 3;
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->OpenAdvanced(
      file_path, true, tkrzw::File::OPEN_DEFAULT, tuning_params));
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->RebuildAdvanced(tuning_params));
  for (int32_t i = 1; i <= 20; i++) {
    const std::string& key = tkrzw::ToString(i * i);
    const std::string& value = tkrzw::ToString(i % 2 == 0 ? i : i * i * i);
    EXPECT_EQ(value, dbm->GetSimple(key, ""));
    if (i % 3 == 0) {
      EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Remove(key));
      EXPECT_EQ("", dbm->GetSimple(key, ""));
    }
  }
  EXPECT_EQ(14, dbm->CountSimple());
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Close());
  tuning_params.update_mode = tkrzw::HashDBM::UPDATE_IN_PLACE;
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->OpenAdvanced(
      file_path, true, tkrzw::File::OPEN_DEFAULT, tuning_params));
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->RebuildAdvanced(tuning_params));
  for (int32_t i = 1; i <= 20; i++) {
    const std::string& key = tkrzw::ToString(i * i);
    const std::string& value = tkrzw::ToString(i % 2 == 0 ? i : i * i * i);
    if (i % 3 == 0) {
      EXPECT_EQ("", dbm->GetSimple(key, ""));
    } else {
      EXPECT_EQ(value, dbm->GetSimple(key, ""));
    }
    EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Set(key, value, true));
    EXPECT_EQ(value, dbm->GetSimple(key, ""));
  }
  EXPECT_EQ(20, dbm->CountSimple());
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Close());
  const int64_t file_size = tkrzw::GetFileSize(file_path);
  EXPECT_GE(file_size, 0);
  EXPECT_EQ(0, file_size % 512);
  const int64_t trunc_size = tkrzw::AlignNumber(file_size, 8192);
  tkrzw::PositionalParallelFile file;
  EXPECT_EQ(tkrzw::Status::SUCCESS, file.Open(file_path, true));
  EXPECT_EQ(tkrzw::Status::SUCCESS, file.Truncate(trunc_size));
  EXPECT_EQ(tkrzw::Status::SUCCESS, file.Close());
  EXPECT_EQ(trunc_size, tkrzw::GetFileSize(file_path));
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Open(file_path, false));
  EXPECT_EQ("6859", dbm->GetSimple("361", ""));
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Close());
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Open(file_path, true));
  EXPECT_EQ("20", dbm->GetSimple("400", ""));
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Set("japan", "tokyo", false));
  EXPECT_EQ("tokyo", dbm->GetSimple("japan", ""));
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Close());
}

void TreeDBMTest::TreeDBMUpdateLoggerTest(tkrzw::TreeDBM* dbm) {
  tkrzw::TemporaryDirectory tmp_dir(true, "tkrzw-");
  const std::string file_path = tmp_dir.MakeUniquePath();
  const std::vector<tkrzw::HashDBM::RecordCompressionMode> record_comp_modes =
      {tkrzw::HashDBM::RECORD_COMP_NONE, tkrzw::HashDBM::RECORD_COMP_ZSTD};
  for (const auto& record_comp_mode : record_comp_modes) {
    if (!tkrzw::HashDBM::CheckRecordCompressionModeIsSupported(record_comp_mode)) {
      continue;
    }
    tkrzw::TreeDBM::TuningParameters tuning_params;
    tuning_params.update_mode = tkrzw::HashDBM::UPDATE_IN_PLACE;
    tuning_params.record_comp_mode = record_comp_mode;
    tuning_params.offset_width = 4;
    tuning_params.align_pow = 0;
    tuning_params.num_buckets = 10;
    tuning_params.restore_mode = tkrzw::HashDBM::RESTORE_READ_ONLY;
    EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->OpenAdvanced(
        file_path, true, tkrzw::File::OPEN_TRUNCATE, tuning_params));
    UpdateLoggerTest(dbm);
    EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Close());
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

TEST_F(TreeDBMTest, ProcessMulti) {
  tkrzw::TreeDBM dbm(std::make_unique<tkrzw::MemoryMapParallelFile>());
  TreeDBMProcessMultiTest(&dbm);
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

TEST_F(TreeDBMTest, Queue) {
  tkrzw::TreeDBM dbm(std::make_unique<tkrzw::MemoryMapParallelFile>());
  TreeDBMQueueTest(&dbm);
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

TEST_F(TreeDBMTest, VariousSize) {
  tkrzw::TreeDBM dbm(std::make_unique<tkrzw::MemoryMapParallelFile>());
  TreeDBMVariousSizeTest(&dbm);
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

TEST_F(TreeDBMTest, AutoRestore) {
  tkrzw::TreeDBM dbm(std::make_unique<tkrzw::MemoryMapParallelFile>());
  TreeDBMAutoRestoreTest(&dbm);
}

TEST_F(TreeDBMTest, Corruption) {
  tkrzw::TreeDBM dbm(std::make_unique<tkrzw::MemoryMapParallelFile>());
  TreeDBMCorruptionTest(&dbm);
}

TEST_F(TreeDBMTest, PageUpdate) {
  tkrzw::TreeDBM dbm(std::make_unique<tkrzw::MemoryMapParallelFile>());
  TreeDBMPageUpdateTest(&dbm);
}

TEST_F(TreeDBMTest, DirectIO) {
  auto file = std::make_unique<tkrzw::PositionalParallelFile>();
  EXPECT_EQ(tkrzw::Status::SUCCESS, file->SetAccessStrategy(
      512, tkrzw::PositionalFile::ACCESS_DIRECT | tkrzw::PositionalFile::ACCESS_PADDING));
  tkrzw::TreeDBM dbm(std::move(file));
  TreeDBMDirectIOTest(&dbm);
}

TEST_F(TreeDBMTest, UpdateLogger) {
  tkrzw::TreeDBM dbm(std::make_unique<tkrzw::MemoryMapParallelFile>());
  TreeDBMUpdateLoggerTest(&dbm);
}

// END OF FILE
