/*************************************************************************************************
 * Tests for tkrzw_dbm_hash.h
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
#include "tkrzw_dbm_hash.h"
#include "tkrzw_dbm_hash_impl.h"
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

class HashDBMTest : public CommonDBMTest {
 protected:
  void HashDBMEmptyDatabaseTest(tkrzw::HashDBM* dbm);
  void HashDBMFileTest(tkrzw::HashDBM* dbm);
  void HashDBMLargeRecordTest(tkrzw::HashDBM* dbm);
  void HashDBMBasicTest(tkrzw::HashDBM* dbm);
  void HashDBMSequenceTest(tkrzw::HashDBM* dbm);
  void HashDBMAppendTest(tkrzw::HashDBM* dbm);
  void HashDBMProcessTest(tkrzw::HashDBM* dbm);
  void HashDBMProcessMultiTest(tkrzw::HashDBM* dbm);
  void HashDBMProcessEachTest(tkrzw::HashDBM* dbm);
  void HashDBMRandomTestOne(tkrzw::HashDBM* dbm);
  void HashDBMRandomTestThread(tkrzw::HashDBM* dbm);
  void HashDBMRecordMigrationTest(tkrzw::HashDBM* dbm, tkrzw::File* file);
  void HashDBMOpenCloseTest(tkrzw::HashDBM* dbm);
  void HashDBMUpdateInPlaceTest(tkrzw::HashDBM* dbm);
  void HashDBMUpdateAppendingTest(tkrzw::HashDBM* dbm);
  void HashDBMVariousSizeTest(tkrzw::HashDBM* dbm);
  void HashDBMRebuildStaticTestOne(
      tkrzw::HashDBM* dbm, const tkrzw::HashDBM::TuningParameters& tuning_params);
  void HashDBMRebuildStaticTestAll(tkrzw::HashDBM* dbm);
  void HashDBMRebuildRandomTest(tkrzw::HashDBM* dbm);
  void HashDBMRestoreTest(tkrzw::HashDBM* dbm);
  void HashDBMAutoRestoreTest(tkrzw::HashDBM* dbm);
  void HashDBMCorruptionTest(tkrzw::HashDBM* dbm);
  void HashDBMDirectIOTest(tkrzw::HashDBM* dbm);
  void HashDBMUpdateLoggerTest(tkrzw::HashDBM* dbm);
};

void HashDBMTest::HashDBMEmptyDatabaseTest(tkrzw::HashDBM* dbm) {
  tkrzw::TemporaryDirectory tmp_dir(true, "tkrzw-");
  const std::string file_path = tmp_dir.MakeUniquePath();
  EXPECT_EQ(typeid(tkrzw::HashDBM), static_cast<tkrzw::DBM*>(dbm)->GetType());
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
  EXPECT_TRUE(tkrzw::CheckMap(meta_map, "offset_width"));
  EXPECT_TRUE(tkrzw::CheckMap(meta_map, "align_pow"));
  EXPECT_EQ("true", tkrzw::SearchMap(meta_map, "healthy", ""));
  EXPECT_FALSE(dbm->IsOrdered());
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Close());
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Open(file_path, true));
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Clear());
  EXPECT_EQ(0, dbm->CountSimple());
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Synchronize(true));
  EXPECT_EQ(file_size, dbm->GetFileSizeSimple());
  EXPECT_EQ(123, dbm->GetDatabaseType());
  EXPECT_EQ("0123456789", dbm->GetOpaqueMetadata().substr(0, 10));
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Synchronize(false));
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Close());
  auto file = dbm->GetInternalFile()->MakeFile();
  EXPECT_EQ(tkrzw::Status::SUCCESS, file->Open(file_path, false));
  int32_t meta_cyclic_magic = 0;
  int32_t meta_pkg_major_version = 0;
  int32_t meta_pkg_minor_version = 0;
  int32_t meta_static_flags = 0;
  int32_t meta_offset_width = 0;
  int32_t meta_align_pow = 0;
  int32_t meta_closure_flags = 0;
  int64_t meta_num_buckets = 0;
  int64_t meta_num_records = 0;
  int64_t meta_eff_data_size = 0;
  int64_t meta_file_size = 0;
  int64_t meta_mod_time = 0;
  int32_t meta_db_type = 0;
  std::string meta_opaque;
  EXPECT_EQ(tkrzw::Status::SUCCESS, tkrzw::HashDBM::ReadMetadata(
      file.get(), &meta_cyclic_magic, &meta_pkg_major_version, &meta_pkg_minor_version,
      &meta_static_flags, &meta_offset_width, &meta_align_pow,
      &meta_closure_flags, &meta_num_buckets, &meta_num_records,
      &meta_eff_data_size, &meta_file_size, &meta_mod_time, &meta_db_type, &meta_opaque));
  EXPECT_GT(meta_cyclic_magic, 0);
  EXPECT_GT(meta_pkg_major_version + meta_pkg_minor_version, 0);
  EXPECT_GT(meta_static_flags, 0);
  EXPECT_GT(meta_offset_width, 0);
  EXPECT_GE(meta_align_pow, 0);
  EXPECT_GT(meta_closure_flags, 0);
  EXPECT_GT(meta_num_buckets, 0);
  EXPECT_EQ(0, meta_num_records);
  EXPECT_EQ(0, meta_eff_data_size);
  EXPECT_GT(meta_file_size, 0);
  EXPECT_GT(meta_mod_time, 0);
  EXPECT_EQ(123, meta_db_type);
  EXPECT_EQ("0123456789", meta_opaque.substr(0, 10));
  EXPECT_EQ(tkrzw::Status::SUCCESS, file->Close());
}

void HashDBMTest::HashDBMFileTest(tkrzw::HashDBM* dbm) {
  tkrzw::TemporaryDirectory tmp_dir(true, "tkrzw-");
  const std::string file_path = tmp_dir.MakeUniquePath();
  FileTest(dbm, file_path);
}

void HashDBMTest::HashDBMLargeRecordTest(tkrzw::HashDBM* dbm) {
  tkrzw::TemporaryDirectory tmp_dir(true, "tkrzw-");
  const std::string file_path = tmp_dir.MakeUniquePath();
  const std::vector<tkrzw::HashDBM::UpdateMode> update_modes =
      {tkrzw::HashDBM::UPDATE_IN_PLACE, tkrzw::HashDBM::UPDATE_APPENDING};
  const std::vector<tkrzw::HashDBM::RecordCRCMode> record_crc_modes =
      {tkrzw::HashDBM::RECORD_CRC_NONE};
  const std::vector<tkrzw::HashDBM::RecordCompressionMode> record_comp_modes =
      {tkrzw::HashDBM::RECORD_COMP_NONE, tkrzw::HashDBM::RECORD_COMP_ZSTD};
  const std::vector<int32_t> offset_widths = {4};
  const std::vector<int32_t> align_pows = {3};
  const std::vector<int32_t> nums_buckets = {64};
  for (const auto& update_mode : update_modes) {
    for (const auto& record_crc_mode : record_crc_modes) {
      for (const auto& record_comp_mode : record_comp_modes) {
        if (!tkrzw::HashDBM::CheckRecordCompressionModeIsSupported(record_comp_mode)) {
          continue;
        }
        for (const auto& offset_width : offset_widths) {
          for (const auto& align_pow : align_pows) {
            for (const auto& num_buckets : nums_buckets) {
              tkrzw::HashDBM::TuningParameters tuning_params;
              tuning_params.update_mode = update_mode;
              tuning_params.record_crc_mode = record_crc_mode;
              tuning_params.record_comp_mode = record_comp_mode;
              tuning_params.offset_width = offset_width;
              tuning_params.align_pow = align_pow;
              tuning_params.num_buckets = num_buckets;
              tuning_params.restore_mode = tkrzw::HashDBM::RESTORE_READ_ONLY;
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
}

void HashDBMTest::HashDBMBasicTest(tkrzw::HashDBM* dbm) {
  tkrzw::TemporaryDirectory tmp_dir(true, "tkrzw-");
  const std::string file_path = tmp_dir.MakeUniquePath();
  const std::vector<tkrzw::HashDBM::UpdateMode> update_modes =
      {tkrzw::HashDBM::UPDATE_IN_PLACE, tkrzw::HashDBM::UPDATE_APPENDING};
  const std::vector<tkrzw::HashDBM::RecordCRCMode> record_crc_modes =
      {tkrzw::HashDBM::RECORD_CRC_NONE, tkrzw::HashDBM::RECORD_CRC_16};
  const std::vector<tkrzw::HashDBM::RecordCompressionMode> record_comp_modes =
      {tkrzw::HashDBM::RECORD_COMP_NONE, tkrzw::HashDBM::RECORD_COMP_ZSTD};
  const std::vector<int32_t> offset_widths = {3, 4, 5};
  const std::vector<int32_t> align_pows = {0, 1, 2, 3, 9, 10};
  const std::vector<int32_t> nums_buckets = {1, 8, 64, 512};
  for (const auto& update_mode : update_modes) {
    for (const auto& record_crc_mode : record_crc_modes) {
      for (const auto& record_comp_mode : record_comp_modes) {
        if (!tkrzw::HashDBM::CheckRecordCompressionModeIsSupported(record_comp_mode)) {
          continue;
        }
        for (const auto& offset_width : offset_widths) {
          for (const auto& align_pow : align_pows) {
            for (const auto& num_buckets : nums_buckets) {
              tkrzw::HashDBM::TuningParameters tuning_params;
              tuning_params.update_mode = update_mode;
              tuning_params.record_crc_mode = record_crc_mode;
              tuning_params.record_comp_mode = record_comp_mode;
              tuning_params.offset_width = offset_width;
              tuning_params.align_pow = align_pow;
              tuning_params.num_buckets = num_buckets;
              tuning_params.restore_mode = tkrzw::HashDBM::RESTORE_READ_ONLY;
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

void HashDBMTest::HashDBMSequenceTest(tkrzw::HashDBM* dbm) {
  tkrzw::TemporaryDirectory tmp_dir(true, "tkrzw-");
  const std::string file_path = tmp_dir.MakeUniquePath();
  const std::vector<tkrzw::HashDBM::UpdateMode> update_modes =
      {tkrzw::HashDBM::UPDATE_IN_PLACE, tkrzw::HashDBM::UPDATE_APPENDING};
  const std::vector<tkrzw::HashDBM::RecordCRCMode> record_crc_modes =
      {tkrzw::HashDBM::RECORD_CRC_NONE, tkrzw::HashDBM::RECORD_CRC_16};
  const std::vector<tkrzw::HashDBM::RecordCompressionMode> record_comp_modes =
      {tkrzw::HashDBM::RECORD_COMP_NONE, tkrzw::HashDBM::RECORD_COMP_ZSTD};
  const std::vector<int32_t> offset_widths = {4};
  const std::vector<int32_t> align_pows = {0, 1, 2, 10};
  const std::vector<int32_t> nums_buckets = {64, 512};
  for (const auto& update_mode : update_modes) {
    for (const auto& record_crc_mode : record_crc_modes) {
      for (const auto& record_comp_mode : record_comp_modes) {
        if (!tkrzw::HashDBM::CheckRecordCompressionModeIsSupported(record_comp_mode)) {
          continue;
        }
        for (const auto& offset_width : offset_widths) {
          for (const auto& align_pow : align_pows) {
            for (const auto& num_buckets : nums_buckets) {
              tkrzw::HashDBM::TuningParameters tuning_params;
              tuning_params.update_mode = update_mode;
              tuning_params.record_crc_mode = record_crc_mode;
              tuning_params.record_comp_mode = record_comp_mode;
              tuning_params.offset_width = offset_width;
              tuning_params.align_pow = align_pow;
              tuning_params.num_buckets = num_buckets;
              tuning_params.restore_mode = tkrzw::HashDBM::RESTORE_READ_ONLY;
              EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->OpenAdvanced(
                  file_path, true, tkrzw::File::OPEN_TRUNCATE, tuning_params));
              SequenceTest(dbm);
              EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Close());
            }
          }
        }
      }
    }
  }
}

void HashDBMTest::HashDBMAppendTest(tkrzw::HashDBM* dbm) {
  tkrzw::TemporaryDirectory tmp_dir(true, "tkrzw-");
  const std::string file_path = tmp_dir.MakeUniquePath();
  const std::vector<tkrzw::HashDBM::UpdateMode> update_modes =
      {tkrzw::HashDBM::UPDATE_IN_PLACE, tkrzw::HashDBM::UPDATE_APPENDING};
  const std::vector<tkrzw::HashDBM::RecordCRCMode> record_crc_modes =
      {tkrzw::HashDBM::RECORD_CRC_NONE, tkrzw::HashDBM::RECORD_CRC_16};
  const std::vector<tkrzw::HashDBM::RecordCompressionMode> record_comp_modes =
      {tkrzw::HashDBM::RECORD_COMP_NONE, tkrzw::HashDBM::RECORD_COMP_ZSTD};
  const std::vector<int32_t> offset_widths = {4};
  const std::vector<int32_t> align_pows = {0};
  const std::vector<int32_t> nums_buckets = {100};
  for (const auto& update_mode : update_modes) {
    for (const auto& record_crc_mode : record_crc_modes) {
      for (const auto& record_comp_mode : record_comp_modes) {
        if (!tkrzw::HashDBM::CheckRecordCompressionModeIsSupported(record_comp_mode)) {
          continue;
        }
        for (const auto& offset_width : offset_widths) {
          for (const auto& align_pow : align_pows) {
            for (const auto& num_buckets : nums_buckets) {
              tkrzw::HashDBM::TuningParameters tuning_params;
              tuning_params.update_mode = update_mode;
              tuning_params.record_crc_mode = record_crc_mode;
              tuning_params.record_comp_mode = record_comp_mode;
              tuning_params.offset_width = offset_width;
              tuning_params.align_pow = align_pow;
              tuning_params.num_buckets = num_buckets;
              tuning_params.restore_mode = tkrzw::HashDBM::RESTORE_READ_ONLY;
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
}

void HashDBMTest::HashDBMProcessTest(tkrzw::HashDBM* dbm) {
  tkrzw::TemporaryDirectory tmp_dir(true, "tkrzw-");
  const std::string file_path = tmp_dir.MakeUniquePath();
  const std::vector<tkrzw::HashDBM::UpdateMode> update_modes =
      {tkrzw::HashDBM::UPDATE_IN_PLACE, tkrzw::HashDBM::UPDATE_APPENDING};
  const std::vector<tkrzw::HashDBM::RecordCRCMode> record_crc_modes =
      {tkrzw::HashDBM::RECORD_CRC_NONE, tkrzw::HashDBM::RECORD_CRC_16};
  const std::vector<tkrzw::HashDBM::RecordCompressionMode> record_comp_modes =
      {tkrzw::HashDBM::RECORD_COMP_NONE, tkrzw::HashDBM::RECORD_COMP_ZSTD};
  const std::vector<int32_t> offset_widths = {3, 4, 5};
  const std::vector<int32_t> align_pows = {0, 1, 2, 3, 9, 10};
  const std::vector<int32_t> nums_buckets = {1, 8, 64, 512};
  for (const auto& update_mode : update_modes) {
    for (const auto& record_crc_mode : record_crc_modes) {
      for (const auto& record_comp_mode : record_comp_modes) {
        if (!tkrzw::HashDBM::CheckRecordCompressionModeIsSupported(record_comp_mode)) {
          continue;
        }
        for (const auto& offset_width : offset_widths) {
          for (const auto& align_pow : align_pows) {
            for (const auto& num_buckets : nums_buckets) {
              tkrzw::HashDBM::TuningParameters tuning_params;
              tuning_params.update_mode = update_mode;
              tuning_params.record_crc_mode = record_crc_mode;
              tuning_params.record_comp_mode = record_comp_mode;
              tuning_params.offset_width = offset_width;
              tuning_params.align_pow = align_pow;
              tuning_params.num_buckets = num_buckets;
              tuning_params.restore_mode = tkrzw::HashDBM::RESTORE_READ_ONLY;
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
}

void HashDBMTest::HashDBMProcessMultiTest(tkrzw::HashDBM* dbm) {
  tkrzw::TemporaryDirectory tmp_dir(true, "tkrzw-");
  const std::string file_path = tmp_dir.MakeUniquePath();
  const std::vector<tkrzw::HashDBM::UpdateMode> update_modes =
      {tkrzw::HashDBM::UPDATE_IN_PLACE, tkrzw::HashDBM::UPDATE_APPENDING};
  const std::vector<tkrzw::HashDBM::RecordCRCMode> record_crc_modes =
      {tkrzw::HashDBM::RECORD_CRC_16};
  const std::vector<tkrzw::HashDBM::RecordCompressionMode> record_comp_modes =
      {tkrzw::HashDBM::RECORD_COMP_NONE, tkrzw::HashDBM::RECORD_COMP_ZSTD};
  const std::vector<int32_t> offset_widths = {4};
  const std::vector<int32_t> align_pows = {0};
  const std::vector<int32_t> nums_buckets = {5000};
  for (const auto& update_mode : update_modes) {
    for (const auto& record_crc_mode : record_crc_modes) {
      for (const auto& record_comp_mode : record_comp_modes) {
        if (!tkrzw::HashDBM::CheckRecordCompressionModeIsSupported(record_comp_mode)) {
          continue;
        }
        for (const auto& offset_width : offset_widths) {
          for (const auto& align_pow : align_pows) {
            for (const auto& num_buckets : nums_buckets) {
              tkrzw::HashDBM::TuningParameters tuning_params;
              tuning_params.update_mode = update_mode;
              tuning_params.record_crc_mode = record_crc_mode;
              tuning_params.record_comp_mode = record_comp_mode;
              tuning_params.offset_width = offset_width;
              tuning_params.align_pow = align_pow;
              tuning_params.num_buckets = num_buckets;
              tuning_params.restore_mode = tkrzw::HashDBM::RESTORE_READ_ONLY;
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
}

void HashDBMTest::HashDBMProcessEachTest(tkrzw::HashDBM* dbm) {
  tkrzw::TemporaryDirectory tmp_dir(true, "tkrzw-");
  const std::string file_path = tmp_dir.MakeUniquePath();
  const std::vector<tkrzw::HashDBM::UpdateMode> update_modes =
      {tkrzw::HashDBM::UPDATE_IN_PLACE, tkrzw::HashDBM::UPDATE_APPENDING};
  const std::vector<tkrzw::HashDBM::RecordCRCMode> record_crc_modes =
      {tkrzw::HashDBM::RECORD_CRC_NONE, tkrzw::HashDBM::RECORD_CRC_16};
  const std::vector<tkrzw::HashDBM::RecordCompressionMode> record_comp_modes =
      {tkrzw::HashDBM::RECORD_COMP_NONE, tkrzw::HashDBM::RECORD_COMP_ZSTD};
  const std::vector<int32_t> offset_widths = {4};
  const std::vector<int32_t> align_pows = {0, 3, 9};
  const std::vector<int32_t> nums_buckets = {1000};
  for (const auto& update_mode : update_modes) {
    for (const auto& record_crc_mode : record_crc_modes) {
      for (const auto& record_comp_mode : record_comp_modes) {
        if (!tkrzw::HashDBM::CheckRecordCompressionModeIsSupported(record_comp_mode)) {
          continue;
        }
        for (const auto& offset_width : offset_widths) {
          for (const auto& align_pow : align_pows) {
            for (const auto& num_buckets : nums_buckets) {
              tkrzw::HashDBM::TuningParameters tuning_params;
              tuning_params.update_mode = update_mode;
              tuning_params.record_crc_mode = record_crc_mode;
              tuning_params.record_comp_mode = record_comp_mode;
              tuning_params.offset_width = offset_width;
              tuning_params.align_pow = align_pow;
              tuning_params.num_buckets = num_buckets;
              tuning_params.restore_mode = tkrzw::HashDBM::RESTORE_READ_ONLY;
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

void HashDBMTest::HashDBMRandomTestOne(tkrzw::HashDBM* dbm) {
  tkrzw::TemporaryDirectory tmp_dir(true, "tkrzw-");
  const std::string file_path = tmp_dir.MakeUniquePath();
  const std::vector<tkrzw::HashDBM::UpdateMode> update_modes =
      {tkrzw::HashDBM::UPDATE_IN_PLACE, tkrzw::HashDBM::UPDATE_APPENDING};
  const std::vector<tkrzw::HashDBM::RecordCRCMode> record_crc_modes =
      {tkrzw::HashDBM::RECORD_CRC_NONE, tkrzw::HashDBM::RECORD_CRC_16};
  const std::vector<tkrzw::HashDBM::RecordCompressionMode> record_comp_modes =
      {tkrzw::HashDBM::RECORD_COMP_NONE, tkrzw::HashDBM::RECORD_COMP_ZSTD};
  const std::vector<int32_t> offset_widths = {3, 4};
  const std::vector<int32_t> align_pows = {0, 2, 4, 8, 10};
  const std::vector<int32_t> nums_buckets = {10000};
  for (const auto& update_mode : update_modes) {
    for (const auto& record_crc_mode : record_crc_modes) {
      for (const auto& record_comp_mode : record_comp_modes) {
        if (!tkrzw::HashDBM::CheckRecordCompressionModeIsSupported(record_comp_mode)) {
          continue;
        }
        for (const auto& offset_width : offset_widths) {
          for (const auto& align_pow : align_pows) {
            for (const auto& num_buckets : nums_buckets) {
              tkrzw::HashDBM::TuningParameters tuning_params;
              tuning_params.update_mode = update_mode;
              tuning_params.record_crc_mode = record_crc_mode;
              tuning_params.record_comp_mode = record_comp_mode;
              tuning_params.offset_width = offset_width;
              tuning_params.align_pow = align_pow;
              tuning_params.num_buckets = num_buckets;
              tuning_params.restore_mode = tkrzw::HashDBM::RESTORE_READ_ONLY;
              EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->OpenAdvanced(
                  file_path, true, tkrzw::File::OPEN_TRUNCATE, tuning_params));
              RandomTest(dbm, 1);
              EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Close());
            }
          }
        }
      }
    }
  }
}

void HashDBMTest::HashDBMRandomTestThread(tkrzw::HashDBM* dbm) {
  tkrzw::TemporaryDirectory tmp_dir(true, "tkrzw-");
  const std::string file_path = tmp_dir.MakeUniquePath();
  const std::vector<tkrzw::HashDBM::UpdateMode> update_modes =
      {tkrzw::HashDBM::UPDATE_IN_PLACE, tkrzw::HashDBM::UPDATE_APPENDING};
  const std::vector<tkrzw::HashDBM::RecordCRCMode> record_crc_modes =
      {tkrzw::HashDBM::RECORD_CRC_NONE, tkrzw::HashDBM::RECORD_CRC_16};
  const std::vector<tkrzw::HashDBM::RecordCompressionMode> record_comp_modes =
      {tkrzw::HashDBM::RECORD_COMP_NONE, tkrzw::HashDBM::RECORD_COMP_ZSTD};
  const std::vector<int32_t> offset_widths = {4};
  const std::vector<int32_t> align_pows = {0, 2, 4, 10};
  const std::vector<int32_t> nums_buckets = {10000};
  for (const auto& update_mode : update_modes) {
    for (const auto& record_crc_mode : record_crc_modes) {
      for (const auto& record_comp_mode : record_comp_modes) {
        if (!tkrzw::HashDBM::CheckRecordCompressionModeIsSupported(record_comp_mode)) {
          continue;
        }
        for (const auto& offset_width : offset_widths) {
          for (const auto& align_pow : align_pows) {
            for (const auto& num_buckets : nums_buckets) {
              tkrzw::HashDBM::TuningParameters tuning_params;
              tuning_params.update_mode = update_mode;
              tuning_params.record_crc_mode = record_crc_mode;
              tuning_params.record_comp_mode = record_comp_mode;
              tuning_params.offset_width = offset_width;
              tuning_params.align_pow = align_pow;
              tuning_params.num_buckets = num_buckets;
              tuning_params.restore_mode = tkrzw::HashDBM::RESTORE_READ_ONLY;
              EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->OpenAdvanced(
                  file_path, true, tkrzw::File::OPEN_TRUNCATE, tuning_params));
              RandomTestThread(dbm);
              EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Close());
            }
          }
        }
      }
    }
  }
}

void HashDBMTest::HashDBMRecordMigrationTest(tkrzw::HashDBM* dbm, tkrzw::File* file) {
  tkrzw::TemporaryDirectory tmp_dir(true, "tkrzw-");
  const std::string file_path = tmp_dir.MakeUniquePath();
  const std::string flat_file_path = tmp_dir.MakeUniquePath();
  const std::vector<tkrzw::HashDBM::UpdateMode> update_modes =
      {tkrzw::HashDBM::UPDATE_IN_PLACE, tkrzw::HashDBM::UPDATE_APPENDING};
  const std::vector<tkrzw::HashDBM::RecordCRCMode> record_crc_modes =
      {tkrzw::HashDBM::RECORD_CRC_NONE, tkrzw::HashDBM::RECORD_CRC_16};
  const std::vector<tkrzw::HashDBM::RecordCompressionMode> record_comp_modes =
      {tkrzw::HashDBM::RECORD_COMP_NONE, tkrzw::HashDBM::RECORD_COMP_ZSTD};
  for (const auto& update_mode : update_modes) {
    for (const auto& record_crc_mode : record_crc_modes) {
      for (const auto& record_comp_mode : record_comp_modes) {
        if (!tkrzw::HashDBM::CheckRecordCompressionModeIsSupported(record_comp_mode)) {
          continue;
        }
        tkrzw::HashDBM::TuningParameters tuning_params;
        tuning_params.update_mode = update_mode;
        tuning_params.record_crc_mode = record_crc_mode;
        tuning_params.record_comp_mode = record_comp_mode;
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
  }
}

void HashDBMTest::HashDBMOpenCloseTest(tkrzw::HashDBM* dbm) {
  tkrzw::TemporaryDirectory tmp_dir(true, "tkrzw-");
  const std::string file_path = tmp_dir.MakeUniquePath();
  constexpr int32_t num_iterations = 10000;
  constexpr int32_t num_keys = 1000;
  constexpr int32_t num_levels = 1000;
  tkrzw::HashDBM::TuningParameters tuning_params;
  tuning_params.update_mode = tkrzw::HashDBM::UPDATE_IN_PLACE;
  tuning_params.record_crc_mode = tkrzw::HashDBM::RECORD_CRC_8;
  tuning_params.offset_width = 4;
  tuning_params.align_pow = 0;
  tuning_params.num_buckets = num_keys;
  tuning_params.restore_mode = tkrzw::HashDBM::RESTORE_READ_ONLY;
  std::mt19937 mt(1);
  std::uniform_int_distribution<int32_t> key_dist(1, num_keys);
  std::uniform_int_distribution<int32_t> value_len_dist(0, 8);
  std::uniform_int_distribution<int32_t> value_char_dist('a', 'z');
  std::uniform_int_distribution<int32_t> op_dist(0, tkrzw::INT32MAX);
  std::map<std::string, std::string> map;
  EXPECT_FALSE(dbm->IsOpen());
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->OpenAdvanced(
      file_path, true, tkrzw::File::OPEN_TRUNCATE, tuning_params));
  EXPECT_TRUE(dbm->IsOpen());
  EXPECT_TRUE(dbm->IsWritable());
  std::string got_path;
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->GetFilePath(&got_path));
  EXPECT_EQ(file_path, got_path);
  for (int32_t i = 0; i < num_iterations; i++) {
    const int32_t level = i / (num_iterations / num_levels);
    if (op_dist(mt) % 100 == 0) {
      EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Close());
      EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Open(file_path, true));
    }
    const std::string& key = tkrzw::ToString(key_dist(mt));
    const int32_t value_len = level < num_levels / 2 ? level : 0;
    const std::string value(value_len, value_char_dist(mt));
    if (level <= num_levels / 4 * 3 && op_dist(mt) % 2 == 0) {
      const tkrzw::Status status = dbm->Remove(key);
      EXPECT_TRUE(status == tkrzw::Status::SUCCESS || status == tkrzw::Status::NOT_FOUND_ERROR);
      map.erase(key);
    } else {
      EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Set(key, value));
      map[key] = value;
    }
  }
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Close());
  {
    tkrzw::HashDBM second_dbm;
    EXPECT_EQ(tkrzw::Status::SUCCESS, second_dbm.Open(file_path, true));
    EXPECT_EQ(map.size(), second_dbm.CountSimple());
    for (const auto& rec : map) {
      EXPECT_EQ(rec.second, second_dbm.GetSimple(rec.first));
    }
  }
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Open(file_path, false));
  EXPECT_TRUE(dbm->IsOpen());
  EXPECT_FALSE(dbm->IsWritable());
  EXPECT_EQ(map.size(), dbm->CountSimple());
  auto iter = dbm->MakeIterator();
  EXPECT_EQ(tkrzw::Status::SUCCESS, iter->First());
  std::string key, value;
  while (iter->Get(&key, &value) == tkrzw::Status::SUCCESS) {
    EXPECT_EQ(tkrzw::SearchMap(map, key, "*"), value);
    EXPECT_EQ(tkrzw::Status::SUCCESS, iter->Next());
  }
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Close());
}

void HashDBMTest::HashDBMUpdateInPlaceTest(tkrzw::HashDBM* dbm) {
  tkrzw::TemporaryDirectory tmp_dir(true, "tkrzw-");
  const std::string file_path = tmp_dir.MakeUniquePath();
  tkrzw::HashDBM::TuningParameters tuning_params;
  tuning_params.update_mode = tkrzw::HashDBM::UPDATE_IN_PLACE;
  tuning_params.record_crc_mode = tkrzw::HashDBM::RECORD_CRC_8;
  tuning_params.offset_width = 4;
  tuning_params.align_pow = 2;
  tuning_params.num_buckets = 10;
  tuning_params.restore_mode = tkrzw::HashDBM::RESTORE_READ_ONLY;
  tuning_params.fbp_capacity = tkrzw::INT32MAX;
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->OpenAdvanced(
      file_path, true, tkrzw::File::OPEN_DEFAULT, tuning_params));
  EXPECT_TRUE(dbm->IsHealthy());
  EXPECT_FALSE(dbm->IsAutoRestored());
  EXPECT_EQ(0, dbm->CountSimple());
  EXPECT_EQ(10, dbm->CountBuckets());
  EXPECT_EQ(0, dbm->CountUsedBuckets());
  EXPECT_EQ(tkrzw::HashDBM::UPDATE_IN_PLACE, dbm->GetUpdateMode());
  constexpr int32_t num_records = 100;
  for (int32_t i = 0; i < num_records; i++) {
    const std::string& key = tkrzw::SPrintF("%010d", i);
    const std::string& value = tkrzw::SPrintF("%10d", i);
    EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Set(key, value, false));
  }
  EXPECT_EQ(num_records, dbm->CountSimple());
  EXPECT_EQ(num_records * 20, dbm->GetEffectiveDataSize());
  int64_t file_size = dbm->GetFileSizeSimple();
  for (int32_t i = 0; i < num_records; i++) {
    const std::string& expr = tkrzw::SPrintF("%010d", i);
    EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Set(expr, expr, true));
  }
  EXPECT_EQ(num_records, dbm->CountSimple());
  EXPECT_EQ(file_size, dbm->GetFileSizeSimple());
  EXPECT_EQ(10, dbm->CountUsedBuckets());
  for (int32_t i = 0; i < num_records; i += 2) {
    const std::string& expr = tkrzw::SPrintF("%010d", i);
    EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Set(expr, "", true));
  }
  EXPECT_EQ(num_records, dbm->CountSimple());
  EXPECT_EQ(num_records * 15, dbm->GetEffectiveDataSize());
  EXPECT_EQ(file_size, dbm->GetFileSizeSimple());
  for (int32_t i = 1; i < num_records; i += 2) {
    const std::string& key = tkrzw::SPrintF("%010d", i);
    const std::string& value = tkrzw::SPrintF("%020d", i);
    EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Set(key, value, true));
  }
  EXPECT_EQ(num_records, dbm->CountSimple());
  EXPECT_EQ(num_records * 20, dbm->GetEffectiveDataSize());
  EXPECT_GT(dbm->GetFileSizeSimple(), file_size);
  file_size = dbm->GetFileSizeSimple();
  for (int32_t i = 1; i < num_records; i += 2) {
    const std::string& key = tkrzw::SPrintF("%010d", i);
    const std::string& value = tkrzw::SPrintF("%030d", i);
    EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Set(key, value, true));
  }
  EXPECT_EQ(num_records, dbm->CountSimple());
  EXPECT_EQ(num_records * 25, dbm->GetEffectiveDataSize());
  EXPECT_GT(dbm->GetFileSizeSimple(), file_size);
  for (int32_t i = 0; i < num_records; i += 2) {
    const std::string& key = tkrzw::SPrintF("%010d", i);
    const std::string& value = tkrzw::SPrintF("%020d", i);
    EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Set(key, value, true));
  }
  EXPECT_EQ(num_records, dbm->CountSimple());
  EXPECT_EQ(num_records * 35, dbm->GetEffectiveDataSize());
  EXPECT_GT(dbm->GetFileSizeSimple(), file_size);
  file_size = dbm->GetFileSizeSimple();
  for (int32_t i = 0; i < num_records; i += 2) {
    const std::string& expr = tkrzw::SPrintF("%010d", i);
    EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Remove(expr));
  }
  EXPECT_EQ(num_records / 2, dbm->CountSimple());
  EXPECT_EQ(num_records * 20, dbm->GetEffectiveDataSize());
  EXPECT_EQ(file_size, dbm->GetFileSizeSimple());
  for (int32_t i = 1; i < num_records; i += 2) {
    const std::string& expr = tkrzw::SPrintF("%010d", i);
    EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Remove(expr));
  }
  EXPECT_EQ(0, dbm->CountSimple());
  EXPECT_EQ(0, dbm->GetEffectiveDataSize());
  EXPECT_EQ(file_size, dbm->GetFileSizeSimple());
  EXPECT_EQ(0, dbm->CountUsedBuckets());
  for (int32_t i = 0; i < num_records; i++) {
    const std::string& expr = tkrzw::SPrintF("%010d", i);
    EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Set(expr, expr, false));
  }
  EXPECT_EQ(num_records, dbm->CountSimple());
  EXPECT_EQ(num_records * 20, dbm->GetEffectiveDataSize());
  EXPECT_EQ(file_size, dbm->GetFileSizeSimple());
  file_size = dbm->GetFileSizeSimple();
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Close());
  EXPECT_EQ(file_size, tkrzw::GetFileSize(file_path));
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Open(file_path, true));
  EXPECT_EQ(num_records, dbm->CountSimple());
  EXPECT_EQ(num_records * 20, dbm->GetEffectiveDataSize());
  EXPECT_EQ(file_size, dbm->GetFileSizeSimple());
  EXPECT_EQ(tkrzw::HashDBM::UPDATE_IN_PLACE, dbm->GetUpdateMode());
  auto iter = dbm->MakeIterator();
  EXPECT_EQ(tkrzw::Status::SUCCESS, iter->First());
  int32_t iter_count = 0;
  while (true) {
    std::string key, value;
    const tkrzw::Status status = iter->Get(&key, &value);
    if (status != tkrzw::Status::SUCCESS) {
      EXPECT_EQ(tkrzw::Status::NOT_FOUND_ERROR, status);
      break;
    }
    EXPECT_EQ(value, dbm->GetSimple(key));
    iter_count++;
    EXPECT_EQ(tkrzw::Status::SUCCESS, iter->Next());
  }
  EXPECT_EQ(num_records, iter_count);
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Close());
}

void HashDBMTest::HashDBMUpdateAppendingTest(tkrzw::HashDBM* dbm) {
  tkrzw::TemporaryDirectory tmp_dir(true, "tkrzw-");
  const std::string file_path = tmp_dir.MakeUniquePath();
  const std::string offset_file_path = tmp_dir.MakeUniquePath();
  const std::string restore_file_path = tmp_dir.MakeUniquePath();
  tkrzw::HashDBM::TuningParameters tuning_params;
  tuning_params.update_mode = tkrzw::HashDBM::UPDATE_APPENDING;
  tuning_params.record_crc_mode = tkrzw::HashDBM::RECORD_CRC_8;
  tuning_params.offset_width = 4;
  tuning_params.align_pow = 2;
  tuning_params.num_buckets = 10;
  tuning_params.restore_mode = tkrzw::HashDBM::RESTORE_READ_ONLY;
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->OpenAdvanced(
      file_path, true, tkrzw::File::OPEN_DEFAULT, tuning_params));
  EXPECT_TRUE(dbm->IsHealthy());
  EXPECT_FALSE(dbm->IsAutoRestored());
  EXPECT_EQ(0, dbm->CountSimple());
  EXPECT_EQ(10, dbm->CountBuckets());
  EXPECT_EQ(0, dbm->CountUsedBuckets());
  EXPECT_EQ(tkrzw::HashDBM::UPDATE_APPENDING, dbm->GetUpdateMode());
  constexpr int32_t num_records = 100;
  for (int32_t i = 0; i < num_records; i++) {
    const std::string& key = tkrzw::SPrintF("%010d", i);
    const std::string& value = tkrzw::SPrintF("%10d", i);
    EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Set(key, value, false));
  }
  EXPECT_EQ(num_records, dbm->CountSimple());
  EXPECT_EQ(num_records * 20, dbm->GetEffectiveDataSize());
  const int64_t first_file_size = dbm->GetFileSizeSimple();
  for (int32_t i = 0; i < num_records; i++) {
    const std::string& expr = tkrzw::SPrintF("%010d", i);
    EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Set(expr, expr, true));
  }
  EXPECT_EQ(num_records, dbm->CountSimple());
  EXPECT_EQ(num_records * 20, dbm->GetEffectiveDataSize());
  const int64_t second_file_size = dbm->GetFileSizeSimple();
  EXPECT_GT(second_file_size, first_file_size);
  EXPECT_EQ(10, dbm->CountUsedBuckets());
  for (int32_t i = 0; i < num_records; i += 2) {
    const std::string& expr = tkrzw::SPrintF("%010d", i);
    EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Remove(expr));
  }
  EXPECT_EQ(num_records / 2, dbm->CountSimple());
  EXPECT_EQ(num_records * 10, dbm->GetEffectiveDataSize());
  const int64_t third_file_size = dbm->GetFileSizeSimple();
  EXPECT_GT(third_file_size, second_file_size);
  auto iter = dbm->MakeIterator();
  EXPECT_EQ(tkrzw::Status::SUCCESS, iter->First());
  while (true) {
    const tkrzw::Status status = iter->Remove();
    if (status != tkrzw::Status::SUCCESS) {
      EXPECT_EQ(tkrzw::Status::NOT_FOUND_ERROR, status);
      break;
    }
  }
  EXPECT_EQ(0, dbm->CountSimple());
  EXPECT_EQ(0, dbm->GetEffectiveDataSize());
  const int64_t fourth_file_size = dbm->GetFileSizeSimple();
  EXPECT_GT(fourth_file_size, third_file_size);
  EXPECT_EQ(10, dbm->CountUsedBuckets());
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Close());
  EXPECT_EQ(fourth_file_size, tkrzw::GetFileSize(file_path));
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Open(file_path, true));
  for (int32_t i = 0; i < num_records; i++) {
    const std::string& key = tkrzw::SPrintF("%010d", i);
    const std::string& value = tkrzw::SPrintF("%d", i);
    EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Set(key, value, false));
  }
  EXPECT_EQ(num_records, dbm->CountSimple());
  const int64_t fifth_file_size = dbm->GetFileSizeSimple();
  EXPECT_GT(fifth_file_size, fourth_file_size);
  EXPECT_EQ(tkrzw::HashDBM::UPDATE_APPENDING, dbm->GetUpdateMode());
  for (int32_t i = 0; i < num_records; i++) {
    const std::string& key = tkrzw::SPrintF("%010d", i);
    const std::string& value = tkrzw::SPrintF("%d", i);
    EXPECT_EQ(value, dbm->GetSimple(key));
  }
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Close());
  tkrzw::MemoryMapParallelFile in_file;
  EXPECT_EQ(tkrzw::Status::SUCCESS, in_file.Open(file_path, false));
  tkrzw::MemoryMapParallelFile offset_file;
  EXPECT_EQ(tkrzw::Status::SUCCESS, offset_file.Open(offset_file_path, true));
  int64_t in_record_base = 0;
  int32_t in_static_flags = 0;
  int32_t in_offset_width = 0;
  int32_t in_align_pow = 0;
  int64_t last_sync_size = 0;
  EXPECT_EQ(tkrzw::Status::SUCCESS, tkrzw::HashDBM::FindRecordBase(
      &in_file, &in_record_base, &in_static_flags, &in_offset_width, &in_align_pow,
      &last_sync_size));
  EXPECT_GT(in_record_base, 0);
  EXPECT_GT(in_offset_width, 0);
  EXPECT_EQ(tuning_params.offset_width, in_offset_width);
  EXPECT_EQ(tuning_params.align_pow, in_align_pow);
  EXPECT_EQ(in_file.GetSizeSimple(), last_sync_size);
  EXPECT_EQ(tkrzw::Status::SUCCESS, in_file.Close());
  EXPECT_EQ(tkrzw::Status::SUCCESS, in_file.Open(file_path, true));
  EXPECT_EQ(tkrzw::Status::SUCCESS, in_file.Write(0, "0123", 4));
  EXPECT_EQ(tkrzw::Status::SUCCESS, tkrzw::HashDBM::FindRecordBase(
      &in_file, &in_record_base, &in_static_flags, &in_offset_width, &in_align_pow,
      &last_sync_size));
  EXPECT_GT(in_record_base, 0);
  EXPECT_GT(in_offset_width, 0);
  EXPECT_EQ(tuning_params.offset_width, in_offset_width);
  EXPECT_EQ(tuning_params.align_pow, in_align_pow);
  EXPECT_EQ(0, last_sync_size);
  const int32_t in_crc_width = tkrzw::HashDBM::GetCRCWidthFromStaticFlags(in_static_flags);
  EXPECT_EQ(tkrzw::Status::SUCCESS, tkrzw::HashRecord::ExtractOffsets(
      &in_file, &offset_file, in_record_base, in_crc_width, in_offset_width, in_align_pow,
      false, -1));
  EXPECT_GT(offset_file.GetSizeSimple(), 0);
  EXPECT_EQ(tkrzw::Status::SUCCESS, offset_file.Close());
  EXPECT_EQ(tkrzw::Status::SUCCESS, in_file.Close());
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->OpenAdvanced(
      restore_file_path, true, tkrzw::File::OPEN_TRUNCATE, tuning_params));
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->ImportFromFileForward(file_path, false, -1, -1));
  EXPECT_EQ(num_records, dbm->CountSimple());
  EXPECT_EQ(fifth_file_size, dbm->GetFileSizeSimple());
  for (int32_t i = 0; i < num_records; i++) {
    const std::string& key = tkrzw::SPrintF("%010d", i);
    const std::string& value = tkrzw::SPrintF("%d", i);
    EXPECT_EQ(value, dbm->GetSimple(key));
  }
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Close());
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->OpenAdvanced(
      restore_file_path, true, tkrzw::File::OPEN_TRUNCATE, tuning_params));
  EXPECT_EQ(tkrzw::Status::SUCCESS,
            dbm->ImportFromFileForward(file_path, false, -1, first_file_size));
  EXPECT_EQ(num_records, dbm->CountSimple());
  EXPECT_EQ(first_file_size, dbm->GetFileSizeSimple());
  for (int32_t i = 0; i < num_records; i++) {
    const std::string& key = tkrzw::SPrintF("%010d", i);
    const std::string& value = tkrzw::SPrintF("%10d", i);
    EXPECT_EQ(value, dbm->GetSimple(key));
  }
  EXPECT_EQ(num_records, dbm->CountSimple());
  EXPECT_EQ(num_records * 20, dbm->GetEffectiveDataSize());
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Close());
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->OpenAdvanced(
      restore_file_path, true, tkrzw::File::OPEN_TRUNCATE, tuning_params));
  EXPECT_EQ(tkrzw::Status::SUCCESS,
            dbm->ImportFromFileForward(file_path, false, -1, second_file_size));
  EXPECT_EQ(num_records, dbm->CountSimple());
  EXPECT_EQ(second_file_size, dbm->GetFileSizeSimple());
  for (int32_t i = 0; i < num_records; i++) {
    const std::string& key = tkrzw::SPrintF("%010d", i);
    const std::string& value = tkrzw::SPrintF("%010d", i);
    EXPECT_EQ(value, dbm->GetSimple(key));
  }
  EXPECT_EQ(num_records, dbm->CountSimple());
  EXPECT_EQ(num_records * 20, dbm->GetEffectiveDataSize());
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Close());
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->OpenAdvanced(
      restore_file_path, true, tkrzw::File::OPEN_TRUNCATE, tuning_params));
  EXPECT_EQ(tkrzw::Status::SUCCESS,
            dbm->ImportFromFileForward(file_path, false, -1, third_file_size));
  EXPECT_EQ(num_records / 2, dbm->CountSimple());
  EXPECT_EQ(num_records * 10, dbm->GetEffectiveDataSize());
  EXPECT_EQ(third_file_size, dbm->GetFileSizeSimple());
  for (int32_t i = 0; i < num_records; i++) {
    const std::string& expr = tkrzw::SPrintF("%010d", i);
    if (i % 2 == 0) {
      EXPECT_EQ(tkrzw::Status::NOT_FOUND_ERROR, dbm->Get(expr));
    } else {
      EXPECT_EQ(expr, dbm->GetSimple(expr));
    }
  }
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Close());
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->OpenAdvanced(
      restore_file_path, true, tkrzw::File::OPEN_TRUNCATE, tuning_params));
  EXPECT_EQ(tkrzw::Status::SUCCESS,
            dbm->ImportFromFileForward(file_path, false, -1, fourth_file_size));
  EXPECT_EQ(0, dbm->CountSimple());
  EXPECT_EQ(0, dbm->GetEffectiveDataSize());
  EXPECT_EQ(fourth_file_size, dbm->GetFileSizeSimple());
  for (int32_t i = 0; i < num_records; i++) {
    const std::string& expr = tkrzw::SPrintF("%010d", i);
    EXPECT_EQ(tkrzw::Status::NOT_FOUND_ERROR, dbm->Get(expr));
  }
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Close());
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->OpenAdvanced(
      restore_file_path, true, tkrzw::File::OPEN_TRUNCATE, tuning_params));
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->ImportFromFileBackward(file_path, false, -1, -1));
  EXPECT_EQ(num_records, dbm->CountSimple());
  const int64_t sixth_size = dbm->GetFileSizeSimple();
  EXPECT_LE(sixth_size, first_file_size);
  for (int32_t i = 0; i < num_records; i++) {
    const std::string& key = tkrzw::SPrintF("%010d", i);
    const std::string& value = tkrzw::SPrintF("%d", i);
    EXPECT_EQ(value, dbm->GetSimple(key));
  }
  class MixProc final : public tkrzw::DBM::RecordProcessor {
   public:
    std::string_view ProcessFull(std::string_view key, std::string_view value) override {
      if (tkrzw::StrToInt(key) % 2 == 0) {
        return REMOVE;
      }
      return "";
    }
    std::string_view ProcessEmpty(std::string_view key) override {
      return NOOP;
    }
   private:
    std::string new_value_;
  } mix_proc;
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->ProcessEach(&mix_proc, true));
  EXPECT_EQ(num_records / 2, dbm->CountSimple());
  const int64_t seventh_size = dbm->GetFileSizeSimple();
  EXPECT_GT(seventh_size, sixth_size);
  for (int32_t i = 0; i < num_records; i++) {
    const std::string& key = tkrzw::SPrintF("%010d", i);
    if (i % 2 == 0) {
      EXPECT_EQ(tkrzw::Status::NOT_FOUND_ERROR, dbm->Get(key));
    } else {
      std::string value;
      EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Get(key, &value));
      EXPECT_EQ("", value);
    }
  }
  iter = dbm->MakeIterator();
  EXPECT_EQ(tkrzw::Status::SUCCESS, iter->First());
  while (true) {
    std::string key, value;
    const tkrzw::Status status = iter->Get(&key, &value);
    if (status != tkrzw::Status::SUCCESS) {
      EXPECT_EQ(tkrzw::Status::NOT_FOUND_ERROR, status);
      break;
    }
    EXPECT_EQ("", value);
    EXPECT_EQ(tkrzw::Status::SUCCESS, iter->Set("*"));
    EXPECT_EQ(tkrzw::Status::SUCCESS, iter->Get(&key, &value));
    EXPECT_EQ("*", value);
    EXPECT_EQ(tkrzw::Status::SUCCESS, iter->Next());
  }
  EXPECT_GT(dbm->GetFileSizeSimple(), seventh_size);
  class CheckProc final : public tkrzw::DBM::RecordProcessor {
   public:
    std::string_view ProcessFull(std::string_view key, std::string_view value) override {
      EXPECT_EQ("*", value);
      return NOOP;
    }
  } check_proc;
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->ProcessEach(&check_proc, false));
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Close());
}

void HashDBMTest::HashDBMVariousSizeTest(tkrzw::HashDBM* dbm) {
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
          tkrzw::HashDBM::TuningParameters tuning_params;
          tuning_params.update_mode = update_mode;
          tuning_params.record_crc_mode = record_crc_mode;
          tuning_params.record_comp_mode = record_comp_mode;
          tuning_params.align_pow = align_pow;
          tuning_params.num_buckets = 100;
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

void HashDBMTest::HashDBMRebuildStaticTestOne(
    tkrzw::HashDBM* dbm, const tkrzw::HashDBM::TuningParameters& tuning_params) {
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
  EXPECT_EQ(tkrzw::Status::NOT_FOUND_ERROR, iter->Get());
  for (const auto& record : first_data) {
    EXPECT_EQ(record.second, dbm->GetSimple(record.first));
  }
  EXPECT_EQ(first_data.size(), dbm->CountSimple());
  EXPECT_EQ(first_eff_data_size, dbm->GetEffectiveDataSize());
  EXPECT_EQ(tuning_params.update_mode, dbm->GetUpdateMode());
  EXPECT_EQ(123, dbm->GetDatabaseType());
  EXPECT_EQ("0123456789", dbm->GetOpaqueMetadata().substr(0, 10));
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Close());
}

void HashDBMTest::HashDBMRebuildStaticTestAll(tkrzw::HashDBM* dbm) {
  const std::vector<tkrzw::HashDBM::UpdateMode> update_modes =
      {tkrzw::HashDBM::UPDATE_IN_PLACE, tkrzw::HashDBM::UPDATE_APPENDING};
  const std::vector<tkrzw::HashDBM::RecordCRCMode> record_crc_modes =
      {tkrzw::HashDBM::RECORD_CRC_NONE, tkrzw::HashDBM::RECORD_CRC_16};
  const std::vector<tkrzw::HashDBM::RecordCompressionMode> record_comp_modes =
      {tkrzw::HashDBM::RECORD_COMP_NONE, tkrzw::HashDBM::RECORD_COMP_ZSTD};
  const std::vector<int32_t> offset_widths = {3, 4};
  const std::vector<int32_t> align_pows = {0, 2, 4};
  const std::vector<int32_t> nums_buckets = {1000};
  for (const auto& update_mode : update_modes) {
    for (const auto& record_crc_mode : record_crc_modes) {
      for (const auto& record_comp_mode : record_comp_modes) {
        if (!tkrzw::HashDBM::CheckRecordCompressionModeIsSupported(record_comp_mode)) {
          continue;
        }
        for (const auto& offset_width : offset_widths) {
          for (const auto& align_pow : align_pows) {
            for (const auto& num_buckets : nums_buckets) {
              tkrzw::HashDBM::TuningParameters tuning_params;
              tuning_params.update_mode = update_mode;
              tuning_params.record_crc_mode = record_crc_mode;
              tuning_params.record_comp_mode = record_comp_mode;
              tuning_params.offset_width = offset_width;
              tuning_params.align_pow = align_pow;
              tuning_params.num_buckets = num_buckets;
              tuning_params.restore_mode = tkrzw::HashDBM::RESTORE_READ_ONLY;
              HashDBMRebuildStaticTestOne(dbm, tuning_params);
            }
          }
        }
      }
    }
  }
}

void HashDBMTest::HashDBMRebuildRandomTest(tkrzw::HashDBM* dbm) {
  tkrzw::TemporaryDirectory tmp_dir(true, "tkrzw-");
  const std::string file_path = tmp_dir.MakeUniquePath();
  const std::vector<tkrzw::HashDBM::UpdateMode> update_modes =
      {tkrzw::HashDBM::UPDATE_IN_PLACE, tkrzw::HashDBM::UPDATE_APPENDING};
  const std::vector<tkrzw::HashDBM::RecordCRCMode> record_crc_modes =
      {tkrzw::HashDBM::RECORD_CRC_16};
  const std::vector<tkrzw::HashDBM::RecordCompressionMode> record_comp_modes =
      {tkrzw::HashDBM::RECORD_COMP_NONE, tkrzw::HashDBM::RECORD_COMP_ZSTD};
  const std::vector<int32_t> offset_widths = {4};
  const std::vector<int32_t> align_pows = {0, 2};
  const std::vector<int32_t> nums_buckets = {1000};
  for (const auto& update_mode : update_modes) {
    for (const auto& record_crc_mode : record_crc_modes) {
      for (const auto& record_comp_mode : record_comp_modes) {
        if (!tkrzw::HashDBM::CheckRecordCompressionModeIsSupported(record_comp_mode)) {
          continue;
        }
        for (const auto& offset_width : offset_widths) {
          for (const auto& align_pow : align_pows) {
            for (const auto& num_buckets : nums_buckets) {
              tkrzw::HashDBM::TuningParameters tuning_params;
              tuning_params.update_mode = update_mode;
              tuning_params.record_crc_mode = record_crc_mode;
              tuning_params.record_comp_mode = record_comp_mode;
              tuning_params.offset_width = offset_width;
              tuning_params.align_pow = align_pow;
              tuning_params.num_buckets = num_buckets;
              tuning_params.restore_mode = tkrzw::HashDBM::RESTORE_READ_ONLY;
              EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->OpenAdvanced(
                  file_path, true, tkrzw::File::OPEN_TRUNCATE, tuning_params));
              RebuildRandomTest(dbm);
              EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Close());
            }
          }
        }
      }
    }
  }
}

void HashDBMTest::HashDBMRestoreTest(tkrzw::HashDBM* dbm) {
  tkrzw::TemporaryDirectory tmp_dir(true, "tkrzw-");
  const std::string first_file_path = tmp_dir.MakeUniquePath();
  const std::string second_file_path = tmp_dir.MakeUniquePath();
  const std::string third_file_path = tmp_dir.MakeUniquePath();
  tkrzw::HashDBM second_dbm;
  tkrzw::HashDBM::TuningParameters tuning_params;
  tuning_params.update_mode = tkrzw::HashDBM::UPDATE_APPENDING;
  tuning_params.record_crc_mode = tkrzw::HashDBM::RECORD_CRC_16;
  tuning_params.offset_width = 3;
  tuning_params.align_pow = 0;
  tuning_params.num_buckets = 10;
  tuning_params.restore_mode = tkrzw::HashDBM::RESTORE_READ_ONLY;
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->OpenAdvanced(
      first_file_path, true, tkrzw::File::OPEN_TRUNCATE, tuning_params));
  EXPECT_TRUE(dbm->IsHealthy());
  EXPECT_FALSE(dbm->IsAutoRestored());
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->SetDatabaseType(123));
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->SetOpaqueMetadata("0123456789"));
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Set("x", "x"));
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Set("x", "xx"));
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Remove("x"));
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Set("x", "xxx"));
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Synchronize(false));
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Remove("x"));
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Set("x", "xxxx"));
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Set("y", "yyyy"));
  EXPECT_EQ(2, dbm->CountSimple());
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->ValidateHashBuckets());
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->ValidateRecords(-1, -1));
  EXPECT_EQ(tkrzw::Status::SUCCESS, second_dbm.OpenAdvanced(
      second_file_path, true, tkrzw::File::OPEN_TRUNCATE, tuning_params));
  EXPECT_EQ(tkrzw::Status::SUCCESS, second_dbm.ImportFromFileForward(
      first_file_path, false, -1, 0));
  EXPECT_EQ(1, second_dbm.CountSimple());
  EXPECT_EQ(tkrzw::Status::SUCCESS, second_dbm.ValidateHashBuckets());
  EXPECT_EQ(tkrzw::Status::SUCCESS, second_dbm.ValidateRecords(-1, -1));
  EXPECT_EQ("xxx", second_dbm.GetSimple("x"));
  EXPECT_EQ(tkrzw::Status::SUCCESS, second_dbm.Close());
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Remove("x"));
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Remove("y"));
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Set("x", "xx"));
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Set("y", "yy"));
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Synchronize(false));
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Set("z", "zz"));
  EXPECT_EQ(3, dbm->CountSimple());
  EXPECT_EQ(tkrzw::Status::SUCCESS, second_dbm.OpenAdvanced(
      second_file_path, true, tkrzw::File::OPEN_TRUNCATE, tuning_params));
  EXPECT_EQ(tkrzw::Status::SUCCESS, second_dbm.ImportFromFileBackward(
      first_file_path, false, -1, 0));
  EXPECT_EQ(2, second_dbm.CountSimple());
  EXPECT_EQ(tkrzw::Status::SUCCESS, second_dbm.ValidateHashBuckets());
  EXPECT_EQ(tkrzw::Status::SUCCESS, second_dbm.ValidateRecords(-1, -1));
  EXPECT_EQ("xx", second_dbm.GetSimple("x"));
  EXPECT_EQ("yy", second_dbm.GetSimple("y"));
  EXPECT_EQ(tkrzw::Status::SUCCESS, second_dbm.Close());
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Close());
  EXPECT_EQ(tkrzw::Status::SUCCESS, tkrzw::RemoveFile(second_file_path));
  EXPECT_EQ(tkrzw::Status::SUCCESS,
            tkrzw::HashDBM::RestoreDatabase(first_file_path, second_file_path, -1));
  EXPECT_EQ(tkrzw::Status::SUCCESS, second_dbm.Open(second_file_path, true));
  EXPECT_EQ(123, second_dbm.GetDatabaseType());
  EXPECT_EQ("0123456789", second_dbm.GetOpaqueMetadata().substr(0, 10));
  EXPECT_EQ(tkrzw::HashDBM::UPDATE_APPENDING, second_dbm.GetUpdateMode());
  EXPECT_EQ(3, second_dbm.CountSimple());
  EXPECT_EQ(tkrzw::Status::SUCCESS, second_dbm.ValidateHashBuckets());
  EXPECT_EQ(tkrzw::Status::SUCCESS, second_dbm.ValidateRecords(-1, -1));
  EXPECT_EQ("xx", second_dbm.GetSimple("x"));
  EXPECT_EQ("yy", second_dbm.GetSimple("y"));
  EXPECT_EQ("zz", second_dbm.GetSimple("z"));
  EXPECT_EQ(tkrzw::Status::SUCCESS, second_dbm.Close());
  EXPECT_EQ(tkrzw::Status::SUCCESS, second_dbm.Open(
      second_file_path, true, tkrzw::File::OPEN_TRUNCATE));
  EXPECT_EQ(tkrzw::Status::SUCCESS, second_dbm.Close());
  EXPECT_EQ(tkrzw::Status::SUCCESS,
            tkrzw::HashDBM::RestoreDatabase(first_file_path, second_file_path, -1));
  EXPECT_EQ(tkrzw::Status::SUCCESS, second_dbm.Open(second_file_path, true));
  EXPECT_TRUE(second_dbm.IsHealthy());
  EXPECT_FALSE(second_dbm.IsAutoRestored());
  EXPECT_EQ(123, second_dbm.GetDatabaseType());
  EXPECT_EQ("0123456789", second_dbm.GetOpaqueMetadata().substr(0, 10));
  EXPECT_EQ(tkrzw::HashDBM::UPDATE_IN_PLACE, second_dbm.GetUpdateMode());
  EXPECT_EQ(3, second_dbm.CountSimple());
  EXPECT_EQ(tkrzw::Status::SUCCESS, second_dbm.ValidateHashBuckets());
  EXPECT_EQ(tkrzw::Status::SUCCESS, second_dbm.ValidateRecords(-1, -1));
  EXPECT_EQ("xx", second_dbm.GetSimple("x"));
  EXPECT_EQ("yy", second_dbm.GetSimple("y"));
  EXPECT_EQ("zz", second_dbm.GetSimple("z"));
  EXPECT_EQ(tkrzw::Status::SUCCESS, second_dbm.Close());
  EXPECT_EQ(tkrzw::Status::SUCCESS,
            tkrzw::HashDBM::RestoreDatabase(second_file_path, third_file_path, -1));
  EXPECT_EQ(tkrzw::Status::SUCCESS, second_dbm.Open(third_file_path, true));
  EXPECT_EQ(123, second_dbm.GetDatabaseType());
  EXPECT_EQ("0123456789", second_dbm.GetOpaqueMetadata().substr(0, 10));
  EXPECT_EQ(tkrzw::HashDBM::UPDATE_IN_PLACE, second_dbm.GetUpdateMode());
  EXPECT_EQ(3, second_dbm.CountSimple());
  EXPECT_EQ(tkrzw::Status::SUCCESS, second_dbm.ValidateHashBuckets());
  EXPECT_EQ(tkrzw::Status::SUCCESS, second_dbm.ValidateRecords(-1, -1));
  EXPECT_EQ("xx", second_dbm.GetSimple("x"));
  EXPECT_EQ("yy", second_dbm.GetSimple("y"));
  EXPECT_EQ("zz", second_dbm.GetSimple("z"));
  EXPECT_EQ(tkrzw::Status::SUCCESS, second_dbm.Close());
  EXPECT_EQ(tkrzw::Status::SUCCESS, tkrzw::RemoveFile(first_file_path));
  EXPECT_EQ(tkrzw::Status::SUCCESS, tkrzw::RemoveFile(second_file_path));
  EXPECT_EQ(tkrzw::Status::SUCCESS, tkrzw::RemoveFile(third_file_path));
  const std::vector<tkrzw::HashDBM::UpdateMode> update_modes =
      {tkrzw::HashDBM::UPDATE_IN_PLACE, tkrzw::HashDBM::UPDATE_APPENDING};
  constexpr int32_t num_iterations = 10000;
  for (const auto& update_mode : update_modes) {
    tuning_params.update_mode = update_mode;
    tuning_params.num_buckets = num_iterations / 2;
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

void HashDBMTest::HashDBMAutoRestoreTest(tkrzw::HashDBM* dbm) {
  tkrzw::TemporaryDirectory tmp_dir(true, "tkrzw-");
  const std::string file_path = tmp_dir.MakeUniquePath();
  const std::vector<tkrzw::HashDBM::UpdateMode> update_modes =
      {tkrzw::HashDBM::UPDATE_IN_PLACE, tkrzw::HashDBM::UPDATE_APPENDING};
  const std::vector<tkrzw::HashDBM::RecordCRCMode> record_crc_modes =
      {tkrzw::HashDBM::RECORD_CRC_NONE, tkrzw::HashDBM::RECORD_CRC_16};
  const std::vector<tkrzw::HashDBM::RecordCompressionMode> record_comp_modes =
      {tkrzw::HashDBM::RECORD_COMP_NONE, tkrzw::HashDBM::RECORD_COMP_ZSTD};
  for (const auto& update_mode : update_modes) {
    for (const auto& record_crc_mode : record_crc_modes) {
      for (const auto& record_comp_mode : record_comp_modes) {
        if (!tkrzw::HashDBM::CheckRecordCompressionModeIsSupported(record_comp_mode)) {
          continue;
        }
        tkrzw::HashDBM::TuningParameters tuning_params;
        tuning_params.update_mode = update_mode;
        tuning_params.record_crc_mode = record_crc_mode;
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
        EXPECT_EQ(tkrzw::Status::BROKEN_DATA_ERROR, dbm->ValidateRecords(-1, -1));
        std::string value;
        EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Get("one", &value));
        EXPECT_EQ("first", value);
        EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Get("two", &value));
        EXPECT_EQ("second", value);
        EXPECT_EQ(0, dbm->CountSimple());
        EXPECT_EQ(tkrzw::Status::PRECONDITION_ERROR, dbm->Set("three", "third"));
        EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Close());
        tuning_params.restore_mode =
            tkrzw::HashDBM::RESTORE_DEFAULT | tkrzw::HashDBM::RESTORE_WITH_HARDSYNC;
        EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->OpenAdvanced(
            file_path, true, tkrzw::File::OPEN_DEFAULT, tuning_params));
        EXPECT_TRUE(dbm->IsHealthy());
        EXPECT_TRUE(dbm->IsAutoRestored());
        EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->ValidateRecords(-1, -1));
        EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Get("one", &value));
        EXPECT_EQ("first", value);
        EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Get("two", &value));
        EXPECT_EQ("second", value);
        EXPECT_EQ(2, dbm->CountSimple());
        EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Close());
        EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->OpenAdvanced(
            file_path, true, tkrzw::File::OPEN_TRUNCATE, tuning_params));
        EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Set("one", "first"));
        EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Synchronize(false));
        EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Set("two", "second"));
        file = dbm->GetInternalFile();
        EXPECT_EQ(tkrzw::Status::SUCCESS, file->Close());
        EXPECT_EQ(tkrzw::Status::PRECONDITION_ERROR, dbm->Close());
        tuning_params.restore_mode =
            tkrzw::HashDBM::RESTORE_SYNC | tkrzw::HashDBM::RESTORE_WITH_HARDSYNC;
        EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->OpenAdvanced(
            file_path, true, tkrzw::File::OPEN_DEFAULT, tuning_params));
        EXPECT_TRUE(dbm->IsHealthy());
        EXPECT_TRUE(dbm->IsAutoRestored());
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
        EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->OpenAdvanced(
            file_path, true, tkrzw::File::OPEN_TRUNCATE, tuning_params));
        EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Set("one", "first"));
        EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Set("two", "second"));
        EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Synchronize(false));
        file = dbm->GetInternalFile();
        EXPECT_EQ(tkrzw::Status::SUCCESS, file->Close());
        EXPECT_EQ(tkrzw::Status::PRECONDITION_ERROR, dbm->Close());
        tuning_params.restore_mode =
            tkrzw::HashDBM::RESTORE_DEFAULT | tkrzw::HashDBM::RESTORE_WITH_HARDSYNC;
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
        EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Get("two", &value));
        EXPECT_EQ("second", value);
        EXPECT_EQ(2, dbm->CountSimple());
        EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Set("three", "third"));
        EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Get("three", &value));
        EXPECT_EQ("third", value);
        EXPECT_EQ(3, dbm->CountSimple());
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
        EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Get("two", &value));
        EXPECT_EQ("second", value);
        EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Get("three", &value));
        EXPECT_EQ("third", value);
        EXPECT_EQ(3, dbm->CountSimple());
        EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Close());
        EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->OpenAdvanced(
            file_path, true, tkrzw::File::OPEN_TRUNCATE, tuning_params));
        EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Set("one", "first"));
        EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Set("two", "second"));
        EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Synchronize(false));
        EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Set("three", "third"));
        file = dbm->GetInternalFile();
        EXPECT_EQ(tkrzw::Status::SUCCESS, file->Expand(256 * 1024));
        EXPECT_EQ(tkrzw::Status::SUCCESS, file->Close());
        EXPECT_EQ(tkrzw::Status::PRECONDITION_ERROR, dbm->Close());
        EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->OpenAdvanced(file_path, true));
        EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Get("one", &value));
        EXPECT_EQ("first", value);
        EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Get("two", &value));
        EXPECT_EQ("second", value);
        EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Get("three", &value));
        EXPECT_EQ("third", value);
        EXPECT_EQ(3, dbm->CountSimple());
        EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Close());
        tuning_params.num_buckets = 1;
        EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->OpenAdvanced(
            file_path, true, tkrzw::File::OPEN_TRUNCATE, tuning_params));
        file = dbm->GetInternalFile();
        EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Set("one", "first"));
        char bucket_buf[4];
        EXPECT_EQ(tkrzw::Status::SUCCESS, file->Read(128, bucket_buf, 4));
        EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Set(
            "two", "This is a pen. I am a boy. They have three dogs."));
        EXPECT_EQ(tkrzw::Status::SUCCESS, file->Write(128, bucket_buf, 4));
        EXPECT_EQ(tkrzw::Status::SUCCESS, file->Write(
            file->GetSizeSimple() - 10, "\xFF\xEE", 2));
        EXPECT_EQ(tkrzw::Status::SUCCESS, file->Close());
        EXPECT_EQ(tkrzw::Status::PRECONDITION_ERROR, dbm->Close());
        EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->OpenAdvanced(file_path, true));
        EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Get("one", &value));
        EXPECT_EQ("first", value);
        EXPECT_EQ(tkrzw::Status::NOT_FOUND_ERROR, dbm->Get("two", &value));
        EXPECT_EQ(1, dbm->CountSimple());
        EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Close());
      }
    }
  }
}

void HashDBMTest::HashDBMCorruptionTest(tkrzw::HashDBM* dbm) {
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
        tkrzw::HashDBM::TuningParameters tuning_params;
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
        const tkrzw::Status status = dbm->Get("abc", &rec_value);
        EXPECT_TRUE(status == tkrzw::Status::SUCCESS ||
                    status == tkrzw::Status::BROKEN_DATA_ERROR);
        EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Close());
        EXPECT_EQ(tkrzw::Status::SUCCESS,
                  tkrzw::HashDBM::RestoreDatabase(corrupt_file_path, restored_file_path, -1));
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

void HashDBMTest::HashDBMDirectIOTest(tkrzw::HashDBM* dbm) {
  tkrzw::TemporaryDirectory tmp_dir(true, "tkrzw-");
  const std::string file_path = tmp_dir.MakeUniquePath();
  tkrzw::HashDBM::TuningParameters tuning_params;
  tuning_params.update_mode = tkrzw::HashDBM::UPDATE_IN_PLACE;
  tuning_params.record_crc_mode = tkrzw::HashDBM::RECORD_CRC_32;
  tuning_params.offset_width = 4;
  tuning_params.align_pow = 0;
  tuning_params.num_buckets = 3;
  tuning_params.restore_mode = tkrzw::HashDBM::RESTORE_READ_ONLY;
  tuning_params.cache_buckets = 1;
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
    std::string value;
    EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Get(key, &value));
    EXPECT_EQ(tkrzw::ToString(i % 2 == 0 ? i : i * i * i), value);
  }
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Close());
  tuning_params.offset_width = 4;
  tuning_params.align_pow = 0;
  tuning_params.num_buckets = 3;
  tuning_params.restore_mode = tkrzw::HashDBM::RESTORE_READ_ONLY;
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->OpenAdvanced(
      file_path, true, tkrzw::File::OPEN_DEFAULT, tuning_params));
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->RebuildAdvanced(tuning_params));
  for (int32_t i = 1; i <= 20; i++) {
    const std::string& key = tkrzw::ToString(i * i);
    std::string value;
    EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Get(key, &value));
    EXPECT_EQ(tkrzw::ToString(i % 2 == 0 ? i : i * i * i), value);
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

void HashDBMTest::HashDBMUpdateLoggerTest(tkrzw::HashDBM* dbm) {
  tkrzw::TemporaryDirectory tmp_dir(true, "tkrzw-");
  const std::string file_path = tmp_dir.MakeUniquePath();
  const std::vector<tkrzw::HashDBM::RecordCompressionMode> record_comp_modes =
      {tkrzw::HashDBM::RECORD_COMP_NONE, tkrzw::HashDBM::RECORD_COMP_ZSTD};
  for (const auto& record_comp_mode : record_comp_modes) {
    if (!tkrzw::HashDBM::CheckRecordCompressionModeIsSupported(record_comp_mode)) {
      continue;
    }
    tkrzw::HashDBM::TuningParameters tuning_params;
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

TEST_F(HashDBMTest, EmptyDatabase) {
  tkrzw::HashDBM dbm(std::make_unique<tkrzw::MemoryMapParallelFile>());
  HashDBMEmptyDatabaseTest(&dbm);
}

TEST_F(HashDBMTest, File) {
  tkrzw::HashDBM dbm(std::make_unique<tkrzw::MemoryMapParallelFile>());
  HashDBMFileTest(&dbm);
}

TEST_F(HashDBMTest, LargeRecord) {
  tkrzw::HashDBM dbm(std::make_unique<tkrzw::MemoryMapParallelFile>());
  HashDBMLargeRecordTest(&dbm);
}

TEST_F(HashDBMTest, Basic) {
  tkrzw::HashDBM dbm(std::make_unique<tkrzw::MemoryMapParallelFile>());
  HashDBMBasicTest(&dbm);
}

TEST_F(HashDBMTest, Sequence) {
  tkrzw::HashDBM dbm(std::make_unique<tkrzw::MemoryMapParallelFile>());
  HashDBMSequenceTest(&dbm);
}

TEST_F(HashDBMTest, Append) {
  tkrzw::HashDBM dbm(std::make_unique<tkrzw::MemoryMapParallelFile>());
  HashDBMAppendTest(&dbm);
}

TEST_F(HashDBMTest, Process) {
  tkrzw::HashDBM dbm(std::make_unique<tkrzw::MemoryMapParallelFile>());
  HashDBMProcessTest(&dbm);
}

TEST_F(HashDBMTest, ProcessMulti) {
  tkrzw::HashDBM dbm(std::make_unique<tkrzw::MemoryMapParallelFile>());
  HashDBMProcessMultiTest(&dbm);
}

TEST_F(HashDBMTest, ProcessEach) {
  tkrzw::HashDBM dbm(std::make_unique<tkrzw::MemoryMapParallelFile>());
  HashDBMProcessEachTest(&dbm);
}

TEST_F(HashDBMTest, Random) {
  tkrzw::HashDBM dbm(std::make_unique<tkrzw::MemoryMapParallelFile>());
  HashDBMRandomTestOne(&dbm);
}

TEST_F(HashDBMTest, RandomThread) {
  tkrzw::HashDBM dbm(std::make_unique<tkrzw::MemoryMapParallelFile>());
  HashDBMRandomTestThread(&dbm);
}

TEST_F(HashDBMTest, RecordMigration) {
  tkrzw::HashDBM dbm(std::make_unique<tkrzw::MemoryMapParallelFile>());
  tkrzw::MemoryMapParallelFile file;
  HashDBMRecordMigrationTest(&dbm, &file);
}

TEST_F(HashDBMTest, OpenClose) {
  tkrzw::HashDBM dbm(std::make_unique<tkrzw::MemoryMapParallelFile>());
  HashDBMOpenCloseTest(&dbm);
}

TEST_F(HashDBMTest, UpdateInPlace) {
  tkrzw::HashDBM dbm(std::make_unique<tkrzw::MemoryMapParallelFile>());
  HashDBMUpdateInPlaceTest(&dbm);
}

TEST_F(HashDBMTest, UpdateAppending) {
  tkrzw::HashDBM dbm(std::make_unique<tkrzw::MemoryMapParallelFile>());
  HashDBMUpdateAppendingTest(&dbm);
}

TEST_F(HashDBMTest, VariousSize) {
  tkrzw::HashDBM dbm(std::make_unique<tkrzw::MemoryMapParallelFile>());
  HashDBMVariousSizeTest(&dbm);
}

TEST_F(HashDBMTest, RebuildStatic) {
  tkrzw::HashDBM dbm(std::make_unique<tkrzw::MemoryMapParallelFile>());
  HashDBMRebuildStaticTestAll(&dbm);
}

TEST_F(HashDBMTest, RebuildRandom) {
  tkrzw::HashDBM dbm(std::make_unique<tkrzw::MemoryMapParallelFile>());
  HashDBMRebuildRandomTest(&dbm);
}

TEST_F(HashDBMTest, Restore) {
  tkrzw::HashDBM dbm(std::make_unique<tkrzw::MemoryMapParallelFile>());
  HashDBMRestoreTest(&dbm);
}

TEST_F(HashDBMTest, AutoRestore) {
  tkrzw::HashDBM dbm(std::make_unique<tkrzw::MemoryMapParallelFile>());
  HashDBMAutoRestoreTest(&dbm);
}

TEST_F(HashDBMTest, Corruption) {
  tkrzw::HashDBM dbm(std::make_unique<tkrzw::MemoryMapParallelFile>());
  HashDBMCorruptionTest(&dbm);
}

TEST_F(HashDBMTest, DirectIO) {
  auto file = std::make_unique<tkrzw::PositionalParallelFile>();
  EXPECT_EQ(tkrzw::Status::SUCCESS, file->SetAccessStrategy(
      512, tkrzw::PositionalFile::ACCESS_DIRECT | tkrzw::PositionalFile::ACCESS_PADDING));
  tkrzw::HashDBM dbm(std::move(file));
  HashDBMDirectIOTest(&dbm);
}

TEST_F(HashDBMTest, UpdateLogger) {
  tkrzw::HashDBM dbm(std::make_unique<tkrzw::MemoryMapParallelFile>());
  HashDBMUpdateLoggerTest(&dbm);
}

// END OF FILE
