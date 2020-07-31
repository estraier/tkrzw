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

#include "gtest/gtest.h"
#include "gmock/gmock.h"

#include "tkrzw_dbm.h"
#include "tkrzw_dbm_hash.h"
#include "tkrzw_dbm_hash_impl.h"
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

class HashDBMTest : public CommonDBMTest {
 protected:
  void HashDBMEmptyDatabaseTest(tkrzw::HashDBM* dbm);
  void HashDBMFileTest(tkrzw::HashDBM* dbm);
  void HashDBMLargeRecordTest(tkrzw::HashDBM* dbm);
  void HashDBMBasicTest(tkrzw::HashDBM* dbm);
  void HashDBMSequenceTest(tkrzw::HashDBM* dbm);
  void HashDBMAppendTest(tkrzw::HashDBM* dbm);
  void HashDBMProcessTest(tkrzw::HashDBM* dbm);
  void HashDBMProcessEachTest(tkrzw::HashDBM* dbm);
  void HashDBMRandomTestOne(tkrzw::HashDBM* dbm);
  void HashDBMRandomTestThread(tkrzw::HashDBM* dbm);
  void HashDBMRecordMigrationTest(tkrzw::HashDBM* dbm, tkrzw::File* file);
  void HashDBMOpenCloseTest(tkrzw::HashDBM* dbm);
  void HashDBMUpdateInPlaceTest(tkrzw::HashDBM* dbm);
  void HashDBMUpdateAppendingTest(tkrzw::HashDBM* dbm);
  void HashDBMRebuildStaticTestOne(
      tkrzw::HashDBM* dbm, const tkrzw::HashDBM::TuningParameters& tuning_params);
  void HashDBMRebuildStaticTestAll(tkrzw::HashDBM* dbm);
  void HashDBMRebuildRandomTest(tkrzw::HashDBM* dbm);
  void HashDBMRestoreTest(tkrzw::HashDBM* dbm);
};

void HashDBMTest::HashDBMEmptyDatabaseTest(tkrzw::HashDBM* dbm) {
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
  const std::vector<int32_t> offset_widths = {4};
  const std::vector<int32_t> align_pows = {3};
  const std::vector<int32_t> nums_buckets = {64};
  for (const auto& update_mode : update_modes) {
    for (const auto& offset_width : offset_widths) {
      for (const auto& align_pow : align_pows) {
        for (const auto& num_buckets : nums_buckets) {
          tkrzw::HashDBM::TuningParameters tuning_params;
          tuning_params.update_mode = update_mode;
          tuning_params.offset_width = offset_width;
          tuning_params.align_pow = align_pow;
          tuning_params.num_buckets = num_buckets;
          tuning_params.lock_mem_buckets = true;
          EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->OpenAdvanced(
              file_path, true, tkrzw::File::OPEN_TRUNCATE, tuning_params));
          LargeRecordTest(dbm);
          EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Close());
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
  const std::vector<int32_t> offset_widths = {3, 4, 5};
  const std::vector<int32_t> align_pows = {0, 1, 2, 3, 9, 10};
  const std::vector<int32_t> nums_buckets = {1, 8, 64, 512};
  for (const auto& update_mode : update_modes) {
    for (const auto& offset_width : offset_widths) {
      for (const auto& align_pow : align_pows) {
        for (const auto& num_buckets : nums_buckets) {
          tkrzw::HashDBM::TuningParameters tuning_params;
          tuning_params.update_mode = update_mode;
          tuning_params.offset_width = offset_width;
          tuning_params.align_pow = align_pow;
          tuning_params.num_buckets = num_buckets;
          tuning_params.lock_mem_buckets = true;
          EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->OpenAdvanced(
              file_path, true, tkrzw::File::OPEN_TRUNCATE, tuning_params));
          BasicTest(dbm);
          EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Close());
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
  const std::vector<int32_t> offset_widths = {4};
  const std::vector<int32_t> align_pows = {0, 1, 2, 10};
  const std::vector<int32_t> nums_buckets = {64, 512};
  for (const auto& update_mode : update_modes) {
    for (const auto& offset_width : offset_widths) {
      for (const auto& align_pow : align_pows) {
        for (const auto& num_buckets : nums_buckets) {
          tkrzw::HashDBM::TuningParameters tuning_params;
          tuning_params.update_mode = update_mode;
          tuning_params.offset_width = offset_width;
          tuning_params.align_pow = align_pow;
          tuning_params.num_buckets = num_buckets;
          tuning_params.lock_mem_buckets = true;
          EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->OpenAdvanced(
              file_path, true, tkrzw::File::OPEN_TRUNCATE, tuning_params));
          SequenceTest(dbm);
          EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Close());
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
  const std::vector<int32_t> offset_widths = {4};
  const std::vector<int32_t> align_pows = {0};
  const std::vector<int32_t> nums_buckets = {100};
  for (const auto& update_mode : update_modes) {
    for (const auto& offset_width : offset_widths) {
      for (const auto& align_pow : align_pows) {
        for (const auto& num_buckets : nums_buckets) {
          tkrzw::HashDBM::TuningParameters tuning_params;
          tuning_params.update_mode = update_mode;
          tuning_params.offset_width = offset_width;
          tuning_params.align_pow = align_pow;
          tuning_params.num_buckets = num_buckets;
          tuning_params.lock_mem_buckets = true;
          EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->OpenAdvanced(
              file_path, true, tkrzw::File::OPEN_TRUNCATE, tuning_params));
          AppendTest(dbm);
          EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Close());
        }
      }
    }
  }
}

void HashDBMTest::HashDBMProcessTest(tkrzw::HashDBM* dbm) {
  tkrzw::TemporaryDirectory tmp_dir(true, "tkrzw-");
  const std::vector<tkrzw::HashDBM::UpdateMode> update_modes =
      {tkrzw::HashDBM::UPDATE_IN_PLACE, tkrzw::HashDBM::UPDATE_APPENDING};
  const std::string file_path = tmp_dir.MakeUniquePath();
  const std::vector<int32_t> offset_widths = {3, 4, 5};
  const std::vector<int32_t> align_pows = {0, 1, 2, 3, 9, 10};
  const std::vector<int32_t> nums_buckets = {1, 8, 64, 512};
  for (const auto& update_mode : update_modes) {
    for (const auto& offset_width : offset_widths) {
      for (const auto& align_pow : align_pows) {
        for (const auto& num_buckets : nums_buckets) {
          tkrzw::HashDBM::TuningParameters tuning_params;
          tuning_params.update_mode = update_mode;
          tuning_params.offset_width = offset_width;
          tuning_params.align_pow = align_pow;
          tuning_params.num_buckets = num_buckets;
          EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->OpenAdvanced(
              file_path, true, tkrzw::File::OPEN_TRUNCATE, tuning_params));
          ProcessTest(dbm);
          EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Close());
        }
      }
    }
  }
}

void HashDBMTest::HashDBMProcessEachTest(tkrzw::HashDBM* dbm) {
  tkrzw::TemporaryDirectory tmp_dir(true, "tkrzw-");
  const std::vector<tkrzw::HashDBM::UpdateMode> update_modes =
      {tkrzw::HashDBM::UPDATE_IN_PLACE, tkrzw::HashDBM::UPDATE_APPENDING};
  const std::string file_path = tmp_dir.MakeUniquePath();
  const std::vector<int32_t> offset_widths = {4};
  const std::vector<int32_t> align_pows = {0, 3, 9};
  const std::vector<int32_t> nums_buckets = {1000};
  for (const auto& update_mode : update_modes) {
    for (const auto& offset_width : offset_widths) {
      for (const auto& align_pow : align_pows) {
        for (const auto& num_buckets : nums_buckets) {
          tkrzw::HashDBM::TuningParameters tuning_params;
          tuning_params.update_mode = update_mode;
          tuning_params.offset_width = offset_width;
          tuning_params.align_pow = align_pow;
          tuning_params.num_buckets = num_buckets;
          EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->OpenAdvanced(
              file_path, true, tkrzw::File::OPEN_TRUNCATE, tuning_params));
          ProcessEachTest(dbm);
          EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Close());
        }
      }
    }
  }
}

void HashDBMTest::HashDBMRandomTestOne(tkrzw::HashDBM* dbm) {
  tkrzw::TemporaryDirectory tmp_dir(true, "tkrzw-");
  const std::vector<tkrzw::HashDBM::UpdateMode> update_modes =
      {tkrzw::HashDBM::UPDATE_IN_PLACE, tkrzw::HashDBM::UPDATE_APPENDING};
  const std::string file_path = tmp_dir.MakeUniquePath();
  const std::vector<int32_t> offset_widths = {3, 4};
  const std::vector<int32_t> align_pows = {0, 2, 4, 8, 10};
  const std::vector<int32_t> nums_buckets = {10000};
  for (const auto& update_mode : update_modes) {
    for (const auto& offset_width : offset_widths) {
      for (const auto& align_pow : align_pows) {
        for (const auto& num_buckets : nums_buckets) {
          tkrzw::HashDBM::TuningParameters tuning_params;
          tuning_params.update_mode = update_mode;
          tuning_params.offset_width = offset_width;
          tuning_params.align_pow = align_pow;
          tuning_params.num_buckets = num_buckets;
          tuning_params.lock_mem_buckets = true;
          EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->OpenAdvanced(
              file_path, true, tkrzw::File::OPEN_TRUNCATE, tuning_params));
          RandomTest(dbm, 1);
          EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Close());
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
  const std::vector<int32_t> offset_widths = {4};
  const std::vector<int32_t> align_pows = {0, 2, 4, 10};
  const std::vector<int32_t> nums_buckets = {10000};
  for (const auto& update_mode : update_modes) {
    for (const auto& offset_width : offset_widths) {
      for (const auto& align_pow : align_pows) {
        for (const auto& num_buckets : nums_buckets) {
          tkrzw::HashDBM::TuningParameters tuning_params;
          tuning_params.update_mode = update_mode;
          tuning_params.offset_width = offset_width;
          tuning_params.align_pow = align_pow;
          tuning_params.num_buckets = num_buckets;
          tuning_params.lock_mem_buckets = true;
          EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->OpenAdvanced(
              file_path, true, tkrzw::File::OPEN_TRUNCATE, tuning_params));
          RandomTestThread(dbm);
          EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Close());
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
  for (const auto& update_mode : update_modes) {
    tkrzw::HashDBM::TuningParameters tuning_params;
    tuning_params.update_mode = update_mode;
    tuning_params.num_buckets = 1000;
    tuning_params.lock_mem_buckets = true;
    EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->OpenAdvanced(
        file_path, true, tkrzw::File::OPEN_TRUNCATE, tuning_params));
    EXPECT_EQ(tkrzw::Status::SUCCESS, file->Open(flat_file_path, true));
    RecordMigrationTest(dbm, file);
    EXPECT_EQ(tkrzw::Status::SUCCESS, file->Close());
    EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Close());
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
  tuning_params.offset_width = 4;
  tuning_params.align_pow = 0;
  tuning_params.num_buckets = num_keys;
  tuning_params.lock_mem_buckets = true;
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
  tuning_params.offset_width = 4;
  tuning_params.align_pow = 2;
  tuning_params.num_buckets = 10;
  tuning_params.fbp_capacity = tkrzw::INT32MAX;
  tuning_params.lock_mem_buckets = true;
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->OpenAdvanced(
      file_path, true, tkrzw::File::OPEN_DEFAULT, tuning_params));
  EXPECT_TRUE(dbm->IsHealthy());
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
  tuning_params.offset_width = 4;
  tuning_params.align_pow = 2;
  tuning_params.num_buckets = 10;
  tuning_params.lock_mem_buckets = true;
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->OpenAdvanced(
      file_path, true, tkrzw::File::OPEN_DEFAULT, tuning_params));
  EXPECT_TRUE(dbm->IsHealthy());
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
  int32_t in_offset_width = 0;
  int32_t in_align_pow = 0;
  int64_t last_sync_size = 0;
  EXPECT_EQ(tkrzw::Status::SUCCESS, tkrzw::HashDBM::FindRecordBase(
      &in_file, &in_record_base, &in_offset_width, &in_align_pow, &last_sync_size));
  EXPECT_GT(in_record_base, 0);
  EXPECT_EQ(tuning_params.offset_width, in_offset_width);
  EXPECT_EQ(tuning_params.align_pow, in_align_pow);
  EXPECT_EQ(in_file.GetSizeSimple(), last_sync_size);
  EXPECT_EQ(tkrzw::Status::SUCCESS, in_file.Close());
  EXPECT_EQ(tkrzw::Status::SUCCESS, in_file.Open(file_path, true));
  EXPECT_EQ(tkrzw::Status::SUCCESS, in_file.Write(0, "0123", 4));
  EXPECT_EQ(tkrzw::Status::SUCCESS, tkrzw::HashDBM::FindRecordBase(
      &in_file, &in_record_base, &in_offset_width, &in_align_pow, &last_sync_size));
  EXPECT_GT(in_record_base, 0);
  EXPECT_EQ(tuning_params.offset_width, in_offset_width);
  EXPECT_EQ(tuning_params.align_pow, in_align_pow);
  EXPECT_EQ(0, last_sync_size);
  EXPECT_EQ(tkrzw::Status::SUCCESS, tkrzw::HashRecord::ExtractOffsets(
      &in_file, &offset_file, in_record_base, in_offset_width, in_align_pow,
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
  const std::vector<int32_t> offset_widths = {3, 4};
  const std::vector<int32_t> align_pows = {0, 2, 4};
  const std::vector<int32_t> nums_buckets = {1000};
  for (const auto& update_mode : update_modes) {
    for (const auto& offset_width : offset_widths) {
      for (const auto& align_pow : align_pows) {
        for (const auto& num_buckets : nums_buckets) {
          tkrzw::HashDBM::TuningParameters tuning_params;
          tuning_params.update_mode = update_mode;
          tuning_params.offset_width = offset_width;
          tuning_params.align_pow = align_pow;
          tuning_params.num_buckets = num_buckets;
          tuning_params.lock_mem_buckets = true;
          HashDBMRebuildStaticTestOne(dbm, tuning_params);
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
  const std::vector<int32_t> offset_widths = {4};
  const std::vector<int32_t> align_pows = {0, 2};
  const std::vector<int32_t> nums_buckets = {1000};
  for (const auto& update_mode : update_modes) {
    for (const auto& offset_width : offset_widths) {
      for (const auto& align_pow : align_pows) {
        for (const auto& num_buckets : nums_buckets) {
          tkrzw::HashDBM::TuningParameters tuning_params;
          tuning_params.update_mode = update_mode;
          tuning_params.offset_width = offset_width;
          tuning_params.align_pow = align_pow;
          tuning_params.num_buckets = num_buckets;
          tuning_params.lock_mem_buckets = true;
          EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->OpenAdvanced(
              file_path, true, tkrzw::File::OPEN_TRUNCATE, tuning_params));
          RebuildRandomTest(dbm);
          EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Close());
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
  tuning_params.offset_width = 3;
  tuning_params.align_pow = 0;
  tuning_params.num_buckets = 10;
  tuning_params.lock_mem_buckets = true;
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->OpenAdvanced(
      first_file_path, true, tkrzw::File::OPEN_TRUNCATE, tuning_params));
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
  EXPECT_EQ(tkrzw::Status::SUCCESS, second_dbm.OpenAdvanced(
      second_file_path, true, tkrzw::File::OPEN_TRUNCATE, tuning_params));
  EXPECT_EQ(tkrzw::Status::SUCCESS, second_dbm.ImportFromFileForward(
      first_file_path, false, -1, 0));
  EXPECT_EQ(1, second_dbm.CountSimple());
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
  EXPECT_EQ(123, second_dbm.GetDatabaseType());
  EXPECT_EQ("0123456789", second_dbm.GetOpaqueMetadata().substr(0, 10));
  EXPECT_EQ(tkrzw::HashDBM::UPDATE_IN_PLACE, second_dbm.GetUpdateMode());
  EXPECT_EQ(3, second_dbm.CountSimple());
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
  EXPECT_EQ("xx", second_dbm.GetSimple("x"));
  EXPECT_EQ("yy", second_dbm.GetSimple("y"));
  EXPECT_EQ("zz", second_dbm.GetSimple("z"));
  EXPECT_EQ(tkrzw::Status::SUCCESS, second_dbm.Close());
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

// END OF FILE
