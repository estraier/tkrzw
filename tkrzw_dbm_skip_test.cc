/*************************************************************************************************
 * Tests for tkrzw_dbm_skip.h
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
#include "tkrzw_dbm_skip.h"
#include "tkrzw_dbm_skip_impl.h"
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

class SkipDBMTest : public CommonDBMTest {
 protected:
  void SkipDBMEmptyDatabaseTest(tkrzw::SkipDBM* dbm);
  void SkipDBMFileTest(tkrzw::SkipDBM* dbm);
  void SkipDBMLargeRecordTest(tkrzw::SkipDBM* dbm);
  void SkipDBMBackIteratorTest(tkrzw::SkipDBM* dbm);
  void SkipDBMIteratorBoundTest(tkrzw::SkipDBM* dbm);
  void SkipDBMBasicTestOne(tkrzw::SkipDBM* dbm, bool insert_in_order);
  void SkipDBMBasicTestAll(tkrzw::SkipDBM* dbm);
  void SkipDBMAdvancedTest(tkrzw::SkipDBM* dbm);
  void SkipDBMProcessTest(tkrzw::SkipDBM* dbm);
  void SkipDBMRestoreTest(tkrzw::SkipDBM* dbm);
  void SkipDBMMergeTest(tkrzw::SkipDBM* dbm);
};

void SkipDBMTest::SkipDBMEmptyDatabaseTest(tkrzw::SkipDBM* dbm) {
  tkrzw::TemporaryDirectory tmp_dir(true, "tkrzw-");
  const std::string file_path = tmp_dir.MakeUniquePath();
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Open(file_path, true));
  EXPECT_TRUE(dbm->IsOpen());
  EXPECT_TRUE(dbm->IsWritable());
  EXPECT_TRUE(dbm->IsHealthy());
  EXPECT_EQ(0, dbm->CountSimple());
  EXPECT_EQ(128, dbm->GetFileSizeSimple());
  EXPECT_GT(dbm->GetModificationTime(), 0);
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->SetDatabaseType(123));
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->SetOpaqueMetadata("0123456789"));
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Close());
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Open(file_path, false));
  EXPECT_TRUE(dbm->IsOpen());
  EXPECT_FALSE(dbm->IsWritable());
  EXPECT_EQ(123, dbm->GetDatabaseType());
  EXPECT_TRUE(dbm->IsHealthy());
  EXPECT_EQ(0, dbm->CountSimple());
  EXPECT_EQ(128, dbm->GetFileSizeSimple());
  EXPECT_GT(dbm->GetModificationTime(), 0);
  EXPECT_EQ(123, dbm->GetDatabaseType());
  EXPECT_EQ("0123456789", dbm->GetOpaqueMetadata().substr(0, 10));
  const auto& meta = dbm->Inspect();
  const std::map<std::string, std::string> meta_map(meta.begin(), meta.end());
  EXPECT_TRUE(tkrzw::CheckMap(meta_map, "pkg_major_version"));
  EXPECT_TRUE(tkrzw::CheckMap(meta_map, "offset_width"));
  EXPECT_TRUE(tkrzw::CheckMap(meta_map, "step_unit"));
  EXPECT_TRUE(tkrzw::CheckMap(meta_map, "max_level"));
  EXPECT_EQ("true", tkrzw::SearchMap(meta_map, "healthy", ""));
  EXPECT_TRUE(dbm->IsOrdered());
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Close());
}

void SkipDBMTest::SkipDBMFileTest(tkrzw::SkipDBM* dbm) {
  tkrzw::TemporaryDirectory tmp_dir(true, "tkrzw-");
  const std::string file_path = tmp_dir.MakeUniquePath();
  FileTest(dbm, file_path);
}

void SkipDBMTest::SkipDBMLargeRecordTest(tkrzw::SkipDBM* dbm) {
  tkrzw::TemporaryDirectory tmp_dir(true, "tkrzw-");
  const std::string file_path = tmp_dir.MakeUniquePath();
  const std::vector<int32_t> offset_widths = {4};
  const std::vector<int32_t> step_units = {2};
  const std::vector<int32_t> max_levels = {4};
  for (const auto& offset_width : offset_widths) {
    for (const auto& step_unit : step_units) {
      for (const auto& max_level : max_levels) {
        tkrzw::SkipDBM::TuningParameters tuning_params;
        tuning_params.offset_width = offset_width;
        tuning_params.step_unit = step_unit;
        tuning_params.max_level = max_level;
        tuning_params.sort_mem_size = 128 * 128 * 128 * 10;
        EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->OpenAdvanced(
            file_path, true, tkrzw::File::OPEN_TRUNCATE, tuning_params));
        LargeRecordTest(dbm);
        EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Close());
      }
    }
  }
}

void SkipDBMTest::SkipDBMBackIteratorTest(tkrzw::SkipDBM* dbm) {
  tkrzw::TemporaryDirectory tmp_dir(true, "tkrzw-");
  const std::string file_path = tmp_dir.MakeUniquePath();
  tkrzw::SkipDBM::TuningParameters tuning_params;
  tuning_params.step_unit = 3;
  tuning_params.max_level = 4;
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->OpenAdvanced(
      file_path, true, tkrzw::File::OPEN_TRUNCATE, tuning_params));
  BackIteratorTest(dbm);
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Close());
}

void SkipDBMTest::SkipDBMIteratorBoundTest(tkrzw::SkipDBM* dbm) {
  tkrzw::TemporaryDirectory tmp_dir(true, "tkrzw-");
  const std::string file_path = tmp_dir.MakeUniquePath();
  tkrzw::SkipDBM::TuningParameters tuning_params;
  tuning_params.step_unit = 2;
  tuning_params.max_level = 4;
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->OpenAdvanced(
      file_path, true, tkrzw::File::OPEN_TRUNCATE, tuning_params));
  IteratorBoundTest(dbm);
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Close());
}

void SkipDBMTest::SkipDBMBasicTestOne(tkrzw::SkipDBM* dbm, bool insert_in_order) {
  constexpr int32_t num_records = 200;
  int64_t eff_data_size = 0;
  std::map<std::string, std::string> map;
  for (int32_t i = 0; i < num_records; i++) {
    const std::string key = tkrzw::SPrintF("%08d", i);
    const std::string value = tkrzw::SPrintF("%d", i * i);
    EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Set(key, value, false));
    eff_data_size += key.size() + value.size();
    map.emplace(key, value);
  }
  if (insert_in_order) {
    EXPECT_EQ(num_records, dbm->CountSimple());
  } else {
    EXPECT_EQ(0, dbm->CountSimple());
  }
  EXPECT_EQ(tkrzw::Status::NOT_FOUND_ERROR, dbm->Get("00000000"));
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Synchronize(false));
  EXPECT_EQ(num_records, dbm->CountSimple());
  EXPECT_EQ(eff_data_size, dbm->GetEffectiveDataSize());
  for (int32_t i = 0; i < num_records; i++) {
    const std::string key = tkrzw::SPrintF("%08d", i);
    std::string value;
    EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Get(key, &value));
    EXPECT_EQ(tkrzw::SPrintF("%d", i * i), value);
  }
  EXPECT_EQ(tkrzw::Status::NOT_FOUND_ERROR, dbm->Get(tkrzw::SPrintF("%08d", num_records)));
  for (int32_t i = 0; i < num_records; i++) {
    std::string key, value;
    EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->GetByIndex(i, &key, &value));
    EXPECT_EQ(tkrzw::SPrintF("%08d", i), key);
    EXPECT_EQ(tkrzw::SPrintF("%d", i * i), value);
  }
  EXPECT_EQ(tkrzw::Status::NOT_FOUND_ERROR, dbm->GetByIndex(num_records));
  EXPECT_EQ(tkrzw::Status::NOT_FOUND_ERROR, dbm->Get(""));
  EXPECT_EQ(tkrzw::Status::NOT_FOUND_ERROR, dbm->Get("x"));
  auto iter = dbm->MakeIterator();
  EXPECT_EQ(tkrzw::Status::NOT_FOUND_ERROR, iter->Get());
  EXPECT_EQ(tkrzw::Status::NOT_FOUND_ERROR, iter->Next());
  EXPECT_EQ(tkrzw::Status::SUCCESS, iter->First());
  while (true) {
    std::string key, value;
    tkrzw::Status status = iter->Get(&key, &value);
    if (status != tkrzw::Status::SUCCESS) {
      EXPECT_EQ(tkrzw::Status::NOT_FOUND_ERROR, status);
      break;
    }
    EXPECT_EQ(map[key], value);
    EXPECT_EQ(tkrzw::Status::SUCCESS, iter->Next());
    map.erase(key);
  }
  EXPECT_EQ(tkrzw::Status::NOT_FOUND_ERROR, iter->Get());
  EXPECT_EQ(tkrzw::Status::NOT_FOUND_ERROR, iter->Next());
  EXPECT_TRUE(map.empty());
  for (int32_t i = 0; i < num_records; i++) {
    const std::string key = tkrzw::SPrintF("%08d", i);
    const std::string value = tkrzw::SPrintF("[%d]", i);
    EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Set(key, value));
    eff_data_size += key.size() + value.size();
  }
  if (insert_in_order) {
    EXPECT_EQ(num_records * 2, dbm->CountSimple());
  } else {
    EXPECT_EQ(num_records, dbm->CountSimple());
  }
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Synchronize(false));
  EXPECT_EQ(num_records * 2, dbm->CountSimple());
  eff_data_size = 0;
  for (int32_t i = 0; i < num_records / 10; i++) {
    const std::string prefix = tkrzw::SPrintF("%07d", i);
    EXPECT_EQ(tkrzw::Status::SUCCESS, iter->Jump(prefix));
    int32_t hit_count = 0;
    while (true) {
      std::string key, value;
      tkrzw::Status status = iter->Get(&key, &value);
      if (status != tkrzw::Status::SUCCESS) {
        EXPECT_EQ(tkrzw::Status::NOT_FOUND_ERROR, status);
        break;
      }
      if (!tkrzw::StrBeginsWith(key, prefix)) {
        break;
      }
      const int32_t key_num = i * 10 + hit_count / 2;
      EXPECT_EQ(tkrzw::SPrintF("%08d", key_num), key);
      if (hit_count % 2 == 0) {
        EXPECT_EQ(tkrzw::SPrintF("%d", key_num * key_num), value);
      } else {
        EXPECT_EQ(tkrzw::SPrintF("[%d]", key_num), value);
      }
      hit_count++;
      eff_data_size += key.size() + value.size();
      EXPECT_EQ(tkrzw::Status::SUCCESS, iter->Next());
    }
    EXPECT_EQ(20, hit_count);
  }
  EXPECT_EQ(eff_data_size, dbm->GetEffectiveDataSize());
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->SynchronizeAdvanced(
      false, nullptr, tkrzw::SkipDBM::ReduceToFirst));
  EXPECT_EQ(num_records, dbm->CountSimple());
  for (int32_t i = 0; i < num_records; i++) {
    const std::string key = tkrzw::SPrintF("%08d", i);
    std::string value;
    EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Get(key, &value));
    EXPECT_EQ(tkrzw::SPrintF("%d", i * i), value);
  }
  for (int32_t i = 0; i < num_records; i++) {
    const std::string key = tkrzw::SPrintF("%08d", i);
    const std::string value(i % 150, 'v');
    EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Set(key, value));
    eff_data_size += key.size() + value.size();
  }
  if (insert_in_order) {
    EXPECT_EQ(num_records * 2, dbm->CountSimple());
  } else {
    EXPECT_EQ(num_records, dbm->CountSimple());
  }
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->SynchronizeAdvanced(
      false, nullptr, tkrzw::SkipDBM::ReduceToLast));
  EXPECT_EQ(num_records, dbm->CountSimple());
  eff_data_size = 0;
  for (int32_t i = 0; i < num_records; i++) {
    const std::string key = tkrzw::SPrintF("%08d", i);
    std::string value;
    EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Get(key, &value));
    EXPECT_EQ(std::string(i % 150, 'v'), value);
    eff_data_size += key.size() + value.size();
  }
  EXPECT_EQ(eff_data_size, dbm->GetEffectiveDataSize());
  for (int32_t i = 0; i < num_records; i++) {
    const std::string key = tkrzw::SPrintF("%08d", i);
    EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Set(key, "x"));
    EXPECT_EQ(tkrzw::Status::DUPLICATION_ERROR, dbm->Set(key, "y", false));
  }
  if (insert_in_order) {
    EXPECT_EQ(num_records * 2, dbm->CountSimple());
    EXPECT_GT(dbm->GetEffectiveDataSize(), eff_data_size);
  } else {
    EXPECT_EQ(num_records, dbm->CountSimple());
    EXPECT_EQ(eff_data_size, dbm->GetEffectiveDataSize());
  }
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Revert());
  EXPECT_EQ(num_records, dbm->CountSimple());
  EXPECT_EQ(eff_data_size, dbm->GetEffectiveDataSize());
  for (int32_t i = 0; i < num_records; i++) {
    const std::string key = tkrzw::SPrintF("%08d", i);
    std::string value;
    EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Get(key, &value));
    EXPECT_EQ(std::string(i % 150, 'v'), value);
    EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Set(key, "second"));
    if (i % 2 == 0) {
      EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Remove(key));
    }
    if (i % 4 == 0) {
      EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Set(key, "third"));
    }
  }
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->SynchronizeAdvanced(
      false, nullptr, tkrzw::SkipDBM::ReduceToSecond));
  EXPECT_EQ(num_records / 4 * 3, dbm->CountSimple());
  for (int32_t i = 0; i < num_records; i++) {
    const std::string key = tkrzw::SPrintF("%08d", i);
    if (i % 4 == 0) {
      EXPECT_EQ("third", dbm->GetSimple(key));
    } else if (i % 2 == 0) {
      EXPECT_EQ("", dbm->GetSimple(key));
    } else {
      EXPECT_EQ("second", dbm->GetSimple(key));
    }
  }
}

void SkipDBMTest::SkipDBMBasicTestAll(tkrzw::SkipDBM* dbm) {
  tkrzw::TemporaryDirectory tmp_dir(true, "tkrzw-");
  const std::string file_path = tmp_dir.MakeUniquePath();
  const std::vector<int32_t> offset_widths = {3, 4};
  const std::vector<int32_t> step_units = {2, 4, 8};
  const std::vector<int32_t> max_levels = {4, 8, 16};
  const std::vector<bool> insert_in_orders = {true, false};
  for (const auto& offset_width : offset_widths) {
    for (const auto& step_unit : step_units) {
      for (const auto& max_level : max_levels) {
        for (const auto& insert_in_order : insert_in_orders) {
          tkrzw::SkipDBM::TuningParameters tuning_params;
          tuning_params.offset_width = offset_width;
          tuning_params.step_unit = step_unit;
          tuning_params.max_level = max_level;
          tuning_params.sort_mem_size = 3000;
          tuning_params.insert_in_order = insert_in_order;
          EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->OpenAdvanced(
              file_path, true, tkrzw::File::OPEN_TRUNCATE, tuning_params));
          SkipDBMBasicTestOne(dbm, insert_in_order);
          EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Close());
        }
      }
    }
  }
}

void SkipDBMTest::SkipDBMAdvancedTest(tkrzw::SkipDBM* dbm) {
  tkrzw::TemporaryDirectory tmp_dir(true, "tkrzw-");
  const std::string file_path = tmp_dir.MakeUniquePath();
  constexpr int32_t num_records = 10;
  tkrzw::SkipDBM::TuningParameters tuning_params;
  tuning_params.offset_width = 5;
  tuning_params.step_unit = 5;
  tuning_params.max_level = 15;
  tuning_params.sort_mem_size = 3000;
  tuning_params.insert_in_order = false;
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->OpenAdvanced(
      file_path, true, tkrzw::File::OPEN_TRUNCATE, tuning_params));
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->SetDatabaseType(123));
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->SetOpaqueMetadata("0123456789"));
  EXPECT_FALSE(dbm->IsUpdated());
  int64_t eff_data_size = 0;
  for (int i = 0; i < num_records; i++) {
    const std::string key = tkrzw::ToString(i * i);
    const std::string value = tkrzw::ToString(i);
    EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Set(key, value));
    eff_data_size += key.size() + value.size();
  }
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Close());
  tuning_params.insert_in_order = true;
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->OpenAdvanced(
      file_path, true, tkrzw::File::OPEN_DEFAULT, tuning_params));
  EXPECT_TRUE(dbm->IsHealthy());
  EXPECT_FALSE(dbm->IsUpdated());
  EXPECT_EQ(num_records, dbm->CountSimple());
  for (int i = 0; i < num_records; i++) {
    const std::string key = tkrzw::SPrintF("%08d", i);
    const std::string value = tkrzw::ToString(i) + ":first";
    EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Set(key, value));
    eff_data_size += key.size() + value.size();
  }
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Close());
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Open(file_path, false));
  EXPECT_TRUE(dbm->IsHealthy());
  EXPECT_EQ(num_records * 2, dbm->CountSimple());
  EXPECT_EQ(eff_data_size, dbm->GetEffectiveDataSize());
  EXPECT_FALSE(dbm->IsUpdated());
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Close());
  tuning_params.insert_in_order = false;
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->OpenAdvanced(
      file_path, true, tkrzw::File::OPEN_DEFAULT, tuning_params));
  for (int i = 0; i < num_records; i++) {
    const std::string key = tkrzw::SPrintF("%08d", i);
    const std::string value = tkrzw::ToString(i) + ":second";
    EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Set(key, value));
    eff_data_size += key.size() + value.size();
  }
  bool tobe = false;
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->ShouldBeRebuilt(&tobe));
  EXPECT_TRUE(tobe);
  EXPECT_TRUE(dbm->IsUpdated());
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Rebuild());
  EXPECT_FALSE(dbm->IsUpdated());
  EXPECT_EQ(num_records * 3, dbm->CountSimple());
  EXPECT_EQ(eff_data_size, dbm->GetEffectiveDataSize());
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->ShouldBeRebuilt(&tobe));
  EXPECT_FALSE(tobe);
  for (int i = 0; i < num_records; i++) {
    const std::string key = tkrzw::SPrintF("%d", i * i);
    const std::string value = tkrzw::ToString(i);
    EXPECT_EQ(value, dbm->GetSimple(key));
  }
  for (int i = 0; i < num_records; i++) {
    const std::string key = tkrzw::SPrintF("%08d", i);
    const std::string value = tkrzw::ToString(i) + ":third";
    EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Set(key, value));
    eff_data_size += key.size() + value.size();
  }
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Synchronize(false));
  for (int i = 0; i < num_records; i++) {
    const std::string key = tkrzw::SPrintF("%08d", i);
    const std::string value = tkrzw::ToString(i) + ":first";
    EXPECT_EQ(value, dbm->GetSimple(key));
  }
  auto iter = dbm->MakeIterator();
  for (int i = 0; i < num_records; i++) {
    const std::string key = tkrzw::SPrintF("%08d", i);
    EXPECT_EQ(tkrzw::Status::SUCCESS, iter->Jump(key));
    int32_t iter_count = 0;
    while (true) {
      std::string iter_key, iter_value;
      const tkrzw::Status status = iter->Get(&iter_key, &iter_value);
      if (status != tkrzw::Status::SUCCESS) {
        EXPECT_EQ(tkrzw::Status::NOT_FOUND_ERROR, status);
        break;
      }
      if (iter_key != key) {
        break;
      }
      if (iter_count == 0) {
        EXPECT_EQ(tkrzw::ToString(i) + ":first", iter_value);
      } else if (iter_count == 1) {
        EXPECT_EQ(tkrzw::ToString(i) + ":second", iter_value);
      } else {
        EXPECT_EQ(tkrzw::ToString(i) + ":third", iter_value);
      }
      iter_count++;
      EXPECT_EQ(tkrzw::Status::SUCCESS, iter->Next());
    }
    EXPECT_EQ(3, iter_count);
  }
  tuning_params.offset_width = 6;
  tuning_params.step_unit = 6;
  tuning_params.max_level = 6;
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->RebuildAdvanced(tuning_params));
  EXPECT_EQ(num_records * 4, dbm->CountSimple());
  EXPECT_EQ(eff_data_size, dbm->GetEffectiveDataSize());
  EXPECT_EQ(true, dbm->ShouldBeRebuiltSimple());
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Set("x", "y"));
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Clear());
  EXPECT_TRUE(dbm->IsHealthy());
  EXPECT_EQ(0, dbm->CountSimple());
  EXPECT_EQ(0, dbm->GetEffectiveDataSize());
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Set("x", "z"));
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Synchronize(false));
  EXPECT_EQ("z", dbm->GetSimple("x"));
  EXPECT_EQ(1, dbm->CountSimple());
  EXPECT_EQ(2, dbm->GetEffectiveDataSize());
  EXPECT_EQ(123, dbm->GetDatabaseType());
  EXPECT_EQ("0123456789", dbm->GetOpaqueMetadata().substr(0, 10));
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Close());
}

TEST_F(SkipDBMTest, Reducer) {
  EXPECT_THAT(tkrzw::SkipDBM::ReduceRemove("", {"a", "b", "c"}), ElementsAre("a", "b", "c"));
  EXPECT_THAT(tkrzw::SkipDBM::ReduceRemove("", {
        tkrzw::SkipDBM::REMOVING_VALUE}), ElementsAre());
  EXPECT_THAT(tkrzw::SkipDBM::ReduceRemove("", {
        tkrzw::SkipDBM::REMOVING_VALUE, "a"}), ElementsAre("a"));
  EXPECT_THAT(tkrzw::SkipDBM::ReduceRemove("", {
        "a", tkrzw::SkipDBM::REMOVING_VALUE, "b"}), ElementsAre("b"));
  EXPECT_THAT(tkrzw::SkipDBM::ReduceRemove("", {
        "a", "b", tkrzw::SkipDBM::REMOVING_VALUE}), ElementsAre());
  EXPECT_THAT(tkrzw::SkipDBM::ReduceRemove("", {
        "a", "b", tkrzw::SkipDBM::REMOVING_VALUE, "c", "d"}), ElementsAre("c", "d"));
  EXPECT_THAT(tkrzw::SkipDBM::ReduceToFirst("", {"a", "b", "c"}), ElementsAre("a"));
  EXPECT_THAT(tkrzw::SkipDBM::ReduceToSecond("", {"a"}), ElementsAre("a"));
  EXPECT_THAT(tkrzw::SkipDBM::ReduceToSecond("", {"a", "b", "c"}), ElementsAre("b"));
  EXPECT_THAT(tkrzw::SkipDBM::ReduceToLast("", {"a", "b", "c"}), ElementsAre("c"));
  EXPECT_THAT(tkrzw::SkipDBM::ReduceConcat("", {"a", "b", "c"}), ElementsAre("abc"));
  EXPECT_THAT(tkrzw::SkipDBM::ReduceConcatWithNull("", {"a", "b", "c"}),
              ElementsAre(std::string("a\0b\0c", 5)));
  EXPECT_THAT(tkrzw::SkipDBM::ReduceConcatWithTab("", {"a", "b", "c"}), ElementsAre("a\tb\tc"));
  EXPECT_THAT(tkrzw::SkipDBM::ReduceConcatWithLine("", {"a", "b", "c"}), ElementsAre("a\nb\nc"));
  EXPECT_THAT(tkrzw::SkipDBM::ReduceToTotal("", {"11", "22", "33"}), ElementsAre("66"));
}

void SkipDBMTest::SkipDBMProcessTest(tkrzw::SkipDBM* dbm) {
  tkrzw::TemporaryDirectory tmp_dir(true, "tkrzw-");
  const std::string file_path = tmp_dir.MakeUniquePath();
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Open(file_path, true));
  for (char c = 'a'; c <= 'z'; c++) {
    const std::string key(3, c);
    const std::string value(5, c);
    EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Set(key, value));
  }
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Close());
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Open(file_path, true));
  EXPECT_EQ(26, dbm->CountSimple());
  class CaseChanger final : public tkrzw::DBM::RecordProcessor {
   public:
    explicit CaseChanger(bool upper) : upper_(upper) {}
    std::string_view ProcessFull(std::string_view key, std::string_view value) override {
      value_ = upper_ ? tkrzw::StrUpperCase(value) : tkrzw::StrLowerCase(value);
      return value_;
    }
   private:
    bool upper_;
    std::string value_;
  };
  for (char c = 'a'; c <= 'z'; c++) {
    const std::string key(3, c);
    CaseChanger proc(true);
    EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Process(key, &proc, true));
  }
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->SynchronizeAdvanced(
      false, nullptr, tkrzw::SkipDBM::ReduceToLast));
  for (char c = 'a'; c <= 'z'; c++) {
    const std::string key(3, c);
    const std::string value = tkrzw::StrUpperCase(std::string(5, c));
    EXPECT_EQ(value, dbm->GetSimple(key));
  }
  auto iter = dbm->MakeIterator();
  EXPECT_EQ(tkrzw::Status::SUCCESS, iter->First());
  int32_t iter_count = 0;
  while (true) {
    CaseChanger proc(false);
    const tkrzw::Status status = iter->Process(&proc, true);
    if (status != tkrzw::Status::SUCCESS) {
      EXPECT_EQ(tkrzw::Status::NOT_FOUND_ERROR, status);
      break;
    }
    iter_count++;
    EXPECT_EQ(tkrzw::Status::SUCCESS, iter->Next());
  }
  EXPECT_EQ(26, iter_count);
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->SynchronizeAdvanced(
      false, nullptr, tkrzw::SkipDBM::ReduceToLast));
  for (char c = 'a'; c <= 'z'; c++) {
    const std::string key(3, c);
    const std::string value(5, c);
    EXPECT_EQ(value, dbm->GetSimple(key));
  }
  class Counter final : public tkrzw::DBM::RecordProcessor {
   public:
    std::string_view ProcessFull(std::string_view key, std::string_view value) override {
      count_full_++;
      return NOOP;
    }
    std::string_view ProcessEmpty(std::string_view key) override {
      count_empty_++;
      return NOOP;
    }
    int64_t CountFull() {
      return count_full_;
    }
    int64_t CountEmpty() {
      return count_empty_;
    }
   private:
    int32_t count_full_ = 0;
    int32_t count_empty_ = 0;
  };
  Counter counter;
  EXPECT_EQ(tkrzw::Status::SUCCESS, iter->First());
  while (true) {
    CaseChanger proc(false);
    const tkrzw::Status status = iter->Process(&counter, false);
    if (status != tkrzw::Status::SUCCESS) {
      EXPECT_EQ(tkrzw::Status::NOT_FOUND_ERROR, status);
      break;
    }
    EXPECT_EQ(tkrzw::Status::SUCCESS, iter->Next());
  }
  EXPECT_EQ(26, counter.CountFull());
  EXPECT_EQ(0, counter.CountEmpty());
  Counter counter_each_reader;
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->ProcessEach(&counter_each_reader, false));
  EXPECT_EQ(26, counter_each_reader.CountFull());
  EXPECT_EQ(2, counter_each_reader.CountEmpty());
  CaseChanger upper(true);
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->ProcessEach(&upper, true));
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->SynchronizeAdvanced(
      false, nullptr, tkrzw::SkipDBM::ReduceToLast));
  for (char c = 'a'; c <= 'z'; c++) {
    const std::string key(3, c);
    const std::string value = tkrzw::StrUpperCase(std::string(5, c));
    EXPECT_EQ(value, dbm->GetSimple(key));
  }
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Close());
}

void SkipDBMTest::SkipDBMRestoreTest(tkrzw::SkipDBM* dbm) {
  tkrzw::TemporaryDirectory tmp_dir(true, "tkrzw-");
  const std::string old_file_path = tmp_dir.MakeUniquePath();
  const std::string new_file_path = tmp_dir.MakeUniquePath();
  constexpr int32_t num_records = 100;
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Open(old_file_path, true));
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->SetDatabaseType(123));
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->SetOpaqueMetadata("0123456789"));
  for (int32_t i = 0; i < num_records; i++) {
    const std::string key = tkrzw::ToString(i * i);
    const std::string value = tkrzw::ToString(i);
    EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Set(key, value));
  }
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Synchronize(false));
  EXPECT_EQ(tkrzw::Status::SUCCESS, tkrzw::SkipDBM::RestoreDatabase(
      old_file_path, new_file_path));
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Close());
  tkrzw::SkipDBM new_dbm;
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

void SkipDBMTest::SkipDBMMergeTest(tkrzw::SkipDBM* dbm) {
  constexpr int32_t num_files = 3;
  constexpr int32_t num_records = 100;
  tkrzw::TemporaryDirectory tmp_dir(true, "tkrzw-");
  std::vector<std::string> src_paths;
  int32_t count = 0;
  for (int32_t i = 0; i < num_files; i++) {
    const std::string src_path = tmp_dir.MakeUniquePath();
    auto src_dbm = dbm->MakeDBM();
    EXPECT_EQ(tkrzw::Status::SUCCESS, src_dbm->Open(src_path, true));
    for (int32_t j = 0; j < num_records; j++) {
      count++;
      const std::string key = tkrzw::ToString(count * count);
      const std::string value = tkrzw::ToString(count);
      EXPECT_EQ(tkrzw::Status::SUCCESS, src_dbm->Set(key, value));
    }
    EXPECT_EQ(tkrzw::Status::SUCCESS, src_dbm->Close());
    src_paths.emplace_back(src_path);
  }
  const std::string file_path = tmp_dir.MakeUniquePath();
  tkrzw::SkipDBM::TuningParameters tuning_params;
  tuning_params.sort_mem_size = 1000;
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->OpenAdvanced(
      file_path, true, tkrzw::File::OPEN_TRUNCATE, tuning_params));
  EXPECT_FALSE(dbm->IsUpdated());
  for (const auto& src_path : src_paths) {
    EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->MergeSkipDatabase(src_path));
  }
  EXPECT_TRUE(dbm->IsUpdated());
  for (int32_t i = 1; i <= num_records; i++) {
    const std::string key = tkrzw::ToString(i * i);
    EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Set(key, "LAST"));
  }
  EXPECT_EQ(0, dbm->CountSimple());
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Synchronize(false));
  EXPECT_EQ((num_files + 1) * num_records, dbm->CountSimple());
  const int32_t max_count = num_files * num_records;
  for (int32_t i = 1; i <= max_count; i++) {
    const std::string key = tkrzw::ToString(i * i);
    EXPECT_EQ(tkrzw::ToString(i), dbm->GetSimple(key));
  }
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->SynchronizeAdvanced(
      false, nullptr, tkrzw::SkipDBM::ReduceToLast));
  EXPECT_EQ(num_files * num_records, dbm->CountSimple());
  for (int32_t i = 1; i <= max_count; i++) {
    const std::string key = tkrzw::ToString(i * i);
    if (i <= num_records) {
      EXPECT_EQ("LAST", dbm->GetSimple(key));
    } else {
      EXPECT_EQ(tkrzw::ToString(i), dbm->GetSimple(key));
    }
  }
  EXPECT_EQ(tkrzw::Status::SUCCESS, dbm->Close());
}

TEST_F(SkipDBMTest, EmptyDatabase) {
  tkrzw::SkipDBM dbm(std::make_unique<tkrzw::MemoryMapParallelFile>());
  SkipDBMEmptyDatabaseTest(&dbm);
}

TEST_F(SkipDBMTest, File) {
  tkrzw::SkipDBM dbm(std::make_unique<tkrzw::MemoryMapParallelFile>());
  SkipDBMFileTest(&dbm);
}

TEST_F(SkipDBMTest, LargeRecord) {
  tkrzw::SkipDBM dbm(std::make_unique<tkrzw::MemoryMapParallelFile>());
  SkipDBMLargeRecordTest(&dbm);
}

TEST_F(SkipDBMTest, BackIterator) {
  tkrzw::SkipDBM dbm(std::make_unique<tkrzw::MemoryMapParallelFile>());
  SkipDBMBackIteratorTest(&dbm);
}

TEST_F(SkipDBMTest, IteratorBound) {
  tkrzw::SkipDBM dbm(std::make_unique<tkrzw::MemoryMapParallelFile>());
  SkipDBMIteratorBoundTest(&dbm);
}

TEST_F(SkipDBMTest, Basic) {
  tkrzw::SkipDBM dbm(std::make_unique<tkrzw::MemoryMapParallelFile>());
  SkipDBMBasicTestAll(&dbm);
}

TEST_F(SkipDBMTest, Advanced) {
  tkrzw::SkipDBM dbm(std::make_unique<tkrzw::MemoryMapParallelFile>());
  SkipDBMAdvancedTest(&dbm);
}

TEST_F(SkipDBMTest, Process) {
  tkrzw::SkipDBM dbm(std::make_unique<tkrzw::MemoryMapParallelFile>());
  SkipDBMProcessTest(&dbm);
}

TEST_F(SkipDBMTest, Restore) {
  tkrzw::SkipDBM dbm(std::make_unique<tkrzw::MemoryMapParallelFile>());
  SkipDBMRestoreTest(&dbm);
}

TEST_F(SkipDBMTest, Merge) {
  tkrzw::SkipDBM dbm(std::make_unique<tkrzw::MemoryMapParallelFile>());
  SkipDBMMergeTest(&dbm);
}

// END OF FILE
