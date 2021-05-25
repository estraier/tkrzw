/*************************************************************************************************
 * Tests for tkrzw_dbm_skip_impl.h
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
#include "tkrzw_dbm_skip_impl.h"
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

TEST(DBMSkipImplTest, SkipRecord) {
  tkrzw::TemporaryDirectory tmp_dir(true, "tkrzw-");
  const std::string file_path = tmp_dir.MakeUniquePath();
  tkrzw::MemoryMapParallelFile file;
  EXPECT_EQ(tkrzw::Status::SUCCESS, file.Open(file_path, true));
  const std::vector<int32_t> offset_widths = {3, 4, 5};
  const std::vector<int32_t> step_units = {2, 3, 4, 5};
  const std::vector<int32_t> max_levels = {2, 4, 8, 10};
  const std::vector<int32_t> value_sizes = {
    0, 1, 2, 4, 8, 15, 16, 31, 32, 63, 64, 127, 128, 230, 16385, 16386};
  constexpr int32_t cache_capacity = 16;
  const int32_t num_iters = 10;
  constexpr int64_t record_base = 16;
  for (const auto& offset_width : offset_widths) {
    for (const auto& step_unit : step_units) {
      for (const auto& max_level : max_levels) {
        EXPECT_EQ(tkrzw::Status::SUCCESS, file.Truncate(record_base));
        std::vector<int64_t> past_offsets(max_level);
        tkrzw::SkipRecord rec(&file, offset_width, step_unit, max_level);
        int64_t index = 0;
        for (const auto& value_size : value_sizes) {
          for (int32_t i = 0; i < num_iters; i++) {
            const std::string& key = tkrzw::SPrintF("%08d", index * 2);
            std::string value(value_size, 'v');
            rec.SetData(index, key.data(), key.size(), value.data(), value.size());
            EXPECT_EQ(index, rec.GetIndex());
            EXPECT_EQ(tkrzw::Status::SUCCESS, rec.Write());
            const int64_t offset = rec.GetOffset();
            EXPECT_EQ(tkrzw::Status::SUCCESS,
                      rec.UpdatePastRecords(index, offset, &past_offsets));
            EXPECT_EQ(tkrzw::Status::SUCCESS, rec.ReadMetadataKey(offset, index));
            index++;
          }
        }
        const int32_t max_index = index;
        const int64_t end_offset = file.GetSizeSimple();
        int64_t offset = record_base;
        index = 0;
        while (offset < end_offset) {
          EXPECT_EQ(tkrzw::Status::SUCCESS, rec.ReadMetadataKey(offset, index));
          EXPECT_EQ(tkrzw::SPrintF("%08d", index * 2), rec.GetKey());
          std::string_view value = rec.GetValue();
          if (value.data() == nullptr) {
            EXPECT_EQ(tkrzw::Status::SUCCESS, rec.ReadBody());
            value = rec.GetValue();
          }
          offset += rec.GetWholeSize();
          index++;
        }
        EXPECT_EQ(max_index, index);
        tkrzw::SkipRecordCache cache(step_unit, cache_capacity, index);
        EXPECT_EQ(tkrzw::Status::NOT_FOUND_ERROR, rec.Search(record_base, &cache, "", false));
        EXPECT_EQ(tkrzw::Status::NOT_FOUND_ERROR, rec.Search(record_base, &cache, "0000", false));
        for (int32_t index = 0; index < max_index; ++index) {
          const std::string& key = tkrzw::SPrintF("%08d", index);
          if (index % 2 == 0) {
            EXPECT_EQ(tkrzw::Status::SUCCESS, rec.Search(record_base, &cache, key, false));
            const std::string_view rec_key = rec.GetKey();
            EXPECT_EQ(key, rec_key);
          } else {
            EXPECT_EQ(tkrzw::Status::NOT_FOUND_ERROR, rec.Search(record_base, &cache, key, false));
            if (index < max_index - 1) {
              EXPECT_EQ(tkrzw::Status::SUCCESS, rec.Search(record_base, &cache, key, true));
              const std::string& next_key = tkrzw::SPrintF("%08d", index + 1);
              const std::string_view rec_key = rec.GetKey();
              EXPECT_EQ(next_key, rec_key);
            }
          }
          EXPECT_EQ(tkrzw::Status::SUCCESS, rec.SearchByIndex(record_base, &cache, index));
          EXPECT_EQ(tkrzw::SPrintF("%08d", index * 2), rec.GetKey());
        }
        for (int32_t i = 0; i < 10; i++) {
          const std::string& key = tkrzw::SPrintF("%07d", i);
          EXPECT_EQ(tkrzw::Status::SUCCESS, rec.Search(record_base, &cache, key, true));
          std::string_view rec_key = rec.GetKey();
          EXPECT_EQ(key + "0", rec_key);
          int64_t offset = rec.GetOffset() + rec.GetWholeSize();
          int64_t index = rec.GetIndex() + 1;
          int32_t count = 0;
          while (offset < end_offset) {
            EXPECT_EQ(tkrzw::Status::SUCCESS, rec.ReadMetadataKey(offset, index));
            std::string_view rec_key = rec.GetKey();
            if (!tkrzw::StrBeginsWith(rec_key, key)) {
              break;
            }
            offset += rec.GetWholeSize();
            index++;
            count++;
          }
          EXPECT_EQ(4, count);
        }
      }
    }
  }
  EXPECT_EQ(tkrzw::Status::SUCCESS, file.Close());
}

TEST(DBMSkipImplTest, SkipRecordMulti) {
  tkrzw::TemporaryDirectory tmp_dir(true, "tkrzw-");
  const std::string file_path = tmp_dir.MakeUniquePath();
  tkrzw::MemoryMapParallelFile file;
  EXPECT_EQ(tkrzw::Status::SUCCESS, file.Open(file_path, true));
  const std::vector<int32_t> offset_widths = {3, 4};
  const std::vector<int32_t> step_units = {2, 3, 4, 5};
  const std::vector<int32_t> max_levels = {2, 4, 8, 10};
  constexpr int32_t num_keys = 100;
  constexpr int32_t num_values = 10;
  constexpr int64_t record_base = 16;
  constexpr int32_t cache_capacity = 16;
  for (const auto& offset_width : offset_widths) {
    for (const auto& step_unit : step_units) {
      for (const auto& max_level : max_levels) {
        EXPECT_EQ(tkrzw::Status::SUCCESS, file.Truncate(record_base));
        std::vector<int64_t> past_offsets(max_level);
        tkrzw::SkipRecord rec(&file, offset_width, step_unit, max_level);
        int32_t index = 0;
        for (int32_t ki = 0; ki < num_keys; ki++) {
          const std::string& key = tkrzw::SPrintF("%08d", ki);
          for (int32_t vi = 0; vi < num_values; vi++) {
            const std::string& value = tkrzw::SPrintF("%08d", vi);
            rec.SetData(index, key.data(), key.size(), value.data(), value.size());
            EXPECT_EQ(tkrzw::Status::SUCCESS, rec.Write());
            const int64_t offset = rec.GetOffset();
            EXPECT_EQ(tkrzw::Status::SUCCESS,
                      rec.UpdatePastRecords(index, offset, &past_offsets));
            index++;
          }
        }
        tkrzw::SkipRecordCache cache(step_unit, cache_capacity, index);
        for (int32_t ki = 0; ki < num_keys; ki++) {
          const std::string& key = tkrzw::SPrintF("%08d", ki);
          EXPECT_EQ(tkrzw::Status::SUCCESS, rec.Search(record_base, &cache, key, false));
          int64_t offset = rec.GetOffset();
          int64_t index = rec.GetIndex();
          for (int32_t vi = 0; vi < num_values; vi++) {
            const std::string& value = tkrzw::SPrintF("%08d", vi);
            EXPECT_EQ(tkrzw::Status::SUCCESS, rec.ReadMetadataKey(offset, index));
            EXPECT_EQ(key, rec.GetKey());
            std::string_view rec_value = rec.GetValue();
            if (rec_value.data() == nullptr) {
              EXPECT_EQ(tkrzw::Status::SUCCESS, rec.ReadBody());
              rec_value = rec.GetValue();
            }
            EXPECT_EQ(value, rec_value);
            offset += rec.GetWholeSize();
            EXPECT_EQ(tkrzw::Status::SUCCESS, rec.SearchByIndex(record_base, &cache, index));
            EXPECT_EQ(key, rec.GetKey());
            rec_value = rec.GetValue();
            if (rec_value.data() == nullptr) {
              EXPECT_EQ(tkrzw::Status::SUCCESS, rec.ReadBody());
              rec_value = rec.GetValue();
            }
            EXPECT_EQ(value, rec_value);
            index++;
          }
        }
      }
    }
  }
  EXPECT_EQ(tkrzw::Status::SUCCESS, file.Close());
}

TEST(DBMSkipImplTest, RecordSorter) {
  tkrzw::TemporaryDirectory tmp_dir(true, "tkrzw-");
  const std::string base_path = tmp_dir.MakeUniquePath();
  const std::string skip_path = tmp_dir.MakeUniquePath();
  constexpr int32_t num_records = 100;
  tkrzw::RecordSorter sorter(base_path, 100, true);
  EXPECT_FALSE(sorter.IsUpdated());
  std::map<std::string, std::string> map;
  for (int32_t i = 1; i <= num_records; ++i) {
    const std::string& key = tkrzw::ToString(i);
    const std::string& value = tkrzw::ToString(i * i);
    EXPECT_EQ(tkrzw::Status::SUCCESS, sorter.Add(key, value));
    EXPECT_TRUE(sorter.IsUpdated());
    map.emplace(key, value);
  }
  tkrzw::MemoryMapParallelFile skip_file;
  EXPECT_EQ(tkrzw::Status::SUCCESS, skip_file.Open(skip_path, true));
  const int64_t skip_record_base = 16;
  EXPECT_EQ(tkrzw::Status::SUCCESS, skip_file.Truncate(skip_record_base));
  auto* skip_rec = new tkrzw::SkipRecord(&skip_file, 4, 4, 4);
  for (int32_t i = 0; i < num_records; i++) {
    const std::string& key = tkrzw::SPrintF("%08d", i);
    const std::string& value = tkrzw::SPrintF("%d", i * i);
    skip_rec->SetData(i, key.data(), key.size(), value.data(), value.size());
    EXPECT_EQ(tkrzw::Status::SUCCESS, skip_rec->Write());
    map.emplace(key, value);
  }
  sorter.AddSkipRecord(skip_rec, skip_record_base);
  sorter.TakeFileOwnership(skip_file.MakeFile());
  EXPECT_EQ(tkrzw::Status::SUCCESS, sorter.Finish());
  int32_t count = 0;
  std::string last_key;
  while (true) {
    std::string key, value;
    tkrzw::Status status = sorter.Get(&key, &value);
    if (status != tkrzw::Status::SUCCESS) {
      EXPECT_EQ(status, tkrzw::Status::NOT_FOUND_ERROR);
      break;
    }
    EXPECT_GE(key, last_key);
    EXPECT_EQ(map[key], value);
    count++;
    last_key = key;
    map.erase(key);
  }
  EXPECT_EQ(num_records * 2, count);
  EXPECT_TRUE(map.empty());
}

// END OF FILE
