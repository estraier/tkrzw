/*************************************************************************************************
 * Tests for tkrzw_dbm_hash_impl.h
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

#include "tkrzw_compress.h"
#include "tkrzw_dbm.h"
#include "tkrzw_dbm_hash_impl.h"
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

TEST(DBMHashImplTest, HashRecord) {
  tkrzw::TemporaryDirectory tmp_dir(true, "tkrzw-");
  const std::string file_path = tmp_dir.MakeUniquePath();
  const std::string offset_file_path = tmp_dir.MakeUniquePath();
  tkrzw::MemoryMapParallelFile file;
  EXPECT_EQ(tkrzw::Status::SUCCESS, file.Open(file_path, true));
  tkrzw::MemoryMapParallelFile offset_file;
  EXPECT_EQ(tkrzw::Status::SUCCESS, offset_file.Open(offset_file_path, true));
  const std::vector<int32_t> crc_widths = {0, 1, 2, 4};
  const std::vector<int32_t> offset_widths = {3, 4, 5};
  const std::vector<int32_t> align_pows = {0, 1, 2, 3, 9, 10};
  const std::vector<int32_t> key_sizes = {
    0, 1, 2, 4, 8, 15, 16, 31, 32, 63, 64, 127, 128, 230, 16385, 16386};
  const std::vector<int32_t> value_sizes = {
    0, 1, 2, 4, 8, 15, 16, 31, 32, 63, 64, 127, 128, 230, 16385, 16386};
  for (const auto& crc_width : crc_widths) {
    for (const auto& offset_width : offset_widths) {
      for (const auto& align_pow : align_pows) {
        EXPECT_EQ(tkrzw::Status::SUCCESS, file.Truncate(0));
        int64_t off = 0;
        tkrzw::HashRecord r1(&file, crc_width, offset_width, align_pow);
        for (const auto& key_size : key_sizes) {
          for (const auto& value_size : value_sizes) {
            char* key_buf = new char[key_size];
            std::memset(key_buf, 'k', key_size);
            char* value_buf = new char[value_size];
            std::memset(value_buf, 'v', value_size);
            const int64_t child_offset = (key_size + value_size) << align_pow;
            r1.SetData(tkrzw::HashRecord::OP_SET, 0, key_buf, key_size,
                       value_buf, value_size, child_offset);
            EXPECT_EQ(tkrzw::HashRecord::OP_SET, r1.GetOperationType());
            EXPECT_EQ(tkrzw::Status::SUCCESS, r1.Write(off, nullptr));
            tkrzw::HashRecord r2(&file, crc_width, offset_width, align_pow);
            EXPECT_EQ(tkrzw::Status::SUCCESS, r2.ReadMetadataKey(off, 48));
            EXPECT_EQ(tkrzw::HashRecord::OP_SET, r2.GetOperationType());
            EXPECT_EQ(r1.GetKey(), r2.GetKey());
            EXPECT_EQ(r1.GetChildOffset(), r2.GetChildOffset());
            if (r2.GetWholeSize() == 0) {
              EXPECT_EQ(tkrzw::Status::SUCCESS, r2.ReadBody());
              EXPECT_EQ(r1.GetWholeSize(), r2.GetWholeSize());
            }
            std::string_view r2_value = r2.GetValue();
            if (r2_value.data() == nullptr) {
              EXPECT_EQ(tkrzw::Status::SUCCESS, r2.ReadBody());
              r2_value = r2.GetValue();
            }
            EXPECT_EQ(r1.GetValue(), r2_value);
            EXPECT_EQ(r1.GetWholeSize(), r2.GetWholeSize());
            off += r2.GetWholeSize();
            EXPECT_EQ(file.GetSizeSimple(), off);
            int64_t new_off = 0;
            EXPECT_EQ(tkrzw::Status::SUCCESS, r2.Write(-1, &new_off));
            EXPECT_EQ(off, new_off);
            off = new_off + r2.GetWholeSize();
            delete[] value_buf;
            delete[] key_buf;
          }
        }
        off = 0;
        int32_t count_first = 0;
        tkrzw::HashRecord r3(&file, crc_width, offset_width, align_pow);
        while (off < file.GetSizeSimple()) {
          EXPECT_EQ(tkrzw::Status::SUCCESS, r3.ReadMetadataKey(off, 48));
          EXPECT_EQ(tkrzw::HashRecord::OP_SET, r3.GetOperationType());
          if (r3.GetWholeSize() == 0) {
            EXPECT_EQ(tkrzw::Status::SUCCESS, r3.ReadBody());
          }
          const size_t whole_size = r3.GetWholeSize();
          r3.SetData(count_first % 2 == 0 ?
                     tkrzw::HashRecord::OP_SET : tkrzw::HashRecord::OP_REMOVE,
                     whole_size, "", 0, "", 0, 0);
          EXPECT_EQ(whole_size, r3.GetWholeSize());
          EXPECT_EQ(tkrzw::Status::SUCCESS, r3.Write(off, nullptr));
          off += whole_size;
          count_first++;
        }
        EXPECT_EQ(file.GetSizeSimple(), off);
        std::set<int64_t> offsets;
        off = 0;
        int32_t count_second = 0;
        while (off < file.GetSizeSimple()) {
          offsets.emplace(off);
          EXPECT_EQ(tkrzw::Status::SUCCESS, r3.ReadMetadataKey(off, 48));
          if (count_second % 2 == 0) {
            EXPECT_EQ(tkrzw::HashRecord::OP_SET, r3.GetOperationType());
          } else {
            EXPECT_EQ(tkrzw::HashRecord::OP_REMOVE, r3.GetOperationType());
          }
          if (r3.GetWholeSize() == 0) {
            EXPECT_EQ(tkrzw::Status::SUCCESS, r3.ReadBody());
          }
          off += r3.GetWholeSize();
          count_second++;
        }
        EXPECT_EQ(file.GetSizeSimple(), off);
        EXPECT_EQ(count_first, count_second);
        class Counter final : public tkrzw::DBM::RecordProcessor {
         public:
          std::string_view ProcessFull(std::string_view key, std::string_view value) override {
            count_++;
            return NOOP;
          }
          std::string_view ProcessEmpty(std::string_view key) override {
            count_++;
            return NOOP;
          }
          int32_t GetCount() const {
            return count_;
          }
         private:
          int32_t count_ = 0;
        } counter;
        EXPECT_EQ(tkrzw::Status::SUCCESS, tkrzw::HashRecord::ReplayOperations(
            &file, &counter, 0, crc_width, nullptr,
            offset_width, align_pow, 48, false, -1));
        EXPECT_EQ(count_first, counter.GetCount());
        EXPECT_EQ(tkrzw::Status::SUCCESS, offset_file.Truncate(0));
        EXPECT_EQ(tkrzw::Status::SUCCESS, tkrzw::HashRecord::ExtractOffsets(
            &file, &offset_file, 0, crc_width, offset_width, align_pow, false, -1));
        const int64_t num_offsets = offset_file.GetSizeSimple() / offset_width;
        EXPECT_EQ(count_first, num_offsets);
        std::set<int64_t> rev_offsets(offsets.begin(), offsets.end());
        tkrzw::OffsetReader reader(&offset_file, offset_width, align_pow, false);
        while (true) {
          int64_t offset = 0;
          const tkrzw::Status status = reader.ReadOffset(&offset);
          if (status != tkrzw::Status::SUCCESS) {
            EXPECT_EQ(tkrzw::Status::NOT_FOUND_ERROR, status);
            break;
          }
          EXPECT_EQ(1, offsets.erase(offset));
        }
        EXPECT_TRUE(offsets.empty());
        tkrzw::OffsetReader rev_reader(&offset_file, offset_width, align_pow, true);
        while (true) {
          int64_t offset = 0;
          const tkrzw::Status status = rev_reader.ReadOffset(&offset);
          if (status != tkrzw::Status::SUCCESS) {
            EXPECT_EQ(tkrzw::Status::NOT_FOUND_ERROR, status);
            break;
          }
          EXPECT_EQ(1, rev_offsets.erase(offset));
        }
        EXPECT_TRUE(rev_offsets.empty());
        int64_t next_offset = 0;
        EXPECT_EQ(tkrzw::Status::SUCCESS, r1.FindNextOffset(0, 48, &next_offset));
        EXPECT_GT(next_offset, 0);
        EXPECT_EQ(tkrzw::Status::SUCCESS, r1.ReadMetadataKey(0, 48));
        int64_t first_rec_size = r1.GetWholeSize();
        if (first_rec_size == 0) {
          EXPECT_EQ(tkrzw::Status::SUCCESS, r1.ReadBody());
          first_rec_size = r1.GetWholeSize();
        }
        EXPECT_EQ(tkrzw::Status::SUCCESS, r1.ReadMetadataKey(first_rec_size, 48));
        int64_t second_rec_size = r1.GetWholeSize();
        if (second_rec_size == 0) {
          EXPECT_EQ(tkrzw::Status::SUCCESS, r1.ReadBody());
          second_rec_size = r1.GetWholeSize();
        }
        const std::string dummy_data(1 + offset_width + 3, 0xFF);
        EXPECT_EQ(tkrzw::Status::SUCCESS, file.Write(0, dummy_data.data(), dummy_data.size()));
        EXPECT_EQ(tkrzw::Status::SUCCESS,
                  file.Write(first_rec_size, dummy_data.data(), dummy_data.size()));
        EXPECT_EQ(tkrzw::Status::BROKEN_DATA_ERROR, r1.ReadMetadataKey(0, 48));
        EXPECT_EQ(tkrzw::Status::BROKEN_DATA_ERROR, r1.ReadMetadataKey(first_rec_size, 48));
        Counter broken_counter;
        EXPECT_EQ(tkrzw::Status::BROKEN_DATA_ERROR, tkrzw::HashRecord::ReplayOperations(
            &file, &broken_counter, 0, crc_width, nullptr,
            offset_width, align_pow, 48, false, -1));
        EXPECT_EQ(tkrzw::Status::SUCCESS, offset_file.Truncate(0));
        EXPECT_EQ(tkrzw::Status::BROKEN_DATA_ERROR, tkrzw::HashRecord::ExtractOffsets(
            &file, &offset_file, 0, crc_width, offset_width, align_pow, false, -1));
        Counter skip_counter;
        EXPECT_EQ(tkrzw::Status::SUCCESS, tkrzw::HashRecord::ReplayOperations(
            &file, &skip_counter, 0, crc_width, nullptr,
            offset_width, align_pow, 48, true, -1));
        EXPECT_EQ(counter.GetCount() - 2, skip_counter.GetCount());
        EXPECT_EQ(tkrzw::Status::SUCCESS, offset_file.Truncate(0));
        EXPECT_EQ(tkrzw::Status::SUCCESS, tkrzw::HashRecord::ExtractOffsets(
            &file, &offset_file, 0, crc_width, offset_width, align_pow, true, -1));
        const int64_t skip_num_offsets = offset_file.GetSizeSimple() / offset_width;
        EXPECT_EQ(count_first - 2, skip_num_offsets);
        r1.SetData(tkrzw::HashRecord::OP_SET, first_rec_size, "", 0, "", 0, 0);
        EXPECT_EQ(first_rec_size, r1.GetWholeSize());
        EXPECT_EQ(tkrzw::Status::SUCCESS, r1.Write(0, nullptr));
        r1.SetData(tkrzw::HashRecord::OP_REMOVE, second_rec_size, "", 0, "", 0, 0);
        EXPECT_EQ(second_rec_size, r1.GetWholeSize());
        EXPECT_EQ(tkrzw::Status::SUCCESS, r1.Write(first_rec_size, nullptr));
        EXPECT_EQ(tkrzw::Status::SUCCESS, r1.ReadMetadataKey(0, 48));
        EXPECT_EQ(tkrzw::HashRecord::OP_SET, r1.GetOperationType());
        EXPECT_EQ(0, r1.GetKey().size());
        EXPECT_EQ(tkrzw::Status::SUCCESS, r1.ReadMetadataKey(first_rec_size, 48));
        EXPECT_EQ(tkrzw::HashRecord::OP_REMOVE, r1.GetOperationType());
        EXPECT_EQ(0, r1.GetKey().size());
        Counter restore_counter;
        EXPECT_EQ(tkrzw::Status::SUCCESS, tkrzw::HashRecord::ReplayOperations(
            &file, &restore_counter, 0, crc_width, nullptr,
            offset_width, align_pow, 48, false, -1));
        EXPECT_EQ(counter.GetCount(), restore_counter.GetCount());
        EXPECT_EQ(tkrzw::Status::SUCCESS, offset_file.Truncate(0));
        EXPECT_EQ(tkrzw::Status::SUCCESS, tkrzw::HashRecord::ExtractOffsets(
            &file, &offset_file, 0, crc_width, offset_width, align_pow, false, -1));
        const int64_t restore_num_offsets = offset_file.GetSizeSimple() / offset_width;
        EXPECT_EQ(count_first, restore_num_offsets);
        off = 0;
        int32_t count_void = 0;
        while (off < file.GetSizeSimple()) {
          EXPECT_EQ(tkrzw::Status::SUCCESS, r1.ReadMetadataKey(off, 48));
          if (r1.GetWholeSize() == 0) {
            EXPECT_EQ(tkrzw::Status::SUCCESS, r1.ReadBody());
          }
          const size_t whole_size = r1.GetWholeSize();
          r1.SetData(count_void % 2 == 0 ? tkrzw::HashRecord::OP_SET : tkrzw::HashRecord::OP_VOID,
                     whole_size, "", 0, "", 0, 0);
          EXPECT_EQ(whole_size, r1.GetWholeSize());
          EXPECT_EQ(tkrzw::Status::SUCCESS, r1.Write(off, nullptr));
          off += whole_size;
          count_void++;
        }
        EXPECT_EQ(file.GetSizeSimple(), off);
        Counter void_counter;
        EXPECT_EQ(tkrzw::Status::SUCCESS, tkrzw::HashRecord::ReplayOperations(
            &file, &void_counter, 0, crc_width, nullptr,
            offset_width, align_pow, 48, false, -1));
        EXPECT_EQ(count_void / 2, void_counter.GetCount());
        EXPECT_EQ(tkrzw::Status::SUCCESS, offset_file.Truncate(0));
        EXPECT_EQ(tkrzw::Status::SUCCESS, tkrzw::HashRecord::ExtractOffsets(
            &file, &offset_file, 0, crc_width, offset_width, align_pow, false, -1));
        const int64_t void_num_offsets = offset_file.GetSizeSimple() / offset_width;
        EXPECT_EQ(count_void / 2, void_num_offsets);
      }
    }
  }
  EXPECT_EQ(tkrzw::Status::SUCCESS, offset_file.Close());
  EXPECT_EQ(tkrzw::Status::SUCCESS, file.Close());
}

TEST(DBMHashImplTest, FreeBlockPool) {
  tkrzw::FreeBlockPool fbp(3);
  tkrzw::FreeBlock fb;
  EXPECT_FALSE(fbp.FetchFreeBlock(0, &fb));
  fbp.InsertFreeBlock(0, 10);
  EXPECT_EQ(1, fbp.Size());
  EXPECT_FALSE(fbp.FetchFreeBlock(11, &fb));
  EXPECT_TRUE(fbp.FetchFreeBlock(10, &fb));
  EXPECT_EQ(0, fb.offset);
  EXPECT_EQ(10, fb.size);
  EXPECT_EQ(0, fbp.Size());
  fbp.InsertFreeBlock(0, 10);
  fbp.InsertFreeBlock(10, 10);
  fbp.InsertFreeBlock(20, 10);
  fbp.InsertFreeBlock(30, 10);
  EXPECT_EQ(3, fbp.Size());
  EXPECT_TRUE(fbp.FetchFreeBlock(10, &fb));
  EXPECT_EQ(10, fb.size);
  EXPECT_EQ(2, fbp.Size());
  EXPECT_FALSE(fbp.FetchFreeBlock(20, &fb));
  fbp.InsertFreeBlock(40, 20);
  EXPECT_TRUE(fbp.FetchFreeBlock(20, &fb));
  EXPECT_EQ(40, fb.offset);
  EXPECT_EQ(20, fb.size);
  EXPECT_EQ(2, fbp.Size());
  EXPECT_FALSE(fbp.FetchFreeBlock(20, &fb));
  fbp.InsertFreeBlock(40, 20);
  fbp.InsertFreeBlock(60, 30);
  EXPECT_TRUE(fbp.FetchFreeBlock(10, &fb));
  EXPECT_EQ(20, fb.offset);
  EXPECT_EQ(10, fb.size);
  fbp.InsertFreeBlock(90, 10);
  fbp.SetCapacity(1);
  EXPECT_EQ(1, fbp.Size());
  EXPECT_TRUE(fbp.FetchFreeBlock(30, &fb));
  EXPECT_EQ(60, fb.offset);
  EXPECT_EQ(30, fb.size);
  EXPECT_EQ(0, fbp.Size());
  fbp.InsertFreeBlock(10, 10);
  fbp.Clear();
  EXPECT_EQ(0, fbp.Size());
  fbp.SetCapacity(10);
  for (int32_t offset = 0; offset < 80; offset += 8) {
    fbp.InsertFreeBlock(offset, 8);
  }
  EXPECT_EQ(10, fbp.Size());
  std::string str = fbp.Serialize(4, 3, 80);
  EXPECT_EQ(80, str.size());
  fbp.Clear();
  fbp.Deserialize(str, 4, 3);
  EXPECT_EQ(10, fbp.Size());
  str = fbp.Serialize(4, 3, 100);
  EXPECT_EQ(100, str.size());
  fbp.Deserialize(str, 4, 3);
  EXPECT_EQ(10, fbp.Size());
  str = fbp.Serialize(4, 3, 50);
  EXPECT_EQ(50, str.size());
  fbp.Deserialize(str, 4, 3);
  EXPECT_EQ(6, fbp.Size());
  EXPECT_TRUE(fbp.FetchFreeBlock(8, &fb));
  EXPECT_EQ(32, fb.offset);
}

TEST(DBMHashImplTest, MagicChecksum) {
  EXPECT_EQ(14, tkrzw::MagicChecksum("", 0, "", 0));
  EXPECT_EQ(15, tkrzw::MagicChecksum("\x01", 1, "", 0));
  EXPECT_EQ(15, tkrzw::MagicChecksum("", 0, "\x01", 1));
  for (int32_t i = 0; i < 256; i++) {
    const uint32_t sum = tkrzw::MagicChecksum(reinterpret_cast<char*>(&i), sizeof(i), "", 0);
    EXPECT_GE(sum, 3);
    EXPECT_LT(sum, 64);
  }
}

TEST(DBMHashImplTest, CallRecordProcess) {
  class Checker : public tkrzw::DBM::RecordProcessor {
   public:
    explicit Checker(std::string_view new_value) : new_value_(new_value) {}
    std::string_view ProcessFull(std::string_view key, std::string_view value) {
      key_ = key;
      value_ = value;
      return new_value_;
    }
    std::string_view ProcessEmpty(std::string_view key) {
      key_ = key;
      value_.clear();
      return new_value_;
    }
    std::string GetKey() const {
      return key_;
    }
    std::string GetValue() const {
      return value_;
    }
    std::string_view new_value_;
    std::string key_;
    std::string value_;
  };
  tkrzw::DummyCompressor dummy_compressor;
  tkrzw::ZLibCompressor real_compressor;
  tkrzw::Compressor* compressor = real_compressor.IsSupported() ?
      static_cast<tkrzw::Compressor*>(&real_compressor) :
      static_cast<tkrzw::Compressor*>(&dummy_compressor);
  tkrzw::ScopedStringView placeholder;
  {
    Checker proc(tkrzw::DBM::RecordProcessor::NOOP);
    std::string_view new_value_orig;
    std::string_view new_value = tkrzw::CallRecordProcessFull(
        &proc, "foo", "barbar", &new_value_orig, nullptr, &placeholder);
    EXPECT_EQ(tkrzw::DBM::RecordProcessor::NOOP.data(), new_value.data());
    EXPECT_EQ("foo", proc.GetKey());
    EXPECT_EQ("barbar", proc.GetValue());
    new_value = tkrzw::CallRecordProcessEmpty(
        &proc, "boo", &new_value_orig, nullptr, &placeholder);
    EXPECT_EQ(tkrzw::DBM::RecordProcessor::NOOP.data(), new_value.data());
    EXPECT_EQ("boo", proc.GetKey());
    EXPECT_EQ("", proc.GetValue());
  }
  {
    Checker proc(tkrzw::DBM::RecordProcessor::NOOP);
    size_t comp_size = 0;
    char* comp_data = compressor->Compress("barbar", 6, &comp_size);
    std::string_view new_value_orig;
    std::string_view new_value = tkrzw::CallRecordProcessFull(
        &proc, "foo", std::string(comp_data, comp_size), &new_value_orig,
        compressor, &placeholder);
    EXPECT_EQ(tkrzw::DBM::RecordProcessor::NOOP.data(), new_value.data());
    EXPECT_EQ("foo", proc.GetKey());
    EXPECT_EQ("barbar", proc.GetValue());
    new_value = tkrzw::CallRecordProcessEmpty(
        &proc, "boo", &new_value_orig, compressor, &placeholder);
    EXPECT_EQ(tkrzw::DBM::RecordProcessor::NOOP.data(), new_value.data());
    EXPECT_EQ("boo", proc.GetKey());
    EXPECT_EQ("", proc.GetValue());
    tkrzw::xfree(comp_data);
  }
  {
    Checker proc("hello");
    size_t comp_size = 0;
    char* comp_data = compressor->Compress("barbar", 6, &comp_size);
    std::string_view new_value_orig;
    std::string_view new_value = tkrzw::CallRecordProcessFull(
        &proc, "foo", std::string(comp_data, comp_size), &new_value_orig,
        compressor, &placeholder);
    EXPECT_NE(nullptr, new_value.data());
    {
      size_t decomp_size = 0;
      char* decomp_data = compressor->Decompress(
          new_value.data(), new_value.size(), &decomp_size);
      EXPECT_EQ("hello", std::string_view(decomp_data, decomp_size));
      tkrzw::xfree(decomp_data);
    }
    EXPECT_EQ("foo", proc.GetKey());
    EXPECT_EQ("barbar", proc.GetValue());
    new_value = tkrzw::CallRecordProcessEmpty(
        &proc, "boo", &new_value_orig, compressor, &placeholder);
    EXPECT_NE(nullptr, new_value.data());
    {
      size_t decomp_size = 0;
      char* decomp_data = compressor->Decompress(
          new_value.data(), new_value.size(), &decomp_size);
      EXPECT_EQ("hello", std::string_view(decomp_data, decomp_size));
      tkrzw::xfree(decomp_data);
    }
    EXPECT_EQ("boo", proc.GetKey());
    EXPECT_EQ("", proc.GetValue());
    tkrzw::xfree(comp_data);
  }
}

// END OF FILE
