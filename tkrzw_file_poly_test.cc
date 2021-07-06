/*************************************************************************************************
 * Tests for tkrzw_file_poly.h
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

#include "tkrzw_file.h"
#include "tkrzw_file_poly.h"
#include "tkrzw_file_std.h"
#include "tkrzw_file_test_common.h"
#include "tkrzw_file_util.h"
#include "tkrzw_lib_common.h"

using namespace testing;

// Main routine
int main(int argc, char** argv) {
  InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

class PolyFileTest : public Test {};

TEST_F(PolyFileTest, Attributes) {
  tkrzw::TemporaryDirectory tmp_dir(true, "tkrzw-");
  const std::string file_path = tmp_dir.MakeUniquePath();
  tkrzw::PolyFile file;
  EXPECT_EQ(tkrzw::Status::SUCCESS, file.Open(file_path, true));
  EXPECT_TRUE(file.IsMemoryMapping());
  EXPECT_FALSE(file.IsAtomic());
  EXPECT_EQ(tkrzw::Status::SUCCESS, file.Close());
  EXPECT_EQ(tkrzw::Status::SUCCESS,
            file.OpenAdvanced(file_path, true, 0, {{"file", "mmap-atom"}}));
  EXPECT_TRUE(file.IsMemoryMapping());
  EXPECT_TRUE(file.IsAtomic());
  EXPECT_EQ(tkrzw::Status::SUCCESS, file.Close());
  EXPECT_EQ(tkrzw::Status::SUCCESS,
            file.OpenAdvanced(file_path, true, 0, {{"file", "pos-para"}}));
  EXPECT_FALSE(file.IsMemoryMapping());
  EXPECT_FALSE(file.IsAtomic());
  EXPECT_EQ(tkrzw::Status::SUCCESS, file.Close());
  EXPECT_EQ(tkrzw::Status::SUCCESS,
            file.OpenAdvanced(file_path, true, 0, {{"file", "pos-atom"}}));
  EXPECT_FALSE(file.IsMemoryMapping());
  EXPECT_TRUE(file.IsAtomic());
  EXPECT_EQ(tkrzw::Status::SUCCESS, file.Close());
  EXPECT_EQ(tkrzw::Status::SUCCESS,
            file.OpenAdvanced(file_path, true, 0, {{"file", "std"}}));
  EXPECT_FALSE(file.IsMemoryMapping());
  EXPECT_TRUE(file.IsAtomic());
  EXPECT_EQ(tkrzw::Status::SUCCESS, file.Close());
}

TEST_F(PolyFileTest, Basic) {
  tkrzw::TemporaryDirectory tmp_dir(true, "tkrzw-");
  const std::string file_path = tmp_dir.MakeUniquePath();
  const std::string rename_file_path = tmp_dir.MakeUniquePath();
  tkrzw::PolyFile file;
  EXPECT_EQ(tkrzw::Status::SUCCESS,
            file.OpenAdvanced(file_path, true, 0, {{"file", "pos-atom"}}));
  EXPECT_EQ(tkrzw::Status::SUCCESS, file.Write(5, "123", 3));
  EXPECT_EQ(tkrzw::Status::SUCCESS, file.Write(0, "ABCDE", 5));
  EXPECT_EQ(tkrzw::Status::SUCCESS, file.Append("45", 2));
  EXPECT_EQ(tkrzw::Status::SUCCESS, file.Expand(2));
  EXPECT_EQ(tkrzw::Status::SUCCESS, file.Append("FGH", 3));
  EXPECT_EQ(tkrzw::Status::SUCCESS, file.Write(10, "XY", 2));
  EXPECT_EQ(15, file.GetSizeSimple());
  EXPECT_EQ(tkrzw::Status::SUCCESS, file.Synchronize(false));
  EXPECT_EQ(file_path, file.GetPathSimple());
  char buf[15];
  EXPECT_EQ(tkrzw::Status::SUCCESS, file.Read(0, buf, 15));
  EXPECT_EQ("ABCDE12345XYFGH", std::string(buf, 15));
  EXPECT_EQ(tkrzw::Status::SUCCESS, file.Truncate(20));
  EXPECT_EQ(20, file.GetSizeSimple());
  EXPECT_EQ(tkrzw::Status::SUCCESS, file.TruncateFakely(10));
  EXPECT_EQ(10, file.GetSizeSimple());
  EXPECT_EQ(tkrzw::Status::SUCCESS, file.Rename(rename_file_path));
  EXPECT_EQ(tkrzw::Status::SUCCESS, file.DisablePathOperations());
  EXPECT_EQ(tkrzw::Status::SUCCESS, file.Read(3, buf, 4));
  EXPECT_EQ("DE12", std::string(buf, 4));
  EXPECT_EQ(tkrzw::Status::SUCCESS, file.Close());
  EXPECT_EQ(tkrzw::Status::SUCCESS, file.OpenAdvanced(
      rename_file_path, false, tkrzw::File::OPEN_NO_CREATE, {{"file", "std"}}));
  EXPECT_EQ(tkrzw::Status::SUCCESS, file.Read(0, buf, 10));
  EXPECT_EQ("ABCDE12345", std::string(buf, 10));
  EXPECT_EQ(10, file.GetSizeSimple());
  auto* in_file = dynamic_cast<tkrzw::StdFile*>(file.GetInternalFile());
  EXPECT_EQ(10, in_file->GetSizeSimple());
  EXPECT_EQ(tkrzw::Status::SUCCESS, file.Close());
  auto made_file = file.MakeFile();
  EXPECT_EQ(tkrzw::Status::SUCCESS, file.Open(
      rename_file_path, false, tkrzw::File::OPEN_NO_CREATE));
  EXPECT_EQ(10, file.GetSizeSimple());
  EXPECT_EQ(tkrzw::Status::SUCCESS, file.Close());
}

// END OF FILE
