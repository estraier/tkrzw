/*************************************************************************************************
 * Tests for tkrzw_dbm_tree_impl.h
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
#include "tkrzw_dbm_tree_impl.h"
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

TEST(DBMTreeImplTest, TreeRecord) {
  tkrzw::TreeRecord* rec = tkrzw::CreateTreeRecord("key", "value");
  EXPECT_EQ("key", rec->GetKey());
  EXPECT_EQ("value", rec->GetValue());
  EXPECT_EQ(10, rec->GetSerializedSize());
  tkrzw::TreeRecord* mod_rec = tkrzw::ModifyTreeRecord(rec, "VALUE");
  ASSERT_EQ(rec, mod_rec);
  EXPECT_EQ("key", mod_rec->GetKey());
  EXPECT_EQ("VALUE", mod_rec->GetValue());
  EXPECT_EQ(10, mod_rec->GetSerializedSize());
  const std::string value(1000, 'v');
  rec = tkrzw::ModifyTreeRecord(rec, value);
  EXPECT_EQ("key", rec->GetKey());
  EXPECT_EQ(value, rec->GetValue());
  EXPECT_EQ(1006, rec->GetSerializedSize());
  tkrzw::TreeRecord* rec2 = tkrzw::CreateTreeRecord("j", "xyz");
  EXPECT_FALSE(tkrzw::TreeRecordComparator(tkrzw::LexicalKeyComparator)(rec, rec2));
  EXPECT_TRUE(tkrzw::TreeRecordComparator(tkrzw::LexicalKeyComparator)(rec2, rec));
  EXPECT_FALSE(tkrzw::TreeRecordComparator(tkrzw::LexicalKeyComparator)(rec, rec));
  EXPECT_FALSE(tkrzw::TreeRecordComparator(tkrzw::LexicalKeyComparator)(rec2, rec2));
  tkrzw::FreeTreeRecord(rec2);
  tkrzw::FreeTreeRecord(rec);
}

TEST(DBMTreeImplTest, TreeRecordSearch) {
  std::vector<tkrzw::TreeRecord*> records;
  for (int32_t i = 0; i < 10; i++) {
    const std::string key = tkrzw::SPrintF("%03d", i);
    const std::string value = tkrzw::SPrintF("%08d", i);
    records.emplace_back(tkrzw::CreateTreeRecord(key, value));
  }
  tkrzw::TreeRecordComparator comp(tkrzw::LexicalKeyComparator);
  {
    tkrzw::TreeRecordOnStack stack("");
    tkrzw::TreeRecord* record = stack.record;
    auto it = std::lower_bound(records.begin(), records.end(), record, comp);
    ASSERT_NE(records.end(), it);
    EXPECT_EQ("000", (*it)->GetKey());
    EXPECT_EQ("00000000", (*it)->GetValue());
  }
  for (int32_t i = 0; i < 10; i++) {
    const std::string key = tkrzw::SPrintF("%03d", i);
    const std::string value = tkrzw::SPrintF("%08d", i);
    tkrzw::TreeRecordOnStack stack(key);
    tkrzw::TreeRecord* record = stack.record;
    auto it = std::lower_bound(records.begin(), records.end(), record, comp);
    ASSERT_NE(records.end(), it);
    EXPECT_EQ(key, (*it)->GetKey());
    EXPECT_EQ(value, (*it)->GetValue());
  }
  {
    std::string key(1024, '9');
    tkrzw::TreeRecordOnStack stack(key);
    tkrzw::TreeRecord* record = stack.record;
    auto it = std::lower_bound(records.begin(), records.end(), record, comp);
    EXPECT_EQ(records.end(), it);
  }
  tkrzw::FreeTreeRecords(&records);
}

TEST(DBMTreeImplTest, TreeLink) {
  tkrzw::TreeLink* link = tkrzw::CreateTreeLink("key", 1);
  EXPECT_EQ("key", link->GetKey());
  EXPECT_EQ(1, link->child);
  tkrzw::TreeLink* link2 = tkrzw::CreateTreeLink("j", 2);
  EXPECT_FALSE(tkrzw::TreeLinkComparator(tkrzw::LexicalKeyComparator)(link, link2));
  EXPECT_TRUE(tkrzw::TreeLinkComparator(tkrzw::LexicalKeyComparator)(link2, link));
  EXPECT_FALSE(tkrzw::TreeLinkComparator(tkrzw::LexicalKeyComparator)(link, link));
  EXPECT_FALSE(tkrzw::TreeLinkComparator(tkrzw::LexicalKeyComparator)(link2, link2));
  EXPECT_EQ(10, link->GetSerializedSize(6));
  EXPECT_EQ(8, link2->GetSerializedSize(6));
  tkrzw::FreeTreeLink(link2);
  tkrzw::FreeTreeLink(link);
}

TEST(DBMTreeImplTest, TreeLinkSearch) {
  std::vector<tkrzw::TreeLink*> links;
  for (int32_t i = 0; i < 10; i++) {
    const std::string key = tkrzw::SPrintF("%03d", i);
    links.emplace_back(tkrzw::CreateTreeLink(key, i));
  }
  tkrzw::TreeLinkComparator comp(tkrzw::LexicalKeyComparator);
  {
    tkrzw::TreeLinkOnStack stack("");
    tkrzw::TreeLink* link = stack.link;
    auto it = std::lower_bound(links.begin(), links.end(), link, comp);
    ASSERT_NE(links.end(), it);
    EXPECT_EQ("000", (*it)->GetKey());
    EXPECT_EQ(0, (*it)->child);
    EXPECT_EQ(10, (*it)->GetSerializedSize(6));
  }
  for (int32_t i = 0; i < 10; i++) {
    const std::string key = tkrzw::SPrintF("%03d", i);
    tkrzw::TreeLinkOnStack stack(key);
    tkrzw::TreeLink* link = stack.link;
    auto it = std::lower_bound(links.begin(), links.end(), link, comp);
    ASSERT_NE(links.end(), it);
    EXPECT_EQ(key, (*it)->GetKey());
    EXPECT_EQ(i, (*it)->child);
    EXPECT_EQ(10, (*it)->GetSerializedSize(6));
  }
  {
    std::string key(1024, '9');
    tkrzw::TreeLinkOnStack stack(key);
    tkrzw::TreeLink* link = stack.link;
    auto it = std::lower_bound(links.begin(), links.end(), link, comp);
    EXPECT_EQ(links.end(), it);
  }
  links.emplace_back(tkrzw::CreateTreeLink(std::string(1000, 'z'), 999));
  {
    tkrzw::TreeLinkOnStack stack("z");
    tkrzw::TreeLink* link = stack.link;
    auto it = std::lower_bound(links.begin(), links.end(), link, comp);
    ASSERT_NE(links.end(), it);
    EXPECT_EQ(std::string(1000, 'z'), (*it)->GetKey());
    EXPECT_EQ(999, (*it)->child);
    EXPECT_EQ(1008, (*it)->GetSerializedSize(6));
  }
  tkrzw::FreeTreeLinks(&links);
}

// END OF FILE
