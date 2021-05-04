/*************************************************************************************************
 * Tests for tkrzw_file_util.h
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

#include "tkrzw_file_util.h"
#include "tkrzw_lib_common.h"

using namespace testing;

// Main routine
int main(int argc, char** argv) {
  InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

TEST(FileUtilTest, MakeTemporaryName) {
  const std::string& n1 = tkrzw::MakeTemporaryName();
  EXPECT_EQ(20, n1.size());
  const std::string& n2 = tkrzw::MakeTemporaryName();
  EXPECT_EQ(20, n2.size());
  const std::string& n3 = tkrzw::MakeTemporaryName();
  EXPECT_EQ(20, n3.size());
  EXPECT_NE(n1, n2);
  EXPECT_NE(n1, n3);
  EXPECT_NE(n2, n3);
}

TEST(FileUtilTest, JoinPath) {
  EXPECT_EQ("/a/", tkrzw::JoinPath("/a/", ""));
  EXPECT_EQ("/b", tkrzw::JoinPath("/a/", "/b"));
  EXPECT_EQ("/a/b", tkrzw::JoinPath("/a", "b"));
  EXPECT_EQ("/a/b", tkrzw::JoinPath("/a/", "b"));
  EXPECT_EQ("/a//b", tkrzw::JoinPath("/a//", "b"));
}

TEST(FileUtilTest, NormalizePath) {
  EXPECT_EQ("", tkrzw::NormalizePath(""));
  EXPECT_EQ("/", tkrzw::NormalizePath("/"));
  EXPECT_EQ("/", tkrzw::NormalizePath("//"));
  EXPECT_EQ("/a", tkrzw::NormalizePath("/a"));
  EXPECT_EQ("/a", tkrzw::NormalizePath("/a/"));
  EXPECT_EQ("/a/b", tkrzw::NormalizePath("/a/b"));
  EXPECT_EQ("/a/b/c", tkrzw::NormalizePath("/a/b/c"));
  EXPECT_EQ("a/b/c", tkrzw::NormalizePath("a/b/c"));
  EXPECT_EQ("a", tkrzw::NormalizePath("a/b/c/../.."));
  EXPECT_EQ("/a/d", tkrzw::NormalizePath("/a/./b/./c/../../d"));
  EXPECT_EQ("/d", tkrzw::NormalizePath("/a/../b/../c/../../d"));
  EXPECT_EQ("c:/tako/uni/kani", tkrzw::NormalizePath("c:/tako/ika/../uni/./kani"));
}

TEST(FileUtilTest, PathToBaseName) {
  EXPECT_EQ("", tkrzw::PathToBaseName(""));
  EXPECT_EQ("/", tkrzw::PathToBaseName("/"));
  EXPECT_EQ("/", tkrzw::PathToBaseName("//"));
  EXPECT_EQ("a", tkrzw::PathToBaseName("a"));
  EXPECT_EQ("a", tkrzw::PathToBaseName("a/"));
  EXPECT_EQ("a", tkrzw::PathToBaseName("a//"));
  EXPECT_EQ("b", tkrzw::PathToBaseName("a/b"));
  EXPECT_EQ("b", tkrzw::PathToBaseName("/a/b"));
  EXPECT_EQ("c", tkrzw::PathToBaseName("a/b/c"));
  EXPECT_EQ("c", tkrzw::PathToBaseName("/a/b/c"));
  EXPECT_EQ("c", tkrzw::PathToBaseName("/a/b/c/"));
}

TEST(FileUtilTest, PathToDirectoryName) {
  EXPECT_EQ(".", tkrzw::PathToDirectoryName(""));
  EXPECT_EQ("/", tkrzw::PathToDirectoryName("/"));
  EXPECT_EQ("/", tkrzw::PathToDirectoryName("//"));
  EXPECT_EQ(".", tkrzw::PathToDirectoryName("a"));
  EXPECT_EQ(".", tkrzw::PathToDirectoryName("a/"));
  EXPECT_EQ(".", tkrzw::PathToDirectoryName("a//"));
  EXPECT_EQ("a", tkrzw::PathToDirectoryName("a/b"));
  EXPECT_EQ("/a", tkrzw::PathToDirectoryName("/a/b"));
  EXPECT_EQ("a/b", tkrzw::PathToDirectoryName("a/b/c"));
  EXPECT_EQ("/a/b", tkrzw::PathToDirectoryName("/a/b/c"));
  EXPECT_EQ("/a/b", tkrzw::PathToDirectoryName("/a/b/c/"));
}

TEST(FileUtilTest, PathToExtension) {
  EXPECT_EQ("", tkrzw::PathToExtension(""));
  EXPECT_EQ("", tkrzw::PathToExtension("foo"));
  EXPECT_EQ("txt", tkrzw::PathToExtension("foo.txt"));
  EXPECT_EQ("back", tkrzw::PathToExtension("foo.txt.back"));
  EXPECT_EQ("txt", tkrzw::PathToExtension("foo/bar.txt"));
  EXPECT_EQ("txt", tkrzw::PathToExtension("foo.bar/baz.txt"));
}

TEST(FileUtilTest, GetRealPath) {
  std::string real_path;
  EXPECT_EQ(tkrzw::Status::SUCCESS, tkrzw::GetRealPath("/", &real_path));
  EXPECT_EQ("/", real_path);
  EXPECT_EQ(tkrzw::Status::SUCCESS, tkrzw::GetRealPath("///", &real_path));
  EXPECT_EQ("/", real_path);
  std::string noop_path = "/x-" + tkrzw::MakeTemporaryName();
  EXPECT_EQ(tkrzw::Status::NOT_FOUND_ERROR, tkrzw::GetRealPath(noop_path, &real_path));
}

TEST(FileUtilTest, ReadFileStatus) {
  tkrzw::FileStatus fstats;
  EXPECT_EQ(tkrzw::Status::SUCCESS, tkrzw::ReadFileStatus("/", &fstats));
  EXPECT_FALSE(fstats.is_file);
  EXPECT_TRUE(fstats.is_directory);
  EXPECT_GT(fstats.file_size, 0);
  EXPECT_GT(fstats.modified_time, 0);
  EXPECT_FALSE(tkrzw::PathIsFile("/"));
  EXPECT_TRUE(tkrzw::PathIsDirectory("/"));
  EXPECT_EQ(-1, tkrzw::GetFileSize("/"));
}

TEST(FileUtilTest, GetPathToTemporaryDirectory) {
  const std::string& t1 = tkrzw::GetPathToTemporaryDirectory();
  EXPECT_FALSE(t1.empty());
  const std::string& t2 = tkrzw::GetPathToTemporaryDirectory();
  EXPECT_FALSE(t1.empty());
  EXPECT_EQ(t1, t2);
}

TEST(FileUtilTest, FileOperations) {
  const std::string& base_dir = tkrzw::GetPathToTemporaryDirectory();
  tkrzw::FileStatus fstats;
  EXPECT_EQ(tkrzw::Status::SUCCESS, ReadFileStatus(base_dir, &fstats));
  EXPECT_TRUE(fstats.is_directory);
  const std::string& file_path =
      tkrzw::JoinPath(base_dir, "tkrzw-test-" + tkrzw::MakeTemporaryName());
  EXPECT_EQ(tkrzw::Status::NOT_FOUND_ERROR, ReadFileStatus(file_path, &fstats));
  EXPECT_EQ(tkrzw::Status::SUCCESS, tkrzw::WriteFile(file_path, "abc\ndef\n"));
  EXPECT_EQ(tkrzw::Status::SUCCESS, ReadFileStatus(file_path, &fstats));
  EXPECT_TRUE(fstats.is_file);
  EXPECT_FALSE(fstats.is_directory);
  EXPECT_TRUE(tkrzw::PathIsFile(file_path));
  EXPECT_FALSE(tkrzw::PathIsDirectory(file_path));
  EXPECT_EQ(8, tkrzw::GetFileSize(file_path));
  std::string content;
  EXPECT_EQ(tkrzw::Status::SUCCESS, tkrzw::ReadFile(file_path, &content));
  EXPECT_EQ("abc\ndef\n", content);
  EXPECT_EQ("abc\ndef\n", tkrzw::ReadFileSimple(file_path));
  EXPECT_EQ(tkrzw::Status::SUCCESS, tkrzw::RemoveFile(file_path));
  EXPECT_EQ(tkrzw::Status::NOT_FOUND_ERROR, tkrzw::ReadFile(file_path, &content));
  EXPECT_EQ("miss", tkrzw::ReadFileSimple(file_path, "miss"));
  const std::string& new_file_path =
      tkrzw::JoinPath(base_dir, "tkrzw-test-" + tkrzw::MakeTemporaryName());
  EXPECT_EQ(tkrzw::Status::SUCCESS, tkrzw::WriteFile(file_path, "abc\ndef\nefg\n"));
  EXPECT_EQ(tkrzw::Status::SUCCESS, tkrzw::WriteFile(file_path, "abc\n"));
  EXPECT_EQ(tkrzw::Status::SUCCESS, tkrzw::RenameFile(file_path, new_file_path));
  EXPECT_EQ(tkrzw::Status::NOT_FOUND_ERROR, tkrzw::RenameFile(file_path, new_file_path));
  EXPECT_EQ(tkrzw::Status::NOT_FOUND_ERROR, tkrzw::ReadFile(file_path, &content));
  EXPECT_EQ(tkrzw::Status::SUCCESS, tkrzw::ReadFile(new_file_path, &content));
  EXPECT_EQ("abc\n", content);
  EXPECT_EQ(tkrzw::Status::SUCCESS, tkrzw::RemoveFile(new_file_path));
  EXPECT_EQ(tkrzw::Status::SUCCESS, tkrzw::WriteFile(file_path, "old"));
  EXPECT_EQ(tkrzw::Status::SUCCESS, tkrzw::WriteFile(new_file_path, "new"));
  EXPECT_EQ(tkrzw::Status::SUCCESS, tkrzw::RenameFile(file_path, new_file_path));
  EXPECT_FALSE(tkrzw::PathIsFile(file_path));
  EXPECT_EQ("old", tkrzw::ReadFileSimple(new_file_path));
  EXPECT_EQ(tkrzw::Status::SUCCESS, tkrzw::RemoveFile(new_file_path));
}

TEST(FileUtilTest, CopyFileData) {
  const std::string& base_dir = tkrzw::GetPathToTemporaryDirectory();
  const std::string& src_path =
      tkrzw::JoinPath(base_dir, "tkrzw-test-" + tkrzw::MakeTemporaryName());
  const std::string& dest_path =
      tkrzw::JoinPath(base_dir, "tkrzw-test-" + tkrzw::MakeTemporaryName());
  EXPECT_EQ(tkrzw::Status::SUCCESS, tkrzw::WriteFile(src_path, "abcd0123"));
  EXPECT_EQ(tkrzw::Status::SUCCESS, tkrzw::CopyFileData(src_path, dest_path));
  std::string content;
  EXPECT_EQ(tkrzw::Status::SUCCESS, tkrzw::ReadFile(dest_path, &content));
  EXPECT_EQ("abcd0123", content);
  std::string src_content;
  for (int32_t i = 0; i < 2000; i++) {
    src_content.append("0123456789");
  }
  EXPECT_EQ(tkrzw::Status::SUCCESS, tkrzw::WriteFile(src_path, src_content));
  EXPECT_EQ(tkrzw::Status::SUCCESS, tkrzw::CopyFileData(src_path, dest_path));
  std::string dest_content;
  EXPECT_EQ(tkrzw::Status::SUCCESS, tkrzw::ReadFile(dest_path, &dest_content));
  EXPECT_EQ(src_content, dest_content);
}

TEST(FileUtilTest, DirectoryOperation) {
  const std::string& base_dir = tkrzw::GetPathToTemporaryDirectory();
  const std::string& dir_path =
      tkrzw::JoinPath(base_dir, "tkrzw-test-" + tkrzw::MakeTemporaryName());
  std::vector<std::string> child_names;
  EXPECT_EQ(tkrzw::Status::NOT_FOUND_ERROR, tkrzw::ReadDirectory(dir_path, &child_names));
  EXPECT_EQ(tkrzw::Status::SUCCESS, tkrzw::MakeDirectory(dir_path));
  EXPECT_EQ(tkrzw::Status::SUCCESS, tkrzw::WriteFile(tkrzw::JoinPath(dir_path, "file1"), ""));
  EXPECT_EQ(tkrzw::Status::SUCCESS, tkrzw::WriteFile(tkrzw::JoinPath(dir_path, "file2"), ""));
  EXPECT_EQ(tkrzw::Status::SUCCESS, tkrzw::MakeDirectory(tkrzw::JoinPath(dir_path, "dir1")));
  EXPECT_EQ(tkrzw::Status::SUCCESS, tkrzw::MakeDirectory(tkrzw::JoinPath(dir_path, "dir2")));
  EXPECT_EQ(tkrzw::Status::SUCCESS, tkrzw::MakeDirectory(tkrzw::JoinPath(dir_path, "dir3")));
  EXPECT_EQ(tkrzw::Status::SUCCESS, tkrzw::ReadDirectory(dir_path, &child_names));
  EXPECT_THAT(child_names, UnorderedElementsAre("file1", "file2", "dir1", "dir2", "dir3"));
  EXPECT_EQ(tkrzw::Status::SUCCESS, tkrzw::RemoveFile(tkrzw::JoinPath(dir_path, "file2")));
  EXPECT_EQ(tkrzw::Status::SUCCESS, tkrzw::RemoveDirectory(tkrzw::JoinPath(dir_path, "dir3")));
  const std::string& deep_dir_path = tkrzw::JoinPath(dir_path, "hop/step/jump");
  EXPECT_EQ(tkrzw::Status::NOT_FOUND_ERROR, tkrzw::MakeDirectory(deep_dir_path));
  EXPECT_EQ(tkrzw::Status::SUCCESS, tkrzw::MakeDirectory(deep_dir_path, true));
  EXPECT_EQ(tkrzw::Status::DUPLICATION_ERROR, tkrzw::MakeDirectory(deep_dir_path));
  EXPECT_EQ(tkrzw::Status::SUCCESS, tkrzw::MakeDirectory(deep_dir_path, true));
  EXPECT_EQ(tkrzw::Status::SUCCESS, tkrzw::WriteFile(tkrzw::JoinPath(deep_dir_path, "air1"), ""));
  EXPECT_EQ(tkrzw::Status::INFEASIBLE_ERROR, tkrzw::RemoveDirectory(dir_path));
  EXPECT_EQ(tkrzw::Status::SUCCESS, tkrzw::RemoveDirectory(dir_path, true));
  EXPECT_EQ(tkrzw::Status::NOT_FOUND_ERROR, tkrzw::RemoveDirectory(dir_path, true));
  const std::string& new_dir_path =
      tkrzw::JoinPath(base_dir, "tkrzw-test-" + tkrzw::MakeTemporaryName());
  EXPECT_EQ(tkrzw::Status::SUCCESS, tkrzw::MakeDirectory(dir_path));
  EXPECT_EQ(tkrzw::Status::SUCCESS, tkrzw::WriteFile(tkrzw::JoinPath(dir_path, "child"), ""));
  EXPECT_EQ(tkrzw::Status::SUCCESS, tkrzw::MakeDirectory(new_dir_path));
  EXPECT_EQ(tkrzw::Status::SUCCESS, tkrzw::RenameFile(dir_path, new_dir_path));
  EXPECT_FALSE(tkrzw::PathIsDirectory(dir_path));
  child_names.clear();
  EXPECT_EQ(tkrzw::Status::SUCCESS, tkrzw::ReadDirectory(new_dir_path, &child_names));
  EXPECT_THAT(child_names, UnorderedElementsAre("child"));
}

TEST(FileUtilTest, TemporaryDirectory) {
  std::string tmp_path;
  {
    tkrzw::TemporaryDirectory tmp_dir(true, "tkrzw-");
    EXPECT_EQ(tkrzw::Status::SUCCESS, tmp_dir.CreationStatus());
    tmp_path = tmp_dir.Path();
    EXPECT_GT(tmp_path.find("/tkrzw-"), 0);
    EXPECT_TRUE(tkrzw::PathIsDirectory(tmp_path));
    const std::string path1 = tmp_dir.MakeUniquePath();
    EXPECT_EQ(tkrzw::Status::SUCCESS, tkrzw::WriteFile(path1, ""));
    const std::string path2 = tmp_dir.MakeUniquePath("prefix-", "-suffix");
    EXPECT_EQ(tkrzw::PathToBaseName(path2).find("prefix-"), 0);
    EXPECT_GT(tkrzw::PathToBaseName(path2).find("-suffix"), 0);
    EXPECT_EQ(tkrzw::Status::SUCCESS, tkrzw::WriteFile(path2, ""));
    const std::string path3 = tmp_dir.MakeUniquePath("prefix-", "-suffix");
    EXPECT_EQ(tkrzw::Status::SUCCESS, tkrzw::MakeDirectory(path3));
    std::vector<std::string> child_names;
    EXPECT_EQ(tkrzw::Status::SUCCESS, tkrzw::ReadDirectory(tmp_path, &child_names));
    EXPECT_EQ(3, child_names.size());
  }
  EXPECT_FALSE(tkrzw::PathIsDirectory(tmp_path));
  {
    tkrzw::TemporaryDirectory tmp_dir(false, "tkrzw-");
    tmp_path = tmp_dir.Path();
    EXPECT_TRUE(tkrzw::PathIsDirectory(tmp_path));
    const std::string path1 = tmp_dir.MakeUniquePath();
    EXPECT_EQ(tkrzw::Status::SUCCESS, tkrzw::WriteFile(path1, ""));
    const std::string path2 = tkrzw::JoinPath(tmp_path, "hop/step/jump");
    EXPECT_EQ(tkrzw::Status::SUCCESS, tkrzw::MakeDirectory(path2, true));
    EXPECT_EQ(tkrzw::Status::SUCCESS, tmp_dir.CleanUp());
    std::vector<std::string> child_names;
    EXPECT_EQ(tkrzw::Status::SUCCESS, tkrzw::ReadDirectory(tmp_path, &child_names));
    EXPECT_TRUE(child_names.empty());
    EXPECT_EQ(tkrzw::Status::SUCCESS, tkrzw::WriteFile(path1, ""));
    EXPECT_EQ(tkrzw::Status::SUCCESS, tkrzw::MakeDirectory(path2, true));
  }
  EXPECT_TRUE(tkrzw::PathIsDirectory(tmp_path));
  EXPECT_EQ(tkrzw::Status::SUCCESS, tkrzw::RemoveDirectory(tmp_path, true));
}

// END OF FILE
