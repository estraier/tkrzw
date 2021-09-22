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
#include "tkrzw_thread_util.h"

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
  EXPECT_EQ("../tako", tkrzw::NormalizePath("../tako"));
  EXPECT_EQ("../../tako", tkrzw::NormalizePath(".././../tako"));
  EXPECT_EQ("../tako", tkrzw::NormalizePath("../tako/ika/.."));
  EXPECT_EQ("..", tkrzw::NormalizePath("../tako/ika/../.."));
  EXPECT_EQ("../..", tkrzw::NormalizePath("../tako/ika/../../.."));
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
  EXPECT_EQ("var", tkrzw::PathToBaseName("/var/"));
  EXPECT_EQ("log", tkrzw::PathToBaseName("/var/log/"));
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
  EXPECT_EQ("/", tkrzw::PathToDirectoryName("/var/"));
  EXPECT_EQ("/var", tkrzw::PathToDirectoryName("/var/log/"));
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
  EXPECT_EQ(tkrzw::Status::SUCCESS, tkrzw::ReadFile(file_path, &content, 3));
  EXPECT_EQ("abc", content);
  EXPECT_EQ(tkrzw::Status::SUCCESS, tkrzw::ReadFile(file_path, &content));
  EXPECT_EQ("abc\ndef\n", content);
  EXPECT_EQ("abc\ndef\n", tkrzw::ReadFileSimple(file_path));
  EXPECT_EQ("abc\n", tkrzw::ReadFileSimple(file_path, "", 4));
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
  EXPECT_EQ(3, tkrzw::GetFileSize(new_file_path));
  EXPECT_EQ(tkrzw::Status::SUCCESS, tkrzw::TruncateFile(new_file_path, 100));
  EXPECT_EQ(100, tkrzw::GetFileSize(new_file_path));
  EXPECT_EQ(tkrzw::Status::SUCCESS, tkrzw::TruncateFile(new_file_path, 0));
  EXPECT_EQ(0, tkrzw::GetFileSize(new_file_path));
  EXPECT_EQ(tkrzw::Status::SUCCESS, tkrzw::TruncateFile(new_file_path, 10));
  EXPECT_EQ(10, tkrzw::GetFileSize(new_file_path));
  EXPECT_EQ(tkrzw::Status::SUCCESS, tkrzw::RemoveFile(new_file_path));
  EXPECT_EQ(-1, tkrzw::GetFileSize(new_file_path));
  EXPECT_EQ(tkrzw::Status::SUCCESS, tkrzw::WriteFileAtomic(file_path, "1234", new_file_path));
  EXPECT_TRUE(tkrzw::PathIsFile(file_path));
  EXPECT_FALSE(tkrzw::PathIsFile(new_file_path));
  EXPECT_EQ(tkrzw::Status::SUCCESS, tkrzw::ReadFile(file_path, &content));
  EXPECT_EQ("1234", content);
  EXPECT_EQ(tkrzw::Status::SUCCESS, tkrzw::WriteFileAtomic(file_path, "5678"));
  EXPECT_EQ(tkrzw::Status::SUCCESS, tkrzw::ReadFile(file_path, &content));
  EXPECT_FALSE(tkrzw::PathIsFile(file_path + ".tmp"));
  EXPECT_EQ("5678", content);
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
  for (int32_t i = 0; i < 8000; i++) {
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

  EXPECT_EQ(tkrzw::Status::SUCCESS, tkrzw::SynchronizeFile(new_dir_path));
  EXPECT_EQ(tkrzw::Status::SUCCESS,
            tkrzw::SynchronizeFile(tkrzw::JoinPath(new_dir_path, "child")));
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

TEST(FileUtilTest, PageCache) {
  constexpr int64_t num_threads = 4;
  constexpr int64_t file_size = 8192;
  constexpr int64_t block_size = 8;
  char file_buffer[file_size + block_size];
  for (int32_t i = 0; i < file_size; i++) {
    file_buffer[i] = '0' + i % 10;
  }
  const int64_t padded_size = tkrzw::AlignNumber(file_size, block_size);
  for (int32_t i = file_size; i < padded_size; i++) {
    file_buffer[i] = 'X';
  }
  auto read_func = [&](int64_t off, void* buf, size_t size) {
                     EXPECT_LE(off + size, padded_size);
                     std::memcpy(buf, file_buffer + off, size);
                     return tkrzw::Status(tkrzw::Status::SUCCESS);
                   };
  auto write_func = [&](int64_t off, const void* buf, size_t size) {
                      EXPECT_LE(off + size, padded_size);
                      std::memcpy(file_buffer + off, buf, size);
                      return tkrzw::Status(tkrzw::Status::SUCCESS);
                    };
  tkrzw::PageCache cache(block_size, 200, read_func, write_func);
  EXPECT_EQ(0, cache.GetRegionSize());
  char buf[256];
  EXPECT_EQ(tkrzw::Status::SUCCESS, cache.Read(0, buf, 4));
  EXPECT_EQ("0123", std::string_view(buf, 4));
  EXPECT_EQ(0, cache.GetRegionSize());
  cache.Clear();
  EXPECT_EQ(tkrzw::Status::SUCCESS, cache.Read(4, buf, 4));
  EXPECT_EQ("4567", std::string_view(buf, 4));
  cache.Clear();
  EXPECT_EQ(tkrzw::Status::SUCCESS, cache.Read(4, buf, 8));
  EXPECT_EQ("45678901", std::string_view(buf, 8));
  EXPECT_EQ(tkrzw::Status::SUCCESS, cache.Read(0, buf, 16));
  EXPECT_EQ("0123456789012345", std::string_view(buf, 16));
  cache.Clear();
  EXPECT_EQ(tkrzw::Status::SUCCESS, cache.Write(0, "ABCD", 4));
  EXPECT_EQ(4, cache.GetRegionSize());
  EXPECT_EQ(tkrzw::Status::SUCCESS, cache.Read(0, buf, 8));
  EXPECT_EQ("ABCD4567", std::string_view(buf, 8));
  EXPECT_EQ(4, cache.GetRegionSize());
  EXPECT_EQ(tkrzw::Status::SUCCESS, cache.Write(4, "EFGH", 4));
  EXPECT_EQ(tkrzw::Status::SUCCESS, cache.Read(0, buf, 8));
  EXPECT_EQ("ABCDEFGH", std::string_view(buf, 8));
  EXPECT_EQ(8, cache.GetRegionSize());
  EXPECT_EQ(tkrzw::Status::SUCCESS, cache.Write(6, "XXXX", 4));
  EXPECT_EQ(tkrzw::Status::SUCCESS, cache.Write(10, "YYYY", 4));
  EXPECT_EQ(tkrzw::Status::SUCCESS, cache.Read(0, buf, 25));
  EXPECT_EQ("ABCDEFXXXXYYYY45678901234", std::string_view(buf, 25));
  EXPECT_EQ("0123456789012345678901234", std::string_view(file_buffer, 25));
  EXPECT_EQ(tkrzw::Status::SUCCESS, cache.Flush());
  EXPECT_EQ("ABCDEFXXXXYYYY45678901234", std::string_view(file_buffer, 25));
  EXPECT_EQ(14, cache.GetRegionSize());
  cache.SetRegionSize(105);
  EXPECT_EQ(105, cache.GetRegionSize());
  cache.Clear();
  EXPECT_EQ(105, cache.GetRegionSize());
  for (int32_t i = 0; i < file_size; i++) {
    file_buffer[i] = 'x';
  }
  EXPECT_EQ(tkrzw::Status::SUCCESS, cache.Write(0, "0000000000", 8));
  EXPECT_EQ(tkrzw::Status::SUCCESS, cache.Write(16, "0000000000", 8));
  EXPECT_EQ(tkrzw::Status::SUCCESS, cache.Write(32, "0000000000", 8));
  EXPECT_EQ(tkrzw::Status::SUCCESS, cache.Flush(22, 1));
  EXPECT_EQ("xxxxxxxxxxxxxxxx00000000xxxxxxxxxxxxxxxx",
            std::string_view(file_buffer, 40));
  cache.Clear();
  EXPECT_EQ(tkrzw::Status::SUCCESS, cache.Write(0, "1111111111", 8));
  EXPECT_EQ(tkrzw::Status::SUCCESS, cache.Write(16, "1111111111", 8));
  EXPECT_EQ(tkrzw::Status::SUCCESS, cache.Write(32, "1111111111", 8));
  EXPECT_EQ(tkrzw::Status::SUCCESS, cache.Flush(0, 1));
  EXPECT_EQ("11111111xxxxxxxx00000000xxxxxxxxxxxxxxxx",
            std::string_view(file_buffer, 40));
  cache.Clear();
  EXPECT_EQ(tkrzw::Status::SUCCESS, cache.Write(0, "2222222222", 8));
  EXPECT_EQ(tkrzw::Status::SUCCESS, cache.Write(16, "2222222222", 8));
  EXPECT_EQ(tkrzw::Status::SUCCESS, cache.Write(32, "2222222222", 8));
  EXPECT_EQ(tkrzw::Status::SUCCESS, cache.Flush(5, 25));
  EXPECT_EQ("22222222xxxxxxxx22222222xxxxxxxxxxxxxxxx",
            std::string_view(file_buffer, 40));
  cache.Clear();
  EXPECT_EQ(tkrzw::Status::SUCCESS, cache.Write(0, "3333333333", 8));
  EXPECT_EQ(tkrzw::Status::SUCCESS, cache.Write(16, "3333333333", 8));
  EXPECT_EQ(tkrzw::Status::SUCCESS, cache.Write(32, "3333333333", 8));
  EXPECT_EQ(tkrzw::Status::SUCCESS, cache.Flush(0, 0));
  EXPECT_EQ("33333333xxxxxxxx33333333xxxxxxxx33333333",
            std::string_view(file_buffer, 40));
  cache.Clear();
  for (int32_t i = 0; i < file_size; i++) {
    file_buffer[i] = '0' + i % 10;
  }
  auto task = [&](int32_t id) {
                std::mt19937 mt(id);
                std::uniform_int_distribution<int32_t> dist(0, tkrzw::INT32MAX);
                char buf[256];
                for (int32_t i = 0; i < 2000; i++) {
                  const int64_t off = dist(mt) % file_size;
                  const int64_t size = std::min<int64_t>(dist(mt) % 50, file_size - off);
                  if (dist(mt) % 2 == 0) {
                    for (int32_t j = 0; j < size; j++) {
                      buf[j] = 'a' + (off+ j) % 10;
                    }
                    EXPECT_EQ(tkrzw::Status::SUCCESS, cache.Write(off, buf, size));
                  } else {
                    EXPECT_EQ(tkrzw::Status::SUCCESS, cache.Read(off, buf, size));
                    for (int32_t j = 0; j < size; j++) {
                      const int32_t est_number = '0' + (off + j) % 10;
                      const int32_t est_alpha = 'a' + (off + j) % 10;
                      EXPECT_TRUE(buf[j] == est_number || buf[j] == est_alpha);
                    }
                  }
                }
              };
  task(0);
  EXPECT_EQ(tkrzw::Status::SUCCESS, cache.Flush());
  std::vector<std::thread> threads;
  std::vector<std::unordered_map<std::string, std::string>> maps(num_threads);
  for (int32_t i = 0; i < num_threads; i++) {
    threads.emplace_back(std::thread(task, i));
  }
  for (auto& thread : threads) {
    thread.join();
  }
  EXPECT_EQ(tkrzw::Status::SUCCESS, cache.Flush());
}

// END OF FILE
