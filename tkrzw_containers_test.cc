/*************************************************************************************************
 * Tests for tkrzw_containers.h
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

#include "tkrzw_containers.h"
#include "tkrzw_lib_common.h"
#include "tkrzw_thread_util.h"
#include "tkrzw_str_util.h"

using namespace testing;

// Main routine
int main(int argc, char** argv) {
  InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

TEST(LinkedHashMapTest, Basic) {
  typedef tkrzw::LinkedHashMap<std::string, std::string> MAP;
  auto serialize = [](const MAP& map) {
    std::vector<std::string> records;
    for (const auto& rec : map) {
      records.emplace_back(tkrzw::StrCat(rec.key, ":", rec.value));
    }
    return records;
  };
  MAP map(3);
  for (int32_t i = 1; i <= 6; i++) {
    const std::string key = tkrzw::ToString(i);
    const std::string value = tkrzw::ToString(i * i);
    const MAP::Record* rec = map.Set(key, value);
    ASSERT_NE(nullptr, rec);
    EXPECT_EQ(value, rec->value);
  }
  EXPECT_EQ(6, map.size());
  EXPECT_DOUBLE_EQ(2.0, map.load_factor());
  EXPECT_THAT(serialize(map), ElementsAre("1:1", "2:4", "3:9", "4:16", "5:25", "6:36"));
  for (int32_t i = 1; i <= 6; i++) {
    const std::string key = tkrzw::ToString(i);
    const std::string value = tkrzw::ToString(i * i);
    const MAP::Record* rec = map.Get(key);
    ASSERT_NE(nullptr, rec);
    EXPECT_EQ(value, rec->value);
  }
  map.rehash(5);
  EXPECT_THAT(serialize(map), ElementsAre("1:1", "2:4", "3:9", "4:16", "5:25", "6:36"));
  for (int32_t i = 1; i <= 6; i++) {
    const std::string key = tkrzw::ToString(i);
    const std::string value = tkrzw::ToString(i * i);
    const MAP::Record* rec = map.Get(key);
    ASSERT_NE(nullptr, rec);
    EXPECT_EQ(value, rec->value);
  }
  {
    MAP::Iterator it = map.find("3");
    ASSERT_NE(it, map.end());
    EXPECT_EQ("3", it->key);
    EXPECT_EQ("9", it->value);
    EXPECT_EQ("3", (it--)->key);
    ASSERT_NE(it, map.end());
    EXPECT_EQ("2", it->key);
    EXPECT_EQ("1", (--it)->key);
    EXPECT_EQ("1", (it++)->key);
    ASSERT_NE(it, map.end());
    EXPECT_EQ("2", it->key);
    EXPECT_EQ("3", (++it)->key);
  }
  {
    const MAP& const_map = map;
    MAP::ConstIterator it = const_map.find("3");
    ASSERT_NE(it, map.end());
    EXPECT_EQ("3", it->key);
    EXPECT_EQ("9", it->value);
    EXPECT_EQ("3", (it--)->key);
    ASSERT_NE(it, map.end());
    EXPECT_EQ("2", it->key);
    EXPECT_EQ("1", (--it)->key);
    EXPECT_EQ("1", (it++)->key);
    ASSERT_NE(it, map.end());
    EXPECT_EQ("2", it->key);
    EXPECT_EQ("3", (++it)->key);
    it = const_map.begin();
    EXPECT_EQ("1", (it++)->key);
    EXPECT_EQ("2", (it++)->key);
    EXPECT_TRUE(it != map.end());
    EXPECT_FALSE(it == map.end());
    EXPECT_TRUE(map.end() != it);
    EXPECT_FALSE(map.end() == it);
    it = const_map.find("6");
    EXPECT_EQ("6", (it++)->key);
    EXPECT_TRUE(it == map.end());
    EXPECT_FALSE(it != map.end());
    EXPECT_TRUE(map.end() == it);
    EXPECT_FALSE(map.end() != it);
  }
  EXPECT_NE(nullptr, map.Get("3", MAP::MOVE_LAST));
  EXPECT_THAT(serialize(map), ElementsAre("1:1", "2:4", "4:16", "5:25", "6:36", "3:9"));
  MAP::Iterator it = map.find("3");
  EXPECT_EQ("6", (--it)->key);
  EXPECT_EQ("3", (++it)->key);
  EXPECT_EQ(map.end(), ++it);
  EXPECT_TRUE(map.Remove("6"));
  EXPECT_FALSE(map.Remove("6"));
  EXPECT_TRUE(map.Remove("1"));
  EXPECT_FALSE(map.Remove("1"));
  EXPECT_THAT(serialize(map), ElementsAre("2:4", "4:16", "5:25", "3:9"));
  EXPECT_NE(nullptr, map.Get("5", MAP::MOVE_FIRST));
  EXPECT_NE(nullptr, map.Get("3", MAP::MOVE_FIRST));
  EXPECT_THAT(serialize(map), ElementsAre("3:9", "5:25", "2:4", "4:16"));
  MAP::Record* rec = map.Set("2", "two", false, MAP::MOVE_FIRST);
  ASSERT_NE(nullptr, rec);
  EXPECT_EQ("4", rec->value);
  EXPECT_EQ("4", map.GetSimple("2"));
  rec = map.Set("2", "two", true, MAP::MOVE_FIRST);
  ASSERT_NE(nullptr, rec);
  EXPECT_EQ("two", rec->value);
  EXPECT_EQ("two", map.GetSimple("2"));
  EXPECT_THAT(serialize(map), ElementsAre("2:two", "3:9", "5:25", "4:16"));
  EXPECT_NE(nullptr, map.Get("2", MAP::MOVE_FIRST));
  EXPECT_NE(nullptr, map.Get("4", MAP::MOVE_FIRST));
  EXPECT_THAT(serialize(map), ElementsAre("4:16", "2:two", "3:9", "5:25"));
  EXPECT_NE(nullptr, map.Get("4", MAP::MOVE_LAST));
  EXPECT_NE(nullptr, map.Get("2", MAP::MOVE_LAST));
  EXPECT_THAT(serialize(map), ElementsAre("3:9", "5:25", "4:16", "2:two"));
  EXPECT_NE(nullptr, map.Set("5", "five", true, MAP::MOVE_LAST));
  EXPECT_THAT(serialize(map), ElementsAre("3:9", "4:16", "2:two", "5:five"));
  EXPECT_NE(nullptr, map.Set("3", "three", true, MAP::MOVE_LAST));
  EXPECT_THAT(serialize(map), ElementsAre("4:16", "2:two", "5:five", "3:three"));
  EXPECT_EQ("16", map.GetSimple("4", "", MAP::MOVE_LAST));
  EXPECT_EQ("five", map.GetSimple("5", "", MAP::MOVE_FIRST));
  EXPECT_THAT(serialize(map), ElementsAre("5:five", "2:two", "3:three", "4:16"));
  EXPECT_TRUE(map.Remove("5"));
  EXPECT_TRUE(map.Remove("4"));
  EXPECT_THAT(serialize(map), ElementsAre("2:two", "3:three"));
  EXPECT_EQ("2", map.front().key);
  EXPECT_EQ("3", map.back().key);
  MAP new_map(3);
  new_map["red"] = "aka";
  new_map["green"] = "midori";
  new_map["blue"] = "ao";
  EXPECT_EQ("", new_map["orange"]);
  EXPECT_EQ(4, new_map.size());
  for (MAP::Iterator new_it = new_map.begin(); new_it != new_map.end(); ++new_it) {
    new_it->value = tkrzw::StrUpperCase(new_it->value);
  }
  EXPECT_THAT(serialize(new_map), ElementsAre("red:AKA", "green:MIDORI", "blue:AO", "orange:"));
  EXPECT_TRUE(new_map.Remove("orange"));
  rec = new_map.Migrate("red", &map, MAP::MOVE_FIRST);
  ASSERT_NE(nullptr, rec);
  EXPECT_EQ("AKA", rec->value);
  EXPECT_EQ(nullptr, new_map.Migrate("red", &map, MAP::MOVE_FIRST));
  rec = new_map.Migrate("blue", &map, MAP::MOVE_LAST);
  ASSERT_NE(nullptr, rec);
  EXPECT_EQ("AO", rec->value);
  EXPECT_EQ(1, new_map.size());
  EXPECT_EQ(4, map.size());
  EXPECT_THAT(serialize(new_map), ElementsAre("green:MIDORI"));
  EXPECT_THAT(serialize(map), ElementsAre("red:AKA", "2:two", "3:three", "blue:AO"));
  EXPECT_NE(nullptr, map.Migrate("2", &new_map, MAP::MOVE_CURRENT));
  EXPECT_NE(nullptr, map.Migrate("3", &new_map, MAP::MOVE_CURRENT));
  EXPECT_THAT(serialize(new_map), ElementsAre("green:MIDORI", "2:two", "3:three"));
  EXPECT_THAT(serialize(map), ElementsAre("red:AKA", "blue:AO"));
  EXPECT_EQ("AKA", map.GetSimple("red"));
  auto insert_res = map.insert(MAP::Record("orange", "daidai"));
  EXPECT_EQ("orange", insert_res.first->key);
  EXPECT_EQ("daidai", insert_res.first->value);
  EXPECT_TRUE(insert_res.second);
  insert_res = map.insert(MAP::Record("orange", "mikan"));
  EXPECT_EQ("orange", insert_res.first->key);
  EXPECT_EQ("daidai", insert_res.first->value);
  EXPECT_FALSE(insert_res.second);
  EXPECT_THAT(serialize(map), ElementsAre("red:AKA", "blue:AO", "orange:daidai"));
  EXPECT_NE(nullptr, map.Get("orange"));
  EXPECT_NE(nullptr, map.Get("blue"));
  EXPECT_NE(nullptr, map.Get("red"));
  EXPECT_EQ(3, map.size());
  EXPECT_EQ(1, map.erase("blue"));
  EXPECT_EQ(0, map.erase("blue"));
  EXPECT_THAT(serialize(map), ElementsAre("red:AKA", "orange:daidai"));
  EXPECT_EQ(2, map.size());
  EXPECT_FALSE(map.empty());
  map.clear();
  EXPECT_EQ(0, map.size());
  EXPECT_TRUE(map.empty());
  EXPECT_THAT(serialize(map), ElementsAre());
  EXPECT_EQ("*", map.GetSimple("red", "*"));
}

TEST(LinkedHashMapTest, Random) {
  constexpr int32_t num_iterations = 10000;
  typedef tkrzw::LinkedHashMap<std::string, std::string> MAP;
  MAP map1(num_iterations), map2(num_iterations);
  MAP::Iterator it = map1.begin();
  std::mt19937 mt(1);
  std::uniform_int_distribution<int32_t> map_dist(0, 1);
  std::uniform_int_distribution<int32_t> key_dist(1, num_iterations);
  std::uniform_int_distribution<int32_t> op_dist(0, INT32_MAX);
  std::uniform_int_distribution<int32_t> move_dist(MAP::MOVE_CURRENT, MAP::MOVE_LAST);
  for (int32_t i = 0; i < num_iterations; i++) {
    const std::string& key = tkrzw::ToString(key_dist(mt));
    const std::string& value = tkrzw::ToString(i);
    MAP::MoveMode move_mode = MAP::MoveMode(move_dist(mt));
    MAP* src_map = &map1;
    MAP* trg_map = &map2;
    if (map_dist(mt) == 0) {
      std::swap(src_map, trg_map);
    }
    if (op_dist(mt) % num_iterations / 4 == 0) {
      src_map->clear();
    } else if (op_dist(mt) % 10 == 0) {
      src_map->Remove(key);
    } else if (op_dist(mt) % 3 == 0) {
      src_map->Migrate(key, trg_map, move_mode);
    } else if (op_dist(mt) % 3 == 0) {
      src_map->Get(key, move_mode);
    } else if (op_dist(mt) % 3 == 0) {
      it = map1.find(key);
    } else if (op_dist(mt) % 3 == 0) {
      if (it != map1.end()) {
        if (op_dist(mt) % 2 == 0) {
          ++it;
        } else {
          it++;
        }
      }
    } else {
      src_map->Set(key, value, true, move_mode);
    }
  }
  for (MAP* map : {&map1, &map2}) {
    for (const auto& rec : *map) {
      EXPECT_EQ(rec.value, map->GetSimple(rec.key));
    }
  }
}

TEST(LRUCacheTest, Basic) {
  tkrzw::LRUCache<int32_t> cache(5);
  for (int32_t i = 0; i < 8; i++) {
    EXPECT_EQ(i, *cache.Add(i, new int32_t(i)));
    if (i < 5) {
      EXPECT_FALSE(cache.IsSaturated());
    } else {
      EXPECT_TRUE(cache.IsSaturated());
    }
  }
  EXPECT_EQ(8, cache.Size());
  for (int32_t i = 0; i < 5; i++) {
    EXPECT_EQ(i, *cache.Get(i));
  }
  auto iter = cache.MakeIterator();
  int64_t it_id = -1;
  std::shared_ptr<int32_t> it_value;
  std::vector<int32_t> it_keys;
  while ((it_value = iter.Get(&it_id)) != nullptr) {
    EXPECT_EQ(it_id, *it_value);
    it_keys.emplace_back(it_id);
    iter.Next();
  }
  EXPECT_THAT(it_keys, UnorderedElementsAre(0, 1, 2, 3, 4, 5, 6, 7));
  std::vector<int32_t> values;
  while (!cache.IsEmpty()) {
    int64_t id = -1;
    std::shared_ptr<int32_t> value = cache.RemoveLRU(&id);
    EXPECT_EQ(id, *value);
    values.emplace_back(*value);
  }
  EXPECT_THAT(values, ElementsAre(5, 6, 7, 0, 1, 2, 3, 4));
  EXPECT_EQ(34, *cache.Add(12, new int32_t(34)));
  EXPECT_EQ(78, *cache.Add(56, new int32_t(78)));
  EXPECT_EQ(2, cache.Size());
  it_keys.clear();
  iter = cache.MakeIterator();
  while ((it_value = iter.Get(&it_id)) != nullptr) {
    it_keys.emplace_back(it_id);
    iter.Next();
  }
  EXPECT_THAT(it_keys, UnorderedElementsAre(12, 56));
  values.clear();
  while (!cache.IsEmpty()) {
    int64_t id = -1;
    std::shared_ptr<int32_t> value = cache.RemoveLRU(&id);
    values.emplace_back(*value);
  }
  EXPECT_THAT(values, ElementsAre(34, 78));
  cache.Add(1, new int32_t(111));
  cache.Add(2, new int32_t(222));
  EXPECT_EQ(111, *cache.Get(1));
  cache.Remove(1);
  cache.Remove(2);
  EXPECT_EQ(0, cache.Size());
  EXPECT_EQ(nullptr, cache.Get(1));
  EXPECT_EQ(nullptr, cache.Get(2));
  cache.Add(1, new int32_t(111));
  cache.Add(2, new int32_t(222));
  EXPECT_EQ(222, *cache.Get(2));
  EXPECT_EQ(2, cache.Size());
  cache.Clear();
  EXPECT_EQ(0, cache.Size());
  EXPECT_EQ(nullptr, cache.Get(1));
  EXPECT_EQ(nullptr, cache.Get(2));
  cache.Add(1, new int32_t(111));
  int64_t removed_id = 0;
  std::shared_ptr<int32_t> removed_value = cache.RemoveLRU(&removed_id);
  EXPECT_EQ(1, removed_id);
  EXPECT_EQ(111, *removed_value);
  EXPECT_TRUE(cache.IsEmpty());
  cache.GiveBack(1, std::move(removed_value));
  EXPECT_FALSE(cache.IsEmpty());
  EXPECT_EQ(111, *cache.Get(1));
}

TEST(DoubleLRUCacheTest, Basic) {
  tkrzw::DoubleLRUCache<int32_t> cache(2, 5);
  for (int32_t i = 0; i < 8; i++) {
    EXPECT_EQ(i, *cache.Add(i, new int32_t(i)));
    if (i < 5) {
      EXPECT_FALSE(cache.IsSaturated());
    } else {
      EXPECT_TRUE(cache.IsSaturated());
    }
  }
  EXPECT_EQ(8, cache.Size());
  for (int32_t i = 0; i < 5; i++) {
    EXPECT_EQ(i, *cache.Get(i, i < 4));
  }
  EXPECT_EQ(8, cache.Size());
  auto iter = cache.MakeIterator();
  int64_t it_id = -1;
  std::shared_ptr<int32_t> it_value;
  std::vector<int32_t> it_keys;
  while ((it_value = iter.Get(&it_id)) != nullptr) {
    EXPECT_EQ(it_id, *it_value);
    it_keys.emplace_back(it_id);
    iter.Next();
  }
  EXPECT_THAT(it_keys, UnorderedElementsAre(0, 1, 2, 3, 4, 5, 6, 7));
  std::vector<int32_t> values;
  while (!cache.IsEmpty()) {
    int64_t id = -1;
    std::shared_ptr<int32_t> value = cache.RemoveLRU(&id);
    EXPECT_EQ(id, *value);
    values.emplace_back(*value);
  }
  EXPECT_THAT(values, ElementsAre(5, 6, 7, 0, 1, 4, 2, 3));
  EXPECT_EQ(34, *cache.Add(12, new int32_t(34)));
  EXPECT_EQ(78, *cache.Add(56, new int32_t(78)));
  EXPECT_EQ(2, cache.Size());
  it_keys.clear();
  iter = cache.MakeIterator();
  while ((it_value = iter.Get(&it_id)) != nullptr) {
    it_keys.emplace_back(it_id);
    iter.Next();
  }
  EXPECT_THAT(it_keys, UnorderedElementsAre(12, 56));
  values.clear();
  while (!cache.IsEmpty()) {
    int64_t id = -1;
    std::shared_ptr<int32_t> value = cache.RemoveLRU(&id);
    values.emplace_back(*value);
  }
  EXPECT_THAT(values, ElementsAre(34, 78));
  cache.Add(1, new int32_t(111));
  cache.Add(2, new int32_t(222));
  EXPECT_EQ(111, *cache.Get(1, true));
  cache.Remove(1);
  cache.Remove(2);
  EXPECT_EQ(0, cache.Size());
  EXPECT_EQ(nullptr, cache.Get(1, true));
  EXPECT_EQ(nullptr, cache.Get(2, true));
  cache.Add(1, new int32_t(111));
  cache.Add(2, new int32_t(222));
  EXPECT_EQ(222, *cache.Get(2, true));
  EXPECT_EQ(2, cache.Size());
  cache.Clear();
  EXPECT_EQ(0, cache.Size());
  EXPECT_EQ(nullptr, cache.Get(1, true));
  EXPECT_EQ(nullptr, cache.Get(2, true));
  cache.Add(1, new int32_t(111));
  int64_t removed_id = 0;
  std::shared_ptr<int32_t> removed_value = cache.RemoveLRU(&removed_id);
  EXPECT_EQ(1, removed_id);
  EXPECT_EQ(111, *removed_value);
  EXPECT_TRUE(cache.IsEmpty());
  cache.GiveBack(1, std::move(removed_value));
  EXPECT_FALSE(cache.IsEmpty());
  EXPECT_EQ(111, *cache.Get(1, true));
}

TEST(AtomicSetTest, Basic) {
  tkrzw::AtomicSet<std::string> set;
  EXPECT_TRUE(set.IsEmpty());
  EXPECT_EQ("", set.Pop());
  EXPECT_FALSE(set.Check("3"));
  EXPECT_TRUE(set.Insert("3"));
  EXPECT_FALSE(set.Insert("3"));
  EXPECT_TRUE(set.Check("3"));
  EXPECT_FALSE(set.IsEmpty());
  EXPECT_TRUE(set.Insert("1"));
  const std::string value("2");
  EXPECT_TRUE(set.Insert(value));
  EXPECT_FALSE(set.IsEmpty());
  EXPECT_EQ("1", set.Pop());
  EXPECT_FALSE(set.IsEmpty());
  EXPECT_EQ("2", set.Pop());
  EXPECT_EQ("3", set.Pop());
  EXPECT_TRUE(set.IsEmpty());
  EXPECT_EQ("", set.Pop());
  set.Insert("4");
  EXPECT_FALSE(set.IsEmpty());
  set.Clear();
  EXPECT_TRUE(set.IsEmpty());
  EXPECT_EQ("", set.Pop());
  EXPECT_TRUE(set.Insert("1"));
  EXPECT_FALSE(set.IsEmpty());
  EXPECT_TRUE(set.Insert("2"));
  EXPECT_TRUE(set.Remove("1"));
  EXPECT_FALSE(set.IsEmpty());
  EXPECT_TRUE(set.Remove("2"));
  EXPECT_TRUE(set.IsEmpty());
  EXPECT_FALSE(set.Remove("2"));
}

TEST(MiscTest, HeapByCost) {
  constexpr size_t capacity = 5;
  std::vector<std::pair<int32_t, int32_t>> heap;
  int32_t id = 0;
  for (int32_t cost = 0; cost < 10; cost++) {
    tkrzw::HeapByCostAdd(cost, id++, capacity, &heap);
    tkrzw::HeapByCostAdd(cost, id++, capacity, &heap);
  }
  tkrzw::HeapByCostFinish(&heap);
  const std::vector<std::pair<int32_t, int32_t>> expected = {
    {0, 0}, {0, 1}, {1, 2}, {1, 3}, {2, 4}};
  EXPECT_THAT(heap, ElementsAreArray(expected));
}

// END OF FILE
