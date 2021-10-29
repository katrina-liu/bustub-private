//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// Identifier: Example concurrency test for students to extend
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <algorithm>
// NOLINTNEXTLINE
#include <chrono>
#include <cstdio>
#include <functional>
// NOLINTNEXTLINE
#include <future>
#include <iostream>
// NOLINTNEXTLINE
#include <thread>
#include <vector>

#include "buffer/buffer_pool_manager_instance.h"
#include "common/logger.h"
#include "container/hash/extendible_hash_table.h"
#include "gtest/gtest.h"
#include "murmur3/MurmurHash3.h"

namespace bustub {

// helper function to insert
void InsertHelper(ExtendibleHashTable<int, int, IntComparator> *hash_table, const std::vector<int> &keys, uint64_t tid,
                  __attribute__((unused)) uint64_t thread_itr = 0) {
  for (auto key : keys) {
    int value = key;
    // printf("Inserting %d\n",key);
    hash_table->Insert(nullptr, key, value);
  }
  EXPECT_NE(keys[0], keys[1]);
}

void RemoveHelper(ExtendibleHashTable<int, int, IntComparator> *hash_table, const std::vector<int> &keys, uint64_t tid,
                  __attribute__((unused)) uint64_t thread_itr = 0) {
  for (auto key : keys) {
    int value = key;
    // printf("Removing %d\n",key);
    EXPECT_EQ(hash_table->Remove(nullptr, key, value), true);
  }
}

void RemoveInvalidHelper(ExtendibleHashTable<int, int, IntComparator> *hash_table, const std::vector<int> &keys,
                         uint64_t tid, __attribute__((unused)) uint64_t thread_itr = 0) {
  for (auto key : keys) {
    int value = key;
    // printf("Removing %d\n",key);
    EXPECT_EQ(hash_table->Remove(nullptr, key, value), false);
  }
}

void LookupHelper(ExtendibleHashTable<int, int, IntComparator> *hash_table, const std::vector<int> &keys, uint64_t tid,
                  __attribute__((unused)) uint64_t thread_itr = 0) {
  for (auto key : keys) {
    int value = key;
    std::vector<int> result;
    bool res = hash_table->GetValue(nullptr, key, &result);
    EXPECT_EQ(res, true);
    EXPECT_EQ(result.size(), 1);
    EXPECT_EQ(result[0], value);
  }
}

void ConcurrencyTest1Call() {
  auto *disk_manager = new DiskManager("test.db");
  auto *bpm = new BufferPoolManagerInstance(257, disk_manager);
  ExtendibleHashTable<int, int, IntComparator> hash_table("my_table", bpm, IntComparator(), HashFunction<int>());

  //  Setup keys
  std::vector<int> lookup_keys;
  std::vector<int> insert_keys;
  size_t total_keys = 12345;
  for (size_t i = 1; i <= total_keys; i++) {
    if (i % 2 == 0) {
      lookup_keys.emplace_back(i);
    } else {
      insert_keys.emplace_back(i);
    }
  }
  printf("Fine here.\n");
  InsertHelper(&hash_table, lookup_keys, 1);

  printf("Fine here.\n");

  auto insert_task = [&](int tid) { InsertHelper(&hash_table, insert_keys, tid); };
  auto lookup_task = [&](int tid) { LookupHelper(&hash_table, lookup_keys, tid); };

  std::vector<std::thread> threads;
  std::vector<std::function<void(int)>> tasks;
  tasks.emplace_back(insert_task);
  tasks.emplace_back(insert_task);
  tasks.emplace_back(lookup_task);

  size_t num_threads = 3;
  for (size_t i = 0; i < num_threads; i++) {
    threads.emplace_back(std::thread{tasks[i % tasks.size()], i});
  }
  for (size_t i = 0; i < num_threads; i++) {
    threads[i].join();
  }

  for (auto key : lookup_keys) {
    std::vector<int> result;
    assert(hash_table.GetValue(nullptr, key, &result));
  }

  disk_manager->ShutDown();
  delete disk_manager;
  delete bpm;
  remove("test.db");
  remove("test.log");
}

TEST(HashTableConcurrentTest, ConcurrencyTest1Call) { ConcurrencyTest1Call(); }

TEST(HashTableConcurrentTest, SplitTest) {
  auto *disk_manager = new DiskManager("test.db");
  auto *bpm = new BufferPoolManagerInstance(4, disk_manager);
  ExtendibleHashTable<int, int, IntComparator> hash_table("my_table", bpm, IntComparator(), HashFunction<int>());

  //  Setup keys
  std::vector<int> lookup_keys;
  std::vector<int> insert_keys;
  size_t total_keys = 942;
  for (size_t i = 1; i <= total_keys; i++) {
    if (i % 2 == 0) {
      lookup_keys.emplace_back(i);
    }
    insert_keys.emplace_back(i);
  }
  printf("Fine here.\n");
  InsertHelper(&hash_table, insert_keys, 1);
}

TEST(HashTableConcurrentTest, RemoveTest) {
  auto *disk_manager = new DiskManager("test.db");
  auto *bpm = new BufferPoolManagerInstance(4, disk_manager);
  ExtendibleHashTable<int, int, IntComparator> hash_table("my_table", bpm, IntComparator(), HashFunction<int>());

  //  Setup keys
  std::vector<int> lookup_keys;
  std::vector<int> insert_keys;
  size_t total_keys = 497;
  for (size_t i = 1; i <= total_keys; i++) {
    if (i % 2 == 0) {
      lookup_keys.emplace_back(i);
    }
    insert_keys.emplace_back(i);
  }
  printf("Fine here.\n");
  InsertHelper(&hash_table, insert_keys, 1);
  RemoveHelper(&hash_table, insert_keys, 1);
}

TEST(HashTableConcurrentTest, DoubleInsertTest) {
  auto *disk_manager = new DiskManager("test.db");
  auto *bpm = new BufferPoolManagerInstance(4, disk_manager);
  ExtendibleHashTable<int, int, IntComparator> hash_table("my_table", bpm, IntComparator(), HashFunction<int>());

  //  Setup keys
  std::vector<int> lookup_keys;
  std::vector<int> insert_keys;
  size_t total_keys = 942;
  for (size_t i = 1; i <= total_keys; i++) {
    insert_keys.emplace_back(i);
  }
  for (size_t i = 1; i <= total_keys; i++) {
    insert_keys.emplace_back(i);
  }
  printf("Fine here.\n");
  InsertHelper(&hash_table, insert_keys, 2);
  InsertHelper(&hash_table, insert_keys, 1);
}

TEST(HashTableConcurrentTest, DoubleRemoveTest) {
  auto *disk_manager = new DiskManager("test.db");
  auto *bpm = new BufferPoolManagerInstance(4, disk_manager);
  ExtendibleHashTable<int, int, IntComparator> hash_table("my_table", bpm, IntComparator(), HashFunction<int>());

  //  Setup keys
  std::vector<int> lookup_keys;
  std::vector<int> insert_keys;
  size_t total_keys = 497;
  for (size_t i = 1; i <= total_keys; i++) {
    if (i % 2 == 0) {
      lookup_keys.emplace_back(i);
    }
    insert_keys.emplace_back(i);
  }
  printf("Fine here.\n");
  InsertHelper(&hash_table, insert_keys, 1);
  RemoveHelper(&hash_table, insert_keys, 1);
  RemoveInvalidHelper(&hash_table, insert_keys, 1);
}

}  // namespace bustub
