//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "container/hash/extendible_hash_table.h"

namespace bustub {

template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_TYPE::ExtendibleHashTable(const std::string &name, BufferPoolManager *buffer_pool_manager,
                                     const KeyComparator &comparator, HashFunction<KeyType> hash_fn)
    : buffer_pool_manager_(buffer_pool_manager), comparator_(comparator), hash_fn_(std::move(hash_fn)) {
  //  implement me!
  // Create a directory page using the buffer pool
  // Initially the global depth is 0, there is no bucket.
  table_latch_.WLock();
  auto directory_page = reinterpret_cast<HashTableDirectoryPage *>(
      buffer_pool_manager_->NewPage(&directory_page_id_, nullptr)->GetData());
  page_id_t bucket_page_id = INVALID_PAGE_ID;
  buffer_pool_manager_->NewPage(&bucket_page_id, nullptr);
  directory_page->SetBucketPageId(0, bucket_page_id);
  directory_page->SetLocalDepth(0, 0);
  table_latch_.WUnlock();
  buffer_pool_manager_->UnpinPage(directory_page_id_, true);
  buffer_pool_manager_->UnpinPage(bucket_page_id, false);
}

/*****************************************************************************
 * HELPERS
 *****************************************************************************/
/**
 * Hash - simple helper to downcast MurmurHash's 64-bit hash to 32-bit
 * for extendible hashing.
 *
 * @param key the key to hash
 * @return the downcasted 32-bit hash
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
uint32_t HASH_TABLE_TYPE::Hash(KeyType key) {
  return static_cast<uint32_t>(hash_fn_.GetHash(key));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
inline uint32_t HASH_TABLE_TYPE::KeyToDirectoryIndex(KeyType key, HashTableDirectoryPage *dir_page) {
  uint32_t hash_val = Hash(key);
  return hash_val & dir_page->GetGlobalDepthMask();
}

template <typename KeyType, typename ValueType, typename KeyComparator>
inline uint32_t HASH_TABLE_TYPE::KeyToPageId(KeyType key, HashTableDirectoryPage *dir_page) {
  uint32_t bucket_index = KeyToDirectoryIndex(key, dir_page);
  return dir_page->GetBucketPageId(bucket_index);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
HashTableDirectoryPage *HASH_TABLE_TYPE::FetchDirectoryPage() {
  return reinterpret_cast<HashTableDirectoryPage *>(buffer_pool_manager_->FetchPage(directory_page_id_)->GetData());
}

template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_BUCKET_TYPE *HASH_TABLE_TYPE::FetchBucketPage(page_id_t bucket_page_id) {
  return reinterpret_cast<HashTableBucketPage<KeyType, ValueType, KeyComparator> *>(
      buffer_pool_manager_->FetchPage(bucket_page_id)->GetData());
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::PrintTable() {
  printf("=====================Printing Table========================\n");
  HashTableDirectoryPage *directory_page = FetchDirectoryPage();
  for (size_t i = 0; i < directory_page->Size(); i++) {
    auto page = FetchBucketPage(directory_page->GetBucketPageId(i));
    printf("Bucket index: %ld, Bucket page id: %d, number of slots: %d\n", i, directory_page->GetBucketPageId(i),
           page->NumReadable());
    buffer_pool_manager_->UnpinPage(directory_page->GetBucketPageId(i), false);
  }
  printf("============================================================\n");
  buffer_pool_manager_->UnpinPage(directory_page_id_, false);
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::GetValue(Transaction *transaction, const KeyType &key, std::vector<ValueType> *result) {
  table_latch_.RLock();
  // Get bucket id from directory page using ther key
  page_id_t bucket_page_id = KeyToPageId(key, FetchDirectoryPage());
  buffer_pool_manager_->UnpinPage(directory_page_id_, false);
  auto bucket_page = FetchBucketPage(bucket_page_id);
  Page *p = reinterpret_cast<Page *>(bucket_page);
  p->RLatch();
  bool res = bucket_page->GetValue(key, comparator_, result);
  p->RUnlatch();
  buffer_pool_manager_->UnpinPage(bucket_page_id, false);
  table_latch_.RUnlock();
  return res;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Insert(Transaction *transaction, const KeyType &key, const ValueType &value) {
  table_latch_.RLock();

  // std::cout << "Key " << key << "\n";
  // std::cout << "Value " << key << "\n";
  // PrintTable();

  // Get the bucket page based on the key
  page_id_t bucket_page_id = KeyToPageId(key, FetchDirectoryPage());
  auto bucket_page = FetchBucketPage(bucket_page_id);
  Page *p = reinterpret_cast<Page *>(bucket_page);
  p->WLatch();

  // Check if value is there
  std::vector<ValueType> result;
  bucket_page->GetValue(key, comparator_, &result);
  if (std::find(result.begin(), result.end(), value) != result.end()) {
    p->WUnlatch();
    buffer_pool_manager_->UnpinPage(bucket_page_id, false);
    buffer_pool_manager_->UnpinPage(directory_page_id_, false);
    table_latch_.RUnlock();
    return false;
  }

  // Check if the bucket is full, if it is full we need to split the bucket
  if (bucket_page->IsFull()) {
    p->WUnlatch();
    buffer_pool_manager_->UnpinPage(bucket_page_id, false);
    buffer_pool_manager_->UnpinPage(directory_page_id_, false);
    table_latch_.RUnlock();
    table_latch_.WLock();
    return SplitInsert(transaction, key, value);
  }
  bool res = bucket_page->Insert(key, value, comparator_);
  p->WUnlatch();
  buffer_pool_manager_->UnpinPage(bucket_page_id, true);
  buffer_pool_manager_->UnpinPage(directory_page_id_, false);
  table_latch_.RUnlock();
  return res;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::SplitInsert(Transaction *transaction, const KeyType &key, const ValueType &value) {
  HashTableDirectoryPage *directory_page = FetchDirectoryPage();
  uint32_t bucket_index = KeyToDirectoryIndex(key, directory_page);
  uint32_t depth = directory_page->GetLocalDepth(bucket_index);
  page_id_t bucket_page_id = KeyToPageId(key, directory_page);
  auto bucket_page = FetchBucketPage(bucket_page_id);
  Page *p = reinterpret_cast<Page *>(bucket_page);
  p->WLatch();

  // Double check if the value is there and if the bucket is full
  std::vector<ValueType> result;
  bucket_page->GetValue(key, comparator_, &result);
  if (std::find(result.begin(), result.end(), value) != result.end()) {
    p->WUnlatch();
    buffer_pool_manager_->UnpinPage(bucket_page_id, false);
    buffer_pool_manager_->UnpinPage(directory_page_id_, false);
    table_latch_.WUnlock();
    return false;
  }

  if (!bucket_page->IsFull()) {
    bool res = bucket_page->Insert(key, value, comparator_);
    p->WUnlatch();
    buffer_pool_manager_->UnpinPage(bucket_page_id, false);
    buffer_pool_manager_->UnpinPage(directory_page_id_, false);
    table_latch_.WUnlock();
    return res;
  }

  if (directory_page->GetGlobalDepth() == depth) {
    p->WUnlatch();
    buffer_pool_manager_->UnpinPage(bucket_page_id, false);
    // Need to expand the entire table;
    // Direct the new indexes to the corresponding bucket;
    for (size_t index = 0; index < directory_page->Size(); index++) {
      size_t new_index = index | (1 << directory_page->GetGlobalDepth());
      directory_page->SetBucketPageId(new_index, directory_page->GetBucketPageId(index));
      directory_page->SetLocalDepth(new_index, directory_page->GetLocalDepth(index));
    }
    directory_page->IncrGlobalDepth();
    buffer_pool_manager_->UnpinPage(directory_page_id_, true);
    return SplitInsert(transaction, key, value);
  }
  // Find the directory indexes pointing at the old buckets
  // Split the current bucket into two buckets by creating a new bucket page;
  // Point the directory indexes to the right buckets
  // Increase local depth of the directory indexes
  // Move the elements to the corresbonding buckets
  // Try Inserting again

  uint32_t old_identifier = bucket_index & directory_page->GetLocalDepthMask(bucket_index);
  uint32_t new_identifier = old_identifier | (1 << depth);
  // Creating new bucket
  page_id_t new_bucket_page_id = INVALID_PAGE_ID;
  auto new_bucket_page = reinterpret_cast<HashTableBucketPage<KeyType, ValueType, KeyComparator> *>(
      buffer_pool_manager_->NewPage(&new_bucket_page_id, nullptr)->GetData());
  Page *new_p = reinterpret_cast<Page *>(new_bucket_page);
  new_p->WLatch();
  // Redirect indexes and update depth
  directory_page->IncrLocalDepth(old_identifier);
  directory_page->IncrLocalDepth(new_identifier);
  for (uint32_t index = 0; index < directory_page->Size(); index++) {
    uint32_t bucket_index = index & directory_page->GetLocalDepthMask(new_identifier);
    if (bucket_index == new_identifier) {
      directory_page->SetLocalDepth(index, directory_page->GetLocalDepth(new_identifier));
      directory_page->SetBucketPageId(index, new_bucket_page_id);
    }
    if (bucket_index == old_identifier) {
      directory_page->SetLocalDepth(index, directory_page->GetLocalDepth(new_identifier));
    }
  }

  // Moving the elements
  for (uint32_t index = 0; index < BUCKET_ARRAY_SIZE; index++) {
    if (!bucket_page->IsOccupied(index)) {
      break;
    }

    if (bucket_page->IsReadable(index)) {
      KeyType curr_key = bucket_page->KeyAt(index);
      uint32_t identifier = (Hash(curr_key) & directory_page->GetLocalDepthMask(new_identifier));

      if (identifier == new_identifier) {
        new_bucket_page->Insert(curr_key, bucket_page->ValueAt(index), comparator_);
        bucket_page->RemoveAt(index);
      }
    }
  }
  p->WUnlatch();
  new_p->WUnlatch();
  buffer_pool_manager_->UnpinPage(directory_page_id_, true);
  buffer_pool_manager_->UnpinPage(bucket_page_id, true);
  buffer_pool_manager_->UnpinPage(new_bucket_page_id, true);
  table_latch_.WUnlock();
  return Insert(transaction, key, value);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Remove(Transaction *transaction, const KeyType &key, const ValueType &value) {
  table_latch_.RLock();
  HashTableDirectoryPage *directory_page = FetchDirectoryPage();
  uint32_t bucket_index = KeyToDirectoryIndex(key, directory_page);
  page_id_t bucket_page_id = KeyToPageId(key, directory_page);
  auto bucket_page = FetchBucketPage(bucket_page_id);

  // Check if the value is there.
  Page *p = reinterpret_cast<Page *>(bucket_page);
  p->WLatch();
  std::vector<ValueType> result;
  bucket_page->GetValue(key, comparator_, &result);
  if (std::find(result.begin(), result.end(), value) == result.end()) {
    p->WUnlatch();
    buffer_pool_manager_->UnpinPage(bucket_page_id, false);
    buffer_pool_manager_->UnpinPage(directory_page_id_, false);
    table_latch_.RUnlock();
    return false;
  }

  if (bucket_page->Remove(key, value, comparator_)) {
    if (directory_page->GetLocalDepth(bucket_index) > 0) {
      p->WUnlatch();
      buffer_pool_manager_->UnpinPage(directory_page_id_, false);
      buffer_pool_manager_->UnpinPage(bucket_page_id, true);
      table_latch_.RUnlock();
      table_latch_.WLock();
      Merge(transaction, key, value);
      table_latch_.WUnlock();
      return true;
    }

    buffer_pool_manager_->UnpinPage(directory_page_id_, false);
    buffer_pool_manager_->UnpinPage(bucket_page_id, true);
    p->WUnlatch();
    table_latch_.RUnlock();
    return true;
  }

  buffer_pool_manager_->UnpinPage(directory_page_id_, false);
  buffer_pool_manager_->UnpinPage(bucket_page_id, true);
  p->WUnlatch();
  table_latch_.RUnlock();
  return false;
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::Merge(Transaction *transaction, const KeyType &key, const ValueType &value) {
  HashTableDirectoryPage *directory_page = FetchDirectoryPage();

  // Check global depth is larger than 0
  if (directory_page->GetGlobalDepth() == 0) {
    buffer_pool_manager_->UnpinPage(directory_page_id_, false);
    return;
  }

  uint32_t bucket_index = KeyToDirectoryIndex(key, directory_page);
  page_id_t bucket_page_id = KeyToPageId(key, directory_page);

  // Check local depth is larger than 0
  if (directory_page->GetLocalDepth(bucket_index) == 0) {
    buffer_pool_manager_->UnpinPage(directory_page_id_, false);
    return;
  }

  uint32_t split_image_index = directory_page->GetSplitImageIndex(bucket_index);
  page_id_t split_image_id = directory_page->GetBucketPageId(split_image_index);

  // Check if bucket depths are equal
  if (directory_page->GetLocalDepth(bucket_index) != directory_page->GetLocalDepth(split_image_index)) {
    // printf("Depth not is 0\n");
    buffer_pool_manager_->UnpinPage(directory_page_id_, false);
    return;
  }

  auto bucket_page = FetchBucketPage(bucket_page_id);
  auto split_bucket_page = FetchBucketPage(split_image_id);
  Page *p = reinterpret_cast<Page *>(bucket_page);
  Page *split_p = reinterpret_cast<Page *>(split_bucket_page);
  p->RLatch();
  split_p->RLatch();

  // Check if both bucket and image are non-empty
  if (!(bucket_page->IsEmpty() | split_bucket_page->IsEmpty())) {
    // printf("Both buckets non empty\n");
    p->RUnlatch();
    split_p->RUnlatch();
    buffer_pool_manager_->UnpinPage(directory_page_id_, false);
    buffer_pool_manager_->UnpinPage(bucket_page_id, false);
    buffer_pool_manager_->UnpinPage(split_image_id, false);
    return;
  }

  // Update depth
  directory_page->DecrLocalDepth(bucket_index);
  directory_page->DecrLocalDepth(split_image_index);
  uint32_t depth = directory_page->GetLocalDepth(bucket_index);
  uint32_t identifier = bucket_index & directory_page->GetLocalDepthMask(bucket_index);

  // current bucket is empty, point the buckets to splitbucket, delete the empty page
  if (bucket_page->IsEmpty()) {
    for (size_t i = 0; i < BUCKET_ARRAY_SIZE; i++) {
      if ((i & directory_page->GetLocalDepthMask(bucket_index)) == identifier) {
        directory_page->SetLocalDepth(i, depth);
        directory_page->SetBucketPageId(i, split_image_id);
      }
    }
    p->RUnlatch();
    split_p->RUnlatch();
    buffer_pool_manager_->UnpinPage(bucket_page_id, false);
    buffer_pool_manager_->UnpinPage(split_image_id, false);
    // Delete the original page
    buffer_pool_manager_->DeletePage(bucket_page_id);

  } else {
    for (size_t i = 0; i < directory_page->Size(); i++) {
      if ((i & directory_page->GetLocalDepthMask(bucket_index)) == identifier) {
        directory_page->SetLocalDepth(i, depth);
        directory_page->SetBucketPageId(i, bucket_page_id);
      }
    }
    // Delete the original page
    p->RUnlatch();
    split_p->RUnlatch();
    buffer_pool_manager_->UnpinPage(bucket_page_id, false);
    buffer_pool_manager_->UnpinPage(split_image_id, false);
    buffer_pool_manager_->DeletePage(split_image_id);
  }

  // Shrink
  if (directory_page->CanShrink()) {
    directory_page->DecrGlobalDepth();
  }

  buffer_pool_manager_->UnpinPage(directory_page_id_, true);
  Merge(transaction, key, value);
}

/*****************************************************************************
 * GETGLOBALDEPTH - DO NOT TOUCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
uint32_t HASH_TABLE_TYPE::GetGlobalDepth() {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  uint32_t global_depth = dir_page->GetGlobalDepth();
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr));
  table_latch_.RUnlock();
  return global_depth;
}

/*****************************************************************************
 * VERIFY INTEGRITY - DO NOT TOUCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::VerifyIntegrity() {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  dir_page->VerifyIntegrity();
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr));
  table_latch_.RUnlock();
}

/*****************************************************************************
 * TEMPLATE DEFINITIONS - DO NOT TOUCH
 *****************************************************************************/
template class ExtendibleHashTable<int, int, IntComparator>;

template class ExtendibleHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class ExtendibleHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class ExtendibleHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class ExtendibleHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class ExtendibleHashTable<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
