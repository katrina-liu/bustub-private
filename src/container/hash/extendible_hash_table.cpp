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
  auto directory_page = reinterpret_cast<HashTableDirectoryPage *>(buffer_pool_manager_->NewPage(&directory_page_id_, nullptr)->GetData());
  page_id_t bucket_page_id = INVALID_PAGE_ID;
  reinterpret_cast<HashTableBucketPage<KeyType, ValueType, KeyComparator> *>(
      buffer_pool_manager_->NewPage(&bucket_page_id, nullptr)->GetData());
  directory_page->SetBucketPageId(0, bucket_page_id);
  directory_page->SetLocalDepth(0, 0);
  
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
  return reinterpret_cast<HashTableBucketPage<KeyType, ValueType, KeyComparator> *>(buffer_pool_manager_->FetchPage(bucket_page_id)->GetData());
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::GetValue(Transaction *transaction, const KeyType &key, std::vector<ValueType> *result) {
  // Get bucket id from directory page using the key
  page_id_t bucket_page_id = KeyToPageId(key, FetchDirectoryPage());
  auto bucket_page = FetchBucketPage(bucket_page_id);
  //printf("Get Value Page id: %d\n", bucket_page_id);
  return bucket_page->GetValue(key, comparator_, result);
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Insert(Transaction *transaction, const KeyType &key, const ValueType &value) {
  // std::cout << "Key " << key << "\n";
  // std::cout << "Val " << value << "\n";
  // Get the bucket page based on the key
  page_id_t bucket_page_id = KeyToPageId(key, FetchDirectoryPage());
  auto bucket_page = FetchBucketPage(bucket_page_id);
  //printf("Insert Page id: %d\n", bucket_page_id);
  // Check if the bucket is full, if it is full we need to split the bucket
  if (bucket_page->IsFull()){
    //printf("Bucket page full, split\n");
    return SplitInsert(transaction, key, value);
  } else {
    //printf("Bucket page insert\n");
    return bucket_page->Insert(key, value, comparator_);
  }
  return false;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::SplitInsert(Transaction *transaction, const KeyType &key, const ValueType &value) {
  HashTableDirectoryPage* directory_page = FetchDirectoryPage();
  uint32_t bucket_index = KeyToDirectoryIndex(key, directory_page);
  page_id_t bucket_page_id = KeyToPageId(key, directory_page);
  auto bucket_page = FetchBucketPage(bucket_page_id);
  uint32_t depth = directory_page->GetLocalDepth(bucket_index);
  if (directory_page->GetGlobalDepth() == depth){
    // Need to expand the entire table;
    // Direct the new indexes to the corresponding bucket;
    for (size_t index=0; index<directory_page->Size(); index++){
      size_t new_index = index | (1 << directory_page->GetGlobalDepth());
      directory_page->SetBucketPageId(new_index, directory_page->GetBucketPageId(index));
      directory_page->SetLocalDepth(new_index, directory_page->GetLocalDepth(new_index));
    }
    directory_page->IncrGlobalDepth();
    return SplitInsert(transaction, key, value);
  } else {
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
    


    // Redirect indexes and update depth
    directory_page->IncrLocalDepth(new_identifier);
    for (uint32_t index=0; index < directory_page->Size(); index++){
      if ((index & directory_page->GetLocalDepthMask(new_identifier)) == new_identifier){
        directory_page->SetLocalDepth(index, directory_page->GetLocalDepth(new_identifier));
        directory_page->SetBucketPageId(index, new_bucket_page_id);
      }
    }

    // Moving the elements
    for (uint32_t index=0; index < BUCKET_ARRAY_SIZE; index++){
      if (!bucket_page->IsOccupied(index)){
        break;
      }

      if (bucket_page->IsReadable(index)){
        KeyType curr_key = bucket_page->KeyAt(index);
        if ((Hash(curr_key) & directory_page->GetLocalDepthMask(new_identifier)) == new_identifier){
          new_bucket_page->Insert(curr_key, bucket_page->ValueAt(index), comparator_);
          bucket_page->RemoveAt(index);
        }
      }
    }
    


  }

  return Insert(transaction, key, value);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Remove(Transaction *transaction, const KeyType &key, const ValueType &value) {
  page_id_t bucket_page_id = KeyToPageId(key, FetchDirectoryPage());
  auto bucket_page = FetchBucketPage(bucket_page_id);
  if (bucket_page->Remove(key, value, comparator_)){
    
    if (FetchDirectoryPage()->CanShrink()){
      Merge(transaction, key, value);
    }
    return true;
  }
  return false;
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::Merge(Transaction *transaction, const KeyType &key, const ValueType &value) {
  HashTableDirectoryPage* directory_page = FetchDirectoryPage();
  
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
