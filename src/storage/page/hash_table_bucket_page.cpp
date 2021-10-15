//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_table_bucket_page.cpp
//
// Identification: src/storage/page/hash_table_bucket_page.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/hash_table_bucket_page.h"
#include "common/logger.h"
#include "common/util/hash_util.h"
#include "storage/index/generic_key.h"
#include "storage/index/hash_comparator.h"
#include "storage/table/tmp_tuple.h"

namespace bustub {

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::GetValue(KeyType key, KeyComparator cmp, std::vector<ValueType> *result) {
  //PrintArray();
  for (size_t bucket_idx = 0; bucket_idx < BUCKET_ARRAY_SIZE; bucket_idx++) {
    if (!IsOccupied(bucket_idx)) {
      break;
    }

    if (IsReadable(bucket_idx)) {
      if (cmp(array_[bucket_idx].first, key) == 0){
        result->push_back(array_[bucket_idx].second);
      }
    }
  }
  return result->size()>0;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::PrintArray() {
  for (size_t bucket_idx = 0; bucket_idx < BUCKET_ARRAY_SIZE; bucket_idx++) {
    if (!IsOccupied(bucket_idx)) {
      break;
    }

    if (IsReadable(bucket_idx)) {
      std::cout << "Slot " << bucket_idx << "\n";
      std::cout << "Key " << array_[bucket_idx].first << "\n";
      std::cout << "Val " << array_[bucket_idx].second << "\n";
    }
  }
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::Insert(KeyType key, ValueType value, KeyComparator cmp) {
  //PrintBucket();
  //PrintArray();
  MappingType map = std::pair(key, value);
  // Check for duplicate pair and find slot at the same time.
  bool find_slot = false;
  bool check_end = false;
  size_t slot = 0;

  for (size_t bucket_idx = 0; bucket_idx < BUCKET_ARRAY_SIZE; bucket_idx++) {
    // Check for occupation
    if (!IsOccupied(bucket_idx)) {
      check_end = true;
    }
    // Check if the slot contains readable pair
    if (IsReadable(bucket_idx)) {
      // Check for identical pair
      MappingType curr_map = array_[bucket_idx];
      if (cmp(curr_map.first, key) == 0 && (curr_map.second == value)) {
        return false;
      }
    } else if (!find_slot) {  // If the slot is empty
      find_slot = true;       // Use this slot
      slot = bucket_idx;
    }
    // If both no more pairs to check and find a slot, end early
    if (check_end && find_slot) {
      break;
    }
  }

  // If no empty slot found (bucket full) return false
  if (!find_slot) {
    return false;
  }

  // Insert the key-val pair at slot bucket_size
  // Set occupied if not, set readable
  array_[slot] = map;
  SetOccupied(slot);
  SetReadable(slot);
  element_num++;
  return true;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::Remove(KeyType key, ValueType value, KeyComparator cmp) {
  bool found = false;
  size_t slot = 0;
  // PrintArray();
  // Go through the bucket to find the key-val pair, stop is unoccupied or found
  for (size_t bucket_idx = 0; bucket_idx < BUCKET_ARRAY_SIZE; bucket_idx++) {
    // Check for occupation
    if (!IsOccupied(bucket_idx)) {
      break;
    }
    // Check if the slot contains readable pair
    if (IsReadable(bucket_idx)) {
      // Check for identical pair
      LOG_INFO("Checking index %lu.", bucket_idx);
      MappingType curr_map = array_[bucket_idx];
      if (cmp(curr_map.first, key) == 0 && (curr_map.second == value)) {
        found = true;
        slot = bucket_idx;
        break;
      }
    }
  }

  // Cannot find key-val pair
  if (!found) {
    LOG_INFO("Cannot Find Pair.");
    return false;
  }

  UnsetReadable(slot);  // remove pair by setting the slot to unreadable
  element_num--;
  return true;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
KeyType HASH_TABLE_BUCKET_TYPE::KeyAt(uint32_t bucket_idx) const {
  return array_[bucket_idx].first;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
ValueType HASH_TABLE_BUCKET_TYPE::ValueAt(uint32_t bucket_idx) const {
  return array_[bucket_idx].second;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::RemoveAt(uint32_t bucket_idx) {
  if (IsReadable(bucket_idx)){
    UnsetReadable(bucket_idx);
    element_num--;
  }
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::IsOccupied(uint32_t bucket_idx) const {
  size_t char_idx = bucket_idx / 8;
  size_t bit_idx = bucket_idx % 8;
  char bucket = occupied_[char_idx];
  return ((bucket >> (7 - bit_idx)) & 1) == 1;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::SetOccupied(uint32_t bucket_idx) {
  size_t char_idx = bucket_idx / 8;
  size_t bit_idx = bucket_idx % 8;
  char bucket = occupied_[char_idx];
  occupied_[char_idx] = bucket|(1 << (7 - bit_idx));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::IsReadable(uint32_t bucket_idx) const {
  size_t char_idx = bucket_idx / 8;
  size_t bit_idx = bucket_idx % 8;
  char bucket = readable_[char_idx];
  return ((bucket >> (7 - bit_idx)) & 1) == 1;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::SetReadable(uint32_t bucket_idx) {
  size_t char_idx = bucket_idx / 8;
  size_t bit_idx = bucket_idx % 8;
  char bucket = readable_[char_idx];
  readable_[char_idx] = bucket|(1 << (7 - bit_idx));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::UnsetReadable(uint32_t bucket_idx) {
  size_t char_idx = bucket_idx / 8;
  size_t bit_idx = bucket_idx % 8;
  char bucket = readable_[char_idx];
  readable_[char_idx] = bucket&(~(1UL << (7 - bit_idx)));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::IsFull() {
  return NumReadable() == BUCKET_ARRAY_SIZE;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
uint32_t HASH_TABLE_BUCKET_TYPE::NumReadable() {
  return element_num;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::IsEmpty() {
  return NumReadable() == 0;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::PrintBucket() {
  uint32_t size = 0;
  uint32_t taken = 0;
  uint32_t free = 0;
  for (size_t bucket_idx = 0; bucket_idx < BUCKET_ARRAY_SIZE; bucket_idx++) {
    if (!IsOccupied(bucket_idx)) {
      break;
    }

    size++;

    if (IsReadable(bucket_idx)) {
      taken++;
    } else {
      free++;
    }
  }

  LOG_INFO("Bucket Capacity: %lu, Size: %u, Taken: %u, Free: %u", BUCKET_ARRAY_SIZE, size, taken, free);
}

// DO NOT REMOVE ANYTHING BELOW THIS LINE
template class HashTableBucketPage<int, int, IntComparator>;

template class HashTableBucketPage<GenericKey<4>, RID, GenericComparator<4>>;
template class HashTableBucketPage<GenericKey<8>, RID, GenericComparator<8>>;
template class HashTableBucketPage<GenericKey<16>, RID, GenericComparator<16>>;
template class HashTableBucketPage<GenericKey<32>, RID, GenericComparator<32>>;
template class HashTableBucketPage<GenericKey<64>, RID, GenericComparator<64>>;

// template class HashTableBucketPage<hash_t, TmpTuple, HashComparator>;

}  // namespace bustub
