//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.cpp
//
// Identification: src/buffer/lru_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_replacer.h"
#include <algorithm>
#include <iostream>

namespace bustub {

LRUReplacer::LRUReplacer(size_t num_pages) {}

LRUReplacer::~LRUReplacer() = default;

bool LRUReplacer::Victim(frame_id_t *frame_id) {
  std::lock_guard<std::mutex> guard(replacer_mutex_);
  if (replacer_.empty()) {
    return false;
  }
  *frame_id = replacer_.back();
  replacer_map_.erase(*frame_id);
  replacer_.pop_back();
  return true;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  std::lock_guard<std::mutex> guard(replacer_mutex_);
  auto it = replacer_map_.find(frame_id);
  if (it != replacer_map_.end()) {
    replacer_.erase(it->second);
    replacer_map_.erase(it);
  }
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  std::lock_guard<std::mutex> guard(replacer_mutex_);
  auto it = replacer_map_.find(frame_id);
  if (it == replacer_map_.end()) {
    replacer_.push_front(frame_id);
    replacer_map_[frame_id] = replacer_.begin();
  }
}

size_t LRUReplacer::Size() { return replacer_.size(); }

}  // namespace bustub
