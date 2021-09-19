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

namespace bustub {

LRUReplacer::LRUReplacer(size_t num_pages) {
}

LRUReplacer::~LRUReplacer() = default;

bool LRUReplacer::Victim(frame_id_t *frame_id) { 
    if (this->replacer_.size() == 0){
        return false;
    }
    *frame_id = this->replacer_.front();
    this->replacer_.pop_front();
    return true; 
}

void LRUReplacer::Pin(frame_id_t frame_id) {
    auto it = std::find(this->replacer_.begin(), this->replacer_.end(), frame_id);
    if (it != this->replacer_.end()) {
        this->replacer_.erase(it);
    }
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
    auto it = std::find(this->replacer_.begin(), this->replacer_.end(), frame_id);
    if (it == this->replacer_.end()) {
        this->replacer_.push_back(frame_id);
    }
    
}

size_t LRUReplacer::Size() { return this->replacer_.size(); }

}  // namespace bustub
