//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_instance.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager_instance.h"

#include "common/macros.h"

namespace bustub {

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager,
                                                     LogManager *log_manager)
    : BufferPoolManagerInstance(pool_size, 1, 0, disk_manager, log_manager) {}

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, uint32_t num_instances, uint32_t instance_index,
                                                     DiskManager *disk_manager, LogManager *log_manager)
    : pool_size_(pool_size),
      num_instances_(num_instances),
      instance_index_(instance_index),
      next_page_id_(instance_index),
      disk_manager_(disk_manager),
      log_manager_(log_manager) {
  BUSTUB_ASSERT(num_instances > 0, "If BPI is not part of a pool, then the pool size should just be 1");
  BUSTUB_ASSERT(
      instance_index < num_instances,
      "BPI index cannot be greater than the number of BPIs in the pool. In non-parallel case, index should just be 1.");
  // We allocate a consecutive memory space for the buffer pool.
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete replacer_;
}

void PrintMap(std::unordered_map<page_id_t, frame_id_t> const &m) {
  for (auto const &pair : m) {
    std::cout << "{" << pair.first << ": " << pair.second << "}\n";
  }
}

void BufferPoolManagerInstance::PrintPageTable() {
  for (auto const &pair : page_table_) {
    frame_id_t frame_id = pair.second;
    Page *p = &(pages_[frame_id]);
    std::cout << "{" << pair.first << ": " << pair.second << " pin count: " << p->pin_count_ << "}\n";
  }
}

bool BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) {
  // Make sure you call DiskManager::WritePage!
  std::lock_guard<std::mutex> guard(latch_);
  auto search = page_table_.find(page_id);
  frame_id_t frame_id;
  // P exist
  if (search != page_table_.end() || page_id == INVALID_PAGE_ID) {
    frame_id = search->second;
    Page *p = &(pages_[frame_id]);
    disk_manager_->WritePage(p->page_id_, p->data_);
    return true;
  }

  return false;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  // You can do it!
  std::lock_guard<std::mutex> guard(latch_);
  for (auto it : page_table_) {
    page_id_t page_id = it.first;
    FlushPgImp(page_id);
  }
}

Page *BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) {
  // 0.   Make sure you call AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.
  std::lock_guard<std::mutex> guard(latch_);
  frame_id_t f;
  // printf("NewPage\n");
  // PrintPageTable();

  if (!free_list_.empty()) {
    // Find in free_list_
    f = this->free_list_.front();
    this->free_list_.pop_front();
  } else {
    // Find in the replacer_
    bool found = this->replacer_->Victim(&f);

    // All pages are pinned
    if (!found) {
      return nullptr;
    }
  }
  *page_id = AllocatePage();
  Page *p = &(pages_[f]);
  auto search = page_table_.find(p->page_id_);
  if (search != page_table_.end()) {
    page_table_.erase(search);
  }
  replacer_->Pin(f);
  if (p->is_dirty_) {
    disk_manager_->WritePage(p->page_id_, p->data_);
  }

  p->ResetMemory();
  p->page_id_ = *page_id;
  p->pin_count_ = 1;
  p->is_dirty_ = false;

  page_table_.insert({*page_id, f});
  return p;
}

Page *BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  // 3.     Delete R from the page table and insert P.
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
  // printf("Entering fetch\n");
  std::lock_guard<std::mutex> guard(latch_);
  // printf("Fetching\n");
  // PrintPageTable();
  auto search = page_table_.find(page_id);
  frame_id_t frame_id;
  // P exist
  if (search != page_table_.end()) {
    frame_id = search->second;
    Page *p = &(pages_[frame_id]);
    replacer_->Pin(frame_id);
    p->pin_count_++;
    return p;
  }

  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    this->free_list_.pop_front();
  } else {
    // Find in the replacer_
    bool found = this->replacer_->Victim(&frame_id);
    if (!found) {
      return nullptr;
    }
  }
  Page *r = &(pages_[frame_id]);
  if (r->is_dirty_) {
    disk_manager_->WritePage(r->page_id_, r->data_);
  }
  search = page_table_.find(r->page_id_);
  if (search != page_table_.end()) {
    page_table_.erase(search);
  }
  page_table_.insert({page_id, frame_id});
  replacer_->Pin(frame_id);
  r->page_id_ = page_id;
  r->pin_count_ = 1;
  r->is_dirty_ = false;
  disk_manager_->ReadPage(page_id, r->data_);

  return r;
}

bool BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) {
  // 0.   Make sure you call DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  std::lock_guard<std::mutex> guard(latch_);
  // PrintPageTable();
  auto search = this->page_table_.find(page_id);
  // P does not exist
  if (search == page_table_.end()) {
    return true;
  }
  frame_id_t frame_id = search->second;
  Page *p = &(pages_[frame_id]);
  if (p->pin_count_ != 0) {
    return false;
  }
  DeallocatePage(page_id);
  page_table_.erase(search);
  p->pin_count_ = 0;
  p->is_dirty_ = false;
  p->page_id_ = INVALID_PAGE_ID;
  free_list_.push_back(frame_id);
  p->ResetMemory();
  return true;
}

bool BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) {
  std::lock_guard<std::mutex> guard(latch_);
  // printf("Unpin\n");
  // PrintPageTable();
  auto search = this->page_table_.find(page_id);

  // P exist
  if (search != page_table_.end()) {
    frame_id_t frame_id = search->second;
    Page *p = &(pages_[frame_id]);
    if (is_dirty) {
      p->is_dirty_ = is_dirty;
    }
    if (p->pin_count_ == 0) {
      return false;
    }
    p->pin_count_--;
    if (p->pin_count_ == 0) {
      replacer_->Unpin(frame_id);
    }
    return true;
  }
  return false;
}

page_id_t BufferPoolManagerInstance::AllocatePage() {
  const page_id_t next_page_id = next_page_id_;
  next_page_id_ += num_instances_;
  ValidatePageId(next_page_id);
  return next_page_id;
}

void BufferPoolManagerInstance::ValidatePageId(const page_id_t page_id) const {
  assert(page_id % num_instances_ == instance_index_);  // allocated pages mod back to this BPI
}

}  // namespace bustub
