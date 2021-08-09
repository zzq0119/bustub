//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include <list>
#include <unordered_map>

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // We allocate a consecutive memory space for the buffer pool.
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() {
  delete[] pages_;
  delete replacer_;
}
// ok
Page *BufferPoolManager::FetchPageImpl(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  std::unique_lock lock(latch_);
  frame_id_t frame;
  if (auto iter = page_table_.find(page_id); iter != page_table_.end()) {
    replacer_->Pin(iter->second);
    pages_[iter->second].pin_count_++;
    return &pages_[iter->second];
  }
  if (!free_list_.empty()) {
    // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
    //        Note that pages are always found from the free list first.
    frame = free_list_.front();
    free_list_.pop_front();
    replacer_->Unpin(frame);
    page_table_.emplace(page_id, frame);
  } else if (!replacer_->Victim(&frame)) {
    return nullptr;
  }
  // 2.     If R is dirty, write it back to the disk.
  if (pages_[frame].IsDirty()) {
    lock.unlock();
    FlushPageImpl(pages_[frame].page_id_);
    lock.lock();
  }
  // 3.     Delete R from the page table and insert P.
  page_table_.erase(pages_[frame].page_id_);
  page_table_.insert({page_id, frame});
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
  disk_manager_->ReadPage(page_id, pages_[frame].data_);
  pages_[frame].pin_count_ = 1;
  pages_[frame].is_dirty_ = false;
  pages_[frame].page_id_ = page_id;
  return &pages_[frame];
}
// ok
bool BufferPoolManager::UnpinPageImpl(page_id_t page_id, bool is_dirty) {
  std::lock_guard guard(latch_);
  auto iter = page_table_.find(page_id);
  if (iter == page_table_.end()) {
    return false;
  }
  pages_[iter->second].is_dirty_ |= is_dirty;
  if (pages_[iter->second].pin_count_ <= 0) {
    assert(false);
    return false;
  }
  --pages_[iter->second].pin_count_;
  if (pages_[iter->second].pin_count_ == 0) {
    replacer_->Unpin(iter->second);
  }
  return true;
}
// optimize?
bool BufferPoolManager::FlushPageImpl(page_id_t page_id) {
  // Make sure you call DiskManager::WritePage!
  std::lock_guard guard(latch_);
  auto iter = page_table_.find(page_id);
  if (iter == page_table_.end()) {
    return false;
  }
  if (pages_[iter->second].IsDirty()) {
    pages_[iter->second].WLatch();
    pages_[iter->second].is_dirty_ = false;
    disk_manager_->WritePage(page_id, pages_[iter->second].GetData());
    pages_[iter->second].WUnlatch();
  }
  return true;
}
// ok
Page *BufferPoolManager::NewPageImpl(page_id_t *page_id) {
  // 0.   Make sure you call DiskManager::AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  std::lock_guard guard(latch_);
  frame_id_t frame;
  if (!free_list_.empty()) {
    frame = free_list_.front();
    free_list_.pop_front();
    // replacer_->Unpin(frame);
  } else if (!replacer_->Victim(&frame)) {
    return nullptr;
  }
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  *page_id = disk_manager_->AllocatePage();
  if (pages_[frame].is_dirty_) {
    disk_manager_->WritePage(pages_[frame].page_id_, pages_[frame].data_);
  }
  page_table_.erase(pages_[frame].page_id_);
  page_table_.insert({*page_id, frame});
  // 4.   Set the page ID output parameter. Return a pointer to P.
  pages_[frame].page_id_ = *page_id;
  pages_[frame].ResetMemory();
  pages_[frame].is_dirty_ = false;
  pages_[frame].pin_count_ = 1;
  return &pages_[frame];
}
// ok
bool BufferPoolManager::DeletePageImpl(page_id_t page_id) {
  // 0.   Make sure you call DiskManager::DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  std::lock_guard guard(latch_);
  if (auto iter = page_table_.find(page_id); iter != page_table_.end()) {
    // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
    // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free
    // list.
    frame_id_t frame = iter->second;
    if (pages_[frame].pin_count_ > 0) {
      // assert(false);
      return false;
    }
    replacer_->Pin(frame);
    page_table_.erase(iter);
    pages_[frame].ResetMemory();
    pages_[frame].is_dirty_ = false;
    free_list_.push_back(frame);
  }
  disk_manager_->DeallocatePage(page_id);
  return true;
}

void BufferPoolManager::FlushAllPagesImpl() {
  // You can do it!
  for (size_t i = 0; i < pool_size_; ++i) {
    if (pages_[i].is_dirty_) {
      disk_manager_->WritePage(pages_[i].page_id_, pages_[i].data_);
      pages_[i].is_dirty_ = false;
    }
  }
}

}  // namespace bustub
