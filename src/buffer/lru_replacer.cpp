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

namespace bustub {

LRUReplacer::LRUReplacer(size_t num_pages) : num_pages_(num_pages) {}

LRUReplacer::~LRUReplacer() = default;

bool LRUReplacer::Victim(frame_id_t *frame_id) {
  std::lock_guard<std::mutex> guard(latch);
  if (frame_map_.empty()) {
    return false;
  } else {
    *frame_id = *frame_list_.begin();
    EraseFrame(*frame_id);
    return true;
  }
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  std::lock_guard<std::mutex> guard(latch);
  if (frame_map_.find(frame_id) != frame_map_.end()) {
    EraseFrame(frame_id);
  }
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  std::lock_guard<std::mutex> guard(latch);
  if (auto iter = frame_map_.find(frame_id); iter == frame_map_.end()) {
    if (frame_map_.size() >= num_pages_) {
      EraseFrame(*frame_list_.begin());
    }
    AddFrame(frame_id);
  } else {
    auto val = *iter->second;
    frame_list_.erase(iter->second);
    frame_list_.emplace_back(val);
    iter->second = (--frame_list_.end());
  }
}

size_t LRUReplacer::Size() {
  std::lock_guard<std::mutex> guard(latch);
  return frame_map_.size();
}

}  // namespace bustub
