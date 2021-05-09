//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// clock_replacer.cpp
//
// Identification: src/buffer/clock_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/clock_replacer.h"

namespace bustub {

ClockReplacer::ClockReplacer(size_t num_pages) : num_pages_(num_pages) {}

ClockReplacer::~ClockReplacer() = default;

bool ClockReplacer::Victim(frame_id_t *frame_id) {
  std::lock_guard<std::mutex> guard(latch);
  if (frame_map_.empty()) {
    return false;
  } else {
    ReplaceFrame();
    *frame_id = frame_list_.front().first;
    EraseFrame(*frame_id);
    return true;
  }
}

void ClockReplacer::Pin(frame_id_t frame_id) {
  std::lock_guard<std::mutex> guard(latch);
  if (frame_map_.find(frame_id) != frame_map_.end()) {
    EraseFrame(frame_id);
  }
}

void ClockReplacer::Unpin(frame_id_t frame_id) {
  std::lock_guard<std::mutex> guard(latch);
  if (frame_map_.find(frame_id) != frame_map_.end()) {
    frame_map_.at(frame_id)->second = true;
  } else {
    ReplaceFrame();
    AddFrame(frame_id);
  }
}

size_t ClockReplacer::Size() {
  std::lock_guard<std::mutex> guard(latch);
  return frame_list_.size();
}

}  // namespace bustub
