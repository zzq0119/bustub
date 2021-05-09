//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// clock_replacer.h
//
// Identification: src/include/buffer/clock_replacer.h
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <list>
#include <mutex>  // NOLINT
#include <unordered_map>
#include <vector>

#include "buffer/replacer.h"
#include "common/config.h"

namespace bustub {

/**
 * ClockReplacer implements the clock replacement policy, which approximates the Least Recently Used policy.
 */
class ClockReplacer : public Replacer {
 public:
  /**
   * Create a new ClockReplacer.
   * @param num_pages the maximum number of pages the ClockReplacer will be required to store
   */
  explicit ClockReplacer(size_t num_pages);

  /**
   * Destroys the ClockReplacer.
   */
  ~ClockReplacer() override;

  bool Victim(frame_id_t *frame_id) override;

  void Pin(frame_id_t frame_id) override;

  void Unpin(frame_id_t frame_id) override;

  size_t Size() override;

 private:
  // TODO(student): implement me!
  std::list<std::pair<frame_id_t, bool>> frame_list_;

  using Iter = std::list<std::pair<frame_id_t, bool>>::iterator;
  std::unordered_map<frame_id_t, Iter> frame_map_;

  size_t num_pages_;

  mutable std::mutex latch;

  inline void AddFrame(frame_id_t frame_id) {
    frame_list_.emplace_back(frame_id, false);
    frame_map_.insert({frame_id, --frame_list_.end()});
  }

  inline void EraseFrame(frame_id_t frame_id) {
    frame_list_.erase(frame_map_.at(frame_id));
    frame_map_.erase(frame_id);
  }

  inline void ReplaceFrame() {
    while (frame_map_.size() == num_pages_) {
      if (auto p = frame_list_.front(); p.second) {
        frame_list_.pop_front();
        p.second = false;
        frame_list_.emplace_back(p);
      } else {
        EraseFrame(p.first);
      }
    }
  }
};

}  // namespace bustub
