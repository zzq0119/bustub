//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.h
//
// Identification: src/include/buffer/lru_replacer.h
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <list>
#include <mutex>  // NOLINT
#include <unordered_map>

#include "buffer/replacer.h"
#include "common/config.h"

namespace bustub {

/**
 * LRUReplacer implements the lru replacement policy, which approximates the Least Recently Used policy.
 */
class LRUReplacer : public Replacer {
 public:
  /**
   * Create a new LRUReplacer.
   * @param num_pages the maximum number of pages the LRUReplacer will be required to store
   */
  explicit LRUReplacer(size_t num_pages);

  /**
   * Destroys the LRUReplacer.
   */
  ~LRUReplacer() override;

  bool Victim(frame_id_t *frame_id) override;

  void Pin(frame_id_t frame_id) override;

  void Unpin(frame_id_t frame_id) override;

  size_t Size() override;

 private:
  // TODO(student): implement me!
  using Iter = std::list<frame_id_t>::iterator;
  std::unordered_map<frame_id_t, Iter> frame_map_;

  std::list<frame_id_t> frame_list_;

  size_t num_pages_;

  mutable std::mutex latch;

  inline void AddFrame(frame_id_t frame_id) {
    frame_list_.emplace_back(frame_id);
    frame_map_.insert({frame_id, --frame_list_.end()});
  }

  inline void EraseFrame(frame_id_t frame_id) {
    frame_list_.erase(frame_map_.at(frame_id));
    frame_map_.erase(frame_id);
  }
};

}  // namespace bustub
