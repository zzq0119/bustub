//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/index/index_iterator.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
/**
 * index_iterator.h
 * For range scan of b+ tree
 */
#pragma once
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

#define INDEXITERATOR_TYPE IndexIterator<KeyType, ValueType, KeyComparator>

INDEX_TEMPLATE_ARGUMENTS
class IndexIterator {
 public:
  // you may define your own constructor based on your member variables
  IndexIterator(B_PLUS_TREE_LEAF_PAGE_TYPE *page, BufferPoolManager *manager, int position)
      : page_(page), manager_(manager), position_(position){};
  ~IndexIterator() = default;

  bool isEnd();

  const MappingType &operator*();

  IndexIterator &operator++();

  bool operator==(const IndexIterator &itr) const {
    if (page_ == nullptr && itr.page_ == nullptr) {
      return true;
    }
    return page_ == itr.page_ && position_ == itr.position_;
  }

  bool operator!=(const IndexIterator &itr) const { return !this->operator==(itr); }

 private:
  // add your own private member variables here
  B_PLUS_TREE_LEAF_PAGE_TYPE *page_;
  BufferPoolManager *manager_;
  int position_;
};

}  // namespace bustub
