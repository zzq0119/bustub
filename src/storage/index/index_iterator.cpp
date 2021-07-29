/**
 * index_iterator.cpp
 */
#include <cassert>

#include "storage/index/index_iterator.h"

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
bool INDEXITERATOR_TYPE::isEnd() { return page_ == nullptr; }

INDEX_TEMPLATE_ARGUMENTS
const MappingType &INDEXITERATOR_TYPE::operator*() { return page_->GetItem(position_); }

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE &INDEXITERATOR_TYPE::operator++() {
  position_++;
  if (position_ >= page_->GetSize()) {
    // manager_->UnpinPage(page_->GetPageId(), false);
    UnlockAndUnPin();
    if (auto next_id = page_->GetNextPageId(); next_id != INVALID_PAGE_ID) {
      auto next = manager_->FetchPage(next_id);
      next->RLatch();
      page_ = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(next->GetData());
      position_ = 0;
    } else {
      page_ = nullptr;
    }
  }
  return *this;
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
