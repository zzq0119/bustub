//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/index/b_plus_tree.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <string>
#include <type_traits>

#include "common/exception.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"
#include "storage/page/header_page.h"

namespace bustub {
INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                          int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      root_page_id_(INVALID_PAGE_ID),
      buffer_pool_manager_(buffer_pool_manager),
      comparator_(comparator),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size) {}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::IsEmpty() const { return root_page_id_ == INVALID_PAGE_ID; }
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction) {
  if (IsEmpty()) {
    return false;
  }
  auto page = FindLeafPage(key, false, OpType::READ, transaction);
  ValueType res;
  auto ret = page->Lookup(key, &res, comparator_);
  if (ret) {
    result->push_back(std::move(res));
  }
  FreePagesInTransaction(false, transaction, page->GetPageId());
  assert(buffer_pool_manager_->CheckAllUnpined());
  return ret;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *transaction) {
  if (IsEmpty()) {
    StartNewTree(key, value);
    return true;
  }
  return InsertIntoLeaf(key, value, transaction);
}
/*
 * Insert constant key & value pair into an empty tree
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then update b+
 * tree's root page id and insert entry directly into leaf page.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::StartNewTree(const KeyType &key, const ValueType &value) {
  auto page = buffer_pool_manager_->NewPage(&root_page_id_);
  if (page == nullptr) {
    throw Exception(ExceptionType::OUT_OF_MEMORY, "OUT_OF_MEMORY");
  }
  auto root = reinterpret_cast<LeafPage *>(page->GetData());
  root->Init(root_page_id_);
  UpdateRootPageId(true);
  root->Insert(key, value, comparator_);
  buffer_pool_manager_->UnpinPage(root_page_id_, true);
}

/*
 * Insert constant key & value pair into leaf page
 * User needs to first find the right leaf page as insertion target, then look
 * through leaf page to see whether insert key exist or not. If exist, return
 * immdiately, otherwise insert entry. Remember to deal with split if necessary.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::InsertIntoLeaf(const KeyType &key, const ValueType &value, Transaction *transaction) {
  auto leaf = FindLeafPage(key, false, OpType::INSERT, transaction);
  RID rid;
  if (leaf->Lookup(key, &rid, comparator_)) {
    FreePagesInTransaction(true, transaction);
    return false;
  }
  leaf->Insert(key, value, comparator_);
  if (leaf->GetSize() > leaf->GetMaxSize()) {
    auto new_page = Split(leaf);
    InsertIntoParent(leaf, new_page->KeyAt(0), new_page, transaction);
  }
  // buffer_pool_manager_->UnpinPage(leaf->GetPageId(), true);
  FreePagesInTransaction(true, transaction);
  return true;
}

/*
 * Split input page and return newly created page.
 * Using template N to represent either internal page or leaf page.
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then move half
 * of key & value pairs from input page to newly created page
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
N *BPLUSTREE_TYPE::Split(N *node) {
  page_id_t id;
  auto page = reinterpret_cast<N *>(buffer_pool_manager_->NewPage(&id)->GetData());
  if (page == nullptr) {
    throw Exception(ExceptionType::OUT_OF_MEMORY, "OUT_OF_MEMORY");
  }
  page->Init(id, node->GetParentPageId());
  if constexpr (std::is_same_v<N, LeafPage>) {
    node->MoveHalfTo(page);
    page->SetNextPageId(node->GetNextPageId());
    node->SetNextPageId(id);
  } else {
    node->MoveHalfTo(page, buffer_pool_manager_);
  }

  return page;
}

/*
 * Insert key & value pair into internal page after split
 * @param   old_node      input page from split() method
 * @param   key
 * @param   new_node      returned page from split() method
 * User needs to first find the parent page of old_node, parent node must be
 * adjusted to take info of new_node into account. Remember to deal with split
 * recursively if necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertIntoParent(BPlusTreePage *old_node, const KeyType &key, BPlusTreePage *new_node,
                                      Transaction *transaction) {
  if (old_node->IsRootPage()) {
    auto page = buffer_pool_manager_->NewPage(&root_page_id_);
    if (page == nullptr) {
      throw Exception(ExceptionType::OUT_OF_MEMORY, "OUT_OF_MEMORY");
    }
    auto root = reinterpret_cast<InternalPage *>(page->GetData());
    root->Init(root_page_id_);
    root->PopulateNewRoot(old_node->GetPageId(), key, new_node->GetPageId());
    old_node->SetParentPageId(root_page_id_);
    new_node->SetParentPageId(root_page_id_);
    UpdateRootPageId();
    buffer_pool_manager_->UnpinPage(root_page_id_, true);
  } else {
    auto parent_id = old_node->GetParentPageId();
    new_node->SetParentPageId(parent_id);
    auto ppage = buffer_pool_manager_->FetchPage(parent_id);
    auto parent_page = reinterpret_cast<InternalPage *>(ppage->GetData());
    parent_page->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());
    if (parent_page->GetSize() > parent_page->GetMaxSize()) {
      auto new_page = Split(parent_page);
      InsertIntoParent(parent_page, new_page->KeyAt(0), new_page, transaction);
    }
    buffer_pool_manager_->UnpinPage(parent_id, true);
  }
  buffer_pool_manager_->UnpinPage(new_node->GetPageId(), true);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immdiately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
  if (IsEmpty()) return;
  auto leaf = FindLeafPage(key);
  leaf->RemoveAndDeleteRecord(key, comparator_);
  if (leaf->GetSize() < leaf->GetMinSize()) {
    CoalesceOrRedistribute(leaf, transaction);
  } else {
    buffer_pool_manager_->UnpinPage(leaf->GetPageId(), true);
  }
}

/*
 * User needs to first find the sibling of input page. If sibling's size + input
 * page's size > page's max size, then redistribute. Otherwise, merge.
 * Using template N to represent either internal page or leaf page.
 * @return: true means target leaf page should be deleted, false means no
 * deletion happens
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::CoalesceOrRedistribute(N *node, Transaction *transaction) {
  //一定会Unpin node
  static_assert(std::is_same_v<N, InternalPage> || std::is_same_v<N, LeafPage>);
  if (node->IsRootPage()) {
    return AdjustRoot(node);
  }

  // 逻辑：
  // 1. 获取前后index
  // 2. 判断能否移动元素
  // 3. 不能的话，进行合并
  auto page = buffer_pool_manager_->FetchPage(node->GetParentPageId());
  auto parent = reinterpret_cast<InternalPage *>(page->GetData());
  int index = parent->ValueIndex(node->GetPageId());

  N *prev = nullptr;
  N *next = nullptr;

  if (index > 0) {
    prev = reinterpret_cast<N *>(buffer_pool_manager_->FetchPage(parent->ValueAt(index - 1))->GetData());
  }
  if (index < parent->GetSize() - 1) {
    next = reinterpret_cast<N *>(buffer_pool_manager_->FetchPage(parent->ValueAt(index + 1))->GetData());
  }

  if (next && node->GetSize() + next->GetSize() > node->GetMaxSize()) {
    Redistribute(next, node, 0);
    buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
    if (prev) buffer_pool_manager_->UnpinPage(prev->GetPageId(), false);
    return true;
  }
  if (prev && node->GetSize() + prev->GetSize() > node->GetMaxSize()) {
    Redistribute(prev, node, 1);
    buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
    if (next) buffer_pool_manager_->UnpinPage(next->GetPageId(), false);
    return true;
  }
  if (next) {
    Coalesce(next, node, parent, index, transaction);
    if (prev) buffer_pool_manager_->UnpinPage(prev->GetPageId(), false);
  } else if (prev) {
    Coalesce(node, prev, parent, index - 1, transaction);
    if (next) buffer_pool_manager_->UnpinPage(next->GetPageId(), false);
  } else {
    assert(false);
  }
  return false;
}

/*
 * Move all the key & value pairs from one page to its sibling page, and notify
 * buffer pool manager to delete this page. Parent page must be adjusted to
 * take info of deletion into account. Remember to deal with coalesce or
 * redistribute recursively if necessary.
 * Using template N to represent either internal page or leaf page.
 * @param   next               sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 * @param   parent             parent page of input "node"
 * @param   index              position of this page in parent page
 * @return  true means parent node should be deleted, false means no deletion
 * happend
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::Coalesce(N *&next, N *&node, InternalPage *&parent, int index, Transaction *transaction) {
  // 一定会Unpin next, node, parent，删除next
  // 逻辑：
  // 1. 将next的所有元素移动到node。
  // 2. 删除next，以及parent里面对应next的key
  // 3. 如果parent元素过少，递归处理
  if constexpr (std::is_same_v<N, InternalPage>) {
    next->MoveAllTo(node, parent->KeyAt(index + 1), buffer_pool_manager_);
  } else {
    next->MoveAllTo(node);
  }

  auto pid = next->GetPageId();
  buffer_pool_manager_->UnpinPage(pid, true);
  buffer_pool_manager_->DeletePage(pid);
  buffer_pool_manager_->UnpinPage(node->GetPageId(), true);
  parent->Remove(index + 1);
  if (parent->GetSize() <= parent->GetMinSize()) {
    return CoalesceOrRedistribute(parent, transaction);
  }
  buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
  return false;
}

/*
 * Redistribute key & value pairs from one page to its sibling page. If index ==
 * 0, move sibling page's first key & value pair into end of input "node",
 * otherwise move sibling page's last key & value pair into head of input
 * "node".
 * Using template N to represent either internal page or leaf page.
 * @param   next               sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
void BPLUSTREE_TYPE::Redistribute(N *neighbor_node, N *node, int index) {
  //一定会Unpin node，neighbor_node
  // index==0 说明neighbor_node在node后面，否则说明在前面。将neighbor_node的内容移到node里面。
  auto parent = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(node->GetParentPageId())->GetData());
  int pos = parent->ValueIndex(node->GetPageId());
  if constexpr (std::is_same_v<N, InternalPage>) {
    //将需要移动的那个元素的key，放到node和neighbor中间
    if (index == 0) {
      auto val = parent->KeyAt(pos + 1);
      parent->SetKeyAt(pos + 1, neighbor_node->KeyAt(1));
      neighbor_node->MoveFirstToEndOf(node, val, buffer_pool_manager_);
    } else {
      auto val = parent->KeyAt(pos);
      parent->SetKeyAt(pos, neighbor_node->KeyAt(neighbor_node->GetSize() - 1));
      neighbor_node->MoveLastToFrontOf(node, val, buffer_pool_manager_);
    }
  } else {
    if (index == 0) {
      parent->SetKeyAt(pos + 1, neighbor_node->KeyAt(1));
      neighbor_node->MoveFirstToEndOf(node);
    } else {
      parent->SetKeyAt(pos, neighbor_node->KeyAt(neighbor_node->GetSize() - 1));
      neighbor_node->MoveLastToFrontOf(node);
    }
  }
  buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
  buffer_pool_manager_->UnpinPage(node->GetPageId(), true);
  buffer_pool_manager_->UnpinPage(neighbor_node->GetPageId(), true);
}
/*
 * Update root page if necessary
 * NOTE: size of root page can be less than min size and this method is only
 * called within coalesceOrRedistribute() method
 * case 1: when you delete the last element in root page, but root page still
 * has one last child
 * case 2: when you delete the last element in whole b+ tree
 * @return : true means root page should be deleted, false means no deletion
 * happend
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::AdjustRoot(BPlusTreePage *old_root_node) {
  //一定会Unpin node
  if (old_root_node->GetSize() == 0) {
    buffer_pool_manager_->UnpinPage(old_root_node->GetPageId(), false);
    buffer_pool_manager_->DeletePage(old_root_node->GetPageId());
    root_page_id_ = INVALID_PAGE_ID;
    UpdateRootPageId();
    return true;
  }
  if (old_root_node->GetSize() == 1 && !old_root_node->IsLeafPage()) {
    auto page_id = reinterpret_cast<InternalPage *>(old_root_node)->ValueAt(0);
    buffer_pool_manager_->UnpinPage(old_root_node->GetPageId(), false);
    buffer_pool_manager_->DeletePage(old_root_node->GetPageId());
    root_page_id_ = page_id;
    auto new_root = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(page_id)->GetData());
    new_root->SetParentPageId(INVALID_PAGE_ID);
    buffer_pool_manager_->UnpinPage(new_root->GetPageId(), true);
    return true;
  }
  buffer_pool_manager_->UnpinPage(old_root_node->GetPageId(), false);
  return false;
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leaftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::begin() {
  KeyType k;
  auto page = FindLeafPage(k, true);
  return INDEXITERATOR_TYPE(page, buffer_pool_manager_, 0);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin(const KeyType &key) {
  auto page = FindLeafPage(key, true);
  auto index = page->KeyIndex(key, comparator_);
  if (index == -1) {
    return end();
  }
  return INDEXITERATOR_TYPE(page, buffer_pool_manager_, index);
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::end() { return INDEXITERATOR_TYPE(nullptr, buffer_pool_manager_, 0); }

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Find leaf page containing particular key, if leftMost flag == true, find
 * the left most leaf page
 */
INDEX_TEMPLATE_ARGUMENTS
B_PLUS_TREE_LEAF_PAGE_TYPE *BPLUSTREE_TYPE::FindLeafPage(const KeyType &key, bool leftMost, OpType op,
                                                         Transaction *transaction) {
  page_id_t id = root_page_id_;
  auto page = CrabingFetchPage(root_page_id_, op, -1, transaction);
  while (!page->IsLeafPage()) {
    auto internal = reinterpret_cast<InternalPage *>(page);
    page_id_t tmp;
    if (leftMost) {
      tmp = internal->ValueAt(0);
    } else {
      tmp = internal->Lookup(key, comparator_);
    }
    page = CrabingFetchPage(tmp, op, id, transaction);
    id = tmp;
  }
  return reinterpret_cast<LeafPage *>(page);
}

/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      defualt value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
  HeaderPage *header_page = static_cast<HeaderPage *>(buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
  if (insert_record != 0) {
    // create a new record<index_name + root_page_id> in header_page
    header_page->InsertRecord(index_name_, root_page_id_);
  } else {
    // update root_page_id in header_page
    header_page->UpdateRecord(index_name_, root_page_id_);
  }
  buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Use crabing protocol to fetch page. Concurrency is supported when transaction is not null.
 * For GetValue, we use rlatch, otherwise we use wlatch. If the page is safe, we can release latches of its descendants.
 * Then we add this page to transaction's page_set.
 */
INDEX_TEMPLATE_ARGUMENTS
BPlusTreePage *BPLUSTREE_TYPE::CrabingFetchPage(page_id_t page_id, OpType type, page_id_t prev,
                                                Transaction *transaction) {
  // prev会被Unpin
  auto page = buffer_pool_manager_->FetchPage(page_id);
  bool exclusive = (type != OpType::READ);
  if (exclusive) {
    page->WLatch();
  } else {
    page->RLatch();
  }
  auto tree_page = reinterpret_cast<BPlusTreePage *>(page->GetData());
  if (prev > 0 && (!exclusive || tree_page->IsSafe(type))) {
    FreePagesInTransaction(exclusive, transaction, prev);
  }
  if (transaction) {
    transaction->AddIntoPageSet(page);
  }
  return tree_page;
}

/*
 * Free all pages in pageset and delete pages that should be deleted.
 * If transaction==nullptr, we only need to unlatch and unpin `cur` page. Otherwise `cur` is useless.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::FreePagesInTransaction(bool exclusive, Transaction *transaction, page_id_t cur) {
  if (transaction == nullptr) {
    assert(!exclusive && cur >= 0);
    buffer_pool_manager_->FetchPage(cur)->RUnlatch();
    buffer_pool_manager_->UnpinPage(cur, false);
    buffer_pool_manager_->UnpinPage(cur, false);
    return;
  }
  for (auto page : *transaction->GetPageSet()) {
    if (exclusive) {
      page->WUnlatch();
    } else {
      page->RUnlatch();
    }
    int pid = page->GetPageId();
    buffer_pool_manager_->UnpinPage(pid, exclusive);
    auto set = transaction->GetDeletedPageSet();
    if (set->find(pid) != set->end()) {
      buffer_pool_manager_->DeletePage(pid);
      set->erase(pid);
    }
  }
  assert(transaction->GetDeletedPageSet()->empty());
  transaction->GetPageSet()->clear();
}

/*****************************************************************************
 * FOR TEST
 *****************************************************************************/

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, transaction);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, transaction);
  }
}

/**
 * This method is used for debug only, You don't  need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 * @param out
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    LeafPage *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    InternalPage *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    LeafPage *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    InternalPage *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/***************************************************************************
 *  Check integrity of B+ tree data structure.
 ***************************************************************************/

INDEX_TEMPLATE_ARGUMENTS
int BPLUSTREE_TYPE::isBalanced(page_id_t pid) {
  if (IsEmpty()) return true;
  auto node = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(pid)->GetData());
  if (node == nullptr) {
    // throw Exception(EXCEPTION_TYPE_INDEX, "all page are pinned while isBalanced");
  }
  int ret = 0;
  if (!node->IsLeafPage()) {
    auto page = reinterpret_cast<InternalPage *>(node);
    int last = -2;
    for (int i = 0; i < page->GetSize(); i++) {
      int cur = isBalanced(page->ValueAt(i));
      if (cur >= 0 && last == -2) {
        last = cur;
        ret = last + 1;
      } else if (last != cur) {
        ret = -1;
        break;
      }
    }
  }
  buffer_pool_manager_->UnpinPage(pid, false);
  return ret;
}

INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::isPageCorr(page_id_t pid, std::pair<KeyType, KeyType> &out) {
  if (IsEmpty()) return true;
  auto node = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(pid)->GetData());
  if (node == nullptr) {
    // throw Exception(EXCEPTION_TYPE_INDEX, "all page are pinned while isPageCorr");
  }
  bool ret = true;
  if (node->IsLeafPage()) {
    auto page = reinterpret_cast<LeafPage *>(node);
    int size = page->GetSize();
    ret = ret && (size >= node->GetMinSize() && size <= node->GetMaxSize());
    for (int i = 1; i < size; i++) {
      if (comparator_(page->KeyAt(i - 1), page->KeyAt(i)) > 0) {
        ret = false;
        break;
      }
    }
    out = std::pair<KeyType, KeyType>{page->KeyAt(0), page->KeyAt(size - 1)};
  } else {
    auto page = reinterpret_cast<InternalPage *>(node);
    int size = page->GetSize();
    ret = ret && (size >= node->GetMinSize() && size <= node->GetMaxSize());
    std::pair<KeyType, KeyType> left, right;
    for (int i = 1; i < size; i++) {
      if (i == 1) {
        ret = ret && isPageCorr(page->ValueAt(0), left);
      }
      ret = ret && isPageCorr(page->ValueAt(i), right);
      ret = ret && (comparator_(page->KeyAt(i), left.second) > 0 && comparator_(page->KeyAt(i), right.first) <= 0);
      ret = ret && (i == 1 || comparator_(page->KeyAt(i - 1), page->KeyAt(i)) < 0);
      if (!ret) break;
      left = right;
    }
    out = std::pair<KeyType, KeyType>{page->KeyAt(0), page->KeyAt(size - 1)};
  }
  buffer_pool_manager_->UnpinPage(pid, false);
  return ret;
}

INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::Check(bool forceCheck) {
  if (!forceCheck && !openCheck) {
    return true;
  }
  std::pair<KeyType, KeyType> in;
  bool isPageInOrderAndSizeCorr = isPageCorr(root_page_id_, in);
  bool isBal = (isBalanced(root_page_id_) >= 0);
  bool isAllUnpin = buffer_pool_manager_->CheckAllUnpined();
  if (!isPageInOrderAndSizeCorr) std::cout << "problem in page order or page size" << std::endl;
  if (!isBal) std::cout << "problem in balance" << std::endl;
  if (!isAllUnpin) std::cout << "problem in page unpin" << std::endl;
  return isPageInOrderAndSizeCorr && isBal && isAllUnpin;
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
