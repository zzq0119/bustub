//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan), index_info_(nullptr), iter_(nullptr, nullptr, 0) {}

void IndexScanExecutor::Init() {
  index_info_ = exec_ctx_->GetCatalog()->GetIndex(plan_->GetIndexOid());
  heap_ = exec_ctx_->GetCatalog()->GetTable(index_info_->table_name_)->table_.get();
  iter_ = dynamic_cast<BPlusTreeIndex<GenericKey<8>, RID, GenericComparator<8>> *>(index_info_->index_.get())
              ->GetBeginIterator();
}

bool IndexScanExecutor::Next(Tuple *tuple, RID *rid) {
  auto schema = exec_ctx_->GetCatalog()->GetTable(index_info_->table_name_)->schema_;
  if (!iter_.isEnd()) {
    auto pred = plan_->GetPredicate();
    std::vector<uint32_t> values;
    for (auto &col : GetOutputSchema()->GetColumns()) {
      values.push_back(schema.GetColIdx(col.GetName()));
    }
    while (!iter_.isEnd()) {
      heap_->GetTuple((*iter_).second, tuple, nullptr);
      auto t = tuple->KeyFromTuple(schema, *GetOutputSchema(), values);
      if (pred == nullptr || pred->Evaluate(&t, &schema).GetAs<bool>()) {
        *tuple = t;
        *rid = (*iter_).second;
        ++iter_;
        return true;
      }
      ++iter_;
    }
  }
  return false;
}

}  // namespace bustub
