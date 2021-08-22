//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan), heap_(nullptr), iter_(nullptr, RID(), nullptr) {}

void SeqScanExecutor::Init() {
  heap_ = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid())->table_.get();
  iter_ = heap_->Begin(exec_ctx_->GetTransaction());
}

bool SeqScanExecutor::Next(Tuple *tuple, RID *rid) {
  // table整体schema
  auto schema = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid())->schema_;
  if (iter_ != heap_->End()) {
    auto pred = plan_->GetPredicate();
    // values为output schema的idx
    std::vector<uint32_t> values;
    for (auto &col : GetOutputSchema()->GetColumns()) {
      values.push_back(schema.GetColIdx(col.GetName()));
    }
    while (iter_ != heap_->End()) {
      auto t = iter_->KeyFromTuple(schema, *GetOutputSchema(), values);
      //如果不满足条件，就跳过
      if (pred == nullptr || pred->Evaluate(&t, &schema).GetAs<bool>()) {
        *tuple = t;
        *rid = iter_->GetRid();
        iter_++;
        return true;
      }
      iter_++;
    }
  }
  return false;
}

}  // namespace bustub
