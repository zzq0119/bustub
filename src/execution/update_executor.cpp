//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-20, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/update_executor.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void UpdateExecutor::Init() {
  child_executor_->Init();
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
}

bool UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  bool has_next = child_executor_->Next(tuple, rid);
  if (!has_next) {
    return false;
  }
  auto new_tuple = GenerateUpdatedTuple(*tuple);
  table_info_->table_->UpdateTuple(new_tuple, *rid, exec_ctx_->GetTransaction());
  try {
    for (auto index : exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_)) {
      index->index_->DeleteEntry(*tuple, *rid, exec_ctx_->GetTransaction());
      index->index_->InsertEntry(new_tuple, *rid, exec_ctx_->GetTransaction());
    }
  } catch (const std::out_of_range &e) {
    return true;
  }
  return true;
}
}  // namespace bustub
