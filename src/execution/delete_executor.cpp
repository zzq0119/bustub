//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void DeleteExecutor::Init() {
  child_executor_->Init();
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
}

bool DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  bool has_next = child_executor_->Next(tuple, rid);
  if (!has_next) {
    return false;
  }
  table_info_->table_->MarkDelete(*rid, exec_ctx_->GetTransaction());
  try {
    for (auto index : exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_)) {
      index->index_->DeleteEntry(*tuple, *rid, exec_ctx_->GetTransaction());
    }
  } catch (const std::out_of_range &e) {
    return true;
  }
  return true;
}

}  // namespace bustub
