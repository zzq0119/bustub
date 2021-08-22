//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_(std::move(child_executor)) {}

void InsertExecutor::Init() {
  if (child_) {
    child_->Init();
  }
  tabledata_ = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
}

bool InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  if (plan_->IsRawInsert()) {
    if (inserted_ >= plan_->RawValues().size()) {
      return false;
    }
    tabledata_->table_->InsertTuple(Tuple(plan_->RawValuesAt(inserted_), &tabledata_->schema_), rid,
                                    exec_ctx_->GetTransaction());
    inserted_++;
    try {
      for (auto index : exec_ctx_->GetCatalog()->GetTableIndexes(tabledata_->name_)) {
        index->index_->InsertEntry(Tuple(plan_->RawValuesAt(inserted_ - 1), &tabledata_->schema_), *rid,
                                   exec_ctx_->GetTransaction());
      }
    } catch (const std::out_of_range &e) {
      return true;
    }
    return true;
  }
  bool has_next = child_->Next(tuple, rid);
  if (!has_next) {
    return false;
  }
  tabledata_->table_->InsertTuple(*tuple, rid, exec_ctx_->GetTransaction());
  inserted_++;
  try {
    for (auto index : exec_ctx_->GetCatalog()->GetTableIndexes(tabledata_->name_)) {
      index->index_->InsertEntry(*tuple, *rid, exec_ctx_->GetTransaction());
    }
  } catch (const std::out_of_range &e) {
    return true;
  }
  return true;
}

}  // namespace bustub
