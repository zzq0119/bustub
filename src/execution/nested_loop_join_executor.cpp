//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), left_(std::move(left_executor)), right_(std::move(right_executor)) {}

void NestedLoopJoinExecutor::Init() {
  left_->Init();
  right_->Init();
}

bool NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) {
  if (!left_cand_.IsAllocated()) {
    if (!left_->Next(&left_cand_, &left_id_)) {
      return false;
    }
  }
  while (true) {
    if (!right_->Next(tuple, rid)) {
      if (!left_->Next(&left_cand_, &left_id_)) {
        return false;
      }
      right_->Init();
      continue;
    }

    auto pred = plan_->Predicate();
    auto left_schema = left_->GetOutputSchema();
    auto right_schema = right_->GetOutputSchema();
    if (pred == nullptr || pred->EvaluateJoin(&left_cand_, left_schema, tuple, right_schema).GetAs<bool>()) {
      std::vector<Value> values;
      for (auto &col : left_schema->GetColumns()) {
        values.emplace_back(left_cand_.GetValue(left_schema, left_schema->GetColIdx(col.GetName())));
      }
      for (auto &col : right_schema->GetColumns()) {
        values.emplace_back(tuple->GetValue(right_schema, right_schema->GetColIdx(col.GetName())));
      }
      *tuple = Tuple(values, GetOutputSchema());
      return true;
    }
  }
  return false;
}

}  // namespace bustub
