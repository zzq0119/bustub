//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// limit_executor.cpp
//
// Identification: src/execution/limit_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/limit_executor.h"

namespace bustub {

LimitExecutor::LimitExecutor(ExecutorContext *exec_ctx, const LimitPlanNode *plan,
                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void LimitExecutor::Init() {
  if (child_executor_) {
    child_executor_->Init();
  }
}

bool LimitExecutor::Next(Tuple *tuple, RID *rid) {
  // 超过了就不再从child取
  if (count_ >= plan_->GetOffset() + plan_->GetLimit()) {
    return false;
  }
  // 否则取一下试试
  bool has_next = child_executor_->Next(tuple, rid);
  if (!has_next) {
    return false;
  }
  if (count_++ < plan_->GetOffset()) {
    return false;
  }
  return true;
}

}  // namespace bustub
