//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_index_join_executor.cpp
//
// Identification: src/execution/nested_index_join_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_index_join_executor.h"

namespace bustub {

NestIndexJoinExecutor::NestIndexJoinExecutor(ExecutorContext *exec_ctx, const NestedIndexJoinPlanNode *plan,
                                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_(std::move(child_executor)) {}

void NestIndexJoinExecutor::Init() {
  child_->Init();
  inner_table_ = exec_ctx_->GetCatalog()->GetTable(plan_->GetInnerTableOid());
  index_info_ = exec_ctx_->GetCatalog()->GetIndex(plan_->GetIndexName(), inner_table_->name_);
}

bool NestIndexJoinExecutor::Next(Tuple *tuple, RID *rid) {
  Tuple out_tuple;
  RID out_rid;
  while (true) {
    if (!child_->Next(&out_tuple, &out_rid)) {
      return false;
    }
    std::vector<RID> rids;
    index_info_->index_->ScanKey(*tuple, &rids, exec_ctx_->GetTransaction());
    if (rids.empty()) {
      continue;
    }
    assert(rids.size() == 1);
    inner_table_->table_->GetTuple(rids[0], tuple, exec_ctx_->GetTransaction());
    std::vector<Value> values;
    auto inner_schema = plan_->InnerTableSchema();
    auto outer_schema = plan_->OuterTableSchema();
    for (auto &col : inner_schema->GetColumns()) {
      values.emplace_back(tuple->GetValue(inner_schema, inner_schema->GetColIdx(col.GetName())));
    }
    for (auto &col : outer_schema->GetColumns()) {
      values.emplace_back(out_tuple.GetValue(outer_schema, outer_schema->GetColIdx(col.GetName())));
    }
    *tuple = Tuple(values, GetOutputSchema());
    break;
  }
  return true;
}

}  // namespace bustub
