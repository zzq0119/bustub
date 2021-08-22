//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_(std::move(child)),
      aht_(plan_->GetAggregates(), plan_->GetAggregateTypes()),
      aht_iterator_(aht_.End()) {}

const AbstractExecutor *AggregationExecutor::GetChildExecutor() const { return child_.get(); }

void AggregationExecutor::Init() {
  child_->Init();
  Tuple tuple;
  RID rid;
  while (child_->Next(&tuple, &rid)) {
    aht_.InsertCombine(MakeKey(&tuple), MakeVal(&tuple));
  }
  aht_iterator_ = aht_.Begin();
}

bool AggregationExecutor::Next(Tuple *tuple, RID *rid) {
  while (true) {
    if (aht_iterator_ == aht_.End()) {
      return false;
    }
    auto having = plan_->GetHaving();
    auto &group_bys = aht_iterator_.Key().group_bys_;
    auto &aggregates = aht_iterator_.Val().aggregates_;
    ++aht_iterator_;
    if (having == nullptr || having->EvaluateAggregate(group_bys, aggregates).GetAs<bool>()) {
      std::vector<Value> values;
      for (auto &col : GetOutputSchema()->GetColumns()) {
        values.emplace_back(col.GetExpr()->EvaluateAggregate(group_bys, aggregates));
      }
      *tuple = Tuple(values, GetOutputSchema());
      break;
    }
  }
  return true;
}

}  // namespace bustub
