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
    : AbstractExecutor(exec_ctx), plan_(plan), child_(std::move(child)),
      aht_(SimpleAggregationHashTable(plan_->GetAggregates() ,plan_->GetAggregateTypes())),
      aht_iterator_(aht_.Begin()) {}

const AbstractExecutor *AggregationExecutor::GetChildExecutor() const { return child_.get(); }

void AggregationExecutor::Init() {
  child_->Init();
  Tuple tuple;
  RID rid;
  while (child_->Next(&tuple, &rid)) {
    AggregateKey aggregate_key = MakeKey(&tuple);
    AggregateValue aggregate_value = MakeVal(&tuple);
    aht_.InsertCombine(aggregate_key, aggregate_value);
  }
  aht_iterator_ = aht_.Begin();
}

bool AggregationExecutor::Next(Tuple *tuple, RID *rid) {
  while (aht_iterator_ != aht_.End()) {
    auto group_bys = aht_iterator_.Key().group_bys_;
    auto aggregates = aht_iterator_.Val().aggregates_;
    if (plan_->GetHaving() == nullptr || plan_->GetHaving()->EvaluateAggregate(group_bys, aggregates).GetAs<bool>()) {
      std::vector<Value> out_vec;
      int count = plan_->OutputSchema()->GetColumnCount();
      for (int i=0; i < count; i++) {
        out_vec.emplace_back(plan_->OutputSchema()->GetColumn(i).GetExpr()->EvaluateAggregate(group_bys, aggregates));
      }
      *tuple = Tuple(out_vec, plan_->OutputSchema());
      ++aht_iterator_;
      return true;
    }
    ++aht_iterator_;
  }
  return false;
}

}  // namespace bustub
