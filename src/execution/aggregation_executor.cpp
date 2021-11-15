//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"
#include "execution/expressions/aggregate_value_expression.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_(std::move(child)),
      aht_(SimpleAggregationHashTable(plan_->GetAggregates(), plan_->GetAggregateTypes())),
      aht_iterator_(aht_.Begin()) {}

void AggregationExecutor::Init() {
  // aht_ = SimpleAggregationHashTable(plan_->GetAggregates(), plan_->GetAggregateTypes());
  child_->Init();
  Tuple tuple;
  RID rid;
  while (child_->Next(&tuple, &rid)) {
    aht_.InsertCombine(MakeAggregateKey(&tuple), MakeAggregateValue(&tuple));
  }
  aht_iterator_ = aht_.Begin();
}

bool AggregationExecutor::Next(Tuple *tuple, RID *rid) {
  while (aht_iterator_ != aht_.End()) {
    AggregateKey aggr_key = aht_iterator_.Key();
    AggregateValue aggr_val = aht_iterator_.Val();
    ++aht_iterator_;
    auto output_schema = GetOutputSchema();
    if (plan_->GetHaving() == nullptr) {
      std::vector<Value> new_values;
      for (auto const &column : output_schema->GetColumns()) {
        auto aggr_express = reinterpret_cast<const AggregateValueExpression *>(column.GetExpr());
        auto val = aggr_express->EvaluateAggregate(aggr_key.group_bys_, aggr_val.aggregates_);
        new_values.push_back(val);
      }
      *tuple = Tuple(new_values, output_schema);
      return true;
    }
    auto having = reinterpret_cast<const ComparisonExpression *>(plan_->GetHaving());
    if (having->EvaluateAggregate(aggr_key.group_bys_, aggr_val.aggregates_).GetAs<bool>()) {
      std::vector<Value> new_values;
      for (auto const &column : output_schema->GetColumns()) {
        auto aggr_express = reinterpret_cast<const AggregateValueExpression *>(column.GetExpr());
        auto val = aggr_express->EvaluateAggregate(aggr_key.group_bys_, aggr_val.aggregates_);
        new_values.push_back(val);
      }
      *tuple = Tuple(new_values, output_schema);
      return true;
    }
  }
  return false;
}

const AbstractExecutor *AggregationExecutor::GetChildExecutor() const { return child_.get(); }

}  // namespace bustub
