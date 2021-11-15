//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"
#include "execution/expressions/column_value_expression.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_executor)),
      right_executor_(std::move(right_executor)) {}

void NestedLoopJoinExecutor::Init() {
  left_executor_->Init();
  right_executor_->Init();
  if (!left_executor_->Next(&left_tuple_, &left_rid_)) {
    left_end_ = true;
  }
}

bool NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) {
  auto left_schema = left_executor_->GetOutputSchema();
  auto right_schema = right_executor_->GetOutputSchema();
  auto output_schema = plan_->OutputSchema();
  while (!left_end_) {
    Tuple right_tuple;
    RID right_rid;
    while (right_executor_->Next(&right_tuple, &right_rid)) {
      if ((plan_->Predicate() == nullptr) ||
          (plan_->Predicate()->EvaluateJoin(&left_tuple_, left_schema, &right_tuple, right_schema).GetAs<bool>())) {
        std::vector<Value> new_values;
        for (auto const &column : output_schema->GetColumns()) {
          auto column_express = reinterpret_cast<const ColumnValueExpression *>(column.GetExpr());
          if (column_express->GetTupleIdx() == 0) {
            new_values.push_back(left_tuple_.GetValue(left_schema, column_express->GetColIdx()));
          } else {
            new_values.push_back(right_tuple.GetValue(right_schema, column_express->GetColIdx()));
          }
        }
        *tuple = Tuple(new_values, output_schema);
        return true;
      }
    }
    right_executor_->Init();
    if (!left_executor_->Next(&left_tuple_, &left_rid_)) {
      left_end_ = true;
    }
  }
  return false;
}

}  // namespace bustub
