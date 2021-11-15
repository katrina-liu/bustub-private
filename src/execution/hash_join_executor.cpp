//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <unordered_map>
#include <utility>
#include <vector>

#include "common/util/hash_util.h"
#include "execution/executors/hash_join_executor.h"
#include "execution/expressions/column_value_expression.h"

namespace bustub {

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_child_(std::move(left_child)),
      right_child_(std::move(right_child)) {}

void HashJoinExecutor::Init() {
  left_child_->Init();
  right_child_->Init();
  Tuple left_tuple;
  RID left_rid;
  auto left_schema = left_child_->GetOutputSchema();
  auto right_schema = right_child_->GetOutputSchema();
  while (left_child_->Next(&left_tuple, &left_rid)) {
    auto left_join_express = reinterpret_cast<const ColumnValueExpression *>(plan_->LeftJoinKeyExpression());
    Value value = left_join_express->Evaluate(&left_tuple, left_schema);
    hash_t hash_val = HashUtil::HashValue(&value);
    auto search = hash_table_.find(hash_val);
    if (search != hash_table_.end()) {
      search->second.push_back(left_tuple);
    } else {
      std::vector<Tuple> new_tuples = {left_tuple};
      hash_table_.insert({hash_val, new_tuples});
    }
  }
  // printf("Init fine middle\n");
  RID right_rid;
  left_index_ = 0;
  left_tuples_.clear();
  if (right_child_->Next(&right_tuple_, &right_rid)) {
    auto right_join_expression = reinterpret_cast<const ColumnValueExpression *>(plan_->RightJoinKeyExpression());
    Value right_value = right_join_expression->Evaluate(&right_tuple_, right_schema);
    hash_t hash_value = HashUtil::HashValue(&right_value);
    auto search = hash_table_.find(hash_value);
    if (search != hash_table_.end()) {
      left_tuples_ = search->second;
    }
  } else {
    right_end_ = true;
  }
  // printf("Init end\n");
}

bool HashJoinExecutor::Next(Tuple *tuple, RID *rid) {
  // printf("Next\n");
  auto left_schema = left_child_->GetOutputSchema();
  auto right_schema = right_child_->GetOutputSchema();
  auto output_schema = plan_->OutputSchema();
  while (!right_end_) {
    while (left_index_ < left_tuples_.size()) {
      Tuple left_tuple = left_tuples_[left_index_];
      std::vector<Value> new_values;
      for (auto const &column : output_schema->GetColumns()) {
        auto column_express = reinterpret_cast<const ColumnValueExpression *>(column.GetExpr());
        if (column_express->GetTupleIdx() == 0) {
          new_values.push_back(left_tuple.GetValue(left_schema, column_express->GetColIdx()));
        } else {
          new_values.push_back(right_tuple_.GetValue(right_schema, column_express->GetColIdx()));
        }
      }
      *tuple = Tuple(new_values, output_schema);
      left_index_++;
      return true;
    }
    left_index_ = 0;
    left_tuples_.clear();
    if (right_child_->Next(&right_tuple_, rid)) {
      auto right_join_expression = reinterpret_cast<const ColumnValueExpression *>(plan_->RightJoinKeyExpression());
      Value right_value = right_join_expression->Evaluate(&right_tuple_, right_schema);
      hash_t hash_value = HashUtil::HashValue(&right_value);
      auto search = hash_table_.find(hash_value);
      if (search != hash_table_.end()) {
        left_tuples_ = search->second;
      }
    } else {
      right_end_ = true;
    }
  }

  return false;
}
}  // namespace bustub
