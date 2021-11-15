//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// distinct_executor.h
//
// Identification: src/include/execution/executors/distinct_executor.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <unordered_set>
#include <utility>
#include <vector>

#include "execution/executors/abstract_executor.h"
#include "execution/executors/aggregation_executor.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/plans/distinct_plan.h"

namespace bustub {

/**
 * DistinctExecutor removes duplicate rows from child ouput.
 */
class DistinctExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new DistinctExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The limit plan to be executed
   * @param child_executor The child executor from which tuples are pulled
   */
  DistinctExecutor(ExecutorContext *exec_ctx, const DistinctPlanNode *plan,
                   std::unique_ptr<AbstractExecutor> &&child_executor);

  /** Initialize the distinct */
  void Init() override;

  /**
   * Yield the next tuple from the distinct.
   * @param[out] tuple The next tuple produced by the distinct
   * @param[out] rid The next tuple RID produced by the distinct
   * @return `true` if a tuple was produced, `false` if there are no more tuples
   */
  bool Next(Tuple *tuple, RID *rid) override;

  /** @return The output schema for the distinct */
  const Schema *GetOutputSchema() override { return plan_->OutputSchema(); };

 private:
  /** The distinct plan node to be executed */
  const DistinctPlanNode *plan_;
  /** The child executor from which tuples are obtained */
  std::unique_ptr<AbstractExecutor> child_executor_;
  std::unordered_set<AggregateKey> seen_;
  /** @return The tuple as an AggregateKey */
  AggregateKey MakeDistinctKey(const Tuple *tuple) {
    std::vector<Value> keys;
    // auto output_schema = plan_->OutputSchema();
    auto child_schema = child_executor_->GetOutputSchema();
    for (size_t i = 0; i < child_schema->GetColumns().size(); i++) {
      // auto column_express = reinterpret_cast<const ColumnValueExpression *>(column.GetExpr());
      // auto column_idx = column_express->GetColIdx();
      Value val = tuple->GetValue(child_schema, i);
      keys.push_back(val);
    }
    return {keys};
  }
};
}  // namespace bustub
