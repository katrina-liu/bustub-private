//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"
#include "execution/expressions/column_value_expression.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      table_info_(exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid())),
      iter_(exec_ctx->GetCatalog()->GetTable(plan_->GetTableOid())->table_->Begin(exec_ctx_->GetTransaction())) {}

void SeqScanExecutor::Init() { iter_ = table_info_->table_->Begin(exec_ctx_->GetTransaction()); }

bool SeqScanExecutor::Next(Tuple *tuple, RID *rid) {
  // printf("Enter select next\n");
  TableIterator end = table_info_->table_->End();
  Schema schema = table_info_->schema_;
  auto output_schema = plan_->OutputSchema();
  // printf("Before iter loop\n");
  while (iter_ != end) {
    // printf("Enter iter loop\n");
    Tuple curr = *iter_;
    if ((plan_->GetPredicate() == nullptr) || (plan_->GetPredicate()->Evaluate(&curr, &schema).GetAs<bool>())) {
      // printf("tuple\n");
      std::vector<Value> new_values;
      for (auto const &column : output_schema->GetColumns()) {
        auto column_express = reinterpret_cast<const ColumnValueExpression *>(column.GetExpr());
        new_values.push_back(curr.GetValue(&schema, column_express->GetColIdx()));
      }
      *tuple = Tuple(new_values, output_schema);
      *rid = curr.GetRid();
      iter_++;
      return true;
    }
    iter_++;
  }
  return false;
}

}  // namespace bustub
