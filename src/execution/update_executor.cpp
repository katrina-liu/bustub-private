//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/update_executor.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      table_info_(exec_ctx_->GetCatalog()->GetTable(plan_->TableOid())),
      child_executor_(std::move(child_executor)) {}

void UpdateExecutor::Init() { child_executor_->Init(); }

bool UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  Schema schema = table_info_->schema_;
  std::vector<IndexInfo *> index_info = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
  if (child_executor_->Next(tuple, rid)) {
    // See if there is a shared lock
    if (exec_ctx_->GetTransaction()->IsSharedLocked(*rid)) {
      if (!exec_ctx_->GetLockManager()->LockUpgrade(exec_ctx_->GetTransaction(), *rid)) {
        return false;
      }
    } else if (!exec_ctx_->GetLockManager()->LockExclusive(exec_ctx_->GetTransaction(), *rid)) {
      return false;
    }

    for (auto index : index_info) {
      Tuple old_key_tuple = tuple->KeyFromTuple(schema, index->key_schema_, index->index_->GetKeyAttrs());
      index->index_->DeleteEntry(old_key_tuple, *rid, exec_ctx_->GetTransaction());
      exec_ctx_->GetTransaction()->AppendTableWriteRecord(
          IndexWriteRecord(*rid, plan_->TableOid(), WType::DELETE, *tuple, index->index_oid_, exec_ctx_->GetCatalog()));
    }

    Tuple updated_tuple = GenerateUpdatedTuple(*tuple);
    if (!table_info_->table_->UpdateTuple(updated_tuple, *rid, exec_ctx_->GetTransaction())) {
      if (!table_info_->table_->MarkDelete(*rid, exec_ctx_->GetTransaction())) {
        return false;
      }
      // exec_ctx_->GetTransaction()->AppendTableWriteRecord(
      //       TableWriteRecord(*rid, WType::DELETE, *tuple, table_info_->table_.get()));
      if (!table_info_->table_->InsertTuple(updated_tuple, rid, exec_ctx_->GetTransaction())) {
        return false;
      }
      // exec_ctx_->GetTransaction()->AppendTableWriteRecord(
      //       TableWriteRecord(*rid, WType::INSERT, *tuple, table_info_->table_.get()));
    }
    for (auto index : index_info) {
      Tuple new_key_tuple = updated_tuple.KeyFromTuple(schema, index->key_schema_, index->index_->GetKeyAttrs());
      index->index_->InsertEntry(new_key_tuple, *rid, exec_ctx_->GetTransaction());
      exec_ctx_->GetTransaction()->AppendTableWriteRecord(IndexWriteRecord(
          *rid, plan_->TableOid(), WType::INSERT, updated_tuple, index->index_oid_, exec_ctx_->GetCatalog()));
    }

    return true;
  }
  return false;
}

Tuple UpdateExecutor::GenerateUpdatedTuple(const Tuple &src_tuple) {
  const auto &update_attrs = plan_->GetUpdateAttr();
  Schema schema = table_info_->schema_;
  uint32_t col_count = schema.GetColumnCount();
  std::vector<Value> values;
  for (uint32_t idx = 0; idx < col_count; idx++) {
    if (update_attrs.find(idx) == update_attrs.cend()) {
      values.emplace_back(src_tuple.GetValue(&schema, idx));
    } else {
      const UpdateInfo info = update_attrs.at(idx);
      Value val = src_tuple.GetValue(&schema, idx);
      switch (info.type_) {
        case UpdateType::Add:
          values.emplace_back(val.Add(ValueFactory::GetIntegerValue(info.update_val_)));
          break;
        case UpdateType::Set:
          values.emplace_back(ValueFactory::GetIntegerValue(info.update_val_));
          break;
      }
    }
  }
  return Tuple{values, &schema};
}

}  // namespace bustub
