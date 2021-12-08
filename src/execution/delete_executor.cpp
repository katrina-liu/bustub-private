//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::move(child_executor)),
      table_info_(exec_ctx_->GetCatalog()->GetTable(plan_->TableOid())) {}

void DeleteExecutor::Init() { child_executor_->Init(); }

bool DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  Schema schema = table_info_->schema_;
  if (child_executor_->Next(tuple, rid)) {
    // See if there is a shared lock
    if (exec_ctx_->GetTransaction()->IsSharedLocked(*rid)) {
      if (!exec_ctx_->GetLockManager()->LockUpgrade(exec_ctx_->GetTransaction(), *rid)) {
        return false;
      }
    } else if (!exec_ctx_->GetLockManager()->LockExclusive(exec_ctx_->GetTransaction(), *rid)) {
      return false;
    }

    if (table_info_->table_->MarkDelete(*rid, exec_ctx_->GetTransaction())) {
      std::vector<IndexInfo *> index_info = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
      for (auto index : index_info) {
        Tuple key_tuple = tuple->KeyFromTuple(schema, index->key_schema_, index->index_->GetKeyAttrs());
        index->index_->DeleteEntry(key_tuple, *rid, exec_ctx_->GetTransaction());

        // Modify index write records
        exec_ctx_->GetTransaction()->AppendTableWriteRecord(IndexWriteRecord(
            *rid, plan_->TableOid(), WType::DELETE, *tuple, index->index_oid_, exec_ctx_->GetCatalog()));
      }

      // Modify table write records
      // exec_ctx_->GetTransaction()->AppendTableWriteRecord(
      //     TableWriteRecord(*rid, WType::DELETE, *tuple, table_info_->table_.get()));
    }
    return true;
  }
  return false;
}

}  // namespace bustub
