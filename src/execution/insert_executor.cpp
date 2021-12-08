//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void InsertExecutor::Init() {
  if (!plan_->IsRawInsert()) {
    child_executor_->Init();
  }
}

bool InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  Schema schema = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid())->schema_;
  std::vector<IndexInfo *> index_info =
      exec_ctx_->GetCatalog()->GetTableIndexes(exec_ctx_->GetCatalog()->GetTable(plan_->TableOid())->name_);
  // Raw Insert
  if (plan_->IsRawInsert()) {
    if (raw_count_ < plan_->RawValues().size()) {
      *tuple = Tuple(plan_->RawValuesAt(raw_count_), &schema);
      *rid = tuple->GetRid();
      // std::cout << rid->ToString() << "\n";
      // See if there is a shared lock
      if (exec_ctx_->GetTransaction()->IsSharedLocked(*rid)) {
        if (!exec_ctx_->GetLockManager()->LockUpgrade(exec_ctx_->GetTransaction(), *rid)) {
          throw TransactionAbortException(exec_ctx_->GetTransaction()->GetTransactionId(), AbortReason::DEADLOCK);
        }
      } else if (!exec_ctx_->GetLockManager()->LockExclusive(exec_ctx_->GetTransaction(), *rid)) {
        throw TransactionAbortException(exec_ctx_->GetTransaction()->GetTransactionId(), AbortReason::DEADLOCK);
      }

      if (!exec_ctx_->GetCatalog()
               ->GetTable(plan_->TableOid())
               ->table_->InsertTuple(*tuple, rid, exec_ctx_->GetTransaction())) {
        return false;
      }
      // exec_ctx_->GetTransaction()->AppendTableWriteRecord(TableWriteRecord(
      //       *rid, WType::INSERT, *tuple, exec_ctx_->GetCatalog()->GetTable(plan_->TableOid())->table_.get()));
      for (auto index : index_info) {
        Tuple key_tuple = tuple->KeyFromTuple(schema, index->key_schema_, index->index_->GetKeyAttrs());
        index->index_->InsertEntry(key_tuple, *rid, exec_ctx_->GetTransaction());
        exec_ctx_->GetTransaction()->AppendTableWriteRecord(IndexWriteRecord(
            *rid, plan_->TableOid(), WType::INSERT, *tuple, index->index_oid_, exec_ctx_->GetCatalog()));
      }
      raw_count_++;
      return true;
    }

    // for (size_t i = 0; i < plan_->RawValues().size(); i++) {
    //   //printf("Raw Values\n");
    //   tuple = new Tuple(plan_->RawValuesAt(i), &schema);
    //   *rid = tuple->GetRid();
    //   if (!exec_ctx_->GetCatalog()
    //            ->GetTable(plan_->TableOid())
    //            ->table_->InsertTuple(*tuple, rid, exec_ctx_->GetTransaction())) {
    //     return false;
    //   }
    //   for (auto index : index_info) {
    //     Tuple key_tuple = tuple->KeyFromTuple(schema, index->key_schema_, index->index_->GetKeyAttrs());
    //     index->index_->InsertEntry(key_tuple, key_tuple.GetRid(), exec_ctx_->GetTransaction());
    //   }
    // }
    return false;
  }
  // Insert child

  if (child_executor_->Next(tuple, rid)) {
    // See if there is a shared lock
    if (exec_ctx_->GetTransaction()->IsSharedLocked(*rid)) {
      if (!exec_ctx_->GetLockManager()->LockUpgrade(exec_ctx_->GetTransaction(), *rid)) {
        throw TransactionAbortException(exec_ctx_->GetTransaction()->GetTransactionId(), AbortReason::DEADLOCK);
      }
    } else if (!exec_ctx_->GetLockManager()->LockExclusive(exec_ctx_->GetTransaction(), *rid)) {
      throw TransactionAbortException(exec_ctx_->GetTransaction()->GetTransactionId(), AbortReason::DEADLOCK);
    }

    if (!exec_ctx_->GetCatalog()
             ->GetTable(plan_->TableOid())
             ->table_->InsertTuple(*tuple, rid, exec_ctx_->GetTransaction())) {
      return false;
    }
    // exec_ctx_->GetTransaction()->AppendTableWriteRecord(TableWriteRecord(
    //       *rid, WType::INSERT, *tuple, exec_ctx_->GetCatalog()->GetTable(plan_->TableOid())->table_.get()));
    for (auto index : index_info) {
      Tuple key_tuple = tuple->KeyFromTuple(schema, index->key_schema_, index->index_->GetKeyAttrs());
      index->index_->InsertEntry(key_tuple, *rid, exec_ctx_->GetTransaction());
      exec_ctx_->GetTransaction()->AppendTableWriteRecord(
          IndexWriteRecord(*rid, plan_->TableOid(), WType::INSERT, *tuple, index->index_oid_, exec_ctx_->GetCatalog()));
    }
    return true;
  }
  return false;
}

}  // namespace bustub
