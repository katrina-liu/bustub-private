//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.cpp
//
// Identification: src/concurrency/lock_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/lock_manager.h"

#include <utility>
#include <vector>

#include "concurrency/transaction_manager.h"

namespace bustub {

bool LockManager::LockShared(Transaction *txn, const RID &rid) {
  // printf("Enter lock share\n");
  // Isolation Level:
  // - Read_Uncommited: cannot acquire S lock
  // -- Throw transaction abort exception with LOCKSHARED_ON_READ_UNCOMMITTED
  // -- return false
  // - Repeatable Read: Can only acquire when growing
  // -- Throw transaction abort exception with LOCK_ON_SHRINKING
  // - Read Commited: can acquire S lock in any phase
  if (txn->IsExclusiveLocked(rid) || txn->IsSharedLocked(rid)) {
    return true;
  }
  IsolationLevel isolation_level = txn->GetIsolationLevel();
  TransactionState transaction_state = txn->GetState();
  if (transaction_state == TransactionState::ABORTED) {
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::DEADLOCK);
  }

  if (isolation_level == IsolationLevel::READ_UNCOMMITTED) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCKSHARED_ON_READ_UNCOMMITTED);
  }
  if (isolation_level == IsolationLevel::REPEATABLE_READ) {
    if (transaction_state == TransactionState::SHRINKING) {
      txn->SetState(TransactionState::ABORTED);
      throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    }
  }
  // Creating a new lock request
  // LockRequest lock_request = LockRequest(txn->GetTransactionId(), LockMode::SHARED);

  // See if lock table has a lock request queue for rid
  std::unique_lock<std::mutex> lock(latch_);
  auto &lock_request_queue = lock_table_[rid].request_queue_;
  LockRequest &req =
      lock_table_[rid].request_queue_.emplace_back(LockRequest(txn->GetTransactionId(), LockMode::SHARED));
  auto &cv = lock_table_[rid].cv_;
  lock.unlock();
  // printf("lock queue size: %ld\n", lock_table_[rid].request_queue_.size());

  // // Insert the lock request
  // lock_queue.request_queue_.push_back(lock_request);

  std::unique_lock<std::mutex> lck(lock_table_[rid].mtx_);
  // Wound wait: check only exclusive lock if young abort

  auto ite = lock_request_queue.begin();
  while ((ite->txn_id_ != txn->GetTransactionId()) || (ite->lock_mode_ != LockMode::SHARED) || (ite->granted_)) {
    if (ite->lock_mode_ == LockMode::EXCLUSIVE) {
      if (ite->txn_id_ > txn->GetTransactionId()) {
        auto abort_trans = TransactionManager::GetTransaction(ite->txn_id_);
        abort_trans->SetState(TransactionState::ABORTED);
        ite = lock_table_[rid].request_queue_.erase(ite);
        cv.notify_all();
      } else {
        ++ite;
      }
    } else {
      ++ite;
    }
  }
  cv.notify_all();

  // std::mutex mtx;

  cv.wait(lck, [&]() -> bool {
    if (txn->GetState() == TransactionState::ABORTED) {
      return true;
    }
    for (auto &it : lock_request_queue) {
      if ((it.txn_id_ == txn->GetTransactionId()) && (it.lock_mode_ == LockMode::SHARED) && (!it.granted_)) {
        return true;
      }
      if (it.lock_mode_ == LockMode::EXCLUSIVE) {
        return false;
      }
    }
    return false;
  });
  if (txn->GetState() == TransactionState::ABORTED) {
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::DEADLOCK);
  }
  req.granted_ = true;
  txn->GetSharedLockSet()->emplace(rid);
  return true;
}

bool LockManager::LockExclusive(Transaction *txn, const RID &rid) {
  // printf("Enter lock exclusive\n");
  if (txn->IsExclusiveLocked(rid)) {
    return true;
  }
  // Isolation level:
  // All 2pl

  // IsolationLevel isolation_level = txn->GetIsolationLevel();
  TransactionState transaction_state = txn->GetState();
  if (transaction_state == TransactionState::ABORTED) {
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::DEADLOCK);
  }
  if (transaction_state == TransactionState::SHRINKING) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
  }
  LockRequest lock_request = LockRequest(txn->GetTransactionId(), LockMode::EXCLUSIVE);

  // printf("lock queue size: %ld\n", lock_table_[rid].request_queue_.size());
  std::unique_lock<std::mutex> lock(latch_);
  auto &lock_request_queue = lock_table_[rid].request_queue_;
  LockRequest &req = lock_table_[rid].request_queue_.emplace_back(lock_request);
  auto &cv = lock_table_[rid].cv_;
  lock.unlock();
  // printf("lock queue size: %ld\n", lock_table_[rid].request_queue_.size());

  std::unique_lock<std::mutex> lck(lock_table_[rid].mtx_);
  // Wound wait: check everything if young abort
  auto ite = lock_request_queue.begin();
  while ((ite->txn_id_ != txn->GetTransactionId()) || (ite->lock_mode_ != LockMode::EXCLUSIVE) || (ite->granted_)) {
    if (ite->txn_id_ > txn->GetTransactionId()) {
      // printf("Wound\n");
      auto abort_trans = TransactionManager::GetTransaction(ite->txn_id_);
      abort_trans->SetState(TransactionState::ABORTED);
      ite = lock_request_queue.erase(ite);
      cv.notify_all();
    } else {
      ++ite;
    }
  }
  cv.notify_all();

  cv.wait(lck, [&]() -> bool {
    if (lock_request_queue.empty()) {
      return true;
    }
    auto it = lock_request_queue.begin();
    if (txn->GetState() == TransactionState::ABORTED) {
      return true;
    }
    return (it->txn_id_ == txn->GetTransactionId()) && (it->lock_mode_ == LockMode::EXCLUSIVE) && (!it->granted_);
  });
  // printf("lock\n");
  if (txn->GetState() == TransactionState::ABORTED) {
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::DEADLOCK);
  }
  req.granted_ = true;
  txn->GetExclusiveLockSet()->emplace(rid);
  // printf("size %ld\n",txn->GetExclusiveLockSet()->size());
  return true;
}

bool LockManager::LockUpgrade(Transaction *txn, const RID &rid) {
  // printf("Enter upgrade %d\n", txn->GetTransactionId());
  if (txn->IsExclusiveLocked(rid)) {
    return true;
  }
  // std::lock_guard<std::mutex> guard(latch_);
  IsolationLevel isolation_level = txn->GetIsolationLevel();
  TransactionState transaction_state = txn->GetState();
  if (transaction_state == TransactionState::ABORTED) {
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::DEADLOCK);
  }
  if (isolation_level == IsolationLevel::READ_UNCOMMITTED) {
    return false;
  }
  if (transaction_state == TransactionState::SHRINKING) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
  }

  if (lock_table_[rid].upgrading_ != INVALID_TXN_ID) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
  }

  std::unique_lock<std::mutex> lock(latch_);
  auto &lock_request_queue = lock_table_[rid].request_queue_;
  // LockRequest &req = lock_request_queue.back();
  auto &cv = lock_table_[rid].cv_;
  auto &upgrading = lock_table_[rid].upgrading_;
  lock.unlock();

  std::unique_lock<std::mutex> lck(lock_table_[rid].mtx_);
  LockRequest lock_request = LockRequest(txn->GetTransactionId(), LockMode::EXCLUSIVE);

  printf("lock request queue size: %ld\n", lock_request_queue.size());
  for (auto &it : lock_request_queue) {
    printf("transaction id: %d\n", it.txn_id_);
  }
  upgrading = txn->GetTransactionId();

  for (auto iter = lock_request_queue.begin(); iter != lock_request_queue.end(); iter++) {
    if (iter->txn_id_ == txn->GetTransactionId()) {
      // printf("erased shared\n");
      printf("Transaction id: %d\n", iter->txn_id_);
      iter = lock_request_queue.erase(iter);
      break;
    }
  }

  // printf("lock request queue size: %ld\n", lock_request_queue.size());
  // for (auto &it : lock_request_queue) {
  //   printf("transaction id: %d\n", it.txn_id_);
  // }
  bool found_exclusive = false;

  auto it = lock_request_queue.begin();
  while (it != lock_request_queue.end()) {
    if (it->lock_mode_ == LockMode::EXCLUSIVE) {
      // printf("Found exclusive\n");
      it = lock_request_queue.emplace(it, lock_request);
      // req = *it;
      found_exclusive = true;
      break;
    }
    ++it;
  }

  if (!found_exclusive) {
    // printf("Not found exclusive\n");
    lock_request_queue.emplace_back(lock_request);
  }

  // printf("lock request queue size: %ld\n", lock_request_queue.size());
  // for (auto &it : lock_request_queue) {
  //   printf("transaction id: %d\n", it.txn_id_);
  // }
  // Wound wait: check everything if young abort
  auto ite = lock_request_queue.begin();

  while (ite != lock_request_queue.end()) {
    if ((ite->txn_id_ == txn->GetTransactionId()) || (ite->lock_mode_ == LockMode::EXCLUSIVE)) {
      break;
    }
    if (ite->txn_id_ > txn->GetTransactionId()) {
      // printf("Wound\n");
      auto abort_trans = TransactionManager::GetTransaction(ite->txn_id_);
      abort_trans->SetState(TransactionState::ABORTED);
      ite = lock_request_queue.erase(ite);
      cv.notify_all();
    } else {
      ++ite;
    }
  }

  cv.notify_all();

  lock_table_[rid].cv_.wait(lck, [&]() -> bool {
    if (lock_request_queue.empty()) {
      return true;
    }
    auto it = lock_request_queue.begin();
    if (txn->GetState() == TransactionState::ABORTED) {
      return true;
    }
    return (it->txn_id_ == txn->GetTransactionId()) && (it->lock_mode_ == LockMode::EXCLUSIVE) && (!it->granted_);
  });

  // printf("lock\n");
  upgrading = INVALID_TXN_ID;

  if (txn->GetState() == TransactionState::ABORTED) {
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::DEADLOCK);
  }
  lock_request_queue.front().granted_ = true;

  txn->GetSharedLockSet()->erase(rid);
  txn->GetExclusiveLockSet()->emplace(rid);
  return true;
}

bool LockManager::Unlock(Transaction *txn, const RID &rid) {
  // printf("Enter unlock\n");
  // printf("%ld %ld\n", txn->GetSharedLockSet()->size(), txn->GetExclusiveLockSet()->size());
  IsolationLevel isolation_level = txn->GetIsolationLevel();
  TransactionState transaction_state = txn->GetState();
  // if (transaction_state == TransactionState::ABORTED) {
  //   return false;
  // }
  // Isolation Level:
  // - Read_Uncommited: cannot acquire S lock
  // -- Throw transaction abort exception with LOCKSHARED_ON_READ_UNCOMMITTED
  // -- return false
  // - Repeatable Read: Can only acquire when growing
  // -- Throw transaction abort exception with LOCK_ON_SHRINKING
  // - Read Commited: can acquire S lock in any phase

  auto found = lock_table_.find(rid);
  if (found == lock_table_.end()) {
    // printf("No rid lock request queue\n");
    return true;
  }
  std::unique_lock<std::mutex> lock(latch_);
  auto &lock_queue = lock_table_[rid].request_queue_;
  auto &cv = lock_table_[rid].cv_;
  lock.unlock();

  // printf("lock queue size: %ld\n", lock_queue.size());

  std::unique_lock<std::mutex> lck(lock_table_[rid].mtx_);
  if (isolation_level == IsolationLevel::REPEATABLE_READ) {
    // printf("Repeatable read\n");
    if (transaction_state == TransactionState::GROWING) {
      txn->SetState(TransactionState::SHRINKING);
    }
    // bool found_lock = false;
    auto it = lock_queue.begin();
    while (it != lock_queue.end()) {
      if (it->txn_id_ == txn->GetTransactionId()) {
        // found_lock = true;
        // printf("Erased lock\n");
        if (it->lock_mode_ == LockMode::SHARED) {
          txn->GetSharedLockSet()->erase(rid);
        } else {
          txn->GetExclusiveLockSet()->erase(rid);
        }
        it = lock_queue.erase(it);

        break;
      }
      ++it;
    }
    // if (!found_lock) {
    //   // printf("No lock found\n");
    //   return true;
    // }

  } else if (isolation_level == IsolationLevel::READ_UNCOMMITTED) {
    // printf("Read uncommited\n");
    auto it = lock_queue.begin();
    while (it != lock_queue.end()) {
      if (it->txn_id_ == txn->GetTransactionId()) {
        if (transaction_state == TransactionState::GROWING) {
          txn->SetState(TransactionState::SHRINKING);
        }

        if (it->lock_mode_ == LockMode::SHARED) {
          txn->GetSharedLockSet()->erase(rid);
        } else {
          txn->GetExclusiveLockSet()->erase(rid);
        }
        it = lock_queue.erase(it);
        break;
      }
      ++it;
    }
  } else {
    auto it = lock_queue.begin();
    while (it != lock_queue.end()) {
      if (it->txn_id_ == txn->GetTransactionId()) {
        if (it->lock_mode_ == LockMode::EXCLUSIVE) {
          if (transaction_state == TransactionState::GROWING) {
            txn->SetState(TransactionState::SHRINKING);
          }
        }
        if (it->lock_mode_ == LockMode::SHARED) {
          txn->GetSharedLockSet()->erase(rid);
        } else {
          txn->GetExclusiveLockSet()->erase(rid);
        }
        it = lock_queue.erase(it);

        break;
      }
      ++it;
    }
  }

  cv.notify_all();
  return true;
}

}  // namespace bustub
