#include "txn_manager.hpp"

#include <memory>
#include <mutex>
#include <shared_mutex>
#include <unordered_map>

#include "storage/bplus-tree-storage.hpp"
#include "storage/storage.hpp"
#include "transaction/lock_manager.hpp"
#include "transaction/txn.hpp"
namespace wing {
std::unordered_map<txn_id_t, std::unique_ptr<Txn>> TxnManager::txn_table_ = {};
std::shared_mutex TxnManager::rw_latch_ = {};

Txn* TxnManager::Begin() {
  auto txn = new Txn(txn_id_++);
  std::unique_lock lock(rw_latch_);
  txn_table_[txn->txn_id_] = std::unique_ptr<Txn>(txn);
  return txn;
}

void TxnManager::Commit(Txn* txn) {
  txn->state_ = TxnState::COMMITTED;
  // Release all the locks
  ReleaseAllLocks(txn);
}

void TxnManager::Abort(Txn* txn) {
  // P4 TODO: rollback
  while (!txn->modify_records_.empty())
  {
    ModifyRecord now=txn->modify_records_.top();txn->modify_records_.pop();
    if (now.type_==ModifyType::INSERT) storage_.GetModifyHandle(std::make_unique<TxnExecCtx>(txn,std::move(now.table_name_),&lock_manager_))->Delete(std::string_view(now.key_));
    else if (now.type_==ModifyType::DELETE) storage_.GetModifyHandle(std::make_unique<TxnExecCtx>(txn,std::move(now.table_name_),&lock_manager_))->Insert(std::string_view(now.key_),std::string_view(now.old_value_.value()));
    else storage_.GetModifyHandle(std::make_unique<TxnExecCtx>(txn,std::move(now.table_name_),&lock_manager_))->Update(std::string_view(now.key_),std::string_view(now.old_value_.value()));
    txn->modify_records_.pop();
  }
  txn->state_=TxnState::ABORTED;
  ReleaseAllLocks(txn);
}

// Don't need latches here because txn is already committed or aborted.
void TxnManager::ReleaseAllLocks(Txn* txn) {
  // Release all the locks. Tuple locks first.
  auto tuple_lock_set_copy = txn->tuple_lock_set_;
  for (auto& [mode, table_map] : tuple_lock_set_copy) {
    for (auto& [table_name, key_set] : table_map) {
      for (auto& key : key_set) {
        lock_manager_.ReleaseTupleLock(table_name, key, mode, txn);
      }
    }
  }
  // Release table locks.
  auto table_lock_set_copy = txn->table_lock_set_;
  for (auto& [mode, table_set] : table_lock_set_copy) {
    for (auto& table_name : table_set) {
      lock_manager_.ReleaseTableLock(table_name, mode, txn);
    }
  }
}

}  // namespace wing