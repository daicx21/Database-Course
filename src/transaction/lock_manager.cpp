#include "lock_manager.hpp"

#include <memory>
#include <shared_mutex>

#include "common/exception.hpp"
#include "common/logging.hpp"
#include "fmt/core.h"
#include "transaction/lock_mode.hpp"
#include "transaction/txn.hpp"
#include "transaction/txn_manager.hpp"

namespace wing {

inline bool CheckModeUpgrade(const LockMode &x,const LockMode &y)
{
  if (x==LockMode::IS) return (y==LockMode::S||y==LockMode::X||y==LockMode::IX||y==LockMode::SIX);
  if (x==LockMode::S||x==LockMode::IX) return (y==LockMode::X||y==LockMode::SIX);
  if (x==LockMode::SIX) return (y==LockMode::X);
  return false;
}

inline bool CheckModeConflict(const LockMode &x,const LockMode &y)
{
  if (x==LockMode::X||y==LockMode::X) return false;
  if (x==LockMode::IS||y==LockMode::IS) return true;
  if (x==LockMode::SIX||y==LockMode::SIX) return false;
  return (x==y);
}

void LockManager::AcquireTableLock(
    std::string_view table_name, LockMode mode, Txn *txn) {
  // P4 TODO
  if (txn->state_==TxnState::ABORTED) throw TxnInvalidBehaviorException("2");
  if (txn->state_==TxnState::SHRINKING)
  {
    txn->state_=TxnState::ABORTED;
    throw TxnInvalidBehaviorException("3");
  }
  std::string table_name_=std::basic_string(table_name.data(),table_name.size());
  table_lock_table_latch_.lock();
  if (!table_lock_table_.count(table_name_)) table_lock_table_.insert(std::make_pair(table_name_,std::make_unique<LockRequestList>()));
  auto &list=*(table_lock_table_[table_name_].get());
  table_lock_table_latch_.unlock();
  std::unique_lock<std::mutex> lock(list.latch_);
  auto &l=list.list_;
  for (auto &&it=l.begin();it!=l.end();it++) if ((*it)->txn_id_==txn->txn_id_)
  {
    if ((*it)->mode_==mode) return;
    if (list.upgrading_!=INVALID_TXN_ID)
    {
      txn->state_=TxnState::ABORTED;
      throw MultiUpgradeException("6a");
    }
    if (!CheckModeUpgrade((*it)->mode_,mode))
    {
      txn->state_=TxnState::ABORTED;
      throw TxnInvalidBehaviorException("6b");
    }
    list.upgrading_=txn->txn_id_;
    break;
  }
  l.push_back(std::make_shared<LockRequest>(txn->txn_id_,mode));
  while (233)
  {
    bool flag=false;
    for (auto &&it=l.begin();it!=l.end();it++)
    {
      if (!(*it)->granted_)
      {
        if ((*it)->txn_id_==txn->txn_id_) flag=true;
        break;
      }
      else
      {
        if (((*it)->txn_id_!=txn->txn_id_)&&!CheckModeConflict((*it)->mode_,mode))
        {
          if ((*it)->txn_id_<txn->txn_id_)
          {
            txn->state_=TxnState::ABORTED;
            for (auto &&it1=l.begin();it1!=l.end();it1++) if (!(*it1)->granted_&&(*it1)->txn_id_==txn->txn_id_)
            {
              l.erase(it1);
              break;
            }
            if (list.upgrading_==txn->txn_id_) list.upgrading_=INVALID_TXN_ID;
            list.cv_.notify_all();
            throw TxnDLAbortException("WaitDie");
          }
          break;
        }
      }
    }
    if (flag) break;
    list.cv_.wait(lock);
  }
  for (auto &&it=l.begin();it!=l.end();it++) if (!(*it)->granted_)
  {
    (*it)->granted_=true;
    txn->rw_latch_.lock();
    txn->table_lock_set_[mode].insert(table_name_);
    txn->rw_latch_.unlock();
    if (list.upgrading_==txn->txn_id_)
    {
      for (auto &&it1=l.begin();it1!=l.end();it1++) if ((*it1)->txn_id_==txn->txn_id_)
      {
        l.erase(it1);
        txn->table_lock_set_[(*it1)->mode_].erase(table_name_);
        break;
      }
      list.upgrading_=INVALID_TXN_ID;
    }
    break;
  }
  list.cv_.notify_all();
}

void LockManager::ReleaseTableLock(
    std::string_view table_name, LockMode mode, Txn *txn) {
  // P4 TODO
  std::string table_name_=std::basic_string(table_name.data(),table_name.size());
  table_lock_table_latch_.lock();
  if (!table_lock_table_.count(table_name_)) table_lock_table_.insert(std::make_pair(table_name_,std::make_unique<LockRequestList>()));
  auto &list=*(table_lock_table_[table_name_].get());
  table_lock_table_latch_.unlock();
  std::unique_lock<std::mutex> lock(list.latch_);
  auto &l=list.list_;
  for (auto &&it=l.begin();it!=l.end();it++) if ((*it)->txn_id_==txn->txn_id_)
  {
    l.erase(it);
    txn->rw_latch_.lock();
    txn->table_lock_set_[mode].erase(table_name_);
    if (txn->state_!=TxnState::COMMITTED) txn->state_=TxnState::SHRINKING;
    txn->rw_latch_.unlock();
    break;
  }
  list.cv_.notify_all();
}

void LockManager::AcquireTupleLock(std::string_view table_name,
    std::string_view key, LockMode mode, Txn *txn) {
  // P4 TODO
  std::string table_name_=std::basic_string(table_name.data(),table_name.size()),key_=std::basic_string(key.data(),key.size());
  if (txn->state_==TxnState::ABORTED) throw TxnInvalidBehaviorException("2");
  if (txn->state_==TxnState::SHRINKING)
  {
    txn->state_=TxnState::ABORTED;
    throw TxnInvalidBehaviorException("3");
  }
  if (mode!=LockMode::S&&mode!=LockMode::X)
  {
    txn->state_=TxnState::ABORTED;
    throw TxnInvalidBehaviorException("7a");
  }
  if ((mode==LockMode::S&&!txn->table_lock_set_[LockMode::IS].count(table_name_)&&!txn->table_lock_set_[LockMode::S].count(table_name_)&&!txn->table_lock_set_[LockMode::X].count(table_name_)&&!txn->table_lock_set_[LockMode::IX].count(table_name_)&&!txn->table_lock_set_[LockMode::SIX].count(table_name_))||(mode==LockMode::X&&!txn->table_lock_set_[LockMode::IX].count(table_name_)&&!txn->table_lock_set_[LockMode::X].count(table_name_)&&!txn->table_lock_set_[LockMode::SIX].count(table_name_)))
  {
    txn->state_=TxnState::ABORTED;
    throw TxnInvalidBehaviorException("7b");
  }
  tuple_lock_table_latch_.lock();
  if (!tuple_lock_table_[table_name_].count(key_)) tuple_lock_table_[table_name_].insert(std::make_pair(key_,std::make_unique<LockRequestList>()));
  auto &list=*(tuple_lock_table_[table_name_][key_].get());
  tuple_lock_table_latch_.unlock();
  std::unique_lock<std::mutex> lock(list.latch_);
  auto &l=list.list_;
  for (auto &&it=l.begin();it!=l.end();it++) if ((*it)->txn_id_==txn->txn_id_)
  {
    if ((*it)->mode_==mode) return;
    if (list.upgrading_!=INVALID_TXN_ID)
    {
      txn->state_=TxnState::ABORTED;
      throw MultiUpgradeException("6a");
    }
    if (!CheckModeUpgrade((*it)->mode_,mode))
    {
      txn->state_=TxnState::ABORTED;
      throw TxnInvalidBehaviorException("6b");
    }
    list.upgrading_=txn->txn_id_;
    break;
  }
  l.push_back(std::make_shared<LockRequest>(txn->txn_id_,mode));
  while (233)
  {
    bool flag=false;
    for (auto &&it=l.begin();it!=l.end();it++)
    {
      if (!(*it)->granted_)
      {
        if ((*it)->txn_id_==txn->txn_id_) flag=true;
        break;
      }
      else
      {
        if (((*it)->txn_id_!=txn->txn_id_)&&!CheckModeConflict((*it)->mode_,mode))
        {
          if ((*it)->txn_id_<txn->txn_id_)
          {
            txn->state_=TxnState::ABORTED;
            for (auto &&it1=l.begin();it1!=l.end();it1++) if (!(*it1)->granted_&&(*it1)->txn_id_==txn->txn_id_)
            {
              l.erase(it1);
              break;
            }
            if (list.upgrading_==txn->txn_id_) list.upgrading_=INVALID_TXN_ID;
            list.cv_.notify_all();
            throw TxnDLAbortException("WaitDie");
          }
          break;
        }
      }
    }
    if (flag) break;
    list.cv_.wait(lock);
  }
  for (auto &&it=l.begin();it!=l.end();it++) if (!(*it)->granted_)
  {
    (*it)->granted_=true;
    txn->tuple_lock_set_[mode][table_name_].insert(key_);
    if (list.upgrading_==txn->txn_id_)
    {
      for (auto &&it1=l.begin();it1!=l.end();it1++) if ((*it1)->txn_id_==txn->txn_id_)
      {
        l.erase(it1);
        txn->tuple_lock_set_[(*it1)->mode_][table_name_].erase(key_);
        break;
      }
      list.upgrading_=INVALID_TXN_ID;
    }
    break;
  }
  list.cv_.notify_all();
}

void LockManager::ReleaseTupleLock(std::string_view table_name,
    std::string_view key, LockMode mode, Txn *txn) {
  // P4 TODO
  std::string table_name_=std::basic_string(table_name.data(),table_name.size()),key_=std::basic_string(key.data(),key.size());
  tuple_lock_table_latch_.lock();
  if (!tuple_lock_table_[table_name_].count(key_)) tuple_lock_table_[table_name_].insert(std::make_pair(key_,std::make_unique<LockRequestList>()));
  auto &list=*(tuple_lock_table_[table_name_][key_].get());
  tuple_lock_table_latch_.unlock();
  std::unique_lock<std::mutex> lock(list.latch_);
  auto &l=list.list_;
  for (auto &&it=l.begin();it!=l.end();it++) if ((*it)->txn_id_==txn->txn_id_)
  {
    l.erase(it);
    txn->rw_latch_.lock();
    txn->tuple_lock_set_[mode][table_name_].erase(key_);
    if (txn->state_!=TxnState::COMMITTED) txn->state_=TxnState::SHRINKING;
    txn->rw_latch_.unlock();
    break;
  }
  list.cv_.notify_all();
}
}  // namespace wing