#ifndef SAKURA_LIMIT_EXECUTOR_H__
#define SAKURA_LIMIT_EXECUTOR_H__

#include <iostream>

#include "execution/executor.hpp"
#include "plan/output_schema.hpp"
#include "type/vector.hpp"

namespace wing {

class LimitExecutor : public Executor {
  public:
  LimitExecutor(
    size_t limit_size,
    size_t offset,
    std::unique_ptr<Executor> ch):
    limit_size_(limit_size),offset_(offset),ch_(std::move(ch)) {}
  void Init() override { ch_->Init();it=0; }
  InputTuplePtr Next() override {
    while (it<offset_) ch_->Next(),it++;
    if (it>=offset_+limit_size_) return {};
    it++;
    return ch_->Next();
  }

 private:
  std::unique_ptr<Executor> ch_;
  size_t limit_size_,offset_,it;
};

}  // namespace wing

#endif