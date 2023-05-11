#ifndef SAKURA_DISTINCT_EXECUTOR_H__
#define SAKURA_DISTINCT_EXECUTOR_H__

#include <iostream>

#include "execution/executor.hpp"
#include "plan/output_schema.hpp"
#include "type/vector.hpp"

namespace wing {

class DistinctExecutor : public Executor {
 public:
  DistinctExecutor(
	const OutputSchema& output_schema,
	std::unique_ptr<Executor> ch):ch_(std::move(ch)),output_schema_(output_schema) {
        ans=TupleStore(output_schema);
    }
  void Init() override { 
  	ch_->Init();ans.Clear();
  }
  inline bool equal(const StaticFieldRef *x,const StaticFieldRef *y) {
    for (size_t i=0;i<output_schema_.Size();i++)
    {
        auto type=output_schema_.GetCols()[i].type_;
        if (type==FieldType::CHAR||type==FieldType::VARCHAR)
        {
            if (x->ReadStringView()!=y->ReadStringView()) return false;
        }
        else
        {
            if (x->ReadInt()!=y->ReadInt()) return false;
        }
        x++;y++;
    }
    return true;
  }
  InputTuplePtr Next() override {
    for (InputTuplePtr ch_ret=ch_->Next();;ch_ret=ch_->Next())
    {
        if (!ch_ret) return {};
        bool flag=false;
        for (auto &hh:ans.GetPointerVec()) if (equal((const StaticFieldRef *)hh,(const StaticFieldRef *)(ch_ret.Data()))) flag=true;
        if (!flag)
        {
            ans.Append(ch_ret.Data());
            return ch_ret;
        }
    }
  }

 private:
  std::unique_ptr<Executor> ch_;
  const OutputSchema output_schema_;
  TupleStore ans;
};

}  // namespace wing

#endif