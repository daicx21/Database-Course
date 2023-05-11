#ifndef SAKURA_JOIN_EXECUTOR_H__
#define SAKURA_JOIN_EXECUTOR_H__

#include <iostream>

#include "execution/executor.hpp"
#include "plan/output_schema.hpp"
#include "type/vector.hpp"

namespace wing {

class JoinExecutor : public Executor {
 public:
  JoinExecutor(const std::unique_ptr<Expr>& expr,
    const OutputSchema& left_input_schema,
	const OutputSchema& right_input_schema,
	const OutputSchema& output_schema,
	std::unique_ptr<Executor> ch,
	std::unique_ptr<Executor> ch2)
    :predicate_(JoinExprFunction(expr.get(),left_input_schema,right_input_schema)),
		ch_(std::move(ch)),
		ch2_(std::move(ch2)), 
		left_input_schema_(left_input_schema),
		right_input_schema_(right_input_schema),
		output_schema_(output_schema) {}
  void Init() override { 
  	ch_->Init();
	ch2_->Init();
	left_=TupleStore(left_input_schema_);
	left_.Clear();
	for (auto ch_ret=ch_->Next();ch_ret;ch_ret=ch_->Next()) left_.Append(ch_ret.Data());
	ans=TupleStore(output_schema_);
	ans.Clear();
	mergeTuple.resize(output_schema_.Size());
	it=ans.GetPointerVec().begin();
  }
  InputTuplePtr Next() override {
	while (it==ans.GetPointerVec().end())
	{
		auto ch2_ret=ch2_->Next();
		if (!ch2_ret) return {};
		ans.Clear();
		auto hh=mergeTuple.data();
		if (right_input_schema_.IsRaw()) Tuple::DeSerialize((uint8_t *)(hh+left_input_schema_.Size()),(uint8_t *)ch2_ret.Data(),right_input_schema_.GetCols());
		else memcpy(hh+left_input_schema_.Size(),ch2_ret.Data(),right_input_schema_.Size()*sizeof(StaticFieldRef));
		for (auto &now:left_.GetPointerVec())
		{
			auto ch_ret=InputTuplePtr(now);
			if (!(predicate_&&predicate_.Evaluate(ch_ret,ch2_ret).ReadInt()==0))
			{
				memcpy(hh,now,left_input_schema_.Size()*sizeof(StaticFieldRef));
				ans.Append((const uint8_t *)hh);
			}
		}
		it=ans.GetPointerVec().begin();
	}
	return InputTuplePtr(*it++);
  }

 private:
  JoinExprFunction predicate_;
  std::unique_ptr<Executor> ch_;
  std::unique_ptr<Executor> ch2_;
  const OutputSchema left_input_schema_;
  const OutputSchema right_input_schema_;
  const OutputSchema output_schema_;
  TupleStore left_;
  TupleStore ans;
  std::vector<uint8_t *>::const_iterator it;
  std::vector<StaticFieldRef> mergeTuple;
};

}  // namespace wing

#endif