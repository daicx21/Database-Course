#ifndef SAKURA_AGGREGATE_EXECUTOR_H__
#define SAKURA_AGGREGATE_EXECUTOR_H__

#include <iostream>

#include "execution/executor.hpp"
#include "plan/output_schema.hpp"
#include "type/vector.hpp"
#include "common/murmurhash.hpp"

namespace wing {

class AggregateExecutor : public Executor {
  public:
  AggregateExecutor(const std::unique_ptr<Expr>& expr,
    const OutputSchema& input_schema,
	const OutputSchema& output_schema,
	const std::vector<std::unique_ptr<Expr> > &group_by_exprs_,
	const std::vector<std::unique_ptr<Expr> > &output_exprs_,
	std::unique_ptr<Executor> ch)
    :predicate_(AggregateExprFunction(expr.get(),input_schema)),
		ch_(std::move(ch)),
		input_schema_(input_schema),
		output_schema_(output_schema) {
			ch_functions_.clear();
            types_.clear();
			for (auto &hh:group_by_exprs_)
            {
                ch_functions_.push_back(ExprFunction(hh.get(),input_schema));
                types_.push_back(hh->ret_type_);
            }
            m=types_.size();
            output_functions_.clear();
            for (auto &hh:output_exprs_) output_functions_.push_back(AggregateExprFunction(hh.get(),input_schema));
            n=output_functions_.size();
		}
  void Init() override { 
  	ch_->Init();
    v1.clear();v2.clear();v3.clear();
    mp.clear();tot=0;
	for (auto ch_ret=ch_->Next();ch_ret;ch_ret=ch_->Next())
    {
        size_t res=233;
        for (int i=0;i<m;i++)
        {
            StaticFieldRef hh=ch_functions_[i].Evaluate(ch_ret);
            if (types_[i]==RetType::STRING)
            {
                std::string_view str=hh.ReadStringView();
                res=utils::Hash(str.data(),str.size(),res);
            }
            else res=utils::Hash((const char *)&hh.data_.int_data,8,res);
        }
        if (!mp.count(res))
        {
            mp[res]=tot++;
            int id=mp[res];
            v3.resize(tot);v3[id]=TupleStore(input_schema_);v3[id].Clear();
            v3[id].Append(ch_ret.Data());
            v1.resize(tot);
            if (predicate_)
            {
                v1[id].resize(predicate_.GetImmediateDataSize());
                predicate_.FirstEvaluate(v1[id].data(),ch_ret);
            }
            v2.resize(tot);v2[id].resize(n);
            for (int i=0;i<n;i++)
            {
                v2[id][i].resize(output_functions_[i].GetImmediateDataSize());
                output_functions_[i].FirstEvaluate(v2[id][i].data(),ch_ret);
            }
        }
        else
        {
            int id=mp[res];
            if (predicate_) predicate_.Aggregate(v1[id].data(),ch_ret);
            for (int i=0;i<n;i++) output_functions_[i].Aggregate(v2[id][i].data(),ch_ret);
        }
    }
	mergeTuple.resize(output_schema_.Size());
	it=0;
  }
  InputTuplePtr Next() override {
    int id;
    while (true)
    {
        if (it>=tot) return {};
        id=it++;
        if (!(predicate_&&predicate_.LastEvaluate(v1[id].data(),InputTuplePtr(v3[id].GetPointerVec()[0])).ReadInt()==0)) break;
    }
    for (int i=0;i<n;i++) mergeTuple[i]=output_functions_[i].LastEvaluate(v2[id][i].data(),InputTuplePtr(v3[id].GetPointerVec()[0]));
    return InputTuplePtr((const uint8_t *)mergeTuple.data());
  }

 private:
  AggregateExprFunction predicate_;
  std::vector<ExprFunction> ch_functions_;
  std::vector<AggregateExprFunction> output_functions_;
  std::vector<RetType> types_;
  std::unique_ptr<Executor> ch_;
  const OutputSchema input_schema_;
  const OutputSchema output_schema_;
  std::vector<StaticFieldRef> mergeTuple;
  std::unordered_map<size_t,int> mp;
  std::vector<std::vector<AggregateIntermediateData> > v1;
  std::vector<std::vector<std::vector<AggregateIntermediateData> > > v2;
  std::vector<TupleStore> v3;
  int n,m,it,tot;
};

}  // namespace wing

#endif