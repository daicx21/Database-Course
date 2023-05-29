#ifndef SAKURA_HASHJOIN_EXECUTOR_H__
#define SAKURA_HASHJOIN_EXECUTOR_H__

#include <iostream>

#include "execution/executor.hpp"
#include "plan/output_schema.hpp"
#include "type/vector.hpp"
#include "common/murmurhash.hpp"

namespace wing {

class HashJoinExecutor : public Executor {
 public:
  HashJoinExecutor(const std::unique_ptr<Expr>& expr,
    const OutputSchema& left_input_schema,
	const OutputSchema& right_input_schema,
	const OutputSchema& output_schema,
	const std::vector<std::unique_ptr<Expr> > &left_hash_exprs_,
	const std::vector<std::unique_ptr<Expr> > &right_hash_exprs_,
	std::unique_ptr<Executor> ch,
	std::unique_ptr<Executor> ch2)
    :predicate_(JoinExprFunction(expr.get(),left_input_schema,right_input_schema)),
		ch_(std::move(ch)),
		ch2_(std::move(ch2)), 
		left_input_schema_(left_input_schema),
		right_input_schema_(right_input_schema),
		output_schema_(output_schema) {
			left_functions_.clear();
			right_functions_.clear();
            types_.clear();
			for (auto &hh:left_hash_exprs_) left_functions_.push_back(ExprFunction(hh.get(),left_input_schema));
			for (auto &hh:right_hash_exprs_) right_functions_.push_back(ExprFunction(hh.get(),right_input_schema));
            for (auto &hh:left_hash_exprs_) types_.push_back(hh->ret_type_);
            m=types_.size();
		}
  void Init() override { 
  	ch_->Init();
	ch2_->Init();
	left_=TupleStore(left_input_schema_);
	left_.Clear();
    mp.clear();
    cnt=cnt1=cnt2=0;
	for (auto ch_ret=ch_->Next();ch_ret;ch_ret=ch_->Next())
    {
        auto now=left_.Append_(ch_ret.Data());
        size_t res=233;
        for (int i=0;i<m;i++)
        {
            StaticFieldRef hh=left_functions_[i].Evaluate(ch_ret);
            if (types_[i]==RetType::STRING)
            {
                std::string_view str=hh.ReadStringView();
                res=utils::Hash(str,res);
            }
            else res=utils::Hash8(hh.data_.int_data,res);
        }
        mp[res].push_back(now);
        cnt++;
    }
    //printf("%d\n",cnt);
	ans=TupleStore(output_schema_);
	ans.Clear();
	mergeTuple.resize(output_schema_.Size());
	it=ans.GetPointerVec().begin();
  }
  InputTuplePtr Next() override {
	while (it==ans.GetPointerVec().end())
	{
		auto ch2_ret=ch2_->Next();
        //if (!ch2_ret) printf("%d %d %d 233\n",cnt,cnt1,cnt2);
        cnt1++;
		if (!ch2_ret) return {};
		ans.Clear();
		auto hh=mergeTuple.data();
        size_t res=233;
        bool flag=false;
        for (int i=0;i<m;i++)
        {
            StaticFieldRef hh=right_functions_[i].Evaluate(ch2_ret);
            if (types_[i]==RetType::STRING)
            {
                std::string_view str=hh.ReadStringView();
                res=utils::Hash(str,res);
            }
            else res=utils::Hash8(hh.data_.int_data,res);
        }
		if (mp.count(res)) for (auto &now:mp[res])
		{
            if (predicate_)
            {
                auto ch_ret=InputTuplePtr(now);
                if (!(predicate_.Evaluate(ch_ret,ch2_ret).ReadInt()==0))
                {
                    if (!flag)
                    {
                        flag=true;
                        if (right_input_schema_.IsRaw()) Tuple::DeSerialize((uint8_t *)(hh+left_input_schema_.Size()),(uint8_t *)ch2_ret.Data(),right_input_schema_.GetCols());
		                else memcpy(hh+left_input_schema_.Size(),ch2_ret.Data(),right_input_schema_.Size()*sizeof(StaticFieldRef));
                    }
                    memcpy(hh,now,left_input_schema_.Size()*sizeof(StaticFieldRef));
				    ans.Append((const uint8_t *)hh);
                }
            }
            else
            {
                if (!flag)
                {
                    flag=true;
                    if (right_input_schema_.IsRaw()) Tuple::DeSerialize((uint8_t *)(hh+left_input_schema_.Size()),(uint8_t *)ch2_ret.Data(),right_input_schema_.GetCols());
		            else memcpy(hh+left_input_schema_.Size(),ch2_ret.Data(),right_input_schema_.Size()*sizeof(StaticFieldRef));
                }
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
  std::vector<ExprFunction> left_functions_,right_functions_;
  std::vector<RetType> types_;
  std::unique_ptr<Executor> ch_;
  std::unique_ptr<Executor> ch2_;
  const OutputSchema left_input_schema_;
  const OutputSchema right_input_schema_;
  const OutputSchema output_schema_;
  TupleStore left_;
  TupleStore ans;
  std::vector<uint8_t *>::const_iterator it;
  std::vector<StaticFieldRef> mergeTuple;
  std::unordered_map<size_t,std::vector<uint8_t*> > mp;
  int m,cnt,cnt1,cnt2;
};

}  // namespace wing

#endif