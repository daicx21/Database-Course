#ifndef SAKURA_ORDERBY_EXECUTOR_H__
#define SAKURA_ORDERBY_EXECUTOR_H__

#include <iostream>

#include "execution/executor.hpp"
#include "plan/output_schema.hpp"
#include "type/vector.hpp"

namespace wing {

class OrderByExecutor : public Executor {
 public:
  OrderByExecutor(
    const OutputSchema& input_schema,
	const OutputSchema& output_schema,
	const std::vector<std::pair<RetType,bool> > &order_by_exprs,
	size_t order_by_offset_,
	std::unique_ptr<Executor> ch):
        ch_(std::move(ch)), input_schema_(input_schema), output_schema_(output_schema) {
			m=order_by_offset_;
            order_by_exprs_=order_by_exprs;
            ans=TupleStore(input_schema);
		}
  void Init() override { 
  	ch_->Init();ans.Clear();
	for (auto ch_ret=ch_->Next();ch_ret;ch_ret=ch_->Next()) ans.Append(ch_ret.Data());
    v.resize(ans.GetPointerVec().size());
    for (size_t i=0;i<v.size();i++) v[i]=i;
    std::sort(v.begin(),v.end(),[&](const size_t &x, const size_t &y) -> bool {
        const StaticFieldRef *h1=(const StaticFieldRef *)(ans.GetPointerVec()[x]);
        const StaticFieldRef *h2=(const StaticFieldRef *)(ans.GetPointerVec()[y]);
        for (size_t i=0;i<m;i++)
        {
            bool flag=(order_by_exprs_[i].second^1);
            if (order_by_exprs_[i].first==RetType::STRING)
            {
                std::string_view hh1=h1->ReadStringView(),hh2=h2->ReadStringView();
                if (hh1!=hh2) return (hh1<hh2)^flag;
            }
            else if (order_by_exprs_[i].first==RetType::FLOAT)
            {
                double hh1=h1->ReadFloat(),hh2=h2->ReadFloat();
                if (hh1!=hh2) return (hh1<hh2)^flag;
            }
            else
            {
                int64_t hh1=h1->ReadInt(),hh2=h2->ReadInt();
                if (hh1!=hh2) return (hh1<hh2)^flag;
            }
            h1++;h2++;
        }
        return x<y;
    });
	it=0;
  }
  InputTuplePtr Next() override {
    if (it==v.size()) return {};
    size_t id=v[it++];
    return InputTuplePtr(ans.GetPointerVec()[id]+m*8);
  }

 private:
  std::vector<std::pair<RetType,bool> > order_by_exprs_;
  std::unique_ptr<Executor> ch_;
  const OutputSchema input_schema_;
  const OutputSchema output_schema_;
  TupleStore ans;
  std::vector<size_t> v;
  size_t m,it;
};

}  // namespace wing

#endif