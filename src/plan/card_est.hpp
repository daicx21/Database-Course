#ifndef SAKURA_CARD_EST_H__
#define SAKURA_CARD_EST_H__

#include <iostream>
#include <map>
#include <string>
#include "catalog/db.hpp"
#include "parser/expr.hpp"
#include "plan/plan_expr.hpp"
#include "plan/output_schema.hpp"
#include "type/field.hpp"
#include "type/field_type.hpp"

namespace wing {

class CardEstimator {
 public:
  
  // Necessary data for a group of tables. 
  class Summary {
    public:
      double size_{0};
      std::vector<std::pair<int, double>> distinct_rate_;
  };

  // Use DB statistics to estimate the size of the output of seq scan.
  // We assume that columns are uniformly distributed and independent.
  // You should consider predicates which contain two operands and one is a constant.
  // There are some cases:
  // (1) A.a = 1; 1 = A.a; Use CountMinSketch.
  // (2) A.a > 1; 1 > A.a; or A.a <= 1; Use the maximum element and the minimum element of the table.
  // (3) You should ignore other predicates, such as A.a * 2 + A.b < 1000 and A.a < A.b.
  // (4) 1 > 2; Return 0. You can ignore it, because it should be filtered before optimization.
  // You should check the type of each column and write codes for each case, unfortunately.

  inline static bool check(ExprType tp) { return (tp==ExprType::LITERAL_FLOAT||tp==ExprType::LITERAL_INTEGER||tp==ExprType::LITERAL_STRING); }

  static Summary EstimateTable(std::string_view table_name, const PredicateVec& predicates, const OutputSchema& schema, DB& db) {
    Summary ret;
    auto stat=db.GetTableStat(table_name);
    auto index=db.GetDBSchema().Find(table_name);
    auto &tab=db.GetDBSchema()[index.value()];
    int n=schema.Size();
    for (int i=0;i<n;i++)
    {
      uint32_t I=tab.Find(schema[i].column_name_).value();
      ret.distinct_rate_.push_back({schema[i].id_,stat->GetDistinctRate(I)});
    }
    Field L[n],R[n];
    bool empty1[n],empty2[n],flag1[n],flag2[n];
    for (int i=0;i<n;i++) empty1[i]=empty2[i]=true;
    std::unordered_map<uint32_t,int> mp;
    for (int i=0;i<n;i++) mp[ret.distinct_rate_[i].first]=i;
    for (auto &hh:predicates.GetVec())
    {
      Field cur;
      if (hh.GetLeftColId()!=std::nullopt)
      {
        int id=mp[hh.GetLeftColId().value()];
        ExprType tp=hh.GetRightExprType();
        if (check(tp))
        {
          if (tp==ExprType::LITERAL_FLOAT)
          {
            auto ch1=static_cast<const LiteralFloatExpr*>(hh.expr_->ch1_.get());
            cur=Field::CreateFloat(FieldType::FLOAT64,8,ch1->literal_value_);
          }
          else if (tp==ExprType::LITERAL_INTEGER)
          {
            auto ch1=static_cast<const LiteralIntegerExpr*>(hh.expr_->ch1_.get());
            cur=Field::CreateInt(FieldType::INT64,8,ch1->literal_value_);
          }
          else
          {
            auto ch1=static_cast<const LiteralStringExpr*>(hh.expr_->ch1_.get());
            std::string str=ch1->literal_value_;
            cur=Field::CreateString(FieldType::VARCHAR,std::string_view(str));
          }
        }
        else continue;
        if (hh.expr_->op_==OpType::GT)
        {
          if (empty1[id]) empty1[id]=false,L[id]=cur,flag1[id]=false;
          else if (cur>=L[id]) L[id]=cur,flag1[id]=false;
        }
        else if (hh.expr_->op_==OpType::LT)
        {
          if (empty2[id]) empty2[id]=false,R[id]=cur,flag2[id]=false;
          else if (cur<=R[id]) R[id]=cur,flag2[id]=false;
        }
        else if (hh.expr_->op_==OpType::GEQ)
        {
          if (empty1[id]) empty1[id]=false,L[id]=cur,flag1[id]=true;
          else if (cur>L[id]) L[id]=cur,flag1[id]=true;
        }
        else if (hh.expr_->op_==OpType::LEQ)
        {
          if (empty2[id]) empty2[id]=false,R[id]=cur,flag2[id]=true;
          else if (cur<R[id]) R[id]=cur,flag2[id]=true;
        }
        else if (hh.expr_->op_==OpType::EQ)
        {
          if (empty1[id]) empty1[id]=false,L[id]=cur,flag1[id]=true;
          else if (cur>L[id]) L[id]=cur,flag1[id]=true;
          if (empty2[id]) empty2[id]=false,R[id]=cur,flag2[id]=true;
          else if (cur<R[id]) R[id]=cur,flag2[id]=true;
        }
      }
      if (hh.GetRightColId()!=std::nullopt)
      {
        int id=mp[hh.GetRightColId().value()];
        ExprType tp=hh.GetLeftExprType();
        if (check(tp))
        {
          if (tp==ExprType::LITERAL_FLOAT)
          {
            auto ch0=static_cast<const LiteralFloatExpr*>(hh.expr_->ch0_.get());
            cur=Field::CreateFloat(FieldType::FLOAT64,8,ch0->literal_value_);
          }
          else if (tp==ExprType::LITERAL_INTEGER)
          {
            auto ch0=static_cast<const LiteralIntegerExpr*>(hh.expr_->ch0_.get());
            cur=Field::CreateInt(FieldType::INT64,8,ch0->literal_value_);
          }
          else
          {
            auto ch0=static_cast<const LiteralStringExpr*>(hh.expr_->ch0_.get());
            cur=Field::CreateString(FieldType::VARCHAR,std::string_view(ch0->literal_value_));
          }
        }
        else continue;
        if (hh.expr_->op_==OpType::LT)
        {
          if (empty1[id]) empty1[id]=false,L[id]=cur,flag1[id]=false;
          else if (cur>=L[id]) L[id]=cur,flag1[id]=false;
        }
        else if (hh.expr_->op_==OpType::GT)
        {
          if (empty2[id]) empty2[id]=false,R[id]=cur,flag2[id]=false;
          else if (cur<=R[id]) R[id]=cur,flag2[id]=false;
        }
        else if (hh.expr_->op_==OpType::LEQ)
        {
          if (empty1[id]) empty1[id]=false,L[id]=cur,flag1[id]=true;
          else if (cur>L[id]) L[id]=cur,flag1[id]=true;
        }
        else if (hh.expr_->op_==OpType::GEQ)
        {
          if (empty2[id]) empty2[id]=false,R[id]=cur,flag2[id]=true;
          else if (cur<R[id]) R[id]=cur,flag2[id]=true;
        }
        else if (hh.expr_->op_==OpType::EQ)
        {
          if (empty1[id]) empty1[id]=false,L[id]=cur,flag1[id]=true;
          else if (cur>L[id]) L[id]=cur,flag1[id]=true;
          if (empty2[id]) empty2[id]=false,R[id]=cur,flag2[id]=true;
          else if (cur<R[id]) R[id]=cur,flag2[id]=true;
        }
      }
    }
    double hh=1;
    ret.size_=stat->GetTupleNum();
    if (stat->GetTupleNum()==0) return ret;
    double cnt[n];
    for (uint32_t i=0;i<n;i++)
    {
      uint32_t I=tab.Find(schema[i].column_name_).value();
      if (empty1[i]&&empty2[i]) { cnt[i]=stat->GetDistinctRate(I)*ret.size_;continue; }
      Field l=L[i],r=R[i];
      if (empty1[i]) l=stat->GetMin(I),flag1[i]=true;
      if (empty2[i]) r=stat->GetMax(I),flag2[i]=true;
      if (l<=r&&r<=l)
      {
        if (!flag1[i]||!flag2[i]) { ret.size_=0;return ret; }
        double sum=stat->GetCountMinSketch(I).GetFreqCount(l.GetView());
        if (sum==0) { ret.size_=0;return ret; }
        cnt[i]=1;
        hh*=sum/ret.size_;
        continue;
      }
      if ((schema[i].type_==FieldType::INT32||schema[i].type_==FieldType::INT64)&&(r.ReadInt()-l.ReadInt()<=100))
      {
        double sum=0;
        for (int64_t j=l.ReadInt()+1-flag1[i];j<=r.ReadInt()-1+flag2[i];j++)
        {
          double hh=stat->GetCountMinSketch(I).GetFreqCount(Field::CreateInt(FieldType::INT64,8,j).GetView());
          if (hh>0) cnt[i]+=1;
          sum+=hh;
        }
        if (cnt[i]==0) { ret.size_=0;return ret; }
        hh*=sum/ret.size_;
        continue;
      }
      if (l>r) { ret.size_=0;return ret; }
      double sum;
      if (schema[i].type_==FieldType::FLOAT64) sum=(r.ReadFloat()-l.ReadFloat())/(stat->GetMax(I).ReadFloat()-stat->GetMin(I).ReadFloat());
      else if (schema[i].type_==FieldType::INT32||schema[i].type_==FieldType::INT64) sum=(double)(r.ReadInt()-l.ReadInt())/(double)(stat->GetMax(I).ReadInt()-stat->GetMin(I).ReadInt());
      else sum=1;
      cnt[i]=sum*stat->GetDistinctRate(I)*ret.size_;
      hh*=sum;
    }
    ret.size_*=hh;
    for (uint32_t i=0;i<n;i++) ret.distinct_rate_[i].second=std::max(std::min(cnt[i]/ret.size_,1.0),1.0/ret.size_);
    return ret;
  }

  // Only consider about equality predicates such as 'A.a = B.b'
  // For other join predicates, you should ignore them.
  static Summary EstimateJoinEq(
    const PredicateVec& predicates, 
    const Summary& build, 
    const Summary& probe
  ) {
    Summary ret;
    ret.size_=build.size_*probe.size_;
    for (size_t i=0;i<build.distinct_rate_.size();i++) ret.distinct_rate_.push_back(build.distinct_rate_[i]);
    for (size_t i=0;i<probe.distinct_rate_.size();i++) ret.distinct_rate_.push_back(probe.distinct_rate_[i]);
    if (ret.size_==0) return ret;
    std::unordered_map<uint32_t,uint32_t> mp,id,id1,id2;
    for (size_t i=0;i<build.distinct_rate_.size();i++) mp[build.distinct_rate_[i].first]=1,id1[build.distinct_rate_[i].first]=i;
    for (size_t i=0;i<probe.distinct_rate_.size();i++) mp[probe.distinct_rate_[i].first]=2,id2[probe.distinct_rate_[i].first]=i;
    for (size_t i=0;i<ret.distinct_rate_.size();i++) id[ret.distinct_rate_[i].first]=i;
    for (auto &hh:predicates.GetVec())
    {
      if (!hh.IsEq()) continue;
      if (hh.GetLeftColId()!=std::nullopt&&hh.GetRightColId()!=std::nullopt)
      {
        uint32_t h1=hh.GetLeftColId().value(),h2=hh.GetRightColId().value();
        if (mp[h1]==1&&mp[h2]==2)
        {
          double hh1=build.distinct_rate_[id1[h1]].second,hh2=probe.distinct_rate_[id2[h2]].second;
          ret.distinct_rate_[id[h1]].second=ret.distinct_rate_[id[h2]].second=std::min(hh1,hh2);
          ret.size_/=std::max(build.size_*hh1,probe.size_*hh2);
          break;
        }
        else if (mp[h1]==2&&mp[h2]==1)
        {
          double hh1=build.distinct_rate_[id1[h2]].second,hh2=probe.distinct_rate_[id2[h1]].second;
          ret.distinct_rate_[id[h1]].second=ret.distinct_rate_[id[h2]].second=std::min(hh1,hh2);
          ret.size_/=std::max(build.size_*hh1,probe.size_*hh2);
          break;
        }
      }
    }
    return ret;
  }
};

}

#endif