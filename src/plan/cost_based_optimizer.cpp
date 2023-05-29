#include <bits/stdint-intn.h>
#include <cstddef>
#include <queue>

#include "plan/optimizer.hpp"
#include "plan/card_est.hpp"
#include "plan/plan_expr.hpp"
#include "plan/cost_model.hpp"
#include "plan/rules/push_down_filter.hpp"
#include "plan/rules/push_down_join_predicate.hpp"
#include "plan/rules/convert_to_range_scan_rule.hpp"
#include "rules/convert_to_hash_join.hpp"

namespace wing {

std::unique_ptr<PlanNode> Apply(
    std::unique_ptr<PlanNode> plan,
    const std::vector<std::unique_ptr<OptRule>>& rules) {
  while (true) {
    bool flag = false;
    for (auto& a : rules)
      if (a->Match(plan.get())) {
        plan = a->Transform(std::move(plan));
        flag = true;
      }
    if (!flag)
      break;
  }

  if (plan->ch_ != nullptr) {
    plan->ch_ = Apply(std::move(plan->ch_), rules);
  }
  if (plan->ch2_ != nullptr) {
    plan->ch2_ = Apply(std::move(plan->ch2_), rules);
  }
  return plan;
}

size_t GetTableNum(const PlanNode* plan) {
  /* We don't want to consider values clause in cost based optimizer. */
  if (plan->type_ == PlanType::Print) {
    return 10000;
  }
  
  if (plan->type_ == PlanType::SeqScan) {
    return 1;
  }

  size_t ret = 0;
  if (plan->ch2_ != nullptr) {
    ret += GetTableNum(plan->ch_.get());
    ret += GetTableNum(plan->ch2_.get());
  } else if (plan->ch_ != nullptr) {
    ret += GetTableNum(plan->ch_.get());
  }
  return ret;
}

bool CheckIsAllJoin(const PlanNode* plan) {
  if (plan->type_ == PlanType::Print || plan->type_ == PlanType::SeqScan || plan->type_ == PlanType::RangeScan) {
    return true;
  }
  if (plan->type_ != PlanType::Join) {
    return false;
  }
  return CheckIsAllJoin(plan->ch_.get()) && CheckIsAllJoin(plan->ch2_.get());
}

bool CheckHasStat(const PlanNode* plan, const DB& db) {
  if (plan->type_ == PlanType::Print) {
    return false;
  }
  if (plan->type_ == PlanType::SeqScan) {
    auto stat = db.GetTableStat(static_cast<const SeqScanPlanNode*>(plan)->table_name_);
    return stat != nullptr;
  }
  if (plan->type_ == PlanType::RangeScan) {
    auto stat = db.GetTableStat(static_cast<const RangeScanPlanNode*>(plan)->table_name_);
    return stat != nullptr;
  }
  if (plan->type_ != PlanType::Join) {
    return false;
  }
  return CheckHasStat(plan->ch_.get(), db) && CheckHasStat(plan->ch2_.get(), db);
}

/** 
 * Check whether we can use cost based optimizer. 
 * For simplicity, we only use cost based optimizer when:
 * (1) The root plan node is Project, and there is only one Project.
 * (2) The other plan nodes can only be Join or SeqScan or RangeScan.
 * (3) The number of tables is <= 10. 
 * (4) All tables have statistics.
*/
bool CheckCondition(const PlanNode* plan, const DB& db) {
  if (GetTableNum(plan) > 10) return false;
  if (plan->type_ != PlanType::Project && plan->type_ != PlanType::Aggregate) return false;
  if (!CheckIsAllJoin(plan->ch_.get())) return false;
  return CheckHasStat(plan->ch_.get(), db);
}
static int n;
static CardEstimator::Summary h[20];
static PredicateVec v;
std::unique_ptr<PlanNode> P[20];
void dfs1(const PlanNode *plan,const DB &db)
{
  if (plan->type_==PlanType::Join)
  {
    auto node=static_cast<const JoinPlanNode*>(plan);
    v.Append(node->predicate_);
  }
  if (plan->ch2_!=nullptr)
  {
    dfs1(plan->ch_.get(),db);
    dfs1(plan->ch2_.get(),db);
  }
  else if (plan->ch_!=nullptr) dfs1(plan->ch_.get(),db);
}
std::unique_ptr<PlanNode> dfs2(std::unique_ptr<PlanNode> plan,DB &db)
{
  if (plan->type_==PlanType::SeqScan)
  {
    auto node=static_cast<const SeqScanPlanNode*>(plan.get());
    h[n++]=CardEstimator::EstimateTable(node->table_name_,node->predicate_,node->output_schema_,db);
    P[n-1]=std::move(plan);
    return plan;
  }
  if (plan->type_==PlanType::RangeScan)
  {
    auto node=static_cast<const RangeScanPlanNode*>(plan.get());
    h[n++]=CardEstimator::EstimateTable(node->table_name_,node->predicate_,node->output_schema_,db);
    P[n-1]=std::move(plan);
    return plan;
  }
  if (plan->ch2_!=nullptr)
  {
    dfs2(std::move(plan->ch_),db);
    dfs2(std::move(plan->ch2_),db);
  }
  else if (plan->ch_!=nullptr) dfs2(std::move(plan->ch_),db);
  return plan;
}
static double f[4096];
static int cnt[4096],id[4096];
static CardEstimator::Summary g[4096];
std::unique_ptr<PlanNode> GetPlan(int S,DB &db)
{
  if (cnt[S]==1) return std::move(P[id[S]]);
  std::unique_ptr<PlanNode> h1=GetPlan(id[S],db),h2=GetPlan(S^id[S],db);
  auto res=std::make_unique<JoinPlanNode>();
  res->ch_=std::move(h1);
  res->ch2_=std::move(h2);
  res->output_schema_=OutputSchema::Concat(res->ch_->output_schema_,res->ch2_->output_schema_);
  res->output_schema_.SetRaw(false);
  res->table_bitset_=res->ch_->table_bitset_|res->ch2_->table_bitset_;
  if (S==(1<<n)-1) res->predicate_=v.clone();
  return res;
}
std::unique_ptr<PlanNode> CostBasedOptimizer::Optimize(std::unique_ptr<PlanNode> plan, DB& db)
{
  if (CheckCondition(plan.get(), db)) {
    std::vector<std::unique_ptr<OptRule>> R;
    R.push_back(std::make_unique<PushDownFilterRule>());
    R.push_back(std::make_unique<PushDownJoinPredicateRule>());
    R.push_back(std::make_unique<ConvertToRangeScanRule>(db));
    plan=Apply(std::move(plan),R);
    n=0;v.GetVec().clear();
    dfs1(plan.get(),db);
    plan=dfs2(std::move(plan),db);
    for (int64_t i=1;i<(1<<n);i++) cnt[i]=cnt[i>>1]+(i&1);
    for (int i=1;i<(1<<n);i++)
    {
      if (cnt[i]==1)
      {
        for (int j=0;j<n;j++) if ((i>>j)&1) id[i]=j;
        g[i]=h[id[i]];f[i]=CostCalculator::SeqScanCost(g[i].size_);
        continue;
      }
      f[i]=1e100;
      for (int j=(i-1)&i;j;j=(j-1)&i)
      {
        bool flag=false;
        std::unordered_map<uint32_t,uint32_t> mp;
        for (size_t k=0;k<g[j].distinct_rate_.size();k++) mp[g[j].distinct_rate_[k].first]=1;
        for (size_t k=0;k<g[i^j].distinct_rate_.size();k++) mp[g[i^j].distinct_rate_[k].first]=2;
        for (auto &hh:v.GetVec())
        {
          if (hh.GetLeftColId()!=std::nullopt&&hh.GetRightColId()!=std::nullopt)
          {
            uint32_t h1=hh.GetLeftColId().value(),h2=hh.GetRightColId().value();
            if (((mp[h1]==1&&mp[h2]==2)||(mp[h1]==2&&mp[h2]==1))&&hh.IsEq()) flag=true;
          }
        }
        double res=f[j]+f[i^j];
        if (flag) res+=CostCalculator::HashJoinCost(g[j].size_,g[i^j].size_);
        else res+=CostCalculator::NestloopJoinCost(g[j].size_,f[i^j]);
        if (res<f[i]) f[i]=res,g[i]=CardEstimator::EstimateJoinEq(v,g[j],g[i^j]),id[i]=j;
      }
    }
    auto res=GetPlan((1<<n)-1,db);
    plan->ch_=std::move(res);
    R.clear();
    R.push_back(std::make_unique<PushDownFilterRule>());
    R.push_back(std::make_unique<PushDownJoinPredicateRule>());
    R.push_back(std::make_unique<ConvertToHashJoinRule>());
    R.push_back(std::make_unique<ConvertToRangeScanRule>(db));
    plan=Apply(std::move(plan),R);
    return plan;
  }
  else
  {
    std::vector<std::unique_ptr<OptRule>> R;
    R.push_back(std::make_unique<PushDownFilterRule>());
    R.push_back(std::make_unique<PushDownJoinPredicateRule>());
    R.push_back(std::make_unique<ConvertToHashJoinRule>());
    R.push_back(std::make_unique<ConvertToRangeScanRule>(db));
    plan=Apply(std::move(plan),R);
  }
  return plan;
}

}  // namespace wing
