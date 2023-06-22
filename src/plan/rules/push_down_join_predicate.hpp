#ifndef SAKURA_PUSHDOWN_JOIN_PREDICATE_H__
#define SAKURA_PUSHDOWN_JOIN_PREDICATE_H__

#include "common/logging.hpp"
#include "plan/expr_utils.hpp"
#include "plan/output_schema.hpp"
#include "plan/plan.hpp"
#include "plan/rules/rule.hpp"

namespace wing {

class PushDownJoinPredicateRule : public OptRule {
 public:
  bool Match(const PlanNode* node) override {
    if (node->type_==PlanType::Join)
    {
        auto t_node = static_cast<const JoinPlanNode*>(node);
        bool flag=false;
        for (auto &&hh:t_node->predicate_.GetVec())
        {
            if (!hh.CheckLeft(t_node->ch_->table_bitset_)&&!hh.CheckRight(t_node->ch_->table_bitset_))
            {
                flag=true;
            }
            else if (!hh.CheckLeft(t_node->ch2_->table_bitset_)&&!hh.CheckRight(t_node->ch2_->table_bitset_))
            {
                flag=true;
            }
        }
        return flag;
    }
    else if (node->type_==PlanType::HashJoin)
    {
        auto t_node = static_cast<const HashJoinPlanNode*>(node);
        bool flag=false;
        for (auto &&hh:t_node->predicate_.GetVec())
        {
            if (!hh.CheckLeft(t_node->ch_->table_bitset_)&&!hh.CheckRight(t_node->ch_->table_bitset_))
            {
                flag=true;
            }
            else if (!hh.CheckLeft(t_node->ch2_->table_bitset_)&&!hh.CheckRight(t_node->ch2_->table_bitset_))
            {
                flag=true;
            }
        }
        return flag;
    }
    return false;
  }
  std::unique_ptr<PlanNode> Transform(std::unique_ptr<PlanNode> node) override {
    if (node->type_==PlanType::Join)
    {
        auto t_node=static_cast<JoinPlanNode*>(node.get());
        PredicateVec v,v1,v2;
        bool flag1=false,flag2=false;
        for (auto &&hh:t_node->predicate_.GetVec())
        {
            if (!hh.CheckLeft(t_node->ch_->table_bitset_)&&!hh.CheckRight(t_node->ch_->table_bitset_))
            {
                flag2=true;
                v2.Append({_trans(hh.expr_->clone()),hh.left_bits_,hh.right_bits_});
            }
            else if (!hh.CheckLeft(t_node->ch2_->table_bitset_)&&!hh.CheckRight(t_node->ch2_->table_bitset_))
            {
                flag1=true;
                v1.Append({_trans(hh.expr_->clone()),hh.left_bits_,hh.right_bits_});
            }
            else
            {
                v.Append({_trans(hh.expr_->clone()),hh.left_bits_,hh.right_bits_});
            }
        }
        if (flag1)
        {
            auto ch=std::make_unique<FilterPlanNode>();
            ch->predicate_.Append(v1);
            ch->output_schema_=t_node->ch_->output_schema_;
            ch->table_bitset_=t_node->ch_->table_bitset_;
            ch->ch_=std::move(t_node->ch_);
            t_node->ch_=std::move(ch);
        }
        if (flag2)
        {
            auto ch2=std::make_unique<FilterPlanNode>();
            ch2->predicate_.Append(v2);
            ch2->output_schema_=t_node->ch2_->output_schema_;
            ch2->table_bitset_=t_node->ch2_->table_bitset_;
            ch2->ch_=std::move(t_node->ch2_);
            t_node->ch2_=std::move(ch2);
        }
        if (flag1||flag2) t_node->predicate_=v.clone();
        return std::move(node);
    }
    else if (node->type_==PlanType::HashJoin)
    {
        auto t_node=static_cast<HashJoinPlanNode*>(node.get());
        PredicateVec v,v1,v2;
        bool flag1=false,flag2=false;
        for (auto &&hh:t_node->predicate_.GetVec())
        {
            if (!hh.CheckLeft(t_node->ch_->table_bitset_)&&!hh.CheckRight(t_node->ch_->table_bitset_))
            {
                flag2=true;
                v2.Append({_trans(hh.expr_->clone()),hh.left_bits_,hh.right_bits_});
            }
            else if (!hh.CheckLeft(t_node->ch2_->table_bitset_)&&!hh.CheckRight(t_node->ch2_->table_bitset_))
            {
                flag1=true;
                v1.Append({_trans(hh.expr_->clone()),hh.left_bits_,hh.right_bits_});
            }
            else
            {
                v.Append({_trans(hh.expr_->clone()),hh.left_bits_,hh.right_bits_});
            }
        }
        if (flag1)
        {
            auto ch=std::make_unique<FilterPlanNode>();
            ch->predicate_.Append(v1);
            ch->output_schema_=t_node->ch_->output_schema_;
            ch->table_bitset_=t_node->ch_->table_bitset_;
            ch->ch_=std::move(t_node->ch_);
            t_node->ch_=std::move(ch);
        }
        if (flag2)
        {
            auto ch2=std::make_unique<FilterPlanNode>();
            ch2->predicate_.Append(v2);
            ch2->output_schema_=t_node->ch2_->output_schema_;
            ch2->table_bitset_=t_node->ch2_->table_bitset_;
            ch2->ch_=std::move(t_node->ch2_);
            t_node->ch2_=std::move(ch2);
        }
        if (flag1||flag2) t_node->predicate_=v.clone();
        return std::move(node);
    }
    DB_ERR("Invalid node.");
  }
 private:
  static std::unique_ptr<BinaryConditionExpr> _trans(std::unique_ptr<Expr> e) {
    auto ret = static_cast<BinaryConditionExpr*>(e.get());
    e.release();
    return std::unique_ptr<BinaryConditionExpr>(ret);
  }
};
}
// namespace wing

#endif