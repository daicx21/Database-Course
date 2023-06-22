#ifndef SAKURA_CONVERT_TO_RANGE_SCAN_H__
#define SAKURA_CONVERT_TO_RANGE_SCAN_H__

#include "catalog/db.hpp"
#include "common/logging.hpp"
#include "parser/expr.hpp"
#include "plan/expr_utils.hpp"
#include "plan/output_schema.hpp"
#include "plan/plan.hpp"
#include "plan/rules/rule.hpp"
#include <iostream>

namespace wing {

class ConvertToRangeScanRule: public OptRule {
 public:
  ConvertToRangeScanRule(DB& db_):db(db_) {}
  bool Match(const PlanNode* node) override {
    if (node->type_==PlanType::SeqScan)
    {
        auto t_node=static_cast<const SeqScanPlanNode*>(node);
        for (auto &&hh:t_node->predicate_.GetVec())
        {
            if (hh.GetLeftColId()!=std::nullopt)
            {
                auto now=static_cast<const ColumnExpr*>(hh.expr_->ch0_.get());
                const auto &tab=db.GetDBSchema()[db.GetDBSchema().Find(t_node->table_name_).value()];
                std::string pk_name=tab.GetPrimaryKeySchema().name_;
                if (pk_name==now->column_name_)
                {
                    auto val=hh.GetRightExprType();
                    if (val==ExprType::LITERAL_FLOAT||val==ExprType::LITERAL_INTEGER||val==ExprType::LITERAL_STRING)
                    {
                        tp=val;
                        return true;
                    }
                }
            }
            if (hh.GetRightColId()!=std::nullopt)
            {
                auto now=static_cast<const ColumnExpr*>(hh.expr_->ch1_.get());
                const auto &tab=db.GetDBSchema()[db.GetDBSchema().Find(t_node->table_name_).value()];
                std::string pk_name=tab.GetPrimaryKeySchema().name_;
                if (pk_name==now->column_name_)
                {
                    auto val=hh.GetLeftExprType();
                    if (val==ExprType::LITERAL_FLOAT||val==ExprType::LITERAL_INTEGER||val==ExprType::LITERAL_STRING)
                    {
                        tp=val;
                        return true;
                    }
                }
            }
        }
    }
    return false;
  }
  std::unique_ptr<PlanNode> Transform(std::unique_ptr<PlanNode> node) override {
    if (node->type_==PlanType::SeqScan)
    {
        auto t_node=static_cast<const SeqScanPlanNode*>(node.get());
        Field L,R;
        bool empty1=true,empty2=true,flag1=false,flag2=false;
        for (auto &&hh:t_node->predicate_.GetVec())
        {
            Field cur;
            if (hh.GetLeftColId()!=std::nullopt)
            {
                auto now=static_cast<const ColumnExpr*>(hh.expr_->ch0_.get());
                const auto &tab=db.GetDBSchema()[db.GetDBSchema().Find(t_node->table_name_).value()];
                std::string pk_name=tab.GetPrimaryKeySchema().name_;
                if (pk_name==now->column_name_&&hh.GetRightExprType()==tp)
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
                    if (empty1)
                    {
                        empty1=false;
                        L=cur;flag1=false;
                    }
                    else if (cur>=L) L=cur,flag1=false;
                }
                else if (hh.expr_->op_==OpType::LT)
                {
                    if (empty2)
                    {
                        empty2=false;
                        R=cur;flag2=false;
                    }
                    else if (cur<=R) R=cur,flag2=false;
                }
                else if (hh.expr_->op_==OpType::GEQ)
                {
                    if (empty1)
                    {
                        empty1=false;
                        L=cur;flag1=true;
                    }
                    else if (cur>L) L=cur,flag1=true;
                }
                else if (hh.expr_->op_==OpType::LEQ)
                {
                    if (empty2)
                    {
                        empty2=false;
                        R=cur;flag2=true;
                    }
                    else if (cur<R) R=cur,flag2=true;
                }
                else if (hh.expr_->op_==OpType::EQ)
                {
                    if (empty1)
                    {
                        empty1=false;
                        L=cur;flag1=true;
                    }
                    else if (cur>L) L=cur,flag1=true;
                    if (empty2)
                    {
                        empty2=false;
                        R=cur;flag2=true;
                    }
                    else if (cur<R) R=cur,flag2=true;
                }
            }
            if (hh.GetRightColId()!=std::nullopt)
            {
                auto now=static_cast<const ColumnExpr*>(hh.expr_->ch1_.get());
                const auto &tab=db.GetDBSchema()[db.GetDBSchema().Find(t_node->table_name_).value()];
                std::string pk_name=tab.GetPrimaryKeySchema().name_;
                if (pk_name==now->column_name_&&hh.GetLeftExprType()==tp)
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
                        std::string str=ch0->literal_value_;
                        cur=Field::CreateString(FieldType::VARCHAR,std::string_view(str));
                    }
                }
                else continue;
                if (hh.expr_->op_==OpType::LT)
                {
                    if (empty1)
                    {
                        empty1=false;
                        L=cur;flag1=false;
                    }
                    else if (cur>=L) L=cur,flag1=false;
                }
                else if (hh.expr_->op_==OpType::GT)
                {
                    if (empty2)
                    {
                        empty2=false;
                        R=cur;flag2=false;
                    }
                    else if (cur<=R) R=cur,flag2=false;
                }
                else if (hh.expr_->op_==OpType::LEQ)
                {
                    if (empty1)
                    {
                        empty1=false;
                        L=cur;flag1=true;
                    }
                    else if (cur>L) L=cur,flag1=true;
                }
                else if (hh.expr_->op_==OpType::GEQ)
                {
                    if (empty2)
                    {
                        empty2=false;
                        R=cur;flag2=true;
                    }
                    else if (cur<R) R=cur,flag2=true;
                }
                else if (hh.expr_->op_==OpType::EQ)
                {
                    if (empty1)
                    {
                        empty1=false;
                        L=cur;flag1=true;
                    }
                    else if (cur>L) L=cur,flag1=true;
                    if (empty2)
                    {
                        empty2=false;
                        R=cur;flag2=true;
                    }
                    else if (cur<R) R=cur,flag2=true;
                }
            }
        }
        auto ch=std::make_unique<RangeScanPlanNode>();
        ch->output_schema_=t_node->output_schema_;
        ch->table_bitset_=t_node->table_bitset_;
        ch->table_name_=t_node->table_name_;
        ch->predicate_=t_node->predicate_.clone();
        ch->range_l_=std::pair(L,flag1);
        ch->range_r_=std::pair(R,flag2);
        return std::move(ch);
    }
    DB_ERR("Invalid node.");
  }
 private:
  DB& db;
  ExprType tp;
};
}
// namespace wing

#endif