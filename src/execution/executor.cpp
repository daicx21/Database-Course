#include "execution/executor.hpp"

#include "catalog/schema.hpp"
#include "execution/delete_executor.hpp"
#include "execution/filter_executor.hpp"
#include "execution/insert_executor.hpp"
#include "execution/print_executor.hpp"
#include "execution/project_executor.hpp"
#include "execution/seqscan_executor.hpp"
#include "execution/join_executor.hpp"
#include "execution/hashjoin_executor.hpp"
#include "execution/aggregate_executor.hpp"
#include "execution/orderby_executor.hpp"
#include "execution/limit_executor.hpp"
#include "execution/distinct_executor.hpp"
#include "plan/plan.hpp"

namespace wing {

std::unique_ptr<Executor> ExecutorGenerator::Generate(
    const PlanNode* plan, DB& db, txn_id_t txn_id) {
  if (plan == nullptr) {
    throw DBException("Invalid PlanNode.");
  }

  else if (plan->type_ == PlanType::Project) {
    auto project_plan = static_cast<const ProjectPlanNode*>(plan);
    return std::make_unique<ProjectExecutor>(project_plan->output_exprs_,
        project_plan->ch_->output_schema_,
        Generate(project_plan->ch_.get(), db, txn_id));
  }

  else if (plan->type_ == PlanType::Filter) {
    auto filter_plan = static_cast<const FilterPlanNode*>(plan);
    return std::make_unique<FilterExecutor>(filter_plan->predicate_.GenExpr(),
        filter_plan->ch_->output_schema_,
        Generate(filter_plan->ch_.get(), db, txn_id));
  }

  else if (plan->type_ == PlanType::Print) {
    auto print_plan = static_cast<const PrintPlanNode*>(plan);
    return std::make_unique<PrintExecutor>(
        print_plan->values_, print_plan->num_fields_per_tuple_);
  }

  else if (plan->type_ == PlanType::Insert) {
    auto insert_plan = static_cast<const InsertPlanNode*>(plan);
    auto table_schema_index = db.GetDBSchema().Find(insert_plan->table_name_);
    if (!table_schema_index) {
      throw DBException("Cannot find table \'{}\'", insert_plan->table_name_);
    }
    auto& tab = db.GetDBSchema()[table_schema_index.value()];
    auto gen_pk = tab.GetAutoGenFlag()
                      ? db.GetGenPKHandle(txn_id, tab.GetName())
                      : nullptr;
    return std::make_unique<InsertExecutor>(
        db.GetModifyHandle(txn_id, tab.GetName()),
        Generate(insert_plan->ch_.get(), db, txn_id),
        FKChecker(tab.GetFK(), tab, txn_id, db), gen_pk, tab);
  }

  else if (plan->type_ == PlanType::SeqScan) {
    auto seqscan_plan = static_cast<const SeqScanPlanNode*>(plan);
    auto table_schema_index = db.GetDBSchema().Find(seqscan_plan->table_name_);
    if (!table_schema_index) {
      throw DBException("Cannot find table \'{}\'", seqscan_plan->table_name_);
    }
    auto& tab = db.GetDBSchema()[table_schema_index.value()];
    return std::make_unique<SeqScanExecutor>(
        db.GetIterator(txn_id, tab.GetName()),
        seqscan_plan->predicate_.GenExpr(), seqscan_plan->output_schema_);
  }

  else if (plan->type_ == PlanType::Delete) {
    auto delete_plan = static_cast<const DeletePlanNode*>(plan);
    auto table_schema_index = db.GetDBSchema().Find(delete_plan->table_name_);
    if (!table_schema_index) {
      throw DBException("Cannot find table \'{}\'", delete_plan->table_name_);
    }
    auto& tab = db.GetDBSchema()[table_schema_index.value()];
    return std::make_unique<DeleteExecutor>(
        db.GetModifyHandle(txn_id, tab.GetName()),
        Generate(delete_plan->ch_.get(), db, txn_id),
        FKChecker(tab.GetFK(), tab, txn_id, db),
        PKChecker(tab.GetName(), tab.GetHidePKFlag(), txn_id, db), tab);
  }

  else if (plan->type_ == PlanType::Join) {
    auto join_plan = static_cast<const JoinPlanNode*>(plan);
    return std::make_unique<JoinExecutor>(
      join_plan->predicate_.GenExpr(),
      join_plan->ch_->output_schema_,
      join_plan->ch2_->output_schema_,
      join_plan->output_schema_,
      Generate(join_plan->ch_.get(), db, txn_id),
      Generate(join_plan->ch2_.get(), db, txn_id)
    );
  }

  else if (plan->type_ == PlanType::HashJoin) {
    auto hashjoin_plan = static_cast<const HashJoinPlanNode*>(plan);
    return std::make_unique<HashJoinExecutor>(
      hashjoin_plan->predicate_.GenExpr(),
      hashjoin_plan->ch_->output_schema_,
      hashjoin_plan->ch2_->output_schema_,
      hashjoin_plan->output_schema_,
      hashjoin_plan->left_hash_exprs_,
      hashjoin_plan->right_hash_exprs_,
      Generate(hashjoin_plan->ch_.get(), db, txn_id),
      Generate(hashjoin_plan->ch2_.get(), db, txn_id)
    );
  }

  else if (plan->type_ == PlanType::Aggregate) {
    auto aggregate_plan = static_cast<const AggregatePlanNode*>(plan);
    return std::make_unique<AggregateExecutor>(
      aggregate_plan->group_predicate_.GenExpr(),
      aggregate_plan->ch_->output_schema_,
      aggregate_plan->output_schema_,
      aggregate_plan->group_by_exprs_,
      aggregate_plan->output_exprs_,
      Generate(aggregate_plan->ch_.get(), db, txn_id)
    );
  }

  else if (plan->type_ == PlanType::Order) {
    auto order_plan = static_cast<const OrderByPlanNode*>(plan);
    return std::make_unique<OrderByExecutor>(
      order_plan->ch_->output_schema_,
      order_plan->output_schema_,
      order_plan->order_by_exprs_,
      order_plan->order_by_offset_,
      Generate(order_plan->ch_.get(), db, txn_id)
    );
  }

  else if (plan->type_ == PlanType::Limit) {
    auto limit_plan = static_cast<const LimitPlanNode*>(plan);
    return std::make_unique<LimitExecutor>(
      limit_plan->limit_size_,
      limit_plan->offset_,
      Generate(limit_plan->ch_.get(), db, txn_id)
    );
  }

  else if (plan->type_ == PlanType::Distinct) {
    auto distinct_plan = static_cast<const DistinctPlanNode*>(plan);
    return std::make_unique<DistinctExecutor>(
      distinct_plan->output_schema_,
      Generate(distinct_plan->ch_.get(), db, txn_id)
    );
  }

  else if (plan->type_ == PlanType::RangeScan) {
    auto rangescan_plan = static_cast<const RangeScanPlanNode*>(plan);
    return std::make_unique<SeqScanExecutor>(
      db.GetRangeIterator(txn_id,rangescan_plan->table_name_,std::tuple<std::string_view,bool,bool>(rangescan_plan->range_l_.first.GetView(),rangescan_plan->range_l_.first.type_==FieldType::EMPTY,rangescan_plan->range_l_.second),std::tuple<std::string_view,bool,bool>(rangescan_plan->range_r_.first.GetView(),rangescan_plan->range_r_.first.type_==FieldType::EMPTY,rangescan_plan->range_r_.second)),
      rangescan_plan->predicate_.GenExpr(),
      rangescan_plan->output_schema_
    );
  }

  throw DBException("Unsupported plan node.");
}

}  // namespace wing