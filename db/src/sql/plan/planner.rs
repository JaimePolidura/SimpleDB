use crate::selection::Selection;
use crate::sql::expression::Expression;
use crate::sql::plan::plan_step::Plan;
use crate::sql::plan::scan_type::ScanType;
use crate::sql::plan::steps::primary_exact_scan_step::PrimaryExactScanStep;
use crate::sql::plan::steps::filter_step::FilterStep;
use crate::sql::plan::steps::full_scan_step::FullScanStep;
use crate::sql::plan::steps::limit_step::LimitStep;
use crate::sql::plan::steps::range_scan_step::RangeScanStep;
use crate::sql::statement::{DeleteStatement, Limit, SelectStatement, UpdateStatement};
use crate::table::table::Table;
use shared::SimpleDbError::{FullScanNotAllowed, MalformedQuery, RangeScanNotAllowed};
use shared::{SimpleDbError, SimpleDbOptions};
use std::sync::Arc;
use storage::transactions::transaction::Transaction;
use crate::sql::plan::scan_type::ScanType::{MergeIntersection, MergeUnion};
use crate::sql::plan::scan_type_analyzer::ScanTypeAnalyzer;
use crate::sql::plan::steps::merge_intersection_scan_type::MergeIntersectionScanType;
use crate::sql::plan::steps::merge_union_scan_step::MergeUnionScanStep;
use crate::sql::plan::steps::secondary_scan_type::SecondaryExactScanType;

pub struct Planner {
    options: Arc<SimpleDbOptions>
}

impl Planner {
    pub fn create(options: Arc<SimpleDbOptions>) -> Planner {
        Planner {
            options: options.clone()
        }
    }

    pub fn plan_select(
        &self,
        table: &Arc<Table>,
        select_statement: SelectStatement,
        transaction: &Transaction
    ) -> Result<Plan, SimpleDbError> {
        let scan_type = self.get_scan_type(
            &select_statement.where_expr,
            table,
        )?;
        let mut last_step = self.build_scan_step(scan_type, transaction, select_statement.selection, table)?;

        if let Some(where_expr) = select_statement.where_expr {
            last_step = FilterStep::create(where_expr, last_step);
        }
        if !matches!(select_statement.limit, Limit::None) {
            last_step = LimitStep::create(select_statement.limit, last_step);
        }

        Ok(last_step)
    }

    pub fn plan_update(
        &self,
        table: &Arc<Table>,
        update_statement: &UpdateStatement,
        transaction: &Transaction,
    ) -> Result<Plan, SimpleDbError> {
        let scan_type = self.get_scan_type(
            &update_statement.where_expr,
            table
        )?;
        let updated_values = update_statement.get_updated_values();
        let mut last_step = self.build_scan_step(scan_type, transaction, updated_values, table)?;

        if let Some(where_expr) = &update_statement.where_expr {
            last_step = FilterStep::create(where_expr.clone(), last_step);
        }

        Ok(last_step)
    }

    pub fn plan_delete(
        &self,
        table: &Arc<Table>,
        select_statement: DeleteStatement,
        transaction: &Transaction
    ) -> Result<Plan, SimpleDbError> {
        let scan_type = self.get_scan_type(
            &select_statement.where_expr,
            table,
        )?;
        let mut last_step = self.build_scan_step(scan_type, transaction, Selection::All, table)?;

        if let Some(where_expr) = select_statement.where_expr {
            last_step = FilterStep::create(where_expr, last_step);
        }
        if !matches!(select_statement.limit, Limit::None) {
            last_step = LimitStep::create(select_statement.limit, last_step);
        }

        Ok(last_step)
    }

    fn build_scan_step(
        &self,
        scan_type: ScanType,
        transaction: &Transaction,
        selection: Selection,
        table: &Arc<Table>,
    ) -> Result<Plan, SimpleDbError> {
        match scan_type {
            ScanType::ExactSecondary(column, exact_id_expr) => {
                SecondaryExactScanType::create(table.clone(), &column, exact_id_expr.serialize(), transaction, selection)
            },
            ScanType::ExactPrimary(exact_id_expr) => {
                PrimaryExactScanStep::create(table.clone(), exact_id_expr.serialize(), selection, transaction)
            },
            ScanType::Range(range) => {
                RangeScanStep::create(table.clone(), selection, transaction, range)
            },
            ScanType::Full => {
                FullScanStep::create(table.clone(), selection, transaction)
            },
            ScanType::MergeUnion(left_scan_type, right_scan_type) => {
                let left_scan_step = self.build_scan_step(*left_scan_type, transaction, selection.clone(), table)?;
                let right_scan_step = self.build_scan_step(*right_scan_type, transaction, selection.clone(), table)?;
                MergeUnionScanStep::create(left_scan_step, right_scan_step)
            }
            ScanType::MergeIntersection(left_scan_type, right_scan_type) => {
                let left_scan_step = self.build_scan_step(*left_scan_type, transaction, selection.clone(), table)?;
                let right_scan_step = self.build_scan_step(*right_scan_type, transaction, selection.clone(), table)?;
                MergeIntersectionScanType::create(left_scan_step, right_scan_step)
            }
        }
    }

    fn get_scan_type(
        &self,
        expression: &Option<Expression>,
        table: &Arc<Table>,
    ) -> Result<ScanType, SimpleDbError> {
        match expression {
            Some(expression) => {
                let scan_type_analyzer = ScanTypeAnalyzer::create(
                    table.clone(),
                    expression.clone()
                );
                scan_type_analyzer.analyze()
            },
            None => Ok(ScanType::Full),
        }
    }
}