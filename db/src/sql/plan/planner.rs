use crate::selection::Selection;
use crate::sql::expression::Expression;
use crate::sql::plan::plan_step::Plan;
use crate::sql::plan::scan_type::ScanType;
use crate::sql::plan::steps::exact_scan_step::ExactScanStep;
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
use crate::sql::plan::scan_type_analyzer::ScanTypeAnalyzer;

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
        let scan_type = self.get_and_validate_scan_type(
            &select_statement.where_expr,
            table,
            &select_statement.limit
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
        let scan_type = self.get_and_validate_scan_type(
            &update_statement.where_expr,
            table,
            &Limit::None
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
        let scan_type = self.get_and_validate_scan_type(
            &select_statement.where_expr,
            table,
            &select_statement.limit
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

    fn get_and_validate_scan_type(
        &self,
        expression: &Option<Expression>,
        table: &Arc<Table>,
        limit: &Limit,
    ) -> Result<ScanType, SimpleDbError> {
        let scan_type = self.get_scan_type(expression, &table, limit)?;
        match scan_type {
            ScanType::Full => {
                if !self.options.db_full_scan_allowed {
                    return Err(FullScanNotAllowed());
                }
            },
            ScanType::Range(_) => {
                if !self.options.db_range_scan_allowed {
                    return Err(RangeScanNotAllowed());
                }
            }
            ScanType::ExactPrimary(_) => {}
            ScanType::ExactSecondary(_, _) => panic!("")
        };

        Ok(scan_type)
    }

    fn build_scan_step(
        &self,
        scan_type: ScanType,
        transaction: &Transaction,
        selection: Selection,
        table: &Arc<Table>,
    ) -> Result<Plan, SimpleDbError> {
        match scan_type {
            ScanType::ExactPrimary(exact_id_expr) => ExactScanStep::create(table.clone(), exact_id_expr.serialize(), selection, transaction),
            ScanType::Range(range) => RangeScanStep::create(table.clone(), selection, transaction, range),
            ScanType::Full => FullScanStep::create(table.clone(), selection, transaction),
            _ => panic!("")
        }
    }

    fn get_scan_type(
        &self,
        expression: &Option<Expression>,
        table: &Arc<Table>,
        limit: &Limit,
    ) -> Result<ScanType, SimpleDbError> {
        match expression {
            Some(expression) => {
                let scan_type_analyzer = ScanTypeAnalyzer::create(
                    table.clone(),
                    limit.clone(),
                    expression.clone()
                );
                scan_type_analyzer.analyze()
            },
            None => Ok(ScanType::Full),
        }
    }
}