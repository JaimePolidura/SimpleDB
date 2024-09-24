use crate::selection::Selection;
use crate::sql::expression::Expression;
use crate::sql::plan::scan_type::ScanType;
use crate::sql::plan::steps::exact_scan_step::ExactScanStep;
use crate::sql::plan::steps::filter_step::FilterStep;
use crate::sql::plan::steps::full_scan_step::FullScanStep;
use crate::sql::plan::steps::limit_step::LimitStep;
use crate::sql::plan::steps::range_scan_step::RangeScanStep;
use crate::sql::statement::{DeleteStatement, Limit, SelectStatement, Statement, UpdateStatement};
use crate::Table;
use shared::SimpleDbError::MalformedQuery;
use shared::{SimpleDbError, SimpleDbOptions};
use std::sync::Arc;
use storage::transactions::transaction::Transaction;
use crate::sql::plan::plan_step::Plan;

struct Planner {
    options: Arc<SimpleDbOptions>
}

impl Planner {
    pub fn create(options: Arc<SimpleDbOptions>) -> Planner {
        Planner {
            options: options.clone()
        }
    }

    pub fn plan(
        &self,
        statement: Statement,
        transaction: &Transaction,
        table: &Arc<Table>,
    ) -> Result<Plan, SimpleDbError> {
        match statement {
            Statement::Select(statement) => self.plan_select(table, statement, transaction),
            Statement::Delete(statement) => self.plan_delete(table, statement, transaction),
            Statement::Update(statement) => self.plan_update(table, statement, transaction),
            _ => panic!("Query cannot be planned")
        }
    }

    pub fn can_be_planned(&self, statement: &Statement) -> bool {
        match statement {
            Statement::Select(_) |
            Statement::Update(_) |
            Statement::Delete(_) => true,
            _ => false
        }
    }

    fn plan_select(
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

        if !matches!(select_statement.where_expr, Expression::None) {
            last_step = FilterStep::create(select_statement.where_expr, last_step);
        }
        if !matches!(select_statement.limit, Limit::None) {
            last_step = LimitStep::create(select_statement.limit, last_step);
        }

        Ok(last_step)
    }

    fn plan_update(
        &self,
        table: &Arc<Table>,
        update_statement: UpdateStatement,
        transaction: &Transaction,
    ) -> Result<Plan, SimpleDbError> {
        let scan_type = self.get_and_validate_scan_type(
            &update_statement.where_expr,
            table,
            &Limit::None
        )?;
        let updated_values = update_statement.get_updated_values();
        let mut last_step = self.build_scan_step(scan_type, transaction, updated_values, table)?;

        if !matches!(update_statement.where_expr, Expression::None) {
            last_step = FilterStep::create(update_statement.where_expr, last_step);
        }

        Ok(last_step)
    }

    fn plan_delete(
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

        if !matches!(select_statement.where_expr, Expression::None) {
            last_step = FilterStep::create(select_statement.where_expr, last_step);
        }
        if !matches!(select_statement.limit, Limit::None) {
            last_step = LimitStep::create(select_statement.limit, last_step);
        }

        Ok(last_step)
    }

    fn get_and_validate_scan_type(
        &self,
        expression: &Expression,
        table: &Arc<Table>,
        limit: &Limit,
    ) -> Result<ScanType, SimpleDbError> {
        let primary_column_name = table.get_primary_column_data().unwrap().column_name;
        let scan_type = ScanType::get_scan_type(&primary_column_name, limit, expression)?;
        match scan_type {
            ScanType::Full => {
                if !self.options.db_full_scan_allowed {
                    return Err(MalformedQuery(String::from("Full steps is not allowed")));
                }
            },
            ScanType::Range(_) => {
                if !self.options.db_range_scan_allowed {
                    return Err(MalformedQuery(String::from("Range steps is not allowed")));
                }
            }
            ScanType::Exact(_) => {}
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
            ScanType::Exact(exact_id_expr) => ExactScanStep::create(table.clone(), exact_id_expr.serialize(), selection, transaction),
            ScanType::Range(range) => RangeScanStep::create(table.clone(), selection, transaction, range),
            ScanType::Full => FullScanStep::create(table.clone(), selection, transaction),
        }
    }
}