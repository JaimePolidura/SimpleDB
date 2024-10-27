use std::collections::HashSet;
use crate::table::selection::Selection;
use crate::sql::parser::expression::Expression;
use crate::sql::parser::statement::{DeleteStatement, Limit, SelectStatement, UpdateStatement};
use crate::sql::plan::plan_step::PlanStep;
use crate::sql::plan::scan_type::ScanType;
use crate::sql::plan::scan_type_analyzer::ScanTypeAnalyzer;
use crate::sql::plan::steps::filter_step::FilterStep;
use crate::sql::plan::steps::full_scan_step::FullScanStep;
use crate::sql::plan::steps::limit_step::LimitStep;
use crate::sql::plan::steps::merge_intersection_scan_step::MergeIntersectionStep;
use crate::sql::plan::steps::merge_union_scan_step::MergeUnionStep;
use crate::sql::plan::steps::primary_exact_scan_step::PrimaryExactScanStep;
use crate::sql::plan::steps::primary_range_scan_step::PrimaryRangeScanStep;
use crate::sql::plan::steps::project_selection_step::ProjectSelectionStep;
use crate::sql::plan::steps::secondary_exact_scan_step::SecondaryExactScanStep;
use crate::sql::plan::steps::secondary_range_scan_step::SecondaryRangeScanStep;
use crate::table::table::Table;
use shared::{SimpleDbError, SimpleDbOptions};
use std::sync::Arc;
use storage::transactions::transaction::Transaction;
use crate::sql::plan::steps::full_sort_step::FullSortStep;
use crate::sql::plan::steps::top_n_sort::TopNSortStep;

pub struct Planner {
    options: Arc<SimpleDbOptions>
}

impl Planner {
    pub fn create(
        options: Arc<SimpleDbOptions>
    ) -> Planner {
        Planner { options }
    }

    pub fn plan_select(
        &self,
        table: &Arc<Table>,
        mut select_statement: SelectStatement,
        transaction: &Transaction
    ) -> Result<PlanStep, SimpleDbError> {
        let query_selection = select_statement.selection.clone();
        let (needs_projection_of_selection, storage_engine_selection) = Self::get_selection_select(&select_statement);

        let scan_type = self.get_scan_type(
            &select_statement.where_expr,
            table,
        )?;
        let mut last_step = self.build_scan_step(scan_type, transaction, storage_engine_selection.clone(), table)?;

        //Where expression
        if select_statement.has_where_expression() {
            let where_expr = select_statement.take_where_expression();
            last_step = PlanStep::Filter(Box::new(FilterStep::create(where_expr.clone(), last_step)));
        }
        //Only sorted, not with limit
        if select_statement.is_sorted() && !select_statement.is_limit() {
            let sort = select_statement.sort.take().unwrap();
            let produced_rows_sorted_by = last_step.get_column_sorted(table.get_schema());
            if produced_rows_sorted_by.is_none() || !produced_rows_sorted_by.as_ref().unwrap().eq(&sort.column_name) {
                last_step = PlanStep::FullSort(Box::new(FullSortStep::create(self.options.clone(), query_selection.clone(), table.clone(), last_step, sort)?))
            }
        }
        //Only Limit
        if !select_statement.is_sorted() && select_statement.is_limit() {
            last_step = PlanStep::Limit(Box::new(LimitStep::create(select_statement.limit.clone(), last_step)));
        }
        //Sorted with limit
        if select_statement.is_sorted() && select_statement.is_limit() {
            let sort = select_statement.sort.take().unwrap();
            last_step = PlanStep::TopNSort(Box::new(TopNSortStep::create(last_step, select_statement.get_limit(), sort)));
        }

        if needs_projection_of_selection {
            last_step = PlanStep::ProjectSelection(Box::new(ProjectSelectionStep::create(query_selection, last_step)))
        }

        Ok(last_step)
    }

    pub fn plan_update(
        &self,
        table: &Arc<Table>,
        update_statement: &UpdateStatement,
        transaction: &Transaction,
    ) -> Result<PlanStep, SimpleDbError> {
        let scan_type = self.get_scan_type(
            &update_statement.where_expr,
            table
        )?;
        let updated_values = update_statement.get_updated_values();
        let mut last_step = self.build_scan_step(scan_type, transaction, updated_values, table)?;

        if let Some(where_expr) = &update_statement.where_expr {
            last_step = PlanStep::Filter(Box::new(FilterStep::create(where_expr.clone(), last_step)));
        }

        Ok(last_step)
    }

    pub fn plan_delete(
        &self,
        table: &Arc<Table>,
        select_statement: DeleteStatement,
        transaction: &Transaction
    ) -> Result<PlanStep, SimpleDbError> {
        let scan_type = self.get_scan_type(
            &select_statement.where_expr,
            table,
        )?;
        let mut last_step = self.build_scan_step(scan_type, transaction, Selection::All, table)?;

        if let Some(where_expr) = select_statement.where_expr {
            last_step = PlanStep::Filter(Box::new(FilterStep::create(where_expr, last_step)));
        }
        if !matches!(select_statement.limit, Limit::None) {
            last_step = PlanStep::Limit(Box::new(LimitStep::create(select_statement.limit, last_step)));
        }

        Ok(last_step)
    }

    fn build_scan_step(
        &self,
        scan_type: ScanType,
        transaction: &Transaction,
        selection: Selection,
        table: &Arc<Table>,
    ) -> Result<PlanStep, SimpleDbError> {
        let schema = table.get_schema();

        match scan_type {
            ScanType::ExactSecondary(column, exact_id_expr) => {
                Ok(PlanStep::SecondaryExactExactScan(SecondaryExactScanStep::create(table.clone(), &column, exact_id_expr.get_literal_bytes(), transaction, selection)?))
            },
            ScanType::ExactPrimary(exact_id_expr) => {
                Ok(PlanStep::PrimaryExactScan(PrimaryExactScanStep::create(table.clone(), exact_id_expr.get_literal_bytes(), selection, transaction)?))
            },
            ScanType::Range(range) => {
                if schema.is_secondary_indexed(&range.column_name) {
                    Ok(PlanStep::SecondaryRangeScan(
                        SecondaryRangeScanStep::create(table.clone(), selection, &range.column_name, transaction, range.clone())?
                    ))
                } else {
                    Ok(PlanStep::PrimaryRangeScan(
                        PrimaryRangeScanStep::create(table.clone(), selection, transaction, range)?
                    ))
                }
            },
            ScanType::Full => {
                Ok(PlanStep::FullScan(FullScanStep::create(table.clone(), selection, transaction)?))
            },
            ScanType::MergeUnion(left_scan_type, right_scan_type) => {
                let left_scan_step = self.build_scan_step(*left_scan_type, transaction, selection.clone(), table)?;
                let right_scan_step = self.build_scan_step(*right_scan_type, transaction, selection.clone(), table)?;
                Ok(PlanStep::MergeUnion(MergeUnionStep::create(schema, left_scan_step, right_scan_step)?))
            }
            ScanType::MergeIntersection(left_scan_type, right_scan_type) => {
                let left_scan_step = self.build_scan_step(*left_scan_type, transaction, selection.clone(), table)?;
                let right_scan_step = self.build_scan_step(*right_scan_type, transaction, selection.clone(), table)?;
                Ok(PlanStep::MergeIntersection(MergeIntersectionStep::create(schema, left_scan_step, right_scan_step)?))
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
                    expression.clone(),
                    table.get_schema().clone(),
                );
                scan_type_analyzer.analyze()
            },
            None => Ok(ScanType::Full),
        }
    }

    //Returns the selection of columns to be scanned from the storage engine
    //Returns true if projection will be needed
    //and the ones being returned from the storage engine
    //For example: SELECT nombre WHERE dinero > 100. We will need nombre and dinero to be scanned from the stoage engine
    //But we will only return dinero to the final user.
    fn get_selection_select(
        select: &SelectStatement
    ) -> (bool, Selection) {
        match &select.selection {
            Selection::All => (false, Selection::All),
            Selection::Some(query_selection) => {
                let mut storage_engine_selection = HashSet::new();

                storage_engine_selection.extend(query_selection.iter().map(|it| it.clone()));

                if let Some(sort) = &select.sort {
                    storage_engine_selection.insert(sort.column_name.clone());
                }
                if let Some(where_expr) = &select.where_expr {
                    storage_engine_selection.extend(where_expr.get_identifiers());
                }

                let mut projection_needed = query_selection.len() != storage_engine_selection.len();

                (projection_needed, Selection::Some(storage_engine_selection.into_iter().collect()))
            }
        }
    }
}