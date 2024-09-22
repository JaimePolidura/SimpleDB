use crate::sql::expression::Expression;
use crate::sql::scan_type::ScanType;
use crate::sql::statement::{DeleteStatement, SelectStatement, Statement, UpdateStatement};
use crate::Table;
use shared::SimpleDbError::MalformedQuery;
use shared::{SimpleDbError, SimpleDbOptions};
use std::sync::Arc;

struct Planner {
    options: Arc<SimpleDbOptions>
}

pub struct Plan;

impl Planner {
    pub fn create(options: Arc<SimpleDbOptions>) -> Planner {
        Planner {
            options: options.clone()
        }
    }

    pub fn plan(
        &self,
        statement: Statement,
        table: &Arc<Table>,
    ) -> Result<Plan, SimpleDbError> {
        match statement {
            Statement::Select(statement) => self.plan_select(table, statement),
            Statement::Update(statement) => self.plan_update(table, statement),
            Statement::Delete(statement) => self.plan_delete(table, statement),
            _ => panic!("Query cannot be planned")
        }
    }

    fn plan_select(
        &self,
        table: &Arc<Table>,
        select_statement: SelectStatement
    ) -> Result<Plan, SimpleDbError> {
        let scan_type = self.get_and_validate_scan_type(&select_statement.where_expr, table)?;



        todo!()
    }

    fn plan_update(
        &self,
        table: &Arc<Table>,
        select_statement: UpdateStatement
    ) -> Result<Plan, SimpleDbError> {
        todo!()
    }

    fn plan_delete(
        &self,
        table: &Arc<Table>,
        select_statement: DeleteStatement
    ) -> Result<Plan, SimpleDbError> {
        let where_scan_type = self.get_and_validate_scan_type(&select_statement.where_expr, table)?;

        todo!()
    }

    pub fn can_be_planned(&self, statement: &Statement) -> bool {
        match statement {
            Statement::Select(_) |
            Statement::Update(_) |
            Statement::Delete(_) => true,
            _ => false
        }
    }

    fn get_and_validate_scan_type(
        &self,
        expression: &Expression,
        table: &Arc<Table>
    ) -> Result<ScanType, SimpleDbError> {
        let scan_type = ScanType::get_scan_type(expression, table);
        match scan_type {
            ScanType::Full => {
                if !self.options.db_full_scan_allowed {
                    return Err(MalformedQuery(String::from("Full scan is not allowed")));
                }
            },
            ScanType::Range => {
                if !self.options.db_range_scan_allowed {
                    return Err(MalformedQuery(String::from("Range scan is not allowed")));
                }
            }
            ScanType::Exact => {}
        };

        Ok(scan_type)
    }
}