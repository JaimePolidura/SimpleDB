use crate::selection::Selection;
use crate::sql::plan::plan_step::PlanStep;
use crate::{Row, Table};
use bytes::Bytes;
use shared::SimpleDbError;
use std::sync::Arc;
use storage::transactions::transaction::Transaction;

pub struct ExactScan {
    row: Option<Row>
}

impl ExactScan {
    pub fn create(
        table: Arc<Table>,
        id: Bytes,
        selection: Selection,
        transaction: &Transaction
    ) -> Result<ExactScan, SimpleDbError> {
        Ok(ExactScan {
            row: table.get_by_primary_column(&id, transaction, selection)?
        })
    }
}

impl PlanStep for ExactScan {
    fn next(&mut self) -> Option<&Row> {
        self.row.as_ref()
    }
}