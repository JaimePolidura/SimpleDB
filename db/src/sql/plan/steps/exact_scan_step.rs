use crate::selection::Selection;
use crate::sql::plan::plan_step::PlanStep;
use crate::{Row, Table};
use bytes::Bytes;
use shared::SimpleDbError;
use std::sync::Arc;
use storage::transactions::transaction::Transaction;

pub struct ExactScanStep {
    row: Option<Row>
}

impl ExactScanStep {
    pub fn create(
        table: Arc<Table>,
        id: Bytes,
        selection: Selection,
        transaction: &Transaction
    ) -> Result<ExactScanStep, SimpleDbError> {
        Ok(ExactScanStep {
            row: table.get_by_primary_column(&id, transaction, selection)?
        })
    }
}

impl PlanStep for ExactScanStep {
    fn next(&mut self) -> Result<Option<&Row>, SimpleDbError> {
        Ok(self.row.as_ref())
    }
}