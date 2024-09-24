use std::sync::Arc;
use shared::SimpleDbError;
use storage::transactions::transaction::Transaction;
use crate::{Row, Table, TableIterator};
use crate::selection::Selection;
use crate::sql::plan::plan_step::PlanStep;

pub struct FullScanStep {
    iterator: TableIterator,
}

impl FullScanStep {
    pub fn create(
        table: Arc<Table>,
        selection: Selection,
        transaction: &Transaction
    ) -> Result<FullScanStep, SimpleDbError> {
        Ok(FullScanStep {
            iterator: table.scan_all(transaction, selection)?
        })
    }
}

impl PlanStep for FullScanStep {
    fn next(&mut self) -> Option<&Row> {
        if self.iterator.next() {
            Some(self.iterator.row())
        } else {
            None
        }
    }
}