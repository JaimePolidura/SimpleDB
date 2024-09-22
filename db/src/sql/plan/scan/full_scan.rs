use std::sync::Arc;
use shared::SimpleDbError;
use storage::transactions::transaction::Transaction;
use crate::{Row, Table, TableIterator};
use crate::selection::Selection;
use crate::sql::plan::plan_step::PlanStep;

pub struct FullScan {
    iterator: TableIterator,
}

impl FullScan {
    pub fn create(
        table: Arc<Table>,
        selection: Selection,
        transaction: &Transaction
    ) -> Result<FullScan, SimpleDbError> {
        Ok(FullScan {
            iterator: table.scan_all(transaction, selection)?
        })
    }
}

impl PlanStep for FullScan {
    fn next(&mut self) -> Option<&Row> {
        if self.iterator.next() {
            Some(self.iterator.row())
        } else {
            None
        }
    }
}