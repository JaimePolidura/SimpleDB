use crate::table::selection::Selection;
use crate::{Row};
use bytes::Bytes;
use shared::{SimpleDbError, Value};
use std::sync::Arc;
use storage::transactions::transaction::Transaction;
use crate::sql::plan::plan_step::{PlanStepDesc, PlanStepTrait};
use crate::table::table::Table;

#[derive(Clone)]
pub struct PrimaryExactScanStep {
    pub(crate) row: Option<Row>,
    pub(crate) primary_key_value: Value,
}

impl PrimaryExactScanStep {
    pub(crate) fn create(
        table: Arc<Table>,
        id: Bytes,
        selection: Selection,
        transaction: &Transaction
    ) -> Result<PrimaryExactScanStep, SimpleDbError> {
        let primary_column_type = table.get_schema().get_primary_column()
            .column_type;

        Ok(PrimaryExactScanStep {
            row: table.get_by_primary_column(&id, transaction, &selection)?,
            primary_key_value: Value::create(id, primary_column_type)?,
        })
    }
}

impl PlanStepTrait for PrimaryExactScanStep {
    fn next(&mut self) -> Result<Option<Row>, SimpleDbError> {
        Ok(self.row.take())
    }

    fn desc(&self) -> PlanStepDesc {
        PlanStepDesc::PrimaryExactScan(self.primary_key_value.get_bytes().clone())
    }
}