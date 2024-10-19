use crate::index::secondary_index_iterator::SecondaryIndexIterator;
use crate::selection::{IndexSelectionType, Selection};
use crate::sql::plan::plan_step::{PlanStepDesc, PlanStepTrait};
use crate::sql::plan::scan_type::{RangeKeyPosition, RangeScan};
use crate::table::table::Table;
use crate::Row;
use shared::{SimpleDbError, Type, Value};
use std::sync::Arc;
use shared::key::Key;
use storage::transactions::transaction::Transaction;
use storage::{SimpleDbStorageIterator, Storage};
use crate::table::row::RowBuilder;

#[derive(Clone)]
pub struct SecondaryRangeScanStep {
    pub(crate) secondary_iterator: SecondaryIndexIterator<SimpleDbStorageIterator>,
    pub(crate) selection: Selection,
    pub(crate) range: RangeScan,
    pub(crate) table: Arc<Table>,
    pub(crate) transaction: Transaction,
    pub(crate) column_name: String,
    pub(crate) index_selection_type: IndexSelectionType,
}

impl SecondaryRangeScanStep {
    pub(crate) fn create(
        table: Arc<Table>,
        selection: Selection,
        column_name: &str,
        transaction: &Transaction,
        range: RangeScan,
    ) -> Result<SecondaryRangeScanStep, SimpleDbError> {
        let iterator = if let Some(star_range_key_expr) = range.start() {
            let star_range_key_bytes = star_range_key_expr.get_literal_bytes();
            table.scan_from_key_secondary_index(&star_range_key_bytes, range.is_start_inclusive(), transaction, &column_name)
        } else {
            table.scan_all_secondary_index(transaction, &column_name)
        }?;

        Ok(SecondaryRangeScanStep {
            index_selection_type: selection.get_index_selection_type(table.get_schema()),
            column_name: column_name.to_string(),
            transaction: transaction.clone(),
            secondary_iterator: iterator,
            selection,
            table,
            range,
        })
    }
}

impl PlanStepTrait for SecondaryRangeScanStep {
    fn next(&mut self) -> Result<Option<Row>, SimpleDbError> {
        if let Some((indexed_value, primary_key)) = self.secondary_iterator.next() {
            match self.range.get_position(indexed_value.get_value()) {
                RangeKeyPosition::Inside => {
                    match self.index_selection_type {
                        IndexSelectionType::Primary |
                        IndexSelectionType::Secondary |
                        IndexSelectionType::OnlyPrimaryAndSecondary => {
                            let mut row_builder = RowBuilder::create(self.table.get_schema().clone());
                            row_builder.add_primary_value(primary_key.get_value().clone());
                            row_builder.add_by_column_name(indexed_value.get_value().get_bytes().clone(), &self.column_name);
                            Ok(Some(row_builder.build()))
                        },
                        IndexSelectionType::All => {
                            self.table.get_by_primary_column(
                                primary_key.as_bytes(),
                                &self.transaction,
                                &self.selection,
                            )
                        },
                    }
                },
                RangeKeyPosition::Above => Ok(None),
                //Not possible because, the iterator have been seeked in construction time
                RangeKeyPosition::Bellow => panic!(""),
            }
        } else {
            Ok(None)
        }
    }

    fn desc(&self) -> PlanStepDesc {
        PlanStepDesc::RangeScan(self.range.clone())
    }
}