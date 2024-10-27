use crate::sql::execution::sort::sorter::Sorter;
use crate::sql::plan::plan_step::{PlanStep, PlanStepTrait};
use crate::table::table::Table;
use crate::{PlanStepDesc, QueryIterator, Row, Selection, Sort};
use shared::{SimpleDbError, SimpleDbOptions};
use std::sync::Arc;
use crate::sql::execution::sort::sorted_result_iterator::SortedResultIterator;
use crate::table::row::RowIterator;

#[derive(Clone)]
enum FullSortStepState {
    PendingSort,
    Sorted
}

#[derive(Clone)]
pub struct FullSortStep {
    pub(crate) state: FullSortStepState,
    pub(crate) sort: Sort,
    pub(crate) source: PlanStep,
    pub(crate) table: Arc<Table>,

    //Used when state is PendingSort
    pub(crate) sorter: Sorter,

    //Used when state is Sorted
    pub(crate) sorted_rows_iterator: Option<QueryIterator<SortedResultIterator>>,
}

impl FullSortStep {
    pub fn create(
        options: Arc<SimpleDbOptions>,
        selection: Selection,
        table: Arc<Table>,
        source: PlanStep,
        sort: Sort,
    ) -> Result<FullSortStep, SimpleDbError> {
        Ok(FullSortStep {
            sorter: Sorter::create(options, selection, table.clone(), source.clone(), sort.clone())?,
            state: FullSortStepState::PendingSort,
            sorted_rows_iterator: None,
            sort: sort.clone(),
            source,
            table,
        })
    }
}

impl PlanStepTrait for FullSortStep {
    fn next(&mut self) -> Result<Option<Row>, SimpleDbError> {
        match self.state {
            FullSortStepState::PendingSort => {
                let mut sorted_rows_iterator = self.sorter.sort()?;
                let sorted_row = sorted_rows_iterator.next();
                self.state = FullSortStepState::Sorted;
                self.sorted_rows_iterator = Some(sorted_rows_iterator);
                sorted_row
            }
            FullSortStepState::Sorted => {
                self.sorted_rows_iterator
                    .as_mut()
                    .unwrap()
                    .next()
            }
        }
    }

    fn desc(&self) -> PlanStepDesc {
        PlanStepDesc::FullSort(self.sort.clone(), Box::new(self.source.desc()))
    }
}