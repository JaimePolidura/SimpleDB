use crate::sql::execution::sort::sorter::Sorter;
use crate::sql::plan::plan_step::{PlanStep, PlanStepTrait};
use crate::table::table::Table;
use crate::{PlanStepDesc, QueryIterator, Row, Sort};
use shared::SimpleDbError;
use std::sync::Arc;

#[derive(Clone)]
enum SortStepState {
    PendingSort,
    Sorted
}

#[derive(Clone)]
pub struct SortStep {
    pub(crate) state: SortStepState,
    pub(crate) sort: Sort,
    pub(crate) source: PlanStep,
    pub(crate) table: Arc<Table>,

    //Used when state is PendingSort
    pub(crate) sorter: Sorter,

    //Used when state is Sorted
    pub(crate) sorted_rows_iterator: Option<QueryIterator>,
}

impl SortStep {
    pub fn create(
        table: Arc<Table>,
        source: PlanStep,
        sort: Sort,
    ) -> SortStep {
        SortStep {
            state: SortStepState::PendingSort,
            sorted_rows_iterator: None,
            sorter: Sorter::create(),
            sort: sort.clone(),
            source,
            table,
        }
    }
}

impl PlanStepTrait for SortStep {
    fn next(&mut self) -> Result<Option<Row>, SimpleDbError> {
        match self.state {
            SortStepState::PendingSort => {
                let mut sorted_rows_iterator = self.sorter.sort(
                    self.source.clone(), self.table.clone(), self.sort.clone()
                )?;
                let sorted_row = sorted_rows_iterator.next();
                self.state = SortStepState::Sorted;
                self.sorted_rows_iterator = Some(sorted_rows_iterator);
                sorted_row
            }
            SortStepState::Sorted => {
                self.sorted_rows_iterator
                    .as_mut()
                    .unwrap()
                    .next()
            }
        }
    }

    fn desc(&self) -> PlanStepDesc {
        PlanStepDesc::Sort(self.sort.clone(), Box::new(self.source.desc()))
    }
}