use crate::sql::execution::sort::sorter::Sorter;
use crate::sql::plan::plan_step::{PlanStep, PlanStepTrait};
use crate::table::table::Table;
use crate::{PlanStepDesc, QueryIterator, Row, Sort};
use shared::{SimpleDbError, SimpleDbOptions};
use std::sync::Arc;
use crate::table::row::RowIterator;

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
    // pub(crate) sorted_rows_iterator: Option<QueryIterator>,
}

impl SortStep {
    pub fn create(
        options: Arc<SimpleDbOptions>,
        table: Arc<Table>,
        source: PlanStep,
        sort: Sort,
    ) -> Result<SortStep, SimpleDbError> {
        Ok(SortStep {
            sorter: Sorter::create(options, table.clone(), source.clone(), sort.clone())?,
            state: SortStepState::PendingSort,
            // sorted_rows_iterator: None,
            sort: sort.clone(),
            source,
            table,
        })
    }
}

impl PlanStepTrait for SortStep {
    fn next(&mut self) -> Result<Option<Row>, SimpleDbError> {
        match self.state {
            SortStepState::PendingSort => {
                let mut sorted_rows_iterator = self.sorter.sort()?;
                // let sorted_row = sorted_rows_iterator.next();
                // self.state = SortStepState::Sorted;
                // self.sorted_rows_iterator = Some(sorted_rows_iterator);
                // sorted_row
                todo!()
            }
            SortStepState::Sorted => {
                // self.sorted_rows_iterator
                //     .as_mut()
                //     .unwrap()
                //     .next()
                todo!()
            }
        }
    }

    fn desc(&self) -> PlanStepDesc {
        PlanStepDesc::Sort(self.sort.clone(), Box::new(self.source.desc()))
    }
}