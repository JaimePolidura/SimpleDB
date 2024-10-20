use std::sync::Arc;
use shared::SimpleDbError;
use crate::{PlanStepDesc, QueryIterator, Row, Sort};
use crate::selection::Selection;
use crate::sql::plan::plan_step::{PlanStep, PlanStepTrait};
use crate::table::table::Table;

#[derive(Clone)]
pub struct SortStep {
    pub(crate) sort: Sort,
    pub(crate) table: Arc<Table>,
    pub(crate) source: QueryIterator,
}

impl SortStep {
    pub fn create(
        sort: Sort,
        table: Arc<Table>,
        source: PlanStep,
    ) -> SortStep {
        SortStep {
            source: QueryIterator::create(Selection::All, source, table.get_schema().clone()),
            table,
            sort,
        }
    }
}

impl PlanStepTrait for SortStep {
    fn next(&mut self) -> Result<Option<Row>, SimpleDbError> {

    }

    fn desc(&self) -> PlanStepDesc {
        PlanStepDesc::Sort(self.sort.clone())
    }
}