use std::sync::Arc;
use crate::{QueryIterator, Sort};
use shared::SimpleDbError;
use crate::sql::plan::plan_step::PlanStep;
use crate::table::table::Table;

#[derive(Clone)]
pub struct Sorter {
}

impl Sorter {
    pub fn create() -> Sorter {
        Sorter {}
    }

    pub fn sort(
        &mut self,
        source: PlanStep,
        table: Arc<Table>,
        sort: Sort,
    ) -> Result<QueryIterator, SimpleDbError> {
        todo!()
    }
}