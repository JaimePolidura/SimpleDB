use std::collections::BinaryHeap;
use shared::SimpleDbError;
use crate::{PlanStepDesc, Row, Sort};
use crate::sql::plan::plan_step::{PlanStep, PlanStepTrait};

//Optimization when executing queries like ORDER BY column LIMIT 3
pub struct TopNHeapSortStep {
    source: PlanStep,

    heap: BinaryHeap<Row>,
    n: usize, //N elements to maintain in the heap,
    sort: Sort,
}

struct Entry {
    row: Row,
}

impl TopNHeapSortStep {
    pub fn create(
        source: PlanStep,
        n: usize, //N elements to maintain in the heap,
        sort: Sort,
    ) -> Result<TopNHeapSortStep, SimpleDbError> {
        Ok(TopNHeapSortStep {
            source,
            n,
            sort,
            heap: BinaryHeap::new(),
        })
    }
}

impl PlanStepTrait for TopNHeapSortStep {
    fn next(&mut self) -> Result<Option<Row>, SimpleDbError> {
        todo!()
    }

    fn desc(&self) -> PlanStepDesc {
        todo!()
    }
}