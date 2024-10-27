use crate::sql::plan::plan_step::{PlanStep, PlanStepTrait};
use crate::table::row::RowIterator;
use crate::{PlanStepDesc, Row, Sort};
use shared::{SimpleDbError, Value};
use std::cmp::Ordering;
use std::collections::{BTreeMap, BinaryHeap};
use std::sync::Arc;

//Optimization when executing queries like ORDER BY column LIMIT 3
#[derive(Clone)]
pub struct TopNSortStep {
    pub(crate) source: PlanStep,
    pub(crate) heap: BTreeMap<TopNSortHeapEntry, ()>,
    pub(crate) n: usize, //N elements to maintain in the heap,
    pub(crate) sort: Arc<Sort>,
    pub(crate) state: TopNSortStepState,
}

#[derive(Clone)]
enum TopNSortStepState {
    Sorting,
    Sorted
}

#[derive(Clone)]
struct TopNSortHeapEntry {
    row: Row,
    sort: Arc<Sort>,
}

impl TopNSortStep {
    pub fn create(
        source: PlanStep,
        n: usize, //N elements to maintain in the heap,
        sort: Sort,
    ) -> TopNSortStep {
        TopNSortStep {
            state: TopNSortStepState::Sorting,
            heap: BTreeMap::new(),
            sort: Arc::new(sort),
            source,
            n,
        }
    }

    fn top_n_sort(&mut self) -> Result<(), SimpleDbError> {
        while let Some(row) = self.source.next()? {
            self.heap.insert(TopNSortHeapEntry {
                sort: self.sort.clone(),
                row
            }, ());

            if self.heap.len() > self.n {
                self.remove_largest();
            }
        }

        Ok(())
    }

    fn remove_largest(&mut self) {
        let max_key_opt = self.heap.keys().next_back().map(|it| it.clone());
        if let Some(max_key) = max_key_opt {
            self.heap.remove(&max_key);
        }
    }

    fn get_next_row_in_sorted_heap(&mut self) -> Option<Row> {
        let min_key_optional = self.heap.keys().next().map(|it| it.clone());

        if let Some(min_key) = min_key_optional {
            self.heap.remove(&min_key);
            Some(min_key.row.clone())
        } else {
            None
        }
    }
}

impl PlanStepTrait for TopNSortStep {
    fn next(&mut self) -> Result<Option<Row>, SimpleDbError> {
        match self.state {
            TopNSortStepState::Sorting => {
                self.top_n_sort()?;
                self.state = TopNSortStepState::Sorted;
                Ok(self.get_next_row_in_sorted_heap())
            }
            TopNSortStepState::Sorted => {
                Ok(self.get_next_row_in_sorted_heap())
            }
        }
    }

    fn desc(&self) -> PlanStepDesc {
        PlanStepDesc::TopNSort(self.sort.as_ref().clone(), self.n, Box::new(self.source.desc()))
    }
}

impl TopNSortHeapEntry {
    pub fn get_sorted_value(&self) -> Value {
        self.row.get_column_value(&self.sort.column_name).unwrap()
    }
}

impl Eq for TopNSortHeapEntry {}

impl Ord for TopNSortHeapEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(other).unwrap()
    }
}

impl PartialEq<Self> for TopNSortHeapEntry {
    fn eq(&self, other: &Self) -> bool {
        self.get_sorted_value().eq(&other.get_sorted_value())
    }
}

impl PartialOrd for TopNSortHeapEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.sort.compare(&self.row, &other.row))
    }
}