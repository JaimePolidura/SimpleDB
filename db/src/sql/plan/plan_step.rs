use crate::table::selection::Selection;
use crate::sql::plan::scan_type::RangeScan;
use crate::sql::plan::steps::filter_step::FilterStep;
use crate::sql::plan::steps::full_scan_step::FullScanStep;
use crate::sql::plan::steps::limit_step::LimitStep;
use crate::sql::plan::steps::merge_intersection_scan_step::MergeIntersectionStep;
use crate::sql::plan::steps::merge_union_scan_step::MergeUnionStep;
use crate::sql::plan::steps::primary_exact_scan_step::PrimaryExactScanStep;
use crate::sql::plan::steps::primary_range_scan_step::PrimaryRangeScanStep;
use crate::sql::plan::steps::project_selection_step::ProjectSelectionStep;
use crate::sql::plan::steps::secondary_exact_scan_step::SecondaryExactScanStep;
use crate::sql::plan::steps::secondary_range_scan_step::SecondaryRangeScanStep;
use crate::{Limit, Row, Schema, Sort};
use bytes::Bytes;
use shared::{SimpleDbError, Value};
use crate::sql::plan::steps::full_sort_step::FullSortStep;
use crate::sql::plan::steps::reverse_step::ReverseStep;
use crate::sql::plan::steps::top_n_sort::TopNSortStep;
use crate::table::row::RowIterator;

pub(crate) trait PlanStepTrait {
    fn next(&mut self) -> Result<Option<Row>, SimpleDbError>;
    fn desc(&self) -> PlanStepDesc;
}

#[derive(Clone)]
pub enum PlanStep {
    ProjectSelection(Box<ProjectSelectionStep>),
    Limit(Box<LimitStep>),
    TopNSort(Box<TopNSortStep>),
    FullSort(Box<FullSortStep>),
    Filter(Box<FilterStep>),
    Reverse(Box<ReverseStep>),

    MergeIntersection(MergeIntersectionStep),
    MergeUnion(MergeUnionStep),

    FullScan(FullScanStep),
    PrimaryRangeScan(PrimaryRangeScanStep),
    SecondaryRangeScan(SecondaryRangeScanStep),
    PrimaryExactScan(PrimaryExactScanStep),
    SecondaryExactExactScan(SecondaryExactScanStep),

    //Only used for testing
    Mock(MockStep)
}

pub enum PlanStepDesc {
    ProjectionSelectionStep(Selection, Box<PlanStepDesc>),
    Limit(Limit, Box<PlanStepDesc>),
    Filter(Box<PlanStepDesc>),
    MergeIntersection(Box<PlanStepDesc>, Box<PlanStepDesc>),
    MergeUnion(Box<PlanStepDesc>, Box<PlanStepDesc>),
    FullSort(Sort, Box<PlanStepDesc>),
    TopNSort(Sort, usize, Box<PlanStepDesc>),
    Revserse(Box<PlanStepDesc>),

    FullScan,
    RangeScan(RangeScan),
    PrimaryExactScan(Bytes),
    SecondaryExactExactScan(String, Bytes),
}

impl RowIterator for PlanStep {
    fn next(&mut self) -> Result<Option<Row>, SimpleDbError> {
        match self {
            PlanStep::Limit(step) => step.next(),
            PlanStep::Filter(step) => step.next(),
            PlanStep::MergeIntersection(step) => step.next(),
            PlanStep::MergeUnion(step) => step.next(),
            PlanStep::FullScan(step) => step.next(),
            PlanStep::PrimaryRangeScan(step) => step.next(),
            PlanStep::PrimaryExactScan(step) => step.next(),
            PlanStep::SecondaryExactExactScan(step) => step.next(),
            PlanStep::SecondaryRangeScan(step) => step.next(),
            PlanStep::ProjectSelection(step) => step.next(),
            PlanStep::FullSort(step) => step.next(),
            PlanStep::Mock(step) => step.next(),
            PlanStep::TopNSort(step) => step.next(),
            PlanStep::Reverse(step) => step.next(),
        }
    }
}

impl PlanStep {
    pub fn get_column_sorted_by_plans(
        schema: &Schema,
        left: &PlanStep,
        right: &PlanStep
    ) -> Option<String> {
        let sorted_right = right.get_column_sorted(schema);
        let sorted_left = left.get_column_sorted(schema);

        match (sorted_left, sorted_right) {
            (Option::None, _) |
            (_, Option::None) => None,
            (Option::Some(sorted_left), Option::Some(sorted_right)) => {
                if sorted_left.eq(&sorted_right) {
                    Some(sorted_left)
                } else {
                    None
                }
            }
        }
    }

    //Returns the column name if the plan produces a sorted result.
    //If the plan produces a result which is not sorted, it will return None
    pub fn get_column_sorted(&self, schema: &Schema) -> Option<String> {
        match &self {
            PlanStep::ProjectSelection(step) => step.source.get_column_sorted(schema),
            PlanStep::Limit(step) => step.source.get_column_sorted(schema),
            PlanStep::FullSort(step) => Some(step.sort.column_name.clone()),
            PlanStep::TopNSort(step) => Some(step.sort.column_name.clone()),
            PlanStep::Filter(step) => step.source.get_column_sorted(schema),
            PlanStep::Reverse(step) => step.source.get_column_sorted(schema),
            PlanStep::MergeIntersection(_) |
            PlanStep::MergeUnion(_) => {
                let left = self.get_merge_left();
                let right = self.get_merge_right();
                Self::get_column_sorted_by_plans(schema, left, right)
            },
            PlanStep::FullScan(_) => {
                //Full scans are always ordered by primary key
                Some(schema.get_primary_column().column_name)
            }
            PlanStep::PrimaryRangeScan(_) => {
                Some(schema.get_primary_column().column_name)
            }
            PlanStep::SecondaryRangeScan(range) => {
                Some(range.column_name.clone())
            }
            PlanStep::PrimaryExactScan(step) => {
                Some(schema.get_primary_column().column_name)
            },
            PlanStep::SecondaryExactExactScan(step) => {
                Some(step.column_name.clone())
            }
            PlanStep::Mock(step) => {
                if step.sorted_by_primary {
                    Some(schema.get_primary_column().column_name)
                } else {
                    None
                }
            }
        }
    }

    pub fn desc(&self) -> PlanStepDesc {
        match self {
            PlanStep::Limit(step) => step.desc(),
            PlanStep::Filter(step) => step.desc(),
            PlanStep::FullSort(step) => step.desc(),
            PlanStep::MergeIntersection(step) => step.desc(),
            PlanStep::MergeUnion(step) => step.desc(),
            PlanStep::FullScan(step) => step.desc(),
            PlanStep::PrimaryRangeScan(step) => step.desc(),
            PlanStep::PrimaryExactScan(step) => step.desc(),
            PlanStep::SecondaryExactExactScan(step) => step.desc(),
            PlanStep::SecondaryRangeScan(step) => step.desc(),
            PlanStep::ProjectSelection(step) => step.desc(),
            PlanStep::Mock(step) => step.desc(),
            PlanStep::TopNSort(step) => step.desc(),
            PlanStep::Reverse(step) => step.desc()
        }
    }

    pub fn is_union(&self) -> bool {
        matches!(self, PlanStep::MergeUnion(_))
    }

    pub fn get_merge_left(&self) -> &PlanStep {
        match self {
            PlanStep::MergeIntersection(step) => &step.plans[0],
            PlanStep::MergeUnion(step) => &step.plans[0],
            _ => panic!("Illegal code path")
        }
    }

    pub fn get_merge_right(&self) -> &PlanStep {
        match self {
            PlanStep::MergeIntersection(step) => &step.plans[1],
            PlanStep::MergeUnion(step) => &step.plans[1],
            _ => panic!("Illegal code path")
        }
    }

    pub fn set_merge_left(&mut self, other: PlanStep) {
        match self {
            PlanStep::MergeIntersection(step) =>{
                step.plans[0] = other
            },
            PlanStep::MergeUnion(step) => {
                step.plans[0] = other
            },
            _ => panic!("Illegal code path")
        }
    }

    pub fn set_merge_right(&mut self, other: PlanStep) {
        match self {
            PlanStep::MergeIntersection(step) =>{
                step.plans[1] = other
            },
            PlanStep::MergeUnion(step) => {
                step.plans[1] = other
            },
            _ => panic!("Illegal code path")
        }
    }

    pub fn is_same_column(&self, other: &PlanStep) -> bool {
        match (&self, other) {
            (PlanStep::PrimaryRangeScan(_), PlanStep::PrimaryRangeScan(_)) |
            (PlanStep::PrimaryExactScan(_), PlanStep::PrimaryExactScan(_)) => true,
            (PlanStep::SecondaryRangeScan(left), PlanStep::SecondaryRangeScan(right)) => {
                left.column_name.eq(&right.column_name)
            },
            (PlanStep::SecondaryExactExactScan(left), PlanStep::SecondaryExactExactScan(right)) => {
                left.column_name.eq(&right.column_name)
            }
            (_, _) => false
        }
    }

    pub fn get_exact_value(&self) -> &Value {
        match &self {
            PlanStep::PrimaryExactScan(step) => {
                &step.primary_key_value
            },
            PlanStep::SecondaryExactExactScan(step) => {
                &step.secondary_column_value
            },
            _ => panic!("Illegal code path")
        }
    }

    pub fn is_exact(&self) -> bool {
        match &self {
            PlanStep::SecondaryExactExactScan(_) |
            PlanStep::PrimaryExactScan(_) => true,
            _ => false
        }
    }

    pub fn get_range_scan(&self) -> RangeScan {
        match &self {
            PlanStep::SecondaryRangeScan(step) => step.range.clone(),
            PlanStep::PrimaryRangeScan(step) => step.range.clone(),
            _ => panic!("Illegal code path")
        }
    }

    pub fn is_range(&self) -> bool {
        match &self {
            PlanStep::SecondaryRangeScan(_) |
            PlanStep::PrimaryRangeScan(_) => true,
            _ => false
        }
    }
}

#[derive(Clone)]
pub struct MockStep {
    pub(crate) sorted_by_primary: bool,
    pub(crate) rows: Vec<Row>,
}

impl MockStep {
    pub fn create(sorted_by_primary: bool, rows: Vec<Row>) -> MockStep {
        MockStep {
            sorted_by_primary,
            rows
        }
    }
}

impl PlanStepTrait for MockStep {
    fn next(&mut self) -> Result<Option<Row>, SimpleDbError> {
        if !self.rows.is_empty() {
            Ok(Some(self.rows.remove(0)))
        } else {
            Ok(None)
        }
    }

    fn desc(&self) -> PlanStepDesc {
        unimplemented!()
    }
}