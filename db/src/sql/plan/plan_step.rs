use crate::sql::plan::scan_type::RangeScan;
use crate::sql::plan::steps::filter_step::FilterStep;
use crate::sql::plan::steps::full_scan_step::FullScanStep;
use crate::sql::plan::steps::limit_step::LimitStep;
use crate::sql::plan::steps::merge_intersection_scan_step::MergeIntersectionStep;
use crate::sql::plan::steps::merge_union_scan_step::MergeUnionStep;
use crate::sql::plan::steps::primary_exact_scan_step::PrimaryExactScanStep;
use crate::sql::plan::steps::primary_range_scan_step::PrimaryRangeScanStep;
use crate::sql::plan::steps::secondary_exact_scan_step::SecondaryExactScanStep;
use crate::{Limit, Row};
use bytes::Bytes;
use shared::{SimpleDbError, Value};
use crate::selection::Selection;
use crate::sql::plan::steps::project_selection_step::ProjectSelectionStep;
use crate::sql::plan::steps::secondary_range_scan_step::SecondaryRangeScanStep;

pub(crate) trait PlanStepTrait {
    fn next(&mut self) -> Result<Option<Row>, SimpleDbError>;
    fn desc(&self) -> PlanStepDesc;
}

#[derive(Clone)]
pub enum PlanStep {
    ProjectSelection(Box<ProjectSelectionStep>),
    Limit(Box<LimitStep>),
    Filter(Box<FilterStep>),

    MergeIntersection(MergeIntersectionStep),
    MergeUnion(MergeUnionStep),

    FullScan(FullScanStep),
    PrimaryRangeScan(PrimaryRangeScanStep),
    SecondaryRangeScan(SecondaryRangeScanStep),
    PrimaryExactScan(PrimaryExactScanStep),
    SecondaryExactExactScan(SecondaryExactScanStep),
}

pub enum PlanStepDesc {
    ProjectionSelectionStep(Selection, Box<PlanStepDesc>),
    Limit(Limit, Box<PlanStepDesc>),
    Filter(Box<PlanStepDesc>),
    MergeIntersection(Box<PlanStepDesc>, Box<PlanStepDesc>),
    MergeUnion(Box<PlanStepDesc>, Box<PlanStepDesc>),

    FullScan,
    RangeScan(RangeScan),
    PrimaryExactScan(Bytes),
    SecondaryExactExactScan(String, Bytes),
}

impl PlanStep {
    pub fn next(&mut self) -> Result<Option<Row>, SimpleDbError> {
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
        }
    }

    pub fn desc(&self) -> PlanStepDesc {
        match self {
            PlanStep::Limit(step) => step.desc(),
            PlanStep::Filter(step) => step.desc(),
            PlanStep::MergeIntersection(step) => step.desc(),
            PlanStep::MergeUnion(step) => step.desc(),
            PlanStep::FullScan(step) => step.desc(),
            PlanStep::PrimaryRangeScan(step) => step.desc(),
            PlanStep::PrimaryExactScan(step) => step.desc(),
            PlanStep::SecondaryExactExactScan(step) => step.desc(),
            PlanStep::SecondaryRangeScan(step) => step.desc(),
            PlanStep::ProjectSelection(step) => step.desc(),
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