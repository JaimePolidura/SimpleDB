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
use shared::SimpleDbError;
use crate::selection::Selection;
use crate::sql::plan::steps::project_selection_step::ProjectSelectionStep;
use crate::sql::plan::steps::secondary_range_scan_step::SecondaryRangeScanStep;

pub(crate) trait PlanStepTrait {
    fn next(&mut self) -> Result<Option<Row>, SimpleDbError>;
    fn desc(&self) -> PlanStepDesc;
}

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
}