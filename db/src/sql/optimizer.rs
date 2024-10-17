use std::sync::Arc;
use shared::SimpleDbError;
use shared::SimpleDbError::MalformedQuery;
use crate::sql::plan::plan_step::PlanStep;
use crate::sql::plan::plan_step::PlanStep::{PrimaryRangeScan, SecondaryRangeScan};
use crate::sql::plan::steps::primary_range_scan_step::PrimaryRangeScanStep;
use crate::sql::plan::steps::secondary_range_scan_step::SecondaryRangeScanStep;
use crate::table::table::Table;

pub struct PlanOptimizer {}

impl PlanOptimizer {
    pub fn create() -> PlanOptimizer {
        PlanOptimizer {}
    }

    pub fn optimize(
        &self,
        mut plan: PlanStep,
        table: &Arc<Table>
    ) -> Result<PlanStep, SimpleDbError> {
        // plan = self.merge_scans(plan, &table)?;

        Ok(plan)
    }

    fn merge_scans(&self, parent_plan: PlanStep, table: &Arc<Table>) -> Result<PlanStep, SimpleDbError> {
        match &parent_plan {
            PlanStep::ProjectSelection(projection_step) => self.merge_scans(projection_step.source.clone(), table),
            PlanStep::Limit(limit_step) => self.merge_scans(limit_step.source.clone(), table),
            PlanStep::Filter(filter_step) => self.merge_scans(filter_step.source.clone(), table),

            PlanStep::MergeIntersection(_) |
            PlanStep::MergeUnion(_) => {
                self.merge(table, &parent_plan)
            },
            //Shouldn't be called
            PlanStep::FullScan(_) => Ok(parent_plan),
            PlanStep::PrimaryRangeScan(_) => Ok(parent_plan),
            PlanStep::SecondaryRangeScan(_) => Ok(parent_plan),
            PlanStep::PrimaryExactScan(_) => Ok(parent_plan),
            PlanStep::SecondaryExactExactScan(_) => Ok(parent_plan),
        }
    }

    fn merge(
        &self,
        table: &Arc<Table>,
        parent: &PlanStep
    ) -> Result<PlanStep, SimpleDbError> {
        let is_or = matches!(parent, PlanStep::MergeUnion(_));
        let mut right_child = parent.get_merge_right().clone();
        let mut left_child = parent.get_merge_left().clone();

        if !left_child.is_range() && !left_child.is_exact() {
            left_child = self.merge_scans(left_child.clone(), table)?;
        }
        if !right_child.is_range() && !right_child.is_exact() {
            right_child = self.merge_scans(right_child, table)?;
        }

        if left_child.is_same_column(&right_child) && is_or {
            self.merge_or(table, left_child, right_child, parent)
        } else if left_child.is_same_column(&right_child) && !is_or {
            self.merge_and(table, left_child, right_child)
        } else {
            let mut parent = parent.clone();
            parent.set_merge_right(right_child);
            parent.set_merge_left(left_child);
            Ok(parent)
        }
    }

    //Expect left and right to be the same column
    //range AND range -> range (merged) | Illegal
    //exact AND exact > Illegal
    //range AND exact -> range | Illegal
    fn merge_and(
        &self,
        table: &Arc<Table>,
        left: PlanStep,
        right: PlanStep,
    ) -> Result<PlanStep, SimpleDbError> {
        if left.is_exact() && right.is_exact() {
            Err(MalformedQuery(String::from("Illegal AND range.")))

        } else if left.is_range() && right.is_range() {
            let new_range = left.get_range_scan().and(right.get_range_scan())?;
            match left {
                PrimaryRangeScan(step) => {
                    Ok(PrimaryRangeScan(PrimaryRangeScanStep::create(table.clone(), step.selection, &step.transaction, new_range)?))
                },
                SecondaryRangeScan(step) => {
                    Ok(SecondaryRangeScan(SecondaryRangeScanStep::create(table.clone(), step.selection, &step.column_name,
                                                                         &step.transaction, new_range)?))
                },
                _ => panic!("Illegal code path")
            }

        } else if left.is_exact() && right.is_range() {
            let range_right = right.get_range_scan();
            let exact_value_left = left.get_exact_value();

            if range_right.is_inside_range(exact_value_left) {
                Ok(right.clone())
            } else {
                Err(MalformedQuery(String::from("Illegal AND range.")))
            }

        } else { //left.is_range() && right.is_exact()
            let range_left = left.get_range_scan();
            let exact_value_right = right.get_exact_value();

            if range_left.is_inside_range(exact_value_right) {
                Ok(left.clone())
            } else {
                Err(MalformedQuery(String::from("Illegal AND range.")))
            }
        }
    }

    //Expect left and right to be the same column
    //range OR range -> range (merged) | Illegal
    //exact OR exact > union(exact, exact)
    //range OR exact -> range | union(range, exact)
    fn merge_or(
        &self,
        table: &Arc<Table>,
        left: PlanStep,
        right: PlanStep,
        parent: &PlanStep,
    ) -> Result<PlanStep, SimpleDbError> {
        if left.is_exact() && right.is_exact() {
            Ok(parent.clone())
        } else if left.is_range() && right.is_range() {
            //There is small benefit when trying to optimize OR scans!
            Ok(parent.clone())
        } else if left.is_exact() && right.is_range() {
            let exact_value_left = left.get_exact_value();
            let range_right = right.get_range_scan();

            if range_right.is_inside_range(exact_value_left) {
                match right {
                    SecondaryRangeScan(step) => {
                        Ok(SecondaryRangeScan(
                            SecondaryRangeScanStep::create(table.clone(), step.selection, &step.column_name, &step.transaction, range_right)?
                        ))
                    }
                    PrimaryRangeScan(step) => {
                        Ok(PrimaryRangeScan(
                            PrimaryRangeScanStep::create(table.clone(), step.selection, &step.transaction, range_right)?
                        ))
                    }
                    _ => panic!("Illegal code path")
                }
            } else {
                Ok(parent.clone())
            }

        } else {
            //left.is_range() && right.is_exact()
            let exact_value_right = right.get_exact_value();
            let range_left = left.get_range_scan();

            if range_left.is_inside_range(exact_value_right) {
                match left {
                    SecondaryRangeScan(step) => {
                        Ok(SecondaryRangeScan(
                            SecondaryRangeScanStep::create(table.clone(), step.selection, &step.column_name, &step.transaction, range_left)?
                        ))
                    }
                    PrimaryRangeScan(step) => {
                        Ok(PrimaryRangeScan(
                            PrimaryRangeScanStep::create(table.clone(), step.selection, &step.transaction, range_left)?
                        ))
                    }
                    _ => panic!("Illegal code path")
                }
            } else {
                Ok(parent.clone())
            }
        }
    }
}