use shared::SimpleDbError;
use crate::{PlanStepDesc, Row};
use crate::selection::Selection;
use crate::sql::plan::plan_step::{PlanStep, PlanStepTrait};

pub struct ProjectSelectionStep {
    source: PlanStep,
    selection_to_project: Selection,
}

impl ProjectSelectionStep {
    pub fn create(
        required_selection: Selection,
        source: PlanStep,
    ) -> ProjectSelectionStep {
        ProjectSelectionStep {
            selection_to_project: required_selection,
            source
        }
    }
}

impl PlanStepTrait for ProjectSelectionStep {
    fn next(&mut self) -> Result<Option<Row>, SimpleDbError> {
        match self.source.next()? {
            Some(mut row) => {
                row.project_selection(&self.selection_to_project);
                Ok(Some(row))
            },
            None => Ok(None)
        }
    }

    fn desc(&self) -> PlanStepDesc {
        PlanStepDesc::ProjectionSelectionStep(
            self.selection_to_project.clone(),
            Box::new(self.source.desc())
        )
    }
}