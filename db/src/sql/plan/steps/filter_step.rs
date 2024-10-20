use crate::sql::parser::expression::Expression;
use crate::sql::plan::plan_step::{PlanStep, PlanStepDesc, PlanStepTrait};
use crate::Row;
use shared::SimpleDbError;
use crate::sql::execution::expression_evaluator::evaluate_where_expression;

#[derive(Clone)]
pub struct FilterStep {
    pub(crate) filter_expression: Expression,
    pub(crate) source: PlanStep,
}

impl FilterStep {
    pub(crate) fn create(
        filter_expression: Expression,
        source: PlanStep,
    ) -> FilterStep {
        FilterStep {
            filter_expression,
            source
        }
    }
}

impl PlanStepTrait for FilterStep {
    fn next(&mut self) -> Result<Option<Row>, SimpleDbError> {
        while let Some(next_row) = self.source.next()? {
            if evaluate_where_expression(&next_row, &self.filter_expression)? {
                return Ok(Some(next_row));
            }
        }

        Ok(None)
    }

    fn desc(&self) -> PlanStepDesc {
        PlanStepDesc::Filter(Box::new(self.source.desc()))
    }
}