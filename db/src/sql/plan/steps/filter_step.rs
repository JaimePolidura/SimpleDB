use shared::SimpleDbError;
use crate::sql::expression::Expression;
use crate::sql::plan::plan_step::PlanStep;
use crate::Row;
use crate::sql::expression_evaluator::evaluate_where_expression;

pub struct FilterStep {
    filter_expression: Expression,
    source: Box<dyn PlanStep>,
}

impl FilterStep {
    pub fn create(
        filter_expression: Expression,
        source: Box<dyn PlanStep>,
    ) -> FilterStep {
        FilterStep {
            filter_expression,
            source
        }
    }
}

impl PlanStep for FilterStep {
    fn next(&mut self) -> Result<Option<Row>, SimpleDbError> {
        while let Some(next_row) = self.source.next()? {
            if evaluate_where_expression(&next_row, &self.filter_expression)? {
                return Ok(Some(next_row));
            }
        }

        Ok(None)
    }
}