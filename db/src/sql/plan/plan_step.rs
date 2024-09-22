use crate::Row;

pub trait PlanStep {
    fn next(&mut self) -> Option<&Row>;
}
