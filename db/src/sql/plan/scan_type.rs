use crate::sql::expression::{BinaryOperator, Expression};
use crate::sql::statement::Limit;
use crate::Table;
use shared::SimpleDbError;
use std::sync::Arc;
use SimpleDbError::MalformedQuery;

#[derive(Debug, Clone, PartialEq)]
pub enum ScanType {
    Full,
    // min < values < expression
    Range(RangeScan),
    //This expression should produce the literal value which will be the primary key
    Exact(Expression)
}

#[derive(Debug, Clone, PartialEq)]
pub struct RangeScan {
    start: Option<Expression>,
    start_inclusive: bool,
    end: Option<Expression>,
    end_inclusive: bool,
}

impl ScanType {
    //Expect expressions to have been passed to evaluate_deterministic() before calling this function
    pub fn get_scan_type(
        expression: &Expression,
        table: &Arc<Table>,
        limit: &Limit
    ) -> Result<ScanType, SimpleDbError> {
        let primary_column_name = table.get_primary_column_data().unwrap()
            .column_name;

        match expression {
            Expression::Binary(operator, left, right) => {
                Self::get_scan_type_binary_expr(table, *operator, left, right, limit)
            },
            Expression::Unary(_, expr) => {
                if expr.is_deterministic() {
                    Ok(ScanType::Exact(*expr.clone()))
                } else {
                    Ok(ScanType::Full)
                }
            },
            Expression::Identifier(identifier) => {
                if identifier == &primary_column_name {
                    Ok(ScanType::Exact(expression.clone()))
                } else {
                    //Not querying by primary key -> full scan
                    Ok(ScanType::Full)
                }
            }
            //The rest of expressions are literals: String, Numbers, boolean etc.
            other => Ok(ScanType::Exact(other.clone())),
        }
    }

    fn get_scan_type_binary_expr(
        table: &Arc<Table>,
        operator: BinaryOperator,
        left: &Box<Expression>,
        right: &Box<Expression>,
        limit: &Limit
    ) -> Result<ScanType, SimpleDbError> {
        let primary_column_name = table.get_primary_column_data().unwrap().column_name;

        match operator {
            BinaryOperator::And => {
                Self::get_scan_type_logical_expr(operator, left, right, table, limit)
            },
            BinaryOperator::Or => {
                Self::get_scan_type_logical_expr(operator, left, right, table, limit)
            },
            BinaryOperator::Add |
            BinaryOperator::Subtract |
            BinaryOperator::Multiply |
            BinaryOperator::Divide => {
                Ok(ScanType::Full)
            },
            BinaryOperator::Equal => {
                if right.is_deterministic() {
                    Ok(ScanType::Exact(Expression::Binary(operator, left.clone(), right.clone())))
                } else {
                    Ok(ScanType::Full)
                }
            }
            BinaryOperator::NotEqual => {
                Ok(ScanType::Full)
            },
            BinaryOperator::GreaterEqual |
            BinaryOperator::Greater => {
                if right.is_deterministic() && left.is_identifier(&primary_column_name) {
                    Ok(ScanType::Range(RangeScan{
                        start: Some(*right.clone()),
                        start_inclusive: matches!(operator, BinaryOperator::GreaterEqual),
                        end: None,
                        end_inclusive: false,
                    }))
                } else {
                    Ok(ScanType::Full)
                }
            },
            BinaryOperator::LessEqual |
            BinaryOperator::Less => {
                if right.is_deterministic() && left.is_identifier(&primary_column_name){
                    Ok(ScanType::Range(RangeScan{
                        start: None,
                        start_inclusive: false,
                        end: Some(*right.clone()),
                        end_inclusive: matches!(operator, BinaryOperator::LessEqual),
                    }))
                } else {
                    Ok(ScanType::Full)
                }
            }
        }
    }

    fn get_scan_type_logical_expr(
        binary_operator: BinaryOperator,
        left: &Box<Expression>,
        right: &Box<Expression>,
        table: &Arc<Table>,
        limit: &Limit
    ) -> Result<ScanType, SimpleDbError> {
        let scan_type_left = Self::get_scan_type(left, table, &limit)?;
        let scan_type_right = Self::get_scan_type(right, table, &limit)?;

        if scan_type_left == scan_type_right {
            return Self::merge_scan_types(binary_operator, scan_type_left, scan_type_right);
        }

        match binary_operator {
            BinaryOperator::And => {
                let full_range = (scan_type_left.is_full() && scan_type_right.is_range()) ||
                    (scan_type_left.is_range() && scan_type_right.is_full());
                let full_exact = (scan_type_left.is_full() && scan_type_right.is_exact()) ||
                    (scan_type_left.is_exact() && scan_type_right.is_full());
                let range_exact = (scan_type_left.is_range() && scan_type_right.is_exact()) ||
                    (scan_type_left.is_exact() && scan_type_right.is_range());

                if full_range {
                    let range = match scan_type_left {
                        ScanType::Full => match scan_type_right { ScanType::Range(range) => range, _ => panic!("") },
                        ScanType::Range(range) => range,
                        _ => panic!("Invalid code path")
                    };
                    return Ok(ScanType::Range(range));
                } else if full_exact || range_exact {
                    return Ok(ScanType::Exact(Expression::Binary(binary_operator, left.clone(), right.clone())));
                }

                 panic!("Illegal code path");
            },
            BinaryOperator::Or => Ok(ScanType::Full),
            _ => panic!("Illegal code path")
        }
    }

    fn merge_scan_types(
        binary_operator: BinaryOperator,
        a: ScanType,
        b: ScanType
    ) -> Result<ScanType, SimpleDbError> {
        match (a, b) {
            (ScanType::Full, ScanType::Full) => Ok(ScanType::Full),
            (ScanType::Exact(a), ScanType::Exact(b)) => {
                if matches!(binary_operator, BinaryOperator::And) {
                    Err(MalformedQuery(String::from("Invalid range")))
                } else {
                    Ok(ScanType::Full)
                }
            },
            (ScanType::Range(range_left), ScanType::Range(range_right)) => {
                match binary_operator {
                    BinaryOperator::And => {
                        Ok(ScanType::Range(range_left.and(range_right)?))
                    },
                    BinaryOperator::Or => Ok(ScanType::Full), //TODO Optimize
                    _ => panic!("")
                }
            },
            _ => panic!("Illegal code path")
        }
    }

    pub fn is_range(&self) -> bool {
        matches!(self, ScanType::Range(_))
    }

    pub fn is_exact(&self) -> bool {
        matches!(self, ScanType::Exact(_))
    }

    pub fn is_full(&self) -> bool {
        matches!(self, ScanType::Full)
    }
}

impl RangeScan {
    // WHERE id > 100 AND id < 200 -> Range(100, _) AND Range(_, 200) = Range(100, 200)
    // WHERE id > 200 AND id < 100 -> Range(200, _) AND Range(_, 100) = Range(_, _) Invalid!
    // WHERE id > 100 AND id > 200 -> Range(100, _) AND Range(200, _) = Range(200, _)
    // WHERE id < 100 AND id < 200 -> Range(_, 100) AND Range(_, 200) = Range(_, 100)
    pub fn and(&self, other: RangeScan) -> Result<RangeScan, SimpleDbError> {
        if self.has_only_start() && other.has_only_end() {
            let range_scan = RangeScan {
                start: self.start.clone(),
                start_inclusive: self.start_inclusive,
                end: other.end.clone(),
                end_inclusive: other.end_inclusive
            };
            range_scan.is_valid()?;
            return Ok(range_scan);
        }
        if self.has_only_end() && other.has_only_start() {
            let range_scan = RangeScan {
                start: other.start.clone(),
                start_inclusive: other.start_inclusive,
                end: self.end.clone(),
                end_inclusive: self.end_inclusive
            };
            range_scan.is_valid()?;
            return Ok(range_scan);
        }
        let mut result = RangeScan::empty();
        if self.has_start() && other.has_start() {
            let start_self = self.start.clone().unwrap();
            let start_other = other.start.clone().unwrap();
            let is_self_less_than_other = start_self.less_equal(&start_other)?.get_boolean()?;
            let is_new_start_inclusive = if is_self_less_than_other { self.start_inclusive } else { other.start_inclusive };
            let new_start = if is_self_less_than_other { start_self } else { start_other };

            result.start_inclusive = is_new_start_inclusive;
            result.start = Some(new_start);
        }
        if self.has_end() && other.has_end() {
            let end_other = other.end.clone().unwrap();
            let end_self = self.end.clone().unwrap();
            let is_self_less_than_other = end_self.less_equal(&end_other)?.get_boolean()?;
            let is_new_end_inclusive = if is_self_less_than_other { self.start_inclusive } else { other.start_inclusive };
            let new_end = if is_self_less_than_other { end_self } else { end_other };

            result.end_inclusive = is_new_end_inclusive;
            result.end = Some(new_end);
        }

        result.is_valid()?;
        Ok(result)
    }

    fn is_valid(&self) -> Result<(), SimpleDbError> {
        if self.has_start() && self.has_end() {
            let start = self.start.as_ref().unwrap();
            let end = self.end.as_ref().unwrap();

            if start.greater_equal(end)?.get_boolean()? {
                return Err(MalformedQuery(String::from("Invalid range")))
            }
        }

        Ok(())
    }

    pub fn has_end(&self) -> bool {
        self.end.is_some()
    }

    pub fn has_start(&self) -> bool {
        self.start.is_some()
    }

    pub fn has_only_start(&self) -> bool {
        self.start.is_some() && self.end.is_none()
    }

    pub fn has_only_end(&self) -> bool {
        self.start.is_none() && self.end.is_some()
    }

    pub fn empty() -> RangeScan {
        RangeScan {
            start: None,
            start_inclusive: false,
            end: None,
            end_inclusive: false,
        }
    }
}