use std::sync::Arc;
use crate::sql::expression::{BinaryOperator, Expression};
use crate::Table;

#[derive(Debug, Clone, PartialEq)]
pub enum ScanType {
    Full,
    Range,
    Exact
}

impl ScanType {
    pub fn get_scan_type(
        expression: &Expression,
        table: &Arc<Table>
    ) -> ScanType {
        let primary_column_name = table.get_primary_column_data().unwrap()
            .column_name;

        match expression {
            Expression::Binary(operator, left, right) => {
                Self::get_scan_type_binary_expr(&primary_column_name, *operator, left, right)
            },
            Expression::Unary(_, expr) => {
                if expr.is_deterministic() {
                    ScanType::Exact
                } else {
                    ScanType::Full
                }
            }
            Expression::Identifier(identifier) => {
                if identifier == &primary_column_name {
                    ScanType::Exact
                } else {
                    //Not querying by primary key -> full scan
                    ScanType::Full
                }
            }
            //The rest of expressions are literals: String, Numbers, boolean etc.
            _ => ScanType::Exact,
        }
    }

    fn get_scan_type_binary_expr(
        primary_column_name: &String,
        operator: BinaryOperator,
        left: &Box<Expression>,
        right: &Box<Expression>
    ) -> ScanType {
        if !left.is_identifier(primary_column_name) {
            //Not querying by primary key -> full scan
            return ScanType::Full;
        }

        match operator {
            BinaryOperator::And => {
                let scan_type_left = left.get_scan_type(primary_column_name);
                let scan_type_right = right.get_scan_type(primary_column_name);
                Self::scan_type_logical_operation(scan_type_left, scan_type_right, BinaryOperator::And)
            },
            BinaryOperator::Or => {
                let scan_type_left = left.get_scan_type(primary_column_name);
                let scan_type_right = right.get_scan_type(primary_column_name);
                Self::scan_type_logical_operation(scan_type_left, scan_type_right, BinaryOperator::Or)
            },

            BinaryOperator::Add |
            BinaryOperator::Subtract |
            BinaryOperator::Multiply |
            BinaryOperator::Divide => {
                if left.is_deterministic() && right.is_deterministic() {
                    ScanType::Exact
                } else {
                    ScanType::Full
                }
            },

            BinaryOperator::Equal => {
                if right.is_deterministic() {
                    ScanType::Exact
                } else {
                    ScanType::Full
                }
            }
            BinaryOperator::NotEqual |
            BinaryOperator::Greater |
            BinaryOperator::Less |
            BinaryOperator::GreaterEqual |
            BinaryOperator::LessEqual  => {
                if right.is_deterministic() {
                    ScanType::Range
                } else {
                    ScanType::Full
                }
            }
        }
    }

    fn scan_type_logical_operation(
        left: ScanType,
        right: ScanType,
        binary_operator: BinaryOperator
    ) -> ScanType {
        if left == right {
            return left
        }

        match binary_operator {
            BinaryOperator::And => {
                let full_range = (matches!(left, ScanType::Full) && matches!(right, ScanType::Range)) ||
                    matches!(left, ScanType::Range) && matches!(right, ScanType::Full);
                let full_exact = (matches!(left, ScanType::Full) && matches!(right, ScanType::Exact)) ||
                    matches!(left, ScanType::Exact) && matches!(right, ScanType::Full);
                let range_exact = (matches!(left, ScanType::Range) && matches!(right, ScanType::Exact)) ||
                    matches!(left, ScanType::Exact) && matches!(right, ScanType::Range);

                if full_range {
                    return ScanType::Range;
                } else if full_exact {
                    return ScanType::Exact;
                } else if range_exact {
                    return ScanType::Exact;
                }

                panic!("Illegal code path");
            },
            BinaryOperator::Or => ScanType::Full,
            _ => panic!("Illegal code path")
        }
    }
}