use crate::sql::scan_type::ScanType;
use std::cmp::PartialEq;

#[derive(Debug, PartialEq)]
pub enum Expression {
    None,

    Binary(BinaryOperator, Box<Expression>, Box<Expression>),
    Unary(UnaryOperator, Box<Expression>),
    Identifier(String),
    String(String),
    Boolean(bool),
    NumberF64(f64),
    NumberI64(i64),
}

#[derive(Debug, PartialEq)]
pub enum UnaryOperator {
    Plus,
    Minus,
}

#[derive(Debug, PartialEq, Clone, Copy)]
pub enum BinaryOperator {
    Add,
    Subtract,
    Multiply,
    Divide,
    And,
    Or,
    NotEqual,
    Equal,
    Greater,
    GreaterEqual,
    Less,
    LessEqual,
}

impl Expression {
    pub fn get_scan_type(
        &self,
        primary_column_name: &String,
    ) -> ScanType {
        match self {
            Expression::Binary(operator, left, right) => {
                self.get_scan_type_binary_expr(primary_column_name, *operator, left, right)
            },
            Expression::Unary(_, expr) => {
                if expr.is_deterministic() {
                    ScanType::Exact
                } else {
                    ScanType::Full
                }
            }
            Expression::Identifier(identifier) => {
                if identifier == primary_column_name {
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
        &self,
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
                self.scan_type_logical_operation(scan_type_left, scan_type_right, BinaryOperator::And)
            },
            BinaryOperator::Or => {
                let scan_type_left = left.get_scan_type(primary_column_name);
                let scan_type_right = right.get_scan_type(primary_column_name);
                self.scan_type_logical_operation(scan_type_left, scan_type_right, BinaryOperator::Or)
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
        &self,
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

    fn is_identifier(&self, expected_identifier: &String) -> bool {
        match self {
            Expression::Identifier(actual_identifier) => actual_identifier == expected_identifier,
            _ => false
        }
    }

    pub fn is_deterministic(&self) -> bool {
        match self {
            Expression::None => panic!(""),
            Expression::Binary(_, left, right) => {
                left.is_deterministic() && right.is_deterministic()
            },
            Expression::Unary(_, expr) => expr.is_deterministic(),
            Expression::String(_) => true,
            Expression::Identifier(_) => true,
            Expression::Boolean(_) => true,
            Expression::NumberF64(_) => true,
            Expression::NumberI64(_) => true,
        }
    }
}

impl BinaryOperator {
    //Takes booleans, Produces boolean
    pub fn is_logical(&self) -> bool {
        match self {
            BinaryOperator::And |
            BinaryOperator::Or => true,
            _ => false
        }
    }

    //Takes numbers, Produces boolean
    pub fn is_arithmetic(&self) -> bool {
        match self {
            BinaryOperator::Add |
            BinaryOperator::Subtract |
            BinaryOperator::Multiply |
            BinaryOperator::Divide => true,
            BinaryOperator::And |
            BinaryOperator::Or |
            BinaryOperator::NotEqual |
            BinaryOperator::Equal |
            BinaryOperator::Greater |
            BinaryOperator::GreaterEqual |
            BinaryOperator::Less |
            BinaryOperator::LessEqual => false
        }
    }

    //Takes comparable args, Produces boolean
    pub fn is_comparation(&self) -> bool {
        match self {
            BinaryOperator::Add |
            BinaryOperator::Subtract |
            BinaryOperator::Multiply |
            BinaryOperator::Divide => false,
            BinaryOperator::And |
            BinaryOperator::Or |
            BinaryOperator::NotEqual |
            BinaryOperator::Equal |
            BinaryOperator::Greater |
            BinaryOperator::GreaterEqual |
            BinaryOperator::Less |
            BinaryOperator::LessEqual => true
        }
    }
}