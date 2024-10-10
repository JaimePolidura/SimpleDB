use crate::sql::expression::{BinaryOperator, Expression};
use crate::sql::plan::scan_type::{RangeScan, ScanType};
use crate::table::table::Table;
use shared::SimpleDbError;
use shared::SimpleDbError::MalformedQuery;
use std::sync::Arc;

pub struct ScanTypeAnalyzer {
    table: Arc<Table>,
    expression: Expression,
}

impl ScanTypeAnalyzer {
    //Expect expressions to have been passed to evaluate_constant() before calling this function
    pub fn create(
        table: Arc<Table>,
        expression: Expression,
    ) -> ScanTypeAnalyzer {
        ScanTypeAnalyzer {
            expression,
            table,
        }
    }

    pub fn analyze(&self) -> Result<ScanType, SimpleDbError> {
        match &self.expression {
            Expression::Binary(operator, left, right) => {
                self.get_scan_type_binary_expr(*operator, &left, &right)
            },
            Expression::Unary(_, _) => Err(MalformedQuery(String::from("Illegal unary expression"))),
            _ => Err(MalformedQuery(String::from("Illegal literal expression"))),
        }
    }

    fn get_scan_type_binary_expr(
        &self,
        operator: BinaryOperator,
        left: &Box<Expression>,
        right: &Box<Expression>,
    ) -> Result<ScanType, SimpleDbError> {
        match operator {
            BinaryOperator::And => {
                self.get_scan_type_logical_expr(operator, left, right)
            },
            BinaryOperator::Or => {
                self.get_scan_type_logical_expr(operator, left, right)
            },
            BinaryOperator::Add |
            BinaryOperator::Subtract |
            BinaryOperator::Multiply |
            BinaryOperator::Divide => {
                Ok(ScanType::Full)
            },
            BinaryOperator::Equal => {
                if right.is_constant() && self.table.is_secondary_indexed(&left.get_identifier()?) {
                    Ok(ScanType::ExactSecondary(left.get_identifier()?, *right.clone()))
                } else if right.is_constant() && left.identifier_eq(&self.table.primary_column_name) {
                    Ok(ScanType::ExactPrimary(*right.clone()))
                } else {
                    Ok(ScanType::Full)
                }
            }
            BinaryOperator::NotEqual => {
                Ok(ScanType::Full)
            },
            BinaryOperator::GreaterEqual |
            BinaryOperator::Greater => {
                if right.is_constant() && left.identifier_eq(&self.table.primary_column_name) {
                    Ok(ScanType::Range(RangeScan{
                        column_name: right.get_identifier()?,
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
                if right.is_constant() && left.identifier_eq(&self.table.primary_column_name){
                    Ok(ScanType::Range(RangeScan{
                        column_name: right.get_identifier()?,
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
        &self,
        binary_operator: BinaryOperator,
        left: &Box<Expression>,
        right: &Box<Expression>,
    ) -> Result<ScanType, SimpleDbError> {
        let scan_type_right = self.analyze_sub_expression(right)?;
        let scan_type_left = self.analyze_sub_expression(left)?;

        self.merge_scan_types(binary_operator, scan_type_left, scan_type_right)
    }

    //Rules:
    // full AND|OR full -> full
    // full AND primary|secondary -> primary|secondary
    // full OR primary|secondary -> full
    // full AND merge_union|merge_intersection -> merge|merge_intersection
    // full OR merge_union|merge_intersection -> full
    // full OR range -> full
    // full AND range -> range
    //
    // primary AND primary -> error
    // primary OR primary -> merge_union
    // primary AND secondary -> primary
    // primary OR secondary -> secondary
    // primary AND merge_union|merge_intersection -> primary
    // primary OR merge_union|merge_intersection -> merge|merge_intersection
    // primary AND range -> primary
    // primary OR range -> merge
    //
    // secondary AND secondary -> error
    // secondary OR secondary -> merge
    // secondary AND merge_union|merge_intersection -> secondary
    // secondary OR merge_union|merge_intersection -> merge_union|merge_intersection
    // secondary AND range -> secondary
    // secondary OR range -> merge
    //
    // merge_union AND merge_union|merge_intersection -> merge_intersection
    // merge_union OR merge_union|merge_intersection -> merge_union
    // merge_union AND range -> merge_intersection
    // merge_union OR range -> merge_union
    //
    // merge_intersection AND merge_intersection -> merge_intersection
    // merge_intersection OR merge_intersection -> merge_union
    // merge_intersection AND range -> merge_intersection
    // merge_intersection OR range -> merge_union
    //
    // range AND range -> merge_intersection
    // range OR range -> merge_union
    fn merge_scan_types(
        &self,
        binary_operator: BinaryOperator,
        a: ScanType,
        b: ScanType
    ) -> Result<ScanType, SimpleDbError> {
        match (&a, &b) {
            //Full rules
            (ScanType::Full, ScanType::Full) => Ok(ScanType::Full),
            (ScanType::ExactPrimary(primary_expr), ScanType::Full) |
            (ScanType::Full, ScanType::ExactPrimary(primary_expr)) => {
                if matches!(binary_operator, BinaryOperator::And) {
                    Ok(ScanType::ExactPrimary(primary_expr.clone()))
                } else { //Or binary operator
                    Ok(ScanType::Full)
                }
            },
            (ScanType::ExactSecondary(column_name, secondary_expr), ScanType::Full) |
            (ScanType::Full, ScanType::ExactSecondary(column_name, secondary_expr)) => {
                if matches!(binary_operator, BinaryOperator::And) {
                    Ok(ScanType::ExactSecondary(column_name.clone(), secondary_expr.clone()))
                } else { //Or binary operator
                    Ok(ScanType::Full)
                }
            },
            (ScanType::Full, ScanType::MergeUnion(left_merge, right_merge)) |
            (ScanType::MergeUnion(left_merge, right_merge), ScanType::Full) => {
                if matches!(binary_operator, BinaryOperator::And) {
                    Ok(ScanType::MergeUnion(left_merge.clone(), right_merge.clone()))
                } else { //Or binary operator
                    Ok(ScanType::Full)
                }
            },
            (ScanType::Full, ScanType::MergeIntersection(left_merge, right_merge)) |
            (ScanType::MergeIntersection(left_merge, right_merge), ScanType::Full) => {
                if matches!(binary_operator, BinaryOperator::And) {
                    Ok(ScanType::MergeIntersection(left_merge.clone(), right_merge.clone()))
                } else { //Or binary operator
                    Ok(ScanType::Full)
                }
            },
            (ScanType::Full, ScanType::Range(range)) |
            (ScanType::Range(range), ScanType::Full) => {
                if matches!(binary_operator, BinaryOperator::And) {
                    Ok(ScanType::Range(range.clone()))
                } else { //Or binary operator
                    Ok(ScanType::Full)
                }
            }
            //Primary rules
            (ScanType::ExactPrimary(_), ScanType::ExactPrimary(_)) => {
                if matches!(binary_operator, BinaryOperator::And) {
                    Err(MalformedQuery(String::from("Invalid range")))
                } else { //Or binary operator
                    Ok(ScanType::MergeUnion(Box::new(a.clone()), Box::new(b.clone())))
                }
            },
            (ScanType::ExactPrimary(primary_expr), ScanType::ExactSecondary(_, _)) |
            (ScanType::ExactSecondary(_, _), ScanType::ExactPrimary(primary_expr)) => {
                if matches!(binary_operator, BinaryOperator::And) {
                    Ok(ScanType::ExactPrimary(primary_expr.clone()))
                } else { //Or binary operator
                    Ok(ScanType::MergeUnion(Box::new(a.clone()), Box::new(b.clone())))
                }
            },
            (ScanType::ExactPrimary(primary_expr), ScanType::MergeUnion(_, _)) |
            (ScanType::MergeUnion(_, _), ScanType::ExactPrimary(primary_expr)) => {
                if matches!(binary_operator, BinaryOperator::And) {
                    Ok(ScanType::ExactPrimary(primary_expr.clone()))
                } else {
                    Ok(ScanType::MergeUnion(Box::new(a.clone()), Box::new(b.clone())))
                }
            },
            (ScanType::ExactPrimary(primary_expr), ScanType::MergeIntersection(_, _)) |
            (ScanType::MergeIntersection(_, _), ScanType::ExactPrimary(primary_expr)) => {
                if matches!(binary_operator, BinaryOperator::And) {
                    Ok(ScanType::ExactPrimary(primary_expr.clone()))
                } else {
                    Ok(ScanType::MergeIntersection(Box::new(a.clone()), Box::new(b.clone())))
                }
            },
            (ScanType::ExactPrimary(primary_expr), ScanType::Range(_)) |
            (ScanType::Range(_), ScanType::ExactPrimary(primary_expr)) => {
                if matches!(binary_operator, BinaryOperator::And) {
                    Ok(ScanType::ExactPrimary(primary_expr.clone()))
                } else {
                    Ok(ScanType::MergeUnion(Box::new(a.clone()), Box::new(b.clone())))
                }
            },
            //Secondary rules
            (ScanType::ExactSecondary(_, _), ScanType::ExactSecondary(_, _)) => {
                if matches!(binary_operator, BinaryOperator::And) {
                    Err(MalformedQuery(String::from("Invalid range")))
                } else {
                    Ok(ScanType::MergeUnion(Box::new(a.clone()), Box::new(b.clone())))
                }
            }
            (ScanType::ExactSecondary(column, secondary_expr), ScanType::MergeUnion(_, _)) |
            (ScanType::MergeUnion(_, _), ScanType::ExactSecondary(column, secondary_expr)) => {
                if matches!(binary_operator, BinaryOperator::And) {
                    Ok(ScanType::ExactSecondary(column.clone(), secondary_expr.clone()))
                } else {
                    Ok(ScanType::MergeUnion(Box::new(a.clone()), Box::new(b.clone())))
                }
            },
            (ScanType::ExactSecondary(column, secondary_expr), ScanType::MergeIntersection(_, _)) |
            (ScanType::MergeIntersection(_, _), ScanType::ExactSecondary(column, secondary_expr)) => {
                if matches!(binary_operator, BinaryOperator::And) {
                    Ok(ScanType::ExactSecondary(column.clone(), secondary_expr.clone()))
                } else {
                    Ok(ScanType::MergeIntersection(Box::new(a.clone()), Box::new(b.clone())))
                }
            },
            (ScanType::ExactSecondary(column, secondary_expr), ScanType::Range(_)) |
            (ScanType::Range(_), ScanType::ExactSecondary(column, secondary_expr)) => {
                if matches!(binary_operator, BinaryOperator::And) {
                    Ok(ScanType::ExactSecondary(column.clone(), secondary_expr.clone()))
                } else {
                    Ok(ScanType::MergeUnion(Box::new(a.clone()), Box::new(b.clone())))
                }
            },
            //merge rules
            (ScanType::MergeUnion(_, _), ScanType::MergeUnion(_, _)) |
            (ScanType::MergeUnion(_, _), ScanType::MergeIntersection(_, _)) |
            (ScanType::MergeIntersection(_, _), ScanType::MergeUnion(_, _)) => {
                if matches!(binary_operator, BinaryOperator::And) {
                    Ok(ScanType::MergeIntersection(Box::new(a.clone()), Box::new(b.clone())))
                } else {
                    Ok(ScanType::MergeUnion(Box::new(a.clone()), Box::new(b.clone())))
                }
            },
            (ScanType::MergeUnion(_, _), ScanType::Range(_)) |
            (ScanType::Range(_), ScanType::MergeUnion(_, _)) => {
                if matches!(binary_operator, BinaryOperator::And) {
                    Ok(ScanType::MergeIntersection(Box::new(a.clone()), Box::new(b.clone())))
                } else {
                    Ok(ScanType::MergeUnion(Box::new(a.clone()), Box::new(b.clone())))
                }
            },
            //Intersection merge rules
            (ScanType::MergeIntersection(_, _), ScanType::MergeIntersection(_, _)) => {
                if matches!(binary_operator, BinaryOperator::And) {
                    Ok(ScanType::MergeIntersection(Box::new(a.clone()), Box::new(b.clone())))
                } else {
                    Ok(ScanType::MergeUnion(Box::new(a.clone()), Box::new(b.clone())))
                }
            }
            (ScanType::MergeIntersection(_, _), ScanType::Range(_)) |
            (ScanType::Range(_), ScanType::MergeIntersection(_, _)) => {
                if matches!(binary_operator, BinaryOperator::And) {
                    Ok(ScanType::MergeIntersection(Box::new(a.clone()), Box::new(b.clone())))
                } else {
                    Ok(ScanType::MergeUnion(Box::new(a.clone()), Box::new(b.clone())))
                }
            },
            //Range rules
            (ScanType::Range(range_left), ScanType::Range(range_right)) => {
                if matches!(binary_operator, BinaryOperator::And) && range_left.same_column(range_right){
                    //Merge ranges
                    Ok(ScanType::Range(range_left.and(range_right.clone())?))
                } else if matches!(binary_operator, BinaryOperator::And) && !range_left.same_column(range_right) {
                    Ok(ScanType::MergeIntersection(Box::new(a.clone()), Box::new(b.clone())))
                } else if matches!(binary_operator, BinaryOperator::Or) {
                    Ok(ScanType::MergeUnion(Box::new(a.clone()), Box::new(b.clone())))
                } else {
                    panic!("Illegal code path");
                }
            },
        }
    }

    fn analyze_sub_expression(&self, expression: &Expression) -> Result<ScanType, SimpleDbError> {
        let analyzer = ScanTypeAnalyzer::create(
            self.table.clone(),
            expression.clone(),
        );

        analyzer.analyze()
    }
}

#[cfg(test)]
mod test {
    use crate::sql::expression::{BinaryOperator, Expression};
    use crate::sql::plan::scan_type::ScanType;
    use crate::sql::plan::scan_type_analyzer::ScanTypeAnalyzer;
    use crate::table::table::Table;
    use crate::value::Value;
    use crate::ColumnDescriptor;

    //WHERE id >= 1 OR dinero < 100
    #[test]
    fn range_compound_or() {
        let analyzer = ScanTypeAnalyzer::create(
            Table::create_mock(vec![ColumnDescriptor::create_primary("id"), ColumnDescriptor::create("dinero", 1)]),
            Expression::Binary(
                BinaryOperator::Or,
                Box::new(Expression::Binary(
                    BinaryOperator::GreaterEqual,
                    Box::new(Expression::Identifier(String::from("id"))),
                    Box::new(Expression::Literal(Value::I64(1))),
                )),
                Box::new(Expression::Binary(
                    BinaryOperator::Less,
                    Box::new(Expression::Identifier(String::from("dinero"))),
                    Box::new(Expression::Literal(Value::I64(100))),
                ))
            )
        );
        let result = analyzer.analyze().unwrap();
        assert_eq!(result, ScanType::Full);
    }

    // WHERE id >= 1 AND dinero < 100
    #[test]
    fn range_compound_and() {
        let analyzer = ScanTypeAnalyzer::create(
            Table::create_mock(vec![ColumnDescriptor::create_primary("id"), ColumnDescriptor::create("dinero", 1)]),
            Expression::Binary(
                BinaryOperator::And,
                Box::new(Expression::Binary(
                    BinaryOperator::GreaterEqual,
                    Box::new(Expression::Identifier(String::from("id"))),
                    Box::new(Expression::Literal(Value::I64(1))),
                )),
                Box::new(Expression::Binary(
                    BinaryOperator::Less,
                    Box::new(Expression::Identifier(String::from("dinero"))),
                    Box::new(Expression::Literal(Value::I64(100))),
                ))
            )
        );
        let result = analyzer.analyze().unwrap();

        let range_scan = match result { ScanType::Range(value) => value, _ => panic!("") };
        assert!(range_scan.start.is_some());
        assert!(range_scan.start_inclusive);
        assert_eq!(range_scan.start.as_ref().unwrap().clone(), Expression::Literal(Value::I64(1)));

        assert!(range_scan.end.is_some());
        assert!(!range_scan.end_inclusive);
        assert_eq!(range_scan.end.as_ref().unwrap().clone(), Expression::Literal(Value::I64(100)));
    }

    //WHERE id >= 1 OR dinero == 100
    #[test]
    fn simple_range_or() {
        let analyzer = ScanTypeAnalyzer::create(
            Table::create_mock(vec![ColumnDescriptor::create_primary("id"), ColumnDescriptor::create("dinero", 1)]),
            Expression::Binary(
                BinaryOperator::Or,
                Box::new(Expression::Binary(
                    BinaryOperator::GreaterEqual,
                    Box::new(Expression::Identifier(String::from("id"))),
                    Box::new(Expression::Literal(Value::I64(1))),
                )),
                Box::new(Expression::Binary(
                    BinaryOperator::Equal,
                    Box::new(Expression::Identifier(String::from("dinero"))),
                    Box::new(Expression::Literal(Value::I64(100))),
                ))
            )
        );
        let result = analyzer.analyze().unwrap();

        assert_eq!(result, ScanType::Full);
    }

    //WHERE id >= 1 AND dinero == 100
    #[test]
    fn simple_range_and() {
        let analyzer = ScanTypeAnalyzer::create(
            Table::create_mock(vec![ColumnDescriptor::create_primary("id"), ColumnDescriptor::create("dinero", 1)]),
            Expression::Binary(
                BinaryOperator::And,
                Box::new(Expression::Binary(
                    BinaryOperator::GreaterEqual,
                    Box::new(Expression::Identifier(String::from("id"))),
                    Box::new(Expression::Literal(Value::I64(1))),
                )),
                Box::new(Expression::Binary(
                    BinaryOperator::Equal,
                    Box::new(Expression::Identifier(String::from("dinero"))),
                    Box::new(Expression::Literal(Value::I64(100))),
                ))
            )
        );
        let result = analyzer.analyze().unwrap();

        let range_scan = match result { ScanType::Range(value) => value, _ => panic!("") };
        assert!(range_scan.start.is_some());
        assert!(range_scan.start_inclusive);
        assert_eq!(range_scan.start.as_ref().unwrap().clone(), Expression::Literal(Value::I64(1)));
    }

    //WHERE id == 1 AND dinero == 100
    #[test]
    fn simple_exact_and() {
        let analyzer = ScanTypeAnalyzer::create(
            Table::create_mock(vec![ColumnDescriptor::create_primary("id"), ColumnDescriptor::create("dinero", 1)]),
            Expression::Binary(
                BinaryOperator::And,
                Box::new(Expression::Binary(
                    BinaryOperator::Equal,
                    Box::new(Expression::Identifier(String::from("id"))),
                    Box::new(Expression::Literal(Value::I64(1))),
                )),
                Box::new(Expression::Binary(
                    BinaryOperator::Equal,
                    Box::new(Expression::Identifier(String::from("dinero"))),
                    Box::new(Expression::Literal(Value::I64(100))),
                ))
            )
        );
        let result = analyzer.analyze().unwrap();
        let result = match result { ScanType::ExactPrimary(value) => value, _ => panic!("") };

        assert_eq!(result, Expression::Literal(Value::I64(1)));
    }

    //WHERE id == 1 OR dinero == 100
    #[test]
    fn simple_full_or() {
        let analyzer = ScanTypeAnalyzer::create(
            Table::create_mock(vec![ColumnDescriptor::create_primary("id"), ColumnDescriptor::create("dinero", 1)]),
            Expression::Binary(
                BinaryOperator::Or,
                Box::new(Expression::Binary(
                    BinaryOperator::Equal,
                    Box::new(Expression::Identifier(String::from("id"))),
                    Box::new(Expression::Literal(Value::I64(1))),
                )),
                Box::new(Expression::Binary(
                    BinaryOperator::Equal,
                    Box::new(Expression::Identifier(String::from("dinero"))),
                    Box::new(Expression::Literal(Value::I64(100))),
                )))
        );

        let result = analyzer.analyze().unwrap();
        assert_eq!(result, ScanType::Full);
    }
}