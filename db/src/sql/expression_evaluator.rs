use std::f32::consts::E;
use bytes::Bytes;
use crate::sql::expression::Expression::Binary;
use crate::sql::expression::{BinaryOperator, Expression, UnaryOperator};
use crate::Row;
use shared::SimpleDbError;
use SimpleDbError::MalformedQuery;

//expression is expected to have been passed to evaluate_constant_expressions() before calling this function
//If the row returns null, we will return false
pub fn evaluate_expression(
    row: &Row,
    expression: &Expression
) -> Result<bool, SimpleDbError> {
    match do_evaluate_expression(row, expression)? {
        Expression::Boolean(value) => Ok(value),
        Expression::Null => Ok(false),
        _ => Err(MalformedQuery(String::from("Expression should produce a boolean value")))
    }
}

//If the row returns a null value, we will propagate the null value, the function will return a null expression
fn do_evaluate_expression(
    row: &Row,
    expression: &Expression
) -> Result<Expression, SimpleDbError> {
    match expression {
        Expression::Binary(operation, left, right) => {
            let left = do_evaluate_expression(row, &*left.clone())?;
            let right = do_evaluate_expression(row, &*right.clone())?;
            evaluate_constant_binary_op(left, right, operation.clone())
        },
        Expression::Unary(operation, unary_expr) => {
            let unary_expr = do_evaluate_expression(row, &*unary_expr.clone())?;
            evaluate_constant_unary_op(unary_expr, operation.clone())
        },
        Expression::Identifier(column_name) => {
            match row.get_column_value(column_name) {
                Some(value) => {
                    Ok(Expression::deserialize(row.get_column_desc(column_name)
                        .ok_or(MalformedQuery(String::from("Unknown column")))?
                        .column_type, value)?)
                },
                None => Ok(Expression::Null)
            }
        },
        Expression::String(string) => Ok(Expression::String(string.clone())),
        Expression::Boolean(boolean) => Ok(Expression::Boolean(*boolean)),
        Expression::NumberI64(number) => Ok(Expression::NumberI64(*number)),
        Expression::NumberF64(number) => Ok(Expression::NumberF64(*number)),
        Expression::Null => Ok(Expression::Null),
        Expression::None => panic!("Invalid code path")
    }
}

pub fn evaluate_constant_expressions(
    expression: Expression
) -> Result<Expression, SimpleDbError> {
    match expression {
        Expression::Binary(operator, left, right) => {
            let left = evaluate_constant_expressions(*left)?;
            let right = evaluate_constant_expressions(*right)?;
            evaluate_constant_binary_op(left, right, operator)
        },
        Expression::Unary(operator, expression) => {
            let expression = evaluate_constant_expressions(*expression)?;
            evaluate_constant_unary_op(expression, operator)
        },
        Expression::Identifier(_) => Ok(expression),
        Expression::String(_) => Ok(expression),
        Expression::Boolean(_) => Ok(expression),
        Expression::NumberF64(_) => Ok(expression),
        Expression::NumberI64(_) => Ok(expression),
        Expression::Null => Ok(expression),
        Expression::None => panic!(""),
    }
}

fn evaluate_constant_unary_op(
    expression: Expression,
    operator: UnaryOperator,
) -> Result<Expression, SimpleDbError> {
    if !expression.is_constant() {
        return Ok(expression);
    }
    if !expression.is_number() {
        return Err(MalformedQuery(String::from("Unary expressions should produce a number")));
    }

    match operator {
        UnaryOperator::Plus => Ok(expression),
        UnaryOperator::Minus => {
            match expression {
                Expression::NumberF64(f64) => Ok(Expression::NumberF64(- f64)),
                Expression::NumberI64(i64) => Ok(Expression::NumberI64(- i64)),
                Expression::Null => Ok(Expression::Null),
                _ => panic!("")
            }
        }
    }
}

fn evaluate_constant_binary_op(
    left: Expression,
    right: Expression,
    operator: BinaryOperator,
) -> Result<Expression, SimpleDbError> {
    if !left.is_constant_expression() || !right.is_constant_expression() {
        return Ok(Binary(operator, Box::new(left), Box::new(right)));
    }

    match operator {
        BinaryOperator::Add => left.add(&right),
        BinaryOperator::Subtract => left.subtract(&right),
        BinaryOperator::Multiply => left.multiply(&right),
        BinaryOperator::Divide => left.divide(&right),
        BinaryOperator::And => left.and(&right),
        BinaryOperator::Or => left.or(&right),
        BinaryOperator::NotEqual => left.not_equal(&right),
        BinaryOperator::Equal => left.equal(&right),
        BinaryOperator::Greater => left.greater(&right),
        BinaryOperator::GreaterEqual => left.greater_equal(&right),
        BinaryOperator::Less => left.less(&right),
        BinaryOperator::LessEqual => left.less_equal(&right),
    }
}

#[cfg(test)]
mod test {
    use crate::sql::expression::Expression::Binary;
    use crate::sql::expression::{BinaryOperator, Expression, UnaryOperator};
    use crate::sql::expression_evaluator::evaluate_constant_expressions;

    // dinero > (1 + 20) OR id > 10 -> dinero > 21 OR id > 10
    #[test]
    fn mixed() {
        let result = evaluate_constant_expressions(Binary(
            BinaryOperator::Or,
            Box::new(Binary(
                BinaryOperator::Greater,
                Box::new(Expression::Identifier(String::from("dinero"))),
                Box::new(Expression::Binary(
                    BinaryOperator::Add,
                    Box::new(Expression::NumberI64(1)),
                    Box::new(Expression::NumberI64(20)),
                )))),
            Box::new(Binary(
                BinaryOperator::Greater,
                Box::new(Expression::Identifier(String::from("id"))),
                Box::new(Expression::NumberI64(10))
            ))
        ));

        assert!(result.is_ok());
        let result = result.unwrap();
        assert_eq!(result, Expression::Binary(
            BinaryOperator::Or,
            Box::new(Binary(
                BinaryOperator::Greater,
                Box::new(Expression::Identifier(String::from("dinero"))),
                Box::new(Expression::NumberI64(21)),
            )),
            Box::new(Binary(
                BinaryOperator::Greater,
                Box::new(Expression::Identifier(String::from("id"))),
                Box::new(Expression::NumberI64(10)),
            )),
        ))
    }

    // (1 + 2) + (3.1 + -(4 * 2))
    #[test]
    fn arithmetic_operations() {
        let result = evaluate_constant_expressions(Binary(
            BinaryOperator::Add,
            Box::new(Binary(
                BinaryOperator::Add,
                Box::new(Expression::NumberI64(1)),
                Box::new(Expression::NumberI64(2)))),
            Box::new(Binary(
                BinaryOperator::Add,
                Box::new(Expression::NumberF64(3.1)),
                Box::new(Expression::Unary(
                    UnaryOperator::Minus,
                    Box::new(Binary(
                        BinaryOperator::Multiply,
                        Box::new(Expression::NumberI64(4)),
                        Box::new(Expression::NumberI64(2))
                    ))
                ))
            ))
        ));

        assert!(result.is_ok());
        let result = result.unwrap();
        assert!(matches!(result, Expression::NumberF64(_)));
        assert_eq!(result.get_f64().unwrap(), (1 + 2) as f64 + (3.1 + -(4 * 2) as f64));
    }

    //((1 > 2) OR (1 <= 2)) AND (1 == 1)
    #[test]
    fn comparation_logical_operations() {
        let result = evaluate_constant_expressions(Binary(
            BinaryOperator::And,
            Box::new(Binary(
                BinaryOperator::Equal,
                Box::new(Expression::NumberI64(1)),
                Box::new(Expression::NumberI64(1)))),
            Box::new(Binary(
                BinaryOperator::Or,
                Box::new(Expression::Binary(
                    BinaryOperator::Greater,
                    Box::new(Expression::NumberI64(1)),
                    Box::new(Expression::NumberI64(2))
                )),
                Box::new(Expression::Binary(
                    BinaryOperator::LessEqual,
                    Box::new(Expression::NumberI64(1)),
                    Box::new(Expression::NumberI64(2))
                )),
            ))
        ));

        assert!(result.is_ok());
        let result = result.unwrap();
        assert!(matches!(result, Expression::Boolean(_)));
        assert_eq!(result.get_boolean().unwrap(), ((1 > 2) || (1 <= 2)) && (1 == 1));
    }
}