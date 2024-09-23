use crate::sql::expression::{BinaryOperator, Expression, UnaryOperator};
use shared::SimpleDbError;
use SimpleDbError::MalformedQuery;

pub fn evaluate_constant_expressions(
    expression: Expression
) -> Result<Expression, SimpleDbError> {
    if !expression.is_constant() {
        return Ok(expression);
    }

    match expression {
        Expression::Binary(operator, left, right) => {
            let left = evaluate_constant_expressions(*left)?;
            let right = evaluate_constant_expressions(*right)?;
            evaluate_deterministic_binary_op(left, right, operator)
        },
        Expression::Unary(operator, expression) => {
            let expression = evaluate_constant_expressions(*expression)?;
            evaluate_deterministic_unary_op(expression, operator)
        },
        Expression::Identifier(_) => Ok(expression),
        Expression::String(_) => Ok(expression),
        Expression::Boolean(_) => Ok(expression),
        Expression::NumberF64(_) => Ok(expression),
        Expression::NumberI64(_) => Ok(expression),
        Expression::None => panic!(""),
    }
}

fn evaluate_deterministic_unary_op(
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
                _ => panic!("")
            }
        }
    }
}

fn evaluate_deterministic_binary_op(
    left: Expression,
    right: Expression,
    operator: BinaryOperator,
) -> Result<Expression, SimpleDbError> {
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
    fn simple_expression() {

    }
}