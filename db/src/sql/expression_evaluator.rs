use crate::sql::expression::Expression::Binary;
use crate::sql::expression::{BinaryOperator, Expression, UnaryOperator};
use crate::Row;
use shared::SimpleDbError;
use SimpleDbError::MalformedQuery;

//expression is expected to have been passed to evaluate_constant_expressions() before calling this function
//If the row returns null, we will return false
pub fn evaluate_where_expression(
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
    use crate::sql::expression::{BinaryOperator, Expression};
    use crate::sql::expression_evaluator::{evaluate_constant_expressions, evaluate_where_expression};
    use crate::sql::parser::parser::Parser;
    use crate::table::record::Record;
    use crate::table::table_descriptor::ColumnDescriptor;
    use crate::{ColumnType, Row, Table};
    use bytes::Bytes;
    use crossbeam_skiplist::SkipMap;
    use shared::{SimpleDbFile, SimpleDbFileWrapper, SimpleDbOptions};
    use std::cell::UnsafeCell;
    use std::sync::atomic::AtomicUsize;
    use std::sync::Arc;

    //Where id == 10 OR dinero > 100
    #[test]
    fn where_no_nulls() {
        let mut parser = Parser::create(String::from("id == 10 OR dinero > 100"));
        let expression = parser.parse_expression().unwrap();
        let row = id_dinero_nombre_row(11, Some(110), None);
        let result = evaluate_where_expression(&row, &expression);

        assert!(result.unwrap());
    }

    //Where id == 10 AND (dinero > 100 OR nombre == 'Jaime')
    #[test]
    fn where_null_nombre_or() {
        let mut parser = Parser::create(String::from("id == 10 AND (dinero > 100 OR nombre == \"Jaime\")"));
        let expression = parser.parse_expression().unwrap();
        let row = id_dinero_nombre_row(10, Some(110), None);
        let result = evaluate_where_expression(&row, &expression);

        assert!(result.unwrap());
    }

    #[test]
    fn where_null_nombre_and() {
        let mut parser = Parser::create(String::from("id == 10 AND (dinero > 100 AND nombre == \"Jaime\")"));
        let expression = parser.parse_expression().unwrap();
        let row = id_dinero_nombre_row(10, Some(110), None);
        let result = evaluate_where_expression(&row, &expression);

        assert!(!result.unwrap());
    }

    #[test]
    fn constant_mixed() {
        let mut parser = Parser::create(String::from("dinero > (1 + 20) OR id > 10"));
        let expression = parser.parse_expression().unwrap();
        let result = evaluate_constant_expressions(expression);

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

    #[test]
    fn constant_arithmetic_operations() {
        let mut parser = Parser::create(String::from("(1 + 2) + (3.1 + -(4 * 2))"));
        let expression = parser.parse_expression().unwrap();
        let result = evaluate_constant_expressions(expression);

        assert!(result.is_ok());
        let result = result.unwrap();
        assert!(matches!(result, Expression::NumberF64(_)));
        assert_eq!(result.get_f64().unwrap(), (1 + 2) as f64 + (3.1 + -(4 * 2) as f64));
    }

    #[test]
    fn constant_comparation_logical_operations() {
        let mut parser = Parser::create(String::from("((1 > 2) OR (1 <= 2)) AND (1 == 1)"));
        let expression = parser.parse_expression().unwrap();
        let result = evaluate_constant_expressions(expression);

        assert!(result.is_ok());
        let result = result.unwrap();
        assert!(matches!(result, Expression::Boolean(_)));
        assert_eq!(result.get_boolean().unwrap(), ((1 > 2) || (1 <= 2)) && (1 == 1));
    }

    fn id_dinero_nombre_row(
        id: usize, //0
        dinero: Option<usize>, //1
        nombre: Option<&str> //2
    ) -> Row {
        let mut record = Record::builder();

        record.add_column(0, Bytes::copy_from_slice(id.to_le_bytes().as_slice()));
        if let Some(dinero) = dinero {
            record.add_column(1, Bytes::copy_from_slice(dinero.to_le_bytes().as_slice()));
        }
        if let Some(nombre) = nombre {
            record.add_column(2, Bytes::copy_from_slice(nombre.as_bytes()));
        }

        let mut table = Table {
            table_descriptor_file: SimpleDbFileWrapper{ file: UnsafeCell::new(SimpleDbFile::mock()) },
            storage: Arc::new(storage::mock(&Arc::new(SimpleDbOptions::default()))),
            primary_column_name: String::from("id"),
            table_name: String::from("personas"),
            next_column_id: AtomicUsize::new(0),
            storage_keyspace_id: 1,
            columns_by_name: SkipMap::new(),
            columns_by_id: SkipMap::new(),
        };

        table.add_columns(vec![
            (String::from("id"), ColumnType::I64, true),
            (String::from("dinero"), ColumnType::I64, false),
            (String::from("nombre"), ColumnType::Varchar, false),
        ]);

        Row {
            selection: Arc::new(Vec::new()),
            key_bytes: Bytes::copy_from_slice(id.to_le_bytes().as_slice()),
            storage_engine_record: record.build(),
            table: Arc::new(table),
        }
    }
}