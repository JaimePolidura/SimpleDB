use bytes::Bytes;
use crate::selection::Selection;
use crate::sql::expression::Expression;
use crate::table::column_type::ColumnType;

pub enum Statement {
    Select(SelectStatement),
    Update(UpdateStatement),
    Delete(DeleteStatement),
    Insert(InsertStatement),
    CreateTable(CreateTableStatement),
    CreateDatabase(String),
    StartTransaction,
    Rollback,
    Commit
}

pub enum Limit {
    None,
    Some(usize)
}

pub struct SelectStatement {
    pub(crate) where_expr: Expression,
    pub(crate) selection: Selection,
    pub(crate) table_name: String,
    pub(crate) limit: Limit,
}

pub struct UpdateStatement {
    pub(crate) table_name: String,
    pub(crate) updated_values: Vec<(String, Expression)>,
    pub(crate) where_expr: Expression
}

pub struct DeleteStatement {
    pub(crate) table_name: String,
    pub(crate) where_expr: Expression,
    pub(crate) limit: Limit
}

pub struct InsertStatement {
    pub(crate) table_name: String,
    //Column name, Value, Value type
    pub(crate) values: Vec<(String, Bytes, ColumnType)>,
}

pub struct CreateTableStatement {
    pub(crate) table_name: String,
    //Column name, Column type, is primary
    pub(crate) columns: Vec<(String, ColumnType, bool)>
}

impl Statement {
    pub fn terminates_transaction(&self) -> bool {
        match *self {
            Statement::Rollback | Statement::Commit => true,
            _ => false
        }
    }
}

impl UpdateStatement {
    pub fn get_updated_values(&self) -> Selection {
        let mut column_names = Vec::new();
        for (column_name, _) in &self.updated_values {
            column_names.push(column_name.clone());
        }

        Selection::Some(column_names)
    }
}