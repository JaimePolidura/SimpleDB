use bytes::Bytes;
use crate::ColumnType;
use crate::selection::Selection;
use crate::sql::expression::Expression;

pub enum Statement {
    Select(SelectStatement),
    Update(UpdateStatement),
    Delete(DeleteStatement),
    Insert(InsertStatement),
    CreateTable(CreateTableStatement),
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
    //Column name, Value
    pub(crate) values: Vec<(String, Bytes)>,
}

pub struct CreateTableStatement {
    pub(crate) table_name: String,
    //Column name, Column type, is primary
    pub(crate) columns: Vec<(String, ColumnType, bool)>
}