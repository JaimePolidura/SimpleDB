use bytes::Bytes;
use crate::ColumnType;
use crate::selection::Selection;

pub enum Statement {
    Select(SelectStatement),
    Update(UpdateStatement),
    Delete(DeleteStatement),
    Insert(InsertStatement),
    CreateTable(CreateStatement),
    StartTransaction,
    Rollback,
    Commit
}

pub enum Limit {
    None,
    Some(usize)
}

pub struct SelectStatement {
    table_name: String,
    selection: Selection,
    limit: Limit,
}

pub struct UpdateStatement {
    table_name: String,
}

pub struct DeleteStatement {
    table_name: String,
}

pub struct InsertStatement {
    table_name: String,
    //Column name, Value
    values: Vec<(String, Bytes)>,
}

pub struct CreateStatement {
    pub(crate) table_name: String,
    //Column name, Column type, is primary
    pub(crate) columns: Vec<(String, ColumnType, bool)>
}