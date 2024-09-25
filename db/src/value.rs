use bytes::Bytes;

#[derive(Clone, Debug, PartialEq)]
pub struct Value {
    bytes: Bytes,
    value_type: Type
}

#[derive(Clone, Debug, PartialEq)]
pub enum Type {
    I8,
    U8,
    I16,
    U16,
    U32,
    I32,
    U64,
    I64,
    F32,
    F64,
    Boolean,
    Varchar, //AKA Strings
    Date, //TODO
    Blob, //TODO
    //This cannot be used by users, it is just a "wildcard" to easily evaluate expressions with NULL keywords
    Null,
}