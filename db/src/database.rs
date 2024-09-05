use std::iter::Map;
use std::sync::Arc;
use storage::lsm;
use crate::table::Table;

pub struct Database {
    name: String,
    storage: Arc<lsm::Lsm>,
    tables: Map<String, Table>
}