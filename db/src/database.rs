use std::sync::Arc;
use crossbeam_skiplist::SkipMap;
use storage::storage;
use crate::table::table::Table;

pub struct Database {
    name: String,
    storage: Arc<storage::Storage>,
    tables: SkipMap<String, Arc<Table>>,

    options: Arc<storage::LsmOptions>
}