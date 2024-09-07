use std::sync::Arc;
use crossbeam_skiplist::SkipMap;
use storage::lsm;
use crate::table::table::Table;

pub struct Database {
    name: String,
    storage: Arc<lsm::Lsm>,
    tables: SkipMap<String, Arc<Table>>,

    options: Arc<storage::LsmOptions>
}