use std::sync::Arc;
use crate::lsm_options::LsmOptions;
use crate::utils::lsm_file::LsmFile;

pub struct TransactionLog {
    log_file: LsmFile,
}

impl TransactionLog {
    pub fn create(options: Arc<LsmOptions>) -> TransactionLog {

    }
}