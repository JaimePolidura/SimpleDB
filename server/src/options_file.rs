use std::sync::Arc;
use shared::{SimpleDbError, SimpleDbOptions};

pub struct OptionsFile {

}

impl OptionsFile {
    pub fn create() -> OptionsFile {
        OptionsFile {  }
    }

    pub fn load_options(&self) -> Result<Arc<SimpleDbOptions>, SimpleDbError> {
        todo!()
    }
}