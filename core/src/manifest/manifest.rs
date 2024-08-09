use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use serde::{Deserialize, Serialize};
use crate::lsm_options::LsmOptions;
use crate::utils::lsm_file::LsmFile;

#[derive(Serialize, Deserialize)]
pub enum ManifestRecord {
    SSFlush(usize),
}

pub struct Manifest {
    file: Mutex<LsmFile>
}

impl Manifest {
    pub fn open(options: Arc<LsmOptions>) -> Result<Manifest, ()> {
        match LsmFile::open(Self::manifest_path(&options).as_path()) {
            Ok(file) => Ok(Manifest{file: Mutex::new(file)}),
            Err(_) => Err(())
        }
    }

    pub fn add_record(&self, record: ManifestRecord) -> Result<(), ()> {
        let mut file_lock_result = self.file.lock();
        let file = file_lock_result
            .as_mut()
            .unwrap();

        match serde_json::to_vec(&record) {
            Ok(serialized) => {
                let write_result = file.write_replace(&serialized);
                file.fsync();
                write_result
            },
            Err(_) => Err(())
        }
    }

    fn manifest_path(options: &Arc<LsmOptions>) -> PathBuf {
        let mut path_buf = PathBuf::from(&options.base_path);
        path_buf.push("MANIFEST");
        path_buf
    }
}