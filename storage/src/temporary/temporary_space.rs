use shared::SimpleDbError::{CannotCreateTemporaryFile, CannotGetTemporaryFile};
use shared::{SimpleDbError, SimpleDbFile, SimpleDbFileMode};
use std::fs;
use std::path::PathBuf;

#[derive(Clone)]
pub struct TemporarySpace {
    base_path: PathBuf,
}

impl TemporarySpace {
    pub fn create(base_path: PathBuf) -> TemporarySpace {
        TemporarySpace {
            base_path
        }
    }

    pub fn create_file(
        &self,
        file_name: &str,
        mode: SimpleDbFileMode
    ) -> Result<SimpleDbFile, SimpleDbError> {
        let mut file_path = self.base_path.clone();
        file_path.push(file_name);
        SimpleDbFile::create(file_path.as_path(), &vec![], mode)
            .map_err(|e| CannotCreateTemporaryFile(e))
    }

    pub fn get_file(
        &self,
        file_name: &str,
        mode: SimpleDbFileMode
    ) -> Result<SimpleDbFile, SimpleDbError> {
        let mut file_path = self.base_path.clone();
        file_path.push(file_name);
        SimpleDbFile::open(file_path.as_path(), mode)
            .map_err(|e| CannotGetTemporaryFile(e))
    }
}

impl Drop for TemporarySpace {
    fn drop(&mut self) {
        match fs::read_dir(self.base_path.clone()) {
            Ok(temp_files_iterator) => {
                for entry in temp_files_iterator {
                    if let Ok(entry) = entry {
                        let path = entry.path();
                        if path.is_dir() {
                            let _ = fs::remove_dir_all(path.as_path());
                        } else {
                            let _ = fs::remove_file(path.as_path());
                        }
                    }
                }
            },
            Err(_) => {}
        }
    }
}