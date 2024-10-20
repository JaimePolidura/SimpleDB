use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use shared::{SimpleDbError, SimpleDbOptions};
use shared::SimpleDbError::{CannotCreateTemporarySpace, CannotInitTemporarySpaces};
use crate::temp::temporary_space::TemporarySpace;

//This is not used by the storage engine, this is exposed to the user, so that it can create its own temporary files
pub struct TemporarySpaces {
    next_temporary_id: AtomicUsize,
    options: Arc<SimpleDbOptions>,
    base_path: PathBuf,
}

impl TemporarySpaces {
    pub fn create_mock() -> TemporarySpaces {
        TemporarySpaces {
            next_temporary_id: AtomicUsize::new(0),
            options: Arc::new(SimpleDbOptions::default()),
            base_path: PathBuf::new(),
        }
    }

    pub fn create(options: Arc<SimpleDbOptions>) -> Result<TemporarySpaces, SimpleDbError> {
        let mut base_path = PathBuf::from(options.base_path.clone());
        base_path.push("tmp");

        let mut temporary_files = TemporarySpaces {
            next_temporary_id: AtomicUsize::new(0),
            base_path,
            options,
        };

        temporary_files.init_temporary_spaces()?;

        Ok(temporary_files)
    }

    pub fn create_temporary_space(&self) -> Result<TemporarySpace, SimpleDbError> {
        let temporary_space_id = self.next_temporary_id.fetch_add(1, Ordering::Relaxed);
        let mut temporary_space_path = self.base_path.clone();
        temporary_space_path.push(temporary_space_id.to_string());
        fs::create_dir(&temporary_space_path)
            .map_err(|e| CannotCreateTemporarySpace(e))?;

        Ok(TemporarySpace::create(temporary_space_path))
    }

    fn init_temporary_spaces(&mut self) -> Result<(), SimpleDbError> {
        let temporary_path = self.base_path.as_path();
        fs::create_dir_all(temporary_path)
            .map_err(|e| CannotInitTemporarySpaces(e))?;

        for entry in fs::read_dir(temporary_path)
            .map_err(|e| CannotInitTemporarySpaces(e))? {

            if let Ok(entry) = entry {
                let path = entry.path();
                if path.is_dir() {
                    fs::remove_dir_all(path.as_path())
                        .map_err(|e| CannotInitTemporarySpaces(e))?;
                } else {
                    fs::remove_file(path.as_path())
                        .map_err(|e| CannotInitTemporarySpaces(e))?;
                }
            }
        }

        Ok(())
    }
}