use std::fs::File;
use std::os::windows::fs::FileExt;
use std::path::{Path, PathBuf};

pub struct LsmFile {
    file: Option<File>,
    path: Option<PathBuf>,
    size: usize
}

impl LsmFile {
    pub fn empty() -> LsmFile {
        LsmFile {
            path: None,
            file: None,
            size: 0
        }
    }

    pub fn read_all(&self) -> Result<Vec<u8>, ()> {
        unimplemented!();
    }

    pub fn open(path: &Path) -> Result<LsmFile, ()> {
        unimplemented!();
    }

    pub fn create(
        path: &Path,
        data: &Vec<u8>
    ) -> Result<LsmFile, ()> {
        std::fs::write(path, data).expect("Cannot create file");

        match File::open(path) {
            Ok(file) => Ok(LsmFile { size: data.len(), file: Some(file), path: Some(path.to_path_buf()) }),
            Err(_) => Err(())
        }
    }

    pub fn delete(&self) {
        match &self.file {
            Some(_) => {
                std::fs::remove_file(self.path.as_ref().unwrap().as_path()).expect("Cannot delete file");
            },
            None => {},
        }
    }

    pub fn read(&self, offset: usize, length: usize) -> Result<Vec<u8>, ()> {
        let mut result: Vec<u8> = Vec::with_capacity(length);
        match self.file.as_ref().unwrap().seek_read(&mut result[..], offset as u64) {
            Ok(_) => Ok(result),
            Err(_) => Err(())
        }
    }
}