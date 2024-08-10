use std::fs::File;
use std::io::{Read, SeekFrom, Write};
use std::os::windows::fs::FileExt;
use std::path::{Path, PathBuf};

pub struct LsmFile {
    file: Option<File>,
    path: Option<PathBuf>,
    size_bytes: usize
}

impl LsmFile {
    pub fn empty() -> LsmFile {
        LsmFile {
            path: None,
            file: None,
            size_bytes: 0
        }
    }

    pub fn read_all(&self) -> Result<Vec<u8>, ()> {
        let mut buff: Vec<u8> = Vec::with_capacity(self.size_bytes);
        self.file
            .as_ref()
            .unwrap()
            .read_to_end(&mut buff)
            .map_err(|e| ())?;

        Ok(buff)
    }

    pub fn open(path: &Path) -> Result<LsmFile, ()> {
        let file = File::open(path).map_err(|e| ())?;
        let metadata = file.metadata().map_err(|e| ())?;

        Ok(LsmFile{
            file: Some(file),
            path: Some(path.to_path_buf()),
            size_bytes: metadata.len() as usize
        })
    }

    pub fn create(
        path: &Path,
        data: &Vec<u8>
    ) -> Result<LsmFile, ()> {
        std::fs::write(path, data).expect("Cannot create file");

        match File::open(path) {
            Ok(file) => Ok(LsmFile { size_bytes: data.len(), file: Some(file), path: Some(path.to_path_buf()) }),
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

    pub fn size(&self) -> usize {
        self.size_bytes
    }

    pub fn fsync(&self) {
        self.file
            .as_ref()
            .unwrap()
            .sync_all();
    }

    pub fn write_replace(&mut self, bytes: &[u8]) -> Result<(), ()> {
        self.file
            .as_mut()
            .unwrap()
            .write_all(bytes)
            .map_err(|e| ())
    }

    pub fn read(&self, offset: usize, length: usize) -> Result<Vec<u8>, ()> {
        let mut result: Vec<u8> = vec![0; length];
        match self.file.as_ref().unwrap().seek_read(&mut result, offset as u64) {
            Ok(_) => Ok(result),
            Err(_) => Err(())
        }
    }
}