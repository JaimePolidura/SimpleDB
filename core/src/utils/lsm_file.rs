use crate::utils::lsm_file::LsmFileMode::RandomWrites;
use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::os::windows::fs::FileExt;
use std::path::{Path, PathBuf};

pub enum LsmFileMode {
    RandomWrites,
    AppendOnly,
    ReadOnly
}

pub struct LsmFile {
    file: Option<File>,
    path: Option<PathBuf>,

    size_bytes: usize,
    mode: LsmFileMode,
}

impl LsmFile {
    pub fn empty() -> LsmFile {
        LsmFile {
            mode: RandomWrites,
            size_bytes: 0,
            path: None,
            file: None,
        }
    }

    pub fn open(path: &Path, mode: LsmFileMode) -> Result<LsmFile, ()> {
        let is_append_only = matches!(mode, LsmFileMode::AppendOnly);
        let is_read_only = matches!(mode, LsmFileMode::ReadOnly);
        let file: File = OpenOptions::new()
            .create(true)
            .append(is_append_only)
            .write(!is_read_only)
            .read(true)
            .open(path)
            .map_err(|e| ())?;
        let metadata = file.metadata().map_err(|e| ())?;

        Ok(LsmFile{
            size_bytes: metadata.len() as usize,
            path: Some(path.to_path_buf()),
            file: Some(file),
            mode,
        })
    }

    pub fn create(
        path: &Path,
        data: &Vec<u8>,
        mode: LsmFileMode
    ) -> Result<LsmFile, ()> {
        std::fs::write(path, data).expect("Cannot create file");

        match File::open(path) {
            Ok(file) => Ok(LsmFile {
                path: Some(path.to_path_buf()),
                size_bytes: data.len(),
                file: Some(file),
                mode
            }),
            Err(_) => Err(())
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

    pub fn clear(&mut self) -> Result<(), ()> {
        self.size_bytes = 0;
        self.file.as_mut().unwrap().set_len(0)
            .map_err(|e| ())
    }

    pub fn delete(&self)  -> Result<(), ()> {
        match &self.file {
            Some(_) => {
                std::fs::remove_file(self.path.as_ref().unwrap().as_path())
                    .map_err(|e| ())
            },
            None => Ok(()),
        }
    }

    pub fn size(&self) -> usize {
        self.size_bytes
    }

    pub fn fsync(&self) -> Result<(), ()> {
        self.file
            .as_ref()
            .unwrap()
            .sync_all()
            .map_err(|e| ())
    }

    pub fn write(&mut self, bytes: &[u8]) -> Result<(), ()> {
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