use crate::utils::lsm_file::LsmFileMode::{Mock, RandomWrites};
use std::fs::{File, OpenOptions};
use std::hash::Hash;
use std::io::{Read, Write};
use std::os::windows::fs::FileExt;
use std::path::{Path, PathBuf};

pub enum LsmFileMode {
    RandomWrites,
    AppendOnly,
    ReadOnly,
    Mock //Only used for testing
}

pub struct LsmFile {
    file: Option<File>,
    path: Option<PathBuf>,

    size_bytes: usize,
    mode: LsmFileMode,
}

impl LsmFile {
    //Only used for testing purposes
    pub fn mock() -> LsmFile {
        LsmFile {
            mode: Mock,
            size_bytes: 0,
            path: None,
            file: None,
        }
    }

    pub fn open(path: &Path, mode: LsmFileMode) -> Result<LsmFile, std::io::Error> {
        let is_append_only = matches!(mode, LsmFileMode::AppendOnly);
        let is_read_only = matches!(mode, LsmFileMode::ReadOnly);
        let file: File = OpenOptions::new()
            .create(true)
            .append(is_append_only)
            .write(!is_read_only)
            .read(true)
            .open(path)?;
        let metadata = file.metadata()?;

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
    ) -> Result<LsmFile, std::io::Error> {
        std::fs::write(path, data)?;

        match File::open(path) {
            Ok(file) => Ok(LsmFile {
                path: Some(path.to_path_buf()),
                size_bytes: data.len(),
                file: Some(file),
                mode
            }),
            Err(e) => Err(e)
        }
    }

    pub fn read_all(&self) -> Result<Vec<u8>, std::io::Error> {
        match self.mode {
            LsmFileMode::Mock => Ok(Vec::new()),
            _ => {
                let mut buff: Vec<u8> = Vec::with_capacity(self.size_bytes);
                self.file
                    .as_ref()
                    .unwrap()
                    .read_to_end(&mut buff)?;

                Ok(buff)
            }
        }
    }

    pub fn clear(&mut self) -> Result<(), std::io::Error> {
        match self.mode {
            LsmFileMode::Mock => Ok(()),
            _ => {
                self.size_bytes = 0;
                self.file.as_mut().unwrap().set_len(0)
            }
        }
    }

    pub fn delete(&self)  -> Result<(), std::io::Error> {
        match self.mode {
            LsmFileMode::Mock => Ok(()),
            _ => std::fs::remove_file(self.path.as_ref().unwrap().as_path()),
        }
    }

    pub fn size(&self) -> usize {
        self.size_bytes
    }

    pub fn fsync(&self) -> Result<(), std::io::Error> {
        match self.mode {
            LsmFileMode::Mock => Ok(()),
            _ => self.file
                    .as_ref()
                    .unwrap()
                    .sync_all()
        }
    }

    pub fn write(&mut self, bytes: &[u8]) -> Result<(), std::io::Error> {
        match self.mode {
            LsmFileMode::AppendOnly => self.size_bytes = self.size_bytes + bytes.len(),
            LsmFileMode::Mock => return Ok(()),
            _ => self.size_bytes = bytes.len()
        };

        self.file
            .as_mut()
            .unwrap()
            .write_all(bytes)
    }

    pub fn read(&self, offset: usize, length: usize) -> Result<Vec<u8>, std::io::Error> {
        match self.mode {
            LsmFileMode::Mock => Ok(Vec::new()),
            _ => {
                let mut result: Vec<u8> = vec![0; length];
                self.file.as_ref().unwrap().seek_read(&mut result, offset as u64)?;
                Ok(result)
            }
        }
    }

    pub fn path(&self) -> PathBuf {
        match self.mode {
            LsmFileMode::Mock => PathBuf::new(),
            _ => self.path.as_ref().unwrap().clone()
        }
    }
}