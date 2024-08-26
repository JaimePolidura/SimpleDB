use crate::utils::lsm_file::LsmFileMode::Mock;
use std::fs;
use std::fs::{File, OpenOptions};
use std::hash::Hash;
use std::io::{Read, Write};
use std::os::windows::fs::FileExt;
use std::path::{Path, PathBuf};

#[derive(Clone)]
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
        let is_backup = Self::is_backup_path(path);
        let file: File = OpenOptions::new()
            .create(true)
            .append(is_append_only)
            .write(!is_read_only)
            .read(true)
            .open(path)?;
        let metadata = file.metadata()?;

        let file = LsmFile {
            size_bytes: metadata.len() as usize,
            path: Some(path.to_path_buf()),
            file: Some(file),
            mode,
        };

        if !is_backup {
            Self::recover_from_backup(&file)?;
        }

        Ok(file)
    }

    fn recover_from_backup(orignal_file: &LsmFile) -> Result<(), std::io::Error> {

    }

    pub fn create (
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

    pub fn save_write(&mut self, bytes: &[u8]) -> Result<(), std::io::Error> {
        match self.mode {
            LsmFileMode::RandomWrites => {
                let file_path = self.path.as_ref().unwrap();
                let backup_file = self.copy(Self::create_file_backup_path(file_path), self.mode.clone())?;
                self.clear()?;
                self.write(bytes)?;
                self.fsync()?;
                backup_file.delete()?;
            },
            _ => self.write(bytes),
        }
    }

    pub fn copy(&self, new_path: &Path, mode: LsmFileMode) -> Result<LsmFile, std::io::Error> {
        fs::copy(self.path.as_ref().unwrap().as_path(), new_path)?;
        LsmFile::open(new_path, mode)
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

    fn is_backup_path(path: &PathBuf) -> bool {
        let path_as_string = path.to_str().unwrap();
        path_as_string.ends_with(".safe")
    }

    fn create_file_backup_path(path: &Path) -> PathBuf {
        let path_as_string = path.to_str().unwrap();
        let new_path = format!("{}.safe", path_as_string);
        PathBuf::from(new_path)
    }
}