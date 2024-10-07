use std::cell::UnsafeCell;
use std::fs;
use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::os::windows::fs::FileExt;
use std::path::{Path, PathBuf};

#[derive(Clone)]
pub enum SimpleDbFileMode {
    RandomWrites,
    AppendOnly,
    ReadOnly,
    Mock //Only used for testing
}

pub struct SimpleDbFile {
    file: Option<File>,
    path: Option<PathBuf>,

    size_bytes: usize,
    mode: SimpleDbFileMode,
}

//As log file is append only. Concurrency is resolved by OS
//UnsafeCell does not implement Sync, so it cannot be passed to threads
//We need to wrap it in a struct that implements Sync
pub struct SimpleDbFileWrapper {
    pub file: UnsafeCell<SimpleDbFile>,
}

unsafe impl Send for SimpleDbFileWrapper {}
unsafe impl Sync for SimpleDbFileWrapper {}

impl SimpleDbFile {
    //Only used for testing purposes
    pub fn mock() -> SimpleDbFile {
        SimpleDbFile {
            mode: SimpleDbFileMode::Mock,
            size_bytes: 0,
            path: None,
            file: None,
        }
    }

    pub fn create (
        path: &Path,
        data: &Vec<u8>,
        mode: SimpleDbFileMode
    ) -> Result<SimpleDbFile, std::io::Error> {
        let mut file = Self::open(path, mode)?;
        file.write(data)?;
        Ok(file)
    }

    pub fn open(path: &Path, mode: SimpleDbFileMode) -> Result<SimpleDbFile, std::io::Error> {
        if !Self::is_backup_path(path) {
            Self::recover_from_backup(path)?;
        }

        let is_append_only = matches!(mode, SimpleDbFileMode::AppendOnly);
        let is_read_only = matches!(mode, SimpleDbFileMode::ReadOnly);
        let file: File = OpenOptions::new()
            .create(true)
            .append(is_append_only)
            .write(!is_read_only)
            .create(true) //Create file if it doest exist
            .read(true)
            .open(path)?;
        let metadata = file.metadata()?;

        Ok(SimpleDbFile {
            size_bytes: metadata.len() as usize,
            path: Some(path.to_path_buf()),
            file: Some(file),
            mode,
        })
    }

    fn recover_from_backup(original_file_path: &Path) -> Result<(), std::io::Error> {
        let mut original_file = SimpleDbFile {
            path: Some(original_file_path.to_path_buf()),
            mode: SimpleDbFileMode::RandomWrites,
            size_bytes: 0,
            file: Some(OpenOptions::new()
                .create(true)
                .write(true)
                .create(true) //Create file if it doest exist
                .read(true)
                .open(original_file_path)?),
        };

        let backup_path = Self::create_file_backup_path(original_file_path);
        let backup_path = backup_path.as_path();

        if backup_path.exists() {
            let mut backup_file = SimpleDbFile::open(backup_path, SimpleDbFileMode::RandomWrites)?;
            let backup_contents = backup_file.read_all()?;

            original_file.clear()?;
            original_file.write(&backup_contents)?;
            backup_file.delete()?;
        }

        Ok(())
    }

    pub fn read_all(&self) -> Result<Vec<u8>, std::io::Error> {
        match self.mode {
            SimpleDbFileMode::Mock => Ok(Vec::new()),
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
            SimpleDbFileMode::Mock => Ok(()),
            _ => {
                self.size_bytes = 0;
                self.file.as_mut().unwrap().set_len(0)
            }
        }
    }

    pub fn delete(&self)  -> Result<(), std::io::Error> {
        //TODO, Take owner ship of file, make it to go out of scope so the fd gets removed, and then remove the path
        match self.mode {
            SimpleDbFileMode::Mock => Ok(()),
            _ => fs::remove_file(self.path.as_ref().unwrap().as_path()),
        }
    }

    pub fn size(&self) -> usize {
        self.size_bytes
    }

    pub fn fsync(&self) -> Result<(), std::io::Error> {
        match self.mode {
            SimpleDbFileMode::Mock => Ok(()),
            _ => self.file
                    .as_ref()
                    .unwrap()
                    .sync_all()
        }
    }

    pub fn write(&mut self, bytes: &[u8]) -> Result<(), std::io::Error> {
        if bytes.is_empty() {
            return Ok(());
        }

        match self.mode {
            SimpleDbFileMode::AppendOnly => self.size_bytes = self.size_bytes + bytes.len(),
            SimpleDbFileMode::Mock => return Ok(()),
            _ => self.size_bytes = bytes.len()
        };

        self.file
            .as_mut()
            .unwrap()
            .write_all(bytes)
    }

    pub fn safe_replace(&mut self, bytes: &[u8]) -> Result<(), std::io::Error> {
        let file_path = self.path.as_ref().unwrap();
        let mut backup_file = self.copy(Self::create_file_backup_path(file_path).as_path(), self.mode.clone())?;
        self.clear()?;
        self.write(bytes)?;
        self.fsync()?;
        backup_file.delete()?;
        Ok(())
    }

    pub fn copy(&self, new_path: &Path, mode: SimpleDbFileMode) -> Result<SimpleDbFile, std::io::Error> {
        fs::copy(self.path.as_ref().unwrap().as_path(), new_path)?;
        SimpleDbFile::open(new_path, mode)
    }

    pub fn read(&self, offset: usize, length: usize) -> Result<Vec<u8>, std::io::Error> {
        match self.mode {
            SimpleDbFileMode::Mock => Ok(Vec::new()),
            _ => {
                let mut result: Vec<u8> = vec![0; length];
                self.file.as_ref().unwrap().seek_read(&mut result, offset as u64)?;
                Ok(result)
            }
        }
    }

    pub fn path(&self) -> PathBuf {
        match self.mode {
            SimpleDbFileMode::Mock => PathBuf::new(),
            _ => self.path.as_ref().unwrap().clone()
        }
    }

    fn is_backup_path(path: &Path) -> bool {
        let path_as_string = path.to_str().unwrap();
        path_as_string.ends_with(".safe")
    }

    fn create_file_backup_path(path: &Path) -> PathBuf {
        let path_as_string = path.to_str().unwrap();
        let new_path = format!("{}.safe", path_as_string);
        PathBuf::from(new_path)
    }
}