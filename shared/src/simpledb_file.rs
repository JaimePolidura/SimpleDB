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
    pub fn create_mock() -> SimpleDbFile {
        SimpleDbFile {
            mode: SimpleDbFileMode::Mock,
            size_bytes: 0,
            path: None,
            file: None,
        }
    }

    pub fn create(
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

        let open_options = Self::create_open_options_from_mode(&mode);
        let file = open_options.open(path)?;
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
        self.size_bytes = 0;
        self.file.as_mut().unwrap().set_len(0)?;
        Ok(())
    }

    pub fn delete(&mut self)  -> Result<(), std::io::Error> {
        match self.mode {
            SimpleDbFileMode::Mock => Ok(()),
            _ => {
                {
                    //It goes out of scope, the fd gets closed
                    let _ = self.file.take().unwrap();
                }

                fs::remove_file(self.path.as_ref().unwrap().as_path())
            },
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
        match self.mode {
            SimpleDbFileMode::Mock => Ok(()),
            _ => {
                let file_path = self.path.clone().unwrap();

                let prev_mode = self.upgrade_mode()?;
                let mut backup_file = self.copy(Self::create_file_backup_path(&file_path).as_path(), self.mode.clone())?;
                self.clear()?;
                self.write(bytes)?;
                self.fsync()?;
                self.restore_mode(prev_mode)?;
                backup_file.delete()?;

                Ok(())
            }
        }
    }

    pub fn copy(&self, new_path: &Path, mode: SimpleDbFileMode) -> Result<SimpleDbFile, std::io::Error> {
        match self.mode {
            SimpleDbFileMode::Mock => Ok(SimpleDbFile::create_mock()),
            _ => {
                fs::copy(self.path.as_ref().unwrap().as_path(), new_path)?;
                SimpleDbFile::open(new_path, mode)
            }
        }
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

    fn restore_mode(&mut self, prev_monde: SimpleDbFileMode) -> Result<(), std::io::Error> {
        match prev_monde {
            SimpleDbFileMode::Mock => Ok(()),
            prev_mode => {
                let open_options = Self::create_open_options_from_mode(&prev_mode);
                let file = open_options.open(self.path.as_ref().unwrap().as_path())?;
                self.file = Some(file);
                self.mode = prev_mode;
                Ok(())
            }
        }
    }

    //Upgrades permissions to allow writes and reads.
    //Returns prev mode
    fn upgrade_mode(&mut self) -> Result<SimpleDbFileMode, std::io::Error> {
        match self.mode.clone() {
            SimpleDbFileMode::Mock => Ok(SimpleDbFileMode::Mock),
            prev_mode => {
                self.file = Some(OpenOptions::new()
                    .create(true)
                    .write(true)
                    .read(true)
                    .open(self.path.as_ref().unwrap().as_path())?);

                self.mode = SimpleDbFileMode::RandomWrites;
                Ok(prev_mode.clone())
            }
        }
    }

    fn create_open_options_from_mode(mode: &SimpleDbFileMode) -> OpenOptions {
        let is_append_only = matches!(mode, SimpleDbFileMode::AppendOnly);
        let is_read_only = matches!(mode, SimpleDbFileMode::ReadOnly);

        OpenOptions::new()
            .create(true)
            .append(is_append_only)
            .write(!is_read_only)
            .create(true) //Create file if it doest exist
            .read(true)
            .clone()
    }
}