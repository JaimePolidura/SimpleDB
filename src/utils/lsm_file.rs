use std::fs::File;
use std::os::windows::fs::FileExt;
use std::path::Path;

pub struct LSMFile {
    file: Option<File>,
    size: usize
}

impl LSMFile {
    pub fn empty() -> LSMFile {
        LSMFile{
            file: None,
            size: 0
        }
    }

    pub fn create(
        path: &Path,
        data: &Vec<u8>
    ) -> Result<LSMFile, ()> {
        std::fs::write(path, data);

        match File::open(path) {
            Ok(file) => Ok(LSMFile { size: data.len(), file: Some(file) }),
            Err(_) => Err(())
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