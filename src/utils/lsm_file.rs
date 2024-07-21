use std::fs::File;
use std::os::windows::fs::FileExt;
use std::path::Path;

pub struct LSMFile {
    file: File,
    size: usize
}

impl LSMFile {
    pub fn create(
        path: &Path,
        data: &Vec<u8>
    ) -> Result<LSMFile, ()> {
        std::fs::write(path, data);

        match File::open(path) {
            Ok(file) => Ok(LSMFile { size: data.len(), file }),
            Err(_) => Err(())
        }
    }

    pub fn read(&self, offset: usize, length: usize) -> Result<Vec<u8>, ()> {
        let mut result: Vec<u8> = vec![0; length];
        match self.file.seek_read(&mut result[..], offset as u64) {
            Ok(_) => Ok(result),
            Err(_) => Err(())
        }
    }
}