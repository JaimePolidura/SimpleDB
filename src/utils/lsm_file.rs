use std::fs::File;
use std::path::Path;

pub struct LSMFile {
    file: Option<File>,
    size: usize
}

impl LSMFile {
    pub fn create(
        path: &Path,
        data: &Vec<u8>
    ) -> Result<LSMFile, ()> {
        std::fs::write(path, data);

        match File::open(path) {
            Some(file) => Ok(LSMFile { size: data.len(), file }),
            Err(_) => Err(())
        }
    }
}