use crate::key::Key;
use crate::utils::lsm_file::LSMFile;

pub struct BlockMetadata {
    pub(crate) offset: usize,
    pub(crate) first_key: Key,
    pub(crate) last_key: Key
}

impl BlockMetadata {
    pub fn encode(metadata: &Vec<BlockMetadata>) -> Vec<u8> {
        return Vec::new();
    }
}

pub struct SSTable {
    file: LSMFile,
    id: usize
}

impl SSTable {
    pub fn new(
        file: LSMFile,
        id: usize
    ) -> SSTable {
        SSTable{ file, id }
    }
}