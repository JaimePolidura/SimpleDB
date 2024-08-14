use crate::manifest::manifest::ManifestOperationContent;
use std::fmt::{Debug, Formatter};
use std::string::FromUtf8Error;
use std::path::PathBuf;

pub enum DecodeErrorType {
    CorruptedCrc(u32, u32), //Expected crc, actual crc
    Utf8Decode(FromUtf8Error),
    JsonSerdeDeserialization(serde_json::Error),
    IllegalSize(usize, usize), //Expected size, actual size
    UnknownFlag(usize), //Current flag value
}

pub struct DecodeErrorInfo {
    pub path: PathBuf,
    pub offset: usize,
    pub index: usize,
    pub error_type: DecodeErrorType,
}

pub enum SSTableCorruptedPart {
    BlockMetadata,
    BloomFilter,
    Block(usize), //Block ID
}

pub enum LsmError {
    //Wal errors
    CannotCreateWal(usize, std::io::Error),
    CannotWriteWalEntry(usize, std::io::Error),
    CannotReadWalEntries(usize, std::io::Error),
    CannotReadWalFiles(std::io::Error),
    CannotDecodeWal(usize, DecodeErrorInfo),

    //Manifest errors
    CannotCreateManifest(std::io::Error),
    CannotWriteManifestOperation(ManifestOperationContent, std::io::Error),
    CannotReadManifestOperations(std::io::Error),
    CannotDecodeManifest(DecodeErrorInfo),
    CannotResetManifest(std::io::Error),

    //SSTable errors
    CannotOpenSSTableFile(usize, std::io::Error),
    CannotReadSSTableFile(usize, std::io::Error),
    CannotDecodeSSTable(usize, SSTableCorruptedPart, DecodeErrorInfo),
    CannotDeleteSSTable(usize, std::io::Error),
    CannotCreateSSTableFile(usize, std::io::Error),

    //This error cannot be returned to the final user,
    //It will only be used internally in the lsm engine code
    Internal
}

impl Debug for LsmError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        unimplemented!();
    }
}