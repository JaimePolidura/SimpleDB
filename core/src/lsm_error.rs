use crate::manifest::manifest::ManifestOperationContent;
use std::fmt::{format, Debug, Formatter};
use std::string::FromUtf8Error;
use std::path::PathBuf;

pub enum DecodeErrorType {
    CorruptedCrc(u32, u32), //Expected crc, actual crc
    Utf8Decode(FromUtf8Error),
    JsonSerdeDeserialization(serde_json::Error),
    IllegalSize(usize, usize), //Expected size, actual size
    UnknownFlag(usize), //Current flag value
}

pub struct DecodeError {
    pub path: PathBuf,
    pub offset: usize,
    pub index: usize,
    pub error_type: DecodeErrorType,
}

#[derive(Copy, Clone)]
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
    CannotDecodeWal(usize, DecodeError),

    //Manifest errors
    CannotCreateManifest(std::io::Error),
    CannotWriteManifestOperation(ManifestOperationContent, std::io::Error),
    CannotReadManifestOperations(std::io::Error),
    CannotDecodeManifest(DecodeError),
    CannotResetManifest(std::io::Error),

    //SSTable errors
    CannotOpenSSTableFile(usize, std::io::Error),
    CannotReadSSTableFile(usize, std::io::Error),
    CannotDecodeSSTable(usize, SSTableCorruptedPart, DecodeError),
    CannotDeleteSSTable(usize, std::io::Error),
    CannotCreateSSTableFile(usize, std::io::Error),

    //This error cannot be returned to the final user,
    //It will only be used internally in the lsm engine code
    Internal
}

impl Debug for LsmError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            LsmError::CannotCreateWal(memtable_id, io_error) => {
                write!(f, "Cannot create WAL file. Memtable ID: {}. IO Error: {}", memtable_id, io_error)
            }
            LsmError::CannotWriteWalEntry(memtable_id, io_error) => {
                write!(f, "Cannot write WAL entry. Memtable ID: {}. IO Error: {}", memtable_id, io_error)
            }
            LsmError::CannotReadWalEntries(memtable_id, io_error) => {
                write!(f, "Cannot read WAL entries. Memtable ID: {}. IO Error: {}", memtable_id, io_error)
            }
            LsmError::CannotReadWalFiles(io_error) => {
                write!(f, "Cannot list WAL files in base path. IO Error: {}", io_error)
            }
            LsmError::CannotDecodeWal(memtable_id, decode_error) => {
                write!(f, "Cannot decode WAL. Memtable ID: {} Error: {}", memtable_id, decode_error_to_message(&decode_error))
            }
            LsmError::CannotCreateManifest(io_error) => {
                write!(f, "Cannot create manifest file. IO Error: {}", io_error)
            }
            LsmError::CannotWriteManifestOperation(_, io_error) => {
                write!(f, "Cannot write manifest operation. IO Error: {}", io_error)
            }
            LsmError::CannotReadManifestOperations(io_error) => {
                write!(f, "Cannot read manifest operations. IO Error: {}", io_error)
            }
            LsmError::CannotDecodeManifest(decode_error) => {
                write!(f, "Cannot decode manifest. Error: {}", decode_error_to_message(&decode_error))
            }
            LsmError::CannotResetManifest(io_error) => {
                write!(f, "Cannot clear manifest. Error: {}", io_error)
            }
            LsmError::CannotOpenSSTableFile(sstable_id, io_error) => {
                write!(f, "Cannot open SSTable. SSTable ID: {}. Error: {}", sstable_id, io_error)
            }
            LsmError::CannotReadSSTableFile(sstable_id, io_error) => {
                write!(f, "Cannot read SSTable. SSTable ID: {}. Error: {}", sstable_id, io_error)
            }
            LsmError::CannotDecodeSSTable(sstable_id, error_part, decode_error) => {
                write!(f, "{}", sstable_decode_error_to_message(*sstable_id, error_part.clone(), decode_error))
            }
            LsmError::CannotDeleteSSTable(sstable_id, io_error) => {
                write!(f, "Cannot delete SSTable. SSTable ID: {} Error: {}", sstable_id, io_error)
            }
            LsmError::CannotCreateSSTableFile(sstable_id, io_error) => {
                write!(f, "Cannot create SSTable file. SSTable ID: {} Error: {}", sstable_id, io_error)
            }
            LsmError::Internal => {
                panic!("This error shoudnt be returned to the final user!! Invalid code path");
            }
        }
    }
}

impl Debug for DecodeErrorType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let message = decode_error_type_to_message(self);
        write!(f, "{}", message)
    }
}

impl Debug for DecodeError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let message = decode_error_to_message(self);
        write!(f, "{}", message)
    }
}

fn sstable_decode_error_to_message(
    sstable_id: usize,
    corrupted_part: SSTableCorruptedPart,
    decode_error: &DecodeError
) -> String {
    let mut message = String::new();

    let corrupted_part: String = match corrupted_part {
        SSTableCorruptedPart::BlockMetadata => "block metadata".to_string(),
        SSTableCorruptedPart::BloomFilter => "bloom filter".to_string(),
        SSTableCorruptedPart::Block(block_id) => format!("block ID {}", block_id),
    };

    message.push_str(format!("Cannot decode SSTable {}. SSTable ID: {}. Error: {}", corrupted_part,
                             sstable_id, decode_error_to_message(decode_error)).as_str());

    message
}

fn decode_error_to_message(decode_error: &DecodeError) -> String {
    let mut message = String::new();

    message.push_str(format!("File {} in file offset {} in index {}: ", decode_error.path.as_path().to_str().unwrap(),
                         decode_error.offset, decode_error.index).as_str());
    message.push_str(decode_error_type_to_message(&decode_error.error_type).as_str());

    message
}

fn decode_error_type_to_message(decode_error_type: &DecodeErrorType) -> String {
    match &decode_error_type {
        DecodeErrorType::CorruptedCrc(expected, actual) => {
            format!("Corrupted CRC Expected {} Actual {}", expected, actual)
        },
        DecodeErrorType::Utf8Decode(utf8_error) => {
            format!("Invalid UTF-8: {}", utf8_error)
        },
        DecodeErrorType::JsonSerdeDeserialization(serde_error) => {
            format!("Invalid JSON format when deserializing: {}", serde_error)
        },
        DecodeErrorType::IllegalSize(expected, actual) => {
            format!("Illegal size. Expected {}, Actual {}", expected, actual)
        }
        DecodeErrorType::UnknownFlag(unknown_flgag) => {
            format!("Unknown flag {}", unknown_flgag)
        },
    }
}