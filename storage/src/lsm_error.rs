use crate::manifest::manifest::ManifestOperationContent;
use std::fmt::{format, Debug, Formatter};
use std::string::FromUtf8Error;
use std::path::PathBuf;
use crate::lsm::KeyspaceId;
use crate::memtables::memtable::MemtableId;
use crate::sst::sstable::SSTableId;

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
    //Keyspaces
    KeyspaceNotFound(KeyspaceId),
    CannotReadKeyspacesDirectories(std::io::Error),
    CannotReadKeyspaceFile(KeyspaceId, std::io::Error),
    CannotCreateKeyspaceDirectory(KeyspaceId, std::io::Error),

    //Wal errors
    CannotCreateWal(KeyspaceId, MemtableId, std::io::Error),
    CannotWriteWalEntry(KeyspaceId, MemtableId, std::io::Error),
    CannotReadWalEntries(KeyspaceId, MemtableId, std::io::Error),
    CannotReadWalFiles(KeyspaceId, std::io::Error),
    CannotDecodeWal(KeyspaceId, MemtableId, DecodeError),

    //Manifest errors
    CannotCreateManifest(KeyspaceId, std::io::Error),
    CannotWriteManifestOperation(KeyspaceId, ManifestOperationContent, std::io::Error),
    CannotReadManifestOperations(KeyspaceId, std::io::Error),
    CannotDecodeManifest(KeyspaceId, DecodeError),
    CannotResetManifest(KeyspaceId, std::io::Error),

    //SSTable errors
    CannotOpenSSTableFile(KeyspaceId, SSTableId, std::io::Error),
    CannotReadSSTableFile(KeyspaceId, SSTableId, std::io::Error),
    CannotReadSSTablesFiles(KeyspaceId, std::io::Error),
    CannotDecodeSSTable(KeyspaceId, SSTableId, SSTableCorruptedPart, DecodeError),
    CannotDeleteSSTable(KeyspaceId, SSTableId, std::io::Error),
    CannotCreateSSTableFile(KeyspaceId, SSTableId, std::io::Error),

    //Transaction log errors
    CannotCreateTransactionLog(std::io::Error),
    CannotWriteTransactionLogEntry(std::io::Error),
    CannotReadTransactionLogEntries(std::io::Error),
    CannotDecodeTransactionLogEntry(DecodeError),
    CannotResetTransacionLog(std::io::Error),

    //This error cannot be returned to the final user,
    //It will only be used internally in the lsm engine code
    Internal
}

impl Debug for LsmError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            LsmError::CannotCreateWal(keyspace_id, memtable_id, io_error) => {
                write!(f, "Cannot create WAL file. Memtable ID: {}. IO Error: {}. Keyspace ID: {}", memtable_id, io_error, keyspace_id)
            }
            LsmError::CannotWriteWalEntry(keyspace_id, memtable_id, io_error) => {
                write!(f, "Cannot write WAL entry. Memtable ID: {}. IO Error: {}. Keyspace ID: {}", memtable_id, io_error, keyspace_id)
            }
            LsmError::CannotReadWalEntries(keyspace_id, memtable_id, io_error) => {
                write!(f, "Cannot read WAL entries. Memtable ID: {}. IO Error: {}. Keyspace ID: {}", memtable_id, io_error, keyspace_id)
            }
            LsmError::CannotReadWalFiles(keyspace_id, io_error) => {
                write!(f, "Cannot list WAL files in base path. IO Error: {}. Keyspace ID: {}", io_error, keyspace_id)
            }
            LsmError::CannotDecodeWal(keyspace_id, memtable_id, decode_error) => {
                write!(f, "Cannot decode WAL. Memtable ID: {} Error: {}. Keyspace ID: {}", memtable_id, decode_error_to_message(&decode_error), keyspace_id)
            }
            LsmError::CannotCreateManifest(keyspace_id, io_error) => {
                write!(f, "Cannot create manifest file. IO Error: {}. Keyspace ID: {}", io_error, keyspace_id)
            }
            LsmError::CannotWriteManifestOperation(keyspace_id, _, io_error) => {
                write!(f, "Cannot write manifest operation. IO Error: {}. Keyspace ID: {}", io_error, keyspace_id)
            }
            LsmError::CannotReadManifestOperations(keyspace_id, io_error) => {
                write!(f, "Cannot read manifest operations. IO Error: {}. Keyspace ID: {}", io_error, keyspace_id)
            }
            LsmError::CannotDecodeManifest(keyspace_id, decode_error) => {
                write!(f, "Cannot decode manifest. Error: {}. Keyspace ID: {}", decode_error_to_message(&decode_error), keyspace_id)
            }
            LsmError::CannotResetManifest(keyspace_id, io_error) => {
                write!(f, "Cannot clear manifest. Error: {}. Keyspace ID: {}", io_error, keyspace_id)
            }
            LsmError::CannotOpenSSTableFile(keyspace_id, sstable_id, io_error) => {
                write!(f, "Cannot open SSTable. SSTable ID: {}. Error: {}. Keyspace ID: {}", sstable_id, io_error, keyspace_id)
            }
            LsmError::CannotReadSSTableFile(keyspace_id, sstable_id, io_error) => {
                write!(f, "Cannot read SSTable. SSTable ID: {}. Error: {}. Keyspace ID: {}", sstable_id, io_error, keyspace_id)
            }
            LsmError::CannotDecodeSSTable(keyspace_id, sstable_id, error_part, decode_error) => {
                write!(f, "{}", sstable_decode_error_to_message(*sstable_id, error_part.clone(), decode_error))
            }
            LsmError::CannotDeleteSSTable(keyspace_id, sstable_id, io_error) => {
                write!(f, "Cannot delete SSTable. SSTable ID: {} Error: {}. Keyspace ID: {}", sstable_id, io_error, keyspace_id)
            }
            LsmError::CannotCreateSSTableFile(keyspace_id, sstable_id, io_error) => {
                write!(f, "Cannot create SSTable file. SSTable ID: {} Error: {}. Keyspace ID: {}", sstable_id, io_error, keyspace_id)
            }
            LsmError::CannotCreateTransactionLog(io_error) => {
                write!(f, "Cannot create transaction log file. Error: {}", io_error)
            }
            LsmError::CannotWriteTransactionLogEntry(io_error) => {
                write!(f, "Cannot write transaction log entry. IO Error: {}", io_error)
            }
            LsmError::CannotReadTransactionLogEntries(io_error) => {
                write!(f, "Cannot read transactionlog entries entries. IO Error: {}", io_error)
            },
            LsmError::CannotReadSSTablesFiles(keyspace_id, io_error) => {
                write!(f, "Cannot list SSTables files in base path. IO Error: {}. Keyspace ID: {}", io_error, keyspace_id)
            }
            LsmError::CannotDecodeTransactionLogEntry(decode_error) => {
                write!(f, "Cannot decode transaction log entry. Error: {}", decode_error_to_message(&decode_error))
            }
            LsmError::CannotResetTransacionLog(io_error) => {
                write!(f, "Cannot reset transaction log. Error: {}", io_error)
            }
            LsmError::KeyspaceNotFound(keyspace_id) => {
                write!(f, "Keyspace with ID {} not found", keyspace_id)
            },
            LsmError::Internal => {
                panic!("This error shoudnt be returned to the final user!! Invalid code path");
            }
            LsmError::CannotReadKeyspacesDirectories(io_error) => {
                write!(f, "Cannot list keyspaces directories in base path. IO Error: {}", io_error)
            }
            LsmError::CannotReadKeyspaceFile(keyspace_id, io_error) => {
                write!(f, "Cannot read keyspace directory. Keyspace ID: {}. Error: {}", keyspace_id, io_error)
            }
            LsmError::CannotCreateKeyspaceDirectory(keyspace_id, io_error) => {
                write!(f, "Cannot create keyspace directory. IO Error: {}. Keyspace ID: {}", io_error, keyspace_id)
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