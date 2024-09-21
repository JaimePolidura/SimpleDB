use crate::types;
use std::fmt::{Debug, Formatter};
use std::panic::Location;
use std::path::PathBuf;
use std::string::FromUtf8Error;
use bytes::Bytes;

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

pub struct TokenLocation {
    pub line: usize, //Starts form 1
    pub column_index: usize, //Starts from 0
}

pub enum SimpleDbError {
    //SQL Parsing
    IllegalToken(TokenLocation, String),
    MalformedQuery(String),
    FullScanNotAllowed(),

    //General db layer errors
    ColumnNotFound(types::KeyspaceId, String),
    TableNotFound(String),
    TableAlreadyExists(String),
    PrimaryColumnNotIncluded(),
    OnlyOnePrimaryColumnAllowed(),
    ColumnNameAlreadyDefined(String),
    UnknownColumn(String),
    InvalidType(String),

    //Databases
    DatabaseAlreadyExists(String),
    DatabaseNotFound(String),

    //Table Descriptor
    CannotCreateTableDescriptor(types::KeyspaceId, std::io::Error),
    CannotOpenTableDescriptor(types::KeyspaceId, std::io::Error),
    CannotReadTableDescriptor(types::KeyspaceId, std::io::Error),
    CannotDecodeTableDescriptor(types::KeyspaceId, DecodeError),
    CannotWriteTableDescriptor(types::KeyspaceId, std::io::Error),

    //Database descriptor
    CannotReadDatabases(std::io::Error),
    CannotOpenDatabaseDescriptor(String, std::io::Error),
    CannotReaDatabaseDescriptor(String, std::io::Error),
    CannotDecodeDatabaseDescriptor(String, DecodeError),
    CannotCreateDatabaseDescriptor(String, std::io::Error),
    CannotWriteDatabaseDescriptor(std::io::Error),

    //Keyspaces
    KeyspaceNotFound(types::KeyspaceId),
    CannotReadKeyspacesDirectories(std::io::Error),
    CannotReadKeyspaceFile(types::KeyspaceId, std::io::Error),
    CannotCreateKeyspaceDirectory(types::KeyspaceId, std::io::Error),

    //Wal errors
    CannotCreateWal(types::KeyspaceId, types::MemtableId, std::io::Error),
    CannotWriteWalEntry(types::KeyspaceId, types::MemtableId, std::io::Error),
    CannotReadWalEntries(types::KeyspaceId, types::MemtableId, std::io::Error),
    CannotReadWalFiles(types::KeyspaceId, std::io::Error),
    CannotDecodeWal(types::KeyspaceId, types::MemtableId, DecodeError),

    //Manifest errors
    CannotCreateManifest(types::KeyspaceId, std::io::Error),
    CannotWriteManifestOperation(types::KeyspaceId, std::io::Error),
    CannotReadManifestOperations(types::KeyspaceId, std::io::Error),
    CannotDecodeManifest(types::KeyspaceId, DecodeError),
    CannotResetManifest(types::KeyspaceId, std::io::Error),

    //SSTable errors
    CannotOpenSSTableFile(types::KeyspaceId, types::SSTableId, std::io::Error),
    CannotReadSSTableFile(types::KeyspaceId, types::SSTableId, std::io::Error),
    CannotReadSSTablesFiles(types::KeyspaceId, std::io::Error),
    CannotDecodeSSTable(types::KeyspaceId, types::SSTableId, SSTableCorruptedPart, DecodeError),
    CannotDeleteSSTable(types::KeyspaceId, types::SSTableId, std::io::Error),
    CannotCreateSSTableFile(types::KeyspaceId, types::SSTableId, std::io::Error),

    //Transaction log errors
    CannotCreateTransactionLog(std::io::Error),
    CannotWriteTransactionLogEntry(std::io::Error),
    CannotReadTransactionLogEntries(std::io::Error),
    CannotDecodeTransactionLogEntry(DecodeError),
    CannotResetTransactionLog(std::io::Error),

    //This error cannot be returned to the final user,
    //It will only be used internally in the storage engine code
    Internal
}

impl Debug for SimpleDbError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            SimpleDbError::CannotOpenTableDescriptor(keyspace_id, io_error) => {
                write!(f, "Cannot open Table descriptor. Error: {}. Keyspace ID: {}", io_error, keyspace_id)
            },
            SimpleDbError::CannotReadTableDescriptor(keyspace_id, io_error) => {
                write!(f, "Cannot read table descriptor. IO Error: {}. Keyspace ID: {}", io_error, keyspace_id)
            },
            SimpleDbError::CannotDecodeTableDescriptor(keyspace_id, decode_error) => {
                write!(f, "Cannot decode table descriptor. Error: {}. Keyspace ID: {}", decode_error_to_message(&decode_error), keyspace_id)
            },
            SimpleDbError::CannotOpenDatabaseDescriptor(database_name, io_error) => {
                write!(f, "Cannot open Database descriptor. Error: {}. Database ID: {}", io_error, database_name)
            },
            SimpleDbError::CannotReaDatabaseDescriptor(database_name, io_error) => {
                write!(f, "Cannot read table descriptor. IO Error: {}. Database: {}", io_error, database_name)
            },
            SimpleDbError::CannotDecodeDatabaseDescriptor(database_name, descode_error) => {
                write!(f, "Cannot database descriptor. Error: {}. Database: {}", decode_error_to_message(&descode_error), database_name)
            },
            SimpleDbError::CannotCreateWal(keyspace_id, memtable_id, io_error) => {
                write!(f, "Cannot create WAL file. Memtable ID: {}. IO Error: {}. Keyspace ID: {}", memtable_id, io_error, keyspace_id)
            }
            SimpleDbError::CannotWriteWalEntry(keyspace_id, memtable_id, io_error) => {
                write!(f, "Cannot write WAL entry. Memtable ID: {}. IO Error: {}. Keyspace ID: {}", memtable_id, io_error, keyspace_id)
            }
            SimpleDbError::CannotReadWalEntries(keyspace_id, memtable_id, io_error) => {
                write!(f, "Cannot read WAL entries. Memtable ID: {}. IO Error: {}. Keyspace ID: {}", memtable_id, io_error, keyspace_id)
            }
            SimpleDbError::CannotReadWalFiles(keyspace_id, io_error) => {
                write!(f, "Cannot list WAL files in base path. IO Error: {}. Keyspace ID: {}", io_error, keyspace_id)
            }
            SimpleDbError::CannotDecodeWal(keyspace_id, memtable_id, decode_error) => {
                write!(f, "Cannot decode WAL. Memtable ID: {} Error: {}. Keyspace ID: {}", memtable_id, decode_error_to_message(&decode_error), keyspace_id)
            }
            SimpleDbError::CannotCreateManifest(keyspace_id, io_error) => {
                write!(f, "Cannot create manifest file. IO Error: {}. Keyspace ID: {}", io_error, keyspace_id)
            }
            SimpleDbError::CannotWriteManifestOperation(keyspace_id, io_error) => {
                write!(f, "Cannot write manifest operation. IO Error: {}. Keyspace ID: {}", io_error, keyspace_id)
            }
            SimpleDbError::CannotReadManifestOperations(keyspace_id, io_error) => {
                write!(f, "Cannot read manifest operations. IO Error: {}. Keyspace ID: {}", io_error, keyspace_id)
            }
            SimpleDbError::CannotDecodeManifest(keyspace_id, decode_error) => {
                write!(f, "Cannot decode manifest. Error: {}. Keyspace ID: {}", decode_error_to_message(&decode_error), keyspace_id)
            }
            SimpleDbError::CannotResetManifest(keyspace_id, io_error) => {
                write!(f, "Cannot clear manifest. Error: {}. Keyspace ID: {}", io_error, keyspace_id)
            }
            SimpleDbError::CannotOpenSSTableFile(keyspace_id, sstable_id, io_error) => {
                write!(f, "Cannot open SSTable. SSTable ID: {}. Error: {}. Keyspace ID: {}", sstable_id, io_error, keyspace_id)
            }
            SimpleDbError::CannotReadSSTableFile(keyspace_id, sstable_id, io_error) => {
                write!(f, "Cannot read SSTable. SSTable ID: {}. Error: {}. Keyspace ID: {}", sstable_id, io_error, keyspace_id)
            }
            SimpleDbError::CannotDecodeSSTable(keyspace_id, sstable_id, error_part, decode_error) => {
                write!(f, "{}", sstable_decode_error_to_message(*sstable_id, error_part.clone(), decode_error))
            }
            SimpleDbError::CannotDeleteSSTable(keyspace_id, sstable_id, io_error) => {
                write!(f, "Cannot delete SSTable. SSTable ID: {} Error: {}. Keyspace ID: {}", sstable_id, io_error, keyspace_id)
            }
            SimpleDbError::CannotCreateSSTableFile(keyspace_id, sstable_id, io_error) => {
                write!(f, "Cannot create SSTable file. SSTable ID: {} Error: {}. Keyspace ID: {}", sstable_id, io_error, keyspace_id)
            }
            SimpleDbError::CannotCreateTransactionLog(io_error) => {
                write!(f, "Cannot create transaction log file. Error: {}", io_error)
            }
            SimpleDbError::CannotWriteTransactionLogEntry(io_error) => {
                write!(f, "Cannot write transaction log entry. IO Error: {}", io_error)
            }
            SimpleDbError::CannotReadTransactionLogEntries(io_error) => {
                write!(f, "Cannot read transactionlog entries entries. IO Error: {}", io_error)
            },
            SimpleDbError::CannotReadSSTablesFiles(keyspace_id, io_error) => {
                write!(f, "Cannot list SSTables files in base path. IO Error: {}. Keyspace ID: {}", io_error, keyspace_id)
            }
            SimpleDbError::CannotDecodeTransactionLogEntry(decode_error) => {
                write!(f, "Cannot decode transaction log entry. Error: {}", decode_error_to_message(&decode_error))
            }
            SimpleDbError::CannotResetTransactionLog(io_error) => {
                write!(f, "Cannot reset transaction log. Error: {}", io_error)
            }
            SimpleDbError::KeyspaceNotFound(keyspace_id) => {
                write!(f, "Keyspace with ID {} not found", keyspace_id)
            },
            SimpleDbError::Internal => {
                panic!("This error shoudnt be returned to the final user!! Invalid code path");
            }
            SimpleDbError::CannotReadKeyspacesDirectories(io_error) => {
                write!(f, "Cannot list keyspaces directories in base path. IO Error: {}", io_error)
            }
            SimpleDbError::CannotReadKeyspaceFile(keyspace_id, io_error) => {
                write!(f, "Cannot read keyspace directory. Keyspace ID: {}. Error: {}", keyspace_id, io_error)
            }
            SimpleDbError::CannotCreateKeyspaceDirectory(keyspace_id, io_error) => {
                write!(f, "Cannot create keyspace directory. IO Error: {}. Keyspace ID: {}", io_error, keyspace_id)
            }
            SimpleDbError::CannotReadDatabases(io_error) => {
                write!(f, "Cannot list database files in base path. IO Error: {}", io_error)
            }
            SimpleDbError::DatabaseAlreadyExists(database_name) => {
                write!(f, "Database {} already exists", database_name)
            }
            SimpleDbError::CannotCreateDatabaseDescriptor(database_name, io_error) => {
                write!(f, "Cannot create database descriptor. Database name: {}, Error: {}", database_name, io_error)
            }
            SimpleDbError::CannotCreateTableDescriptor(keyspace_id, io_error) => {
                write!(f, "Cannot create table descriptor. Keyspace ID: {}, Error: {}", keyspace_id, io_error)
            }
            SimpleDbError::CannotWriteTableDescriptor(keyspace_id, io_error) => {
                write!(f, "Cannot write table descriptor. IO Error: {}. Keyspace ID: {}", io_error, keyspace_id)
            }
            SimpleDbError::ColumnNotFound(keyspace_id, column_nmae) => {
                write!(f, "Column {} not found. KeyspaceID: {}", column_nmae, keyspace_id)
            }
            SimpleDbError::TableNotFound(table_name) => {
                write!(f, "Table with name: {} not found", table_name)
            }
            SimpleDbError::TableAlreadyExists(table_name) => {
                write!(f, "Table with name: {} already exists", table_name)
            }
            SimpleDbError::PrimaryColumnNotIncluded() => {
                write!(f, "Every table should have a primary column defined at creation time")
            }
            SimpleDbError::OnlyOnePrimaryColumnAllowed() => {
                write!(f, "Every table can only have at least one primary column")
            }
            SimpleDbError::ColumnNameAlreadyDefined(column_name) => {
                write!(f, "Column: {} already defined int able", column_name)
            }
            SimpleDbError::CannotWriteDatabaseDescriptor(io_error) => {
                write!(f, "Cannot write to database descriptor. IO Error: {}", io_error)
            }
            SimpleDbError::IllegalToken(location, message) => {
                write!(f, "Unexpected token at line {} and index {} Message: {}", location.line, location.column_index, message)
            }
            SimpleDbError::MalformedQuery(message) => {
                write!(f, "Malformed query: {}", message)
            }
            SimpleDbError::DatabaseNotFound(database) => {
                write!(f, "Database not found: {}", database)
            }
            SimpleDbError::UnknownColumn(column_name) => {
                write!(f, "Unknown column: {}", column_name)
            }
            SimpleDbError::InvalidType(column_name) => {
                write!(f, "Invalid type for column: {}", column_name)
            }
            SimpleDbError::FullScanNotAllowed() => {
                write!(f, "Full scan is not allowed")
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