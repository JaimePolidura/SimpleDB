use crate::types;
use bytes::Bytes;
use std::fmt::{Debug, Formatter};
use std::string::FromUtf8Error;

pub enum DecodeErrorType {
    CorruptedCrc(u32, u32), //Expected crc, actual crc
    Utf8Decode(FromUtf8Error),
    JsonSerdeDeserialization(serde_json::Error),
    IllegalSize(usize, usize), //Expected size, actual size
    UnknownFlag(usize), //Current flag value
}

pub struct DecodeError {
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

pub type ErrorTypeId = u8;

pub enum SimpleDbError {
    //Network layer errors
    InvalidPassword,
    InvalidRequestBinaryFormat,
    NetworkError(std::io::Error),

    //DB Layer errors
    IllegalToken(TokenLocation, String),
    MalformedQuery(String),
    FullScanNotAllowed(),
    RangeScanNotAllowed(),
    InvalidContext(&'static str),
    ColumnNotFound(types::KeyspaceId, String),
    TableNotFound(String),
    TableAlreadyExists(String),
    PrimaryColumnNotIncluded(),
    OnlyOnePrimaryColumnAllowed(),
    ColumnNameAlreadyDefined(String),
    UnknownColumn(String),
    InvalidType(String),
    CannotDecodeColumn(String, Bytes),
    DatabaseAlreadyExists(String),
    DatabaseNotFound(String),
    CannotCreateTableDescriptor(types::KeyspaceId, std::io::Error),
    CannotOpenTableDescriptor(types::KeyspaceId, std::io::Error),
    CannotReadTableDescriptor(types::KeyspaceId, std::io::Error),
    CannotDecodeTableDescriptor(types::KeyspaceId, DecodeError),
    CannotWriteTableDescriptor(types::KeyspaceId, std::io::Error),
    CannotReadDatabases(std::io::Error),
    CannotOpenDatabaseDescriptor(String, std::io::Error),
    CannotReaDatabaseDescriptor(String, std::io::Error),
    CannotDecodeDatabaseDescriptor(String, DecodeError),
    CannotCreateDatabaseDescriptor(String, std::io::Error),
    CannotWriteDatabaseDescriptor(std::io::Error),
    CannotCreateDatabaseFolder(String, std::io::Error),

    //Storage layer errors
    CannotCreateKeyspaceDescriptorFile(types::KeyspaceId, std::io::Error),
    CannotReadKeyspaceDescriptorFile(types::KeyspaceId, std::io::Error),
    CannotOpenKeyspaceDescriptorFile(types::KeyspaceId, std::io::Error),
    KeyspaceNotFound(types::KeyspaceId),
    CannotReadKeyspacesDirectories(std::io::Error),
    CannotReadKeyspaceFile(types::KeyspaceId, std::io::Error),
    CannotCreateKeyspaceDirectory(types::KeyspaceId, std::io::Error),
    CannotCreateWal(types::KeyspaceId, types::MemtableId, std::io::Error),
    CannotWriteWalEntry(types::KeyspaceId, types::MemtableId, std::io::Error),
    CannotReadWalEntries(types::KeyspaceId, types::MemtableId, std::io::Error),
    CannotReadWalFiles(types::KeyspaceId, std::io::Error),
    CannotDecodeWal(types::KeyspaceId, types::MemtableId, DecodeError),
    CannotCreateManifest(types::KeyspaceId, std::io::Error),
    CannotWriteManifestOperation(types::KeyspaceId, std::io::Error),
    CannotReadManifestOperations(types::KeyspaceId, std::io::Error),
    CannotDecodeManifest(types::KeyspaceId, DecodeError),
    CannotResetManifest(types::KeyspaceId, std::io::Error),
    CannotOpenSSTableFile(types::KeyspaceId, types::SSTableId, std::io::Error),
    CannotReadSSTableFile(types::KeyspaceId, types::SSTableId, std::io::Error),
    CannotReadSSTablesFiles(types::KeyspaceId, std::io::Error),
    CannotDecodeSSTable(types::KeyspaceId, types::SSTableId, SSTableCorruptedPart, DecodeError),
    CannotDeleteSSTable(types::KeyspaceId, types::SSTableId, std::io::Error),
    CannotCreateSSTableFile(types::KeyspaceId, types::SSTableId, std::io::Error),
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
            SimpleDbError::CannotDecodeSSTable(_, sstable_id, error_part, decode_error) => {
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
            SimpleDbError::CannotCreateKeyspaceDescriptorFile(keyspace_id, io_error) => {
                write!(f, "Cannot create keyspace descriptor. IO Error: {}, Keyspace ID: {}", io_error, keyspace_id)
            },
            SimpleDbError::CannotReadKeyspaceDescriptorFile(keyspace_id, io_error) => {
                write!(f, "Cannot read keyspace descriptor file. IO Error: {} Keyspace ID: {}", io_error, keyspace_id)
            },
            SimpleDbError::CannotOpenKeyspaceDescriptorFile(keyspace_id, io_error) => {
                write!(f, "Cannot open keyspace descriptor file. IO Error: {} Keyspace ID: {}", io_error, keyspace_id)
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
            SimpleDbError::CannotCreateDatabaseFolder(database_name, io_error) => {
                write!(f, "Cannot create database {} folder. IO Error: {}", database_name, io_error)
            }
            SimpleDbError::CannotDecodeColumn(column_name, _) => {
                write!(f, "Cannot decode column: {}", column_name)
            }
            SimpleDbError::InvalidContext(message) => {
                write!(f, "Invalid context: {}", message)
            }
            SimpleDbError::InvalidRequestBinaryFormat => {
                write!(f, "Invalid request.")
            }
            SimpleDbError::InvalidPassword => {
                write!(f, "Invalid password.")
            }
            SimpleDbError::NetworkError(e) => {
                write!(f, "Network error: {}", e)
            }
            SimpleDbError::RangeScanNotAllowed() => {
                write!(f, "Range scan not allowed")
            }
        }
    }
}

impl SimpleDbError {
    pub fn serialize(&self) -> ErrorTypeId {
        match self {
            SimpleDbError::RangeScanNotAllowed() => 2,
            SimpleDbError::IllegalToken(_, _) => 3,
            SimpleDbError::MalformedQuery(_) => 4,
            SimpleDbError::FullScanNotAllowed() => 5,
            SimpleDbError::InvalidContext(_) => 6,
            SimpleDbError::ColumnNotFound(_, _) => 7,
            SimpleDbError::TableNotFound(_) => 8,
            SimpleDbError::TableAlreadyExists(_) => 9,
            SimpleDbError::PrimaryColumnNotIncluded() => 10,
            SimpleDbError::OnlyOnePrimaryColumnAllowed() => 11,
            SimpleDbError::ColumnNameAlreadyDefined(_) => 12,
            SimpleDbError::UnknownColumn(_) => 13,
            SimpleDbError::InvalidType(_) => 14,
            SimpleDbError::CannotDecodeColumn(_, _) => 15,
            SimpleDbError::DatabaseAlreadyExists(_) => 16,
            SimpleDbError::DatabaseNotFound(_) => 17,
            SimpleDbError::CannotCreateTableDescriptor(_, _) => 18,
            SimpleDbError::CannotOpenTableDescriptor(_, _) => 19,
            SimpleDbError::CannotReadTableDescriptor(_, _) => 20,
            SimpleDbError::CannotDecodeTableDescriptor(_, _) => 21,
            SimpleDbError::CannotWriteTableDescriptor(_, _) => 22,
            SimpleDbError::CannotReadDatabases(_) => 23,
            SimpleDbError::CannotOpenDatabaseDescriptor(_, _) => 24,
            SimpleDbError::CannotReaDatabaseDescriptor(_, _) => 25,
            SimpleDbError::CannotDecodeDatabaseDescriptor(_, _) => 26,
            SimpleDbError::CannotCreateDatabaseDescriptor(_, _) => 27,
            SimpleDbError::CannotWriteDatabaseDescriptor(_) => 28,
            SimpleDbError::CannotCreateDatabaseFolder(_, _) => 29,
            SimpleDbError::KeyspaceNotFound(_) => 30,
            SimpleDbError::CannotReadKeyspacesDirectories(_) => 31,
            SimpleDbError::CannotReadKeyspaceFile(_, _) => 32,
            SimpleDbError::CannotCreateKeyspaceDirectory(_, _) => 33,
            SimpleDbError::CannotCreateWal(_, _, _) => 34,
            SimpleDbError::CannotWriteWalEntry(_, _, _) => 35,
            SimpleDbError::CannotReadWalEntries(_, _, _) => 36,
            SimpleDbError::CannotReadWalFiles(_, _) => 37,
            SimpleDbError::CannotDecodeWal(_, _, _) => 38,
            SimpleDbError::CannotCreateManifest(_, _) => 39,
            SimpleDbError::CannotWriteManifestOperation(_, _) => 40,
            SimpleDbError::CannotReadManifestOperations(_, _) => 41,
            SimpleDbError::CannotDecodeManifest(_, _) => 42,
            SimpleDbError::CannotResetManifest(_, _) => 43,
            SimpleDbError::CannotOpenSSTableFile(_, _, _) => 44,
            SimpleDbError::CannotReadSSTableFile(_, _, _) => 45,
            SimpleDbError::CannotReadSSTablesFiles(_, _) => 46,
            SimpleDbError::CannotDecodeSSTable(_, _, _, _) => 47,
            SimpleDbError::CannotDeleteSSTable(_, _, _) => 48,
            SimpleDbError::CannotCreateSSTableFile(_, _, _) => 49,
            SimpleDbError::CannotCreateTransactionLog(_) => 50,
            SimpleDbError::CannotWriteTransactionLogEntry(_) => 51,
            SimpleDbError::CannotReadTransactionLogEntries(_) => 52,
            SimpleDbError::CannotDecodeTransactionLogEntry(_) => 53,
            SimpleDbError::CannotResetTransactionLog(_) => 54,
            SimpleDbError::Internal => 55,
            SimpleDbError::InvalidRequestBinaryFormat => 56,
            SimpleDbError::InvalidPassword => 57,
            SimpleDbError::NetworkError(_) => 58,
            SimpleDbError::CannotCreateKeyspaceDescriptorFile(_, _) => 59,
            SimpleDbError::CannotReadKeyspaceDescriptorFile(_, _) => 60,
            SimpleDbError::CannotOpenKeyspaceDescriptorFile(_, _) => 61,
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

    message.push_str(format!("File offset {} in index {}: ", decode_error.offset,
                             decode_error.index).as_str());
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