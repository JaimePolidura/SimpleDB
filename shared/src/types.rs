pub type KeyspaceId = usize; //Table ID
pub type SSTableId = usize;
pub type MemtableId = usize;
pub type TxnId = usize;
pub type ConnectionId = usize;

pub type ColumnId = u16;

pub type Flag = u64;

pub type TypeId = u8;

pub trait FlagMethods {
    fn has(&self, other: Flag) -> bool;
}

impl FlagMethods for Flag {
    fn has(&self, other: Flag) -> bool {
        self & other == other
    }
}

pub const MAX_TXN_ID: TxnId = usize::MAX as TxnId;