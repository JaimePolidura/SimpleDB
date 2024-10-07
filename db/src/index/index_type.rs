pub enum IndexType {
    Primary,
    Secondary,
}

impl IndexType {
    pub fn serialize(&self) -> u8 {
        match &self {
            IndexType::Primary => 1,
            IndexType::Secondary => 2
        }
    }
}