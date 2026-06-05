use crate::error::{HematiteError, Result};

pub(crate) const DATABASE_HEADER_SIZE: usize = 100;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub(crate) enum PageKind {
    InteriorIndex = 0x02,
    InteriorTable = 0x05,
    LeafIndex = 0x0A,
    LeafTable = 0x0D,
    Overflow = 0x20,
    FreelistTrunk = 0x30,
    FreelistLeaf = 0x31,
}

impl PageKind {
    pub(crate) fn from_byte(byte: u8) -> Result<Self> {
        match byte {
            0x02 => Ok(Self::InteriorIndex),
            0x05 => Ok(Self::InteriorTable),
            0x0A => Ok(Self::LeafIndex),
            0x0D => Ok(Self::LeafTable),
            0x20 => Ok(Self::Overflow),
            0x30 => Ok(Self::FreelistTrunk),
            0x31 => Ok(Self::FreelistLeaf),
            _ => Err(HematiteError::StorageError(format!(
                "Unknown page kind byte {byte:#04x}"
            ))),
        }
    }

    pub(crate) fn is_interior(self) -> bool {
        matches!(self, Self::InteriorIndex | Self::InteriorTable)
    }
}
