//! B-tree-owned stored-value format for inline and overflow-backed payloads.

use crate::error::{HematiteError, Result};
use crate::storage::overflow::{free_overflow_chain, read_overflow_chain, write_overflow_chain};
use crate::storage::{Pager, INVALID_PAGE_ID};

use super::node::MAX_VALUE_SIZE;

pub const STORED_VALUE_INLINE_TAG: u8 = 0;
pub const STORED_VALUE_OVERFLOW_TAG: u8 = 1;
pub const STORED_VALUE_HEADER_SIZE: usize = 11;
pub const STORED_VALUE_LOCAL_CAPACITY: usize = MAX_VALUE_SIZE - STORED_VALUE_HEADER_SIZE;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredValueLayout {
    pub total_len: u32,
    pub local_payload: Vec<u8>,
    pub overflow_first_page: u32,
}

impl StoredValueLayout {
    pub fn new_inline(payload: &[u8]) -> Result<Self> {
        if payload.len() > STORED_VALUE_LOCAL_CAPACITY {
            return Err(HematiteError::StorageError(format!(
                "Inline payload length {} exceeds local capacity {}",
                payload.len(),
                STORED_VALUE_LOCAL_CAPACITY
            )));
        }

        Ok(Self {
            total_len: payload.len() as u32,
            local_payload: payload.to_vec(),
            overflow_first_page: INVALID_PAGE_ID,
        })
    }

    pub fn new_overflow(
        total_len: usize,
        local_payload: Vec<u8>,
        overflow_first_page: u32,
    ) -> Result<Self> {
        if local_payload.len() > STORED_VALUE_LOCAL_CAPACITY {
            return Err(HematiteError::StorageError(format!(
                "Local payload length {} exceeds local capacity {}",
                local_payload.len(),
                STORED_VALUE_LOCAL_CAPACITY
            )));
        }
        if local_payload.len() > total_len {
            return Err(HematiteError::StorageError(
                "Local payload cannot exceed total payload length".to_string(),
            ));
        }

        Ok(Self {
            total_len: total_len as u32,
            local_payload,
            overflow_first_page,
        })
    }

    pub fn tag(&self) -> u8 {
        if self.overflow_first_page == INVALID_PAGE_ID {
            STORED_VALUE_INLINE_TAG
        } else {
            STORED_VALUE_OVERFLOW_TAG
        }
    }

    pub fn overflow_len(&self) -> usize {
        self.total_len as usize - self.local_payload.len()
    }

    pub fn encode(&self) -> Result<Vec<u8>> {
        if self.local_payload.len() > STORED_VALUE_LOCAL_CAPACITY {
            return Err(HematiteError::StorageError(format!(
                "Local payload length {} exceeds local capacity {}",
                self.local_payload.len(),
                STORED_VALUE_LOCAL_CAPACITY
            )));
        }
        if self.local_payload.len() > self.total_len as usize {
            return Err(HematiteError::StorageError(
                "Local payload cannot exceed total payload length".to_string(),
            ));
        }

        let mut out = Vec::with_capacity(STORED_VALUE_HEADER_SIZE + self.local_payload.len());
        out.push(self.tag());
        out.extend_from_slice(&self.total_len.to_le_bytes());
        out.extend_from_slice(&(self.local_payload.len() as u16).to_le_bytes());
        out.extend_from_slice(&self.overflow_first_page.to_le_bytes());
        out.extend_from_slice(&self.local_payload);
        Ok(out)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < STORED_VALUE_HEADER_SIZE {
            return Err(HematiteError::CorruptedData(
                "Stored value header is truncated".to_string(),
            ));
        }

        let tag = bytes[0];
        if tag != STORED_VALUE_INLINE_TAG && tag != STORED_VALUE_OVERFLOW_TAG {
            return Err(HematiteError::CorruptedData(format!(
                "Unsupported stored value tag {}",
                tag
            )));
        }

        let total_len = u32::from_le_bytes([bytes[1], bytes[2], bytes[3], bytes[4]]);
        let local_len = u16::from_le_bytes([bytes[5], bytes[6]]) as usize;
        let overflow_first_page = u32::from_le_bytes([bytes[7], bytes[8], bytes[9], bytes[10]]);

        if local_len > STORED_VALUE_LOCAL_CAPACITY {
            return Err(HematiteError::CorruptedData(
                "Stored value local payload exceeds local capacity".to_string(),
            ));
        }
        if STORED_VALUE_HEADER_SIZE + local_len != bytes.len() {
            return Err(HematiteError::CorruptedData(
                "Stored value local payload length mismatch".to_string(),
            ));
        }
        if local_len > total_len as usize {
            return Err(HematiteError::CorruptedData(
                "Stored value local payload exceeds total payload length".to_string(),
            ));
        }

        match tag {
            STORED_VALUE_INLINE_TAG if overflow_first_page != INVALID_PAGE_ID => {
                return Err(HematiteError::CorruptedData(
                    "Inline stored value cannot reference overflow pages".to_string(),
                ));
            }
            STORED_VALUE_OVERFLOW_TAG if overflow_first_page == INVALID_PAGE_ID => {
                return Err(HematiteError::CorruptedData(
                    "Overflow stored value is missing an overflow head page".to_string(),
                ));
            }
            _ => {}
        }

        Ok(Self {
            total_len,
            local_payload: bytes[STORED_VALUE_HEADER_SIZE..].to_vec(),
            overflow_first_page,
        })
    }
}

pub fn materialize_stored_value(storage: &mut Pager, logical_value: &[u8]) -> Result<Vec<u8>> {
    if logical_value.len() <= STORED_VALUE_LOCAL_CAPACITY {
        return StoredValueLayout::new_inline(logical_value)?.encode();
    }

    let local_payload = logical_value[..STORED_VALUE_LOCAL_CAPACITY].to_vec();
    let overflow_payload = &logical_value[STORED_VALUE_LOCAL_CAPACITY..];
    let overflow_first_page =
        write_overflow_chain(storage, overflow_payload)?.ok_or_else(|| {
            HematiteError::StorageError(
                "Expected overflow pages for a large B-tree value".to_string(),
            )
        })?;

    StoredValueLayout::new_overflow(logical_value.len(), local_payload, overflow_first_page)?
        .encode()
}

pub fn hydrate_stored_value(storage: &mut Pager, stored_value: &[u8]) -> Result<Vec<u8>> {
    let layout = StoredValueLayout::decode(stored_value)?;
    let overflow_payload = read_overflow_chain(
        storage,
        if layout.overflow_first_page == INVALID_PAGE_ID {
            None
        } else {
            Some(layout.overflow_first_page)
        },
        layout.overflow_len(),
    )?;

    let mut value = layout.local_payload;
    value.extend_from_slice(&overflow_payload);
    Ok(value)
}

pub fn free_stored_value_overflow(storage: &mut Pager, stored_value: &[u8]) -> Result<()> {
    let layout = StoredValueLayout::decode(stored_value)?;
    free_overflow_chain(
        storage,
        if layout.overflow_first_page == INVALID_PAGE_ID {
            None
        } else {
            Some(layout.overflow_first_page)
        },
    )
}
