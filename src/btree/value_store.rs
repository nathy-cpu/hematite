//! B-tree-owned representation of inline and overflow-backed values.
//!
//! B-tree leaf values are not stored directly as arbitrary bytes. They are wrapped in a small
//! format that records:
//! - the total logical payload length;
//! - the portion stored locally in the leaf cell;
//! - the first overflow page, if any.
//!
//! Layout:
//!
//! ```text
//! +------+----------------+----------------------+-------------------+
//! | tag  | total_len (4)  | local payload bytes  | overflow page id  |
//! +------+----------------+----------------------+-------------------+
//! ```
//!
//! This allows the generic tree layer to support large values internally without forcing higher
//! layers to manage overflow pages themselves.

use crate::error::{HematiteError, Result};
use crate::storage::overflow::{free_overflow_chain, read_overflow_chain, write_overflow_chain};
use crate::storage::{Pager, INVALID_PAGE_ID, USABLE_PAGE_SIZE};

pub const STORED_VALUE_INLINE_TAG: u8 = 0;
pub const STORED_VALUE_OVERFLOW_TAG: u8 = 1;
/// Header bytes on an inline stored value: just the tag.
///
/// It was 11 — tag, `total_len` u32, `local_len` u16, `overflow_first_page` u32 — on
/// every value, including the empty ones that secondary-index entries store. For an
/// inline value all four of those beyond the tag are recoverable: the payload length
/// is the cell's own length prefix minus this header, `total_len` equals it, and
/// there is no overflow page. Ten bytes per row and per index entry, for nothing.
pub const STORED_VALUE_INLINE_HEADER_SIZE: usize = 1;

/// Header bytes on a spilled stored value: tag, total length, first overflow page.
///
/// `local_len` is still recoverable from the cell length, so it is not stored.
pub const STORED_VALUE_OVERFLOW_HEADER_SIZE: usize = 9;

/// Maximum bytes of payload stored locally in a leaf cell (SQLite-style formula).
/// For 4096-byte pages: (4096 - 12) * 64 / 255 - 23 = 1001.
pub const MAX_LOCAL_PAYLOAD: usize = (USABLE_PAGE_SIZE - 12) * 64 / 255 - 23;

/// Bytes of a spilled payload kept in the leaf cell.
///
/// Was 489, from SQLite's `btreePayloadToLocal`. That formula picks a local size
/// that makes the *last overflow page* come out full, trading leaf space for
/// overflow-page packing. It is the wrong trade here.
///
/// A leaf is where scans pay. At 489 bytes of every large value held locally, a leaf
/// fits about eight such rows, so a scan that never looks at the large column still
/// reads a page for every eight rows. At 32 it fits around a hundred. What the old
/// formula saved was at most one partly-filled overflow page *per value*, on values
/// that are already over a kilobyte.
///
/// 32 rather than 0 keeps a prefix in the cell, which is a place to answer things
/// like `LIKE 'abc%'` without following the chain.
pub const MIN_LOCAL_PAYLOAD: usize = 32;

/// How many bytes of `total_payload_len` live in the leaf cell.
///
/// Either all of it, or almost none: there is no longer a surplus calculation, since
/// the point is to keep leaves dense rather than overflow pages full.
pub fn local_payload_size(total_payload_len: usize) -> usize {
    if total_payload_len <= MAX_LOCAL_PAYLOAD {
        total_payload_len
    } else {
        MIN_LOCAL_PAYLOAD
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredValueLayout {
    pub total_len: u32,
    pub local_payload: Vec<u8>,
    pub overflow_first_page: u32,
}

impl StoredValueLayout {
    pub fn new_inline(payload: &[u8]) -> Result<Self> {
        if payload.len() > MAX_LOCAL_PAYLOAD {
            return Err(HematiteError::StorageError(format!(
                "Inline payload length {} exceeds max local capacity {}",
                payload.len(),
                MAX_LOCAL_PAYLOAD
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
        if local_payload.len() > MAX_LOCAL_PAYLOAD {
            return Err(HematiteError::StorageError(format!(
                "Local payload length {} exceeds max local capacity {}",
                local_payload.len(),
                MAX_LOCAL_PAYLOAD
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
        if self.local_payload.len() > MAX_LOCAL_PAYLOAD {
            return Err(HematiteError::StorageError(format!(
                "Local payload length {} exceeds max local capacity {}",
                self.local_payload.len(),
                MAX_LOCAL_PAYLOAD
            )));
        }
        if self.local_payload.len() > self.total_len as usize {
            return Err(HematiteError::StorageError(
                "Local payload cannot exceed total payload length".to_string(),
            ));
        }

        let header = if self.overflow_first_page == INVALID_PAGE_ID {
            STORED_VALUE_INLINE_HEADER_SIZE
        } else {
            STORED_VALUE_OVERFLOW_HEADER_SIZE
        };
        let mut out = Vec::with_capacity(header + self.local_payload.len());
        out.push(self.tag());
        if self.overflow_first_page != INVALID_PAGE_ID {
            out.extend_from_slice(&self.total_len.to_le_bytes());
            out.extend_from_slice(&self.overflow_first_page.to_le_bytes());
        }
        out.extend_from_slice(&self.local_payload);
        Ok(out)
    }

    /// Validates a stored value's header and borrows its local payload.
    ///
    /// [`decode`](Self::decode) is this plus a copy of the payload into an owned
    /// `Vec`. Readers that only look at the bytes do not need that copy, and on an
    /// inline value — the common case, since a row only spills past
    /// [`MIN_LOCAL_PAYLOAD`] — the borrowed slice is the whole value.
    pub fn decode_borrowed(bytes: &[u8]) -> Result<(Self, &[u8])> {
        let Some(&tag) = bytes.first() else {
            return Err(HematiteError::CorruptedData(
                "Stored value header is truncated".to_string(),
            ));
        };

        let (total_len, overflow_first_page, local_payload) = match tag {
            STORED_VALUE_INLINE_TAG => {
                // Everything the old header spelled out is implied: the payload is the
                // rest of the cell, and that is the whole value.
                let local_payload = &bytes[STORED_VALUE_INLINE_HEADER_SIZE..];
                (local_payload.len() as u32, INVALID_PAGE_ID, local_payload)
            }
            STORED_VALUE_OVERFLOW_TAG => {
                if bytes.len() < STORED_VALUE_OVERFLOW_HEADER_SIZE {
                    return Err(HematiteError::CorruptedData(
                        "Stored value header is truncated".to_string(),
                    ));
                }
                let total_len = u32::from_le_bytes([bytes[1], bytes[2], bytes[3], bytes[4]]);
                let overflow_first_page =
                    u32::from_le_bytes([bytes[5], bytes[6], bytes[7], bytes[8]]);
                if overflow_first_page == INVALID_PAGE_ID {
                    return Err(HematiteError::CorruptedData(
                        "Overflow stored value is missing an overflow head page".to_string(),
                    ));
                }
                let local_payload = &bytes[STORED_VALUE_OVERFLOW_HEADER_SIZE..];
                if local_payload.len() > total_len as usize {
                    return Err(HematiteError::CorruptedData(
                        "Stored value local payload exceeds total payload length".to_string(),
                    ));
                }
                (total_len, overflow_first_page, local_payload)
            }
            other => {
                return Err(HematiteError::CorruptedData(format!(
                    "Unsupported stored value tag {}",
                    other
                )));
            }
        };

        if local_payload.len() > MAX_LOCAL_PAYLOAD {
            return Err(HematiteError::CorruptedData(
                "Stored value local payload exceeds local capacity".to_string(),
            ));
        }

        Ok((
            Self {
                total_len,
                local_payload: Vec::new(),
                overflow_first_page,
            },
            local_payload,
        ))
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let (layout, local_payload) = Self::decode_borrowed(bytes)?;
        Ok(Self {
            local_payload: local_payload.to_vec(),
            ..layout
        })
    }
}

pub fn materialize_stored_value(storage: &mut Pager, logical_value: &[u8]) -> Result<Vec<u8>> {
    let local_size = local_payload_size(logical_value.len());

    if local_size >= logical_value.len() {
        // Entire payload fits locally — no overflow needed.
        return StoredValueLayout::new_inline(logical_value)?.encode();
    }

    let local_payload = logical_value[..local_size].to_vec();
    let overflow_payload = &logical_value[local_size..];
    let overflow_first_page =
        write_overflow_chain(storage, overflow_payload)?.ok_or_else(|| {
            HematiteError::StorageError(
                "Expected overflow pages for a large B-tree value".to_string(),
            )
        })?;

    StoredValueLayout::new_overflow(logical_value.len(), local_payload, overflow_first_page)?
        .encode()
}

/// Borrows a stored value's bytes when they all live in the page.
///
/// Returns `None` when the value spills, in which case the caller has to go through
/// [`hydrate_stored_value`] and pay for the chain walk and the owned buffer.
///
/// This exists because scans dominate reads and almost never spill: hydrating an
/// inline value allocated a `Vec` per row purely to hand back bytes that were
/// already contiguous in the page. It also needs no pager lock, since there is no
/// overflow chain to read.
pub fn inline_payload(stored_value: &[u8]) -> Result<Option<&[u8]>> {
    let (layout, local_payload) = StoredValueLayout::decode_borrowed(stored_value)?;
    if layout.overflow_first_page == INVALID_PAGE_ID {
        Ok(Some(local_payload))
    } else {
        Ok(None)
    }
}

pub fn hydrate_stored_value(storage: &Pager, stored_value: &[u8]) -> Result<Vec<u8>> {
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
