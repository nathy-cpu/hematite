//! Page manager for handling page-level operations

use crate::error::{HematiteError, Result};
use crate::storage::{Page, PAGE_SIZE};

pub struct PageManager;

impl PageManager {
    pub fn new() -> Self {
        Self
    }

    /// Read a specific value from a page at given offset
    pub fn read_u32(page: &Page, offset: usize) -> Result<u32> {
        if offset + 4 > PAGE_SIZE {
            return Err(HematiteError::InvalidPage(page.id.as_u32()));
        }

        let bytes = &page.data[offset..offset + 4];
        Ok(u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
    }

    /// Write a specific value to a page at given offset
    pub fn write_u32(page: &mut Page, offset: usize, value: u32) -> Result<()> {
        if offset + 4 > PAGE_SIZE {
            return Err(HematiteError::InvalidPage(page.id.as_u32()));
        }

        let bytes = value.to_le_bytes();
        page.data[offset..offset + 4].copy_from_slice(&bytes);
        Ok(())
    }

    /// Read a byte slice from a page
    pub fn read_bytes(page: &Page, offset: usize, length: usize) -> Result<&[u8]> {
        if offset + length > PAGE_SIZE {
            return Err(HematiteError::InvalidPage(page.id.as_u32()));
        }

        Ok(&page.data[offset..offset + length])
    }

    /// Write a byte slice to a page
    pub fn write_bytes(page: &mut Page, offset: usize, data: &[u8]) -> Result<()> {
        if offset + data.len() > PAGE_SIZE {
            return Err(HematiteError::InvalidPage(page.id.as_u32()));
        }

        page.data[offset..offset + data.len()].copy_from_slice(data);
        Ok(())
    }

    /// Find free space in a page (simple first-fit algorithm)
    pub fn find_free_space(page: &Page, required_size: usize) -> Option<usize> {
        // Simple implementation: look for consecutive zeros
        for start in 0..=PAGE_SIZE - required_size {
            let mut found = true;
            for i in 0..required_size {
                if page.data[start + i] != 0 {
                    found = false;
                    break;
                }
            }
            if found {
                return Some(start);
            }
        }
        None
    }
}
