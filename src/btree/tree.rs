//! B-tree operations and management

use crate::btree::{BTreeIndex, BTreeNode, NodeType};
use crate::error::{HematiteError, Result};
use crate::storage::{Page, PageId, StorageEngine};

pub struct BTreeManager {
    storage: StorageEngine,
}

impl BTreeManager {
    pub fn new(storage: StorageEngine) -> Self {
        Self { storage }
    }

    pub fn create_tree(&mut self) -> Result<PageId> {
        let root_page_id = self.storage.allocate_page()?;
        let mut root_page = Page::new(root_page_id);

        let root_node = BTreeNode::new_leaf(root_page_id);
        BTreeNode::to_page(&root_node, &mut root_page)?;

        self.storage.write_page(root_page)?;
        Ok(root_page_id)
    }

    pub fn open_tree(&mut self, root_page_id: PageId) -> Result<BTreeIndex> {
        // Verify that the root page exists and is a valid B-tree node
        let _page = self.storage.read_page(root_page_id)?;
        let _node = BTreeNode::from_page(_page)?; // Will error if invalid

        // Note: We'll need to refactor BTreeIndex to not require cloning StorageEngine
        Err(HematiteError::StorageError(
            "open_tree needs refactoring".to_string(),
        ))
    }

    pub fn delete_tree(&mut self, _root_page_id: PageId) -> Result<()> {
        // TODO: Implement tree deletion (recursively free all pages)
        // For now, this is a placeholder
        Err(HematiteError::StorageError(
            "Tree deletion not implemented yet".to_string(),
        ))
    }

    pub fn validate_tree(&mut self, root_page_id: PageId) -> Result<bool> {
        let page = self.storage.read_page(root_page_id)?;
        let root_node = BTreeNode::from_page(page)?;

        self.validate_node_recursive(&root_node)
    }

    fn validate_node_recursive(&mut self, node: &BTreeNode) -> Result<bool> {
        // Check key ordering
        for i in 1..node.keys.len() {
            if node.keys[i - 1] >= node.keys[i] {
                return Ok(false);
            }
        }

        match node.node_type {
            NodeType::Leaf => {
                // Leaf nodes should have matching keys and values
                if node.keys.len() != node.values.len() {
                    return Ok(false);
                }
                Ok(true)
            }
            NodeType::Internal => {
                // Internal nodes should have children = keys + 1
                if node.children.len() != node.keys.len() + 1 {
                    return Ok(false);
                }

                // Recursively validate children
                for child_page_id in &node.children {
                    let page = self.storage.read_page(*child_page_id)?;
                    let child_node = BTreeNode::from_page(page)?;

                    if !self.validate_node_recursive(&child_node)? {
                        return Ok(false);
                    }
                }

                Ok(true)
            }
        }
    }

    pub fn get_tree_stats(&mut self, root_page_id: PageId) -> Result<TreeStats> {
        let page = self.storage.read_page(root_page_id)?;
        let root_node = BTreeNode::from_page(page)?;

        let mut stats = TreeStats::default();
        self.collect_stats_recursive(&root_node, &mut stats, 0)?;

        Ok(stats)
    }

    pub fn collect_stats_recursive(
        &mut self,
        node: &BTreeNode,
        stats: &mut TreeStats,
        depth: usize,
    ) -> Result<()> {
        stats.total_nodes += 1;
        stats.total_keys += node.keys.len();
        stats.max_depth = stats.max_depth.max(depth);

        match node.node_type {
            NodeType::Leaf => {
                stats.leaf_nodes += 1;
            }
            NodeType::Internal => {
                stats.internal_nodes += 1;

                for child_page_id in &node.children {
                    let page = self.storage.read_page(*child_page_id)?;
                    let child_node = BTreeNode::from_page(page)?;
                    self.collect_stats_recursive(&child_node, stats, depth + 1)?;
                }
            }
        }

        Ok(())
    }
}

#[derive(Debug, Default)]
pub struct TreeStats {
    pub total_nodes: usize,
    pub leaf_nodes: usize,
    pub internal_nodes: usize,
    pub total_keys: usize,
    pub max_depth: usize,
}

impl TreeStats {
    pub fn average_keys_per_node(&self) -> f64 {
        if self.total_nodes == 0 {
            0.0
        } else {
            self.total_keys as f64 / self.total_nodes as f64
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::btree::{BTreeKey, BTreeValue};

    #[test]
    fn test_btree_key_comparison() {
        let key1 = BTreeKey::new(vec![1, 2, 3]);
        let key2 = BTreeKey::new(vec![1, 2, 4]);
        let key3 = BTreeKey::new(vec![1, 2, 3]);

        assert!(key1 < key2);
        assert!(key2 > key1);
        assert_eq!(key1, key3);
    }

    #[test]
    fn test_btree_node_creation() {
        let page_id = PageId::new(1);
        let leaf_node = BTreeNode::new_leaf(page_id);
        assert!(matches!(leaf_node.node_type, NodeType::Leaf));
        assert_eq!(leaf_node.keys.len(), 0);
        assert_eq!(leaf_node.values.len(), 0);

        let internal_node = BTreeNode::new_internal(page_id);
        assert!(matches!(internal_node.node_type, NodeType::Internal));
        assert_eq!(internal_node.keys.len(), 0);
        assert_eq!(internal_node.children.len(), 0);
    }

    #[test]
    fn test_btree_node_serialization() -> Result<()> {
        let page_id = PageId::new(1);
        let mut node = BTreeNode::new_leaf(page_id);

        node.keys.push(BTreeKey::new(vec![1, 2, 3]));
        node.keys.push(BTreeKey::new(vec![4, 5, 6]));
        node.values.push(BTreeValue::new(vec![7, 8, 9]));
        node.values.push(BTreeValue::new(vec![10, 11, 12]));

        let mut page = Page::new(page_id);
        BTreeNode::to_page(&node, &mut page)?;

        let deserialized_node = BTreeNode::from_page(page)?;
        assert_eq!(deserialized_node.node_type, node.node_type);
        assert_eq!(deserialized_node.keys.len(), node.keys.len());
        assert_eq!(deserialized_node.values.len(), node.values.len());

        for (i, key) in node.keys.iter().enumerate() {
            assert_eq!(deserialized_node.keys[i].data, key.data);
        }

        for (i, value) in node.values.iter().enumerate() {
            assert_eq!(deserialized_node.values[i].data, value.data);
        }

        Ok(())
    }
}
