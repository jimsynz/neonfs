//! Chunked copy-on-write B-tree for in-volume metadata indexes.
//!
//! Each tree node is a chunk in the volume — internal nodes hold
//! `(min_key, child_hash)` pairs, leaf nodes hold `(key, value)` pairs.
//! Writes are copy-on-write: every mutation produces a new chunk
//! at every level from the touched leaf back to the root, leaving
//! all old chunks reachable through the *previous* root hash. This
//! is the data structure the FileIndex / ChunkIndex / StripeIndex
//! sub-issues build on (#784, #785).
//!
//! The tree is parameterised by a `ChunkStore` so callers can plug
//! in either the production volume blob store or an in-memory store
//! for tests. Keys and values are opaque bytes — the caller is
//! responsible for serialising whatever record type they store.
//!
//! Deletes leave a tombstone in place of the value so anti-entropy
//! can replicate the delete; `purge_tombstones` reaps tombstones
//! older than a configurable grace period (the caller decides when
//! to run it).

use crate::hash::Hash;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use thiserror::Error;

/// Errors that can occur during tree operations.
#[derive(Error, Debug)]
pub enum IndexTreeError {
    /// The tree referenced a chunk hash that the store does not have.
    #[error("missing chunk: {0}")]
    MissingChunk(String),
    /// A node chunk failed to decode.
    #[error("decode error: {0}")]
    Decode(String),
    /// A node failed to encode.
    #[error("encode error: {0}")]
    Encode(String),
    /// The chunk store returned an underlying error.
    #[error("chunk store error: {0}")]
    Store(String),
}

pub type Result<T> = std::result::Result<T, IndexTreeError>;

/// The chunk store the tree reads from and writes to. Production uses
/// a wrapper around [`crate::store::BlobStore`]; tests use the
/// [`InMemoryChunkStore`] in this module.
pub trait ChunkStore {
    /// Fetch a chunk by hash. `Ok(None)` means the chunk is not in
    /// the store; `Err` means a transient read error.
    fn get(&self, hash: &Hash) -> Result<Option<Vec<u8>>>;
    /// Write a chunk. Returns the chunk's content hash. The store is
    /// expected to dedupe by hash.
    fn put(&mut self, data: &[u8]) -> Result<Hash>;
}

/// Tunables for splitting nodes. Split when an encoded node exceeds
/// the target size; nodes always hold at least one entry, so a
/// single oversized entry will simply produce a single oversized
/// leaf rather than panic.
#[derive(Clone, Debug)]
pub struct TreeConfig {
    pub target_leaf_bytes: usize,
    pub target_internal_bytes: usize,
    pub tombstone_grace: Duration,
}

impl Default for TreeConfig {
    fn default() -> Self {
        Self {
            target_leaf_bytes: 64 * 1024,
            target_internal_bytes: 64 * 1024,
            tombstone_grace: Duration::from_secs(7 * 24 * 60 * 60),
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum LeafValue {
    Present(Vec<u8>),
    /// `deleted_at` is unix-epoch nanoseconds.
    Tombstone {
        deleted_at: u128,
    },
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct LeafEntry {
    pub key: Vec<u8>,
    pub value: LeafValue,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct InternalEntry {
    /// Smallest key in the subtree rooted at `child`.
    pub min_key: Vec<u8>,
    /// 32-byte SHA-256 of the child node chunk.
    pub child: [u8; 32],
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum Node {
    Leaf(Vec<LeafEntry>),
    Internal(Vec<InternalEntry>),
}

/// In-memory `ChunkStore` for tests and benchmarks. Uses a `Mutex`
/// so it can be shared between reader and writer halves without
/// requiring `&mut` for `get`.
#[derive(Default)]
pub struct InMemoryChunkStore {
    inner: Mutex<HashMap<[u8; 32], Vec<u8>>>,
}

impl InMemoryChunkStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn len(&self) -> usize {
        self.inner.lock().unwrap().len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.lock().unwrap().is_empty()
    }
}

impl ChunkStore for InMemoryChunkStore {
    fn get(&self, hash: &Hash) -> Result<Option<Vec<u8>>> {
        let inner = self
            .inner
            .lock()
            .map_err(|e| IndexTreeError::Store(e.to_string()))?;
        Ok(inner.get(hash.as_bytes()).cloned())
    }

    fn put(&mut self, data: &[u8]) -> Result<Hash> {
        let hash = crate::hash::sha256(data);
        let mut inner = self
            .inner
            .lock()
            .map_err(|e| IndexTreeError::Store(e.to_string()))?;
        inner
            .entry(*hash.as_bytes())
            .or_insert_with(|| data.to_vec());
        Ok(hash)
    }
}

/// The tree itself. `S` is the chunk store; the tree is otherwise
/// stateless — callers thread the current root hash through every
/// call.
pub struct IndexTree<S: ChunkStore> {
    pub store: S,
    pub config: TreeConfig,
}

impl<S: ChunkStore> IndexTree<S> {
    pub fn new(store: S, config: TreeConfig) -> Self {
        Self { store, config }
    }

    /// Look up a key. Returns `Ok(None)` if the key is absent or
    /// tombstoned.
    pub fn get(&self, root: Option<&Hash>, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let Some(root) = root else { return Ok(None) };
        let mut current = self.load_node(root)?;
        loop {
            match current {
                Node::Leaf(entries) => return Ok(find_leaf_value(&entries, key)),
                Node::Internal(entries) => {
                    let child = pick_child(&entries, key);
                    let hash = Hash::from_bytes(child);
                    current = self.load_node(&hash)?;
                }
            }
        }
    }

    /// Insert or replace `key`'s value. Returns the new root hash.
    pub fn put(&mut self, root: Option<&Hash>, key: Vec<u8>, value: Vec<u8>) -> Result<Hash> {
        let entry = LeafEntry {
            key,
            value: LeafValue::Present(value),
        };
        self.upsert_leaf_entry(root, entry)
    }

    /// Tombstone a key. The tombstone propagates via anti-entropy
    /// before being purged by [`purge_tombstones`]. Tombstoning a
    /// missing key is a no-op-equivalent (still writes the marker).
    pub fn delete(&mut self, root: Option<&Hash>, key: &[u8]) -> Result<Hash> {
        let entry = LeafEntry {
            key: key.to_vec(),
            value: LeafValue::Tombstone {
                deleted_at: now_nanos(),
            },
        };
        self.upsert_leaf_entry(root, entry)
    }

    /// Walk all live (non-tombstoned) entries with `start <= key < end`,
    /// in sort order. `start.is_empty()` means "from the beginning";
    /// `end.is_empty()` means "to the end".
    pub fn range(
        &self,
        root: Option<&Hash>,
        start: &[u8],
        end: &[u8],
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let Some(root) = root else {
            return Ok(Vec::new());
        };
        let mut out = Vec::new();
        self.walk_range(root, start, end, &mut out)?;
        Ok(out)
    }

    /// List every node chunk hash reachable from `root` — both
    /// internal-page chunks and leaf-page chunks. Used by the
    /// anti-entropy runner (#955) so the per-volume reconciliation
    /// pass enumerates index-tree pages as well as data chunks,
    /// catching tree-page divergence between replicas that the
    /// read-path cross-node fallback (#947) would otherwise leave
    /// undetected.
    ///
    /// An empty (`None`) root returns an empty vector. The output
    /// is in pre-order (root first, then left-to-right depth-first
    /// over children) — order doesn't matter for anti-entropy but
    /// is stable for tests.
    pub fn list_referenced_chunks(&self, root: Option<&Hash>) -> Result<Vec<Hash>> {
        let Some(root) = root else {
            return Ok(Vec::new());
        };
        let mut out = Vec::new();
        self.walk_all_nodes(root, &mut out)?;
        Ok(out)
    }

    /// Reap tombstones whose `deleted_at` falls before `before`.
    /// Walks every leaf and rewrites any that change. Returns the
    /// new root hash (equal to the input root if nothing was reaped).
    pub fn purge_tombstones(&mut self, root: &Hash, before: SystemTime) -> Result<Hash> {
        let cutoff_nanos = before
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        match self.purge_node(root, cutoff_nanos)? {
            Some(new_hash) => Ok(new_hash),
            None => Ok(*root),
        }
    }

    // --- internals ---

    fn upsert_leaf_entry(&mut self, root: Option<&Hash>, entry: LeafEntry) -> Result<Hash> {
        match root {
            None => {
                let node = Node::Leaf(vec![entry]);
                self.write_node(&node)
            }
            Some(root) => {
                let pieces = self.upsert_recurse(root, entry)?;
                self.assemble_root(pieces)
            }
        }
    }

    /// Recursive insert. Returns `Vec<(min_key, child_hash)>` representing
    /// the post-write layout of the node identified by `node_hash`. A
    /// non-split write produces a single tuple; a split produces two.
    fn upsert_recurse(
        &mut self,
        node_hash: &Hash,
        entry: LeafEntry,
    ) -> Result<Vec<(Vec<u8>, [u8; 32])>> {
        let node = self.load_node(node_hash)?;
        match node {
            Node::Leaf(mut entries) => {
                upsert_into_sorted(&mut entries, entry, |e| &e.key);
                self.split_and_write_leaves(entries)
            }
            Node::Internal(entries) => {
                let idx = pick_child_index(&entries, &entry.key);
                let mut new_entries = entries.clone();
                let child_hash = Hash::from_bytes(entries[idx].child);
                let replacements = self.upsert_recurse(&child_hash, entry)?;

                // Replace entries[idx] with the (possibly split) replacements.
                new_entries.splice(
                    idx..=idx,
                    replacements
                        .into_iter()
                        .map(|(min_key, child)| InternalEntry { min_key, child }),
                );

                self.split_and_write_internals(new_entries)
            }
        }
    }

    fn split_and_write_leaves(
        &mut self,
        entries: Vec<LeafEntry>,
    ) -> Result<Vec<(Vec<u8>, [u8; 32])>> {
        let target = self.config.target_leaf_bytes;
        let chunks = split_entries(entries, target, |entry| {
            // Approximate encoded size. The 8 bytes are length prefixes.
            entry.key.len() + leaf_value_size(&entry.value) + 8
        });

        let mut out = Vec::with_capacity(chunks.len());
        for chunk in chunks {
            let min_key = chunk[0].key.clone();
            let node = Node::Leaf(chunk);
            let hash = self.write_node(&node)?;
            out.push((min_key, *hash.as_bytes()));
        }
        Ok(out)
    }

    fn split_and_write_internals(
        &mut self,
        entries: Vec<InternalEntry>,
    ) -> Result<Vec<(Vec<u8>, [u8; 32])>> {
        let target = self.config.target_internal_bytes;
        let chunks = split_entries(entries, target, |entry| entry.min_key.len() + 32 + 8);

        let mut out = Vec::with_capacity(chunks.len());
        for chunk in chunks {
            let min_key = chunk[0].min_key.clone();
            let node = Node::Internal(chunk);
            let hash = self.write_node(&node)?;
            out.push((min_key, *hash.as_bytes()));
        }
        Ok(out)
    }

    /// Build a new root from the (possibly multi-piece) result of an
    /// upsert. If the upsert returned a single piece, the root is just
    /// that piece's hash. Otherwise wrap them in a fresh internal node.
    fn assemble_root(&mut self, pieces: Vec<(Vec<u8>, [u8; 32])>) -> Result<Hash> {
        if pieces.len() == 1 {
            return Ok(Hash::from_bytes(pieces.into_iter().next().unwrap().1));
        }
        // Multi-piece root may itself need splitting if it grows huge.
        let entries: Vec<InternalEntry> = pieces
            .into_iter()
            .map(|(min_key, child)| InternalEntry { min_key, child })
            .collect();
        let mut layer = self.split_and_write_internals(entries)?;
        while layer.len() > 1 {
            let entries: Vec<InternalEntry> = layer
                .into_iter()
                .map(|(min_key, child)| InternalEntry { min_key, child })
                .collect();
            layer = self.split_and_write_internals(entries)?;
        }
        Ok(Hash::from_bytes(layer.into_iter().next().unwrap().1))
    }

    fn walk_all_nodes(&self, node_hash: &Hash, out: &mut Vec<Hash>) -> Result<()> {
        out.push(*node_hash);
        let node = self.load_node(node_hash)?;
        if let Node::Internal(entries) = node {
            for entry in entries {
                let child = Hash::from_bytes(entry.child);
                self.walk_all_nodes(&child, out)?;
            }
        }
        Ok(())
    }

    fn walk_range(
        &self,
        node_hash: &Hash,
        start: &[u8],
        end: &[u8],
        out: &mut Vec<(Vec<u8>, Vec<u8>)>,
    ) -> Result<()> {
        let node = self.load_node(node_hash)?;
        match node {
            Node::Leaf(entries) => {
                for entry in entries {
                    if !start.is_empty() && entry.key.as_slice() < start {
                        continue;
                    }
                    if !end.is_empty() && entry.key.as_slice() >= end {
                        break;
                    }
                    if let LeafValue::Present(v) = entry.value {
                        out.push((entry.key, v));
                    }
                }
            }
            Node::Internal(entries) => {
                for (idx, entry) in entries.iter().enumerate() {
                    let next_min = entries.get(idx + 1).map(|e| e.min_key.as_slice());
                    // Subtree key range is [entry.min_key, next_min).
                    if !end.is_empty() && entry.min_key.as_slice() >= end {
                        break;
                    }
                    if let Some(nm) = next_min {
                        if !start.is_empty() && nm <= start {
                            continue;
                        }
                    }
                    let child = Hash::from_bytes(entry.child);
                    self.walk_range(&child, start, end, out)?;
                }
            }
        }
        Ok(())
    }

    /// Returns `Some(new_hash)` if the subtree changed, `None` if it
    /// did not (so the parent can avoid rewriting itself).
    fn purge_node(&mut self, node_hash: &Hash, cutoff_nanos: u128) -> Result<Option<Hash>> {
        let node = self.load_node(node_hash)?;
        match node {
            Node::Leaf(entries) => {
                let mut kept = Vec::with_capacity(entries.len());
                let mut changed = false;
                for entry in entries {
                    let drop = matches!(
                        entry.value,
                        LeafValue::Tombstone { deleted_at } if deleted_at < cutoff_nanos
                    );
                    if drop {
                        changed = true;
                    } else {
                        kept.push(entry);
                    }
                }
                if !changed {
                    return Ok(None);
                }
                if kept.is_empty() {
                    // Empty leaf still needs *some* representation; keep an empty leaf.
                    let new_hash = self.write_node(&Node::Leaf(Vec::new()))?;
                    Ok(Some(new_hash))
                } else {
                    let new_hash = self.write_node(&Node::Leaf(kept))?;
                    Ok(Some(new_hash))
                }
            }
            Node::Internal(entries) => {
                let mut new_entries = entries.clone();
                let mut changed = false;
                for slot in new_entries.iter_mut() {
                    let child = Hash::from_bytes(slot.child);
                    if let Some(new_child) = self.purge_node(&child, cutoff_nanos)? {
                        slot.child = *new_child.as_bytes();
                        changed = true;
                    }
                }
                if !changed {
                    return Ok(None);
                }
                let new_hash = self.write_node(&Node::Internal(new_entries))?;
                Ok(Some(new_hash))
            }
        }
    }

    fn load_node(&self, hash: &Hash) -> Result<Node> {
        let bytes = self
            .store
            .get(hash)?
            .ok_or_else(|| IndexTreeError::MissingChunk(hash.to_hex()))?;
        decode_node(&bytes)
    }

    fn write_node(&mut self, node: &Node) -> Result<Hash> {
        let bytes = encode_node(node)?;
        self.store.put(&bytes)
    }
}

// --- helpers ---

fn encode_node(node: &Node) -> Result<Vec<u8>> {
    bincode::serde::encode_to_vec(node, bincode::config::standard())
        .map_err(|e| IndexTreeError::Encode(e.to_string()))
}

fn decode_node(bytes: &[u8]) -> Result<Node> {
    let (node, _) = bincode::serde::decode_from_slice(bytes, bincode::config::standard())
        .map_err(|e| IndexTreeError::Decode(e.to_string()))?;
    Ok(node)
}

fn leaf_value_size(v: &LeafValue) -> usize {
    match v {
        LeafValue::Present(b) => b.len() + 1,
        LeafValue::Tombstone { .. } => 16 + 1,
    }
}

fn now_nanos() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0)
}

/// Find the value for `key` in a sorted leaf, treating tombstones as
/// absent. Returns `None` if the key is missing or tombstoned.
fn find_leaf_value(entries: &[LeafEntry], key: &[u8]) -> Option<Vec<u8>> {
    let pos = entries
        .binary_search_by(|e| e.key.as_slice().cmp(key))
        .ok()?;
    match &entries[pos].value {
        LeafValue::Present(v) => Some(v.clone()),
        LeafValue::Tombstone { .. } => None,
    }
}

/// Pick the child slot that should contain `key`. Returns the index
/// such that `entries[index].min_key <= key` and any later entry's
/// min_key is greater. Falls back to 0 when `key` is smaller than
/// every entry (the leftmost subtree owns "out of range left").
fn pick_child_index(entries: &[InternalEntry], key: &[u8]) -> usize {
    let mut idx = 0;
    for (i, entry) in entries.iter().enumerate() {
        match entry.min_key.as_slice().cmp(key) {
            Ordering::Less | Ordering::Equal => idx = i,
            Ordering::Greater => break,
        }
    }
    idx
}

fn pick_child(entries: &[InternalEntry], key: &[u8]) -> [u8; 32] {
    entries[pick_child_index(entries, key)].child
}

/// Insert `entry` into a sorted slice by key, replacing any existing
/// entry with the same key. The slice is mutated in place.
fn upsert_into_sorted<E, F>(entries: &mut Vec<E>, entry: E, key: F)
where
    F: Fn(&E) -> &Vec<u8>,
{
    let probe_key = key(&entry).clone();
    match entries.binary_search_by(|e| key(e).cmp(&probe_key)) {
        Ok(pos) => entries[pos] = entry,
        Err(pos) => entries.insert(pos, entry),
    }
}

/// Split `entries` into chunks whose approximate encoded size stays
/// at or below `target`. Always produces at least one chunk; a single
/// oversized entry yields its own one-element chunk rather than
/// being dropped. `size_of` returns the per-entry approximate size.
fn split_entries<E, F>(entries: Vec<E>, target: usize, size_of: F) -> Vec<Vec<E>>
where
    F: Fn(&E) -> usize,
{
    if entries.is_empty() {
        return vec![Vec::new()];
    }

    let mut out: Vec<Vec<E>> = Vec::new();
    let mut current: Vec<E> = Vec::new();
    let mut current_size: usize = 0;

    for entry in entries {
        let entry_size = size_of(&entry);
        if !current.is_empty() && current_size + entry_size > target {
            out.push(std::mem::take(&mut current));
            current_size = 0;
        }
        current_size += entry_size;
        current.push(entry);
    }
    if !current.is_empty() {
        out.push(current);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use std::collections::BTreeMap;

    fn small_config() -> TreeConfig {
        // Force splits at low entry counts.
        TreeConfig {
            target_leaf_bytes: 256,
            target_internal_bytes: 256,
            tombstone_grace: Duration::from_secs(60),
        }
    }

    fn fresh<C: Fn() -> TreeConfig>(make: C) -> IndexTree<InMemoryChunkStore> {
        IndexTree::new(InMemoryChunkStore::new(), make())
    }

    #[test]
    fn empty_tree_returns_none() {
        let t = fresh(TreeConfig::default);
        assert_eq!(t.get(None, b"missing").unwrap(), None);
    }

    #[test]
    fn put_then_get_single_entry() {
        let mut t = fresh(TreeConfig::default);
        let root = t.put(None, b"hello".to_vec(), b"world".to_vec()).unwrap();
        assert_eq!(
            t.get(Some(&root), b"hello").unwrap(),
            Some(b"world".to_vec())
        );
        assert_eq!(t.get(Some(&root), b"missing").unwrap(), None);
    }

    #[test]
    fn put_replaces_existing_value() {
        let mut t = fresh(TreeConfig::default);
        let root = t.put(None, b"k".to_vec(), b"v1".to_vec()).unwrap();
        let root = t.put(Some(&root), b"k".to_vec(), b"v2".to_vec()).unwrap();
        assert_eq!(t.get(Some(&root), b"k").unwrap(), Some(b"v2".to_vec()));
    }

    #[test]
    fn delete_makes_get_return_none() {
        let mut t = fresh(TreeConfig::default);
        let root = t.put(None, b"k".to_vec(), b"v".to_vec()).unwrap();
        let root = t.delete(Some(&root), b"k").unwrap();
        assert_eq!(t.get(Some(&root), b"k").unwrap(), None);
    }

    #[test]
    fn put_after_delete_resurrects() {
        let mut t = fresh(TreeConfig::default);
        let root = t.put(None, b"k".to_vec(), b"v1".to_vec()).unwrap();
        let root = t.delete(Some(&root), b"k").unwrap();
        let root = t.put(Some(&root), b"k".to_vec(), b"v2".to_vec()).unwrap();
        assert_eq!(t.get(Some(&root), b"k").unwrap(), Some(b"v2".to_vec()));
    }

    #[test]
    fn split_produces_internal_root() {
        let mut t = fresh(small_config);
        let mut root: Option<Hash> = None;
        for i in 0u32..50 {
            let key = format!("key-{:04}", i).into_bytes();
            let value = vec![0u8; 32];
            root = Some(t.put(root.as_ref(), key, value).unwrap());
        }
        // Expect at least one internal node (more chunks than entries × 1).
        assert!(
            t.store.len() > 1,
            "expected splits, got {} chunks",
            t.store.len()
        );

        for i in 0u32..50 {
            let key = format!("key-{:04}", i).into_bytes();
            assert_eq!(t.get(root.as_ref(), &key).unwrap(), Some(vec![0u8; 32]));
        }
    }

    #[test]
    fn range_returns_sorted_present_entries_only() {
        let mut t = fresh(small_config);
        let mut root: Option<Hash> = None;
        for i in 0u32..20 {
            let key = format!("key-{:02}", i).into_bytes();
            let value = format!("val-{:02}", i).into_bytes();
            root = Some(t.put(root.as_ref(), key, value).unwrap());
        }
        // Tombstone every even entry.
        for i in (0u32..20).step_by(2) {
            let key = format!("key-{:02}", i).into_bytes();
            root = Some(t.delete(root.as_ref(), &key).unwrap());
        }
        let got = t.range(root.as_ref(), b"key-05", b"key-15").unwrap();
        let expected: Vec<(Vec<u8>, Vec<u8>)> = (5u32..15)
            .filter(|i| i % 2 != 0)
            .map(|i| {
                (
                    format!("key-{:02}", i).into_bytes(),
                    format!("val-{:02}", i).into_bytes(),
                )
            })
            .collect();
        assert_eq!(got, expected);
    }

    #[test]
    fn range_open_ended_bounds() {
        let mut t = fresh(small_config);
        let mut root: Option<Hash> = None;
        for i in 0u32..5 {
            let key = format!("k{}", i).into_bytes();
            root = Some(t.put(root.as_ref(), key.clone(), key).unwrap());
        }
        let all: Vec<_> = t.range(root.as_ref(), b"", b"").unwrap();
        assert_eq!(all.len(), 5);
        let lower: Vec<_> = t.range(root.as_ref(), b"k2", b"").unwrap();
        assert_eq!(lower.len(), 3);
        let upper: Vec<_> = t.range(root.as_ref(), b"", b"k3").unwrap();
        assert_eq!(upper.len(), 3);
    }

    #[test]
    fn list_referenced_chunks_returns_empty_for_empty_root() {
        let t = fresh(TreeConfig::default);
        assert_eq!(t.list_referenced_chunks(None).unwrap(), Vec::<Hash>::new());
    }

    #[test]
    fn list_referenced_chunks_includes_root_for_single_leaf() {
        let mut t = fresh(TreeConfig::default);
        let root = t.put(None, b"k".to_vec(), b"v".to_vec()).unwrap();
        let hashes = t.list_referenced_chunks(Some(&root)).unwrap();
        assert_eq!(hashes, vec![root]);
    }

    #[test]
    fn list_referenced_chunks_walks_internal_and_leaf_nodes() {
        // Force splits so the tree has internal pages.
        let mut t = fresh(small_config);
        let mut root: Option<Hash> = None;
        for i in 0u32..50 {
            let key = format!("k{:04}", i).into_bytes();
            root = Some(t.put(root.as_ref(), key, vec![0u8; 32]).unwrap());
        }

        let hashes = t.list_referenced_chunks(root.as_ref()).unwrap();

        // Every chunk in the store should be reachable from the
        // root walk. There may be older / orphaned chunks from
        // intermediate writes, so the walk count is ≤ store size.
        assert!(!hashes.is_empty());
        assert!(hashes.len() <= t.store.len());

        // The walk starts at the root.
        assert_eq!(hashes[0], root.unwrap());

        // No duplicates within a single walk.
        let mut sorted = hashes.clone();
        sorted.sort_by_key(|h| *h.as_bytes());
        sorted.dedup();
        assert_eq!(sorted.len(), hashes.len());
    }

    #[test]
    fn purge_drops_only_old_tombstones() {
        let mut t = fresh(TreeConfig::default);
        let mut root: Option<Hash> = None;
        for i in 0u32..5 {
            let key = format!("k{}", i).into_bytes();
            root = Some(t.put(root.as_ref(), key.clone(), key).unwrap());
        }
        // Delete some entries.
        for i in [0u32, 2, 4] {
            let key = format!("k{}", i).into_bytes();
            root = Some(t.delete(root.as_ref(), &key).unwrap());
        }
        // Purge with a cutoff in the past — keeps all tombstones.
        let early = SystemTime::UNIX_EPOCH;
        let same = t.purge_tombstones(root.as_ref().unwrap(), early).unwrap();
        assert_eq!(same.as_bytes(), root.as_ref().unwrap().as_bytes());

        // Purge with a cutoff in the future — drops all tombstones.
        let future = SystemTime::now() + Duration::from_secs(60);
        let purged = t.purge_tombstones(root.as_ref().unwrap(), future).unwrap();
        assert_eq!(t.get(Some(&purged), b"k1").unwrap(), Some(b"k1".to_vec()));
        assert_eq!(t.get(Some(&purged), b"k0").unwrap(), None);
        // Range now sees only the live keys (no tombstones to filter).
        let live = t.range(Some(&purged), b"", b"").unwrap();
        let live_keys: Vec<&[u8]> = live.iter().map(|(k, _)| k.as_slice()).collect();
        assert_eq!(live_keys, vec![b"k1".as_ref(), b"k3".as_ref()]);
    }

    proptest! {
        #![proptest_config(ProptestConfig { cases: 50, .. ProptestConfig::default() })]

        /// A random sequence of put/delete/get operations matches a
        /// `BTreeMap` reference implementation.
        #[test]
        fn matches_btreemap(
            ops in proptest::collection::vec(any_op(), 1..200),
        ) {
            let mut t = fresh(small_config);
            let mut reference: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
            let mut root: Option<Hash> = None;

            for op in ops {
                match op {
                    Op::Put(k, v) => {
                        root = Some(t.put(root.as_ref(), k.clone(), v.clone()).unwrap());
                        reference.insert(k, v);
                    }
                    Op::Delete(k) => {
                        root = Some(t.delete(root.as_ref(), &k).unwrap());
                        reference.remove(&k);
                    }
                    Op::Get(k) => {
                        let got = t.get(root.as_ref(), &k).unwrap();
                        let expected = reference.get(&k).cloned();
                        prop_assert_eq!(got, expected);
                    }
                }
            }

            // Final invariant: range(b"", b"") matches a sorted dump of the reference.
            let live = t.range(root.as_ref(), b"", b"").unwrap();
            let expected: Vec<(Vec<u8>, Vec<u8>)> =
                reference.into_iter().collect();
            prop_assert_eq!(live, expected);
        }
    }

    #[derive(Debug, Clone)]
    enum Op {
        Put(Vec<u8>, Vec<u8>),
        Delete(Vec<u8>),
        Get(Vec<u8>),
    }

    fn any_op() -> impl Strategy<Value = Op> {
        let key = proptest::collection::vec(any::<u8>(), 1..8);
        let value = proptest::collection::vec(any::<u8>(), 0..16);
        prop_oneof![
            (key.clone(), value).prop_map(|(k, v)| Op::Put(k, v)),
            key.clone().prop_map(Op::Delete),
            key.prop_map(Op::Get),
        ]
    }
}
