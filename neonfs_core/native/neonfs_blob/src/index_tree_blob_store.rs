//! `ChunkStore` adapter that backs `IndexTree` with the production
//! `BlobStore` (#813).
//!
//! Index tree nodes are stored as plain (uncompressed, unencrypted)
//! chunks at the configured tier. Hot is the default for volume
//! metadata — read latency is what matters and the chunks are small
//! enough that compression doesn't pay off.

use std::collections::HashSet;

use crate::error::StoreError;
use crate::hash::{self, Hash};
use crate::index_tree::{ChunkStore, IndexTreeError, Result};
use crate::path::Tier;
use crate::store::BlobStore;

/// Borrowed adapter — does not own the `BlobStore`. The caller (the
/// NIF in lib.rs) holds the `BlobStoreResource` for the lifetime of
/// the call and constructs this adapter for each operation.
///
/// A mutation writes the copy-on-write nodes it produces only to this
/// one local store. The adapter records each new chunk so the caller
/// can replicate them to the volume's other metadata drives — index
/// trees would otherwise be cross-node readable only after anti-entropy
/// caught up, never synchronously (#903).
pub struct BlobStoreChunkStore<'a> {
    store: &'a BlobStore,
    tier: Tier,
    written: Vec<(Hash, Vec<u8>)>,
    seen: HashSet<Hash>,
}

impl<'a> BlobStoreChunkStore<'a> {
    pub fn new(store: &'a BlobStore, tier: Tier) -> Self {
        Self {
            store,
            tier,
            written: Vec::new(),
            seen: HashSet::new(),
        }
    }

    pub fn tier(&self) -> Tier {
        self.tier
    }

    /// Drains the chunks `put` wrote during this adapter's lifetime, in
    /// write order and deduplicated by hash. Reads (`get`) are not
    /// recorded.
    pub fn take_written(&mut self) -> Vec<(Hash, Vec<u8>)> {
        self.seen.clear();
        std::mem::take(&mut self.written)
    }
}

impl ChunkStore for BlobStoreChunkStore<'_> {
    fn get(&self, hash: &Hash) -> Result<Option<Vec<u8>>> {
        match self.store.read_chunk(hash, self.tier) {
            Ok(data) => Ok(Some(data)),
            Err(StoreError::ChunkNotFound(_)) => Ok(None),
            Err(e) => Err(IndexTreeError::Store(e.to_string())),
        }
    }

    fn put(&mut self, data: &[u8]) -> Result<Hash> {
        let h = hash::sha256(data);
        match self.store.write_chunk(&h, data, self.tier) {
            Ok(()) => {
                if self.seen.insert(h) {
                    self.written.push((h, data.to_vec()));
                }
                Ok(h)
            }
            Err(e) => Err(IndexTreeError::Store(e.to_string())),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index_tree::{IndexTree, TreeConfig};
    use crate::store::StoreConfig;
    use tempfile::TempDir;

    fn fresh_store() -> (TempDir, BlobStore) {
        let dir = TempDir::new().unwrap();
        let store = BlobStore::new(dir.path(), StoreConfig::default()).unwrap();
        (dir, store)
    }

    #[test]
    fn round_trips_a_single_chunk() {
        let (_dir, store) = fresh_store();
        let mut adapter = BlobStoreChunkStore::new(&store, Tier::Hot);

        let h = adapter.put(b"some bytes").unwrap();
        assert_eq!(adapter.get(&h).unwrap(), Some(b"some bytes".to_vec()));
    }

    #[test]
    fn returns_none_for_a_hash_that_was_never_written() {
        let (_dir, store) = fresh_store();
        let adapter = BlobStoreChunkStore::new(&store, Tier::Hot);

        let bogus = Hash::from_bytes([0u8; 32]);
        assert_eq!(adapter.get(&bogus).unwrap(), None);
    }

    #[test]
    fn put_is_idempotent_under_identical_input() {
        let (_dir, store) = fresh_store();
        let mut adapter = BlobStoreChunkStore::new(&store, Tier::Hot);

        let h1 = adapter.put(b"identical").unwrap();
        let h2 = adapter.put(b"identical").unwrap();
        assert_eq!(h1, h2);
        assert_eq!(adapter.get(&h1).unwrap(), Some(b"identical".to_vec()));
    }

    #[test]
    fn records_written_chunks_in_order_deduped_and_ignores_reads() {
        let (_dir, store) = fresh_store();
        let mut adapter = BlobStoreChunkStore::new(&store, Tier::Hot);

        let ha = adapter.put(b"chunk-a").unwrap();
        let hb = adapter.put(b"chunk-b").unwrap();
        // Re-writing an identical chunk must not record a duplicate.
        let _ = adapter.put(b"chunk-a").unwrap();
        // Reads are not recorded.
        let _ = adapter.get(&ha).unwrap();

        let written = adapter.take_written();
        assert_eq!(
            written,
            vec![(ha, b"chunk-a".to_vec()), (hb, b"chunk-b".to_vec())]
        );

        // Draining resets the recorder for the next mutation.
        assert!(adapter.take_written().is_empty());
    }

    #[test]
    fn drives_an_indextree_through_put_get_round_trip() {
        let (dir, store) = fresh_store();
        // Use a fresh BlobStore inside IndexTree by wrapping the
        // adapter — the adapter takes a borrowed `&BlobStore`, so we
        // give IndexTree a value-typed wrapper here.
        let owning = OwnedBlobStore { store, _dir: dir };
        let mut tree = IndexTree::new(owning, TreeConfig::default());

        let mut root = None;
        for i in 0u32..16 {
            let key = format!("k-{:02}", i).into_bytes();
            let value = format!("v-{:02}", i).into_bytes();
            root = Some(tree.put(root.as_ref(), key, value).unwrap());
        }

        for i in 0u32..16 {
            let key = format!("k-{:02}", i).into_bytes();
            let expected = format!("v-{:02}", i).into_bytes();
            assert_eq!(tree.get(root.as_ref(), &key).unwrap(), Some(expected));
        }
    }

    /// Owns a `BlobStore` so `IndexTree<S: ChunkStore>` can take
    /// ownership of `S`. The borrowed `BlobStoreChunkStore` is the
    /// production wrapper; this owning form is only here for tests
    /// that drive the tree end-to-end.
    struct OwnedBlobStore {
        store: BlobStore,
        _dir: TempDir,
    }

    impl ChunkStore for OwnedBlobStore {
        fn get(&self, hash: &Hash) -> Result<Option<Vec<u8>>> {
            BlobStoreChunkStore::new(&self.store, Tier::Hot).get(hash)
        }

        fn put(&mut self, data: &[u8]) -> Result<Hash> {
            BlobStoreChunkStore::new(&self.store, Tier::Hot).put(data)
        }
    }
}
