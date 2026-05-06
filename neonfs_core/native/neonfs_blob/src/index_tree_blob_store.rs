//! `ChunkStore` adapter that backs `IndexTree` with the production
//! `BlobStore` (#813).
//!
//! Index tree nodes are stored as plain (uncompressed, unencrypted)
//! chunks at the configured tier. Hot is the default for volume
//! metadata — read latency is what matters and the chunks are small
//! enough that compression doesn't pay off.

use crate::error::StoreError;
use crate::hash::{self, Hash};
use crate::index_tree::{ChunkStore, IndexTreeError, Result};
use crate::path::Tier;
use crate::store::BlobStore;

/// Borrowed adapter — does not own the `BlobStore`. The caller (the
/// NIF in lib.rs) holds the `BlobStoreResource` for the lifetime of
/// the call and constructs this adapter for each operation.
pub struct BlobStoreChunkStore<'a> {
    store: &'a BlobStore,
    tier: Tier,
}

impl<'a> BlobStoreChunkStore<'a> {
    pub fn new(store: &'a BlobStore, tier: Tier) -> Self {
        Self { store, tier }
    }

    pub fn tier(&self) -> Tier {
        self.tier
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
            Ok(()) => Ok(h),
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
