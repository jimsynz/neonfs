pub mod error;
pub mod hash;
pub mod path;
pub mod store;

use crate::hash::Hash;
use crate::path::Tier;
use crate::store::{BlobStore, ReadOptions, StoreConfig};
use rustler::{Binary, Env, NewBinary, Resource, ResourceArc};
use std::sync::Mutex;

/// Resource wrapper for BlobStore to share across NIF calls.
pub struct BlobStoreResource {
    store: Mutex<BlobStore>,
}

#[rustler::resource_impl]
impl Resource for BlobStoreResource {}

#[rustler::nif]
fn add(a: i64, b: i64) -> i64 {
    a + b
}

/// Computes SHA-256 hash of the given binary data.
///
/// Returns the hash as a 32-byte binary.
#[rustler::nif]
fn compute_hash<'a>(env: Env<'a>, data: Binary) -> Binary<'a> {
    let hash = hash::sha256(data.as_slice());
    let bytes = hash.as_bytes();

    let mut output = NewBinary::new(env, bytes.len());
    output.copy_from_slice(bytes);
    output.into()
}

/// Opens a blob store at the given base directory.
///
/// # Arguments
/// * `base_dir` - Path to the base directory for blob storage.
/// * `prefix_depth` - Number of prefix directory levels (e.g., 2 for 65K directories).
///
/// # Returns
/// A resource reference to the opened store, or an error tuple.
#[rustler::nif]
fn store_open(
    base_dir: String,
    prefix_depth: usize,
) -> Result<ResourceArc<BlobStoreResource>, String> {
    let config = StoreConfig { prefix_depth };

    match BlobStore::new(&base_dir, config) {
        Ok(store) => Ok(ResourceArc::new(BlobStoreResource {
            store: Mutex::new(store),
        })),
        Err(e) => Err(e.to_string()),
    }
}

/// Writes a chunk to the blob store.
///
/// # Arguments
/// * `store` - Resource reference to the blob store.
/// * `hash` - 32-byte binary hash of the chunk.
/// * `data` - Binary data to write.
/// * `tier` - Storage tier ("hot", "warm", or "cold").
///
/// # Returns
/// `:ok` on success, or an error tuple.
#[rustler::nif]
fn store_write_chunk(
    store: ResourceArc<BlobStoreResource>,
    hash_bytes: Binary,
    data: Binary,
    tier: String,
) -> Result<(), String> {
    let hash = parse_hash(&hash_bytes)?;
    let tier = parse_tier(&tier)?;

    let store_guard = store.store.lock().map_err(|e| e.to_string())?;
    store_guard
        .write_chunk(&hash, data.as_slice(), tier)
        .map_err(|e| e.to_string())
}

/// Reads a chunk from the blob store without verification.
///
/// # Arguments
/// * `env` - Rustler environment.
/// * `store` - Resource reference to the blob store.
/// * `hash` - 32-byte binary hash of the chunk.
/// * `tier` - Storage tier ("hot", "warm", or "cold").
///
/// # Returns
/// The chunk data as a binary, or an error tuple.
#[rustler::nif]
fn store_read_chunk<'a>(
    env: Env<'a>,
    store: ResourceArc<BlobStoreResource>,
    hash_bytes: Binary,
    tier: String,
) -> Result<Binary<'a>, String> {
    let hash = parse_hash(&hash_bytes)?;
    let tier = parse_tier(&tier)?;

    let store_guard = store.store.lock().map_err(|e| e.to_string())?;
    let data = store_guard
        .read_chunk(&hash, tier)
        .map_err(|e| e.to_string())?;

    let mut output = NewBinary::new(env, data.len());
    output.copy_from_slice(&data);
    Ok(output.into())
}

/// Reads a chunk from the blob store with optional verification.
///
/// # Arguments
/// * `env` - Rustler environment.
/// * `store` - Resource reference to the blob store.
/// * `hash` - 32-byte binary hash of the chunk.
/// * `tier` - Storage tier ("hot", "warm", or "cold").
/// * `verify` - If true, verify the data matches the hash after reading.
///
/// # Returns
/// The chunk data as a binary, or an error tuple.
/// If `verify` is true and the data is corrupt, returns an error.
#[rustler::nif]
fn store_read_chunk_verified<'a>(
    env: Env<'a>,
    store: ResourceArc<BlobStoreResource>,
    hash_bytes: Binary,
    tier: String,
    verify: bool,
) -> Result<Binary<'a>, String> {
    let hash = parse_hash(&hash_bytes)?;
    let tier = parse_tier(&tier)?;

    let options = ReadOptions { verify };

    let store_guard = store.store.lock().map_err(|e| e.to_string())?;
    let data = store_guard
        .read_chunk_with_options(&hash, tier, &options)
        .map_err(|e| e.to_string())?;

    let mut output = NewBinary::new(env, data.len());
    output.copy_from_slice(&data);
    Ok(output.into())
}

/// Deletes a chunk from the blob store.
///
/// # Arguments
/// * `store` - Resource reference to the blob store.
/// * `hash` - 32-byte binary hash of the chunk.
/// * `tier` - Storage tier ("hot", "warm", or "cold").
///
/// # Returns
/// `:ok` on success, or an error tuple.
#[rustler::nif]
fn store_delete_chunk(
    store: ResourceArc<BlobStoreResource>,
    hash_bytes: Binary,
    tier: String,
) -> Result<(), String> {
    let hash = parse_hash(&hash_bytes)?;
    let tier = parse_tier(&tier)?;

    let store_guard = store.store.lock().map_err(|e| e.to_string())?;
    store_guard
        .delete_chunk(&hash, tier)
        .map_err(|e| e.to_string())
}

/// Checks if a chunk exists in the blob store.
///
/// # Arguments
/// * `store` - Resource reference to the blob store.
/// * `hash` - 32-byte binary hash of the chunk.
/// * `tier` - Storage tier ("hot", "warm", or "cold").
///
/// # Returns
/// `true` if the chunk exists, `false` otherwise.
#[rustler::nif]
fn store_chunk_exists(
    store: ResourceArc<BlobStoreResource>,
    hash_bytes: Binary,
    tier: String,
) -> Result<bool, String> {
    let hash = parse_hash(&hash_bytes)?;
    let tier = parse_tier(&tier)?;

    let store_guard = store.store.lock().map_err(|e| e.to_string())?;
    Ok(store_guard.chunk_exists(&hash, tier))
}

/// Parses a 32-byte binary into a Hash.
fn parse_hash(hash_bytes: &Binary) -> Result<Hash, String> {
    if hash_bytes.len() != 32 {
        return Err(format!(
            "invalid hash length: expected 32 bytes, got {}",
            hash_bytes.len()
        ));
    }
    let mut bytes = [0u8; 32];
    bytes.copy_from_slice(hash_bytes.as_slice());
    Ok(Hash::from_bytes(bytes))
}

/// Parses a tier string into a Tier enum.
fn parse_tier(tier: &str) -> Result<Tier, String> {
    match tier {
        "hot" => Ok(Tier::Hot),
        "warm" => Ok(Tier::Warm),
        "cold" => Ok(Tier::Cold),
        _ => Err(format!(
            "invalid tier: expected 'hot', 'warm', or 'cold', got '{}'",
            tier
        )),
    }
}

rustler::init!("Elixir.NeonFS.Core.Blob.Native");
