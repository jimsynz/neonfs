pub mod chunking;
pub mod compression;
pub mod erasure;
pub mod error;
pub mod hash;
pub mod path;
pub mod store;

use crate::chunking::{auto_strategy, chunk_data, ChunkStrategy};
use crate::compression::Compression;
use crate::hash::Hash;
use crate::path::Tier;
use crate::store::{BlobStore, ReadOptions, StoreConfig, WriteOptions};
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

/// Writes a chunk to the blob store without compression.
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

/// Writes a chunk to the blob store with optional compression.
///
/// # Arguments
/// * `store` - Resource reference to the blob store.
/// * `hash` - 32-byte binary hash of the original (uncompressed) chunk data.
/// * `data` - Binary data to write.
/// * `tier` - Storage tier ("hot", "warm", or "cold").
/// * `compression` - Compression type: "none" or "zstd".
/// * `compression_level` - Compression level (1-19, used for zstd only).
///
/// # Returns
/// A map with chunk info: original_size, stored_size, compression.
#[rustler::nif]
fn store_write_chunk_compressed(
    store: ResourceArc<BlobStoreResource>,
    hash_bytes: Binary,
    data: Binary,
    tier: String,
    compression: String,
    compression_level: i32,
) -> Result<(usize, usize, String), String> {
    let hash = parse_hash(&hash_bytes)?;
    let tier = parse_tier(&tier)?;
    let compression = parse_compression(&compression, compression_level)?;

    let options = WriteOptions {
        compression: Some(compression),
    };

    let store_guard = store.store.lock().map_err(|e| e.to_string())?;
    let chunk_info = store_guard
        .write_chunk_with_options(&hash, data.as_slice(), tier, &options)
        .map_err(|e| e.to_string())?;

    let compression_str = match chunk_info.compression {
        Compression::None => "none".to_string(),
        Compression::Zstd { level } => format!("zstd:{}", level),
    };

    Ok((
        chunk_info.original_size,
        chunk_info.stored_size,
        compression_str,
    ))
}

/// Reads a chunk from the blob store without verification or decompression.
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

    let options = ReadOptions {
        verify,
        decompress: false,
    };

    let store_guard = store.store.lock().map_err(|e| e.to_string())?;
    let data = store_guard
        .read_chunk_with_options(&hash, tier, &options)
        .map_err(|e| e.to_string())?;

    let mut output = NewBinary::new(env, data.len());
    output.copy_from_slice(&data);
    Ok(output.into())
}

/// Reads a chunk from the blob store with optional verification and decompression.
///
/// # Arguments
/// * `env` - Rustler environment.
/// * `store` - Resource reference to the blob store.
/// * `hash` - 32-byte binary hash of the original (uncompressed) chunk data.
/// * `tier` - Storage tier ("hot", "warm", or "cold").
/// * `verify` - If true, verify the data matches the hash after reading/decompression.
/// * `decompress` - If true, decompress the data after reading.
///
/// # Returns
/// The chunk data as a binary, or an error tuple.
#[rustler::nif]
fn store_read_chunk_with_options<'a>(
    env: Env<'a>,
    store: ResourceArc<BlobStoreResource>,
    hash_bytes: Binary,
    tier: String,
    verify: bool,
    decompress: bool,
) -> Result<Binary<'a>, String> {
    let hash = parse_hash(&hash_bytes)?;
    let tier = parse_tier(&tier)?;

    let options = ReadOptions { verify, decompress };

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

/// Migrates a chunk from one tier to another.
///
/// This operation is atomic: the chunk is written to the destination tier,
/// verified, and only then deleted from the source tier.
///
/// # Arguments
/// * `store` - Resource reference to the blob store.
/// * `hash` - 32-byte binary hash of the chunk.
/// * `from_tier` - Source storage tier ("hot", "warm", or "cold").
/// * `to_tier` - Destination storage tier ("hot", "warm", or "cold").
///
/// # Returns
/// `:ok` on success, or an error tuple.
#[rustler::nif]
fn store_migrate_chunk(
    store: ResourceArc<BlobStoreResource>,
    hash_bytes: Binary,
    from_tier: String,
    to_tier: String,
) -> Result<(), String> {
    let hash = parse_hash(&hash_bytes)?;
    let from = parse_tier(&from_tier)?;
    let to = parse_tier(&to_tier)?;

    let store_guard = store.store.lock().map_err(|e| e.to_string())?;
    store_guard
        .migrate_chunk(&hash, from, to)
        .map_err(|e| e.to_string())
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

/// Parses compression string into a Compression enum.
fn parse_compression(compression: &str, level: i32) -> Result<Compression, String> {
    match compression {
        "none" => Ok(Compression::None),
        "zstd" => Ok(Compression::Zstd { level }),
        _ => Err(format!(
            "invalid compression: expected 'none' or 'zstd', got '{}'",
            compression
        )),
    }
}

/// Chunk result as returned to Elixir.
/// Each chunk is a tuple of (data_binary, hash_binary, offset, size).
type ChunkResultTuple<'a> = (Binary<'a>, Binary<'a>, usize, usize);

/// Splits data into chunks using the specified strategy.
///
/// # Arguments
/// * `env` - Rustler environment.
/// * `data` - Binary data to split.
/// * `strategy` - Chunking strategy: "single", "fixed", or "auto".
/// * `strategy_param` - Additional parameter for strategy:
///   - For "fixed": chunk size in bytes
///   - For "auto": ignored (pass 0)
///   - For "single": ignored (pass 0)
///
/// # Returns
/// A list of tuples, each containing (data, hash, offset, size).
#[rustler::nif]
fn nif_chunk_data<'a>(
    env: Env<'a>,
    data: Binary,
    strategy: String,
    strategy_param: usize,
) -> Result<Vec<ChunkResultTuple<'a>>, String> {
    let chunk_strategy = parse_chunk_strategy(&strategy, strategy_param)?;
    let chunks = chunk_data(data.as_slice(), &chunk_strategy);

    let results: Vec<ChunkResultTuple<'a>> = chunks
        .into_iter()
        .map(|chunk| {
            // Create binary for chunk data
            let mut data_bin = NewBinary::new(env, chunk.data.len());
            data_bin.copy_from_slice(&chunk.data);

            // Create binary for hash
            let mut hash_bin = NewBinary::new(env, 32);
            hash_bin.copy_from_slice(chunk.hash.as_bytes());

            (data_bin.into(), hash_bin.into(), chunk.offset, chunk.size)
        })
        .collect();

    Ok(results)
}

/// Determines the appropriate chunking strategy for the given data length.
///
/// # Arguments
/// * `data_len` - Length of the data to be chunked.
///
/// # Returns
/// A tuple of (strategy_name, strategy_param) where:
/// - "single" with param 0 for small data (< 64KB)
/// - "fixed" with param 262144 (256KB) for medium data (64KB - 1MB)
/// - "fastcdc" with param 262144 (avg chunk size) for large data (>= 1MB)
#[rustler::nif]
fn nif_auto_chunk_strategy(data_len: usize) -> (String, usize) {
    match auto_strategy(data_len) {
        ChunkStrategy::Single => ("single".to_string(), 0),
        ChunkStrategy::Fixed { size } => ("fixed".to_string(), size),
        ChunkStrategy::FastCDC { avg, .. } => ("fastcdc".to_string(), avg),
    }
}

/// Parses a strategy string and parameter into a ChunkStrategy.
fn parse_chunk_strategy(strategy: &str, param: usize) -> Result<ChunkStrategy, String> {
    match strategy {
        "single" => Ok(ChunkStrategy::Single),
        "fixed" => {
            if param == 0 {
                Err("fixed strategy requires non-zero chunk size".to_string())
            } else {
                Ok(ChunkStrategy::Fixed { size: param })
            }
        }
        "auto" => Ok(auto_strategy(param)), // param is data length for auto
        "fastcdc" => {
            // Use param as average size, derive min/max from it
            let avg = if param == 0 { 256 * 1024 } else { param };
            let min = avg / 4;
            let max = avg * 4;
            Ok(ChunkStrategy::FastCDC { min, avg, max })
        }
        _ => Err(format!(
            "invalid strategy: expected 'single', 'fixed', 'auto', or 'fastcdc', got '{}'",
            strategy
        )),
    }
}

/// Encodes data shards and returns parity shards using Reed-Solomon erasure coding.
///
/// # Arguments
/// * `env` - Rustler environment
/// * `data_shards` - List of equal-sized binary data shards
/// * `parity_count` - Number of parity shards to generate
///
/// # Returns
/// A list of parity shard binaries, or an error string.
#[rustler::nif]
fn erasure_encode<'a>(
    env: Env<'a>,
    data_shards: Vec<Binary>,
    parity_count: usize,
) -> Result<Vec<Binary<'a>>, String> {
    let data: Vec<Vec<u8>> = data_shards.iter().map(|b| b.as_slice().to_vec()).collect();

    let parity = erasure::encode(data, parity_count).map_err(|e| e.to_string())?;

    let result: Vec<Binary<'a>> = parity
        .into_iter()
        .map(|shard| {
            let mut bin = NewBinary::new(env, shard.len());
            bin.copy_from_slice(&shard);
            bin.into()
        })
        .collect();

    Ok(result)
}

/// Decodes (reconstructs) missing data shards from available shards.
///
/// # Arguments
/// * `env` - Rustler environment
/// * `shards_with_indices` - List of `(index, binary)` tuples for available shards
/// * `data_count` - Number of data shards in the original encoding
/// * `parity_count` - Number of parity shards in the original encoding
/// * `shard_size` - Expected size of each shard in bytes
///
/// # Returns
/// A list of all data shard binaries (reconstructed), or an error string.
#[rustler::nif]
fn erasure_decode<'a>(
    env: Env<'a>,
    shards_with_indices: Vec<(usize, Binary<'a>)>,
    data_count: usize,
    parity_count: usize,
    shard_size: usize,
) -> Result<Vec<Binary<'a>>, String> {
    let indexed: Vec<(usize, Vec<u8>)> = shards_with_indices
        .into_iter()
        .map(|(i, b)| (i, b.as_slice().to_vec()))
        .collect();

    let data_shards = erasure::decode(indexed, data_count, parity_count, shard_size)
        .map_err(|e| e.to_string())?;

    let result: Vec<Binary<'a>> = data_shards
        .into_iter()
        .map(|shard| {
            let mut bin = NewBinary::new(env, shard.len());
            bin.copy_from_slice(&shard);
            bin.into()
        })
        .collect();

    Ok(result)
}

/// Writes metadata to the blob store's metadata namespace.
///
/// # Arguments
/// * `store` - Resource reference to the blob store.
/// * `segment_id_hex` - 64-char hex string identifying the metadata segment.
/// * `key_hash_bytes` - 32-byte binary hash of the metadata key.
/// * `data` - Binary metadata to write.
#[rustler::nif]
fn metadata_write(
    store: ResourceArc<BlobStoreResource>,
    segment_id_hex: String,
    key_hash_bytes: Binary,
    data: Binary,
) -> Result<(), String> {
    let key_hash = parse_hash(&key_hash_bytes)?;

    let store_guard = store.store.lock().map_err(|e| e.to_string())?;
    store_guard
        .write_metadata(&segment_id_hex, &key_hash, data.as_slice())
        .map_err(|e| e.to_string())
}

/// Reads metadata from the blob store's metadata namespace.
///
/// # Arguments
/// * `env` - Rustler environment.
/// * `store` - Resource reference to the blob store.
/// * `segment_id_hex` - 64-char hex string identifying the metadata segment.
/// * `key_hash_bytes` - 32-byte binary hash of the metadata key.
#[rustler::nif]
fn metadata_read<'a>(
    env: Env<'a>,
    store: ResourceArc<BlobStoreResource>,
    segment_id_hex: String,
    key_hash_bytes: Binary,
) -> Result<Binary<'a>, String> {
    let key_hash = parse_hash(&key_hash_bytes)?;

    let store_guard = store.store.lock().map_err(|e| e.to_string())?;
    let data = store_guard
        .read_metadata(&segment_id_hex, &key_hash)
        .map_err(|e| e.to_string())?;

    let mut output = NewBinary::new(env, data.len());
    output.copy_from_slice(&data);
    Ok(output.into())
}

/// Deletes metadata from the blob store's metadata namespace.
///
/// # Arguments
/// * `store` - Resource reference to the blob store.
/// * `segment_id_hex` - 64-char hex string identifying the metadata segment.
/// * `key_hash_bytes` - 32-byte binary hash of the metadata key.
#[rustler::nif]
fn metadata_delete(
    store: ResourceArc<BlobStoreResource>,
    segment_id_hex: String,
    key_hash_bytes: Binary,
) -> Result<(), String> {
    let key_hash = parse_hash(&key_hash_bytes)?;

    let store_guard = store.store.lock().map_err(|e| e.to_string())?;
    store_guard
        .delete_metadata(&segment_id_hex, &key_hash)
        .map_err(|e| e.to_string())
}

/// Lists all metadata key hashes in a segment.
///
/// # Arguments
/// * `env` - Rustler environment.
/// * `store` - Resource reference to the blob store.
/// * `segment_id_hex` - 64-char hex string identifying the metadata segment.
#[rustler::nif]
fn metadata_list_segment<'a>(
    env: Env<'a>,
    store: ResourceArc<BlobStoreResource>,
    segment_id_hex: String,
) -> Result<Vec<Binary<'a>>, String> {
    let store_guard = store.store.lock().map_err(|e| e.to_string())?;
    let hashes = store_guard
        .list_metadata_segment(&segment_id_hex)
        .map_err(|e| e.to_string())?;

    let result: Vec<Binary<'a>> = hashes
        .into_iter()
        .map(|hash| {
            let mut bin = NewBinary::new(env, 32);
            bin.copy_from_slice(hash.as_bytes());
            bin.into()
        })
        .collect();

    Ok(result)
}

rustler::init!("Elixir.NeonFS.Core.Blob.Native");
