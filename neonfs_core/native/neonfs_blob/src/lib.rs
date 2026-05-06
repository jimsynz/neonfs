pub mod chunking;
pub mod codec;
pub mod compression;
pub mod encryption;
pub mod erasure;
pub mod error;
pub mod hash;
pub mod index_tree;
pub mod index_tree_blob_store;
pub mod path;
pub mod store;

use crate::chunking::{auto_strategy, chunk_data, ChunkResult, ChunkStrategy, IncrementalChunker};
use crate::compression::Compression;
use crate::encryption::EncryptionParams;
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

/// Writes a chunk to the blob store with optional compression and encryption.
///
/// # Arguments
/// * `env` - Rustler environment.
/// * `store` - Resource reference to the blob store.
/// * `hash` - 32-byte binary hash of the original (uncompressed) chunk data.
/// * `data` - Binary data to write.
/// * `tier` - Storage tier ("hot", "warm", or "cold").
/// * `compression` - Compression type: "none" or "zstd".
/// * `compression_level` - Compression level (1-19, used for zstd only).
/// * `key_binary` - 32-byte AES-256 key, or empty binary for no encryption.
/// * `nonce_binary` - 12-byte nonce, or empty binary for no encryption.
///
/// # Returns
/// A tuple of (original_size, stored_size, compression, encryption_algorithm, nonce).
#[rustler::nif]
#[allow(clippy::too_many_arguments)]
fn store_write_chunk_compressed<'a>(
    env: Env<'a>,
    store: ResourceArc<BlobStoreResource>,
    hash_bytes: Binary,
    data: Binary,
    tier: String,
    compression: String,
    compression_level: i32,
    key_binary: Binary,
    nonce_binary: Binary,
) -> Result<(usize, usize, String, String, Binary<'a>), String> {
    let hash = parse_hash(&hash_bytes)?;
    let tier = parse_tier(&tier)?;
    let compression = parse_compression(&compression, compression_level)?;
    let encryption = parse_encryption_params(&key_binary, &nonce_binary)?;

    let options = WriteOptions {
        compression: Some(compression),
        encryption,
    };

    let store_guard = store.store.lock().map_err(|e| e.to_string())?;
    let chunk_info = store_guard
        .write_chunk_with_options(&hash, data.as_slice(), tier, &options)
        .map_err(|e| e.to_string())?;

    let compression_str = match chunk_info.compression {
        Compression::None => "none".to_string(),
        Compression::Zstd { level } => format!("zstd:{}", level),
    };

    let encryption_algorithm = chunk_info.encryption_algorithm.unwrap_or_default();

    let nonce_bytes = chunk_info.encryption_nonce.unwrap_or_default();
    let mut nonce_out = NewBinary::new(env, nonce_bytes.len());
    if !nonce_bytes.is_empty() {
        nonce_out.copy_from_slice(&nonce_bytes);
    }

    Ok((
        chunk_info.original_size,
        chunk_info.stored_size,
        compression_str,
        encryption_algorithm,
        nonce_out.into(),
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
        compression: None,
        encryption: None,
    };

    let store_guard = store.store.lock().map_err(|e| e.to_string())?;
    let data = store_guard
        .read_chunk_with_options(&hash, tier, &options)
        .map_err(|e| e.to_string())?;

    let mut output = NewBinary::new(env, data.len());
    output.copy_from_slice(&data);
    Ok(output.into())
}

/// Reads a chunk from the blob store with optional verification, decompression,
/// and decryption.
///
/// # Arguments
/// * `env` - Rustler environment.
/// * `store` - Resource reference to the blob store.
/// * `hash` - 32-byte binary hash of the original (uncompressed) chunk data.
/// * `tier` - Storage tier ("hot", "warm", or "cold").
/// * `verify` - If true, verify the data matches the hash after reading/decompression.
/// * `decompress` - If true, decompress the data after reading.
/// * `key_binary` - 32-byte AES-256 key, or empty binary for no decryption.
/// * `nonce_binary` - 12-byte nonce, or empty binary for no decryption.
///
/// # Returns
/// The chunk data as a binary, or an error tuple.
/// Codec locator tuple passed as a single arg from Elixir to avoid inflating
/// arity on NIFs that already carry a lot of positional state.
/// `(compression_kind, compression_level, key, nonce)` — empty strings/binaries
/// mean "not applicable".
type CodecLocator<'a> = (String, i32, Binary<'a>, Binary<'a>);

fn parse_codec_locator<'a>(
    locator: &CodecLocator<'a>,
) -> Result<(Option<Compression>, Option<EncryptionParams>), String> {
    let (ref comp, level, ref key, ref nonce) = *locator;
    let compression = parse_optional_compression(comp, level)?;
    let encryption = parse_encryption_params(key, nonce)?;
    Ok((compression, encryption))
}

#[rustler::nif]
fn store_read_chunk_with_options<'a>(
    env: Env<'a>,
    store: ResourceArc<BlobStoreResource>,
    hash_bytes: Binary,
    tier: String,
    verify: bool,
    decompress: bool,
    codec: CodecLocator<'a>,
) -> Result<Binary<'a>, String> {
    let hash = parse_hash(&hash_bytes)?;
    let tier = parse_tier(&tier)?;
    let (compression, encryption) = parse_codec_locator(&codec)?;

    let options = ReadOptions {
        verify,
        decompress,
        compression,
        encryption,
    };

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
/// `{:ok, bytes_freed}` on success, or an error tuple.
#[allow(clippy::too_many_arguments)]
#[rustler::nif]
fn store_delete_chunk(
    store: ResourceArc<BlobStoreResource>,
    hash_bytes: Binary,
    tier: String,
    compression: String,
    compression_level: i32,
    key_binary: Binary,
    nonce_binary: Binary,
) -> Result<u64, String> {
    let hash = parse_hash(&hash_bytes)?;
    let tier = parse_tier(&tier)?;
    let encryption = parse_encryption_params(&key_binary, &nonce_binary)?;
    let compression = parse_optional_compression(&compression, compression_level)?;

    let store_guard = store.store.lock().map_err(|e| e.to_string())?;
    store_guard
        .delete_chunk(&hash, tier, compression.as_ref(), encryption.as_ref())
        .map_err(|e| e.to_string())
}

/// Deletes every codec variant of a chunk at a tier.
///
/// Intended for orphan cleanup paths that don't track codec settings.
/// Returns the total bytes freed across all variants.
#[rustler::nif]
fn store_delete_chunk_any_codec(
    store: ResourceArc<BlobStoreResource>,
    hash_bytes: Binary,
    tier: String,
) -> Result<u64, String> {
    let hash = parse_hash(&hash_bytes)?;
    let tier = parse_tier(&tier)?;

    let store_guard = store.store.lock().map_err(|e| e.to_string())?;
    store_guard
        .delete_chunk_any_codec(&hash, tier)
        .map_err(|e| e.to_string())
}

/// Checks if a specific codec variant of a chunk exists in the blob store.
#[allow(clippy::too_many_arguments)]
#[rustler::nif]
fn store_chunk_exists(
    store: ResourceArc<BlobStoreResource>,
    hash_bytes: Binary,
    tier: String,
    compression: String,
    compression_level: i32,
    key_binary: Binary,
    nonce_binary: Binary,
) -> Result<bool, String> {
    let hash = parse_hash(&hash_bytes)?;
    let tier = parse_tier(&tier)?;
    let encryption = parse_encryption_params(&key_binary, &nonce_binary)?;
    let compression = parse_optional_compression(&compression, compression_level)?;

    let store_guard = store.store.lock().map_err(|e| e.to_string())?;
    Ok(store_guard.chunk_exists(&hash, tier, compression.as_ref(), encryption.as_ref()))
}

/// Checks whether any codec variant of a chunk exists at a tier.
#[rustler::nif]
fn store_chunk_exists_any_codec(
    store: ResourceArc<BlobStoreResource>,
    hash_bytes: Binary,
    tier: String,
) -> Result<bool, String> {
    let hash = parse_hash(&hash_bytes)?;
    let tier = parse_tier(&tier)?;

    let store_guard = store.store.lock().map_err(|e| e.to_string())?;
    store_guard
        .chunk_exists_any_codec(&hash, tier)
        .map_err(|e| e.to_string())
}

/// Returns the on-disk size of any codec variant of the chunk, or `0` if no
/// variant exists. Use together with `store_chunk_exists_any_codec` to
/// distinguish "missing" from "zero-byte".
#[rustler::nif]
fn store_chunk_any_codec_size(
    store: ResourceArc<BlobStoreResource>,
    hash_bytes: Binary,
    tier: String,
) -> Result<u64, String> {
    let hash = parse_hash(&hash_bytes)?;
    let tier = parse_tier(&tier)?;

    let store_guard = store.store.lock().map_err(|e| e.to_string())?;
    store_guard
        .chunk_any_codec_size(&hash, tier)
        .map(|opt| opt.unwrap_or(0))
        .map_err(|e| e.to_string())
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
#[allow(clippy::too_many_arguments)]
#[rustler::nif]
fn store_migrate_chunk(
    store: ResourceArc<BlobStoreResource>,
    hash_bytes: Binary,
    from_tier: String,
    to_tier: String,
    compression: String,
    compression_level: i32,
    key_binary: Binary,
    nonce_binary: Binary,
) -> Result<(), String> {
    let hash = parse_hash(&hash_bytes)?;
    let from = parse_tier(&from_tier)?;
    let to = parse_tier(&to_tier)?;
    let encryption = parse_encryption_params(&key_binary, &nonce_binary)?;
    let compression = parse_optional_compression(&compression, compression_level)?;

    let store_guard = store.store.lock().map_err(|e| e.to_string())?;
    store_guard
        .migrate_chunk(&hash, from, to, compression.as_ref(), encryption.as_ref())
        .map_err(|e| e.to_string())
}

/// Re-encrypts a chunk in place: decrypts with old key/nonce, re-encrypts
/// with new key/nonce, writes back atomically.
///
/// # Arguments
/// * `store` - Resource reference to the blob store.
/// * `hash` - 32-byte binary hash of the chunk.
/// * `tier` - Storage tier ("hot", "warm", or "cold").
/// * `old_key` - 32-byte AES-256 key used for current encryption.
/// * `old_nonce` - 12-byte nonce used for current encryption.
/// * `new_key` - 32-byte AES-256 key for new encryption.
/// * `new_nonce` - 12-byte nonce for new encryption.
///
/// # Returns
/// The stored size of the re-encrypted chunk, or an error tuple.
#[rustler::nif]
fn store_reencrypt_chunk(
    store: ResourceArc<BlobStoreResource>,
    hash_bytes: Binary,
    tier: String,
    compression: (String, i32),
    old_enc: (Binary, Binary),
    new_enc: (Binary, Binary),
) -> Result<usize, String> {
    let hash = parse_hash(&hash_bytes)?;
    let tier = parse_tier(&tier)?;
    let (comp_str, comp_level) = compression;
    let compression = parse_optional_compression(&comp_str, comp_level)?;

    let old_params = parse_required_encryption_params(&old_enc.0, &old_enc.1)?;
    let new_params = parse_required_encryption_params(&new_enc.0, &new_enc.1)?;

    let store_guard = store.store.lock().map_err(|e| e.to_string())?;
    store_guard
        .reencrypt_chunk(&hash, tier, compression.as_ref(), &old_params, &new_params)
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

/// Parses the codec-locator compression string.
///
/// `""` (empty) means "caller doesn't know or doesn't care" → `None`. Any other
/// value parses as a normal compression spec; `"none"` returns
/// `Some(Compression::None)` which maps to the same plain-codec suffix.
fn parse_optional_compression(
    compression: &str,
    level: i32,
) -> Result<Option<Compression>, String> {
    if compression.is_empty() {
        Ok(None)
    } else {
        parse_compression(compression, level).map(Some)
    }
}

/// Parses optional encryption parameters from key and nonce binaries.
/// Empty binaries mean no encryption. Returns None for no encryption.
fn parse_encryption_params(
    key_binary: &Binary,
    nonce_binary: &Binary,
) -> Result<Option<EncryptionParams>, String> {
    if key_binary.is_empty() && nonce_binary.is_empty() {
        return Ok(None);
    }
    let params = parse_required_encryption_params(key_binary, nonce_binary)?;
    Ok(Some(params))
}

/// Parses required encryption parameters (non-empty key and nonce).
fn parse_required_encryption_params(
    key_binary: &Binary,
    nonce_binary: &Binary,
) -> Result<EncryptionParams, String> {
    EncryptionParams::new(key_binary.as_slice(), nonce_binary.as_slice()).map_err(|e| e.to_string())
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

/// Resource wrapping an `IncrementalChunker` so it can be passed back and
/// forth between Elixir calls. The mutex protects state that must be mutated
/// across feed/finish calls; lock contention is irrelevant in normal use
/// because callers own the chunker and call it serially.
pub struct ChunkerResource {
    inner: Mutex<IncrementalChunker>,
}

#[rustler::resource_impl]
impl Resource for ChunkerResource {}

fn chunks_to_tuples<'a>(env: Env<'a>, chunks: Vec<ChunkResult>) -> Vec<ChunkResultTuple<'a>> {
    chunks
        .into_iter()
        .map(|chunk| {
            let mut data_bin = NewBinary::new(env, chunk.data.len());
            data_bin.copy_from_slice(&chunk.data);

            let mut hash_bin = NewBinary::new(env, 32);
            hash_bin.copy_from_slice(chunk.hash.as_bytes());

            (data_bin.into(), hash_bin.into(), chunk.offset, chunk.size)
        })
        .collect()
}

/// Creates an incremental chunker for the given strategy. The strategy/param
/// arguments follow the same convention as [`nif_chunk_data`].
///
/// # Returns
/// A resource handle that must be passed to [`chunker_feed`] and
/// [`chunker_finish`].
#[rustler::nif]
fn chunker_init(
    strategy: String,
    strategy_param: usize,
) -> Result<ResourceArc<ChunkerResource>, String> {
    let strategy = parse_chunk_strategy(&strategy, strategy_param)?;
    Ok(ResourceArc::new(ChunkerResource {
        inner: Mutex::new(IncrementalChunker::new(strategy)),
    }))
}

/// Feeds a slice of data into the chunker and returns any complete chunks
/// that became available. Bytes that may still belong to a future chunk
/// remain buffered inside the resource.
#[rustler::nif]
fn chunker_feed<'a>(
    env: Env<'a>,
    chunker: ResourceArc<ChunkerResource>,
    data: Binary,
) -> Vec<ChunkResultTuple<'a>> {
    let mut state = chunker.inner.lock().expect("chunker mutex poisoned");
    let chunks = state.feed(data.as_slice());
    chunks_to_tuples(env, chunks)
}

/// Flushes any remaining buffered data as the final chunks. The chunker may
/// be reused after `finish`; offsets continue from where they left off.
#[rustler::nif]
fn chunker_finish<'a>(
    env: Env<'a>,
    chunker: ResourceArc<ChunkerResource>,
) -> Vec<ChunkResultTuple<'a>> {
    let mut state = chunker.inner.lock().expect("chunker mutex poisoned");
    let chunks = state.finish();
    chunks_to_tuples(env, chunks)
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

/// Returns filesystem capacity information for the given path.
///
/// Uses `statvfs()` to query the filesystem containing `path`.
///
/// # Returns
/// A tuple of `(total_bytes, available_bytes, used_bytes)`.
#[rustler::nif]
fn filesystem_info(path: String) -> Result<(u64, u64, u64), String> {
    use std::ffi::CString;
    use std::mem::MaybeUninit;

    let c_path = CString::new(path.as_bytes()).map_err(|e| e.to_string())?;

    let mut stat = MaybeUninit::<libc::statvfs>::uninit();

    let result = unsafe { libc::statvfs(c_path.as_ptr(), stat.as_mut_ptr()) };

    if result != 0 {
        let errno = std::io::Error::last_os_error();
        return Err(format!("statvfs failed for '{}': {}", path, errno));
    }

    let stat = unsafe { stat.assume_init() };

    let block_size = stat.f_frsize;
    let total_bytes = stat.f_blocks * block_size;
    let available_bytes = stat.f_bavail * block_size;
    let used_bytes = total_bytes.saturating_sub(stat.f_bfree * block_size);

    Ok((total_bytes, available_bytes, used_bytes))
}

rustler::init!("Elixir.NeonFS.Core.Blob.Native");
