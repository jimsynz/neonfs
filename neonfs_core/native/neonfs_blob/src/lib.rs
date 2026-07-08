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
use crate::index_tree::{IndexTree, TreeConfig};
use crate::index_tree_blob_store::BlobStoreChunkStore;
use crate::path::Tier;
use crate::store::{BlobStore, DriveOpenState, ReadOptions, StoreConfig, WriteOptions};
use rustler::{Atom, Binary, Encoder, Env, NewBinary, OwnedEnv, Resource, ResourceArc, Term};
use std::panic::AssertUnwindSafe;
use std::sync::{Arc, Mutex, OnceLock};

mod marker_atoms {
    rustler::atoms! { clean, dirty, fresh }
}

/// Resource wrapper for BlobStore to share across NIF calls.
///
/// `BlobStore`'s methods all take `&self` and its only mutable state is the
/// filesystem itself, so the resource shares it unsynchronised across every
/// NIF call — concurrent calls to the same drive run in parallel rather than
/// serialising on a lock (#1479). The store is held behind an `Arc` so async
/// NIFs can hand a cheap clone to a tokio blocking task that outlives the
/// resource reference the caller passed in (#1484).
pub struct BlobStoreResource {
    store: Arc<BlobStore>,
}

/// Process-wide tokio runtime backing the async blob NIFs (#1484, part of
/// #1197). Blob store operations are synchronous disk I/O, so async NIFs
/// submit them to this runtime's blocking pool via `spawn_blocking` and return
/// immediately; the completion is delivered to the caller as a message. A
/// single shared runtime avoids multiplying blocking-thread pools per drive.
static BLOB_RUNTIME: OnceLock<tokio::runtime::Runtime> = OnceLock::new();

fn blob_runtime() -> &'static tokio::runtime::Runtime {
    BLOB_RUNTIME.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .thread_name("neonfs-blob")
            .build()
            .expect("failed to build blob storage tokio runtime")
    })
}

/// A drive's clean/dirty classification, carried through the async reply so
/// the `:clean | :dirty | :fresh` atom is built in the reply env (#1486).
enum DriveMarker {
    Clean,
    Dirty,
    Fresh,
}

/// The `:ok` payload of an async blob NIF completion, built in the reply env.
/// One variant per reply shape across the async blob NIFs (#1484/#1485/#1486).
enum BlobReply {
    /// `{:ok, {}}` — an operation with no return value (e.g. plain write).
    Unit,
    /// `{:ok, binary}` — chunk data.
    Data(Vec<u8>),
    /// `{:ok, boolean}` — an existence check.
    Bool(bool),
    /// `{:ok, integer}` — a byte count (chunk size, bytes freed, stored size).
    Size(u64),
    /// `{:ok, :clean | :dirty | :fresh}` — a drive open-marker classification.
    Marker(DriveMarker),
    /// `{:ok, {total, available, used}}` — filesystem capacity in bytes.
    IntTriple(u64, u64, u64),
    /// `{:ok, {original_size, stored_size, compression, encryption_algorithm,
    /// nonce}}` — the codec metadata returned by a compressed/encrypted write.
    WriteInfo {
        original_size: usize,
        stored_size: usize,
        compression: String,
        encryption_algorithm: String,
        nonce: Vec<u8>,
    },
}

/// Runs `produce` on the tokio blocking pool and delivers its result to the
/// calling process as a `{request_ref, {:ok, value}}` or `{request_ref,
/// {:error, reason}}` message. `request_ref` is the caller's own `make_ref/0`,
/// echoed back so the caller's selective `receive` matches only its reply —
/// and, being created immediately before the `receive`, lets the BEAM skip the
/// process mailbox backlog (important because blob ops run inside the single
/// `BlobStore` GenServer).
///
/// The submit NIF returns immediately, freeing its scheduler. `catch_unwind`
/// guarantees a completion is always sent even if `produce` panics, so the
/// caller's `receive` can never hang.
fn spawn_reply<F>(env: Env<'_>, request_ref: Term<'_>, produce: F) -> Atom
where
    F: FnOnce() -> Result<BlobReply, String> + Send + 'static,
{
    let pid = env.pid();
    let reply_env = OwnedEnv::new();
    let saved_ref = reply_env.save(request_ref);

    blob_runtime().spawn_blocking(move || {
        let result = std::panic::catch_unwind(AssertUnwindSafe(produce))
            .unwrap_or_else(|_| Err("blob task panicked".to_string()));

        let mut reply_env = reply_env;
        let _ = reply_env.send_and_clear(&pid, |env| {
            encode_blob_reply(env, saved_ref.load(env), result)
        });
    });

    rustler::types::atom::ok()
}

/// Convenience wrapper over [`spawn_reply`] for the store-backed NIFs: hands the
/// blocking `work` a shared `&BlobStore` that outlives the caller's resource
/// reference.
fn submit_blob<F>(store: Arc<BlobStore>, env: Env<'_>, request_ref: Term<'_>, work: F) -> Atom
where
    F: FnOnce(&BlobStore) -> Result<BlobReply, String> + Send + 'static,
{
    spawn_reply(env, request_ref, move || work(&store))
}

fn encode_blob_reply<'a>(
    env: Env<'a>,
    request_ref: Term<'a>,
    result: Result<BlobReply, String>,
) -> Term<'a> {
    let ok = rustler::types::atom::ok();
    match result {
        Ok(BlobReply::Unit) => (request_ref, (ok, ())).encode(env),
        Ok(BlobReply::Data(data)) => {
            let mut binary = NewBinary::new(env, data.len());
            binary.copy_from_slice(&data);
            let binary: Binary = binary.into();
            (request_ref, (ok, binary)).encode(env)
        }
        Ok(BlobReply::Bool(value)) => (request_ref, (ok, value)).encode(env),
        Ok(BlobReply::Size(value)) => (request_ref, (ok, value)).encode(env),
        Ok(BlobReply::Marker(marker)) => {
            let marker = match marker {
                DriveMarker::Clean => marker_atoms::clean(),
                DriveMarker::Dirty => marker_atoms::dirty(),
                DriveMarker::Fresh => marker_atoms::fresh(),
            };
            (request_ref, (ok, marker)).encode(env)
        }
        Ok(BlobReply::IntTriple(a, b, c)) => (request_ref, (ok, (a, b, c))).encode(env),
        Ok(BlobReply::WriteInfo {
            original_size,
            stored_size,
            compression,
            encryption_algorithm,
            nonce,
        }) => {
            let mut nonce_binary = NewBinary::new(env, nonce.len());
            if !nonce.is_empty() {
                nonce_binary.copy_from_slice(&nonce);
            }
            let nonce_binary: Binary = nonce_binary.into();
            let info = (
                original_size,
                stored_size,
                compression,
                encryption_algorithm,
                nonce_binary,
            );
            (request_ref, (ok, info)).encode(env)
        }
        Err(reason) => (request_ref, (rustler::types::atom::error(), reason)).encode(env),
    }
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
#[rustler::nif(schedule = "DirtyCpu")]
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
#[rustler::nif(schedule = "DirtyIo")]
fn store_open(
    base_dir: String,
    prefix_depth: usize,
) -> Result<ResourceArc<BlobStoreResource>, String> {
    let config = StoreConfig { prefix_depth };

    match BlobStore::new(&base_dir, config) {
        Ok(store) => Ok(ResourceArc::new(BlobStoreResource {
            store: Arc::new(store),
        })),
        Err(e) => Err(e.to_string()),
    }
}

/// Brings a drive up and reports how it presented itself (#1425/#1426).
///
/// Reads the prior per-drive marker, classifies the drive (`:clean` = was
/// cleanly closed by this same `node_id`; `:dirty` = a marker was present
/// but not clean — crash / foreign / stale, needs verification; `:fresh`
/// = no marker, a brand-new drive), then stamps a fresh `dirty` marker
/// durably. Call once after `store_open`, before serving the drive.
///
/// # Returns
/// `:ok`; the classification arrives as a
/// `{request_ref, {:ok, :clean | :dirty | :fresh} | {:error, reason}}` message.
#[rustler::nif]
fn store_open_marker_submit<'a>(
    env: Env<'a>,
    store: ResourceArc<BlobStoreResource>,
    request_ref: Term<'a>,
    node_id: String,
) -> Atom {
    submit_blob(store.store.clone(), env, request_ref, move |store| {
        store
            .open_marker(&node_id)
            .map(|state| match state {
                DriveOpenState::Clean => BlobReply::Marker(DriveMarker::Clean),
                DriveOpenState::Dirty => BlobReply::Marker(DriveMarker::Dirty),
                DriveOpenState::Fresh => BlobReply::Marker(DriveMarker::Fresh),
            })
            .map_err(|e| e.to_string())
    })
}

/// Rewrites a drive's marker `clean` (#1425). Call on graceful shutdown
/// so the next `store_open_marker` reports the drive clean.
///
/// # Returns
/// `:ok`; the outcome arrives as a
/// `{request_ref, {:ok, {}} | {:error, reason}}` message.
#[rustler::nif]
fn store_mark_clean_submit<'a>(
    env: Env<'a>,
    store: ResourceArc<BlobStoreResource>,
    request_ref: Term<'a>,
    node_id: String,
) -> Atom {
    submit_blob(store.store.clone(), env, request_ref, move |store| {
        store
            .mark_clean(&node_id)
            .map(|()| BlobReply::Unit)
            .map_err(|e| e.to_string())
    })
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
/// `:ok`; the outcome arrives as a
/// `{request_ref, {:ok, {}} | {:error, reason}}` message (#1485).
///
/// `data` is a single chunk (bounded by the volume's chunk size), copied into
/// an owned buffer for the blocking task — the working set never scales with
/// file size.
#[rustler::nif]
fn store_write_chunk_submit<'a>(
    env: Env<'a>,
    store: ResourceArc<BlobStoreResource>,
    request_ref: Term<'a>,
    hash_bytes: Binary<'a>,
    data: Binary<'a>,
    tier: String,
) -> Atom {
    let hash_bytes = hash_bytes.as_slice().to_vec();
    let data = data.as_slice().to_vec();
    submit_blob(store.store.clone(), env, request_ref, move |store| {
        let hash = hash_from_bytes(&hash_bytes)?;
        let tier = parse_tier(&tier)?;
        store
            .write_chunk(&hash, &data, tier)
            .map(|()| BlobReply::Unit)
            .map_err(|e| e.to_string())
    })
}

/// Writes a chunk to the blob store with optional compression and encryption.
///
/// # Arguments
/// * `env` - Rustler environment.
/// * `store` - Resource reference to the blob store.
/// * `hash` - 32-byte binary hash of the original (uncompressed) chunk data.
/// * `data` - Binary data to write.
/// * `tier` - Storage tier ("hot", "warm", or "cold").
/// * `codec` - `(compression, compression_level, key, nonce)` locator, as on
///   the read path; empty key/nonce mean no encryption.
///
/// # Returns
/// `:ok`; the codec metadata arrives as a `{request_ref, {:ok, {original_size,
/// stored_size, compression, encryption_algorithm, nonce}} | {:error, reason}}`
/// message. `data` is a single chunk, copied into an owned buffer for the
/// blocking task — the working set never scales with file size.
#[rustler::nif]
fn store_write_chunk_compressed_submit<'a>(
    env: Env<'a>,
    store: ResourceArc<BlobStoreResource>,
    request_ref: Term<'a>,
    hash_bytes: Binary<'a>,
    data: Binary<'a>,
    tier: String,
    codec: CodecLocator<'a>,
) -> Atom {
    let hash_bytes = hash_bytes.as_slice().to_vec();
    let data = data.as_slice().to_vec();
    let (compression, compression_level, key_binary, nonce_binary) = codec;
    let key_binary = key_binary.as_slice().to_vec();
    let nonce_binary = nonce_binary.as_slice().to_vec();
    submit_blob(store.store.clone(), env, request_ref, move |store| {
        let hash = hash_from_bytes(&hash_bytes)?;
        let tier = parse_tier(&tier)?;
        let compression = parse_compression(&compression, compression_level)?;
        let encryption = encryption_from_bytes(&key_binary, &nonce_binary)?;

        let options = WriteOptions {
            compression: Some(compression),
            encryption,
        };

        let chunk_info = store
            .write_chunk_with_options(&hash, &data, tier, &options)
            .map_err(|e| e.to_string())?;

        let compression = match chunk_info.compression {
            Compression::None => "none".to_string(),
            Compression::Zstd { level } => format!("zstd:{}", level),
        };

        Ok(BlobReply::WriteInfo {
            original_size: chunk_info.original_size,
            stored_size: chunk_info.stored_size,
            compression,
            encryption_algorithm: chunk_info.encryption_algorithm.unwrap_or_default(),
            nonce: chunk_info.encryption_nonce.unwrap_or_default(),
        })
    })
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
/// `:ok`; the chunk data arrives as a
/// `{request_ref, {:ok, binary} | {:error, reason}}` message (#1484).
#[rustler::nif]
fn store_read_chunk_submit<'a>(
    env: Env<'a>,
    store: ResourceArc<BlobStoreResource>,
    request_ref: Term<'a>,
    hash_bytes: Binary<'a>,
    tier: String,
) -> Atom {
    let hash_bytes = hash_bytes.as_slice().to_vec();
    submit_blob(store.store.clone(), env, request_ref, move |store| {
        let hash = hash_from_bytes(&hash_bytes)?;
        let tier = parse_tier(&tier)?;
        store
            .read_chunk(&hash, tier)
            .map(BlobReply::Data)
            .map_err(|e| e.to_string())
    })
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
/// `:ok`; the chunk data arrives as a
/// `{request_ref, {:ok, binary} | {:error, reason}}` message. If `verify` is
/// true and the data is corrupt, the reply is an error.
#[rustler::nif]
fn store_read_chunk_verified_submit<'a>(
    env: Env<'a>,
    store: ResourceArc<BlobStoreResource>,
    request_ref: Term<'a>,
    hash_bytes: Binary<'a>,
    tier: String,
    verify: bool,
) -> Atom {
    let hash_bytes = hash_bytes.as_slice().to_vec();
    submit_blob(store.store.clone(), env, request_ref, move |store| {
        let hash = hash_from_bytes(&hash_bytes)?;
        let tier = parse_tier(&tier)?;

        let options = ReadOptions {
            verify,
            decompress: false,
            compression: None,
            encryption: None,
        };

        store
            .read_chunk_with_options(&hash, tier, &options)
            .map(BlobReply::Data)
            .map_err(|e| e.to_string())
    })
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
/// A request tag (integer); the chunk data arrives as a
/// `{tag, {:ok, binary} | {:error, reason}}` message.
/// Codec locator tuple passed as a single arg from Elixir to avoid inflating
/// arity on NIFs that already carry a lot of positional state.
/// `(compression_kind, compression_level, key, nonce)` — empty strings/binaries
/// mean "not applicable".
type CodecLocator<'a> = (String, i32, Binary<'a>, Binary<'a>);

#[allow(clippy::too_many_arguments)]
#[rustler::nif]
fn store_read_chunk_with_options_submit<'a>(
    env: Env<'a>,
    store: ResourceArc<BlobStoreResource>,
    request_ref: Term<'a>,
    hash_bytes: Binary<'a>,
    tier: String,
    verify: bool,
    decompress: bool,
    codec: CodecLocator<'a>,
) -> Atom {
    let hash_bytes = hash_bytes.as_slice().to_vec();
    let (compression, compression_level, key, nonce) = codec;
    let key = key.as_slice().to_vec();
    let nonce = nonce.as_slice().to_vec();
    submit_blob(store.store.clone(), env, request_ref, move |store| {
        let hash = hash_from_bytes(&hash_bytes)?;
        let tier = parse_tier(&tier)?;
        let compression = parse_optional_compression(&compression, compression_level)?;
        let encryption = encryption_from_bytes(&key, &nonce)?;

        let options = ReadOptions {
            verify,
            decompress,
            compression,
            encryption,
        };

        store
            .read_chunk_with_options(&hash, tier, &options)
            .map(BlobReply::Data)
            .map_err(|e| e.to_string())
    })
}

/// Deletes a chunk from the blob store.
///
/// # Arguments
/// * `store` - Resource reference to the blob store.
/// * `hash` - 32-byte binary hash of the chunk.
/// * `tier` - Storage tier ("hot", "warm", or "cold").
///
/// # Returns
/// `:ok`; the bytes freed arrive as a
/// `{request_ref, {:ok, bytes_freed} | {:error, reason}}` message (#1485).
#[allow(clippy::too_many_arguments)]
#[rustler::nif]
fn store_delete_chunk_submit<'a>(
    env: Env<'a>,
    store: ResourceArc<BlobStoreResource>,
    request_ref: Term<'a>,
    hash_bytes: Binary<'a>,
    tier: String,
    compression: String,
    compression_level: i32,
    key_binary: Binary<'a>,
    nonce_binary: Binary<'a>,
) -> Atom {
    let hash_bytes = hash_bytes.as_slice().to_vec();
    let key_binary = key_binary.as_slice().to_vec();
    let nonce_binary = nonce_binary.as_slice().to_vec();
    submit_blob(store.store.clone(), env, request_ref, move |store| {
        let hash = hash_from_bytes(&hash_bytes)?;
        let tier = parse_tier(&tier)?;
        let encryption = encryption_from_bytes(&key_binary, &nonce_binary)?;
        let compression = parse_optional_compression(&compression, compression_level)?;

        store
            .delete_chunk(&hash, tier, compression.as_ref(), encryption.as_ref())
            .map(BlobReply::Size)
            .map_err(|e| e.to_string())
    })
}

/// Deletes every codec variant of a chunk at a tier.
///
/// Intended for orphan cleanup paths that don't track codec settings.
///
/// Returns `:ok`; the total bytes freed across all variants arrive as a
/// `{request_ref, {:ok, bytes_freed} | {:error, reason}}` message.
#[rustler::nif]
fn store_delete_chunk_any_codec_submit<'a>(
    env: Env<'a>,
    store: ResourceArc<BlobStoreResource>,
    request_ref: Term<'a>,
    hash_bytes: Binary<'a>,
    tier: String,
) -> Atom {
    let hash_bytes = hash_bytes.as_slice().to_vec();
    submit_blob(store.store.clone(), env, request_ref, move |store| {
        let hash = hash_from_bytes(&hash_bytes)?;
        let tier = parse_tier(&tier)?;
        store
            .delete_chunk_any_codec(&hash, tier)
            .map(BlobReply::Size)
            .map_err(|e| e.to_string())
    })
}

/// Checks if a specific codec variant of a chunk exists in the blob store.
///
/// Returns `:ok`; the result arrives as a
/// `{request_ref, {:ok, boolean} | {:error, reason}}` message.
#[allow(clippy::too_many_arguments)]
#[rustler::nif]
fn store_chunk_exists_submit<'a>(
    env: Env<'a>,
    store: ResourceArc<BlobStoreResource>,
    request_ref: Term<'a>,
    hash_bytes: Binary<'a>,
    tier: String,
    compression: String,
    compression_level: i32,
    key_binary: Binary<'a>,
    nonce_binary: Binary<'a>,
) -> Atom {
    let hash_bytes = hash_bytes.as_slice().to_vec();
    let key_binary = key_binary.as_slice().to_vec();
    let nonce_binary = nonce_binary.as_slice().to_vec();
    submit_blob(store.store.clone(), env, request_ref, move |store| {
        let hash = hash_from_bytes(&hash_bytes)?;
        let tier = parse_tier(&tier)?;
        let encryption = encryption_from_bytes(&key_binary, &nonce_binary)?;
        let compression = parse_optional_compression(&compression, compression_level)?;

        Ok(BlobReply::Bool(store.chunk_exists(
            &hash,
            tier,
            compression.as_ref(),
            encryption.as_ref(),
        )))
    })
}

/// Checks whether any codec variant of a chunk exists at a tier.
///
/// Returns `:ok`; the result arrives as a
/// `{request_ref, {:ok, boolean} | {:error, reason}}` message.
#[rustler::nif]
fn store_chunk_exists_any_codec_submit<'a>(
    env: Env<'a>,
    store: ResourceArc<BlobStoreResource>,
    request_ref: Term<'a>,
    hash_bytes: Binary<'a>,
    tier: String,
) -> Atom {
    let hash_bytes = hash_bytes.as_slice().to_vec();
    submit_blob(store.store.clone(), env, request_ref, move |store| {
        let hash = hash_from_bytes(&hash_bytes)?;
        let tier = parse_tier(&tier)?;
        store
            .chunk_exists_any_codec(&hash, tier)
            .map(BlobReply::Bool)
            .map_err(|e| e.to_string())
    })
}

/// Returns the on-disk size of any codec variant of the chunk, or `0` if no
/// variant exists. Use together with `store_chunk_exists_any_codec` to
/// distinguish "missing" from "zero-byte".
///
/// Returns `:ok`; the result arrives as a
/// `{request_ref, {:ok, size} | {:error, reason}}` message.
#[rustler::nif]
fn store_chunk_any_codec_size_submit<'a>(
    env: Env<'a>,
    store: ResourceArc<BlobStoreResource>,
    request_ref: Term<'a>,
    hash_bytes: Binary<'a>,
    tier: String,
) -> Atom {
    let hash_bytes = hash_bytes.as_slice().to_vec();
    submit_blob(store.store.clone(), env, request_ref, move |store| {
        let hash = hash_from_bytes(&hash_bytes)?;
        let tier = parse_tier(&tier)?;
        store
            .chunk_any_codec_size(&hash, tier)
            .map(|opt| BlobReply::Size(opt.unwrap_or(0)))
            .map_err(|e| e.to_string())
    })
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
/// * `codec` - `(compression, compression_level, key, nonce)` locator; empty
///   key/nonce mean no encryption.
///
/// # Returns
/// `:ok`; the outcome arrives as a
/// `{request_ref, {:ok, {}} | {:error, reason}}` message (#1486).
#[rustler::nif]
fn store_migrate_chunk_submit<'a>(
    env: Env<'a>,
    store: ResourceArc<BlobStoreResource>,
    request_ref: Term<'a>,
    hash_bytes: Binary<'a>,
    from_tier: String,
    to_tier: String,
    codec: CodecLocator<'a>,
) -> Atom {
    let hash_bytes = hash_bytes.as_slice().to_vec();
    let (compression, compression_level, key, nonce) = codec;
    let key = key.as_slice().to_vec();
    let nonce = nonce.as_slice().to_vec();
    submit_blob(store.store.clone(), env, request_ref, move |store| {
        let hash = hash_from_bytes(&hash_bytes)?;
        let from = parse_tier(&from_tier)?;
        let to = parse_tier(&to_tier)?;
        let encryption = encryption_from_bytes(&key, &nonce)?;
        let compression = parse_optional_compression(&compression, compression_level)?;

        store
            .migrate_chunk(&hash, from, to, compression.as_ref(), encryption.as_ref())
            .map(|()| BlobReply::Unit)
            .map_err(|e| e.to_string())
    })
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
/// `:ok`; the stored size of the re-encrypted chunk arrives as a
/// `{request_ref, {:ok, size} | {:error, reason}}` message (#1486).
#[allow(clippy::too_many_arguments)]
#[rustler::nif]
fn store_reencrypt_chunk_submit<'a>(
    env: Env<'a>,
    store: ResourceArc<BlobStoreResource>,
    request_ref: Term<'a>,
    hash_bytes: Binary<'a>,
    tier: String,
    compression: (String, i32),
    old_enc: (Binary<'a>, Binary<'a>),
    new_enc: (Binary<'a>, Binary<'a>),
) -> Atom {
    let hash_bytes = hash_bytes.as_slice().to_vec();
    let (comp_str, comp_level) = compression;
    let old_key = old_enc.0.as_slice().to_vec();
    let old_nonce = old_enc.1.as_slice().to_vec();
    let new_key = new_enc.0.as_slice().to_vec();
    let new_nonce = new_enc.1.as_slice().to_vec();
    submit_blob(store.store.clone(), env, request_ref, move |store| {
        let hash = hash_from_bytes(&hash_bytes)?;
        let tier = parse_tier(&tier)?;
        let compression = parse_optional_compression(&comp_str, comp_level)?;
        let old_params = required_encryption_from_bytes(&old_key, &old_nonce)?;
        let new_params = required_encryption_from_bytes(&new_key, &new_nonce)?;

        store
            .reencrypt_chunk(&hash, tier, compression.as_ref(), &old_params, &new_params)
            .map(|size| BlobReply::Size(size as u64))
            .map_err(|e| e.to_string())
    })
}

/// Parses a 32-byte binary into a Hash.
fn parse_hash(hash_bytes: &Binary) -> Result<Hash, String> {
    hash_from_bytes(hash_bytes.as_slice())
}

/// Slice-based hash parse, callable from an async blob task where the caller's
/// `Binary` has already been copied into an owned buffer (#1484).
fn hash_from_bytes(bytes: &[u8]) -> Result<Hash, String> {
    if bytes.len() != 32 {
        return Err(format!(
            "invalid hash length: expected 32 bytes, got {}",
            bytes.len()
        ));
    }
    let mut hash = [0u8; 32];
    hash.copy_from_slice(bytes);
    Ok(Hash::from_bytes(hash))
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

/// Slice-based encryption-param parse, callable from an async blob task where
/// the caller's key/nonce `Binary`s have been copied into owned buffers.
/// Empty key and nonce mean no encryption.
fn encryption_from_bytes(key: &[u8], nonce: &[u8]) -> Result<Option<EncryptionParams>, String> {
    if key.is_empty() && nonce.is_empty() {
        return Ok(None);
    }
    EncryptionParams::new(key, nonce)
        .map(Some)
        .map_err(|e| e.to_string())
}

/// Slice-based required-encryption parse (non-empty key and nonce), callable
/// from an async blob task where the key/nonce have been copied into owned
/// buffers (#1486).
fn required_encryption_from_bytes(key: &[u8], nonce: &[u8]) -> Result<EncryptionParams, String> {
    EncryptionParams::new(key, nonce).map_err(|e| e.to_string())
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
#[rustler::nif(schedule = "DirtyCpu")]
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
#[rustler::nif(schedule = "DirtyCpu")]
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
#[rustler::nif(schedule = "DirtyCpu")]
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
#[rustler::nif(schedule = "DirtyCpu")]
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
#[rustler::nif(schedule = "DirtyCpu")]
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
#[rustler::nif(schedule = "DirtyIo")]
fn metadata_write(
    store: ResourceArc<BlobStoreResource>,
    segment_id_hex: String,
    key_hash_bytes: Binary,
    data: Binary,
) -> Result<(), String> {
    let key_hash = parse_hash(&key_hash_bytes)?;

    let store = &store.store;
    store
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
#[rustler::nif(schedule = "DirtyIo")]
fn metadata_read<'a>(
    env: Env<'a>,
    store: ResourceArc<BlobStoreResource>,
    segment_id_hex: String,
    key_hash_bytes: Binary,
) -> Result<Binary<'a>, String> {
    let key_hash = parse_hash(&key_hash_bytes)?;

    let store = &store.store;
    let data = store
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
#[rustler::nif(schedule = "DirtyIo")]
fn metadata_delete(
    store: ResourceArc<BlobStoreResource>,
    segment_id_hex: String,
    key_hash_bytes: Binary,
) -> Result<(), String> {
    let key_hash = parse_hash(&key_hash_bytes)?;

    let store = &store.store;
    store
        .delete_metadata(&segment_id_hex, &key_hash)
        .map_err(|e| e.to_string())
}

/// Lists all metadata key hashes in a segment.
///
/// # Arguments
/// * `env` - Rustler environment.
/// * `store` - Resource reference to the blob store.
/// * `segment_id_hex` - 64-char hex string identifying the metadata segment.
#[rustler::nif(schedule = "DirtyIo")]
fn metadata_list_segment<'a>(
    env: Env<'a>,
    store: ResourceArc<BlobStoreResource>,
    segment_id_hex: String,
) -> Result<Vec<Binary<'a>>, String> {
    let store = &store.store;
    let hashes = store
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
/// `:ok`; the capacity arrives as a `{request_ref, {:ok, {total_bytes,
/// available_bytes, used_bytes}} | {:error, reason}}` message (#1486).
#[rustler::nif]
fn filesystem_info_submit<'a>(env: Env<'a>, request_ref: Term<'a>, path: String) -> Atom {
    spawn_reply(env, request_ref, move || {
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

        Ok(BlobReply::IntTriple(
            total_bytes,
            available_bytes,
            used_bytes,
        ))
    })
}

/// Open `path` (a directory) and fsync it so directory-entry changes —
/// such as a rename into it — are durable. The BEAM's `:file.open/2`
/// refuses directories with `:eisdir`, so callers that need to flush a
/// directory after an atomic rename route through this NIF.
///
/// # Returns
/// `:ok`; the outcome arrives as a
/// `{request_ref, {:ok, {}} | {:error, reason}}` message (#1486).
#[rustler::nif]
fn fsync_dir_submit<'a>(env: Env<'a>, request_ref: Term<'a>, path: String) -> Atom {
    spawn_reply(env, request_ref, move || {
        let dir =
            std::fs::File::open(&path).map_err(|e| format!("open '{}' failed: {}", path, e))?;
        dir.sync_all()
            .map(|()| BlobReply::Unit)
            .map_err(|e| format!("fsync '{}' failed: {}", path, e))
    })
}

/// Look up a key in an index tree (#781) backed by the volume's
/// BlobStore (#813). `root_hash` is a 32-byte chunk hash, or an
/// empty binary for a tree that has never been written. `tier` is
/// the storage tier the tree's nodes live in (typically "hot").
///
/// Returns `{:ok, value}` (with `value` either a binary or `nil`)
/// or `{:error, reason}`. `nil` means the key is absent or
/// tombstoned.
#[rustler::nif(schedule = "DirtyIo")]
fn index_tree_get<'a>(
    env: Env<'a>,
    store: ResourceArc<BlobStoreResource>,
    root_hash: Binary,
    tier: String,
    key: Binary,
) -> Result<Option<Binary<'a>>, String> {
    let parsed_tier = parse_tier(&tier)?;
    let store = &store.store;

    let adapter = BlobStoreChunkStore::new(store, parsed_tier);
    let tree = IndexTree::new(adapter, TreeConfig::default());

    let root = parse_optional_root(&root_hash)?;

    match tree
        .get(root.as_ref(), key.as_slice())
        .map_err(|e| e.to_string())?
    {
        None => Ok(None),
        Some(value) => Ok(Some(vec_to_binary(env, &value))),
    }
}

/// Range query in an index tree (#781). `start_key` / `end_key`
/// are inclusive-start, exclusive-end; an empty binary on either
/// side means "open-ended" in that direction. Tombstones are
/// filtered out.
///
/// Returns `{:ok, [{key, value}, ...]}` in ascending key order, or
/// `{:error, reason}`.
#[rustler::nif(schedule = "DirtyIo")]
fn index_tree_range<'a>(
    env: Env<'a>,
    store: ResourceArc<BlobStoreResource>,
    root_hash: Binary,
    tier: String,
    start_key: Binary,
    end_key: Binary,
) -> Result<Vec<(Binary<'a>, Binary<'a>)>, String> {
    let parsed_tier = parse_tier(&tier)?;
    let store = &store.store;

    let adapter = BlobStoreChunkStore::new(store, parsed_tier);
    let tree = IndexTree::new(adapter, TreeConfig::default());

    let root = parse_optional_root(&root_hash)?;

    let entries = tree
        .range(root.as_ref(), start_key.as_slice(), end_key.as_slice())
        .map_err(|e| e.to_string())?;

    let mut out = Vec::with_capacity(entries.len());
    for (k, v) in entries {
        out.push((vec_to_binary(env, &k), vec_to_binary(env, &v)));
    }
    Ok(out)
}

/// Insert or replace `key`'s value in an index tree (#781) backed
/// by the volume's BlobStore (#813). Empty `root_hash` creates a
/// new tree. The IndexTree is copy-on-write — every put produces a
/// new root chunk plus the rewritten leaf→root path.
///
/// Returns `{new_root_hash, [{chunk_hash, chunk_bytes}]}` — the new
/// root plus the copy-on-write node chunks this op wrote, so the
/// caller can replicate them to the volume's other metadata drives
/// (#903).
#[rustler::nif(schedule = "DirtyIo")]
fn index_tree_put<'a>(
    env: Env<'a>,
    store: ResourceArc<BlobStoreResource>,
    root_hash: Binary,
    tier: String,
    key: Binary,
    value: Binary,
) -> Result<(Binary<'a>, Vec<(Binary<'a>, Binary<'a>)>), String> {
    let parsed_tier = parse_tier(&tier)?;
    let store = &store.store;

    let adapter = BlobStoreChunkStore::new(store, parsed_tier);
    let mut tree = IndexTree::new(adapter, TreeConfig::default());

    let root = parse_optional_root(&root_hash)?;

    let new_root = tree
        .put(
            root.as_ref(),
            key.as_slice().to_vec(),
            value.as_slice().to_vec(),
        )
        .map_err(|e| e.to_string())?;

    let written = written_nodes_to_binaries(env, tree.store.take_written());
    Ok((vec_to_binary(env, new_root.as_bytes()), written))
}

/// Tombstone `key` in an index tree. Even on a never-written tree
/// (`root_hash == <<>>`) this writes a tombstone so anti-entropy
/// can replicate the delete.
///
/// Returns `{new_root_hash, [{chunk_hash, chunk_bytes}]}` — see
/// `index_tree_put`.
#[rustler::nif(schedule = "DirtyIo")]
fn index_tree_delete<'a>(
    env: Env<'a>,
    store: ResourceArc<BlobStoreResource>,
    root_hash: Binary,
    tier: String,
    key: Binary,
) -> Result<(Binary<'a>, Vec<(Binary<'a>, Binary<'a>)>), String> {
    let parsed_tier = parse_tier(&tier)?;
    let store = &store.store;

    let adapter = BlobStoreChunkStore::new(store, parsed_tier);
    let mut tree = IndexTree::new(adapter, TreeConfig::default());

    let root = parse_optional_root(&root_hash)?;

    let new_root = tree
        .delete(root.as_ref(), key.as_slice())
        .map_err(|e| e.to_string())?;

    let written = written_nodes_to_binaries(env, tree.store.take_written());
    Ok((vec_to_binary(env, new_root.as_bytes()), written))
}

/// Reap tombstones whose `deleted_at` is older than
/// `before_unix_nanos`. Walks every leaf and rewrites those that
/// changed. Returns the new root hash (equal to the input if
/// nothing was reaped) plus the rewritten node chunks — see
/// `index_tree_put`.
///
/// Errors if the input root_hash is `<<>>` — there's nothing to
/// purge from a tree that was never written.
#[rustler::nif(schedule = "DirtyIo")]
fn index_tree_purge_tombstones<'a>(
    env: Env<'a>,
    store: ResourceArc<BlobStoreResource>,
    root_hash: Binary,
    tier: String,
    before_unix_nanos: u64,
) -> Result<(Binary<'a>, Vec<(Binary<'a>, Binary<'a>)>), String> {
    if root_hash.is_empty() {
        return Err("cannot purge tombstones on an empty tree (root_hash is <<>>)".to_string());
    }

    let parsed_tier = parse_tier(&tier)?;
    let store = &store.store;

    let adapter = BlobStoreChunkStore::new(store, parsed_tier);
    let mut tree = IndexTree::new(adapter, TreeConfig::default());

    let root = parse_hash(&root_hash)?;

    let before = std::time::UNIX_EPOCH + std::time::Duration::from_nanos(before_unix_nanos);

    let new_root = tree
        .purge_tombstones(&root, before)
        .map_err(|e| e.to_string())?;

    let written = written_nodes_to_binaries(env, tree.store.take_written());
    Ok((vec_to_binary(env, new_root.as_bytes()), written))
}

/// Encodes the adapter's drained write set as `[{hash, bytes}]`
/// binaries for the return to Elixir.
fn written_nodes_to_binaries<'a>(
    env: Env<'a>,
    written: Vec<(crate::hash::Hash, Vec<u8>)>,
) -> Vec<(Binary<'a>, Binary<'a>)> {
    written
        .into_iter()
        .map(|(hash, bytes)| {
            (
                vec_to_binary(env, hash.as_bytes()),
                vec_to_binary(env, &bytes),
            )
        })
        .collect()
}

/// Parses a 0-byte or 32-byte binary into `Option<Hash>`. The
/// 0-byte form represents a never-written tree (the IndexTree's
/// `None` root).
fn parse_optional_root(root_hash: &Binary) -> Result<Option<Hash>, String> {
    if root_hash.is_empty() {
        Ok(None)
    } else {
        parse_hash(root_hash).map(Some)
    }
}

fn vec_to_binary<'a>(env: Env<'a>, bytes: &[u8]) -> Binary<'a> {
    let mut out = NewBinary::new(env, bytes.len());
    out.copy_from_slice(bytes);
    out.into()
}

/// List every node chunk hash reachable from `root_hash` — both
/// internal-page chunks and leaf-page chunks. Empty `root_hash`
/// returns `{:ok, []}`. Used by the per-volume anti-entropy runner
/// (#955) so index-tree pages are enumerated alongside data chunks.
#[rustler::nif(schedule = "DirtyIo")]
fn index_tree_list_referenced_chunks<'a>(
    env: Env<'a>,
    store: ResourceArc<BlobStoreResource>,
    root_hash: Binary,
    tier: String,
) -> Result<Vec<Binary<'a>>, String> {
    let parsed_tier = parse_tier(&tier)?;
    let store = &store.store;

    let adapter = BlobStoreChunkStore::new(store, parsed_tier);
    let tree = IndexTree::new(adapter, TreeConfig::default());

    let root = parse_optional_root(&root_hash)?;

    let hashes = tree
        .list_referenced_chunks(root.as_ref())
        .map_err(|e| e.to_string())?;

    let mut out = Vec::with_capacity(hashes.len());
    for hash in hashes {
        out.push(vec_to_binary(env, hash.as_bytes()));
    }
    Ok(out)
}

rustler::init!("Elixir.NeonFS.Core.Blob.Native");
