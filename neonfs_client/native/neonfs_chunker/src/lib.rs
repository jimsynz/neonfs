//! Rustler NIF for client-side chunking.
//!
//! Exposes the chunker surface that `NeonFS.Client.ChunkWriter` uses to
//! split a byte stream into content-addressed chunks before shipping each
//! chunk to a data-plane replica (see #408, #449). The chunker and hash
//! modules are byte-for-byte copies of
//! `neonfs_core/native/neonfs_blob/src/{chunking,hash}.rs` — dedup across
//! packages relies on identical `(hash, offset, size)` output for the
//! same input, and the parity property test in `neonfs_client/test/`
//! keeps the two copies honest. Deduplicating the source into a shared
//! crate is explicitly deferred per #449.

pub mod chunking;
pub mod hash;

use crate::chunking::{auto_strategy, chunk_data, ChunkResult, ChunkStrategy, IncrementalChunker};
use rustler::{Binary, Env, NewBinary, Resource, ResourceArc};
use std::sync::Mutex;

/// Each chunk is a tuple of `(data, hash, offset, size)`.
type ChunkResultTuple<'a> = (Binary<'a>, Binary<'a>, usize, usize);

/// Computes SHA-256 hash of the given binary data. Returns a 32-byte
/// binary. Matches `NeonFS.Core.Blob.Native.compute_hash/1` — kept in
/// the client surface so callers on interface nodes do not need a
/// round-trip to core just to hash.
#[rustler::nif]
fn compute_hash<'a>(env: Env<'a>, data: Binary) -> Binary<'a> {
    let h = hash::sha256(data.as_slice());
    let mut out = NewBinary::new(env, 32);
    out.copy_from_slice(h.as_bytes());
    out.into()
}

/// Determines the appropriate chunking strategy for the given data
/// length. See `chunking::auto_strategy` for the threshold rules.
#[rustler::nif]
fn nif_auto_chunk_strategy(data_len: usize) -> (String, usize) {
    match auto_strategy(data_len) {
        ChunkStrategy::Single => ("single".to_string(), 0),
        ChunkStrategy::Fixed { size } => ("fixed".to_string(), size),
        ChunkStrategy::FastCDC { avg, .. } => ("fastcdc".to_string(), avg),
    }
}

/// Stateless chunker — splits `data` into chunks according to
/// `(strategy, strategy_param)` and returns `[{data, hash, offset,
/// size}, ...]`.
#[rustler::nif]
fn nif_chunk_data<'a>(
    env: Env<'a>,
    data: Binary,
    strategy: String,
    strategy_param: usize,
) -> Result<Vec<ChunkResultTuple<'a>>, String> {
    let chunk_strategy = parse_chunk_strategy(&strategy, strategy_param)?;
    Ok(chunks_to_tuples(
        env,
        chunk_data(data.as_slice(), &chunk_strategy),
    ))
}

/// Resource wrapping an `IncrementalChunker` so it can be passed back
/// and forth between Elixir calls. The mutex protects state mutated
/// across feed/finish calls; lock contention is irrelevant in normal
/// use because callers own the chunker and call it serially.
pub struct ChunkerResource {
    inner: Mutex<IncrementalChunker>,
}

#[rustler::resource_impl]
impl Resource for ChunkerResource {}

/// Creates an incremental chunker for the given strategy.
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

/// Feeds a slice of data into the chunker and returns any complete
/// chunks that became available. Bytes that may still belong to a
/// future chunk remain buffered inside the resource.
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

/// Flushes any remaining buffered data as the final chunks. The
/// chunker may be reused afterwards; offsets continue from where they
/// left off.
#[rustler::nif]
fn chunker_finish<'a>(
    env: Env<'a>,
    chunker: ResourceArc<ChunkerResource>,
) -> Vec<ChunkResultTuple<'a>> {
    let mut state = chunker.inner.lock().expect("chunker mutex poisoned");
    let chunks = state.finish();
    chunks_to_tuples(env, chunks)
}

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
        "auto" => Ok(auto_strategy(param)),
        "fastcdc" => {
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

rustler::init!("Elixir.NeonFS.Client.Chunker.Native");
