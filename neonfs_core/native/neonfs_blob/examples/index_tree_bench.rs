//! Establishes baseline read / write latency for `IndexTree` (#781).
//!
//! Run with:
//!
//!     cargo run --release --example index_tree_bench
//!
//! Defaults to 1 000 000 entries with random 16-byte keys and
//! 32-byte values. Override either via the env vars `BENCH_ENTRIES`
//! and `BENCH_VALUE_BYTES`. The bench uses the in-memory chunk store
//! so it measures the tree algorithm itself, not the production
//! BlobStore I/O path.

use neonfs_blob::index_tree::{InMemoryChunkStore, IndexTree, TreeConfig};
use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};
use std::env;
use std::time::Instant;

fn parse_env(name: &str, default: usize) -> usize {
    env::var(name)
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}

fn main() {
    let entries = parse_env("BENCH_ENTRIES", 1_000_000);
    let value_bytes = parse_env("BENCH_VALUE_BYTES", 32);
    let mut rng = StdRng::seed_from_u64(0xC0FFEE);

    println!("=== IndexTree benchmark ===");
    println!(
        "entries={} value_bytes={} (key size = 16 bytes)",
        entries, value_bytes
    );

    // Pre-generate keys so the timer doesn't include rand cost.
    let mut keys: Vec<[u8; 16]> = Vec::with_capacity(entries);
    let value: Vec<u8> = (0..value_bytes).map(|_| rng.random()).collect();
    for _ in 0..entries {
        let k: [u8; 16] = rng.random();
        keys.push(k);
    }

    let mut tree = IndexTree::new(InMemoryChunkStore::new(), TreeConfig::default());
    let mut root = None;

    // --- write phase ---
    let t0 = Instant::now();
    for k in &keys {
        root = Some(tree.put(root.as_ref(), k.to_vec(), value.clone()).unwrap());
    }
    let write_elapsed = t0.elapsed();
    let write_ns = write_elapsed.as_nanos() as f64 / entries as f64;
    println!(
        "write: {:>10} entries in {:>8.2?}  →  {:>8.0} ns/op  ({:>7.0} ops/s)  chunks={}",
        entries,
        write_elapsed,
        write_ns,
        1.0e9 / write_ns,
        tree.store.len()
    );

    // --- read phase: 100K random gets ---
    let read_n = entries.min(100_000);
    let t0 = Instant::now();
    for i in 0..read_n {
        let k = &keys[i * (entries / read_n.max(1)) % entries];
        let _ = tree.get(root.as_ref(), k).unwrap();
    }
    let read_elapsed = t0.elapsed();
    let read_ns = read_elapsed.as_nanos() as f64 / read_n as f64;
    println!(
        "read:  {:>10} gets    in {:>8.2?}  →  {:>8.0} ns/op  ({:>7.0} ops/s)",
        read_n,
        read_elapsed,
        read_ns,
        1.0e9 / read_ns
    );
}
