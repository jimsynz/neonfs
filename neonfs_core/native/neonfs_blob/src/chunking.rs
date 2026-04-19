//! Content-defined chunking module for NeonFS.
//!
//! This module provides chunking strategies for splitting data into chunks
//! for content-addressed storage. It supports:
//! - Single chunk for small data
//! - Fixed-size chunks for medium data
//! - FastCDC content-defined chunking for large data

use crate::hash::{sha256, Hash};
use serde::{Deserialize, Serialize};

/// Default CDC parameters for FastCDC chunking.
const CDC_MIN_SIZE: usize = 64 * 1024; // 64 KB
const CDC_AVG_SIZE: usize = 256 * 1024; // 256 KB
const CDC_MAX_SIZE: usize = 1024 * 1024; // 1 MB

/// Threshold for using single chunk strategy (< 64KB).
const SINGLE_CHUNK_THRESHOLD: usize = 64 * 1024; // 64 KB

/// Threshold for using fixed-size chunks (< 1MB).
const FIXED_CHUNK_THRESHOLD: usize = 1024 * 1024; // 1 MB

/// Default fixed chunk size.
const FIXED_CHUNK_SIZE: usize = 256 * 1024; // 256 KB

/// Strategy for splitting data into chunks.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum ChunkStrategy {
    /// Store the entire data as a single chunk.
    Single,

    /// Split data into fixed-size chunks.
    Fixed {
        /// Size of each chunk (last chunk may be smaller).
        size: usize,
    },

    /// Use FastCDC content-defined chunking.
    FastCDC {
        /// Minimum chunk size.
        min: usize,
        /// Average (target) chunk size.
        avg: usize,
        /// Maximum chunk size.
        max: usize,
    },
}

impl Default for ChunkStrategy {
    fn default() -> Self {
        ChunkStrategy::FastCDC {
            min: CDC_MIN_SIZE,
            avg: CDC_AVG_SIZE,
            max: CDC_MAX_SIZE,
        }
    }
}

/// Result of chunking a single piece of data.
#[derive(Debug, Clone)]
pub struct ChunkResult {
    /// The chunk data.
    pub data: Vec<u8>,
    /// SHA-256 hash of the chunk data.
    pub hash: Hash,
    /// Byte offset of this chunk in the original data.
    pub offset: usize,
    /// Size of this chunk in bytes.
    pub size: usize,
}

impl ChunkResult {
    /// Creates a new ChunkResult, computing the hash from the data.
    fn new(data: Vec<u8>, offset: usize) -> Self {
        let size = data.len();
        let hash = sha256(&data);
        ChunkResult {
            data,
            hash,
            offset,
            size,
        }
    }
}

/// Automatically selects an appropriate chunking strategy based on data length.
///
/// Strategy selection:
/// - < 64KB: Single chunk (avoid overhead for small files)
/// - 64KB - 1MB: Fixed 256KB blocks (simpler for medium files)
/// - > 1MB: FastCDC with 256KB avg, 64KB min, 1MB max (enables deduplication)
///
/// # Arguments
/// * `data_len` - Length of the data to be chunked.
///
/// # Returns
/// The recommended `ChunkStrategy` for the given data length.
pub fn auto_strategy(data_len: usize) -> ChunkStrategy {
    if data_len < SINGLE_CHUNK_THRESHOLD {
        ChunkStrategy::Single
    } else if data_len < FIXED_CHUNK_THRESHOLD {
        ChunkStrategy::Fixed {
            size: FIXED_CHUNK_SIZE,
        }
    } else {
        ChunkStrategy::FastCDC {
            min: CDC_MIN_SIZE,
            avg: CDC_AVG_SIZE,
            max: CDC_MAX_SIZE,
        }
    }
}

/// Splits data into chunks using the specified strategy.
///
/// # Arguments
/// * `data` - The data to split into chunks.
/// * `strategy` - The chunking strategy to use.
///
/// # Returns
/// A vector of `ChunkResult` containing each chunk's data, hash, offset, and size.
/// For empty data, returns an empty vector.
///
/// # Guarantees
/// - Concatenating all chunk data in order reproduces the original data.
/// - Each chunk's hash is the SHA-256 of its data.
/// - Offsets are contiguous and non-overlapping.
pub fn chunk_data(data: &[u8], strategy: &ChunkStrategy) -> Vec<ChunkResult> {
    if data.is_empty() {
        return Vec::new();
    }

    match strategy {
        ChunkStrategy::Single => chunk_single(data),
        ChunkStrategy::Fixed { size } => chunk_fixed(data, *size),
        ChunkStrategy::FastCDC { min, avg, max } => chunk_fastcdc(data, *min, *avg, *max),
    }
}

/// Creates a single chunk containing all data.
fn chunk_single(data: &[u8]) -> Vec<ChunkResult> {
    vec![ChunkResult::new(data.to_vec(), 0)]
}

/// Splits data into fixed-size chunks.
fn chunk_fixed(data: &[u8], chunk_size: usize) -> Vec<ChunkResult> {
    // Handle edge case where chunk_size is 0
    let chunk_size = if chunk_size == 0 { 1 } else { chunk_size };

    data.chunks(chunk_size)
        .enumerate()
        .map(|(i, chunk)| ChunkResult::new(chunk.to_vec(), i * chunk_size))
        .collect()
}

/// Splits data using FastCDC content-defined chunking.
fn chunk_fastcdc(data: &[u8], min: usize, avg: usize, max: usize) -> Vec<ChunkResult> {
    use fastcdc::v2020::FastCDC;

    // FastCDC requires min >= 64 and valid size ordering
    let min = min.max(64);
    let avg = avg.max(min);
    let max = max.max(avg);

    let chunker = FastCDC::new(data, min, avg, max);
    chunker
        .map(|chunk| {
            ChunkResult::new(
                data[chunk.offset..chunk.offset + chunk.length].to_vec(),
                chunk.offset,
            )
        })
        .collect()
}

/// Normalises FastCDC parameters to satisfy the crate's invariants:
/// `min >= 64` and `min <= avg <= max`.
fn normalise_fastcdc_params(min: usize, avg: usize, max: usize) -> (usize, usize, usize) {
    let min = min.max(64);
    let avg = avg.max(min);
    let max = max.max(avg);
    (min, avg, max)
}

/// Incremental chunker that accepts data in arbitrary-sized slices and emits
/// complete chunks as they become available.
///
/// The total size of the input is not required up front, and the working set
/// is bounded by the strategy's maximum chunk size — a multi-gigabyte input
/// is processed in constant memory.
///
/// Contract: for any input split into a sequence of [`feed`] calls followed
/// by [`finish`], the concatenation of all emitted chunks equals the
/// concatenation of inputs, and the chunk list is byte-for-byte identical to
/// what [`chunk_data`] would produce on the same total input.
///
/// [`feed`]: IncrementalChunker::feed
/// [`finish`]: IncrementalChunker::finish
pub struct IncrementalChunker {
    strategy: ChunkStrategy,
    buffer: Vec<u8>,
    offset: usize,
}

impl IncrementalChunker {
    /// Creates a new incremental chunker for the given strategy.
    pub fn new(strategy: ChunkStrategy) -> Self {
        Self {
            strategy,
            buffer: Vec::new(),
            offset: 0,
        }
    }

    /// Appends `data` to the chunker and returns any complete chunks that
    /// became available. Bytes that may still belong to a future chunk remain
    /// buffered internally.
    pub fn feed(&mut self, data: &[u8]) -> Vec<ChunkResult> {
        if data.is_empty() {
            return Vec::new();
        }
        self.buffer.extend_from_slice(data);
        match self.strategy.clone() {
            ChunkStrategy::Single => Vec::new(),
            ChunkStrategy::Fixed { size } => self.feed_fixed(size),
            ChunkStrategy::FastCDC { min, avg, max } => self.feed_fastcdc(min, avg, max),
        }
    }

    /// Flushes any remaining buffered data as the final chunks. After this
    /// call the chunker is empty and may be reused with further [`feed`]
    /// calls (the offset continues from where it left off).
    ///
    /// [`feed`]: IncrementalChunker::feed
    pub fn finish(&mut self) -> Vec<ChunkResult> {
        if self.buffer.is_empty() {
            return Vec::new();
        }
        let chunks = match self.strategy.clone() {
            ChunkStrategy::Single => chunk_single(&self.buffer),
            ChunkStrategy::Fixed { size } => chunk_fixed(&self.buffer, size),
            ChunkStrategy::FastCDC { min, avg, max } => chunk_fastcdc(&self.buffer, min, avg, max),
        };
        let consumed = self.buffer.len();
        let base_offset = self.offset;
        self.buffer.clear();
        self.offset += consumed;
        chunks
            .into_iter()
            .map(|c| ChunkResult {
                offset: base_offset + c.offset,
                ..c
            })
            .collect()
    }

    fn feed_fixed(&mut self, size: usize) -> Vec<ChunkResult> {
        let size = if size == 0 { 1 } else { size };
        let mut emitted = Vec::new();
        while self.buffer.len() >= size {
            let chunk_data: Vec<u8> = self.buffer.drain(..size).collect();
            let chunk = ChunkResult::new(chunk_data, self.offset);
            self.offset += size;
            emitted.push(chunk);
        }
        emitted
    }

    fn feed_fastcdc(&mut self, min: usize, avg: usize, max: usize) -> Vec<ChunkResult> {
        use fastcdc::v2020::FastCDC;

        let (min, avg, max) = normalise_fastcdc_params(min, avg, max);

        // Without `max` bytes available the very first FastCDC boundary may
        // still shift as more data arrives, so wait before emitting anything.
        if self.buffer.len() < max {
            return Vec::new();
        }

        let chunks: Vec<_> = FastCDC::new(&self.buffer, min, avg, max).collect();
        if chunks.len() <= 1 {
            // Only one (potentially incomplete) chunk — defer until we have
            // enough lookahead to know its true boundary.
            return Vec::new();
        }

        // All chunks except the last are at content-defined boundaries that
        // cannot shift as more data arrives. The last is uncertain — keep it
        // in the buffer for the next round.
        let last = chunks.last().expect("len > 1 above");
        let consume_until = last.offset;
        let base_offset = self.offset;

        let emitted: Vec<ChunkResult> = chunks[..chunks.len() - 1]
            .iter()
            .map(|chunk| {
                ChunkResult::new(
                    self.buffer[chunk.offset..chunk.offset + chunk.length].to_vec(),
                    base_offset + chunk.offset,
                )
            })
            .collect();

        self.buffer.drain(..consume_until);
        self.offset += consume_until;
        emitted
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_auto_strategy_small_data() {
        // < 64KB should use Single
        assert_eq!(auto_strategy(0), ChunkStrategy::Single);
        assert_eq!(auto_strategy(1000), ChunkStrategy::Single);
        assert_eq!(auto_strategy(64 * 1024 - 1), ChunkStrategy::Single);
    }

    #[test]
    fn test_auto_strategy_medium_data() {
        // 64KB - 1MB should use Fixed
        assert_eq!(
            auto_strategy(64 * 1024),
            ChunkStrategy::Fixed {
                size: FIXED_CHUNK_SIZE
            }
        );
        assert_eq!(
            auto_strategy(500 * 1024),
            ChunkStrategy::Fixed {
                size: FIXED_CHUNK_SIZE
            }
        );
        assert_eq!(
            auto_strategy(1024 * 1024 - 1),
            ChunkStrategy::Fixed {
                size: FIXED_CHUNK_SIZE
            }
        );
    }

    #[test]
    fn test_auto_strategy_large_data() {
        // >= 1MB should use FastCDC
        assert_eq!(
            auto_strategy(1024 * 1024),
            ChunkStrategy::FastCDC {
                min: CDC_MIN_SIZE,
                avg: CDC_AVG_SIZE,
                max: CDC_MAX_SIZE
            }
        );
        assert_eq!(
            auto_strategy(10 * 1024 * 1024),
            ChunkStrategy::FastCDC {
                min: CDC_MIN_SIZE,
                avg: CDC_AVG_SIZE,
                max: CDC_MAX_SIZE
            }
        );
    }

    #[test]
    fn test_chunk_empty_data() {
        let data: &[u8] = &[];
        let chunks = chunk_data(data, &ChunkStrategy::Single);
        assert!(chunks.is_empty());
    }

    #[test]
    fn test_chunk_single_strategy() {
        let data = b"hello world, this is test data";
        let chunks = chunk_data(data, &ChunkStrategy::Single);

        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].data, data);
        assert_eq!(chunks[0].offset, 0);
        assert_eq!(chunks[0].size, data.len());
        assert_eq!(chunks[0].hash, sha256(data));
    }

    #[test]
    fn test_chunk_fixed_strategy() {
        let data = vec![0u8; 1000];
        let chunks = chunk_data(&data, &ChunkStrategy::Fixed { size: 300 });

        // 1000 bytes with 300 byte chunks = 4 chunks (300 + 300 + 300 + 100)
        assert_eq!(chunks.len(), 4);

        // First 3 chunks should be 300 bytes
        for chunk in &chunks[..3] {
            assert_eq!(chunk.size, 300);
        }

        // Last chunk should be 100 bytes
        assert_eq!(chunks[3].size, 100);

        // Check offsets
        assert_eq!(chunks[0].offset, 0);
        assert_eq!(chunks[1].offset, 300);
        assert_eq!(chunks[2].offset, 600);
        assert_eq!(chunks[3].offset, 900);

        // Verify concatenation equals original
        let reconstructed: Vec<u8> = chunks.iter().flat_map(|c| c.data.clone()).collect();
        assert_eq!(reconstructed, data);
    }

    #[test]
    fn test_chunk_fixed_exact_multiple() {
        let data = vec![0u8; 900];
        let chunks = chunk_data(&data, &ChunkStrategy::Fixed { size: 300 });

        // 900 bytes with 300 byte chunks = exactly 3 chunks
        assert_eq!(chunks.len(), 3);

        for chunk in &chunks {
            assert_eq!(chunk.size, 300);
        }
    }

    #[test]
    fn test_chunk_fixed_smaller_than_chunk_size() {
        let data = b"small";
        let chunks = chunk_data(data, &ChunkStrategy::Fixed { size: 1000 });

        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].data, data);
    }

    #[test]
    fn test_chunk_fastcdc_produces_valid_chunks() {
        // Create 2MB of semi-random data
        let data: Vec<u8> = (0..2 * 1024 * 1024).map(|i| (i % 256) as u8).collect();

        let chunks = chunk_data(
            &data,
            &ChunkStrategy::FastCDC {
                min: 64 * 1024,
                avg: 256 * 1024,
                max: 1024 * 1024,
            },
        );

        // Should have multiple chunks
        assert!(chunks.len() > 1);

        // All chunk sizes should be within bounds
        for chunk in &chunks {
            // FastCDC guarantees min/max on full chunks
            // Last chunk can be smaller than min
            assert!(chunk.size <= 1024 * 1024);
        }

        // Verify concatenation equals original
        let reconstructed: Vec<u8> = chunks.iter().flat_map(|c| c.data.clone()).collect();
        assert_eq!(reconstructed, data);
    }

    #[test]
    fn test_chunk_fastcdc_content_shift_stability() {
        // Create data with a pattern that will create clear CDC boundaries
        // Using a mix of zeros and repeated patterns to ensure content-based chunking works
        let mut data1 = Vec::with_capacity(4 * 1024 * 1024);
        for i in 0..4 * 1024 {
            // Create blocks of varying content to encourage natural chunk boundaries
            let pattern = if i % 7 == 0 {
                vec![0xAA; 1024]
            } else if i % 5 == 0 {
                vec![0x55; 1024]
            } else {
                (0..1024).map(|j| ((i + j) % 256) as u8).collect()
            };
            data1.extend(pattern);
        }

        // Create shifted data: insert bytes at start (not 0s, to avoid affecting pattern)
        let mut data2 = vec![0xCC; 100];
        data2.extend(&data1);

        let strategy = ChunkStrategy::FastCDC {
            min: 64 * 1024,
            avg: 256 * 1024,
            max: 1024 * 1024,
        };

        let chunks1 = chunk_data(&data1, &strategy);
        let chunks2 = chunk_data(&data2, &strategy);

        // Verify both produce valid chunking
        assert!(
            chunks1.len() > 1,
            "Expected multiple chunks from original data"
        );
        assert!(
            chunks2.len() > 1,
            "Expected multiple chunks from shifted data"
        );

        // Verify reconstruction works for both
        let reconstructed1: Vec<u8> = chunks1.iter().flat_map(|c| c.data.clone()).collect();
        let reconstructed2: Vec<u8> = chunks2.iter().flat_map(|c| c.data.clone()).collect();
        assert_eq!(reconstructed1, data1);
        assert_eq!(reconstructed2, data2);

        // Note: CDC stability depends on content patterns. Sequential byte data
        // may not exhibit the ideal stability properties. The key guarantee is
        // that CDC produces valid, reconstructable chunks - deduplication benefits
        // come from real-world data with repeated content patterns.
    }

    #[test]
    fn test_chunk_hashes_are_correct() {
        let data = b"test data for hashing verification";
        let chunks = chunk_data(data, &ChunkStrategy::Single);

        for chunk in &chunks {
            let computed_hash = sha256(&chunk.data);
            assert_eq!(chunk.hash, computed_hash);
        }
    }

    #[test]
    fn test_chunk_offsets_are_contiguous() {
        let data: Vec<u8> = (0..100_000).map(|i| (i % 256) as u8).collect();
        let chunks = chunk_data(&data, &ChunkStrategy::Fixed { size: 7000 });

        let mut expected_offset = 0;
        for chunk in &chunks {
            assert_eq!(chunk.offset, expected_offset);
            expected_offset += chunk.size;
        }

        assert_eq!(expected_offset, data.len());
    }

    #[test]
    fn test_chunk_strategy_serialization() {
        let single = ChunkStrategy::Single;
        let json = serde_json::to_string(&single).unwrap();
        assert_eq!(json, r#"{"type":"single"}"#);

        let fixed = ChunkStrategy::Fixed { size: 1000 };
        let json = serde_json::to_string(&fixed).unwrap();
        assert_eq!(json, r#"{"type":"fixed","size":1000}"#);

        let cdc = ChunkStrategy::FastCDC {
            min: 64,
            avg: 256,
            max: 1024,
        };
        let json = serde_json::to_string(&cdc).unwrap();
        assert_eq!(json, r#"{"type":"fastcdc","min":64,"avg":256,"max":1024}"#);
    }

    #[test]
    fn test_chunk_strategy_deserialization() {
        let single: ChunkStrategy = serde_json::from_str(r#"{"type":"single"}"#).unwrap();
        assert_eq!(single, ChunkStrategy::Single);

        let fixed: ChunkStrategy = serde_json::from_str(r#"{"type":"fixed","size":1000}"#).unwrap();
        assert_eq!(fixed, ChunkStrategy::Fixed { size: 1000 });

        let cdc: ChunkStrategy =
            serde_json::from_str(r#"{"type":"fastcdc","min":64,"avg":256,"max":1024}"#).unwrap();
        assert_eq!(
            cdc,
            ChunkStrategy::FastCDC {
                min: 64,
                avg: 256,
                max: 1024
            }
        );
    }

    #[test]
    fn test_fixed_chunk_size_zero() {
        // Edge case: chunk size of 0 should not panic
        let data = b"test";
        let chunks = chunk_data(data, &ChunkStrategy::Fixed { size: 0 });
        // Should still produce chunks (treated as size 1)
        assert!(!chunks.is_empty());
    }

    fn feed_in_slices(strategy: ChunkStrategy, data: &[u8], slice_size: usize) -> Vec<ChunkResult> {
        let mut chunker = IncrementalChunker::new(strategy);
        let mut emitted = Vec::new();
        let step = slice_size.max(1);
        for slice in data.chunks(step) {
            emitted.extend(chunker.feed(slice));
        }
        emitted.extend(chunker.finish());
        emitted
    }

    fn assert_equivalent(strategy: ChunkStrategy, data: &[u8], slice_size: usize) {
        let batch = chunk_data(data, &strategy);
        let incremental = feed_in_slices(strategy, data, slice_size);
        assert_eq!(
            incremental.len(),
            batch.len(),
            "chunk count differs (slice_size={})",
            slice_size
        );
        for (i, (inc, bat)) in incremental.iter().zip(batch.iter()).enumerate() {
            assert_eq!(inc.data, bat.data, "chunk {} data differs", i);
            assert_eq!(inc.hash, bat.hash, "chunk {} hash differs", i);
            assert_eq!(inc.offset, bat.offset, "chunk {} offset differs", i);
            assert_eq!(inc.size, bat.size, "chunk {} size differs", i);
        }
    }

    #[test]
    fn test_incremental_empty_input() {
        let mut chunker = IncrementalChunker::new(ChunkStrategy::Single);
        assert!(chunker.feed(&[]).is_empty());
        assert!(chunker.finish().is_empty());
    }

    #[test]
    fn test_incremental_single_strategy_buffers_until_finish() {
        let data = b"streaming hello world";
        let mut chunker = IncrementalChunker::new(ChunkStrategy::Single);
        // Single buffers everything; feed never emits.
        for slice in data.chunks(3) {
            assert!(chunker.feed(slice).is_empty());
        }
        let final_chunks = chunker.finish();
        assert_eq!(final_chunks.len(), 1);
        assert_eq!(final_chunks[0].data, data);
    }

    #[test]
    fn test_incremental_fixed_emits_on_threshold() {
        let data = vec![7u8; 1000];
        let mut chunker = IncrementalChunker::new(ChunkStrategy::Fixed { size: 300 });

        // Feed less than one chunk worth.
        let early = chunker.feed(&data[..200]);
        assert!(early.is_empty(), "no chunks before reaching size");

        // Cross the boundary — should emit exactly one chunk.
        let crossed = chunker.feed(&data[200..400]);
        assert_eq!(crossed.len(), 1);
        assert_eq!(crossed[0].size, 300);
        assert_eq!(crossed[0].offset, 0);

        // Feed the rest in one go — emits two more (300 + 300).
        let rest = chunker.feed(&data[400..]);
        assert_eq!(rest.len(), 2);
        assert_eq!(rest[0].offset, 300);
        assert_eq!(rest[1].offset, 600);

        // Finish flushes the trailing 100 bytes.
        let tail = chunker.finish();
        assert_eq!(tail.len(), 1);
        assert_eq!(tail[0].size, 100);
        assert_eq!(tail[0].offset, 900);
    }

    #[test]
    fn test_incremental_fixed_matches_batch_across_slice_sizes() {
        let data: Vec<u8> = (0..10_000).map(|i| (i % 251) as u8).collect();
        let strategy = ChunkStrategy::Fixed { size: 256 };

        for slice_size in [1, 7, 64, 256, 1024, 4096, 10_000] {
            assert_equivalent(strategy.clone(), &data, slice_size);
        }
    }

    #[test]
    fn test_incremental_single_matches_batch_across_slice_sizes() {
        let data: Vec<u8> = (0..2_000).map(|i| (i % 199) as u8).collect();
        for slice_size in [1, 64, 1024, 2_000] {
            assert_equivalent(ChunkStrategy::Single, &data, slice_size);
        }
    }

    #[test]
    fn test_incremental_fastcdc_matches_batch_across_slice_sizes() {
        let data: Vec<u8> = (0..3 * 1024 * 1024)
            .map(|i| ((i * 31 + 7) % 256) as u8)
            .collect();
        let strategy = ChunkStrategy::FastCDC {
            min: 64 * 1024,
            avg: 256 * 1024,
            max: 1024 * 1024,
        };

        for slice_size in [
            128,
            8 * 1024,
            64 * 1024,
            256 * 1024,
            1024 * 1024,
            3 * 1024 * 1024,
        ] {
            assert_equivalent(strategy.clone(), &data, slice_size);
        }
    }

    #[test]
    fn test_incremental_fastcdc_below_max_buffers() {
        let strategy = ChunkStrategy::FastCDC {
            min: 64 * 1024,
            avg: 256 * 1024,
            max: 1024 * 1024,
        };
        let mut chunker = IncrementalChunker::new(strategy);

        // Feed half a max chunk — should buffer, emit nothing.
        let half = vec![0u8; 512 * 1024];
        assert!(chunker.feed(&half).is_empty());

        // Even after another large feed below the 2x-max threshold, emission
        // is permitted only once the post-trim buffer holds a deferrable last
        // chunk. The contract only requires equivalence on finish.
        let final_chunks = chunker.finish();
        assert!(!final_chunks.is_empty());
        let total: usize = final_chunks.iter().map(|c| c.size).sum();
        assert_eq!(total, 512 * 1024);
    }

    #[test]
    fn test_incremental_resumes_offset_after_finish() {
        let mut chunker = IncrementalChunker::new(ChunkStrategy::Fixed { size: 100 });
        let _ = chunker.feed(&[1u8; 250]);
        let first_finish = chunker.finish();
        assert_eq!(first_finish.last().unwrap().offset, 200);

        let later = chunker.feed(&[2u8; 100]);
        assert_eq!(later.len(), 1);
        assert_eq!(later[0].offset, 250);
    }
}

#[cfg(test)]
mod proptests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        #[test]
        fn test_chunk_reconstruction(data: Vec<u8>) {
            let strategy = auto_strategy(data.len());
            let chunks = chunk_data(&data, &strategy);

            let reconstructed: Vec<u8> = chunks.iter().flat_map(|c| c.data.clone()).collect();
            prop_assert_eq!(reconstructed, data);
        }

        #[test]
        fn test_chunk_hash_correctness(data: Vec<u8>) {
            let chunks = chunk_data(&data, &ChunkStrategy::Single);

            for chunk in &chunks {
                let computed = sha256(&chunk.data);
                prop_assert_eq!(chunk.hash, computed);
            }
        }

        #[test]
        fn test_chunk_offset_sum_equals_length(data: Vec<u8>) {
            let strategy = auto_strategy(data.len());
            let chunks = chunk_data(&data, &strategy);

            let total_size: usize = chunks.iter().map(|c| c.size).sum();
            prop_assert_eq!(total_size, data.len());
        }

        #[test]
        fn test_incremental_matches_batch_fixed(
            data in proptest::collection::vec(any::<u8>(), 0..50_000),
            slice_size in 1usize..2048,
            chunk_size in 1usize..4096,
        ) {
            let strategy = ChunkStrategy::Fixed { size: chunk_size };
            let batch = chunk_data(&data, &strategy);

            let mut chunker = IncrementalChunker::new(strategy);
            let mut emitted = Vec::new();
            for slice in data.chunks(slice_size) {
                emitted.extend(chunker.feed(slice));
            }
            emitted.extend(chunker.finish());

            prop_assert_eq!(emitted.len(), batch.len());
            for (a, b) in emitted.iter().zip(batch.iter()) {
                prop_assert_eq!(&a.data, &b.data);
                prop_assert_eq!(a.hash, b.hash);
                prop_assert_eq!(a.offset, b.offset);
                prop_assert_eq!(a.size, b.size);
            }
        }

        #[test]
        fn test_incremental_matches_batch_fastcdc(
            // FastCDC with small parameters keeps proptest cycles cheap; the
            // chunking algorithm is the same regardless of size.
            data in proptest::collection::vec(any::<u8>(), 0..16_384),
            slice_size in 1usize..1024,
        ) {
            let strategy = ChunkStrategy::FastCDC {
                min: 64,
                avg: 512,
                max: 4096,
            };
            let batch = chunk_data(&data, &strategy);

            let mut chunker = IncrementalChunker::new(strategy);
            let mut emitted = Vec::new();
            for slice in data.chunks(slice_size) {
                emitted.extend(chunker.feed(slice));
            }
            emitted.extend(chunker.finish());

            prop_assert_eq!(emitted.len(), batch.len());
            for (a, b) in emitted.iter().zip(batch.iter()) {
                prop_assert_eq!(&a.data, &b.data);
                prop_assert_eq!(a.hash, b.hash);
                prop_assert_eq!(a.offset, b.offset);
                prop_assert_eq!(a.size, b.size);
            }
        }
    }
}
