//! Reed-Solomon erasure coding for NeonFS.
//!
//! Provides encode and decode operations backed by `reed_solomon_erasure`.
//! Encoding computes parity shards from data shards; decoding reconstructs
//! missing shards from any K-of-N available shards.

use reed_solomon_erasure::galois_8::ReedSolomon;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ErasureError {
    #[error("data shard count must be > 0")]
    ZeroDataShards,
    #[error("parity count must be > 0")]
    ZeroParity,
    #[error("no data shards provided")]
    EmptyData,
    #[error("shard sizes are not equal")]
    UnequalShardSizes,
    #[error("reed-solomon error: {0}")]
    ReedSolomon(String),
    #[error("insufficient shards: need at least {needed}, got {got}")]
    InsufficientShards { needed: usize, got: usize },
    #[error("shard index {index} out of range (total shards: {total})")]
    IndexOutOfRange { index: usize, total: usize },
    #[error("shard size {got} does not match expected size {expected}")]
    ShardSizeMismatch { expected: usize, got: usize },
}

/// Encodes data shards and produces parity shards.
///
/// # Arguments
/// * `data_shards` - Equal-sized data shard buffers
/// * `parity_count` - Number of parity shards to generate
///
/// # Returns
/// A `Vec<Vec<u8>>` containing only the parity shards.
pub fn encode(
    data_shards: Vec<Vec<u8>>,
    parity_count: usize,
) -> Result<Vec<Vec<u8>>, ErasureError> {
    if data_shards.is_empty() {
        return Err(ErasureError::EmptyData);
    }
    if parity_count == 0 {
        return Err(ErasureError::ZeroParity);
    }

    let shard_size = data_shards[0].len();
    for shard in &data_shards[1..] {
        if shard.len() != shard_size {
            return Err(ErasureError::UnequalShardSizes);
        }
    }

    let data_count = data_shards.len();
    let rs = ReedSolomon::new(data_count, parity_count)
        .map_err(|e| ErasureError::ReedSolomon(e.to_string()))?;

    // Build the full shard vector: data shards followed by empty parity shards
    let mut all_shards: Vec<Vec<u8>> = data_shards;
    for _ in 0..parity_count {
        all_shards.push(vec![0u8; shard_size]);
    }

    rs.encode(&mut all_shards)
        .map_err(|e| ErasureError::ReedSolomon(e.to_string()))?;

    // Return only the parity shards
    let parity_shards = all_shards.split_off(data_count);
    Ok(parity_shards)
}

/// Decodes (reconstructs) missing shards from available shards.
///
/// # Arguments
/// * `shards_with_indices` - Available shards as `(index, data)` pairs.
///   Indices 0..data_count are data shards, data_count..total are parity shards.
/// * `data_count` - Number of data shards in the original encoding
/// * `parity_count` - Number of parity shards in the original encoding
/// * `shard_size` - Expected size of each shard in bytes
///
/// # Returns
/// A `Vec<Vec<u8>>` of all data shards (indices 0..data_count), reconstructed.
pub fn decode(
    shards_with_indices: Vec<(usize, Vec<u8>)>,
    data_count: usize,
    parity_count: usize,
    shard_size: usize,
) -> Result<Vec<Vec<u8>>, ErasureError> {
    if data_count == 0 {
        return Err(ErasureError::ZeroDataShards);
    }
    if parity_count == 0 {
        return Err(ErasureError::ZeroParity);
    }

    let total = data_count + parity_count;

    if shards_with_indices.len() < data_count {
        return Err(ErasureError::InsufficientShards {
            needed: data_count,
            got: shards_with_indices.len(),
        });
    }

    // Validate indices and shard sizes
    for (index, shard) in &shards_with_indices {
        if *index >= total {
            return Err(ErasureError::IndexOutOfRange {
                index: *index,
                total,
            });
        }
        if shard.len() != shard_size {
            return Err(ErasureError::ShardSizeMismatch {
                expected: shard_size,
                got: shard.len(),
            });
        }
    }

    let rs = ReedSolomon::new(data_count, parity_count)
        .map_err(|e| ErasureError::ReedSolomon(e.to_string()))?;

    // Build Option<Vec<u8>> array: None for missing, Some for present
    let mut shard_opts: Vec<Option<Vec<u8>>> = vec![None; total];
    for (index, shard) in shards_with_indices {
        shard_opts[index] = Some(shard);
    }

    rs.reconstruct_data(&mut shard_opts)
        .map_err(|e| ErasureError::ReedSolomon(e.to_string()))?;

    // Extract data shards (should all be Some after reconstruction)
    let data_shards: Vec<Vec<u8>> = shard_opts
        .into_iter()
        .take(data_count)
        .map(|opt| opt.expect("data shard should be reconstructed"))
        .collect();

    Ok(data_shards)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encode_basic() {
        let data = vec![vec![1u8, 2, 3, 4], vec![5, 6, 7, 8], vec![9, 10, 11, 12]];
        let parity = encode(data, 2).unwrap();
        assert_eq!(parity.len(), 2);
        assert_eq!(parity[0].len(), 4);
        assert_eq!(parity[1].len(), 4);
    }

    #[test]
    fn test_encode_empty_data() {
        let result = encode(vec![], 2);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("no data shards"));
    }

    #[test]
    fn test_encode_zero_parity() {
        let result = encode(vec![vec![1, 2, 3]], 0);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("parity count must be > 0"));
    }

    #[test]
    fn test_encode_unequal_shard_sizes() {
        let data = vec![vec![1, 2, 3], vec![4, 5]];
        let result = encode(data, 1);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("not equal"));
    }

    #[test]
    fn test_encode_decode_roundtrip() {
        let data = vec![
            vec![10, 20, 30, 40],
            vec![50, 60, 70, 80],
            vec![90, 100, 110, 120],
        ];
        let parity = encode(data.clone(), 2).unwrap();

        // Drop shard 1 (data) and shard 4 (parity 1) — keep shards 0, 2, 3
        let available: Vec<(usize, Vec<u8>)> = vec![
            (0, data[0].clone()),
            (2, data[2].clone()),
            (3, parity[0].clone()),
        ];

        let recovered = decode(available, 3, 2, 4).unwrap();
        assert_eq!(recovered, data);
    }

    #[test]
    fn test_decode_all_data_shards_present() {
        let data = vec![vec![1, 2], vec![3, 4]];
        let parity = encode(data.clone(), 1).unwrap();

        // All data shards present, no reconstruction needed
        let available: Vec<(usize, Vec<u8>)> = vec![
            (0, data[0].clone()),
            (1, data[1].clone()),
            (2, parity[0].clone()),
        ];

        let recovered = decode(available, 2, 1, 2).unwrap();
        assert_eq!(recovered, data);
    }

    #[test]
    fn test_decode_insufficient_shards() {
        let result = decode(vec![(0, vec![1, 2])], 3, 2, 2);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("insufficient"));
    }

    #[test]
    fn test_decode_zero_data_count() {
        let result = decode(vec![], 0, 2, 4);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("data shard count must be > 0"));
    }

    #[test]
    fn test_decode_zero_parity_count() {
        let result = decode(vec![(0, vec![1])], 1, 0, 1);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("parity count must be > 0"));
    }

    #[test]
    fn test_decode_index_out_of_range() {
        let result = decode(vec![(10, vec![1, 2]), (11, vec![3, 4])], 2, 1, 2);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("out of range"));
    }

    #[test]
    fn test_decode_shard_size_mismatch() {
        let result = decode(vec![(0, vec![1, 2, 3]), (1, vec![4, 5])], 2, 1, 3);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("does not match"));
    }

    #[test]
    fn test_encode_single_data_shard() {
        let data = vec![vec![42, 43, 44, 45]];
        let parity = encode(data.clone(), 2).unwrap();
        assert_eq!(parity.len(), 2);

        // Drop the data shard, use only parity to recover (need 1 of 1 data, but we only have 2 parity)
        // Actually need at least data_count shards total — 1 data shard needs 1 of 3 available
        let available: Vec<(usize, Vec<u8>)> = vec![(1, parity[0].clone()), (2, parity[1].clone())];
        // This should fail — need at least 1 shard to reconstruct 1 data shard, and we have 2 >= 1
        // But both are parity. Reed-Solomon can reconstruct from any K-of-N.
        let recovered = decode(available, 1, 2, 4).unwrap();
        assert_eq!(recovered, data);
    }

    #[test]
    fn test_large_shards() {
        let shard_size = 256 * 1024; // 256KB
        let data: Vec<Vec<u8>> = (0..10).map(|i| vec![i as u8; shard_size]).collect();

        let parity = encode(data.clone(), 4).unwrap();
        assert_eq!(parity.len(), 4);

        // Drop 4 shards (max recoverable)
        let mut available: Vec<(usize, Vec<u8>)> = Vec::new();
        for (i, shard) in data.iter().enumerate().skip(4) {
            available.push((i, shard.clone()));
        }
        for (i, shard) in parity.iter().enumerate() {
            available.push((10 + i, shard.clone()));
        }

        let recovered = decode(available, 10, 4, shard_size).unwrap();
        assert_eq!(recovered, data);
    }
}

#[cfg(test)]
mod proptests {
    use super::*;
    use proptest::prelude::*;

    prop_compose! {
        fn erasure_params()(
            dc in 1usize..=10,
            pc in 1usize..=4,
            shard_size in 1usize..=1024,
        )(
            pc in Just(pc),
            data in proptest::collection::vec(
                proptest::collection::vec(any::<u8>(), shard_size..=shard_size),
                dc..=dc,
            ),
        ) -> (Vec<Vec<u8>>, usize) {
            (data, pc)
        }
    }

    proptest! {
        #[test]
        fn test_encode_decode_roundtrip((data, parity_count) in erasure_params()) {
            let data_count = data.len();
            let shard_size = data[0].len();

            let parity = encode(data.clone(), parity_count).unwrap();
            prop_assert_eq!(parity.len(), parity_count);

            // All shards available — trivial decode
            let mut available: Vec<(usize, Vec<u8>)> = data
                .iter()
                .enumerate()
                .map(|(i, s)| (i, s.clone()))
                .collect();
            for (i, s) in parity.iter().enumerate() {
                available.push((data_count + i, s.clone()));
            }

            let recovered = decode(available, data_count, parity_count, shard_size).unwrap();
            prop_assert_eq!(&recovered, &data);
        }

        #[test]
        fn test_encode_decode_with_drops((data, parity_count) in erasure_params()) {
            let data_count = data.len();
            let shard_size = data[0].len();
            let total = data_count + parity_count;

            let parity = encode(data.clone(), parity_count).unwrap();

            // Build all shards
            let mut all_shards: Vec<Vec<u8>> = data.clone();
            all_shards.extend(parity);

            // Keep exactly data_count shards (drop parity_count shards)
            // Use a deterministic selection: keep first data_count shards by index
            let available: Vec<(usize, Vec<u8>)> = all_shards
                .into_iter()
                .enumerate()
                .take(total)
                .collect::<Vec<_>>()
                .into_iter()
                .take(data_count)
                .collect();

            let recovered = decode(available, data_count, parity_count, shard_size).unwrap();
            prop_assert_eq!(&recovered, &data);
        }
    }
}
