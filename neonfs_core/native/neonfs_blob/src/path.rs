//! Blob store directory layout module.
//!
//! This module implements the content-addressed directory hierarchy for chunk storage.
//! Chunks are stored in sharded directories using the first characters of the hash
//! as directory prefixes to prevent any single directory from containing too many entries.

use crate::hash::Hash;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

/// Storage tier for a chunk.
///
/// Tiers represent different storage classes with varying performance
/// and durability characteristics.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Tier {
    /// Hot tier: fastest access, typically SSD storage.
    Hot,
    /// Warm tier: balanced performance and cost.
    Warm,
    /// Cold tier: archival storage, slower access.
    Cold,
}

impl Tier {
    /// Returns the tier as a lowercase string for use in paths.
    pub fn as_str(&self) -> &'static str {
        match self {
            Tier::Hot => "hot",
            Tier::Warm => "warm",
            Tier::Cold => "cold",
        }
    }
}

impl fmt::Display for Tier {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// Generates the path for a chunk file based on its hash and storage tier.
///
/// The path follows the format:
/// `{base_dir}/blobs/{tier}/{prefix_1}/{prefix_2}/.../{hash}`
///
/// # Arguments
/// * `base_dir` - The base directory for blob storage.
/// * `hash` - The SHA-256 hash of the chunk.
/// * `tier` - The storage tier (hot, warm, cold).
/// * `prefix_depth` - Number of 2-character prefix directories (1=256 dirs, 2=65K dirs).
///
/// # Examples
/// ```
/// use neonfs_blob::path::{chunk_path, Tier};
/// use neonfs_blob::hash::Hash;
/// use std::path::Path;
///
/// let hash = Hash::from_hex("abcd7c9e1234567890abcdef1234567890abcdef1234567890abcdef12345678").unwrap();
/// let path = chunk_path(Path::new("/var/lib/neonfs"), &hash, Tier::Hot, 2);
/// assert_eq!(
///     path,
///     Path::new("/var/lib/neonfs/blobs/hot/ab/cd/abcd7c9e1234567890abcdef1234567890abcdef1234567890abcdef12345678")
/// );
/// ```
pub fn chunk_path(base_dir: &Path, hash: &Hash, tier: Tier, prefix_depth: usize) -> PathBuf {
    let hex = hash.to_hex();
    let mut path = base_dir.to_path_buf();

    // Add the blobs directory and tier
    path.push("blobs");
    path.push(tier.as_str());

    // Add prefix directories based on depth
    // Each prefix is 2 hex characters (1 byte)
    for i in 0..prefix_depth {
        let start = i * 2;
        let end = start + 2;
        path.push(&hex[start..end]);
    }

    // Add the full hash as the filename
    path.push(&hex);

    path
}

/// Generates the path for a metadata file based on its segment and key hash.
///
/// The path follows the format:
/// `{base_dir}/meta/{segment_id_hex}/{prefix_1}/{prefix_2}/.../{key_hash_hex}`
///
/// # Arguments
/// * `base_dir` - The base directory for blob storage.
/// * `segment_id_hex` - The 64-character hex string identifying the segment.
/// * `key_hash` - The SHA-256 hash of the metadata key.
/// * `prefix_depth` - Number of 2-character prefix directories (1=256 dirs, 2=65K dirs).
///
/// # Examples
/// ```
/// use neonfs_blob::path::metadata_path;
/// use neonfs_blob::hash::Hash;
/// use std::path::Path;
///
/// let key_hash = Hash::from_hex("abcd7c9e1234567890abcdef1234567890abcdef1234567890abcdef12345678").unwrap();
/// let segment_hex = "ff".repeat(32);
/// let path = metadata_path(Path::new("/var/lib/neonfs"), &segment_hex, &key_hash, 2);
/// assert_eq!(
///     path,
///     Path::new(&format!("/var/lib/neonfs/meta/{}/ab/cd/abcd7c9e1234567890abcdef1234567890abcdef1234567890abcdef12345678", segment_hex))
/// );
/// ```
pub fn metadata_path(
    base_dir: &Path,
    segment_id_hex: &str,
    key_hash: &Hash,
    prefix_depth: usize,
) -> PathBuf {
    let hex = key_hash.to_hex();
    let mut path = base_dir.to_path_buf();

    // Add the meta directory and segment
    path.push("meta");
    path.push(segment_id_hex);

    // Add prefix directories based on depth
    // Each prefix is 2 hex characters (1 byte)
    for i in 0..prefix_depth {
        let start = i * 2;
        let end = start + 2;
        path.push(&hex[start..end]);
    }

    // Add the full key hash as the filename
    path.push(&hex);

    path
}

/// Creates the parent directories for a path atomically.
///
/// This function is idempotent - calling it multiple times with the same path
/// will succeed without error if the directories already exist.
///
/// # Arguments
/// * `path` - The file path whose parent directories should be created.
///
/// # Errors
/// Returns an error if the directories cannot be created (e.g., permission denied).
///
/// # Examples
/// ```no_run
/// use neonfs_blob::path::ensure_parent_dirs;
/// use std::path::Path;
///
/// let chunk_path = Path::new("/var/lib/neonfs/blobs/hot/ab/cd/abcd...");
/// ensure_parent_dirs(chunk_path)?;
/// // Now /var/lib/neonfs/blobs/hot/ab/cd/ exists
/// # Ok::<(), std::io::Error>(())
/// ```
pub fn ensure_parent_dirs(path: &Path) -> io::Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    // Test hash: abcd7c9e followed by zeroes
    const TEST_HASH_HEX: &str = "abcd7c9e00000000000000000000000000000000000000000000000000000000";

    fn test_hash() -> Hash {
        Hash::from_hex(TEST_HASH_HEX).unwrap()
    }

    #[test]
    fn test_tier_as_str() {
        assert_eq!(Tier::Hot.as_str(), "hot");
        assert_eq!(Tier::Warm.as_str(), "warm");
        assert_eq!(Tier::Cold.as_str(), "cold");
    }

    #[test]
    fn test_tier_display() {
        assert_eq!(format!("{}", Tier::Hot), "hot");
        assert_eq!(format!("{}", Tier::Warm), "warm");
        assert_eq!(format!("{}", Tier::Cold), "cold");
    }

    #[test]
    fn test_tier_serialize() {
        let json = serde_json::to_string(&Tier::Hot).unwrap();
        assert_eq!(json, "\"hot\"");

        let json = serde_json::to_string(&Tier::Warm).unwrap();
        assert_eq!(json, "\"warm\"");

        let json = serde_json::to_string(&Tier::Cold).unwrap();
        assert_eq!(json, "\"cold\"");
    }

    #[test]
    fn test_tier_deserialize() {
        let tier: Tier = serde_json::from_str("\"hot\"").unwrap();
        assert_eq!(tier, Tier::Hot);

        let tier: Tier = serde_json::from_str("\"warm\"").unwrap();
        assert_eq!(tier, Tier::Warm);

        let tier: Tier = serde_json::from_str("\"cold\"").unwrap();
        assert_eq!(tier, Tier::Cold);
    }

    #[test]
    fn test_chunk_path_hot_depth_1() {
        let hash = test_hash();
        let path = chunk_path(Path::new("/var/lib/neonfs"), &hash, Tier::Hot, 1);
        assert_eq!(
            path,
            Path::new(&format!("/var/lib/neonfs/blobs/hot/ab/{}", TEST_HASH_HEX))
        );
    }

    #[test]
    fn test_chunk_path_hot_depth_2() {
        let hash = test_hash();
        let path = chunk_path(Path::new("/var/lib/neonfs"), &hash, Tier::Hot, 2);
        assert_eq!(
            path,
            Path::new(&format!(
                "/var/lib/neonfs/blobs/hot/ab/cd/{}",
                TEST_HASH_HEX
            ))
        );
    }

    #[test]
    fn test_chunk_path_warm_depth_2() {
        let hash = test_hash();
        let path = chunk_path(Path::new("/var/lib/neonfs"), &hash, Tier::Warm, 2);
        assert_eq!(
            path,
            Path::new(&format!(
                "/var/lib/neonfs/blobs/warm/ab/cd/{}",
                TEST_HASH_HEX
            ))
        );
    }

    #[test]
    fn test_chunk_path_cold_depth_2() {
        let hash = test_hash();
        let path = chunk_path(Path::new("/var/lib/neonfs"), &hash, Tier::Cold, 2);
        assert_eq!(
            path,
            Path::new(&format!(
                "/var/lib/neonfs/blobs/cold/ab/cd/{}",
                TEST_HASH_HEX
            ))
        );
    }

    #[test]
    fn test_chunk_path_depth_0() {
        let hash = test_hash();
        let path = chunk_path(Path::new("/var/lib/neonfs"), &hash, Tier::Hot, 0);
        assert_eq!(
            path,
            Path::new(&format!("/var/lib/neonfs/blobs/hot/{}", TEST_HASH_HEX))
        );
    }

    #[test]
    fn test_chunk_path_depth_3() {
        let hash = test_hash();
        let path = chunk_path(Path::new("/var/lib/neonfs"), &hash, Tier::Hot, 3);
        assert_eq!(
            path,
            Path::new(&format!(
                "/var/lib/neonfs/blobs/hot/ab/cd/7c/{}",
                TEST_HASH_HEX
            ))
        );
    }

    #[test]
    fn test_chunk_path_lowercase_hex() {
        // Hash with uppercase in source should still produce lowercase paths
        let hash =
            Hash::from_hex("ABCD7C9E00000000000000000000000000000000000000000000000000000000")
                .unwrap();
        let path = chunk_path(Path::new("/base"), &hash, Tier::Hot, 2);

        // Path should have lowercase hex components
        let path_str = path.to_string_lossy();
        assert!(path_str.contains("/ab/cd/"));
        assert!(!path_str.contains("/AB/"));
        assert!(!path_str.contains("/CD/"));
    }

    #[test]
    fn test_ensure_parent_dirs_creates_directories() {
        let temp_dir = std::env::temp_dir().join(format!("neonfs_test_{}", std::process::id()));
        let chunk_path = temp_dir.join("blobs/hot/ab/cd/test_chunk");

        // Ensure parent doesn't exist yet
        let parent = chunk_path.parent().unwrap();
        if parent.exists() {
            fs::remove_dir_all(&temp_dir).ok();
        }

        // Create parent directories
        ensure_parent_dirs(&chunk_path).expect("should create parent directories");

        // Verify parent exists
        assert!(parent.exists());
        assert!(parent.is_dir());

        // Cleanup
        fs::remove_dir_all(&temp_dir).ok();
    }

    #[test]
    fn test_ensure_parent_dirs_idempotent() {
        let temp_dir =
            std::env::temp_dir().join(format!("neonfs_test_idem_{}", std::process::id()));
        let chunk_path = temp_dir.join("blobs/hot/ab/cd/test_chunk");

        // Clean up if exists
        if temp_dir.exists() {
            fs::remove_dir_all(&temp_dir).ok();
        }

        // Call multiple times - should all succeed
        ensure_parent_dirs(&chunk_path).expect("first call should succeed");
        ensure_parent_dirs(&chunk_path).expect("second call should succeed");
        ensure_parent_dirs(&chunk_path).expect("third call should succeed");

        // Verify parent exists
        assert!(chunk_path.parent().unwrap().exists());

        // Cleanup
        fs::remove_dir_all(&temp_dir).ok();
    }

    #[test]
    fn test_ensure_parent_dirs_root_path() {
        // A path with no parent (like "/" on Unix) should not fail
        let result = ensure_parent_dirs(Path::new("/"));
        assert!(result.is_ok());
    }

    #[test]
    fn test_chunk_path_different_base_dirs() {
        let hash = test_hash();

        let path1 = chunk_path(Path::new("/data"), &hash, Tier::Hot, 2);
        let path2 = chunk_path(Path::new("/storage"), &hash, Tier::Hot, 2);

        assert!(path1.starts_with("/data"));
        assert!(path2.starts_with("/storage"));
        assert_ne!(path1, path2);
    }

    // Metadata path tests

    const TEST_SEGMENT_HEX: &str =
        "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff";

    #[test]
    fn test_metadata_path_depth_2() {
        let key_hash = test_hash();
        let path = metadata_path(Path::new("/var/lib/neonfs"), TEST_SEGMENT_HEX, &key_hash, 2);
        assert_eq!(
            path,
            Path::new(&format!(
                "/var/lib/neonfs/meta/{}/ab/cd/{}",
                TEST_SEGMENT_HEX, TEST_HASH_HEX
            ))
        );
    }

    #[test]
    fn test_metadata_path_depth_1() {
        let key_hash = test_hash();
        let path = metadata_path(Path::new("/var/lib/neonfs"), TEST_SEGMENT_HEX, &key_hash, 1);
        assert_eq!(
            path,
            Path::new(&format!(
                "/var/lib/neonfs/meta/{}/ab/{}",
                TEST_SEGMENT_HEX, TEST_HASH_HEX
            ))
        );
    }

    #[test]
    fn test_metadata_path_depth_0() {
        let key_hash = test_hash();
        let path = metadata_path(Path::new("/var/lib/neonfs"), TEST_SEGMENT_HEX, &key_hash, 0);
        assert_eq!(
            path,
            Path::new(&format!(
                "/var/lib/neonfs/meta/{}/{}",
                TEST_SEGMENT_HEX, TEST_HASH_HEX
            ))
        );
    }

    #[test]
    fn test_metadata_path_lowercase_hex() {
        let key_hash =
            Hash::from_hex("ABCD7C9E00000000000000000000000000000000000000000000000000000000")
                .unwrap();
        let path = metadata_path(Path::new("/base"), TEST_SEGMENT_HEX, &key_hash, 2);

        let path_str = path.to_string_lossy();
        assert!(path_str.contains("/ab/cd/"));
        assert!(!path_str.contains("/AB/"));
    }

    #[test]
    fn test_metadata_path_different_segments() {
        let key_hash = test_hash();
        let seg_a = "aa".repeat(32);
        let seg_b = "bb".repeat(32);

        let path_a = metadata_path(Path::new("/data"), &seg_a, &key_hash, 2);
        let path_b = metadata_path(Path::new("/data"), &seg_b, &key_hash, 2);

        assert_ne!(path_a, path_b);
        assert!(path_a.to_string_lossy().contains(&seg_a));
        assert!(path_b.to_string_lossy().contains(&seg_b));
    }
}
