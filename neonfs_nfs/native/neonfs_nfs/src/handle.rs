/// NeonFS file handle for NFSv3
///
/// The nfs3_server framework reserves the first 8 bytes of the wire handle
/// for its own generation counter. Our handle uses the remaining space:
///
/// ```text
/// Bytes 0-7:   Inode number (u64)
/// Bytes 8-23:  Volume ID (16 bytes, UUID)
/// ```
///
/// Total: 24 bytes (within the 56 usable byte limit).
use nfs3_server::vfs::FileHandle;

/// File handle encoding NeonFS inode and volume.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NeonFileHandle {
    data: [u8; HANDLE_SIZE],
}

const HANDLE_SIZE: usize = 24;

impl NeonFileHandle {
    pub fn new(inode: u64, volume_id: [u8; 16]) -> Self {
        let mut data = [0u8; HANDLE_SIZE];
        data[0..8].copy_from_slice(&inode.to_le_bytes());
        data[8..24].copy_from_slice(&volume_id);
        Self { data }
    }

    /// Create a root handle (inode 1, null volume)
    pub fn root() -> Self {
        Self::new(1, [0u8; 16])
    }

    pub fn inode(&self) -> u64 {
        u64::from_le_bytes(self.data[0..8].try_into().unwrap())
    }

    pub fn volume_id(&self) -> [u8; 16] {
        self.data[8..24].try_into().unwrap()
    }
}

impl FileHandle for NeonFileHandle {
    fn len(&self) -> usize {
        HANDLE_SIZE
    }

    fn as_bytes(&self) -> &[u8] {
        &self.data
    }

    fn from_bytes(bytes: &[u8]) -> Option<Self> {
        if bytes.len() < HANDLE_SIZE {
            return None;
        }
        let mut data = [0u8; HANDLE_SIZE];
        data.copy_from_slice(&bytes[..HANDLE_SIZE]);
        Some(Self { data })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_handle_round_trip() {
        let handle = NeonFileHandle::new(42, [1u8; 16]);
        let bytes = handle.as_bytes();
        assert_eq!(bytes.len(), HANDLE_SIZE);
        let decoded = NeonFileHandle::from_bytes(bytes).unwrap();
        assert_eq!(handle, decoded);
        assert_eq!(decoded.inode(), 42);
        assert_eq!(decoded.volume_id(), [1u8; 16]);
    }

    #[test]
    fn test_root_handle() {
        let handle = NeonFileHandle::root();
        assert_eq!(handle.inode(), 1);
        assert_eq!(handle.volume_id(), [0u8; 16]);
    }

    #[test]
    fn test_handle_from_short_bytes() {
        let result = NeonFileHandle::from_bytes(&[0u8; 10]);
        assert!(result.is_none());
    }

    #[test]
    fn test_handle_len() {
        let handle = NeonFileHandle::root();
        assert_eq!(handle.len(), HANDLE_SIZE);
    }
}
