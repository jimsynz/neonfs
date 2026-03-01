//! FUSE mount integration tests
//!
//! These tests verify that FUSE mounting and filesystem operations work correctly.
//! They run as normal OS processes (not under BEAM), so fusermount's fork/waitpid
//! works properly.
//!
//! Tests require /dev/fuse to be available and will fail if FUSE is not supported.

use fuser::{
    BsdFileFlags, Config, Errno, FileAttr, FileHandle, FileType, Filesystem, FopenFlags,
    Generation, INodeNo, LockOwner, MountOption, OpenFlags, RenameFlags, ReplyAttr, ReplyData,
    ReplyDirectory, ReplyEntry, Request, WriteFlags,
};
use std::collections::HashMap;
use std::ffi::OsStr;
use std::fs::{self, File};
use std::io::Write;
use std::path::PathBuf;
use std::process::Command;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// TTL for FUSE caching
const TTL: Duration = Duration::from_secs(1);

/// Root inode number
const ROOT_INO: u64 = 1;

/// Entry in our test filesystem
#[derive(Clone)]
struct TestEntry {
    ino: u64,
    name: String,
    kind: FileType,
    data: Vec<u8>,
    children: Vec<u64>, // For directories: inode numbers of children
}

/// A simple in-memory filesystem for testing FUSE operations
struct TestFilesystem {
    entries: Arc<Mutex<HashMap<u64, TestEntry>>>,
    next_ino: Arc<Mutex<u64>>,
    uid: u32,
    gid: u32,
}

impl TestFilesystem {
    fn new() -> Self {
        let mut entries = HashMap::new();

        // Create root directory
        entries.insert(
            ROOT_INO,
            TestEntry {
                ino: ROOT_INO,
                name: "/".to_string(),
                kind: FileType::Directory,
                data: Vec::new(),
                children: Vec::new(),
            },
        );

        Self {
            entries: Arc::new(Mutex::new(entries)),
            next_ino: Arc::new(Mutex::new(2)),
            uid: unsafe { libc::getuid() },
            gid: unsafe { libc::getgid() },
        }
    }

    /// Create a filesystem pre-populated with test files
    fn with_test_files() -> Self {
        let fs = Self::new();

        // Add a test file with known content
        {
            let mut entries = fs.entries.lock().unwrap();
            let mut next_ino = fs.next_ino.lock().unwrap();

            let file_ino = *next_ino;
            *next_ino += 1;

            entries.insert(
                file_ino,
                TestEntry {
                    ino: file_ino,
                    name: "hello.txt".to_string(),
                    kind: FileType::RegularFile,
                    data: b"Hello, FUSE!\n".to_vec(),
                    children: Vec::new(),
                },
            );

            // Add to root's children
            entries.get_mut(&ROOT_INO).unwrap().children.push(file_ino);

            // Add a subdirectory
            let dir_ino = *next_ino;
            *next_ino += 1;

            entries.insert(
                dir_ino,
                TestEntry {
                    ino: dir_ino,
                    name: "subdir".to_string(),
                    kind: FileType::Directory,
                    data: Vec::new(),
                    children: Vec::new(),
                },
            );

            entries.get_mut(&ROOT_INO).unwrap().children.push(dir_ino);

            // Add a file inside the subdirectory
            let nested_file_ino = *next_ino;
            *next_ino += 1;

            entries.insert(
                nested_file_ino,
                TestEntry {
                    ino: nested_file_ino,
                    name: "nested.txt".to_string(),
                    kind: FileType::RegularFile,
                    data: b"Nested content\n".to_vec(),
                    children: Vec::new(),
                },
            );

            entries
                .get_mut(&dir_ino)
                .unwrap()
                .children
                .push(nested_file_ino);
        }

        fs
    }

    fn make_attr(&self, entry: &TestEntry) -> FileAttr {
        let now = SystemTime::now();
        FileAttr {
            ino: INodeNo(entry.ino),
            size: entry.data.len() as u64,
            blocks: (entry.data.len() as u64).div_ceil(512),
            atime: now,
            mtime: now,
            ctime: now,
            crtime: now,
            kind: entry.kind,
            perm: if entry.kind == FileType::Directory {
                0o755
            } else {
                0o644
            },
            nlink: 1,
            uid: self.uid,
            gid: self.gid,
            rdev: 0,
            blksize: 4096,
            flags: 0,
        }
    }

    fn find_child(&self, parent_ino: u64, name: &str) -> Option<TestEntry> {
        let entries = self.entries.lock().unwrap();
        let parent = entries.get(&parent_ino)?;

        for &child_ino in &parent.children {
            if let Some(child) = entries.get(&child_ino) {
                if child.name == name {
                    return Some(child.clone());
                }
            }
        }
        None
    }
}

impl Filesystem for TestFilesystem {
    fn lookup(&self, _req: &Request, parent: INodeNo, name: &OsStr, reply: ReplyEntry) {
        let name_str = name.to_string_lossy();

        if let Some(entry) = self.find_child(parent.0, &name_str) {
            let attr = self.make_attr(&entry);
            reply.entry(&TTL, &attr, Generation(0));
        } else {
            reply.error(Errno::ENOENT);
        }
    }

    fn getattr(&self, _req: &Request, ino: INodeNo, _fh: Option<FileHandle>, reply: ReplyAttr) {
        let entries = self.entries.lock().unwrap();
        if let Some(entry) = entries.get(&ino.0) {
            let attr = self.make_attr(entry);
            reply.attr(&TTL, &attr);
        } else {
            reply.error(Errno::ENOENT);
        }
    }

    fn read(
        &self,
        _req: &Request,
        ino: INodeNo,
        _fh: FileHandle,
        offset: u64,
        size: u32,
        _flags: OpenFlags,
        _lock_owner: Option<LockOwner>,
        reply: ReplyData,
    ) {
        let entries = self.entries.lock().unwrap();
        if let Some(entry) = entries.get(&ino.0) {
            let offset = offset as usize;
            let size = size as usize;

            if offset >= entry.data.len() {
                reply.data(&[]);
            } else {
                let end = std::cmp::min(offset + size, entry.data.len());
                reply.data(&entry.data[offset..end]);
            }
        } else {
            reply.error(Errno::ENOENT);
        }
    }

    fn readdir(
        &self,
        _req: &Request,
        ino: INodeNo,
        _fh: FileHandle,
        offset: u64,
        mut reply: ReplyDirectory,
    ) {
        let entries = self.entries.lock().unwrap();

        if let Some(dir) = entries.get(&ino.0) {
            if dir.kind != FileType::Directory {
                reply.error(Errno::ENOTDIR);
                return;
            }

            let mut all_entries = vec![
                (ino.0, FileType::Directory, "."),
                (ino.0, FileType::Directory, ".."),
            ];

            for &child_ino in &dir.children {
                if let Some(child) = entries.get(&child_ino) {
                    all_entries.push((child.ino, child.kind, &child.name));
                }
            }

            for (idx, (entry_ino, kind, name)) in
                all_entries.iter().enumerate().skip(offset as usize)
            {
                let full = reply.add(INodeNo(*entry_ino), (idx + 1) as u64, *kind, name);
                if full {
                    break;
                }
            }
            reply.ok();
        } else {
            reply.error(Errno::ENOENT);
        }
    }

    fn open(&self, _req: &Request, ino: INodeNo, _flags: OpenFlags, reply: fuser::ReplyOpen) {
        let entries = self.entries.lock().unwrap();
        if entries.contains_key(&ino.0) {
            reply.opened(FileHandle(0), FopenFlags::empty());
        } else {
            reply.error(Errno::ENOENT);
        }
    }

    fn release(
        &self,
        _req: &Request,
        _ino: INodeNo,
        _fh: FileHandle,
        _flags: OpenFlags,
        _lock_owner: Option<LockOwner>,
        _flush: bool,
        reply: fuser::ReplyEmpty,
    ) {
        reply.ok();
    }

    fn create(
        &self,
        _req: &Request,
        parent: INodeNo,
        name: &OsStr,
        _mode: u32,
        _umask: u32,
        _flags: i32,
        reply: fuser::ReplyCreate,
    ) {
        let name_str = name.to_string_lossy().to_string();

        let mut entries = self.entries.lock().unwrap();
        let mut next_ino = self.next_ino.lock().unwrap();

        // Check parent exists and is a directory
        if let Some(parent_entry) = entries.get(&parent.0) {
            if parent_entry.kind != FileType::Directory {
                reply.error(Errno::ENOTDIR);
                return;
            }
        } else {
            reply.error(Errno::ENOENT);
            return;
        }

        // Check file doesn't already exist
        let parent_children = &entries.get(&parent.0).unwrap().children;
        for &child_ino in parent_children {
            if let Some(child) = entries.get(&child_ino) {
                if child.name == name_str {
                    reply.error(Errno::EEXIST);
                    return;
                }
            }
        }

        // Create new file
        let ino = *next_ino;
        *next_ino += 1;

        let entry = TestEntry {
            ino,
            name: name_str,
            kind: FileType::RegularFile,
            data: Vec::new(),
            children: Vec::new(),
        };

        let attr = self.make_attr(&entry);
        entries.insert(ino, entry);
        entries.get_mut(&parent.0).unwrap().children.push(ino);

        reply.created(
            &TTL,
            &attr,
            Generation(0),
            FileHandle(0),
            FopenFlags::empty(),
        );
    }

    fn write(
        &self,
        _req: &Request,
        ino: INodeNo,
        _fh: FileHandle,
        offset: u64,
        data: &[u8],
        _write_flags: WriteFlags,
        _flags: OpenFlags,
        _lock_owner: Option<LockOwner>,
        reply: fuser::ReplyWrite,
    ) {
        let mut entries = self.entries.lock().unwrap();

        if let Some(entry) = entries.get_mut(&ino.0) {
            if entry.kind != FileType::RegularFile {
                reply.error(Errno::EISDIR);
                return;
            }

            let offset = offset as usize;
            let new_len = offset + data.len();

            // Extend file if needed
            if new_len > entry.data.len() {
                entry.data.resize(new_len, 0);
            }

            entry.data[offset..offset + data.len()].copy_from_slice(data);
            reply.written(data.len() as u32);
        } else {
            reply.error(Errno::ENOENT);
        }
    }

    fn unlink(&self, _req: &Request, parent: INodeNo, name: &OsStr, reply: fuser::ReplyEmpty) {
        let name_str = name.to_string_lossy().to_string();

        let mut entries = self.entries.lock().unwrap();

        // Find the file
        let file_ino = {
            let parent_entry = match entries.get(&parent.0) {
                Some(e) => e,
                None => {
                    reply.error(Errno::ENOENT);
                    return;
                }
            };

            let mut found_ino = None;
            for &child_ino in &parent_entry.children {
                if let Some(child) = entries.get(&child_ino) {
                    if child.name == name_str {
                        if child.kind == FileType::Directory {
                            reply.error(Errno::EISDIR);
                            return;
                        }
                        found_ino = Some(child_ino);
                        break;
                    }
                }
            }
            match found_ino {
                Some(ino) => ino,
                None => {
                    reply.error(Errno::ENOENT);
                    return;
                }
            }
        };

        // Remove from parent's children
        if let Some(parent_entry) = entries.get_mut(&parent.0) {
            parent_entry.children.retain(|&ino| ino != file_ino);
        }

        // Remove the entry
        entries.remove(&file_ino);
        reply.ok();
    }

    fn mkdir(
        &self,
        _req: &Request,
        parent: INodeNo,
        name: &OsStr,
        _mode: u32,
        _umask: u32,
        reply: ReplyEntry,
    ) {
        let name_str = name.to_string_lossy().to_string();

        let mut entries = self.entries.lock().unwrap();
        let mut next_ino = self.next_ino.lock().unwrap();

        // Check parent exists and is a directory
        if let Some(parent_entry) = entries.get(&parent.0) {
            if parent_entry.kind != FileType::Directory {
                reply.error(Errno::ENOTDIR);
                return;
            }

            // Check directory doesn't already exist
            for &child_ino in &parent_entry.children {
                if let Some(child) = entries.get(&child_ino) {
                    if child.name == name_str {
                        reply.error(Errno::EEXIST);
                        return;
                    }
                }
            }
        } else {
            reply.error(Errno::ENOENT);
            return;
        }

        // Create new directory
        let ino = *next_ino;
        *next_ino += 1;

        let entry = TestEntry {
            ino,
            name: name_str,
            kind: FileType::Directory,
            data: Vec::new(),
            children: Vec::new(),
        };

        let attr = self.make_attr(&entry);
        entries.insert(ino, entry);
        entries.get_mut(&parent.0).unwrap().children.push(ino);

        reply.entry(&TTL, &attr, Generation(0));
    }

    fn rmdir(&self, _req: &Request, parent: INodeNo, name: &OsStr, reply: fuser::ReplyEmpty) {
        let name_str = name.to_string_lossy().to_string();

        let mut entries = self.entries.lock().unwrap();

        // Find the directory
        let dir_ino = {
            let parent_entry = match entries.get(&parent.0) {
                Some(e) => e,
                None => {
                    reply.error(Errno::ENOENT);
                    return;
                }
            };

            let mut found_ino = None;
            for &child_ino in &parent_entry.children {
                if let Some(child) = entries.get(&child_ino) {
                    if child.name == name_str {
                        if child.kind != FileType::Directory {
                            reply.error(Errno::ENOTDIR);
                            return;
                        }
                        if !child.children.is_empty() {
                            reply.error(Errno::ENOTEMPTY);
                            return;
                        }
                        found_ino = Some(child_ino);
                        break;
                    }
                }
            }
            match found_ino {
                Some(ino) => ino,
                None => {
                    reply.error(Errno::ENOENT);
                    return;
                }
            }
        };

        // Remove from parent's children
        if let Some(parent_entry) = entries.get_mut(&parent.0) {
            parent_entry.children.retain(|&ino| ino != dir_ino);
        }

        // Remove the entry
        entries.remove(&dir_ino);
        reply.ok();
    }

    fn rename(
        &self,
        _req: &Request,
        parent: INodeNo,
        name: &OsStr,
        newparent: INodeNo,
        newname: &OsStr,
        _flags: RenameFlags,
        reply: fuser::ReplyEmpty,
    ) {
        let old_name = name.to_string_lossy().to_string();
        let new_name = newname.to_string_lossy().to_string();

        let mut entries = self.entries.lock().unwrap();

        // Find the source entry
        let entry_ino = {
            let parent_entry = match entries.get(&parent.0) {
                Some(e) => e,
                None => {
                    reply.error(Errno::ENOENT);
                    return;
                }
            };

            let mut found_ino = None;
            for &child_ino in &parent_entry.children {
                if let Some(child) = entries.get(&child_ino) {
                    if child.name == old_name {
                        found_ino = Some(child_ino);
                        break;
                    }
                }
            }
            match found_ino {
                Some(ino) => ino,
                None => {
                    reply.error(Errno::ENOENT);
                    return;
                }
            }
        };

        // Check new parent exists
        if !entries.contains_key(&newparent.0) {
            reply.error(Errno::ENOENT);
            return;
        }

        // Remove from old parent
        if let Some(parent_entry) = entries.get_mut(&parent.0) {
            parent_entry.children.retain(|&ino| ino != entry_ino);
        }

        // Update name and add to new parent
        if let Some(entry) = entries.get_mut(&entry_ino) {
            entry.name = new_name;
        }

        if let Some(new_parent_entry) = entries.get_mut(&newparent.0) {
            new_parent_entry.children.push(entry_ino);
        }

        reply.ok();
    }

    fn setattr(
        &self,
        _req: &Request,
        ino: INodeNo,
        _mode: Option<u32>,
        _uid: Option<u32>,
        _gid: Option<u32>,
        size: Option<u64>,
        _atime: Option<fuser::TimeOrNow>,
        _mtime: Option<fuser::TimeOrNow>,
        _ctime: Option<SystemTime>,
        _fh: Option<FileHandle>,
        _crtime: Option<SystemTime>,
        _chgtime: Option<SystemTime>,
        _bkuptime: Option<SystemTime>,
        _flags: Option<BsdFileFlags>,
        reply: ReplyAttr,
    ) {
        let mut entries = self.entries.lock().unwrap();

        if let Some(entry) = entries.get_mut(&ino.0) {
            // Handle truncation
            if let Some(new_size) = size {
                entry.data.resize(new_size as usize, 0);
            }

            let attr = self.make_attr(entry);
            reply.attr(&TTL, &attr);
        } else {
            reply.error(Errno::ENOENT);
        }
    }
}

// ============================================================================
// Test helpers
// ============================================================================

/// Create a unique temporary mount point
fn create_mount_point() -> PathBuf {
    let id = std::process::id();
    let timestamp = std::time::SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let path = std::env::temp_dir().join(format!("neonfs_rust_test_{}_{}", id, timestamp));
    fs::create_dir_all(&path).expect("Failed to create mount point");
    path
}

/// Clean up a mount point, attempting to unmount first if needed
fn cleanup_mount_point(path: &std::path::Path) {
    // Try to unmount in case it's still mounted
    let _ = Command::new("fusermount3")
        .args(["-u", path.to_str().unwrap()])
        .status();
    let _ = Command::new("fusermount")
        .args(["-u", path.to_str().unwrap()])
        .status();

    // Small delay for unmount to complete
    std::thread::sleep(Duration::from_millis(100));

    // Remove the directory
    let _ = fs::remove_dir(path);
}

/// Mount a filesystem and return the session handle
fn mount_filesystem(
    fs: impl Filesystem + 'static,
    mount_point: &std::path::Path,
    read_only: bool,
) -> Option<fuser::BackgroundSession> {
    let mut mount_options = vec![
        MountOption::FSName("neonfs_test".to_string()),
        MountOption::AutoUnmount,
    ];

    if read_only {
        mount_options.push(MountOption::RO);
    }

    let mut config = Config::default();
    config.mount_options = mount_options;
    // fuser 0.17 requires acl != Owner when AutoUnmount is enabled
    config.acl = fuser::SessionACL::RootAndOwner;

    // Record the mount point's inode before mounting so we can detect when
    // the FUSE mount has taken over (its root inode will differ).
    let pre_mount_ino = {
        use std::os::unix::fs::MetadataExt;
        fs::metadata(mount_point).map(|m| m.ino()).ok()
    };

    match fuser::spawn_mount2(fs, mount_point, &config) {
        Ok(session) => {
            // Poll until the mount point's inode changes, indicating the
            // FUSE daemon has completed init and is serving requests.
            let deadline = std::time::Instant::now() + Duration::from_secs(5);
            while std::time::Instant::now() < deadline {
                use std::os::unix::fs::MetadataExt;
                if let Ok(metadata) = fs::metadata(mount_point) {
                    if Some(metadata.ino()) != pre_mount_ino {
                        return Some(session);
                    }
                }
                std::thread::sleep(Duration::from_millis(10));
            }
            eprintln!("Warning: mount may not be fully established after 5s");
            Some(session)
        }
        Err(e) => {
            eprintln!("Mount failed: {}", e);
            None
        }
    }
}

/// Check if a path is mounted
fn is_mounted(path: &std::path::Path) -> bool {
    let output = Command::new("mount")
        .output()
        .expect("Failed to run mount command");
    let mount_output = String::from_utf8_lossy(&output.stdout);
    mount_output.contains(path.to_str().unwrap())
}

// ============================================================================
// Mount lifecycle tests
// ============================================================================

#[test]
fn test_mount_and_unmount() {
    let mount_point = create_mount_point();
    let fs = TestFilesystem::new();

    let session = mount_filesystem(fs, &mount_point, true);
    assert!(session.is_some(), "Mount should succeed");
    assert!(
        is_mounted(&mount_point),
        "Filesystem should appear in mount table"
    );

    // Drop session to unmount
    drop(session);
    std::thread::sleep(Duration::from_millis(200));

    cleanup_mount_point(&mount_point);
}

#[test]
fn test_mount_point_must_exist() {
    let nonexistent = PathBuf::from("/nonexistent/path/that/does/not/exist");

    let fs = TestFilesystem::new();
    let config = Config::default();

    let result = fuser::spawn_mount2(fs, &nonexistent, &config);
    assert!(result.is_err(), "Mount to non-existent path should fail");
}

#[test]
fn test_mount_options() {
    // Test that we can mount with various options
    let mount_point = create_mount_point();
    let fs = TestFilesystem::new();

    let mut config = Config::default();
    config.mount_options = vec![
        MountOption::FSName("neonfs_options_test".to_string()),
        MountOption::AutoUnmount,
        MountOption::RO,
        MountOption::DefaultPermissions,
    ];
    // fuser 0.17 requires acl != Owner when AutoUnmount is enabled
    config.acl = fuser::SessionACL::RootAndOwner;

    let result = fuser::spawn_mount2(fs, &mount_point, &config);
    assert!(result.is_ok(), "Mount with multiple options should succeed");

    // Verify it's mounted
    assert!(
        is_mounted(&mount_point),
        "Filesystem should appear in mount table"
    );

    drop(result);
    std::thread::sleep(Duration::from_millis(200));
    cleanup_mount_point(&mount_point);
}

// ============================================================================
// Directory operations
// ============================================================================

#[test]
fn test_list_root_directory() {
    let mount_point = create_mount_point();
    let fs = TestFilesystem::with_test_files();

    let session = mount_filesystem(fs, &mount_point, true);
    assert!(session.is_some(), "Mount should succeed");

    // List root directory
    let entries: Vec<_> = fs::read_dir(&mount_point)
        .expect("Should read directory")
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().to_string_lossy().to_string())
        .collect();

    assert!(
        entries.contains(&"hello.txt".to_string()),
        "Should contain hello.txt"
    );
    assert!(
        entries.contains(&"subdir".to_string()),
        "Should contain subdir"
    );

    drop(session);
    cleanup_mount_point(&mount_point);
}

#[test]
fn test_list_subdirectory() {
    let mount_point = create_mount_point();
    let fs = TestFilesystem::with_test_files();

    let session = mount_filesystem(fs, &mount_point, true);
    assert!(session.is_some(), "Mount should succeed");

    // List subdirectory
    let subdir = mount_point.join("subdir");
    let entries: Vec<_> = fs::read_dir(&subdir)
        .expect("Should read subdirectory")
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().to_string_lossy().to_string())
        .collect();

    assert!(
        entries.contains(&"nested.txt".to_string()),
        "Subdirectory should contain nested.txt"
    );

    drop(session);
    cleanup_mount_point(&mount_point);
}

#[test]
fn test_create_and_remove_directory() {
    let mount_point = create_mount_point();
    let fs = TestFilesystem::new();

    let session = mount_filesystem(fs, &mount_point, false);
    assert!(session.is_some(), "Mount should succeed");

    let new_dir = mount_point.join("newdir");

    // Create directory
    fs::create_dir(&new_dir).expect("Should create directory");
    assert!(new_dir.exists(), "Directory should exist");
    assert!(new_dir.is_dir(), "Should be a directory");

    // Remove directory
    fs::remove_dir(&new_dir).expect("Should remove directory");
    assert!(
        !new_dir.exists(),
        "Directory should not exist after removal"
    );

    drop(session);
    cleanup_mount_point(&mount_point);
}

// ============================================================================
// File read operations
// ============================================================================

#[test]
fn test_read_file_content() {
    let mount_point = create_mount_point();
    let fs = TestFilesystem::with_test_files();

    let session = mount_filesystem(fs, &mount_point, true);
    assert!(session.is_some(), "Mount should succeed");

    // Read file content
    let file_path = mount_point.join("hello.txt");
    let content = fs::read_to_string(&file_path).expect("Should read file");

    assert_eq!(content, "Hello, FUSE!\n", "File content should match");

    drop(session);
    cleanup_mount_point(&mount_point);
}

#[test]
fn test_read_nested_file() {
    let mount_point = create_mount_point();
    let fs = TestFilesystem::with_test_files();

    let session = mount_filesystem(fs, &mount_point, true);
    assert!(session.is_some(), "Mount should succeed");

    // Read nested file
    let file_path = mount_point.join("subdir").join("nested.txt");
    let content = fs::read_to_string(&file_path).expect("Should read nested file");

    assert_eq!(
        content, "Nested content\n",
        "Nested file content should match"
    );

    drop(session);
    cleanup_mount_point(&mount_point);
}

#[test]
fn test_read_nonexistent_file() {
    let mount_point = create_mount_point();
    let fs = TestFilesystem::with_test_files();

    let session = mount_filesystem(fs, &mount_point, true);
    assert!(session.is_some(), "Mount should succeed");

    let file_path = mount_point.join("nonexistent.txt");
    let result = fs::read_to_string(&file_path);

    assert!(result.is_err(), "Reading non-existent file should fail");

    drop(session);
    cleanup_mount_point(&mount_point);
}

#[test]
fn test_file_metadata() {
    let mount_point = create_mount_point();
    let fs = TestFilesystem::with_test_files();

    let session = mount_filesystem(fs, &mount_point, true);
    assert!(session.is_some(), "Mount should succeed");

    let file_path = mount_point.join("hello.txt");
    let metadata = fs::metadata(&file_path).expect("Should get metadata");

    assert!(metadata.is_file(), "Should be a file");
    assert_eq!(metadata.len(), 13, "File size should be 13 bytes");

    let dir_path = mount_point.join("subdir");
    let dir_metadata = fs::metadata(&dir_path).expect("Should get directory metadata");

    assert!(dir_metadata.is_dir(), "Should be a directory");

    drop(session);
    cleanup_mount_point(&mount_point);
}

// ============================================================================
// File write operations
// ============================================================================

#[test]
fn test_create_and_write_file() {
    let mount_point = create_mount_point();
    let fs = TestFilesystem::new();

    let session = mount_filesystem(fs, &mount_point, false);
    assert!(session.is_some(), "Mount should succeed");

    let file_path = mount_point.join("newfile.txt");
    let test_content = "This is new content\n";

    // Create and write file
    fs::write(&file_path, test_content).expect("Should write file");

    // Read it back
    let read_content = fs::read_to_string(&file_path).expect("Should read file");
    assert_eq!(read_content, test_content, "Content should match");

    drop(session);
    cleanup_mount_point(&mount_point);
}

#[test]
fn test_append_to_file() {
    let mount_point = create_mount_point();
    let fs = TestFilesystem::new();

    let session = mount_filesystem(fs, &mount_point, false);
    assert!(session.is_some(), "Mount should succeed");

    let file_path = mount_point.join("appendfile.txt");

    // Create file with initial content
    fs::write(&file_path, "Initial\n").expect("Should write initial content");

    // Append more content
    let mut file = File::options()
        .append(true)
        .open(&file_path)
        .expect("Should open for append");
    file.write_all(b"Appended\n").expect("Should append");
    drop(file);

    // Read back
    let content = fs::read_to_string(&file_path).expect("Should read file");
    assert_eq!(
        content, "Initial\nAppended\n",
        "Content should include both parts"
    );

    drop(session);
    cleanup_mount_point(&mount_point);
}

#[test]
fn test_delete_file() {
    let mount_point = create_mount_point();
    let fs = TestFilesystem::new();

    let session = mount_filesystem(fs, &mount_point, false);
    assert!(session.is_some(), "Mount should succeed");

    let file_path = mount_point.join("deleteme.txt");

    // Create file
    fs::write(&file_path, "delete me").expect("Should write file");
    assert!(file_path.exists(), "File should exist");

    // Delete file
    fs::remove_file(&file_path).expect("Should delete file");
    assert!(!file_path.exists(), "File should not exist after deletion");

    drop(session);
    cleanup_mount_point(&mount_point);
}

// ============================================================================
// Rename operations
// ============================================================================

#[test]
fn test_rename_file() {
    let mount_point = create_mount_point();
    let fs = TestFilesystem::new();

    let session = mount_filesystem(fs, &mount_point, false);
    assert!(session.is_some(), "Mount should succeed");

    let old_path = mount_point.join("oldname.txt");
    let new_path = mount_point.join("newname.txt");

    // Create file
    fs::write(&old_path, "content").expect("Should write file");
    assert!(old_path.exists(), "Old file should exist");

    // Rename
    fs::rename(&old_path, &new_path).expect("Should rename file");

    assert!(!old_path.exists(), "Old path should not exist");
    assert!(new_path.exists(), "New path should exist");

    // Content should be preserved
    let content = fs::read_to_string(&new_path).expect("Should read renamed file");
    assert_eq!(content, "content", "Content should be preserved");

    drop(session);
    cleanup_mount_point(&mount_point);
}

#[test]
fn test_move_file_to_subdirectory() {
    let mount_point = create_mount_point();
    let fs = TestFilesystem::new();

    let session = mount_filesystem(fs, &mount_point, false);
    assert!(session.is_some(), "Mount should succeed");

    // Create directory and file
    let subdir = mount_point.join("subdir");
    fs::create_dir(&subdir).expect("Should create subdir");

    let file_path = mount_point.join("moveme.txt");
    fs::write(&file_path, "moving content").expect("Should write file");

    // Move file to subdirectory
    let new_path = subdir.join("moved.txt");
    fs::rename(&file_path, &new_path).expect("Should move file");

    assert!(!file_path.exists(), "Original should not exist");
    assert!(new_path.exists(), "Moved file should exist");

    let content = fs::read_to_string(&new_path).expect("Should read moved file");
    assert_eq!(content, "moving content", "Content should be preserved");

    drop(session);
    cleanup_mount_point(&mount_point);
}

// ============================================================================
// Error handling
// ============================================================================

#[test]
fn test_rmdir_nonempty_fails() {
    let mount_point = create_mount_point();
    let fs = TestFilesystem::new();

    let session = mount_filesystem(fs, &mount_point, false);
    assert!(session.is_some(), "Mount should succeed");

    // Create directory with file inside
    let dir_path = mount_point.join("nonempty");
    fs::create_dir(&dir_path).expect("Should create directory");

    let file_path = dir_path.join("file.txt");
    fs::write(&file_path, "content").expect("Should write file");

    // Try to remove non-empty directory
    let result = fs::remove_dir(&dir_path);
    assert!(result.is_err(), "Removing non-empty directory should fail");

    // Directory should still exist
    assert!(dir_path.exists(), "Directory should still exist");

    drop(session);
    cleanup_mount_point(&mount_point);
}

#[test]
fn test_create_existing_file_truncates() {
    let mount_point = create_mount_point();
    let fs = TestFilesystem::new();

    let session = mount_filesystem(fs, &mount_point, false);
    assert!(session.is_some(), "Mount should succeed");

    let file_path = mount_point.join("truncate.txt");

    // Create file with content
    fs::write(&file_path, "original long content").expect("Should write file");

    // Overwrite with shorter content
    fs::write(&file_path, "short").expect("Should overwrite file");

    let content = fs::read_to_string(&file_path).expect("Should read file");
    assert_eq!(content, "short", "Content should be replaced");

    drop(session);
    cleanup_mount_point(&mount_point);
}

// ============================================================================
// fusermount functionality
// ============================================================================

#[test]
fn test_fusermount_available() {
    let output = Command::new("fusermount3")
        .arg("--version")
        .output()
        .expect("Failed to run fusermount3");

    assert!(
        output.status.success() || !output.stderr.is_empty(),
        "fusermount3 --version should succeed or produce output"
    );

    // The key test: fusermount3 didn't fail with "waitpid: No child processes"
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        !stderr.contains("waitpid: No child processes"),
        "fusermount should not have SIGCHLD issues in Rust tests"
    );
}
