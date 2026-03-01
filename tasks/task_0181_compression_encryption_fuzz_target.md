# Task 0181: Compression and Encryption Fuzz Target

## Status
Blocked — depends on 0179 (fuzz scaffolding); needs CI design

## Phase
Gap Analysis — H-3 (3/5)

## Description
Create a fuzz target that exercises the compression and encryption code
paths in `neonfs_blob`. This target feeds arbitrary data into compression
round-trips (compress then decompress) and encryption round-trips (encrypt
then decrypt) to find crashes or data corruption.

## Acceptance Criteria
- [ ] `neonfs_core/native/neonfs_blob/fuzz/fuzz_targets/compression.rs` created
- [ ] Fuzz target exercises LZ4 compression round-trip: `compress(data)` then `decompress(result)`
- [ ] Fuzz target exercises Zstandard compression round-trip if supported
- [ ] Fuzz target exercises decompression of arbitrary (potentially invalid) compressed data
- [ ] Fuzz target exercises AES-256-GCM encryption round-trip: `encrypt(data, key, nonce)` then `decrypt(result, key, nonce)`
- [ ] Fuzz target exercises decryption of arbitrary (potentially invalid) ciphertext
- [ ] All functions handle malformed input without panicking
- [ ] Compression round-trip property: `decompress(compress(data)) == data` for all valid data
- [ ] Encryption round-trip property: `decrypt(encrypt(data, k, n), k, n) == data` for all valid data
- [ ] Seed corpus created at `fuzz/corpus/compression/` with:
  - Valid LZ4 compressed data
  - Valid AES-256-GCM ciphertext
  - Empty input
  - Highly compressible input (repeated bytes)
  - Incompressible input (random bytes)
- [ ] 5-minute local fuzz run completes without finding crashes
- [ ] `cargo fuzz list` shows `compression` target

## Testing Strategy
- Run `cargo fuzz run compression -- -max_total_time=300` (5 minutes)
- Verify no crashes or panics found
- Verify no round-trip property violations (data corruption)

## Dependencies
- Task 0179 (neonfs_blob cargo-fuzz scaffolding)

## Files to Create/Modify
- `neonfs_core/native/neonfs_blob/fuzz/fuzz_targets/compression.rs` (create — fuzz target)
- `neonfs_core/native/neonfs_blob/fuzz/corpus/compression/` (create — seed corpus files)

## Reference
- `spec/gap-analysis.md` — H-3
- `spec/testing.md` lines 450–470 (compression fuzz target)
- Existing: `neonfs_core/native/neonfs_blob/src/` (compression and encryption modules)

## Notes
The fuzz target should use structured fuzzing where possible. For the
encryption round-trip, derive a fixed key and nonce from the fuzz input
rather than using the raw bytes directly:

```rust
fuzz_target!(|data: &[u8]| {
    // Compression round-trip
    if let Ok(compressed) = compress(data) {
        let decompressed = decompress(&compressed).expect("round-trip must succeed");
        assert_eq!(data, &decompressed[..]);
    }

    // Decompression of arbitrary data (should not panic)
    let _ = decompress(data);

    // Encryption round-trip (use fixed key/nonce)
    let key = [0x42u8; 32];
    let nonce = [0x00u8; 12];
    if let Ok(ciphertext) = encrypt(data, &key, &nonce) {
        let plaintext = decrypt(&ciphertext, &key, &nonce).expect("round-trip must succeed");
        assert_eq!(data, &plaintext[..]);
    }

    // Decryption of arbitrary data (should return Err, not panic)
    let _ = decrypt(data, &key, &nonce);
});
```

The key insight: round-trip failures (assert_eq fails) are as important as
panics. The fuzzer catches both.
