pub mod hash;
pub mod path;

use rustler::{Binary, Env, NewBinary};

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

rustler::init!("Elixir.NeonFS.Core.Blob.Native");
