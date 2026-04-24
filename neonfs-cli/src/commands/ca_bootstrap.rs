//! Emergency CA bootstrap helpers (#503).
//!
//! This slice (B.1) carries the tarball **validation** layer that gates
//! the real install. Install itself + node cert regeneration land in a
//! follow-up (slice B.2). The validation layer is:
//!
//!   1. Open the backup tarball (gzipped tar) and enumerate entries.
//!   2. Confirm every expected member is present (`ca.crt`, `ca.key`,
//!      `serial`, `crl.pem`).
//!   3. Parse the CA certificate's subject and extract the cluster name
//!      (`/O=NeonFS/CN=<cluster_name> CA`).
//!   4. Read the local `cluster.json` and compare the cluster name with
//!      the tarball's CA subject. Refuse if they disagree — an operator
//!      pointing the wrong backup at the wrong cluster would overwrite
//!      the current CA material with foreign certs.
//!
//! Kept deliberately out of scope:
//!   - Live-service refusal (handled by the Rust CLI at step 0 via the
//!     daemon connection check — see slice B.2).
//!   - Atomic install, node cert regeneration, audit-log emission —
//!     slice B.2.
//!   - `--new-key` fresh-CA generation — slice B.2.

use crate::error::{CliError, Result};
use flate2::read::GzDecoder;
use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{BufReader, Read};
use std::path::{Path, PathBuf};
use tar::Archive;

/// Files the emergency-bootstrap tarball MUST carry. Any other member
/// is ignored — we neither reject nor extract extraneous files here.
pub const REQUIRED_MEMBERS: &[&str] = &["ca.crt", "ca.key", "serial", "crl.pem"];

/// Default on-disk layout. Overridable via `NEONFS_DATA_DIR`.
const DEFAULT_DATA_DIR: &str = "/var/lib/neonfs";

/// Outcome of validating a CA backup tarball.
#[derive(Debug, PartialEq, Eq)]
pub struct BackupValidation {
    /// Cluster name extracted from the tarball's `ca.crt` subject CN.
    /// The CA subject follows the `/O=NeonFS/CN=<cluster_name> CA`
    /// convention set by `NeonFS.Transport.TLS.generate_ca/1`.
    pub cluster_name: String,
    /// Extracted tarball members keyed by basename. Exactly the files
    /// in [`REQUIRED_MEMBERS`] — other entries in the tarball are
    /// ignored. Kept on the validation struct so callers (e.g. the
    /// atomic-install step) do not have to re-parse the tarball.
    pub entries: HashMap<String, Vec<u8>>,
}

/// Validate a CA backup tarball and extract the cluster name it belongs
/// to. The caller is responsible for comparing that name against the
/// local cluster.
pub fn validate_backup_tarball(path: &Path) -> Result<BackupValidation> {
    let file = File::open(path).map_err(|e| io_err("open backup tarball", path, e))?;

    let entries = read_tar_members(BufReader::new(file), path)?;
    ensure_required_members(&entries, path)?;

    let ca_crt = entries
        .get("ca.crt")
        .expect("ensure_required_members covers ca.crt");
    let cluster_name = extract_cluster_name_from_ca_cert(ca_crt)?;

    Ok(BackupValidation {
        cluster_name,
        entries,
    })
}

/// Read the local cluster's name from `$NEONFS_DATA_DIR/meta/cluster.json`.
///
/// Surfaces a [`CliError::InvalidArgument`] when the file is missing, is
/// not readable, is not JSON, or does not contain a `cluster_name`
/// string field. Each failure mode names the path involved so an
/// operator can act without re-deriving the env.
pub fn local_cluster_name() -> Result<String> {
    let cluster_json = local_cluster_json_path();
    // audit:bounded cluster.json is a NeonFS-written config (not user data).
    let contents = std::fs::read_to_string(&cluster_json).map_err(|e| {
        CliError::InvalidArgument(format!(
            "cannot read local cluster state at {}: {e}. \
             Confirm `NEONFS_DATA_DIR` points at this node's data directory.",
            cluster_json.display()
        ))
    })?;

    let parsed: serde_json::Value = serde_json::from_str(&contents).map_err(|e| {
        CliError::InvalidArgument(format!(
            "local cluster state at {} is not valid JSON: {e}",
            cluster_json.display()
        ))
    })?;

    parsed
        .get("cluster_name")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
        .ok_or_else(|| {
            CliError::InvalidArgument(format!(
                "local cluster state at {} is missing the `cluster_name` field.",
                cluster_json.display()
            ))
        })
}

/// Reject the backup if its CA belongs to a different cluster than this
/// node's. Returns the validated backup on success.
pub fn refuse_foreign_backup(
    validation: BackupValidation,
    local_name: &str,
) -> Result<BackupValidation> {
    if validation.cluster_name == local_name {
        Ok(validation)
    } else {
        Err(CliError::InvalidArgument(format!(
            "backup tarball belongs to cluster '{}', but this node's local cluster is '{}'. \
             Refusing — emergency-bootstrap must run against a backup of the SAME cluster. \
             If this is intentional (rebuilding a cluster with a different backup), edit \
             `cluster.json` first and try again.",
            validation.cluster_name, local_name
        )))
    }
}

/// Atomically install the validated CA material to `tls_dir`.
///
/// Each member is written to a hidden staging directory inside `tls_dir`,
/// fsynced, and then renamed into place. `fs::rename` is atomic on POSIX
/// for same-filesystem renames, so per-file the install is either
/// fully-present or fully-absent. Across files the install is not atomic —
/// if a rename fails partway, some members may land and others not. The
/// operator can recover by re-running `emergency-bootstrap --from-backup`
/// with the same tarball (install is idempotent).
///
/// The staging directory is cleaned up on function return via an RAII
/// guard, whether the install succeeded or failed.
pub fn install_ca_material(validation: &BackupValidation, tls_dir: &Path) -> Result<()> {
    fs::create_dir_all(tls_dir).map_err(|e| io_err("create TLS directory", tls_dir, e))?;

    let stage_dir = tls_dir.join(format!(".emergency-bootstrap-{}", std::process::id()));
    fs::create_dir_all(&stage_dir)
        .map_err(|e| io_err("create staging directory", &stage_dir, e))?;

    let _guard = StageDirGuard(stage_dir.clone());

    // Stage every required member — write + fsync. Fail fast if any
    // entry is missing from `validation.entries` (shouldn't happen if
    // `validate_backup_tarball` produced the value, but defensively
    // check rather than `expect` in a public API path).
    for member in REQUIRED_MEMBERS {
        let data = validation.entries.get(*member).ok_or_else(|| {
            CliError::InvalidArgument(format!(
                "BackupValidation is missing required member `{member}` — \
                 was it constructed by a path other than validate_backup_tarball/1?"
            ))
        })?;

        let staged = stage_dir.join(member);
        fs::write(&staged, data)
            .map_err(|e| io_err(&format!("write staged `{}`", member), &staged, e))?;
        File::open(&staged)
            .and_then(|f| f.sync_all())
            .map_err(|e| io_err(&format!("fsync staged `{}`", member), &staged, e))?;
    }

    // Atomically rename each staged file into place. Per-file atomicity
    // only; see function docs above.
    for member in REQUIRED_MEMBERS {
        let src = stage_dir.join(member);
        let dst = tls_dir.join(member);
        fs::rename(&src, &dst).map_err(|e| io_err(&format!("install `{}`", member), &dst, e))?;
    }

    // fsync the directory so the renames are durable across a crash.
    File::open(tls_dir)
        .and_then(|f| f.sync_all())
        .map_err(|e| io_err("fsync TLS directory", tls_dir, e))?;

    Ok(())
}

/// RAII guard that removes the staging directory when dropped,
/// whether the install succeeded or failed.
struct StageDirGuard(PathBuf);

impl Drop for StageDirGuard {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

// ─── Internals ───────────────────────────────────────────────────────

fn local_cluster_json_path() -> PathBuf {
    let data_dir =
        std::env::var("NEONFS_DATA_DIR").unwrap_or_else(|_| DEFAULT_DATA_DIR.to_string());
    PathBuf::from(data_dir).join("meta").join("cluster.json")
}

fn read_tar_members<R: Read>(reader: R, path: &Path) -> Result<HashMap<String, Vec<u8>>> {
    let gz = GzDecoder::new(reader);
    let mut archive = Archive::new(gz);
    let mut out = HashMap::new();

    let entries = archive
        .entries()
        .map_err(|e| io_err("enumerate tarball entries", path, e))?;

    for entry in entries {
        let mut entry = entry.map_err(|e| io_err("read tarball entry", path, e))?;
        let entry_path = entry
            .path()
            .map_err(|e| io_err("read tarball entry path", path, e))?
            .into_owned();

        let name = entry_path
            .file_name()
            .and_then(|n| n.to_str())
            .map(|s| s.to_string());

        let name = match name {
            Some(n) => n,
            // Entries with non-UTF-8 names are ignored — they cannot be
            // one of our required members.
            None => continue,
        };

        if !REQUIRED_MEMBERS.contains(&name.as_str()) {
            continue;
        }

        let mut buf = Vec::new();
        // audit:bounded CA material (PEM / serial / CRL — tens of KB ceiling).
        entry
            .read_to_end(&mut buf)
            .map_err(|e| io_err(&format!("read tarball entry `{}`", name), path, e))?;
        out.insert(name, buf);
    }

    Ok(out)
}

fn ensure_required_members(entries: &HashMap<String, Vec<u8>>, path: &Path) -> Result<()> {
    let missing: Vec<&str> = REQUIRED_MEMBERS
        .iter()
        .copied()
        .filter(|m| !entries.contains_key(*m))
        .collect();

    if missing.is_empty() {
        Ok(())
    } else {
        Err(CliError::InvalidArgument(format!(
            "backup tarball at {} is missing required files: {}. \
             The tarball must contain: {}.",
            path.display(),
            missing.join(", "),
            REQUIRED_MEMBERS.join(", ")
        )))
    }
}

fn extract_cluster_name_from_ca_cert(pem_bytes: &[u8]) -> Result<String> {
    let (_, cert) = x509_parser::pem::parse_x509_pem(pem_bytes).map_err(|e| {
        CliError::InvalidArgument(format!("ca.crt in backup tarball is not valid PEM: {e:?}"))
    })?;

    let (_, x509) = x509_parser::parse_x509_certificate(&cert.contents).map_err(|e| {
        CliError::InvalidArgument(format!(
            "ca.crt in backup tarball is not a valid X.509 certificate: {e:?}"
        ))
    })?;

    let subject = x509.subject();
    let cn = subject
        .iter_common_name()
        .next()
        .ok_or_else(|| {
            CliError::InvalidArgument(
                "ca.crt subject is missing a CN attribute — expected `/O=NeonFS/CN=<cluster> CA`."
                    .to_string(),
            )
        })?
        .as_str()
        .map_err(|e| {
            CliError::InvalidArgument(format!("ca.crt subject CN is not valid UTF-8: {e:?}"))
        })?;

    // CA subject convention: `<cluster_name> CA`. Strip the trailing " CA".
    // If it's not there the CA wasn't generated by `NeonFS.Transport.TLS.generate_ca/1`;
    // accept the CN as-is and let the operator judge by eye.
    let cluster_name = cn.strip_suffix(" CA").unwrap_or(cn).trim().to_string();

    if cluster_name.is_empty() {
        Err(CliError::InvalidArgument(
            "ca.crt subject CN is empty after stripping the trailing ' CA' — cannot identify the \
             cluster."
                .to_string(),
        ))
    } else {
        Ok(cluster_name)
    }
}

fn io_err(what: &str, path: &Path, e: std::io::Error) -> CliError {
    CliError::InvalidArgument(format!("cannot {} at {}: {e}", what, path.display()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::TempDir;

    fn write_test_tarball(dir: &TempDir, contents: &[(&str, &[u8])]) -> PathBuf {
        let path = dir.path().join("ca-backup.tar.gz");
        let file = File::create(&path).unwrap();
        let gz = flate2::write::GzEncoder::new(file, flate2::Compression::default());
        let mut builder = tar::Builder::new(gz);

        for (name, data) in contents {
            let mut header = tar::Header::new_gnu();
            header.set_size(data.len() as u64);
            header.set_mode(0o644);
            header.set_cksum();
            builder.append_data(&mut header, name, *data).unwrap();
        }

        let gz = builder.into_inner().unwrap();
        gz.finish().unwrap();
        path
    }

    // Self-signed ECDSA P-256 CA with subject `/O=NeonFS/CN=parity-test CA`,
    // generated once with `openssl req -new -x509 -key … -subj …` and pinned
    // here so the test is deterministic and does not depend on runtime key
    // generation. Validity: 2026-04-24 → 2036-04-21. If this starts failing
    // because the cert expired, regenerate with:
    //
    //     openssl ecparam -name prime256v1 -genkey -noout -out ca.key
    //     openssl req -new -x509 -key ca.key \
    //       -subj "/O=NeonFS/CN=parity-test CA" -days 3650 -out ca.crt
    const PARITY_CA_PEM: &[u8] = b"-----BEGIN CERTIFICATE-----
MIIBqTCCAU+gAwIBAgIUU9IrMrpKYbrsbkDmN5ovsLqm9y4wCgYIKoZIzj0EAwIw
KjEPMA0GA1UECgwGTmVvbkZTMRcwFQYDVQQDDA5wYXJpdHktdGVzdCBDQTAeFw0y
NjA0MjQyMTA0MjBaFw0zNjA0MjEyMTA0MjBaMCoxDzANBgNVBAoMBk5lb25GUzEX
MBUGA1UEAwwOcGFyaXR5LXRlc3QgQ0EwWTATBgcqhkjOPQIBBggqhkjOPQMBBwNC
AAT7lgtd6feBcsaqyZTxnts/IeDhYwOzakz3SNodfuTtFh8MWizuUIL1GgmFAaNQ
1h3f6GjY+0PnoOipo0Z9qMWio1MwUTAdBgNVHQ4EFgQUDJdYOEfffSxCcBuBgd1c
rLxWiSMwHwYDVR0jBBgwFoAUDJdYOEfffSxCcBuBgd1crLxWiSMwDwYDVR0TAQH/
BAUwAwEB/zAKBggqhkjOPQQDAgNIADBFAiACTei1FcxUGVhalh/7CJPM5Jsedhwg
FGS64EIpGW9fvwIhALGssHz1WS2xFXKoUHPlzvIQqBOrfTFFY8jlHJT3X/ru
-----END CERTIFICATE-----
";

    #[test]
    fn extract_cluster_name_strips_trailing_ca() {
        let name = extract_cluster_name_from_ca_cert(PARITY_CA_PEM).unwrap();
        assert_eq!(name, "parity-test");
    }

    #[test]
    fn extract_cluster_name_rejects_invalid_pem() {
        let err = extract_cluster_name_from_ca_cert(b"not-a-pem").unwrap_err();
        match err {
            CliError::InvalidArgument(msg) => assert!(msg.contains("not valid PEM")),
            other => panic!("expected InvalidArgument, got {other:?}"),
        }
    }

    #[test]
    fn validate_backup_tarball_success() {
        let dir = TempDir::new().unwrap();
        let path = write_test_tarball(
            &dir,
            &[
                ("ca.crt", PARITY_CA_PEM),
                ("ca.key", b"PRIVATE KEY STUB"),
                ("serial", b"42\n"),
                ("crl.pem", b"CRL STUB"),
            ],
        );

        let validation = validate_backup_tarball(&path).unwrap();
        assert_eq!(validation.cluster_name, "parity-test");
    }

    #[test]
    fn validate_backup_tarball_missing_member() {
        let dir = TempDir::new().unwrap();
        let path = write_test_tarball(
            &dir,
            &[
                ("ca.crt", PARITY_CA_PEM),
                ("ca.key", b"PRIVATE KEY STUB"),
                // missing serial
                ("crl.pem", b"CRL STUB"),
            ],
        );

        let err = validate_backup_tarball(&path).unwrap_err();
        match err {
            CliError::InvalidArgument(msg) => {
                assert!(msg.contains("missing required files"));
                assert!(msg.contains("serial"));
            }
            other => panic!("expected InvalidArgument, got {other:?}"),
        }
    }

    #[test]
    fn validate_backup_tarball_ignores_extra_members() {
        let dir = TempDir::new().unwrap();
        let path = write_test_tarball(
            &dir,
            &[
                ("ca.crt", PARITY_CA_PEM),
                ("ca.key", b"PRIVATE KEY STUB"),
                ("serial", b"42\n"),
                ("crl.pem", b"CRL STUB"),
                ("node.crt", b"NODE CERT STUB"),
                ("ssl_dist.conf", b"DIST CONF STUB"),
            ],
        );

        // Extra members are ignored, not rejected.
        let validation = validate_backup_tarball(&path).unwrap();
        assert_eq!(validation.cluster_name, "parity-test");
    }

    fn make_validation(name: &str) -> BackupValidation {
        BackupValidation {
            cluster_name: name.to_string(),
            entries: [
                ("ca.crt", PARITY_CA_PEM.to_vec()),
                ("ca.key", b"PRIVATE KEY STUB".to_vec()),
                ("serial", b"42\n".to_vec()),
                ("crl.pem", b"CRL STUB".to_vec()),
            ]
            .iter()
            .map(|(k, v)| (k.to_string(), v.clone()))
            .collect(),
        }
    }

    #[test]
    fn refuse_foreign_backup_matches_local() {
        let validation = make_validation("cluster-alpha");
        let ok = refuse_foreign_backup(validation, "cluster-alpha").unwrap();
        assert_eq!(ok.cluster_name, "cluster-alpha");
    }

    #[test]
    fn refuse_foreign_backup_rejects_mismatch() {
        let validation = make_validation("cluster-alpha");
        let err = refuse_foreign_backup(validation, "cluster-beta").unwrap_err();
        match err {
            CliError::InvalidArgument(msg) => {
                assert!(msg.contains("cluster-alpha"));
                assert!(msg.contains("cluster-beta"));
                assert!(msg.contains("SAME cluster"));
            }
            other => panic!("expected InvalidArgument, got {other:?}"),
        }
    }

    #[test]
    fn install_writes_every_required_member() {
        let dir = TempDir::new().unwrap();
        let tls_dir = dir.path().join("tls");

        let validation = make_validation("install-test");
        install_ca_material(&validation, &tls_dir).unwrap();

        for member in REQUIRED_MEMBERS {
            let path = tls_dir.join(member);
            assert!(path.exists(), "{member} not installed");

            let on_disk = std::fs::read(&path).unwrap();
            let expected = validation.entries.get(*member).unwrap();
            assert_eq!(&on_disk, expected, "contents mismatch for {member}");
        }
    }

    #[test]
    fn install_overwrites_existing_members() {
        let dir = TempDir::new().unwrap();
        let tls_dir = dir.path().join("tls");
        std::fs::create_dir_all(&tls_dir).unwrap();

        // Seed the tls_dir with stale material that should be replaced.
        std::fs::write(tls_dir.join("ca.crt"), b"STALE CA").unwrap();
        std::fs::write(tls_dir.join("ca.key"), b"STALE KEY").unwrap();
        std::fs::write(tls_dir.join("serial"), b"1").unwrap();
        std::fs::write(tls_dir.join("crl.pem"), b"STALE CRL").unwrap();

        // Also seed non-member files that install should leave alone.
        std::fs::write(tls_dir.join("local-ca.crt"), b"KEEP ME").unwrap();
        std::fs::write(tls_dir.join("cli.crt"), b"KEEP ME TOO").unwrap();

        let validation = make_validation("install-test");
        install_ca_material(&validation, &tls_dir).unwrap();

        // Replaced: contents match the validation payload.
        assert_eq!(
            std::fs::read(tls_dir.join("ca.crt")).unwrap(),
            *validation.entries.get("ca.crt").unwrap()
        );

        // Preserved: unrelated files are untouched.
        assert_eq!(
            std::fs::read(tls_dir.join("local-ca.crt")).unwrap(),
            b"KEEP ME"
        );
        assert_eq!(
            std::fs::read(tls_dir.join("cli.crt")).unwrap(),
            b"KEEP ME TOO"
        );
    }

    #[test]
    fn install_cleans_up_staging_directory() {
        let dir = TempDir::new().unwrap();
        let tls_dir = dir.path().join("tls");

        let validation = make_validation("install-test");
        install_ca_material(&validation, &tls_dir).unwrap();

        // No `.emergency-bootstrap-*` staging directory left behind.
        let leftover: Vec<_> = std::fs::read_dir(&tls_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| {
                e.file_name()
                    .to_string_lossy()
                    .starts_with(".emergency-bootstrap-")
            })
            .collect();

        assert!(
            leftover.is_empty(),
            "staging directory not cleaned up: {:?}",
            leftover.iter().map(|e| e.path()).collect::<Vec<_>>()
        );
    }

    #[test]
    fn install_creates_tls_dir_if_missing() {
        let dir = TempDir::new().unwrap();
        // tls_dir is two levels deep and does not yet exist.
        let tls_dir = dir.path().join("nested").join("tls");

        let validation = make_validation("install-test");
        install_ca_material(&validation, &tls_dir).unwrap();

        for member in REQUIRED_MEMBERS {
            assert!(tls_dir.join(member).exists(), "{member} not installed");
        }
    }

    #[test]
    fn local_cluster_name_missing_file() {
        // Point NEONFS_DATA_DIR at a non-existent dir and ensure the
        // error message names that dir.
        let dir = TempDir::new().unwrap();
        let nonexistent = dir.path().join("does-not-exist");

        let old = std::env::var("NEONFS_DATA_DIR").ok();
        // SAFETY: serialise with `--test-threads=1` or via separate
        // tests that don't also mutate this env var. In our test
        // module no other test touches `NEONFS_DATA_DIR`.
        unsafe {
            std::env::set_var("NEONFS_DATA_DIR", &nonexistent);
        }

        let err = local_cluster_name().unwrap_err();

        match old {
            Some(v) => unsafe { std::env::set_var("NEONFS_DATA_DIR", v) },
            None => unsafe { std::env::remove_var("NEONFS_DATA_DIR") },
        }

        match err {
            CliError::InvalidArgument(msg) => {
                assert!(msg.contains("cannot read local cluster state"));
                assert!(msg.contains("cluster.json"));
            }
            other => panic!("expected InvalidArgument, got {other:?}"),
        }
    }

    #[test]
    fn local_cluster_name_reads_json() {
        let dir = TempDir::new().unwrap();
        let meta_dir = dir.path().join("meta");
        std::fs::create_dir_all(&meta_dir).unwrap();
        let cluster_json = meta_dir.join("cluster.json");

        let mut f = File::create(&cluster_json).unwrap();
        f.write_all(br#"{"cluster_name":"prod-west-1","cluster_id":"abc"}"#)
            .unwrap();

        let old = std::env::var("NEONFS_DATA_DIR").ok();
        unsafe {
            std::env::set_var("NEONFS_DATA_DIR", dir.path());
        }

        let name = local_cluster_name();

        match old {
            Some(v) => unsafe { std::env::set_var("NEONFS_DATA_DIR", v) },
            None => unsafe { std::env::remove_var("NEONFS_DATA_DIR") },
        }

        assert_eq!(name.unwrap(), "prod-west-1");
    }
}
