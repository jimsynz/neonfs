//! Emergency CA bootstrap helpers (#503).
//!
//! Slice ordering (for future edits to this module):
//!
//!   - **B.1** (#503) — Tarball validation: open backup, enumerate
//!     entries, confirm required members present, parse CA subject,
//!     compare against local `cluster.json`.
//!   - **B.2a** (#516) — Atomic on-disk install of validated members
//!     to `$NEONFS_TLS_DIR/` via stage + fsync + rename.
//!   - **B.2b.1** (#518) — Live-service refusal: TCP probe on the
//!     distribution port read from `/run/neonfs/dist_port`. Refuses
//!     the bootstrap if the daemon appears to be running.
//!   - **B.2b.2** (#518) — Node cert regeneration: after install,
//!     generate a fresh ECDSA P-256 keypair, build a cert with
//!     subject `/O=NeonFS/CN=neonfs_core@<host>` + SAN `[hostname]` +
//!     extended key usage `ServerAuth + ClientAuth`, sign with the
//!     installed CA key via rcgen, atomically write `node.crt` +
//!     `node.key` + bumped `serial` to `$NEONFS_TLS_DIR/`.
//!
//! Still deferred to follow-ups under #518:
//!   - `--new-key` fresh-CA generation.
//!   - Audit-log emission (JSONL at `$NEONFS_DATA_DIR/audit/…`).

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

/// Refuse emergency-bootstrap if the local `neonfs-core` daemon appears
/// to be running.
///
/// Detects the daemon by reading the distribution port from
/// `/run/neonfs/dist_port` (overridable via the `NEONFS_DIST_PORT_FILE`
/// env var, mostly for tests) and attempting a TCP connect. If the file
/// is absent or the port is not responding, we treat that as "daemon is
/// stopped" and allow the bootstrap to proceed.
///
/// The signal is deliberately permissive-on-absence — the port file is
/// written by the release wrapper but may not exist in every deployment
/// shape (operator-built releases, CI environments). False negatives on
/// a running daemon are the worst outcome, so prefer refusal when the
/// probe succeeds.
pub fn refuse_if_daemon_live() -> Result<()> {
    refuse_if_daemon_live_with(&read_dist_port, &probe_local_port)
}

type DistPortReader = dyn Fn() -> Option<u16>;
type PortProber = dyn Fn(u16) -> bool;

/// Test-seam variant of [`refuse_if_daemon_live`]. The public
/// [`refuse_if_daemon_live`] wires the real filesystem + TCP probes.
fn refuse_if_daemon_live_with(read_port: &DistPortReader, probe: &PortProber) -> Result<()> {
    let Some(port) = read_port() else {
        // No port file → treat as "daemon is stopped".
        return Ok(());
    };

    if probe(port) {
        Err(CliError::InvalidArgument(format!(
            "neonfs-core daemon appears to be running on distribution port {port}. \
             Stop the service (`systemctl stop neonfs-core` or equivalent) before \
             running emergency-bootstrap — otherwise the daemon would continue \
             holding stale CA material in memory after the install, and clients \
             connecting during the window would see trust-chain errors."
        )))
    } else {
        Ok(())
    }
}

fn read_dist_port() -> Option<u16> {
    let path = std::env::var("NEONFS_DIST_PORT_FILE")
        .unwrap_or_else(|_| "/run/neonfs/dist_port".to_string());
    // audit:bounded dist_port is a single line of ASCII digits.
    let contents = std::fs::read_to_string(&path).ok()?;
    contents.trim().parse().ok()
}

fn probe_local_port(port: u16) -> bool {
    use std::net::{SocketAddr, TcpStream};
    use std::time::Duration;

    let Ok(addr) = format!("127.0.0.1:{port}").parse::<SocketAddr>() else {
        return false;
    };

    TcpStream::connect_timeout(&addr, Duration::from_millis(500)).is_ok()
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

/// Regenerate the local node's cert and key against the freshly
/// installed CA. Required because the outgoing `node.crt` was signed
/// by the CA that emergency-bootstrap just replaced — it no longer
/// validates against the newly-installed `ca.crt`.
///
/// Shape matches `NeonFS.Transport.TLS.sign_csr/5`:
///   - Subject: `/O=NeonFS/CN=neonfs_core@<hostname>`.
///   - SubjectAlternativeName: `[hostname]` (DNS).
///   - ExtendedKeyUsage: `ServerAuth + ClientAuth`.
///   - Signing algorithm: ECDSA P-256 SHA-256 (rcgen `generate()` default).
///
/// Also advances `$tls_dir/serial` by one so the fresh node cert does
/// not collide with the backed-up CA's previously-issued serials.
///
/// Writes are atomic per-file via the same stage + fsync + rename
/// pattern used by [`install_ca_material`], but NOT atomic across
/// files. If a rename fails partway, `node.crt` and `node.key` can
/// diverge from `serial`. Recovery: re-run `emergency-bootstrap`
/// (idempotent).
pub fn regenerate_node_cert(tls_dir: &Path) -> Result<()> {
    regenerate_node_cert_with(tls_dir, &resolve_hostname)
}

type HostnameResolver = dyn Fn() -> Result<String>;

fn regenerate_node_cert_with(tls_dir: &Path, resolve: &HostnameResolver) -> Result<()> {
    let hostname = resolve()?;
    let node_name = format!("neonfs_core@{hostname}");

    // Read the CA material we just installed.
    let ca_cert_path = tls_dir.join("ca.crt");
    let ca_key_path = tls_dir.join("ca.key");
    let serial_path = tls_dir.join("serial");

    // audit:bounded CA PEMs (tens of KB ceiling).
    let ca_cert_pem = std::fs::read_to_string(&ca_cert_path)
        .map_err(|e| io_err("read installed ca.crt", &ca_cert_path, e))?;
    // audit:bounded CA key PEM (tens of KB ceiling).
    let ca_key_pem = std::fs::read_to_string(&ca_key_path)
        .map_err(|e| io_err("read installed ca.key", &ca_key_path, e))?;
    // audit:bounded serial is a single line of ASCII digits.
    let serial_raw = std::fs::read_to_string(&serial_path)
        .map_err(|e| io_err("read installed serial", &serial_path, e))?;

    // Parse + bump the serial counter so the fresh node cert doesn't
    // collide with a serial the backed-up CA previously issued.
    let current_serial: u64 = serial_raw.trim().parse().map_err(|e| {
        CliError::InvalidArgument(format!(
            "installed `serial` file at {} is not a positive integer: {e}",
            serial_path.display()
        ))
    })?;
    let next_serial = current_serial + 1;

    // Reconstitute the CA from the installed PEMs. rcgen's `from_pem`
    // only accepts PKCS#8; NeonFS's Elixir X509 lib produces SEC1 by
    // default (`EC PRIVATE KEY` header). Detect SEC1 and rewrap as
    // PKCS#8 so both formats work.
    let ca_key = parse_ca_key_pem(&ca_key_pem)?;
    let issuer = rcgen::Issuer::from_ca_cert_pem(&ca_cert_pem, ca_key).map_err(|e| {
        CliError::InvalidArgument(format!(
            "installed ca.crt cannot be used as an rcgen issuer: {e:?}"
        ))
    })?;

    // Build node cert params. DN CN is the Erlang node name (matches
    // `NeonFS.Transport.TLS.create_csr/2` convention); SAN is the
    // hostname; ext key usage is ServerAuth + ClientAuth (matches
    // `NeonFS.Transport.TLS.sign_csr/5`).
    let mut params = rcgen::CertificateParams::new(vec![hostname.clone()]).map_err(|e| {
        CliError::InvalidArgument(format!(
            "rcgen rejected hostname `{hostname}` as a SAN: {e:?}"
        ))
    })?;
    params
        .distinguished_name
        .push(rcgen::DnType::OrganizationName, "NeonFS");
    params
        .distinguished_name
        .push(rcgen::DnType::CommonName, &node_name);
    params.serial_number = Some(rcgen::SerialNumber::from_slice(&next_serial.to_be_bytes()));
    params
        .extended_key_usages
        .push(rcgen::ExtendedKeyUsagePurpose::ServerAuth);
    params
        .extended_key_usages
        .push(rcgen::ExtendedKeyUsagePurpose::ClientAuth);

    // Generate node keypair (ECDSA P-256 SHA-256, rcgen default).
    let node_key = rcgen::KeyPair::generate().map_err(|e| {
        CliError::InvalidArgument(format!("failed to generate node keypair: {e:?}"))
    })?;

    // Sign with the installed CA key.
    let node_cert = params.signed_by(&node_key, &issuer).map_err(|e| {
        CliError::InvalidArgument(format!("failed to sign node cert for `{node_name}`: {e:?}"))
    })?;

    // Atomically write node.crt + node.key + bumped serial. Uses the
    // same stage + rename pattern as install_ca_material (per-file
    // atomicity; re-run on partial failure).
    atomic_write_files(
        tls_dir,
        &[
            ("node.crt", node_cert.pem().into_bytes()),
            ("node.key", node_key.serialize_pem().into_bytes()),
            ("serial", format!("{next_serial}\n").into_bytes()),
        ],
    )?;

    Ok(())
}

/// Helper that writes a batch of files atomically per-file via stage +
/// fsync + rename. Same machinery as [`install_ca_material`] but
/// parameterised over the payload list.
fn atomic_write_files(tls_dir: &Path, files: &[(&str, Vec<u8>)]) -> Result<()> {
    fs::create_dir_all(tls_dir).map_err(|e| io_err("create TLS directory", tls_dir, e))?;

    let stage_dir = tls_dir.join(format!(".emergency-bootstrap-cert-{}", std::process::id()));
    fs::create_dir_all(&stage_dir)
        .map_err(|e| io_err("create staging directory", &stage_dir, e))?;

    let _guard = StageDirGuard(stage_dir.clone());

    for (name, data) in files {
        let staged = stage_dir.join(name);
        fs::write(&staged, data)
            .map_err(|e| io_err(&format!("write staged `{}`", name), &staged, e))?;
        File::open(&staged)
            .and_then(|f| f.sync_all())
            .map_err(|e| io_err(&format!("fsync staged `{}`", name), &staged, e))?;
    }

    for (name, _) in files {
        let src = stage_dir.join(name);
        let dst = tls_dir.join(name);
        fs::rename(&src, &dst).map_err(|e| io_err(&format!("install `{}`", name), &dst, e))?;
    }

    File::open(tls_dir)
        .and_then(|f| f.sync_all())
        .map_err(|e| io_err("fsync TLS directory", tls_dir, e))?;

    Ok(())
}

/// Parse a CA EC private key from PEM.
///
/// **Only PKCS#8 (`-----BEGIN PRIVATE KEY-----`) is supported in this
/// slice.** NeonFS's Elixir X509 lib emits SEC1 (`-----BEGIN EC PRIVATE
/// KEY-----`) by default, which rcgen does not accept directly;
/// supporting SEC1 requires either an upstream conversion helper or
/// DER-level rewrapping and is tracked as a separate follow-up issue.
///
/// Operators whose backup tarballs contain a SEC1 CA key can convert
/// before restoring:
///
/// ```sh
/// openssl pkcs8 -topk8 -nocrypt -in ca.key -out ca.key.pkcs8
/// mv ca.key.pkcs8 ca.key
/// # then re-tar the backup
/// ```
fn parse_ca_key_pem(pem: &str) -> Result<rcgen::KeyPair> {
    rcgen::KeyPair::from_pem(pem).map_err(|e| {
        if pem.contains("BEGIN EC PRIVATE KEY") {
            CliError::InvalidArgument(format!(
                "installed ca.key is in SEC1 format (`EC PRIVATE KEY`), which this slice \
                 does not yet support. Convert to PKCS#8 with \
                 `openssl pkcs8 -topk8 -nocrypt -in ca.key -out ca.key.pkcs8`. \
                 rcgen error: {e:?}"
            ))
        } else {
            CliError::InvalidArgument(format!(
                "installed ca.key is not a parseable PKCS#8 keypair: {e:?}"
            ))
        }
    })
}

/// Resolve the local hostname for the new node cert's SAN + CN.
///
/// Tries, in order:
///   1. `NEONFS_NODE_NAME` env var (the release wrapper sets this to
///      `neonfs_core@<host>`). Splits on `@` and returns the right
///      half.
///   2. `/run/neonfs/core_node_name` (written by the daemon wrapper).
///      Same shape as above.
///   3. `hostname(1)` via `uname -n` in a plain subprocess call.
///
/// Returns a clear error if all three fail — the operator can set
/// `NEONFS_NODE_NAME` manually.
fn resolve_hostname() -> Result<String> {
    if let Ok(node_name) = std::env::var("NEONFS_NODE_NAME") {
        if let Some(host) = node_name.split_once('@').map(|(_, h)| h) {
            if !host.is_empty() {
                return Ok(host.to_string());
            }
        }
    }

    // audit:bounded core_node_name is a single line of ASCII.
    if let Ok(contents) = std::fs::read_to_string("/run/neonfs/core_node_name") {
        if let Some(host) = contents.trim().split_once('@').map(|(_, h)| h) {
            if !host.is_empty() {
                return Ok(host.to_string());
            }
        }
    }

    // Last resort: `uname -n`. Plain subprocess; no shell involvement.
    if let Ok(output) = std::process::Command::new("uname").arg("-n").output() {
        if output.status.success() {
            let host = String::from_utf8_lossy(&output.stdout).trim().to_string();
            if !host.is_empty() {
                return Ok(host);
            }
        }
    }

    Err(CliError::InvalidArgument(
        "cannot resolve local hostname for new node cert. \
         Set `NEONFS_NODE_NAME=neonfs_core@<hostname>` explicitly and re-run."
            .to_string(),
    ))
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

    // Self-signed ECDSA P-256 CA with subject `/O=NeonFS/CN=parity-test CA`
    // and its matching private key. Generated once with:
    //
    //     openssl ecparam -name prime256v1 -genkey -noout -out ca.key
    //     openssl req -new -x509 -key ca.key \
    //       -subj "/O=NeonFS/CN=parity-test CA" -days 3650 -out ca.crt
    //
    // Validity: 2026-04-24 → 2036-04-21. Both PEMs are pinned so the
    // tests are deterministic and don't depend on runtime key gen.
    const PARITY_CA_PEM: &[u8] = b"-----BEGIN CERTIFICATE-----
MIIBqjCCAU+gAwIBAgIUeyGOIzhUa1Xlwg0XUejUs84Lz7AwCgYIKoZIzj0EAwIw
KjEPMA0GA1UECgwGTmVvbkZTMRcwFQYDVQQDDA5wYXJpdHktdGVzdCBDQTAeFw0y
NjA0MjQyMzQ2NTBaFw0zNjA0MjEyMzQ2NTBaMCoxDzANBgNVBAoMBk5lb25GUzEX
MBUGA1UEAwwOcGFyaXR5LXRlc3QgQ0EwWTATBgcqhkjOPQIBBggqhkjOPQMBBwNC
AAQFq0d9vDUx+XU1aJATTsLof2hFfQBNZrbuAQ2ClscminzzA+15JSfToEHJkGdO
btg8bJUpnjDQ0dhQ/MOeQoezo1MwUTAdBgNVHQ4EFgQUJ/e7ws2hg7tS+TLBC29F
i+ny8lUwHwYDVR0jBBgwFoAUJ/e7ws2hg7tS+TLBC29Fi+ny8lUwDwYDVR0TAQH/
BAUwAwEB/zAKBggqhkjOPQQDAgNJADBGAiEAiHUtcY1DLqe30XAklGF1tbA2uHvq
3gefZmfleMvFywACIQCQpNg6OPBmWamx/eO77ISJvJXX9RdEtbRKDKeHKWqZag==
-----END CERTIFICATE-----
";

    // Matching PKCS#8 private key for `PARITY_CA_PEM`. Produced from
    // the SEC1 output via:
    //
    //     openssl pkcs8 -topk8 -nocrypt -in ca.key -out ca.key.pkcs8
    const PARITY_CA_KEY_PEM: &[u8] = b"-----BEGIN PRIVATE KEY-----
MIGHAgEAMBMGByqGSM49AgEGCCqGSM49AwEHBG0wawIBAQQgP3kBkvyNhXC5sMuG
g/4xCMbfMQlK3JPFoNcx+220ts6hRANCAAQFq0d9vDUx+XU1aJATTsLof2hFfQBN
ZrbuAQ2ClscminzzA+15JSfToEHJkGdObtg8bJUpnjDQ0dhQ/MOeQoez
-----END PRIVATE KEY-----
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
    fn refuse_if_daemon_live_allows_when_port_file_missing() {
        let read_port: &DistPortReader = &|| None;
        let probe: &PortProber = &|_| unreachable!("probe should not run when port is absent");

        assert!(refuse_if_daemon_live_with(read_port, probe).is_ok());
    }

    #[test]
    fn refuse_if_daemon_live_allows_when_probe_fails() {
        let read_port: &DistPortReader = &|| Some(12_345);
        let probe: &PortProber = &|port| {
            assert_eq!(port, 12_345);
            false
        };

        assert!(refuse_if_daemon_live_with(read_port, probe).is_ok());
    }

    #[test]
    fn refuse_if_daemon_live_refuses_when_probe_succeeds() {
        let read_port: &DistPortReader = &|| Some(12_345);
        let probe: &PortProber = &|_port| true;

        let err = refuse_if_daemon_live_with(read_port, probe).unwrap_err();
        match err {
            CliError::InvalidArgument(msg) => {
                assert!(msg.contains("daemon appears to be running"));
                assert!(msg.contains("12345"));
                assert!(msg.contains("Stop the service"));
            }
            other => panic!("expected InvalidArgument, got {other:?}"),
        }
    }

    #[test]
    fn read_dist_port_from_env_override() {
        let dir = TempDir::new().unwrap();
        let port_file = dir.path().join("dist_port");
        std::fs::write(&port_file, "9568\n").unwrap();

        let old = std::env::var("NEONFS_DIST_PORT_FILE").ok();
        unsafe {
            std::env::set_var("NEONFS_DIST_PORT_FILE", &port_file);
        }

        let port = read_dist_port();

        match old {
            Some(v) => unsafe { std::env::set_var("NEONFS_DIST_PORT_FILE", v) },
            None => unsafe { std::env::remove_var("NEONFS_DIST_PORT_FILE") },
        }

        assert_eq!(port, Some(9568));
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

    fn seed_installed_ca(tls_dir: &Path, serial: &str) {
        std::fs::create_dir_all(tls_dir).unwrap();
        std::fs::write(tls_dir.join("ca.crt"), PARITY_CA_PEM).unwrap();
        std::fs::write(tls_dir.join("ca.key"), PARITY_CA_KEY_PEM).unwrap();
        std::fs::write(tls_dir.join("serial"), serial).unwrap();
        std::fs::write(tls_dir.join("crl.pem"), b"UNUSED").unwrap();
    }

    #[test]
    fn regenerate_node_cert_writes_cert_key_and_serial() {
        let dir = TempDir::new().unwrap();
        let tls_dir = dir.path().join("tls");
        seed_installed_ca(&tls_dir, "42\n");

        let resolve: &HostnameResolver = &|| Ok("testhost".to_string());
        regenerate_node_cert_with(&tls_dir, resolve).unwrap();

        assert!(tls_dir.join("node.crt").exists());
        assert!(tls_dir.join("node.key").exists());

        // audit:bounded test-only; serial file is a few ASCII bytes.
        let bumped = std::fs::read_to_string(tls_dir.join("serial")).unwrap();
        assert_eq!(bumped.trim(), "43");
    }

    #[test]
    fn regenerate_node_cert_issues_pem_that_parses_as_x509() {
        let dir = TempDir::new().unwrap();
        let tls_dir = dir.path().join("tls");
        seed_installed_ca(&tls_dir, "100\n");

        let resolve: &HostnameResolver = &|| Ok("node-alpha".to_string());
        regenerate_node_cert_with(&tls_dir, resolve).unwrap();

        let node_pem = std::fs::read(tls_dir.join("node.crt")).unwrap();
        let (_, pem_obj) = x509_parser::pem::parse_x509_pem(&node_pem).unwrap();
        let (_, cert) = x509_parser::parse_x509_certificate(&pem_obj.contents).unwrap();

        // Subject CN follows `neonfs_core@<hostname>` convention.
        let cn = cert
            .subject()
            .iter_common_name()
            .next()
            .unwrap()
            .as_str()
            .unwrap();
        assert_eq!(cn, "neonfs_core@node-alpha");

        // Organisation is NeonFS.
        let org = cert
            .subject()
            .iter_organization()
            .next()
            .unwrap()
            .as_str()
            .unwrap();
        assert_eq!(org, "NeonFS");
    }

    #[test]
    fn regenerate_node_cert_refuses_non_integer_serial() {
        let dir = TempDir::new().unwrap();
        let tls_dir = dir.path().join("tls");
        seed_installed_ca(&tls_dir, "not-a-number\n");

        let resolve: &HostnameResolver = &|| Ok("testhost".to_string());
        let err = regenerate_node_cert_with(&tls_dir, resolve).unwrap_err();

        match err {
            CliError::InvalidArgument(msg) => {
                assert!(msg.contains("not a positive integer"));
                assert!(msg.contains("serial"));
            }
            other => panic!("expected InvalidArgument, got {other:?}"),
        }
    }

    #[test]
    fn regenerate_node_cert_propagates_hostname_resolution_error() {
        let dir = TempDir::new().unwrap();
        let tls_dir = dir.path().join("tls");
        seed_installed_ca(&tls_dir, "1\n");

        let resolve: &HostnameResolver = &|| {
            Err(CliError::InvalidArgument(
                "hostname unknown in test".to_string(),
            ))
        };
        let err = regenerate_node_cert_with(&tls_dir, resolve).unwrap_err();

        match err {
            CliError::InvalidArgument(msg) => assert!(msg.contains("hostname unknown in test")),
            other => panic!("expected InvalidArgument, got {other:?}"),
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
