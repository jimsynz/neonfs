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
//!   - **B.2b.3** (#518) — `--new-key` fresh-CA generation: when the
//!     operator has no usable backup, mint a brand new CA keypair +
//!     self-signed CA cert + empty CRL via rcgen, advance the serial
//!     counter past any value the previous CA might have issued, and
//!     install atomically. Reuses [`install_ca_material`] for the
//!     install step and [`regenerate_node_cert`] for the post-install
//!     node cert.
//!
//!   - **B.2b.4** (#518) — Audit-log emission: append a single
//!     `cluster_ca_emergency_bootstrap_completed` JSONL line to
//!     `$NEONFS_DATA_DIR/audit/emergency-bootstrap.log` after a
//!     successful install. Captures cluster id, source, before/after
//!     CA fingerprints, operator UID, RFC-3339 timestamp. The daemon
//!     can ingest this file on next start (consumer side tracks
//!     separately).

use crate::error::{CliError, Result};
use flate2::read::GzDecoder;
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{BufReader, Read, Write};
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

/// Generate fresh CA material in memory (CA cert + key + initial serial +
/// empty CRL) for the `--new-key` emergency-bootstrap path.
///
/// Produces a [`BackupValidation`] with the same shape as
/// [`validate_backup_tarball`], so callers can feed the result straight
/// into [`install_ca_material`] and reuse the existing atomic install +
/// node-cert-regen pipeline.
///
/// ## Inputs
///
/// - `cluster_name` — identifies the new CA via the subject CN
///   (`/O=NeonFS/CN=<cluster_name> CA`). Read from the local
///   `cluster.json` by the caller; this function does not touch disk.
/// - `existing_serial` — last serial number known to have been issued
///   by the OUTGOING CA, if recoverable. The new CA's serial counter
///   starts at `max(existing_serial, 1000) + 1` so freshly-issued
///   certs never collide with anything the previous CA signed.
///
/// ## Outputs
///
/// `BackupValidation.entries` carries the four [`REQUIRED_MEMBERS`] —
/// `ca.crt` (PEM), `ca.key` (PKCS#8 PEM), `serial` (ASCII integer +
/// trailing newline), `crl.pem` (empty CRL signed by the new CA key).
/// Validity matches the existing Elixir defaults: 10-year CA cert,
/// 1-year CRL `next_update`.
pub fn generate_new_ca_material(
    cluster_name: &str,
    existing_serial: u64,
) -> Result<BackupValidation> {
    let next_serial = existing_serial.max(MIN_NEW_CA_SERIAL) + 1;

    let ca_keypair = rcgen::KeyPair::generate()
        .map_err(|e| CliError::InvalidArgument(format!("failed to generate CA keypair: {e:?}")))?;
    let ca_key_pem = ca_keypair.serialize_pem();

    let mut ca_params = rcgen::CertificateParams::new(Vec::<String>::new()).map_err(|e| {
        CliError::InvalidArgument(format!("rcgen rejected empty CA SAN list: {e:?}"))
    })?;
    ca_params
        .distinguished_name
        .push(rcgen::DnType::OrganizationName, "NeonFS");
    ca_params
        .distinguished_name
        .push(rcgen::DnType::CommonName, format!("{cluster_name} CA"));
    ca_params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
    ca_params.key_usages = vec![
        rcgen::KeyUsagePurpose::KeyCertSign,
        rcgen::KeyUsagePurpose::DigitalSignature,
        rcgen::KeyUsagePurpose::CrlSign,
    ];
    let now = time::OffsetDateTime::now_utc();
    ca_params.not_before = now;
    ca_params.not_after = now + time::Duration::days(CA_VALIDITY_DAYS);

    let certified = rcgen::CertifiedIssuer::self_signed(ca_params, ca_keypair).map_err(|e| {
        CliError::InvalidArgument(format!("failed to self-sign new CA cert: {e:?}"))
    })?;
    let ca_cert_pem = certified.pem();

    let crl_params = rcgen::CertificateRevocationListParams {
        this_update: now,
        next_update: now + time::Duration::days(CRL_NEXT_UPDATE_DAYS),
        crl_number: rcgen::SerialNumber::from(1u64),
        issuing_distribution_point: None,
        revoked_certs: Vec::new(),
        key_identifier_method: rcgen::KeyIdMethod::Sha256,
    };
    let crl = crl_params.signed_by(&certified).map_err(|e| {
        CliError::InvalidArgument(format!("failed to sign empty CRL with new CA key: {e:?}"))
    })?;
    let crl_pem = crl
        .pem()
        .map_err(|e| CliError::InvalidArgument(format!("failed to PEM-encode new CRL: {e:?}")))?;

    let mut entries: HashMap<String, Vec<u8>> = HashMap::new();
    entries.insert("ca.crt".to_string(), ca_cert_pem.into_bytes());
    entries.insert("ca.key".to_string(), ca_key_pem.into_bytes());
    entries.insert(
        "serial".to_string(),
        format!("{next_serial}\n").into_bytes(),
    );
    entries.insert("crl.pem".to_string(), crl_pem.into_bytes());

    Ok(BackupValidation {
        cluster_name: cluster_name.to_string(),
        entries,
    })
}

const CA_VALIDITY_DAYS: i64 = 3650;
const CRL_NEXT_UPDATE_DAYS: i64 = 365;
const MIN_NEW_CA_SERIAL: u64 = 1000;

/// Read `$tls_dir/serial` and return its parsed value if present and
/// well-formed. Returns `None` for any failure mode (file missing,
/// unreadable, not an integer) — callers in the `--new-key` path treat
/// "no recoverable serial" as zero and let
/// [`generate_new_ca_material`] floor at [`MIN_NEW_CA_SERIAL`].
pub fn read_installed_serial(tls_dir: &Path) -> Option<u64> {
    let path = tls_dir.join("serial");
    // audit:bounded serial file is a single line of ASCII digits.
    let raw = std::fs::read_to_string(&path).ok()?;
    raw.trim().parse().ok()
}

/// Source of CA material for the audit-log entry. Mirrors the CLI flag
/// shape, not the in-memory `EmergencyBootstrapSource` enum (which is
/// `cluster.rs`-private and carries borrowed paths).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuditSource {
    /// `--from-backup <path>`. Path is recorded as a string so the
    /// audit log entry stays serialisable across the function boundary.
    Backup(String),
    /// `--new-key`. No additional payload.
    NewKey,
}

impl AuditSource {
    fn as_audit_string(&self) -> String {
        match self {
            AuditSource::Backup(path) => format!("backup:{path}"),
            AuditSource::NewKey => "new_key".to_string(),
        }
    }
}

/// Compute the SHA-256 fingerprint of the DER inside a PEM-encoded
/// certificate. Returns the lowercase hex string — the same format
/// `openssl x509 -fingerprint -sha256` emits (minus the `SHA256
/// Fingerprint=` prefix and colons).
///
/// Returns `None` if the bytes are not parseable PEM.
pub fn ca_fingerprint_from_pem(pem: &[u8]) -> Option<String> {
    let (_, pem_obj) = x509_parser::pem::parse_x509_pem(pem).ok()?;
    let digest = Sha256::digest(&pem_obj.contents);
    Some(hex::encode(digest))
}

/// Read `$tls_dir/ca.crt` and compute its SHA-256 DER fingerprint, if
/// the file exists and parses. Returns `None` for any failure mode.
/// Callers use this to capture the OUTGOING fingerprint *before*
/// `install_ca_material` overwrites the file.
pub fn read_ca_fingerprint(tls_dir: &Path) -> Option<String> {
    let path = tls_dir.join("ca.crt");
    // audit:bounded ca.crt is a single PEM cert (tens of KB ceiling).
    let pem = std::fs::read(&path).ok()?;
    ca_fingerprint_from_pem(&pem)
}

/// Append a `cluster_ca_emergency_bootstrap_completed` JSON line to
/// `$NEONFS_DATA_DIR/audit/emergency-bootstrap.log`.
///
/// The audit log is best-effort — the install + node-cert-regen have
/// already landed on disk before this function runs, and we should not
/// roll those back on a logging failure. Any `io_err` here is surfaced
/// to the caller, which decides whether to treat it as fatal (current
/// behaviour: warn but succeed, since the install is the operator's
/// objective).
///
/// The file is created with parents-as-needed; subsequent runs append
/// (no truncation). Each line is one self-contained JSON object —
/// future ingestion tooling can `read_to_string` and `split('\n')`.
pub fn write_audit_log_entry(
    data_dir: &Path,
    cluster_id: &str,
    source: AuditSource,
    old_ca_fingerprint: Option<&str>,
    new_ca_fingerprint: &str,
    operator_uid: u32,
    timestamp_rfc3339: &str,
) -> Result<()> {
    let audit_dir = data_dir.join("audit");
    fs::create_dir_all(&audit_dir).map_err(|e| io_err("create audit directory", &audit_dir, e))?;

    let log_path = audit_dir.join("emergency-bootstrap.log");

    let entry = serde_json::json!({
        "event": "cluster_ca_emergency_bootstrap_completed",
        "cluster_id": cluster_id,
        "source": source.as_audit_string(),
        "old_ca_fingerprint": old_ca_fingerprint,
        "new_ca_fingerprint": new_ca_fingerprint,
        "operator_uid": operator_uid,
        "timestamp": timestamp_rfc3339,
    });

    let mut line = serde_json::to_string(&entry)
        .map_err(|e| CliError::InvalidArgument(format!("failed to serialise audit entry: {e}")))?;
    line.push('\n');

    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&log_path)
        .map_err(|e| io_err("open audit log", &log_path, e))?;

    file.write_all(line.as_bytes())
        .map_err(|e| io_err("append audit log entry", &log_path, e))?;

    file.sync_all()
        .map_err(|e| io_err("fsync audit log", &log_path, e))?;

    Ok(())
}

/// Read `cluster_id` from the local `cluster.json`. Mirrors
/// [`local_cluster_name`] but extracts the `cluster_id` field, which
/// is what the audit log entry pins.
pub fn local_cluster_id() -> Result<String> {
    let cluster_json = local_cluster_json_path();
    // audit:bounded cluster.json is a NeonFS-written config (not user data).
    let contents = std::fs::read_to_string(&cluster_json).map_err(|e| {
        CliError::InvalidArgument(format!(
            "cannot read local cluster state at {}: {e}.",
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
        .get("cluster_id")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
        .ok_or_else(|| {
            CliError::InvalidArgument(format!(
                "local cluster state at {} is missing the `cluster_id` field.",
                cluster_json.display()
            ))
        })
}

/// Resolve `$NEONFS_DATA_DIR` (default `/var/lib/neonfs`) — re-exposed
/// so `cluster.rs` can pass it to [`write_audit_log_entry`] without
/// duplicating the env-fallback logic.
pub fn data_dir() -> PathBuf {
    PathBuf::from(std::env::var("NEONFS_DATA_DIR").unwrap_or_else(|_| DEFAULT_DATA_DIR.to_string()))
}

/// Return the current process's real user ID via `libc::getuid()`.
/// Wrapped here so audit-log call sites do not embed an `unsafe`
/// block of their own and so tests can replace the value via the
/// `NEONFS_AUDIT_OPERATOR_UID` env override (set by the integration
/// suite to make assertions deterministic).
pub fn operator_uid() -> u32 {
    if let Ok(raw) = std::env::var("NEONFS_AUDIT_OPERATOR_UID") {
        if let Ok(parsed) = raw.parse::<u32>() {
            return parsed;
        }
    }

    // SAFETY: getuid() is documented as always-succeeds on Linux/POSIX.
    // It does no allocation and has no side effects.
    unsafe { libc::getuid() }
}

/// Format the current UTC time in RFC-3339. Pulled out so tests can
/// override via the `NEONFS_AUDIT_TIMESTAMP` env var (set to a fixed
/// value by integration / CLI tests so assertions are deterministic).
pub fn audit_timestamp() -> String {
    if let Ok(fixed) = std::env::var("NEONFS_AUDIT_TIMESTAMP") {
        return fixed;
    }

    let now = time::OffsetDateTime::now_utc();
    now.format(&time::format_description::well_known::Rfc3339)
        .unwrap_or_else(|_| now.unix_timestamp().to_string())
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

    // ─── #518 B.2b.3: --new-key fresh-CA generation ────────────────

    #[test]
    fn read_installed_serial_returns_none_when_missing() {
        let dir = TempDir::new().unwrap();
        let tls_dir = dir.path().join("tls");
        std::fs::create_dir_all(&tls_dir).unwrap();
        // No serial file written.
        assert_eq!(read_installed_serial(&tls_dir), None);
    }

    #[test]
    fn read_installed_serial_parses_existing_file() {
        let dir = TempDir::new().unwrap();
        let tls_dir = dir.path().join("tls");
        std::fs::create_dir_all(&tls_dir).unwrap();
        std::fs::write(tls_dir.join("serial"), "42\n").unwrap();
        assert_eq!(read_installed_serial(&tls_dir), Some(42));
    }

    #[test]
    fn read_installed_serial_returns_none_for_garbage() {
        let dir = TempDir::new().unwrap();
        let tls_dir = dir.path().join("tls");
        std::fs::create_dir_all(&tls_dir).unwrap();
        std::fs::write(tls_dir.join("serial"), "not-a-number\n").unwrap();
        assert_eq!(read_installed_serial(&tls_dir), None);
    }

    #[test]
    fn generate_new_ca_material_carries_required_members_and_cluster_name() {
        let validation = generate_new_ca_material("prod-west-1", 0).unwrap();
        assert_eq!(validation.cluster_name, "prod-west-1");

        for member in REQUIRED_MEMBERS {
            assert!(
                validation.entries.contains_key(*member),
                "missing required member `{member}` in fresh CA validation"
            );
            let bytes = validation.entries.get(*member).unwrap();
            assert!(!bytes.is_empty(), "`{member}` has zero bytes");
        }
    }

    #[test]
    fn generate_new_ca_material_produces_pkcs8_ca_key() {
        let validation = generate_new_ca_material("k", 0).unwrap();
        let key_pem = std::str::from_utf8(validation.entries.get("ca.key").unwrap()).unwrap();
        // rcgen's serialize_pem emits PKCS#8 (`-----BEGIN PRIVATE KEY-----`),
        // which the regenerate_node_cert path can consume directly.
        assert!(
            key_pem.contains("-----BEGIN PRIVATE KEY-----"),
            "expected PKCS#8 envelope, got:\n{key_pem}"
        );
        assert!(!key_pem.contains("BEGIN EC PRIVATE KEY"));
    }

    #[test]
    fn generate_new_ca_material_self_signs_ca_with_expected_subject() {
        let validation = generate_new_ca_material("acme-prod", 0).unwrap();
        let ca_pem = validation.entries.get("ca.crt").unwrap();

        let (_, pem_obj) = x509_parser::pem::parse_x509_pem(ca_pem).unwrap();
        let (_, cert) = x509_parser::parse_x509_certificate(&pem_obj.contents).unwrap();

        let cn = cert
            .subject()
            .iter_common_name()
            .next()
            .unwrap()
            .as_str()
            .unwrap();
        assert_eq!(cn, "acme-prod CA");

        let org = cert
            .subject()
            .iter_organization()
            .next()
            .unwrap()
            .as_str()
            .unwrap();
        assert_eq!(org, "NeonFS");

        // Self-signed: subject == issuer.
        assert_eq!(cert.subject().to_string(), cert.issuer().to_string());

        // CA: BasicConstraints must mark this as a CA.
        let bc = cert
            .basic_constraints()
            .ok()
            .flatten()
            .expect("CA cert must carry BasicConstraints");
        assert!(
            bc.value.ca,
            "BasicConstraints.ca must be true on the new CA"
        );
    }

    #[test]
    fn generate_new_ca_material_emits_parseable_empty_crl() {
        let validation = generate_new_ca_material("k", 0).unwrap();
        let crl_pem = validation.entries.get("crl.pem").unwrap();

        let (_, pem_obj) = x509_parser::pem::parse_x509_pem(crl_pem).unwrap();
        let (_, crl) = x509_parser::parse_x509_crl(&pem_obj.contents).unwrap();
        assert_eq!(crl.iter_revoked_certificates().count(), 0);
    }

    #[test]
    fn generate_new_ca_material_serial_floors_at_min_new_ca_serial() {
        // Existing serial 0 → next = max(0, 1000) + 1 = 1001.
        let validation = generate_new_ca_material("k", 0).unwrap();
        let serial = std::str::from_utf8(validation.entries.get("serial").unwrap()).unwrap();
        assert_eq!(serial.trim(), "1001");
    }

    #[test]
    fn generate_new_ca_material_serial_advances_past_existing() {
        // Existing serial 5000 → next = max(5000, 1000) + 1 = 5001.
        let validation = generate_new_ca_material("k", 5000).unwrap();
        let serial = std::str::from_utf8(validation.entries.get("serial").unwrap()).unwrap();
        assert_eq!(serial.trim(), "5001");
    }

    #[test]
    fn fresh_ca_round_trips_through_install_and_regenerate_node_cert() {
        // Full slice happy-path: generate fresh CA → install → regen
        // node cert → assert node cert validates against the new CA.
        let dir = TempDir::new().unwrap();
        let tls_dir = dir.path().join("tls");

        let validation = generate_new_ca_material("ring-zero", 100).unwrap();
        install_ca_material(&validation, &tls_dir).unwrap();

        let resolve: &HostnameResolver = &|| Ok("ring-zero-host".to_string());
        regenerate_node_cert_with(&tls_dir, resolve).unwrap();

        // Node cert is signed by the issuing CA — assert subject CN +
        // the Issuer DN matches the CA we just installed.
        let node_pem = std::fs::read(tls_dir.join("node.crt")).unwrap();
        let (_, node_pem_obj) = x509_parser::pem::parse_x509_pem(&node_pem).unwrap();
        let (_, node_cert) = x509_parser::parse_x509_certificate(&node_pem_obj.contents).unwrap();

        let ca_pem = std::fs::read(tls_dir.join("ca.crt")).unwrap();
        let (_, ca_pem_obj) = x509_parser::pem::parse_x509_pem(&ca_pem).unwrap();
        let (_, ca_cert) = x509_parser::parse_x509_certificate(&ca_pem_obj.contents).unwrap();

        assert_eq!(
            node_cert.issuer().to_string(),
            ca_cert.subject().to_string()
        );

        let node_cn = node_cert
            .subject()
            .iter_common_name()
            .next()
            .unwrap()
            .as_str()
            .unwrap();
        assert_eq!(node_cn, "neonfs_core@ring-zero-host");

        // generate_new_ca_material(_, 100) floors at MIN_NEW_CA_SERIAL = 1000,
        // so the installed serial is 1001. regenerate_node_cert reads it,
        // bumps to 1002, and writes the bumped value back.
        // audit:bounded serial file is a single line of ASCII digits.
        let on_disk = std::fs::read_to_string(tls_dir.join("serial")).unwrap();
        assert_eq!(on_disk.trim(), "1002");
    }

    // ─── #518 B.2b.4: audit-log JSONL emission ─────────────────────

    #[test]
    fn ca_fingerprint_from_pem_returns_lowercase_hex_sha256() {
        let fp = ca_fingerprint_from_pem(PARITY_CA_PEM).unwrap();
        // 64 lowercase hex chars (SHA-256 = 32 bytes).
        assert_eq!(fp.len(), 64);
        assert!(
            fp.chars()
                .all(|c| c.is_ascii_hexdigit() && !c.is_uppercase()),
            "fingerprint not lowercase hex: {fp}"
        );

        // Same bytes → same fingerprint (function is deterministic).
        let fp2 = ca_fingerprint_from_pem(PARITY_CA_PEM).unwrap();
        assert_eq!(fp, fp2);
    }

    #[test]
    fn ca_fingerprint_from_pem_returns_none_for_garbage() {
        assert!(ca_fingerprint_from_pem(b"not a pem").is_none());
    }

    #[test]
    fn read_ca_fingerprint_returns_none_when_file_missing() {
        let dir = TempDir::new().unwrap();
        let tls_dir = dir.path().join("tls");
        std::fs::create_dir_all(&tls_dir).unwrap();
        // No ca.crt written.
        assert!(read_ca_fingerprint(&tls_dir).is_none());
    }

    #[test]
    fn read_ca_fingerprint_matches_computed_fingerprint() {
        let dir = TempDir::new().unwrap();
        let tls_dir = dir.path().join("tls");
        std::fs::create_dir_all(&tls_dir).unwrap();
        std::fs::write(tls_dir.join("ca.crt"), PARITY_CA_PEM).unwrap();

        let on_disk = read_ca_fingerprint(&tls_dir).unwrap();
        let computed = ca_fingerprint_from_pem(PARITY_CA_PEM).unwrap();
        assert_eq!(on_disk, computed);
    }

    #[test]
    fn audit_source_serialises_to_canonical_string() {
        assert_eq!(AuditSource::NewKey.as_audit_string(), "new_key");
        assert_eq!(
            AuditSource::Backup("/var/backups/ca-2026-04-25.tar.gz".to_string()).as_audit_string(),
            "backup:/var/backups/ca-2026-04-25.tar.gz"
        );
    }

    #[test]
    fn write_audit_log_entry_appends_jsonl_with_all_fields() {
        let dir = TempDir::new().unwrap();

        write_audit_log_entry(
            dir.path(),
            "cluster-abc",
            AuditSource::NewKey,
            Some("oldfp"),
            "newfp",
            1234,
            "2026-04-25T13:00:00Z",
        )
        .unwrap();

        let log_path = dir.path().join("audit").join("emergency-bootstrap.log");
        // audit:bounded test-only; small JSONL written by this test.
        let contents = std::fs::read_to_string(&log_path).unwrap();
        assert!(contents.ends_with('\n'), "missing trailing newline");

        let line = contents.trim_end();
        let parsed: serde_json::Value = serde_json::from_str(line).unwrap();
        assert_eq!(parsed["event"], "cluster_ca_emergency_bootstrap_completed");
        assert_eq!(parsed["cluster_id"], "cluster-abc");
        assert_eq!(parsed["source"], "new_key");
        assert_eq!(parsed["old_ca_fingerprint"], "oldfp");
        assert_eq!(parsed["new_ca_fingerprint"], "newfp");
        assert_eq!(parsed["operator_uid"], 1234);
        assert_eq!(parsed["timestamp"], "2026-04-25T13:00:00Z");
    }

    #[test]
    fn write_audit_log_entry_appends_without_truncating() {
        let dir = TempDir::new().unwrap();

        for i in 0..3 {
            write_audit_log_entry(
                dir.path(),
                "cluster-abc",
                AuditSource::NewKey,
                None,
                &format!("fp-{i}"),
                0,
                "2026-04-25T13:00:00Z",
            )
            .unwrap();
        }

        let log_path = dir.path().join("audit").join("emergency-bootstrap.log");
        // audit:bounded test-only; small JSONL written by this test.
        let contents = std::fs::read_to_string(&log_path).unwrap();
        let lines: Vec<&str> = contents.trim_end().split('\n').collect();
        assert_eq!(lines.len(), 3, "expected three appended lines");

        for (i, line) in lines.iter().enumerate() {
            let parsed: serde_json::Value = serde_json::from_str(line).unwrap();
            assert_eq!(parsed["new_ca_fingerprint"], format!("fp-{i}"));
        }
    }

    #[test]
    fn write_audit_log_entry_emits_null_for_missing_old_fingerprint() {
        let dir = TempDir::new().unwrap();

        write_audit_log_entry(
            dir.path(),
            "c",
            AuditSource::Backup("/tmp/x.tar.gz".to_string()),
            None,
            "newfp",
            0,
            "t",
        )
        .unwrap();

        let log_path = dir.path().join("audit").join("emergency-bootstrap.log");
        // audit:bounded test-only; single JSONL line.
        let contents = std::fs::read_to_string(&log_path).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(contents.trim_end()).unwrap();
        assert!(parsed["old_ca_fingerprint"].is_null());
        assert_eq!(parsed["source"], "backup:/tmp/x.tar.gz");
    }

    #[test]
    fn local_cluster_id_reads_field() {
        let dir = TempDir::new().unwrap();
        let meta_dir = dir.path().join("meta");
        std::fs::create_dir_all(&meta_dir).unwrap();
        let cluster_json = meta_dir.join("cluster.json");

        let mut f = File::create(&cluster_json).unwrap();
        f.write_all(br#"{"cluster_name":"prod","cluster_id":"abc-123"}"#)
            .unwrap();

        let old = std::env::var("NEONFS_DATA_DIR").ok();
        unsafe {
            std::env::set_var("NEONFS_DATA_DIR", dir.path());
        }

        let id = local_cluster_id();

        match old {
            Some(v) => unsafe { std::env::set_var("NEONFS_DATA_DIR", v) },
            None => unsafe { std::env::remove_var("NEONFS_DATA_DIR") },
        }

        assert_eq!(id.unwrap(), "abc-123");
    }

    #[test]
    fn operator_uid_honours_env_override() {
        let old = std::env::var("NEONFS_AUDIT_OPERATOR_UID").ok();
        unsafe {
            std::env::set_var("NEONFS_AUDIT_OPERATOR_UID", "9999");
        }

        let uid = operator_uid();

        match old {
            Some(v) => unsafe { std::env::set_var("NEONFS_AUDIT_OPERATOR_UID", v) },
            None => unsafe { std::env::remove_var("NEONFS_AUDIT_OPERATOR_UID") },
        }

        assert_eq!(uid, 9999);
    }

    #[test]
    fn audit_timestamp_honours_env_override() {
        let old = std::env::var("NEONFS_AUDIT_TIMESTAMP").ok();
        unsafe {
            std::env::set_var("NEONFS_AUDIT_TIMESTAMP", "2026-04-25T00:00:00Z");
        }

        let ts = audit_timestamp();

        match old {
            Some(v) => unsafe { std::env::set_var("NEONFS_AUDIT_TIMESTAMP", v) },
            None => unsafe { std::env::remove_var("NEONFS_AUDIT_TIMESTAMP") },
        }

        assert_eq!(ts, "2026-04-25T00:00:00Z");
    }

    #[test]
    fn audit_timestamp_default_is_rfc3339_when_no_override() {
        // No env override: should produce something parseable as RFC-3339.
        let prior = std::env::var("NEONFS_AUDIT_TIMESTAMP").ok();
        unsafe {
            std::env::remove_var("NEONFS_AUDIT_TIMESTAMP");
        }

        let ts = audit_timestamp();

        if let Some(v) = prior {
            unsafe {
                std::env::set_var("NEONFS_AUDIT_TIMESTAMP", v);
            }
        }

        let parsed =
            time::OffsetDateTime::parse(&ts, &time::format_description::well_known::Rfc3339);
        assert!(parsed.is_ok(), "default timestamp `{ts}` is not RFC-3339");
    }
}
