//! TLS certificate utilities for NeonFS CLI.
//!
//! Loads the CLI certificate, key, and local CA from the TLS directory.
//! Used for building TLS client config when TLS distribution is enabled.

use crate::error::{CliError, Result};
use std::fs;
use std::io::BufReader;
use std::path::PathBuf;
use std::sync::Arc;

/// Default TLS directory
const DEFAULT_TLS_DIR: &str = "/var/lib/neonfs/tls";

/// Returns the TLS directory path (from env or default).
pub fn tls_dir() -> PathBuf {
    std::env::var("NEONFS_TLS_DIR")
        .ok()
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(DEFAULT_TLS_DIR))
}

/// Build a rustls ClientConfig for TLS distribution.
///
/// - Client identity: `cli.crt` + `cli.key`
/// - Trusted CA: `local-ca.crt` (and `ca.crt` if present)
pub fn build_client_config() -> Result<Arc<rustls::ClientConfig>> {
    let dir = tls_dir();

    let ca_path = dir.join("local-ca.crt");
    let cert_path = dir.join("cli.crt");
    let key_path = dir.join("cli.key");

    for (path, name) in [
        (&ca_path, "local-ca.crt"),
        (&cert_path, "cli.crt"),
        (&key_path, "cli.key"),
    ] {
        if !path.exists() {
            return Err(CliError::TlsCertNotFound(format!(
                "{} (in {})",
                name,
                dir.display()
            )));
        }
    }

    // Load CA certificate
    let ca_pem = fs::read(&ca_path)
        .map_err(|e| CliError::TlsError(format!("Failed to read {}: {}", ca_path.display(), e)))?;
    let mut root_store = rustls::RootCertStore::empty();
    let ca_certs = rustls_pemfile::certs(&mut BufReader::new(ca_pem.as_slice()))
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|e| CliError::TlsError(format!("Failed to parse CA cert: {}", e)))?;
    for cert in ca_certs {
        root_store
            .add(cert)
            .map_err(|e| CliError::TlsError(format!("Failed to add CA cert: {}", e)))?;
    }

    // Also add cluster CA if present
    let cluster_ca_path = dir.join("ca.crt");
    if cluster_ca_path.exists() {
        let cluster_ca_pem = fs::read(&cluster_ca_path)
            .map_err(|e| CliError::TlsError(format!("Failed to read cluster CA: {}", e)))?;
        let cluster_certs = rustls_pemfile::certs(&mut BufReader::new(cluster_ca_pem.as_slice()))
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|e| CliError::TlsError(format!("Failed to parse cluster CA: {}", e)))?;
        for cert in cluster_certs {
            root_store
                .add(cert)
                .map_err(|e| CliError::TlsError(format!("Failed to add cluster CA: {}", e)))?;
        }
    }

    // Load client certificate chain
    let cert_pem = fs::read(&cert_path).map_err(|e| {
        CliError::TlsError(format!("Failed to read {}: {}", cert_path.display(), e))
    })?;
    let client_certs = rustls_pemfile::certs(&mut BufReader::new(cert_pem.as_slice()))
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|e| CliError::TlsError(format!("Failed to parse client cert: {}", e)))?;

    // Load client private key
    let key_pem = fs::read(&key_path)
        .map_err(|e| CliError::TlsError(format!("Failed to read {}: {}", key_path.display(), e)))?;
    let client_key = rustls_pemfile::private_key(&mut BufReader::new(key_pem.as_slice()))
        .map_err(|e| CliError::TlsError(format!("Failed to parse client key: {}", e)))?
        .ok_or_else(|| CliError::TlsError("No private key found in cli.key".to_string()))?;

    let provider = rustls::crypto::ring::default_provider();
    let config = rustls::ClientConfig::builder_with_provider(provider.into())
        .with_safe_default_protocol_versions()
        .map_err(|e| CliError::TlsError(format!("Failed to configure TLS versions: {}", e)))?
        .with_root_certificates(root_store)
        .with_client_auth_cert(client_certs, client_key)
        .map_err(|e| CliError::TlsError(format!("Failed to build TLS config: {}", e)))?;

    Ok(Arc::new(config))
}

/// Check if the daemon has TLS distribution configured.
///
/// Returns true when `ssl_dist.conf` exists in the TLS directory,
/// which means the daemon wrapper generated certs and the BEAM VM
/// was started with `-proto_dist inet_tls`.
pub fn tls_configured() -> bool {
    tls_dir().join("ssl_dist.conf").exists()
}
