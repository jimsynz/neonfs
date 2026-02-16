//! Response types for CLI commands

use super::*;
use serde::Serialize;

/// Cluster status response
#[derive(Debug, Serialize)]
pub struct ClusterStatus {
    pub name: String,
    pub node: String,
    pub status: String,
    pub volumes: u64,
    pub uptime_seconds: u64,
}

/// Cluster initialization response
#[derive(Debug, Serialize)]
pub struct ClusterInitResult {
    pub cluster_id: String,
    pub cluster_name: String,
    pub node_id: String,
    pub node_name: String,
    pub created_at: String,
}

impl ClusterInitResult {
    /// Parse from Erlang term (map)
    pub fn from_term(term: Term) -> Result<Self> {
        let map = term_to_map(&term)?;

        Ok(Self {
            cluster_id: term_to_string(map.get("cluster_id").ok_or_else(|| {
                CliError::TermConversionError("Missing 'cluster_id' field".to_string())
            })?)?,
            cluster_name: term_to_string(map.get("cluster_name").ok_or_else(|| {
                CliError::TermConversionError("Missing 'cluster_name' field".to_string())
            })?)?,
            node_id: term_to_string(map.get("node_id").ok_or_else(|| {
                CliError::TermConversionError("Missing 'node_id' field".to_string())
            })?)?,
            node_name: term_to_string(map.get("node_name").ok_or_else(|| {
                CliError::TermConversionError("Missing 'node_name' field".to_string())
            })?)?,
            created_at: term_to_string(map.get("created_at").ok_or_else(|| {
                CliError::TermConversionError("Missing 'created_at' field".to_string())
            })?)?,
        })
    }
}

impl ClusterStatus {
    /// Parse from Erlang term (map)
    pub fn from_term(term: Term) -> Result<Self> {
        let map = term_to_map(&term)?;

        Ok(Self {
            name: term_to_string(map.get("name").ok_or_else(|| {
                CliError::TermConversionError("Missing 'name' field".to_string())
            })?)?,
            node: term_to_string(map.get("node").ok_or_else(|| {
                CliError::TermConversionError("Missing 'node' field".to_string())
            })?)?,
            status: term_to_string(map.get("status").ok_or_else(|| {
                CliError::TermConversionError("Missing 'status' field".to_string())
            })?)?,
            volumes: term_to_u64(map.get("volumes").ok_or_else(|| {
                CliError::TermConversionError("Missing 'volumes' field".to_string())
            })?)?,
            uptime_seconds: term_to_u64(map.get("uptime_seconds").ok_or_else(|| {
                CliError::TermConversionError("Missing 'uptime_seconds' field".to_string())
            })?)?,
        })
    }

    /// Format uptime as human-readable string
    pub fn uptime_string(&self) -> String {
        let seconds = self.uptime_seconds;
        let days = seconds / 86400;
        let hours = (seconds % 86400) / 3600;
        let minutes = (seconds % 3600) / 60;

        if days > 0 {
            format!("{}d {}h {}m", days, hours, minutes)
        } else if hours > 0 {
            format!("{}h {}m", hours, minutes)
        } else {
            format!("{}m", minutes)
        }
    }
}

/// CA information response
#[derive(Debug, Serialize)]
pub struct CaInfo {
    pub subject: String,
    pub algorithm: String,
    pub valid_from: String,
    pub valid_to: String,
    pub current_serial: u64,
    pub nodes_issued: u64,
}

impl CaInfo {
    /// Parse from Erlang term (map)
    pub fn from_term(term: Term) -> Result<Self> {
        let map = term_to_map(&term)?;

        Ok(Self {
            subject: term_to_string(map.get("subject").ok_or_else(|| {
                CliError::TermConversionError("Missing 'subject' field".to_string())
            })?)?,
            algorithm: term_to_string(map.get("algorithm").ok_or_else(|| {
                CliError::TermConversionError("Missing 'algorithm' field".to_string())
            })?)?,
            valid_from: term_to_string(map.get("valid_from").ok_or_else(|| {
                CliError::TermConversionError("Missing 'valid_from' field".to_string())
            })?)?,
            valid_to: term_to_string(map.get("valid_to").ok_or_else(|| {
                CliError::TermConversionError("Missing 'valid_to' field".to_string())
            })?)?,
            current_serial: term_to_u64(map.get("current_serial").ok_or_else(|| {
                CliError::TermConversionError("Missing 'current_serial' field".to_string())
            })?)?,
            nodes_issued: term_to_u64(map.get("nodes_issued").ok_or_else(|| {
                CliError::TermConversionError("Missing 'nodes_issued' field".to_string())
            })?)?,
        })
    }
}

/// Certificate entry in CA list output
#[derive(Debug, Serialize)]
pub struct CertificateEntry {
    pub node_name: String,
    pub hostname: String,
    pub serial: u64,
    pub expires: String,
    pub status: String,
}

impl CertificateEntry {
    /// Parse from Erlang term (map)
    pub fn from_term(term: Term) -> Result<Self> {
        let map = term_to_map(&term)?;

        Ok(Self {
            node_name: term_to_string(map.get("node_name").ok_or_else(|| {
                CliError::TermConversionError("Missing 'node_name' field".to_string())
            })?)?,
            hostname: term_to_string(map.get("hostname").ok_or_else(|| {
                CliError::TermConversionError("Missing 'hostname' field".to_string())
            })?)?,
            serial: term_to_u64(map.get("serial").ok_or_else(|| {
                CliError::TermConversionError("Missing 'serial' field".to_string())
            })?)?,
            expires: term_to_string(map.get("expires").ok_or_else(|| {
                CliError::TermConversionError("Missing 'expires' field".to_string())
            })?)?,
            status: term_to_string(map.get("status").ok_or_else(|| {
                CliError::TermConversionError("Missing 'status' field".to_string())
            })?)?,
        })
    }
}

/// CA revocation response
#[derive(Debug, Serialize)]
pub struct CaRevokeResult {
    pub serial: u64,
    pub node_name: String,
    pub status: String,
}

impl CaRevokeResult {
    /// Parse from Erlang term (map)
    pub fn from_term(term: Term) -> Result<Self> {
        let map = term_to_map(&term)?;

        Ok(Self {
            serial: term_to_u64(map.get("serial").ok_or_else(|| {
                CliError::TermConversionError("Missing 'serial' field".to_string())
            })?)?,
            node_name: term_to_string(map.get("node_name").ok_or_else(|| {
                CliError::TermConversionError("Missing 'node_name' field".to_string())
            })?)?,
            status: term_to_string(map.get("status").ok_or_else(|| {
                CliError::TermConversionError("Missing 'status' field".to_string())
            })?)?,
        })
    }
}

/// Volume information response
#[derive(Debug, Serialize)]
pub struct VolumeInfo {
    pub id: String,
    pub name: String,
    pub logical_size: u64,
    pub physical_size: u64,
    pub chunk_count: u64,
    pub durability_type: String,
    pub durability_factor: u64,
    pub encryption_mode: String,
    pub encryption_key_version: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rotation: Option<RotationInfo>,
}

/// Embedded rotation info within a volume
#[derive(Debug, Serialize)]
pub struct RotationInfo {
    pub from_version: u64,
    pub to_version: u64,
    pub total_chunks: u64,
    pub migrated: u64,
}

impl VolumeInfo {
    /// Parse from Erlang term (map)
    pub fn from_term(term: Term) -> Result<Self> {
        let map = term_to_map(&term)?;

        // Extract durability config
        let durability = term_to_map(map.get("durability").ok_or_else(|| {
            CliError::TermConversionError("Missing 'durability' field".to_string())
        })?)?;

        // Extract encryption config (optional for backward compatibility)
        let (encryption_mode, encryption_key_version, rotation) =
            if let Some(enc_term) = map.get("encryption") {
                parse_encryption(enc_term)?
            } else {
                ("none".to_string(), 0, None)
            };

        Ok(Self {
            id: term_to_string(
                map.get("id").ok_or_else(|| {
                    CliError::TermConversionError("Missing 'id' field".to_string())
                })?,
            )?,
            name: term_to_string(map.get("name").ok_or_else(|| {
                CliError::TermConversionError("Missing 'name' field".to_string())
            })?)?,
            logical_size: term_to_u64(map.get("logical_size").ok_or_else(|| {
                CliError::TermConversionError("Missing 'logical_size' field".to_string())
            })?)?,
            physical_size: term_to_u64(map.get("physical_size").ok_or_else(|| {
                CliError::TermConversionError("Missing 'physical_size' field".to_string())
            })?)?,
            chunk_count: term_to_u64(map.get("chunk_count").ok_or_else(|| {
                CliError::TermConversionError("Missing 'chunk_count' field".to_string())
            })?)?,
            durability_type: term_to_string(durability.get("type").ok_or_else(|| {
                CliError::TermConversionError("Missing 'type' field in durability".to_string())
            })?)?,
            durability_factor: term_to_u64(durability.get("factor").ok_or_else(|| {
                CliError::TermConversionError("Missing 'factor' field in durability".to_string())
            })?)?,
            encryption_mode,
            encryption_key_version,
            rotation,
        })
    }

    /// Format size in human-readable format
    pub fn format_size(bytes: u64) -> String {
        const KB: u64 = 1024;
        const MB: u64 = KB * 1024;
        const GB: u64 = MB * 1024;
        const TB: u64 = GB * 1024;

        if bytes >= TB {
            format!("{:.1} TB", bytes as f64 / TB as f64)
        } else if bytes >= GB {
            format!("{:.1} GB", bytes as f64 / GB as f64)
        } else if bytes >= MB {
            format!("{:.1} MB", bytes as f64 / MB as f64)
        } else if bytes >= KB {
            format!("{:.1} KB", bytes as f64 / KB as f64)
        } else {
            format!("{} B", bytes)
        }
    }

    /// Format durability as string (e.g., "replicate:3")
    pub fn durability_string(&self) -> String {
        format!("{}:{}", self.durability_type, self.durability_factor)
    }
}

/// Parse encryption map from Erlang term
fn parse_encryption(term: &Term) -> Result<(String, u64, Option<RotationInfo>)> {
    let enc_map = term_to_map(term)?;

    let mode = term_to_string(enc_map.get("mode").ok_or_else(|| {
        CliError::TermConversionError("Missing 'mode' field in encryption".to_string())
    })?)?;

    let key_version = enc_map
        .get("current_key_version")
        .and_then(|t| term_to_u64(t).ok())
        .unwrap_or(0);

    let rotation = if let Some(rot_term) = enc_map.get("rotation") {
        // rotation can be nil (atom)
        match rot_term {
            Term::Atom(Atom { name }) if name == "nil" => None,
            _ => {
                let rot_map = term_to_map(rot_term)?;
                let progress = rot_map
                    .get("progress")
                    .and_then(|t| term_to_map(t).ok())
                    .unwrap_or_default();

                Some(RotationInfo {
                    from_version: rot_map
                        .get("from_version")
                        .and_then(|t| term_to_u64(t).ok())
                        .unwrap_or(0),
                    to_version: rot_map
                        .get("to_version")
                        .and_then(|t| term_to_u64(t).ok())
                        .unwrap_or(0),
                    total_chunks: progress
                        .get("total_chunks")
                        .and_then(|t| term_to_u64(t).ok())
                        .unwrap_or(0),
                    migrated: progress
                        .get("migrated")
                        .and_then(|t| term_to_u64(t).ok())
                        .unwrap_or(0),
                })
            }
        }
    } else {
        None
    };

    Ok((mode, key_version, rotation))
}

/// Key rotation status response
#[derive(Debug, Serialize)]
pub struct RotationStatus {
    pub from_version: u64,
    pub to_version: u64,
    pub total_chunks: u64,
    pub migrated: u64,
    pub started_at: String,
}

impl RotationStatus {
    /// Parse from Erlang term (map)
    pub fn from_term(term: Term) -> Result<Self> {
        let map = term_to_map(&term)?;

        // Handle both start_rotation response and rotation_status response
        let (total_chunks, migrated) = if let Some(progress_term) = map.get("progress") {
            let progress = term_to_map(progress_term)?;
            let total = progress
                .get("total_chunks")
                .and_then(|t| term_to_u64(t).ok())
                .unwrap_or(0);
            let mig = progress
                .get("migrated")
                .and_then(|t| term_to_u64(t).ok())
                .unwrap_or(0);
            (total, mig)
        } else {
            let total = map
                .get("total_chunks")
                .and_then(|t| term_to_u64(t).ok())
                .unwrap_or(0);
            (total, 0)
        };

        let started_at = map
            .get("started_at")
            .and_then(|t| term_to_string(t).ok())
            .unwrap_or_default();

        Ok(Self {
            from_version: term_to_u64(map.get("from_version").ok_or_else(|| {
                CliError::TermConversionError("Missing 'from_version' field".to_string())
            })?)?,
            to_version: term_to_u64(map.get("to_version").ok_or_else(|| {
                CliError::TermConversionError("Missing 'to_version' field".to_string())
            })?)?,
            total_chunks,
            migrated,
            started_at,
        })
    }
}

/// Volume ACL information
#[derive(Debug, Serialize)]
pub struct AclInfo {
    pub volume_id: String,
    pub owner_uid: u64,
    pub owner_gid: u64,
    pub entries: Vec<AclEntry>,
}

/// Single ACL entry
#[derive(Debug, Serialize)]
pub struct AclEntry {
    pub principal: String,
    pub permissions: Vec<String>,
}

impl AclInfo {
    /// Parse from Erlang term (map)
    pub fn from_term(term: Term) -> Result<Self> {
        let map = term_to_map(&term)?;

        let entries_terms = map
            .get("entries")
            .and_then(|t| term_to_list(t).ok())
            .unwrap_or_default();

        let entries: Result<Vec<AclEntry>> = entries_terms
            .into_iter()
            .map(|t| {
                let entry_map = term_to_map(&t)?;
                let principal = term_to_string(entry_map.get("principal").ok_or_else(|| {
                    CliError::TermConversionError("Missing 'principal' field".to_string())
                })?)?;
                let perm_terms = entry_map
                    .get("permissions")
                    .and_then(|t| term_to_list(t).ok())
                    .unwrap_or_default();
                let permissions: Result<Vec<String>> =
                    perm_terms.iter().map(term_to_string).collect();
                Ok(AclEntry {
                    principal,
                    permissions: permissions?,
                })
            })
            .collect();

        Ok(Self {
            volume_id: term_to_string(map.get("volume_id").ok_or_else(|| {
                CliError::TermConversionError("Missing 'volume_id' field".to_string())
            })?)?,
            owner_uid: term_to_u64(map.get("owner_uid").ok_or_else(|| {
                CliError::TermConversionError("Missing 'owner_uid' field".to_string())
            })?)?,
            owner_gid: term_to_u64(map.get("owner_gid").ok_or_else(|| {
                CliError::TermConversionError("Missing 'owner_gid' field".to_string())
            })?)?,
            entries: entries?,
        })
    }
}

/// File ACL information
#[derive(Debug, Serialize)]
pub struct FileAclInfo {
    pub mode: u64,
    pub uid: u64,
    pub gid: u64,
    pub acl_entries: Vec<String>,
}

impl FileAclInfo {
    /// Parse from Erlang term (map)
    pub fn from_term(term: Term) -> Result<Self> {
        let map = term_to_map(&term)?;

        let acl_entries = map
            .get("acl_entries")
            .and_then(|t| term_to_list(t).ok())
            .unwrap_or_default()
            .iter()
            .filter_map(|t| term_to_string(t).ok())
            .collect();

        Ok(Self {
            mode: term_to_u64(map.get("mode").ok_or_else(|| {
                CliError::TermConversionError("Missing 'mode' field".to_string())
            })?)?,
            uid: term_to_u64(map.get("uid").ok_or_else(|| {
                CliError::TermConversionError("Missing 'uid' field".to_string())
            })?)?,
            gid: term_to_u64(map.get("gid").ok_or_else(|| {
                CliError::TermConversionError("Missing 'gid' field".to_string())
            })?)?,
            acl_entries,
        })
    }
}

/// Audit log entry
#[derive(Debug, Serialize)]
pub struct AuditEntry {
    pub id: String,
    pub timestamp: String,
    pub event_type: String,
    pub actor_uid: u64,
    pub actor_node: String,
    pub resource: String,
    pub outcome: String,
}

impl AuditEntry {
    /// Parse from Erlang term (map)
    pub fn from_term(term: Term) -> Result<Self> {
        let map = term_to_map(&term)?;

        Ok(Self {
            id: term_to_string(
                map.get("id").ok_or_else(|| {
                    CliError::TermConversionError("Missing 'id' field".to_string())
                })?,
            )?,
            timestamp: term_to_string(map.get("timestamp").ok_or_else(|| {
                CliError::TermConversionError("Missing 'timestamp' field".to_string())
            })?)?,
            event_type: term_to_string(map.get("event_type").ok_or_else(|| {
                CliError::TermConversionError("Missing 'event_type' field".to_string())
            })?)?,
            actor_uid: term_to_u64(map.get("actor_uid").ok_or_else(|| {
                CliError::TermConversionError("Missing 'actor_uid' field".to_string())
            })?)?,
            actor_node: term_to_string(map.get("actor_node").ok_or_else(|| {
                CliError::TermConversionError("Missing 'actor_node' field".to_string())
            })?)?,
            resource: term_to_string(map.get("resource").ok_or_else(|| {
                CliError::TermConversionError("Missing 'resource' field".to_string())
            })?)?,
            outcome: term_to_string(map.get("outcome").ok_or_else(|| {
                CliError::TermConversionError("Missing 'outcome' field".to_string())
            })?)?,
        })
    }
}

/// Mount information response
#[derive(Debug, Serialize)]
pub struct MountInfo {
    #[serde(rename = "mount_id")]
    pub id: String,
    pub volume_name: String,
    pub mount_point: String,
    #[serde(rename = "mounted_at")]
    pub started_at: String,
}

impl MountInfo {
    /// Parse from Erlang term (map)
    pub fn from_term(term: Term) -> Result<Self> {
        let map = term_to_map(&term)?;

        Ok(Self {
            id: term_to_string(
                map.get("id").ok_or_else(|| {
                    CliError::TermConversionError("Missing 'id' field".to_string())
                })?,
            )?,
            volume_name: term_to_string(map.get("volume_name").ok_or_else(|| {
                CliError::TermConversionError("Missing 'volume_name' field".to_string())
            })?)?,
            mount_point: term_to_string(map.get("mount_point").ok_or_else(|| {
                CliError::TermConversionError("Missing 'mount_point' field".to_string())
            })?)?,
            started_at: term_to_string(map.get("started_at").ok_or_else(|| {
                CliError::TermConversionError("Missing 'started_at' field".to_string())
            })?)?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_volume_format_size() {
        assert_eq!(VolumeInfo::format_size(512), "512 B");
        assert_eq!(VolumeInfo::format_size(1536), "1.5 KB");
        assert_eq!(VolumeInfo::format_size(1_572_864), "1.5 MB");
        assert_eq!(VolumeInfo::format_size(1_610_612_736), "1.5 GB");
    }

    #[test]
    fn test_cluster_status_uptime() {
        let status = ClusterStatus {
            name: "test".to_string(),
            node: "test@localhost".to_string(),
            status: "running".to_string(),
            volumes: 3,
            uptime_seconds: 186300, // 2d 3h 45m
        };
        assert_eq!(status.uptime_string(), "2d 3h 45m");
    }

    #[test]
    fn test_volume_durability_string() {
        let volume = VolumeInfo {
            id: "test-id".to_string(),
            name: "test".to_string(),
            logical_size: 0,
            physical_size: 0,
            chunk_count: 0,
            durability_type: "replicate".to_string(),
            durability_factor: 3,
            encryption_mode: "none".to_string(),
            encryption_key_version: 0,
            rotation: None,
        };
        assert_eq!(volume.durability_string(), "replicate:3");
    }

    #[test]
    fn test_rotation_status_json_serialization() {
        let status = RotationStatus {
            from_version: 1,
            to_version: 2,
            total_chunks: 100,
            migrated: 50,
            started_at: "2026-01-01T00:00:00Z".to_string(),
        };
        let json = serde_json::to_value(&status).unwrap();
        assert_eq!(json["from_version"], 1);
        assert_eq!(json["to_version"], 2);
        assert_eq!(json["total_chunks"], 100);
        assert_eq!(json["migrated"], 50);
    }

    #[test]
    fn test_acl_info_json_serialization() {
        let acl = AclInfo {
            volume_id: "vol-1".to_string(),
            owner_uid: 1000,
            owner_gid: 1000,
            entries: vec![AclEntry {
                principal: "uid:1001".to_string(),
                permissions: vec!["read".to_string(), "write".to_string()],
            }],
        };
        let json = serde_json::to_value(&acl).unwrap();
        assert_eq!(json["owner_uid"], 1000);
        assert_eq!(json["entries"][0]["principal"], "uid:1001");
    }

    #[test]
    fn test_audit_entry_json_serialization() {
        let entry = AuditEntry {
            id: "evt-1".to_string(),
            timestamp: "2026-01-01T00:00:00Z".to_string(),
            event_type: "volume_created".to_string(),
            actor_uid: 0,
            actor_node: "neonfs_core@localhost".to_string(),
            resource: "vol-1".to_string(),
            outcome: "success".to_string(),
        };
        let json = serde_json::to_value(&entry).unwrap();
        assert_eq!(json["event_type"], "volume_created");
        assert_eq!(json["outcome"], "success");
    }

    #[test]
    fn test_ca_info_json_serialization() {
        let info = CaInfo {
            subject: "/O=NeonFS/CN=test-cluster CA".to_string(),
            algorithm: "ECDSA P-256".to_string(),
            valid_from: "2026-02-16T00:00:00Z".to_string(),
            valid_to: "2036-02-16T00:00:00Z".to_string(),
            current_serial: 3,
            nodes_issued: 3,
        };
        let json = serde_json::to_value(&info).unwrap();
        assert_eq!(json["algorithm"], "ECDSA P-256");
        assert_eq!(json["current_serial"], 3);
        assert_eq!(json["nodes_issued"], 3);
    }

    #[test]
    fn test_ca_info_from_term() {
        let term = Term::Map(eetf::Map {
            entries: vec![
                (
                    Term::Binary(Binary {
                        bytes: b"subject".to_vec(),
                    }),
                    Term::Binary(Binary {
                        bytes: b"/O=NeonFS/CN=test CA".to_vec(),
                    }),
                ),
                (
                    Term::Binary(Binary {
                        bytes: b"algorithm".to_vec(),
                    }),
                    Term::Binary(Binary {
                        bytes: b"ECDSA P-256".to_vec(),
                    }),
                ),
                (
                    Term::Binary(Binary {
                        bytes: b"valid_from".to_vec(),
                    }),
                    Term::Binary(Binary {
                        bytes: b"2026-02-16T00:00:00Z".to_vec(),
                    }),
                ),
                (
                    Term::Binary(Binary {
                        bytes: b"valid_to".to_vec(),
                    }),
                    Term::Binary(Binary {
                        bytes: b"2036-02-16T00:00:00Z".to_vec(),
                    }),
                ),
                (
                    Term::Binary(Binary {
                        bytes: b"current_serial".to_vec(),
                    }),
                    Term::FixInteger(FixInteger::from(5)),
                ),
                (
                    Term::Binary(Binary {
                        bytes: b"nodes_issued".to_vec(),
                    }),
                    Term::FixInteger(FixInteger::from(5)),
                ),
            ],
        });
        let info = CaInfo::from_term(term).unwrap();
        assert_eq!(info.subject, "/O=NeonFS/CN=test CA");
        assert_eq!(info.current_serial, 5);
    }

    #[test]
    fn test_certificate_entry_from_term() {
        let term = Term::Map(eetf::Map {
            entries: vec![
                (
                    Term::Binary(Binary {
                        bytes: b"node_name".to_vec(),
                    }),
                    Term::Binary(Binary {
                        bytes: b"/O=NeonFS/CN=node-1".to_vec(),
                    }),
                ),
                (
                    Term::Binary(Binary {
                        bytes: b"hostname".to_vec(),
                    }),
                    Term::Binary(Binary {
                        bytes: b"node-1.example.com".to_vec(),
                    }),
                ),
                (
                    Term::Binary(Binary {
                        bytes: b"serial".to_vec(),
                    }),
                    Term::FixInteger(FixInteger::from(1)),
                ),
                (
                    Term::Binary(Binary {
                        bytes: b"expires".to_vec(),
                    }),
                    Term::Binary(Binary {
                        bytes: b"2027-02-16T00:00:00Z".to_vec(),
                    }),
                ),
                (
                    Term::Binary(Binary {
                        bytes: b"status".to_vec(),
                    }),
                    Term::Binary(Binary {
                        bytes: b"valid".to_vec(),
                    }),
                ),
            ],
        });
        let entry = CertificateEntry::from_term(term).unwrap();
        assert_eq!(entry.hostname, "node-1.example.com");
        assert_eq!(entry.serial, 1);
        assert_eq!(entry.status, "valid");
    }

    #[test]
    fn test_ca_revoke_result_from_term() {
        let term = Term::Map(eetf::Map {
            entries: vec![
                (
                    Term::Binary(Binary {
                        bytes: b"serial".to_vec(),
                    }),
                    Term::FixInteger(FixInteger::from(2)),
                ),
                (
                    Term::Binary(Binary {
                        bytes: b"node_name".to_vec(),
                    }),
                    Term::Binary(Binary {
                        bytes: b"/O=NeonFS/CN=node-2".to_vec(),
                    }),
                ),
                (
                    Term::Binary(Binary {
                        bytes: b"status".to_vec(),
                    }),
                    Term::Binary(Binary {
                        bytes: b"revoked".to_vec(),
                    }),
                ),
            ],
        });
        let result = CaRevokeResult::from_term(term).unwrap();
        assert_eq!(result.serial, 2);
        assert_eq!(result.node_name, "/O=NeonFS/CN=node-2");
        assert_eq!(result.status, "revoked");
    }
}
