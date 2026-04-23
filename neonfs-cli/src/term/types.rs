//! Response types for CLI commands

use super::*;
use serde::Serialize;

/// Drive information response
#[derive(Debug, Serialize)]
pub struct DriveInfo {
    pub id: String,
    pub node: String,
    pub path: String,
    pub tier: String,
    pub capacity_bytes: u64,
    pub used_bytes: u64,
    pub state: String,
}

impl DriveInfo {
    /// Parse from Erlang term (map)
    pub fn from_term(term: Term) -> Result<Self> {
        let map = term_to_map(&term)?;

        Ok(Self {
            id: term_to_string(
                map.get("id").ok_or_else(|| {
                    CliError::TermConversionError("Missing 'id' field".to_string())
                })?,
            )?,
            node: term_to_string(map.get("node").ok_or_else(|| {
                CliError::TermConversionError("Missing 'node' field".to_string())
            })?)?,
            path: term_to_string(map.get("path").ok_or_else(|| {
                CliError::TermConversionError("Missing 'path' field".to_string())
            })?)?,
            tier: term_to_string(map.get("tier").ok_or_else(|| {
                CliError::TermConversionError("Missing 'tier' field".to_string())
            })?)?,
            capacity_bytes: term_to_u64(map.get("capacity_bytes").ok_or_else(|| {
                CliError::TermConversionError("Missing 'capacity_bytes' field".to_string())
            })?)?,
            used_bytes: term_to_u64(map.get("used_bytes").ok_or_else(|| {
                CliError::TermConversionError("Missing 'used_bytes' field".to_string())
            })?)?,
            state: term_to_string(map.get("state").ok_or_else(|| {
                CliError::TermConversionError("Missing 'state' field".to_string())
            })?)?,
        })
    }

    /// Format capacity in human-readable format (0 = "unlimited")
    pub fn format_capacity(bytes: u64) -> String {
        if bytes == 0 {
            "unlimited".to_string()
        } else {
            Self::format_bytes(bytes)
        }
    }

    /// Format bytes in human-readable format (0 = "0 B")
    pub fn format_bytes(bytes: u64) -> String {
        const KB: u64 = 1024;
        const MB: u64 = KB * 1024;
        const GB: u64 = MB * 1024;
        const TB: u64 = GB * 1024;

        if bytes >= TB {
            format!("{:.1} TiB", bytes as f64 / TB as f64)
        } else if bytes >= GB {
            format!("{:.1} GiB", bytes as f64 / GB as f64)
        } else if bytes >= MB {
            format!("{:.1} MiB", bytes as f64 / MB as f64)
        } else if bytes >= KB {
            format!("{:.1} KiB", bytes as f64 / KB as f64)
        } else {
            format!("{} B", bytes)
        }
    }
}

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

/// Node information response
#[derive(Debug, Serialize)]
pub struct NodeInfo {
    pub node: String,
    #[serde(rename = "type")]
    pub node_type: String,
    pub role: String,
    pub status: String,
    pub uptime_seconds: u64,
}

impl NodeInfo {
    /// Parse from Erlang term (map)
    pub fn from_term(term: Term) -> Result<Self> {
        let map = term_to_map(&term)?;

        Ok(Self {
            node: term_to_string(map.get("node").ok_or_else(|| {
                CliError::TermConversionError("Missing 'node' field".to_string())
            })?)?,
            node_type: term_to_string(map.get("type").ok_or_else(|| {
                CliError::TermConversionError("Missing 'type' field".to_string())
            })?)?,
            role: term_to_string(map.get("role").ok_or_else(|| {
                CliError::TermConversionError("Missing 'role' field".to_string())
            })?)?,
            status: term_to_string(map.get("status").ok_or_else(|| {
                CliError::TermConversionError("Missing 'status' field".to_string())
            })?)?,
            uptime_seconds: term_to_u64(map.get("uptime_seconds").ok_or_else(|| {
                CliError::TermConversionError("Missing 'uptime_seconds' field".to_string())
            })?)?,
        })
    }

    /// Format uptime as human-readable string
    pub fn format_uptime(&self) -> String {
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

/// `cluster remove-node` response
#[derive(Debug, Serialize)]
pub struct RemoveNodeResult {
    pub node: String,
    pub status: String,
    pub remaining_members: u64,
    pub certificate_revoked: bool,
}

impl RemoveNodeResult {
    /// Parse from Erlang term (map)
    pub fn from_term(term: Term) -> Result<Self> {
        let map = term_to_map(&term)?;

        Ok(Self {
            node: term_to_string(map.get("node").ok_or_else(|| {
                CliError::TermConversionError("Missing 'node' field".to_string())
            })?)?,
            status: term_to_string(map.get("status").ok_or_else(|| {
                CliError::TermConversionError("Missing 'status' field".to_string())
            })?)?,
            remaining_members: term_to_u64(map.get("remaining_members").ok_or_else(|| {
                CliError::TermConversionError("Missing 'remaining_members' field".to_string())
            })?)?,
            certificate_revoked: term_to_bool(map.get("certificate_revoked").ok_or_else(
                || CliError::TermConversionError("Missing 'certificate_revoked' field".to_string()),
            )?)?,
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
    pub node: String,
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
            node: term_to_string(map.get("node").ok_or_else(|| {
                CliError::TermConversionError("Missing 'node' field".to_string())
            })?)?,
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

/// NFS export information response
#[derive(Debug, Serialize)]
pub struct NfsExportInfo {
    pub node: String,
    pub volume_name: String,
    pub exported_at: String,
    pub server_address: String,
    pub port: u16,
}

impl NfsExportInfo {
    /// Parse from Erlang term (map)
    pub fn from_term(term: Term) -> Result<Self> {
        let map = term_to_map(&term)?;

        Ok(Self {
            node: term_to_string(map.get("node").ok_or_else(|| {
                CliError::TermConversionError("Missing 'node' field".to_string())
            })?)?,
            volume_name: term_to_string(map.get("volume_name").ok_or_else(|| {
                CliError::TermConversionError("Missing 'volume_name' field".to_string())
            })?)?,
            exported_at: term_to_string(map.get("exported_at").ok_or_else(|| {
                CliError::TermConversionError("Missing 'exported_at' field".to_string())
            })?)?,
            server_address: term_to_string(map.get("server_address").ok_or_else(|| {
                CliError::TermConversionError("Missing 'server_address' field".to_string())
            })?)?,
            port: term_to_u64(map.get("port").ok_or_else(|| {
                CliError::TermConversionError("Missing 'port' field".to_string())
            })?)? as u16,
        })
    }
}

/// A single choice option on an escalation
#[derive(Debug, Serialize)]
pub struct EscalationOption {
    pub value: String,
    pub label: String,
}

impl EscalationOption {
    /// Parse from Erlang term (map with `value` and `label` keys)
    pub fn from_term(term: &Term) -> Result<Self> {
        let map = term_to_map(term)?;

        Ok(Self {
            value: term_to_string(map.get("value").ok_or_else(|| {
                CliError::TermConversionError("Missing 'value' field in option".to_string())
            })?)?,
            label: term_to_string(map.get("label").ok_or_else(|| {
                CliError::TermConversionError("Missing 'label' field in option".to_string())
            })?)?,
        })
    }
}

/// A decision escalation awaiting operator input
#[derive(Debug, Serialize)]
pub struct EscalationEntry {
    pub id: String,
    pub category: String,
    pub severity: String,
    pub description: String,
    pub options: Vec<EscalationOption>,
    pub status: String,
    pub choice: Option<String>,
    pub created_at: String,
    pub expires_at: Option<String>,
    pub resolved_at: Option<String>,
}

impl EscalationEntry {
    /// Parse from Erlang term (map)
    pub fn from_term(term: Term) -> Result<Self> {
        let map = term_to_map(&term)?;

        let option_terms = map
            .get("options")
            .and_then(|t| term_to_list(t).ok())
            .unwrap_or_default();
        let options: Result<Vec<EscalationOption>> = option_terms
            .iter()
            .map(EscalationOption::from_term)
            .collect();

        Ok(Self {
            id: term_to_string(
                map.get("id").ok_or_else(|| {
                    CliError::TermConversionError("Missing 'id' field".to_string())
                })?,
            )?,
            category: term_to_string(map.get("category").ok_or_else(|| {
                CliError::TermConversionError("Missing 'category' field".to_string())
            })?)?,
            severity: term_to_string(map.get("severity").ok_or_else(|| {
                CliError::TermConversionError("Missing 'severity' field".to_string())
            })?)?,
            description: term_to_string(map.get("description").ok_or_else(|| {
                CliError::TermConversionError("Missing 'description' field".to_string())
            })?)?,
            options: options?,
            status: term_to_string(map.get("status").ok_or_else(|| {
                CliError::TermConversionError("Missing 'status' field".to_string())
            })?)?,
            choice: map
                .get("choice")
                .and_then(|t| term_to_string(t).ok())
                .filter(|s| s != "nil"),
            created_at: term_to_string(map.get("created_at").ok_or_else(|| {
                CliError::TermConversionError("Missing 'created_at' field".to_string())
            })?)?,
            expires_at: map
                .get("expires_at")
                .and_then(|t| term_to_string(t).ok())
                .filter(|s| s != "nil"),
            resolved_at: map
                .get("resolved_at")
                .and_then(|t| term_to_string(t).ok())
                .filter(|s| s != "nil"),
        })
    }

    /// Returns a truncated ID for table display
    pub fn id_short(&self) -> String {
        if self.id.len() > 12 {
            self.id[..12].to_string()
        } else {
            self.id.clone()
        }
    }
}

/// Job information response
#[derive(Debug, Serialize)]
pub struct JobInfo {
    pub id: String,
    #[serde(rename = "type")]
    pub job_type: String,
    pub node: String,
    pub status: String,
    pub progress_total: u64,
    pub progress_completed: u64,
    pub progress_description: String,
    pub params: std::collections::HashMap<String, String>,
    pub error: Option<String>,
    pub created_at: String,
    pub started_at: Option<String>,
    pub updated_at: String,
    pub completed_at: Option<String>,
}

impl JobInfo {
    /// Parse from Erlang term (map)
    pub fn from_term(term: Term) -> Result<Self> {
        let map = term_to_map(&term)?;

        let params = match map.get("params") {
            Some(params_term) => match term_to_map(params_term) {
                Ok(m) => m
                    .into_iter()
                    .filter_map(|(k, v)| term_to_string(&v).ok().map(|vs| (k, vs)))
                    .collect(),
                Err(_) => std::collections::HashMap::new(),
            },
            None => std::collections::HashMap::new(),
        };

        Ok(Self {
            id: term_to_string(
                map.get("id").ok_or_else(|| {
                    CliError::TermConversionError("Missing 'id' field".to_string())
                })?,
            )?,
            job_type: term_to_string(map.get("type").ok_or_else(|| {
                CliError::TermConversionError("Missing 'type' field".to_string())
            })?)?,
            node: term_to_string(map.get("node").ok_or_else(|| {
                CliError::TermConversionError("Missing 'node' field".to_string())
            })?)?,
            status: term_to_string(map.get("status").ok_or_else(|| {
                CliError::TermConversionError("Missing 'status' field".to_string())
            })?)?,
            progress_total: term_to_u64(map.get("progress_total").ok_or_else(|| {
                CliError::TermConversionError("Missing 'progress_total' field".to_string())
            })?)?,
            progress_completed: term_to_u64(map.get("progress_completed").ok_or_else(|| {
                CliError::TermConversionError("Missing 'progress_completed' field".to_string())
            })?)?,
            progress_description: term_to_string(map.get("progress_description").ok_or_else(
                || {
                    CliError::TermConversionError(
                        "Missing 'progress_description' field".to_string(),
                    )
                },
            )?)?,
            params,
            error: map
                .get("error")
                .and_then(|t| term_to_string(t).ok())
                .filter(|s| s != "nil"),
            created_at: term_to_string(map.get("created_at").ok_or_else(|| {
                CliError::TermConversionError("Missing 'created_at' field".to_string())
            })?)?,
            started_at: map
                .get("started_at")
                .and_then(|t| term_to_string(t).ok())
                .filter(|s| s != "nil"),
            updated_at: term_to_string(map.get("updated_at").ok_or_else(|| {
                CliError::TermConversionError("Missing 'updated_at' field".to_string())
            })?)?,
            completed_at: map
                .get("completed_at")
                .and_then(|t| term_to_string(t).ok())
                .filter(|s| s != "nil"),
        })
    }

    /// Returns a truncated ID for table display
    pub fn id_short(&self) -> String {
        if self.id.len() > 12 {
            self.id[..12].to_string()
        } else {
            self.id.clone()
        }
    }

    /// Returns progress as "completed/total (percent%)"
    pub fn progress_string(&self) -> String {
        if self.progress_total == 0 {
            return "—".to_string();
        }
        let percent = (self.progress_completed * 100) / self.progress_total;
        format!(
            "{}/{} ({}%)",
            self.progress_completed, self.progress_total, percent
        )
    }

    /// Returns detailed progress with description
    pub fn progress_detail(&self) -> String {
        let base = self.progress_string();
        if self.progress_description.is_empty() {
            base
        } else {
            format!("{} — {}", base, self.progress_description)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_drive_info_format_capacity() {
        assert_eq!(DriveInfo::format_capacity(0), "unlimited");
        assert_eq!(DriveInfo::format_capacity(1_099_511_627_776), "1.0 TiB");
        assert_eq!(DriveInfo::format_capacity(1_073_741_824), "1.0 GiB");
        assert_eq!(DriveInfo::format_capacity(104_857_600), "100.0 MiB");
    }

    #[test]
    fn test_drive_info_format_bytes() {
        assert_eq!(DriveInfo::format_bytes(0), "0 B");
        assert_eq!(DriveInfo::format_bytes(512), "512 B");
        assert_eq!(DriveInfo::format_bytes(1_099_511_627_776), "1.0 TiB");
        assert_eq!(DriveInfo::format_bytes(1_073_741_824), "1.0 GiB");
        assert_eq!(DriveInfo::format_bytes(104_857_600), "100.0 MiB");
    }

    #[test]
    fn test_drive_info_from_term() {
        let term = Term::Map(eetf::Map {
            map: HashMap::from([
                (
                    Term::Binary(Binary {
                        bytes: b"id".to_vec(),
                    }),
                    Term::Binary(Binary {
                        bytes: b"nvme0".to_vec(),
                    }),
                ),
                (
                    Term::Binary(Binary {
                        bytes: b"node".to_vec(),
                    }),
                    Term::Binary(Binary {
                        bytes: b"neonfs_core@localhost".to_vec(),
                    }),
                ),
                (
                    Term::Binary(Binary {
                        bytes: b"path".to_vec(),
                    }),
                    Term::Binary(Binary {
                        bytes: b"/data/nvme0".to_vec(),
                    }),
                ),
                (
                    Term::Binary(Binary {
                        bytes: b"tier".to_vec(),
                    }),
                    Term::Binary(Binary {
                        bytes: b"hot".to_vec(),
                    }),
                ),
                (
                    Term::Binary(Binary {
                        bytes: b"capacity_bytes".to_vec(),
                    }),
                    Term::FixInteger(FixInteger::from(0)),
                ),
                (
                    Term::Binary(Binary {
                        bytes: b"used_bytes".to_vec(),
                    }),
                    Term::FixInteger(FixInteger::from(0)),
                ),
                (
                    Term::Binary(Binary {
                        bytes: b"state".to_vec(),
                    }),
                    Term::Binary(Binary {
                        bytes: b"active".to_vec(),
                    }),
                ),
            ]),
        });
        let info = DriveInfo::from_term(term).unwrap();
        assert_eq!(info.id, "nvme0");
        assert_eq!(info.path, "/data/nvme0");
        assert_eq!(info.tier, "hot");
    }

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
            map: HashMap::from([
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
            ]),
        });
        let info = CaInfo::from_term(term).unwrap();
        assert_eq!(info.subject, "/O=NeonFS/CN=test CA");
        assert_eq!(info.current_serial, 5);
    }

    #[test]
    fn test_certificate_entry_from_term() {
        let term = Term::Map(eetf::Map {
            map: HashMap::from([
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
            ]),
        });
        let entry = CertificateEntry::from_term(term).unwrap();
        assert_eq!(entry.hostname, "node-1.example.com");
        assert_eq!(entry.serial, 1);
        assert_eq!(entry.status, "valid");
    }

    #[test]
    fn test_job_info_progress_string() {
        let job = JobInfo {
            id: "abc123def456abcd".to_string(),
            job_type: "key-rotation".to_string(),
            node: "neonfs_core@localhost".to_string(),
            status: "running".to_string(),
            progress_total: 1000,
            progress_completed: 450,
            progress_description: "Re-encrypting chunks".to_string(),
            params: std::collections::HashMap::new(),
            error: None,
            created_at: "2026-02-20T08:15:00Z".to_string(),
            started_at: Some("2026-02-20T08:15:01Z".to_string()),
            updated_at: "2026-02-20T08:20:00Z".to_string(),
            completed_at: None,
        };
        assert_eq!(job.progress_string(), "450/1000 (45%)");
        assert_eq!(
            job.progress_detail(),
            "450/1000 (45%) — Re-encrypting chunks"
        );
        assert_eq!(job.id_short(), "abc123def456");
    }

    #[test]
    fn test_job_info_json_serialization() {
        let job = JobInfo {
            id: "abc123".to_string(),
            job_type: "key-rotation".to_string(),
            node: "node1@localhost".to_string(),
            status: "completed".to_string(),
            progress_total: 100,
            progress_completed: 100,
            progress_description: "Complete".to_string(),
            params: std::collections::HashMap::new(),
            error: None,
            created_at: "2026-02-20T08:15:00Z".to_string(),
            started_at: Some("2026-02-20T08:15:01Z".to_string()),
            updated_at: "2026-02-20T08:20:00Z".to_string(),
            completed_at: Some("2026-02-20T08:20:00Z".to_string()),
        };
        let json = serde_json::to_value(&job).unwrap();
        assert_eq!(json["type"], "key-rotation");
        assert_eq!(json["status"], "completed");
        assert_eq!(json["progress_total"], 100);
        assert_eq!(json["progress_completed"], 100);
    }

    #[test]
    fn test_ca_revoke_result_from_term() {
        let term = Term::Map(eetf::Map {
            map: HashMap::from([
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
            ]),
        });
        let result = CaRevokeResult::from_term(term).unwrap();
        assert_eq!(result.serial, 2);
        assert_eq!(result.node_name, "/O=NeonFS/CN=node-2");
        assert_eq!(result.status, "revoked");
    }

    #[test]
    fn test_node_info_from_term() {
        let term = Term::Map(eetf::Map {
            map: HashMap::from([
                (
                    Term::Binary(Binary {
                        bytes: b"node".to_vec(),
                    }),
                    Term::Binary(Binary {
                        bytes: b"neonfs_core@host1".to_vec(),
                    }),
                ),
                (
                    Term::Binary(Binary {
                        bytes: b"type".to_vec(),
                    }),
                    Term::Binary(Binary {
                        bytes: b"core".to_vec(),
                    }),
                ),
                (
                    Term::Binary(Binary {
                        bytes: b"role".to_vec(),
                    }),
                    Term::Binary(Binary {
                        bytes: b"leader".to_vec(),
                    }),
                ),
                (
                    Term::Binary(Binary {
                        bytes: b"status".to_vec(),
                    }),
                    Term::Binary(Binary {
                        bytes: b"online".to_vec(),
                    }),
                ),
                (
                    Term::Binary(Binary {
                        bytes: b"uptime_seconds".to_vec(),
                    }),
                    Term::FixInteger(FixInteger::from(186300)),
                ),
            ]),
        });
        let info = NodeInfo::from_term(term).unwrap();
        assert_eq!(info.node, "neonfs_core@host1");
        assert_eq!(info.node_type, "core");
        assert_eq!(info.role, "leader");
        assert_eq!(info.status, "online");
        assert_eq!(info.uptime_seconds, 186300);
    }

    #[test]
    fn test_escalation_entry_from_term() {
        let option_map: HashMap<Term, Term> = HashMap::from([
            (
                Term::Binary(Binary {
                    bytes: b"value".to_vec(),
                }),
                Term::Binary(Binary {
                    bytes: b"approve".to_vec(),
                }),
            ),
            (
                Term::Binary(Binary {
                    bytes: b"label".to_vec(),
                }),
                Term::Binary(Binary {
                    bytes: b"Approve write".to_vec(),
                }),
            ),
        ]);

        let entries = vec![
            (
                Term::Binary(Binary {
                    bytes: b"id".to_vec(),
                }),
                Term::Binary(Binary {
                    bytes: b"esc-abc-123".to_vec(),
                }),
            ),
            (
                Term::Binary(Binary {
                    bytes: b"category".to_vec(),
                }),
                Term::Binary(Binary {
                    bytes: b"quorum_loss".to_vec(),
                }),
            ),
            (
                Term::Binary(Binary {
                    bytes: b"severity".to_vec(),
                }),
                Term::Binary(Binary {
                    bytes: b"critical".to_vec(),
                }),
            ),
            (
                Term::Binary(Binary {
                    bytes: b"description".to_vec(),
                }),
                Term::Binary(Binary {
                    bytes: b"Quorum lost during write".to_vec(),
                }),
            ),
            (
                Term::Binary(Binary {
                    bytes: b"options".to_vec(),
                }),
                Term::List(eetf::List {
                    elements: vec![Term::Map(eetf::Map { map: option_map })],
                }),
            ),
            (
                Term::Binary(Binary {
                    bytes: b"status".to_vec(),
                }),
                Term::Binary(Binary {
                    bytes: b"pending".to_vec(),
                }),
            ),
            (
                Term::Binary(Binary {
                    bytes: b"choice".to_vec(),
                }),
                Term::Atom(Atom::from("nil")),
            ),
            (
                Term::Binary(Binary {
                    bytes: b"created_at".to_vec(),
                }),
                Term::Binary(Binary {
                    bytes: b"2026-04-20T10:00:00Z".to_vec(),
                }),
            ),
            (
                Term::Binary(Binary {
                    bytes: b"expires_at".to_vec(),
                }),
                Term::Binary(Binary {
                    bytes: b"2026-04-21T10:00:00Z".to_vec(),
                }),
            ),
            (
                Term::Binary(Binary {
                    bytes: b"resolved_at".to_vec(),
                }),
                Term::Atom(Atom::from("nil")),
            ),
        ];

        let term = Term::Map(eetf::Map {
            map: entries.into_iter().collect(),
        });

        let entry = EscalationEntry::from_term(term).unwrap();
        assert_eq!(entry.id, "esc-abc-123");
        assert_eq!(entry.category, "quorum_loss");
        assert_eq!(entry.severity, "critical");
        assert_eq!(entry.status, "pending");
        assert_eq!(entry.options.len(), 1);
        assert_eq!(entry.options[0].value, "approve");
        assert_eq!(entry.options[0].label, "Approve write");
        assert_eq!(entry.choice, None);
        assert_eq!(entry.resolved_at, None);
        assert_eq!(entry.expires_at.as_deref(), Some("2026-04-21T10:00:00Z"));
    }

    #[test]
    fn test_escalation_entry_resolved_from_term() {
        let entries = vec![
            (
                Term::Binary(Binary {
                    bytes: b"id".to_vec(),
                }),
                Term::Binary(Binary {
                    bytes: b"esc-x".to_vec(),
                }),
            ),
            (
                Term::Binary(Binary {
                    bytes: b"category".to_vec(),
                }),
                Term::Binary(Binary {
                    bytes: b"drive_flapping".to_vec(),
                }),
            ),
            (
                Term::Binary(Binary {
                    bytes: b"severity".to_vec(),
                }),
                Term::Binary(Binary {
                    bytes: b"warning".to_vec(),
                }),
            ),
            (
                Term::Binary(Binary {
                    bytes: b"description".to_vec(),
                }),
                Term::Binary(Binary {
                    bytes: b"desc".to_vec(),
                }),
            ),
            (
                Term::Binary(Binary {
                    bytes: b"options".to_vec(),
                }),
                Term::List(eetf::List { elements: vec![] }),
            ),
            (
                Term::Binary(Binary {
                    bytes: b"status".to_vec(),
                }),
                Term::Binary(Binary {
                    bytes: b"resolved".to_vec(),
                }),
            ),
            (
                Term::Binary(Binary {
                    bytes: b"choice".to_vec(),
                }),
                Term::Binary(Binary {
                    bytes: b"retire".to_vec(),
                }),
            ),
            (
                Term::Binary(Binary {
                    bytes: b"created_at".to_vec(),
                }),
                Term::Binary(Binary {
                    bytes: b"2026-04-19T00:00:00Z".to_vec(),
                }),
            ),
            (
                Term::Binary(Binary {
                    bytes: b"expires_at".to_vec(),
                }),
                Term::Atom(Atom::from("nil")),
            ),
            (
                Term::Binary(Binary {
                    bytes: b"resolved_at".to_vec(),
                }),
                Term::Binary(Binary {
                    bytes: b"2026-04-19T01:00:00Z".to_vec(),
                }),
            ),
        ];

        let term = Term::Map(eetf::Map {
            map: entries.into_iter().collect(),
        });

        let entry = EscalationEntry::from_term(term).unwrap();
        assert_eq!(entry.status, "resolved");
        assert_eq!(entry.choice.as_deref(), Some("retire"));
        assert_eq!(entry.resolved_at.as_deref(), Some("2026-04-19T01:00:00Z"));
        assert_eq!(entry.expires_at, None);
        assert_eq!(entry.id_short(), "esc-x");
    }

    #[test]
    fn test_node_info_format_uptime() {
        let make_node = |secs: u64| NodeInfo {
            node: "test@host".to_string(),
            node_type: "core".to_string(),
            role: "leader".to_string(),
            status: "online".to_string(),
            uptime_seconds: secs,
        };

        assert_eq!(make_node(186300).format_uptime(), "2d 3h 45m"); // 2d 3h 45m
        assert_eq!(make_node(3660).format_uptime(), "1h 1m"); // 1h 1m
        assert_eq!(make_node(300).format_uptime(), "5m"); // 5m
        assert_eq!(make_node(0).format_uptime(), "0m"); // 0m
    }
}
