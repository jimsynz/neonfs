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
}

impl VolumeInfo {
    /// Parse from Erlang term (map)
    pub fn from_term(term: Term) -> Result<Self> {
        let map = term_to_map(&term)?;

        // Extract durability config
        let durability = term_to_map(map.get("durability").ok_or_else(|| {
            CliError::TermConversionError("Missing 'durability' field".to_string())
        })?)?;

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
}
