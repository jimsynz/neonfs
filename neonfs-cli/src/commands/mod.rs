//! CLI command implementations

pub mod acl;
pub mod audit;
pub mod cluster;
pub mod drive;
pub mod fuse;
pub mod gc;
pub mod job;
pub mod nfs;
pub mod node;
pub mod s3;
pub mod scrub;
pub mod volume;
pub mod worker;

use crate::error::Result;
use crate::output::OutputFormat;

/// Trait for CLI commands
#[allow(dead_code)]
pub trait Command {
    /// Execute the command
    fn execute(&self, format: OutputFormat) -> Result<()>;
}
