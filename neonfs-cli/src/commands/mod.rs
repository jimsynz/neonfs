//! CLI command implementations

pub mod cluster;
pub mod mount;
pub mod node;
pub mod volume;

use crate::error::Result;
use crate::output::OutputFormat;

/// Trait for CLI commands
#[allow(dead_code)]
pub trait Command {
    /// Execute the command
    fn execute(&self, format: OutputFormat) -> Result<()>;
}
