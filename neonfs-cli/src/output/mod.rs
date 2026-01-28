//! Output formatting modules

pub mod json;
pub mod table;

use crate::error::Result;
use serde::Serialize;

/// Output format mode
#[derive(Debug, Clone, Copy)]
pub enum OutputFormat {
    /// Human-readable table format
    Table,
    /// Machine-readable JSON format
    Json,
}

/// Trait for types that can be displayed in multiple formats
#[allow(dead_code)]
pub trait FormattedOutput: Serialize {
    /// Render output in the specified format
    fn display(&self, format: OutputFormat) -> Result<String>;
}
