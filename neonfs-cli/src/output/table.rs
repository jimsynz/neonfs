//! Table output formatting

use crate::error::Result;

/// Simple table formatter
pub struct Table {
    headers: Vec<String>,
    rows: Vec<Vec<String>>,
}

impl Table {
    /// Create a new table with headers
    pub fn new(headers: Vec<String>) -> Self {
        Self {
            headers,
            rows: Vec::new(),
        }
    }

    /// Add a row to the table
    pub fn add_row(&mut self, row: Vec<String>) {
        self.rows.push(row);
    }

    /// Render the table as a string
    pub fn render(&self) -> Result<String> {
        let mut output = String::new();

        // Calculate column widths
        let mut widths = self.headers.iter().map(|h| h.len()).collect::<Vec<usize>>();

        for row in &self.rows {
            for (i, cell) in row.iter().enumerate() {
                if let Some(width) = widths.get_mut(i) {
                    *width = (*width).max(cell.len());
                }
            }
        }

        // Render header
        for (i, header) in self.headers.iter().enumerate() {
            if i > 0 {
                output.push_str("  ");
            }
            output.push_str(&format!("{:<width$}", header, width = widths[i]));
        }
        output.push('\n');

        // Render separator
        for (i, width) in widths.iter().enumerate() {
            if i > 0 {
                output.push_str("  ");
            }
            output.push_str(&"-".repeat(*width));
        }
        output.push('\n');

        // Render rows
        for row in &self.rows {
            for (i, cell) in row.iter().enumerate() {
                if i > 0 {
                    output.push_str("  ");
                }
                let width = widths.get(i).copied().unwrap_or(0);
                output.push_str(&format!("{:<width$}", cell));
            }
            output.push('\n');
        }

        Ok(output)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_table_render() {
        let mut table = Table::new(vec!["Name".to_string(), "Value".to_string()]);
        table.add_row(vec!["foo".to_string(), "123".to_string()]);
        table.add_row(vec!["bar".to_string(), "456".to_string()]);

        let output = table.render().unwrap();
        assert!(output.contains("Name"));
        assert!(output.contains("Value"));
        assert!(output.contains("foo"));
        assert!(output.contains("123"));
    }

    #[test]
    fn test_empty_table() {
        let table = Table::new(vec!["Col1".to_string(), "Col2".to_string()]);
        let output = table.render().unwrap();
        assert!(output.contains("Col1"));
        assert!(output.contains("Col2"));
    }
}
