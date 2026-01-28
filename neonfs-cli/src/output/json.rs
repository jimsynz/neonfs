//! JSON output formatting

use crate::error::Result;
use serde::Serialize;

/// Format data as JSON
pub fn format<T: Serialize>(data: &T) -> Result<String> {
    Ok(serde_json::to_string_pretty(data)?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Serialize;

    #[derive(Serialize)]
    struct TestData {
        name: String,
        value: i32,
    }

    #[test]
    fn test_format() {
        let data = TestData {
            name: "test".to_string(),
            value: 42,
        };
        let result = format(&data).unwrap();
        assert!(result.contains("\"name\""));
        assert!(result.contains("\"test\""));
        assert!(result.contains("\"value\""));
        assert!(result.contains("42"));
    }
}
