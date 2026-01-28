//! Term conversion utilities for converting Erlang terms to Rust types

pub mod types;

use crate::error::{CliError, Result};
use eetf::{Atom, Binary, FixInteger, List, Map, Term, Tuple};

/// Convert Erlang term to string
pub fn term_to_string(term: &Term) -> Result<String> {
    match term {
        Term::Binary(Binary { bytes }) => String::from_utf8(bytes.clone())
            .map_err(|e| CliError::TermConversionError(e.to_string())),
        Term::Atom(atom) => Ok(atom.name.clone()),
        _ => Err(CliError::TermConversionError(format!(
            "Cannot convert {:?} to string",
            term
        ))),
    }
}

/// Convert Erlang term to integer
#[allow(dead_code)]
pub fn term_to_i64(term: &Term) -> Result<i64> {
    match term {
        Term::FixInteger(FixInteger { value }) => Ok(*value as i64),
        Term::BigInteger(_) => Err(CliError::TermConversionError(
            "BigInteger conversion not supported (value too large)".to_string(),
        )),
        _ => Err(CliError::TermConversionError(format!(
            "Cannot convert {:?} to integer",
            term
        ))),
    }
}

/// Convert Erlang term to u64
pub fn term_to_u64(term: &Term) -> Result<u64> {
    match term {
        Term::FixInteger(FixInteger { value }) => {
            if *value >= 0 {
                Ok(*value as u64)
            } else {
                Err(CliError::TermConversionError(
                    "Negative integer cannot be converted to u64".to_string(),
                ))
            }
        }
        Term::BigInteger(_) => Err(CliError::TermConversionError(
            "BigInteger conversion not supported (value too large)".to_string(),
        )),
        _ => Err(CliError::TermConversionError(format!(
            "Cannot convert {:?} to u64",
            term
        ))),
    }
}

/// Convert Erlang map to HashMap<String, Term>
pub fn term_to_map(term: &Term) -> Result<std::collections::HashMap<String, Term>> {
    match term {
        Term::Map(Map { entries }) => {
            let mut map = std::collections::HashMap::new();
            for (key, value) in entries {
                let key_str = term_to_string(key)?;
                map.insert(key_str, value.clone());
            }
            Ok(map)
        }
        _ => Err(CliError::TermConversionError(format!(
            "Cannot convert {:?} to map",
            term
        ))),
    }
}

/// Convert Erlang list to Vec<Term>
pub fn term_to_list(term: &Term) -> Result<Vec<Term>> {
    match term {
        Term::List(List { elements }) => {
            // Handle both proper lists and empty lists
            if elements.is_empty() {
                Ok(vec![])
            } else {
                Ok(elements.clone())
            }
        }
        _ => Err(CliError::TermConversionError(format!(
            "Cannot convert {:?} to list",
            term
        ))),
    }
}

/// Check if term is an :ok tuple and extract the value
pub fn unwrap_ok_tuple(term: Term) -> Result<Term> {
    match term {
        Term::Tuple(Tuple { elements }) => {
            if elements.len() == 2 {
                if let Term::Atom(Atom { name }) = &elements[0] {
                    if name == "ok" {
                        return Ok(elements[1].clone());
                    }
                }
            }
            Err(CliError::RpcError(format!(
                "Expected {{:ok, value}}, got {:?}",
                elements
            )))
        }
        _ => Err(CliError::RpcError(format!(
            "Expected tuple, got {:?}",
            term
        ))),
    }
}

/// Check if term is an :error tuple and extract the error message
pub fn extract_error(term: &Term) -> Option<String> {
    match term {
        Term::Tuple(Tuple { elements }) => {
            if elements.len() == 2 {
                if let Term::Atom(Atom { name }) = &elements[0] {
                    if name == "error" {
                        if let Ok(msg) = term_to_string(&elements[1]) {
                            return Some(msg);
                        }
                    }
                }
            }
            None
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_term_to_string_from_binary() {
        let term = Term::Binary(Binary {
            bytes: b"hello".to_vec(),
        });
        assert_eq!(term_to_string(&term).unwrap(), "hello");
    }

    #[test]
    fn test_term_to_string_from_atom() {
        let term = Term::Atom(Atom::from("test"));
        assert_eq!(term_to_string(&term).unwrap(), "test");
    }

    #[test]
    fn test_term_to_i64() {
        let term = Term::FixInteger(FixInteger::from(42));
        assert_eq!(term_to_i64(&term).unwrap(), 42);
    }

    #[test]
    fn test_term_to_u64() {
        let term = Term::FixInteger(FixInteger::from(42));
        assert_eq!(term_to_u64(&term).unwrap(), 42);
    }

    #[test]
    fn test_term_to_list() {
        let elements = vec![
            Term::FixInteger(FixInteger::from(1)),
            Term::FixInteger(FixInteger::from(2)),
        ];
        let term = Term::List(List {
            elements: elements.clone(),
        });
        let result = term_to_list(&term).unwrap();
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn test_unwrap_ok_tuple() {
        let term = Term::Tuple(Tuple {
            elements: vec![
                Term::Atom(Atom::from("ok")),
                Term::Binary(Binary {
                    bytes: b"success".to_vec(),
                }),
            ],
        });
        let result = unwrap_ok_tuple(term).unwrap();
        assert_eq!(term_to_string(&result).unwrap(), "success");
    }

    #[test]
    fn test_extract_error() {
        let term = Term::Tuple(Tuple {
            elements: vec![
                Term::Atom(Atom::from("error")),
                Term::Binary(Binary {
                    bytes: b"failed".to_vec(),
                }),
            ],
        });
        assert_eq!(extract_error(&term).unwrap(), "failed");
    }
}
