//! Term conversion utilities for converting Erlang terms to Rust types

pub mod types;

use crate::error::{CliError, ErrorClass, Result};
use eetf::{Atom, Binary, FixInteger, List, Map, Term, Tuple};
use num_traits::ToPrimitive;
use std::collections::HashMap;

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
        Term::BigInteger(big) => big.to_i64().ok_or_else(|| {
            CliError::TermConversionError("BigInteger value overflows i64".to_string())
        }),
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
        Term::BigInteger(big) => big.to_u64().ok_or_else(|| {
            CliError::TermConversionError(
                "BigInteger value overflows u64 or is negative".to_string(),
            )
        }),
        _ => Err(CliError::TermConversionError(format!(
            "Cannot convert {:?} to u64",
            term
        ))),
    }
}

/// Convert Erlang map to HashMap<String, Term>
pub fn term_to_map(term: &Term) -> Result<HashMap<String, Term>> {
    match term {
        Term::Map(Map { map }) => {
            let mut result = HashMap::new();
            for (key, value) in map {
                let key_str = term_to_string(key)?;
                result.insert(key_str, value.clone());
            }
            Ok(result)
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

/// Check if term is an {:error, reason} tuple and extract a `CliError`.
///
/// Handles three formats:
/// 1. **Structured**: `{:error, %NeonFS.Error.SomeError{class: :not_found, message: "..."}}`
///    — returns `CliError::NeonfsError` with class, message, and details
/// 2. **Legacy atom**: `{:error, :some_atom}` — returns `CliError::RpcError`
/// 3. **Legacy string**: `{:error, "some message"}` — returns `CliError::RpcError`
pub fn extract_error(term: &Term) -> Option<CliError> {
    match term {
        Term::Tuple(Tuple { elements }) => {
            if elements.len() == 2 {
                if let Term::Atom(Atom { name }) = &elements[0] {
                    if name == "error" {
                        return extract_error_reason(&elements[1]);
                    }
                }
            }
            None
        }
        _ => None,
    }
}

/// Extract a `CliError` from the reason term of an `{:error, reason}` tuple.
fn extract_error_reason(reason: &Term) -> Option<CliError> {
    // Try structured NeonFS.Error struct first (map with __struct__ key)
    if let Some(err) = try_extract_struct_error(reason) {
        return Some(err);
    }

    // Fall back to legacy: atom or binary string
    if let Ok(msg) = term_to_string(reason) {
        return Some(CliError::RpcError(msg));
    }

    None
}

/// Try to parse an Erlang map as a NeonFS.Error struct.
///
/// Elixir structs are encoded as maps with a `__struct__` atom key.
/// We look for `class` (atom) and `message` (binary) fields.
fn try_extract_struct_error(term: &Term) -> Option<CliError> {
    let map = match term {
        Term::Map(Map { map }) => map,
        _ => return None,
    };

    // Check for __struct__ key to confirm this is an Elixir struct
    let has_struct_key = map
        .iter()
        .any(|(k, _)| matches!(k, Term::Atom(Atom { name }) if name == "__struct__"));

    if !has_struct_key {
        return None;
    }

    // Extract class atom
    let class = map
        .iter()
        .find_map(|(k, v)| match (k, v) {
            (Term::Atom(Atom { name: key }), Term::Atom(Atom { name: val })) if key == "class" => {
                ErrorClass::from_atom(val)
            }
            _ => None,
        })
        .unwrap_or(ErrorClass::Internal);

    // Extract message (binary string)
    let message = map
        .iter()
        .find_map(|(k, v)| match k {
            Term::Atom(Atom { name }) if name == "message" => term_to_string(v).ok(),
            _ => None,
        })
        .unwrap_or_else(|| "Unknown error".to_string());

    // Extract details map (best-effort: string keys and string values)
    let details = map
        .iter()
        .find_map(|(k, v)| match k {
            Term::Atom(Atom { name }) if name == "details" => extract_details_map(v),
            _ => None,
        })
        .unwrap_or_default();

    Some(CliError::NeonfsError {
        class,
        message,
        details,
    })
}

/// Best-effort extraction of the details map into string key-value pairs.
fn extract_details_map(term: &Term) -> Option<HashMap<String, String>> {
    match term {
        Term::Map(Map { map: entries }) => {
            let mut map = HashMap::new();
            for (k, v) in entries {
                if let Ok(key) = term_to_string(k) {
                    if let Ok(val) = term_to_string(v) {
                        map.insert(key, val);
                    }
                }
            }
            Some(map)
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

    // --- Legacy error format tests ---

    #[test]
    fn test_extract_error_legacy_binary() {
        let term = Term::Tuple(Tuple {
            elements: vec![
                Term::Atom(Atom::from("error")),
                Term::Binary(Binary {
                    bytes: b"failed".to_vec(),
                }),
            ],
        });
        let err = extract_error(&term).unwrap();
        assert!(matches!(err, CliError::RpcError(ref msg) if msg == "failed"));
    }

    #[test]
    fn test_extract_error_legacy_atom() {
        let term = Term::Tuple(Tuple {
            elements: vec![
                Term::Atom(Atom::from("error")),
                Term::Atom(Atom::from("not_found")),
            ],
        });
        let err = extract_error(&term).unwrap();
        assert!(matches!(err, CliError::RpcError(ref msg) if msg == "not_found"));
    }

    #[test]
    fn test_extract_error_not_error_tuple() {
        let term = Term::Tuple(Tuple {
            elements: vec![
                Term::Atom(Atom::from("ok")),
                Term::Binary(Binary {
                    bytes: b"data".to_vec(),
                }),
            ],
        });
        assert!(extract_error(&term).is_none());
    }

    #[test]
    fn test_extract_error_non_tuple() {
        let term = Term::Atom(Atom::from("ok"));
        assert!(extract_error(&term).is_none());
    }

    // --- Structured NeonFS.Error tests ---

    /// Build a minimal NeonFS.Error struct as an eetf Map term.
    fn make_error_struct(
        struct_name: &str,
        class: &str,
        message: &str,
        details: Vec<(&str, &str)>,
    ) -> Term {
        let mut entries: Vec<(Term, Term)> = vec![
            (
                Term::Atom(Atom::from("__struct__")),
                Term::Atom(Atom::from(struct_name)),
            ),
            (
                Term::Atom(Atom::from("__exception__")),
                Term::Atom(Atom::from("true")),
            ),
            (
                Term::Atom(Atom::from("class")),
                Term::Atom(Atom::from(class)),
            ),
            (
                Term::Atom(Atom::from("message")),
                Term::Binary(Binary {
                    bytes: message.as_bytes().to_vec(),
                }),
            ),
        ];

        let detail_map: HashMap<Term, Term> = details
            .into_iter()
            .map(|(k, v)| {
                (
                    Term::Atom(Atom::from(k)),
                    Term::Binary(Binary {
                        bytes: v.as_bytes().to_vec(),
                    }),
                )
            })
            .collect();

        entries.push((
            Term::Atom(Atom::from("details")),
            Term::Map(Map { map: detail_map }),
        ));

        Term::Map(Map {
            map: entries.into_iter().collect(),
        })
    }

    /// Wrap a reason term in an {:error, reason} tuple.
    fn wrap_error(reason: Term) -> Term {
        Term::Tuple(Tuple {
            elements: vec![Term::Atom(Atom::from("error")), reason],
        })
    }

    #[test]
    fn test_extract_struct_error_not_found() {
        let reason = make_error_struct(
            "Elixir.NeonFS.Error.VolumeNotFound",
            "not_found",
            "Volume 'mydata' not found",
            vec![("volume_name", "mydata")],
        );
        let term = wrap_error(reason);

        let err = extract_error(&term).unwrap();
        match err {
            CliError::NeonfsError {
                class,
                message,
                details,
            } => {
                assert_eq!(class, ErrorClass::NotFound);
                assert_eq!(message, "Volume 'mydata' not found");
                assert_eq!(details.get("volume_name"), Some(&"mydata".to_string()));
            }
            other => panic!("Expected NeonfsError, got {:?}", other),
        }
    }

    #[test]
    fn test_extract_struct_error_invalid() {
        let reason = make_error_struct(
            "Elixir.NeonFS.Error.Invalid",
            "invalid",
            "Cluster already initialised",
            vec![],
        );
        let term = wrap_error(reason);

        let err = extract_error(&term).unwrap();
        match err {
            CliError::NeonfsError { class, message, .. } => {
                assert_eq!(class, ErrorClass::Invalid);
                assert_eq!(message, "Cluster already initialised");
            }
            other => panic!("Expected NeonfsError, got {:?}", other),
        }
    }

    #[test]
    fn test_extract_struct_error_forbidden() {
        let reason = make_error_struct(
            "Elixir.NeonFS.Error.PermissionDenied",
            "forbidden",
            "Not allowed",
            vec![("operation", "admin")],
        );
        let term = wrap_error(reason);

        let err = extract_error(&term).unwrap();
        match err {
            CliError::NeonfsError { class, message, .. } => {
                assert_eq!(class, ErrorClass::Forbidden);
                assert_eq!(message, "Not allowed");
            }
            other => panic!("Expected NeonfsError, got {:?}", other),
        }
    }

    #[test]
    fn test_extract_struct_error_unavailable() {
        let reason = make_error_struct(
            "Elixir.NeonFS.Error.QuorumUnavailable",
            "unavailable",
            "No quorum available",
            vec![],
        );
        let term = wrap_error(reason);

        let err = extract_error(&term).unwrap();
        match err {
            CliError::NeonfsError { class, message, .. } => {
                assert_eq!(class, ErrorClass::Unavailable);
                assert_eq!(message, "No quorum available");
            }
            other => panic!("Expected NeonfsError, got {:?}", other),
        }
    }

    #[test]
    fn test_extract_struct_error_internal() {
        let reason = make_error_struct(
            "Elixir.NeonFS.Error.Internal",
            "internal",
            "something broke",
            vec![],
        );
        let term = wrap_error(reason);

        let err = extract_error(&term).unwrap();
        match err {
            CliError::NeonfsError { class, message, .. } => {
                assert_eq!(class, ErrorClass::Internal);
                assert_eq!(message, "something broke");
            }
            other => panic!("Expected NeonfsError, got {:?}", other),
        }
    }

    #[test]
    fn test_extract_struct_error_unknown_class_defaults_to_internal() {
        let mut entries: Vec<(Term, Term)> = vec![
            (
                Term::Atom(Atom::from("__struct__")),
                Term::Atom(Atom::from("Elixir.NeonFS.Error.Something")),
            ),
            (
                Term::Atom(Atom::from("class")),
                Term::Atom(Atom::from("weird_class")),
            ),
            (
                Term::Atom(Atom::from("message")),
                Term::Binary(Binary {
                    bytes: b"oops".to_vec(),
                }),
            ),
        ];
        entries.push((
            Term::Atom(Atom::from("details")),
            Term::Map(Map {
                map: HashMap::new(),
            }),
        ));
        let reason = Term::Map(Map {
            map: entries.into_iter().collect(),
        });
        let term = wrap_error(reason);

        let err = extract_error(&term).unwrap();
        match err {
            CliError::NeonfsError { class, message, .. } => {
                assert_eq!(class, ErrorClass::Internal);
                assert_eq!(message, "oops");
            }
            other => panic!("Expected NeonfsError, got {:?}", other),
        }
    }

    #[test]
    fn test_extract_struct_error_missing_message_defaults() {
        let entries: Vec<(Term, Term)> = vec![
            (
                Term::Atom(Atom::from("__struct__")),
                Term::Atom(Atom::from("Elixir.NeonFS.Error.Internal")),
            ),
            (
                Term::Atom(Atom::from("class")),
                Term::Atom(Atom::from("internal")),
            ),
        ];
        let reason = Term::Map(Map {
            map: entries.into_iter().collect(),
        });
        let term = wrap_error(reason);

        let err = extract_error(&term).unwrap();
        match err {
            CliError::NeonfsError { message, .. } => {
                assert_eq!(message, "Unknown error");
            }
            other => panic!("Expected NeonfsError, got {:?}", other),
        }
    }

    #[test]
    fn test_exit_code_from_struct_errors() {
        let cases = vec![
            ("invalid", 1),
            ("not_found", 1),
            ("forbidden", 2),
            ("unavailable", 3),
            ("internal", 4),
        ];
        for (class_name, expected_code) in cases {
            let reason = make_error_struct(
                "Elixir.NeonFS.Error.Test",
                class_name,
                "test message",
                vec![],
            );
            let term = wrap_error(reason);
            let err = extract_error(&term).unwrap();
            assert_eq!(
                err.exit_code(),
                expected_code,
                "class {class_name} should have exit code {expected_code}"
            );
        }
    }

    #[test]
    fn test_exit_code_from_legacy_errors() {
        let term = Term::Tuple(Tuple {
            elements: vec![
                Term::Atom(Atom::from("error")),
                Term::Binary(Binary {
                    bytes: b"legacy error".to_vec(),
                }),
            ],
        });
        let err = extract_error(&term).unwrap();
        assert_eq!(err.exit_code(), 1); // RpcError default
    }

    #[test]
    fn test_struct_error_prefers_over_legacy() {
        // A map with __struct__ should be parsed as a struct, not fall through to legacy
        let reason = make_error_struct(
            "Elixir.NeonFS.Error.NotFound",
            "not_found",
            "Volume not found",
            vec![],
        );
        let term = wrap_error(reason);
        let err = extract_error(&term).unwrap();
        assert!(matches!(err, CliError::NeonfsError { .. }));
    }
}
