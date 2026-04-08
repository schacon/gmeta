//! Named return types for database query methods.
//!
//! These structs replace anonymous tuples in the public API, making
//! field access explicit and preventing mis-ordering bugs.

use crate::types::ValueType;

/// Result of looking up a single metadata value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetadataValue {
    /// The stored value (JSON-encoded for strings, encoded entries for lists/sets).
    pub value: String,
    /// The storage type of this value.
    pub value_type: ValueType,
    /// Whether the value is stored as a git blob reference.
    pub is_git_ref: bool,
}

/// A metadata entry with its key (returned by `get_all`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetadataEntry {
    /// The metadata key name.
    pub key: String,
    /// The stored value.
    pub value: String,
    /// The storage type of this value.
    pub value_type: ValueType,
    /// Whether the value is stored as a git blob reference.
    pub is_git_ref: bool,
}

/// A metadata entry with full target information (returned by `get_all_with_target_prefix`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetadataRecord {
    /// The target value (e.g. a commit SHA).
    pub target_value: String,
    /// The metadata key name.
    pub key: String,
    /// The stored value.
    pub value: String,
    /// The storage type of this value.
    pub value_type: ValueType,
    /// Whether the value is stored as a git blob reference.
    pub is_git_ref: bool,
    /// Whether this is a promised (not-yet-hydrated) entry.
    pub is_promised: bool,
}

/// Authorship information from the metadata log.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Authorship {
    /// The email of the last person who modified this key.
    pub email: String,
    /// The timestamp of the last modification.
    pub timestamp: i64,
}

/// A complete metadata record for serialization (includes target type and timestamp).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SerializableEntry {
    /// The target type string (e.g. `"commit"`).
    pub target_type: String,
    /// The target value (e.g. a commit SHA).
    pub target_value: String,
    /// The metadata key name.
    pub key: String,
    /// The stored value.
    pub value: String,
    /// The storage type of this value.
    pub value_type: ValueType,
    /// The last modification timestamp.
    pub last_timestamp: i64,
    /// Whether the value is stored as a git blob reference.
    pub is_git_ref: bool,
}
