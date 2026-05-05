//! Named return types for database query methods.
//!
//! These structs replace anonymous tuples in the public API, making
//! field access explicit and preventing mis-ordering bugs.

use std::fmt;
use std::str::FromStr;

use crate::error::Error;
use crate::types::{TargetType, ValueType};

/// The kind of mutation recorded in the metadata log.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Operation {
    /// A key was set or updated.
    Set,
    /// A key was removed.
    Remove,
    /// A value was pushed onto a list.
    Push,
    /// A value was popped from a list.
    Pop,
    /// A list entry was removed by index.
    ListRemove,
    /// A member was added to a set.
    SetAdd,
    /// A member was removed from a set.
    SetRemove,
}

impl fmt::Display for Operation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl Operation {
    /// Returns the wire-format string for this operation.
    pub fn as_str(&self) -> &str {
        match self {
            Operation::Set => "set",
            Operation::Remove => "rm",
            Operation::Push => "push",
            Operation::Pop => "pop",
            Operation::ListRemove => "list_rm",
            Operation::SetAdd => "set_add",
            Operation::SetRemove => "set_rm",
        }
    }
}

impl FromStr for Operation {
    type Err = Error;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s {
            "set" => Ok(Operation::Set),
            "rm" => Ok(Operation::Remove),
            "push" => Ok(Operation::Push),
            "pop" => Ok(Operation::Pop),
            "list_rm" | "list:rm" => Ok(Operation::ListRemove),
            "set_add" | "set:add" => Ok(Operation::SetAdd),
            "set_rm" | "set:rm" => Ok(Operation::SetRemove),
            _ => Err(Error::Other(format!("unknown operation: {s}"))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Operation;

    #[test]
    fn parses_canonical_operations() {
        assert!(matches!(
            "list_rm".parse::<Operation>(),
            Ok(Operation::ListRemove)
        ));
        assert!(matches!(
            "set_add".parse::<Operation>(),
            Ok(Operation::SetAdd)
        ));
        assert!(matches!(
            "set_rm".parse::<Operation>(),
            Ok(Operation::SetRemove)
        ));
    }

    #[test]
    fn parses_legacy_cli_style_operations() {
        assert!(matches!(
            "list:rm".parse::<Operation>(),
            Ok(Operation::ListRemove)
        ));
        assert!(matches!(
            "set:add".parse::<Operation>(),
            Ok(Operation::SetAdd)
        ));
        assert!(matches!(
            "set:rm".parse::<Operation>(),
            Ok(Operation::SetRemove)
        ));
    }
}

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

/// A tombstone record for a deleted metadata key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TombstoneRecord {
    /// The target type.
    pub target_type: TargetType,
    /// The target value (e.g. a commit SHA).
    pub target_value: String,
    /// The metadata key name.
    pub key: String,
    /// The timestamp when the key was deleted.
    pub timestamp: i64,
    /// The email of the person who deleted this key.
    pub email: String,
}

/// A tombstone record for a deleted set member.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SetTombstoneRecord {
    /// The target type.
    pub target_type: TargetType,
    /// The target value (e.g. a commit SHA).
    pub target_value: String,
    /// The metadata key name.
    pub key: String,
    /// The set member identifier.
    pub member_id: String,
    /// The set member value.
    pub value: String,
    /// The timestamp when the member was deleted.
    pub timestamp: i64,
    /// The email of the person who deleted this member.
    pub email: String,
}

/// A tombstone record for a deleted list entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListTombstoneRecord {
    /// The target type.
    pub target_type: TargetType,
    /// The target value (e.g. a commit SHA).
    pub target_value: String,
    /// The metadata key name.
    pub key: String,
    /// The list entry name (e.g. `"{timestamp_ms}-{hash5}"`).
    pub entry_name: String,
    /// The timestamp when the entry was deleted.
    pub timestamp: i64,
    /// The email of the person who deleted this entry.
    pub email: String,
}

/// An entry from the metadata mutation log (for incremental serialization).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModifiedEntry {
    /// The target type.
    pub target_type: TargetType,
    /// The target value (e.g. a commit SHA).
    pub target_value: String,
    /// The metadata key name.
    pub key: String,
    /// The operation performed.
    pub operation: Operation,
    /// The current value (empty string if deleted).
    pub value: String,
    /// The current value type (`None` if deleted).
    pub value_type: Option<ValueType>,
}

/// A complete metadata record for serialization (includes target type and timestamp).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SerializableEntry {
    /// The target type.
    pub target_type: TargetType,
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
