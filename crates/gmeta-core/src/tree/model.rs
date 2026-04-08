//! Shared types for the gmeta tree format.
//!
//! These types represent the in-memory model of a serialized gmeta tree:
//! values, tombstones, and the parsed tree structure itself.

use std::collections::BTreeMap;

/// A key uniquely identifying a metadata entry.
///
/// Composed of the target type (e.g. `"commit"`), the target value
/// (e.g. a SHA), and the metadata key name (e.g. `"agent:model"`).
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Key {
    /// The target type, e.g. `"commit"`, `"branch"`, `"path"`.
    pub target_type: String,
    /// The target value, e.g. a commit SHA or branch name.
    pub target_value: String,
    /// The metadata key name, e.g. `"agent:model"`.
    pub key: String,
}

/// A parsed metadata entry from a Git tree.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TreeValue {
    /// A single string value.
    String(String),
    /// An ordered list of `(entry_name, content)` pairs.
    List(Vec<(String, String)>),
    /// A set of `member_id -> content` pairs.
    Set(BTreeMap<String, String>),
}

/// A tombstone recording who deleted a key and when.
///
/// Used both as the in-memory representation and for JSON serialization
/// into Git tree blobs.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Tombstone {
    /// Millisecond timestamp of the deletion.
    pub timestamp: i64,
    /// Email of the person who performed the deletion.
    pub email: String,
}

/// The fully parsed contents of a serialized gmeta Git tree.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct ParsedTree {
    /// Metadata values keyed by `(target_type, target_value, key)`.
    pub values: BTreeMap<Key, TreeValue>,
    /// Whole-key tombstones.
    pub tombstones: BTreeMap<Key, Tombstone>,
    /// Set-member tombstones: `(key, member_id) -> original member content`.
    pub set_tombstones: BTreeMap<(Key, String), String>,
    /// List-entry tombstones: `(key, entry_name) -> Tombstone`.
    pub list_tombstones: BTreeMap<(Key, String), Tombstone>,
}
