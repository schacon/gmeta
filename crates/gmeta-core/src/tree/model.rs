//! Shared types for the gmeta tree format.
//!
//! These types represent the in-memory model of a serialized gmeta tree:
//! values, tombstones, and the parsed tree structure itself.

use std::collections::BTreeMap;

/// A key uniquely identifying a metadata entry: `(target_type, target_value, key)`.
pub type Key = (String, String, String);

/// A parsed metadata entry from a Git tree.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TreeValue {
    /// A single string value.
    String(String),
    /// An ordered list of `(entry_name, content)` pairs.
    List(Vec<(String, String)>),
    /// A set of `member_id -> content` pairs.
    Set(BTreeMap<String, String>),
}

/// A tombstone entry recording who deleted a key and when.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TombstoneEntry {
    /// Millisecond timestamp of the deletion.
    pub timestamp: i64,
    /// Email of the person who performed the deletion.
    pub email: String,
}

/// JSON-serializable tombstone blob stored in Git trees.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct TombstoneBlob {
    /// Millisecond timestamp of the deletion.
    pub timestamp: i64,
    /// Email of the person who performed the deletion.
    pub email: String,
}

/// The fully parsed contents of a serialized gmeta Git tree.
#[derive(Debug, Default, Clone)]
pub struct ParsedTree {
    /// Metadata values keyed by `(target_type, target_value, key)`.
    pub values: BTreeMap<Key, TreeValue>,
    /// Whole-key tombstones.
    pub tombstones: BTreeMap<Key, TombstoneEntry>,
    /// Set-member tombstones: `(key, member_id) -> original member content`.
    pub set_tombstones: BTreeMap<(Key, String), String>,
    /// List-entry tombstones: `(key, entry_name) -> TombstoneEntry`.
    pub list_tombstones: BTreeMap<(Key, String), TombstoneEntry>,
}
