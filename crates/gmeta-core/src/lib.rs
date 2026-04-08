#![allow(clippy::type_complexity, clippy::too_many_arguments)]

/// Typed error types for all gmeta-core operations.
pub mod error;

/// Local SQLite database for caching and querying metadata.
pub mod db;

// --- Primary API modules (documented, stable) ---

/// Materialize remote metadata into the local SQLite store.
pub mod materialize;
/// Pull remote metadata: fetch, materialize, and index history.
pub mod pull;
/// Push local metadata to a remote: serialize, push, and conflict resolution.
pub mod push;
/// Serialize local metadata to Git tree(s) and commit(s).
pub mod serialize;
/// The library entry point: a session combining a git repo with a metadata store.
pub mod session;
/// Session-scoped target handle with automatic email and timestamp.
pub mod session_handle;
/// Git tree serialization, parsing, merging, and filtering.
pub mod tree;
/// Core metadata types: targets, value types, and path-building helpers.
pub mod types;

// --- Internal modules (available for advanced use, not part of the primary API) ---

/// Git utility helpers for resolving commits and working with objects.
///
/// Most consumers should use [`Session`] methods instead of calling these directly.
#[doc(hidden)]
pub mod git_utils;
/// Timestamped list entry encoding and decoding.
///
/// Internal encoding format. Consumers should use [`MetaValue`] and
/// [`SessionTargetHandle`] for typed access.
#[doc(hidden)]
pub mod list_value;
/// Auto-prune rule evaluation and tree-size computation.
///
/// Used internally by [`serialize`]. Exposed for CLI prune commands.
#[doc(hidden)]
pub mod prune;
/// High-level sync operations: promisor entries, commit change parsing.
///
/// Used internally by [`Session::index_history`] and [`Session::keys_in_tree`].
#[doc(hidden)]
pub mod sync;

// Re-export the most commonly used types at the crate root for convenience.
pub use db::Store;
pub use error::{Error, Result};
pub use materialize::{MaterializeOutput, MaterializeRefResult, MaterializeStrategy};
pub use pull::PullOutput;
pub use push::PushOutput;
pub use serialize::SerializeOutput;
pub use session::Session;
pub use session_handle::SessionTargetHandle;
pub use types::{MetaValue, Target, TargetType, ValueType};
