#![allow(
    clippy::type_complexity,
    clippy::too_many_arguments,
    clippy::should_implement_trait
)]

/// Typed error types for all gmeta-core operations.
pub mod error;

/// Local SQLite database for caching and querying metadata.
pub mod db;
/// Git utility helpers for resolving commits and working with objects.
pub mod git_utils;
/// Timestamped list entry encoding and decoding.
pub mod list_value;
/// The library entry point: a session combining a git repo with a metadata store.
pub mod session;
/// Git tree serialization, parsing, merging, and filtering.
pub mod tree;
/// Core metadata types: targets, value types, and path-building helpers.
pub mod types;

// Re-export the most commonly used types at the crate root for convenience.
pub use db::{Batch, Store, TargetHandle};
pub use error::{Error, Result};
pub use session::Session;
pub use types::{MetaValue, Target, TargetType, ValueType};
