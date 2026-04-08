/// Typed error enum for all gmeta-core operations.
///
/// Covers database, git, parsing, and domain-specific errors.
/// External consumers can match on variants to handle specific failure modes.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// SQLite operation failed.
    #[error(transparent)]
    Sqlite(#[from] rusqlite::Error),

    /// Git operation failed.
    #[error("git error: {0}")]
    Git(#[from] Box<dyn std::error::Error + Send + Sync>),

    /// JSON serialization or deserialization failed.
    #[error(transparent)]
    Json(#[from] serde_json::Error),

    /// Unknown target type string (e.g. not "commit", "branch", etc.).
    #[error("unknown target type: {0}")]
    UnknownTargetType(String),

    /// Target string could not be parsed (wrong format, too short, etc.).
    #[error("{0}")]
    InvalidTarget(String),

    /// Key validation failed (empty, reserved segment, invalid characters).
    #[error("{0}")]
    InvalidKey(String),

    /// Unknown value type string (not "string", "list", or "set").
    #[error("unknown value type: {0}")]
    UnknownValueType(String),

    /// Metadata key does not exist for the given target.
    #[error("key '{key}' not found")]
    KeyNotFound {
        /// The key that was not found.
        key: String,
    },

    /// Key exists but has a different value type than expected.
    #[error("key '{key}' is not a {expected}")]
    TypeMismatch {
        /// The key whose type didn't match.
        key: String,
        /// The type that was expected (e.g. "list", "set").
        expected: String,
    },

    /// A specific value was not found in a list or set.
    #[error("value not found: {0}")]
    ValueNotFound(String),

    /// Index is out of range for a list operation.
    #[error("index {index} out of range ({size} entries)")]
    IndexOutOfRange {
        /// The requested index.
        index: usize,
        /// The actual size of the collection.
        size: usize,
    },

    /// A value was malformed or invalid (JSON encoding, list entry format, etc.).
    #[error("{0}")]
    InvalidValue(String),

    /// No git repository found in the current directory or any parent.
    #[error("not a git repository (or any parent up to mount point)")]
    NotARepository,

    /// Could not resolve a partial commit SHA or ref.
    #[error("could not resolve: {0}")]
    ResolveError(String),

    /// A git subprocess command failed.
    #[error("git command failed: {0}")]
    GitCommand(String),

    /// No metadata remotes are configured.
    #[error("no metadata remotes configured")]
    NoRemotes,

    /// The specified remote is not a metadata remote.
    #[error("'{0}' is not a metadata remote")]
    RemoteNotFound(String),

    /// A filter rule string could not be parsed.
    #[error("invalid filter rule: {0}")]
    InvalidFilterRule(String),

    /// A tree path could not be parsed or is structurally invalid.
    #[error("invalid tree path: {0}")]
    InvalidTreePath(String),

    /// Catch-all for errors that don't fit other variants.
    #[error("{0}")]
    Other(String),
}

/// A `Result` type alias using [`Error`] as the default error type.
pub type Result<T, E = Error> = std::result::Result<T, E>;
