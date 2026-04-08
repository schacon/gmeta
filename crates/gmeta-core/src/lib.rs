/// Typed error types for all gmeta-core operations.
pub mod error;

/// The library entry point: a session combining a git repo with a metadata store.
pub mod session;
/// Session-scoped target handle with automatic email and timestamp.
pub mod session_handle;
/// Core metadata types: targets, value types, and path-building helpers.
pub mod types;

// Modules that are `pub(crate)` by default, `pub` with the `internal` feature.
// The CLI enables this feature; library consumers do not.

#[cfg(not(feature = "internal"))]
pub(crate) mod db;
#[cfg(feature = "internal")]
pub mod db;

#[cfg(not(feature = "internal"))]
pub(crate) mod git_utils;
#[cfg(feature = "internal")]
pub mod git_utils;

#[cfg(not(feature = "internal"))]
pub(crate) mod list_value;
#[cfg(feature = "internal")]
pub mod list_value;

#[cfg(not(feature = "internal"))]
pub(crate) mod materialize;
#[cfg(feature = "internal")]
pub mod materialize;

#[cfg(not(feature = "internal"))]
pub(crate) mod prune;
#[cfg(feature = "internal")]
pub mod prune;

#[cfg(not(feature = "internal"))]
pub(crate) mod pull;
#[cfg(feature = "internal")]
pub mod pull;

#[cfg(not(feature = "internal"))]
pub(crate) mod push;
#[cfg(feature = "internal")]
pub mod push;

#[cfg(not(feature = "internal"))]
pub(crate) mod serialize;
#[cfg(feature = "internal")]
pub mod serialize;

#[cfg(not(feature = "internal"))]
pub(crate) mod sync;
#[cfg(feature = "internal")]
pub mod sync;

#[cfg(not(feature = "internal"))]
pub(crate) mod tree;
#[cfg(feature = "internal")]
pub mod tree;

#[cfg(not(feature = "internal"))]
pub(crate) mod tree_paths;
#[cfg(feature = "internal")]
pub mod tree_paths;

// Public API re-exports: these are visible regardless of feature flags.
// The `pub use` makes specific types public even when the source module
// is `pub(crate)`.

pub use error::{Error, Result};
pub use list_value::ListEntry;
pub use session::Session;
pub use session_handle::SessionTargetHandle;
pub use types::{MetaValue, Target, TargetType, ValueType};

// Workflow output types
pub use materialize::{MaterializeOutput, MaterializeRefResult, MaterializeStrategy};
pub use pull::PullOutput;
pub use push::PushOutput;
pub use serialize::SerializeOutput;
