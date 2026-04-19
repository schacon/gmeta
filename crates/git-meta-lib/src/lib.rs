//! # gmeta
//!
//! A library for storing and exchanging structured metadata in Git repositories.
//! This is the reference implementation of the [gmeta spec](https://git-meta.com/).
//!
//! ## Core Concepts
//!
//! gmeta attaches key-value metadata to **targets** in a Git project:
//!
//! - **Commits** — attach agent info, review status, provenance to specific commits
//! - **Paths** — attach code ownership, agent rules to directories or files
//! - **Branches** — attach review comments, CI status to branches
//! - **Change IDs** — attach metadata to logical changesets (for tools like Jujutsu)
//! - **Project** — global project-wide metadata (configuration, ownership)
//!
//! Values can be **strings** (single values), **lists** (ordered, append-friendly),
//! or **sets** (unordered, unique members).
//!
//! ## Quick Start
//!
//! ```no_run
//! use git_meta_lib::{Session, Target, MetaValue};
//!
//! // Open a session for the current git repository
//! let session = Session::discover()?;
//!
//! // Write metadata
//! let commit = session.target(&Target::commit("abc123")?);
//! commit.set("agent:model", "claude-4.6")?;
//! commit.set("review:status", "approved")?;
//!
//! // Read metadata
//! if let Some(model) = commit.get_value("agent:model")? {
//!     println!("Model: {model}");
//! }
//!
//! // Sync with remote
//! session.serialize()?;
//! session.push_once(None)?;
//! # Ok::<(), git_meta_lib::Error>(())
//! ```
//!
//! If you already have a [`gix::Repository`] (e.g. in a host application like
//! GitButler), clone it cheaply and pass it in:
//!
//! ```no_run
//! # use git_meta_lib::Session;
//! let repo = gix::open(".")?;
//! let session = Session::open(repo.clone())?;
//! // `repo` is still fully usable here
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```
//!
//! ## Data Exchange
//!
//! Metadata is stored locally in SQLite for fast reads/writes, and exchanged
//! via Git's object format and transfer protocols:
//!
//! - [`Session::serialize()`] writes local metadata to a Git tree and commit
//! - [`Session::materialize()`] reads remote metadata and merges it locally
//! - [`Session::pull()`] fetches + materializes in one step
//! - [`Session::push_once()`] serializes + pushes to the remote
//!
//! The exchange format uses Git trees with a deterministic path layout, enabling
//! standard Git merge strategies. See the [spec](https://git-meta.com/)
//! for the full format description.
//!
//! ## Blobless Clone Support
//!
//! For large metadata histories (e.g., AI transcripts), gmeta supports Git's
//! partial/blobless clone feature. Only tree objects are fetched initially;
//! blob data is fetched on demand when accessed. The [`Session::pull()`] method
//! automatically indexes historical keys for lazy loading.

/// Typed error types for all gmeta operations.
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
