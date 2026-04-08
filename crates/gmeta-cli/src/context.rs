use anyhow::Result;
use time::OffsetDateTime;

use gmeta_core::db::Db;
use gmeta_core::git_utils;
use gmeta_core::types::Target;

/// Shared context for all commands.
///
/// Holds the gix repository handle, database, user email,
/// and a timestamp. Commands construct a `CommandContext` at the start and
/// use it throughout their execution, eliminating repeated boilerplate for
/// repo discovery, database opening, and timestamp generation.
pub struct CommandContext {
    repo: gix::Repository,
    /// The gmeta SQLite database.
    pub db: Db,
    /// The user's email from git config, used for authorship tracking.
    pub email: String,
    /// Millisecond-precision timestamp for this command invocation.
    pub timestamp: i64,
    /// The metadata namespace from git config (e.g. `"meta"`).
    /// Used to construct ref paths like `refs/{ns}/local/main`.
    pub namespace: String,
}

impl CommandContext {
    /// Discover the repository via `gix` and build the command context.
    ///
    /// # Parameters
    /// - `timestamp_override`: If `Some`, uses the given timestamp instead
    ///   of the current wall-clock time. Useful for deterministic tests and
    ///   the `set --timestamp` flag.
    pub fn open(timestamp_override: Option<i64>) -> Result<Self> {
        let repo = git_utils::discover_repo()?;
        let db_path = git_utils::db_path(&repo)?;
        let email = git_utils::get_email(&repo)?;
        let namespace = git_utils::get_namespace(&repo)?;
        let timestamp = timestamp_override
            .unwrap_or_else(|| OffsetDateTime::now_utc().unix_timestamp_nanos() as i64 / 1_000_000);
        let db = Db::open(&db_path)?;

        Ok(Self {
            repo,
            db,
            email,
            timestamp,
            namespace,
        })
    }

    /// Access the `gix` repository handle.
    pub fn repo(&self) -> &gix::Repository {
        &self.repo
    }

    /// The local serialization ref, e.g. `refs/meta/local/main`.
    pub fn local_ref(&self) -> String {
        format!("refs/{}/local/main", self.namespace)
    }

    /// A ref for a named destination, e.g. `refs/meta/local/{destination}`.
    pub fn destination_ref(&self, destination: &str) -> String {
        format!("refs/{}/local/{}", self.namespace, destination)
    }

    /// Resolve a target's partial commit SHA using the `gix` repository.
    ///
    /// # Parameters
    /// - `target`: The target whose commit SHA should be expanded to 40 characters.
    pub fn resolve_target(&self, target: &mut Target) -> Result<()> {
        Ok(target.resolve(&self.repo)?)
    }
}
