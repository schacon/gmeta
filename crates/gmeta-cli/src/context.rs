use std::cell::OnceCell;

use anyhow::Result;
use time::OffsetDateTime;

use gmeta_core::db::Db;
use gmeta_core::git_utils;
use gmeta_core::types::Target;

/// Shared context for all commands.
///
/// Holds the git repositories (lazily initialized), database, user email,
/// and a timestamp. Commands construct a `CommandContext` at the start and
/// use it throughout their execution, eliminating repeated boilerplate for
/// repo discovery, database opening, and timestamp generation.
///
/// Inspired by GitButler's `but-ctx`, the context holds both `git2` and
/// `gix` repository handles via `OnceCell`. The primary repo (whichever
/// was used for discovery) is eagerly initialized; the other is lazily
/// discovered on first access if a command needs it.
pub struct CommandContext {
    git2_repo: OnceCell<git2::Repository>,
    gix_repo: OnceCell<gix::Repository>,
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
    /// The `gix` repository is eagerly initialized; `git2` is lazily
    /// discovered on first access via [`git2_repo`](Self::git2_repo).
    ///
    /// # Parameters
    /// - `timestamp_override`: If `Some`, uses the given timestamp instead
    ///   of the current wall-clock time. Useful for deterministic tests and
    ///   the `set --timestamp` flag.
    pub fn open_gix(timestamp_override: Option<i64>) -> Result<Self> {
        let repo = git_utils::discover_repo()?;
        let db_path = git_utils::db_path(&repo)?;
        let email = git_utils::get_email(&repo)?;
        let namespace = git_utils::get_namespace(&repo)?;
        let timestamp = timestamp_override
            .unwrap_or_else(|| OffsetDateTime::now_utc().unix_timestamp_nanos() as i64 / 1_000_000);
        let db = Db::open(&db_path)?;

        let gix_cell = OnceCell::new();
        // Safe: cell is freshly created, set always succeeds.
        let _ = gix_cell.set(repo);

        Ok(Self {
            git2_repo: OnceCell::new(),
            gix_repo: gix_cell,
            db,
            email,
            timestamp,
            namespace,
        })
    }

    /// Discover the repository via `git2` and build the command context.
    ///
    /// The `git2` repository is eagerly initialized; `gix` is lazily
    /// discovered on first access via [`gix_repo`](Self::gix_repo).
    ///
    /// # Parameters
    /// - `timestamp_override`: If `Some`, uses the given timestamp instead
    ///   of the current wall-clock time.
    pub fn open_git2(timestamp_override: Option<i64>) -> Result<Self> {
        let repo = git_utils::git2_discover_repo()?;
        let db_path = git_utils::git2_db_path(&repo)?;
        let email = git_utils::git2_get_email(&repo)?;
        let namespace = git_utils::git2_get_namespace(&repo)?;
        let timestamp = timestamp_override
            .unwrap_or_else(|| OffsetDateTime::now_utc().unix_timestamp_nanos() as i64 / 1_000_000);
        let db = Db::open(&db_path)?;

        let git2_cell = OnceCell::new();
        let _ = git2_cell.set(repo);

        Ok(Self {
            git2_repo: git2_cell,
            gix_repo: OnceCell::new(),
            db,
            email,
            timestamp,
            namespace,
        })
    }

    /// Access the `git2` repository handle.
    ///
    /// If the context was opened via `gix`, the `git2` repository is
    /// lazily discovered on the first call.
    pub fn git2_repo(&self) -> Result<&git2::Repository> {
        if let Some(repo) = self.git2_repo.get() {
            return Ok(repo);
        }
        let repo = git_utils::git2_discover_repo()?;
        // Cell is single-threaded and was just checked empty; set succeeds.
        let _ = self.git2_repo.set(repo);
        self.git2_repo
            .get()
            .ok_or_else(|| anyhow::anyhow!("git2 repository cell unexpectedly empty after set"))
    }

    /// Access the `gix` repository handle.
    ///
    /// If the context was opened via `git2`, the `gix` repository is
    /// lazily discovered on the first call.
    pub fn gix_repo(&self) -> Result<&gix::Repository> {
        if let Some(repo) = self.gix_repo.get() {
            return Ok(repo);
        }
        let repo = git_utils::discover_repo()?;
        let _ = self.gix_repo.set(repo);
        self.gix_repo
            .get()
            .ok_or_else(|| anyhow::anyhow!("gix repository cell unexpectedly empty after set"))
    }

    /// The local serialization ref, e.g. `refs/meta/local/main`.
    pub fn local_ref(&self) -> String {
        format!("refs/{}/local/main", self.namespace)
    }

    /// A ref for a named destination, e.g. `refs/meta/local/{destination}`.
    pub fn destination_ref(&self, destination: &str) -> String {
        format!("refs/{}/local/{}", self.namespace, destination)
    }

    /// Resolve a target's partial commit SHA using the `git2` repository.
    ///
    /// # Parameters
    /// - `target`: The target whose commit SHA should be expanded to 40 characters.
    pub fn git2_resolve_target(&self, target: &mut Target) -> Result<()> {
        Ok(target.git2_resolve(self.git2_repo()?)?)
    }

    /// Resolve a target's partial commit SHA using the `gix` repository.
    ///
    /// # Parameters
    /// - `target`: The target whose commit SHA should be expanded to 40 characters.
    pub fn resolve_target(&self, target: &mut Target) -> Result<()> {
        Ok(target.resolve(self.gix_repo()?)?)
    }
}
