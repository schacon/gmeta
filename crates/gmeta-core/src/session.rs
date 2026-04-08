use time::OffsetDateTime;

/// A session combining a Git repository with its gmeta metadata store.
///
/// This is the primary entry point for gmeta-core consumers. It owns the
/// `gix::Repository`, the SQLite [`Store`](crate::db::Store), and resolved
/// configuration values (namespace, user email).
///
/// # Timestamps
///
/// By default, workflow operations use the wall clock for timestamps.
/// For deterministic tests, call [`with_timestamp()`](Self::with_timestamp)
/// to pin all operations to a fixed time:
///
/// ```ignore
/// let session = Session::discover()?.with_timestamp(1_700_000_000_000);
/// session.serialize()?; // uses the fixed timestamp
/// ```
///
/// # Example
///
/// ```no_run
/// use gmeta_core::Session;
///
/// let session = Session::discover()?;
/// println!("email: {}", session.email());
/// println!("namespace: {}", session.namespace());
/// # Ok::<(), gmeta_core::Error>(())
/// ```
pub struct Session {
    pub(crate) repo: gix::Repository,
    pub(crate) store: crate::db::Store,
    pub(crate) namespace: String,
    pub(crate) email: String,
    pub(crate) name: String,
    pub(crate) timestamp_override: Option<i64>,
}

impl Session {
    /// Discover a git repository from the current directory and open its
    /// metadata store.
    ///
    /// Walks upward from the current directory to find a `.git` directory,
    /// reads `user.email` and `meta.namespace` from git config, and opens
    /// (or creates) the SQLite database at `.git/gmeta.sqlite`.
    pub fn discover() -> crate::error::Result<Self> {
        let repo = crate::git_utils::discover_repo()?;
        Self::from_repo(repo)
    }

    /// Open a session for a known repository.
    ///
    /// Use this when you already have a `gix::Repository` handle (e.g. from
    /// `gix::open()` or `gix::init()`).
    pub fn open(repo: gix::Repository) -> crate::error::Result<Self> {
        Self::from_repo(repo)
    }

    /// Pin all workflow operations to a fixed timestamp.
    ///
    /// The value is milliseconds since the Unix epoch. When set,
    /// [`now()`](Self::now) returns this value instead of the wall clock.
    /// Useful for deterministic tests and replay scenarios.
    #[must_use]
    pub fn with_timestamp(mut self, timestamp_ms: i64) -> Self {
        self.timestamp_override = Some(timestamp_ms);
        self
    }

    /// The current timestamp in milliseconds since the Unix epoch.
    ///
    /// Returns the fixed timestamp if [`with_timestamp()`](Self::with_timestamp)
    /// was called, otherwise the wall clock.
    pub(crate) fn now(&self) -> i64 {
        self.timestamp_override
            .unwrap_or_else(|| OffsetDateTime::now_utc().unix_timestamp_nanos() as i64 / 1_000_000)
    }

    fn from_repo(repo: gix::Repository) -> crate::error::Result<Self> {
        let db_path = crate::git_utils::db_path(&repo)?;
        let email = crate::git_utils::get_email(&repo)?;
        let name = crate::git_utils::get_name(&repo)?;
        let namespace = crate::git_utils::get_namespace(&repo)?;
        let store_repo =
            gix::open(repo.git_dir()).map_err(|e| crate::error::Error::Other(format!("{e}")))?;
        let store = crate::db::Store::open_with_repo(&db_path, store_repo)?;

        Ok(Self {
            repo,
            store,
            namespace,
            email,
            name,
            timestamp_override: None,
        })
    }

    /// Access the metadata store directly.
    ///
    /// This is an advanced API for custom queries. Most consumers should use
    /// [`target()`](Self::target) for read/write operations.
    #[cfg(feature = "internal")]
    pub fn store(&self) -> &crate::db::Store {
        &self.store
    }

    /// Access the underlying gix repository.
    ///
    /// This is an advanced API. Most consumers should use Session's workflow
    /// methods (serialize, materialize, pull, push) instead.
    #[cfg(feature = "internal")]
    pub fn repo(&self) -> &gix::Repository {
        &self.repo
    }

    /// The metadata namespace (from git config `meta.namespace`, default `"meta"`).
    ///
    /// Used to construct ref paths like `refs/{namespace}/local/main`.
    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    /// The user email from git config `user.email`.
    ///
    /// Used for authorship tracking on metadata mutations.
    pub fn email(&self) -> &str {
        &self.email
    }

    /// The user name from git config `user.name`.
    ///
    /// Used for commit signatures during serialization.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// The local serialization ref path (e.g. `refs/meta/local/main`).
    pub(crate) fn local_ref(&self) -> String {
        format!("refs/{}/local/main", self.namespace)
    }

    /// A ref path for a named destination (e.g. `refs/meta/local/{destination}`).
    pub(crate) fn destination_ref(&self, destination: &str) -> String {
        format!("refs/{}/local/{}", self.namespace, destination)
    }

    /// Create a scoped handle for operations on a specific target.
    ///
    /// The handle carries the session's email and timestamp, so write
    /// operations don't need them as parameters:
    ///
    /// ```ignore
    /// let handle = session.target(&Target::parse("commit:abc123")?);
    /// handle.set_value("key", &MetaValue::String("value".into()))?;
    /// ```
    pub fn target(
        &self,
        target: &crate::types::Target,
    ) -> crate::session_handle::SessionTargetHandle<'_> {
        crate::session_handle::SessionTargetHandle::new(self, target.clone())
    }

    /// Resolve a target's partial commit SHA using this session's repository.
    pub fn resolve_target(&self, target: &mut crate::types::Target) -> crate::error::Result<()> {
        target.resolve(&self.repo)
    }

    /// Resolve which metadata remote to use.
    ///
    /// If `remote` is `Some`, validates that it is a configured meta remote.
    /// If `None`, returns the first configured meta remote.
    ///
    /// # Parameters
    ///
    /// - `remote`: optional remote name to validate; if `None`, the first
    ///   configured metadata remote is returned
    ///
    /// # Returns
    ///
    /// The name of the resolved meta remote.
    ///
    /// # Errors
    ///
    /// Returns [`Error::NoRemotes`](crate::error::Error::NoRemotes) if no
    /// meta remotes are configured, or
    /// [`Error::RemoteNotFound`](crate::error::Error::RemoteNotFound) if the
    /// specified name is not a meta remote.
    pub fn resolve_remote(&self, remote: Option<&str>) -> crate::error::Result<String> {
        crate::git_utils::resolve_meta_remote(&self.repo, remote)
    }

    /// Index metadata keys from commit history for blobless clone support.
    ///
    /// Walks commits from `tip_oid` backward (optionally stopping at `old_tip`)
    /// and inserts promisor entries for all keys found in commit messages or
    /// root-commit trees. Returns the number of new entries indexed.
    ///
    /// Call this after a blobless fetch to build an index of historical keys
    /// that can be hydrated on demand.
    pub(crate) fn index_history(
        &self,
        tip_oid: gix::ObjectId,
        old_tip: Option<gix::ObjectId>,
    ) -> crate::error::Result<usize> {
        crate::sync::insert_promisor_entries(&self.repo, &self.store, tip_oid, old_tip)
    }

    /// Serialize local metadata to Git tree(s) and commit(s).
    ///
    /// Determines incremental vs full mode automatically. Applies filter
    /// routing and pruning rules. Updates local refs and the materialization
    /// timestamp.
    pub fn serialize(&self) -> crate::error::Result<crate::serialize::SerializeOutput> {
        crate::serialize::run(self, self.now())
    }

    /// Materialize remote metadata into the local store.
    ///
    /// For each matching remote ref, determines the merge strategy and
    /// applies changes. Updates tracking refs and materialization timestamp.
    ///
    /// # Parameters
    ///
    /// - `remote`: optional remote name filter. If `None`, all remotes are
    ///   materialized.
    pub fn materialize(
        &self,
        remote: Option<&str>,
    ) -> crate::error::Result<crate::materialize::MaterializeOutput> {
        crate::materialize::run(self, remote, self.now())
    }

    /// Pull metadata from remote: fetch, materialize, and index history.
    ///
    /// Resolves the remote, fetches the metadata ref, hydrates tip blobs,
    /// serializes local state for merge, materializes remote changes, and
    /// indexes historical keys for lazy loading.
    ///
    /// # Parameters
    ///
    /// - `remote`: optional remote name to pull from. If `None`, the first
    ///   configured metadata remote is used.
    pub fn pull(&self, remote: Option<&str>) -> crate::error::Result<crate::pull::PullOutput> {
        crate::pull::run(self, remote, self.now())
    }

    /// Serialize and attempt a single push to the remote.
    ///
    /// Returns the result of the push attempt. On non-fast-forward failure,
    /// the caller is responsible for calling [`resolve_push_conflict()`](Self::resolve_push_conflict)
    /// and retrying.
    ///
    /// # Parameters
    ///
    /// - `remote`: optional remote name to push to. If `None`, the first
    ///   configured metadata remote is used.
    pub fn push_once(&self, remote: Option<&str>) -> crate::error::Result<crate::push::PushOutput> {
        crate::push::push_once(self, remote, self.now())
    }

    /// After a failed push, fetch remote changes, materialize, re-serialize,
    /// and rebase local ref for clean fast-forward.
    ///
    /// Call this between push retries.
    ///
    /// # Parameters
    ///
    /// - `remote`: optional remote name. If `None`, the first configured
    ///   metadata remote is used.
    pub fn resolve_push_conflict(&self, remote: Option<&str>) -> crate::error::Result<()> {
        crate::push::resolve_push_conflict(self, remote, self.now())
    }
}
