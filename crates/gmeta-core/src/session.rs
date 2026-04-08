/// A session combining a Git repository with its gmeta metadata store.
///
/// This is the primary entry point for gmeta-core consumers. It owns the
/// `gix::Repository`, the SQLite [`Store`](crate::db::Store), and resolved
/// configuration values (namespace, user email).
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
    repo: gix::Repository,
    store: crate::db::Store,
    namespace: String,
    email: String,
    name: String,
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

    fn from_repo(repo: gix::Repository) -> crate::error::Result<Self> {
        let db_path = crate::git_utils::db_path(&repo)?;
        let email = crate::git_utils::get_email(&repo)?;
        let name = crate::git_utils::get_name(&repo)?;
        let namespace = crate::git_utils::get_namespace(&repo)?;
        let store = crate::db::Store::open(&db_path)?;

        Ok(Self {
            repo,
            store,
            namespace,
            email,
            name,
        })
    }

    /// Access the metadata store.
    pub fn store(&self) -> &crate::db::Store {
        &self.store
    }

    /// Access the metadata store mutably.
    pub fn store_mut(&mut self) -> &mut crate::db::Store {
        &mut self.store
    }

    /// Access the underlying `gix` repository.
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
    pub fn local_ref(&self) -> String {
        format!("refs/{}/local/main", self.namespace)
    }

    /// A ref path for a named destination (e.g. `refs/meta/local/{destination}`).
    pub fn destination_ref(&self, destination: &str) -> String {
        format!("refs/{}/local/{}", self.namespace, destination)
    }

    /// Resolve a target's partial commit SHA using this session's repository.
    pub fn resolve_target(&self, target: &mut crate::types::Target) -> crate::error::Result<()> {
        target.resolve(&self.repo)
    }

    /// Index metadata keys from commit history for blobless clone support.
    ///
    /// Walks commits from `tip_oid` backward (optionally stopping at `old_tip`)
    /// and inserts promisor entries for all keys found in commit messages or
    /// root-commit trees. Returns the number of new entries indexed.
    ///
    /// Call this after a blobless fetch to build an index of historical keys
    /// that can be hydrated on demand.
    pub fn index_history(
        &self,
        tip_oid: gix::ObjectId,
        old_tip: Option<gix::ObjectId>,
    ) -> crate::error::Result<usize> {
        crate::sync::insert_promisor_entries(&self.repo, &self.store, tip_oid, old_tip)
    }

    /// Extract all metadata keys from a git tree without reading blob content.
    ///
    /// Useful for discovering what keys exist in a tree fetched via blobless
    /// clone. Only parses path names — works even when blobs are missing.
    pub fn keys_in_tree(
        &self,
        tree_id: gix::ObjectId,
    ) -> crate::error::Result<Vec<(String, String, String)>> {
        crate::sync::extract_keys_from_tree(&self.repo, tree_id)
    }

    /// Serialize local metadata to Git tree(s) and commit(s).
    ///
    /// Determines incremental vs full mode automatically. Applies filter
    /// routing and pruning rules. Updates local refs and the materialization
    /// timestamp.
    ///
    /// See [`crate::serialize::run()`] for full details.
    pub fn serialize(&self) -> crate::error::Result<crate::serialize::SerializeOutput> {
        crate::serialize::run(self)
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
    /// - `now`: the current timestamp in milliseconds since the Unix epoch,
    ///   used for database writes and the `last_materialized` marker.
    ///
    /// See [`crate::materialize::run()`] for full details.
    pub fn materialize(
        &self,
        remote: Option<&str>,
        now: i64,
    ) -> crate::error::Result<crate::materialize::MaterializeOutput> {
        crate::materialize::run(self, remote, now)
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
    /// - `now`: the current timestamp in milliseconds since the Unix epoch,
    ///   used for database writes during materialization.
    ///
    /// See [`crate::pull::run()`] for full details.
    pub fn pull(
        &self,
        remote: Option<&str>,
        now: i64,
    ) -> crate::error::Result<crate::pull::PullOutput> {
        crate::pull::run(self, remote, now)
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
    ///
    /// See [`crate::push::push_once()`] for full details.
    pub fn push_once(&self, remote: Option<&str>) -> crate::error::Result<crate::push::PushOutput> {
        crate::push::push_once(self, remote)
    }

    /// After a failed push, fetch remote changes, materialize, re-serialize,
    /// and rebase local ref for clean fast-forward.
    ///
    /// Call this between push retries. See [`crate::push::resolve_push_conflict()`]
    /// for full details.
    ///
    /// # Parameters
    ///
    /// - `remote`: optional remote name. If `None`, the first configured
    ///   metadata remote is used.
    /// - `now`: the current timestamp in milliseconds since the Unix epoch,
    ///   used for database writes during materialization.
    pub fn resolve_push_conflict(
        &self,
        remote: Option<&str>,
        now: i64,
    ) -> crate::error::Result<()> {
        crate::push::resolve_push_conflict(self, remote, now)
    }
}
