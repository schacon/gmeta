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
        let namespace = crate::git_utils::get_namespace(&repo)?;
        let store = crate::db::Store::open(&db_path)?;

        Ok(Self {
            repo,
            store,
            namespace,
            email,
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
}
