use anyhow::Result;
use time::OffsetDateTime;

use gmeta_core::Session;

/// CLI command context wrapping a gmeta [`Session`] with a timestamp.
///
/// The timestamp is a CLI concern (overridable via `--timestamp` flag)
/// that doesn't belong in the library session.
pub struct CommandContext {
    session: Session,
    /// Millisecond-precision timestamp for this command invocation.
    pub timestamp: i64,
}

impl CommandContext {
    /// Discover the repository and build the command context.
    ///
    /// # Parameters
    /// - `timestamp_override`: If `Some`, uses the given timestamp instead
    ///   of the current wall-clock time. Useful for deterministic tests and
    ///   the `set --timestamp` flag.
    pub fn open(timestamp_override: Option<i64>) -> Result<Self> {
        let session = Session::discover()?;
        let timestamp = timestamp_override
            .unwrap_or_else(|| OffsetDateTime::now_utc().unix_timestamp_nanos() as i64 / 1_000_000);

        Ok(Self { session, timestamp })
    }

    /// Access the underlying gmeta session.
    #[allow(dead_code)]
    pub fn session(&self) -> &Session {
        &self.session
    }

    /// Access the underlying gmeta session mutably.
    #[allow(dead_code)]
    pub fn session_mut(&mut self) -> &mut Session {
        &mut self.session
    }

    /// Access the metadata store (convenience shorthand for `session().store()`).
    pub fn store(&self) -> &gmeta_core::Store {
        self.session.store()
    }

    /// Access the metadata store mutably.
    pub fn store_mut(&mut self) -> &mut gmeta_core::Store {
        self.session.store_mut()
    }

    /// Access the `gix` repository handle.
    pub fn repo(&self) -> &gix::Repository {
        self.session.repo()
    }

    /// The user's email from git config.
    pub fn email(&self) -> &str {
        self.session.email()
    }

    /// The metadata namespace from git config.
    pub fn namespace(&self) -> &str {
        self.session.namespace()
    }

    /// The local serialization ref, e.g. `refs/meta/local/main`.
    pub fn local_ref(&self) -> String {
        self.session.local_ref()
    }

    /// A ref for a named destination, e.g. `refs/meta/local/{destination}`.
    pub fn destination_ref(&self, destination: &str) -> String {
        self.session.destination_ref(destination)
    }

    /// Resolve a target's partial commit SHA.
    pub fn resolve_target(&self, target: &mut gmeta_core::Target) -> Result<()> {
        Ok(self.session.resolve_target(target)?)
    }
}
