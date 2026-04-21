use anyhow::Result;

use git_meta_lib::Session;

/// CLI command context: a gmeta [`Session`] with optional timestamp override.
///
/// Access session methods directly via `ctx.session`.
pub struct CommandContext {
    /// The gmeta session (repo + store + config + timestamp).
    pub session: Session,
}

impl CommandContext {
    /// Discover the repository and build the command context.
    ///
    /// # Parameters
    /// - `timestamp_override`: If `Some`, pins the session to a fixed
    ///   timestamp (milliseconds since Unix epoch) instead of the wall clock.
    pub fn open(timestamp_override: Option<i64>) -> Result<Self> {
        let mut session = Session::discover()?;
        if let Some(ts) = timestamp_override {
            session = session.with_timestamp(ts);
        }

        Ok(Self { session })
    }
}
