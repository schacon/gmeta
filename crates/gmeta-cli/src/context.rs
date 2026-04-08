use anyhow::Result;
use time::OffsetDateTime;

use gmeta_core::Session;

/// CLI command context: a gmeta [`Session`] plus a timestamp.
///
/// The timestamp is a CLI concern (overridable via `--timestamp` flag).
/// Access session methods directly via `ctx.session`.
pub struct CommandContext {
    /// The gmeta session (repo + store + config).
    pub session: Session,
    /// Millisecond-precision timestamp for this command invocation.
    pub timestamp: i64,
}

impl CommandContext {
    /// Discover the repository and build the command context.
    ///
    /// # Parameters
    /// - `timestamp_override`: If `Some`, uses the given timestamp instead
    ///   of the current wall-clock time.
    pub fn open(timestamp_override: Option<i64>) -> Result<Self> {
        let session = Session::discover()?;
        let timestamp = timestamp_override
            .unwrap_or_else(|| OffsetDateTime::now_utc().unix_timestamp_nanos() as i64 / 1_000_000);

        Ok(Self { session, timestamp })
    }
}
