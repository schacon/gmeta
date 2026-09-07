//! Resumable state for history indexing.
//!
//! Indexing walks every commit in the metadata history to record which keys
//! exist and where. On a long-lived project that is minutes of work, so it runs
//! in the background and has to survive being interrupted — a closed terminal,
//! a reboot, a `git meta` command that the user cancelled.
//!
//! Progress is checkpointed to a file in the git directory. Any later `git meta`
//! command reads it, sees unfinished work, and picks up where the last one
//! stopped rather than starting the walk again.

use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};

/// Filename, inside the git directory, holding indexing progress.
const STATE_FILE: &str = "git-meta-index.json";

/// How long a checkpoint can go unrefreshed before the indexer that wrote it is
/// presumed dead and another may take over.
///
/// The running indexer refreshes far more often than this, so the window only
/// has to outlast an unlucky pause.
pub const STALE_AFTER_MS: i64 = 30_000;

/// Progress of a history-indexing pass.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IndexState {
    /// The tip this pass is indexing toward. If the ref has moved on, the
    /// checkpoint describes different work and is not resumable.
    pub tip: String,
    /// The commit to resume from: the last one indexed. `None` means the walk
    /// has not started.
    pub resume_from: Option<String>,
    /// Commits walked so far.
    pub commits_indexed: usize,
    /// Promisor entries inserted so far.
    pub keys_indexed: usize,
    /// Whether the walk reached the end of history.
    pub complete: bool,
    /// When the running indexer last refreshed this, in milliseconds since the
    /// Unix epoch.
    pub heartbeat_ms: i64,
    /// Process that wrote the checkpoint, for reporting.
    pub pid: u32,
}

impl IndexState {
    /// A fresh checkpoint for indexing toward `tip`.
    #[must_use]
    pub fn starting(tip: &gix::oid, now_ms: i64) -> Self {
        IndexState {
            tip: tip.to_string(),
            resume_from: None,
            commits_indexed: 0,
            keys_indexed: 0,
            complete: false,
            heartbeat_ms: now_ms,
            pid: std::process::id(),
        }
    }

    /// Whether an indexer appears to be working on this checkpoint right now.
    ///
    /// A process that was killed leaves its checkpoint behind, so liveness is
    /// judged by how recently the checkpoint was refreshed rather than by the
    /// file existing.
    #[must_use]
    pub fn looks_active(&self, now_ms: i64) -> bool {
        !self.complete && now_ms.saturating_sub(self.heartbeat_ms) < STALE_AFTER_MS
    }

    /// Whether this checkpoint has work left that a new indexer should pick up.
    #[must_use]
    pub fn is_resumable(&self, tip: &gix::oid, now_ms: i64) -> bool {
        self.indexes(tip) && !self.complete && !self.looks_active(now_ms)
    }
}

impl IndexState {
    /// Whether this checkpoint describes work toward `tip`.
    ///
    /// If the metadata ref has moved on since the checkpoint was written, it
    /// describes a different walk and cannot be resumed.
    #[must_use]
    pub fn indexes(&self, tip: &gix::oid) -> bool {
        gix::ObjectId::from_hex(self.tip.as_bytes()).is_ok_and(|stored| stored.as_ref() == tip)
    }

    /// The commit to resume the walk from, if the checkpoint names a valid one.
    #[must_use]
    pub fn resume_point(&self) -> Option<gix::ObjectId> {
        let oid = self.resume_from.as_ref()?;
        gix::ObjectId::from_hex(oid.as_bytes()).ok()
    }
}

/// Path of the checkpoint file for a repository.
fn state_path(repo: &gix::Repository) -> PathBuf {
    repo.git_dir().join(STATE_FILE)
}

/// Read the indexing checkpoint, if there is one.
///
/// A malformed or unreadable checkpoint is reported as absent: it only ever
/// costs a repeated walk, and refusing to run because a cache file is corrupt
/// would be worse than redoing the work.
///
/// # Errors
///
/// Never returns an error; the signature matches the rest of the API.
pub fn load(repo: &gix::Repository) -> Result<Option<IndexState>> {
    let Ok(contents) = std::fs::read_to_string(state_path(repo)) else {
        return Ok(None);
    };
    Ok(serde_json::from_str(&contents).ok())
}

/// Write the indexing checkpoint.
///
/// # Errors
///
/// Returns an error if the file cannot be written.
pub fn save(repo: &gix::Repository, state: &IndexState) -> Result<()> {
    let contents = serde_json::to_string_pretty(state)?;
    std::fs::write(state_path(repo), contents)
        .map_err(|e| Error::Other(format!("could not write index state: {e}")))
}

/// Remove the indexing checkpoint.
///
/// # Errors
///
/// Never returns an error; a missing file is already the desired state.
pub fn clear(repo: &gix::Repository) -> Result<()> {
    let _ = std::fs::remove_file(state_path(repo));
    Ok(())
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use super::*;

    fn repo() -> (tempfile::TempDir, gix::Repository) {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let repo = gix::init(dir.path()).expect("init");
        (dir, repo)
    }

    fn oid(byte: u8) -> gix::ObjectId {
        gix::ObjectId::from_bytes_or_panic(&[byte; 20])
    }

    #[test]
    fn a_checkpoint_round_trips() {
        let (_dir, repo) = repo();
        assert!(load(&repo).expect("load").is_none());

        let mut state = IndexState::starting(&oid(1), 1_000);
        state.resume_from = Some(oid(2).to_string());
        state.commits_indexed = 4_200;
        state.keys_indexed = 91_000;
        save(&repo, &state).expect("save");

        let loaded = load(&repo).expect("load").expect("a checkpoint");
        assert_eq!(loaded, state);
        assert_eq!(loaded.resume_point(), Some(oid(2)));

        clear(&repo).expect("clear");
        assert!(load(&repo).expect("load").is_none());
    }

    #[test]
    fn a_corrupt_checkpoint_reads_as_absent() {
        let (_dir, repo) = repo();
        std::fs::write(repo.git_dir().join(STATE_FILE), "not json").expect("write");
        assert!(
            load(&repo).expect("load").is_none(),
            "a damaged cache file must not stop indexing, only cost a rewalk"
        );
    }

    #[test]
    fn a_fresh_heartbeat_means_someone_else_is_working() {
        let state = IndexState::starting(&oid(1), 1_000);
        assert!(state.looks_active(1_000 + STALE_AFTER_MS - 1));
        assert!(!state.is_resumable(&oid(1), 1_000 + STALE_AFTER_MS - 1));
    }

    #[test]
    fn a_stale_heartbeat_means_the_indexer_died() {
        let state = IndexState::starting(&oid(1), 1_000);
        let much_later = 1_000 + STALE_AFTER_MS + 1;
        assert!(!state.looks_active(much_later));
        assert!(
            state.is_resumable(&oid(1), much_later),
            "an interrupted pass must be resumable by the next command"
        );
    }

    #[test]
    fn a_checkpoint_for_a_different_tip_is_not_resumed() {
        let state = IndexState::starting(&oid(1), 1_000);
        let much_later = 1_000 + STALE_AFTER_MS + 1;
        assert!(!state.indexes(&oid(9)));
        assert!(
            !state.is_resumable(&oid(9), much_later),
            "the ref moved on, so the checkpoint describes different work"
        );
    }

    #[test]
    fn a_finished_pass_is_neither_active_nor_resumable() {
        let mut state = IndexState::starting(&oid(1), 1_000);
        state.complete = true;
        assert!(!state.looks_active(1_000));
        assert!(!state.is_resumable(&oid(1), 1_000 + STALE_AFTER_MS + 1));
    }
}
