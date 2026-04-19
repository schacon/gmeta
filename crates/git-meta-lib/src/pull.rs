//! Pull remote metadata: fetch, materialize, and index history.
//!
//! This module implements the full pull workflow: resolving the remote,
//! fetching the metadata ref, counting new commits, hydrating tip blobs,
//! serializing local state for merge, materializing remote changes, and
//! indexing historical keys for lazy loading.
//!
//! The public entry point is [`run()`], which takes a [`Session`](crate::Session)
//! and returns a [`PullOutput`] describing what happened.

use crate::error::Result;
use crate::git_utils;
use crate::session::Session;

/// Result of a pull operation.
///
/// Contains all the information needed by a CLI or other consumer
/// to report what happened, without performing any I/O itself.
#[must_use]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PullOutput {
    /// The remote that was pulled from.
    pub remote_name: String,
    /// Number of new commits fetched.
    pub new_commits: usize,
    /// Number of historical keys indexed for lazy loading.
    pub indexed_keys: usize,
    /// Whether materialization was performed.
    pub materialized: bool,
}

/// Pull remote metadata: fetch, materialize, and index history.
///
/// Resolves the remote, fetches the metadata ref, hydrates tip blobs,
/// serializes local state for merge, materializes remote changes, and
/// indexes historical keys for lazy loading.
///
/// # Parameters
///
/// - `session`: the gmeta session providing the repository, store, and config.
/// - `remote`: optional remote name to pull from. If `None`, the first
///   configured metadata remote is used.
/// - `now`: the current timestamp in milliseconds since the Unix epoch,
///   used for database writes during materialization.
///
/// # Returns
///
/// A [`PullOutput`] describing the remote pulled from, new commits fetched,
/// whether materialization occurred, and how many history keys were indexed.
///
/// # Errors
///
/// Returns an error if the remote cannot be resolved, fetch fails,
/// materialization fails, or history indexing fails.
pub fn run(session: &Session, remote: Option<&str>, now: i64) -> Result<PullOutput> {
    let repo = &session.repo;
    let ns = session.namespace();

    let remote_name = git_utils::resolve_meta_remote(repo, remote)?;
    let remote_refspec = format!("refs/{ns}/main");
    let tracking_ref = format!("refs/{ns}/remotes/main");
    let fetch_refspec = format!("{remote_refspec}:{tracking_ref}");

    // Record the old tip so we can count new commits
    let old_tip = repo
        .find_reference(&tracking_ref)
        .ok()
        .and_then(|r| r.into_fully_peeled_id().ok());

    // Fetch latest remote metadata
    git_utils::run_git(repo, &["fetch", &remote_name, &fetch_refspec])?;

    // Get the new tip
    let new_tip = repo
        .find_reference(&tracking_ref)
        .ok()
        .and_then(|r| r.into_fully_peeled_id().ok());

    // Check if we need to materialize even if no new commits were fetched
    // (e.g. remote add fetched but never materialized)
    let needs_materialize = session.store.get_last_materialized()?.is_none()
        || repo.find_reference(&session.local_ref()).is_err();

    // Count new commits
    let new_commits = match (old_tip.as_ref(), new_tip.as_ref()) {
        (Some(old), Some(new)) if old == new => {
            if !needs_materialize {
                return Ok(PullOutput {
                    remote_name,
                    new_commits: 0,
                    indexed_keys: 0,
                    materialized: false,
                });
            }
            0
        }
        (Some(old), Some(new)) => count_commits_between(repo, old.detach(), new.detach()),
        (None, Some(_)) => 1, // initial fetch, at least one commit
        _ => 0,
    };

    // Hydrate tip tree blobs so gix can read them
    let short_ref = format!("{ns}/remotes/main");
    git_utils::hydrate_tip_blobs(repo, &remote_name, &short_ref)?;

    // Serialize local state so materialize can do a proper 3-way merge
    let _ = crate::serialize::run(session, now)?;

    // Materialize: merge remote tree into local DB
    let _ = crate::materialize::run(session, None, now)?;

    // Insert promisor entries from non-tip commits so we know what keys exist
    // in the history even though we haven't fetched their blob data yet.
    // On first materialize, walk the entire history (pass None as old_tip).
    let indexed_keys = if let Some(new) = new_tip {
        let walk_from = if needs_materialize {
            None
        } else {
            old_tip.map(gix::Id::detach)
        };
        session.index_history(new.detach(), walk_from)?
    } else {
        0
    };

    Ok(PullOutput {
        remote_name,
        new_commits,
        indexed_keys,
        materialized: true,
    })
}

/// Count commits reachable from `new` but not from `old`.
fn count_commits_between(repo: &gix::Repository, old: gix::ObjectId, new: gix::ObjectId) -> usize {
    let walk = repo.rev_walk(Some(new)).with_boundary(Some(old));
    match walk.all() {
        // Subtract the boundary commit itself
        Ok(iter) => iter
            .filter(std::result::Result::is_ok)
            .count()
            .saturating_sub(1),
        Err(_) => 0,
    }
}
