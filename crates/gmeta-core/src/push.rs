//! Push local metadata to a remote: serialize, push, and conflict resolution.
//!
//! This module implements the single-attempt push workflow and the
//! conflict resolution step. The retry loop is intentionally left to
//! the caller (CLI or other consumer) since retry policy is a UX concern.
//!
//! The public entry points are [`push_once()`] for a single push attempt
//! and [`resolve_push_conflict()`] for fetching, materializing, and
//! rebasing after a non-fast-forward rejection.

use gix::prelude::ObjectIdExt;
use gix::refs::transaction::PreviousValue;

use crate::error::{Error, Result};
use crate::git_utils;
use crate::session::Session;

/// Result of a single push attempt.
///
/// Contains all the information needed by a CLI or other consumer
/// to report what happened, without performing any I/O itself.
#[derive(Debug)]
pub struct PushOutput {
    /// Whether the push succeeded (or was already up-to-date).
    pub success: bool,
    /// Whether the push was rejected as non-fast-forward.
    pub non_fast_forward: bool,
    /// Whether local and remote were already in sync (nothing to push).
    pub up_to_date: bool,
    /// The resolved remote name that was pushed to.
    pub remote_name: String,
    /// The remote refspec that was pushed to (e.g. `refs/meta/main`).
    pub remote_ref: String,
    /// The commit OID that was pushed (or attempted).
    pub commit_oid: String,
}

/// Execute a single push attempt: serialize, then git push.
///
/// Does NOT retry on failure. Returns whether it succeeded or was
/// rejected. The caller (CLI) implements retry policy.
///
/// # Parameters
///
/// - `session`: the gmeta session providing the repository, store, and config.
/// - `remote`: optional remote name to push to. If `None`, the first
///   configured metadata remote is used.
///
/// # Returns
///
/// A [`PushOutput`] indicating success or failure, whether the failure
/// was a non-fast-forward rejection, and the commit OID that was pushed
/// or attempted.
///
/// # Errors
///
/// Returns an error if serialization fails, the local ref cannot be read,
/// or the push fails for a reason other than non-fast-forward rejection
/// (in which case `success` is `false` and `non_fast_forward` is `false`).
pub fn push_once(session: &Session, remote: Option<&str>) -> Result<PushOutput> {
    let repo = session.repo();
    let ns = session.namespace();

    let remote_name = git_utils::resolve_meta_remote(repo, remote)?;
    let local_ref = session.local_ref();
    let remote_refspec = format!("refs/{}/main", ns);

    // Serialize local metadata to the local ref
    crate::serialize::run(session)?;

    // Verify we have something to push
    if repo.find_reference(&local_ref).is_err() {
        return Err(Error::Other(
            "nothing to push (no local metadata ref)".into(),
        ));
    }

    // Check if local ref already matches the remote ref (nothing new to push)
    let remote_tracking_ref = format!("refs/{}/remotes/main", ns);
    let local_oid = repo
        .find_reference(&local_ref)
        .ok()
        .and_then(|r| r.into_fully_peeled_id().ok())
        .map(|id| id.detach());
    let remote_oid = repo
        .find_reference(&remote_tracking_ref)
        .ok()
        .and_then(|r| r.into_fully_peeled_id().ok())
        .map(|id| id.detach());

    if let (Some(local), Some(remote_id)) = (local_oid.as_ref(), remote_oid.as_ref()) {
        if local == remote_id {
            return Ok(PushOutput {
                success: true,
                non_fast_forward: false,
                up_to_date: true,
                remote_name,
                remote_ref: remote_refspec,
                commit_oid: local.to_string(),
            });
        }
    }

    let commit_oid_str = local_oid
        .as_ref()
        .map(ToString::to_string)
        .unwrap_or_default();

    // Attempt push
    let push_refspec = format!("{}:{}", local_ref, remote_refspec);
    let result = git_utils::run_git(repo, &["push", &remote_name, &push_refspec]);

    match result {
        Ok(_) => Ok(PushOutput {
            success: true,
            non_fast_forward: false,
            up_to_date: false,
            remote_name,
            remote_ref: remote_refspec,
            commit_oid: commit_oid_str,
        }),
        Err(e) => {
            let err_msg = e.to_string();
            let is_non_ff = err_msg.contains("non-fast-forward")
                || err_msg.contains("rejected")
                || err_msg.contains("fetch first");

            if is_non_ff {
                Ok(PushOutput {
                    success: false,
                    non_fast_forward: true,
                    up_to_date: false,
                    remote_name,
                    remote_ref: remote_refspec,
                    commit_oid: commit_oid_str,
                })
            } else {
                Err(Error::GitCommand(format!("push failed: {err_msg}")))
            }
        }
    }
}

/// After a failed push, fetch remote changes, materialize, re-serialize,
/// and rebase local ref for clean fast-forward.
///
/// Call this between push retries. It fetches the latest remote data,
/// hydrates tip blobs, materializes changes into the local store,
/// re-serializes the merged data, and rebases the local ref on top of
/// the remote tip so the next push is a clean fast-forward.
///
/// # Parameters
///
/// - `session`: the gmeta session providing the repository, store, and config.
/// - `remote`: optional remote name. If `None`, the first configured
///   metadata remote is used.
/// - `now`: the current timestamp in milliseconds since the Unix epoch,
///   used for database writes during materialization.
///
/// # Errors
///
/// Returns an error if fetch, materialization, serialization, or rebase fails.
pub fn resolve_push_conflict(session: &Session, remote: Option<&str>, now: i64) -> Result<()> {
    let repo = session.repo();
    let ns = session.namespace();

    let remote_name = git_utils::resolve_meta_remote(repo, remote)?;
    let local_ref = session.local_ref();
    let remote_refspec = format!("refs/{}/main", ns);
    let remote_tracking_ref = format!("refs/{}/remotes/main", ns);

    // Fetch latest remote data
    let fetch_refspec = format!("{}:{}", remote_refspec, remote_tracking_ref);
    git_utils::run_git(repo, &["fetch", &remote_name, &fetch_refspec])?;

    // Hydrate tip tree blobs so gix can read them
    let short_ref = format!("{}/remotes/main", ns);
    git_utils::hydrate_tip_blobs(repo, &remote_name, &short_ref)?;

    // Materialize the remote data (merge into local DB)
    crate::materialize::run(session, None, now)?;

    // Re-serialize with merged data
    crate::serialize::run(session)?;

    // Rewrite local ref as a single commit on top of the remote tip.
    // This avoids merge commits in the pushed history — the spec
    // requires that push always produces a single fast-forward commit.
    rebase_local_on_remote(repo, &local_ref, &remote_tracking_ref)?;

    Ok(())
}

/// Rewrite the local ref as a single non-merge commit whose parent is the
/// remote tip and whose tree is the current local ref's tree.
///
/// This ensures the pushed history is always a clean fast-forward with
/// no merge commits.
fn rebase_local_on_remote(repo: &gix::Repository, local_ref: &str, remote_ref: &str) -> Result<()> {
    let local_ref_obj = repo
        .find_reference(local_ref)
        .map_err(|e| Error::Other(format!("{e}")))?;
    let local_oid = local_ref_obj
        .into_fully_peeled_id()
        .map_err(|e| Error::Other(format!("{e}")))?
        .detach();
    let local_commit_obj = local_oid
        .attach(repo)
        .object()
        .map_err(|e| Error::Other(format!("{e}")))?
        .into_commit();
    let local_decoded = local_commit_obj
        .decode()
        .map_err(|e| Error::Other(format!("{e}")))?;

    let remote_ref_obj = repo
        .find_reference(remote_ref)
        .map_err(|e| Error::Other(format!("{e}")))?;
    let remote_oid = remote_ref_obj
        .into_fully_peeled_id()
        .map_err(|e| Error::Other(format!("{e}")))?
        .detach();

    // If the local commit is already a single-parent child of remote, nothing to do
    let parent_ids: Vec<gix::ObjectId> = local_decoded.parents().collect();
    if parent_ids.len() == 1 && parent_ids[0] == remote_oid {
        return Ok(());
    }

    let tree_id = local_decoded.tree();
    let message = local_decoded.message.to_owned();
    let author_ref = local_decoded
        .author()
        .map_err(|e| Error::Other(format!("{e}")))?;

    let commit = gix::objs::Commit {
        message,
        tree: tree_id,
        author: gix::actor::Signature {
            name: author_ref.name.into(),
            email: author_ref.email.into(),
            time: author_ref
                .time()
                .map_err(|e| Error::Other(format!("{e}")))?,
        },
        committer: gix::actor::Signature {
            name: author_ref.name.into(),
            email: author_ref.email.into(),
            time: author_ref
                .time()
                .map_err(|e| Error::Other(format!("{e}")))?,
        },
        encoding: None,
        parents: vec![remote_oid].into(),
        extra_headers: Default::default(),
    };

    let new_oid = repo
        .write_object(&commit)
        .map_err(|e| Error::Other(format!("{e}")))?
        .detach();
    repo.reference(
        local_ref,
        new_oid,
        PreviousValue::Any,
        "gmeta: rebase for push",
    )
    .map_err(|e| Error::Other(format!("{e}")))?;

    Ok(())
}
