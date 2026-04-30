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
#[must_use]
#[derive(Debug, Clone, PartialEq, Eq)]
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

/// Progress event emitted during push preparation and conflict resolution.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PushProgress {
    /// Local and remote refs are being inspected.
    CheckingLocalState,
    /// Local SQLite metadata is being serialized to the local metadata ref.
    Serializing,
    /// The local metadata ref is already current enough to push.
    SerializationSkipped,
    /// The local metadata commit is being rewritten onto the tracked remote tip.
    RebasingLocal,
    /// The local metadata ref is being pushed to the remote.
    Pushing {
        /// The resolved remote name.
        remote_name: String,
        /// The local ref being pushed.
        local_ref: String,
        /// The remote ref receiving the push.
        remote_ref: String,
    },
    /// Latest metadata is being fetched after a non-fast-forward rejection.
    FetchingRemote {
        /// The resolved remote name.
        remote_name: String,
        /// The remote metadata ref being fetched.
        remote_ref: String,
    },
    /// Remote tip blobs are being hydrated so the metadata tree can be read.
    HydratingRemoteTip,
    /// Remote metadata is being materialized into the local store.
    MaterializingRemote,
    /// Merged local metadata is being serialized after conflict resolution.
    SerializingMerged,
    /// The merged local metadata ref is being rebased onto the remote tip.
    RebasingMerged,
}

/// Execute a single push attempt: serialize, rewrite onto the tracked remote
/// tip when needed, then git push.
///
/// Does NOT retry on failure. Returns whether it succeeded or was
/// rejected. The caller (CLI) implements retry policy.
///
/// # Parameters
///
/// - `session`: the gmeta session providing the repository, store, and config.
/// - `remote`: optional remote name to push to. If `None`, the first
///   configured metadata remote is used.
/// - `now`: the current timestamp in milliseconds since the Unix epoch,
///   used for the commit signature during serialization.
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
pub fn push_once(session: &Session, remote: Option<&str>, now: i64) -> Result<PushOutput> {
    push_once_with_progress(session, remote, now, |_| {})
}

/// Execute a single push attempt and report phase progress through a callback.
///
/// # Parameters
///
/// - `session`: the gmeta session providing the repository, store, and config.
/// - `remote`: optional remote name to push to. If `None`, the first
///   configured metadata remote is used.
/// - `now`: the current timestamp in milliseconds since the Unix epoch.
/// - `progress`: callback invoked before long-running phases.
///
/// # Errors
///
/// Returns an error if serialization fails, the local ref cannot be read,
/// or the push fails for a reason other than non-fast-forward rejection.
pub fn push_once_with_progress(
    session: &Session,
    remote: Option<&str>,
    now: i64,
    mut progress: impl FnMut(PushProgress),
) -> Result<PushOutput> {
    let repo = &session.repo;
    let ns = session.namespace();

    let remote_name = git_utils::resolve_meta_remote(repo, remote)?;
    let local_ref = session.local_ref();
    let remote_refspec = format!("refs/{ns}/main");
    let remote_tracking_ref = format!("refs/{ns}/remotes/main");

    progress(PushProgress::CheckingLocalState);
    let mut local_oid = peeled_ref_oid(repo, &local_ref);
    let remote_oid = peeled_ref_oid(repo, &remote_tracking_ref);

    if should_serialize_before_push(session, local_oid.as_ref(), remote_oid.as_ref())? {
        progress(PushProgress::Serializing);
        let _ = crate::serialize::run(session, now, false)?;
        local_oid = peeled_ref_oid(repo, &local_ref);
    } else {
        progress(PushProgress::SerializationSkipped);
    }

    // Verify we have something to push
    if local_oid.is_none() {
        return Err(Error::Other(
            "nothing to push (no local metadata ref)".into(),
        ));
    }

    // Check if local ref already matches the remote ref (nothing new to push)
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

        progress(PushProgress::RebasingLocal);
        rebase_local_on_remote(repo, &local_ref, &remote_tracking_ref)?;
        local_oid = repo
            .find_reference(&local_ref)
            .ok()
            .and_then(|r| r.into_fully_peeled_id().ok())
            .map(gix::Id::detach);
    }

    let commit_oid_str = local_oid
        .as_ref()
        .map(ToString::to_string)
        .unwrap_or_default();

    // Attempt push
    let push_refspec = format!("{local_ref}:{remote_refspec}");
    progress(PushProgress::Pushing {
        remote_name: remote_name.clone(),
        local_ref: local_ref.clone(),
        remote_ref: remote_refspec.clone(),
    });
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

fn peeled_ref_oid(repo: &gix::Repository, ref_name: &str) -> Option<gix::ObjectId> {
    repo.find_reference(ref_name)
        .ok()
        .and_then(|r| r.into_fully_peeled_id().ok())
        .map(gix::Id::detach)
}

fn should_serialize_before_push(
    session: &Session,
    local_oid: Option<&gix::ObjectId>,
    remote_oid: Option<&gix::ObjectId>,
) -> Result<bool> {
    let Some(local) = local_oid else {
        return Ok(true);
    };

    if remote_oid.is_some_and(|remote| remote == local) {
        return Ok(true);
    }

    let Some(last_materialized) = session.store.get_last_materialized()? else {
        return Ok(true);
    };

    Ok(!session
        .store
        .get_modified_since(last_materialized)?
        .is_empty())
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
    resolve_push_conflict_with_progress(session, remote, now, |_| {})
}

/// Resolve a non-fast-forward push rejection and report phase progress.
///
/// # Parameters
///
/// - `session`: the gmeta session providing the repository, store, and config.
/// - `remote`: optional remote name. If `None`, the first configured
///   metadata remote is used.
/// - `now`: the current timestamp in milliseconds since the Unix epoch.
/// - `progress`: callback invoked before long-running phases.
///
/// # Errors
///
/// Returns an error if fetch, hydration, materialization, serialization, or
/// rebase fails.
pub fn resolve_push_conflict_with_progress(
    session: &Session,
    remote: Option<&str>,
    now: i64,
    mut progress: impl FnMut(PushProgress),
) -> Result<()> {
    let repo = &session.repo;
    let ns = session.namespace();

    let remote_name = git_utils::resolve_meta_remote(repo, remote)?;
    let local_ref = session.local_ref();
    let remote_refspec = format!("refs/{ns}/main");
    let remote_tracking_ref = format!("refs/{ns}/remotes/main");

    // Fetch latest remote data
    let fetch_refspec = format!("{remote_refspec}:{remote_tracking_ref}");
    progress(PushProgress::FetchingRemote {
        remote_name: remote_name.clone(),
        remote_ref: remote_refspec,
    });
    git_utils::run_git(repo, &["fetch", &remote_name, &fetch_refspec])?;

    // Hydrate tip tree blobs so gix can read them
    let short_ref = format!("{ns}/remotes/main");
    progress(PushProgress::HydratingRemoteTip);
    git_utils::hydrate_tip_blobs(repo, &remote_name, &short_ref)?;

    // Materialize the remote data (merge into local DB)
    progress(PushProgress::MaterializingRemote);
    let _ = crate::materialize::run(session, None, now)?;

    // Re-serialize with merged data
    progress(PushProgress::SerializingMerged);
    let _ = crate::serialize::run(session, now, false)?;

    // Rewrite local ref as a single commit on top of the remote tip.
    // This avoids merge commits in the pushed history — the spec
    // requires that push always produces a single fast-forward commit.
    progress(PushProgress::RebasingMerged);
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
        "git-meta: rebase for push",
    )
    .map_err(|e| Error::Other(format!("{e}")))?;

    Ok(())
}
