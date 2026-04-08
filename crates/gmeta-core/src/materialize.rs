//! Materialize remote metadata into the local SQLite store.
//!
//! This module implements the full materialization workflow: discovering
//! remote metadata refs, determining merge strategies (fast-forward,
//! three-way, or two-way), applying changes to the database, creating
//! merge commits, and updating tracking refs.
//!
//! The public entry point is [`run()`], which takes a [`Session`](crate::Session)
//! and returns a [`MaterializeOutput`] describing what was applied.

use std::collections::BTreeMap;

use gix::prelude::ObjectIdExt;
use gix::refs::transaction::PreviousValue;

use crate::error::{Error, Result};
use crate::session::Session;
use crate::tree::format::{build_merged_tree, parse_tree};
use crate::tree::merge::{
    merge_list_tombstones, merge_set_member_tombstones, merge_tombstones, three_way_merge,
    two_way_merge_no_common_ancestor, ConflictDecision,
};
use crate::tree::model::{Key, ParsedTree, Tombstone, TreeValue};
use crate::types::TargetType;

/// How a remote ref was materialized.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum MaterializeStrategy {
    /// Remote was a strict superset of local — direct apply.
    FastForward,
    /// Both sides had changes — three-way merge with common ancestor.
    ThreeWayMerge,
    /// No common ancestor — two-way merge, local wins on conflicts.
    TwoWayMerge,
    /// Already up-to-date — no changes applied.
    UpToDate,
}

/// Result of materializing a single remote ref.
#[derive(Debug, Clone)]
pub struct MaterializeRefResult {
    /// The ref that was materialized.
    pub ref_name: String,
    /// The merge strategy used.
    pub strategy: MaterializeStrategy,
    /// Number of DB changes applied.
    pub changes: usize,
    /// Conflicts that were resolved during merge.
    pub conflicts: Vec<ConflictDecision>,
}

/// Result of a materialize operation.
#[derive(Debug, Clone)]
pub struct MaterializeOutput {
    /// Results per remote ref.
    pub results: Vec<MaterializeRefResult>,
}

/// Materialize remote metadata into the local SQLite store.
///
/// For each matching remote ref, determines the merge strategy and
/// applies changes to the database. Updates tracking refs and the
/// materialization timestamp.
///
/// # Parameters
///
/// - `session`: the gmeta session providing the repository, store, and config.
/// - `remote`: optional remote name filter. If `None`, all remotes are materialized.
/// - `now`: the current timestamp in milliseconds since the Unix epoch,
///   used for database writes and the `last_materialized` marker.
///
/// # Returns
///
/// A [`MaterializeOutput`] with per-ref results. If no remote refs
/// are found, the `results` vec will be empty.
///
/// # Errors
///
/// Returns an error if Git object reads, database writes, or ref updates fail.
pub fn run(session: &Session, remote: Option<&str>, now: i64) -> Result<MaterializeOutput> {
    let repo = session.repo();
    let ns = session.namespace();
    let local_ref_name = session.local_ref();
    let email = session.email();

    let remote_refs = find_remote_refs(repo, ns, remote)?;

    if remote_refs.is_empty() {
        return Ok(MaterializeOutput {
            results: Vec::new(),
        });
    }

    let mut results = Vec::new();

    for (ref_name, remote_oid) in &remote_refs {
        let remote_commit_obj = remote_oid
            .attach(repo)
            .object()
            .map_err(|e| Error::Other(format!("{e}")))?
            .into_commit();
        let remote_tree_id = remote_commit_obj
            .tree_id()
            .map_err(|e| Error::Other(format!("{e}")))?
            .detach();
        let remote_entries = parse_tree(repo, remote_tree_id, "")?;

        // Get local commit (if any)
        let local_commit_oid = repo
            .find_reference(&local_ref_name)
            .ok()
            .and_then(|r| r.into_fully_peeled_id().ok())
            .map(|id| id.detach());

        // Check if we can fast-forward: local is None, or local is an
        // ancestor of remote (no local-only commits to preserve).
        let can_fast_forward = match &local_commit_oid {
            None => true,
            Some(local_oid) => {
                if *local_oid == *remote_oid {
                    results.push(MaterializeRefResult {
                        ref_name: ref_name.clone(),
                        strategy: MaterializeStrategy::UpToDate,
                        changes: 0,
                        conflicts: Vec::new(),
                    });
                    continue;
                }
                match repo.merge_base(*local_oid, *remote_oid) {
                    Ok(base_oid) => base_oid == *local_oid,
                    Err(_) => false,
                }
            }
        };

        if can_fast_forward {
            let changes =
                materialize_fast_forward(session, &local_commit_oid, &remote_entries, email, now)?;

            // Fast-forward the ref
            repo.reference(
                local_ref_name.as_str(),
                *remote_oid,
                PreviousValue::Any,
                "fast-forward materialize",
            )
            .map_err(|e| Error::Other(format!("{e}")))?;

            results.push(MaterializeRefResult {
                ref_name: ref_name.clone(),
                strategy: MaterializeStrategy::FastForward,
                changes,
                conflicts: Vec::new(),
            });
        } else {
            // Need a real merge
            let local_oid = local_commit_oid.as_ref().ok_or_else(|| {
                Error::Other("expected local commit for merge but found None".into())
            })?;

            let (changes, conflict_decisions, strategy) = materialize_merge(
                session,
                local_oid,
                remote_oid,
                &remote_entries,
                &remote_commit_obj,
                email,
                now,
                &local_ref_name,
            )?;

            results.push(MaterializeRefResult {
                ref_name: ref_name.clone(),
                strategy,
                changes,
                conflicts: conflict_decisions,
            });
        }
    }

    session.store().set_last_materialized(now)?;

    Ok(MaterializeOutput { results })
}

/// Apply a fast-forward materialization: parse the remote tree and apply
/// it directly to the database, handling legacy deletes.
///
/// Returns the number of values in the remote tree (the change count).
fn materialize_fast_forward(
    session: &Session,
    local_commit_oid: &Option<gix::ObjectId>,
    remote_entries: &ParsedTree,
    email: &str,
    now: i64,
) -> Result<usize> {
    let repo = session.repo();

    let local_entries = if let Some(local_oid) = local_commit_oid {
        let lc = local_oid
            .attach(repo)
            .object()
            .map_err(|e| Error::Other(format!("{e}")))?
            .into_commit();
        let lt = lc
            .tree_id()
            .map_err(|e| Error::Other(format!("{e}")))?
            .detach();
        parse_tree(repo, lt, "")?
    } else {
        ParsedTree::default()
    };

    let changes = remote_entries.values.len();

    // Apply remote tree to SQLite
    session.store().apply_tree(
        &remote_entries.values,
        &remote_entries.tombstones,
        &remote_entries.set_tombstones,
        &remote_entries.list_tombstones,
        email,
        now,
    )?;

    // Ensure deletes are applied even for trees produced before tombstones.
    apply_legacy_deletes(session, &local_entries.values, remote_entries, email, now)?;

    Ok(changes)
}

/// Perform a merge materialization (three-way or two-way), apply the
/// merged result to the database, build the merged tree, and create
/// a merge commit.
///
/// Returns `(change_count, conflict_decisions, strategy)`.
#[allow(clippy::too_many_arguments)]
fn materialize_merge(
    session: &Session,
    local_oid: &gix::ObjectId,
    remote_oid: &gix::ObjectId,
    remote_entries: &ParsedTree,
    remote_commit_obj: &gix::Commit<'_>,
    email: &str,
    now: i64,
    local_ref_name: &str,
) -> Result<(usize, Vec<ConflictDecision>, MaterializeStrategy)> {
    let repo = session.repo();

    let local_commit_obj = local_oid
        .attach(repo)
        .object()
        .map_err(|e| Error::Other(format!("{e}")))?
        .into_commit();
    let local_tree_id = local_commit_obj
        .tree_id()
        .map_err(|e| Error::Other(format!("{e}")))?
        .detach();
    let local_entries = parse_tree(repo, local_tree_id, "")?;

    // Get commit timestamps for conflict resolution
    let local_timestamp = extract_author_timestamp(&local_commit_obj)?;
    let remote_timestamp = extract_author_timestamp(remote_commit_obj)?;

    let merge_base_oid = repo.merge_base(*local_oid, *remote_oid).ok();

    let (
        merged_values,
        merged_tombstones,
        merged_set_tombstones,
        merged_list_tombstones,
        conflict_decisions,
        strategy,
        legacy_base_values,
    ) = if let Some(base_oid) = merge_base_oid {
        run_three_way_merge(
            repo,
            base_oid,
            &local_entries,
            remote_entries,
            local_timestamp,
            remote_timestamp,
        )?
    } else {
        run_two_way_merge(&local_entries, remote_entries)?
    };

    let changes = merged_values.len();

    // Update SQLite
    session.store().apply_tree(
        &merged_values,
        &merged_tombstones,
        &merged_set_tombstones,
        &merged_list_tombstones,
        email,
        now,
    )?;

    // Handle removals where no explicit tombstone exists (legacy trees)
    if let Some(base_values) = &legacy_base_values {
        for key in base_values.keys() {
            if !merged_values.contains_key(key) && !merged_tombstones.contains_key(key) {
                let tt = key.target_type.parse::<TargetType>()?;
                session
                    .store()
                    .apply_tombstone(&tt, &key.target_value, &key.key, email, now)?;
            }
        }
    }

    // Build the merged tree and write a merge commit
    let merged_tree_oid = build_merged_tree(
        repo,
        &merged_values,
        &merged_tombstones,
        &merged_set_tombstones,
        &merged_list_tombstones,
    )?;

    let name = session.name();
    let sig = gix::actor::Signature {
        name: name.into(),
        email: email.into(),
        time: gix::date::Time::new(now / 1000, 0),
    };

    let commit = gix::objs::Commit {
        message: "materialize".into(),
        tree: merged_tree_oid,
        author: sig.clone(),
        committer: sig,
        encoding: None,
        parents: vec![*local_oid, *remote_oid].into(),
        extra_headers: Default::default(),
    };

    let merge_commit_oid = repo
        .write_object(&commit)
        .map_err(|e| Error::Other(format!("{e}")))?
        .detach();
    repo.reference(
        local_ref_name,
        merge_commit_oid,
        PreviousValue::Any,
        "materialize merge",
    )
    .map_err(|e| Error::Other(format!("{e}")))?;

    Ok((changes, conflict_decisions, strategy))
}

/// Run a three-way merge using a common ancestor.
///
/// Returns the merged values, tombstones, set tombstones, list tombstones,
/// conflict decisions, strategy, and legacy base values for implicit deletes.
#[allow(clippy::type_complexity)]
fn run_three_way_merge(
    repo: &gix::Repository,
    base_oid: gix::Id<'_>,
    local_entries: &ParsedTree,
    remote_entries: &ParsedTree,
    local_timestamp: i64,
    remote_timestamp: i64,
) -> Result<(
    BTreeMap<Key, TreeValue>,
    BTreeMap<Key, Tombstone>,
    BTreeMap<(Key, String), String>,
    BTreeMap<(Key, String), Tombstone>,
    Vec<ConflictDecision>,
    MaterializeStrategy,
    Option<BTreeMap<Key, TreeValue>>,
)> {
    let base_commit_obj = base_oid
        .object()
        .map_err(|e| Error::Other(format!("{e}")))?
        .into_commit();
    let base_tree_id = base_commit_obj
        .tree_id()
        .map_err(|e| Error::Other(format!("{e}")))?
        .detach();
    let base_entries = parse_tree(repo, base_tree_id, "")?;

    let legacy_base_values = Some(base_entries.values.clone());

    let (merged_values, conflict_decisions) = three_way_merge(
        &base_entries.values,
        &local_entries.values,
        &remote_entries.values,
        local_timestamp,
        remote_timestamp,
    )?;

    let merged_tombstones = merge_tombstones(
        &base_entries.tombstones,
        &local_entries.tombstones,
        &remote_entries.tombstones,
        &merged_values,
    );
    let merged_set_tombstones = merge_set_member_tombstones(
        &local_entries.set_tombstones,
        &remote_entries.set_tombstones,
        &merged_values,
    );
    let merged_list_tombstones = merge_list_tombstones(
        &local_entries.list_tombstones,
        &remote_entries.list_tombstones,
        &merged_values,
    );

    Ok((
        merged_values,
        merged_tombstones,
        merged_set_tombstones,
        merged_list_tombstones,
        conflict_decisions,
        MaterializeStrategy::ThreeWayMerge,
        legacy_base_values,
    ))
}

/// Run a two-way merge when no common ancestor exists.
///
/// Returns the merged values, tombstones, set tombstones, list tombstones,
/// conflict decisions, strategy, and `None` for legacy base values.
#[allow(clippy::type_complexity)]
fn run_two_way_merge(
    local_entries: &ParsedTree,
    remote_entries: &ParsedTree,
) -> Result<(
    BTreeMap<Key, TreeValue>,
    BTreeMap<Key, Tombstone>,
    BTreeMap<(Key, String), String>,
    BTreeMap<(Key, String), Tombstone>,
    Vec<ConflictDecision>,
    MaterializeStrategy,
    Option<BTreeMap<Key, TreeValue>>,
)> {
    let (merged_values, merged_tombstones, conflict_decisions) = two_way_merge_no_common_ancestor(
        &local_entries.values,
        &local_entries.tombstones,
        &remote_entries.values,
        &remote_entries.tombstones,
    );
    let merged_set_tombstones = merge_set_member_tombstones(
        &local_entries.set_tombstones,
        &remote_entries.set_tombstones,
        &merged_values,
    );
    let merged_list_tombstones = merge_list_tombstones(
        &local_entries.list_tombstones,
        &remote_entries.list_tombstones,
        &merged_values,
    );

    Ok((
        merged_values,
        merged_tombstones,
        merged_set_tombstones,
        merged_list_tombstones,
        conflict_decisions,
        MaterializeStrategy::TwoWayMerge,
        None,
    ))
}

/// Apply legacy deletes: entries present in the local tree but absent
/// from the remote tree and not covered by an explicit tombstone.
///
/// This handles the case where trees were produced before tombstone
/// support was added.
fn apply_legacy_deletes(
    session: &Session,
    local_values: &BTreeMap<Key, TreeValue>,
    remote_entries: &ParsedTree,
    email: &str,
    now: i64,
) -> Result<()> {
    for key in local_values.keys() {
        if !remote_entries.values.contains_key(key) {
            let tt = key.target_type.parse::<TargetType>()?;
            session
                .store()
                .apply_tombstone(&tt, &key.target_value, &key.key, email, now)?;
        }
    }
    Ok(())
}

/// Extract the author timestamp (in seconds) from a commit object.
///
/// # Errors
///
/// Returns an error if the commit cannot be decoded or the author
/// signature is malformed.
fn extract_author_timestamp(commit: &gix::Commit<'_>) -> Result<i64> {
    let decoded = commit.decode().map_err(|e| Error::Other(format!("{e}")))?;
    let time = decoded
        .author()
        .map_err(|e| Error::Other(format!("{e}")))?
        .time()
        .map_err(|e| Error::Other(format!("{e}")))?;
    Ok(time.seconds)
}

/// Find remote refs matching the given namespace and optional remote filter.
///
/// Returns a list of `(ref_name, object_id)` pairs for remote metadata refs.
/// Local refs (under `refs/{ns}/local/`) are excluded.
///
/// # Parameters
///
/// - `repo`: the git repository to search.
/// - `ns`: the metadata namespace (e.g. `"meta"`).
/// - `remote`: optional remote name filter. If `None`, all non-local refs
///   under the namespace are returned.
///
/// # Errors
///
/// Returns an error if iterating refs fails.
pub fn find_remote_refs(
    repo: &gix::Repository,
    ns: &str,
    remote: Option<&str>,
) -> Result<Vec<(String, gix::ObjectId)>> {
    let mut results = Vec::new();

    let prefix = match remote {
        Some(r) => format!("refs/{}/{}", ns, r),
        None => format!("refs/{}/", ns),
    };
    let local_prefix = format!("refs/{}/local/", ns);

    let platform = repo
        .references()
        .map_err(|e| Error::Other(format!("{e}")))?;
    for reference in platform.all().map_err(|e| Error::Other(format!("{e}")))? {
        let reference = reference.map_err(|e| Error::Other(format!("{e}")))?;
        let name = reference.name().as_bstr().to_string();
        if name.starts_with(&prefix) && !name.starts_with(&local_prefix) {
            if let Ok(id) = reference.into_fully_peeled_id() {
                results.push((name, id.detach()));
            }
        }
    }

    Ok(results)
}
