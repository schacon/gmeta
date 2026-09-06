//! High-level sync operations: promisor entry insertion, commit change parsing,
//! and tree key extraction for blobless clone support.

use gix::bstr::ByteSlice;
use gix::prelude::ObjectIdExt;

use crate::db::Store;
use crate::error::{Error, Result};
use crate::tree::format::parse_path_parts;
use crate::types::{
    Target, TargetType, ValueType, LIST_VALUE_DIR, SET_VALUE_DIR, STRING_VALUE_BLOB, TOMBSTONE_ROOT,
};

/// A parsed change from a `git-meta` serialize commit message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitChange {
    /// The operation character: 'A' (add), 'M' (modify), 'D' (delete).
    pub op: char,
    /// The target type string (e.g. "commit", "project").
    pub target_type: String,
    /// The target value (e.g. commit SHA, branch name). Empty for project targets.
    pub target_value: String,
    /// The metadata key.
    pub key: String,
}

/// Parse the change list from a `git-meta` serialize commit message.
///
/// Returns `None` if the message is not a serialize commit or if changes were
/// omitted (too many to inline).
///
/// Accepts the current `git-meta: serialize` prefix and the legacy `git-meta: serialize`
/// prefix so historical metadata histories remain readable.
///
/// Each entry describes an operation (add/modify/delete), the target, and key.
#[must_use]
pub fn parse_commit_changes(message: &str) -> Option<Vec<CommitChange>> {
    if !is_serialize_commit_message(message) {
        return None;
    }

    if commit_changes_omitted(message) {
        return None;
    }

    let body_start = message.find("\n\n")?;
    let body = &message[body_start + 2..];

    let mut changes = Vec::new();
    for line in body.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let parts: Vec<&str> = line.splitn(3, '\t').collect();
        if parts.len() != 3 {
            continue;
        }
        let op = parts[0].chars().next()?;
        let target_label = parts[1];
        let key = parts[2].to_string();

        let (target_type, target_value) = if target_label == "project" {
            ("project".to_string(), String::new())
        } else if let Some((t, v)) = target_label.split_once(':') {
            (t.to_string(), v.to_string())
        } else {
            continue;
        };

        changes.push(CommitChange {
            op,
            target_type,
            target_value,
            key,
        });
    }

    Some(changes)
}

/// Return whether a serialize commit omitted its inline change list.
///
/// Large serialize commits include `changes-omitted: true` instead of one
/// line per changed key. Callers can still discover keys by walking the
/// commit's tree, because the tree layout records target/key paths without
/// requiring blob content.
#[must_use]
pub fn commit_changes_omitted(message: &str) -> bool {
    if !is_serialize_commit_message(message) {
        return false;
    }

    let Some(body_start) = message.find("\n\n") else {
        return false;
    };
    let body = &message[body_start + 2..];
    body.contains("changes-omitted: true")
}

fn is_serialize_commit_message(message: &str) -> bool {
    message.starts_with("git-meta: serialize") || message.starts_with("git-meta: serialize")
}

/// Walk non-tip commits and insert promisor entries for keys mentioned
/// in their commit messages.
///
/// Used after a blobless fetch to build an index of all metadata keys
/// in the history without downloading blob content. Returns the number
/// of new promisor entries inserted.
///
/// # Parameters
/// - `repo`: the git repository
/// - `store`: the metadata store (promisor entries are inserted here)
/// - `tip_oid`: the tip commit (already materialized, will be skipped)
/// - `old_tip`: optional boundary — stop walking when this commit is reached
pub fn insert_promisor_entries(
    repo: &gix::Repository,
    store: &Store,
    tip_oid: gix::ObjectId,
    old_tip: Option<gix::ObjectId>,
) -> Result<usize> {
    insert_promisor_entries_with_progress(repo, store, tip_oid, old_tip, |_| {})
}

/// Index history toward `tip`, resuming from a saved checkpoint if there is one.
///
/// The walk goes newest-first, so a checkpoint's `resume_from` commit and its
/// ancestors are exactly the work that is left; restarting the walk there costs
/// nothing already paid for. Insertion is idempotent, so re-covering a little
/// ground after an interruption is harmless.
///
/// Progress is checkpointed every few thousand commits, which also refreshes
/// the heartbeat another process uses to tell a live indexer from a dead one.
///
/// # Errors
///
/// Returns an error if the revision walk, a database write, or a checkpoint
/// write fails.
pub fn index_history_resumable(
    repo: &gix::Repository,
    store: &Store,
    tip_oid: gix::ObjectId,
    now_ms: i64,
    mut progress: impl FnMut(IndexProgress),
) -> Result<usize> {
    /// Commits between checkpoints. Frequent enough that an interruption
    /// wastes little, rare enough not to dominate the walk.
    const CHECKPOINT_INTERVAL: usize = 5_000;

    let existing = crate::index_state::load(repo)?;
    let mut state = match existing {
        Some(state) if state.indexes(&tip_oid) && !state.complete => state,
        // A checkpoint for a different tip, or a finished one, describes other
        // work. Start again.
        _ => crate::index_state::IndexState::starting(&tip_oid, now_ms),
    };

    let start_from = state.resume_point().unwrap_or(tip_oid);
    // Resuming skips the tip-is-materialized rule, because the commit we
    // continue from is an ordinary historical commit.
    let resuming = state.resume_from.is_some();

    let already_walked = state.commits_indexed;
    let already_indexed = state.keys_indexed;
    let mut last_checkpoint = 0usize;

    let inserted = walk_and_index(
        repo,
        store,
        start_from,
        None,
        !resuming,
        |walked, indexed, oid| {
            progress(IndexProgress {
                commits_walked: already_walked + walked,
                keys_indexed: already_indexed + indexed,
            });
            if walked - last_checkpoint >= CHECKPOINT_INTERVAL {
                last_checkpoint = walked;
                state.resume_from = Some(oid.to_string());
                state.commits_indexed = already_walked + walked;
                state.keys_indexed = already_indexed + indexed;
                state.heartbeat_ms = now_ms + walked as i64;
                let _ = crate::index_state::save(repo, &state);
            }
        },
    )?;

    state.resume_from = None;
    state.commits_indexed = already_walked + inserted.commits;
    state.keys_indexed = already_indexed + inserted.keys;
    state.complete = true;
    state.heartbeat_ms = now_ms;
    crate::index_state::save(repo, &state)?;

    Ok(inserted.keys)
}

/// What one indexing walk covered.
struct Indexed {
    commits: usize,
    keys: usize,
}

/// How far history indexing has got.
///
/// Indexing walks every commit in the metadata history, which on a long-lived
/// project is the slowest part of setting up a clone. Callers that show a user
/// what is happening need to say so while it happens, not once it is over.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IndexProgress {
    /// Commits visited so far.
    pub commits_walked: usize,
    /// Promisor entries inserted so far.
    pub keys_indexed: usize,
}

/// Walk history inserting promisor entries, reporting progress as it goes.
///
/// `progress` is called periodically rather than per commit, so a caller can
/// render it directly without throttling.
///
/// # Errors
///
/// Returns an error if the revision walk or any database write fails.
pub fn insert_promisor_entries_with_progress(
    repo: &gix::Repository,
    store: &Store,
    tip_oid: gix::ObjectId,
    old_tip: Option<gix::ObjectId>,
    mut progress: impl FnMut(IndexProgress),
) -> Result<usize> {
    /// Commits between progress reports. Frequent enough to look alive on a
    /// slow walk, rare enough not to dominate a fast one.
    const PROGRESS_INTERVAL: usize = 2_000;

    let indexed = walk_and_index(repo, store, tip_oid, old_tip, true, |walked, keys, _oid| {
        if walked.is_multiple_of(PROGRESS_INTERVAL) {
            progress(IndexProgress {
                commits_walked: walked,
                keys_indexed: keys,
            });
        }
    })?;
    Ok(indexed.keys)
}

/// Walk commits from `from_oid` inserting a promisor entry per key mentioned.
///
/// `skip_first` omits the starting commit, which the caller wants when starting
/// at a tip that has already been materialized, and does not want when resuming
/// mid-history. `observe` is called once per commit with the running totals and
/// the commit just handled, which is what makes checkpointing possible.
fn walk_and_index(
    repo: &gix::Repository,
    store: &Store,
    from_oid: gix::ObjectId,
    boundary: Option<gix::ObjectId>,
    skip_first: bool,
    mut observe: impl FnMut(usize, usize, gix::ObjectId),
) -> Result<Indexed> {
    let mut walk = repo.rev_walk(Some(from_oid));
    if let Some(old) = boundary {
        walk = walk.with_boundary(Some(old));
    }
    let iter = walk
        .all()
        .map_err(|e| Error::Other(format!("rev_walk failed: {e}")))?;

    let mut keys = 0usize;
    let mut walked = 0usize;
    let mut is_first = true;

    for info_result in iter {
        let info = info_result.map_err(|e| Error::Other(format!("rev_walk iter: {e}")))?;
        let oid = info.id;

        if is_first {
            is_first = false;
            if skip_first {
                continue;
            }
        }

        if boundary.is_some() && Some(oid) == boundary {
            break;
        }

        let commit_oid = oid.to_string();
        let commit_obj = oid
            .attach(repo)
            .object()
            .map_err(|e| Error::Other(format!("{e}")))?;
        let commit = commit_obj.into_commit();
        let message = commit.message_raw_sloppy().to_str_lossy().to_string();

        if let Some(changes) = parse_commit_changes(&message) {
            for change in &changes {
                if change.op == 'D' {
                    continue;
                }
                let target_type = change.target_type.parse::<TargetType>()?;
                let target = if target_type == TargetType::Project {
                    Target::project()
                } else {
                    Target::from_parts(target_type, Some(change.target_value.clone()))
                };
                // Record where the value lives. Walking newest-first means the
                // first commit to mention a key is the last one that wrote it,
                // which is the tree still holding its value after pruning.
                if store.insert_promised(
                    &target,
                    &change.key,
                    &ValueType::String,
                    Some(&commit_oid),
                )? {
                    keys += 1;
                }
            }
        } else {
            let decoded = commit.decode().map_err(|e| Error::Other(format!("{e}")))?;
            if decoded.parents().count() == 0 || commit_changes_omitted(&message) {
                // Root commits and omitted-change commits need tree walking
                // because they do not carry an inline per-key change list.
                let tree_id = commit
                    .tree_id()
                    .map_err(|e| Error::Other(format!("{e}")))?
                    .detach();
                keys += insert_promised_tree_keys(repo, store, tree_id, &commit_oid)?;
            }
        }

        walked += 1;
        observe(walked, keys, oid);
    }

    Ok(Indexed {
        commits: walked,
        keys,
    })
}

fn insert_promised_tree_keys(
    repo: &gix::Repository,
    store: &Store,
    tree_id: gix::ObjectId,
    commit_oid: &str,
) -> Result<usize> {
    let keys = extract_keys_from_tree(repo, tree_id)?;
    let mut count = 0;

    for (target_type_str, target_value, key) in &keys {
        let target_type = target_type_str.parse::<TargetType>()?;
        let target = if target_type == TargetType::Project {
            Target::project()
        } else {
            Target::from_parts(target_type, Some(target_value.clone()))
        };
        if store.insert_promised(&target, key, &ValueType::String, Some(commit_oid))? {
            count += 1;
        }
    }

    Ok(count)
}

/// Extract `(target_type, target_value, key)` tuples from a git tree by walking
/// all paths and parsing the tree structure.
///
/// Only looks at path names — does not read blob content, so works on trees
/// with missing blobs (blobless clones).
pub fn extract_keys_from_tree(
    repo: &gix::Repository,
    tree_id: gix::ObjectId,
) -> Result<Vec<(String, String, String)>> {
    let mut keys = Vec::new();
    let mut paths = Vec::new();

    collect_blob_paths(repo, tree_id, "", &mut paths)?;

    for path in &paths {
        if let Some(parsed) = parse_tree_path(path) {
            keys.push(parsed);
        }
    }

    keys.sort();
    keys.dedup();
    Ok(keys)
}

/// Recursively collect all blob paths in a tree.
fn collect_blob_paths(
    repo: &gix::Repository,
    tree_id: gix::ObjectId,
    prefix: &str,
    paths: &mut Vec<String>,
) -> Result<()> {
    let tree = tree_id
        .attach(repo)
        .object()
        .map_err(|e| Error::Other(format!("{e}")))?
        .into_tree();
    for entry_result in tree.iter() {
        let entry = entry_result.map_err(|e| Error::Other(format!("{e}")))?;
        let name = entry.filename().to_str_lossy().to_string();
        let full_path = if prefix.is_empty() {
            name.clone()
        } else {
            format!("{prefix}{name}")
        };
        if entry.mode().is_blob() {
            paths.push(full_path);
        } else if entry.mode().is_tree() {
            collect_blob_paths(repo, entry.object_id(), &format!("{full_path}/"), paths)?;
        }
    }
    Ok(())
}

/// Parse a tree path into `(target_type, target_value, key)`.
///
/// Handles all target type layouts: project, commit (sharded), path (with separator),
/// and branch/change-id (hash-sharded). Returns `None` for tombstone paths or
/// unparseable paths.
fn parse_tree_path(path: &str) -> Option<(String, String, String)> {
    let parts: Vec<&str> = path.split('/').collect();
    if parts.len() < 2 {
        return None;
    }

    if parts.contains(&TOMBSTONE_ROOT) {
        return None;
    }

    let value_type_marker = if parts.contains(&STRING_VALUE_BLOB) {
        STRING_VALUE_BLOB
    } else if parts.contains(&LIST_VALUE_DIR) {
        LIST_VALUE_DIR
    } else if parts.contains(&SET_VALUE_DIR) {
        SET_VALUE_DIR
    } else {
        return None;
    };

    let (target_type, target_value, key_parts) = parse_path_parts(&parts).ok()?;
    let marker_pos = key_parts.iter().position(|&p| p == value_type_marker)?;
    if marker_pos == 0 {
        return None;
    }
    let key = key_parts[..marker_pos].join(":");
    Some((target_type.as_str().to_string(), target_value, key))
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_commit_changes_normal() {
        let msg = "git-meta: serialize (3 changes)\n\n\
                   A\tcommit:abc123\tagent:model\n\
                   M\tproject\tmeta:prune:since\n\
                   D\tbranch:main\treview:status";
        let changes = parse_commit_changes(msg).unwrap();
        assert_eq!(changes.len(), 3);
        assert_eq!(changes[0].op, 'A');
        assert_eq!(changes[0].target_type, "commit");
        assert_eq!(changes[0].target_value, "abc123");
        assert_eq!(changes[0].key, "agent:model");
        assert_eq!(changes[2].op, 'D');
    }

    #[test]
    fn test_parse_commit_changes_legacy_git_meta_prefix() {
        let msg = "git-meta: serialize (1 changes)\n\nA\tproject\told_key";
        let changes = parse_commit_changes(msg).unwrap();
        assert_eq!(changes.len(), 1);
        assert_eq!(changes[0].key, "old_key");
    }

    #[test]
    fn test_parse_commit_changes_non_git_meta() {
        assert_eq!(parse_commit_changes("fix: some bug"), None);
    }

    #[test]
    fn test_parse_commit_changes_omitted() {
        let msg = "git-meta: serialize (5000 changes)\n\nchanges-omitted: true\ncount: 5000";
        assert_eq!(parse_commit_changes(msg), None);
    }

    #[test]
    fn test_parse_commit_changes_no_body() {
        let msg = "git-meta: serialize (0 changes)";
        assert_eq!(parse_commit_changes(msg), None);
    }

    #[test]
    fn test_parse_tree_path_commit() {
        let path = "commit/ab/abc123def456/agent/model/__value";
        let result = parse_tree_path(path).unwrap();
        assert_eq!(
            result,
            ("commit".into(), "abc123def456".into(), "agent:model".into())
        );
    }

    #[test]
    fn test_parse_tree_path_project() {
        let path = "project/testing/__value";
        let result = parse_tree_path(path).unwrap();
        assert_eq!(result, ("project".into(), String::new(), "testing".into()));
    }

    #[test]
    fn test_parse_tree_path_tombstone_ignored() {
        let path = "commit/ab/abc123/__tombstones/key/__deleted";
        assert_eq!(parse_tree_path(path), None);
    }

    #[test]
    fn test_parse_tree_path_list() {
        let path = "commit/ab/abc123/tags/__list/12345-abcde";
        let result = parse_tree_path(path).unwrap();
        assert_eq!(result, ("commit".into(), "abc123".into(), "tags".into()));
    }

    #[test]
    fn test_parse_tree_path_branch() {
        let path = "branch/ab/feature-x/review/status/__value";
        let result = parse_tree_path(path).unwrap();
        assert_eq!(
            result,
            ("branch".into(), "feature-x".into(), "review:status".into())
        );
    }

    #[test]
    fn test_parse_tree_path_branch_with_slash() {
        let path = "branch/a6/alex/trails-multi-pr-a57e52c3/review/status/__value";
        let result = parse_tree_path(path).unwrap();
        assert_eq!(
            result,
            (
                "branch".into(),
                "alex/trails-multi-pr-a57e52c3".into(),
                "review:status".into()
            )
        );
    }

    #[test]
    fn test_parse_tree_path_project_nested_key() {
        let path = "project/meta/prune/since/__value";
        let result = parse_tree_path(path).unwrap();
        assert_eq!(
            result,
            ("project".into(), String::new(), "meta:prune:since".into())
        );
    }
}
