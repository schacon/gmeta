/// High-level sync operations: promisor entry insertion, commit change parsing,
/// and tree key extraction for blobless clone support.
use gix::bstr::ByteSlice;
use gix::prelude::ObjectIdExt;

use crate::db::Store;
use crate::error::{Error, Result};
use crate::types::{
    Target, TargetType, ValueType, LIST_VALUE_DIR, PATH_TARGET_SEPARATOR, SET_VALUE_DIR,
    STRING_VALUE_BLOB, TOMBSTONE_ROOT,
};

/// A parsed change from a gmeta serialize commit message.
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

/// Parse the change list from a gmeta serialize commit message.
///
/// Returns `None` if the message is not a gmeta serialize commit or if
/// changes were omitted (too many to inline).
///
/// Each entry describes an operation (add/modify/delete), the target, and key.
pub fn parse_commit_changes(message: &str) -> Option<Vec<CommitChange>> {
    if !message.starts_with("gmeta: serialize") {
        return None;
    }

    let body_start = message.find("\n\n")?;
    let body = &message[body_start + 2..];

    if body.contains("changes-omitted: true") {
        return None;
    }

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
    let mut walk = repo.rev_walk(Some(tip_oid));
    if let Some(old) = old_tip {
        walk = walk.with_boundary(Some(old));
    }
    let iter = walk
        .all()
        .map_err(|e| Error::Other(format!("rev_walk failed: {e}")))?;

    let mut count = 0;
    let mut is_tip = true;

    for info_result in iter {
        let info = info_result.map_err(|e| Error::Other(format!("rev_walk iter: {e}")))?;
        let oid = info.id;

        // Skip the tip commit — it was already fully materialized
        if is_tip {
            is_tip = false;
            continue;
        }

        // If we're using boundary, stop at the boundary commit
        if old_tip.is_some() && Some(oid) == old_tip {
            break;
        }

        let commit_obj = oid
            .attach(repo)
            .object()
            .map_err(|e| Error::Other(format!("{e}")))?;
        let commit = commit_obj.into_commit();
        let message = commit.message_raw_sloppy().to_str_lossy().to_string();

        match parse_commit_changes(&message) {
            Some(changes) => {
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
                    if store.insert_promised(&target, &change.key, &ValueType::String)? {
                        count += 1;
                    }
                }
            }
            None => {
                let decoded = commit.decode().map_err(|e| Error::Other(format!("{e}")))?;
                if decoded.parents().count() == 0 {
                    // Root commit without a change list — walk its tree to discover keys
                    let tree_id = commit
                        .tree_id()
                        .map_err(|e| Error::Other(format!("{e}")))?
                        .detach();
                    let keys = extract_keys_from_tree(repo, tree_id)?;
                    for (target_type_str, target_value, key) in &keys {
                        let target_type = target_type_str.parse::<TargetType>()?;
                        let target = if target_type == TargetType::Project {
                            Target::project()
                        } else {
                            Target::from_parts(target_type, Some(target_value.clone()))
                        };
                        if store.insert_promised(&target, key, &ValueType::String)? {
                            count += 1;
                        }
                    }
                }
            }
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

    collect_blob_paths(repo, tree_id, String::new(), &mut paths)?;

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
    prefix: String,
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
            format!("{}{}", prefix, name)
        };
        if entry.mode().is_blob() {
            paths.push(full_path);
        } else if entry.mode().is_tree() {
            collect_blob_paths(repo, entry.object_id(), format!("{full_path}/"), paths)?;
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

    let target_type = parts[0];

    match target_type {
        "project" => {
            let marker_pos = parts.iter().position(|&p| p == value_type_marker)?;
            if marker_pos < 2 {
                return None;
            }
            let key = parts[1..marker_pos].join(":");
            Some(("project".to_string(), String::new(), key))
        }
        "commit" => {
            if parts.len() < 5 {
                return None;
            }
            let target_value = parts[2].to_string();
            let marker_pos = parts.iter().position(|&p| p == value_type_marker)?;
            if marker_pos < 4 {
                return None;
            }
            let key = parts[3..marker_pos].join(":");
            Some(("commit".to_string(), target_value, key))
        }
        "path" => {
            let target_pos = parts.iter().position(|&p| p == PATH_TARGET_SEPARATOR)?;
            let marker_pos = parts.iter().position(|&p| p == value_type_marker)?;
            if marker_pos <= target_pos + 1 {
                return None;
            }
            let target_value = parts[1..target_pos].join("/");
            let key = parts[target_pos + 1..marker_pos].join(":");
            Some(("path".to_string(), target_value, key))
        }
        _ => {
            if parts.len() < 5 {
                return None;
            }
            let target_value = parts[2].to_string();
            let marker_pos = parts.iter().position(|&p| p == value_type_marker)?;
            if marker_pos < 4 {
                return None;
            }
            let key = parts[3..marker_pos].join(":");
            Some((target_type.to_string(), target_value, key))
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_commit_changes_normal() {
        let msg = "gmeta: serialize (3 changes)\n\n\
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
    fn test_parse_commit_changes_non_gmeta() {
        assert_eq!(parse_commit_changes("fix: some bug"), None);
    }

    #[test]
    fn test_parse_commit_changes_omitted() {
        let msg = "gmeta: serialize (5000 changes)\n\nchanges-omitted: true\ncount: 5000";
        assert_eq!(parse_commit_changes(msg), None);
    }

    #[test]
    fn test_parse_commit_changes_no_body() {
        let msg = "gmeta: serialize (0 changes)";
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
    fn test_parse_tree_path_project_nested_key() {
        let path = "project/meta/prune/since/__value";
        let result = parse_tree_path(path).unwrap();
        assert_eq!(
            result,
            ("project".into(), String::new(), "meta:prune:since".into())
        );
    }
}
