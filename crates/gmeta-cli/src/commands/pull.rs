use anyhow::Result;

use crate::commands::{materialize, serialize};
use crate::context::CommandContext;
use gmeta_core::db::Db;
use gmeta_core::git_utils;
use gmeta_core::types::{self, TargetType, ValueType};

pub fn run(remote: Option<&str>, verbose: bool) -> Result<()> {
    let ctx = CommandContext::open_git2(None)?;
    let repo = ctx.git2_repo()?;
    let ns = &ctx.namespace;

    let remote_name = git_utils::resolve_meta_remote(repo, remote)?;
    let remote_refspec = format!("refs/{}/main", ns);
    let tracking_ref = format!("refs/{}/remotes/main", ns);
    let fetch_refspec = format!("{}:{}", remote_refspec, tracking_ref);

    if verbose {
        eprintln!("[verbose] remote: {}", remote_name);
        eprintln!("[verbose] fetch refspec: {}", fetch_refspec);
    }

    // Record the old tip so we can count new commits
    let old_tip = repo
        .find_reference(&tracking_ref)
        .ok()
        .and_then(|r| r.peel_to_commit().ok())
        .map(|c| c.id());

    // Fetch latest remote metadata
    eprintln!("Fetching metadata from {}...", remote_name);
    git_utils::git2_run_git(repo, &["fetch", &remote_name, &fetch_refspec])?;

    // Get the new tip
    let new_tip = repo
        .find_reference(&tracking_ref)
        .ok()
        .and_then(|r| r.peel_to_commit().ok())
        .map(|c| c.id());

    // Check if we need to materialize even if no new commits were fetched
    // (e.g. remote add fetched but never materialized)
    let needs_materialize =
        ctx.db.get_last_materialized()?.is_none() || repo.find_reference(&ctx.local_ref()).is_err();

    // Count new commits
    match (old_tip, new_tip) {
        (Some(old), Some(new)) if old == new => {
            if !needs_materialize {
                println!("Already up-to-date.");
                return Ok(());
            }
            eprintln!("No new commits, but local state needs materializing.");
        }
        (Some(old), Some(new)) => {
            let count = count_commits_between(repo, old, new);
            eprintln!(
                "Fetched {} new commit{}.",
                count,
                if count == 1 { "" } else { "s" }
            );
        }
        (None, Some(_)) => {
            eprintln!("Fetched initial metadata history.");
        }
        _ => {}
    }

    // Hydrate tip tree blobs so libgit2 can read them
    let short_ref = format!("{}/remotes/main", ns);
    git_utils::hydrate_tip_blobs(repo, &remote_name, &short_ref)?;

    // Serialize local state so materialize can do a proper 3-way merge
    eprintln!("Serializing local metadata...");
    serialize::run(verbose)?;

    // Materialize: merge remote tree into local DB
    eprintln!("Materializing remote metadata...");
    materialize::run(None, false, verbose)?;

    // Insert promisor entries from non-tip commits so we know what keys exist
    // in the history even though we haven't fetched their blob data yet.
    // On first materialize, walk the entire history (pass None as old_tip).
    if let Some(new) = new_tip {
        let walk_from = if needs_materialize { None } else { old_tip };
        let promisor_count = insert_promisor_entries(repo, &ctx.db, new, walk_from, verbose)?;
        if promisor_count > 0 {
            eprintln!(
                "Indexed {} keys from history (available on demand).",
                promisor_count
            );
        }
    }

    println!("Pulled metadata from {}", remote_name);
    Ok(())
}

/// Public entry point for parsing commit changes (used by promisor command).
pub fn parse_commit_changes_pub(message: &str) -> Option<Vec<(char, String, String, String)>> {
    parse_commit_changes(message)
}

/// Parse the change list from a gmeta serialize commit message.
/// Returns None if the message can't be parsed (not a gmeta commit, or changes omitted).
/// Each entry is (op, target_type, target_value, key).
fn parse_commit_changes(message: &str) -> Option<Vec<(char, String, String, String)>> {
    if !message.starts_with("gmeta: serialize") {
        return None;
    }

    // Find the body (after first blank line)
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

        changes.push((op, target_type, target_value, key));
    }

    Some(changes)
}

/// Public entry point for inserting promisor entries (used by remote add).
pub fn insert_promisor_entries_pub(
    repo: &git2::Repository,
    db: &Db,
    tip_oid: git2::Oid,
    old_tip: Option<git2::Oid>,
    verbose: bool,
) -> Result<usize> {
    insert_promisor_entries(repo, db, tip_oid, old_tip, verbose)
}

/// Walk non-tip commits and insert promisor entries for keys mentioned in their
/// commit messages. Returns the number of new promisor entries inserted.
fn insert_promisor_entries(
    repo: &git2::Repository,
    db: &Db,
    tip_oid: git2::Oid,
    old_tip: Option<git2::Oid>,
    verbose: bool,
) -> Result<usize> {
    let mut revwalk = repo.revwalk()?;
    revwalk.push(tip_oid)?;
    if let Some(old) = old_tip {
        revwalk.hide(old)?;
    }

    let mut count = 0;
    let mut is_tip = true;

    for oid_result in revwalk {
        let oid = oid_result?;

        // Skip the tip commit — it was already fully materialized
        if is_tip {
            is_tip = false;
            continue;
        }

        let commit = repo.find_commit(oid)?;
        let message = commit.message().unwrap_or("");

        match parse_commit_changes(message) {
            Some(changes) => {
                for (op, target_type_str, target_value, key) in &changes {
                    if *op == 'D' {
                        continue;
                    }
                    let target_type = TargetType::from_str(target_type_str)?;
                    if db.insert_promised(&target_type, target_value, key, &ValueType::String)? {
                        count += 1;
                        if verbose {
                            eprintln!(
                                "[verbose] promisor: {} {}:{} {}",
                                op,
                                target_type.as_str(),
                                target_value,
                                key
                            );
                        }
                    }
                }
            }
            None if commit.parent_count() == 0 => {
                // Root commit without a change list — walk its tree to discover keys
                let tree = commit.tree()?;
                let keys = extract_keys_from_tree(repo, &tree)?;
                for (target_type_str, target_value, key) in &keys {
                    let target_type = TargetType::from_str(target_type_str)?;
                    if db.insert_promised(&target_type, target_value, key, &ValueType::String)? {
                        count += 1;
                        if verbose {
                            eprintln!(
                                "[verbose] promisor (tree): {}:{} {}",
                                target_type.as_str(),
                                target_value,
                                key
                            );
                        }
                    }
                }
            }
            None => {}
        }
    }

    Ok(count)
}

/// Public entry point for extracting keys from a tree (used by promisor command).
pub fn extract_keys_from_tree_pub(
    repo: &git2::Repository,
    tree: &git2::Tree,
) -> Result<Vec<(String, String, String)>> {
    extract_keys_from_tree(repo, tree)
}

/// Extract (target_type, target_value, key) tuples from a git tree by walking
/// all paths and parsing the tree structure. Only looks at path names — does not
/// read blob content, so works on trees with missing blobs.
fn extract_keys_from_tree(
    _repo: &git2::Repository,
    tree: &git2::Tree,
) -> Result<Vec<(String, String, String)>> {
    let mut keys = Vec::new();
    let mut paths = Vec::new();

    // Collect all paths via tree walk
    tree.walk(git2::TreeWalkMode::PreOrder, |root, entry| {
        if entry.kind() == Some(git2::ObjectType::Blob) {
            let full_path = format!("{}{}", root, entry.name().unwrap_or(""));
            paths.push(full_path);
        }
        git2::TreeWalkResult::Ok
    })?;

    for path in &paths {
        if let Some(parsed) = parse_tree_path(path) {
            keys.push(parsed);
        }
    }

    keys.sort();
    keys.dedup();
    Ok(keys)
}

/// Parse a tree path like "project/testing/__value" or "commit/ab/ab1234.../agent/model/__value"
/// into (target_type, target_value, key). Returns None for tombstones or unparseable paths.
fn parse_tree_path(path: &str) -> Option<(String, String, String)> {
    let parts: Vec<&str> = path.split('/').collect();
    if parts.len() < 2 {
        return None;
    }

    // Skip tombstone paths
    if parts.contains(&types::TOMBSTONE_ROOT) {
        return None;
    }

    // Find the value marker (__value, or a parent __list or __set dir)
    let value_type_marker = if parts.contains(&types::STRING_VALUE_BLOB) {
        types::STRING_VALUE_BLOB
    } else if parts.contains(&types::LIST_VALUE_DIR) {
        types::LIST_VALUE_DIR
    } else if parts.contains(&types::SET_VALUE_DIR) {
        types::SET_VALUE_DIR
    } else {
        return None;
    };

    let target_type = parts[0];

    match target_type {
        "project" => {
            // project/{key_segments}/__value
            let marker_pos = parts.iter().position(|&p| p == value_type_marker)?;
            if marker_pos < 2 {
                return None;
            }
            let key = parts[1..marker_pos].join(":");
            Some(("project".to_string(), String::new(), key))
        }
        "commit" => {
            // commit/{shard}/{full_sha}/{key_segments}/__value
            if parts.len() < 5 {
                return None;
            }
            let target_value = parts[2].to_string(); // full SHA
            let marker_pos = parts.iter().position(|&p| p == value_type_marker)?;
            if marker_pos < 4 {
                return None;
            }
            let key = parts[3..marker_pos].join(":");
            Some(("commit".to_string(), target_value, key))
        }
        "path" => {
            // path/{encoded_segments}/__target__/{key_segments}/__value
            let target_pos = parts
                .iter()
                .position(|&p| p == types::PATH_TARGET_SEPARATOR)?;
            let marker_pos = parts.iter().position(|&p| p == value_type_marker)?;
            if marker_pos <= target_pos + 1 {
                return None;
            }
            let target_value = parts[1..target_pos].join("/");
            let key = parts[target_pos + 1..marker_pos].join(":");
            Some(("path".to_string(), target_value, key))
        }
        _ => {
            // branch/{shard}/{value}/{key_segments}/__value
            // change-id/{shard}/{value}/{key_segments}/__value
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

/// Count commits reachable from `new` but not from `old`.
fn count_commits_between(repo: &git2::Repository, old: git2::Oid, new: git2::Oid) -> usize {
    let mut revwalk = match repo.revwalk() {
        Ok(rw) => rw,
        Err(_) => return 0,
    };
    if revwalk.push(new).is_err() {
        return 0;
    }
    if revwalk.hide(old).is_err() {
        return 0;
    }
    revwalk.count()
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
        assert_eq!(
            changes[0],
            ('A', "commit".into(), "abc123".into(), "agent:model".into())
        );
        assert_eq!(
            changes[1],
            (
                'M',
                "project".into(),
                String::new(),
                "meta:prune:since".into()
            )
        );
        assert_eq!(
            changes[2],
            ('D', "branch".into(), "main".into(), "review:status".into())
        );
    }

    #[test]
    fn test_parse_commit_changes_omitted() {
        let msg = "gmeta: serialize (5000 changes)\n\nchanges-omitted: true\ncount: 5000";
        assert!(parse_commit_changes(msg).is_none());
    }

    #[test]
    fn test_parse_commit_changes_non_gmeta() {
        let msg = "Initial commit\n\nSome body text";
        assert!(parse_commit_changes(msg).is_none());
    }

    #[test]
    fn test_parse_commit_changes_no_body() {
        let msg = "gmeta: serialize (0 changes)";
        assert!(parse_commit_changes(msg).is_none());
    }

    #[test]
    fn test_parse_tree_path_project() {
        assert_eq!(
            parse_tree_path("project/testing/__value"),
            Some(("project".into(), String::new(), "testing".into()))
        );
    }

    #[test]
    fn test_parse_tree_path_project_nested_key() {
        assert_eq!(
            parse_tree_path("project/agent/model/__value"),
            Some(("project".into(), String::new(), "agent:model".into()))
        );
    }

    #[test]
    fn test_parse_tree_path_commit() {
        assert_eq!(
            parse_tree_path("commit/ab/ab123456/review/status/__value"),
            Some(("commit".into(), "ab123456".into(), "review:status".into()))
        );
    }

    #[test]
    fn test_parse_tree_path_list() {
        assert_eq!(
            parse_tree_path("project/tags/__list/1234-abc12"),
            Some(("project".into(), String::new(), "tags".into()))
        );
    }

    #[test]
    fn test_parse_tree_path_tombstone_ignored() {
        assert_eq!(
            parse_tree_path("project/__tombstones/testing/__deleted"),
            None
        );
    }

    #[test]
    fn test_parse_tree_path_branch() {
        assert_eq!(
            parse_tree_path("branch/f3/main/ci/status/__value"),
            Some(("branch".into(), "main".into(), "ci:status".into()))
        );
    }
}
