use anyhow::Result;
use chrono::Utc;
use std::collections::BTreeMap;

use crate::db::Db;
use crate::git_utils;
use crate::types::Target;

type Key = (String, String, String); // (target_type, target_value, key)

/// A parsed metadata entry from a Git tree.
#[derive(Debug, Clone, PartialEq)]
enum TreeValue {
    String(String),
    List(Vec<(String, String)>), // (entry_name, content)
}

pub fn run(remote: Option<&str>) -> Result<()> {
    let repo = git_utils::discover_repo()?;
    let db_path = git_utils::db_path(&repo)?;
    let db = Db::open(&db_path)?;

    let ns = git_utils::get_namespace(&repo)?;
    let local_ref_name = git_utils::local_ref(&repo)?;

    // Find remote refs to materialize
    let remote_refs = find_remote_refs(&repo, &ns, remote)?;

    if remote_refs.is_empty() {
        println!("no remote metadata refs found");
        return Ok(());
    }

    let email = git_utils::get_email(&repo)?;
    let now = Utc::now().timestamp_millis();

    for (ref_name, remote_oid) in &remote_refs {
        let remote_commit = repo.find_commit(*remote_oid)?;
        let remote_tree = remote_commit.tree()?;
        let remote_entries = parse_tree(&repo, &remote_tree, "")?;

        // Get local commit (if any)
        let local_commit = repo
            .find_reference(&local_ref_name)
            .ok()
            .and_then(|r| r.peel_to_commit().ok());

        // Check if we can fast-forward: local is None, or local is an
        // ancestor of remote (no local-only commits to preserve).
        let can_fast_forward = match &local_commit {
            None => true,
            Some(local_c) => {
                if local_c.id() == *remote_oid {
                    // Already up to date
                    println!("{} already up to date", ref_name);
                    continue;
                }
                match repo.merge_base(local_c.id(), *remote_oid) {
                    Ok(base_oid) => base_oid == local_c.id(),
                    Err(_) => false,
                }
            }
        };

        if can_fast_forward {
            // Fast-forward: just update the ref and import into SQLite
            update_db_from_tree(&db, &remote_entries, &email, now)?;

            // Fast-forward the ref
            repo.reference(&local_ref_name, *remote_oid, true, "fast-forward materialize")?;

            println!("materialized {} (fast-forward)", ref_name);
        } else {
            // Need a real merge
            let local_c = local_commit.as_ref().unwrap();
            let local_entries = parse_tree(&repo, &local_c.tree()?, "")?;

            // Find merge base
            let base_entries = match repo.merge_base(local_c.id(), *remote_oid) {
                Ok(base_oid) => {
                    let base_commit = repo.find_commit(base_oid)?;
                    parse_tree(&repo, &base_commit.tree()?, "")?
                }
                Err(_) => BTreeMap::new(),
            };

            // Get commit timestamps for conflict resolution
            let local_timestamp = local_c.time().seconds();
            let remote_timestamp = remote_commit.time().seconds();

            // Three-way merge
            let merged = three_way_merge(
                &base_entries,
                &local_entries,
                &remote_entries,
                local_timestamp,
                remote_timestamp,
            )?;

            // Update SQLite
            update_db_from_tree(&db, &merged, &email, now)?;

            // Handle removals: keys in base but not in merged
            for key in base_entries.keys() {
                if !merged.contains_key(key) {
                    let (target_type, target_value, key_name) = key;
                    db.rm(target_type, target_value, key_name, &email, now)?;
                }
            }

            // Build the merged tree and write a merge commit
            let merged_tree_oid = build_merged_tree(&repo, &merged)?;
            let merged_tree = repo.find_tree(merged_tree_oid)?;
            let sig = git2::Signature::now(&email, &email)?;

            repo.commit(
                Some(&local_ref_name),
                &sig,
                &sig,
                "materialize",
                &merged_tree,
                &[local_c, &remote_commit],
            )?;

            println!("materialized {}", ref_name);
        }
    }

    db.set_last_materialized(now)?;

    Ok(())
}

/// Update the SQLite database from a set of tree entries, only writing values that differ.
fn update_db_from_tree(
    db: &Db,
    entries: &BTreeMap<Key, TreeValue>,
    email: &str,
    now: i64,
) -> Result<()> {
    for ((target_type, target_value, key_name), tree_val) in entries {
        match tree_val {
            TreeValue::String(s) => {
                let json_val = serde_json::to_string(s)?;
                let existing = db.get(target_type, target_value, key_name)?;
                if existing.as_ref().map(|(v, _)| v.as_str()) != Some(&json_val) {
                    db.set(target_type, target_value, key_name, &json_val, "string", email, now)?;
                }
            }
            TreeValue::List(list_entries) => {
                let items: Vec<String> =
                    list_entries.iter().map(|(_, content)| content.clone()).collect();
                let json_val = serde_json::to_string(&items)?;
                let existing = db.get(target_type, target_value, key_name)?;
                if existing.as_ref().map(|(v, _)| v.as_str()) != Some(&json_val) {
                    db.set(target_type, target_value, key_name, &json_val, "list", email, now)?;
                }
            }
        }
    }
    Ok(())
}

/// Three-way merge: base vs local vs remote.
///
/// For each key:
/// - In base, local, remote (unchanged on both sides): keep as-is
/// - In base, changed only on local: take local
/// - In base, changed only on remote: take remote
/// - In base, changed on both sides (true conflict):
///   - Lists: union of entries
///   - Strings: later commit timestamp wins
/// - Not in base, only in local: take local (new local key)
/// - Not in base, only in remote: take remote (new remote key)
/// - Not in base, in both: same conflict rules as above
/// - In base, removed on one side:
///   - If the other side modified it: keep the modified value
///   - If the other side didn't change it: remove it
fn three_way_merge(
    base: &BTreeMap<Key, TreeValue>,
    local: &BTreeMap<Key, TreeValue>,
    remote: &BTreeMap<Key, TreeValue>,
    local_timestamp: i64,
    remote_timestamp: i64,
) -> Result<BTreeMap<Key, TreeValue>> {
    let mut merged = BTreeMap::new();

    // Collect all keys across all three
    let mut all_keys: BTreeMap<&Key, ()> = BTreeMap::new();
    for k in base.keys().chain(local.keys()).chain(remote.keys()) {
        all_keys.insert(k, ());
    }

    for key in all_keys.keys() {
        let in_base = base.get(*key);
        let in_local = local.get(*key);
        let in_remote = remote.get(*key);

        match (in_base, in_local, in_remote) {
            // In all three — check for changes
            (Some(b), Some(l), Some(r)) => {
                let local_changed = l != b;
                let remote_changed = r != b;

                match (local_changed, remote_changed) {
                    (false, false) => {
                        // No changes, keep base
                        merged.insert((*key).clone(), b.clone());
                    }
                    (true, false) => {
                        // Only local changed
                        merged.insert((*key).clone(), l.clone());
                    }
                    (false, true) => {
                        // Only remote changed
                        merged.insert((*key).clone(), r.clone());
                    }
                    (true, true) => {
                        // Both changed — conflict resolution
                        merged.insert(
                            (*key).clone(),
                            resolve_conflict(l, r, local_timestamp, remote_timestamp),
                        );
                    }
                }
            }

            // In base and local, but removed on remote
            (Some(b), Some(l), None) => {
                if l != b {
                    // Local modified it — modified wins over removal
                    merged.insert((*key).clone(), l.clone());
                }
                // else: local didn't change, remote removed — stay removed
            }

            // In base and remote, but removed on local
            (Some(b), None, Some(r)) => {
                if r != b {
                    // Remote modified it — modified wins over removal
                    merged.insert((*key).clone(), r.clone());
                }
                // else: remote didn't change, local removed — stay removed
            }

            // In base only (both sides removed)
            (Some(_), None, None) => {
                // Both removed, gone
            }

            // Not in base, only in local
            (None, Some(l), None) => {
                merged.insert((*key).clone(), l.clone());
            }

            // Not in base, only in remote
            (None, None, Some(r)) => {
                merged.insert((*key).clone(), r.clone());
            }

            // Not in base, in both local and remote (concurrent add)
            (None, Some(l), Some(r)) => {
                merged.insert(
                    (*key).clone(),
                    resolve_conflict(l, r, local_timestamp, remote_timestamp),
                );
            }

            // Not anywhere (shouldn't happen)
            (None, None, None) => {}
        }
    }

    Ok(merged)
}

/// Resolve a conflict where both sides changed the same key.
/// For strings, the later commit timestamp wins.
fn resolve_conflict(
    local: &TreeValue,
    remote: &TreeValue,
    local_timestamp: i64,
    remote_timestamp: i64,
) -> TreeValue {
    match (local, remote) {
        // Both lists: union of entries
        (TreeValue::List(local_list), TreeValue::List(remote_list)) => {
            let mut combined: BTreeMap<String, String> = BTreeMap::new();
            for (name, content) in local_list {
                combined.insert(name.clone(), content.clone());
            }
            for (name, content) in remote_list {
                combined.entry(name.clone()).or_insert_with(|| content.clone());
            }
            TreeValue::List(combined.into_iter().collect())
        }
        // Both strings: later commit timestamp wins (tie goes to local)
        (TreeValue::String(_), TreeValue::String(_)) => {
            if remote_timestamp > local_timestamp {
                remote.clone()
            } else {
                local.clone()
            }
        }
        // Mismatched types: later timestamp wins
        _ => {
            if remote_timestamp > local_timestamp {
                remote.clone()
            } else {
                local.clone()
            }
        }
    }
}

/// Build a Git tree from the merged metadata entries.
fn build_merged_tree(
    repo: &git2::Repository,
    entries: &BTreeMap<Key, TreeValue>,
) -> Result<git2::Oid> {
    let mut files: BTreeMap<String, Vec<u8>> = BTreeMap::new();

    for ((target_type, target_value, key), tree_val) in entries {
        let target = if target_type == "project" {
            Target::parse("project")?
        } else {
            Target::parse(&format!("{}:{}", target_type, target_value))?
        };

        let base_path = target.tree_base_path();
        let key_path = key.replace(':', "/");

        match tree_val {
            TreeValue::String(s) => {
                let full_path = format!("{}/{}", base_path, key_path);
                files.insert(full_path, s.as_bytes().to_vec());
            }
            TreeValue::List(list_entries) => {
                for (entry_name, content) in list_entries {
                    let full_path = format!("{}/{}/{}", base_path, key_path, entry_name);
                    files.insert(full_path, content.as_bytes().to_vec());
                }
            }
        }
    }

    build_tree_from_paths(repo, &files)
}

fn find_remote_refs(
    repo: &git2::Repository,
    ns: &str,
    remote: Option<&str>,
) -> Result<Vec<(String, git2::Oid)>> {
    let mut results = Vec::new();

    let refs = repo.references()?;
    let prefix = match remote {
        Some(r) => format!("refs/{}/{}", ns, r),
        None => format!("refs/{}/", ns),
    };

    for reference in refs {
        let reference = reference?;
        if let Some(name) = reference.name() {
            if name.starts_with(&prefix) && name != format!("refs/{}/local", ns) {
                if let Ok(commit) = reference.peel_to_commit() {
                    results.push((name.to_string(), commit.id()));
                }
            }
        }
    }

    Ok(results)
}

/// Parse a Git tree into metadata entries.
/// Returns a map of (target_type, target_value, key) -> TreeValue
fn parse_tree(
    repo: &git2::Repository,
    tree: &git2::Tree,
    prefix: &str,
) -> Result<BTreeMap<Key, TreeValue>> {
    let mut result = BTreeMap::new();

    // Walk the tree recursively and collect all blob paths
    let mut paths: BTreeMap<String, Vec<u8>> = BTreeMap::new();
    collect_blobs(repo, tree, prefix, &mut paths)?;

    // Group paths by target and key, detecting lists
    for (path, content) in &paths {
        let parts: Vec<&str> = path.split('/').collect();

        if parts.is_empty() {
            continue;
        }

        let (target_type, target_value, key_parts) = match parse_path_parts(&parts) {
            Ok(v) => v,
            Err(_) => continue,
        };

        if key_parts.is_empty() {
            continue;
        }

        // Check if the last blob name looks like a list entry
        let last = *key_parts.last().unwrap();
        if git_utils::is_list_entry_name(last) {
            // This is a list entry
            let key = key_parts[..key_parts.len() - 1].join(":");
            let content_str = String::from_utf8_lossy(content).to_string();
            let entry = result
                .entry((target_type, target_value, key))
                .or_insert_with(|| TreeValue::List(Vec::new()));
            if let TreeValue::List(ref mut list) = entry {
                list.push((last.to_string(), content_str));
            }
        } else {
            // String value
            let key = key_parts.join(":");
            let content_str = String::from_utf8_lossy(content).to_string();
            result.insert(
                (target_type, target_value, key),
                TreeValue::String(content_str),
            );
        }
    }

    // Sort list entries by name (timestamp-hash)
    for value in result.values_mut() {
        if let TreeValue::List(ref mut list) = value {
            list.sort_by(|a, b| a.0.cmp(&b.0));
        }
    }

    Ok(result)
}

fn collect_blobs(
    repo: &git2::Repository,
    tree: &git2::Tree,
    prefix: &str,
    paths: &mut BTreeMap<String, Vec<u8>>,
) -> Result<()> {
    for entry in tree.iter() {
        let name = entry.name().unwrap_or("");
        let full_path = if prefix.is_empty() {
            name.to_string()
        } else {
            format!("{}/{}", prefix, name)
        };

        match entry.kind() {
            Some(git2::ObjectType::Blob) => {
                let blob = repo.find_blob(entry.id())?;
                paths.insert(full_path, blob.content().to_vec());
            }
            Some(git2::ObjectType::Tree) => {
                let subtree = repo.find_tree(entry.id())?;
                collect_blobs(repo, &subtree, &full_path, paths)?;
            }
            _ => {}
        }
    }
    Ok(())
}

/// Parse path segments into (target_type, target_value, key_parts).
fn parse_path_parts<'a>(parts: &'a [&'a str]) -> Result<(String, String, &'a [&'a str])> {
    if parts.is_empty() {
        anyhow::bail!("empty path");
    }

    let target_type = parts[0];

    if target_type == "project" {
        return Ok(("project".to_string(), "".to_string(), &parts[1..]));
    }

    // Sharded format: type/first2/last3/full_value/key_parts...
    if parts.len() < 4 {
        anyhow::bail!("path too short for sharded target: {:?}", parts);
    }

    let target_value = parts[3].to_string();

    Ok((target_type.to_string(), target_value, &parts[4..]))
}

/// Build a nested Git tree structure from flat file paths.
fn build_tree_from_paths(
    repo: &git2::Repository,
    files: &BTreeMap<String, Vec<u8>>,
) -> Result<git2::Oid> {
    #[derive(Default)]
    struct Dir {
        files: BTreeMap<String, Vec<u8>>,
        dirs: BTreeMap<String, Dir>,
    }

    fn insert_path(dir: &mut Dir, parts: &[&str], content: Vec<u8>) {
        if parts.len() == 1 {
            dir.files.insert(parts[0].to_string(), content);
        } else {
            let child = dir.dirs.entry(parts[0].to_string()).or_default();
            insert_path(child, &parts[1..], content);
        }
    }

    fn build_dir(repo: &git2::Repository, dir: &Dir) -> Result<git2::Oid> {
        let mut tb = repo.treebuilder(None)?;

        for (name, content) in &dir.files {
            let blob_oid = repo.blob(content)?;
            tb.insert(name, blob_oid, 0o100644)?;
        }

        for (name, child_dir) in &dir.dirs {
            let child_oid = build_dir(repo, child_dir)?;
            tb.insert(name, child_oid, 0o040000)?;
        }

        Ok(tb.write()?)
    }

    let mut root = Dir::default();

    for (path, content) in files {
        let parts: Vec<&str> = path.split('/').collect();
        insert_path(&mut root, &parts, content.clone());
    }

    build_dir(repo, &root)
}
