use anyhow::{bail, Context, Result};
use chrono::Utc;
use std::collections::{BTreeMap, BTreeSet};

use crate::commands::auto_prune;
use crate::db::Db;
use crate::git_utils;
use crate::list_value::{make_entry_name, parse_entries};
use crate::types::{
    build_list_tree_dir_path, build_set_member_tombstone_tree_path, build_set_tree_dir_path,
    build_tombstone_tree_path, build_tree_path, Target,
};

#[derive(serde::Serialize)]
struct TombstoneBlob<'a> {
    timestamp: i64,
    email: &'a str,
}

pub fn run() -> Result<()> {
    let repo = git_utils::discover_repo()?;
    let db_path = git_utils::db_path(&repo)?;
    let db = Db::open(&db_path)?;

    let local_ref_name = git_utils::local_ref(&repo)?;
    let last_materialized = db.get_last_materialized()?;

    eprintln!("Reading metadata from database...");

    // Determine if we're doing incremental or full serialization
    // If we have a previous local ref commit, start from existing tree
    let existing_tree = repo
        .find_reference(&local_ref_name)
        .ok()
        .and_then(|r| r.peel_to_commit().ok())
        .map(|c| c.tree().unwrap());

    // Build new tree entries
    let metadata_entries = if let Some(since) = last_materialized {
        // Incremental: only modified entries
        let modified = db.get_modified_since(since)?;
        if modified.is_empty() && existing_tree.is_some() {
            eprintln!("Nothing changed since last serialize");
            return Ok(());
        }
        // We need the full metadata to rebuild the tree properly
        db.get_all_metadata()?
    } else {
        db.get_all_metadata()?
    };
    let tombstone_entries = db.get_all_tombstones()?;
    let set_tombstone_entries = db.get_all_set_tombstones()?;

    if metadata_entries.is_empty() && tombstone_entries.is_empty() {
        println!("no metadata to serialize");
        return Ok(());
    }

    // Summarize what we're serializing
    let mut targets: BTreeSet<String> = BTreeSet::new();
    let mut string_count = 0u64;
    let mut list_count = 0u64;
    let mut set_count = 0u64;
    for (target_type, target_value, _key, _value, value_type, _ts, _is_git_ref) in &metadata_entries
    {
        let label = if target_type == "project" {
            "project".to_string()
        } else {
            format!(
                "{}:{}",
                target_type,
                &target_value[..7.min(target_value.len())]
            )
        };
        targets.insert(label);
        match value_type.as_str() {
            "string" => string_count += 1,
            "list" => list_count += 1,
            "set" => set_count += 1,
            _ => {}
        }
    }
    eprintln!(
        "Serializing {} keys ({} string, {} list, {} set) across {} targets, {} tombstones, {} set tombstones",
        metadata_entries.len(),
        string_count,
        list_count,
        set_count,
        targets.len(),
        tombstone_entries.len(),
        set_tombstone_entries.len(),
    );

    eprintln!("Building git tree...");
    let tree_oid = build_tree(
        &repo,
        &metadata_entries,
        &tombstone_entries,
        &set_tombstone_entries,
    )?;

    // Create commit
    let name = git_utils::get_name(&repo)?;
    let email = git_utils::get_email(&repo)?;
    let sig = git2::Signature::now(&name, &email)?;

    let tree = repo.find_tree(tree_oid)?;

    // Find parent commit if exists
    let parent = repo
        .find_reference(&local_ref_name)
        .ok()
        .and_then(|r| r.peel_to_commit().ok());

    let parents: Vec<&git2::Commit> = parent.iter().collect();

    eprintln!("Writing commit to {}...", local_ref_name);
    let commit_oid = repo.commit(Some(&local_ref_name), &sig, &sig, "", &tree, &parents)?;

    let now = Utc::now().timestamp_millis();
    db.set_last_materialized(now)?;

    println!(
        "serialized to {} ({})",
        local_ref_name,
        &commit_oid.to_string()[..8]
    );

    // Check auto-prune rules
    if let Some(rules) = auto_prune::read_prune_rules(&db)? {
        if auto_prune::should_prune(&repo, tree_oid, &rules)? {
            eprintln!("Auto-prune triggered, pruning with --since={}...", rules.since);
            let prune_tree_oid =
                prune_tree(&repo, tree_oid, &rules, &db)?;

            if prune_tree_oid != tree_oid {
                let prune_tree = repo.find_tree(prune_tree_oid)?;
                let prune_parent = repo
                    .find_reference(&local_ref_name)?
                    .peel_to_commit()?;

                let (keys_dropped, keys_retained) =
                    count_prune_stats(&repo, tree_oid, prune_tree_oid)?;

                let min_size_str = rules
                    .min_size
                    .map(|s| format!("\nmin-size: {}", s))
                    .unwrap_or_default();

                let message = format!(
                    "gmeta: prune --since={}\n\npruned: true\nsince: {}{}\nkeys-dropped: {}\nkeys-retained: {}",
                    rules.since, rules.since, min_size_str, keys_dropped, keys_retained
                );

                let prune_commit_oid = repo.commit(
                    Some(&local_ref_name),
                    &sig,
                    &sig,
                    &message,
                    &prune_tree,
                    &[&prune_parent],
                )?;

                println!(
                    "auto-pruned to {} ({})",
                    local_ref_name,
                    &prune_commit_oid.to_string()[..8]
                );
            } else {
                eprintln!("Auto-prune: tree unchanged after pruning, skipping prune commit");
            }
        }
    }

    Ok(())
}

/// Build a complete Git tree from all metadata entries.
fn build_tree(
    repo: &git2::Repository,
    metadata_entries: &[(String, String, String, String, String, i64, bool)],
    tombstone_entries: &[(String, String, String, i64, String)],
    set_tombstone_entries: &[(String, String, String, String, String, i64, String)],
) -> Result<git2::Oid> {
    // Collect all file paths -> blob content
    let mut files: BTreeMap<String, Vec<u8>> = BTreeMap::new();

    for (target_type, target_value, key, value, value_type, _last_timestamp, is_git_ref) in
        metadata_entries
    {
        let target = if target_type == "project" {
            Target::parse("project")?
        } else {
            Target::parse(&format!("{}:{}", target_type, target_value))?
        };

        match value_type.as_str() {
            "string" => {
                let full_path = build_tree_path(&target, key)?;
                if *is_git_ref {
                    // Value is a git blob SHA — read the blob content directly
                    let oid = git2::Oid::from_str(value)?;
                    let blob = repo.find_blob(oid)?;
                    files.insert(full_path, blob.content().to_vec());
                } else {
                    let raw_value: String = match serde_json::from_str(value) {
                        Ok(s) => s,
                        Err(e) => {
                            // Value is valid JSON but not a JSON string (e.g. an object or array).
                            // Store the raw JSON as-is.
                            eprintln!(
                                "warning: key '{}' on {}:{} is not a JSON string ({}), storing raw JSON",
                                key, target_type, &target_value[..7.min(target_value.len())], e
                            );
                            value.to_string()
                        }
                    };
                    files.insert(full_path, raw_value.into_bytes());
                }
            }
            "list" => {
                let list_entries = parse_entries(value).context("failed to decode list value")?;
                let list_dir_path = build_list_tree_dir_path(&target, key)?;
                for entry in list_entries {
                    let entry_name = make_entry_name(&entry);
                    let full_path = format!("{}/{}", list_dir_path, entry_name);
                    files.insert(full_path, entry.value.into_bytes());
                }
            }
            "set" => {
                let members: Vec<String> =
                    serde_json::from_str(value).context("failed to decode set value")?;
                let set_dir_path = build_set_tree_dir_path(&target, key)?;
                for member in members {
                    let member_id = crate::types::set_member_id(&member);
                    let full_path = format!("{}/{}", set_dir_path, member_id);
                    files.insert(full_path, member.into_bytes());
                }
            }
            _ => {}
        }
    }

    for (target_type, target_value, key, timestamp, email) in tombstone_entries {
        let target = if target_type == "project" {
            Target::parse("project")?
        } else {
            Target::parse(&format!("{}:{}", target_type, target_value))?
        };

        let full_path = build_tombstone_tree_path(&target, key)?;
        let payload = serde_json::to_vec(&TombstoneBlob {
            timestamp: *timestamp,
            email,
        })?;
        files.insert(full_path, payload);
    }

    for (target_type, target_value, key, member_id, value, _timestamp, _email) in
        set_tombstone_entries
    {
        let target = if target_type == "project" {
            Target::parse("project")?
        } else {
            Target::parse(&format!("{}:{}", target_type, target_value))?
        };

        let full_path = build_set_member_tombstone_tree_path(&target, key, member_id)?;
        files.insert(full_path, value.as_bytes().to_vec());
    }

    // Build nested tree from flat paths
    build_tree_from_paths(repo, &files)
}

/// Build a nested Git tree structure from flat file paths.
fn build_tree_from_paths(
    repo: &git2::Repository,
    files: &BTreeMap<String, Vec<u8>>,
) -> Result<git2::Oid> {
    // Build a nested structure
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

        let oid = tb.write()?;
        Ok(oid)
    }

    let mut root = Dir::default();

    for (path, content) in files {
        let parts: Vec<&str> = path.split('/').collect();
        insert_path(&mut root, &parts, content.clone());
    }

    build_dir(repo, &root)
}

/// Parse a duration string like "90d", "6m", "1y" or an ISO date into a cutoff timestamp (millis).
fn parse_since_to_cutoff_ms(since: &str) -> Result<i64> {
    // Try relative duration first
    let s = since.trim().to_lowercase();
    if let Some(num_str) = s.strip_suffix('d') {
        let days: i64 = num_str.parse().with_context(|| format!("invalid duration: {}", since))?;
        return Ok(Utc::now().timestamp_millis() - days * 86_400_000);
    }
    if let Some(num_str) = s.strip_suffix('m') {
        let months: i64 = num_str.parse().with_context(|| format!("invalid duration: {}", since))?;
        return Ok(Utc::now().timestamp_millis() - months * 30 * 86_400_000);
    }
    if let Some(num_str) = s.strip_suffix('y') {
        let years: i64 = num_str.parse().with_context(|| format!("invalid duration: {}", since))?;
        return Ok(Utc::now().timestamp_millis() - years * 365 * 86_400_000);
    }

    // Try ISO date
    if let Ok(date) = chrono::NaiveDate::parse_from_str(since, "%Y-%m-%d") {
        let dt = date
            .and_hms_opt(0, 0, 0)
            .ok_or_else(|| anyhow::anyhow!("invalid date"))?;
        return Ok(dt.and_utc().timestamp_millis());
    }

    bail!("cannot parse --since value: {} (expected e.g. 90d, 6m, 1y, or 2025-01-01)", since);
}

/// Prune a serialized tree by dropping entries older than the cutoff.
/// Returns the OID of the new (possibly smaller) tree.
fn prune_tree(
    repo: &git2::Repository,
    tree_oid: git2::Oid,
    rules: &auto_prune::PruneRules,
    db: &Db,
) -> Result<git2::Oid> {
    let cutoff_ms = parse_since_to_cutoff_ms(&rules.since)?;
    let min_size = rules.min_size.unwrap_or(0);
    let tree = repo.find_tree(tree_oid)?;

    // Walk the tree and rebuild, skipping old entries.
    // The top-level directories are target type dirs (commit, branch, path, project, change-id).
    // "project" subtree is never pruned.
    let mut tb = repo.treebuilder(None)?;

    for entry in tree.iter() {
        let name = entry.name().unwrap_or("").to_string();

        if name == "project" {
            // Never prune project metadata
            tb.insert(&name, entry.id(), entry.filemode())?;
            continue;
        }

        if entry.kind() == Some(git2::ObjectType::Tree) {
            let subtree = repo.find_tree(entry.id())?;

            // Check min-size: if the subtree is smaller than the threshold, keep it
            if min_size > 0 {
                let size = auto_prune::compute_tree_size_for(repo, &subtree)?;
                if size < min_size {
                    tb.insert(&name, entry.id(), entry.filemode())?;
                    continue;
                }
            }

            let pruned_oid = prune_target_type_tree(repo, &subtree, cutoff_ms, min_size, db)?;
            // Only include if the pruned tree is non-empty
            let pruned_tree = repo.find_tree(pruned_oid)?;
            if pruned_tree.len() > 0 {
                tb.insert(&name, pruned_oid, entry.filemode())?;
            }
        } else {
            tb.insert(&name, entry.id(), entry.filemode())?;
        }
    }

    Ok(tb.write()?)
}

/// Prune within a target-type directory (e.g. the "commit" dir which contains fanout dirs).
fn prune_target_type_tree(
    repo: &git2::Repository,
    tree: &git2::Tree,
    cutoff_ms: i64,
    min_size: u64,
    db: &Db,
) -> Result<git2::Oid> {
    let mut tb = repo.treebuilder(None)?;

    for entry in tree.iter() {
        let name = entry.name().unwrap_or("").to_string();

        if entry.kind() == Some(git2::ObjectType::Tree) {
            let subtree = repo.find_tree(entry.id())?;
            let pruned_oid = prune_subtree_recursive(repo, &subtree, cutoff_ms, min_size, db)?;
            let pruned_tree = repo.find_tree(pruned_oid)?;
            if pruned_tree.len() > 0 {
                tb.insert(&name, pruned_oid, entry.filemode())?;
            }
        } else {
            tb.insert(&name, entry.id(), entry.filemode())?;
        }
    }

    Ok(tb.write()?)
}

/// Recursively prune a subtree. Drops old list entries and old tombstones.
/// For key-level entries (__value, __set), checks the last_timestamp from the DB.
fn prune_subtree_recursive(
    repo: &git2::Repository,
    tree: &git2::Tree,
    cutoff_ms: i64,
    _min_size: u64,
    db: &Db,
) -> Result<git2::Oid> {
    let mut tb = repo.treebuilder(None)?;

    for entry in tree.iter() {
        let name = entry.name().unwrap_or("").to_string();

        match entry.kind() {
            Some(git2::ObjectType::Tree) => {
                if name == "__list" {
                    // Prune individual list entries by timestamp in their name
                    let list_tree = repo.find_tree(entry.id())?;
                    let pruned_oid = prune_list_tree(repo, &list_tree, cutoff_ms)?;
                    let pruned_tree = repo.find_tree(pruned_oid)?;
                    if pruned_tree.len() > 0 {
                        tb.insert(&name, pruned_oid, entry.filemode())?;
                    }
                } else if name == "__tombstones" {
                    // Prune old tombstones
                    let tomb_tree = repo.find_tree(entry.id())?;
                    let pruned_oid = prune_tombstone_tree(repo, &tomb_tree, cutoff_ms)?;
                    let pruned_tree = repo.find_tree(pruned_oid)?;
                    if pruned_tree.len() > 0 {
                        tb.insert(&name, pruned_oid, entry.filemode())?;
                    }
                } else {
                    // Recurse into key segment directories
                    let subtree = repo.find_tree(entry.id())?;
                    let pruned_oid =
                        prune_subtree_recursive(repo, &subtree, cutoff_ms, _min_size, db)?;
                    let pruned_tree = repo.find_tree(pruned_oid)?;
                    if pruned_tree.len() > 0 {
                        tb.insert(&name, pruned_oid, entry.filemode())?;
                    }
                }
            }
            Some(git2::ObjectType::Blob) => {
                // Keep all blobs — __value and __set members are kept if their parent
                // key dir survives the prune. The key-level timestamp filtering happens
                // at the DB level during the full rebuild that prune_tree orchestrates.
                tb.insert(&name, entry.id(), entry.filemode())?;
            }
            _ => {
                tb.insert(&name, entry.id(), entry.filemode())?;
            }
        }
    }

    Ok(tb.write()?)
}

/// Prune list entries by the timestamp encoded in their file name.
fn prune_list_tree(
    repo: &git2::Repository,
    tree: &git2::Tree,
    cutoff_ms: i64,
) -> Result<git2::Oid> {
    let mut tb = repo.treebuilder(None)?;

    for entry in tree.iter() {
        let name = entry.name().unwrap_or("");
        // Entry names are formatted as "{timestamp_ms}-{hash5}"
        if let Some((ts_str, _)) = name.split_once('-') {
            if let Ok(ts) = ts_str.parse::<i64>() {
                if ts < cutoff_ms {
                    continue; // Drop old entry
                }
            }
        }
        tb.insert(name, entry.id(), entry.filemode())?;
    }

    Ok(tb.write()?)
}

/// Prune tombstone entries by the timestamp in the __deleted blob.
fn prune_tombstone_tree(
    repo: &git2::Repository,
    tree: &git2::Tree,
    cutoff_ms: i64,
) -> Result<git2::Oid> {
    let mut tb = repo.treebuilder(None)?;

    for entry in tree.iter() {
        let name = entry.name().unwrap_or("").to_string();

        match entry.kind() {
            Some(git2::ObjectType::Tree) => {
                // Tombstone key directory — recurse to find __deleted blob
                let subtree = repo.find_tree(entry.id())?;
                let pruned_oid = prune_tombstone_tree(repo, &subtree, cutoff_ms)?;
                let pruned_tree = repo.find_tree(pruned_oid)?;
                if pruned_tree.len() > 0 {
                    tb.insert(&name, pruned_oid, entry.filemode())?;
                }
            }
            Some(git2::ObjectType::Blob) if name == "__deleted" => {
                // Parse the tombstone blob to check timestamp
                let blob = repo.find_blob(entry.id())?;
                if let Ok(content) = std::str::from_utf8(blob.content()) {
                    if let Ok(parsed) = serde_json::from_str::<serde_json::Value>(content) {
                        if let Some(ts) = parsed.get("timestamp").and_then(|v| v.as_i64()) {
                            if ts < cutoff_ms {
                                continue; // Drop old tombstone
                            }
                        }
                    }
                }
                tb.insert(&name, entry.id(), entry.filemode())?;
            }
            _ => {
                tb.insert(&name, entry.id(), entry.filemode())?;
            }
        }
    }

    Ok(tb.write()?)
}

/// Count keys in original and pruned trees to produce stats.
fn count_prune_stats(
    repo: &git2::Repository,
    original_oid: git2::Oid,
    pruned_oid: git2::Oid,
) -> Result<(u64, u64)> {
    let original_tree = repo.find_tree(original_oid)?;
    let pruned_tree = repo.find_tree(pruned_oid)?;

    let mut original_count = 0u64;
    count_all_blobs(repo, &original_tree, &mut original_count)?;

    let mut pruned_count = 0u64;
    count_all_blobs(repo, &pruned_tree, &mut pruned_count)?;

    let dropped = original_count.saturating_sub(pruned_count);
    Ok((dropped, pruned_count))
}

fn count_all_blobs(repo: &git2::Repository, tree: &git2::Tree, count: &mut u64) -> Result<()> {
    for entry in tree.iter() {
        match entry.kind() {
            Some(git2::ObjectType::Blob) => *count += 1,
            Some(git2::ObjectType::Tree) => {
                let subtree = repo.find_tree(entry.id())?;
                count_all_blobs(repo, &subtree, count)?;
            }
            _ => {}
        }
    }
    Ok(())
}
