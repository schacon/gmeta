use anyhow::{Context, Result};
use chrono::Utc;
use std::collections::{BTreeMap, BTreeSet};

use crate::db::Db;
use crate::git_utils;
use crate::list_value::{make_entry_name, parse_entries};
use crate::types::{build_list_tree_dir_path, build_tombstone_tree_path, build_tree_path, Target};

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

    if metadata_entries.is_empty() && tombstone_entries.is_empty() {
        println!("no metadata to serialize");
        return Ok(());
    }

    // Summarize what we're serializing
    let mut targets: BTreeSet<String> = BTreeSet::new();
    let mut string_count = 0u64;
    let mut list_count = 0u64;
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
            _ => {}
        }
    }
    eprintln!(
        "Serializing {} keys ({} string, {} list) across {} targets, {} tombstones",
        metadata_entries.len(),
        string_count,
        list_count,
        targets.len(),
        tombstone_entries.len(),
    );

    eprintln!("Building git tree...");
    let tree_oid = build_tree(&repo, &metadata_entries, &tombstone_entries)?;

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

    Ok(())
}

/// Build a complete Git tree from all metadata entries.
fn build_tree(
    repo: &git2::Repository,
    metadata_entries: &[(String, String, String, String, String, i64, bool)],
    tombstone_entries: &[(String, String, String, i64, String)],
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
