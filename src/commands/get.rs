use anyhow::{Context, Result};
use git2::Repository;
use serde_json::{json, Map, Value};

use crate::db::Db;
use crate::git_utils;
use crate::list_value::list_values_from_json;
use crate::types::{self, Target, TargetType};

const NODE_VALUE_KEY: &str = "__value";

pub fn run(
    target_str: &str,
    key: Option<&str>,
    json_output: bool,
    with_authorship: bool,
) -> Result<()> {
    let mut target = Target::parse(target_str)?;

    let repo = git_utils::discover_repo()?;
    target.resolve(&repo)?;
    let db_path = git_utils::db_path(&repo)?;
    let db = Db::open(&db_path)?;

    let include_target_subtree = target.target_type == TargetType::Path;
    let mut entries = db.get_all_with_target_prefix(
        target.type_str(),
        target.value_str(),
        include_target_subtree,
        key,
    )?;

    // If no exact match, try prefix expansion for non-commit types
    // (commits are already resolved by git, but change-ids/branches may be partial)
    if entries.is_empty() && target.target_type != TargetType::Path {
        let matches = db.find_target_values_by_prefix(target.type_str(), target.value_str(), 2)?;
        if matches.len() == 1 {
            let expanded = &matches[0];
            entries = db.get_all_with_target_prefix(target.type_str(), expanded, false, key)?;
            if !entries.is_empty() {
                eprintln!("expanded to {}:{}", target.type_str(), expanded);
            }
        } else if matches.len() > 1 {
            eprintln!("ambiguous prefix '{}', matches:", target.value_str());
            for m in &matches {
                eprintln!("  {}:{}", target.type_str(), m);
            }
            return Ok(());
        }
    }

    if entries.is_empty() {
        return Ok(());
    }

    // Hydrate any promised entries on demand
    let promised: Vec<_> = entries
        .iter()
        .filter(|(_, _, _, _, _, is_promised)| *is_promised)
        .map(|(tv, k, _, _, _, _)| (tv.clone(), k.clone()))
        .collect();

    if !promised.is_empty() {
        let hydrated = hydrate_promised_entries(&repo, &db, target.type_str(), &promised)?;
        if hydrated > 0 {
            // Re-query to get the now-resolved values
            entries = db.get_all_with_target_prefix(
                target.type_str(),
                target.value_str(),
                include_target_subtree,
                key,
            )?;
        }
    }

    // Resolve git refs to actual values, skip any remaining promised entries
    let resolved: Vec<(String, String, String, String)> = entries
        .into_iter()
        .filter(|(_, _, _, _, _, is_promised)| !is_promised)
        .map(
            |(entry_target_value, key, value, value_type, is_git_ref, _)| {
                if is_git_ref {
                    let resolved_value = resolve_git_ref(&repo, &value)?;
                    // JSON-encode the resolved content to match normal string format
                    let json_value = serde_json::to_string(&resolved_value)?;
                    Ok((entry_target_value, key, json_value, value_type))
                } else {
                    Ok((entry_target_value, key, value, value_type))
                }
            },
        )
        .collect::<Result<Vec<_>>>()?;

    if resolved.is_empty() {
        return Ok(());
    }

    if json_output {
        print_json(&db, &target, &resolved, with_authorship)?;
    } else {
        print_plain(&target, &resolved, key.is_some() && resolved.len() == 1)?;
    }

    Ok(())
}

/// Hydrate promised entries by looking up their blob OIDs in the tip tree
/// and fetching any that aren't already local. Returns the number hydrated.
fn hydrate_promised_entries(
    repo: &Repository,
    db: &Db,
    target_type: &str,
    entries: &[(String, String)], // (target_value, key)
) -> Result<usize> {
    let ns = git_utils::get_namespace(repo)?;
    let tracking_ref = format!("refs/{}/remotes/main", ns);

    let tip_commit = match repo.find_reference(&tracking_ref) {
        Ok(r) => r.peel_to_commit()?,
        Err(_) => return Ok(0),
    };
    let tip_tree = tip_commit.tree()?;

    // For each promised entry, find its blob OID(s) in the tip tree
    struct PendingEntry {
        idx: usize,
        oids: Vec<git2::Oid>,
        value_type: String, // "string", "list", or "set"
    }

    let mut pending: Vec<PendingEntry> = Vec::new();
    let mut not_found: Vec<usize> = Vec::new();

    for (idx, (target_value, key)) in entries.iter().enumerate() {
        let target_str = if target_type == "project" {
            "project".to_string()
        } else {
            format!("{}:{}", target_type, target_value)
        };
        let parsed_target = match Target::parse(&target_str) {
            Ok(t) => t,
            Err(_) => continue,
        };

        // Try __value (string) first
        if let Ok(path) = types::build_tree_path(&parsed_target, key) {
            if let Some(oid) = git_utils::find_blob_oid_in_tree(repo, &tip_tree, &path)? {
                pending.push(PendingEntry {
                    idx,
                    oids: vec![oid],
                    value_type: "string".to_string(),
                });
                continue;
            }
        }

        // Try __list directory
        if let Ok(path) = types::build_list_tree_dir_path(&parsed_target, key) {
            if let Some(dir_oid) = git_utils::find_blob_oid_in_tree(repo, &tip_tree, &path)? {
                // dir_oid is a tree — collect all blob entries in it
                if let Ok(list_tree) = repo.find_tree(dir_oid) {
                    let mut oids = Vec::new();
                    for entry in list_tree.iter() {
                        let name = entry.name().unwrap_or("");
                        if name == types::TOMBSTONE_ROOT || name.starts_with("__") {
                            continue;
                        }
                        if entry.kind() == Some(git2::ObjectType::Blob) {
                            oids.push(entry.id());
                        }
                    }
                    if !oids.is_empty() {
                        pending.push(PendingEntry {
                            idx,
                            oids,
                            value_type: "list".to_string(),
                        });
                        continue;
                    }
                }
            }
        }

        // Try __set directory
        if let Ok(key_path) = types::build_key_tree_path(&parsed_target, key) {
            let set_path = format!("{}/{}", key_path, types::SET_VALUE_DIR);
            if let Some(dir_oid) = git_utils::find_blob_oid_in_tree(repo, &tip_tree, &set_path)? {
                if let Ok(set_tree) = repo.find_tree(dir_oid) {
                    let mut oids = Vec::new();
                    for entry in set_tree.iter() {
                        let name = entry.name().unwrap_or("");
                        if name == types::TOMBSTONE_ROOT || name.starts_with("__") {
                            continue;
                        }
                        if entry.kind() == Some(git2::ObjectType::Blob) {
                            oids.push(entry.id());
                        }
                    }
                    if !oids.is_empty() {
                        pending.push(PendingEntry {
                            idx,
                            oids,
                            value_type: "set".to_string(),
                        });
                        continue;
                    }
                }
            }
        }

        not_found.push(idx);
    }

    // Clean up entries that no longer exist in the tip
    for idx in &not_found {
        let (target_value, key) = &entries[*idx];
        db.delete_promised(target_type, target_value, key)?;
    }

    if pending.is_empty() {
        return Ok(0);
    }

    // Collect all OIDs, try to read locally first, fetch missing ones
    let all_oids: Vec<git2::Oid> = pending
        .iter()
        .flat_map(|p| p.oids.iter().copied())
        .collect();
    let mut missing: Vec<git2::Oid> = Vec::new();
    for oid in &all_oids {
        if repo.find_blob(*oid).is_err() {
            missing.push(*oid);
        }
    }

    if !missing.is_empty() {
        let remote_name = git_utils::resolve_meta_remote(repo, None)?;
        eprintln!(
            "Fetching {} blob{} from remote...",
            missing.len(),
            if missing.len() == 1 { "" } else { "s" }
        );
        git_utils::fetch_blob_oids(repo, &remote_name, &missing)?;
    }

    // Now read blobs and update DB
    let mut hydrated = 0;
    for entry in &pending {
        let (target_value, key) = &entries[entry.idx];

        match entry.value_type.as_str() {
            "string" => {
                let oid = entry.oids[0];
                let blob = match repo.find_blob(oid) {
                    Ok(b) => b,
                    Err(_) => continue,
                };
                let content = match std::str::from_utf8(blob.content()) {
                    Ok(s) => s,
                    Err(_) => continue,
                };
                let json_value = serde_json::to_string(content)?;
                db.resolve_promised(target_type, target_value, key, &json_value, "string", false)?;
                hydrated += 1;
            }
            "list" => {
                // Read all list entry blobs, build JSON array
                let mut list_entries = Vec::new();
                for oid in &entry.oids {
                    if let Ok(blob) = repo.find_blob(*oid) {
                        if let Ok(s) = std::str::from_utf8(blob.content()) {
                            list_entries.push(s.to_string());
                        }
                    }
                }
                let json_value = serde_json::to_string(&list_entries)?;
                db.resolve_promised(target_type, target_value, key, &json_value, "list", false)?;
                hydrated += 1;
            }
            "set" => {
                // Read all set member blobs, build JSON array
                let mut set_members = Vec::new();
                for oid in &entry.oids {
                    if let Ok(blob) = repo.find_blob(*oid) {
                        if let Ok(s) = std::str::from_utf8(blob.content()) {
                            set_members.push(s.to_string());
                        }
                    }
                }
                set_members.sort();
                let json_value = serde_json::to_string(&set_members)?;
                db.resolve_promised(target_type, target_value, key, &json_value, "set", false)?;
                hydrated += 1;
            }
            _ => {}
        }
    }

    Ok(hydrated)
}

/// Resolve a git blob SHA to its content as a UTF-8 string.
fn resolve_git_ref(repo: &Repository, sha: &str) -> Result<String> {
    let oid = git2::Oid::from_str(sha).with_context(|| format!("invalid git blob SHA: {}", sha))?;
    let blob = repo
        .find_blob(oid)
        .with_context(|| format!("git blob not found: {}", sha))?;
    let content = std::str::from_utf8(blob.content())
        .with_context(|| format!("git blob {} is not valid UTF-8", sha))?;
    Ok(content.to_string())
}

fn truncate_str(s: &str, max: usize) -> String {
    if s.chars().count() <= max {
        s.to_string()
    } else {
        let truncated: String = s.chars().take(max - 3).collect();
        format!("{}...", truncated)
    }
}

fn print_plain(
    target: &Target,
    entries: &[(String, String, String, String)],
    value_only: bool,
) -> Result<()> {
    if value_only {
        for (_, _, value, value_type) in entries {
            print_value_only(value, value_type)?;
        }
        return Ok(());
    }

    // Compute max label width for aligned output
    let labels: Vec<String> = entries
        .iter()
        .map(|(tv, k, _, _)| {
            if target.target_type == TargetType::Path {
                format!("{};{}", tv, k)
            } else {
                k.clone()
            }
        })
        .collect();
    let max_width = labels.iter().map(|l| l.len()).max().unwrap_or(0);

    for (label, (_, _, value, value_type)) in labels.iter().zip(entries.iter()) {
        let display_value = format_value_compact(value, value_type)?;
        let truncated = truncate_str(&display_value, 50);
        println!("{:width$}  {}", label, truncated, width = max_width);
    }

    Ok(())
}

/// Single-key mode: raw string value, or one line per list/set item.
fn print_value_only(value: &str, value_type: &str) -> Result<()> {
    match value_type {
        "string" => {
            let s: String = serde_json::from_str(value)?;
            println!("{}", s);
        }
        "list" => {
            for item in list_values_from_json(value)? {
                println!("{}", item);
            }
        }
        "set" => {
            let mut set: Vec<String> = serde_json::from_str(value)?;
            set.sort();
            for item in set {
                println!("{}", item);
            }
        }
        _ => println!("{}", value),
    }
    Ok(())
}

/// Multi-key mode: compact one-line representation.
fn format_value_compact(value: &str, value_type: &str) -> Result<String> {
    match value_type {
        "string" => {
            let s: String = serde_json::from_str(value)?;
            Ok(s)
        }
        "list" => {
            let list = list_values_from_json(value)?;
            Ok(list.join(", "))
        }
        "set" => {
            let mut set: Vec<String> = serde_json::from_str(value)?;
            set.sort();
            Ok(set.join(", "))
        }
        _ => Ok(value.to_string()),
    }
}

fn print_json(
    db: &Db,
    target: &Target,
    entries: &[(String, String, String, String)],
    with_authorship: bool,
) -> Result<()> {
    let mut root = Map::new();

    for (entry_target_value, key, value, value_type) in entries {
        let parsed_value = parse_stored_value(value, value_type)?;

        let leaf_value = if with_authorship {
            let authorship = db.get_authorship(target.type_str(), entry_target_value, key)?;
            let (author, timestamp) = authorship.unwrap_or_else(|| ("unknown".to_string(), 0));
            json!({
                "value": parsed_value,
                "author": author,
                "timestamp": timestamp
            })
        } else {
            parsed_value
        };

        if target.target_type == TargetType::Path {
            insert_nested(
                &mut root,
                &[entry_target_value.as_str(), key.as_str()],
                leaf_value,
            );
        } else {
            let parts: Vec<&str> = key.split(':').collect();
            insert_nested(&mut root, &parts, leaf_value);
        }
    }

    let output = serde_json::to_string_pretty(&Value::Object(root))?;
    println!("{}", output);
    Ok(())
}

fn parse_stored_value(value: &str, value_type: &str) -> Result<Value> {
    match value_type {
        "string" => {
            let s: String = serde_json::from_str(value)?;
            Ok(Value::String(s))
        }
        "list" => {
            let list = list_values_from_json(value)?;
            Ok(Value::Array(list.into_iter().map(Value::String).collect()))
        }
        "set" => {
            let mut set: Vec<String> = serde_json::from_str(value)?;
            set.sort();
            Ok(Value::Array(set.into_iter().map(Value::String).collect()))
        }
        _ => Ok(serde_json::from_str(value)?),
    }
}

fn insert_nested(map: &mut Map<String, Value>, keys: &[&str], value: Value) {
    if keys.len() == 1 {
        let key = keys[0].to_string();
        match map.get_mut(&key) {
            None => {
                map.insert(key, value);
            }
            Some(existing) => {
                if let Value::Object(obj) = existing {
                    obj.insert(NODE_VALUE_KEY.to_string(), value);
                } else {
                    *existing = value;
                }
            }
        }
        return;
    }

    let entry = map
        .entry(keys[0].to_string())
        .or_insert_with(|| Value::Object(Map::new()));

    if !entry.is_object() {
        let previous = std::mem::replace(entry, Value::Null);
        let mut promoted = Map::new();
        promoted.insert(NODE_VALUE_KEY.to_string(), previous);
        *entry = Value::Object(promoted);
    }

    if let Value::Object(child_map) = entry {
        insert_nested(child_map, &keys[1..], value);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_insert_nested_keeps_leaf_and_nested_values() {
        let mut root = Map::new();
        insert_nested(&mut root, &["agent"], json!("anthropic"));
        insert_nested(&mut root, &["agent", "model"], json!("claude-4.6"));

        assert_eq!(
            Value::Object(root),
            json!({
                "agent": {
                    "__value": "anthropic",
                    "model": "claude-4.6"
                }
            })
        );
    }

    #[test]
    fn test_insert_nested_keeps_leaf_and_nested_values_reverse_order() {
        let mut root = Map::new();
        insert_nested(&mut root, &["agent", "model"], json!("claude-4.6"));
        insert_nested(&mut root, &["agent"], json!("anthropic"));

        assert_eq!(
            Value::Object(root),
            json!({
                "agent": {
                    "__value": "anthropic",
                    "model": "claude-4.6"
                }
            })
        );
    }
}
