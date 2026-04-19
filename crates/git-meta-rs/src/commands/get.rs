use anyhow::{Context, Result};
use gix::bstr::ByteSlice;
use gix::prelude::ObjectIdExt;
use serde_json::{json, Map, Value};

use crate::context::CommandContext;
use git_meta_lib::db::Store;
use git_meta_lib::tree_paths;
use git_meta_lib::types::{Target, TargetType, ValueType};

const NODE_VALUE_KEY: &str = "__value";
const SET_VALUE_DIR: &str = "__set";
const TOMBSTONE_ROOT: &str = "__tombstones";

pub fn run(
    target_str: &str,
    key: Option<&str>,
    json_output: bool,
    with_authorship: bool,
) -> Result<()> {
    let ctx = CommandContext::open(None)?;
    let target = ctx.session.resolve_target(&Target::parse(target_str)?)?;
    let repo = ctx.session.repo();

    let include_target_subtree = *target.target_type() == TargetType::Path;
    let mut entries =
        ctx.session
            .store()
            .get_all_with_target_prefix(&target, include_target_subtree, key)?;

    // If no exact match, try prefix expansion for non-commit types
    // (commits are already resolved by git, but change-ids/branches may be partial)
    if entries.is_empty() && *target.target_type() != TargetType::Path {
        let matches = ctx.session.store().find_target_values_by_prefix(
            target.target_type(),
            target.value().unwrap_or(""),
            2,
        )?;
        if matches.len() == 1 {
            let expanded = &matches[0];
            let expanded_target =
                Target::from_parts(target.target_type().clone(), Some(expanded.clone()));
            entries =
                ctx.session
                    .store()
                    .get_all_with_target_prefix(&expanded_target, false, key)?;
            if !entries.is_empty() {
                eprintln!("expanded to {}:{}", target.target_type().as_str(), expanded);
            }
        } else if matches.len() > 1 {
            eprintln!(
                "ambiguous prefix '{}', matches:",
                target.value().unwrap_or("")
            );
            for m in &matches {
                eprintln!("  {}:{}", target.target_type().as_str(), m);
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
        .filter(|r| r.is_promised)
        .map(|r| (r.target_value.clone(), r.key.clone()))
        .collect();

    if !promised.is_empty() {
        let hydrated = hydrate_promised_entries(&ctx.session, target.target_type(), &promised)?;
        if hydrated > 0 {
            // Re-query to get the now-resolved values
            entries = ctx.session.store().get_all_with_target_prefix(
                &target,
                include_target_subtree,
                key,
            )?;
        }
    }

    // Resolve git refs to actual values, skip any remaining promised entries
    let resolved: Vec<(String, String, String, ValueType)> = entries
        .into_iter()
        .filter(|r| !r.is_promised)
        .map(|r| {
            if r.is_git_ref {
                let resolved_value = resolve_git_ref(repo, &r.value)?;
                let json_value = serde_json::to_string(&resolved_value)?;
                Ok((r.target_value, r.key, json_value, r.value_type))
            } else {
                Ok((r.target_value, r.key, r.value, r.value_type))
            }
        })
        .collect::<Result<Vec<_>>>()?;

    if resolved.is_empty() {
        return Ok(());
    }

    if json_output {
        print_json(ctx.session.store(), &target, &resolved, with_authorship)?;
    } else {
        print_plain(&target, &resolved, key.is_some() && resolved.len() == 1)?;
    }

    Ok(())
}

/// Hydrate promised entries by looking up their blob OIDs in the tip tree
/// and fetching any that aren't already local. Returns the number hydrated.
fn hydrate_promised_entries(
    session: &git_meta_lib::Session,
    target_type: &TargetType,
    entries: &[(String, String)], // (target_value, key)
) -> Result<usize> {
    let repo = session.repo();
    let db = session.store();
    let ns = session.namespace();
    let tracking_ref = format!("refs/{ns}/remotes/main");

    let tip_commit = match repo.find_reference(&tracking_ref) {
        Ok(r) => r.into_fully_peeled_id()?,
        Err(_) => return Ok(0),
    };
    let tip_tree_id = tip_commit.object()?.into_commit().tree_id()?.detach();

    // For each promised entry, find its blob OID(s) in the tip tree
    struct PendingEntry {
        idx: usize,
        oids: Vec<gix::ObjectId>,
        value_type: ValueType,
    }

    let mut pending: Vec<PendingEntry> = Vec::new();
    let mut not_found: Vec<usize> = Vec::new();

    for (idx, (target_value, key)) in entries.iter().enumerate() {
        let target_str = if *target_type == TargetType::Project {
            "project".to_string()
        } else {
            format!("{}:{}", target_type.as_str(), target_value)
        };
        let Ok(parsed_target) = Target::parse(&target_str) else {
            continue;
        };

        // Try __value (string) first
        if let Ok(path) = tree_paths::tree_path(&parsed_target, key) {
            if let Some(oid) =
                git_meta_lib::git_utils::find_blob_oid_in_tree(repo, tip_tree_id, &path)?
            {
                pending.push(PendingEntry {
                    idx,
                    oids: vec![oid],
                    value_type: ValueType::String,
                });
                continue;
            }
        }

        // Try __list directory
        if let Ok(path) = tree_paths::list_dir_path(&parsed_target, key) {
            if let Some(dir_oid) =
                git_meta_lib::git_utils::find_blob_oid_in_tree(repo, tip_tree_id, &path)?
            {
                // dir_oid is a tree — collect all blob entries in it
                let list_tree = dir_oid.attach(repo).object()?.into_tree();
                let oids: Vec<_> = list_tree
                    .iter()
                    .filter_map(|e| {
                        let e = e.ok()?;
                        let name = e.filename().to_str().ok()?;
                        if name.starts_with("__") || name == TOMBSTONE_ROOT {
                            return None;
                        }
                        if e.mode().is_blob() {
                            Some(e.object_id())
                        } else {
                            None
                        }
                    })
                    .collect();
                if !oids.is_empty() {
                    pending.push(PendingEntry {
                        idx,
                        oids,
                        value_type: ValueType::List,
                    });
                    continue;
                }
            }
        }

        // Try __set directory
        if let Ok(key_path) = tree_paths::key_tree_path(&parsed_target, key) {
            let set_path = format!("{key_path}/{SET_VALUE_DIR}");
            if let Some(dir_oid) =
                git_meta_lib::git_utils::find_blob_oid_in_tree(repo, tip_tree_id, &set_path)?
            {
                let set_tree = dir_oid.attach(repo).object()?.into_tree();
                let oids: Vec<_> = set_tree
                    .iter()
                    .filter_map(|e| {
                        let e = e.ok()?;
                        let name = e.filename().to_str().ok()?;
                        if name.starts_with("__") || name == TOMBSTONE_ROOT {
                            return None;
                        }
                        if e.mode().is_blob() {
                            Some(e.object_id())
                        } else {
                            None
                        }
                    })
                    .collect();
                if !oids.is_empty() {
                    pending.push(PendingEntry {
                        idx,
                        oids,
                        value_type: ValueType::Set,
                    });
                    continue;
                }
            }
        }

        not_found.push(idx);
    }

    // Clean up entries that no longer exist in the tip
    for idx in &not_found {
        let (target_value, key) = &entries[*idx];
        let entry_target = if *target_type == TargetType::Project {
            Target::project()
        } else {
            Target::from_parts(target_type.clone(), Some(target_value.clone()))
        };
        db.delete_promised(&entry_target, key)?;
    }

    if pending.is_empty() {
        return Ok(0);
    }

    // Collect all OIDs, try to read locally first, fetch missing ones
    let all_oids: Vec<gix::ObjectId> = pending
        .iter()
        .flat_map(|p| p.oids.iter().copied())
        .collect();
    let mut missing: Vec<gix::ObjectId> = Vec::new();
    for oid in &all_oids {
        if oid.attach(repo).object().is_err() {
            missing.push(*oid);
        }
    }

    if !missing.is_empty() {
        let remote_name = git_meta_lib::git_utils::resolve_meta_remote(repo, None)?;
        eprintln!(
            "Fetching {} blob{} from remote...",
            missing.len(),
            if missing.len() == 1 { "" } else { "s" }
        );
        git_meta_lib::git_utils::fetch_blob_oids(repo, &remote_name, &missing)?;
    }

    // Now read blobs and update DB
    let mut hydrated = 0;
    for entry in &pending {
        let (target_value, key) = &entries[entry.idx];
        let entry_target = if *target_type == TargetType::Project {
            Target::project()
        } else {
            Target::from_parts(target_type.clone(), Some(target_value.clone()))
        };

        match entry.value_type {
            ValueType::String => {
                let oid = entry.oids[0];
                let blob = match oid.attach(repo).object() {
                    Ok(b) => b.into_blob(),
                    Err(_) => continue,
                };
                let Ok(content) = std::str::from_utf8(&blob.data) else {
                    continue;
                };
                let json_value = serde_json::to_string(content)?;
                db.resolve_promised(&entry_target, key, &json_value, &ValueType::String, false)?;
                hydrated += 1;
            }
            ValueType::List => {
                // Read all list entry blobs, build JSON array
                let mut list_entries = Vec::new();
                for oid in &entry.oids {
                    if let Ok(obj) = oid.attach(repo).object() {
                        let blob = obj.into_blob();
                        if let Ok(s) = std::str::from_utf8(&blob.data) {
                            list_entries.push(s.to_string());
                        }
                    }
                }
                let json_value = serde_json::to_string(&list_entries)?;
                db.resolve_promised(&entry_target, key, &json_value, &ValueType::List, false)?;
                hydrated += 1;
            }
            ValueType::Set => {
                // Read all set member blobs, build JSON array
                let mut set_members = Vec::new();
                for oid in &entry.oids {
                    if let Ok(obj) = oid.attach(repo).object() {
                        let blob = obj.into_blob();
                        if let Ok(s) = std::str::from_utf8(&blob.data) {
                            set_members.push(s.to_string());
                        }
                    }
                }
                set_members.sort();
                let json_value = serde_json::to_string(&set_members)?;
                db.resolve_promised(&entry_target, key, &json_value, &ValueType::Set, false)?;
                hydrated += 1;
            }
            _ => anyhow::bail!("unsupported value type"),
        }
    }

    Ok(hydrated)
}

/// Resolve a git blob SHA to its content as a UTF-8 string.
fn resolve_git_ref(repo: &gix::Repository, sha: &str) -> Result<String> {
    let oid = gix::ObjectId::from_hex(sha.as_bytes())
        .with_context(|| format!("invalid git blob SHA: {sha}"))?;
    let obj = oid
        .attach(repo)
        .object()
        .with_context(|| format!("git blob not found: {sha}"))?;
    let blob = obj.into_blob();
    let content = std::str::from_utf8(&blob.data)
        .with_context(|| format!("git blob {sha} is not valid UTF-8"))?;
    Ok(content.to_string())
}

fn truncate_str(s: &str, max: usize) -> String {
    if s.chars().count() <= max {
        s.to_string()
    } else {
        let truncated: String = s.chars().take(max - 3).collect();
        format!("{truncated}...")
    }
}

fn print_plain(
    target: &Target,
    entries: &[(String, String, String, ValueType)],
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
            if *target.target_type() == TargetType::Path {
                format!("{tv};{k}")
            } else {
                k.clone()
            }
        })
        .collect();
    let max_width = labels
        .iter()
        .map(std::string::String::len)
        .max()
        .unwrap_or(0);

    for (label, (_, _, value, value_type)) in labels.iter().zip(entries.iter()) {
        let display_value = format_value_compact(value, value_type)?;
        let truncated = truncate_str(&display_value, 50);
        println!("{label:max_width$}  {truncated}");
    }

    Ok(())
}

/// Single-key mode: raw string value, or one line per list/set item.
fn print_value_only(value: &str, value_type: &ValueType) -> Result<()> {
    match value_type {
        ValueType::String => {
            let s: String = serde_json::from_str(value)?;
            println!("{s}");
        }
        ValueType::List => {
            for item in extract_list_values(value)? {
                println!("{item}");
            }
        }
        ValueType::Set => {
            let mut set: Vec<String> = serde_json::from_str(value)?;
            set.sort();
            for item in set {
                println!("{item}");
            }
        }
        _ => anyhow::bail!("unsupported value type"),
    }
    Ok(())
}

/// Multi-key mode: compact one-line representation.
fn format_value_compact(value: &str, value_type: &ValueType) -> Result<String> {
    match value_type {
        ValueType::String => {
            let s: String = serde_json::from_str(value)?;
            Ok(s)
        }
        ValueType::List => {
            let list = extract_list_values(value)?;
            Ok(list.join(", "))
        }
        ValueType::Set => {
            let mut set: Vec<String> = serde_json::from_str(value)?;
            set.sort();
            Ok(set.join(", "))
        }
        _ => anyhow::bail!("unsupported value type"),
    }
}

fn print_json(
    db: &Store,
    target: &Target,
    entries: &[(String, String, String, ValueType)],
    with_authorship: bool,
) -> Result<()> {
    let mut root = Map::new();

    for (entry_target_value, key, value, value_type) in entries {
        let parsed_value = parse_stored_value(value, value_type)?;

        let leaf_value = if with_authorship {
            let entry_target = Target::from_parts(
                target.target_type().clone(),
                if entry_target_value.is_empty() {
                    None
                } else {
                    Some(entry_target_value.clone())
                },
            );
            let authorship = db.get_authorship(&entry_target, key)?;
            let (author, timestamp) = match authorship {
                Some(a) => (a.email, a.timestamp),
                None => ("unknown".to_string(), 0),
            };
            json!({
                "value": parsed_value,
                "author": author,
                "timestamp": timestamp
            })
        } else {
            parsed_value
        };

        if *target.target_type() == TargetType::Path {
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
    println!("{output}");
    Ok(())
}

fn parse_stored_value(value: &str, value_type: &ValueType) -> Result<Value> {
    match value_type {
        ValueType::String => {
            let s: String = serde_json::from_str(value)?;
            Ok(Value::String(s))
        }
        ValueType::List => {
            let list = extract_list_values(value)?;
            Ok(Value::Array(list.into_iter().map(Value::String).collect()))
        }
        ValueType::Set => {
            let mut set: Vec<String> = serde_json::from_str(value)?;
            set.sort();
            Ok(Value::Array(set.into_iter().map(Value::String).collect()))
        }
        _ => anyhow::bail!("unsupported value type"),
    }
}

/// Extract string values from a stored list JSON blob.
///
/// Handles both the current object format (`[{"value":"a","timestamp":1}]`)
/// and the legacy plain-string format (`["a","b"]`).
fn extract_list_values(raw: &str) -> Result<Vec<String>> {
    let items: Vec<Value> = serde_json::from_str(raw)?;
    items
        .into_iter()
        .map(|item| match item {
            Value::String(s) => Ok(s),
            Value::Object(ref map) => map
                .get("value")
                .and_then(|v| v.as_str())
                .map(std::string::ToString::to_string)
                .ok_or_else(|| anyhow::anyhow!("list entry missing 'value' field")),
            other => anyhow::bail!("unexpected list entry type: {other:?}"),
        })
        .collect()
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
#[allow(clippy::unwrap_used, clippy::expect_used)]
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
