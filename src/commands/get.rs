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
        let matches =
            db.find_target_values_by_prefix(target.type_str(), target.value_str(), 2)?;
        if matches.len() == 1 {
            let expanded = &matches[0];
            entries = db.get_all_with_target_prefix(
                target.type_str(),
                expanded,
                false,
                key,
            )?;
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
        let hydrated = hydrate_promised_entries(&repo, &db, &promised)?;
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
        .map(|(entry_target_value, key, value, value_type, is_git_ref, _)| {
            if is_git_ref {
                let resolved_value = resolve_git_ref(&repo, &value)?;
                // JSON-encode the resolved content to match normal string format
                let json_value = serde_json::to_string(&resolved_value)?;
                Ok((entry_target_value, key, json_value, value_type))
            } else {
                Ok((entry_target_value, key, value, value_type))
            }
        })
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
/// and fetching the blobs from the remote. Returns the number hydrated.
fn hydrate_promised_entries(
    repo: &Repository,
    db: &Db,
    entries: &[(String, String)], // (target_value, key)
) -> Result<usize> {
    let ns = git_utils::get_namespace(repo)?;
    let tracking_ref = format!("refs/{}/remotes/main", ns);

    let tip_commit = match repo.find_reference(&tracking_ref) {
        Ok(r) => r.peel_to_commit()?,
        Err(_) => return Ok(0), // no remote tracking ref, can't hydrate
    };
    let tip_tree = tip_commit.tree()?;

    // For each promised entry, find its blob OID in the tip tree
    let mut to_fetch: Vec<(usize, git2::Oid, String)> = Vec::new(); // (index, oid, tree_path)
    let mut not_found: Vec<usize> = Vec::new();

    // We need to reconstruct Target objects to build tree paths.
    // The target_type comes from the DB query context, but we only have target_value here.
    // We need to also pass target_type. For now, we get it from the DB row.
    // Actually, all entries in a single get call share the same target_type from the query.
    // But we don't have that here. Let me work around this by trying to parse from DB.

    // We'll look up each entry's target_type from the DB
    for (idx, (target_value, key)) in entries.iter().enumerate() {
        // Query the metadata table directly for the target_type
        let target_type: Option<String> = db
            .conn
            .query_row(
                "SELECT target_type FROM metadata WHERE target_value = ?1 AND key = ?2 AND is_promised = 1",
                rusqlite::params![target_value, key],
                |row| row.get(0),
            )
            .ok();

        let target_type = match target_type {
            Some(t) => t,
            None => continue,
        };

        let target_str = if target_type == "project" {
            "project".to_string()
        } else {
            format!("{}:{}", target_type, target_value)
        };
        let parsed_target = match Target::parse(&target_str) {
            Ok(t) => t,
            Err(_) => continue,
        };

        // Try string value first (__value blob)
        let tree_path = match types::build_tree_path(&parsed_target, key) {
            Ok(p) => p,
            Err(_) => continue,
        };

        match git_utils::find_blob_oid_in_tree(repo, &tip_tree, &tree_path)? {
            Some(oid) => to_fetch.push((idx, oid, tree_path)),
            None => {
                // Key doesn't exist in tip tree — it was deleted in a later commit
                not_found.push(idx);
            }
        }
    }

    // Clean up entries that no longer exist in the tip
    for idx in &not_found {
        let (target_value, key) = &entries[*idx];
        // Look up target_type again
        let target_type: Option<String> = db
            .conn
            .query_row(
                "SELECT target_type FROM metadata WHERE target_value = ?1 AND key = ?2 AND is_promised = 1",
                rusqlite::params![target_value, key],
                |row| row.get(0),
            )
            .ok();
        if let Some(tt) = target_type {
            db.delete_promised(&tt, target_value, key)?;
        }
    }

    if to_fetch.is_empty() {
        return Ok(0);
    }

    // Fetch all needed blobs in one call
    let oids: Vec<git2::Oid> = to_fetch.iter().map(|(_, oid, _)| *oid).collect();
    let remote_name = git_utils::resolve_meta_remote(repo, None)?;

    eprintln!("Fetching {} value{} from remote...", oids.len(), if oids.len() == 1 { "" } else { "s" });
    git_utils::fetch_blob_oids(repo, &remote_name, &oids)?;

    // Now read each blob and update the DB
    let mut hydrated = 0;
    for (idx, oid, _tree_path) in &to_fetch {
        let (target_value, key) = &entries[*idx];

        let blob = match repo.find_blob(*oid) {
            Ok(b) => b,
            Err(_) => continue,
        };
        let content = match std::str::from_utf8(blob.content()) {
            Ok(s) => s,
            Err(_) => continue,
        };

        // Look up target_type
        let target_type: Option<String> = db
            .conn
            .query_row(
                "SELECT target_type FROM metadata WHERE target_value = ?1 AND key = ?2 AND is_promised = 1",
                rusqlite::params![target_value, key],
                |row| row.get(0),
            )
            .ok();
        let target_type = match target_type {
            Some(t) => t,
            None => continue,
        };

        // Store as JSON-encoded string value
        let json_value = serde_json::to_string(content)?;
        db.resolve_promised(&target_type, target_value, key, &json_value, "string", false)?;
        hydrated += 1;
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

fn print_plain(target: &Target, entries: &[(String, String, String, String)], value_only: bool) -> Result<()> {
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
