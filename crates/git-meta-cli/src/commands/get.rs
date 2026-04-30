use anyhow::{Context, Result};
use gix::prelude::ObjectIdExt;
use serde_json::{json, Map, Value};

use crate::commands::hydrate::hydrate_promised_entries;
use crate::context::CommandContext;
use git_meta_lib::db::Store;
use git_meta_lib::types::{Target, TargetType, ValueType};

const NODE_VALUE_KEY: &str = "__value";

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
