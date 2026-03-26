use anyhow::{bail, Context, Result};
use chrono::Utc;

use crate::db::Db;

/// Parsed auto-prune rules from project metadata.
pub struct PruneRules {
    pub since: String,
    pub max_keys: Option<u64>,
    pub max_size: Option<u64>,
    pub min_size: Option<u64>,
}

/// Read auto-prune rules from project metadata. Returns None if rules are incomplete.
pub fn read_prune_rules(db: &Db) -> Result<Option<PruneRules>> {
    let since = match read_config_string(db, "meta:prune:since")? {
        Some(s) => s,
        None => return Ok(None),
    };

    let max_keys = read_config_string(db, "meta:prune:max-keys")?
        .map(|s| {
            s.parse::<u64>()
                .with_context(|| format!("invalid meta:prune:max-keys value: {}", s))
        })
        .transpose()?;

    let max_size = read_config_string(db, "meta:prune:max-size")?
        .map(|s| parse_size(&s).with_context(|| format!("invalid meta:prune:max-size value: {}", s)))
        .transpose()?;

    let min_size = read_config_string(db, "meta:prune:min-size")?
        .map(|s| parse_size(&s).with_context(|| format!("invalid meta:prune:min-size value: {}", s)))
        .transpose()?;

    // Need at least one trigger
    if max_keys.is_none() && max_size.is_none() {
        return Ok(None);
    }

    Ok(Some(PruneRules {
        since,
        max_keys,
        max_size,
        min_size,
    }))
}

fn read_config_string(db: &Db, key: &str) -> Result<Option<String>> {
    match db.get("project", "", key)? {
        Some((value, _, _)) => {
            let s: String = serde_json::from_str(&value)?;
            Ok(Some(s))
        }
        None => Ok(None),
    }
}

/// Check whether any prune trigger is exceeded for the given tree.
pub fn should_prune(
    repo: &git2::Repository,
    tree_oid: git2::Oid,
    rules: &PruneRules,
) -> Result<bool> {
    let tree = repo.find_tree(tree_oid)?;

    if let Some(max_keys) = rules.max_keys {
        let key_count = count_keys(repo, &tree)?;
        eprintln!(
            "Auto-prune check: {} keys (threshold: {})",
            key_count, max_keys
        );
        if key_count > max_keys {
            return Ok(true);
        }
    }

    if let Some(max_size) = rules.max_size {
        let total_size = compute_tree_size(repo, &tree)?;
        eprintln!(
            "Auto-prune check: {} bytes (threshold: {})",
            total_size, max_size
        );
        if total_size > max_size {
            return Ok(true);
        }
    }

    Ok(false)
}

/// Count total metadata keys in a serialized tree.
/// A key is identified by the presence of a terminal blob (__value) or directory (__list, __set).
fn count_keys(repo: &git2::Repository, tree: &git2::Tree) -> Result<u64> {
    let mut count = 0u64;
    count_keys_recursive(repo, tree, &mut count)?;
    Ok(count)
}

fn count_keys_recursive(
    repo: &git2::Repository,
    tree: &git2::Tree,
    count: &mut u64,
) -> Result<()> {
    for entry in tree.iter() {
        let name = entry.name().unwrap_or("");
        if name == "__value" {
            *count += 1;
        } else if name == "__list" || name == "__set" {
            *count += 1;
        } else if name == "__tombstones" {
            // Don't count tombstones as keys
            continue;
        } else if entry.kind() == Some(git2::ObjectType::Tree) {
            let subtree = repo.find_tree(entry.id())?;
            count_keys_recursive(repo, &subtree, count)?;
        }
    }
    Ok(())
}

/// Compute total blob size in a serialized tree.
fn compute_tree_size(repo: &git2::Repository, tree: &git2::Tree) -> Result<u64> {
    let mut total = 0u64;
    compute_tree_size_recursive(repo, tree, &mut total)?;
    Ok(total)
}

/// Public helper for computing the size of a subtree (used by serialize for min-size checks).
pub fn compute_tree_size_for(repo: &git2::Repository, tree: &git2::Tree) -> Result<u64> {
    compute_tree_size(repo, tree)
}

fn compute_tree_size_recursive(
    repo: &git2::Repository,
    tree: &git2::Tree,
    total: &mut u64,
) -> Result<()> {
    for entry in tree.iter() {
        match entry.kind() {
            Some(git2::ObjectType::Blob) => {
                let blob = repo.find_blob(entry.id())?;
                *total += blob.size() as u64;
            }
            Some(git2::ObjectType::Tree) => {
                let subtree = repo.find_tree(entry.id())?;
                compute_tree_size_recursive(repo, &subtree, total)?;
            }
            _ => {}
        }
    }
    Ok(())
}

/// Parse a duration string like "90d", "6m", "1y" or an ISO date into a cutoff timestamp (millis).
pub fn parse_since_to_cutoff_ms(since: &str) -> Result<i64> {
    let s = since.trim().to_lowercase();
    if let Some(num_str) = s.strip_suffix('d') {
        let days: i64 = num_str
            .parse()
            .with_context(|| format!("invalid duration: {}", since))?;
        return Ok(Utc::now().timestamp_millis() - days * 86_400_000);
    }
    if let Some(num_str) = s.strip_suffix('m') {
        let months: i64 = num_str
            .parse()
            .with_context(|| format!("invalid duration: {}", since))?;
        return Ok(Utc::now().timestamp_millis() - months * 30 * 86_400_000);
    }
    if let Some(num_str) = s.strip_suffix('y') {
        let years: i64 = num_str
            .parse()
            .with_context(|| format!("invalid duration: {}", since))?;
        return Ok(Utc::now().timestamp_millis() - years * 365 * 86_400_000);
    }

    if let Ok(date) = chrono::NaiveDate::parse_from_str(since, "%Y-%m-%d") {
        let dt = date
            .and_hms_opt(0, 0, 0)
            .ok_or_else(|| anyhow::anyhow!("invalid date"))?;
        return Ok(dt.and_utc().timestamp_millis());
    }

    bail!(
        "cannot parse since value: {} (expected e.g. 90d, 6m, 1y, or 2025-01-01)",
        since
    );
}

/// Parse a human-friendly size string (e.g. "512k", "10m", "1g") into bytes.
pub fn parse_size(s: &str) -> Result<u64> {
    let s = s.trim().to_lowercase();
    if s.is_empty() {
        bail!("empty size string");
    }

    let (num_str, multiplier) = if s.ends_with('g') {
        (&s[..s.len() - 1], 1024 * 1024 * 1024u64)
    } else if s.ends_with('m') {
        (&s[..s.len() - 1], 1024 * 1024u64)
    } else if s.ends_with('k') {
        (&s[..s.len() - 1], 1024u64)
    } else {
        (s.as_str(), 1u64)
    };

    let num: u64 = num_str
        .parse()
        .with_context(|| format!("invalid number: {}", num_str))?;

    Ok(num * multiplier)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_size() {
        assert_eq!(parse_size("1024").unwrap(), 1024);
        assert_eq!(parse_size("512k").unwrap(), 512 * 1024);
        assert_eq!(parse_size("10m").unwrap(), 10 * 1024 * 1024);
        assert_eq!(parse_size("1g").unwrap(), 1024 * 1024 * 1024);
        assert_eq!(parse_size("50M").unwrap(), 50 * 1024 * 1024);
        assert!(parse_size("").is_err());
        assert!(parse_size("abc").is_err());
    }
}
