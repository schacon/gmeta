use anyhow::{bail, Context, Result};
use gix::bstr::ByteSlice;
use gix::prelude::ObjectIdExt;
use time::{Duration, OffsetDateTime};

use gmeta::db::Store;
use gmeta::types::Target;

/// Parsed auto-prune rules from project metadata.
pub struct PruneRules {
    pub since: String,
    pub max_keys: Option<u64>,
    pub max_size: Option<u64>,
    pub min_size: Option<u64>,
}

/// Read auto-prune rules from project metadata. Returns None if rules are incomplete.
pub fn read_prune_rules(db: &Store) -> Result<Option<PruneRules>> {
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
        .map(|s| {
            parse_size(&s).with_context(|| format!("invalid meta:prune:max-size value: {}", s))
        })
        .transpose()?;

    let min_size = read_config_string(db, "meta:prune:min-size")?
        .map(|s| {
            parse_size(&s).with_context(|| format!("invalid meta:prune:min-size value: {}", s))
        })
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

fn read_config_string(db: &Store, key: &str) -> Result<Option<String>> {
    match db.get(&Target::project(), key)? {
        Some(entry) => {
            let s: String = serde_json::from_str(&entry.value)?;
            Ok(Some(s))
        }
        None => Ok(None),
    }
}

/// Check whether any prune trigger is exceeded for the given tree.
pub fn should_prune(
    repo: &gix::Repository,
    tree_oid: gix::ObjectId,
    rules: &PruneRules,
) -> Result<bool> {
    if let Some(max_keys) = rules.max_keys {
        let key_count = count_keys(repo, tree_oid)?;
        eprintln!(
            "Auto-prune check: {} keys (threshold: {})",
            key_count, max_keys
        );
        if key_count > max_keys {
            return Ok(true);
        }
    }

    if let Some(max_size) = rules.max_size {
        let total_size = compute_tree_size(repo, tree_oid)?;
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
fn count_keys(repo: &gix::Repository, tree_oid: gix::ObjectId) -> Result<u64> {
    let mut count = 0u64;
    count_keys_recursive(repo, tree_oid, &mut count)?;
    Ok(count)
}

fn count_keys_recursive(
    repo: &gix::Repository,
    tree_oid: gix::ObjectId,
    count: &mut u64,
) -> Result<()> {
    let tree = tree_oid.attach(repo).object()?.into_tree();
    for entry_result in tree.iter() {
        let entry = entry_result?;
        let name = entry.filename().to_str_lossy().to_string();
        if name == "__value" || name == "__list" || name == "__set" {
            *count += 1;
        } else if name == "__tombstones" {
            // Don't count tombstones as keys
            continue;
        } else if entry.mode().is_tree() {
            count_keys_recursive(repo, entry.object_id(), count)?;
        }
    }
    Ok(())
}

/// Compute total blob size in a serialized tree.
fn compute_tree_size(repo: &gix::Repository, tree_oid: gix::ObjectId) -> Result<u64> {
    let mut total = 0u64;
    compute_tree_size_recursive(repo, tree_oid, &mut total)?;
    Ok(total)
}

/// Public helper for computing the size of a subtree (used by serialize for min-size checks).
pub fn compute_tree_size_for(repo: &gix::Repository, tree_oid: gix::ObjectId) -> Result<u64> {
    compute_tree_size(repo, tree_oid)
}

fn compute_tree_size_recursive(
    repo: &gix::Repository,
    tree_oid: gix::ObjectId,
    total: &mut u64,
) -> Result<()> {
    let tree = tree_oid.attach(repo).object()?.into_tree();
    for entry_result in tree.iter() {
        let entry = entry_result?;
        if entry.mode().is_blob() {
            let blob = entry.object_id().attach(repo).object()?.into_blob();
            *total += blob.data.len() as u64;
        } else if entry.mode().is_tree() {
            compute_tree_size_recursive(repo, entry.object_id(), total)?;
        }
    }
    Ok(())
}

/// Parse a duration string like "90d", "6m", "1y" or an ISO date into a cutoff timestamp (millis).
pub fn parse_since_to_cutoff_ms(since: &str) -> Result<i64> {
    let now_ms = OffsetDateTime::now_utc().unix_timestamp_nanos() as i64 / 1_000_000;
    let s = since.trim().to_lowercase();
    if let Some(num_str) = s.strip_suffix('d') {
        let days: i64 = num_str
            .parse()
            .with_context(|| format!("invalid duration: {}", since))?;
        return Ok(now_ms - Duration::days(days).whole_milliseconds() as i64);
    }
    if let Some(num_str) = s.strip_suffix('m') {
        let months: i64 = num_str
            .parse()
            .with_context(|| format!("invalid duration: {}", since))?;
        return Ok(now_ms - Duration::days(months * 30).whole_milliseconds() as i64);
    }
    if let Some(num_str) = s.strip_suffix('y') {
        let years: i64 = num_str
            .parse()
            .with_context(|| format!("invalid duration: {}", since))?;
        return Ok(now_ms - Duration::days(years * 365).whole_milliseconds() as i64);
    }

    let date_fmt = time::format_description::parse("[year]-[month]-[day]").unwrap_or_default();
    if let Ok(date) = time::Date::parse(since, &date_fmt) {
        let odt = OffsetDateTime::new_utc(date, time::Time::MIDNIGHT);
        return Ok(odt.unix_timestamp_nanos() as i64 / 1_000_000);
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
#[allow(clippy::unwrap_used, clippy::expect_used)]
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
