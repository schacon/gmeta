//! Auto-prune rule evaluation and tree-size computation.
//!
//! Provides the logic for determining when automatic pruning should occur
//! and the helpers for reading prune configuration from the metadata store.

use gix::bstr::ByteSlice;
use gix::prelude::ObjectIdExt;
use time::{Duration, OffsetDateTime};

use crate::db::Store;
use crate::error::{Error, Result};
use crate::types::Target;

/// Parsed auto-prune rules from project metadata.
///
/// These rules control when and how automatic pruning of old metadata
/// entries is triggered during serialization.
pub struct PruneRules {
    /// The time window for retention (e.g. `"90d"`, `"6m"`, `"1y"`, or `"2025-01-01"`).
    pub since: String,
    /// Maximum number of metadata keys before pruning is triggered.
    pub max_keys: Option<u64>,
    /// Maximum total blob size (bytes) before pruning is triggered.
    pub max_size: Option<u64>,
    /// Minimum subtree size (bytes); subtrees smaller than this are never pruned.
    pub min_size: Option<u64>,
}

/// Read auto-prune rules from project metadata.
///
/// Returns `None` if the required `meta:prune:since` key is missing or if
/// neither `max-keys` nor `max-size` triggers are configured.
///
/// # Errors
///
/// Returns an error if database reads fail or if configured values
/// cannot be parsed.
pub fn read_prune_rules(db: &Store) -> Result<Option<PruneRules>> {
    let since = match read_config_string(db, "meta:prune:since")? {
        Some(s) => s,
        None => return Ok(None),
    };

    let max_keys = read_config_string(db, "meta:prune:max-keys")?
        .map(|s| {
            s.parse::<u64>()
                .map_err(|_| Error::InvalidValue(format!("invalid meta:prune:max-keys value: {s}")))
        })
        .transpose()?;

    let max_size = read_config_string(db, "meta:prune:max-size")?
        .map(|s| {
            parse_size(&s)
                .map_err(|_| Error::InvalidValue(format!("invalid meta:prune:max-size value: {s}")))
        })
        .transpose()?;

    let min_size = read_config_string(db, "meta:prune:min-size")?
        .map(|s| {
            parse_size(&s)
                .map_err(|_| Error::InvalidValue(format!("invalid meta:prune:min-size value: {s}")))
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
///
/// Returns `true` if the key count exceeds `max_keys` or the total blob
/// size exceeds `max_size`.
///
/// # Errors
///
/// Returns an error if Git object reads fail.
pub fn should_prune(
    repo: &gix::Repository,
    tree_oid: gix::ObjectId,
    rules: &PruneRules,
) -> Result<bool> {
    if let Some(max_keys) = rules.max_keys {
        let key_count = count_keys(repo, tree_oid)?;
        if key_count > max_keys {
            return Ok(true);
        }
    }

    if let Some(max_size) = rules.max_size {
        let total_size = compute_tree_size(repo, tree_oid)?;
        if total_size > max_size {
            return Ok(true);
        }
    }

    Ok(false)
}

/// Count total metadata keys in a serialized tree.
///
/// A key is identified by the presence of a terminal blob (`__value`) or
/// directory (`__list`, `__set`).
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
    let tree = tree_oid
        .attach(repo)
        .object()
        .map_err(|e| Error::Other(format!("{e}")))?
        .into_tree();
    for entry_result in tree.iter() {
        let entry = entry_result.map_err(|e| Error::Other(format!("{e}")))?;
        let name = entry.filename().to_str_lossy().to_string();
        if name == "__value" || name == "__list" || name == "__set" {
            *count += 1;
        } else if name == "__tombstones" {
            continue;
        } else if entry.mode().is_tree() {
            count_keys_recursive(repo, entry.object_id(), count)?;
        }
    }
    Ok(())
}

/// Compute total blob size in a serialized tree (bytes).
fn compute_tree_size(repo: &gix::Repository, tree_oid: gix::ObjectId) -> Result<u64> {
    let mut total = 0u64;
    compute_tree_size_recursive(repo, tree_oid, &mut total)?;
    Ok(total)
}

/// Compute the blob-size total for a subtree.
///
/// Used by serialize for min-size checks on individual target subtrees.
///
/// # Errors
///
/// Returns an error if Git object reads fail.
pub fn compute_tree_size_for(repo: &gix::Repository, tree_oid: gix::ObjectId) -> Result<u64> {
    compute_tree_size(repo, tree_oid)
}

fn compute_tree_size_recursive(
    repo: &gix::Repository,
    tree_oid: gix::ObjectId,
    total: &mut u64,
) -> Result<()> {
    let tree = tree_oid
        .attach(repo)
        .object()
        .map_err(|e| Error::Other(format!("{e}")))?
        .into_tree();
    for entry_result in tree.iter() {
        let entry = entry_result.map_err(|e| Error::Other(format!("{e}")))?;
        if entry.mode().is_blob() {
            let blob = entry
                .object_id()
                .attach(repo)
                .object()
                .map_err(|e| Error::Other(format!("{e}")))?
                .into_blob();
            *total += blob.data.len() as u64;
        } else if entry.mode().is_tree() {
            compute_tree_size_recursive(repo, entry.object_id(), total)?;
        }
    }
    Ok(())
}

/// Parse a duration string like `"90d"`, `"6m"`, `"1y"` or an ISO date
/// into a cutoff timestamp in milliseconds since the Unix epoch.
///
/// # Parameters
/// - `since`: the duration or date string
/// - `now_ms`: the current time in milliseconds since Unix epoch
///
/// # Errors
///
/// Returns an error if the string cannot be parsed as a supported format.
pub fn parse_since_to_cutoff_ms(since: &str, now_ms: i64) -> Result<i64> {
    let s = since.trim().to_lowercase();
    if let Some(num_str) = s.strip_suffix('d') {
        let days: i64 = num_str
            .parse()
            .map_err(|_| Error::InvalidValue(format!("invalid duration: {since}")))?;
        return Ok(now_ms - Duration::days(days).whole_milliseconds() as i64);
    }
    if let Some(num_str) = s.strip_suffix('m') {
        let months: i64 = num_str
            .parse()
            .map_err(|_| Error::InvalidValue(format!("invalid duration: {since}")))?;
        return Ok(now_ms - Duration::days(months * 30).whole_milliseconds() as i64);
    }
    if let Some(num_str) = s.strip_suffix('y') {
        let years: i64 = num_str
            .parse()
            .map_err(|_| Error::InvalidValue(format!("invalid duration: {since}")))?;
        return Ok(now_ms - Duration::days(years * 365).whole_milliseconds() as i64);
    }

    let date_fmt = time::format_description::parse("[year]-[month]-[day]").unwrap_or_default();
    if let Ok(date) = time::Date::parse(since, &date_fmt) {
        let odt = OffsetDateTime::new_utc(date, time::Time::MIDNIGHT);
        return Ok(odt.unix_timestamp_nanos() as i64 / 1_000_000);
    }

    Err(Error::InvalidValue(format!(
        "cannot parse since value: {since} (expected e.g. 90d, 6m, 1y, or 2025-01-01)"
    )))
}

/// Parse a human-friendly size string (e.g. `"512k"`, `"10m"`, `"1g"`)
/// into bytes.
///
/// Supports bare numbers (bytes), and `k`/`m`/`g` suffixes
/// (case-insensitive, base-1024).
///
/// # Errors
///
/// Returns an error if the string is empty or cannot be parsed.
pub fn parse_size(s: &str) -> Result<u64> {
    let s = s.trim().to_lowercase();
    if s.is_empty() {
        return Err(Error::InvalidValue("empty size string".to_string()));
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
        .map_err(|_| Error::InvalidValue(format!("invalid number: {num_str}")))?;

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
