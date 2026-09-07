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
/// Auto-pruning is a *high-water / low-water* rule, not a retention window.
/// When the serialized tree grows past a maximum it is cut back to the
/// corresponding minimum, keeping the most recently modified keys, and then
/// left alone until it grows past the maximum again.
///
/// Age is deliberately not part of this. A retention window cannot promise to
/// bring the tree under a size limit — if everything is recent, nothing is
/// dropped — so a tree over its limit would re-run the prune on every single
/// serialize and never come down. Date-based pruning belongs to the manual
/// `git meta prune`, where the caller chooses the window.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PruneRules {
    /// Key count that triggers a prune.
    pub max_keys: Option<u64>,
    /// Key count to cut back to. Defaults to half of `max_keys`.
    pub min_keys: Option<u64>,
    /// Total blob size that triggers a prune.
    pub max_size: Option<u64>,
    /// Total blob size to cut back to. Defaults to half of `max_size`.
    pub min_size: Option<u64>,
}

/// Read auto-prune rules from project metadata.
///
/// Returns `None` unless at least one of `meta:prune:max-keys` or
/// `meta:prune:max-size` is configured.
///
/// Where a maximum is set without its matching minimum, the minimum defaults
/// to half the maximum, so that a prune buys room for further growth rather
/// than triggering again on the next serialize.
///
/// # Errors
///
/// Returns an error if database reads fail, if a value cannot be parsed, or if
/// a configured minimum is not below its maximum.
pub fn read_prune_rules(db: &Store) -> Result<Option<PruneRules>> {
    let max_keys = read_config_u64(db, "meta:prune:max-keys")?;
    let min_keys = read_config_u64(db, "meta:prune:min-keys")?;
    let max_size = read_config_size(db, "meta:prune:max-size")?;
    let min_size = read_config_size(db, "meta:prune:min-size")?;

    // Need at least one trigger.
    if max_keys.is_none() && max_size.is_none() {
        return Ok(None);
    }

    let min_keys = resolve_floor(max_keys, min_keys, "keys")?;
    let min_size = resolve_floor(max_size, min_size, "size")?;

    Ok(Some(PruneRules {
        max_keys,
        min_keys,
        max_size,
        min_size,
    }))
}

/// Pair a maximum with the floor to prune back to, defaulting to half.
fn resolve_floor(max: Option<u64>, min: Option<u64>, what: &str) -> Result<Option<u64>> {
    match (max, min) {
        (Some(max), Some(min)) if min >= max => Err(Error::InvalidValue(format!(
            "meta:prune:min-{what} ({min}) must be below meta:prune:max-{what} ({max})"
        ))),
        (Some(_), Some(min)) => Ok(Some(min)),
        // Half, but never zero: a floor of nothing would empty the tree.
        (Some(max), None) => Ok(Some((max / 2).max(1))),
        // A floor without a trigger has nothing to act on.
        (None, _) => Ok(None),
    }
}

fn read_config_u64(db: &Store, key: &str) -> Result<Option<u64>> {
    read_config_string(db, key)?
        .map(|value| {
            value
                .parse::<u64>()
                .map_err(|_| Error::InvalidValue(format!("invalid {key} value: {value}")))
        })
        .transpose()
}

fn read_config_size(db: &Store, key: &str) -> Result<Option<u64>> {
    read_config_string(db, key)?
        .map(|value| {
            parse_size(&value)
                .map_err(|_| Error::InvalidValue(format!("invalid {key} value: {value}")))
        })
        .transpose()
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
    db: &Store,
    tree_oid: gix::ObjectId,
    rules: &PruneRules,
    now_ms: i64,
) -> Result<bool> {
    if let Some(max_keys) = rules.max_keys {
        if count_keys(repo, db, tree_oid, now_ms)? > max_keys {
            return Ok(true);
        }
    }

    if let Some(max_size) = rules.max_size {
        if compute_tree_size(repo, db, tree_oid, now_ms)? > max_size {
            return Ok(true);
        }
    }

    Ok(false)
}

/// Count total metadata keys in a serialized tree.
///
/// A key is identified by the presence of a terminal blob (`__value`) or
/// directory (`__list`, `__set`).
fn count_keys(
    repo: &gix::Repository,
    db: &Store,
    tree_oid: gix::ObjectId,
    now_ms: i64,
) -> Result<u64> {
    measure_tree(repo, db, tree_oid, now_ms, &TreeMeasure::Keys)
}

/// Compute total blob size in a serialized tree (bytes).
fn compute_tree_size(
    repo: &gix::Repository,
    db: &Store,
    tree_oid: gix::ObjectId,
    now_ms: i64,
) -> Result<u64> {
    measure_tree(repo, db, tree_oid, now_ms, &TreeMeasure::Bytes)
}

/// Compute the blob-size total for a subtree, without consulting the cache.
///
/// # Errors
///
/// Returns an error if Git object reads fail.
pub fn compute_tree_size_for(repo: &gix::Repository, tree_oid: gix::ObjectId) -> Result<u64> {
    let mut total = 0u64;
    walk_uncached(repo, tree_oid, &TreeMeasure::Bytes, &mut total)?;
    Ok(total)
}

/// What a tree walk is measuring.
enum TreeMeasure {
    /// Metadata keys: a terminal `__value` blob, or a `__list` / `__set` directory.
    Keys,
    /// Bytes of every blob reachable from the tree.
    Bytes,
}

impl TreeMeasure {
    fn cached(&self, db: &Store, tree_oid: &gix::oid) -> Result<Option<u64>> {
        match self {
            TreeMeasure::Keys => db.tree_key_count(tree_oid),
            TreeMeasure::Bytes => db.tree_byte_size(tree_oid),
        }
    }

    fn remember(&self, db: &Store, tree_oid: &gix::oid, value: u64, now_ms: i64) -> Result<()> {
        match self {
            TreeMeasure::Keys => db.set_tree_key_count(tree_oid, value, now_ms),
            TreeMeasure::Bytes => db.set_tree_byte_size(tree_oid, value, now_ms),
        }
    }
}

/// Measure a tree, consulting and filling the per-tree cache as it descends.
///
/// Tree object IDs are content hashes, so a cached measurement is valid
/// forever. An incremental serialize rewrites only the subtrees along the paths
/// it touched, so almost every subtree here is a cache hit and the walk costs
/// what changed rather than what exists.
fn measure_tree(
    repo: &gix::Repository,
    db: &Store,
    tree_oid: gix::ObjectId,
    now_ms: i64,
    measure: &TreeMeasure,
) -> Result<u64> {
    if let Some(cached) = measure.cached(db, &tree_oid)? {
        return Ok(cached);
    }

    let tree = tree_oid
        .attach(repo)
        .object()
        .map_err(|e| Error::Other(format!("{e}")))?
        .into_tree();

    let mut total = 0u64;
    for entry_result in tree.iter() {
        let entry = entry_result.map_err(|e| Error::Other(format!("{e}")))?;
        let name = entry.filename().to_str_lossy().to_string();

        match measure {
            TreeMeasure::Keys => {
                if name == "__value" || name == "__list" || name == "__set" {
                    total += 1;
                    continue;
                }
                if name == "__tombstones" {
                    continue;
                }
            }
            TreeMeasure::Bytes => {
                if entry.mode().is_blob() {
                    let blob = entry
                        .object_id()
                        .attach(repo)
                        .object()
                        .map_err(|e| Error::Other(format!("{e}")))?
                        .into_blob();
                    total += blob.data.len() as u64;
                    continue;
                }
            }
        }

        if entry.mode().is_tree() {
            total += measure_tree(repo, db, entry.object_id(), now_ms, measure)?;
        }
    }

    measure.remember(db, &tree_oid, total, now_ms)?;
    Ok(total)
}

/// Walk a tree without touching the cache, for callers outside serialization.
fn walk_uncached(
    repo: &gix::Repository,
    tree_oid: gix::ObjectId,
    measure: &TreeMeasure,
    total: &mut u64,
) -> Result<()> {
    let tree = tree_oid
        .attach(repo)
        .object()
        .map_err(|e| Error::Other(format!("{e}")))?
        .into_tree();
    for entry_result in tree.iter() {
        let entry = entry_result.map_err(|e| Error::Other(format!("{e}")))?;
        let name = entry.filename().to_str_lossy().to_string();
        match measure {
            TreeMeasure::Keys => {
                if name == "__value" || name == "__list" || name == "__set" {
                    *total += 1;
                    continue;
                }
                if name == "__tombstones" {
                    continue;
                }
            }
            TreeMeasure::Bytes => {
                if entry.mode().is_blob() {
                    let blob = entry
                        .object_id()
                        .attach(repo)
                        .object()
                        .map_err(|e| Error::Other(format!("{e}")))?
                        .into_blob();
                    *total += blob.data.len() as u64;
                    continue;
                }
            }
        }
        if entry.mode().is_tree() {
            walk_uncached(repo, entry.object_id(), measure, total)?;
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
