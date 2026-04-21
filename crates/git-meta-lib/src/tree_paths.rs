//! Free functions for building Git tree paths from metadata targets and keys.
//!
//! These are serialization implementation details used by [`serialize`](crate::serialize)
//! and [`tree::format`](crate::tree::format) to map `(Target, key)` pairs into
//! filesystem-like paths inside Git trees.

use sha1::{Digest, Sha1};

use crate::error::Result;
use crate::types::{
    validate_key, Target, TargetType, LIST_VALUE_DIR, PATH_TARGET_SEPARATOR, SET_VALUE_DIR,
    STRING_VALUE_BLOB, TOMBSTONE_BLOB, TOMBSTONE_ROOT,
};

/// Split a `:`-separated key into individual path segments.
///
/// # Parameters
/// - `key`: the metadata key, e.g. `"agent:model:version"`
///
/// # Returns
/// A vector of segments, e.g. `["agent", "model", "version"]`.
pub(crate) fn key_to_path_segments(key: &str) -> Vec<String> {
    key.split(':')
        .map(std::string::ToString::to_string)
        .collect()
}

/// Compute a stable 2-char hex shard prefix from the SHA-1 of the target value.
fn value_shard_prefix(value: &str) -> String {
    let mut hasher = Sha1::new();
    hasher.update(value.as_bytes());
    let hash = format!("{:x}", hasher.finalize());
    hash[..2].to_string()
}

fn escape_path_target_segment(segment: &str) -> String {
    if segment.starts_with('~') || segment.starts_with("__") {
        format!("~{segment}")
    } else {
        segment.to_string()
    }
}

/// Encode a path target value by escaping reserved segments for safe tree storage.
pub(crate) fn encode_path_target_value(value: &str) -> String {
    value
        .split('/')
        .map(escape_path_target_segment)
        .collect::<Vec<_>>()
        .join("/")
}

/// Build the common tree path prefix for any key under a target.
///
/// Validates the key and joins the target base path with the key's
/// colon-separated segments turned into nested directories.
///
/// # Parameters
/// - `target`: the metadata target
/// - `key`: the metadata key name
///
/// # Errors
/// Returns an error if the key is invalid.
fn build_key_tree_path(target: &Target, key: &str) -> Result<String> {
    validate_key(key)?;
    let base = tree_base_path(target);
    let segments = key_to_path_segments(key).join("/");
    Ok(format!("{base}/{segments}"))
}

/// Build the tree base path for serialization.
///
/// Scheme per target type:
///   commit    -> `commit/{first2_of_sha}/{full_sha}`
///   path      -> `path/{escaped_path_segments...}/__target__`
///   others    -> `type/{first2_of_sha1(target_value)}/{full_target_value}`
///   project   -> `project`
///
/// # Parameters
/// - `target`: the metadata target
pub fn tree_base_path(target: &Target) -> String {
    match target.target_type() {
        TargetType::Project => "project".to_string(),
        TargetType::Commit => {
            let v = target.value().unwrap_or("");
            let first2 = &v[..2];
            format!("{}/{}/{}", target.target_type().as_str(), first2, v)
        }
        TargetType::Path => {
            let v = target.value().unwrap_or("");
            let encoded = encode_path_target_value(v);
            format!(
                "{}/{}/{}",
                target.target_type().as_str(),
                encoded,
                PATH_TARGET_SEPARATOR
            )
        }
        _ => {
            let v = target.value().unwrap_or("");
            let first2 = value_shard_prefix(v);
            format!("{}/{}/{}", target.target_type().as_str(), first2, v)
        }
    }
}

/// Build the full tree path for a string value.
///
/// # Parameters
/// - `target`: the metadata target
/// - `key`: the metadata key name
///
/// # Errors
/// Returns an error if the key is invalid.
pub fn tree_path(target: &Target, key: &str) -> Result<String> {
    let key_path = build_key_tree_path(target, key)?;
    Ok(format!("{key_path}/{STRING_VALUE_BLOB}"))
}

/// Build the common tree path prefix for any key.
///
/// # Parameters
/// - `target`: the metadata target
/// - `key`: the metadata key name
///
/// # Errors
/// Returns an error if the key is invalid.
#[cfg(feature = "internal")]
pub fn key_tree_path(target: &Target, key: &str) -> Result<String> {
    build_key_tree_path(target, key)
}

/// Build the list directory path for a key.
///
/// # Parameters
/// - `target`: the metadata target
/// - `key`: the metadata key name
///
/// # Errors
/// Returns an error if the key is invalid.
pub fn list_dir_path(target: &Target, key: &str) -> Result<String> {
    let key_path = build_key_tree_path(target, key)?;
    Ok(format!("{key_path}/{LIST_VALUE_DIR}"))
}

/// Build the set directory path for a key.
///
/// # Parameters
/// - `target`: the metadata target
/// - `key`: the metadata key name
///
/// # Errors
/// Returns an error if the key is invalid.
pub fn set_dir_path(target: &Target, key: &str) -> Result<String> {
    let key_path = build_key_tree_path(target, key)?;
    Ok(format!("{key_path}/{SET_VALUE_DIR}"))
}

/// Build the tombstone blob path for a key.
///
/// # Parameters
/// - `target`: the metadata target
/// - `key`: the metadata key name
///
/// # Errors
/// Returns an error if the key is invalid.
pub fn tombstone_path(target: &Target, key: &str) -> Result<String> {
    validate_key(key)?;
    let base = tree_base_path(target);
    let segments = key_to_path_segments(key).join("/");
    Ok(format!(
        "{base}/{TOMBSTONE_ROOT}/{segments}/{TOMBSTONE_BLOB}"
    ))
}

/// Build the tombstone path for a specific list entry.
///
/// # Parameters
/// - `target`: the metadata target
/// - `key`: the metadata key name
/// - `entry`: the list entry name
///
/// # Errors
/// Returns an error if the key is invalid.
pub fn list_entry_tombstone_path(target: &Target, key: &str, entry: &str) -> Result<String> {
    let key_path = build_key_tree_path(target, key)?;
    Ok(format!(
        "{key_path}/{LIST_VALUE_DIR}/{TOMBSTONE_ROOT}/{entry}"
    ))
}

/// Build the tombstone path for a specific set member.
///
/// # Parameters
/// - `target`: the metadata target
/// - `key`: the metadata key name
/// - `member`: the set member ID
///
/// # Errors
/// Returns an error if the key is invalid.
pub fn set_member_tombstone_path(target: &Target, key: &str, member: &str) -> Result<String> {
    let key_path = build_key_tree_path(target, key)?;
    Ok(format!("{key_path}/{TOMBSTONE_ROOT}/{member}"))
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn test_tree_base_path_commit() {
        let t = Target::parse("commit:13a7d29cde8f8557b54fd6474f547a56822180ae").unwrap();
        assert_eq!(
            tree_base_path(&t),
            "commit/13/13a7d29cde8f8557b54fd6474f547a56822180ae"
        );
    }

    #[test]
    fn test_tree_base_path_project() {
        let t = Target::parse("project").unwrap();
        assert_eq!(tree_base_path(&t), "project");
    }

    #[test]
    fn test_tree_path() {
        let t = Target::parse("commit:13a7d29cde8f8557b54fd6474f547a56822180ae").unwrap();
        let path = tree_path(&t, "agent:model").unwrap();
        assert_eq!(
            path,
            "commit/13/13a7d29cde8f8557b54fd6474f547a56822180ae/agent/model/__value"
        );
    }

    #[test]
    fn test_key_to_path_segments() {
        let segments = key_to_path_segments("agent:model:version");
        assert_eq!(segments, vec!["agent", "model", "version"]);
    }

    #[test]
    fn test_tree_base_path_branch() {
        let t = Target::parse("branch:sc-branch-1-deadbeef").unwrap();
        let expected_prefix = value_shard_prefix("sc-branch-1-deadbeef");
        assert_eq!(
            tree_base_path(&t),
            format!("branch/{expected_prefix}/sc-branch-1-deadbeef")
        );
    }

    #[test]
    fn test_tree_base_path_path_uses_raw_segments() {
        let t = Target::parse("path:src/main.rs").unwrap();
        assert_eq!(tree_base_path(&t), "path/src/main.rs/__target__");
    }

    #[test]
    fn test_tree_base_path_path_escapes_reserved_segments() {
        let t = Target::parse("path:src/__generated/file.rs").unwrap();
        assert_eq!(
            tree_base_path(&t),
            "path/src/~__generated/file.rs/__target__"
        );
    }

    #[test]
    fn test_list_dir_path() {
        let t = Target::parse("commit:13a7d29cde8f8557b54fd6474f547a56822180ae").unwrap();
        let path = list_dir_path(&t, "agent:chat").unwrap();
        assert_eq!(
            path,
            "commit/13/13a7d29cde8f8557b54fd6474f547a56822180ae/agent/chat/__list"
        );
    }

    #[test]
    fn test_tombstone_path() {
        let t = Target::parse("commit:13a7d29cde8f8557b54fd6474f547a56822180ae").unwrap();
        let path = tombstone_path(&t, "agent:chat").unwrap();
        assert_eq!(
            path,
            "commit/13/13a7d29cde8f8557b54fd6474f547a56822180ae/__tombstones/agent/chat/__deleted"
        );
    }
}
