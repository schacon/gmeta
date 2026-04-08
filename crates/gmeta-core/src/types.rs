use std::fmt;
use std::str::FromStr;

use sha1::{Digest, Sha1};

use crate::error::{Error, Result};

/// The kind of object a metadata entry is attached to.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum TargetType {
    Commit,
    ChangeId,
    Branch,
    Path,
    Project,
}

impl fmt::Display for TargetType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for TargetType {
    type Err = Error;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s {
            "commit" => Ok(TargetType::Commit),
            "change-id" => Ok(TargetType::ChangeId),
            "branch" => Ok(TargetType::Branch),
            "path" => Ok(TargetType::Path),
            "project" => Ok(TargetType::Project),
            _ => Err(Error::UnknownTargetType(s.to_string())),
        }
    }
}

impl TargetType {
    /// Returns the wire-format string for this target type.
    pub fn as_str(&self) -> &str {
        match self {
            TargetType::Commit => "commit",
            TargetType::ChangeId => "change-id",
            TargetType::Branch => "branch",
            TargetType::Path => "path",
            TargetType::Project => "project",
        }
    }

    /// Returns the English plural form of this target type for display.
    pub fn pluralize(&self) -> &str {
        match self {
            TargetType::Commit => "commits",
            TargetType::ChangeId => "change-ids",
            TargetType::Branch => "branches",
            TargetType::Path => "paths",
            TargetType::Project => "project",
        }
    }
}

/// A resolved metadata target consisting of a type and an optional value.
#[derive(Debug, Clone, PartialEq)]
pub struct Target {
    pub target_type: TargetType,
    pub value: Option<String>,
}

impl fmt::Display for Target {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.value {
            Some(v) => write!(f, "{}:{}", self.target_type, v),
            None => write!(f, "{}", self.target_type),
        }
    }
}

impl Target {
    pub fn parse(s: &str) -> Result<Self> {
        if s == "project" {
            return Ok(Target {
                target_type: TargetType::Project,
                value: None,
            });
        }

        let (type_str, value) = s.split_once(':').ok_or_else(|| {
            Error::InvalidTarget("target must be in type:value format (e.g. commit:abc123)".into())
        })?;

        let target_type = type_str.parse::<TargetType>()?;

        if target_type == TargetType::Project {
            return Ok(Target {
                target_type,
                value: None,
            });
        }

        if value.len() < 3 {
            return Err(Error::InvalidTarget(format!(
                "target value must be at least 3 characters, got: {value}"
            )));
        }

        Ok(Target {
            target_type,
            value: Some(value.to_string()),
        })
    }

    pub fn type_str(&self) -> &str {
        self.target_type.as_str()
    }

    pub fn value_str(&self) -> &str {
        self.value.as_deref().unwrap_or("")
    }

    /// If this is a commit target with a partial SHA, expand it to 40 chars
    /// using the given Git repository.
    pub fn resolve(&mut self, repo: &gix::Repository) -> Result<()> {
        if self.target_type == TargetType::Commit {
            if let Some(ref v) = self.value {
                if v.len() < 40 {
                    let full = crate::git_utils::resolve_commit_sha(repo, v)?;
                    self.value = Some(full);
                }
            }
        }
        Ok(())
    }

    /// Build the common tree path prefix for any key.
    ///
    /// # Parameters
    /// - `key`: the metadata key name
    ///
    /// # Errors
    /// Returns an error if the key is invalid.
    pub fn key_tree_path(&self, key: &str) -> Result<String> {
        build_key_tree_path(self, key)
    }

    /// Build the full tree path for a string value.
    ///
    /// # Parameters
    /// - `key`: the metadata key name
    ///
    /// # Errors
    /// Returns an error if the key is invalid.
    pub fn tree_path(&self, key: &str) -> Result<String> {
        let key_path = build_key_tree_path(self, key)?;
        Ok(format!("{}/{}", key_path, STRING_VALUE_BLOB))
    }

    /// Build the list directory path for a key.
    ///
    /// # Parameters
    /// - `key`: the metadata key name
    ///
    /// # Errors
    /// Returns an error if the key is invalid.
    pub fn list_dir_path(&self, key: &str) -> Result<String> {
        let key_path = build_key_tree_path(self, key)?;
        Ok(format!("{}/{}", key_path, LIST_VALUE_DIR))
    }

    /// Build the set directory path for a key.
    ///
    /// # Parameters
    /// - `key`: the metadata key name
    ///
    /// # Errors
    /// Returns an error if the key is invalid.
    pub fn set_dir_path(&self, key: &str) -> Result<String> {
        let key_path = build_key_tree_path(self, key)?;
        Ok(format!("{}/{}", key_path, SET_VALUE_DIR))
    }

    /// Build the tombstone blob path for a key.
    ///
    /// # Parameters
    /// - `key`: the metadata key name
    ///
    /// # Errors
    /// Returns an error if the key is invalid.
    pub fn tombstone_path(&self, key: &str) -> Result<String> {
        validate_key(key)?;
        let base = self.tree_base_path();
        let segments = key_to_path_segments(key).join("/");
        Ok(format!(
            "{}/{}/{}/{}",
            base, TOMBSTONE_ROOT, segments, TOMBSTONE_BLOB
        ))
    }

    /// Build the tombstone path for a specific list entry.
    ///
    /// # Parameters
    /// - `key`: the metadata key name
    /// - `entry`: the list entry name
    ///
    /// # Errors
    /// Returns an error if the key is invalid.
    pub fn list_entry_tombstone_path(&self, key: &str, entry: &str) -> Result<String> {
        let key_path = build_key_tree_path(self, key)?;
        Ok(format!(
            "{}/{}/{}/{}",
            key_path, LIST_VALUE_DIR, TOMBSTONE_ROOT, entry
        ))
    }

    /// Build the tombstone path for a specific set member.
    ///
    /// # Parameters
    /// - `key`: the metadata key name
    /// - `member`: the set member ID
    ///
    /// # Errors
    /// Returns an error if the key is invalid.
    pub fn set_member_tombstone_path(&self, key: &str, member: &str) -> Result<String> {
        let key_path = build_key_tree_path(self, key)?;
        Ok(format!("{}/{}/{}", key_path, TOMBSTONE_ROOT, member))
    }

    /// Build the tree base path for serialization.
    ///
    /// Scheme per target type:
    ///   commit    → commit/{first2_of_sha}/{full_sha}
    ///   path      → path/{escaped_path_segments...}/__target__
    ///   others    → type/{first2_of_sha1(target_value)}/{full_target_value}
    ///   project   → project
    pub fn tree_base_path(&self) -> String {
        match self.target_type {
            TargetType::Project => "project".to_string(),
            TargetType::Commit => {
                let v = self.value.as_deref().unwrap_or("");
                let first2 = &v[..2];
                format!("{}/{}/{}", self.type_str(), first2, v)
            }
            TargetType::Path => {
                let v = self.value.as_deref().unwrap_or("");
                let encoded = encode_path_target_value(v);
                format!("{}/{}/{}", self.type_str(), encoded, PATH_TARGET_SEPARATOR)
            }
            _ => {
                let v = self.value.as_deref().unwrap_or("");
                let first2 = value_shard_prefix(v);
                format!("{}/{}/{}", self.type_str(), first2, v)
            }
        }
    }
}

/// The storage type of a metadata value.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ValueType {
    String,
    List,
    Set,
}

impl fmt::Display for ValueType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for ValueType {
    type Err = Error;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s {
            "string" => Ok(ValueType::String),
            "list" => Ok(ValueType::List),
            "set" => Ok(ValueType::Set),
            _ => Err(Error::UnknownValueType(s.to_string())),
        }
    }
}

impl ValueType {
    /// Returns the wire-format string for this value type.
    pub fn as_str(&self) -> &str {
        match self {
            ValueType::String => "string",
            ValueType::List => "list",
            ValueType::Set => "set",
        }
    }
}

/// A metadata value with its type.
///
/// Combines value content with type information so they cannot get out of sync.
/// Used as both input to [`Store::set()`](crate::db::Store::set) and output
/// from [`Store::get()`](crate::db::Store::get).
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum MetaValue {
    /// A single string value.
    String(String),
    /// An ordered list of timestamped entries.
    List(Vec<crate::list_value::ListEntry>),
    /// An unordered set of unique string values.
    Set(std::collections::BTreeSet<String>),
}

impl fmt::Display for MetaValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MetaValue::String(s) => write!(f, "{}", s),
            MetaValue::List(entries) => write!(f, "[{} entries]", entries.len()),
            MetaValue::Set(members) => write!(f, "{{{} members}}", members.len()),
        }
    }
}

impl MetaValue {
    /// Returns the corresponding [`ValueType`].
    #[must_use]
    pub fn value_type(&self) -> ValueType {
        match self {
            MetaValue::String(_) => ValueType::String,
            MetaValue::List(_) => ValueType::List,
            MetaValue::Set(_) => ValueType::Set,
        }
    }
}

/// Size threshold (in bytes) above which file values are stored as git blob references.
pub const GIT_REF_THRESHOLD: usize = 1024;

/// Reserved filename for string terminal values.
pub const STRING_VALUE_BLOB: &str = "__value";

/// Reserved directory name for list terminal values.
pub const LIST_VALUE_DIR: &str = "__list";

/// Reserved directory name for set terminal values.
pub const SET_VALUE_DIR: &str = "__set";

/// Reserved directory for tombstone entries.
pub const TOMBSTONE_ROOT: &str = "__tombstones";

/// Reserved filename for tombstone blobs.
pub(crate) const TOMBSTONE_BLOB: &str = "__deleted";

/// Reserved separator between a serialized path target and its key path.
pub const PATH_TARGET_SEPARATOR: &str = "__target__";

/// Compute a stable 2-char hex shard prefix from the SHA-1 of the target value.
fn value_shard_prefix(value: &str) -> String {
    let mut hasher = Sha1::new();
    hasher.update(value.as_bytes());
    let hash = format!("{:x}", hasher.finalize());
    hash[..2].to_string()
}

fn escape_path_target_segment(segment: &str) -> String {
    if segment.starts_with('~') || segment.starts_with("__") {
        format!("~{}", segment)
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

/// Decode escaped path target segments back into a slash-separated path string.
pub(crate) fn decode_path_target_segments(segments: &[&str]) -> Result<String> {
    if segments.is_empty() {
        return Err(Error::InvalidTreePath(
            "path target must include at least one segment".into(),
        ));
    }

    let decoded = segments
        .iter()
        .map(|segment| {
            if let Some(rest) = segment.strip_prefix('~') {
                rest.to_string()
            } else {
                (*segment).to_string()
            }
        })
        .collect::<Vec<_>>()
        .join("/");

    Ok(decoded)
}

/// Compute a deterministic set member ID by hashing the value as a git blob.
pub fn set_member_id(value: &str) -> String {
    let header = format!("blob {}\0", value.len());
    let mut hasher = Sha1::new();
    hasher.update(header.as_bytes());
    hasher.update(value.as_bytes());
    format!("{:x}", hasher.finalize())
}

fn validate_key_segment(segment: &str) -> Result<()> {
    if segment.is_empty() {
        return Err(Error::InvalidKey("key segments cannot be empty".into()));
    }
    if segment == "." || segment == ".." {
        return Err(Error::InvalidKey(format!(
            "key segment '{segment}' is not allowed"
        )));
    }
    if segment.contains('/') {
        return Err(Error::InvalidKey(format!(
            "key segment '{segment}' must not contain '/'"
        )));
    }
    if segment.contains('\0') {
        return Err(Error::InvalidKey(format!(
            "key segment '{segment}' must not contain null byte"
        )));
    }
    if segment.starts_with("__")
        || segment == STRING_VALUE_BLOB
        || segment == LIST_VALUE_DIR
        || segment == SET_VALUE_DIR
    {
        return Err(Error::InvalidKey(format!(
            "key segment '{segment}' is reserved"
        )));
    }
    Ok(())
}

/// Validate that a metadata key can be serialized into the Git tree layout.
pub fn validate_key(key: &str) -> Result<()> {
    if key.is_empty() {
        return Err(Error::InvalidKey("key cannot be empty".into()));
    }
    for segment in key.split(':') {
        validate_key_segment(segment)?;
    }
    Ok(())
}

/// Build the full tree path segments for a key under a target.
/// Key is split by ':' into subtree segments.
pub(crate) fn key_to_path_segments(key: &str) -> Vec<String> {
    key.split(':').map(|s| s.to_string()).collect()
}

/// Decode raw key path segments back into `:`-namespaced key form.
pub(crate) fn decode_key_path_segments(segments: &[&str]) -> Result<String> {
    if segments.is_empty() {
        return Err(Error::InvalidKey(
            "key path must include at least one key segment".into(),
        ));
    }
    let mut decoded = Vec::with_capacity(segments.len());
    for segment in segments {
        validate_key_segment(segment)?;
        decoded.push((*segment).to_string());
    }
    Ok(decoded.join(":"))
}

/// Build the common tree path prefix for any key.
fn build_key_tree_path(target: &Target, key: &str) -> Result<String> {
    validate_key(key)?;
    let base = target.tree_base_path();
    let segments = key_to_path_segments(key).join("/");
    Ok(format!("{}/{}", base, segments))
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_commit_target() {
        let t = Target::parse("commit:abc123").unwrap();
        assert_eq!(t.target_type, TargetType::Commit);
        assert_eq!(t.value, Some("abc123".to_string()));
    }

    #[test]
    fn test_parse_project_target() {
        let t = Target::parse("project").unwrap();
        assert_eq!(t.target_type, TargetType::Project);
        assert_eq!(t.value, None);
    }

    #[test]
    fn test_parse_path_target_with_colon_in_value() {
        // Only the first colon splits type from value
        let t = Target::parse("path:src/foo.rs").unwrap();
        assert_eq!(t.target_type, TargetType::Path);
        assert_eq!(t.value, Some("src/foo.rs".to_string()));
    }

    #[test]
    fn test_parse_short_value_rejected() {
        let result = Target::parse("commit:ab");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_unknown_type_rejected() {
        let result = Target::parse("unknown:abc123");
        assert!(result.is_err());
    }

    #[test]
    fn test_tree_base_path_commit() {
        let t = Target::parse("commit:13a7d29cde8f8557b54fd6474f547a56822180ae").unwrap();
        assert_eq!(
            t.tree_base_path(),
            "commit/13/13a7d29cde8f8557b54fd6474f547a56822180ae"
        );
    }

    #[test]
    fn test_tree_base_path_project() {
        let t = Target::parse("project").unwrap();
        assert_eq!(t.tree_base_path(), "project");
    }

    #[test]
    fn test_tree_path() {
        let t = Target::parse("commit:13a7d29cde8f8557b54fd6474f547a56822180ae").unwrap();
        let path = t.tree_path("agent:model").unwrap();
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
    fn test_value_type_roundtrip() {
        assert_eq!("string".parse::<ValueType>().unwrap(), ValueType::String);
        assert_eq!("list".parse::<ValueType>().unwrap(), ValueType::List);
        assert_eq!("set".parse::<ValueType>().unwrap(), ValueType::Set);
        assert!("hash".parse::<ValueType>().is_err());
    }

    #[test]
    fn test_parse_branch_target() {
        let t = Target::parse("branch:sc-branch-1-deadbeef").unwrap();
        assert_eq!(t.target_type, TargetType::Branch);
        assert_eq!(t.value, Some("sc-branch-1-deadbeef".to_string()));
    }

    #[test]
    fn test_tree_base_path_branch() {
        let t = Target::parse("branch:sc-branch-1-deadbeef").unwrap();
        let expected_prefix = super::value_shard_prefix("sc-branch-1-deadbeef");
        assert_eq!(
            t.tree_base_path(),
            format!("branch/{}/sc-branch-1-deadbeef", expected_prefix)
        );
    }

    #[test]
    fn test_tree_base_path_path_uses_raw_segments() {
        let t = Target::parse("path:src/main.rs").unwrap();
        assert_eq!(t.tree_base_path(), "path/src/main.rs/__target__");
    }

    #[test]
    fn test_tree_base_path_path_escapes_reserved_segments() {
        let t = Target::parse("path:src/__generated/file.rs").unwrap();
        assert_eq!(
            t.tree_base_path(),
            "path/src/~__generated/file.rs/__target__"
        );
    }

    #[test]
    fn test_decode_path_target_segments() {
        let decoded =
            super::decode_path_target_segments(&["src", "~__generated", "file.rs"]).unwrap();
        assert_eq!(decoded, "src/__generated/file.rs");
    }

    #[test]
    fn test_decode_key_path_segments() {
        let decoded = super::decode_key_path_segments(&["agent", "model"]).unwrap();
        assert_eq!(decoded, "agent:model");
    }

    #[test]
    fn test_validate_key_rejects_reserved_segments() {
        assert!(super::validate_key("agent:__value").is_err());
        assert!(super::validate_key("__list:chat").is_err());
        assert!(super::validate_key("__custom:model").is_err());
    }

    #[test]
    fn test_validate_key_rejects_unsafe_segments() {
        assert!(super::validate_key("agent:/model").is_err());
        assert!(super::validate_key("agent::model").is_err());
        assert!(super::validate_key("agent:.").is_err());
        assert!(super::validate_key("agent:..").is_err());
    }

    #[test]
    fn test_validate_key_accepts_normal_segments() {
        assert!(super::validate_key("agent:model:version").is_ok());
    }

    #[test]
    fn test_list_dir_path() {
        let t = Target::parse("commit:13a7d29cde8f8557b54fd6474f547a56822180ae").unwrap();
        let path = t.list_dir_path("agent:chat").unwrap();
        assert_eq!(
            path,
            "commit/13/13a7d29cde8f8557b54fd6474f547a56822180ae/agent/chat/__list"
        );
    }

    #[test]
    fn test_tombstone_path() {
        let t = Target::parse("commit:13a7d29cde8f8557b54fd6474f547a56822180ae").unwrap();
        let path = t.tombstone_path("agent:chat").unwrap();
        assert_eq!(
            path,
            "commit/13/13a7d29cde8f8557b54fd6474f547a56822180ae/__tombstones/agent/chat/__deleted"
        );
    }

    #[test]
    fn test_meta_value_string_type() {
        let v = MetaValue::String("hello".to_string());
        assert_eq!(v.value_type(), ValueType::String);
    }

    #[test]
    fn test_meta_value_list_type() {
        let v = MetaValue::List(vec![crate::list_value::ListEntry {
            value: "item".to_string(),
            timestamp: 1000,
        }]);
        assert_eq!(v.value_type(), ValueType::List);
    }

    #[test]
    fn test_meta_value_set_type() {
        let mut s = std::collections::BTreeSet::new();
        s.insert("a".to_string());
        s.insert("b".to_string());
        let v = MetaValue::Set(s);
        assert_eq!(v.value_type(), ValueType::Set);
    }

    #[test]
    fn test_meta_value_empty_list_type() {
        let v = MetaValue::List(vec![]);
        assert_eq!(v.value_type(), ValueType::List);
    }

    #[test]
    fn test_meta_value_empty_set_type() {
        let v = MetaValue::Set(std::collections::BTreeSet::new());
        assert_eq!(v.value_type(), ValueType::Set);
    }

    #[test]
    fn test_meta_value_clone_eq() {
        let v1 = MetaValue::String("test".to_string());
        let v2 = v1.clone();
        assert_eq!(v1, v2);
    }
}
