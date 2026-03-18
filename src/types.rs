use anyhow::{bail, Result};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

#[derive(Debug, Clone, PartialEq)]
pub enum TargetType {
    Commit,
    ChangeId,
    Branch,
    Path,
    Project,
}

impl TargetType {
    pub fn as_str(&self) -> &str {
        match self {
            TargetType::Commit => "commit",
            TargetType::ChangeId => "change-id",
            TargetType::Branch => "branch",
            TargetType::Path => "path",
            TargetType::Project => "project",
        }
    }

    fn from_str(s: &str) -> Result<Self> {
        match s {
            "commit" => Ok(TargetType::Commit),
            "change-id" => Ok(TargetType::ChangeId),
            "branch" => Ok(TargetType::Branch),
            "path" => Ok(TargetType::Path),
            "project" => Ok(TargetType::Project),
            _ => bail!("unknown target type: {}", s),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Target {
    pub target_type: TargetType,
    pub value: Option<String>,
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
            anyhow::anyhow!("target must be in type:value format (e.g. commit:abc123)")
        })?;

        let target_type = TargetType::from_str(type_str)?;

        if target_type == TargetType::Project {
            return Ok(Target {
                target_type,
                value: None,
            });
        }

        if value.len() < 3 {
            bail!("target value must be at least 3 characters, got: {}", value);
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
    pub fn resolve(&mut self, repo: &git2::Repository) -> anyhow::Result<()> {
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

    /// Build the tree base path for serialization.
    ///
    /// Scheme per target type:
    ///   commit    → commit/{first2_of_sha}/{full_sha}
    ///               SHA is already uniform; one prefix level is enough.
    ///   change-id → change-id/{first2_of_id}/{full_id}
    ///               Change IDs are opaque hex; already uniform.
    ///   branch    → branch/{first2_of_hash(name)}/{name}
    ///               Branch names cluster on prefixes (feature/, fix/, …);
    ///               hashing guarantees uniform shard distribution.
    ///   path      → path/{path}
    ///               The path itself is already hierarchical; git's tree
    ///               structure provides natural directory-level fanout for free.
    ///   project   → project   (singleton, no sharding)
    pub fn tree_base_path(&self) -> String {
        match self.target_type {
            TargetType::Project => "project".to_string(),

            TargetType::Commit | TargetType::ChangeId => {
                let v = self.value.as_deref().unwrap_or("");
                let first2 = &v[..2];
                format!("{}/{}/{}", self.type_str(), first2, v)
            }

            TargetType::Branch => {
                let v = self.value.as_deref().unwrap_or("");
                let first2 = branch_shard_prefix(v);
                format!("{}/{}/{}", self.type_str(), first2, v)
            }

            TargetType::Path => {
                let v = self.value.as_deref().unwrap_or("");
                format!("{}/{}", self.type_str(), v)
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum ValueType {
    String,
    List,
}

impl ValueType {
    pub fn as_str(&self) -> &str {
        match self {
            ValueType::String => "string",
            ValueType::List => "list",
        }
    }

    pub fn from_str(s: &str) -> Result<Self> {
        match s {
            "string" => Ok(ValueType::String),
            "list" => Ok(ValueType::List),
            _ => bail!("unknown value type: {}", s),
        }
    }
}

/// Size threshold (in bytes) above which file values are stored as git blob references.
pub const GIT_REF_THRESHOLD: usize = 1024;

/// Root directory used to store key segments in serialized trees.
pub const KEY_TREE_ROOT: &str = "k";

/// Reserved filename for string terminal values.
pub const STRING_VALUE_BLOB: &str = "__value";

/// Reserved directory name for list terminal values.
pub const LIST_VALUE_DIR: &str = "__list";

/// Reserved directory for tombstone entries.
pub const TOMBSTONE_ROOT: &str = "__tombstones";

/// Reserved filename for tombstone blobs.
pub const TOMBSTONE_BLOB: &str = "__deleted";

/// Compute a stable 2-char hex shard prefix for a branch name by hashing it.
/// Branch names cluster on their textual prefix (feature/, fix/, sc-, …) so
/// we hash first to get a uniform distribution across the 256 buckets.
fn branch_shard_prefix(name: &str) -> String {
    let mut h = DefaultHasher::new();
    name.hash(&mut h);
    let n = h.finish();
    format!("{:02x}", (n & 0xff) as u8)
}

fn validate_key_segment(segment: &str) -> Result<()> {
    if segment.is_empty() {
        bail!("key segments cannot be empty");
    }
    if segment == "." || segment == ".." {
        bail!("key segment '{}' is not allowed", segment);
    }
    if segment.contains('/') {
        bail!("key segment '{}' must not contain '/'", segment);
    }
    if segment.contains('\0') {
        bail!("key segment '{}' must not contain null byte", segment);
    }
    if segment == KEY_TREE_ROOT || segment == STRING_VALUE_BLOB || segment == LIST_VALUE_DIR {
        bail!("key segment '{}' is reserved", segment);
    }
    Ok(())
}

/// Validate that a metadata key can be serialized into the Git tree layout.
pub fn validate_key(key: &str) -> Result<()> {
    if key.is_empty() {
        bail!("key cannot be empty");
    }
    for segment in key.split(':') {
        validate_key_segment(segment)?;
    }
    Ok(())
}

/// Build the full tree path segments for a key under a target.
/// Key is split by ':' into subtree segments.
#[allow(dead_code)]
pub fn key_to_path_segments(key: &str) -> Vec<String> {
    key.split(':').map(|s| s.to_string()).collect()
}

/// Decode raw key path segments back into `:`-namespaced key form.
pub fn decode_key_path_segments(segments: &[&str]) -> Result<String> {
    if segments.is_empty() {
        bail!("key path must include at least one key segment");
    }
    let mut decoded = Vec::with_capacity(segments.len());
    for segment in segments {
        validate_key_segment(segment)?;
        decoded.push((*segment).to_string());
    }
    Ok(decoded.join(":"))
}

/// Build the common tree path prefix for any key (string or list).
pub fn build_key_tree_path(target: &Target, key: &str) -> Result<String> {
    validate_key(key)?;
    let base = target.tree_base_path();
    let segments = key_to_path_segments(key).join("/");
    Ok(format!("{}/{}/{}", base, KEY_TREE_ROOT, segments))
}

/// Build the full tree path for a string value.
#[allow(dead_code)]
pub fn build_tree_path(target: &Target, key: &str) -> Result<String> {
    let key_path = build_key_tree_path(target, key)?;
    Ok(format!("{}/{}", key_path, STRING_VALUE_BLOB))
}

/// Build the list directory path for a key.
#[allow(dead_code)]
pub fn build_list_tree_dir_path(target: &Target, key: &str) -> Result<String> {
    let key_path = build_key_tree_path(target, key)?;
    Ok(format!("{}/{}", key_path, LIST_VALUE_DIR))
}

/// Build the tombstone blob path for a key.
#[allow(dead_code)]
pub fn build_tombstone_tree_path(target: &Target, key: &str) -> Result<String> {
    validate_key(key)?;
    let base = target.tree_base_path();
    let segments = key_to_path_segments(key).join("/");
    Ok(format!(
        "{}/{}/{}/{}/{}",
        base, TOMBSTONE_ROOT, KEY_TREE_ROOT, segments, TOMBSTONE_BLOB
    ))
}

#[cfg(test)]
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
    fn test_build_tree_path() {
        let t = Target::parse("commit:13a7d29cde8f8557b54fd6474f547a56822180ae").unwrap();
        let path = build_tree_path(&t, "agent:model").unwrap();
        assert_eq!(
            path,
            "commit/13/13a7d29cde8f8557b54fd6474f547a56822180ae/k/agent/model/__value"
        );
    }

    #[test]
    fn test_key_to_path_segments() {
        let segments = key_to_path_segments("agent:model:version");
        assert_eq!(segments, vec!["agent", "model", "version"]);
    }

    #[test]
    fn test_value_type_roundtrip() {
        assert_eq!(ValueType::from_str("string").unwrap(), ValueType::String);
        assert_eq!(ValueType::from_str("list").unwrap(), ValueType::List);
        assert!(ValueType::from_str("hash").is_err());
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
        // shard prefix is hash("sc-branch-1-deadbeef") & 0xff, formatted as 2-char hex
        let expected_prefix = super::branch_shard_prefix("sc-branch-1-deadbeef");
        assert_eq!(
            t.tree_base_path(),
            format!("branch/{}/sc-branch-1-deadbeef", expected_prefix)
        );
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
        assert!(super::validate_key("k:model").is_err());
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
    fn test_build_list_tree_dir_path() {
        let t = Target::parse("commit:13a7d29cde8f8557b54fd6474f547a56822180ae").unwrap();
        let path = super::build_list_tree_dir_path(&t, "agent:chat").unwrap();
        assert_eq!(
            path,
            "commit/13/13a7d29cde8f8557b54fd6474f547a56822180ae/k/agent/chat/__list"
        );
    }

    #[test]
    fn test_build_tombstone_tree_path() {
        let t = Target::parse("commit:13a7d29cde8f8557b54fd6474f547a56822180ae").unwrap();
        let path = super::build_tombstone_tree_path(&t, "agent:chat").unwrap();
        assert_eq!(
            path,
            "commit/13/13a7d29cde8f8557b54fd6474f547a56822180ae/__tombstones/k/agent/chat/__deleted"
        );
    }
}
