use std::fmt;
use std::str::FromStr;

use sha1::{Digest, Sha1};

use crate::error::{Error, Result};

/// The kind of object a metadata entry is attached to.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
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
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Target {
    target_type: TargetType,
    value: Option<String>,
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
    /// Create a target from raw parts.
    ///
    /// This is a low-level constructor used when the target type and value are
    /// already known (e.g., when reconstructing targets from database rows or
    /// parsed tree entries). For user-facing construction, prefer the named
    /// constructors ([`commit()`](Self::commit), [`project()`](Self::project), etc.)
    /// or [`parse()`](Self::parse).
    ///
    /// # Parameters
    /// - `target_type`: the kind of target
    /// - `value`: the target value, or `None` for project targets
    #[must_use]
    pub fn from_parts(target_type: TargetType, value: Option<String>) -> Self {
        Target { target_type, value }
    }

    /// Create a commit target from a SHA (full or partial).
    ///
    /// # Parameters
    /// - `sha`: a commit SHA string, must be at least 3 characters.
    ///
    /// # Errors
    /// Returns an error if the SHA is shorter than 3 characters.
    pub fn commit(sha: &str) -> Result<Self> {
        Self::parse(&format!("commit:{sha}"))
    }

    /// Create a project-scoped target (no value needed).
    #[must_use]
    pub fn project() -> Self {
        Target {
            target_type: TargetType::Project,
            value: None,
        }
    }

    /// Create a path target.
    ///
    /// # Parameters
    /// - `path`: the file or directory path this metadata attaches to.
    #[must_use]
    pub fn path(path: &str) -> Self {
        Target {
            target_type: TargetType::Path,
            value: Some(path.to_string()),
        }
    }

    /// Create a branch target.
    ///
    /// # Parameters
    /// - `name`: the branch name this metadata attaches to.
    #[must_use]
    pub fn branch(name: &str) -> Self {
        Target {
            target_type: TargetType::Branch,
            value: Some(name.to_string()),
        }
    }

    /// Create a change-id target.
    ///
    /// # Parameters
    /// - `id`: the change identifier this metadata attaches to.
    #[must_use]
    pub fn change_id(id: &str) -> Self {
        Target {
            target_type: TargetType::ChangeId,
            value: Some(id.to_string()),
        }
    }

    /// Parse a target from a string in `type:value` format (e.g. `"commit:abc123"`).
    ///
    /// This is the CLI-oriented constructor. For programmatic use, prefer the
    /// named constructors: [`commit()`](Self::commit), [`project()`](Self::project),
    /// [`path()`](Self::path), [`branch()`](Self::branch), [`change_id()`](Self::change_id).
    ///
    /// # Parameters
    /// - `s`: the target string in `type:value` format, or `"project"` for project targets.
    ///
    /// # Errors
    /// Returns an error if the format is invalid, the target type is unknown,
    /// or the value is shorter than 3 characters.
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

    /// The type of this target (commit, branch, path, etc.).
    #[must_use]
    pub fn target_type(&self) -> &TargetType {
        &self.target_type
    }

    /// The target's value, if any.
    ///
    /// Returns `None` for project targets, `Some(sha)` for commit targets, etc.
    #[must_use]
    pub fn value(&self) -> Option<&str> {
        self.value.as_deref()
    }

    /// If this is a commit target with a partial SHA, expand it to 40 chars
    /// using the given Git repository. Returns a new target with the expanded SHA,
    /// or a clone of this target if no resolution is needed.
    pub fn resolve(&self, repo: &gix::Repository) -> Result<Target> {
        if self.target_type == TargetType::Commit {
            if let Some(ref v) = self.value {
                if v.len() < 40 {
                    let full = crate::git_utils::resolve_commit_sha(repo, v)?;
                    return Ok(Target {
                        target_type: self.target_type.clone(),
                        value: Some(full),
                    });
                }
            }
        }
        Ok(self.clone())
    }
}

/// The storage type of a metadata value.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
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
            MetaValue::String(s) => write!(f, "{s}"),
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

impl From<&str> for MetaValue {
    fn from(s: &str) -> Self {
        MetaValue::String(s.to_string())
    }
}

impl From<String> for MetaValue {
    fn from(s: String) -> Self {
        MetaValue::String(s)
    }
}

impl From<Vec<crate::list_value::ListEntry>> for MetaValue {
    fn from(entries: Vec<crate::list_value::ListEntry>) -> Self {
        MetaValue::List(entries)
    }
}

impl From<std::collections::BTreeSet<String>> for MetaValue {
    fn from(members: std::collections::BTreeSet<String>) -> Self {
        MetaValue::Set(members)
    }
}

/// Size threshold (in bytes) above which file values are stored as git blob references.
#[cfg(not(feature = "internal"))]
pub(crate) const GIT_REF_THRESHOLD: usize = 1024;
/// Size threshold (in bytes) above which file values are stored as git blob references.
#[cfg(feature = "internal")]
pub const GIT_REF_THRESHOLD: usize = 1024;

/// Reserved filename for string terminal values.
pub(crate) const STRING_VALUE_BLOB: &str = "__value";

/// Reserved directory name for list terminal values.
pub(crate) const LIST_VALUE_DIR: &str = "__list";

/// Reserved directory name for set terminal values.
pub(crate) const SET_VALUE_DIR: &str = "__set";

/// Reserved directory for tombstone entries.
pub(crate) const TOMBSTONE_ROOT: &str = "__tombstones";

/// Reserved filename for tombstone blobs.
pub(crate) const TOMBSTONE_BLOB: &str = "__deleted";

/// Reserved separator between a serialized path target and its key path.
pub(crate) const PATH_TARGET_SEPARATOR: &str = "__target__";

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
pub(crate) fn set_member_id(value: &str) -> String {
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
///
/// Called automatically by Store mutation methods. Library consumers do not
/// need to call this directly unless validating keys before passing them to
/// other systems.
#[cfg(not(feature = "internal"))]
pub(crate) fn validate_key(key: &str) -> Result<()> {
    validate_key_inner(key)
}

/// Validate that a metadata key can be serialized into the Git tree layout.
///
/// Called automatically by Store mutation methods. Library consumers do not
/// need to call this directly unless validating keys before passing them to
/// other systems.
#[cfg(feature = "internal")]
pub fn validate_key(key: &str) -> Result<()> {
    validate_key_inner(key)
}

fn validate_key_inner(key: &str) -> Result<()> {
    if key.is_empty() {
        return Err(Error::InvalidKey("key cannot be empty".into()));
    }
    for segment in key.split(':') {
        validate_key_segment(segment)?;
    }
    Ok(())
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

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_commit_target() {
        let t = Target::parse("commit:abc123").unwrap();
        assert_eq!(t.target_type(), &TargetType::Commit);
        assert_eq!(t.value(), Some("abc123"));
    }

    #[test]
    fn test_parse_project_target() {
        let t = Target::parse("project").unwrap();
        assert_eq!(t.target_type(), &TargetType::Project);
        assert_eq!(t.value(), None);
    }

    #[test]
    fn test_parse_path_target_with_colon_in_value() {
        // Only the first colon splits type from value
        let t = Target::parse("path:src/foo.rs").unwrap();
        assert_eq!(t.target_type(), &TargetType::Path);
        assert_eq!(t.value(), Some("src/foo.rs"));
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
    fn test_value_type_roundtrip() {
        assert_eq!("string".parse::<ValueType>().unwrap(), ValueType::String);
        assert_eq!("list".parse::<ValueType>().unwrap(), ValueType::List);
        assert_eq!("set".parse::<ValueType>().unwrap(), ValueType::Set);
        assert!("hash".parse::<ValueType>().is_err());
    }

    #[test]
    fn test_parse_branch_target() {
        let t = Target::parse("branch:sc-branch-1-deadbeef").unwrap();
        assert_eq!(t.target_type(), &TargetType::Branch);
        assert_eq!(t.value(), Some("sc-branch-1-deadbeef"));
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

    #[test]
    fn test_target_commit_constructor() {
        let t = Target::commit("abc123").unwrap();
        assert_eq!(t.target_type(), &TargetType::Commit);
        assert_eq!(t.value(), Some("abc123"));
    }

    #[test]
    fn test_target_commit_constructor_short_sha_rejected() {
        let result = Target::commit("ab");
        assert!(result.is_err());
    }

    #[test]
    fn test_target_project_constructor() {
        let t = Target::project();
        assert_eq!(t.target_type(), &TargetType::Project);
        assert_eq!(t.value(), None);
    }

    #[test]
    fn test_target_path_constructor() {
        let t = Target::path("src/main.rs");
        assert_eq!(t.target_type(), &TargetType::Path);
        assert_eq!(t.value(), Some("src/main.rs"));
    }

    #[test]
    fn test_target_branch_constructor() {
        let t = Target::branch("feature-x");
        assert_eq!(t.target_type(), &TargetType::Branch);
        assert_eq!(t.value(), Some("feature-x"));
    }

    #[test]
    fn test_target_change_id_constructor() {
        let t = Target::change_id("jj-change-abc");
        assert_eq!(t.target_type(), &TargetType::ChangeId);
        assert_eq!(t.value(), Some("jj-change-abc"));
    }

    #[test]
    fn test_named_constructors_match_parse() {
        // Verify named constructors produce identical results to parse
        let from_parse = Target::parse("commit:abc123").unwrap();
        let from_ctor = Target::commit("abc123").unwrap();
        assert_eq!(from_parse, from_ctor);

        let from_parse = Target::parse("project").unwrap();
        let from_ctor = Target::project();
        assert_eq!(from_parse, from_ctor);

        let from_parse = Target::parse("path:src/main.rs").unwrap();
        let from_ctor = Target::path("src/main.rs");
        assert_eq!(from_parse, from_ctor);

        let from_parse = Target::parse("branch:feature-x").unwrap();
        let from_ctor = Target::branch("feature-x");
        assert_eq!(from_parse, from_ctor);

        let from_parse = Target::parse("change-id:jj-change-abc").unwrap();
        let from_ctor = Target::change_id("jj-change-abc");
        assert_eq!(from_parse, from_ctor);
    }
}
