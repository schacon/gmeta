use anyhow::{bail, Result};

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

        let (type_str, value) = s
            .split_once(':')
            .ok_or_else(|| anyhow::anyhow!("target must be in type:value format (e.g. commit:abc123)"))?;

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
    /// Format: {type}/{first_2}/{last_3}/{full_value}
    /// For project: just "project"
    pub fn tree_base_path(&self) -> String {
        match &self.value {
            None => self.type_str().to_string(),
            Some(v) => {
                let first2 = &v[..2];
                let last3 = &v[v.len() - 3..];
                format!("{}/{}/{}/{}", self.type_str(), first2, last3, v)
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

/// Build the full tree path for a key under a target.
/// Key is split by ':' into subtree segments.
#[allow(dead_code)]
pub fn key_to_path_segments(key: &str) -> Vec<String> {
    key.split(':').map(|s| s.to_string()).collect()
}

/// Build the full tree path for a string value.
#[allow(dead_code)]
pub fn build_tree_path(target: &Target, key: &str) -> String {
    let base = target.tree_base_path();
    let key_path = key.replace(':', "/");
    format!("{}/{}", base, key_path)
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
            "commit/13/0ae/13a7d29cde8f8557b54fd6474f547a56822180ae"
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
        let path = build_tree_path(&t, "agent:model");
        assert_eq!(
            path,
            "commit/13/0ae/13a7d29cde8f8557b54fd6474f547a56822180ae/agent/model"
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
        assert_eq!(
            t.tree_base_path(),
            "branch/sc/eef/sc-branch-1-deadbeef"
        );
    }
}
