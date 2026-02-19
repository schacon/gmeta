use anyhow::{Context, Result};
use git2::Repository;
use std::path::PathBuf;

/// Discover the Git repository from the current directory.
pub fn discover_repo() -> Result<Repository> {
    let repo = Repository::discover(".")
        .context("not a git repository (or any parent up to mount point)")?;
    Ok(repo)
}

/// Get the path to the gmeta SQLite database.
pub fn db_path(repo: &Repository) -> Result<PathBuf> {
    let git_dir = repo.path(); // .git/
    Ok(git_dir.join("gmeta.sqlite"))
}

/// Get the user's email from Git config.
pub fn get_email(repo: &Repository) -> Result<String> {
    let config = repo.config()?;
    let email = config
        .get_string("user.email")
        .unwrap_or_else(|_| "unknown".to_string());
    Ok(email)
}

/// Get the meta namespace from Git config (defaults to "meta").
pub fn get_namespace(repo: &Repository) -> Result<String> {
    let config = repo.config()?;
    let ns = config
        .get_string("meta.namespace")
        .unwrap_or_else(|_| "meta".to_string());
    Ok(ns)
}

/// Get the local ref name for serialization.
pub fn local_ref(repo: &Repository) -> Result<String> {
    let ns = get_namespace(repo)?;
    Ok(format!("refs/{}/local", ns))
}

/// Get the ref pattern for remote metadata.
#[allow(dead_code)]
pub fn remote_ref(repo: &Repository, remote: &str) -> Result<String> {
    let ns = get_namespace(repo)?;
    Ok(format!("refs/{}/{}", ns, remote))
}

/// Expand a partial commit SHA to the full 40-char hex string.
/// Returns an error if the SHA is ambiguous or not found.
pub fn resolve_commit_sha(repo: &Repository, partial: &str) -> Result<String> {
    let obj = repo
        .revparse_single(partial)
        .with_context(|| format!("could not resolve commit: {}", partial))?;
    let commit = obj
        .peel_to_commit()
        .with_context(|| format!("{} does not point to a commit", partial))?;
    Ok(commit.id().to_string())
}

/// Check if a tree entry name looks like a list entry (timestamp-hash format).
pub fn is_list_entry_name(name: &str) -> bool {
    // Format: {ms_epoch}-{first_5_sha256}
    if let Some((ts_part, hash_part)) = name.split_once('-') {
        ts_part.chars().all(|c| c.is_ascii_digit())
            && !ts_part.is_empty()
            && hash_part.len() == 5
            && hash_part.chars().all(|c| c.is_ascii_hexdigit())
    } else {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_list_entry_name() {
        assert!(is_list_entry_name("1771232450203-23c0f"));
        assert!(is_list_entry_name("1771232450204-0d5f2"));
        assert!(!is_list_entry_name("model"));
        assert!(!is_list_entry_name("agent"));
        assert!(!is_list_entry_name("123-toolong"));
        assert!(!is_list_entry_name("123-abc")); // 3 chars, not 5
        assert!(!is_list_entry_name("-23c0f")); // empty timestamp
    }
}
