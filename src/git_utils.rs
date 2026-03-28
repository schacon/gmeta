use anyhow::{bail, Context, Result};
use git2::Repository;
use std::path::PathBuf;
use std::process::Command;

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

/// Get the user's name from Git config.
pub fn get_name(repo: &Repository) -> Result<String> {
    let config = repo.config()?;
    let name = config
        .get_string("user.name")
        .unwrap_or_else(|_| "unknown".to_string());
    Ok(name)
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
    Ok(format!("refs/{}/local/main", ns))
}

/// Get the ref name for a named destination (e.g. "private" -> "refs/meta/local/private").
pub fn destination_ref(repo: &Repository, destination: &str) -> Result<String> {
    let ns = get_namespace(repo)?;
    Ok(format!("refs/{}/local/{}", ns, destination))
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

/// Run a git CLI command in the repository's working directory.
/// Returns stdout on success, or an error with stderr on failure.
pub fn run_git(repo: &Repository, args: &[&str]) -> Result<String> {
    let workdir = repo
        .workdir()
        .or_else(|| Some(repo.path()))
        .context("cannot determine repository directory")?;

    let output = Command::new("git")
        .args(args)
        .current_dir(workdir)
        .output()
        .context("failed to run git command")?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        bail!("git {} failed: {}", args.first().unwrap_or(&""), stderr.trim());
    }

    Ok(String::from_utf8_lossy(&output.stdout).to_string())
}

/// List all git remotes that have `meta = true` in their config.
/// Returns a vec of (name, url) pairs.
pub fn list_meta_remotes(repo: &Repository) -> Result<Vec<(String, String)>> {
    let config = repo.config()?;
    let mut remotes = Vec::new();

    // Get all remote names from the repo
    let remote_names = repo.remotes()?;
    for name in remote_names.iter().flatten() {
        let meta_key = format!("remote.{}.meta", name);
        if let Ok(true) = config.get_bool(&meta_key) {
            let url_key = format!("remote.{}.url", name);
            if let Ok(url) = config.get_string(&url_key) {
                remotes.push((name.to_string(), url));
            }
        }
    }

    Ok(remotes)
}

/// Hydrate tip tree blobs for a blobless-fetched ref.
/// This fetches all blob objects referenced by the tip tree so libgit2 can read them.
pub fn hydrate_tip_blobs(repo: &Repository, remote_name: &str, ref_name: &str) -> Result<()> {
    let blob_list = run_git(repo, &["ls-tree", "-r", "--object-only", ref_name]);

    if let Ok(blobs) = blob_list {
        if !blobs.trim().is_empty() {
            let workdir = repo
                .workdir()
                .or_else(|| Some(repo.path()))
                .context("cannot determine repository directory")?;

            let mut child = Command::new("git")
                .args([
                    "-c",
                    "fetch.negotiationAlgorithm=noop",
                    "fetch",
                    remote_name,
                    "--no-tags",
                    "--no-write-fetch-head",
                    "--recurse-submodules=no",
                    "--filter=blob:none",
                    "--stdin",
                ])
                .current_dir(workdir)
                .stdin(std::process::Stdio::piped())
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::piped())
                .spawn()?;

            if let Some(mut stdin) = child.stdin.take() {
                use std::io::Write;
                stdin.write_all(blobs.as_bytes())?;
            }

            let output = child.wait_with_output()?;
            if !output.status.success() {
                let stderr = String::from_utf8_lossy(&output.stderr);
                eprintln!("Warning: blob hydration failed: {}", stderr.trim());
            }
        }
    }

    Ok(())
}

/// Resolve a meta remote by name, or pick the first one if no name given.
pub fn resolve_meta_remote(repo: &Repository, remote: Option<&str>) -> Result<String> {
    let meta_remotes = list_meta_remotes(repo)?;

    if meta_remotes.is_empty() {
        bail!("no metadata remotes configured. Add one with: gmeta remote add <url>");
    }

    match remote {
        Some(name) => {
            if meta_remotes.iter().any(|(n, _)| n == name) {
                Ok(name.to_string())
            } else {
                bail!(
                    "'{}' is not a metadata remote. Available: {}",
                    name,
                    meta_remotes
                        .iter()
                        .map(|(n, _)| n.as_str())
                        .collect::<Vec<_>>()
                        .join(", ")
                );
            }
        }
        None => Ok(meta_remotes[0].0.clone()),
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
