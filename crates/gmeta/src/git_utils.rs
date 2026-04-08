use std::path::PathBuf;
use std::process::Command;

use crate::error::{Error, Result};

/// Check if a tree entry name looks like a list entry (timestamp-hash format).
pub(crate) fn is_list_entry_name(name: &str) -> bool {
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

/// Resolve the working or git directory from a gix repository for subprocess calls.
fn repo_dir(repo: &gix::Repository) -> Result<&std::path::Path> {
    repo.workdir()
        .unwrap_or_else(|| repo.git_dir())
        .canonicalize()
        .ok();
    Ok(repo.workdir().unwrap_or_else(|| repo.git_dir()))
}

/// Run a git CLI command in the repository's working directory.
///
/// # Parameters
///
/// - `repo`: the Git repository whose working directory is used as `cwd`
/// - `args`: the arguments to pass to `git`
///
/// # Returns
///
/// The stdout output of the command as a string.
///
/// # Errors
///
/// Returns an error if the subprocess fails to spawn or exits with a non-zero status.
pub fn run_git(repo: &gix::Repository, args: &[&str]) -> Result<String> {
    let workdir = repo_dir(repo)?;

    let output = Command::new("git")
        .args(args)
        .current_dir(workdir)
        .output()
        .map_err(|e| Error::GitCommand(format!("{e}")))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(Error::GitCommand(format!(
            "git {} failed: {}",
            args.first().unwrap_or(&""),
            stderr.trim()
        )));
    }

    Ok(String::from_utf8_lossy(&output.stdout).to_string())
}

/// List all git remotes that have `meta = true` in their config.
///
/// # Parameters
///
/// - `repo`: the Git repository to query
///
/// # Returns
///
/// A vec of `(name, url)` pairs for each remote with `remote.<name>.meta = true`.
///
/// # Errors
///
/// Returns an error if reading the git config fails.
pub fn list_meta_remotes(repo: &gix::Repository) -> Result<Vec<(String, String)>> {
    let config = repo.config_snapshot();
    let remote_names = repo.remote_names();
    let mut remotes = Vec::new();

    for name in &remote_names {
        let meta_key = format!("remote.{name}.meta");
        if config.boolean(&meta_key) == Some(true) {
            let url_key = format!("remote.{name}.url");
            if let Some(url) = config.string(&url_key) {
                remotes.push((name.to_string(), url.to_string()));
            }
        }
    }

    Ok(remotes)
}

/// Hydrate tip tree blobs for a blobless-fetched ref.
///
/// This fetches all blob objects referenced by the tip tree so gix can read them.
///
/// # Parameters
///
/// - `repo`: the Git repository to operate on
/// - `remote_name`: the remote to fetch blobs from
/// - `ref_name`: the ref whose tree blobs should be fetched
///
/// # Errors
///
/// Returns an error if the ls-tree or fetch subprocess fails.
pub fn hydrate_tip_blobs(repo: &gix::Repository, remote_name: &str, ref_name: &str) -> Result<()> {
    hydrate_tip_blobs_counted(repo, remote_name, ref_name)?;
    Ok(())
}

/// Like [`hydrate_tip_blobs`] but returns the number of blobs fetched.
///
/// # Parameters
///
/// - `repo`: the Git repository to operate on
/// - `remote_name`: the remote to fetch blobs from
/// - `ref_name`: the ref whose tree blobs should be fetched
///
/// # Returns
///
/// The number of blob OIDs discovered in the tree.
///
/// # Errors
///
/// Returns an error if the ls-tree or fetch subprocess fails.
pub fn hydrate_tip_blobs_counted(
    repo: &gix::Repository,
    remote_name: &str,
    ref_name: &str,
) -> Result<usize> {
    let blob_list = run_git(repo, &["ls-tree", "-r", "--object-only", ref_name]);

    match blob_list {
        Ok(blobs) if !blobs.trim().is_empty() => {
            let count = blobs.lines().count();
            let workdir = repo_dir(repo)?;

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
                .spawn()
                .map_err(|e| Error::GitCommand(format!("{e}")))?;

            if let Some(mut stdin) = child.stdin.take() {
                use std::io::Write;
                stdin
                    .write_all(blobs.as_bytes())
                    .map_err(|e| Error::GitCommand(format!("{e}")))?;
            }

            let output = child
                .wait_with_output()
                .map_err(|e| Error::GitCommand(format!("{e}")))?;
            if !output.status.success() {
                let stderr = String::from_utf8_lossy(&output.stderr);
                return Err(Error::GitCommand(format!(
                    "blob hydration failed: {}",
                    stderr.trim()
                )));
            }

            Ok(count)
        }
        Ok(_) => Ok(0),
        Err(e) => Err(Error::GitCommand(format!(
            "ls-tree failed for {ref_name}: {e}"
        ))),
    }
}

/// Look up a blob OID in a git tree by following a slash-separated path.
///
/// Returns `None` if any path segment is missing. Trees are local (fetched even
/// in blobless clones), so this works without network access.
///
/// # Parameters
///
/// - `repo`: the Git repository containing the tree
/// - `tree_id`: the root tree object ID to start from
/// - `path`: slash-separated path to the blob (e.g. `"a/b/file.txt"`)
///
/// # Returns
///
/// `Some(ObjectId)` of the blob at the path, or `None` if not found.
///
/// # Errors
///
/// Returns an error if reading tree objects from the repository fails.
#[cfg_attr(not(feature = "internal"), allow(dead_code))]
pub fn find_blob_oid_in_tree(
    repo: &gix::Repository,
    tree_id: gix::ObjectId,
    path: &str,
) -> Result<Option<gix::ObjectId>> {
    let segments: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();
    if segments.is_empty() {
        return Ok(None);
    }

    let mut current_tree_id = tree_id;

    for (i, segment) in segments.iter().enumerate() {
        let tree = repo
            .find_tree(current_tree_id)
            .map_err(|e| Error::Other(format!("{e}")))?;
        let Some(entry) = tree.find_entry(*segment) else {
            return Ok(None);
        };

        let entry_id = entry.object_id();
        let is_tree = entry.mode().is_tree();

        if i == segments.len() - 1 {
            return Ok(Some(entry_id));
        }

        if !is_tree {
            return Ok(None);
        }
        current_tree_id = entry_id;
    }

    Ok(None)
}

/// Fetch specific blob OIDs from a remote.
///
/// Similar to [`hydrate_tip_blobs`] but takes an explicit list of OIDs
/// instead of discovering them via ls-tree.
///
/// # Parameters
///
/// - `repo`: the Git repository to operate on
/// - `remote_name`: the remote to fetch blobs from
/// - `oids`: the blob OIDs to fetch
///
/// # Errors
///
/// Returns an error if the fetch subprocess fails.
#[cfg_attr(not(feature = "internal"), allow(dead_code))]
pub fn fetch_blob_oids(
    repo: &gix::Repository,
    remote_name: &str,
    oids: &[gix::ObjectId],
) -> Result<()> {
    if oids.is_empty() {
        return Ok(());
    }

    let workdir = repo_dir(repo)?;

    let oid_list: String = oids.iter().map(|o| format!("{o}\n")).collect();

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
        .stderr(std::process::Stdio::inherit())
        .spawn()
        .map_err(|e| Error::GitCommand(format!("{e}")))?;

    if let Some(mut stdin) = child.stdin.take() {
        use std::io::Write;
        stdin
            .write_all(oid_list.as_bytes())
            .map_err(|e| Error::GitCommand(format!("{e}")))?;
    }

    let output = child
        .wait_with_output()
        .map_err(|e| Error::GitCommand(format!("{e}")))?;
    if !output.status.success() {
        return Err(Error::GitCommand("blob fetch failed".into()));
    }

    Ok(())
}

/// Resolve a meta remote by name, or pick the first one if no name given.
///
/// # Parameters
///
/// - `repo`: the Git repository to query
/// - `remote`: optional remote name; if `None`, returns the first meta remote
///
/// # Returns
///
/// The name of the resolved meta remote.
///
/// # Errors
///
/// Returns [`Error::NoRemotes`] if no meta remotes are configured, or
/// [`Error::RemoteNotFound`] if the specified name is not a meta remote.
pub fn resolve_meta_remote(repo: &gix::Repository, remote: Option<&str>) -> Result<String> {
    let meta_remotes = list_meta_remotes(repo)?;

    if meta_remotes.is_empty() {
        return Err(Error::NoRemotes);
    }

    match remote {
        Some(name) => {
            if meta_remotes.iter().any(|(n, _)| n == name) {
                Ok(name.to_string())
            } else {
                Err(Error::RemoteNotFound(name.to_string()))
            }
        }
        None => Ok(meta_remotes[0].0.clone()),
    }
}

fn gix_config_string(repo: &gix::Repository, key: &str, default: &str) -> String {
    let config = repo.config_snapshot();
    config
        .string(key)
        .map_or_else(|| default.to_string(), |s| s.to_string())
}

/// Discover the Git repository from the current directory.
///
/// # Errors
///
/// Returns [`Error::NotARepository`] if no git repository is found.
pub(crate) fn discover_repo() -> Result<gix::Repository> {
    let repo = gix::discover(".").map_err(|_| Error::NotARepository)?;
    Ok(repo)
}

/// Get the path to the gmeta SQLite database.
///
/// # Parameters
///
/// - `repo`: the Git repository
///
/// # Returns
///
/// The path to `gmeta.sqlite` inside the git directory.
pub(crate) fn db_path(repo: &gix::Repository) -> Result<PathBuf> {
    Ok(repo.git_dir().join("gmeta.sqlite"))
}

/// Get the user's email from Git config.
///
/// # Parameters
///
/// - `repo`: the Git repository
///
/// # Returns
///
/// The configured `user.email`, or `"unknown"` if not set.
pub(crate) fn get_email(repo: &gix::Repository) -> Result<String> {
    Ok(gix_config_string(repo, "user.email", "unknown"))
}

/// Get the user's name from Git config.
///
/// # Parameters
///
/// - `repo`: the Git repository
///
/// # Returns
///
/// The configured `user.name`, or `"unknown"` if not set.
pub(crate) fn get_name(repo: &gix::Repository) -> Result<String> {
    Ok(gix_config_string(repo, "user.name", "unknown"))
}

/// Get the meta namespace from Git config (defaults to "meta").
///
/// # Parameters
///
/// - `repo`: the Git repository
///
/// # Returns
///
/// The configured `meta.namespace`, or `"meta"` if not set.
pub(crate) fn get_namespace(repo: &gix::Repository) -> Result<String> {
    Ok(gix_config_string(repo, "meta.namespace", "meta"))
}

/// Expand a partial commit SHA to the full 40-char hex string.
///
/// # Parameters
///
/// - `repo`: the Git repository
/// - `partial`: a partial (or full) commit SHA or ref name
///
/// # Returns
///
/// The full 40-character hex SHA of the commit.
///
/// # Errors
///
/// Returns [`Error::ResolveError`] if the partial SHA cannot be resolved,
/// or [`Error::Other`] if the resolved object is not a commit.
pub(crate) fn resolve_commit_sha(repo: &gix::Repository, partial: &str) -> Result<String> {
    let obj = repo
        .rev_parse_single(partial.as_bytes())
        .map_err(|_| Error::ResolveError(partial.to_string()))?;
    let id = obj.detach();
    // Verify it's a commit by peeling
    let object = repo
        .find_object(id)
        .map_err(|e| Error::Other(format!("{e}")))?;
    if object.kind != gix::object::Kind::Commit {
        // Try peeling tags etc.
        let peeled = object
            .peel_to_kind(gix::object::Kind::Commit)
            .map_err(|e| Error::Other(format!("{e}")))?;
        Ok(peeled.id.to_string())
    } else {
        Ok(id.to_string())
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
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
