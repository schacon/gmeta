//! Git repository helpers for resolving objects and manipulating metadata refs.

use std::fmt::Display;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::process::Command;

use crate::error::{Error, Result};

// 4000 SHA-1 OIDs plus newlines is roughly 164 KiB, safely below the
// smart-HTTP request size that made GitHub reject one-shot tip hydration.
const FETCH_OID_BATCH_SIZE: usize = 4000;

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
            let oids: Vec<&str> = blobs.lines().filter(|line| !line.is_empty()).collect();
            let count = oids.len();

            fetch_oid_batches(repo, remote_name, &oids, "blob hydration")?;

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

    fetch_oid_batches(repo, remote_name, oids, "blob fetch")
}

fn fetch_oid_batches<T: Display>(
    repo: &gix::Repository,
    remote_name: &str,
    oids: &[T],
    operation: &str,
) -> Result<()> {
    let workdir = repo_dir(repo)?;

    // GitHub rejects very large smart-HTTP request bodies. Keep each
    // `fetch --stdin` want-list bounded while preserving sequential fetches.
    let mut batches = 0;
    for batch in oid_batches(oids) {
        fetch_oid_batch(workdir, remote_name, batch, operation)?;
        batches += 1;
    }

    if batches > 1 {
        consolidate_packs(workdir)?;
    }

    Ok(())
}

/// Merge the packs a batched fetch just produced into fewer, larger ones.
///
/// Every `git fetch` writes its own pack, so hydrating a large tip leaves one
/// pack per batch. That is not merely untidy: `gix` sizes its object-database
/// slot map when a repository is opened, and once the packs outnumber the slots
/// every subsequent open fails with "The slotmap turned out to be too small".
/// A consumer of a large metadata history would be unable to read what it had
/// just downloaded.
///
/// Geometric repacking keeps this proportional to what was added rather than
/// rewriting the whole object store on every fetch. Older Git versions do not
/// support it, and a repository that cannot be repacked is not a reason to fail
/// the fetch that already succeeded, so failures here are ignored.
fn consolidate_packs(workdir: &Path) -> Result<()> {
    let status = Command::new("git")
        .args(["repack", "--geometric=2", "-d", "-q"])
        .current_dir(workdir)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status();

    match status {
        Ok(status) if status.success() => Ok(()),
        // `--geometric` predates neither promisor packs nor every supported
        // Git; fall back to a full repack, and give up quietly if that fails.
        _ => {
            let _ = Command::new("git")
                .args(["repack", "-a", "-d", "-q"])
                .current_dir(workdir)
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::null())
                .status();
            Ok(())
        }
    }
}

fn oid_batches<T>(oids: &[T]) -> impl Iterator<Item = &[T]> {
    oids.chunks(FETCH_OID_BATCH_SIZE)
}

fn fetch_oid_batch<T: Display>(
    workdir: &Path,
    remote_name: &str,
    batch: &[T],
    operation: &str,
) -> Result<()> {
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

    let Some(mut stdin) = child.stdin.take() else {
        return Err(Error::GitCommand(format!(
            "{operation} failed: git fetch stdin was unavailable"
        )));
    };
    let write_result = write_oid_batch(&mut stdin, batch);
    drop(stdin);

    let output = child
        .wait_with_output()
        .map_err(|e| Error::GitCommand(format!("{e}")))?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        let write_error = write_result
            .err()
            .map_or_else(String::new, |e| format!(" (stdin write failed: {e})"));
        return Err(Error::GitCommand(format!(
            "{operation} failed{write_error}: {}",
            stderr.trim()
        )));
    }

    write_result.map_err(|e| Error::GitCommand(format!("{operation} failed: {e}")))?;
    Ok(())
}

fn write_oid_batch<T: Display>(writer: &mut impl Write, batch: &[T]) -> io::Result<()> {
    for oid in batch {
        writeln!(writer, "{oid}")?;
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

    if let Some(name) = remote {
        if meta_remotes.iter().any(|(n, _)| n == name) {
            Ok(name.to_string())
        } else {
            Err(Error::RemoteNotFound(name.to_string()))
        }
    } else {
        let config = repo.config_snapshot();
        meta_remotes
            .iter()
            .find(|(name, _)| config.boolean(&format!("remote.{name}.metaside")) != Some(true))
            .or_else(|| meta_remotes.first())
            .map(|(name, _)| name.clone())
            .ok_or(Error::NoRemotes)
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

/// Get the path to the local SQLite metadata database.
///
/// # Parameters
///
/// - `repo`: the Git repository
///
/// # Returns
///
/// The path to `git-meta.sqlite` inside the git directory.
pub(crate) fn db_path(repo: &gix::Repository) -> Result<PathBuf> {
    Ok(repo.git_dir().join("git-meta.sqlite"))
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

/// Give Git the chance to pack loose objects, as it does after `git commit`.
///
/// Serialization writes its trees and commits straight through `gix`, which has
/// no equivalent of Git's automatic maintenance. Nothing else runs in a
/// metadata-only repository either, so without this the object store only ever
/// grows loose: a long history reaches millions of loose objects, at which
/// point ordinary reads slow down measurably and the working directory is far
/// larger than the packed history it represents.
///
/// `--auto` leaves the decision to Git's own `gc.auto` heuristic, which is
/// cheap when there is nothing to do. Maintenance failing is never a reason to
/// fail the serialization that already succeeded, so errors are ignored.
pub fn maintain_object_store(repo: &gix::Repository) {
    let Ok(workdir) = repo_dir(repo) else {
        return;
    };
    let _ = Command::new("git")
        .args(["gc", "--auto", "--quiet"])
        .current_dir(workdir)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status();
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

    /// Each batched fetch lands its own pack. Once those outnumber the slots
    /// gix sizes its object database with, opening the repository fails
    /// outright — so a multi-batch fetch has to leave the packs consolidated.
    #[test]
    fn consolidate_packs_merges_the_packs_a_batched_fetch_leaves_behind() {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let path = dir.path();
        let run = |args: &[&str]| {
            let output = std::process::Command::new("git")
                .current_dir(path)
                .args(args)
                .output()
                .expect("git");
            assert!(
                output.status.success(),
                "git {args:?}: {}",
                String::from_utf8_lossy(&output.stderr)
            );
            String::from_utf8_lossy(&output.stdout).trim().to_string()
        };

        run(&["init", "--quiet"]);
        run(&["config", "user.email", "test@example.com"]);
        run(&["config", "user.name", "Test User"]);

        // Build several single-object packs, the shape a batched fetch leaves.
        for index in 0..4 {
            std::fs::write(path.join("f"), format!("content {index}")).expect("write");
            let oid = run(&["hash-object", "-w", "f"]);
            let mut child = std::process::Command::new("git")
                .current_dir(path)
                .args(["pack-objects", "--quiet", ".git/objects/pack/pack"])
                .stdin(std::process::Stdio::piped())
                .stdout(std::process::Stdio::null())
                .spawn()
                .expect("pack-objects");
            {
                use std::io::Write;
                let stdin = child.stdin.as_mut().expect("stdin");
                writeln!(stdin, "{oid}").expect("write oid");
            }
            assert!(child.wait().expect("wait").success());
        }

        let count_packs = || {
            std::fs::read_dir(path.join(".git/objects/pack"))
                .expect("pack dir")
                .flatten()
                .filter(|e| e.path().extension().is_some_and(|x| x == "pack"))
                .count()
        };
        assert!(count_packs() >= 4, "test setup did not create packs");

        consolidate_packs(path).expect("consolidate");

        assert!(
            count_packs() < 4,
            "packs were not consolidated, still {}",
            count_packs()
        );
    }

    #[test]
    fn oid_batches_splits_large_oid_lists() {
        assert_eq!(FETCH_OID_BATCH_SIZE, 4000);

        let oids = vec!["oid"; 4001];
        let sizes: Vec<usize> = oid_batches(&oids).map(<[_]>::len).collect();

        assert_eq!(sizes, vec![4000, 1]);
    }

    #[test]
    fn write_oid_batch_terminates_each_oid() {
        let mut payload = Vec::new();

        write_oid_batch(&mut payload, &["abc", "def"]).unwrap();

        assert_eq!(payload, b"abc\ndef\n");
    }
}
