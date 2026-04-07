use anyhow::{bail, Result};

use crate::commands::{materialize, serialize};
use crate::context::CommandContext;
use gmeta_core::git_utils;

/// Push a README commit to refs/heads/main on the meta remote.
/// This only succeeds if the branch doesn't already exist (no force push).
pub fn run_readme(remote: Option<&str>, verbose: bool) -> Result<()> {
    let ctx = CommandContext::open_git2(None)?;
    let repo = ctx.git2_repo()?;

    let remote_name = git_utils::resolve_meta_remote(repo, remote)?;

    // Gather project info from .git/config
    let config = repo.config()?;
    let origin_url = config
        .get_string("remote.origin.url")
        .unwrap_or_else(|_| "unknown".to_string());
    let meta_url = config
        .get_string(&format!("remote.{}.url", remote_name))
        .unwrap_or_else(|_| "unknown".to_string());
    let ns = &ctx.namespace;

    let readme_content = generate_readme(&origin_url, &meta_url, ns);

    if verbose {
        eprintln!("[verbose] remote: {}", remote_name);
        eprintln!("[verbose] origin url: {}", origin_url);
        eprintln!("[verbose] meta url: {}", meta_url);
    }

    // Create blob -> tree -> commit
    let blob_oid = repo.blob(readme_content.as_bytes())?;

    let mut tb = repo.treebuilder(None)?;
    tb.insert("README.md", blob_oid, 0o100644)?;
    let tree_oid = tb.write()?;
    let tree = repo.find_tree(tree_oid)?;

    let sig = repo.signature()?;
    let commit_oid = repo.commit(
        None, // don't update any local ref
        &sig,
        &sig,
        "Initial metadata repository setup\n\nCreated by gmeta to provide documentation for contributors.",
        &tree,
        &[], // no parents — root commit
    )?;

    if verbose {
        eprintln!("[verbose] created blob: {}", blob_oid);
        eprintln!("[verbose] created tree: {}", tree_oid);
        eprintln!("[verbose] created commit: {}", commit_oid);
    }

    // Push commit to refs/heads/main on the remote, but only if it doesn't exist.
    // We use a refspec without '+' so it fails if the ref already exists.
    let push_refspec = format!("{}:refs/heads/main", commit_oid);

    if verbose {
        eprintln!("[verbose] push refspec: {}", push_refspec);
    }

    eprintln!("Pushing README to {}...", remote_name);
    let result = git_utils::git2_run_git(repo, &["push", &remote_name, &push_refspec]);

    match result {
        Ok(_) => {
            println!("Pushed README to {} (refs/heads/main)", remote_name);
            Ok(())
        }
        Err(e) => {
            let err_msg = e.to_string();
            if err_msg.contains("non-fast-forward")
                || err_msg.contains("rejected")
                || err_msg.contains("fetch first")
                || err_msg.contains("already exists")
            {
                bail!("refs/heads/main already exists on {}. The README can only be pushed to a new repository.", remote_name);
            }
            bail!("push failed: {}", err_msg);
        }
    }
}

fn generate_readme(origin_url: &str, meta_url: &str, namespace: &str) -> String {
    format!(
        r#"# Git Metadata Repository

This repository stores structured metadata for the project at:

    {origin_url}

It is managed by [gmeta](https://github.com/schacon/gmeta), a tool for associating
key-value metadata with Git objects (commits, branches, paths, and more) and syncing
them across repositories.

## How It Works

Metadata is stored locally in a SQLite database (`.git/gmeta.sqlite`) and serialized
into Git trees and commits under `refs/{namespace}/` refs for synchronization. This
repository serves as the shared remote for that metadata.

You do **not** need to clone this repository directly. Instead, configure it as a
metadata remote in your local checkout of the main project.

## Setup

1. Install gmeta (see [gmeta README](https://github.com/schacon/gmeta) for details).

2. In your local clone of the main project, add this repository as a metadata remote:

   ```
   gmeta remote add {meta_url}
   ```

3. Pull existing metadata:

   ```
   gmeta pull
   ```

4. You're ready to read and write metadata:

   ```
   gmeta get commit:HEAD
   gmeta set commit:HEAD review:status "approved"
   gmeta push
   ```

## Contributing Metadata

- **Set values:** `gmeta set <target> <key> <value>`
- **Read values:** `gmeta get <target> [key]`
- **Push changes:** `gmeta push`
- **Pull updates:** `gmeta pull`

Target types include `commit:<sha>`, `branch:<name>`, `change-id:<id>`,
`path:<file>`, and `project` (for repo-wide metadata).

See `gmeta --help` for the full command reference.

## Important Notes

- Metadata is stored on `refs/{namespace}/main`, not on `refs/heads/main`.
  The `main` branch you see here is just this README for orientation.
- Never push directly to `refs/{namespace}/main` — always use `gmeta push`,
  which handles serialization and conflict resolution.
- Metadata can be pruned over time. See `gmeta config:prune` for auto-prune rules.
"#
    )
}

const MAX_RETRIES: u32 = 5;

pub fn run(remote: Option<&str>, verbose: bool) -> Result<()> {
    let ctx = CommandContext::open_git2(None)?;
    let repo = ctx.git2_repo()?;
    let ns = &ctx.namespace;

    // Resolve which remote to push to
    let remote_name = git_utils::resolve_meta_remote(repo, remote)?;
    let local_ref = ctx.local_ref();
    let remote_refspec = format!("refs/{}/main", ns);

    if verbose {
        eprintln!("[verbose] remote: {}", remote_name);
        eprintln!("[verbose] local ref: {}", local_ref);
        eprintln!("[verbose] remote refspec: {}", remote_refspec);
    }

    // Serialize local metadata to the local ref
    eprintln!("Serializing local metadata...");
    serialize::run(verbose)?;

    // Verify we have something to push
    if repo.find_reference(&local_ref).is_err() {
        bail!("nothing to push (no local metadata ref)");
    }

    // Check if local ref already matches the remote ref (nothing new to push)
    let remote_tracking_ref = format!("refs/{}/remotes/main", ns);
    let local_oid = repo
        .find_reference(&local_ref)
        .ok()
        .and_then(|r| r.peel_to_commit().ok())
        .map(|c| c.id());
    let remote_oid = repo
        .find_reference(&remote_tracking_ref)
        .ok()
        .and_then(|r| r.peel_to_commit().ok())
        .map(|c| c.id());

    if let (Some(local), Some(remote)) = (local_oid, remote_oid) {
        if local == remote {
            println!("Everything up-to-date");
            return Ok(());
        }
    }

    // Try push with retry loop for non-fast-forward failures
    let push_refspec = format!("{}:{}", local_ref, remote_refspec);

    for attempt in 1..=MAX_RETRIES {
        if verbose {
            eprintln!("[verbose] push attempt {}/{}", attempt, MAX_RETRIES);
        }

        eprintln!("Pushing to {}...", remote_name);
        let result = git_utils::git2_run_git(repo, &["push", &remote_name, &push_refspec]);

        match result {
            Ok(_) => {
                println!("Pushed metadata to {} ({})", remote_name, remote_refspec);
                return Ok(());
            }
            Err(e) => {
                let err_msg = e.to_string();
                let is_non_ff = err_msg.contains("non-fast-forward")
                    || err_msg.contains("rejected")
                    || err_msg.contains("fetch first");

                if !is_non_ff || attempt == MAX_RETRIES {
                    bail!("push failed: {}", err_msg);
                }

                eprintln!(
                    "Push rejected (remote has new data), fetching and merging (attempt {}/{})...",
                    attempt, MAX_RETRIES
                );

                // Fetch latest remote data
                let fetch_refspec = format!("{}:refs/{}/remotes/main", remote_refspec, ns);
                git_utils::git2_run_git(repo, &["fetch", &remote_name, &fetch_refspec])?;

                // Hydrate tip tree blobs so libgit2 can read them
                let short_ref = format!("{}/remotes/main", ns);
                git_utils::hydrate_tip_blobs(repo, &remote_name, &short_ref)?;

                // Materialize the remote data (merge into local DB)
                materialize::run(None, false, verbose)?;

                // Re-serialize with merged data
                eprintln!("Re-serializing after merge...");
                serialize::run(verbose)?;

                // Rewrite local ref as a single commit on top of the remote tip.
                // This avoids merge commits in the pushed history — the spec
                // requires that push always produces a single fast-forward commit.
                rebase_local_on_remote(repo, &local_ref, &remote_tracking_ref, verbose)?;
            }
        }
    }

    bail!("push failed after {} attempts", MAX_RETRIES);
}

/// Rewrite the local ref as a single non-merge commit whose parent is the
/// remote tip and whose tree is the current local ref's tree. This ensures
/// the pushed history is always a clean fast-forward with no merge commits.
fn rebase_local_on_remote(
    repo: &git2::Repository,
    local_ref: &str,
    remote_ref: &str,
    verbose: bool,
) -> anyhow::Result<()> {
    let local_commit = repo.find_reference(local_ref)?.peel_to_commit()?;
    let remote_commit = repo.find_reference(remote_ref)?.peel_to_commit()?;

    // If the local commit is already a single-parent child of remote, nothing to do
    if local_commit.parent_count() == 1 && local_commit.parent_id(0)? == remote_commit.id() {
        return Ok(());
    }

    let tree = local_commit.tree()?;
    let message = local_commit.message().unwrap_or("");
    let sig = local_commit.author();

    // Use None for the ref update here — we'll force-update the ref ourselves,
    // because repo.commit(Some(ref)) requires the current tip to be the first parent.
    let new_oid = repo.commit(None, &sig, &sig, message, &tree, &[&remote_commit])?;
    repo.reference(local_ref, new_oid, true, "gmeta: rebase for push")?;

    if verbose {
        eprintln!(
            "[verbose] rebased local ref onto remote tip: {} -> {} (parent: {})",
            &local_commit.id().to_string()[..8],
            &new_oid.to_string()[..8],
            &remote_commit.id().to_string()[..8],
        );
    }

    Ok(())
}
