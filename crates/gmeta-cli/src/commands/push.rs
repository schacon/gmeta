use anyhow::{bail, Result};
use gix::prelude::ObjectIdExt;
use gix::refs::transaction::PreviousValue;

use crate::commands::{materialize, serialize};
use crate::context::CommandContext;
use gmeta_core::git_utils;

/// Push a README commit to refs/heads/main on the meta remote.
/// This only succeeds if the branch doesn't already exist (no force push).
pub fn run_readme(remote: Option<&str>, verbose: bool) -> Result<()> {
    let ctx = CommandContext::open(None)?;
    let repo = ctx.repo();

    let remote_name = git_utils::resolve_meta_remote(repo, remote)?;

    // Gather project info from git config
    let config = repo.config_snapshot();
    let origin_url = config
        .string("remote.origin.url")
        .map(|s| s.to_string())
        .unwrap_or_else(|| "unknown".to_string());
    let meta_url_key = format!("remote.{}.url", remote_name);
    let meta_url = config
        .string(&meta_url_key)
        .map(|s| s.to_string())
        .unwrap_or_else(|| "unknown".to_string());
    let ns = &ctx.namespace;

    let readme_content = generate_readme(&origin_url, &meta_url, ns);

    if verbose {
        eprintln!("[verbose] remote: {}", remote_name);
        eprintln!("[verbose] origin url: {}", origin_url);
        eprintln!("[verbose] meta url: {}", meta_url);
    }

    // Create blob -> tree -> commit
    let blob_oid: gix::ObjectId = repo.write_blob(readme_content.as_bytes())?.into();

    let tree_oid = {
        let mut editor = repo.empty_tree().edit()?;
        editor.upsert("README.md", gix::objs::tree::EntryKind::Blob, blob_oid)?;
        editor.write()?
    };

    let name = git_utils::get_name(repo)?;
    let email = git_utils::get_email(repo)?;
    let sig = gix::actor::Signature {
        name: name.into(),
        email: email.into(),
        time: gix::date::Time::now_local_or_utc(),
    };

    let commit = gix::objs::Commit {
        message: "Initial metadata repository setup\n\nCreated by gmeta to provide documentation for contributors.".into(),
        tree: tree_oid.into(),
        author: sig.clone(),
        committer: sig,
        encoding: None,
        parents: vec![].into(),
        extra_headers: Default::default(),
    };

    let commit_oid = repo.write_object(&commit)?.detach();

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
    let result = git_utils::run_git(repo, &["push", &remote_name, &push_refspec]);

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
    let ctx = CommandContext::open(None)?;
    let repo = ctx.repo();
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
        .and_then(|r| r.into_fully_peeled_id().ok())
        .map(|id| id.detach());
    let remote_oid = repo
        .find_reference(&remote_tracking_ref)
        .ok()
        .and_then(|r| r.into_fully_peeled_id().ok())
        .map(|id| id.detach());

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
        let result = git_utils::run_git(repo, &["push", &remote_name, &push_refspec]);

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
                git_utils::run_git(repo, &["fetch", &remote_name, &fetch_refspec])?;

                // Hydrate tip tree blobs so gix can read them
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
    repo: &gix::Repository,
    local_ref: &str,
    remote_ref: &str,
    verbose: bool,
) -> anyhow::Result<()> {
    let local_ref_obj = repo.find_reference(local_ref)?;
    let local_oid = local_ref_obj.into_fully_peeled_id()?.detach();
    let local_commit_obj = local_oid.attach(repo).object()?.into_commit();
    let local_decoded = local_commit_obj.decode()?;

    let remote_ref_obj = repo.find_reference(remote_ref)?;
    let remote_oid = remote_ref_obj.into_fully_peeled_id()?.detach();

    // If the local commit is already a single-parent child of remote, nothing to do
    let parent_ids: Vec<gix::ObjectId> = local_decoded.parents().collect();
    if parent_ids.len() == 1 && parent_ids[0] == remote_oid {
        return Ok(());
    }

    let tree_id = local_decoded.tree();
    let message = local_decoded.message.to_owned();
    let author_ref = local_decoded.author().map_err(|e| anyhow::anyhow!("{e}"))?;

    let commit = gix::objs::Commit {
        message,
        tree: tree_id,
        author: gix::actor::Signature {
            name: author_ref.name.into(),
            email: author_ref.email.into(),
            time: author_ref.time().map_err(|e| anyhow::anyhow!("{e}"))?,
        },
        committer: gix::actor::Signature {
            name: author_ref.name.into(),
            email: author_ref.email.into(),
            time: author_ref.time().map_err(|e| anyhow::anyhow!("{e}"))?,
        },
        encoding: None,
        parents: vec![remote_oid].into(),
        extra_headers: Default::default(),
    };

    let new_oid = repo.write_object(&commit)?.detach();
    repo.reference(
        local_ref,
        new_oid,
        PreviousValue::Any,
        "gmeta: rebase for push",
    )?;

    if verbose {
        eprintln!(
            "[verbose] rebased local ref onto remote tip: {} -> {} (parent: {})",
            &local_oid.to_string()[..8],
            &new_oid.to_string()[..8],
            &remote_oid.to_string()[..8],
        );
    }

    Ok(())
}
