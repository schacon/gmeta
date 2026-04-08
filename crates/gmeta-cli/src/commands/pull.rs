use anyhow::Result;

use crate::commands::{materialize, serialize};
use crate::context::CommandContext;
use gmeta_core::git_utils;

pub fn run(remote: Option<&str>, verbose: bool) -> Result<()> {
    let ctx = CommandContext::open(None)?;
    let repo = ctx.session.repo();
    let ns = ctx.session.namespace();

    let remote_name = git_utils::resolve_meta_remote(repo, remote)?;
    let remote_refspec = format!("refs/{}/main", ns);
    let tracking_ref = format!("refs/{}/remotes/main", ns);
    let fetch_refspec = format!("{}:{}", remote_refspec, tracking_ref);

    if verbose {
        eprintln!("[verbose] remote: {}", remote_name);
        eprintln!("[verbose] fetch refspec: {}", fetch_refspec);
    }

    // Record the old tip so we can count new commits
    let old_tip = repo
        .find_reference(&tracking_ref)
        .ok()
        .and_then(|r| r.into_fully_peeled_id().ok());

    // Fetch latest remote metadata
    eprintln!("Fetching metadata from {}...", remote_name);
    git_utils::run_git(repo, &["fetch", &remote_name, &fetch_refspec])?;

    // Get the new tip
    let new_tip = repo
        .find_reference(&tracking_ref)
        .ok()
        .and_then(|r| r.into_fully_peeled_id().ok());

    // Check if we need to materialize even if no new commits were fetched
    // (e.g. remote add fetched but never materialized)
    let needs_materialize = ctx.session.store().get_last_materialized()?.is_none()
        || repo.find_reference(&ctx.session.local_ref()).is_err();

    // Count new commits
    match (old_tip, new_tip) {
        (Some(old), Some(new)) if old == new => {
            if !needs_materialize {
                println!("Already up-to-date.");
                return Ok(());
            }
            eprintln!("No new commits, but local state needs materializing.");
        }
        (Some(old), Some(new)) => {
            let count = count_commits_between(repo, old.into(), new.into());
            eprintln!(
                "Fetched {} new commit{}.",
                count,
                if count == 1 { "" } else { "s" }
            );
        }
        (None, Some(_)) => {
            eprintln!("Fetched initial metadata history.");
        }
        _ => {}
    }

    // Hydrate tip tree blobs so gix can read them
    let short_ref = format!("{}/remotes/main", ns);
    git_utils::hydrate_tip_blobs(repo, &remote_name, &short_ref)?;

    // Serialize local state so materialize can do a proper 3-way merge
    eprintln!("Serializing local metadata...");
    serialize::run(verbose)?;

    // Materialize: merge remote tree into local DB
    eprintln!("Materializing remote metadata...");
    materialize::run(None, false, verbose)?;

    // Insert promisor entries from non-tip commits so we know what keys exist
    // in the history even though we haven't fetched their blob data yet.
    // On first materialize, walk the entire history (pass None as old_tip).
    if let Some(new) = new_tip {
        let walk_from = if needs_materialize { None } else { old_tip };
        let promisor_count = ctx
            .session
            .index_history(new.into(), walk_from.map(|id| id.into()))?;
        if promisor_count > 0 {
            eprintln!(
                "Indexed {} keys from history (available on demand).",
                promisor_count
            );
        }
    }

    println!("Pulled metadata from {}", remote_name);
    Ok(())
}

/// Count commits reachable from `new` but not from `old`.
fn count_commits_between(repo: &gix::Repository, old: gix::ObjectId, new: gix::ObjectId) -> usize {
    let walk = repo.rev_walk(Some(new)).with_boundary(Some(old));
    match walk.all() {
        Ok(iter) => iter.filter(|r| r.is_ok()).count().saturating_sub(1), // subtract the boundary commit
        Err(_) => 0,
    }
}

// Tests for parse_commit_changes and parse_tree_path are in gmeta_core::sync::tests.
