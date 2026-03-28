use anyhow::Result;

use crate::commands::{materialize, serialize};
use crate::git_utils;

pub fn run(remote: Option<&str>, verbose: bool) -> Result<()> {
    let repo = git_utils::discover_repo()?;
    let ns = git_utils::get_namespace(&repo)?;

    let remote_name = git_utils::resolve_meta_remote(&repo, remote)?;
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
        .and_then(|r| r.peel_to_commit().ok())
        .map(|c| c.id());

    // Fetch latest remote metadata
    eprintln!("Fetching metadata from {}...", remote_name);
    git_utils::run_git(&repo, &["fetch", &remote_name, &fetch_refspec])?;

    // Get the new tip
    let new_tip = repo
        .find_reference(&tracking_ref)
        .ok()
        .and_then(|r| r.peel_to_commit().ok())
        .map(|c| c.id());

    // Count new commits
    match (old_tip, new_tip) {
        (Some(old), Some(new)) if old == new => {
            println!("Already up-to-date.");
            return Ok(());
        }
        (Some(old), Some(new)) => {
            let count = count_commits_between(&repo, old, new);
            eprintln!("Fetched {} new commit{}.", count, if count == 1 { "" } else { "s" });
        }
        (None, Some(_)) => {
            eprintln!("Fetched initial metadata history.");
        }
        _ => {}
    }

    // Hydrate tip tree blobs so libgit2 can read them
    let short_ref = format!("{}/remotes/main", ns);
    git_utils::hydrate_tip_blobs(&repo, &remote_name, &short_ref)?;

    // Serialize local state so materialize can do a proper 3-way merge
    eprintln!("Serializing local metadata...");
    serialize::run(verbose)?;

    // Materialize: merge remote tree into local DB
    eprintln!("Materializing remote metadata...");
    materialize::run(None, false, verbose)?;

    println!("Pulled metadata from {}", remote_name);
    Ok(())
}

/// Count commits reachable from `new` but not from `old`.
fn count_commits_between(repo: &git2::Repository, old: git2::Oid, new: git2::Oid) -> usize {
    let mut revwalk = match repo.revwalk() {
        Ok(rw) => rw,
        Err(_) => return 0,
    };
    if revwalk.push(new).is_err() {
        return 0;
    }
    if revwalk.hide(old).is_err() {
        return 0;
    }
    revwalk.count()
}
