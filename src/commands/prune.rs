//! `gmeta prune` — create a prune commit on the serialized ref tree,
//! dropping entries older than the configured retention window.
//!
//! Unlike auto-prune (which only drops list entries and tombstones from the
//! existing tree), this rebuilds the tree from the DB, filtering out all
//! metadata entries (string, list, set) older than the cutoff.

use anyhow::Result;

use crate::commands::auto_prune::parse_since_to_cutoff_ms;
use crate::commands::serialize::{
    build_filtered_tree, classify_key, count_prune_stats, parse_filter_rules, MAIN_DEST,
};
use crate::context::CommandContext;
use crate::git_utils;

pub fn run(dry_run: bool) -> Result<()> {
    let ctx = CommandContext::open_git2(None)?;
    let repo = ctx.git2_repo()?;

    // Read prune rules — need at least meta:prune:since
    let since = match ctx.db.get("project", "", "meta:prune:since")? {
        Some((value, _, _)) => {
            let s: String = serde_json::from_str(&value)?;
            s
        }
        None => {
            eprintln!("No prune rules configured (meta:prune:since is required).");
            eprintln!();
            eprintln!("Set a retention window first:");
            eprintln!("  gmeta config meta:prune:since 6m");
            return Ok(());
        }
    };

    let cutoff_ms = parse_since_to_cutoff_ms(&since)?;
    let cutoff_date = chrono::DateTime::from_timestamp_millis(cutoff_ms)
        .map(|d| d.format("%Y-%m-%d %H:%M:%S UTC").to_string())
        .unwrap_or_else(|| "?".to_string());

    // Find the current serialized tree
    let ref_name = git_utils::git2_local_ref(repo)?;
    let Some(current_commit) = repo
        .find_reference(&ref_name)
        .ok()
        .and_then(|r| r.peel_to_commit().ok())
    else {
        eprintln!(
            "No serialized metadata found at {}. Run `gmeta serialize` first.",
            ref_name
        );
        return Ok(());
    };

    let tree_oid = current_commit.tree()?.id();
    let (_, current_keys) = count_prune_stats(repo, tree_oid, tree_oid)?;

    eprintln!(
        "Pruning {} (cutoff: {} — entries older than {})",
        ref_name, cutoff_date, since
    );
    eprintln!("  current tree: {} keys", current_keys);

    // Read filter rules so we produce the same tree as serialize would
    let filter_rules = parse_filter_rules(&ctx.db)?;

    let is_main_dest = |key: &str| -> bool {
        match classify_key(key, &filter_rules) {
            None => false, // excluded
            Some(dests) => dests.iter().any(|d| d == MAIN_DEST),
        }
    };

    // Read all metadata and split into kept vs pruned by cutoff + serialize filters
    let all_metadata = ctx.db.get_all_metadata()?;
    let all_tombstones = ctx.db.get_all_tombstones()?;
    let all_set_tombstones = ctx.db.get_all_set_tombstones()?;
    let all_list_tombstones = ctx.db.get_all_list_tombstones()?;

    // Count entries that would be pruned (old + in main dest)
    let mut pruned_meta = 0u64;
    let metadata: Vec<_> = all_metadata
        .into_iter()
        .filter(|(tt, _, key, _, _, ts, _)| {
            if !is_main_dest(key) {
                return false;
            }
            if tt != "project" && *ts < cutoff_ms {
                pruned_meta += 1;
                return false;
            }
            true
        })
        .collect();
    let mut pruned_tombs = 0u64;
    let tombstones: Vec<_> = all_tombstones
        .into_iter()
        .filter(|(tt, _, key, ts, _)| {
            if !is_main_dest(key) {
                return false;
            }
            if tt != "project" && *ts < cutoff_ms {
                pruned_tombs += 1;
                return false;
            }
            true
        })
        .collect();
    let set_tombstones: Vec<_> = all_set_tombstones
        .into_iter()
        .filter(|(tt, _, key, _, _, ts, _)| {
            (tt == "project" || *ts >= cutoff_ms) && is_main_dest(key)
        })
        .collect();
    let list_tombstones: Vec<_> = all_list_tombstones
        .into_iter()
        .filter(|(tt, _, key, _, ts, _)| (tt == "project" || *ts >= cutoff_ms) && is_main_dest(key))
        .collect();

    let total_pruned = pruned_meta + pruned_tombs;
    if total_pruned == 0 {
        println!("Nothing to prune — all entries are within the retention window.");
        return Ok(());
    }

    eprintln!(
        "  {} metadata keys and {} tombstones to drop",
        pruned_meta, pruned_tombs
    );

    // Build a fresh tree from the surviving entries
    let pruned_tree_oid = build_filtered_tree(
        repo,
        &metadata,
        &tombstones,
        &set_tombstones,
        &list_tombstones,
    )?;

    let (keys_dropped, keys_retained) = count_prune_stats(repo, tree_oid, pruned_tree_oid)?;

    eprintln!(
        "  pruned tree:  {} keys ({} dropped from tree)",
        keys_retained, keys_dropped
    );

    if dry_run {
        println!(
            "Would drop {} keys, retaining {}. Run without --dry-run to apply.",
            keys_dropped, keys_retained
        );
        return Ok(());
    }

    // Commit the pruned tree
    let name = git_utils::git2_get_name(repo)?;
    let email = git_utils::git2_get_email(repo)?;
    let sig = git2::Signature::now(&name, &email)?;
    let pruned_tree = repo.find_tree(pruned_tree_oid)?;

    let message = format!(
        "gmeta: prune --since={}\n\npruned: true\nsince: {}\nkeys-dropped: {}\nkeys-retained: {}",
        since, since, keys_dropped, keys_retained
    );

    let commit_oid = repo.commit(
        Some(&ref_name),
        &sig,
        &sig,
        &message,
        &pruned_tree,
        &[&current_commit],
    )?;

    println!(
        "pruned to {} ({}) — dropped {} keys, retained {}",
        ref_name,
        &commit_oid.to_string()[..8],
        keys_dropped,
        keys_retained
    );

    Ok(())
}
