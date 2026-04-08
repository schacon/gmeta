//! `gmeta prune` — create a prune commit on the serialized ref tree,
//! dropping entries older than the configured retention window.
//!
//! Unlike auto-prune (which only drops list entries and tombstones from the
//! existing tree), this rebuilds the tree from the DB, filtering out all
//! metadata entries (string, list, set) older than the cutoff.

use anyhow::Result;
use gix::prelude::ObjectIdExt;
use gix::refs::transaction::PreviousValue;

use super::auto::parse_since_to_cutoff_ms;
use crate::commands::serialize::{build_filtered_tree, count_prune_stats};
use crate::context::CommandContext;
use gmeta_core::git_utils;
use gmeta_core::tree::filter::{classify_key, parse_filter_rules, MAIN_DEST};
use gmeta_core::types::TargetType;

pub fn run(dry_run: bool) -> Result<()> {
    let ctx = CommandContext::open(None)?;
    let repo = ctx.repo();

    // Read prune rules -- need at least meta:prune:since
    let since = match ctx
        .store()
        .get(&TargetType::Project, "", "meta:prune:since")?
    {
        Some(entry) => {
            let s: String = serde_json::from_str(&entry.value)?;
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
    let cutoff_date =
        time::OffsetDateTime::from_unix_timestamp_nanos(cutoff_ms as i128 * 1_000_000)
            .ok()
            .and_then(|d| {
                d.format(
                    &time::format_description::parse(
                        "[year]-[month]-[day] [hour]:[minute]:[second] UTC",
                    )
                    .unwrap_or_default(),
                )
                .ok()
            })
            .unwrap_or_else(|| "?".to_string());

    // Find the current serialized tree
    let ref_name = ctx.local_ref();
    let current_commit_oid = match repo
        .find_reference(&ref_name)
        .ok()
        .and_then(|r| r.into_fully_peeled_id().ok())
        .map(|id| id.detach())
    {
        Some(oid) => oid,
        None => {
            eprintln!(
                "No serialized metadata found at {}. Run `gmeta serialize` first.",
                ref_name
            );
            return Ok(());
        }
    };

    let current_commit_obj = current_commit_oid.attach(repo).object()?.into_commit();
    let tree_oid = current_commit_obj.tree_id()?.detach();
    let (_, current_keys) = count_prune_stats(repo, tree_oid, tree_oid)?;

    eprintln!(
        "Pruning {} (cutoff: {} -- entries older than {})",
        ref_name, cutoff_date, since
    );
    eprintln!("  current tree: {} keys", current_keys);

    // Read filter rules so we produce the same tree as serialize would
    let filter_rules = parse_filter_rules(ctx.store())?;

    let is_main_dest = |key: &str| -> bool {
        match classify_key(key, &filter_rules) {
            None => false, // excluded
            Some(dests) => dests.iter().any(|d| d == MAIN_DEST),
        }
    };

    // Read all metadata and split into kept vs pruned by cutoff + serialize filters
    let all_metadata = ctx.store().get_all_metadata()?;
    let all_tombstones = ctx.store().get_all_tombstones()?;
    let all_set_tombstones = ctx.store().get_all_set_tombstones()?;
    let all_list_tombstones = ctx.store().get_all_list_tombstones()?;

    // Count entries that would be pruned (old + in main dest)
    let mut pruned_meta = 0u64;
    let metadata: Vec<_> = all_metadata
        .into_iter()
        .filter(|e| {
            if !is_main_dest(&e.key) {
                return false;
            }
            if e.target_type != "project" && e.last_timestamp < cutoff_ms {
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
        println!("Nothing to prune -- all entries are within the retention window.");
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
    let name = git_utils::get_name(repo)?;
    let email = git_utils::get_email(repo)?;
    let sig = gix::actor::Signature {
        name: name.into(),
        email: email.into(),
        time: gix::date::Time::now_local_or_utc(),
    };

    let message = format!(
        "gmeta: prune --since={}\n\npruned: true\nsince: {}\nkeys-dropped: {}\nkeys-retained: {}",
        since, since, keys_dropped, keys_retained
    );

    let commit = gix::objs::Commit {
        message: message.into(),
        tree: pruned_tree_oid,
        author: sig.clone(),
        committer: sig,
        encoding: None,
        parents: vec![current_commit_oid].into(),
        extra_headers: Default::default(),
    };

    let commit_oid = repo.write_object(&commit)?.detach();
    repo.reference(
        ref_name.as_str(),
        commit_oid,
        PreviousValue::Any,
        "gmeta: prune",
    )?;

    println!(
        "pruned to {} ({}) -- dropped {} keys, retained {}",
        ref_name,
        &commit_oid.to_string()[..8],
        keys_dropped,
        keys_retained
    );

    Ok(())
}
