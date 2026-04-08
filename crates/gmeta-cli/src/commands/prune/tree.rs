//! `gmeta prune` — create a prune commit on the serialized ref tree,
//! dropping entries older than the configured retention window.
//!
//! Unlike auto-prune (which only drops list entries and tombstones from the
//! existing tree), this rebuilds the tree from the DB, filtering out all
//! metadata entries (string, list, set) older than the cutoff.

use anyhow::Result;
use gix::prelude::ObjectIdExt;
use gix::refs::transaction::PreviousValue;

use crate::context::CommandContext;
use gmeta::prune::parse_since_to_cutoff_ms;
use gmeta::serialize::{build_filtered_tree, count_prune_stats};
use gmeta::tree::filter::{classify_key, parse_filter_rules, MAIN_DEST};
use gmeta::types::{Target, TargetType};

pub fn run(dry_run: bool) -> Result<()> {
    let ctx = CommandContext::open(None)?;
    let repo = ctx.session.repo();

    // Read prune rules -- need at least meta:prune:since
    let since = match ctx
        .session
        .store()
        .get(&Target::project(), "meta:prune:since")?
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

    let now_ms = time::OffsetDateTime::now_utc().unix_timestamp_nanos() as i64 / 1_000_000;
    let cutoff_ms = parse_since_to_cutoff_ms(&since, now_ms)?;
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
    let ref_name = format!("refs/{}/local/main", ctx.session.namespace());
    let Some(current_commit_oid) = repo
        .find_reference(&ref_name)
        .ok()
        .and_then(|r| r.into_fully_peeled_id().ok())
        .map(gix::Id::detach)
    else {
        eprintln!("No serialized metadata found at {ref_name}. Run `gmeta serialize` first.");
        return Ok(());
    };

    let current_commit_obj = current_commit_oid.attach(repo).object()?.into_commit();
    let tree_oid = current_commit_obj.tree_id()?.detach();
    let (_, current_keys) = count_prune_stats(repo, tree_oid, tree_oid)?;

    eprintln!("Pruning {ref_name} (cutoff: {cutoff_date} -- entries older than {since})");
    eprintln!("  current tree: {current_keys} keys");

    // Read filter rules so we produce the same tree as serialize would
    let filter_rules = parse_filter_rules(ctx.session.store())?;

    let is_main_dest = |key: &str| -> bool {
        match classify_key(key, &filter_rules) {
            None => false, // excluded
            Some(dests) => dests.iter().any(|d| d == MAIN_DEST),
        }
    };

    // Read all metadata and split into kept vs pruned by cutoff + serialize filters
    let all_metadata = ctx.session.store().get_all_metadata()?;
    let all_tombstones = ctx.session.store().get_all_tombstones()?;
    let all_set_tombstones = ctx.session.store().get_all_set_tombstones()?;
    let all_list_tombstones = ctx.session.store().get_all_list_tombstones()?;

    // Count entries that would be pruned (old + in main dest)
    let mut pruned_meta = 0u64;
    let metadata: Vec<_> = all_metadata
        .into_iter()
        .filter(|e| {
            if !is_main_dest(&e.key) {
                return false;
            }
            if e.target_type != TargetType::Project && e.last_timestamp < cutoff_ms {
                pruned_meta += 1;
                return false;
            }
            true
        })
        .collect();
    let mut pruned_tombs = 0u64;
    let tombstones: Vec<_> = all_tombstones
        .into_iter()
        .filter(|r| {
            if !is_main_dest(&r.key) {
                return false;
            }
            if r.target_type != TargetType::Project && r.timestamp < cutoff_ms {
                pruned_tombs += 1;
                return false;
            }
            true
        })
        .collect();
    let set_tombstones: Vec<_> = all_set_tombstones
        .into_iter()
        .filter(|r| {
            (r.target_type == TargetType::Project || r.timestamp >= cutoff_ms)
                && is_main_dest(&r.key)
        })
        .collect();
    let list_tombstones: Vec<_> = all_list_tombstones
        .into_iter()
        .filter(|r| {
            (r.target_type == TargetType::Project || r.timestamp >= cutoff_ms)
                && is_main_dest(&r.key)
        })
        .collect();

    let total_pruned = pruned_meta + pruned_tombs;
    if total_pruned == 0 {
        println!("Nothing to prune -- all entries are within the retention window.");
        return Ok(());
    }

    eprintln!("  {pruned_meta} metadata keys and {pruned_tombs} tombstones to drop");

    // Build a fresh tree from the surviving entries
    let pruned_tree_oid = build_filtered_tree(
        repo,
        &metadata,
        &tombstones,
        &set_tombstones,
        &list_tombstones,
    )?;

    let (keys_dropped, keys_retained) = count_prune_stats(repo, tree_oid, pruned_tree_oid)?;

    eprintln!("  pruned tree:  {keys_retained} keys ({keys_dropped} dropped from tree)");

    if dry_run {
        println!(
            "Would drop {keys_dropped} keys, retaining {keys_retained}. Run without --dry-run to apply."
        );
        return Ok(());
    }

    // Commit the pruned tree
    let name = ctx.session.name();
    let email = ctx.session.email();
    let sig = gix::actor::Signature {
        name: name.into(),
        email: email.into(),
        time: gix::date::Time::now_local_or_utc(),
    };

    let message = format!(
        "gmeta: prune --since={since}\n\npruned: true\nsince: {since}\nkeys-dropped: {keys_dropped}\nkeys-retained: {keys_retained}"
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
