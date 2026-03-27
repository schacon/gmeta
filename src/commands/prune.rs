//! `gmeta prune` — create a prune commit on the serialized ref tree,
//! dropping entries older than the configured retention window.

use anyhow::Result;

use crate::commands::auto_prune::{self, parse_since_to_cutoff_ms};
use crate::commands::serialize::{count_prune_stats, prune_tree};
use crate::db::Db;
use crate::git_utils;

pub fn run(dry_run: bool) -> Result<()> {
    let repo = git_utils::discover_repo()?;
    let db_path = git_utils::db_path(&repo)?;
    let db = Db::open(&db_path)?;

    // Read prune rules — need at least meta:prune:since
    let since = match db.get("project", "", "meta:prune:since")? {
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

    let min_size = match db.get("project", "", "meta:prune:min-size")? {
        Some((value, _, _)) => {
            let s: String = serde_json::from_str(&value)?;
            Some(auto_prune::parse_size(&s)?)
        }
        None => None,
    };

    let rules = auto_prune::PruneRules {
        since: since.clone(),
        max_keys: None,
        max_size: None,
        min_size,
    };

    let cutoff_ms = parse_since_to_cutoff_ms(&since)?;
    let cutoff_date = chrono::DateTime::from_timestamp_millis(cutoff_ms)
        .map(|d| d.format("%Y-%m-%d %H:%M:%S UTC").to_string())
        .unwrap_or_else(|| "?".to_string());

    // Find the current serialized tree
    let ref_name = git_utils::local_ref(&repo)?;
    let current_commit = match repo.find_reference(&ref_name) {
        Ok(r) => match r.peel_to_commit() {
            Ok(c) => c,
            Err(_) => {
                eprintln!("No serialized metadata found at {}. Run `gmeta serialize` first.", ref_name);
                return Ok(());
            }
        },
        Err(_) => {
            eprintln!("No serialized metadata found at {}. Run `gmeta serialize` first.", ref_name);
            return Ok(());
        }
    };

    let tree_oid = current_commit.tree()?.id();

    // Count current keys
    let (_, current_keys) = count_prune_stats(&repo, tree_oid, tree_oid)?;

    eprintln!(
        "Pruning {} (cutoff: {} — entries older than {})",
        ref_name, cutoff_date, since
    );
    eprintln!("  current tree: {} keys", current_keys);

    // Build the pruned tree
    let pruned_tree_oid = prune_tree(&repo, tree_oid, &rules, &db, false)?;

    if pruned_tree_oid == tree_oid {
        println!("Nothing to prune — tree unchanged.");
        return Ok(());
    }

    let (keys_dropped, keys_retained) = count_prune_stats(&repo, tree_oid, pruned_tree_oid)?;

    eprintln!("  pruned tree:  {} keys ({} dropped)", keys_retained, keys_dropped);

    if dry_run {
        println!(
            "Would drop {} keys, retaining {}. Run without --dry-run to apply.",
            keys_dropped, keys_retained
        );
        return Ok(());
    }

    // Commit the pruned tree
    let name = git_utils::get_name(&repo)?;
    let email = git_utils::get_email(&repo)?;
    let sig = git2::Signature::now(&name, &email)?;
    let pruned_tree = repo.find_tree(pruned_tree_oid)?;

    let min_size_str = min_size
        .map(|s| format!("\nmin-size: {}", s))
        .unwrap_or_default();

    let message = format!(
        "gmeta: prune --since={}\n\npruned: true\nsince: {}{}\nkeys-dropped: {}\nkeys-retained: {}",
        since, since, min_size_str, keys_dropped, keys_retained
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
