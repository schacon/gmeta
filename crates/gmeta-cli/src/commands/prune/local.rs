use anyhow::Result;

use super::auto::{parse_since_to_cutoff_ms, read_prune_rules};
use crate::context::CommandContext;
use gmeta_core::types::TargetType;

pub fn run(dry_run: bool, skip_date: bool) -> Result<()> {
    let ctx = CommandContext::open(None)?;

    let cutoff_ms = if skip_date {
        // Prune everything (cutoff far in the future so all timestamps qualify)
        eprintln!("Pruning all non-project metadata (--skip-date)");
        i64::MAX
    } else {
        // Read the since value directly — for manual prune we only need the retention window,
        // not the triggers (max-keys/max-size).
        let rules = read_prune_rules(ctx.store())?;
        let since = match rules {
            Some(ref r) => r.since.clone(),
            None => {
                // Check if at least meta:prune:since is set (triggers may be absent)
                match ctx
                    .store()
                    .get(&TargetType::Project, "", "meta:prune:since")?
                {
                    Some(entry) => {
                        let s: String = serde_json::from_str(&entry.value)?;
                        s
                    }
                    None => {
                        eprintln!("No prune rules configured.");
                        eprintln!();
                        eprintln!("Run `gmeta config:prune` to set up auto-prune rules, or set them manually:");
                        eprintln!("  gmeta config meta:prune:since 6m");
                        eprintln!("  gmeta config meta:prune:max-keys 10000");
                        eprintln!("  gmeta config meta:prune:max-size 10m");
                        return Ok(());
                    }
                }
            }
        };

        let ms = parse_since_to_cutoff_ms(&since)?;
        let cutoff_date = time::OffsetDateTime::from_unix_timestamp_nanos(ms as i128 * 1_000_000)
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

        eprintln!(
            "Pruning metadata older than {} (cutoff: {})",
            since, cutoff_date
        );
        ms
    };

    if dry_run {
        eprintln!("(dry run — no changes will be made)");
    }

    eprintln!();

    // Count what will be pruned (never prune project target_type)
    let metadata_count = ctx.store().count_metadata_before(cutoff_ms)?;
    let list_values_count = ctx.store().count_list_values_before(cutoff_ms)?;
    let tombstone_count = ctx.store().count_tombstones_before(cutoff_ms)?;
    let set_tombstone_count = ctx.store().count_set_tombstones_before(cutoff_ms)?;
    let log_count = ctx.store().count_log_entries_before(cutoff_ms)?;

    // Count what will survive
    let metadata_remaining = ctx.store().count_metadata_remaining(cutoff_ms)?;
    let list_values_remaining = ctx.store().count_list_values_remaining(cutoff_ms)?;

    let total =
        metadata_count + list_values_count + tombstone_count + set_tombstone_count + log_count;

    println!(
        "  {} metadata keys to prune ({} remaining)",
        metadata_count, metadata_remaining
    );
    println!(
        "  {} list entries to prune ({} remaining)",
        list_values_count, list_values_remaining
    );
    println!("  {} tombstones to prune", tombstone_count);
    println!("  {} set tombstones to prune", set_tombstone_count);
    println!("  {} log entries to prune", log_count);

    if total == 0 {
        println!();
        println!("Nothing to prune.");
        return Ok(());
    }

    if dry_run {
        println!();
        println!(
            "Would prune {} total rows. Run without --dry-run to apply.",
            total
        );
        return Ok(());
    }

    // Delete via Store methods
    ctx.store().prune_metadata_before(cutoff_ms)?;
    ctx.store().prune_tombstones_before(cutoff_ms)?;
    ctx.store().prune_set_tombstones_before(cutoff_ms)?;
    ctx.store().prune_log_before(cutoff_ms)?;

    println!();
    println!("Pruned {} rows.", total);
    Ok(())
}
