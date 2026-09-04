use anyhow::Result;

use crate::context::CommandContext;
use git_meta_lib::prune::parse_since_to_cutoff_ms;
use git_meta_lib::types::Target;

pub(crate) fn run(dry_run: bool, skip_date: bool, since: Option<&str>) -> Result<()> {
    let ctx = CommandContext::open(None)?;

    let cutoff_ms = if skip_date {
        // Prune everything (cutoff far in the future so all timestamps qualify)
        eprintln!("Pruning all non-project metadata (--skip-date)");
        i64::MAX
    } else {
        // Local pruning is date-based, like the manual tree prune: the window
        // comes from --since, or a project default. Auto-prune's key and size
        // limits describe the published tree, not the local store.
        let since = if let Some(since) = since {
            since.to_string()
        } else {
            {
                let Some(entry) = ctx
                    .session
                    .store()
                    .get(&Target::project(), "meta:prune:since")?
                else {
                    eprintln!("No retention window given.");
                    eprintln!();
                    eprintln!("Pass one, or set a project default:");
                    eprintln!("  git meta local-prune --since 6m");
                    eprintln!("  git meta config meta:prune:since 6m");
                    return Ok(());
                };
                serde_json::from_str::<String>(&entry.value)?
            }
        };

        let now_ms = time::OffsetDateTime::now_utc().unix_timestamp_nanos() as i64 / 1_000_000;
        let ms = parse_since_to_cutoff_ms(&since, now_ms)?;
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

        eprintln!("Pruning metadata older than {since} (cutoff: {cutoff_date})");
        ms
    };

    if dry_run {
        eprintln!("(dry run — no changes will be made)");
    }

    eprintln!();

    // Count what will be pruned (never prune project target_type)
    let metadata_count = ctx.session.store().count_metadata_before(cutoff_ms)?;
    let list_values_count = ctx.session.store().count_list_values_before(cutoff_ms)?;
    let tombstone_count = ctx.session.store().count_tombstones_before(cutoff_ms)?;
    let set_tombstone_count = ctx.session.store().count_set_tombstones_before(cutoff_ms)?;
    let log_count = ctx.session.store().count_log_entries_before(cutoff_ms)?;

    // Count what will survive
    let metadata_remaining = ctx.session.store().count_metadata_remaining(cutoff_ms)?;
    let list_values_remaining = ctx.session.store().count_list_values_remaining(cutoff_ms)?;

    let total =
        metadata_count + list_values_count + tombstone_count + set_tombstone_count + log_count;

    println!("  {metadata_count} metadata keys to prune ({metadata_remaining} remaining)");
    println!("  {list_values_count} list entries to prune ({list_values_remaining} remaining)");
    println!("  {tombstone_count} tombstones to prune");
    println!("  {set_tombstone_count} set tombstones to prune");
    println!("  {log_count} log entries to prune");

    if total == 0 {
        println!();
        println!("Nothing to prune.");
        return Ok(());
    }

    if dry_run {
        println!();
        println!("Would prune {total} total rows. Run without --dry-run to apply.");
        return Ok(());
    }

    // Delete via Store methods
    ctx.session.store().prune_metadata_before(cutoff_ms)?;
    ctx.session.store().prune_tombstones_before(cutoff_ms)?;
    ctx.session.store().prune_set_tombstones_before(cutoff_ms)?;
    ctx.session.store().prune_log_before(cutoff_ms)?;

    println!();
    println!("Pruned {total} rows.");
    Ok(())
}
