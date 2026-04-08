use anyhow::Result;
use rusqlite::params;

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
        let rules = read_prune_rules(&ctx.db)?;
        let since = match rules {
            Some(ref r) => r.since.clone(),
            None => {
                // Check if at least meta:prune:since is set (triggers may be absent)
                match ctx.db.get(&TargetType::Project, "", "meta:prune:since")? {
                    Some((value, _, _)) => {
                        let s: String = serde_json::from_str(&value)?;
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

    let metadata_count: u64 = ctx.db.conn.query_row(
        "SELECT COUNT(*) FROM metadata
         WHERE target_type != 'project' AND last_timestamp < ?1",
        params![cutoff_ms],
        |row| row.get(0),
    )?;

    let list_values_count: u64 = ctx.db.conn.query_row(
        "SELECT COUNT(*) FROM list_values
         WHERE timestamp < ?1
           AND metadata_id IN (
               SELECT rowid FROM metadata WHERE target_type != 'project'
           )",
        params![cutoff_ms],
        |row| row.get(0),
    )?;

    let tombstone_count: u64 = ctx.db.conn.query_row(
        "SELECT COUNT(*) FROM metadata_tombstones
         WHERE target_type != 'project' AND timestamp < ?1",
        params![cutoff_ms],
        |row| row.get(0),
    )?;

    let set_tombstone_count: u64 = ctx.db.conn.query_row(
        "SELECT COUNT(*) FROM set_tombstones
         WHERE target_type != 'project' AND timestamp < ?1",
        params![cutoff_ms],
        |row| row.get(0),
    )?;

    let log_count: u64 = ctx.db.conn.query_row(
        "SELECT COUNT(*) FROM metadata_log
         WHERE target_type != 'project' AND timestamp < ?1",
        params![cutoff_ms],
        |row| row.get(0),
    )?;

    // Count what will survive
    let metadata_remaining: u64 = ctx.db.conn.query_row(
        "SELECT COUNT(*) FROM metadata
         WHERE target_type = 'project' OR last_timestamp >= ?1",
        params![cutoff_ms],
        |row| row.get(0),
    )?;

    let list_values_remaining: u64 = ctx.db.conn.query_row(
        "SELECT COUNT(*) FROM list_values
         WHERE timestamp >= ?1
            OR metadata_id IN (
                SELECT rowid FROM metadata WHERE target_type = 'project'
            )",
        params![cutoff_ms],
        |row| row.get(0),
    )?;

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

    // Delete in the right order: child rows first, then parent rows

    // 1. Delete list_values and set_values for metadata rows being pruned
    ctx.db.conn.execute(
        "DELETE FROM list_values
         WHERE metadata_id IN (
             SELECT rowid FROM metadata
             WHERE target_type != 'project' AND last_timestamp < ?1
         )",
        params![cutoff_ms],
    )?;

    ctx.db.conn.execute(
        "DELETE FROM set_values
         WHERE metadata_id IN (
             SELECT rowid FROM metadata
             WHERE target_type != 'project' AND last_timestamp < ?1
         )",
        params![cutoff_ms],
    )?;

    // 2. Delete old list entries from lists that survive (entries older than cutoff)
    ctx.db.conn.execute(
        "DELETE FROM list_values
         WHERE timestamp < ?1
           AND metadata_id IN (
               SELECT rowid FROM metadata WHERE target_type != 'project'
           )",
        params![cutoff_ms],
    )?;

    // 3. Delete the metadata rows themselves
    ctx.db.conn.execute(
        "DELETE FROM metadata
         WHERE target_type != 'project' AND last_timestamp < ?1",
        params![cutoff_ms],
    )?;

    // 4. Delete old tombstones
    ctx.db.conn.execute(
        "DELETE FROM metadata_tombstones
         WHERE target_type != 'project' AND timestamp < ?1",
        params![cutoff_ms],
    )?;

    // 5. Delete old set tombstones
    ctx.db.conn.execute(
        "DELETE FROM set_tombstones
         WHERE target_type != 'project' AND timestamp < ?1",
        params![cutoff_ms],
    )?;

    // 6. Delete old log entries
    ctx.db.conn.execute(
        "DELETE FROM metadata_log
         WHERE target_type != 'project' AND timestamp < ?1",
        params![cutoff_ms],
    )?;

    println!();
    println!("Pruned {} rows.", total);
    Ok(())
}
