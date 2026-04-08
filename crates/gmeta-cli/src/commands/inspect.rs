//! `gmeta inspect` — browse metadata keys and values.
//!
//! - No arguments: show count of keys per target type
//! - `gmeta inspect <target-type>`: list all keys for that target type
//! - `gmeta inspect <target-type> <term>`: fuzzy-match keys/targets on term

use std::collections::BTreeMap;

use anyhow::Result;
use time::{Duration, OffsetDateTime};

use crate::context::CommandContext;
use gmeta_core::db::Store;

const RESET: &str = "\x1b[0m";
const BOLD: &str = "\x1b[1m";
const DIM: &str = "\x1b[2m";
const YELLOW: &str = "\x1b[33m";
const GREEN: &str = "\x1b[32m";
const CYAN: &str = "\x1b[36m";

pub fn run(
    target_type: Option<&str>,
    term: Option<&str>,
    timeline: bool,
    promisor: bool,
) -> Result<()> {
    let ctx = CommandContext::open(None)?;

    if promisor {
        return run_promisor_list(ctx.session.store(), target_type);
    }

    if timeline {
        return run_timeline(ctx.session.store());
    }

    match target_type {
        None => run_overview(ctx.session.store()),
        Some(tt) => run_list(ctx.session.store(), tt, term),
    }
}

/// Show key counts per target type.
fn run_overview(db: &Store) -> Result<()> {
    let keys = db.get_all_keys()?;
    let promised = db.count_promised_keys()?;
    let promised_map: BTreeMap<String, u64> = promised.into_iter().collect();

    if keys.is_empty() && promised_map.is_empty() {
        println!("no metadata stored");
        return Ok(());
    }

    // Count keys and unique targets per target_type
    let mut type_stats: BTreeMap<String, (u64, BTreeMap<String, ()>)> = BTreeMap::new();
    for (target_type, target_value, _key) in &keys {
        let entry = type_stats.entry(target_type.clone()).or_default();
        entry.0 += 1;
        entry.1.insert(target_value.clone(), ());
    }

    // Collect all target types (materialized + promised-only)
    let mut all_types: Vec<String> = type_stats.keys().cloned().collect();
    for tt in promised_map.keys() {
        if !type_stats.contains_key(tt) {
            all_types.push(tt.clone());
        }
    }
    all_types.sort();

    for target_type in &all_types {
        let (hydrated_count, targets) = type_stats
            .get(target_type)
            .map(|(k, t)| (*k, t.len()))
            .unwrap_or((0, 0));
        let promised_count = promised_map.get(target_type).copied().unwrap_or(0);
        let total = hydrated_count + promised_count;
        let targets_label = if targets <= 1 && target_type == "project" {
            String::new()
        } else if targets > 0 {
            format!(" across {} targets", targets)
        } else {
            String::new()
        };
        let breakdown = if promised_count > 0 {
            format!(
                " {DIM}({} hydrated, {} pending){RESET}",
                hydrated_count, promised_count
            )
        } else {
            String::new()
        };
        println!(
            "{YELLOW}{}{RESET}  {} keys{}{}",
            target_type, total, targets_label, breakdown
        );
    }

    Ok(())
}

/// List promisor (not-yet-fetched) keys, optionally filtered by target type.
fn run_promisor_list(db: &Store, target_type: Option<&str>) -> Result<()> {
    let all = db.get_promised_keys()?;

    let entries: Vec<&(String, String, String)> = match target_type {
        Some(tt) => all.iter().filter(|(t, _, _)| t == tt).collect(),
        None => all.iter().collect(),
    };

    if entries.is_empty() {
        println!("no promisor keys");
        return Ok(());
    }

    // Group by target_type, then target_value
    let mut by_type: BTreeMap<&str, BTreeMap<&str, Vec<&str>>> = BTreeMap::new();
    for (tt, tv, key) in &entries {
        by_type
            .entry(tt)
            .or_default()
            .entry(tv)
            .or_default()
            .push(key);
    }

    let mut first_type = true;
    for (tt, targets) in &by_type {
        if !first_type {
            println!();
        }
        first_type = false;

        let mut first_target = true;
        for (tv, keys) in targets {
            if !first_target {
                println!();
            }
            first_target = false;

            let display_target = if *tt == "project" {
                "project".to_string()
            } else {
                format!("{CYAN}{tt}{RESET}:{GREEN}{tv}{RESET}")
            };
            println!("{}", display_target);

            for key in keys {
                println!("  {DIM}{key}{RESET}");
            }
        }
    }

    println!(
        "\n{} promisor key{} total",
        entries.len(),
        if entries.len() == 1 { "" } else { "s" }
    );

    Ok(())
}

/// List keys for a specific target type, optionally fuzzy-filtered.
fn run_list(db: &Store, target_type: &str, term: Option<&str>) -> Result<()> {
    let all = db.get_all_metadata()?;

    // Filter to target type
    let mut entries: Vec<&gmeta_core::db::types::SerializableEntry> = all
        .iter()
        .filter(|e| e.target_type == target_type)
        .collect();

    if entries.is_empty() {
        println!("no metadata for target type '{}'", target_type);
        return Ok(());
    }

    // Fuzzy filter if term is provided
    if let Some(term) = term {
        let lower_term = term.to_lowercase();
        entries.retain(|e| {
            fuzzy_matches(&lower_term, &e.target_value)
                || fuzzy_matches(&lower_term, &e.key)
                || (e.value_type == gmeta_core::types::ValueType::String
                    && fuzzy_matches(&lower_term, &decode_string_value(&e.value)))
        });
    }

    if entries.is_empty() {
        println!("no matches for '{}'", term.unwrap_or(""));
        return Ok(());
    }

    // Determine terminal width
    let term_width = terminal_width();

    // Group by target_value
    let mut by_target: BTreeMap<&str, Vec<&&gmeta_core::db::types::SerializableEntry>> =
        BTreeMap::new();
    for entry in &entries {
        by_target
            .entry(&entry.target_value)
            .or_default()
            .push(entry);
    }

    let mut first = true;
    for (target_value, target_entries) in &by_target {
        if !first {
            println!();
        }
        first = false;

        let short_target = *target_value;
        let display_target = if target_type == "project" {
            "project".to_string()
        } else {
            format!("{CYAN}{target_type}{RESET}:{GREEN}{short_target}{RESET}")
        };
        println!("{}", display_target);

        for entry in target_entries {
            let preview =
                format_value_oneline(&entry.value, &entry.value_type, term_width, entry.key.len());
            println!("  {BOLD}{}{RESET}  {DIM}{preview}{RESET}", entry.key);
        }
    }

    Ok(())
}

/// Show a weekly histogram of metadata entries over the last 20 weeks.
fn run_timeline(db: &Store) -> Result<()> {
    let all = db.get_all_metadata()?;

    if all.is_empty() {
        println!("no metadata stored");
        return Ok(());
    }

    let now = OffsetDateTime::now_utc();
    let weeks = 20usize;
    let week_duration = Duration::weeks(1);
    let start = now - Duration::weeks(weeks as i64);
    let start_ms = start.unix_timestamp_nanos() as i64 / 1_000_000;

    // Bucket entries by week (0 = oldest week, weeks-1 = current week)
    let mut buckets = vec![0u64; weeks];
    let mut older = 0u64;

    for e in &all {
        if e.last_timestamp < start_ms {
            older += 1;
            continue;
        }
        let offset_ms = e.last_timestamp - start_ms;
        let week_ms = week_duration.whole_milliseconds() as i64;
        let bucket = (offset_ms / week_ms) as usize;
        if bucket < weeks {
            buckets[bucket] += 1;
        }
    }

    let max_count = *buckets.iter().max().unwrap_or(&1).max(&1);
    let bar_width = 30usize;

    println!(
        "{BOLD}Metadata entries per week (last {} weeks){RESET}",
        weeks
    );
    println!();

    for (i, count) in buckets.iter().enumerate() {
        let week_start = start + Duration::weeks(i as i64);
        let label = week_start
            .format(
                &time::format_description::parse("[month repr:short] [day]").unwrap_or_default(),
            )
            .unwrap_or_else(|_| "?".to_string());
        let filled = ((*count as f64 / max_count as f64) * bar_width as f64).round() as usize;
        let bar: String = "█".repeat(filled);
        let pad: String = " ".repeat(bar_width - filled);
        if *count > 0 {
            println!("  {DIM}{label}{RESET}  {GREEN}{bar}{pad}{RESET}  {count}");
        } else {
            println!("  {DIM}{label}{RESET}  {pad}  {DIM}·{RESET}");
        }
    }

    if older > 0 {
        println!();
        println!(
            "{DIM}  + {} entries older than {} weeks{RESET}",
            older, weeks
        );
    }

    let total: u64 = buckets.iter().sum::<u64>() + older;
    println!();
    println!("  {BOLD}{}{RESET} total entries", total);

    Ok(())
}

/// Format a value for one-line display, fitting within available width.
fn format_value_oneline(
    value: &str,
    value_type: &gmeta_core::types::ValueType,
    term_width: usize,
    key_len: usize,
) -> String {
    use gmeta_core::types::ValueType;
    // 2 spaces indent + key + 2 spaces gap = overhead
    let overhead = 2 + key_len + 2;
    let available = if term_width > overhead + 5 {
        term_width - overhead
    } else {
        40
    };

    match value_type {
        ValueType::String => {
            let raw = decode_string_value(value);
            let first_line = raw.lines().next().unwrap_or("");
            let has_more = raw.contains('\n') && raw.trim_end_matches('\n') != first_line;
            let mut s = if first_line.len() > available {
                format!("{}...", &first_line[..available.saturating_sub(3)])
            } else {
                first_line.to_string()
            };
            if has_more && s.len() < available {
                s.push_str(" ...");
            }
            s
        }
        ValueType::List => {
            if let Ok(arr) = serde_json::from_str::<Vec<serde_json::Value>>(value) {
                format!("[list: {} items]", arr.len())
            } else {
                "[list]".to_string()
            }
        }
        ValueType::Set => {
            if let Ok(members) = serde_json::from_str::<Vec<String>>(value) {
                format!("[set: {} members]", members.len())
            } else {
                "[set]".to_string()
            }
        }
        _ => "[unknown type]".to_string(),
    }
}

/// Decode a JSON-encoded string value, falling back to raw.
fn decode_string_value(value: &str) -> String {
    serde_json::from_str::<String>(value).unwrap_or_else(|_| value.to_string())
}

/// Simple fuzzy match: all characters of the term appear in order in the haystack.
fn fuzzy_matches(term: &str, haystack: &str) -> bool {
    let haystack_lower = haystack.to_lowercase();
    let mut hay_chars = haystack_lower.chars();
    for tc in term.chars() {
        loop {
            match hay_chars.next() {
                Some(hc) if hc == tc => break,
                Some(_) => continue,
                None => return false,
            }
        }
    }
    true
}

/// Get terminal width, defaulting to 100.
fn terminal_width() -> usize {
    // Try COLUMNS env var first
    if let Ok(cols) = std::env::var("COLUMNS") {
        if let Ok(w) = cols.parse::<usize>() {
            if w > 20 {
                return w;
            }
        }
    }
    // Try `tput cols`
    if let Ok(output) = std::process::Command::new("tput").arg("cols").output() {
        if output.status.success() {
            if let Ok(s) = std::str::from_utf8(&output.stdout) {
                if let Ok(w) = s.trim().parse::<usize>() {
                    if w > 20 {
                        return w;
                    }
                }
            }
        }
    }
    100
}
