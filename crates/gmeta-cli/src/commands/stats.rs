use std::collections::BTreeMap;

use anyhow::Result;

use crate::context::CommandContext;

pub fn run() -> Result<()> {
    let ctx = CommandContext::open_gix(None)?;

    let rows = ctx.db.stats_by_target_type_and_key()?;

    if rows.is_empty() {
        println!("no metadata stored");
        return Ok(());
    }

    // Show storage counts and size histogram at the top
    let (sqlite_count, git_ref_count) = ctx.db.stats_storage_counts()?;
    println!(
        "{} values in sqlite, {} values as git refs",
        sqlite_count, git_ref_count
    );
    println!();

    let (buckets, _) = ctx.db.stats_value_size_histogram()?;
    let max_count = buckets.iter().map(|(_, c)| *c).max().unwrap_or(1).max(1);
    let bar_width = 30usize;
    println!("value sizes (inline):");
    for (label, count) in &buckets {
        let filled = ((*count as f64 / max_count as f64) * bar_width as f64).round() as usize;
        let bar: String = "#".repeat(filled);
        println!("  {:>10}  {:30}  {}", label, bar, count);
    }
    println!();

    // Group: target_type -> { key -> count }
    let mut by_type: BTreeMap<String, BTreeMap<String, u64>> = BTreeMap::new();
    for (target_type, key, count) in &rows {
        by_type
            .entry(target_type.clone())
            .or_default()
            .entry(key.clone())
            .or_insert(*count);
    }

    for (target_type, keys) in &by_type {
        // Collapse keys that differ only by integers into [n] patterns
        let grouped = group_keys_by_integer_pattern(keys);

        let total: u64 = grouped.values().sum();
        let tt = gmeta_core::types::TargetType::from_str(target_type)?;
        let plural = tt.pluralize();
        println!("{}: {} keys", plural, total);

        // Sort keys by count descending, then alphabetically
        let mut sorted: Vec<(&String, &u64)> = grouped.iter().collect();
        sorted.sort_by(|a, b| b.1.cmp(a.1).then(a.0.cmp(b.0)));

        for (key, count) in sorted {
            println!("  {}  {}", key, count);
        }
    }

    Ok(())
}

/// Replace sequences of digits within a key string with `[n]`.
/// Used to produce the canonical pattern for grouping.
fn key_to_pattern(key: &str) -> String {
    // Replace any run of digits with "[n]"
    let mut result = String::new();
    let mut chars = key.chars().peekable();
    while let Some(c) = chars.next() {
        if c.is_ascii_digit() {
            result.push_str("[n]");
            while chars.peek().map(|d| d.is_ascii_digit()).unwrap_or(false) {
                chars.next();
            }
        } else {
            result.push(c);
        }
    }
    result
}

/// Group keys whose patterns (digits replaced with [n]) are identical.
/// The grouped key uses the [n] pattern; the count is the sum of all matching keys.
fn group_keys_by_integer_pattern(keys: &BTreeMap<String, u64>) -> BTreeMap<String, u64> {
    // First pass: find which patterns have multiple distinct keys
    let mut pattern_keys: BTreeMap<String, Vec<&String>> = BTreeMap::new();
    for key in keys.keys() {
        let pattern = key_to_pattern(key);
        pattern_keys.entry(pattern).or_default().push(key);
    }

    let mut result: BTreeMap<String, u64> = BTreeMap::new();
    for (pattern, matching_keys) in &pattern_keys {
        if matching_keys.len() > 1 {
            // Multiple keys share this pattern — collapse into pattern, sum counts
            let total: u64 = matching_keys.iter().map(|k| keys[*k]).sum();
            result.insert(pattern.clone(), total);
        } else {
            // Only one key matches this pattern — keep original key name
            let key = matching_keys[0];
            result.insert(key.clone(), keys[key]);
        }
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_key_to_pattern_no_digits() {
        assert_eq!(key_to_pattern("agent:model"), "agent:model");
    }

    #[test]
    fn test_key_to_pattern_with_digits() {
        assert_eq!(key_to_pattern("agent:session-1"), "agent:session-[n]");
        assert_eq!(key_to_pattern("agent:session-42"), "agent:session-[n]");
        assert_eq!(key_to_pattern("step:1:result"), "step:[n]:result");
    }

    #[test]
    fn test_key_to_pattern_multiple_digit_groups() {
        assert_eq!(key_to_pattern("step:1:sub:2"), "step:[n]:sub:[n]");
    }

    #[test]
    fn test_group_keys_collapses_integer_variants() {
        let mut keys = BTreeMap::new();
        keys.insert("agent:session-1".to_string(), 5u64);
        keys.insert("agent:session-2".to_string(), 3u64);
        keys.insert("agent:session-10".to_string(), 1u64);
        keys.insert("agent:model".to_string(), 9u64);

        let grouped = group_keys_by_integer_pattern(&keys);

        assert_eq!(grouped.get("agent:session-[n]"), Some(&9u64)); // 5+3+1
        assert_eq!(grouped.get("agent:model"), Some(&9u64));
        assert!(!grouped.contains_key("agent:session-1"));
        assert!(!grouped.contains_key("agent:session-2"));
    }

    #[test]
    fn test_group_keys_keeps_single_int_key_as_is() {
        let mut keys = BTreeMap::new();
        keys.insert("agent:session-1".to_string(), 5u64);
        keys.insert("agent:model".to_string(), 9u64);

        let grouped = group_keys_by_integer_pattern(&keys);

        // Only one key matches "agent:session-[n]", so keep original name
        assert_eq!(grouped.get("agent:session-1"), Some(&5u64));
        assert_eq!(grouped.get("agent:model"), Some(&9u64));
        assert!(!grouped.contains_key("agent:session-[n]"));
    }
}
