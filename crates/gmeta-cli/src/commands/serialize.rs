use std::collections::{BTreeMap, BTreeSet};

use anyhow::{bail, Context, Result};
use chrono::Utc;

use crate::commands::prune::auto::{self, parse_since_to_cutoff_ms};
use crate::context::CommandContext;
use gmeta_core::db::Db;
use gmeta_core::git_utils;
use gmeta_core::list_value::{make_entry_name, parse_entries};
use gmeta_core::types::{
    build_list_entry_tombstone_tree_path, build_list_tree_dir_path,
    build_set_member_tombstone_tree_path, build_set_tree_dir_path, build_tombstone_tree_path,
    build_tree_path, Target, TargetType, ValueType,
};

#[derive(serde::Serialize)]
struct TombstoneBlob<'a> {
    timestamp: i64,
    email: &'a str,
}
const META_LOCAL_PREFIX: &str = "meta:local:";
/// The "main" destination name used for the primary ref.
pub const MAIN_DEST: &str = "main";

#[derive(Debug, Clone)]
pub enum FilterAction {
    Exclude,
    Route(Vec<String>), // destination names
}

#[derive(Debug, Clone)]
pub struct FilterRule {
    action: FilterAction,
    pattern: Vec<PatternSegment>,
}

#[derive(Debug, Clone)]
enum PatternSegment {
    Literal(String),
    Star,     // matches one segment
    GlobStar, // matches zero or more segments
}

pub fn parse_filter_rules(db: &Db) -> Result<Vec<FilterRule>> {
    let mut rules = Vec::new();

    // meta:local:filter rules first (higher priority)
    if let Some((value, value_type, _)) = db.get(&TargetType::Project, "", "meta:local:filter")? {
        if value_type == ValueType::Set {
            let members: Vec<String> = serde_json::from_str(&value)?;
            for member in members {
                rules.push(parse_rule(&member)?);
            }
        }
    }

    // Then meta:filter rules (shared/corporate)
    if let Some((value, value_type, _)) = db.get(&TargetType::Project, "", "meta:filter")? {
        if value_type == ValueType::Set {
            let members: Vec<String> = serde_json::from_str(&value)?;
            for member in members {
                rules.push(parse_rule(&member)?);
            }
        }
    }

    Ok(rules)
}

fn parse_rule(s: &str) -> Result<FilterRule> {
    let parts: Vec<&str> = s.split_whitespace().collect();
    if parts.len() < 2 {
        bail!(
            "invalid filter rule (need at least action and pattern): '{}'",
            s
        );
    }

    let action = match parts[0] {
        "exclude" => FilterAction::Exclude,
        "route" => {
            if parts.len() < 3 {
                bail!("route rule requires a destination: '{}'", s);
            }
            let destinations: Vec<String> = parts[2]
                .split(',')
                .map(|d| d.trim().to_string())
                .filter(|d| !d.is_empty())
                .collect();
            FilterAction::Route(destinations)
        }
        other => bail!("unknown filter action '{}' in rule '{}'", other, s),
    };

    let pattern = parse_pattern(parts[1]);
    Ok(FilterRule { action, pattern })
}

fn parse_pattern(s: &str) -> Vec<PatternSegment> {
    s.split(':')
        .map(|seg| match seg {
            "**" => PatternSegment::GlobStar,
            "*" => PatternSegment::Star,
            _ => PatternSegment::Literal(seg.to_string()),
        })
        .collect()
}

fn pattern_matches(pattern: &[PatternSegment], key_segments: &[&str]) -> bool {
    match (pattern.first(), key_segments.first()) {
        (None, None) => true,
        (None, Some(_)) => false,
        (Some(PatternSegment::GlobStar), _) => {
            if pattern.len() == 1 {
                // trailing ** matches everything remaining
                return true;
            }
            // Try matching ** as zero segments, one segment, two segments, etc.
            for skip in 0..=key_segments.len() {
                if pattern_matches(&pattern[1..], &key_segments[skip..]) {
                    return true;
                }
            }
            false
        }
        (Some(_), None) => false,
        (Some(PatternSegment::Star), Some(_)) => pattern_matches(&pattern[1..], &key_segments[1..]),
        (Some(PatternSegment::Literal(lit)), Some(seg)) => {
            lit == seg && pattern_matches(&pattern[1..], &key_segments[1..])
        }
    }
}

/// Determine the destination(s) for a key based on filter rules.
/// Returns None if the key should be excluded, or Some(destinations).
/// An empty destinations vec means "main" (default).
pub fn classify_key(key: &str, rules: &[FilterRule]) -> Option<Vec<String>> {
    // Hard rule: meta:local: keys are never serialized
    if key.starts_with(META_LOCAL_PREFIX) {
        return None;
    }

    let segments: Vec<&str> = key.split(':').collect();
    let mut matched_routes: Vec<String> = Vec::new();
    let mut excluded = false;

    for rule in rules {
        if pattern_matches(&rule.pattern, &segments) {
            match &rule.action {
                FilterAction::Exclude => {
                    excluded = true;
                }
                FilterAction::Route(dests) => {
                    for d in dests {
                        if !matched_routes.contains(d) {
                            matched_routes.push(d.clone());
                        }
                    }
                }
            }
        }
    }

    if excluded {
        return None;
    }

    if matched_routes.is_empty() {
        Some(vec![MAIN_DEST.to_string()])
    } else {
        Some(matched_routes)
    }
}

const MAX_COMMIT_CHANGES: usize = 1000;

fn build_commit_message(changes: &[(char, String, String)]) -> String {
    if changes.len() > MAX_COMMIT_CHANGES {
        format!(
            "gmeta: serialize ({} changes)\n\nchanges-omitted: true\ncount: {}",
            changes.len(),
            changes.len()
        )
    } else {
        let mut msg = format!("gmeta: serialize ({} changes)\n", changes.len());
        for (op, target, key) in changes {
            msg.push('\n');
            msg.push(*op);
            msg.push('\t');
            msg.push_str(target);
            msg.push('\t');
            msg.push_str(key);
        }
        msg
    }
}

pub fn run(verbose: bool) -> Result<()> {
    let ctx = CommandContext::open_git2(None)?;
    let repo = ctx.git2_repo()?;

    let local_ref_name = ctx.local_ref();
    let last_materialized = ctx.db.get_last_materialized()?;

    if verbose {
        eprintln!("[verbose] local ref: {}", local_ref_name);
        eprintln!(
            "[verbose] last materialized: {}",
            match last_materialized {
                Some(ts) => format!(
                    "{} ({}ms)",
                    chrono::DateTime::from_timestamp_millis(ts)
                        .map(|d| d.to_rfc3339())
                        .unwrap_or_else(|| "?".to_string()),
                    ts
                ),
                None => "never".to_string(),
            }
        );
    }

    eprintln!("Reading metadata from database...");

    // Determine if we're doing incremental or full serialization
    // If we have a previous local ref commit, start from existing tree
    let existing_tree = repo
        .find_reference(&local_ref_name)
        .ok()
        .and_then(|r| r.peel_to_commit().ok())
        .and_then(|c| c.tree().ok());

    if verbose {
        if let Some(ref tree) = existing_tree {
            eprintln!(
                "[verbose] existing tree: {} ({} top-level entries)",
                tree.id(),
                tree.len()
            );
        } else {
            eprintln!("[verbose] no existing tree (first serialize)");
        }
    }

    // Build new tree entries.
    // In incremental mode, compute which targets are dirty so we can reuse
    // unchanged subtrees from the existing git tree.
    // changes: Vec<(op_char, target_label, key)> for commit message
    let (
        metadata_entries,
        tombstone_entries,
        set_tombstone_entries,
        list_tombstone_entries,
        dirty_target_bases,
        changes,
    ) = if let Some(since) = last_materialized {
        let modified = ctx.db.get_modified_since(since)?;
        if verbose {
            eprintln!(
                "[verbose] incremental mode: {} entries modified since last materialize",
                modified.len()
            );
        }
        if modified.is_empty() && existing_tree.is_some() {
            eprintln!("Nothing changed since last serialize");
            return Ok(());
        }

        // Build change list for commit message
        let changes: Vec<(char, String, String)> = modified
            .iter()
            .map(|(target_type, target_value, key, op, _val, _vtype)| {
                let op_char = match op.as_str() {
                    "rm" => 'D',
                    "set" => {
                        if existing_tree.is_some() {
                            'M'
                        } else {
                            'A'
                        }
                    }
                    _ => 'M',
                };
                let target_label = if target_type == "project" {
                    "project".to_string()
                } else {
                    format!("{}:{}", target_type, target_value)
                };
                (op_char, target_label, key.clone())
            })
            .collect();

        // Compute dirty target base paths from modified entries
        let mut dirty_bases: BTreeSet<String> = BTreeSet::new();
        for (target_type, target_value, _key, _op, _val, _vtype) in &modified {
            let target = if target_type == "project" {
                Target::parse("project")?
            } else {
                Target::parse(&format!("{}:{}", target_type, target_value))?
            };
            dirty_bases.insert(target.tree_base_path());
        }

        let metadata = ctx.db.get_all_metadata()?;
        let tombstones = ctx.db.get_all_tombstones()?;
        let set_tombstones = ctx.db.get_all_set_tombstones()?;
        let list_tombstones = ctx.db.get_all_list_tombstones()?;

        // Note: tombstone/set-tombstone targets don't need to be added to
        // dirty_bases separately — delete operations are logged in metadata_log,
        // so they're already captured by get_modified_since. Clean targets'
        // tombstones are preserved via subtree reuse from the existing tree.

        if verbose {
            eprintln!("[verbose] dirty target bases: {}", dirty_bases.len());
            for base in &dirty_bases {
                eprintln!("  {}", base);
            }
        }

        (
            metadata,
            tombstones,
            set_tombstones,
            list_tombstones,
            if existing_tree.is_some() {
                Some(dirty_bases)
            } else {
                None
            },
            changes,
        )
    } else {
        if verbose {
            eprintln!("[verbose] full serialization mode (no previous materialize)");
        }

        let metadata = ctx.db.get_all_metadata()?;

        // Full serialize: all entries are adds
        let changes: Vec<(char, String, String)> = metadata
            .iter()
            .map(
                |(target_type, target_value, key, _value, _value_type, _ts, _is_git_ref)| {
                    let target_label = if target_type == "project" {
                        "project".to_string()
                    } else {
                        format!("{}:{}", target_type, target_value)
                    };
                    ('A', target_label, key.clone())
                },
            )
            .collect();

        (
            metadata,
            ctx.db.get_all_tombstones()?,
            ctx.db.get_all_set_tombstones()?,
            ctx.db.get_all_list_tombstones()?,
            None,
            changes,
        )
    };

    if metadata_entries.is_empty() && tombstone_entries.is_empty() {
        println!("no metadata to serialize");
        return Ok(());
    }

    // If meta:prune:since is configured, drop entries older than the cutoff
    // before building the tree. This avoids building a large tree only to
    // prune it, and keeps the summary counts accurate.
    let prune_since = ctx
        .db
        .get(&TargetType::Project, "", "meta:prune:since")?
        .and_then(|(value, _, _)| serde_json::from_str::<String>(&value).ok());
    let prune_rules = auto::read_prune_rules(&ctx.db)?;
    let prune_cutoff_ms = prune_since
        .as_deref()
        .map(parse_since_to_cutoff_ms)
        .transpose()?;
    let mut pruned_count = 0u64;
    let metadata_entries = if let Some(cutoff) = prune_cutoff_ms {
        metadata_entries
            .into_iter()
            .filter(|(target_type, _, _, _, _, ts, _)| {
                if target_type != "project" && *ts < cutoff {
                    pruned_count += 1;
                    false
                } else {
                    true
                }
            })
            .collect()
    } else {
        metadata_entries
    };

    let filter_rules = parse_filter_rules(&ctx.db)?;
    if verbose && !filter_rules.is_empty() {
        eprintln!("[verbose] filter rules: {}", filter_rules.len());
        for rule in &filter_rules {
            eprintln!("  {:?}", rule);
        }
    }

    type MetaEntry = (String, String, String, String, ValueType, i64, bool);
    type TombEntry = (String, String, String, i64, String);
    type SetTombEntry = (String, String, String, String, String, i64, String);
    type ListTombEntry = (String, String, String, String, i64, String);

    let mut dest_metadata: BTreeMap<String, Vec<MetaEntry>> = BTreeMap::new();
    let mut dest_tombstones: BTreeMap<String, Vec<TombEntry>> = BTreeMap::new();
    let mut dest_set_tombstones: BTreeMap<String, Vec<SetTombEntry>> = BTreeMap::new();
    let mut dest_list_tombstones: BTreeMap<String, Vec<ListTombEntry>> = BTreeMap::new();
    let mut excluded_count = 0u64;

    for entry in &metadata_entries {
        let key = &entry.2;
        match classify_key(key, &filter_rules) {
            None => {
                excluded_count += 1;
                if verbose {
                    eprintln!("[verbose] excluding key: {}", key);
                }
            }
            Some(dests) => {
                for dest in dests {
                    dest_metadata.entry(dest).or_default().push(entry.clone());
                }
            }
        }
    }

    for entry in &tombstone_entries {
        let key = &entry.2;
        match classify_key(key, &filter_rules) {
            None => {}
            Some(dests) => {
                for dest in dests {
                    dest_tombstones.entry(dest).or_default().push(entry.clone());
                }
            }
        }
    }

    for entry in &set_tombstone_entries {
        let key = &entry.2;
        match classify_key(key, &filter_rules) {
            None => {}
            Some(dests) => {
                for dest in dests {
                    dest_set_tombstones
                        .entry(dest)
                        .or_default()
                        .push(entry.clone());
                }
            }
        }
    }

    for entry in &list_tombstone_entries {
        let key = &entry.2;
        match classify_key(key, &filter_rules) {
            None => {}
            Some(dests) => {
                for dest in dests {
                    dest_list_tombstones
                        .entry(dest)
                        .or_default()
                        .push(entry.clone());
                }
            }
        }
    }

    // Ensure "main" is always present
    dest_metadata.entry(MAIN_DEST.to_string()).or_default();

    let mut all_dests: BTreeSet<String> = BTreeSet::new();
    all_dests.extend(dest_metadata.keys().cloned());
    all_dests.extend(dest_tombstones.keys().cloned());
    all_dests.extend(dest_set_tombstones.keys().cloned());
    all_dests.extend(dest_list_tombstones.keys().cloned());

    {
        let mut string_count = 0u64;
        let mut list_count = 0u64;
        let mut set_count = 0u64;
        let mut targets: BTreeSet<String> = BTreeSet::new();
        let total_meta: usize = dest_metadata.values().map(|v| v.len()).sum();
        for entries in dest_metadata.values() {
            for (target_type, target_value, _key, _value, value_type, _ts, _is_git_ref) in entries {
                let label = if target_type == "project" {
                    "project".to_string()
                } else {
                    format!(
                        "{}:{}",
                        target_type,
                        &target_value[..7.min(target_value.len())]
                    )
                };
                targets.insert(label);
                match value_type {
                    ValueType::String => string_count += 1,
                    ValueType::List => list_count += 1,
                    ValueType::Set => set_count += 1,
                }
            }
        }
        let total_tombstones: usize = dest_tombstones.values().map(|v| v.len()).sum();
        let total_set_tombstones: usize = dest_set_tombstones.values().map(|v| v.len()).sum();
        let total_list_tombstones: usize = dest_list_tombstones.values().map(|v| v.len()).sum();
        eprintln!(
            "Serializing {} keys ({} string, {} list, {} set) across {} targets, {} tombstones, {} set tombstones, {} list tombstones",
            total_meta, string_count, list_count, set_count, targets.len(),
            total_tombstones, total_set_tombstones, total_list_tombstones,
        );
        if pruned_count > 0 {
            eprintln!("  {} keys outside prune window", pruned_count);
        }
        if excluded_count > 0 {
            eprintln!("  {} keys excluded by filters", excluded_count);
        }
        let non_main: Vec<&String> = all_dests
            .iter()
            .filter(|d| d.as_str() != MAIN_DEST)
            .collect();
        if !non_main.is_empty() {
            eprintln!(
                "  routing to destinations: {}",
                non_main
                    .iter()
                    .map(|d| d.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            );
        }
        if verbose {
            eprintln!("[verbose] targets:");
            for target in &targets {
                eprintln!("  {}", target);
            }
        }
    }

    let name = git_utils::git2_get_name(repo)?;
    let email = git_utils::git2_get_email(repo)?;
    let sig = git2::Signature::now(&name, &email)?;

    for dest in &all_dests {
        let ref_name = ctx.destination_ref(dest);
        let empty_meta: Vec<MetaEntry> = Vec::new();
        let empty_tomb: Vec<TombEntry> = Vec::new();
        let empty_set_tomb: Vec<SetTombEntry> = Vec::new();
        let empty_list_tomb: Vec<ListTombEntry> = Vec::new();

        let meta = dest_metadata.get(dest).unwrap_or(&empty_meta);
        let tombs = dest_tombstones.get(dest).unwrap_or(&empty_tomb);
        let set_tombs = dest_set_tombstones.get(dest).unwrap_or(&empty_set_tomb);
        let list_tombs = dest_list_tombstones.get(dest).unwrap_or(&empty_list_tomb);

        if meta.is_empty() && tombs.is_empty() && set_tombs.is_empty() && list_tombs.is_empty() {
            continue;
        }

        // Use incremental mode only for the main destination
        let (existing, dirty) = if dest == MAIN_DEST {
            (existing_tree.as_ref(), dirty_target_bases.as_ref())
        } else {
            (None, None)
        };

        eprintln!("Building git tree for {}...", ref_name);
        let tree_oid = build_tree(
            repo, meta, tombs, set_tombs, list_tombs, existing, dirty, verbose,
        )?;

        if verbose {
            let tree = repo.find_tree(tree_oid)?;
            eprintln!(
                "[verbose] built tree {} ({} top-level entries)",
                tree_oid,
                tree.len()
            );
        }

        let tree = repo.find_tree(tree_oid)?;

        let parent = repo
            .find_reference(&ref_name)
            .ok()
            .and_then(|r| r.peel_to_commit().ok());

        if verbose {
            if let Some(ref p) = parent {
                eprintln!(
                    "[verbose] parent commit for {}: {}",
                    ref_name,
                    &p.id().to_string()[..8]
                );
            } else {
                eprintln!("[verbose] no parent commit for {} (root)", ref_name);
            }
        }

        let parents: Vec<&git2::Commit> = parent.iter().collect();
        let commit_message = build_commit_message(&changes);
        let commit_oid = repo.commit(
            Some(&ref_name),
            &sig,
            &sig,
            &commit_message,
            &tree,
            &parents,
        )?;

        println!(
            "serialized to {} ({})",
            ref_name,
            &commit_oid.to_string()[..8]
        );

        // Auto-prune only for main destination
        if dest == MAIN_DEST {
            if let Some(ref prune_rules) = prune_rules {
                if verbose {
                    eprintln!(
                        "[verbose] auto-prune rules: since={}, min_size={}",
                        prune_rules.since,
                        prune_rules
                            .min_size
                            .map(|s| s.to_string())
                            .unwrap_or_else(|| "none".to_string())
                    );
                }
                if auto::should_prune(repo, tree_oid, prune_rules)? {
                    eprintln!(
                        "Auto-prune triggered, pruning with --since={}...",
                        prune_rules.since
                    );

                    if verbose {
                        let cutoff_ms = parse_since_to_cutoff_ms(&prune_rules.since)?;
                        eprintln!(
                            "[verbose] prune cutoff: {} ({}ms)",
                            chrono::DateTime::from_timestamp_millis(cutoff_ms)
                                .map(|d| d.to_rfc3339())
                                .unwrap_or_else(|| "?".to_string()),
                            cutoff_ms
                        );
                    }

                    let prune_tree_oid = prune_tree(repo, tree_oid, prune_rules, &ctx.db, verbose)?;

                    if prune_tree_oid != tree_oid {
                        if verbose {
                            eprintln!(
                                "[verbose] pruned tree: {} (changed from {})",
                                prune_tree_oid, tree_oid
                            );
                        }
                        let prune_tree = repo.find_tree(prune_tree_oid)?;
                        let prune_parent = repo.find_reference(&ref_name)?.peel_to_commit()?;

                        let (keys_dropped, keys_retained) =
                            count_prune_stats(repo, tree_oid, prune_tree_oid)?;

                        if verbose {
                            eprintln!(
                                "[verbose] prune stats: {} keys dropped, {} keys retained",
                                keys_dropped, keys_retained
                            );
                        }

                        let min_size_str = prune_rules
                            .min_size
                            .map(|s| format!("\nmin-size: {}", s))
                            .unwrap_or_default();

                        let message = format!(
                            "gmeta: prune --since={}\n\npruned: true\nsince: {}{}\nkeys-dropped: {}\nkeys-retained: {}",
                            prune_rules.since, prune_rules.since, min_size_str, keys_dropped, keys_retained
                        );

                        let prune_commit_oid = repo.commit(
                            Some(&ref_name),
                            &sig,
                            &sig,
                            &message,
                            &prune_tree,
                            &[&prune_parent],
                        )?;

                        println!(
                            "auto-pruned to {} ({})",
                            ref_name,
                            &prune_commit_oid.to_string()[..8]
                        );
                    } else {
                        eprintln!(
                            "Auto-prune: tree unchanged after pruning, skipping prune commit"
                        );
                    }
                } else if verbose {
                    eprintln!("[verbose] auto-prune: conditions not met, skipping");
                }
            } else if verbose {
                eprintln!("[verbose] no auto-prune rules configured");
            }
        }
    }

    let now = Utc::now().timestamp_millis();
    ctx.db.set_last_materialized(now)?;

    Ok(())
}

/// Build a Git tree from pre-filtered metadata (no incremental mode).
/// Used by `gmeta prune` to rebuild a tree from only the surviving entries.
pub fn build_filtered_tree(
    repo: &git2::Repository,
    metadata_entries: &[(String, String, String, String, ValueType, i64, bool)],
    tombstone_entries: &[(String, String, String, i64, String)],
    set_tombstone_entries: &[(String, String, String, String, String, i64, String)],
    list_tombstone_entries: &[(String, String, String, String, i64, String)],
) -> Result<git2::Oid> {
    build_tree(
        repo,
        metadata_entries,
        tombstone_entries,
        set_tombstone_entries,
        list_tombstone_entries,
        None,
        None,
        false,
    )
}

/// Build a complete Git tree from all metadata entries.
/// When `existing_tree` and `dirty_target_bases` are provided, only entries
/// belonging to dirty targets are processed; unchanged subtrees are reused
/// from the existing tree by OID.
fn build_tree(
    repo: &git2::Repository,
    metadata_entries: &[(String, String, String, String, ValueType, i64, bool)],
    tombstone_entries: &[(String, String, String, i64, String)],
    set_tombstone_entries: &[(String, String, String, String, String, i64, String)],
    list_tombstone_entries: &[(String, String, String, String, i64, String)],
    existing_tree: Option<&git2::Tree>,
    dirty_target_bases: Option<&BTreeSet<String>>,
    verbose: bool,
) -> Result<git2::Oid> {
    // Collect file paths -> blob content, skipping clean targets in incremental mode
    let mut files: BTreeMap<String, Vec<u8>> = BTreeMap::new();
    let mut skipped_entries = 0u64;

    for (target_type, target_value, key, value, value_type, _last_timestamp, is_git_ref) in
        metadata_entries
    {
        let target = if target_type == "project" {
            Target::parse("project")?
        } else {
            Target::parse(&format!("{}:{}", target_type, target_value))?
        };

        // Skip entries for clean targets — their subtrees will be reused
        if let Some(dirty) = dirty_target_bases {
            if !dirty.contains(&target.tree_base_path()) {
                skipped_entries += 1;
                continue;
            }
        }

        match value_type {
            ValueType::String => {
                let full_path = build_tree_path(&target, key)?;
                if *is_git_ref {
                    let oid = git2::Oid::from_str(value)?;
                    let blob = repo.find_blob(oid)?;
                    if verbose {
                        eprintln!(
                            "[verbose] tree: {} -> <git-blob {} bytes>",
                            full_path,
                            blob.content().len()
                        );
                    }
                    files.insert(full_path, blob.content().to_vec());
                } else {
                    let raw_value: String = match serde_json::from_str(value) {
                        Ok(s) => s,
                        Err(e) => {
                            eprintln!(
                                "warning: key '{}' on {}:{} is not a JSON string ({}), storing raw JSON",
                                key, target_type, &target_value[..7.min(target_value.len())], e
                            );
                            value.to_string()
                        }
                    };
                    if verbose {
                        eprintln!("[verbose] tree: {} -> {} bytes", full_path, raw_value.len());
                    }
                    files.insert(full_path, raw_value.into_bytes());
                }
            }
            ValueType::List => {
                let list_entries = parse_entries(value).context("failed to decode list value")?;
                let list_dir_path = build_list_tree_dir_path(&target, key)?;
                if verbose {
                    eprintln!(
                        "[verbose] tree: {}/ -> {} list entries",
                        list_dir_path,
                        list_entries.len()
                    );
                }
                for entry in list_entries {
                    let entry_name = make_entry_name(&entry);
                    let full_path = format!("{}/{}", list_dir_path, entry_name);
                    files.insert(full_path, entry.value.into_bytes());
                }
            }
            ValueType::Set => {
                let members: Vec<String> =
                    serde_json::from_str(value).context("failed to decode set value")?;
                let set_dir_path = build_set_tree_dir_path(&target, key)?;
                if verbose {
                    eprintln!(
                        "[verbose] tree: {}/ -> {} set members",
                        set_dir_path,
                        members.len()
                    );
                }
                for member in members {
                    let member_id = gmeta_core::types::set_member_id(&member);
                    let full_path = format!("{}/{}", set_dir_path, member_id);
                    files.insert(full_path, member.into_bytes());
                }
            }
        }
    }

    for (target_type, target_value, key, timestamp, email) in tombstone_entries {
        let target = if target_type == "project" {
            Target::parse("project")?
        } else {
            Target::parse(&format!("{}:{}", target_type, target_value))?
        };

        if let Some(dirty) = dirty_target_bases {
            if !dirty.contains(&target.tree_base_path()) {
                continue;
            }
        }

        let full_path = build_tombstone_tree_path(&target, key)?;
        if verbose {
            eprintln!("[verbose] tree: {} -> tombstone", full_path);
        }
        let payload = serde_json::to_vec(&TombstoneBlob {
            timestamp: *timestamp,
            email,
        })?;
        files.insert(full_path, payload);
    }

    for (target_type, target_value, key, member_id, value, _timestamp, _email) in
        set_tombstone_entries
    {
        let target = if target_type == "project" {
            Target::parse("project")?
        } else {
            Target::parse(&format!("{}:{}", target_type, target_value))?
        };

        if let Some(dirty) = dirty_target_bases {
            if !dirty.contains(&target.tree_base_path()) {
                continue;
            }
        }

        let full_path = build_set_member_tombstone_tree_path(&target, key, member_id)?;
        if verbose {
            eprintln!("[verbose] tree: {} -> set tombstone ({})", full_path, value);
        }
        files.insert(full_path, value.as_bytes().to_vec());
    }

    for (target_type, target_value, key, entry_name, timestamp, email) in list_tombstone_entries {
        let target = if target_type == "project" {
            Target::parse("project")?
        } else {
            Target::parse(&format!("{}:{}", target_type, target_value))?
        };

        if let Some(dirty) = dirty_target_bases {
            if !dirty.contains(&target.tree_base_path()) {
                continue;
            }
        }

        let full_path = build_list_entry_tombstone_tree_path(&target, key, entry_name)?;
        if verbose {
            eprintln!("[verbose] tree: {} -> list tombstone", full_path);
        }
        let payload = serde_json::to_vec(&TombstoneBlob {
            timestamp: *timestamp,
            email,
        })?;
        files.insert(full_path, payload);
    }

    if verbose {
        eprintln!(
            "[verbose] total tree paths: {} (skipped {} clean entries)",
            files.len(),
            skipped_entries
        );
    }

    // Build nested tree, reusing unchanged subtrees from existing tree
    if let (Some(existing), Some(dirty_bases)) = (existing_tree, dirty_target_bases) {
        if verbose {
            eprintln!(
                "[verbose] incremental tree build: patching existing tree with {} dirty targets",
                dirty_bases.len()
            );
        }
        build_tree_incremental(repo, existing, &files, dirty_bases)
    } else {
        build_tree_from_paths(repo, &files)
    }
}

#[derive(Default)]
struct Dir {
    files: BTreeMap<String, Vec<u8>>,
    dirs: BTreeMap<String, Dir>,
}

fn insert_path(dir: &mut Dir, parts: &[&str], content: Vec<u8>) {
    if parts.len() == 1 {
        dir.files.insert(parts[0].to_string(), content);
    } else {
        let child = dir.dirs.entry(parts[0].to_string()).or_default();
        insert_path(child, &parts[1..], content);
    }
}

fn build_dir(repo: &git2::Repository, dir: &Dir) -> Result<git2::Oid> {
    let mut tb = repo.treebuilder(None)?;

    for (name, content) in &dir.files {
        let blob_oid = repo.blob(content)?;
        tb.insert(name, blob_oid, 0o100644)?;
    }

    for (name, child_dir) in &dir.dirs {
        let child_oid = build_dir(repo, child_dir)?;
        tb.insert(name, child_oid, 0o040000)?;
    }

    Ok(tb.write()?)
}

/// Build a nested Git tree structure from flat file paths (full rebuild).
fn build_tree_from_paths(
    repo: &git2::Repository,
    files: &BTreeMap<String, Vec<u8>>,
) -> Result<git2::Oid> {
    let mut root = Dir::default();

    for (path, content) in files {
        let parts: Vec<&str> = path.split('/').collect();
        insert_path(&mut root, &parts, content.clone());
    }

    build_dir(repo, &root)
}

/// Incrementally build a tree by patching an existing tree.
/// Only dirty target subtrees are rebuilt from `files`; all other subtrees
/// are reused from the existing tree by OID.
///
/// Strategy:
/// 1. Remove dirty target subtrees from the existing tree
/// 2. Build a Dir from the dirty files
/// 3. Merge the new Dir into the cleaned tree
fn build_tree_incremental(
    repo: &git2::Repository,
    existing_tree: &git2::Tree,
    files: &BTreeMap<String, Vec<u8>>,
    dirty_target_bases: &BTreeSet<String>,
) -> Result<git2::Oid> {
    // Step 1: Remove dirty target subtrees from existing tree
    let cleaned_oid = remove_subtrees(repo, existing_tree, dirty_target_bases)?;
    let cleaned_tree = repo.find_tree(cleaned_oid)?;

    // Step 2: Build Dir from dirty files only
    let mut root = Dir::default();
    for (path, content) in files {
        let parts: Vec<&str> = path.split('/').collect();
        insert_path(&mut root, &parts, content.clone());
    }

    // Step 3: Merge new content into cleaned tree
    merge_dir_into_tree(repo, &root, &cleaned_tree)
}

/// Remove subtrees at specific paths from an existing tree.
/// Paths like "commit/ab/abc123" are recursively navigated and the leaf
/// entry is removed, cleaning up empty parent dirs.
fn remove_subtrees(
    repo: &git2::Repository,
    tree: &git2::Tree,
    paths: &BTreeSet<String>,
) -> Result<git2::Oid> {
    let mut grouped: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    let mut direct_removes: BTreeSet<String> = BTreeSet::new();

    for path in paths {
        if let Some((first, rest)) = path.split_once('/') {
            grouped
                .entry(first.to_string())
                .or_default()
                .insert(rest.to_string());
        } else {
            direct_removes.insert(path.clone());
        }
    }

    let mut tb = repo.treebuilder(Some(tree))?;

    for name in &direct_removes {
        let _ = tb.remove(name);
    }

    for (name, sub_paths) in &grouped {
        if let Some(entry) = tree.get_name(name) {
            if entry.kind() == Some(git2::ObjectType::Tree) {
                let subtree = repo.find_tree(entry.id())?;
                let new_oid = remove_subtrees(repo, &subtree, sub_paths)?;
                let new_tree = repo.find_tree(new_oid)?;
                if !new_tree.is_empty() {
                    tb.insert(name, new_oid, 0o040000)?;
                } else {
                    let _ = tb.remove(name);
                }
            }
        }
    }

    Ok(tb.write()?)
}

/// Merge a Dir structure into an existing tree.
/// Existing entries not present in `dir` are preserved.
/// Entries in `dir` overwrite existing entries with the same name.
fn merge_dir_into_tree(
    repo: &git2::Repository,
    dir: &Dir,
    existing: &git2::Tree,
) -> Result<git2::Oid> {
    let mut tb = repo.treebuilder(Some(existing))?;

    for (name, content) in &dir.files {
        let blob_oid = repo.blob(content)?;
        tb.insert(name, blob_oid, 0o100644)?;
    }

    for (name, child_dir) in &dir.dirs {
        let existing_child = existing
            .get_name(name)
            .filter(|e| e.kind() == Some(git2::ObjectType::Tree))
            .and_then(|e| repo.find_tree(e.id()).ok());

        let child_oid = if let Some(ref existing_child) = existing_child {
            merge_dir_into_tree(repo, child_dir, existing_child)?
        } else {
            build_dir(repo, child_dir)?
        };
        tb.insert(name, child_oid, 0o040000)?;
    }

    Ok(tb.write()?)
}

/// Prune a serialized tree by dropping entries older than the cutoff.
/// Returns the OID of the new (possibly smaller) tree.
pub fn prune_tree(
    repo: &git2::Repository,
    tree_oid: git2::Oid,
    rules: &auto::PruneRules,
    db: &Db,
    verbose: bool,
) -> Result<git2::Oid> {
    let cutoff_ms = parse_since_to_cutoff_ms(&rules.since)?;
    let min_size = rules.min_size.unwrap_or(0);
    let tree = repo.find_tree(tree_oid)?;

    // Walk the tree and rebuild, skipping old entries.
    // The top-level directories are target type dirs (commit, branch, path, project, change-id).
    // "project" subtree is never pruned.
    let mut tb = repo.treebuilder(None)?;

    for entry in tree.iter() {
        let name = entry.name().unwrap_or("").to_string();

        if name == "project" {
            // Never prune project metadata
            if verbose {
                eprintln!("[verbose] prune: keeping project/ (never pruned)");
            }
            tb.insert(&name, entry.id(), entry.filemode())?;
            continue;
        }

        if entry.kind() == Some(git2::ObjectType::Tree) {
            let subtree = repo.find_tree(entry.id())?;

            // Check min-size: if the subtree is smaller than the threshold, keep it
            if min_size > 0 {
                let size = auto::compute_tree_size_for(repo, &subtree)?;
                if size < min_size {
                    if verbose {
                        eprintln!(
                            "[verbose] prune: keeping {}/ (size {} < min_size {})",
                            name, size, min_size
                        );
                    }
                    tb.insert(&name, entry.id(), entry.filemode())?;
                    continue;
                }
                if verbose {
                    eprintln!(
                        "[verbose] prune: checking {}/ (size {} >= min_size {})",
                        name, size, min_size
                    );
                }
            }

            let pruned_oid =
                prune_target_type_tree(repo, &subtree, cutoff_ms, min_size, db, verbose, &name)?;
            // Only include if the pruned tree is non-empty
            let pruned_tree = repo.find_tree(pruned_oid)?;
            if !pruned_tree.is_empty() {
                if verbose && pruned_oid != entry.id() {
                    eprintln!(
                        "[verbose] prune: {}/ reduced from {} to {} entries",
                        name,
                        subtree.len(),
                        pruned_tree.len()
                    );
                }
                tb.insert(&name, pruned_oid, entry.filemode())?;
            } else if verbose {
                eprintln!("[verbose] prune: {}/ entirely pruned (empty)", name);
            }
        } else {
            tb.insert(&name, entry.id(), entry.filemode())?;
        }
    }

    Ok(tb.write()?)
}

/// Prune within a target-type directory (e.g. the "commit" dir which contains fanout dirs).
fn prune_target_type_tree(
    repo: &git2::Repository,
    tree: &git2::Tree,
    cutoff_ms: i64,
    min_size: u64,
    db: &Db,
    verbose: bool,
    parent_path: &str,
) -> Result<git2::Oid> {
    let mut tb = repo.treebuilder(None)?;

    for entry in tree.iter() {
        let name = entry.name().unwrap_or("").to_string();

        if entry.kind() == Some(git2::ObjectType::Tree) {
            let subtree = repo.find_tree(entry.id())?;
            let pruned_oid = prune_subtree_recursive(
                repo,
                &subtree,
                cutoff_ms,
                min_size,
                db,
                verbose,
                &format!("{}/{}", parent_path, name),
            )?;
            let pruned_tree = repo.find_tree(pruned_oid)?;
            if !pruned_tree.is_empty() {
                tb.insert(&name, pruned_oid, entry.filemode())?;
            } else if verbose {
                eprintln!("[verbose] prune: {}/{}/ entirely pruned", parent_path, name);
            }
        } else {
            tb.insert(&name, entry.id(), entry.filemode())?;
        }
    }

    Ok(tb.write()?)
}

/// Recursively prune a subtree. Drops old list entries and old tombstones.
/// For key-level entries (__value, __set), checks the last_timestamp from the DB.
fn prune_subtree_recursive(
    repo: &git2::Repository,
    tree: &git2::Tree,
    cutoff_ms: i64,
    _min_size: u64,
    _db: &Db,
    verbose: bool,
    parent_path: &str,
) -> Result<git2::Oid> {
    let mut tb = repo.treebuilder(None)?;

    for entry in tree.iter() {
        let name = entry.name().unwrap_or("").to_string();

        match entry.kind() {
            Some(git2::ObjectType::Tree) => {
                if name == "__list" {
                    // Prune individual list entries by timestamp in their name
                    let list_tree = repo.find_tree(entry.id())?;
                    let before_count = list_tree.len();
                    let pruned_oid = prune_list_tree(repo, &list_tree, cutoff_ms)?;
                    let pruned_tree = repo.find_tree(pruned_oid)?;
                    if verbose && pruned_tree.len() < before_count {
                        eprintln!(
                            "[verbose] prune: {}/__list/ dropped {} of {} entries",
                            parent_path,
                            before_count - pruned_tree.len(),
                            before_count
                        );
                    }
                    if !pruned_tree.is_empty() {
                        tb.insert(&name, pruned_oid, entry.filemode())?;
                    }
                } else if name == "__tombstones" {
                    // Prune old tombstones
                    let tomb_tree = repo.find_tree(entry.id())?;
                    let before_count = tomb_tree.len();
                    let pruned_oid = prune_tombstone_tree(repo, &tomb_tree, cutoff_ms)?;
                    let pruned_tree = repo.find_tree(pruned_oid)?;
                    if verbose && pruned_tree.len() < before_count {
                        eprintln!(
                            "[verbose] prune: {}/__tombstones/ dropped {} of {} entries",
                            parent_path,
                            before_count - pruned_tree.len(),
                            before_count
                        );
                    }
                    if !pruned_tree.is_empty() {
                        tb.insert(&name, pruned_oid, entry.filemode())?;
                    }
                } else {
                    // Recurse into key segment directories
                    let subtree = repo.find_tree(entry.id())?;
                    let pruned_oid = prune_subtree_recursive(
                        repo,
                        &subtree,
                        cutoff_ms,
                        _min_size,
                        _db,
                        verbose,
                        &format!("{}/{}", parent_path, name),
                    )?;
                    let pruned_tree = repo.find_tree(pruned_oid)?;
                    if !pruned_tree.is_empty() {
                        tb.insert(&name, pruned_oid, entry.filemode())?;
                    }
                }
            }
            Some(git2::ObjectType::Blob) => {
                // Keep all blobs — __value and __set members are kept if their parent
                // key dir survives the prune. The key-level timestamp filtering happens
                // at the DB level during the full rebuild that prune_tree orchestrates.
                tb.insert(&name, entry.id(), entry.filemode())?;
            }
            _ => {
                tb.insert(&name, entry.id(), entry.filemode())?;
            }
        }
    }

    Ok(tb.write()?)
}

/// Prune list entries by the timestamp encoded in their file name.
fn prune_list_tree(
    repo: &git2::Repository,
    tree: &git2::Tree,
    cutoff_ms: i64,
) -> Result<git2::Oid> {
    let mut tb = repo.treebuilder(None)?;

    for entry in tree.iter() {
        let name = entry.name().unwrap_or("");
        // Entry names are formatted as "{timestamp_ms}-{hash5}"
        if let Some((ts_str, _)) = name.split_once('-') {
            if let Ok(ts) = ts_str.parse::<i64>() {
                if ts < cutoff_ms {
                    continue; // Drop old entry
                }
            }
        }
        tb.insert(name, entry.id(), entry.filemode())?;
    }

    Ok(tb.write()?)
}

/// Prune tombstone entries by the timestamp in the __deleted blob.
fn prune_tombstone_tree(
    repo: &git2::Repository,
    tree: &git2::Tree,
    cutoff_ms: i64,
) -> Result<git2::Oid> {
    let mut tb = repo.treebuilder(None)?;

    for entry in tree.iter() {
        let name = entry.name().unwrap_or("").to_string();

        match entry.kind() {
            Some(git2::ObjectType::Tree) => {
                // Tombstone key directory — recurse to find __deleted blob
                let subtree = repo.find_tree(entry.id())?;
                let pruned_oid = prune_tombstone_tree(repo, &subtree, cutoff_ms)?;
                let pruned_tree = repo.find_tree(pruned_oid)?;
                if !pruned_tree.is_empty() {
                    tb.insert(&name, pruned_oid, entry.filemode())?;
                }
            }
            Some(git2::ObjectType::Blob) if name == "__deleted" => {
                // Parse the tombstone blob to check timestamp
                let blob = repo.find_blob(entry.id())?;
                if let Ok(content) = std::str::from_utf8(blob.content()) {
                    if let Ok(parsed) = serde_json::from_str::<serde_json::Value>(content) {
                        if let Some(ts) = parsed.get("timestamp").and_then(|v| v.as_i64()) {
                            if ts < cutoff_ms {
                                continue; // Drop old tombstone
                            }
                        }
                    }
                }
                tb.insert(&name, entry.id(), entry.filemode())?;
            }
            _ => {
                tb.insert(&name, entry.id(), entry.filemode())?;
            }
        }
    }

    Ok(tb.write()?)
}

/// Count keys in original and pruned trees to produce stats.
pub fn count_prune_stats(
    repo: &git2::Repository,
    original_oid: git2::Oid,
    pruned_oid: git2::Oid,
) -> Result<(u64, u64)> {
    let original_tree = repo.find_tree(original_oid)?;
    let pruned_tree = repo.find_tree(pruned_oid)?;

    let mut original_count = 0u64;
    count_all_blobs(repo, &original_tree, &mut original_count)?;

    let mut pruned_count = 0u64;
    count_all_blobs(repo, &pruned_tree, &mut pruned_count)?;

    let dropped = original_count.saturating_sub(pruned_count);
    Ok((dropped, pruned_count))
}

fn count_all_blobs(repo: &git2::Repository, tree: &git2::Tree, count: &mut u64) -> Result<()> {
    for entry in tree.iter() {
        match entry.kind() {
            Some(git2::ObjectType::Blob) => *count += 1,
            Some(git2::ObjectType::Tree) => {
                let subtree = repo.find_tree(entry.id())?;
                count_all_blobs(repo, &subtree, count)?;
            }
            _ => {}
        }
    }
    Ok(())
}
