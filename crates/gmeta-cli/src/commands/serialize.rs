use gix::bstr::ByteSlice;
use gix::prelude::ObjectIdExt;
use std::collections::{BTreeMap, BTreeSet};

use anyhow::{Context, Result};
use gix::refs::transaction::PreviousValue;
use time::OffsetDateTime;

use crate::commands::prune::auto::{self, parse_since_to_cutoff_ms};
use crate::context::CommandContext;
use gmeta_core::git_utils;
use gmeta_core::list_value::{make_entry_name, parse_entries};
use gmeta_core::tree::filter::{classify_key, parse_filter_rules, MAIN_DEST};
use gmeta_core::tree::format::{build_dir, build_tree_from_paths, insert_path, TreeDir};
use gmeta_core::tree::model::Tombstone;
use gmeta_core::types::{Target, TargetType, ValueType};
use gmeta_core::Store;

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
    let ctx = CommandContext::open(None)?;
    let repo = ctx.repo();

    let local_ref_name = ctx.local_ref();
    let last_materialized = ctx.store().get_last_materialized()?;

    if verbose {
        eprintln!("[verbose] local ref: {}", local_ref_name);
        eprintln!(
            "[verbose] last materialized: {}",
            match last_materialized {
                Some(ts) => format!(
                    "{} ({}ms)",
                    OffsetDateTime::from_unix_timestamp_nanos(ts as i128 * 1_000_000)
                        .ok()
                        .and_then(|d| d
                            .format(&time::format_description::well_known::Rfc3339)
                            .ok())
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
    let existing_tree_oid = repo
        .find_reference(&local_ref_name)
        .ok()
        .and_then(|r| r.into_fully_peeled_id().ok())
        .and_then(|id| {
            id.object()
                .ok()?
                .into_commit()
                .tree_id()
                .ok()
                .map(|t| t.detach())
        });

    if verbose {
        if let Some(ref tree_oid) = existing_tree_oid {
            let tree = tree_oid.attach(repo).object().ok().map(|o| o.into_tree());
            let count = tree.map(|t| t.iter().count()).unwrap_or(0);
            eprintln!(
                "[verbose] existing tree: {} ({} top-level entries)",
                tree_oid, count
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
        let modified = ctx.store().get_modified_since(since)?;
        if verbose {
            eprintln!(
                "[verbose] incremental mode: {} entries modified since last materialize",
                modified.len()
            );
        }
        if modified.is_empty() && existing_tree_oid.is_some() {
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
                        if existing_tree_oid.is_some() {
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

        let metadata = ctx.store().get_all_metadata()?;
        let tombstones = ctx.store().get_all_tombstones()?;
        let set_tombstones = ctx.store().get_all_set_tombstones()?;
        let list_tombstones = ctx.store().get_all_list_tombstones()?;

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
            if existing_tree_oid.is_some() {
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

        let metadata = ctx.store().get_all_metadata()?;

        // Full serialize: all entries are adds
        let changes: Vec<(char, String, String)> = metadata
            .iter()
            .map(|e| {
                let target_label = if e.target_type == "project" {
                    "project".to_string()
                } else {
                    format!("{}:{}", e.target_type, e.target_value)
                };
                ('A', target_label, e.key.clone())
            })
            .collect();

        (
            metadata,
            ctx.store().get_all_tombstones()?,
            ctx.store().get_all_set_tombstones()?,
            ctx.store().get_all_list_tombstones()?,
            None,
            changes,
        )
    };

    if metadata_entries.is_empty() && tombstone_entries.is_empty() {
        println!("no metadata to serialize");
        return Ok(());
    }

    // If meta:prune:since is configured, drop entries older than the cutoff
    // before building the tree.
    let prune_since = ctx
        .store()
        .get(&TargetType::Project, "", "meta:prune:since")?
        .and_then(|e| serde_json::from_str::<String>(&e.value).ok());
    let prune_rules = auto::read_prune_rules(ctx.store())?;
    let prune_cutoff_ms = prune_since
        .as_deref()
        .map(parse_since_to_cutoff_ms)
        .transpose()?;
    let mut pruned_count = 0u64;
    let metadata_entries = if let Some(cutoff) = prune_cutoff_ms {
        metadata_entries
            .into_iter()
            .filter(|e| {
                if e.target_type != "project" && e.last_timestamp < cutoff {
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

    let filter_rules = parse_filter_rules(ctx.store())?;
    if verbose && !filter_rules.is_empty() {
        eprintln!("[verbose] filter rules: {}", filter_rules.len());
        for rule in &filter_rules {
            eprintln!("  {:?}", rule);
        }
    }

    use gmeta_core::db::types::SerializableEntry;
    type TombEntry = (String, String, String, i64, String);
    type SetTombEntry = (String, String, String, String, String, i64, String);
    type ListTombEntry = (String, String, String, String, i64, String);

    let mut dest_metadata: BTreeMap<String, Vec<SerializableEntry>> = BTreeMap::new();
    let mut dest_tombstones: BTreeMap<String, Vec<TombEntry>> = BTreeMap::new();
    let mut dest_set_tombstones: BTreeMap<String, Vec<SetTombEntry>> = BTreeMap::new();
    let mut dest_list_tombstones: BTreeMap<String, Vec<ListTombEntry>> = BTreeMap::new();
    let mut excluded_count = 0u64;

    for entry in &metadata_entries {
        let key = &entry.key;
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
            for e in entries {
                let label = if e.target_type == "project" {
                    "project".to_string()
                } else {
                    format!(
                        "{}:{}",
                        e.target_type,
                        &e.target_value[..7.min(e.target_value.len())]
                    )
                };
                targets.insert(label);
                match e.value_type {
                    ValueType::String => string_count += 1,
                    ValueType::List => list_count += 1,
                    ValueType::Set => set_count += 1,
                    _ => {}
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
        let _ = total_meta; // suppress unused warning
    }

    let name = git_utils::get_name(repo)?;
    let email = git_utils::get_email(repo)?;
    let sig = gix::actor::Signature {
        name: name.into(),
        email: email.into(),
        time: gix::date::Time::now_local_or_utc(),
    };

    for dest in &all_dests {
        let ref_name = ctx.destination_ref(dest);
        let empty_meta: Vec<SerializableEntry> = Vec::new();
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
            (existing_tree_oid, dirty_target_bases.as_ref())
        } else {
            (None, None)
        };

        eprintln!("Building git tree for {}...", ref_name);
        let tree_oid = build_tree(
            repo, meta, tombs, set_tombs, list_tombs, existing, dirty, verbose,
        )?;

        if verbose {
            let tree = tree_oid.attach(repo).object()?.into_tree();
            eprintln!(
                "[verbose] built tree {} ({} top-level entries)",
                tree_oid,
                tree.iter().count()
            );
        }

        let parent_oid = repo
            .find_reference(&ref_name)
            .ok()
            .and_then(|r| r.into_fully_peeled_id().ok())
            .map(|id| id.detach());

        if verbose {
            if let Some(ref p) = parent_oid {
                eprintln!(
                    "[verbose] parent commit for {}: {}",
                    ref_name,
                    &p.to_string()[..8]
                );
            } else {
                eprintln!("[verbose] no parent commit for {} (root)", ref_name);
            }
        }

        let parents: Vec<gix::ObjectId> = parent_oid.into_iter().collect();
        let commit_message = build_commit_message(&changes);
        let commit = gix::objs::Commit {
            message: commit_message.into(),
            tree: tree_oid,
            author: sig.clone(),
            committer: sig.clone(),
            encoding: None,
            parents: parents.into(),
            extra_headers: Default::default(),
        };

        let commit_oid = repo.write_object(&commit)?.detach();
        repo.reference(
            ref_name.as_str(),
            commit_oid,
            PreviousValue::Any,
            "gmeta: serialize",
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
                            OffsetDateTime::from_unix_timestamp_nanos(
                                cutoff_ms as i128 * 1_000_000
                            )
                            .ok()
                            .and_then(|d| d
                                .format(&time::format_description::well_known::Rfc3339)
                                .ok())
                            .unwrap_or_else(|| "?".to_string()),
                            cutoff_ms
                        );
                    }

                    let prune_tree_oid =
                        prune_tree(repo, tree_oid, prune_rules, ctx.store(), verbose)?;

                    if prune_tree_oid != tree_oid {
                        if verbose {
                            eprintln!(
                                "[verbose] pruned tree: {} (changed from {})",
                                prune_tree_oid, tree_oid
                            );
                        }

                        let prune_parent_oid = repo
                            .find_reference(&ref_name)?
                            .into_fully_peeled_id()?
                            .detach();

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

                        let prune_commit = gix::objs::Commit {
                            message: message.into(),
                            tree: prune_tree_oid,
                            author: sig.clone(),
                            committer: sig.clone(),
                            encoding: None,
                            parents: vec![prune_parent_oid].into(),
                            extra_headers: Default::default(),
                        };

                        let prune_commit_oid = repo.write_object(&prune_commit)?.detach();
                        repo.reference(
                            ref_name.as_str(),
                            prune_commit_oid,
                            PreviousValue::Any,
                            "gmeta: auto-prune",
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

    let now = OffsetDateTime::now_utc().unix_timestamp_nanos() as i64 / 1_000_000;
    ctx.store().set_last_materialized(now)?;

    Ok(())
}

/// Build a Git tree from pre-filtered metadata (no incremental mode).
/// Used by `gmeta prune` to rebuild a tree from only the surviving entries.
pub fn build_filtered_tree(
    repo: &gix::Repository,
    metadata_entries: &[gmeta_core::db::types::SerializableEntry],
    tombstone_entries: &[(String, String, String, i64, String)],
    set_tombstone_entries: &[(String, String, String, String, String, i64, String)],
    list_tombstone_entries: &[(String, String, String, String, i64, String)],
) -> Result<gix::ObjectId> {
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
/// When `existing_tree_oid` and `dirty_target_bases` are provided, only entries
/// belonging to dirty targets are processed; unchanged subtrees are reused
/// from the existing tree by OID.
fn build_tree(
    repo: &gix::Repository,
    metadata_entries: &[gmeta_core::db::types::SerializableEntry],
    tombstone_entries: &[(String, String, String, i64, String)],
    set_tombstone_entries: &[(String, String, String, String, String, i64, String)],
    list_tombstone_entries: &[(String, String, String, String, i64, String)],
    existing_tree_oid: Option<gix::ObjectId>,
    dirty_target_bases: Option<&BTreeSet<String>>,
    verbose: bool,
) -> Result<gix::ObjectId> {
    // Collect file paths -> blob content, skipping clean targets in incremental mode
    let mut files: BTreeMap<String, Vec<u8>> = BTreeMap::new();
    let mut skipped_entries = 0u64;

    for e in metadata_entries {
        let target = if e.target_type == "project" {
            Target::parse("project")?
        } else {
            Target::parse(&format!("{}:{}", e.target_type, e.target_value))?
        };

        // Skip entries for clean targets -- their subtrees will be reused
        if let Some(dirty) = dirty_target_bases {
            if !dirty.contains(&target.tree_base_path()) {
                skipped_entries += 1;
                continue;
            }
        }

        match e.value_type {
            ValueType::String => {
                let full_path = target.tree_path(&e.key)?;
                if e.is_git_ref {
                    let oid = gix::ObjectId::from_hex(e.value.as_bytes())?;
                    let blob = oid.attach(repo).object()?.into_blob();
                    if verbose {
                        eprintln!(
                            "[verbose] tree: {} -> <git-blob {} bytes>",
                            full_path,
                            blob.data.len()
                        );
                    }
                    files.insert(full_path, blob.data.to_vec());
                } else {
                    let raw_value: String = match serde_json::from_str(&e.value) {
                        Ok(s) => s,
                        Err(err) => {
                            eprintln!(
                                "warning: key '{}' on {}:{} is not a JSON string ({}), storing raw JSON",
                                e.key, e.target_type, &e.target_value[..7.min(e.target_value.len())], err
                            );
                            e.value.to_string()
                        }
                    };
                    if verbose {
                        eprintln!("[verbose] tree: {} -> {} bytes", full_path, raw_value.len());
                    }
                    files.insert(full_path, raw_value.into_bytes());
                }
            }
            ValueType::List => {
                let list_entries =
                    parse_entries(&e.value).context("failed to decode list value")?;
                let list_dir_path = target.list_dir_path(&e.key)?;
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
                    serde_json::from_str(&e.value).context("failed to decode set value")?;
                let set_dir_path = target.set_dir_path(&e.key)?;
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
            _ => anyhow::bail!("unsupported value type"),
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

        let full_path = target.tombstone_path(key)?;
        if verbose {
            eprintln!("[verbose] tree: {} -> tombstone", full_path);
        }
        let payload = serde_json::to_vec(&Tombstone {
            timestamp: *timestamp,
            email: email.clone(),
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

        let full_path = target.set_member_tombstone_path(key, member_id)?;
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

        let full_path = target.list_entry_tombstone_path(key, entry_name)?;
        if verbose {
            eprintln!("[verbose] tree: {} -> list tombstone", full_path);
        }
        let payload = serde_json::to_vec(&Tombstone {
            timestamp: *timestamp,
            email: email.clone(),
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
    if let (Some(existing_oid), Some(dirty_bases)) = (existing_tree_oid, dirty_target_bases) {
        if verbose {
            eprintln!(
                "[verbose] incremental tree build: patching existing tree with {} dirty targets",
                dirty_bases.len()
            );
        }
        build_tree_incremental(repo, existing_oid, &files, dirty_bases)
    } else {
        Ok(build_tree_from_paths(repo, &files)?)
    }
}

/// Incrementally build a tree by patching an existing tree.
/// Only dirty target subtrees are rebuilt from `files`; all other subtrees
/// are reused from the existing tree by OID.
fn build_tree_incremental(
    repo: &gix::Repository,
    existing_tree_oid: gix::ObjectId,
    files: &BTreeMap<String, Vec<u8>>,
    dirty_target_bases: &BTreeSet<String>,
) -> Result<gix::ObjectId> {
    // Step 1: Remove dirty target subtrees from existing tree
    let cleaned_oid = remove_subtrees(repo, existing_tree_oid, dirty_target_bases)?;

    // Step 2: Build TreeDir from dirty files only
    let mut root = TreeDir::default();
    for (path, content) in files {
        let parts: Vec<&str> = path.split('/').collect();
        insert_path(&mut root, &parts, content.clone());
    }

    // Step 3: Merge new content into cleaned tree
    merge_dir_into_tree(repo, &root, cleaned_oid)
}

/// Remove subtrees at specific paths from an existing tree.
fn remove_subtrees(
    repo: &gix::Repository,
    tree_oid: gix::ObjectId,
    paths: &BTreeSet<String>,
) -> Result<gix::ObjectId> {
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

    let mut editor = repo.edit_tree(tree_oid)?;

    for name in &direct_removes {
        // Try removing; ignore if not present
        let _ = editor.remove(name);
    }

    // For grouped paths, we need to recurse into subtrees
    let tree = tree_oid.attach(repo).object()?.into_tree();
    for (name, sub_paths) in &grouped {
        let entry = tree.iter().find_map(|e| {
            let e = e.ok()?;
            if e.filename().to_str_lossy() == *name && e.mode().is_tree() {
                Some(e.object_id())
            } else {
                None
            }
        });
        if let Some(subtree_oid) = entry {
            let new_oid = remove_subtrees(repo, subtree_oid, sub_paths)?;
            // Check if the resulting tree is empty
            let new_tree = new_oid.attach(repo).object()?.into_tree();
            if new_tree.iter().count() > 0 {
                editor.upsert(name, gix::objs::tree::EntryKind::Tree, new_oid)?;
            } else {
                let _ = editor.remove(name);
            }
        }
    }

    Ok(editor.write()?.detach())
}

/// Merge a TreeDir structure into an existing tree.
/// Existing entries not present in `dir` are preserved.
/// Entries in `dir` overwrite existing entries with the same name.
fn merge_dir_into_tree(
    repo: &gix::Repository,
    dir: &TreeDir,
    existing_oid: gix::ObjectId,
) -> Result<gix::ObjectId> {
    let mut editor = repo.edit_tree(existing_oid)?;

    for (name, content) in &dir.files {
        let blob_oid: gix::ObjectId = repo.write_blob(content)?.into();
        editor.upsert(name, gix::objs::tree::EntryKind::Blob, blob_oid)?;
    }

    let existing_tree = existing_oid.attach(repo).object()?.into_tree();
    for (name, child_dir) in &dir.dirs {
        let existing_child_oid = existing_tree.iter().find_map(|e| {
            let e = e.ok()?;
            if e.filename().to_str_lossy() == *name && e.mode().is_tree() {
                Some(e.object_id())
            } else {
                None
            }
        });

        let child_oid = if let Some(existing_child) = existing_child_oid {
            merge_dir_into_tree(repo, child_dir, existing_child)?
        } else {
            build_dir(repo, child_dir)?
        };
        editor.upsert(name, gix::objs::tree::EntryKind::Tree, child_oid)?;
    }

    Ok(editor.write()?.detach())
}

/// Prune a serialized tree by dropping entries older than the cutoff.
/// Returns the OID of the new (possibly smaller) tree.
pub fn prune_tree(
    repo: &gix::Repository,
    tree_oid: gix::ObjectId,
    rules: &auto::PruneRules,
    db: &Store,
    verbose: bool,
) -> Result<gix::ObjectId> {
    let cutoff_ms = parse_since_to_cutoff_ms(&rules.since)?;
    let min_size = rules.min_size.unwrap_or(0);

    let tree = tree_oid.attach(repo).object()?.into_tree();
    let mut editor = repo.empty_tree().edit()?;

    for entry_result in tree.iter() {
        let entry = entry_result?;
        let name = entry.filename().to_str_lossy().to_string();

        if name == "project" {
            if verbose {
                eprintln!("[verbose] prune: keeping project/ (never pruned)");
            }
            editor.upsert(&name, entry.mode().kind(), entry.object_id())?;
            continue;
        }

        if entry.mode().is_tree() {
            let subtree_oid = entry.object_id();

            // Check min-size
            if min_size > 0 {
                let size = auto::compute_tree_size_for(repo, subtree_oid)?;
                if size < min_size {
                    if verbose {
                        eprintln!(
                            "[verbose] prune: keeping {}/ (size {} < min_size {})",
                            name, size, min_size
                        );
                    }
                    editor.upsert(&name, entry.mode().kind(), subtree_oid)?;
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
                prune_target_type_tree(repo, subtree_oid, cutoff_ms, min_size, db, verbose, &name)?;
            let pruned_tree = pruned_oid.attach(repo).object()?.into_tree();
            if pruned_tree.iter().count() > 0 {
                if verbose && pruned_oid != subtree_oid {
                    let orig_tree = subtree_oid.attach(repo).object()?.into_tree();
                    eprintln!(
                        "[verbose] prune: {}/ reduced from {} to {} entries",
                        name,
                        orig_tree.iter().count(),
                        pruned_tree.iter().count()
                    );
                }
                editor.upsert(&name, gix::objs::tree::EntryKind::Tree, pruned_oid)?;
            } else if verbose {
                eprintln!("[verbose] prune: {}/ entirely pruned (empty)", name);
            }
        } else {
            editor.upsert(&name, entry.mode().kind(), entry.object_id())?;
        }
    }

    Ok(editor.write()?.detach())
}

fn prune_target_type_tree(
    repo: &gix::Repository,
    tree_oid: gix::ObjectId,
    cutoff_ms: i64,
    min_size: u64,
    db: &Store,
    verbose: bool,
    parent_path: &str,
) -> Result<gix::ObjectId> {
    let tree = tree_oid.attach(repo).object()?.into_tree();
    let mut editor = repo.empty_tree().edit()?;

    for entry_result in tree.iter() {
        let entry = entry_result?;
        let name = entry.filename().to_str_lossy().to_string();

        if entry.mode().is_tree() {
            let subtree_oid = entry.object_id();
            let pruned_oid = prune_subtree_recursive(
                repo,
                subtree_oid,
                cutoff_ms,
                min_size,
                db,
                verbose,
                &format!("{}/{}", parent_path, name),
            )?;
            let pruned_tree = pruned_oid.attach(repo).object()?.into_tree();
            if pruned_tree.iter().count() > 0 {
                editor.upsert(&name, gix::objs::tree::EntryKind::Tree, pruned_oid)?;
            } else if verbose {
                eprintln!("[verbose] prune: {}/{}/ entirely pruned", parent_path, name);
            }
        } else {
            editor.upsert(&name, entry.mode().kind(), entry.object_id())?;
        }
    }

    Ok(editor.write()?.detach())
}

fn prune_subtree_recursive(
    repo: &gix::Repository,
    tree_oid: gix::ObjectId,
    cutoff_ms: i64,
    _min_size: u64,
    _db: &Store,
    verbose: bool,
    parent_path: &str,
) -> Result<gix::ObjectId> {
    let tree = tree_oid.attach(repo).object()?.into_tree();
    let mut editor = repo.empty_tree().edit()?;

    for entry_result in tree.iter() {
        let entry = entry_result?;
        let name = entry.filename().to_str_lossy().to_string();

        if entry.mode().is_tree() {
            if name == "__list" {
                let list_tree_oid = entry.object_id();
                let list_tree = list_tree_oid.attach(repo).object()?.into_tree();
                let before_count = list_tree.iter().count();
                let pruned_oid = prune_list_tree(repo, list_tree_oid, cutoff_ms)?;
                let pruned_tree = pruned_oid.attach(repo).object()?.into_tree();
                if verbose && pruned_tree.iter().count() < before_count {
                    eprintln!(
                        "[verbose] prune: {}/__list/ dropped {} of {} entries",
                        parent_path,
                        before_count - pruned_tree.iter().count(),
                        before_count
                    );
                }
                if pruned_tree.iter().count() > 0 {
                    editor.upsert(&name, gix::objs::tree::EntryKind::Tree, pruned_oid)?;
                }
            } else if name == "__tombstones" {
                let tomb_tree_oid = entry.object_id();
                let tomb_tree = tomb_tree_oid.attach(repo).object()?.into_tree();
                let before_count = tomb_tree.iter().count();
                let pruned_oid = prune_tombstone_tree(repo, tomb_tree_oid, cutoff_ms)?;
                let pruned_tree = pruned_oid.attach(repo).object()?.into_tree();
                if verbose && pruned_tree.iter().count() < before_count {
                    eprintln!(
                        "[verbose] prune: {}/__tombstones/ dropped {} of {} entries",
                        parent_path,
                        before_count - pruned_tree.iter().count(),
                        before_count
                    );
                }
                if pruned_tree.iter().count() > 0 {
                    editor.upsert(&name, gix::objs::tree::EntryKind::Tree, pruned_oid)?;
                }
            } else {
                let subtree_oid = entry.object_id();
                let pruned_oid = prune_subtree_recursive(
                    repo,
                    subtree_oid,
                    cutoff_ms,
                    _min_size,
                    _db,
                    verbose,
                    &format!("{}/{}", parent_path, name),
                )?;
                let pruned_tree = pruned_oid.attach(repo).object()?.into_tree();
                if pruned_tree.iter().count() > 0 {
                    editor.upsert(&name, gix::objs::tree::EntryKind::Tree, pruned_oid)?;
                }
            }
        } else {
            editor.upsert(&name, entry.mode().kind(), entry.object_id())?;
        }
    }

    Ok(editor.write()?.detach())
}

fn prune_list_tree(
    repo: &gix::Repository,
    tree_oid: gix::ObjectId,
    cutoff_ms: i64,
) -> Result<gix::ObjectId> {
    let tree = tree_oid.attach(repo).object()?.into_tree();
    let mut editor = repo.empty_tree().edit()?;

    for entry_result in tree.iter() {
        let entry = entry_result?;
        let name = entry.filename().to_str_lossy().to_string();
        // Entry names are formatted as "{timestamp_ms}-{hash5}"
        if let Some((ts_str, _)) = name.split_once('-') {
            if let Ok(ts) = ts_str.parse::<i64>() {
                if ts < cutoff_ms {
                    continue; // Drop old entry
                }
            }
        }
        editor.upsert(&name, entry.mode().kind(), entry.object_id())?;
    }

    Ok(editor.write()?.detach())
}

fn prune_tombstone_tree(
    repo: &gix::Repository,
    tree_oid: gix::ObjectId,
    cutoff_ms: i64,
) -> Result<gix::ObjectId> {
    let tree = tree_oid.attach(repo).object()?.into_tree();
    let mut editor = repo.empty_tree().edit()?;

    for entry_result in tree.iter() {
        let entry = entry_result?;
        let name = entry.filename().to_str_lossy().to_string();

        if entry.mode().is_tree() {
            let subtree_oid = entry.object_id();
            let pruned_oid = prune_tombstone_tree(repo, subtree_oid, cutoff_ms)?;
            let pruned_tree = pruned_oid.attach(repo).object()?.into_tree();
            if pruned_tree.iter().count() > 0 {
                editor.upsert(&name, gix::objs::tree::EntryKind::Tree, pruned_oid)?;
            }
        } else if entry.mode().is_blob() && name == "__deleted" {
            let blob = entry.object_id().attach(repo).object()?.into_blob();
            if let Ok(content) = std::str::from_utf8(&blob.data) {
                if let Ok(parsed) = serde_json::from_str::<serde_json::Value>(content) {
                    if let Some(ts) = parsed.get("timestamp").and_then(|v| v.as_i64()) {
                        if ts < cutoff_ms {
                            continue; // Drop old tombstone
                        }
                    }
                }
            }
            editor.upsert(&name, entry.mode().kind(), entry.object_id())?;
        } else {
            editor.upsert(&name, entry.mode().kind(), entry.object_id())?;
        }
    }

    Ok(editor.write()?.detach())
}

/// Count keys in original and pruned trees to produce stats.
pub fn count_prune_stats(
    repo: &gix::Repository,
    original_oid: gix::ObjectId,
    pruned_oid: gix::ObjectId,
) -> Result<(u64, u64)> {
    let mut original_count = 0u64;
    count_all_blobs(repo, original_oid, &mut original_count)?;

    let mut pruned_count = 0u64;
    count_all_blobs(repo, pruned_oid, &mut pruned_count)?;

    let dropped = original_count.saturating_sub(pruned_count);
    Ok((dropped, pruned_count))
}

fn count_all_blobs(repo: &gix::Repository, tree_oid: gix::ObjectId, count: &mut u64) -> Result<()> {
    let tree = tree_oid.attach(repo).object()?.into_tree();
    for entry_result in tree.iter() {
        let entry = entry_result?;
        if entry.mode().is_blob() {
            *count += 1;
        } else if entry.mode().is_tree() {
            count_all_blobs(repo, entry.object_id(), count)?;
        }
    }
    Ok(())
}
