use gix::prelude::ObjectIdExt;
use std::collections::{BTreeMap, BTreeSet};

use crate::context::CommandContext;
use anyhow::Result;
use gix::refs::transaction::PreviousValue;
use gmeta_core::list_value::{encode_entries, parse_timestamp_from_entry_name, ListEntry};
use gmeta_core::tree::format::{build_merged_tree, parse_tree};
use gmeta_core::tree::merge::{
    merge_list_tombstones, merge_set_member_tombstones, merge_tombstones, three_way_merge,
    two_way_merge_no_common_ancestor, ConflictDecision,
};
use gmeta_core::tree::model::{Key, ParsedTree, Tombstone, TreeValue};
use gmeta_core::types::TargetType;
use gmeta_core::Store;

#[derive(Debug, Clone, PartialEq, Eq)]
enum PlannedDbChange {
    Set {
        target_type: String,
        target_value: String,
        key: String,
        value_type: String,
        value_preview: String,
    },
    Remove {
        target_type: String,
        target_value: String,
        key: String,
    },
}

pub fn run(remote: Option<&str>, dry_run: bool, verbose: bool) -> Result<()> {
    let ctx = CommandContext::open(None)?;
    let repo = ctx.session.repo();

    let ns = ctx.session.namespace();
    let local_ref_name = ctx.session.local_ref();

    if verbose {
        eprintln!("[verbose] namespace: {}", ns);
        eprintln!("[verbose] local ref: {}", local_ref_name);
        eprintln!(
            "[verbose] searching for remote refs matching: {}",
            match remote {
                Some(r) => format!("refs/{}/{}", ns, r),
                None => format!("refs/{}/*/", ns),
            }
        );
    }

    // Find remote refs to materialize
    let remote_refs = find_remote_refs(repo, ns, remote)?;

    if remote_refs.is_empty() {
        println!("no remote metadata refs found");
        return Ok(());
    }

    if verbose {
        eprintln!("[verbose] found {} remote ref(s):", remote_refs.len());
        for (ref_name, oid) in &remote_refs {
            eprintln!("  {} -> {}", ref_name, &oid.to_string()[..8]);
        }
    }

    let email = ctx.session.email();
    let now = ctx.timestamp;

    for (ref_name, remote_oid) in &remote_refs {
        if verbose {
            eprintln!("\n[verbose] === processing {} ===", ref_name);
        }

        let remote_commit_obj = remote_oid.attach(repo).object()?.into_commit();
        let remote_tree_id = remote_commit_obj.tree_id()?.detach();
        let remote_entries = parse_tree(repo, remote_tree_id, "")?;

        if verbose {
            eprintln!(
                "[verbose] remote tree: {} values, {} tombstones, {} set tombstones",
                remote_entries.values.len(),
                remote_entries.tombstones.len(),
                remote_entries.set_tombstones.len()
            );
            for (mk, val) in &remote_entries.values {
                let target = format_target_for_display(
                    &TargetType::from_str(&mk.target_type)?,
                    &mk.target_value,
                );
                let val_desc = match val {
                    TreeValue::String(s) => {
                        if s.len() > 50 {
                            format!("string ({} bytes)", s.len())
                        } else {
                            format!("string = {}", s)
                        }
                    }
                    TreeValue::List(l) => format!("list ({} entries)", l.len()),
                    TreeValue::Set(s) => format!("set ({} members)", s.len()),
                    _ => "unknown type".to_string(),
                };
                eprintln!("  {} {} -> {}", target, mk.key, val_desc);
            }
            for (mk, tomb) in &remote_entries.tombstones {
                let target = format_target_for_display(
                    &TargetType::from_str(&mk.target_type)?,
                    &mk.target_value,
                );
                eprintln!(
                    "  {} {} -> tombstone [ts={}, by={}]",
                    target, mk.key, tomb.timestamp, tomb.email
                );
            }
        }

        // Get local commit (if any)
        let local_commit_oid = repo
            .find_reference(&local_ref_name)
            .ok()
            .and_then(|r| r.into_fully_peeled_id().ok())
            .map(|id| id.detach());

        if verbose {
            match &local_commit_oid {
                Some(c) => eprintln!("[verbose] local commit: {}", &c.to_string()[..8]),
                None => eprintln!("[verbose] no local commit"),
            }
        }

        // Check if we can fast-forward: local is None, or local is an
        // ancestor of remote (no local-only commits to preserve).
        let can_fast_forward = match &local_commit_oid {
            None => {
                if verbose {
                    eprintln!("[verbose] no local commit -> fast-forward");
                }
                true
            }
            Some(local_oid) => {
                if *local_oid == *remote_oid {
                    // Already up to date
                    if dry_run {
                        println!("dry-run: {} already up to date", ref_name);
                    } else {
                        println!("{} already up to date", ref_name);
                    }
                    continue;
                }
                match repo.merge_base(*local_oid, *remote_oid) {
                    Ok(base_oid) => {
                        let is_ff = base_oid == *local_oid;
                        if verbose {
                            eprintln!(
                                "[verbose] merge base: {} (local={}, remote={})",
                                &base_oid.to_string()[..8],
                                &local_oid.to_string()[..8],
                                &remote_oid.to_string()[..8]
                            );
                            if is_ff {
                                eprintln!("[verbose] local is ancestor of remote -> fast-forward");
                            } else {
                                eprintln!("[verbose] diverged histories -> merge required");
                            }
                        }
                        is_ff
                    }
                    Err(_) => {
                        if verbose {
                            eprintln!("[verbose] no merge base found -> merge required (no common ancestor)");
                        }
                        false
                    }
                }
            }
        };

        if can_fast_forward {
            let local_entries = if let Some(local_oid) = &local_commit_oid {
                let lc = local_oid.attach(repo).object()?.into_commit();
                let lt = lc.tree_id()?.detach();
                parse_tree(repo, lt, "")?
            } else {
                ParsedTree::default()
            };

            if verbose {
                eprintln!(
                    "[verbose] local tree: {} values, {} tombstones",
                    local_entries.values.len(),
                    local_entries.tombstones.len()
                );

                // Show what keys are being added/removed/changed
                let mut added = 0usize;
                let mut removed = 0usize;
                let mut changed = 0usize;
                for key in remote_entries.values.keys() {
                    match local_entries.values.get(key) {
                        None => added += 1,
                        Some(local_val)
                            if remote_entries
                                .values
                                .get(key)
                                .is_some_and(|v| local_val != v) =>
                        {
                            changed += 1
                        }
                        _ => {}
                    }
                }
                for key in local_entries.values.keys() {
                    if !remote_entries.values.contains_key(key) {
                        removed += 1;
                    }
                }
                eprintln!(
                    "[verbose] fast-forward delta: {} added, {} changed, {} removed",
                    added, changed, removed
                );
            }

            if dry_run {
                let mut planned_removals = BTreeSet::new();
                let mut planned_changes = collect_db_changes_from_tree(
                    ctx.session.store(),
                    &remote_entries.values,
                    &remote_entries.tombstones,
                    &remote_entries.set_tombstones,
                    &remote_entries.list_tombstones,
                    &mut planned_removals,
                )?;

                // Ensure deletes are represented even for trees produced before tombstones.
                for key in local_entries.values.keys() {
                    if !remote_entries.values.contains_key(key)
                        && !remote_entries.tombstones.contains_key(key)
                    {
                        push_remove_change(&mut planned_changes, &mut planned_removals, key);
                    }
                }

                print_dry_run_report(ref_name, "fast-forward", &planned_changes, &[]);
                continue;
            }

            // Fast-forward: update SQLite from remote tree first.
            ctx.session.store().apply_tree(
                &remote_entries.values,
                &remote_entries.tombstones,
                &remote_entries.set_tombstones,
                &remote_entries.list_tombstones,
                email,
                now,
            )?;

            // Ensure deletes are applied even for trees produced before tombstones.
            for key in local_entries.values.keys() {
                if !remote_entries.values.contains_key(key) {
                    let tt = TargetType::from_str(&key.target_type)?;
                    if verbose {
                        eprintln!(
                            "[verbose] applying implicit delete for {} {}",
                            format_target_for_display(&tt, &key.target_value),
                            key.key
                        );
                    }
                    ctx.session.store().apply_tombstone(
                        &tt,
                        &key.target_value,
                        &key.key,
                        email,
                        now,
                    )?;
                }
            }

            // Fast-forward the ref
            repo.reference(
                local_ref_name.as_str(),
                *remote_oid,
                PreviousValue::Any,
                "fast-forward materialize",
            )?;

            println!("materialized {} (fast-forward)", ref_name);
        } else {
            // Need a real merge
            let local_oid = local_commit_oid
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("expected local commit for merge but found None"))?;
            let local_commit_obj = local_oid.attach(repo).object()?.into_commit();
            let local_tree_id = local_commit_obj.tree_id()?.detach();
            let local_entries = parse_tree(repo, local_tree_id, "")?;

            if verbose {
                eprintln!(
                    "[verbose] local tree: {} values, {} tombstones, {} set tombstones",
                    local_entries.values.len(),
                    local_entries.tombstones.len(),
                    local_entries.set_tombstones.len()
                );
            }

            // Get commit timestamps for conflict resolution
            let local_decoded = local_commit_obj.decode()?;
            let local_timestamp = local_decoded
                .author()
                .map_err(|e| anyhow::anyhow!("{e}"))?
                .time()
                .map_err(|e| anyhow::anyhow!("{e}"))?
                .seconds;
            let remote_decoded = remote_commit_obj.decode()?;
            let remote_timestamp = remote_decoded
                .author()
                .map_err(|e| anyhow::anyhow!("{e}"))?
                .time()
                .map_err(|e| anyhow::anyhow!("{e}"))?
                .seconds;

            if verbose {
                eprintln!(
                    "[verbose] commit timestamps: local={}, remote={}",
                    local_timestamp, remote_timestamp
                );
            }

            let merge_base_oid = repo.merge_base(*local_oid, *remote_oid).ok();
            let mut legacy_base_values: Option<BTreeMap<Key, TreeValue>> = None;

            let (
                merged_values,
                merged_tombstones,
                merged_set_tombstones,
                merged_list_tombstones,
                conflict_decisions,
                merge_strategy,
            ) = if let Some(base_oid) = merge_base_oid {
                let base_commit_obj = base_oid.object()?.into_commit();
                let base_tree_id = base_commit_obj.tree_id()?.detach();
                let base_entries = parse_tree(repo, base_tree_id, "")?;

                if verbose {
                    eprintln!(
                        "[verbose] merge base {} tree: {} values, {} tombstones",
                        &base_oid.to_string()[..8],
                        base_entries.values.len(),
                        base_entries.tombstones.len()
                    );
                    eprintln!("[verbose] performing three-way merge...");
                }

                legacy_base_values = Some(base_entries.values.clone());

                let (merged_values, conflict_decisions) = three_way_merge(
                    &base_entries.values,
                    &local_entries.values,
                    &remote_entries.values,
                    local_timestamp,
                    remote_timestamp,
                )?;

                if verbose {
                    let all_keys: BTreeSet<&Key> = base_entries
                        .values
                        .keys()
                        .chain(local_entries.values.keys())
                        .chain(remote_entries.values.keys())
                        .collect();
                    let mut unchanged = 0usize;
                    let mut local_only_changed = 0usize;
                    let mut remote_only_changed = 0usize;
                    let mut new_local = 0usize;
                    let mut new_remote = 0usize;
                    let mut new_both = 0usize;
                    let mut removed = 0usize;
                    let mut conflicted = 0usize;
                    for key in &all_keys {
                        let in_base = base_entries.values.get(*key);
                        let in_local = local_entries.values.get(*key);
                        let in_remote = remote_entries.values.get(*key);
                        match (in_base, in_local, in_remote) {
                            (Some(b), Some(l), Some(r)) => match (l != b, r != b) {
                                (false, false) => unchanged += 1,
                                (true, false) => local_only_changed += 1,
                                (false, true) => remote_only_changed += 1,
                                (true, true) => conflicted += 1,
                            },
                            (Some(_), None, None) => removed += 1,
                            (Some(_), Some(_), None) | (Some(_), None, Some(_)) => {
                                conflicted += 1;
                            }
                            (None, Some(_), None) => new_local += 1,
                            (None, None, Some(_)) => new_remote += 1,
                            (None, Some(_), Some(_)) => new_both += 1,
                            _ => {}
                        }
                    }
                    eprintln!("[verbose] merge breakdown:");
                    eprintln!("  unchanged:     {}", unchanged);
                    eprintln!("  local changed: {}", local_only_changed);
                    eprintln!("  remote changed:{}", remote_only_changed);
                    eprintln!("  new (local):   {}", new_local);
                    eprintln!("  new (remote):  {}", new_remote);
                    eprintln!("  new (both):    {}", new_both);
                    eprintln!("  removed:       {}", removed);
                    eprintln!("  conflicts:     {}", conflicted);
                }

                let merged_tombstones = merge_tombstones(
                    &base_entries.tombstones,
                    &local_entries.tombstones,
                    &remote_entries.tombstones,
                    &merged_values,
                );
                let merged_set_tombstones = merge_set_member_tombstones(
                    &local_entries.set_tombstones,
                    &remote_entries.set_tombstones,
                    &merged_values,
                );
                let merged_list_tombstones = merge_list_tombstones(
                    &local_entries.list_tombstones,
                    &remote_entries.list_tombstones,
                    &merged_values,
                );

                if verbose {
                    eprintln!(
                        "[verbose] merged result: {} values, {} tombstones, {} set tombstones, {} list tombstones, {} conflicts",
                        merged_values.len(),
                        merged_tombstones.len(),
                        merged_set_tombstones.len(),
                        merged_list_tombstones.len(),
                        conflict_decisions.len()
                    );
                }

                (
                    merged_values,
                    merged_tombstones,
                    merged_set_tombstones,
                    merged_list_tombstones,
                    conflict_decisions,
                    "three-way",
                )
            } else {
                if verbose {
                    eprintln!(
                        "[verbose] no common ancestor, performing two-way merge (local wins)..."
                    );
                }

                let (merged_values, merged_tombstones, conflict_decisions) =
                    two_way_merge_no_common_ancestor(
                        &local_entries.values,
                        &local_entries.tombstones,
                        &remote_entries.values,
                        &remote_entries.tombstones,
                    );
                let merged_set_tombstones = merge_set_member_tombstones(
                    &local_entries.set_tombstones,
                    &remote_entries.set_tombstones,
                    &merged_values,
                );
                let merged_list_tombstones = merge_list_tombstones(
                    &local_entries.list_tombstones,
                    &remote_entries.list_tombstones,
                    &merged_values,
                );

                if verbose {
                    eprintln!(
                        "[verbose] merged result: {} values, {} tombstones, {} set tombstones, {} list tombstones, {} conflicts",
                        merged_values.len(),
                        merged_tombstones.len(),
                        merged_set_tombstones.len(),
                        merged_list_tombstones.len(),
                        conflict_decisions.len()
                    );
                }

                (
                    merged_values,
                    merged_tombstones,
                    merged_set_tombstones,
                    merged_list_tombstones,
                    conflict_decisions,
                    "two-way-no-common-ancestor",
                )
            };

            if verbose && !conflict_decisions.is_empty() {
                eprintln!("[verbose] conflict resolutions:");
                for cd in &conflict_decisions {
                    eprintln!(
                        "  {} reason={} resolution={}",
                        format_key_for_display(&cd.key),
                        cd.reason.as_str(),
                        cd.resolution.as_str()
                    );
                }
            }

            if dry_run {
                let mut planned_removals = BTreeSet::new();
                let mut planned_changes = collect_db_changes_from_tree(
                    ctx.session.store(),
                    &merged_values,
                    &merged_tombstones,
                    &merged_set_tombstones,
                    &merged_list_tombstones,
                    &mut planned_removals,
                )?;

                // Handle removals where no explicit tombstone exists (legacy trees)
                if let Some(base_values) = &legacy_base_values {
                    for key in base_values.keys() {
                        if !merged_values.contains_key(key) && !merged_tombstones.contains_key(key)
                        {
                            push_remove_change(&mut planned_changes, &mut planned_removals, key);
                        }
                    }
                }

                if merge_strategy == "two-way-no-common-ancestor" {
                    println!(
                        "dry-run: no common ancestor between local metadata ref and {}",
                        ref_name
                    );
                }
                print_dry_run_report(
                    ref_name,
                    merge_strategy,
                    &planned_changes,
                    &conflict_decisions,
                );
                continue;
            }

            if merge_strategy == "two-way-no-common-ancestor" {
                println!(
                    "no common ancestor between local metadata ref and {}; using two-way merge (local wins key conflicts)",
                    ref_name
                );
            }

            // Update SQLite
            if verbose {
                eprintln!("[verbose] updating SQLite database...");
            }
            ctx.session.store().apply_tree(
                &merged_values,
                &merged_tombstones,
                &merged_set_tombstones,
                &merged_list_tombstones,
                email,
                now,
            )?;

            // Handle removals where no explicit tombstone exists (legacy trees)
            if let Some(base_values) = &legacy_base_values {
                for key in base_values.keys() {
                    if !merged_values.contains_key(key) && !merged_tombstones.contains_key(key) {
                        let tt = TargetType::from_str(&key.target_type)?;
                        if verbose {
                            eprintln!(
                                "[verbose] applying legacy delete for {} {}",
                                format_target_for_display(&tt, &key.target_value),
                                key.key
                            );
                        }
                        ctx.session.store().apply_tombstone(
                            &tt,
                            &key.target_value,
                            &key.key,
                            email,
                            now,
                        )?;
                    }
                }
            }

            // Build the merged tree and write a merge commit
            if verbose {
                eprintln!("[verbose] building merged tree...");
            }
            let merged_tree_oid = build_merged_tree(
                repo,
                &merged_values,
                &merged_tombstones,
                &merged_set_tombstones,
                &merged_list_tombstones,
            )?;

            if verbose {
                let merged_tree = merged_tree_oid.attach(repo).object()?.into_tree();
                eprintln!(
                    "[verbose] merged tree: {} ({} top-level entries)",
                    merged_tree_oid,
                    merged_tree.iter().count()
                );
            }

            let name = ctx.session.name();
            let sig = gix::actor::Signature {
                name: name.into(),
                email: email.into(),
                time: gix::date::Time::now_local_or_utc(),
            };

            let commit = gix::objs::Commit {
                message: "materialize".into(),
                tree: merged_tree_oid,
                author: sig.clone(),
                committer: sig,
                encoding: None,
                parents: vec![*local_oid, *remote_oid].into(),
                extra_headers: Default::default(),
            };

            let merge_commit_oid = repo.write_object(&commit)?.detach();
            repo.reference(
                local_ref_name.as_str(),
                merge_commit_oid,
                PreviousValue::Any,
                "materialize merge",
            )?;

            if verbose {
                eprintln!(
                    "[verbose] wrote merge commit {} (parents: {}, {})",
                    &merge_commit_oid.to_string()[..8],
                    &local_oid.to_string()[..8],
                    &remote_oid.to_string()[..8]
                );
            }

            println!("materialized {}", ref_name);
        }
    }

    if !dry_run {
        ctx.session.store().set_last_materialized(now)?;
    }

    Ok(())
}

fn collect_db_changes_from_tree(
    db: &Store,
    values: &BTreeMap<Key, TreeValue>,
    tombstones: &BTreeMap<Key, Tombstone>,
    set_tombstones: &BTreeMap<(Key, String), String>,
    list_tombstones: &BTreeMap<(Key, String), Tombstone>,
    planned_removals: &mut BTreeSet<Key>,
) -> Result<Vec<PlannedDbChange>> {
    let mut planned = Vec::new();

    for (mk, tree_val) in values {
        let tt = TargetType::from_str(&mk.target_type)?;
        match tree_val {
            TreeValue::String(s) => {
                let json_val = serde_json::to_string(s)?;
                let existing = db.get(&tt, &mk.target_value, &mk.key)?;
                if existing.as_ref().map(|e| e.value.as_str()) != Some(&json_val) {
                    planned.push(PlannedDbChange::Set {
                        target_type: mk.target_type.clone(),
                        target_value: mk.target_value.clone(),
                        key: mk.key.clone(),
                        value_type: "string".to_string(),
                        value_preview: s.clone(),
                    });
                }
            }
            TreeValue::List(list_entries) => {
                let tombstoned_names: BTreeSet<String> = list_tombstones
                    .iter()
                    .filter_map(|((tk, entry_name), _)| {
                        if *tk == *mk {
                            Some(entry_name.clone())
                        } else {
                            None
                        }
                    })
                    .collect();
                let mut items: Vec<ListEntry> = Vec::with_capacity(list_entries.len());
                for (entry_name, content) in list_entries {
                    if tombstoned_names.contains(entry_name) {
                        continue;
                    }
                    let timestamp =
                        parse_timestamp_from_entry_name(entry_name).unwrap_or(items.len() as i64);
                    items.push(ListEntry {
                        value: content.clone(),
                        timestamp,
                    });
                }
                let json_val = encode_entries(&items)?;
                let existing = db.get(&tt, &mk.target_value, &mk.key)?;
                if existing.as_ref().map(|e| e.value.as_str()) != Some(&json_val) {
                    planned.push(PlannedDbChange::Set {
                        target_type: mk.target_type.clone(),
                        target_value: mk.target_value.clone(),
                        key: mk.key.clone(),
                        value_type: "list".to_string(),
                        value_preview: format!("{} entries", items.len()),
                    });
                }
            }
            TreeValue::Set(set_members) => {
                let tombstoned: BTreeSet<String> = set_tombstones
                    .iter()
                    .filter_map(|((tk, member_id), _)| {
                        if *tk == *mk {
                            Some(member_id.clone())
                        } else {
                            None
                        }
                    })
                    .collect();
                let mut visible: Vec<String> = set_members
                    .iter()
                    .filter_map(|(member_id, value)| {
                        if tombstoned.contains(member_id) {
                            None
                        } else {
                            Some(value.clone())
                        }
                    })
                    .collect();
                visible.sort();
                let json_val = serde_json::to_string(&visible)?;
                let existing = db.get(&tt, &mk.target_value, &mk.key)?;
                if existing.as_ref().map(|e| e.value.as_str()) != Some(&json_val) {
                    planned.push(PlannedDbChange::Set {
                        target_type: mk.target_type.clone(),
                        target_value: mk.target_value.clone(),
                        key: mk.key.clone(),
                        value_type: "set".to_string(),
                        value_preview: format!("{} members", visible.len()),
                    });
                }
            }
            _ => {}
        }
    }

    for key in tombstones.keys() {
        if values.contains_key(key) {
            continue;
        }
        push_remove_change(&mut planned, planned_removals, key);
    }

    Ok(planned)
}

fn push_remove_change(
    planned: &mut Vec<PlannedDbChange>,
    planned_removals: &mut BTreeSet<Key>,
    key: &Key,
) {
    if planned_removals.insert(key.clone()) {
        planned.push(PlannedDbChange::Remove {
            target_type: key.target_type.clone(),
            target_value: key.target_value.clone(),
            key: key.key.clone(),
        });
    }
}

fn print_dry_run_report(
    ref_name: &str,
    strategy: &str,
    planned_changes: &[PlannedDbChange],
    conflicts: &[ConflictDecision],
) {
    println!("dry-run: {}", ref_name);
    println!("dry-run: strategy={}", strategy);

    if conflicts.is_empty() {
        println!("dry-run: no conflict resolutions");
    } else {
        println!("dry-run: conflict resolutions:");
        for conflict in conflicts {
            println!(
                "  conflict {} reason={} resolution={}",
                format_key_for_display(&conflict.key),
                conflict.reason.as_str(),
                conflict.resolution.as_str()
            );
        }
    }

    if planned_changes.is_empty() {
        println!("dry-run: no sqlite changes");
    } else {
        println!("dry-run: planned sqlite changes:");
        for change in planned_changes {
            match change {
                PlannedDbChange::Set {
                    target_type,
                    target_value,
                    key,
                    value_type,
                    value_preview,
                } => {
                    let target_display = if target_type == "project" {
                        "project".to_string()
                    } else {
                        format!("{}:{}", target_type, target_value)
                    };
                    println!(
                        "  set {} {} ({}) = {}",
                        target_display, key, value_type, value_preview
                    );
                }
                PlannedDbChange::Remove {
                    target_type,
                    target_value,
                    key,
                } => {
                    let target_display = if target_type == "project" {
                        "project".to_string()
                    } else {
                        format!("{}:{}", target_type, target_value)
                    };
                    println!("  rm {} {}", target_display, key);
                }
            }
        }
    }
}

fn format_target_for_display(target_type: &TargetType, target_value: &str) -> String {
    if *target_type == TargetType::Project {
        "project".to_string()
    } else {
        format!("{}:{}", target_type.as_str(), target_value)
    }
}

fn format_key_for_display(key: &Key) -> String {
    let target_display = if key.target_type == "project" {
        "project".to_string()
    } else {
        format!("{}:{}", key.target_type, key.target_value)
    };
    format!("{} {}", target_display, key.key)
}

fn find_remote_refs(
    repo: &gix::Repository,
    ns: &str,
    remote: Option<&str>,
) -> Result<Vec<(String, gix::ObjectId)>> {
    let mut results = Vec::new();

    let prefix = match remote {
        Some(r) => format!("refs/{}/{}", ns, r),
        None => format!("refs/{}/", ns),
    };
    let local_prefix = format!("refs/{}/local/", ns);

    let platform = repo.references()?;
    for reference in platform.all()? {
        let reference = reference.map_err(|e| anyhow::anyhow!("{e}"))?;
        let name = reference.name().as_bstr().to_string();
        if name.starts_with(&prefix) && !name.starts_with(&local_prefix) {
            if let Ok(id) = reference.into_fully_peeled_id() {
                results.push((name, id.detach()));
            }
        }
    }

    Ok(results)
}
