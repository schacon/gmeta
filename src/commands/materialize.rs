use anyhow::Result;
use chrono::Utc;
use std::collections::{BTreeMap, BTreeSet};

use crate::db::Db;
use crate::git_utils;
use crate::list_value::{encode_entries, parse_timestamp_from_entry_name, ListEntry};
use crate::types::{
    build_list_tree_dir_path, build_tombstone_tree_path, build_tree_path, decode_key_path_segments,
    Target, KEY_TREE_ROOT, LIST_VALUE_DIR, STRING_VALUE_BLOB, TOMBSTONE_BLOB, TOMBSTONE_ROOT,
};

type Key = (String, String, String); // (target_type, target_value, key)

/// A parsed metadata entry from a Git tree.
#[derive(Debug, Clone, PartialEq, Eq)]
enum TreeValue {
    String(String),
    List(Vec<(String, String)>), // (entry_name, content)
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct TombstoneEntry {
    timestamp: i64,
    email: String,
}

#[derive(Debug, Default, Clone)]
struct ParsedTree {
    values: BTreeMap<Key, TreeValue>,
    tombstones: BTreeMap<Key, TombstoneEntry>,
}

#[derive(serde::Deserialize, serde::Serialize)]
struct TombstoneBlob {
    timestamp: i64,
    email: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum ConflictReason {
    BothChanged,
    ConcurrentAdd,
    LocalModifiedRemoteRemoved,
    RemoteModifiedLocalRemoved,
    NoCommonAncestorRemoteWins,
}

impl ConflictReason {
    fn as_str(&self) -> &'static str {
        match self {
            ConflictReason::BothChanged => "both-changed",
            ConflictReason::ConcurrentAdd => "concurrent-add",
            ConflictReason::LocalModifiedRemoteRemoved => "local-modified-remote-removed",
            ConflictReason::RemoteModifiedLocalRemoved => "remote-modified-local-removed",
            ConflictReason::NoCommonAncestorRemoteWins => "no-common-ancestor-remote-wins",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum ConflictResolution {
    Local,
    Remote,
    Union,
}

impl ConflictResolution {
    fn as_str(&self) -> &'static str {
        match self {
            ConflictResolution::Local => "local",
            ConflictResolution::Remote => "remote",
            ConflictResolution::Union => "union",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ConflictDecision {
    key: Key,
    reason: ConflictReason,
    resolution: ConflictResolution,
}

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

#[derive(Debug, Clone, PartialEq, Eq)]
enum MergeState {
    Absent,
    Value(TreeValue),
    Tombstone(TombstoneEntry),
}

pub fn run(remote: Option<&str>, dry_run: bool) -> Result<()> {
    let repo = git_utils::discover_repo()?;
    let db_path = git_utils::db_path(&repo)?;
    let db = Db::open(&db_path)?;

    let ns = git_utils::get_namespace(&repo)?;
    let local_ref_name = git_utils::local_ref(&repo)?;

    // Find remote refs to materialize
    let remote_refs = find_remote_refs(&repo, &ns, remote)?;

    if remote_refs.is_empty() {
        println!("no remote metadata refs found");
        return Ok(());
    }

    let email = git_utils::get_email(&repo)?;
    let now = Utc::now().timestamp_millis();

    for (ref_name, remote_oid) in &remote_refs {
        let remote_commit = repo.find_commit(*remote_oid)?;
        let remote_tree = remote_commit.tree()?;
        let remote_entries = parse_tree(&repo, &remote_tree, "")?;

        // Get local commit (if any)
        let local_commit = repo
            .find_reference(&local_ref_name)
            .ok()
            .and_then(|r| r.peel_to_commit().ok());

        // Check if we can fast-forward: local is None, or local is an
        // ancestor of remote (no local-only commits to preserve).
        let can_fast_forward = match &local_commit {
            None => true,
            Some(local_c) => {
                if local_c.id() == *remote_oid {
                    // Already up to date
                    if dry_run {
                        println!("dry-run: {} already up to date", ref_name);
                    } else {
                        println!("{} already up to date", ref_name);
                    }
                    continue;
                }
                match repo.merge_base(local_c.id(), *remote_oid) {
                    Ok(base_oid) => base_oid == local_c.id(),
                    Err(_) => false,
                }
            }
        };

        if can_fast_forward {
            let local_entries = if let Some(local_c) = &local_commit {
                parse_tree(&repo, &local_c.tree()?, "")?
            } else {
                ParsedTree::default()
            };

            if dry_run {
                let mut planned_removals = BTreeSet::new();
                let mut planned_changes = collect_db_changes_from_tree(
                    &db,
                    &remote_entries.values,
                    &remote_entries.tombstones,
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
            update_db_from_tree(
                &repo,
                &db,
                &remote_entries.values,
                &remote_entries.tombstones,
                &email,
                now,
            )?;

            // Ensure deletes are applied even for trees produced before tombstones.
            for key in local_entries.values.keys() {
                if !remote_entries.values.contains_key(key) {
                    let (target_type, target_value, key_name) = key;
                    db.apply_tombstone(target_type, target_value, key_name, &email, now)?;
                }
            }

            // Fast-forward the ref
            repo.reference(
                &local_ref_name,
                *remote_oid,
                true,
                "fast-forward materialize",
            )?;

            println!("materialized {} (fast-forward)", ref_name);
        } else {
            // Need a real merge
            let local_c = local_commit.as_ref().unwrap();
            let local_entries = parse_tree(&repo, &local_c.tree()?, "")?;

            // Get commit timestamps for conflict resolution
            let local_timestamp = local_c.time().seconds();
            let remote_timestamp = remote_commit.time().seconds();

            let merge_base_oid = repo.merge_base(local_c.id(), *remote_oid).ok();
            let mut legacy_base_values: Option<BTreeMap<Key, TreeValue>> = None;

            let (merged_values, merged_tombstones, conflict_decisions, merge_strategy) =
                if let Some(base_oid) = merge_base_oid {
                    let base_commit = repo.find_commit(base_oid)?;
                    let base_entries = parse_tree(&repo, &base_commit.tree()?, "")?;
                    legacy_base_values = Some(base_entries.values.clone());

                    let (merged_values, conflict_decisions) = three_way_merge(
                        &base_entries.values,
                        &local_entries.values,
                        &remote_entries.values,
                        local_timestamp,
                        remote_timestamp,
                    )?;
                    let merged_tombstones = merge_tombstones(
                        &base_entries.tombstones,
                        &local_entries.tombstones,
                        &remote_entries.tombstones,
                        &merged_values,
                    );
                    (
                        merged_values,
                        merged_tombstones,
                        conflict_decisions,
                        "three-way",
                    )
                } else {
                    let (merged_values, merged_tombstones, conflict_decisions) =
                        two_way_merge_no_common_ancestor(
                            &local_entries.values,
                            &local_entries.tombstones,
                            &remote_entries.values,
                            &remote_entries.tombstones,
                        );
                    (
                        merged_values,
                        merged_tombstones,
                        conflict_decisions,
                        "two-way-no-common-ancestor",
                    )
                };

            if dry_run {
                let mut planned_removals = BTreeSet::new();
                let mut planned_changes = collect_db_changes_from_tree(
                    &db,
                    &merged_values,
                    &merged_tombstones,
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
                    "no common ancestor between local metadata ref and {}; using two-way merge (remote wins key conflicts)",
                    ref_name
                );
            }

            // Update SQLite
            update_db_from_tree(&repo, &db, &merged_values, &merged_tombstones, &email, now)?;

            // Handle removals where no explicit tombstone exists (legacy trees)
            if let Some(base_values) = &legacy_base_values {
                for key in base_values.keys() {
                    if !merged_values.contains_key(key) && !merged_tombstones.contains_key(key) {
                        let (target_type, target_value, key_name) = key;
                        db.apply_tombstone(target_type, target_value, key_name, &email, now)?;
                    }
                }
            }

            // Build the merged tree and write a merge commit
            let merged_tree_oid = build_merged_tree(&repo, &merged_values, &merged_tombstones)?;
            let merged_tree = repo.find_tree(merged_tree_oid)?;
            let name = git_utils::get_name(&repo)?;
            let sig = git2::Signature::now(&name, &email)?;

            repo.commit(
                Some(&local_ref_name),
                &sig,
                &sig,
                "materialize",
                &merged_tree,
                &[local_c, &remote_commit],
            )?;

            println!("materialized {}", ref_name);
        }
    }

    if !dry_run {
        db.set_last_materialized(now)?;
    }

    Ok(())
}

/// Update the SQLite database from parsed tree data.
fn update_db_from_tree(
    repo: &git2::Repository,
    db: &Db,
    values: &BTreeMap<Key, TreeValue>,
    tombstones: &BTreeMap<Key, TombstoneEntry>,
    email: &str,
    now: i64,
) -> Result<()> {
    use crate::types::GIT_REF_THRESHOLD;

    for ((target_type, target_value, key_name), tree_val) in values {
        match tree_val {
            TreeValue::String(s) => {
                if s.len() > GIT_REF_THRESHOLD {
                    // Large value: store as git blob reference
                    let blob_oid = repo.blob(s.as_bytes())?;
                    let oid_str = blob_oid.to_string();
                    let existing = db.get(target_type, target_value, key_name)?;
                    if existing.as_ref().map(|(v, _, _)| v.as_str()) != Some(&oid_str) {
                        db.set_with_git_ref(
                            None,
                            target_type,
                            target_value,
                            key_name,
                            &oid_str,
                            "string",
                            email,
                            now,
                            true,
                        )?;
                    }
                } else {
                    let json_val = serde_json::to_string(s)?;
                    let existing = db.get(target_type, target_value, key_name)?;
                    if existing.as_ref().map(|(v, _, _)| v.as_str()) != Some(&json_val) {
                        db.set(
                            target_type,
                            target_value,
                            key_name,
                            &json_val,
                            "string",
                            email,
                            now,
                        )?;
                    }
                }
            }
            TreeValue::List(list_entries) => {
                let mut items: Vec<ListEntry> = Vec::with_capacity(list_entries.len());
                for (entry_name, content) in list_entries {
                    let timestamp = parse_timestamp_from_entry_name(entry_name)
                        .unwrap_or_else(|| items.len() as i64);
                    items.push(ListEntry {
                        value: content.clone(),
                        timestamp,
                    });
                }
                let json_val = encode_entries(&items)?;
                let existing = db.get(target_type, target_value, key_name)?;
                if existing.as_ref().map(|(v, _, _)| v.as_str()) != Some(&json_val) {
                    db.set(
                        target_type,
                        target_value,
                        key_name,
                        &json_val,
                        "list",
                        email,
                        now,
                    )?;
                }
            }
        }
    }

    for ((target_type, target_value, key_name), tombstone) in tombstones {
        if values.contains_key(&(target_type.clone(), target_value.clone(), key_name.clone())) {
            continue;
        }
        db.apply_tombstone(
            target_type,
            target_value,
            key_name,
            &tombstone.email,
            tombstone.timestamp,
        )?;
    }

    Ok(())
}

fn collect_db_changes_from_tree(
    db: &Db,
    values: &BTreeMap<Key, TreeValue>,
    tombstones: &BTreeMap<Key, TombstoneEntry>,
    planned_removals: &mut BTreeSet<Key>,
) -> Result<Vec<PlannedDbChange>> {
    let mut planned = Vec::new();

    for ((target_type, target_value, key_name), tree_val) in values {
        match tree_val {
            TreeValue::String(s) => {
                let json_val = serde_json::to_string(s)?;
                let existing = db.get(target_type, target_value, key_name)?;
                if existing.as_ref().map(|(v, _, _)| v.as_str()) != Some(&json_val) {
                    planned.push(PlannedDbChange::Set {
                        target_type: target_type.clone(),
                        target_value: target_value.clone(),
                        key: key_name.clone(),
                        value_type: "string".to_string(),
                        value_preview: s.clone(),
                    });
                }
            }
            TreeValue::List(list_entries) => {
                let mut items: Vec<ListEntry> = Vec::with_capacity(list_entries.len());
                for (entry_name, content) in list_entries {
                    let timestamp = parse_timestamp_from_entry_name(entry_name)
                        .unwrap_or_else(|| items.len() as i64);
                    items.push(ListEntry {
                        value: content.clone(),
                        timestamp,
                    });
                }
                let json_val = encode_entries(&items)?;
                let existing = db.get(target_type, target_value, key_name)?;
                if existing.as_ref().map(|(v, _, _)| v.as_str()) != Some(&json_val) {
                    planned.push(PlannedDbChange::Set {
                        target_type: target_type.clone(),
                        target_value: target_value.clone(),
                        key: key_name.clone(),
                        value_type: "list".to_string(),
                        value_preview: format!("{} entries", list_entries.len()),
                    });
                }
            }
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
            target_type: key.0.clone(),
            target_value: key.1.clone(),
            key: key.2.clone(),
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
                    println!(
                        "  set {} {} ({}) = {}",
                        format_target_for_display(target_type, target_value),
                        key,
                        value_type,
                        value_preview
                    );
                }
                PlannedDbChange::Remove {
                    target_type,
                    target_value,
                    key,
                } => {
                    println!(
                        "  rm {} {}",
                        format_target_for_display(target_type, target_value),
                        key
                    );
                }
            }
        }
    }
}

fn format_target_for_display(target_type: &str, target_value: &str) -> String {
    if target_type == "project" {
        "project".to_string()
    } else {
        format!("{}:{}", target_type, target_value)
    }
}

fn format_key_for_display(key: &Key) -> String {
    format!("{} {}", format_target_for_display(&key.0, &key.1), key.2)
}

/// Three-way merge: base vs local vs remote.
///
/// For each key:
/// - In base, local, remote (unchanged on both sides): keep as-is
/// - In base, changed only on local: take local
/// - In base, changed only on remote: take remote
/// - In base, changed on both sides (true conflict):
///   - Lists: union of entries
///   - Strings: later commit timestamp wins
/// - Not in base, only in local: take local (new local key)
/// - Not in base, only in remote: take remote (new remote key)
/// - Not in base, in both: same conflict rules as above
/// - In base, removed on one side:
///   - If the other side modified it: keep the modified value
///   - If the other side didn't change it: remove it
fn three_way_merge(
    base: &BTreeMap<Key, TreeValue>,
    local: &BTreeMap<Key, TreeValue>,
    remote: &BTreeMap<Key, TreeValue>,
    local_timestamp: i64,
    remote_timestamp: i64,
) -> Result<(BTreeMap<Key, TreeValue>, Vec<ConflictDecision>)> {
    let mut merged = BTreeMap::new();
    let mut conflicts = Vec::new();

    // Collect all keys across all three
    let mut all_keys: BTreeMap<&Key, ()> = BTreeMap::new();
    for k in base.keys().chain(local.keys()).chain(remote.keys()) {
        all_keys.insert(k, ());
    }

    for key in all_keys.keys() {
        let in_base = base.get(*key);
        let in_local = local.get(*key);
        let in_remote = remote.get(*key);

        match (in_base, in_local, in_remote) {
            // In all three — check for changes
            (Some(b), Some(l), Some(r)) => {
                let local_changed = l != b;
                let remote_changed = r != b;

                match (local_changed, remote_changed) {
                    (false, false) => {
                        // No changes, keep base
                        merged.insert((*key).clone(), b.clone());
                    }
                    (true, false) => {
                        // Only local changed
                        merged.insert((*key).clone(), l.clone());
                    }
                    (false, true) => {
                        // Only remote changed
                        merged.insert((*key).clone(), r.clone());
                    }
                    (true, true) => {
                        // Both changed — conflict resolution
                        let (resolved, resolution) =
                            resolve_conflict(l, r, local_timestamp, remote_timestamp);
                        merged.insert((*key).clone(), resolved);
                        conflicts.push(ConflictDecision {
                            key: (*key).clone(),
                            reason: ConflictReason::BothChanged,
                            resolution,
                        });
                    }
                }
            }

            // In base and local, but removed on remote
            (Some(b), Some(l), None) => {
                if l != b {
                    // Local modified it — modified wins over removal
                    merged.insert((*key).clone(), l.clone());
                    conflicts.push(ConflictDecision {
                        key: (*key).clone(),
                        reason: ConflictReason::LocalModifiedRemoteRemoved,
                        resolution: ConflictResolution::Local,
                    });
                }
                // else: local didn't change, remote removed — stay removed
            }

            // In base and remote, but removed on local
            (Some(b), None, Some(r)) => {
                if r != b {
                    // Remote modified it — modified wins over removal
                    merged.insert((*key).clone(), r.clone());
                    conflicts.push(ConflictDecision {
                        key: (*key).clone(),
                        reason: ConflictReason::RemoteModifiedLocalRemoved,
                        resolution: ConflictResolution::Remote,
                    });
                }
                // else: remote didn't change, local removed — stay removed
            }

            // In base only (both sides removed)
            (Some(_), None, None) => {
                // Both removed, gone
            }

            // Not in base, only in local
            (None, Some(l), None) => {
                merged.insert((*key).clone(), l.clone());
            }

            // Not in base, only in remote
            (None, None, Some(r)) => {
                merged.insert((*key).clone(), r.clone());
            }

            // Not in base, in both local and remote (concurrent add)
            (None, Some(l), Some(r)) => {
                let (resolved, resolution) =
                    resolve_conflict(l, r, local_timestamp, remote_timestamp);
                merged.insert((*key).clone(), resolved);
                conflicts.push(ConflictDecision {
                    key: (*key).clone(),
                    reason: ConflictReason::ConcurrentAdd,
                    resolution,
                });
            }

            // Not anywhere (shouldn't happen)
            (None, None, None) => {}
        }
    }

    Ok((merged, conflicts))
}

/// Two-way merge used when local and remote metadata refs have no common ancestor.
/// Remote state wins for any overlapping key conflict.
fn two_way_merge_no_common_ancestor(
    local_values: &BTreeMap<Key, TreeValue>,
    local_tombstones: &BTreeMap<Key, TombstoneEntry>,
    remote_values: &BTreeMap<Key, TreeValue>,
    remote_tombstones: &BTreeMap<Key, TombstoneEntry>,
) -> (
    BTreeMap<Key, TreeValue>,
    BTreeMap<Key, TombstoneEntry>,
    Vec<ConflictDecision>,
) {
    let mut merged_values = BTreeMap::new();
    let mut merged_tombstones = BTreeMap::new();
    let mut conflicts = Vec::new();

    let mut all_keys: BTreeSet<Key> = BTreeSet::new();
    for key in local_values
        .keys()
        .chain(local_tombstones.keys())
        .chain(remote_values.keys())
        .chain(remote_tombstones.keys())
    {
        all_keys.insert(key.clone());
    }

    for key in all_keys {
        let local_state = if let Some(v) = local_values.get(&key) {
            MergeState::Value(v.clone())
        } else if let Some(t) = local_tombstones.get(&key) {
            MergeState::Tombstone(t.clone())
        } else {
            MergeState::Absent
        };

        let remote_state = if let Some(v) = remote_values.get(&key) {
            MergeState::Value(v.clone())
        } else if let Some(t) = remote_tombstones.get(&key) {
            MergeState::Tombstone(t.clone())
        } else {
            MergeState::Absent
        };

        if local_state != MergeState::Absent
            && remote_state != MergeState::Absent
            && local_state != remote_state
        {
            conflicts.push(ConflictDecision {
                key: key.clone(),
                reason: ConflictReason::NoCommonAncestorRemoteWins,
                resolution: ConflictResolution::Remote,
            });
        }

        let selected = if remote_state != MergeState::Absent {
            remote_state
        } else {
            local_state
        };

        match selected {
            MergeState::Absent => {}
            MergeState::Value(v) => {
                merged_values.insert(key, v);
            }
            MergeState::Tombstone(t) => {
                merged_tombstones.insert(key, t);
            }
        }
    }

    (merged_values, merged_tombstones, conflicts)
}

fn merge_tombstones(
    base: &BTreeMap<Key, TombstoneEntry>,
    local: &BTreeMap<Key, TombstoneEntry>,
    remote: &BTreeMap<Key, TombstoneEntry>,
    merged_values: &BTreeMap<Key, TreeValue>,
) -> BTreeMap<Key, TombstoneEntry> {
    let mut merged = BTreeMap::new();

    let mut all_keys: BTreeMap<&Key, ()> = BTreeMap::new();
    for k in base.keys().chain(local.keys()).chain(remote.keys()) {
        all_keys.insert(k, ());
    }

    for key in all_keys.keys() {
        let in_base = base.get(*key);
        let in_local = local.get(*key);
        let in_remote = remote.get(*key);

        let selected = match (in_base, in_local, in_remote) {
            (Some(b), Some(l), Some(r)) => {
                let local_changed = l != b;
                let remote_changed = r != b;
                match (local_changed, remote_changed) {
                    (false, false) => Some(b.clone()),
                    (true, false) => Some(l.clone()),
                    (false, true) => Some(r.clone()),
                    (true, true) => Some(select_newer_tombstone(l, r)),
                }
            }
            (Some(b), Some(l), None) => {
                if l != b {
                    Some(l.clone())
                } else {
                    None
                }
            }
            (Some(b), None, Some(r)) => {
                if r != b {
                    Some(r.clone())
                } else {
                    None
                }
            }
            (Some(_), None, None) => None,
            (None, Some(l), None) => Some(l.clone()),
            (None, None, Some(r)) => Some(r.clone()),
            (None, Some(l), Some(r)) => Some(select_newer_tombstone(l, r)),
            (None, None, None) => None,
        };

        if let Some(tombstone) = selected {
            if !merged_values.contains_key(*key) {
                merged.insert((*key).clone(), tombstone);
            }
        }
    }

    merged
}

fn select_newer_tombstone(local: &TombstoneEntry, remote: &TombstoneEntry) -> TombstoneEntry {
    if remote.timestamp > local.timestamp {
        remote.clone()
    } else {
        local.clone()
    }
}

/// Resolve a conflict where both sides changed the same key.
/// For strings, the later commit timestamp wins.
fn resolve_conflict(
    local: &TreeValue,
    remote: &TreeValue,
    local_timestamp: i64,
    remote_timestamp: i64,
) -> (TreeValue, ConflictResolution) {
    match (local, remote) {
        // Both lists: union of entries
        (TreeValue::List(local_list), TreeValue::List(remote_list)) => {
            let mut combined: BTreeMap<String, String> = BTreeMap::new();
            for (name, content) in local_list {
                combined.insert(name.clone(), content.clone());
            }
            for (name, content) in remote_list {
                combined
                    .entry(name.clone())
                    .or_insert_with(|| content.clone());
            }
            (
                TreeValue::List(combined.into_iter().collect()),
                ConflictResolution::Union,
            )
        }
        // Both strings: later commit timestamp wins (tie goes to local)
        (TreeValue::String(_), TreeValue::String(_)) => {
            if remote_timestamp > local_timestamp {
                (remote.clone(), ConflictResolution::Remote)
            } else {
                (local.clone(), ConflictResolution::Local)
            }
        }
        // Mismatched types: later timestamp wins
        _ => {
            if remote_timestamp > local_timestamp {
                (remote.clone(), ConflictResolution::Remote)
            } else {
                (local.clone(), ConflictResolution::Local)
            }
        }
    }
}

/// Build a Git tree from merged metadata values and tombstones.
fn build_merged_tree(
    repo: &git2::Repository,
    values: &BTreeMap<Key, TreeValue>,
    tombstones: &BTreeMap<Key, TombstoneEntry>,
) -> Result<git2::Oid> {
    let mut files: BTreeMap<String, Vec<u8>> = BTreeMap::new();

    for ((target_type, target_value, key), tree_val) in values {
        let target = if target_type == "project" {
            Target::parse("project")?
        } else {
            Target::parse(&format!("{}:{}", target_type, target_value))?
        };

        match tree_val {
            TreeValue::String(s) => {
                let full_path = build_tree_path(&target, key)?;
                files.insert(full_path, s.as_bytes().to_vec());
            }
            TreeValue::List(list_entries) => {
                let list_dir_path = build_list_tree_dir_path(&target, key)?;
                for (entry_name, content) in list_entries {
                    let full_path = format!("{}/{}", list_dir_path, entry_name);
                    files.insert(full_path, content.as_bytes().to_vec());
                }
            }
        }
    }

    for ((target_type, target_value, key), tombstone) in tombstones {
        let target = if target_type == "project" {
            Target::parse("project")?
        } else {
            Target::parse(&format!("{}:{}", target_type, target_value))?
        };
        let full_path = build_tombstone_tree_path(&target, key)?;
        let payload = serde_json::to_vec(&TombstoneBlob {
            timestamp: tombstone.timestamp,
            email: tombstone.email.clone(),
        })?;
        files.insert(full_path, payload);
    }

    build_tree_from_paths(repo, &files)
}

fn find_remote_refs(
    repo: &git2::Repository,
    ns: &str,
    remote: Option<&str>,
) -> Result<Vec<(String, git2::Oid)>> {
    let mut results = Vec::new();

    let refs = repo.references()?;
    let prefix = match remote {
        Some(r) => format!("refs/{}/{}", ns, r),
        None => format!("refs/{}/", ns),
    };

    for reference in refs {
        let reference = reference?;
        if let Some(name) = reference.name() {
            if name.starts_with(&prefix) && name != format!("refs/{}/local", ns) {
                if let Ok(commit) = reference.peel_to_commit() {
                    results.push((name.to_string(), commit.id()));
                }
            }
        }
    }

    Ok(results)
}

/// Parse a Git tree into value entries and tombstones.
fn parse_tree(repo: &git2::Repository, tree: &git2::Tree, prefix: &str) -> Result<ParsedTree> {
    let mut parsed = ParsedTree::default();

    // Walk the tree recursively and collect all blob paths
    let mut paths: BTreeMap<String, Vec<u8>> = BTreeMap::new();
    collect_blobs(repo, tree, prefix, &mut paths)?;

    // Group paths by target/key.
    for (path, content) in &paths {
        let parts: Vec<&str> = path.split('/').collect();

        if parts.is_empty() {
            continue;
        }

        let (target_type, target_value, key_parts) = match parse_path_parts(&parts) {
            Ok(v) => v,
            Err(_) => continue,
        };

        if key_parts.is_empty() {
            continue;
        }

        // Value path root is .../k/...
        if key_parts[0] == KEY_TREE_ROOT {
            // String value shape:
            //   .../k/<key segments...>/__value
            if key_parts.len() >= 3 && key_parts[key_parts.len() - 1] == STRING_VALUE_BLOB {
                let key_segments = &key_parts[1..key_parts.len() - 1];
                let key = match decode_key_path_segments(key_segments) {
                    Ok(k) => k,
                    Err(_) => continue,
                };
                let content_str = String::from_utf8_lossy(content).to_string();
                parsed.values.insert(
                    (target_type, target_value, key),
                    TreeValue::String(content_str),
                );
                continue;
            }

            // List value shape:
            //   .../k/<key segments...>/__list/<timestamp-hash>
            if key_parts.len() >= 4
                && key_parts[key_parts.len() - 2] == LIST_VALUE_DIR
                && git_utils::is_list_entry_name(key_parts[key_parts.len() - 1])
            {
                let key_segments = &key_parts[1..key_parts.len() - 2];
                let key = match decode_key_path_segments(key_segments) {
                    Ok(k) => k,
                    Err(_) => continue,
                };
                let entry_name = key_parts[key_parts.len() - 1].to_string();
                let content_str = String::from_utf8_lossy(content).to_string();
                let entry = parsed
                    .values
                    .entry((target_type, target_value, key))
                    .or_insert_with(|| TreeValue::List(Vec::new()));
                if let TreeValue::List(ref mut list) = entry {
                    list.push((entry_name, content_str));
                }
                continue;
            }
        }

        // Tombstone path shape:
        //   .../__tombstones/k/<key segments...>/__deleted
        if key_parts[0] == TOMBSTONE_ROOT
            && key_parts.len() >= 4
            && key_parts[1] == KEY_TREE_ROOT
            && key_parts[key_parts.len() - 1] == TOMBSTONE_BLOB
        {
            let key_segments = &key_parts[2..key_parts.len() - 1];
            let key = match decode_key_path_segments(key_segments) {
                Ok(k) => k,
                Err(_) => continue,
            };
            let tombstone = match parse_tombstone_blob(content) {
                Some(t) => t,
                None => continue,
            };
            let entry_key = (target_type, target_value, key);
            match parsed.tombstones.get(&entry_key) {
                Some(existing) if existing.timestamp >= tombstone.timestamp => {}
                _ => {
                    parsed.tombstones.insert(entry_key, tombstone);
                }
            }
        }
    }

    // Sort list entries by name (timestamp-hash)
    for value in parsed.values.values_mut() {
        if let TreeValue::List(ref mut list) = value {
            list.sort_by(|a, b| a.0.cmp(&b.0));
        }
    }

    // If both value and tombstone exist in one snapshot, value wins.
    parsed
        .tombstones
        .retain(|key, _| !parsed.values.contains_key(key));

    Ok(parsed)
}

fn parse_tombstone_blob(content: &[u8]) -> Option<TombstoneEntry> {
    let blob: TombstoneBlob = serde_json::from_slice(content).ok()?;
    Some(TombstoneEntry {
        timestamp: blob.timestamp,
        email: blob.email,
    })
}

fn collect_blobs(
    repo: &git2::Repository,
    tree: &git2::Tree,
    prefix: &str,
    paths: &mut BTreeMap<String, Vec<u8>>,
) -> Result<()> {
    for entry in tree.iter() {
        let name = entry.name().unwrap_or("");
        let full_path = if prefix.is_empty() {
            name.to_string()
        } else {
            format!("{}/{}", prefix, name)
        };

        match entry.kind() {
            Some(git2::ObjectType::Blob) => {
                let blob = repo.find_blob(entry.id())?;
                paths.insert(full_path, blob.content().to_vec());
            }
            Some(git2::ObjectType::Tree) => {
                let subtree = repo.find_tree(entry.id())?;
                collect_blobs(repo, &subtree, &full_path, paths)?;
            }
            _ => {}
        }
    }
    Ok(())
}

/// Parse path segments into (target_type, target_value, key_parts).
fn parse_path_parts<'a>(parts: &'a [&'a str]) -> Result<(String, String, &'a [&'a str])> {
    if parts.is_empty() {
        anyhow::bail!("empty path");
    }

    let target_type = parts[0];

    if target_type == "project" {
        return Ok(("project".to_string(), "".to_string(), &parts[1..]));
    }

    // Path targets: path/{the/file/path}/k/...
    // The target value is everything between the type and the key-root sentinel,
    // joined back with '/' since git splits path components naturally.
    if target_type == "path" {
        // Find the key-root sentinel 'k' (or tombstone root) after the type.
        let sentinel_pos = parts[1..]
            .iter()
            .position(|&p| p == crate::types::KEY_TREE_ROOT || p == crate::types::TOMBSTONE_ROOT)
            .map(|i| i + 1); // adjust for the slice offset
        if let Some(pos) = sentinel_pos {
            let target_value = parts[1..pos].join("/");
            return Ok((target_type.to_string(), target_value, &parts[pos..]));
        }
        anyhow::bail!("could not find key root in path target: {:?}", parts);
    }

    // All other sharded targets use two-level scheme:
    //   type/first2/full_value/key_parts...
    if parts.len() < 3 {
        anyhow::bail!("path too short for sharded target: {:?}", parts);
    }

    let target_value = parts[2].to_string();

    Ok((target_type.to_string(), target_value, &parts[3..]))
}

/// Build a nested Git tree structure from flat file paths.
fn build_tree_from_paths(
    repo: &git2::Repository,
    files: &BTreeMap<String, Vec<u8>>,
) -> Result<git2::Oid> {
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

    let mut root = Dir::default();

    for (path, content) in files {
        let parts: Vec<&str> = path.split('/').collect();
        insert_path(&mut root, &parts, content.clone());
    }

    build_dir(repo, &root)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(name: &str) -> Key {
        ("commit".to_string(), "abc123".to_string(), name.to_string())
    }

    fn string_value(value: &str) -> TreeValue {
        TreeValue::String(value.to_string())
    }

    #[test]
    fn test_three_way_merge_reports_concurrent_add_conflict() {
        let mut local = BTreeMap::new();
        local.insert(key("agent:model"), string_value("local"));

        let mut remote = BTreeMap::new();
        remote.insert(key("agent:model"), string_value("remote"));

        let (merged, conflicts) = three_way_merge(&BTreeMap::new(), &local, &remote, 100, 200)
            .expect("merge should succeed");

        assert_eq!(
            merged.get(&key("agent:model")),
            Some(&string_value("remote"))
        );
        assert_eq!(conflicts.len(), 1);
        assert_eq!(conflicts[0].reason, ConflictReason::ConcurrentAdd);
        assert_eq!(conflicts[0].resolution, ConflictResolution::Remote);
    }

    #[test]
    fn test_three_way_merge_reports_local_modified_remote_removed() {
        let mut base = BTreeMap::new();
        base.insert(key("agent:model"), string_value("base"));

        let mut local = BTreeMap::new();
        local.insert(key("agent:model"), string_value("local"));

        let (merged, conflicts) = three_way_merge(&base, &local, &BTreeMap::new(), 100, 200)
            .expect("merge should succeed");

        assert_eq!(
            merged.get(&key("agent:model")),
            Some(&string_value("local"))
        );
        assert_eq!(conflicts.len(), 1);
        assert_eq!(
            conflicts[0].reason,
            ConflictReason::LocalModifiedRemoteRemoved
        );
        assert_eq!(conflicts[0].resolution, ConflictResolution::Local);
    }

    #[test]
    fn test_three_way_merge_reports_remote_modified_local_removed() {
        let mut base = BTreeMap::new();
        base.insert(key("agent:model"), string_value("base"));

        let mut remote = BTreeMap::new();
        remote.insert(key("agent:model"), string_value("remote"));

        let (merged, conflicts) = three_way_merge(&base, &BTreeMap::new(), &remote, 100, 200)
            .expect("merge should succeed");

        assert_eq!(
            merged.get(&key("agent:model")),
            Some(&string_value("remote"))
        );
        assert_eq!(conflicts.len(), 1);
        assert_eq!(
            conflicts[0].reason,
            ConflictReason::RemoteModifiedLocalRemoved
        );
        assert_eq!(conflicts[0].resolution, ConflictResolution::Remote);
    }

    #[test]
    fn test_two_way_merge_no_common_ancestor_remote_wins_value_conflict() {
        let mut local_values = BTreeMap::new();
        local_values.insert(key("agent:model"), string_value("local"));
        local_values.insert(key("local:only"), string_value("keep-local"));

        let mut remote_values = BTreeMap::new();
        remote_values.insert(key("agent:model"), string_value("remote"));
        remote_values.insert(key("remote:only"), string_value("keep-remote"));

        let (merged_values, merged_tombstones, conflicts) = two_way_merge_no_common_ancestor(
            &local_values,
            &BTreeMap::new(),
            &remote_values,
            &BTreeMap::new(),
        );

        assert!(merged_tombstones.is_empty());
        assert_eq!(
            merged_values.get(&key("agent:model")),
            Some(&string_value("remote"))
        );
        assert_eq!(
            merged_values.get(&key("local:only")),
            Some(&string_value("keep-local"))
        );
        assert_eq!(
            merged_values.get(&key("remote:only")),
            Some(&string_value("keep-remote"))
        );
        assert_eq!(conflicts.len(), 1);
        assert_eq!(
            conflicts[0].reason,
            ConflictReason::NoCommonAncestorRemoteWins
        );
        assert_eq!(conflicts[0].resolution, ConflictResolution::Remote);
    }
}
