//! Merge algorithms for gmeta tree conflict resolution.
//!
//! Provides three-way and two-way merge strategies for combining metadata
//! trees from different sources, along with tombstone merging logic.

use std::collections::{BTreeMap, BTreeSet};

use anyhow::Result;

use super::model::{Key, TombstoneEntry, TreeValue};

/// Reason a merge conflict occurred.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConflictReason {
    /// Both sides changed the same key to different values.
    BothChanged,
    /// Both sides added the same key independently.
    ConcurrentAdd,
    /// Local modified a key that remote removed.
    LocalModifiedRemoteRemoved,
    /// Remote modified a key that local removed.
    RemoteModifiedLocalRemoved,
    /// No common ancestor; local wins by convention.
    NoCommonAncestorLocalWins,
}

impl ConflictReason {
    /// Returns a human-readable identifier for the conflict reason.
    pub fn as_str(&self) -> &'static str {
        match self {
            ConflictReason::BothChanged => "both-changed",
            ConflictReason::ConcurrentAdd => "concurrent-add",
            ConflictReason::LocalModifiedRemoteRemoved => "local-modified-remote-removed",
            ConflictReason::RemoteModifiedLocalRemoved => "remote-modified-local-removed",
            ConflictReason::NoCommonAncestorLocalWins => "no-common-ancestor-local-wins",
        }
    }
}

/// How a conflict was resolved.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConflictResolution {
    /// Local/ours side was chosen.
    Local,
    /// Remote/theirs side was chosen.
    Remote,
    /// Both sides were combined (e.g. list union).
    Union,
}

impl ConflictResolution {
    /// Returns a human-readable identifier for the resolution strategy.
    pub fn as_str(&self) -> &'static str {
        match self {
            ConflictResolution::Local => "local",
            ConflictResolution::Remote => "remote",
            ConflictResolution::Union => "union",
        }
    }
}

/// A record of how one key's conflict was resolved during a merge.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConflictDecision {
    /// The key that had a conflict.
    pub key: Key,
    /// Why the conflict occurred.
    pub reason: ConflictReason,
    /// How it was resolved.
    pub resolution: ConflictResolution,
}

/// Internal state for two-way merge without a common ancestor.
#[derive(Debug, Clone, PartialEq, Eq)]
enum MergeState {
    Absent,
    Value(TreeValue),
    Tombstone(TombstoneEntry),
}

/// Three-way merge: base vs local vs remote.
///
/// For each key:
/// - In base, local, remote (unchanged on both sides): keep as-is
/// - In base, changed only on local: take local
/// - In base, changed only on remote: take remote
/// - In base, changed on both sides (true conflict):
///   - Lists: union of entries
///   - Strings: local/ours wins
/// - Not in base, only in local: take local (new local key)
/// - Not in base, only in remote: take remote (new remote key)
/// - Not in base, in both: same conflict rules as above
/// - In base, removed on one side:
///   - If the other side modified it: keep the modified value
///   - If the other side didn't change it: remove it
///
/// # Parameters
///
/// - `base`: the common ancestor values
/// - `local`: the local/ours values
/// - `remote`: the remote/theirs values
/// - `local_timestamp`: commit timestamp for the local side (used in conflict resolution)
/// - `remote_timestamp`: commit timestamp for the remote side (used in conflict resolution)
///
/// # Returns
///
/// A tuple of `(merged_values, conflict_decisions)`.
///
/// # Errors
///
/// Currently infallible but returns `Result` for future extensibility.
pub fn three_way_merge(
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
            // In all three -- check for changes
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
                        // Both changed -- conflict resolution
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
                    // Local modified it -- modified wins over removal
                    merged.insert((*key).clone(), l.clone());
                    conflicts.push(ConflictDecision {
                        key: (*key).clone(),
                        reason: ConflictReason::LocalModifiedRemoteRemoved,
                        resolution: ConflictResolution::Local,
                    });
                }
                // else: local didn't change, remote removed -- stay removed
            }

            // In base and remote, but removed on local
            (Some(b), None, Some(r)) => {
                if r != b {
                    // Remote modified it -- modified wins over removal
                    merged.insert((*key).clone(), r.clone());
                    conflicts.push(ConflictDecision {
                        key: (*key).clone(),
                        reason: ConflictReason::RemoteModifiedLocalRemoved,
                        resolution: ConflictResolution::Remote,
                    });
                }
                // else: remote didn't change, local removed -- stay removed
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
/// Local/our state wins for any overlapping key conflict.
///
/// # Parameters
///
/// - `local_values`: local side's values
/// - `local_tombstones`: local side's tombstones
/// - `remote_values`: remote side's values
/// - `remote_tombstones`: remote side's tombstones
///
/// # Returns
///
/// A tuple of `(merged_values, merged_tombstones, conflict_decisions)`.
pub fn two_way_merge_no_common_ancestor(
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
                reason: ConflictReason::NoCommonAncestorLocalWins,
                resolution: ConflictResolution::Local,
            });
        }

        let selected = if local_state != MergeState::Absent {
            local_state
        } else {
            remote_state
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

/// Merge tombstones from base, local, and remote using three-way logic.
///
/// Tombstones that correspond to a key with a live value in `merged_values`
/// are dropped (value wins over tombstone).
///
/// # Parameters
///
/// - `base`: base ancestor tombstones
/// - `local`: local tombstones
/// - `remote`: remote tombstones
/// - `merged_values`: the already-merged values (used to suppress tombstones for live keys)
///
/// # Returns
///
/// The merged tombstone map.
pub fn merge_tombstones(
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
                    (true, true) => Some(select_preferred_tombstone(l, r)),
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
            (None, Some(l), Some(r)) => Some(select_preferred_tombstone(l, r)),
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

/// Select the preferred tombstone when both sides have one. Currently always picks local.
fn select_preferred_tombstone(local: &TombstoneEntry, _remote: &TombstoneEntry) -> TombstoneEntry {
    local.clone()
}

/// Merge set-member tombstones from local and remote.
///
/// Local entries override remote entries for the same `(key, member_id)`.
/// Tombstones for members that still exist in `merged_values` are dropped.
///
/// # Parameters
///
/// - `local`: local set-member tombstones
/// - `remote`: remote set-member tombstones
/// - `merged_values`: the already-merged values (used to filter out live members)
///
/// # Returns
///
/// The merged set-member tombstone map.
pub fn merge_set_member_tombstones(
    local: &BTreeMap<(Key, String), String>,
    remote: &BTreeMap<(Key, String), String>,
    merged_values: &BTreeMap<Key, TreeValue>,
) -> BTreeMap<(Key, String), String> {
    let mut merged = remote.clone();
    for (key, value) in local {
        merged.insert(key.clone(), value.clone());
    }

    merged.retain(|(key, member_id), _| match merged_values.get(key) {
        Some(TreeValue::Set(set)) => !set.contains_key(member_id),
        _ => true,
    });
    merged
}

/// Merge list-entry tombstones from local and remote.
///
/// When both sides have a tombstone for the same entry, the one with the
/// higher timestamp wins. Tombstones for entries that still exist in
/// `merged_values` are dropped.
///
/// # Parameters
///
/// - `local`: local list-entry tombstones
/// - `remote`: remote list-entry tombstones
/// - `merged_values`: the already-merged values (used to filter out live entries)
///
/// # Returns
///
/// The merged list-entry tombstone map.
pub fn merge_list_tombstones(
    local: &BTreeMap<(Key, String), TombstoneEntry>,
    remote: &BTreeMap<(Key, String), TombstoneEntry>,
    merged_values: &BTreeMap<Key, TreeValue>,
) -> BTreeMap<(Key, String), TombstoneEntry> {
    let mut merged = remote.clone();
    for (key, entry) in local {
        merged
            .entry(key.clone())
            .and_modify(|existing| {
                if entry.timestamp > existing.timestamp {
                    *existing = entry.clone();
                }
            })
            .or_insert_with(|| entry.clone());
    }

    // Remove tombstones for entries that still exist in the merged values
    merged.retain(|(key, entry_name), _| match merged_values.get(key) {
        Some(TreeValue::List(list)) => !list.iter().any(|(name, _)| name == entry_name),
        _ => true,
    });
    merged
}

/// Resolve a conflict where both sides changed the same key.
/// Lists union; all other direct conflicts prefer the local/ours side.
///
/// # Parameters
///
/// - `local`: the local value
/// - `remote`: the remote value
/// - `_local_timestamp`: local commit timestamp (reserved for future use)
/// - `_remote_timestamp`: remote commit timestamp (reserved for future use)
///
/// # Returns
///
/// A tuple of `(resolved_value, resolution_strategy)`.
pub fn resolve_conflict(
    local: &TreeValue,
    remote: &TreeValue,
    _local_timestamp: i64,
    _remote_timestamp: i64,
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
        // Both sets: union of members, local/ours wins for identical member ids.
        (TreeValue::Set(local_set), TreeValue::Set(remote_set)) => {
            let mut combined = remote_set.clone();
            for (member_id, content) in local_set {
                combined.insert(member_id.clone(), content.clone());
            }
            (TreeValue::Set(combined), ConflictResolution::Union)
        }
        // Both strings: local/ours wins.
        (TreeValue::String(_), TreeValue::String(_)) => (local.clone(), ConflictResolution::Local),
        // Mismatched types: local/ours wins.
        _ => (local.clone(), ConflictResolution::Local),
    }
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
            Some(&string_value("local"))
        );
        assert_eq!(conflicts.len(), 1);
        assert_eq!(conflicts[0].reason, ConflictReason::ConcurrentAdd);
        assert_eq!(conflicts[0].resolution, ConflictResolution::Local);
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
    fn test_two_way_merge_no_common_ancestor_local_wins_value_conflict() {
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
            Some(&string_value("local"))
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
            ConflictReason::NoCommonAncestorLocalWins
        );
        assert_eq!(conflicts[0].resolution, ConflictResolution::Local);
    }
}
