//! Serialize local metadata to Git tree(s) and commit(s).
//!
//! This module implements the full serialization workflow: reading metadata
//! from the SQLite store, building Git trees (full or incremental), creating
//! commits, updating refs, and optionally auto-pruning old entries.
//!
//! The public entry point is [`run()`], which takes a [`Session`](crate::Session)
//! and returns a [`SerializeOutput`] describing what was written.

use std::collections::{BTreeMap, BTreeSet};

use gix::bstr::ByteSlice;
use gix::prelude::ObjectIdExt;
use gix::refs::transaction::PreviousValue;

use crate::db::types::SerializableEntry;
use crate::db::Store;
use crate::error::{Error, Result};
use crate::list_value::{make_entry_name, parse_entries};
use crate::prune::{self, PruneRules};
use crate::session::Session;
use crate::tree::filter::{classify_key, parse_filter_rules, MAIN_DEST};
use crate::tree::format::{build_dir, build_tree_from_paths, insert_path, TreeDir};
use crate::tree::model::Tombstone;
use crate::types::{Target, TargetType, ValueType};

/// Maximum number of individual change lines included in a commit message.
const MAX_COMMIT_CHANGES: usize = 1000;

/// Result of a serialize operation.
///
/// Contains all the information needed by a CLI or other consumer
/// to report what happened, without performing any I/O itself.
pub struct SerializeOutput {
    /// Number of metadata changes serialized (total entries across all destinations).
    pub changes: usize,
    /// Refs that were written, e.g. `["refs/meta/local/main"]`.
    pub refs_written: Vec<String>,
    /// Number of entries dropped by auto-prune (0 if no prune triggered).
    pub pruned: u64,
}

/// Serialize local metadata to Git tree(s) and commit(s).
///
/// Determines incremental vs full mode automatically based on
/// `last_materialized`. Applies filter routing and pruning rules.
/// Updates local refs and the materialization timestamp.
///
/// # Parameters
///
/// - `session`: the gmeta session providing the repository, store, and config.
/// - `now`: the current timestamp in milliseconds since the Unix epoch,
///   used for the commit signature and the `last_materialized` marker.
///
/// # Returns
///
/// A [`SerializeOutput`] with counts and written refs. If there is nothing
/// to serialize, `changes` will be `0` and `refs_written` will be empty.
///
/// # Errors
///
/// Returns an error if database reads, Git object writes, or ref updates fail.
pub fn run(session: &Session, now: i64) -> Result<SerializeOutput> {
    let repo = session.repo();
    let local_ref_name = session.local_ref();
    let last_materialized = session.store().get_last_materialized()?;

    // Determine existing tree for incremental mode
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

    // Determine incremental vs full mode and collect entries + changes
    let (
        metadata_entries,
        tombstone_entries,
        set_tombstone_entries,
        list_tombstone_entries,
        dirty_target_bases,
        changes,
    ) = if let Some(since) = last_materialized {
        let modified = session.store().get_modified_since(since)?;
        if modified.is_empty() && existing_tree_oid.is_some() {
            return Ok(SerializeOutput {
                changes: 0,
                refs_written: Vec::new(),
                pruned: 0,
            });
        }

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
                    format!("{target_type}:{target_value}")
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
                Target::parse(&format!("{target_type}:{target_value}"))?
            };
            dirty_bases.insert(target.tree_base_path());
        }

        let metadata = session.store().get_all_metadata()?;
        let tombstones = session.store().get_all_tombstones()?;
        let set_tombstones = session.store().get_all_set_tombstones()?;
        let list_tombstones = session.store().get_all_list_tombstones()?;

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
        let metadata = session.store().get_all_metadata()?;

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
            session.store().get_all_tombstones()?,
            session.store().get_all_set_tombstones()?,
            session.store().get_all_list_tombstones()?,
            None,
            changes,
        )
    };

    if metadata_entries.is_empty() && tombstone_entries.is_empty() {
        return Ok(SerializeOutput {
            changes: 0,
            refs_written: Vec::new(),
            pruned: 0,
        });
    }

    // Apply prune-since cutoff to filter old entries before building the tree
    let prune_since = session
        .store()
        .get(&TargetType::Project, "", "meta:prune:since")?
        .and_then(|e| serde_json::from_str::<String>(&e.value).ok());
    let prune_rules = prune::read_prune_rules(session.store())?;
    let prune_cutoff_ms = prune_since
        .as_deref()
        .map(|s| prune::parse_since_to_cutoff_ms(s, now))
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

    // Route entries through filter rules to destinations
    let filter_rules = parse_filter_rules(session.store())?;

    type TombEntry = (String, String, String, i64, String);
    type SetTombEntry = (String, String, String, String, String, i64, String);
    type ListTombEntry = (String, String, String, String, i64, String);

    let mut dest_metadata: BTreeMap<String, Vec<SerializableEntry>> = BTreeMap::new();
    let mut dest_tombstones: BTreeMap<String, Vec<TombEntry>> = BTreeMap::new();
    let mut dest_set_tombstones: BTreeMap<String, Vec<SetTombEntry>> = BTreeMap::new();
    let mut dest_list_tombstones: BTreeMap<String, Vec<ListTombEntry>> = BTreeMap::new();

    for entry in &metadata_entries {
        let key = &entry.key;
        if let Some(dests) = classify_key(key, &filter_rules) {
            for dest in dests {
                dest_metadata.entry(dest).or_default().push(entry.clone());
            }
        }
    }

    for entry in &tombstone_entries {
        let key = &entry.2;
        if let Some(dests) = classify_key(key, &filter_rules) {
            for dest in dests {
                dest_tombstones.entry(dest).or_default().push(entry.clone());
            }
        }
    }

    for entry in &set_tombstone_entries {
        let key = &entry.2;
        if let Some(dests) = classify_key(key, &filter_rules) {
            for dest in dests {
                dest_set_tombstones
                    .entry(dest)
                    .or_default()
                    .push(entry.clone());
            }
        }
    }

    for entry in &list_tombstone_entries {
        let key = &entry.2;
        if let Some(dests) = classify_key(key, &filter_rules) {
            for dest in dests {
                dest_list_tombstones
                    .entry(dest)
                    .or_default()
                    .push(entry.clone());
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

    let total_changes: usize = dest_metadata.values().map(|v| v.len()).sum();

    let name = session.name();
    let email = session.email();
    let sig = gix::actor::Signature {
        name: name.into(),
        email: email.into(),
        time: gix::date::Time::new(now / 1000, 0),
    };

    let mut refs_written = Vec::new();
    let mut auto_pruned = 0u64;

    for dest in &all_dests {
        let ref_name = session.destination_ref(dest);
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

        let tree_oid = build_tree(repo, meta, tombs, set_tombs, list_tombs, existing, dirty)?;

        let parent_oid = repo
            .find_reference(&ref_name)
            .ok()
            .and_then(|r| r.into_fully_peeled_id().ok())
            .map(|id| id.detach());

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

        let commit_oid = repo
            .write_object(&commit)
            .map_err(|e| Error::Other(format!("{e}")))?
            .detach();
        repo.reference(
            ref_name.as_str(),
            commit_oid,
            PreviousValue::Any,
            "gmeta: serialize",
        )
        .map_err(|e| Error::Other(format!("{e}")))?;

        refs_written.push(ref_name.clone());

        // Auto-prune only for main destination
        if dest == MAIN_DEST {
            if let Some(ref prune_rules_val) = prune_rules {
                if prune::should_prune(repo, tree_oid, prune_rules_val)? {
                    let prune_tree_oid =
                        prune_tree(repo, tree_oid, prune_rules_val, session.store(), now)?;

                    if prune_tree_oid != tree_oid {
                        let prune_parent_oid = repo
                            .find_reference(&ref_name)
                            .map_err(|e| Error::Other(format!("{e}")))?
                            .into_fully_peeled_id()
                            .map_err(|e| Error::Other(format!("{e}")))?
                            .detach();

                        let (keys_dropped, keys_retained) =
                            count_prune_stats(repo, tree_oid, prune_tree_oid)?;

                        auto_pruned = keys_dropped;

                        let min_size_str = prune_rules_val
                            .min_size
                            .map(|s| format!("\nmin-size: {s}"))
                            .unwrap_or_default();

                        let message = format!(
                            "gmeta: prune --since={}\n\npruned: true\nsince: {}{}\nkeys-dropped: {}\nkeys-retained: {}",
                            prune_rules_val.since, prune_rules_val.since, min_size_str, keys_dropped, keys_retained
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

                        let _prune_commit_oid = repo
                            .write_object(&prune_commit)
                            .map_err(|e| Error::Other(format!("{e}")))?
                            .detach();
                        repo.reference(
                            ref_name.as_str(),
                            _prune_commit_oid,
                            PreviousValue::Any,
                            "gmeta: auto-prune",
                        )
                        .map_err(|e| Error::Other(format!("{e}")))?;
                    }
                }
            }
        }
    }

    session.store().set_last_materialized(now)?;

    Ok(SerializeOutput {
        changes: total_changes,
        refs_written,
        pruned: pruned_count + auto_pruned,
    })
}

/// Build a commit message from a list of changes.
///
/// Each change is `(op_char, target_label, key)`.
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

/// Build a Git tree from pre-filtered metadata (no incremental mode).
///
/// Used by `gmeta prune` to rebuild a tree from only the surviving entries.
///
/// # Parameters
///
/// - `repo`: the Git repository to write objects into
/// - `metadata_entries`: metadata entries to include
/// - `tombstone_entries`: key tombstones
/// - `set_tombstone_entries`: set-member tombstones
/// - `list_tombstone_entries`: list-entry tombstones
///
/// # Returns
///
/// The OID of the root Git tree object.
///
/// # Errors
///
/// Returns an error if target parsing or Git object writes fail.
pub fn build_filtered_tree(
    repo: &gix::Repository,
    metadata_entries: &[SerializableEntry],
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
    )
}

/// Build a complete Git tree from all metadata entries.
///
/// When `existing_tree_oid` and `dirty_target_bases` are provided, only entries
/// belonging to dirty targets are processed; unchanged subtrees are reused
/// from the existing tree by OID (incremental mode).
fn build_tree(
    repo: &gix::Repository,
    metadata_entries: &[SerializableEntry],
    tombstone_entries: &[(String, String, String, i64, String)],
    set_tombstone_entries: &[(String, String, String, String, String, i64, String)],
    list_tombstone_entries: &[(String, String, String, String, i64, String)],
    existing_tree_oid: Option<gix::ObjectId>,
    dirty_target_bases: Option<&BTreeSet<String>>,
) -> Result<gix::ObjectId> {
    let mut files: BTreeMap<String, Vec<u8>> = BTreeMap::new();

    for e in metadata_entries {
        let target = if e.target_type == "project" {
            Target::parse("project")?
        } else {
            Target::parse(&format!("{}:{}", e.target_type, e.target_value))?
        };

        // Skip entries for clean targets -- their subtrees will be reused
        if let Some(dirty) = dirty_target_bases {
            if !dirty.contains(&target.tree_base_path()) {
                continue;
            }
        }

        match e.value_type {
            ValueType::String => {
                let full_path = target.tree_path(&e.key)?;
                if e.is_git_ref {
                    let oid = gix::ObjectId::from_hex(e.value.as_bytes())
                        .map_err(|e| Error::Other(format!("{e}")))?;
                    let blob = oid
                        .attach(repo)
                        .object()
                        .map_err(|e| Error::Other(format!("{e}")))?
                        .into_blob();
                    files.insert(full_path, blob.data.to_vec());
                } else {
                    let raw_value: String = match serde_json::from_str(&e.value) {
                        Ok(s) => s,
                        Err(_) => e.value.to_string(),
                    };
                    files.insert(full_path, raw_value.into_bytes());
                }
            }
            ValueType::List => {
                let list_entries =
                    parse_entries(&e.value).map_err(|e| Error::InvalidValue(format!("{e}")))?;
                let list_dir_path = target.list_dir_path(&e.key)?;
                for entry in list_entries {
                    let entry_name = make_entry_name(&entry);
                    let full_path = format!("{list_dir_path}/{entry_name}");
                    files.insert(full_path, entry.value.into_bytes());
                }
            }
            ValueType::Set => {
                let members: Vec<String> = serde_json::from_str(&e.value)
                    .map_err(|e| Error::InvalidValue(format!("failed to decode set value: {e}")))?;
                let set_dir_path = target.set_dir_path(&e.key)?;
                for member in members {
                    let member_id = crate::types::set_member_id(&member);
                    let full_path = format!("{set_dir_path}/{member_id}");
                    files.insert(full_path, member.into_bytes());
                }
            }
        }
    }

    for (target_type, target_value, key, timestamp, email) in tombstone_entries {
        let target = if target_type == "project" {
            Target::parse("project")?
        } else {
            Target::parse(&format!("{target_type}:{target_value}"))?
        };

        if let Some(dirty) = dirty_target_bases {
            if !dirty.contains(&target.tree_base_path()) {
                continue;
            }
        }

        let full_path = target.tombstone_path(key)?;
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
            Target::parse(&format!("{target_type}:{target_value}"))?
        };

        if let Some(dirty) = dirty_target_bases {
            if !dirty.contains(&target.tree_base_path()) {
                continue;
            }
        }

        let full_path = target.set_member_tombstone_path(key, member_id)?;
        files.insert(full_path, value.as_bytes().to_vec());
    }

    for (target_type, target_value, key, entry_name, timestamp, email) in list_tombstone_entries {
        let target = if target_type == "project" {
            Target::parse("project")?
        } else {
            Target::parse(&format!("{target_type}:{target_value}"))?
        };

        if let Some(dirty) = dirty_target_bases {
            if !dirty.contains(&target.tree_base_path()) {
                continue;
            }
        }

        let full_path = target.list_entry_tombstone_path(key, entry_name)?;
        let payload = serde_json::to_vec(&Tombstone {
            timestamp: *timestamp,
            email: email.clone(),
        })?;
        files.insert(full_path, payload);
    }

    // Build nested tree, reusing unchanged subtrees from existing tree
    if let (Some(existing_oid), Some(dirty_bases)) = (existing_tree_oid, dirty_target_bases) {
        build_tree_incremental(repo, existing_oid, &files, dirty_bases)
    } else {
        build_tree_from_paths(repo, &files)
    }
}

/// Incrementally build a tree by patching an existing tree.
///
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

    let mut editor = repo
        .edit_tree(tree_oid)
        .map_err(|e| Error::Other(format!("{e}")))?;

    for name in &direct_removes {
        let _ = editor.remove(name);
    }

    // For grouped paths, recurse into subtrees
    let tree = tree_oid
        .attach(repo)
        .object()
        .map_err(|e| Error::Other(format!("{e}")))?
        .into_tree();
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
            let new_tree = new_oid
                .attach(repo)
                .object()
                .map_err(|e| Error::Other(format!("{e}")))?
                .into_tree();
            if new_tree.iter().count() > 0 {
                editor
                    .upsert(name, gix::objs::tree::EntryKind::Tree, new_oid)
                    .map_err(|e| Error::Other(format!("{e}")))?;
            } else {
                let _ = editor.remove(name);
            }
        }
    }

    Ok(editor
        .write()
        .map_err(|e| Error::Other(format!("{e}")))?
        .detach())
}

/// Merge a [`TreeDir`] structure into an existing tree.
///
/// Existing entries not present in `dir` are preserved.
/// Entries in `dir` overwrite existing entries with the same name.
fn merge_dir_into_tree(
    repo: &gix::Repository,
    dir: &TreeDir,
    existing_oid: gix::ObjectId,
) -> Result<gix::ObjectId> {
    let mut editor = repo
        .edit_tree(existing_oid)
        .map_err(|e| Error::Other(format!("{e}")))?;

    for (name, content) in &dir.files {
        let blob_oid: gix::ObjectId = repo
            .write_blob(content)
            .map_err(|e| Error::Other(format!("{e}")))?
            .into();
        editor
            .upsert(name, gix::objs::tree::EntryKind::Blob, blob_oid)
            .map_err(|e| Error::Other(format!("{e}")))?;
    }

    let existing_tree = existing_oid
        .attach(repo)
        .object()
        .map_err(|e| Error::Other(format!("{e}")))?
        .into_tree();
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
        editor
            .upsert(name, gix::objs::tree::EntryKind::Tree, child_oid)
            .map_err(|e| Error::Other(format!("{e}")))?;
    }

    Ok(editor
        .write()
        .map_err(|e| Error::Other(format!("{e}")))?
        .detach())
}

/// Prune a serialized tree by dropping entries older than the cutoff.
///
/// Returns the OID of the new (possibly smaller) tree. If the tree would
/// be unchanged, the same OID is returned.
///
/// # Parameters
///
/// - `repo`: the Git repository
/// - `tree_oid`: the root tree to prune
/// - `rules`: the prune rules to apply
/// - `db`: the metadata store (for potential future use by prune helpers)
///
/// # Errors
///
/// Returns an error if Git object reads/writes fail or cutoff parsing fails.
pub fn prune_tree(
    repo: &gix::Repository,
    tree_oid: gix::ObjectId,
    rules: &PruneRules,
    db: &Store,
    now_ms: i64,
) -> Result<gix::ObjectId> {
    let cutoff_ms = prune::parse_since_to_cutoff_ms(&rules.since, now_ms)?;
    let min_size = rules.min_size.unwrap_or(0);

    let tree = tree_oid
        .attach(repo)
        .object()
        .map_err(|e| Error::Other(format!("{e}")))?
        .into_tree();
    let mut editor = repo
        .empty_tree()
        .edit()
        .map_err(|e| Error::Other(format!("{e}")))?;

    for entry_result in tree.iter() {
        let entry = entry_result.map_err(|e| Error::Other(format!("{e}")))?;
        let name = entry.filename().to_str_lossy().to_string();

        if name == "project" {
            editor
                .upsert(&name, entry.mode().kind(), entry.object_id())
                .map_err(|e| Error::Other(format!("{e}")))?;
            continue;
        }

        if entry.mode().is_tree() {
            let subtree_oid = entry.object_id();

            // Check min-size
            if min_size > 0 {
                let size = prune::compute_tree_size_for(repo, subtree_oid)?;
                if size < min_size {
                    editor
                        .upsert(&name, entry.mode().kind(), subtree_oid)
                        .map_err(|e| Error::Other(format!("{e}")))?;
                    continue;
                }
            }

            let pruned_oid = prune_target_type_tree(repo, subtree_oid, cutoff_ms, min_size, db)?;
            let pruned_tree = pruned_oid
                .attach(repo)
                .object()
                .map_err(|e| Error::Other(format!("{e}")))?
                .into_tree();
            if pruned_tree.iter().count() > 0 {
                editor
                    .upsert(&name, gix::objs::tree::EntryKind::Tree, pruned_oid)
                    .map_err(|e| Error::Other(format!("{e}")))?;
            }
        } else {
            editor
                .upsert(&name, entry.mode().kind(), entry.object_id())
                .map_err(|e| Error::Other(format!("{e}")))?;
        }
    }

    Ok(editor
        .write()
        .map_err(|e| Error::Other(format!("{e}")))?
        .detach())
}

fn prune_target_type_tree(
    repo: &gix::Repository,
    tree_oid: gix::ObjectId,
    cutoff_ms: i64,
    min_size: u64,
    db: &Store,
) -> Result<gix::ObjectId> {
    let tree = tree_oid
        .attach(repo)
        .object()
        .map_err(|e| Error::Other(format!("{e}")))?
        .into_tree();
    let mut editor = repo
        .empty_tree()
        .edit()
        .map_err(|e| Error::Other(format!("{e}")))?;

    for entry_result in tree.iter() {
        let entry = entry_result.map_err(|e| Error::Other(format!("{e}")))?;
        let name = entry.filename().to_str_lossy().to_string();

        if entry.mode().is_tree() {
            let subtree_oid = entry.object_id();
            let pruned_oid = prune_subtree_recursive(repo, subtree_oid, cutoff_ms, min_size, db)?;
            let pruned_tree = pruned_oid
                .attach(repo)
                .object()
                .map_err(|e| Error::Other(format!("{e}")))?
                .into_tree();
            if pruned_tree.iter().count() > 0 {
                editor
                    .upsert(&name, gix::objs::tree::EntryKind::Tree, pruned_oid)
                    .map_err(|e| Error::Other(format!("{e}")))?;
            }
        } else {
            editor
                .upsert(&name, entry.mode().kind(), entry.object_id())
                .map_err(|e| Error::Other(format!("{e}")))?;
        }
    }

    Ok(editor
        .write()
        .map_err(|e| Error::Other(format!("{e}")))?
        .detach())
}

fn prune_subtree_recursive(
    repo: &gix::Repository,
    tree_oid: gix::ObjectId,
    cutoff_ms: i64,
    _min_size: u64,
    _db: &Store,
) -> Result<gix::ObjectId> {
    let tree = tree_oid
        .attach(repo)
        .object()
        .map_err(|e| Error::Other(format!("{e}")))?
        .into_tree();
    let mut editor = repo
        .empty_tree()
        .edit()
        .map_err(|e| Error::Other(format!("{e}")))?;

    for entry_result in tree.iter() {
        let entry = entry_result.map_err(|e| Error::Other(format!("{e}")))?;
        let name = entry.filename().to_str_lossy().to_string();

        if entry.mode().is_tree() {
            if name == "__list" {
                let list_tree_oid = entry.object_id();
                let pruned_oid = prune_list_tree(repo, list_tree_oid, cutoff_ms)?;
                let pruned_tree = pruned_oid
                    .attach(repo)
                    .object()
                    .map_err(|e| Error::Other(format!("{e}")))?
                    .into_tree();
                if pruned_tree.iter().count() > 0 {
                    editor
                        .upsert(&name, gix::objs::tree::EntryKind::Tree, pruned_oid)
                        .map_err(|e| Error::Other(format!("{e}")))?;
                }
            } else if name == "__tombstones" {
                let tomb_tree_oid = entry.object_id();
                let pruned_oid = prune_tombstone_tree(repo, tomb_tree_oid, cutoff_ms)?;
                let pruned_tree = pruned_oid
                    .attach(repo)
                    .object()
                    .map_err(|e| Error::Other(format!("{e}")))?
                    .into_tree();
                if pruned_tree.iter().count() > 0 {
                    editor
                        .upsert(&name, gix::objs::tree::EntryKind::Tree, pruned_oid)
                        .map_err(|e| Error::Other(format!("{e}")))?;
                }
            } else {
                let subtree_oid = entry.object_id();
                let pruned_oid =
                    prune_subtree_recursive(repo, subtree_oid, cutoff_ms, _min_size, _db)?;
                let pruned_tree = pruned_oid
                    .attach(repo)
                    .object()
                    .map_err(|e| Error::Other(format!("{e}")))?
                    .into_tree();
                if pruned_tree.iter().count() > 0 {
                    editor
                        .upsert(&name, gix::objs::tree::EntryKind::Tree, pruned_oid)
                        .map_err(|e| Error::Other(format!("{e}")))?;
                }
            }
        } else {
            editor
                .upsert(&name, entry.mode().kind(), entry.object_id())
                .map_err(|e| Error::Other(format!("{e}")))?;
        }
    }

    Ok(editor
        .write()
        .map_err(|e| Error::Other(format!("{e}")))?
        .detach())
}

fn prune_list_tree(
    repo: &gix::Repository,
    tree_oid: gix::ObjectId,
    cutoff_ms: i64,
) -> Result<gix::ObjectId> {
    let tree = tree_oid
        .attach(repo)
        .object()
        .map_err(|e| Error::Other(format!("{e}")))?
        .into_tree();
    let mut editor = repo
        .empty_tree()
        .edit()
        .map_err(|e| Error::Other(format!("{e}")))?;

    for entry_result in tree.iter() {
        let entry = entry_result.map_err(|e| Error::Other(format!("{e}")))?;
        let name = entry.filename().to_str_lossy().to_string();
        // Entry names are formatted as "{timestamp_ms}-{hash5}"
        if let Some((ts_str, _)) = name.split_once('-') {
            if let Ok(ts) = ts_str.parse::<i64>() {
                if ts < cutoff_ms {
                    continue; // Drop old entry
                }
            }
        }
        editor
            .upsert(&name, entry.mode().kind(), entry.object_id())
            .map_err(|e| Error::Other(format!("{e}")))?;
    }

    Ok(editor
        .write()
        .map_err(|e| Error::Other(format!("{e}")))?
        .detach())
}

fn prune_tombstone_tree(
    repo: &gix::Repository,
    tree_oid: gix::ObjectId,
    cutoff_ms: i64,
) -> Result<gix::ObjectId> {
    let tree = tree_oid
        .attach(repo)
        .object()
        .map_err(|e| Error::Other(format!("{e}")))?
        .into_tree();
    let mut editor = repo
        .empty_tree()
        .edit()
        .map_err(|e| Error::Other(format!("{e}")))?;

    for entry_result in tree.iter() {
        let entry = entry_result.map_err(|e| Error::Other(format!("{e}")))?;
        let name = entry.filename().to_str_lossy().to_string();

        if entry.mode().is_tree() {
            let subtree_oid = entry.object_id();
            let pruned_oid = prune_tombstone_tree(repo, subtree_oid, cutoff_ms)?;
            let pruned_tree = pruned_oid
                .attach(repo)
                .object()
                .map_err(|e| Error::Other(format!("{e}")))?
                .into_tree();
            if pruned_tree.iter().count() > 0 {
                editor
                    .upsert(&name, gix::objs::tree::EntryKind::Tree, pruned_oid)
                    .map_err(|e| Error::Other(format!("{e}")))?;
            }
        } else if entry.mode().is_blob() && name == "__deleted" {
            let blob = entry
                .object_id()
                .attach(repo)
                .object()
                .map_err(|e| Error::Other(format!("{e}")))?
                .into_blob();
            if let Ok(content) = std::str::from_utf8(&blob.data) {
                if let Ok(parsed) = serde_json::from_str::<serde_json::Value>(content) {
                    if let Some(ts) = parsed.get("timestamp").and_then(|v| v.as_i64()) {
                        if ts < cutoff_ms {
                            continue; // Drop old tombstone
                        }
                    }
                }
            }
            editor
                .upsert(&name, entry.mode().kind(), entry.object_id())
                .map_err(|e| Error::Other(format!("{e}")))?;
        } else {
            editor
                .upsert(&name, entry.mode().kind(), entry.object_id())
                .map_err(|e| Error::Other(format!("{e}")))?;
        }
    }

    Ok(editor
        .write()
        .map_err(|e| Error::Other(format!("{e}")))?
        .detach())
}

/// Count keys in original and pruned trees to produce stats.
///
/// Returns `(keys_dropped, keys_retained)`.
///
/// # Errors
///
/// Returns an error if Git object reads fail.
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
    let tree = tree_oid
        .attach(repo)
        .object()
        .map_err(|e| Error::Other(format!("{e}")))?
        .into_tree();
    for entry_result in tree.iter() {
        let entry = entry_result.map_err(|e| Error::Other(format!("{e}")))?;
        if entry.mode().is_blob() {
            *count += 1;
        } else if entry.mode().is_tree() {
            count_all_blobs(repo, entry.object_id(), count)?;
        }
    }
    Ok(())
}
