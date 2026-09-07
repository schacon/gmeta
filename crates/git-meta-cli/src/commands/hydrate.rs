use anyhow::Result;
use gix::bstr::ByteSlice;
use gix::prelude::ObjectIdExt;

use git_meta_lib::tree_paths;
use git_meta_lib::types::{Target, TargetType, ValueType};

/// Hydrate promised entries by looking up their blob OIDs in the tip tree
/// and fetching any that aren't already local.
///
/// Returns the number of metadata entries resolved.
pub(super) fn hydrate_promised_entries(
    session: &git_meta_lib::Session,
    target_type: &TargetType,
    entries: &[(String, String)],
) -> Result<usize> {
    let repo = session.repo();
    let db = session.store();
    let ns = session.namespace();
    let tracking_ref = format!("refs/{ns}/remotes/main");

    let tip_commit = match repo.find_reference(&tracking_ref) {
        Ok(r) => r.into_fully_peeled_id()?,
        Err(_) => return Ok(0),
    };
    let tip_tree_id = tip_commit.object()?.into_commit().tree_id()?.detach();

    // A key that pruning removed from the tip is no longer at its path there,
    // and that is precisely the key a promisor exists to fetch. Indexing
    // recorded the commit each key was last written in, so look in that tree
    // when the tip does not have it.
    let tree_for = |target: &Target, key: &str| -> Result<gix::ObjectId> {
        let Some(commit) = db.promised_commit(target, key)? else {
            return Ok(tip_tree_id);
        };
        let Ok(oid) = gix::ObjectId::from_hex(commit.as_bytes()) else {
            return Ok(tip_tree_id);
        };
        match repo.find_object(oid) {
            Ok(object) => Ok(object.into_commit().tree_id()?.detach()),
            // The commit itself can be absent in a shallow or partial clone.
            Err(_) => Ok(tip_tree_id),
        }
    };

    struct PendingEntry {
        idx: usize,
        oids: Vec<gix::ObjectId>,
        value_type: ValueType,
    }

    let mut pending: Vec<PendingEntry> = Vec::new();
    let mut not_found: Vec<usize> = Vec::new();

    for (idx, (target_value, key)) in entries.iter().enumerate() {
        let entry_target = entry_target(target_type, target_value);
        let tree_id = tree_for(&entry_target, key)?;

        if let Ok(path) = tree_paths::tree_path(&entry_target, key) {
            if let Some(oid) = git_meta_lib::git_utils::find_blob_oid_in_tree(repo, tree_id, &path)?
            {
                pending.push(PendingEntry {
                    idx,
                    oids: vec![oid],
                    value_type: ValueType::String,
                });
                continue;
            }
        }

        if let Ok(path) = tree_paths::list_dir_path(&entry_target, key) {
            if let Some(dir_oid) =
                git_meta_lib::git_utils::find_blob_oid_in_tree(repo, tree_id, &path)?
            {
                let list_tree = dir_oid.attach(repo).object()?.into_tree();
                let oids = blob_oids_from_tree(&list_tree);
                if !oids.is_empty() {
                    pending.push(PendingEntry {
                        idx,
                        oids,
                        value_type: ValueType::List,
                    });
                    continue;
                }
            }
        }

        if let Ok(set_path) = tree_paths::set_dir_path(&entry_target, key) {
            if let Some(dir_oid) =
                git_meta_lib::git_utils::find_blob_oid_in_tree(repo, tree_id, &set_path)?
            {
                let set_tree = dir_oid.attach(repo).object()?.into_tree();
                let oids = blob_oids_from_tree(&set_tree);
                if !oids.is_empty() {
                    pending.push(PendingEntry {
                        idx,
                        oids,
                        value_type: ValueType::Set,
                    });
                    continue;
                }
            }
        }

        not_found.push(idx);
    }

    if !not_found.is_empty() {
        // Leave these promised. A value can be temporarily unreachable — the
        // commit holding it may not have been fetched yet — and forgetting the
        // key would lose the only record that it ever existed.
        eprintln!(
            "{} value{} could not be located in the fetched history.",
            not_found.len(),
            if not_found.len() == 1 { "" } else { "s" }
        );
    }

    if pending.is_empty() {
        return Ok(0);
    }

    let all_oids: Vec<gix::ObjectId> = pending
        .iter()
        .flat_map(|p| p.oids.iter().copied())
        .collect();
    let mut missing: Vec<gix::ObjectId> = Vec::new();
    for oid in &all_oids {
        if oid.attach(repo).object().is_err() {
            missing.push(*oid);
        }
    }

    if !missing.is_empty() {
        let remote_name = git_meta_lib::git_utils::resolve_meta_remote(repo, None)?;
        eprintln!(
            "Fetching {} blob{} from remote...",
            missing.len(),
            if missing.len() == 1 { "" } else { "s" }
        );
        git_meta_lib::git_utils::fetch_blob_oids(repo, &remote_name, &missing)?;
    }

    let mut hydrated = 0;
    for entry in &pending {
        let (target_value, key) = &entries[entry.idx];
        let entry_target = entry_target(target_type, target_value);

        match entry.value_type {
            ValueType::String => {
                let oid = entry.oids[0];
                let blob = match oid.attach(repo).object() {
                    Ok(b) => b.into_blob(),
                    Err(_) => continue,
                };
                let Ok(content) = std::str::from_utf8(&blob.data) else {
                    continue;
                };
                // A serialized tree holds the raw string; the store holds it
                // JSON-encoded, as `set_value` does. Writing the raw bytes
                // straight in produces a row nothing can read back.
                let encoded = serde_json::to_string(content)?;
                db.resolve_promised(&entry_target, key, &encoded, &ValueType::String, false)?;
                hydrated += 1;
            }
            ValueType::List => {
                let mut list_entries = Vec::new();
                for oid in &entry.oids {
                    if let Ok(obj) = oid.attach(repo).object() {
                        let blob = obj.into_blob();
                        if let Ok(s) = std::str::from_utf8(&blob.data) {
                            list_entries.push(s.to_string());
                        }
                    }
                }
                let json_value = serde_json::to_string(&list_entries)?;
                db.resolve_promised(&entry_target, key, &json_value, &ValueType::List, false)?;
                hydrated += 1;
            }
            ValueType::Set => {
                let mut set_members = Vec::new();
                for oid in &entry.oids {
                    if let Ok(obj) = oid.attach(repo).object() {
                        let blob = obj.into_blob();
                        if let Ok(s) = std::str::from_utf8(&blob.data) {
                            set_members.push(s.to_string());
                        }
                    }
                }
                set_members.sort();
                let json_value = serde_json::to_string(&set_members)?;
                db.resolve_promised(&entry_target, key, &json_value, &ValueType::Set, false)?;
                hydrated += 1;
            }
            _ => anyhow::bail!("unsupported value type"),
        }
    }

    Ok(hydrated)
}

fn entry_target(target_type: &TargetType, target_value: &str) -> Target {
    if *target_type == TargetType::Project {
        Target::project()
    } else {
        Target::from_parts(target_type.clone(), Some(target_value.to_string()))
    }
}

fn blob_oids_from_tree(tree: &gix::Tree<'_>) -> Vec<gix::ObjectId> {
    tree.iter()
        .filter_map(|e| {
            let e = e.ok()?;
            let name = e.filename().to_str().ok()?;
            if name.starts_with("__") || !e.mode().is_blob() {
                return None;
            }
            Some(e.object_id())
        })
        .collect()
}
