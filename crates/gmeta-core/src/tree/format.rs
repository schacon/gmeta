//! Tree format operations: parsing Git trees into [`ParsedTree`] and building
//! Git trees from metadata values and tombstones.

use std::collections::BTreeMap;

use crate::error::{Error, Result};

use super::model::{Key, ParsedTree, Tombstone, TreeValue};
use crate::git_utils;
use crate::types::{
    decode_key_path_segments, decode_path_target_segments, TargetType, LIST_VALUE_DIR,
    PATH_TARGET_SEPARATOR, SET_VALUE_DIR, STRING_VALUE_BLOB, TOMBSTONE_BLOB, TOMBSTONE_ROOT,
};

/// In-memory representation of a nested directory tree.
///
/// Used as an intermediate structure when building Git trees from
/// flat path-to-content mappings.
#[derive(Default)]
pub struct TreeDir {
    /// Blob entries: file name to content.
    pub files: BTreeMap<String, Vec<u8>>,
    /// Subtree entries: directory name to child `TreeDir`.
    pub dirs: BTreeMap<String, TreeDir>,
}

/// Insert a file into a [`TreeDir`] tree at the given path segments.
///
/// # Parameters
///
/// - `dir`: the root directory to insert into
/// - `parts`: the path components (e.g., `["a", "b", "file.txt"]`)
/// - `content`: the blob content to store
pub fn insert_path(dir: &mut TreeDir, parts: &[&str], content: Vec<u8>) {
    if parts.len() == 1 {
        dir.files.insert(parts[0].to_string(), content);
    } else {
        let child = dir.dirs.entry(parts[0].to_string()).or_default();
        insert_path(child, &parts[1..], content);
    }
}

/// Recursively build a Git tree from a [`TreeDir`] structure.
///
/// # Parameters
///
/// - `repo`: the Git repository to write objects into
/// - `dir`: the directory tree to convert
///
/// # Returns
///
/// The OID of the written Git tree object.
///
/// # Errors
///
/// Returns an error if any Git object write fails.
pub fn build_dir(repo: &gix::Repository, dir: &TreeDir) -> Result<gix::ObjectId> {
    let mut editor = repo
        .empty_tree()
        .edit()
        .map_err(|e| Error::Other(format!("{e}")))?;

    for (name, content) in &dir.files {
        let blob_id = repo
            .write_blob(content)
            .map_err(|e| Error::Other(format!("{e}")))?
            .detach();
        editor
            .upsert(name, gix::objs::tree::EntryKind::Blob, blob_id)
            .map_err(|e| Error::Other(format!("{e}")))?;
    }

    for (name, child_dir) in &dir.dirs {
        let child_oid = build_dir(repo, child_dir)?;
        editor
            .upsert(name, gix::objs::tree::EntryKind::Tree, child_oid)
            .map_err(|e| Error::Other(format!("{e}")))?;
    }

    Ok(editor
        .write()
        .map_err(|e| Error::Other(format!("{e}")))?
        .detach())
}

/// Build a nested Git tree structure from flat file paths (full rebuild).
///
/// # Parameters
///
/// - `repo`: the Git repository to write objects into
/// - `files`: mapping from slash-separated paths to blob content
///
/// # Returns
///
/// The OID of the root Git tree object.
///
/// # Errors
///
/// Returns an error if any Git object write fails.
pub fn build_tree_from_paths(
    repo: &gix::Repository,
    files: &BTreeMap<String, Vec<u8>>,
) -> Result<gix::ObjectId> {
    let mut root = TreeDir::default();

    for (path, content) in files {
        let parts: Vec<&str> = path.split('/').collect();
        insert_path(&mut root, &parts, content.clone());
    }

    build_dir(repo, &root)
}

/// Parse a Git tree into value entries and tombstones.
///
/// Walks the tree recursively, collecting all blobs, then interprets
/// the path structure to reconstruct metadata values, tombstones,
/// set-member tombstones, and list-entry tombstones.
///
/// # Parameters
///
/// - `repo`: the Git repository containing the tree
/// - `tree`: the root tree to parse
/// - `prefix`: path prefix (pass `""` for root)
///
/// # Returns
///
/// A fully populated [`ParsedTree`].
///
/// # Errors
///
/// Returns an error if Git object reads fail or paths are malformed.
pub fn parse_tree(
    repo: &gix::Repository,
    tree_id: gix::ObjectId,
    prefix: &str,
) -> Result<ParsedTree> {
    let mut parsed = ParsedTree::default();

    // Walk the tree recursively and collect all blob paths
    let mut paths: BTreeMap<String, Vec<u8>> = BTreeMap::new();
    collect_blobs(repo, tree_id, prefix, &mut paths)?;

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

        // Whole-key tombstone path shape:
        //   .../__tombstones/<key segments...>/__deleted
        if key_parts[0] == TOMBSTONE_ROOT
            && key_parts.len() >= 3
            && key_parts[key_parts.len() - 1] == TOMBSTONE_BLOB
        {
            let key_segments = &key_parts[1..key_parts.len() - 1];
            let key = match decode_key_path_segments(key_segments) {
                Ok(k) => k,
                Err(_) => continue,
            };
            let tombstone = match parse_tombstone_blob(content) {
                Some(t) => t,
                None => continue,
            };
            let entry_key = Key {
                target_type,
                target_value,
                key,
            };
            match parsed.tombstones.get(&entry_key) {
                Some(existing) if existing.timestamp >= tombstone.timestamp => {}
                _ => {
                    parsed.tombstones.insert(entry_key, tombstone);
                }
            }
            continue;
        }

        // Set member tombstone shape:
        //   .../<key segments...>/__tombstones/<member-id>
        if key_parts.len() >= 3 && key_parts[key_parts.len() - 2] == TOMBSTONE_ROOT {
            let key_segments = &key_parts[..key_parts.len() - 2];
            let key = match decode_key_path_segments(key_segments) {
                Ok(k) => k,
                Err(_) => continue,
            };
            let member_id = key_parts[key_parts.len() - 1].to_string();
            let content_str = String::from_utf8_lossy(content).to_string();
            let entry_key = (
                Key {
                    target_type,
                    target_value,
                    key,
                },
                member_id,
            );
            parsed.set_tombstones.insert(entry_key, content_str);
            continue;
        }

        // Set value shape:
        //   .../<key segments...>/__set/<member-id>
        if key_parts.len() >= 2 && key_parts[key_parts.len() - 2] == SET_VALUE_DIR {
            let key_segments = &key_parts[..key_parts.len() - 2];
            let key = match decode_key_path_segments(key_segments) {
                Ok(k) => k,
                Err(_) => continue,
            };
            let member_id = key_parts[key_parts.len() - 1].to_string();
            let content_str = String::from_utf8_lossy(content).to_string();
            let entry = parsed
                .values
                .entry(Key {
                    target_type,
                    target_value,
                    key,
                })
                .or_insert_with(|| TreeValue::Set(BTreeMap::new()));
            if let TreeValue::Set(ref mut set) = entry {
                set.insert(member_id, content_str);
            }
            continue;
        }

        // List entry tombstone shape:
        //   .../<key segments...>/__list/__tombstones/<entry_name>
        if key_parts.len() >= 4
            && key_parts[key_parts.len() - 3] == LIST_VALUE_DIR
            && key_parts[key_parts.len() - 2] == TOMBSTONE_ROOT
        {
            let key_segments = &key_parts[..key_parts.len() - 3];
            let key = match decode_key_path_segments(key_segments) {
                Ok(k) => k,
                Err(_) => continue,
            };
            let entry_name = key_parts[key_parts.len() - 1].to_string();
            let tombstone = match parse_tombstone_blob(content) {
                Some(t) => t,
                None => continue,
            };
            let entry_key = (
                Key {
                    target_type,
                    target_value,
                    key,
                },
                entry_name,
            );
            match parsed.list_tombstones.get(&entry_key) {
                Some(existing) if existing.timestamp >= tombstone.timestamp => {}
                _ => {
                    parsed.list_tombstones.insert(entry_key, tombstone);
                }
            }
            continue;
        }

        // List value shape:
        //   .../<key segments...>/__list/<timestamp-hash>
        if key_parts.len() >= 3
            && key_parts[key_parts.len() - 2] == LIST_VALUE_DIR
            && git_utils::is_list_entry_name(key_parts[key_parts.len() - 1])
        {
            let key_segments = &key_parts[..key_parts.len() - 2];
            let key = match decode_key_path_segments(key_segments) {
                Ok(k) => k,
                Err(_) => continue,
            };
            let entry_name = key_parts[key_parts.len() - 1].to_string();
            let content_str = String::from_utf8_lossy(content).to_string();
            let entry = parsed
                .values
                .entry(Key {
                    target_type,
                    target_value,
                    key,
                })
                .or_insert_with(|| TreeValue::List(Vec::new()));
            if let TreeValue::List(ref mut list) = entry {
                list.push((entry_name, content_str));
            }
            continue;
        }

        // String value shape:
        //   .../<key segments...>/__value
        if key_parts.len() >= 2 && key_parts[key_parts.len() - 1] == STRING_VALUE_BLOB {
            let key_segments = &key_parts[..key_parts.len() - 1];
            let key = match decode_key_path_segments(key_segments) {
                Ok(k) => k,
                Err(_) => continue,
            };
            let content_str = String::from_utf8_lossy(content).to_string();
            parsed.values.insert(
                Key {
                    target_type,
                    target_value,
                    key,
                },
                TreeValue::String(content_str),
            );
            continue;
        }
    }

    // Sort list entries by name (timestamp-hash)
    for value in parsed.values.values_mut() {
        if let TreeValue::List(ref mut list) = value {
            list.sort_by(|a, b| a.0.cmp(&b.0));
        }
    }

    // If both value and whole-key tombstone exist in one snapshot, value wins.
    parsed
        .tombstones
        .retain(|key, _| !parsed.values.contains_key(key));

    // If both set member value and tombstone exist in one snapshot, value wins.
    parsed
        .set_tombstones
        .retain(|(key, member_id), _| match parsed.values.get(key) {
            Some(TreeValue::Set(set)) => !set.contains_key(member_id),
            _ => true,
        });

    // If both list entry value and tombstone exist in one snapshot, value wins.
    parsed
        .list_tombstones
        .retain(|(key, entry_name), _| match parsed.values.get(key) {
            Some(TreeValue::List(list)) => !list.iter().any(|(name, _)| name == entry_name),
            _ => true,
        });

    Ok(parsed)
}

/// Parse a tombstone blob's JSON content into a [`Tombstone`].
fn parse_tombstone_blob(content: &[u8]) -> Option<Tombstone> {
    serde_json::from_slice(content).ok()
}

/// Recursively collect all blobs from a Git tree into a flat path map.
fn collect_blobs(
    repo: &gix::Repository,
    tree_id: gix::ObjectId,
    prefix: &str,
    paths: &mut BTreeMap<String, Vec<u8>>,
) -> Result<()> {
    let tree = repo
        .find_tree(tree_id)
        .map_err(|e| Error::Other(format!("{e}")))?;
    for entry in tree.iter() {
        let entry = entry.map_err(|e| Error::Other(format!("{e}")))?;
        let name = std::str::from_utf8(entry.filename())
            .map_err(|_| Error::Other("non-UTF8 tree entry".into()))?;
        let full_path = if prefix.is_empty() {
            name.to_string()
        } else {
            format!("{prefix}/{name}")
        };

        if entry.mode().is_blob() {
            let blob = repo
                .find_blob(entry.object_id())
                .map_err(|e| Error::Other(format!("{e}")))?;
            paths.insert(full_path, blob.data.to_vec());
        } else if entry.mode().is_tree() {
            collect_blobs(repo, entry.object_id(), &full_path, paths)?;
        }
    }
    Ok(())
}

/// Parse path segments into `(target_type, target_value, key_parts)`.
///
/// Handles different target layouts:
/// - `project/<key parts...>`
/// - `path/<encoded segments...>/__target__/<key parts...>`
/// - `<type>/<fanout>/<value>/<key parts...>` (sharded targets like commit, branch)
///
/// # Parameters
///
/// - `parts`: the slash-split path components
///
/// # Returns
///
/// A tuple of `(target_type, target_value, remaining_key_parts)`.
///
/// # Errors
///
/// Returns an error if the path is too short or missing required separators.
pub fn parse_path_parts<'a>(parts: &'a [&'a str]) -> Result<(TargetType, String, &'a [&'a str])> {
    if parts.is_empty() {
        return Err(Error::InvalidTreePath("empty path".into()));
    }

    let target_type_str = parts[0];
    let target_type: TargetType = target_type_str
        .parse()
        .map_err(|_| Error::InvalidTreePath(format!("unknown target type: {target_type_str:?}")))?;

    if target_type == TargetType::Project {
        return Ok((TargetType::Project, String::new(), &parts[1..]));
    }

    if target_type == TargetType::Path {
        let separator_index = parts
            .iter()
            .position(|part| *part == PATH_TARGET_SEPARATOR)
            .ok_or_else(|| {
                Error::InvalidTreePath(format!("path target missing separator: {parts:?}"))
            })?;

        if separator_index < 2 || separator_index + 1 >= parts.len() {
            return Err(Error::InvalidTreePath(format!(
                "invalid path target layout: {parts:?}"
            )));
        }

        let target_value = decode_path_target_segments(&parts[1..separator_index])?;
        return Ok((target_type, target_value, &parts[separator_index + 1..]));
    }

    if parts.len() < 4 {
        return Err(Error::InvalidTreePath(format!(
            "path too short for sharded target: {parts:?}"
        )));
    }

    let target_value = parts[2].to_string();
    Ok((target_type, target_value, &parts[3..]))
}

/// Build a Git tree from merged metadata values and tombstones.
///
/// Converts the in-memory representation of merged metadata back into
/// a Git tree structure suitable for committing.
///
/// # Parameters
///
/// - `repo`: the Git repository to write objects into
/// - `values`: merged metadata values
/// - `tombstones`: merged whole-key tombstones
/// - `set_tombstones`: merged set-member tombstones
/// - `list_tombstones`: merged list-entry tombstones
///
/// # Returns
///
/// The OID of the root Git tree object.
///
/// # Errors
///
/// Returns an error if target parsing or Git object writes fail.
pub fn build_merged_tree(
    repo: &gix::Repository,
    values: &BTreeMap<Key, TreeValue>,
    tombstones: &BTreeMap<Key, Tombstone>,
    set_tombstones: &BTreeMap<(Key, String), String>,
    list_tombstones: &BTreeMap<(Key, String), Tombstone>,
) -> Result<gix::ObjectId> {
    let mut files: BTreeMap<String, Vec<u8>> = BTreeMap::new();

    for (k, tree_val) in values {
        let target = k.to_target();

        match tree_val {
            TreeValue::String(s) => {
                let full_path = target.tree_path(&k.key)?;
                files.insert(full_path, s.as_bytes().to_vec());
            }
            TreeValue::List(list_entries) => {
                let list_dir_path = target.list_dir_path(&k.key)?;
                for (entry_name, content) in list_entries {
                    let full_path = format!("{}/{}", list_dir_path, entry_name);
                    files.insert(full_path, content.as_bytes().to_vec());
                }
            }
            TreeValue::Set(set_members) => {
                let set_dir_path = target.set_dir_path(&k.key)?;
                for (member_id, content) in set_members {
                    let full_path = format!("{}/{}", set_dir_path, member_id);
                    files.insert(full_path, content.as_bytes().to_vec());
                }
            }
        }
    }

    for (k, tombstone) in tombstones {
        let target = k.to_target();
        let full_path = target.tombstone_path(&k.key)?;
        let payload = serde_json::to_vec(&Tombstone {
            timestamp: tombstone.timestamp,
            email: tombstone.email.clone(),
        })?;
        files.insert(full_path, payload);
    }

    for ((k, member_id), tombstone_value) in set_tombstones {
        let target = k.to_target();
        let full_path = target.set_member_tombstone_path(&k.key, member_id)?;
        files.insert(full_path, tombstone_value.as_bytes().to_vec());
    }

    for ((k, entry_name), tombstone) in list_tombstones {
        let target = k.to_target();
        let full_path = target.list_entry_tombstone_path(&k.key, entry_name)?;
        let payload = serde_json::to_vec(&Tombstone {
            timestamp: tombstone.timestamp,
            email: tombstone.email.clone(),
        })?;
        files.insert(full_path, payload);
    }

    build_tree_from_paths(repo, &files)
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_path_parts_for_path_target() {
        let parts = [
            "path",
            "src",
            "~__generated",
            "file.rs",
            "__target__",
            "owner",
            "__value",
        ];
        let (target_type, target_value, key_parts) = parse_path_parts(&parts).unwrap();
        assert_eq!(target_type, TargetType::Path);
        assert_eq!(target_value, "src/__generated/file.rs");
        assert_eq!(key_parts, &["owner", "__value"]);
    }
}
