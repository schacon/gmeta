use std::collections::{BTreeMap, BTreeSet};

use rusqlite::{params, OptionalExtension};

use crate::error::Result;
use crate::list_value::{encode_entries, parse_timestamp_from_entry_name, ListEntry};
use crate::tree::model::{Key, Tombstone, TreeValue};
use crate::types::{set_member_id, TargetType, ValueType};

use super::types::Operation;

use super::{encode_list_entries_by_metadata_id, encode_set_values_by_metadata_id, Store};

impl Store {
    /// Get all metadata entries (for serialization).
    pub fn get_all_metadata(&self) -> Result<Vec<super::types::SerializableEntry>> {
        use super::types::SerializableEntry;
        let mut stmt = self.conn.prepare(
            "SELECT rowid, target_type, target_value, key, value, value_type, last_timestamp, is_git_ref
             FROM metadata
             WHERE is_promised = 0 AND source_ref IS NULL
             ORDER BY target_type, target_value, key",
        )?;

        let rows = stmt.query_map([], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, String>(4)?,
                row.get::<_, String>(5)?,
                row.get::<_, i64>(6)?,
                row.get::<_, bool>(7)?,
            ))
        })?;

        let mut results = Vec::new();
        for row in rows {
            let (
                metadata_id,
                target_type_str,
                target_value,
                key,
                value,
                value_type_str,
                last_timestamp,
                is_git_ref,
            ) = row?;
            let target_type = target_type_str.parse::<TargetType>()?;
            let value_type = value_type_str.parse::<ValueType>()?;
            let (value, is_git_ref) = match value_type {
                ValueType::List => (
                    encode_list_entries_by_metadata_id(
                        &self.conn,
                        self.repo.as_ref(),
                        metadata_id,
                    )?,
                    false,
                ),
                ValueType::Set => (
                    encode_set_values_by_metadata_id(&self.conn, metadata_id)?,
                    false,
                ),
                ValueType::String => (value, is_git_ref),
            };
            results.push(SerializableEntry {
                target_type,
                target_value,
                key,
                value,
                value_type,
                last_timestamp,
                is_git_ref,
            });
        }
        Ok(())
    }

    /// Get entries modified since a given timestamp (for incremental serialization).
    ///
    /// The results are sorted by target and key, but deliberately not by SQL:
    /// an `ORDER BY` on the log's lookup index is cheaper for the planner than
    /// a timestamp seek plus a sort, so adding one makes SQLite scan the whole
    /// of `metadata_log` instead of seeking into it. Sorting the (small)
    /// modified set here keeps the seek and the ordering both.
    pub fn get_modified_since(&self, since: i64) -> Result<Vec<super::types::ModifiedEntry>> {
        use super::types::ModifiedEntry;
        let mut stmt = self.conn.prepare(
            "SELECT DISTINCT ml.target_type, ml.target_value, ml.key, ml.operation,
                    COALESCE(m.value, ''), COALESCE(m.value_type, '')
             FROM metadata_log ml
             LEFT JOIN metadata m ON ml.target_type = m.target_type
                 AND ml.target_value = m.target_value AND ml.key = m.key
             WHERE ml.timestamp > ?1",
        )?;

        let rows = stmt.query_map(params![since], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, String>(4)?,
                row.get::<_, String>(5)?,
            ))
        })?;

        let mut results = Vec::new();
        for row in rows {
            let (target_type_str, target_value, key, operation_str, value, value_type_str) = row?;
            let target_type = target_type_str.parse::<TargetType>()?;
            let operation = operation_str.parse::<Operation>()?;
            let value_type = if value_type_str.is_empty() {
                None
            } else {
                Some(value_type_str.parse::<ValueType>()?)
            };
            results.push(ModifiedEntry {
                target_type,
                target_value,
                key,
                operation,
                value,
                value_type,
            });
        }
        results.sort_by(|a, b| {
            (a.target_type.as_str(), &a.target_value, &a.key).cmp(&(
                b.target_type.as_str(),
                &b.target_value,
                &b.key,
            ))
        });
        Ok(results)
    }

    /// Get the last materialized timestamp.
    pub fn get_last_materialized(&self) -> Result<Option<i64>> {
        let mut stmt = self
            .conn
            .prepare("SELECT last_materialized FROM sync_state WHERE id = 1")?;
        let result = stmt
            .query_row([], |row| row.get::<_, Option<i64>>(0))
            .optional()?;
        Ok(result.flatten())
    }

    /// Set the last materialized timestamp.
    pub fn set_last_materialized(&self, timestamp: i64) -> Result<()> {
        self.conn.execute(
            "UPDATE sync_state SET last_materialized = ?1 WHERE id = 1",
            params![timestamp],
        )?;
        Ok(())
    }

    /// Apply parsed tree data to the database.
    ///
    /// Takes the structured output of [`crate::tree::format::parse_tree`] and writes it
    /// to SQLite: string/list/set values are upserted, tombstones are applied for keys
    /// that exist only in the tombstone map. List entries and set members that have
    /// corresponding tombstones are filtered out before writing.
    ///
    /// Large string values (exceeding [`Store::object_max_size`]) are stored as git
    /// blob references if a repository is attached to this `Store` instance.
    pub fn apply_tree(
        &self,
        values: &BTreeMap<Key, TreeValue>,
        tombstones: &BTreeMap<Key, Tombstone>,
        set_tombstones: &BTreeMap<(Key, String), String>,
        list_tombstones: &BTreeMap<(Key, String), Tombstone>,
        email: &str,
        now: i64,
    ) -> Result<()> {
        let object_max_size = self.object_max_size()?;
        for (k, tree_val) in values {
            let target = k.to_target();
            match tree_val {
                TreeValue::String(s) => {
                    if s.len() > object_max_size {
                        if let Some(repo) = &self.repo {
                            let existing = self.get(&target, &k.key)?;
                            let unchanged = existing.as_ref().is_some_and(|entry| {
                                entry.value_type == ValueType::String && entry.value == s.as_str()
                            });
                            if !unchanged {
                                let blob_oid = repo
                                    .write_blob(s.as_bytes())
                                    .map_err(|e| {
                                        crate::error::Error::Other(format!(
                                            "failed to write blob: {e}"
                                        ))
                                    })?
                                    .to_string();
                                self.set_with_git_ref(
                                    &target,
                                    &k.key,
                                    &blob_oid,
                                    &ValueType::String,
                                    email,
                                    now,
                                    true,
                                )?;
                            }
                        }
                    } else {
                        let json_val = serde_json::to_string(s)?;
                        let existing = self.get(&target, &k.key)?;
                        if existing.as_ref().map(|e| e.value.as_str()) != Some(&json_val) {
                            self.set(&target, &k.key, &json_val, &ValueType::String, email, now)?;
                        }
                    }
                }
                TreeValue::List(list_entries) => {
                    let tombstoned_names: BTreeSet<String> = list_tombstones
                        .iter()
                        .filter_map(|((tk, entry_name), _)| {
                            if *tk == *k {
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
                        let timestamp = parse_timestamp_from_entry_name(entry_name)
                            .unwrap_or(items.len() as i64);
                        items.push(ListEntry {
                            value: content.clone(),
                            timestamp,
                        });
                    }
                    let json_val = encode_entries(&items)?;
                    let existing = self.get(&target, &k.key)?;
                    if existing.as_ref().map(|e| e.value.as_str()) != Some(&json_val)
                        || self.list_has_git_ref_entries(k)?
                    {
                        self.set(&target, &k.key, &json_val, &ValueType::List, email, now)?;
                    }
                }
                TreeValue::Set(set_members) => {
                    let tombstoned: BTreeSet<String> = set_tombstones
                        .iter()
                        .filter_map(|((tk, member_id), _)| {
                            if *tk == *k {
                                Some(member_id.clone())
                            } else {
                                None
                            }
                        })
                        .collect();
                    let mut visible: Vec<String> = set_members
                        .values()
                        .filter(|member| !tombstoned.contains(&set_member_id(member)))
                        .cloned()
                        .collect();
                    visible.sort();
                    let json_val = serde_json::to_string(&visible)?;
                    let existing = self.get(&target, &k.key)?;
                    if existing.as_ref().map(|e| e.value.as_str()) != Some(&json_val) {
                        self.set(&target, &k.key, &json_val, &ValueType::Set, email, now)?;
                    }
                }
            }
        }

        for (key, tombstone) in tombstones {
            if values.contains_key(key) {
                continue;
            }
            let target = key.to_target();
            self.apply_tombstone(&target, &key.key, &tombstone.email, tombstone.timestamp)?;
        }

        Ok(())
    }

    /// Apply a non-primary metadata ref as imported local data.
    ///
    /// Imported rows are readable from SQLite, keep the source commit timestamp,
    /// and are excluded from normal serialization until a local write touches
    /// the same key and clears `source_ref`.
    pub fn apply_tree_from_source_ref(
        &self,
        values: &BTreeMap<Key, TreeValue>,
        source_ref: &str,
        source_timestamp: i64,
    ) -> Result<usize> {
        let sp = self.savepoint()?;
        let mut changes = 0;
        for (key, value) in values {
            if !self.should_apply_source_value(key, source_ref)? {
                continue;
            }
            self.apply_source_value(key, value, source_ref, source_timestamp)?;
            changes += 1;
        }
        sp.commit()?;
        Ok(changes)
    }

    fn should_apply_source_value(&self, key: &Key, source_ref: &str) -> Result<bool> {
        let existing: Option<Option<String>> = self
            .conn
            .query_row(
                "SELECT source_ref FROM metadata
                 WHERE target_type = ?1 AND target_value = ?2 AND key = ?3",
                params![key.target_type.as_str(), &key.target_value, &key.key],
                |row| row.get(0),
            )
            .optional()?;
        Ok(match existing {
            None => true,
            Some(Some(existing_source)) => existing_source == source_ref,
            Some(None) => false,
        })
    }

    fn apply_source_value(
        &self,
        key: &Key,
        value: &TreeValue,
        source_ref: &str,
        source_timestamp: i64,
    ) -> Result<()> {
        let target_type = key.target_type.as_str();
        match value {
            TreeValue::String(content) => {
                let json_value = serde_json::to_string(content)?;
                self.conn.execute(
                    "INSERT INTO metadata (target_type, target_value, key, value, value_type, last_timestamp, is_git_ref, is_promised, source_ref)
                     VALUES (?1, ?2, ?3, ?4, 'string', ?5, 0, 0, ?6)
                     ON CONFLICT(target_type, target_value, key) DO UPDATE
                     SET value = excluded.value, value_type = 'string', last_timestamp = excluded.last_timestamp,
                         is_git_ref = 0, is_promised = 0, source_ref = excluded.source_ref",
                    params![target_type, &key.target_value, &key.key, json_value, source_timestamp, source_ref],
                )?;
                let metadata_id = self.metadata_rowid(key)?;
                self.conn.execute(
                    "DELETE FROM list_values WHERE metadata_id = ?1",
                    params![metadata_id],
                )?;
                self.conn.execute(
                    "DELETE FROM set_values WHERE metadata_id = ?1",
                    params![metadata_id],
                )?;
            }
            TreeValue::List(entries) => {
                self.conn.execute(
                    "INSERT INTO metadata (target_type, target_value, key, value, value_type, last_timestamp, is_git_ref, is_promised, source_ref)
                     VALUES (?1, ?2, ?3, '[]', 'list', ?4, 0, 0, ?5)
                     ON CONFLICT(target_type, target_value, key) DO UPDATE
                     SET value = '[]', value_type = 'list', last_timestamp = excluded.last_timestamp,
                         is_git_ref = 0, is_promised = 0, source_ref = excluded.source_ref",
                    params![target_type, &key.target_value, &key.key, source_timestamp, source_ref],
                )?;
                let metadata_id = self.metadata_rowid(key)?;
                self.conn.execute(
                    "DELETE FROM list_values WHERE metadata_id = ?1",
                    params![metadata_id],
                )?;
                self.conn.execute(
                    "DELETE FROM set_values WHERE metadata_id = ?1",
                    params![metadata_id],
                )?;
                for (entry_name, content) in entries {
                    let timestamp =
                        parse_timestamp_from_entry_name(entry_name).unwrap_or(source_timestamp);
                    self.conn.execute(
                        "INSERT INTO list_values (metadata_id, value, timestamp, is_git_ref)
                         VALUES (?1, ?2, ?3, 0)",
                        params![metadata_id, content, timestamp],
                    )?;
                }
            }
            TreeValue::Set(members) => {
                self.conn.execute(
                    "INSERT INTO metadata (target_type, target_value, key, value, value_type, last_timestamp, is_git_ref, is_promised, source_ref)
                     VALUES (?1, ?2, ?3, '[]', 'set', ?4, 0, 0, ?5)
                     ON CONFLICT(target_type, target_value, key) DO UPDATE
                     SET value = '[]', value_type = 'set', last_timestamp = excluded.last_timestamp,
                         is_git_ref = 0, is_promised = 0, source_ref = excluded.source_ref",
                    params![target_type, &key.target_value, &key.key, source_timestamp, source_ref],
                )?;
                let metadata_id = self.metadata_rowid(key)?;
                self.conn.execute(
                    "DELETE FROM list_values WHERE metadata_id = ?1",
                    params![metadata_id],
                )?;
                self.conn.execute(
                    "DELETE FROM set_values WHERE metadata_id = ?1",
                    params![metadata_id],
                )?;
                for member in members.values() {
                    self.conn.execute(
                        "INSERT INTO set_values (metadata_id, member_id, value, timestamp)
                         VALUES (?1, ?2, ?3, ?4)
                         ON CONFLICT(metadata_id, member_id) DO UPDATE
                         SET value = excluded.value, timestamp = excluded.timestamp",
                        params![metadata_id, set_member_id(member), member, source_timestamp],
                    )?;
                }
            }
        }
        Ok(())
    }

    fn metadata_rowid(&self, key: &Key) -> Result<i64> {
        Ok(self.conn.query_row(
            "SELECT rowid FROM metadata WHERE target_type = ?1 AND target_value = ?2 AND key = ?3",
            params![key.target_type.as_str(), &key.target_value, &key.key],
            |row| row.get(0),
        )?)
    }

    fn list_has_git_ref_entries(&self, key: &Key) -> Result<bool> {
        let exists = self.conn.query_row(
            "SELECT EXISTS(
                SELECT 1
                FROM metadata
                JOIN list_values ON list_values.metadata_id = metadata.rowid
                WHERE metadata.target_type = ?1
                  AND metadata.target_value = ?2
                  AND metadata.key = ?3
                  AND list_values.is_git_ref = 1
            )",
            params![key.target_type.as_str(), &key.target_value, &key.key],
            |row| row.get(0),
        )?;
        Ok(exists)
    }
}
