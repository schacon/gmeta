use std::collections::{BTreeMap, BTreeSet};

use rusqlite::{params, OptionalExtension};

use crate::error::Result;
use crate::list_value::{encode_entries, parse_timestamp_from_entry_name, ListEntry};
use crate::tree::model::{Key, TombstoneEntry, TreeValue};
use crate::types::{set_member_id, TargetType, ValueType, GIT_REF_THRESHOLD};

use super::{encode_list_entries_by_metadata_id, encode_set_values_by_metadata_id, Db};

impl Db {
    /// Get all metadata entries (for serialization).
    /// Returns (target_type, target_value, key, value, value_type, last_timestamp, is_git_ref).
    pub fn get_all_metadata(
        &self,
    ) -> Result<Vec<(String, String, String, String, ValueType, i64, bool)>> {
        let mut stmt = self.conn.prepare(
            "SELECT rowid, target_type, target_value, key, value, value_type, last_timestamp, is_git_ref
             FROM metadata
             WHERE is_promised = 0
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
                target_type,
                target_value,
                key,
                value,
                value_type_str,
                last_timestamp,
                is_git_ref,
            ) = row?;
            let vt = ValueType::from_str(&value_type_str)?;
            match vt {
                ValueType::List => {
                    let encoded = encode_list_entries_by_metadata_id(
                        &self.conn,
                        self.repo.as_ref(),
                        metadata_id,
                    )?;
                    results.push((
                        target_type,
                        target_value,
                        key,
                        encoded,
                        vt,
                        last_timestamp,
                        false,
                    ));
                }
                ValueType::Set => {
                    let encoded = encode_set_values_by_metadata_id(&self.conn, metadata_id)?;
                    results.push((
                        target_type,
                        target_value,
                        key,
                        encoded,
                        vt,
                        last_timestamp,
                        false,
                    ));
                }
                ValueType::String => {
                    results.push((
                        target_type,
                        target_value,
                        key,
                        value,
                        vt,
                        last_timestamp,
                        is_git_ref,
                    ));
                }
            }
        }
        Ok(results)
    }

    /// Get entries modified since a given timestamp (for incremental serialization).
    pub fn get_modified_since(
        &self,
        since: i64,
    ) -> Result<Vec<(String, String, String, String, String, String)>> {
        let mut stmt = self.conn.prepare(
            "SELECT DISTINCT ml.target_type, ml.target_value, ml.key, ml.operation,
                    COALESCE(m.value, ''), COALESCE(m.value_type, '')
             FROM metadata_log ml
             LEFT JOIN metadata m ON ml.target_type = m.target_type
                 AND ml.target_value = m.target_value AND ml.key = m.key
             WHERE ml.timestamp > ?1
             ORDER BY ml.target_type, ml.target_value, ml.key",
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
            results.push(row?);
        }
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
    /// Large string values (exceeding [`GIT_REF_THRESHOLD`]) are stored as git blob
    /// references if a repository is attached to this `Db` instance.
    pub fn apply_tree(
        &self,
        values: &BTreeMap<Key, TreeValue>,
        tombstones: &BTreeMap<Key, TombstoneEntry>,
        set_tombstones: &BTreeMap<(Key, String), String>,
        list_tombstones: &BTreeMap<(Key, String), TombstoneEntry>,
        email: &str,
        now: i64,
    ) -> Result<()> {
        for ((target_type, target_value, key_name), tree_val) in values {
            let tt = TargetType::from_str(target_type)?;
            match tree_val {
                TreeValue::String(s) => {
                    if s.len() > GIT_REF_THRESHOLD {
                        if let Some(repo) = &self.repo {
                            let blob_oid = repo
                                .write_blob(s.as_bytes())
                                .map_err(|e| {
                                    crate::error::Error::Other(format!("failed to write blob: {e}"))
                                })?
                                .to_string();
                            let existing = self.get(&tt, target_value, key_name)?;
                            if existing.as_ref().map(|(v, _, _)| v.as_str()) != Some(&blob_oid) {
                                self.set_with_git_ref(
                                    None,
                                    &tt,
                                    target_value,
                                    key_name,
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
                        let existing = self.get(&tt, target_value, key_name)?;
                        if existing.as_ref().map(|(v, _, _)| v.as_str()) != Some(&json_val) {
                            self.set(
                                &tt,
                                target_value,
                                key_name,
                                &json_val,
                                &ValueType::String,
                                email,
                                now,
                            )?;
                        }
                    }
                }
                TreeValue::List(list_entries) => {
                    let key = (target_type.clone(), target_value.clone(), key_name.clone());
                    let tombstoned_names: BTreeSet<String> = list_tombstones
                        .iter()
                        .filter_map(|((k, entry_name), _)| {
                            if *k == key {
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
                    let existing = self.get(&tt, target_value, key_name)?;
                    if existing.as_ref().map(|(v, _, _)| v.as_str()) != Some(&json_val) {
                        self.set(
                            &tt,
                            target_value,
                            key_name,
                            &json_val,
                            &ValueType::List,
                            email,
                            now,
                        )?;
                    }
                }
                TreeValue::Set(set_members) => {
                    let key = (target_type.clone(), target_value.clone(), key_name.clone());
                    let tombstoned: BTreeSet<String> = set_tombstones
                        .iter()
                        .filter_map(|((k, member_id), _)| {
                            if *k == key {
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
                    let existing = self.get(&tt, target_value, key_name)?;
                    if existing.as_ref().map(|(v, _, _)| v.as_str()) != Some(&json_val) {
                        self.set(
                            &tt,
                            target_value,
                            key_name,
                            &json_val,
                            &ValueType::Set,
                            email,
                            now,
                        )?;
                    }
                }
            }
        }

        for (key, tombstone) in tombstones {
            if values.contains_key(key) {
                continue;
            }
            let tt = TargetType::from_str(&key.0)?;
            self.apply_tombstone(&tt, &key.1, &key.2, &tombstone.email, tombstone.timestamp)?;
        }

        Ok(())
    }
}
