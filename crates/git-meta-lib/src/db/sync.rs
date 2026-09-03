use std::collections::{BTreeMap, BTreeSet};

use rusqlite::{params, OptionalExtension};

use crate::error::Result;
use crate::list_value::{encode_entries, parse_timestamp_from_entry_name, ListEntry};
use crate::tree::model::{Key, Tombstone, TreeValue};
use crate::types::{set_member_id, TargetType, ValueType};

use super::types::Operation;

use super::{encode_list_entries_by_metadata_id, encode_set_values_by_metadata_id, Store};

impl Store {
    /// Columns every serializable-entry query selects, in the order
    /// [`Store::collect_serializable`] expects them.
    const SERIALIZABLE_SELECT: &'static str =
        "SELECT rowid, target_type, target_value, key, value, value_type, last_timestamp, is_git_ref
         FROM metadata
         WHERE is_promised = 0 AND source_ref IS NULL";

    /// Get all metadata entries (for serialization).
    pub fn get_all_metadata(&self) -> Result<Vec<super::types::SerializableEntry>> {
        let sql = format!(
            "{} ORDER BY target_type, target_value, key",
            Self::SERIALIZABLE_SELECT
        );
        let mut stmt = self.conn.prepare(&sql)?;
        let mut results = Vec::new();
        self.collect_serializable(&mut stmt, [], &mut results)?;
        Ok(results)
    }

    /// Get metadata entries for a specific set of targets.
    ///
    /// Incremental serialization only rebuilds the subtrees of targets that
    /// changed, so it only needs those targets' rows. Reading the whole table
    /// instead makes the cost of publishing one change scale with the total
    /// amount of metadata a repository holds.
    ///
    /// Each target is a `(type, value)` pair; project-scoped rows use an empty
    /// value. Results are ordered by target and key, as [`get_all_metadata`]
    /// orders them.
    ///
    /// [`get_all_metadata`]: Store::get_all_metadata
    pub fn get_metadata_for_targets(
        &self,
        targets: &[(TargetType, String)],
    ) -> Result<Vec<super::types::SerializableEntry>> {
        if targets.is_empty() {
            return Ok(Vec::new());
        }

        // Keep each statement's parameter count well inside SQLite's limit.
        const TARGETS_PER_QUERY: usize = 100;

        let mut results = Vec::new();
        for chunk in targets.chunks(TARGETS_PER_QUERY) {
            let placeholders = std::iter::repeat_n("(?,?)", chunk.len())
                .collect::<Vec<_>>()
                .join(",");
            let sql = format!(
                "{} AND (target_type, target_value) IN (VALUES {placeholders})",
                Self::SERIALIZABLE_SELECT
            );
            let mut stmt = self.conn.prepare(&sql)?;

            let mut bound: Vec<&dyn rusqlite::ToSql> = Vec::with_capacity(chunk.len() * 2);
            let type_names: Vec<&str> = chunk
                .iter()
                .map(|(target_type, _)| target_type.as_str())
                .collect();
            for (index, (_, target_value)) in chunk.iter().enumerate() {
                bound.push(&type_names[index]);
                bound.push(target_value);
            }

            self.collect_serializable(&mut stmt, bound.as_slice(), &mut results)?;
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

    /// Run a serializable-entry query and append its rows to `results`.
    ///
    /// List and set values are stored in side tables, so each row of those
    /// types is expanded here into the encoded form serialization expects.
    fn collect_serializable(
        &self,
        stmt: &mut rusqlite::Statement<'_>,
        params: impl rusqlite::Params,
        results: &mut Vec<super::types::SerializableEntry>,
    ) -> Result<()> {
        use super::types::SerializableEntry;

        let rows = stmt.query_map(params, |row| {
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

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use crate::db::Store;
    use crate::types::{MetaValue, Target, TargetType};

    fn store_with_entries() -> Store {
        let store = Store::open_in_memory().expect("open store");
        let targets = [
            Target::commit("00112233445566778899aabbccddeeff00112233").expect("commit target"),
            Target::commit("ffeeddccbbaa99887766554433221100ffeeddcc").expect("commit target"),
            Target::path("crates/lib/src/main.rs"),
            Target::branch("feature/x"),
        ];
        for (index, target) in targets.iter().enumerate() {
            store
                .set_value(
                    target,
                    "agent:model",
                    &MetaValue::String(format!("model-{index}")),
                    "test@example.com",
                    1_000 + index as i64,
                )
                .expect("set string");
            store
                .set_value(
                    target,
                    "review:owners",
                    &MetaValue::Set(["alice".to_string()].into_iter().collect()),
                    "test@example.com",
                    1_000 + index as i64,
                )
                .expect("set set");
        }
        store
    }

    #[test]
    fn scoped_read_matches_the_full_read_for_those_targets() {
        let store = store_with_entries();
        let wanted = [
            (
                TargetType::Commit,
                "00112233445566778899aabbccddeeff00112233".to_string(),
            ),
            (TargetType::Branch, "feature/x".to_string()),
        ];

        let scoped = store
            .get_metadata_for_targets(&wanted)
            .expect("scoped read");
        let expected: Vec<_> = store
            .get_all_metadata()
            .expect("full read")
            .into_iter()
            .filter(|entry| {
                wanted
                    .iter()
                    .any(|(t, v)| *t == entry.target_type && *v == entry.target_value)
            })
            .collect();

        assert_eq!(scoped.len(), expected.len());
        for (got, want) in scoped.iter().zip(expected.iter()) {
            assert_eq!(got.target_type, want.target_type);
            assert_eq!(got.target_value, want.target_value);
            assert_eq!(got.key, want.key);
            assert_eq!(
                got.value, want.value,
                "set and list values must be expanded"
            );
            assert_eq!(got.value_type, want.value_type);
            assert_eq!(got.last_timestamp, want.last_timestamp);
        }
        assert!(
            scoped.len() < store.get_all_metadata().expect("full read").len(),
            "the scoped read should be a strict subset"
        );
    }

    #[test]
    fn scoped_read_of_no_targets_is_empty() {
        let store = store_with_entries();
        assert!(store
            .get_metadata_for_targets(&[])
            .expect("scoped read")
            .is_empty());
    }

    #[test]
    fn scoped_read_spans_more_targets_than_one_query_holds() {
        let store = Store::open_in_memory().expect("open store");
        let mut wanted = Vec::new();
        for index in 0..250 {
            let value = format!("branch-{index}");
            let target = Target::branch(&value);
            store
                .set_value(
                    &target,
                    "agent:model",
                    &MetaValue::String("m".into()),
                    "test@example.com",
                    1_000,
                )
                .expect("set");
            wanted.push((TargetType::Branch, value));
        }

        let scoped = store
            .get_metadata_for_targets(&wanted)
            .expect("scoped read");
        assert_eq!(scoped.len(), 250, "chunked queries lost rows");
    }
}
