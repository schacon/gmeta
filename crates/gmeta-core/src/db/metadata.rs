use anyhow::{bail, Result};
use git2::Repository;
use rusqlite::{params, OptionalExtension};

use super::{
    blob_if_large, encode_list_entries_by_metadata_id, encode_set_values_by_metadata_id,
    escape_like_pattern, load_set_values_by_metadata_id_tx, normalize_set_values, resolve_blob, Db,
};
use crate::list_value::parse_entries;
use crate::types::{TargetType, ValueType};

impl Db {
    /// Set a value (upsert). JSON-encodes the value for storage.
    pub fn set(
        &self,
        target_type: &TargetType,
        target_value: &str,
        key: &str,
        value: &str,
        value_type: &ValueType,
        email: &str,
        timestamp: i64,
    ) -> Result<()> {
        self.set_with_git_ref(
            None,
            target_type,
            target_value,
            key,
            value,
            value_type,
            email,
            timestamp,
            false,
        )
    }

    /// Set a value (upsert) with optional git ref flag.
    /// When is_git_ref is true, value contains a git blob SHA instead of the actual content.
    /// For list values, repo is used to store large items as git blob refs.
    pub fn set_with_git_ref(
        &self,
        repo: Option<&Repository>,
        target_type: &TargetType,
        target_value: &str,
        key: &str,
        value: &str,
        value_type: &ValueType,
        email: &str,
        timestamp: i64,
        is_git_ref: bool,
    ) -> Result<()> {
        let target_type_str = target_type.as_str();
        let value_type_str = value_type.as_str();

        // Validate that string values are proper JSON strings (not raw objects/arrays)
        // Skip validation for git refs (they store a SHA, not JSON)
        if *value_type == ValueType::String && !is_git_ref {
            match serde_json::from_str::<serde_json::Value>(value) {
                Ok(v) if !v.is_string() => {
                    bail!(
                        "string value must be a JSON-encoded string (e.g. '\"hello\"'), \
                         got {} for key '{}'. Wrap with serde_json::to_string() first.",
                        if v.is_object() {
                            "an object"
                        } else if v.is_array() {
                            "an array"
                        } else if v.is_number() {
                            "a number"
                        } else if v.is_boolean() {
                            "a boolean"
                        } else {
                            "null"
                        },
                        key,
                    );
                }
                _ => {} // valid JSON string, or not valid JSON at all (legacy)
            }
        }

        let git_ref_val: i64 = if is_git_ref { 1 } else { 0 };
        let tx = self.conn.unchecked_transaction()?;
        match value_type {
            ValueType::String => {
                tx.execute(
                    "INSERT INTO metadata (target_type, target_value, key, value, value_type, last_timestamp, is_git_ref)
                     VALUES (?1, ?2, ?3, ?4, 'string', ?5, ?6)
                     ON CONFLICT(target_type, target_value, key) DO UPDATE
                     SET value = excluded.value, value_type = 'string', last_timestamp = excluded.last_timestamp, is_git_ref = excluded.is_git_ref, is_promised = 0",
                    params![target_type_str, target_value, key, value, timestamp, git_ref_val],
                )?;

                let metadata_id: i64 = tx.query_row(
                    "SELECT rowid FROM metadata WHERE target_type = ?1 AND target_value = ?2 AND key = ?3",
                    params![target_type_str, target_value, key],
                    |row| row.get(0),
                )?;
                tx.execute(
                    "DELETE FROM list_values WHERE metadata_id = ?1",
                    params![metadata_id],
                )?;
                tx.execute(
                    "DELETE FROM set_values WHERE metadata_id = ?1",
                    params![metadata_id],
                )?;
                tx.execute(
                    "DELETE FROM tombstones WHERE tombstone_type = 'set_member' AND target_type = ?1 AND target_value = ?2 AND key = ?3",
                    params![target_type_str, target_value, key],
                )?;
            }
            ValueType::List => {
                tx.execute(
                    "INSERT INTO metadata (target_type, target_value, key, value, value_type, last_timestamp, is_git_ref)
                     VALUES (?1, ?2, ?3, '[]', 'list', ?4, 0)
                     ON CONFLICT(target_type, target_value, key) DO UPDATE
                     SET value = '[]', value_type = 'list', last_timestamp = excluded.last_timestamp, is_git_ref = 0, is_promised = 0",
                    params![target_type_str, target_value, key, timestamp],
                )?;

                let metadata_id: i64 = tx.query_row(
                    "SELECT rowid FROM metadata WHERE target_type = ?1 AND target_value = ?2 AND key = ?3",
                    params![target_type_str, target_value, key],
                    |row| row.get(0),
                )?;

                tx.execute(
                    "DELETE FROM list_values WHERE metadata_id = ?1",
                    params![metadata_id],
                )?;
                tx.execute(
                    "DELETE FROM set_values WHERE metadata_id = ?1",
                    params![metadata_id],
                )?;
                tx.execute(
                    "DELETE FROM tombstones WHERE tombstone_type = 'set_member' AND target_type = ?1 AND target_value = ?2 AND key = ?3",
                    params![target_type_str, target_value, key],
                )?;

                for entry in parse_entries(value)? {
                    let (stored_value, item_is_git_ref) = blob_if_large(repo, &entry.value)?;
                    tx.execute(
                        "INSERT INTO list_values (metadata_id, value, timestamp, is_git_ref)
                         VALUES (?1, ?2, ?3, ?4)",
                        params![
                            metadata_id,
                            stored_value,
                            entry.timestamp,
                            item_is_git_ref as i64
                        ],
                    )?;
                }
            }
            ValueType::Set => {
                tx.execute(
                    "INSERT INTO metadata (target_type, target_value, key, value, value_type, last_timestamp, is_git_ref)
                     VALUES (?1, ?2, ?3, '[]', 'set', ?4, 0)
                     ON CONFLICT(target_type, target_value, key) DO UPDATE
                     SET value = '[]', value_type = 'set', last_timestamp = excluded.last_timestamp, is_git_ref = 0, is_promised = 0",
                    params![target_type_str, target_value, key, timestamp],
                )?;

                let metadata_id: i64 = tx.query_row(
                    "SELECT rowid FROM metadata WHERE target_type = ?1 AND target_value = ?2 AND key = ?3",
                    params![target_type_str, target_value, key],
                    |row| row.get(0),
                )?;

                let existing_members = load_set_values_by_metadata_id_tx(&tx, metadata_id)?;
                let new_members = normalize_set_values(value)?;
                let new_member_ids: std::collections::BTreeSet<String> = new_members
                    .iter()
                    .map(|member| crate::types::set_member_id(member))
                    .collect();

                tx.execute(
                    "DELETE FROM list_values WHERE metadata_id = ?1",
                    params![metadata_id],
                )?;

                for member in &new_members {
                    let member_id = crate::types::set_member_id(member);
                    let member_timestamp = existing_members
                        .get(&member_id)
                        .map(|(_, ts)| *ts)
                        .unwrap_or(timestamp);
                    tx.execute(
                        "INSERT INTO set_values (metadata_id, member_id, value, timestamp)
                         VALUES (?1, ?2, ?3, ?4)
                         ON CONFLICT(metadata_id, member_id) DO UPDATE SET value = excluded.value, timestamp = excluded.timestamp",
                        params![metadata_id, member_id, member, member_timestamp],
                    )?;
                    tx.execute(
                        "DELETE FROM tombstones WHERE tombstone_type = 'set_member' AND target_type = ?1 AND target_value = ?2 AND key = ?3 AND entry_id = ?4",
                        params![target_type_str, target_value, key, crate::types::set_member_id(member)],
                    )?;
                }

                for member_id in existing_members.keys() {
                    if !new_member_ids.contains(member_id) {
                        tx.execute(
                            "DELETE FROM set_values WHERE metadata_id = ?1 AND member_id = ?2",
                            params![metadata_id, member_id],
                        )?;
                        let member_value = existing_members
                            .get(member_id)
                            .map(|(value, _)| value.clone())
                            .unwrap_or_default();
                        tx.execute(
                            "INSERT INTO tombstones (tombstone_type, target_type, target_value, key, entry_id, value, timestamp, email)
                             VALUES ('set_member', ?1, ?2, ?3, ?4, ?5, ?6, ?7)
                             ON CONFLICT(tombstone_type, target_type, target_value, key, entry_id) DO UPDATE
                             SET value = excluded.value, timestamp = excluded.timestamp, email = excluded.email",
                            params![target_type_str, target_value, key, member_id, member_value, timestamp, email],
                        )?;
                    }
                }
            }
        }

        tx.execute(
            "INSERT INTO metadata_log (target_type, target_value, key, value, value_type, operation, email, timestamp)
             VALUES (?1, ?2, ?3, ?4, ?5, 'set', ?6, ?7)",
            params![target_type_str, target_value, key, value, value_type_str, email, timestamp],
        )?;

        tx.execute(
            "DELETE FROM tombstones WHERE tombstone_type = 'metadata' AND target_type = ?1 AND target_value = ?2 AND key = ?3",
            params![target_type_str, target_value, key],
        )?;

        tx.commit()?;

        Ok(())
    }

    /// Get a single value by exact key.
    /// Returns (value, value_type, is_git_ref).
    pub fn get(
        &self,
        target_type: &TargetType,
        target_value: &str,
        key: &str,
    ) -> Result<Option<(String, ValueType, bool)>> {
        let mut stmt = self.conn.prepare(
            "SELECT rowid, value, value_type, is_git_ref FROM metadata
             WHERE target_type = ?1 AND target_value = ?2 AND key = ?3",
        )?;

        let result = stmt
            .query_row(params![target_type.as_str(), target_value, key], |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, bool>(3)?,
                ))
            })
            .optional()?;

        match result {
            Some((metadata_id, _value, ref vt, _is_git_ref))
                if ValueType::from_str(vt)? == ValueType::List =>
            {
                Ok(Some((
                    encode_list_entries_by_metadata_id(
                        &self.conn,
                        self.repo.as_ref(),
                        metadata_id,
                    )?,
                    ValueType::List,
                    false,
                )))
            }
            Some((metadata_id, _value, ref vt, _is_git_ref))
                if ValueType::from_str(vt)? == ValueType::Set =>
            {
                Ok(Some((
                    encode_set_values_by_metadata_id(&self.conn, metadata_id)?,
                    ValueType::Set,
                    false,
                )))
            }
            Some((_, value, vt, is_git_ref)) => {
                let resolved = resolve_blob(self.repo.as_ref(), &value, is_git_ref)?;
                Ok(Some((resolved, ValueType::from_str(&vt)?, is_git_ref)))
            }
            None => Ok(None),
        }
    }

    /// Get all key/value pairs for a target, optionally filtered by key prefix.
    /// Returns (key, value, value_type, is_git_ref).
    /// Note: promised entries are excluded from this wrapper -- use get_all_with_target_prefix directly
    /// if you need to see them.
    pub fn get_all(
        &self,
        target_type: &TargetType,
        target_value: &str,
        key_prefix: Option<&str>,
    ) -> Result<Vec<(String, String, ValueType, bool)>> {
        Ok(self
            .get_all_with_target_prefix(target_type, target_value, false, key_prefix)?
            .into_iter()
            .filter(|(_, _, _, _, _, is_promised)| !is_promised)
            .map(|(_, key, value, value_type, is_git_ref, _)| (key, value, value_type, is_git_ref))
            .collect())
    }

    /// Get all key/value pairs for a target or subtree, optionally filtered by key prefix.
    /// Returns (target_value, key, value, value_type, is_git_ref, is_promised).
    pub fn get_all_with_target_prefix(
        &self,
        target_type: &TargetType,
        target_value: &str,
        include_target_subtree: bool,
        key_prefix: Option<&str>,
    ) -> Result<Vec<(String, String, String, ValueType, bool, bool)>> {
        let target_type_str = target_type.as_str();
        let escaped_target = escape_like_pattern(target_value);
        let target_like = format!("{}/%", escaped_target);

        let (sql, params_vec): (&str, Vec<Box<dyn rusqlite::types::ToSql>>) =
            match (include_target_subtree, key_prefix) {
                (false, Some(prefix)) => (
                    "SELECT rowid, target_value, key, value, value_type, is_git_ref, is_promised FROM metadata
                 WHERE target_type = ?1 AND target_value = ?2
                 AND (key = ?3 OR key LIKE ?4 ESCAPE '\\')
                 ORDER BY target_value, key",
                    vec![
                        Box::new(target_type_str.to_string()),
                        Box::new(target_value.to_string()),
                        Box::new(prefix.to_string()),
                        Box::new(format!("{}:%", escape_like_pattern(prefix))),
                    ],
                ),
                (false, None) => (
                    "SELECT rowid, target_value, key, value, value_type, is_git_ref, is_promised FROM metadata
                 WHERE target_type = ?1 AND target_value = ?2
                 ORDER BY target_value, key",
                    vec![
                        Box::new(target_type_str.to_string()),
                        Box::new(target_value.to_string()),
                    ],
                ),
                (true, Some(prefix)) => (
                    "SELECT rowid, target_value, key, value, value_type, is_git_ref, is_promised FROM metadata
                 WHERE target_type = ?1 AND (target_value = ?2 OR target_value LIKE ?3 ESCAPE '\\')
                 AND (key = ?4 OR key LIKE ?5 ESCAPE '\\')
                 ORDER BY target_value, key",
                    vec![
                        Box::new(target_type_str.to_string()),
                        Box::new(target_value.to_string()),
                        Box::new(target_like),
                        Box::new(prefix.to_string()),
                        Box::new(format!("{}:%", escape_like_pattern(prefix))),
                    ],
                ),
                (true, None) => (
                    "SELECT rowid, target_value, key, value, value_type, is_git_ref, is_promised FROM metadata
                 WHERE target_type = ?1 AND (target_value = ?2 OR target_value LIKE ?3 ESCAPE '\\')
                 ORDER BY target_value, key",
                    vec![
                        Box::new(target_type_str.to_string()),
                        Box::new(target_value.to_string()),
                        Box::new(target_like),
                    ],
                ),
            };

        let mut stmt = self.conn.prepare(sql)?;
        let params_refs: Vec<&dyn rusqlite::types::ToSql> =
            params_vec.iter().map(|p| p.as_ref()).collect();
        let rows = stmt.query_map(params_refs.as_slice(), |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, String>(4)?,
                row.get::<_, bool>(5)?,
                row.get::<_, bool>(6)?,
            ))
        })?;

        let mut results = Vec::new();
        for row in rows {
            let (metadata_id, target_value, key, value, value_type_str, is_git_ref, is_promised) =
                row?;
            let vt = ValueType::from_str(&value_type_str)?;
            if is_promised {
                results.push((target_value, key, value, vt, false, true));
            } else {
                match vt {
                    ValueType::List => {
                        let encoded = encode_list_entries_by_metadata_id(
                            &self.conn,
                            self.repo.as_ref(),
                            metadata_id,
                        )?;
                        results.push((target_value, key, encoded, vt, false, false));
                    }
                    ValueType::Set => {
                        let encoded = encode_set_values_by_metadata_id(&self.conn, metadata_id)?;
                        results.push((target_value, key, encoded, vt, false, false));
                    }
                    ValueType::String => {
                        let resolved = resolve_blob(self.repo.as_ref(), &value, is_git_ref)?;
                        results.push((target_value, key, resolved, vt, is_git_ref, false));
                    }
                }
            }
        }
        Ok(results)
    }

    /// Get authorship info for a key from the log (most recent entry).
    pub fn get_authorship(
        &self,
        target_type: &TargetType,
        target_value: &str,
        key: &str,
    ) -> Result<Option<(String, i64)>> {
        let mut stmt = self.conn.prepare(
            "SELECT email, timestamp FROM metadata_log
             WHERE target_type = ?1 AND target_value = ?2 AND key = ?3
             ORDER BY timestamp DESC LIMIT 1",
        )?;

        let result = stmt
            .query_row(params![target_type.as_str(), target_value, key], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
            })
            .optional()?;

        Ok(result)
    }

    /// Remove a key.
    pub fn rm(
        &self,
        target_type: &TargetType,
        target_value: &str,
        key: &str,
        email: &str,
        timestamp: i64,
    ) -> Result<bool> {
        let target_type_str = target_type.as_str();
        let tx = self.conn.unchecked_transaction()?;

        let metadata_id = tx
            .query_row(
                "SELECT rowid FROM metadata
                 WHERE target_type = ?1 AND target_value = ?2 AND key = ?3",
                params![target_type_str, target_value, key],
                |row| row.get::<_, i64>(0),
            )
            .optional()?;

        let deleted = if let Some(metadata_id) = metadata_id {
            tx.execute(
                "DELETE FROM list_values WHERE metadata_id = ?1",
                params![metadata_id],
            )?;
            tx.execute(
                "DELETE FROM set_values WHERE metadata_id = ?1",
                params![metadata_id],
            )?;
            tx.execute(
                "DELETE FROM metadata WHERE rowid = ?1",
                params![metadata_id],
            )?
        } else {
            0
        };

        if deleted > 0 {
            tx.execute(
                "INSERT INTO tombstones (tombstone_type, target_type, target_value, key, entry_id, value, timestamp, email)
                 VALUES ('metadata', ?1, ?2, ?3, '', '', ?4, ?5)
                 ON CONFLICT(tombstone_type, target_type, target_value, key, entry_id) DO UPDATE
                 SET timestamp = excluded.timestamp, email = excluded.email",
                params![target_type_str, target_value, key, timestamp, email],
            )?;

            // Clear per-entry list tombstones -- the whole-key tombstone supersedes them
            tx.execute(
                "DELETE FROM tombstones
                 WHERE tombstone_type = 'list_entry' AND target_type = ?1 AND target_value = ?2 AND key = ?3",
                params![target_type_str, target_value, key],
            )?;

            tx.execute(
                "INSERT INTO metadata_log (target_type, target_value, key, value, value_type, operation, email, timestamp)
                 VALUES (?1, ?2, ?3, '', '', 'rm', ?4, ?5)",
                params![target_type_str, target_value, key, email, timestamp],
            )?;
        }

        tx.commit()?;

        Ok(deleted > 0)
    }
}
