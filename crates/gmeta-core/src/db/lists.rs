use rusqlite::{params, OptionalExtension};

use crate::error::{Error, Result};

use super::{
    blob_if_large, load_list_entries_by_metadata_id, load_list_entries_by_metadata_id_tx,
    load_list_rows_by_metadata_id_tx, Db,
};
use crate::list_value::{encode_entries, ensure_unique_timestamp, ListEntry};
use crate::types::TargetType;

impl Db {
    /// Push a value onto a list. If the key is currently a string, convert to list first.
    pub fn list_push(
        &self,
        target_type: &TargetType,
        target_value: &str,
        key: &str,
        value: &str,
        email: &str,
        timestamp: i64,
    ) -> Result<()> {
        self.list_push_with_repo(
            None,
            target_type,
            target_value,
            key,
            value,
            email,
            timestamp,
        )
    }

    /// Push a value onto a list, storing large items as git blob refs.
    pub fn list_push_with_repo(
        &self,
        repo: Option<&gix::Repository>,
        target_type: &TargetType,
        target_value: &str,
        key: &str,
        value: &str,
        email: &str,
        timestamp: i64,
    ) -> Result<()> {
        let target_type_str = target_type.as_str();
        let tx = self.conn.unchecked_transaction()?;
        let existing = {
            let mut stmt = tx.prepare(
                "SELECT rowid, value, value_type FROM metadata
                 WHERE target_type = ?1 AND target_value = ?2 AND key = ?3",
            )?;

            stmt.query_row(params![target_type_str, target_value, key], |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                ))
            })
            .optional()?
        };

        let (metadata_id, mut entries) = match existing {
            Some((metadata_id, current_val, current_type)) => {
                if current_type == "list" {
                    let entries = load_list_entries_by_metadata_id_tx(&tx, metadata_id)?;
                    (metadata_id, entries)
                } else {
                    // Convert string to list
                    let current_str: String = serde_json::from_str(&current_val)?;
                    tx.execute(
                        "UPDATE metadata
                         SET value = '[]', value_type = 'list', last_timestamp = ?1
                         WHERE rowid = ?2",
                        params![timestamp, metadata_id],
                    )?;
                    tx.execute(
                        "DELETE FROM list_values WHERE metadata_id = ?1",
                        params![metadata_id],
                    )?;
                    tx.execute(
                        "INSERT INTO list_values (metadata_id, value, timestamp)
                         VALUES (?1, ?2, 0)",
                        params![metadata_id, current_str],
                    )?;
                    let entries = load_list_entries_by_metadata_id_tx(&tx, metadata_id)?;
                    (metadata_id, entries)
                }
            }
            None => {
                tx.execute(
                    "INSERT INTO metadata (target_type, target_value, key, value, value_type, last_timestamp)
                     VALUES (?1, ?2, ?3, '[]', 'list', ?4)",
                    params![target_type_str, target_value, key, timestamp],
                )?;
                let metadata_id = tx.last_insert_rowid();
                (metadata_id, Vec::new())
            }
        };

        let unique_timestamp = ensure_unique_timestamp(timestamp, &entries);
        let (stored_value, item_is_git_ref) = blob_if_large(repo, value)?;
        let new_entry = ListEntry {
            value: stored_value.clone(),
            timestamp: unique_timestamp,
        };
        tx.execute(
            "INSERT INTO list_values (metadata_id, value, timestamp, is_git_ref)
             VALUES (?1, ?2, ?3, ?4)",
            params![
                metadata_id,
                &stored_value,
                new_entry.timestamp,
                item_is_git_ref as i64
            ],
        )?;
        entries.push(new_entry);

        let new_value = encode_entries(&entries)?;

        tx.execute(
            "UPDATE metadata
             SET value = '[]', value_type = 'list', last_timestamp = ?1
             WHERE rowid = ?2",
            params![timestamp, metadata_id],
        )?;

        tx.execute(
            "INSERT INTO metadata_log (target_type, target_value, key, value, value_type, operation, email, timestamp)
             VALUES (?1, ?2, ?3, ?4, 'list', 'push', ?5, ?6)",
            params![target_type_str, target_value, key, &new_value, email, timestamp],
        )?;

        tx.execute(
            "DELETE FROM tombstones
             WHERE tombstone_type = 'metadata' AND target_type = ?1 AND target_value = ?2 AND key = ?3",
            params![target_type_str, target_value, key],
        )?;

        tx.commit()?;

        Ok(())
    }

    /// Pop a value from a list.
    pub fn list_pop(
        &self,
        target_type: &TargetType,
        target_value: &str,
        key: &str,
        value: &str,
        email: &str,
        timestamp: i64,
    ) -> Result<()> {
        let target_type_str = target_type.as_str();
        let tx = self.conn.unchecked_transaction()?;
        let existing = {
            let mut stmt = tx.prepare(
                "SELECT rowid, value_type FROM metadata
                 WHERE target_type = ?1 AND target_value = ?2 AND key = ?3",
            )?;

            stmt.query_row(params![target_type_str, target_value, key], |row| {
                Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
            })
            .optional()?
        };

        match existing {
            Some((metadata_id, current_type)) => {
                if current_type != "list" {
                    return Err(Error::TypeMismatch {
                        key: key.to_string(),
                        expected: "list".into(),
                    });
                }
                let mut list_rows = load_list_rows_by_metadata_id_tx(&tx, metadata_id)?;
                if let Some(pos) = list_rows.iter().rposition(|row| row.value == value) {
                    let removed = list_rows.remove(pos);
                    tx.execute(
                        "DELETE FROM list_values WHERE rowid = ?1",
                        params![removed.rowid],
                    )?;
                } else {
                    return Err(Error::ValueNotFound(format!("'{value}' not found in list")));
                }

                let list_entries: Vec<ListEntry> = list_rows
                    .iter()
                    .map(|row| ListEntry {
                        value: row.value.clone(),
                        timestamp: row.timestamp,
                    })
                    .collect();
                let new_value = encode_entries(&list_entries)?;

                tx.execute(
                    "UPDATE metadata
                     SET value = '[]', value_type = 'list', last_timestamp = ?1
                     WHERE rowid = ?2",
                    params![timestamp, metadata_id],
                )?;

                tx.execute(
                    "INSERT INTO metadata_log (target_type, target_value, key, value, value_type, operation, email, timestamp)
                     VALUES (?1, ?2, ?3, ?4, 'list', 'pop', ?5, ?6)",
                    params![target_type_str, target_value, key, &new_value, email, timestamp],
                )?;

                tx.execute(
                    "DELETE FROM tombstones
                     WHERE tombstone_type = 'metadata' AND target_type = ?1 AND target_value = ?2 AND key = ?3",
                    params![target_type_str, target_value, key],
                )?;

                tx.commit()?;

                Ok(())
            }
            None => Err(Error::KeyNotFound {
                key: key.to_string(),
            }),
        }
    }

    /// Get list entries for display (resolved values with timestamps).
    pub fn list_entries(
        &self,
        target_type: &TargetType,
        target_value: &str,
        key: &str,
    ) -> Result<Vec<ListEntry>> {
        let metadata_id = self
            .conn
            .query_row(
                "SELECT rowid, value_type FROM metadata
                 WHERE target_type = ?1 AND target_value = ?2 AND key = ?3",
                params![target_type.as_str(), target_value, key],
                |row| Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?)),
            )
            .optional()?;

        match metadata_id {
            Some((id, vtype)) => {
                if vtype != "list" {
                    return Err(Error::TypeMismatch {
                        key: key.to_string(),
                        expected: "list".into(),
                    });
                }
                load_list_entries_by_metadata_id(&self.conn, self.repo.as_ref(), id)
            }
            None => Err(Error::KeyNotFound {
                key: key.to_string(),
            }),
        }
    }

    /// Remove a list entry by index, creating a list tombstone for serialization.
    pub fn list_rm(
        &self,
        target_type: &TargetType,
        target_value: &str,
        key: &str,
        index: usize,
        email: &str,
        timestamp: i64,
    ) -> Result<()> {
        let target_type_str = target_type.as_str();
        let tx = self.conn.unchecked_transaction()?;
        let existing = {
            let mut stmt = tx.prepare(
                "SELECT rowid, value_type FROM metadata
                 WHERE target_type = ?1 AND target_value = ?2 AND key = ?3",
            )?;

            stmt.query_row(params![target_type_str, target_value, key], |row| {
                Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
            })
            .optional()?
        };

        match existing {
            Some((metadata_id, current_type)) => {
                if current_type != "list" {
                    return Err(Error::TypeMismatch {
                        key: key.to_string(),
                        expected: "list".into(),
                    });
                }
                let mut list_rows = load_list_rows_by_metadata_id_tx(&tx, metadata_id)?;
                if index >= list_rows.len() {
                    return Err(Error::IndexOutOfRange {
                        index,
                        size: list_rows.len(),
                    });
                }

                let removed = list_rows.remove(index);

                // Build the entry name used in git tree serialization
                let entry_name = crate::list_value::make_entry_name_from_parts(
                    removed.timestamp,
                    &removed.value,
                );

                tx.execute(
                    "DELETE FROM list_values WHERE rowid = ?1",
                    params![removed.rowid],
                )?;

                // Record a list tombstone so serialize propagates the deletion
                tx.execute(
                    "INSERT INTO tombstones (tombstone_type, target_type, target_value, key, entry_id, value, timestamp, email)
                     VALUES ('list_entry', ?1, ?2, ?3, ?4, '', ?5, ?6)
                     ON CONFLICT(tombstone_type, target_type, target_value, key, entry_id) DO UPDATE
                     SET timestamp = excluded.timestamp, email = excluded.email",
                    params![target_type_str, target_value, key, entry_name, timestamp, email],
                )?;

                let list_entries: Vec<ListEntry> = list_rows
                    .iter()
                    .map(|row| ListEntry {
                        value: row.value.clone(),
                        timestamp: row.timestamp,
                    })
                    .collect();
                let new_value = encode_entries(&list_entries)?;

                tx.execute(
                    "UPDATE metadata
                     SET value = '[]', value_type = 'list', last_timestamp = ?1
                     WHERE rowid = ?2",
                    params![timestamp, metadata_id],
                )?;

                tx.execute(
                    "INSERT INTO metadata_log (target_type, target_value, key, value, value_type, operation, email, timestamp)
                     VALUES (?1, ?2, ?3, ?4, 'list', 'list:rm', ?5, ?6)",
                    params![target_type_str, target_value, key, &new_value, email, timestamp],
                )?;

                tx.execute(
                    "DELETE FROM tombstones
                     WHERE tombstone_type = 'metadata' AND target_type = ?1 AND target_value = ?2 AND key = ?3",
                    params![target_type_str, target_value, key],
                )?;

                tx.commit()?;

                Ok(())
            }
            None => Err(Error::KeyNotFound {
                key: key.to_string(),
            }),
        }
    }
}
