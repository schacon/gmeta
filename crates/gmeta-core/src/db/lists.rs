use rusqlite::{params, OptionalExtension};

use crate::error::{Error, Result};

use super::{
    blob_if_large, load_list_entries_by_metadata_id, load_list_entries_by_metadata_id_tx,
    load_list_rows_by_metadata_id_tx, Store,
};
use crate::list_value::{encode_entries, ensure_unique_timestamp, ListEntry};
use crate::types::{validate_key, Target};

impl Store {
    /// Push a value onto a list. If the key is currently a string, convert to list first.
    ///
    /// # Parameters
    ///
    /// - `target`: the metadata target
    /// - `key`: the metadata key name
    /// - `value`: the value to push
    /// - `email`: the email of the user performing the operation
    /// - `timestamp`: the operation timestamp (milliseconds since epoch)
    pub fn list_push(
        &self,
        target: &Target,
        key: &str,
        value: &str,
        email: &str,
        timestamp: i64,
    ) -> Result<()> {
        self.list_push_with_repo(None, target, key, value, email, timestamp)
    }

    /// Push a value onto a list, storing large items as git blob refs.
    ///
    /// # Parameters
    ///
    /// - `repo`: optional git repository for storing large items as blob refs
    /// - `target`: the metadata target
    /// - `key`: the metadata key name
    /// - `value`: the value to push
    /// - `email`: the email of the user performing the operation
    /// - `timestamp`: the operation timestamp (milliseconds since epoch)
    pub fn list_push_with_repo(
        &self,
        repo: Option<&gix::Repository>,
        target: &Target,
        key: &str,
        value: &str,
        email: &str,
        timestamp: i64,
    ) -> Result<()> {
        validate_key(key)?;
        let target_type_str = target.target_type().as_str();
        let target_value = target.value().unwrap_or("");
        let sp = self.savepoint()?;
        let existing = {
            let mut stmt = self.conn.prepare(
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
                    let entries = load_list_entries_by_metadata_id_tx(&self.conn, metadata_id)?;
                    (metadata_id, entries)
                } else {
                    // Convert string to list
                    let current_str: String = serde_json::from_str(&current_val)?;
                    self.conn.execute(
                        "UPDATE metadata
                         SET value = '[]', value_type = 'list', last_timestamp = ?1
                         WHERE rowid = ?2",
                        params![timestamp, metadata_id],
                    )?;
                    self.conn.execute(
                        "DELETE FROM list_values WHERE metadata_id = ?1",
                        params![metadata_id],
                    )?;
                    self.conn.execute(
                        "INSERT INTO list_values (metadata_id, value, timestamp)
                         VALUES (?1, ?2, 0)",
                        params![metadata_id, current_str],
                    )?;
                    let entries = load_list_entries_by_metadata_id_tx(&self.conn, metadata_id)?;
                    (metadata_id, entries)
                }
            }
            None => {
                self.conn.execute(
                    "INSERT INTO metadata (target_type, target_value, key, value, value_type, last_timestamp)
                     VALUES (?1, ?2, ?3, '[]', 'list', ?4)",
                    params![target_type_str, target_value, key, timestamp],
                )?;
                let metadata_id = self.conn.last_insert_rowid();
                (metadata_id, Vec::new())
            }
        };

        let unique_timestamp = ensure_unique_timestamp(timestamp, &entries);
        let (stored_value, item_is_git_ref) = blob_if_large(repo, value)?;
        let new_entry = ListEntry {
            value: stored_value.clone(),
            timestamp: unique_timestamp,
        };
        self.conn.execute(
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

        self.conn.execute(
            "UPDATE metadata
             SET value = '[]', value_type = 'list', last_timestamp = ?1
             WHERE rowid = ?2",
            params![timestamp, metadata_id],
        )?;

        self.conn.execute(
            "INSERT INTO metadata_log (target_type, target_value, key, value, value_type, operation, email, timestamp)
             VALUES (?1, ?2, ?3, ?4, 'list', 'push', ?5, ?6)",
            params![target_type_str, target_value, key, &new_value, email, timestamp],
        )?;

        self.conn.execute(
            "DELETE FROM tombstones
             WHERE tombstone_type = 'metadata' AND target_type = ?1 AND target_value = ?2 AND key = ?3",
            params![target_type_str, target_value, key],
        )?;

        sp.commit()?;

        Ok(())
    }

    /// Pop a value from a list.
    ///
    /// # Parameters
    ///
    /// - `target`: the metadata target
    /// - `key`: the metadata key name
    /// - `value`: the value to pop (removed by matching)
    /// - `email`: the email of the user performing the operation
    /// - `timestamp`: the operation timestamp (milliseconds since epoch)
    pub fn list_pop(
        &self,
        target: &Target,
        key: &str,
        value: &str,
        email: &str,
        timestamp: i64,
    ) -> Result<()> {
        validate_key(key)?;
        let target_type_str = target.target_type().as_str();
        let target_value = target.value().unwrap_or("");
        let sp = self.savepoint()?;
        let existing = {
            let mut stmt = self.conn.prepare(
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
                let mut list_rows = load_list_rows_by_metadata_id_tx(&self.conn, metadata_id)?;
                if let Some(pos) = list_rows.iter().rposition(|row| row.value == value) {
                    let removed = list_rows.remove(pos);
                    self.conn.execute(
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

                self.conn.execute(
                    "UPDATE metadata
                     SET value = '[]', value_type = 'list', last_timestamp = ?1
                     WHERE rowid = ?2",
                    params![timestamp, metadata_id],
                )?;

                self.conn.execute(
                    "INSERT INTO metadata_log (target_type, target_value, key, value, value_type, operation, email, timestamp)
                     VALUES (?1, ?2, ?3, ?4, 'list', 'pop', ?5, ?6)",
                    params![target_type_str, target_value, key, &new_value, email, timestamp],
                )?;

                self.conn.execute(
                    "DELETE FROM tombstones
                     WHERE tombstone_type = 'metadata' AND target_type = ?1 AND target_value = ?2 AND key = ?3",
                    params![target_type_str, target_value, key],
                )?;

                sp.commit()?;

                Ok(())
            }
            None => Err(Error::KeyNotFound {
                key: key.to_string(),
            }),
        }
    }

    /// Get list entries for display (resolved values with timestamps).
    ///
    /// # Parameters
    ///
    /// - `target`: the metadata target
    /// - `key`: the metadata key name
    pub fn list_entries(&self, target: &Target, key: &str) -> Result<Vec<ListEntry>> {
        let target_type_str = target.target_type().as_str();
        let target_value = target.value().unwrap_or("");
        let metadata_id = self
            .conn
            .query_row(
                "SELECT rowid, value_type FROM metadata
                 WHERE target_type = ?1 AND target_value = ?2 AND key = ?3",
                params![target_type_str, target_value, key],
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
    ///
    /// # Parameters
    ///
    /// - `target`: the metadata target
    /// - `key`: the metadata key name
    /// - `index`: the zero-based index of the entry to remove
    /// - `email`: the email of the user performing the operation
    /// - `timestamp`: the operation timestamp (milliseconds since epoch)
    pub fn list_remove(
        &self,
        target: &Target,
        key: &str,
        index: usize,
        email: &str,
        timestamp: i64,
    ) -> Result<()> {
        validate_key(key)?;
        let target_type_str = target.target_type().as_str();
        let target_value = target.value().unwrap_or("");
        let sp = self.savepoint()?;
        let existing = {
            let mut stmt = self.conn.prepare(
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
                let mut list_rows = load_list_rows_by_metadata_id_tx(&self.conn, metadata_id)?;
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

                self.conn.execute(
                    "DELETE FROM list_values WHERE rowid = ?1",
                    params![removed.rowid],
                )?;

                // Record a list tombstone so serialize propagates the deletion
                self.conn.execute(
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

                self.conn.execute(
                    "UPDATE metadata
                     SET value = '[]', value_type = 'list', last_timestamp = ?1
                     WHERE rowid = ?2",
                    params![timestamp, metadata_id],
                )?;

                self.conn.execute(
                    "INSERT INTO metadata_log (target_type, target_value, key, value, value_type, operation, email, timestamp)
                     VALUES (?1, ?2, ?3, ?4, 'list', 'list:rm', ?5, ?6)",
                    params![target_type_str, target_value, key, &new_value, email, timestamp],
                )?;

                self.conn.execute(
                    "DELETE FROM tombstones
                     WHERE tombstone_type = 'metadata' AND target_type = ?1 AND target_value = ?2 AND key = ?3",
                    params![target_type_str, target_value, key],
                )?;

                sp.commit()?;

                Ok(())
            }
            None => Err(Error::KeyNotFound {
                key: key.to_string(),
            }),
        }
    }
}
