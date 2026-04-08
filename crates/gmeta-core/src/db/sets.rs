use rusqlite::{params, OptionalExtension};

use crate::error::{Error, Result};

use super::{encode_set_values_by_metadata_id, Store};
use crate::types::TargetType;

impl Store {
    /// Remove a member from a set.
    pub fn set_remove(
        &self,
        target_type: &TargetType,
        target_value: &str,
        key: &str,
        value: &str,
        email: &str,
        timestamp: i64,
    ) -> Result<()> {
        let target_type_str = target_type.as_str();
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
                if current_type != "set" {
                    return Err(Error::TypeMismatch {
                        key: key.to_string(),
                        expected: "set".into(),
                    });
                }

                let member_id = crate::types::set_member_id(value);
                let deleted = self.conn.execute(
                    "DELETE FROM set_values WHERE metadata_id = ?1 AND member_id = ?2",
                    params![metadata_id, member_id],
                )?;

                if deleted == 0 {
                    return Err(Error::ValueNotFound(format!("'{value}' not found in set")));
                }

                self.conn.execute(
                    "UPDATE metadata
                     SET value = '[]', value_type = 'set', last_timestamp = ?1
                     WHERE rowid = ?2",
                    params![timestamp, metadata_id],
                )?;

                self.conn.execute(
                    "INSERT INTO tombstones (tombstone_type, target_type, target_value, key, entry_id, value, timestamp, email)
                     VALUES ('set_member', ?1, ?2, ?3, ?4, ?5, ?6, ?7)
                     ON CONFLICT(tombstone_type, target_type, target_value, key, entry_id) DO UPDATE
                     SET value = excluded.value, timestamp = excluded.timestamp, email = excluded.email",
                    params![target_type_str, target_value, key, member_id, value, timestamp, email],
                )?;

                let new_value = encode_set_values_by_metadata_id(&self.conn, metadata_id)?;

                self.conn.execute(
                    "INSERT INTO metadata_log (target_type, target_value, key, value, value_type, operation, email, timestamp)
                     VALUES (?1, ?2, ?3, ?4, 'set', 'set:rm', ?5, ?6)",
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

    /// Add a member to a set.
    pub fn set_add(
        &self,
        target_type: &TargetType,
        target_value: &str,
        key: &str,
        value: &str,
        email: &str,
        timestamp: i64,
    ) -> Result<()> {
        let target_type_str = target_type.as_str();
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

        let metadata_id = match existing {
            Some((metadata_id, current_type)) => {
                if current_type != "set" {
                    return Err(Error::TypeMismatch {
                        key: key.to_string(),
                        expected: "set".into(),
                    });
                }

                self.conn.execute(
                    "UPDATE metadata
                     SET value = '[]', value_type = 'set', last_timestamp = ?1
                     WHERE rowid = ?2",
                    params![timestamp, metadata_id],
                )?;
                metadata_id
            }
            None => {
                self.conn.execute(
                    "INSERT INTO metadata (target_type, target_value, key, value, value_type, last_timestamp)
                     VALUES (?1, ?2, ?3, '[]', 'set', ?4)",
                    params![target_type_str, target_value, key, timestamp],
                )?;
                self.conn.last_insert_rowid()
            }
        };

        let member_id = crate::types::set_member_id(value);
        self.conn.execute(
            "INSERT INTO set_values (metadata_id, member_id, value, timestamp)
             VALUES (?1, ?2, ?3, ?4)
             ON CONFLICT(metadata_id, member_id) DO UPDATE
             SET value = excluded.value, timestamp = excluded.timestamp",
            params![metadata_id, member_id, value, timestamp],
        )?;

        self.conn.execute(
            "DELETE FROM tombstones WHERE tombstone_type = 'set_member' AND target_type = ?1 AND target_value = ?2 AND key = ?3 AND entry_id = ?4",
            params![target_type_str, target_value, key, member_id],
        )?;

        let new_value = encode_set_values_by_metadata_id(&self.conn, metadata_id)?;

        self.conn.execute(
            "INSERT INTO metadata_log (target_type, target_value, key, value, value_type, operation, email, timestamp)
             VALUES (?1, ?2, ?3, ?4, 'set', 'set:add', ?5, ?6)",
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
}
