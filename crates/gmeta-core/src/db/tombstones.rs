use anyhow::Result;
use rusqlite::{params, OptionalExtension};

use super::Db;
use crate::types::TargetType;

impl Db {
    /// Apply a tombstone from exchange data:
    /// remove current value (if any), record tombstone, and log the operation.
    pub fn apply_tombstone(
        &self,
        target_type: &TargetType,
        target_value: &str,
        key: &str,
        email: &str,
        timestamp: i64,
    ) -> Result<()> {
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
        if let Some(metadata_id) = metadata_id {
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
            )?;
        }
        tx.execute(
            "DELETE FROM tombstones WHERE tombstone_type = 'set_member' AND target_type = ?1 AND target_value = ?2 AND key = ?3",
            params![target_type_str, target_value, key],
        )?;

        tx.execute(
            "INSERT INTO tombstones (tombstone_type, target_type, target_value, key, entry_id, value, timestamp, email)
             VALUES ('metadata', ?1, ?2, ?3, '', '', ?4, ?5)
             ON CONFLICT(tombstone_type, target_type, target_value, key, entry_id) DO UPDATE
             SET timestamp = excluded.timestamp, email = excluded.email",
            params![target_type_str, target_value, key, timestamp, email],
        )?;

        tx.execute(
            "INSERT INTO metadata_log (target_type, target_value, key, value, value_type, operation, email, timestamp)
             VALUES (?1, ?2, ?3, '', '', 'rm', ?4, ?5)",
            params![target_type_str, target_value, key, email, timestamp],
        )?;

        tx.commit()?;
        Ok(())
    }

    /// Get all tombstones for serialization.
    /// Returns (target_type, target_value, key, timestamp, email).
    pub fn get_all_tombstones(&self) -> Result<Vec<(String, String, String, i64, String)>> {
        let mut stmt = self.conn.prepare(
            "SELECT target_type, target_value, key, timestamp, email
             FROM tombstones
             WHERE tombstone_type = 'metadata'
             ORDER BY target_type, target_value, key",
        )?;

        let rows = stmt.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, i64>(3)?,
                row.get::<_, String>(4)?,
            ))
        })?;

        let mut results = Vec::new();
        for row in rows {
            results.push(row?);
        }
        Ok(results)
    }

    /// Get all set member tombstones for serialization.
    /// Returns (target_type, target_value, key, member_id, value, timestamp, email).
    pub fn get_all_set_tombstones(
        &self,
    ) -> Result<Vec<(String, String, String, String, String, i64, String)>> {
        let mut stmt = self.conn.prepare(
            "SELECT target_type, target_value, key, entry_id, value, timestamp, email
             FROM tombstones
             WHERE tombstone_type = 'set_member'
             ORDER BY target_type, target_value, key, entry_id",
        )?;

        let rows = stmt.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, String>(4)?,
                row.get::<_, i64>(5)?,
                row.get::<_, String>(6)?,
            ))
        })?;

        let mut results = Vec::new();
        for row in rows {
            results.push(row?);
        }
        Ok(results)
    }

    /// Get all list entry tombstones for serialization.
    /// Returns (target_type, target_value, key, entry_name, timestamp, email).
    pub fn get_all_list_tombstones(
        &self,
    ) -> Result<Vec<(String, String, String, String, i64, String)>> {
        let mut stmt = self.conn.prepare(
            "SELECT target_type, target_value, key, entry_id, timestamp, email
             FROM tombstones
             WHERE tombstone_type = 'list_entry'
             ORDER BY target_type, target_value, key, entry_id",
        )?;
        let rows = stmt.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, i64>(4)?,
                row.get::<_, String>(5)?,
            ))
        })?;
        let mut results = Vec::new();
        for row in rows {
            results.push(row?);
        }
        Ok(results)
    }
}
