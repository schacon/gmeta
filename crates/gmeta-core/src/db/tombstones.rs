use rusqlite::{params, OptionalExtension};

use crate::error::Result;

use super::Store;
use crate::types::TargetType;

impl Store {
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
        let sp = self.savepoint()?;

        let metadata_id = self
            .conn
            .query_row(
                "SELECT rowid FROM metadata
                 WHERE target_type = ?1 AND target_value = ?2 AND key = ?3",
                params![target_type_str, target_value, key],
                |row| row.get::<_, i64>(0),
            )
            .optional()?;
        if let Some(metadata_id) = metadata_id {
            self.conn.execute(
                "DELETE FROM list_values WHERE metadata_id = ?1",
                params![metadata_id],
            )?;
            self.conn.execute(
                "DELETE FROM set_values WHERE metadata_id = ?1",
                params![metadata_id],
            )?;
            self.conn.execute(
                "DELETE FROM metadata WHERE rowid = ?1",
                params![metadata_id],
            )?;
        }
        self.conn.execute(
            "DELETE FROM tombstones WHERE tombstone_type = 'set_member' AND target_type = ?1 AND target_value = ?2 AND key = ?3",
            params![target_type_str, target_value, key],
        )?;

        self.conn.execute(
            "INSERT INTO tombstones (tombstone_type, target_type, target_value, key, entry_id, value, timestamp, email)
             VALUES ('metadata', ?1, ?2, ?3, '', '', ?4, ?5)
             ON CONFLICT(tombstone_type, target_type, target_value, key, entry_id) DO UPDATE
             SET timestamp = excluded.timestamp, email = excluded.email",
            params![target_type_str, target_value, key, timestamp, email],
        )?;

        self.conn.execute(
            "INSERT INTO metadata_log (target_type, target_value, key, value, value_type, operation, email, timestamp)
             VALUES (?1, ?2, ?3, '', '', 'rm', ?4, ?5)",
            params![target_type_str, target_value, key, email, timestamp],
        )?;

        sp.commit()?;
        Ok(())
    }

    /// Get all tombstones for serialization.
    pub fn get_all_tombstones(&self) -> Result<Vec<super::types::TombstoneRecord>> {
        use super::types::TombstoneRecord;
        let mut stmt = self.conn.prepare(
            "SELECT target_type, target_value, key, timestamp, email
             FROM tombstones
             WHERE tombstone_type = 'metadata'
             ORDER BY target_type, target_value, key",
        )?;

        let rows = stmt.query_map([], |row| {
            Ok(TombstoneRecord {
                target_type: row.get(0)?,
                target_value: row.get(1)?,
                key: row.get(2)?,
                timestamp: row.get(3)?,
                email: row.get(4)?,
            })
        })?;

        let mut results = Vec::new();
        for row in rows {
            results.push(row?);
        }
        Ok(results)
    }

    /// Get all set member tombstones for serialization.
    pub fn get_all_set_tombstones(&self) -> Result<Vec<super::types::SetTombstoneRecord>> {
        use super::types::SetTombstoneRecord;
        let mut stmt = self.conn.prepare(
            "SELECT target_type, target_value, key, entry_id, value, timestamp, email
             FROM tombstones
             WHERE tombstone_type = 'set_member'
             ORDER BY target_type, target_value, key, entry_id",
        )?;

        let rows = stmt.query_map([], |row| {
            Ok(SetTombstoneRecord {
                target_type: row.get(0)?,
                target_value: row.get(1)?,
                key: row.get(2)?,
                member_id: row.get(3)?,
                value: row.get(4)?,
                timestamp: row.get(5)?,
                email: row.get(6)?,
            })
        })?;

        let mut results = Vec::new();
        for row in rows {
            results.push(row?);
        }
        Ok(results)
    }

    /// Get all list entry tombstones for serialization.
    pub fn get_all_list_tombstones(&self) -> Result<Vec<super::types::ListTombstoneRecord>> {
        use super::types::ListTombstoneRecord;
        let mut stmt = self.conn.prepare(
            "SELECT target_type, target_value, key, entry_id, timestamp, email
             FROM tombstones
             WHERE tombstone_type = 'list_entry'
             ORDER BY target_type, target_value, key, entry_id",
        )?;
        let rows = stmt.query_map([], |row| {
            Ok(ListTombstoneRecord {
                target_type: row.get(0)?,
                target_value: row.get(1)?,
                key: row.get(2)?,
                entry_name: row.get(3)?,
                timestamp: row.get(4)?,
                email: row.get(5)?,
            })
        })?;
        let mut results = Vec::new();
        for row in rows {
            results.push(row?);
        }
        Ok(results)
    }
}
