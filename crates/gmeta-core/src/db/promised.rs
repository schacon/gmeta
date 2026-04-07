use anyhow::Result;
use rusqlite::params;

use super::Db;
use crate::types::{TargetType, ValueType};

impl Db {
    /// Insert a "promised" entry -- we know this key exists in the remote history
    /// but we haven't fetched the blob data yet. Uses INSERT OR IGNORE so existing
    /// entries (e.g. from tip materialization) are never overwritten.
    /// Returns Ok(true) if a row was inserted, Ok(false) if it already existed.
    pub fn insert_promised(
        &self,
        target_type: &TargetType,
        target_value: &str,
        key: &str,
        value_type: &ValueType,
    ) -> Result<bool> {
        let rows = self.conn.execute(
            "INSERT OR IGNORE INTO metadata (target_type, target_value, key, value, value_type, last_timestamp, is_git_ref, is_promised)
             VALUES (?1, ?2, ?3, '', ?4, 0, 0, 1)",
            params![target_type.as_str(), target_value, key, value_type.as_str()],
        )?;
        Ok(rows > 0)
    }

    /// Resolve a promised entry by filling in the real value and clearing the flag.
    pub fn resolve_promised(
        &self,
        target_type: &TargetType,
        target_value: &str,
        key: &str,
        value: &str,
        value_type: &ValueType,
        is_git_ref: bool,
    ) -> Result<()> {
        let git_ref_val: i64 = if is_git_ref { 1 } else { 0 };
        self.conn.execute(
            "UPDATE metadata SET value = ?4, value_type = ?5, is_git_ref = ?6, is_promised = 0
             WHERE target_type = ?1 AND target_value = ?2 AND key = ?3 AND is_promised = 1",
            params![
                target_type.as_str(),
                target_value,
                key,
                value,
                value_type.as_str(),
                git_ref_val
            ],
        )?;
        Ok(())
    }

    /// Delete a promised entry (e.g. if the key no longer exists in the tip tree).
    pub fn delete_promised(
        &self,
        target_type: &TargetType,
        target_value: &str,
        key: &str,
    ) -> Result<()> {
        self.conn.execute(
            "DELETE FROM metadata WHERE target_type = ?1 AND target_value = ?2 AND key = ?3 AND is_promised = 1",
            params![target_type.as_str(), target_value, key],
        )?;
        Ok(())
    }

    /// Count promised (not-yet-hydrated) keys, grouped by target_type.
    pub fn count_promised_keys(&self) -> Result<Vec<(String, u64)>> {
        let mut stmt = self.conn.prepare(
            "SELECT target_type, COUNT(*) FROM metadata WHERE is_promised = 1 GROUP BY target_type ORDER BY target_type",
        )?;
        let rows = stmt.query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, u64>(1)?))
        })?;
        let mut results = Vec::new();
        for row in rows {
            results.push(row?);
        }
        Ok(results)
    }

    /// Get all promised (not-yet-hydrated) keys.
    /// Returns (target_type, target_value, key).
    pub fn get_promised_keys(&self) -> Result<Vec<(String, String, String)>> {
        let mut stmt = self.conn.prepare(
            "SELECT target_type, target_value, key FROM metadata WHERE is_promised = 1 ORDER BY target_type, target_value, key",
        )?;
        let rows = stmt.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
            ))
        })?;
        let mut results = Vec::new();
        for row in rows {
            results.push(row?);
        }
        Ok(results)
    }
}
