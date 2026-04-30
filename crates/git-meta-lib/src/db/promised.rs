use rusqlite::params;

use crate::error::Result;

use super::Store;
use crate::types::{Target, ValueType};

impl Store {
    /// Insert a "promised" entry -- we know this key exists in the remote history
    /// but we haven't fetched the blob data yet. Uses INSERT OR IGNORE so existing
    /// entries (e.g. from tip materialization) are never overwritten.
    ///
    /// # Parameters
    ///
    /// - `target`: the metadata target
    /// - `key`: the metadata key name
    /// - `value_type`: the type of value (string, list, set)
    ///
    /// # Returns
    ///
    /// `Ok(true)` if a row was inserted, `Ok(false)` if it already existed.
    pub fn insert_promised(
        &self,
        target: &Target,
        key: &str,
        value_type: &ValueType,
    ) -> Result<bool> {
        let target_type_str = target.target_type().as_str();
        let target_value = target.value().unwrap_or("");
        let rows = self.conn.execute(
            "INSERT OR IGNORE INTO metadata (target_type, target_value, key, value, value_type, last_timestamp, is_git_ref, is_promised)
             VALUES (?1, ?2, ?3, '', ?4, 0, 0, 1)",
            params![target_type_str, target_value, key, value_type.as_str()],
        )?;
        Ok(rows > 0)
    }

    /// Resolve a promised entry by filling in the real value and clearing the flag.
    ///
    /// # Parameters
    ///
    /// - `target`: the metadata target
    /// - `key`: the metadata key name
    /// - `value`: the resolved value content
    /// - `value_type`: the type of value (string, list, set)
    /// - `is_git_ref`: whether the value is a git blob SHA reference
    #[cfg(feature = "internal")]
    pub fn resolve_promised(
        &self,
        target: &Target,
        key: &str,
        value: &str,
        value_type: &ValueType,
        is_git_ref: bool,
    ) -> Result<()> {
        let target_type_str = target.target_type().as_str();
        let target_value = target.value().unwrap_or("");
        let git_ref_val: i64 = if is_git_ref { 1 } else { 0 };
        self.conn.execute(
            "UPDATE metadata SET value = ?4, value_type = ?5, is_git_ref = ?6, is_promised = 0
             WHERE target_type = ?1 AND target_value = ?2 AND key = ?3 AND is_promised = 1",
            params![
                target_type_str,
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
    ///
    /// # Parameters
    ///
    /// - `target`: the metadata target
    /// - `key`: the metadata key name
    #[cfg(feature = "internal")]
    pub fn delete_promised(&self, target: &Target, key: &str) -> Result<()> {
        let target_type_str = target.target_type().as_str();
        let target_value = target.value().unwrap_or("");
        self.conn.execute(
            "DELETE FROM metadata WHERE target_type = ?1 AND target_value = ?2 AND key = ?3 AND is_promised = 1",
            params![target_type_str, target_value, key],
        )?;
        Ok(())
    }

    /// Count promised (not-yet-hydrated) keys, grouped by target_type.
    #[cfg(feature = "internal")]
    pub fn count_promised_keys(&self) -> Result<Vec<(String, u64)>> {
        let mut stmt = self.conn.prepare(
            "SELECT target_type, COUNT(*) FROM metadata WHERE is_promised = 1 GROUP BY target_type ORDER BY target_type",
        )?;
        let rows = stmt.query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)? as u64))
        })?;
        let mut results = Vec::new();
        for row in rows {
            results.push(row?);
        }
        Ok(results)
    }

    /// Get all promised (not-yet-hydrated) keys.
    /// Returns (target_type, target_value, key).
    #[cfg(feature = "internal")]
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
