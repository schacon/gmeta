use anyhow::Result;
use rusqlite::{params, OptionalExtension};

use super::{encode_list_entries_by_metadata_id, encode_set_values_by_metadata_id, Db};
use crate::types::ValueType;

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
}
