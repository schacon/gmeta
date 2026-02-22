use anyhow::{bail, Result};
use rusqlite::{params, Connection};
use std::path::Path;

pub struct Db {
    pub conn: Connection,
}

impl Db {
    pub fn open(path: &Path) -> Result<Self> {
        let conn = Connection::open(path)?;
        let db = Db { conn };
        db.init_schema()?;
        Ok(db)
    }

    #[cfg(test)]
    pub fn open_in_memory() -> Result<Self> {
        let conn = Connection::open_in_memory()?;
        let db = Db { conn };
        db.init_schema()?;
        Ok(db)
    }

    fn init_schema(&self) -> Result<()> {
        self.conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS metadata (
                target_type TEXT NOT NULL,
                target_value TEXT NOT NULL,
                key TEXT NOT NULL,
                value TEXT NOT NULL,
                value_type TEXT NOT NULL,
                last_timestamp INTEGER NOT NULL DEFAULT 0,
                UNIQUE(target_type, target_value, key)
            );

            CREATE TABLE IF NOT EXISTS metadata_log (
                target_type TEXT NOT NULL,
                target_value TEXT NOT NULL,
                key TEXT NOT NULL,
                value TEXT NOT NULL,
                value_type TEXT NOT NULL,
                operation TEXT NOT NULL,
                email TEXT NOT NULL,
                timestamp INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS sync_state (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                last_materialized INTEGER
            );

            INSERT OR IGNORE INTO sync_state (id, last_materialized) VALUES (1, NULL);",
        )?;
        Ok(())
    }

    /// Set a value (upsert). JSON-encodes the value for storage.
    pub fn set(
        &self,
        target_type: &str,
        target_value: &str,
        key: &str,
        value: &str,
        value_type: &str,
        email: &str,
        timestamp: i64,
    ) -> Result<()> {
        self.conn.execute(
            "INSERT INTO metadata (target_type, target_value, key, value, value_type, last_timestamp)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)
             ON CONFLICT(target_type, target_value, key) DO UPDATE
             SET value = excluded.value, value_type = excluded.value_type, last_timestamp = excluded.last_timestamp",
            params![target_type, target_value, key, value, value_type, timestamp],
        )?;

        self.conn.execute(
            "INSERT INTO metadata_log (target_type, target_value, key, value, value_type, operation, email, timestamp)
             VALUES (?1, ?2, ?3, ?4, ?5, 'set', ?6, ?7)",
            params![target_type, target_value, key, value, value_type, email, timestamp],
        )?;

        Ok(())
    }

    /// Get a single value by exact key.
    pub fn get(
        &self,
        target_type: &str,
        target_value: &str,
        key: &str,
    ) -> Result<Option<(String, String)>> {
        let mut stmt = self.conn.prepare(
            "SELECT value, value_type FROM metadata
             WHERE target_type = ?1 AND target_value = ?2 AND key = ?3",
        )?;

        let result = stmt
            .query_row(params![target_type, target_value, key], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })
            .optional()?;

        Ok(result)
    }

    /// Get all key/value pairs for a target, optionally filtered by key prefix.
    pub fn get_all(
        &self,
        target_type: &str,
        target_value: &str,
        key_prefix: Option<&str>,
    ) -> Result<Vec<(String, String, String)>> {
        let (sql, params_vec): (&str, Vec<Box<dyn rusqlite::types::ToSql>>) = match key_prefix {
            Some(prefix) => (
                "SELECT key, value, value_type FROM metadata
                 WHERE target_type = ?1 AND target_value = ?2
                 AND (key = ?3 OR key LIKE ?4)
                 ORDER BY key",
                vec![
                    Box::new(target_type.to_string()),
                    Box::new(target_value.to_string()),
                    Box::new(prefix.to_string()),
                    Box::new(format!("{}:%", prefix)),
                ],
            ),
            None => (
                "SELECT key, value, value_type FROM metadata
                 WHERE target_type = ?1 AND target_value = ?2
                 ORDER BY key",
                vec![
                    Box::new(target_type.to_string()),
                    Box::new(target_value.to_string()),
                ],
            ),
        };

        let mut stmt = self.conn.prepare(sql)?;
        let params_refs: Vec<&dyn rusqlite::types::ToSql> =
            params_vec.iter().map(|p| p.as_ref()).collect();
        let rows = stmt.query_map(params_refs.as_slice(), |row| {
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

    /// Get authorship info for a key from the log (most recent entry).
    pub fn get_authorship(
        &self,
        target_type: &str,
        target_value: &str,
        key: &str,
    ) -> Result<Option<(String, i64)>> {
        let mut stmt = self.conn.prepare(
            "SELECT email, timestamp FROM metadata_log
             WHERE target_type = ?1 AND target_value = ?2 AND key = ?3
             ORDER BY timestamp DESC LIMIT 1",
        )?;

        let result = stmt
            .query_row(params![target_type, target_value, key], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
            })
            .optional()?;

        Ok(result)
    }

    /// Remove a key.
    pub fn rm(
        &self,
        target_type: &str,
        target_value: &str,
        key: &str,
        email: &str,
        timestamp: i64,
    ) -> Result<bool> {
        let deleted = self.conn.execute(
            "DELETE FROM metadata WHERE target_type = ?1 AND target_value = ?2 AND key = ?3",
            params![target_type, target_value, key],
        )?;

        if deleted > 0 {
            self.conn.execute(
                "INSERT INTO metadata_log (target_type, target_value, key, value, value_type, operation, email, timestamp)
                 VALUES (?1, ?2, ?3, '', '', 'rm', ?4, ?5)",
                params![target_type, target_value, key, email, timestamp],
            )?;
        }

        Ok(deleted > 0)
    }

    /// Push a value onto a list. If the key is currently a string, convert to list first.
    pub fn list_push(
        &self,
        target_type: &str,
        target_value: &str,
        key: &str,
        value: &str,
        email: &str,
        timestamp: i64,
    ) -> Result<()> {
        let existing = self.get(target_type, target_value, key)?;

        let new_value = match existing {
            Some((current_val, current_type)) => {
                if current_type == "list" {
                    let mut list: Vec<serde_json::Value> = serde_json::from_str(&current_val)?;
                    list.push(serde_json::Value::String(value.to_string()));
                    serde_json::to_string(&list)?
                } else {
                    // Convert string to list
                    let current_str: String = serde_json::from_str(&current_val)?;
                    let list = vec![
                        serde_json::Value::String(current_str),
                        serde_json::Value::String(value.to_string()),
                    ];
                    serde_json::to_string(&list)?
                }
            }
            None => {
                serde_json::to_string(&vec![value])?
            }
        };

        self.conn.execute(
            "INSERT INTO metadata (target_type, target_value, key, value, value_type, last_timestamp)
             VALUES (?1, ?2, ?3, ?4, 'list', ?5)
             ON CONFLICT(target_type, target_value, key) DO UPDATE
             SET value = excluded.value, value_type = 'list', last_timestamp = excluded.last_timestamp",
            params![target_type, target_value, key, &new_value, timestamp],
        )?;

        self.conn.execute(
            "INSERT INTO metadata_log (target_type, target_value, key, value, value_type, operation, email, timestamp)
             VALUES (?1, ?2, ?3, ?4, 'list', 'push', ?5, ?6)",
            params![target_type, target_value, key, &new_value, email, timestamp],
        )?;

        Ok(())
    }

    /// Pop a value from a list.
    pub fn list_pop(
        &self,
        target_type: &str,
        target_value: &str,
        key: &str,
        value: &str,
        email: &str,
        timestamp: i64,
    ) -> Result<()> {
        let existing = self.get(target_type, target_value, key)?;

        match existing {
            Some((current_val, current_type)) => {
                if current_type != "list" {
                    bail!("key '{}' is not a list", key);
                }
                let mut list: Vec<serde_json::Value> = serde_json::from_str(&current_val)?;
                let target = serde_json::Value::String(value.to_string());
                if let Some(pos) = list.iter().rposition(|v| v == &target) {
                    list.remove(pos);
                } else {
                    bail!("value '{}' not found in list", value);
                }

                let new_value = serde_json::to_string(&list)?;

                self.conn.execute(
                    "UPDATE metadata SET value = ?1, last_timestamp = ?2 WHERE target_type = ?3 AND target_value = ?4 AND key = ?5",
                    params![&new_value, timestamp, target_type, target_value, key],
                )?;

                self.conn.execute(
                    "INSERT INTO metadata_log (target_type, target_value, key, value, value_type, operation, email, timestamp)
                     VALUES (?1, ?2, ?3, ?4, 'list', 'pop', ?5, ?6)",
                    params![target_type, target_value, key, &new_value, email, timestamp],
                )?;

                Ok(())
            }
            None => bail!("key '{}' not found", key),
        }
    }

    /// Get all metadata entries (for serialization).
    /// Returns (target_type, target_value, key, value, value_type, last_timestamp).
    pub fn get_all_metadata(&self) -> Result<Vec<(String, String, String, String, String, i64)>> {
        let mut stmt = self.conn.prepare(
            "SELECT target_type, target_value, key, value, value_type, last_timestamp FROM metadata ORDER BY target_type, target_value, key",
        )?;

        let rows = stmt.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, String>(4)?,
                row.get::<_, i64>(5)?,
            ))
        })?;

        let mut results = Vec::new();
        for row in rows {
            results.push(row?);
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

    /// Get the set of (target_type, target_value, key) that have been locally
    /// modified since a given timestamp.
    #[allow(dead_code)]
    pub fn get_locally_modified_keys(
        &self,
        since: Option<i64>,
    ) -> Result<std::collections::HashSet<(String, String, String)>> {
        use std::collections::HashSet;

        let since_ts = since.unwrap_or(0);
        let mut stmt = self.conn.prepare(
            "SELECT DISTINCT target_type, target_value, key
             FROM metadata_log
             WHERE timestamp > ?1",
        )?;

        let rows = stmt.query_map(params![since_ts], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
            ))
        })?;

        let mut result = HashSet::new();
        for row in rows {
            result.insert(row?);
        }
        Ok(result)
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

use rusqlite::OptionalExtension;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_set_and_get() {
        let db = Db::open_in_memory().unwrap();
        db.set("commit", "abc123", "agent:model", "\"claude-4.6\"", "string", "test@test.com", 1000)
            .unwrap();
        let result = db.get("commit", "abc123", "agent:model").unwrap();
        assert_eq!(
            result,
            Some(("\"claude-4.6\"".to_string(), "string".to_string()))
        );
    }

    #[test]
    fn test_set_upsert() {
        let db = Db::open_in_memory().unwrap();
        db.set("commit", "abc123", "key", "\"v1\"", "string", "a@b.com", 1000)
            .unwrap();
        db.set("commit", "abc123", "key", "\"v2\"", "string", "a@b.com", 2000)
            .unwrap();
        let result = db.get("commit", "abc123", "key").unwrap();
        assert_eq!(result, Some(("\"v2\"".to_string(), "string".to_string())));
    }

    #[test]
    fn test_get_all_with_prefix() {
        let db = Db::open_in_memory().unwrap();
        db.set("commit", "abc123", "agent:model", "\"claude\"", "string", "a@b.com", 1000)
            .unwrap();
        db.set("commit", "abc123", "agent:provider", "\"anthropic\"", "string", "a@b.com", 1000)
            .unwrap();
        db.set("commit", "abc123", "other", "\"val\"", "string", "a@b.com", 1000)
            .unwrap();

        let results = db.get_all("commit", "abc123", Some("agent")).unwrap();
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn test_rm() {
        let db = Db::open_in_memory().unwrap();
        db.set("commit", "abc123", "key", "\"val\"", "string", "a@b.com", 1000)
            .unwrap();
        assert!(db.rm("commit", "abc123", "key", "a@b.com", 2000).unwrap());
        assert_eq!(db.get("commit", "abc123", "key").unwrap(), None);
    }

    #[test]
    fn test_list_push() {
        let db = Db::open_in_memory().unwrap();
        db.list_push("commit", "abc123", "tags", "first", "a@b.com", 1000)
            .unwrap();
        db.list_push("commit", "abc123", "tags", "second", "a@b.com", 2000)
            .unwrap();
        let (val, vtype) = db.get("commit", "abc123", "tags").unwrap().unwrap();
        assert_eq!(vtype, "list");
        let list: Vec<String> = serde_json::from_str(&val).unwrap();
        assert_eq!(list, vec!["first", "second"]);
    }

    #[test]
    fn test_list_push_converts_string() {
        let db = Db::open_in_memory().unwrap();
        db.set("commit", "abc123", "key", "\"original\"", "string", "a@b.com", 1000)
            .unwrap();
        db.list_push("commit", "abc123", "key", "appended", "a@b.com", 2000)
            .unwrap();
        let (val, vtype) = db.get("commit", "abc123", "key").unwrap().unwrap();
        assert_eq!(vtype, "list");
        let list: Vec<String> = serde_json::from_str(&val).unwrap();
        assert_eq!(list, vec!["original", "appended"]);
    }

    #[test]
    fn test_list_pop() {
        let db = Db::open_in_memory().unwrap();
        db.list_push("commit", "abc123", "tags", "a", "a@b.com", 1000)
            .unwrap();
        db.list_push("commit", "abc123", "tags", "b", "a@b.com", 2000)
            .unwrap();
        db.list_pop("commit", "abc123", "tags", "b", "a@b.com", 3000)
            .unwrap();
        let (val, _) = db.get("commit", "abc123", "tags").unwrap().unwrap();
        let list: Vec<String> = serde_json::from_str(&val).unwrap();
        assert_eq!(list, vec!["a"]);
    }

    #[test]
    fn test_authorship() {
        let db = Db::open_in_memory().unwrap();
        db.set("commit", "abc123", "key", "\"val\"", "string", "user@example.com", 42000)
            .unwrap();
        let (email, ts) = db
            .get_authorship("commit", "abc123", "key")
            .unwrap()
            .unwrap();
        assert_eq!(email, "user@example.com");
        assert_eq!(ts, 42000);
    }

    #[test]
    fn test_sync_state() {
        let db = Db::open_in_memory().unwrap();
        assert_eq!(db.get_last_materialized().unwrap(), None);
        db.set_last_materialized(5000).unwrap();
        assert_eq!(db.get_last_materialized().unwrap(), Some(5000));
    }

    #[test]
    fn test_last_timestamp_stored_and_returned() {
        let db = Db::open_in_memory().unwrap();

        // set stores the timestamp
        db.set("commit", "abc123", "key", "\"val\"", "string", "a@b.com", 5000)
            .unwrap();
        let entries = db.get_all_metadata().unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].5, 5000);

        // upsert updates the timestamp
        db.set("commit", "abc123", "key", "\"val2\"", "string", "a@b.com", 9000)
            .unwrap();
        let entries = db.get_all_metadata().unwrap();
        assert_eq!(entries[0].5, 9000);

        // list_push stores the timestamp
        db.list_push("commit", "abc123", "tags", "first", "a@b.com", 11000)
            .unwrap();
        let entries = db.get_all_metadata().unwrap();
        let tags = entries.iter().find(|e| e.2 == "tags").unwrap();
        assert_eq!(tags.5, 11000);

        // list_pop updates the timestamp
        db.list_push("commit", "abc123", "tags", "second", "a@b.com", 12000)
            .unwrap();
        db.list_pop("commit", "abc123", "tags", "second", "a@b.com", 13000)
            .unwrap();
        let entries = db.get_all_metadata().unwrap();
        let tags = entries.iter().find(|e| e.2 == "tags").unwrap();
        assert_eq!(tags.5, 13000);
    }
}
